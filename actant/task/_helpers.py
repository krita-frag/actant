"""任务序列化 / 执行 / 事件广播辅助函数。

依赖 ``_context``（``get_task_context``），不依赖 ``AsyncResult`` / ``Task``，
避免循环导入。
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import pickle
import struct
import threading
import time
from collections.abc import Callable
from typing import Any, Literal, cast

import cloudpickle

from actant.capabilities import ExecuteCtx, ExecuteOutcome
from actant.exceptions import ActantError, SerializationError, TaskCancelledError
from actant.task._context import get_task_context

CallbackErrorPolicy = Literal["log", "raise", "collect"]

_logger = logging.getLogger("actant.task")


# ---------------------------------------------------------------------------
# 事件 batching：累积 TaskLifecycle 事件，按时间窗口或数量阈值批量派发。
#
# 设计动机：每个 task 触发 started + completed 两次 emit，对 event_bus 形成
# N×2 次独立 publish。在高吞吐场景（4 worker × 1000 tasks = 4000 events）
# 下，emit 调用本身成为瓶颈。Batching 通过累积事件到 buffer，按 1ms 窗口
# 或 100 个事件阈值触发一次批量派发，将 emit 次数降为 N/100。
#
# 使用方式：通过 ``_EventBatcherScope`` 上下文管理器在调用方作用域内
# 启用 batching；未启用时 ``_emit_task_event`` 直接同步 emit，零开销。
# ---------------------------------------------------------------------------

# close() 时 join 后台 flush 线程的超时时间（秒）。
# 暴露为模块常量便于测试覆盖超时分支（避免每次测试等待 5s）。
_EVENT_BATCHER_CLOSE_JOIN_TIMEOUT: float = 5.0

# Dispatch 载荷版本字节（与 `actant/task/_worker.py` 保持一致）。
# v2：控制元数据内联为紧凑二进制头部，载荷仅序列化 (func, args, kwargs)。
_PAYLOAD_VERSION = 0x02

# worker 帧协议上限——正常任务载荷（cloudpickle 函数 + 参数）远小于该值，
# 超限只可能是长度字段被腐蚀（按其读入体会造成无意义巨量分配）或载荷失控
# 膨胀。提交侧（_safe_serialize）与 worker 读帧侧共用此上限：前者快速失败，
# 后者按协议损坏处理。单点定义，worker（_worker.py）从这里导入。
MAX_FRAME_BYTES = 256 * 1024 * 1024


class _EventBatcher:
    """累积 TaskLifecycle 事件并按窗口/阈值批量派发。

    线程安全：内部 lock 保护 buffer。后台线程按 ``flush_interval_ms`` 周期
    触发 flush；调用方也可显式调用 ``flush()`` 强制立即派发。

    派发策略：每次 flush 将 buffer 中所有事件顺序 emit 到 ``TaskLifecycle``
    capability。emit 失败按 ``on_error="log"`` 处理（单事件失败不阻塞其他）。
    """

    def __init__(
        self,
        *,
        flush_interval_ms: int = 1,
        flush_threshold: int = 100,
        render: Callable[[str, str, str, dict[str, Any]], None] | None = None,
    ) -> None:
        if flush_interval_ms < 0:
            raise ValueError(f"flush_interval_ms must be >= 0, got {flush_interval_ms}")
        if flush_threshold < 1:
            raise ValueError(f"flush_threshold must be >= 1, got {flush_threshold}")
        self._flush_interval = flush_interval_ms / 1000.0
        self._flush_threshold = flush_threshold
        # 事件渲染器：``(kind, task_id, workflow_id, kwargs)``。用于 flush 发生在
        # 后台线程时，调用方依赖的能力——如把 ``TaskLifecycle`` 事件绑定到某个
        # Runtime 上下文中再 emit。``None`` 时使用默认 ``_emit_task_event``。
        self._render = render
        # (kind, task_id, workflow_id, kwargs) 四元组列表。
        self._buffer: list[tuple[str, str, str, dict[str, Any]]] = []
        self._lock = threading.Lock()
        self._closed = False
        # 后台 flush 线程：周期性触发 flush，避免低吞吐场景下事件滞留。
        # 仅在 flush_interval > 0 时启动；interval=0 表示仅按 threshold flush。
        self._flush_thread: threading.Thread | None = None
        self._wakeup = threading.Event()
        if self._flush_interval > 0:
            self._flush_thread = threading.Thread(
                target=self._flush_loop,
                name="actant-event-batcher",
                daemon=True,
            )
            self._flush_thread.start()

    def _flush_loop(self) -> None:
        """后台线程：按 flush_interval 周期触发 flush。"""
        while not self._closed:
            # 用 wait 而非 sleep，使 close() 能立即唤醒退出。
            # 被 set() 唤醒且 _closed=True 时退出循环（close 信号）。
            if self._wakeup.wait(timeout=self._flush_interval) and self._closed:
                return
            try:
                self.flush()
            except Exception:
                _logger.debug(
                    "EventBatcher: background flush raised",
                    exc_info=True,
                )

    def add(
        self,
        kind: str,
        task_id: str,
        workflow_id: str,
        **kwargs: Any,
    ) -> None:
        """添加一个事件到 buffer。若 buffer 达到 threshold，立即 flush。"""
        do_flush = False
        with self._lock:
            if self._closed:
                return
            self._buffer.append((kind, task_id, workflow_id, kwargs))
            if len(self._buffer) >= self._flush_threshold:
                do_flush = True
        if do_flush:
            # 在锁外 flush，避免 flush 持有锁时间过长阻塞 add。
            try:
                self.flush()
            except Exception:
                _logger.debug(
                    "EventBatcher: threshold flush raised",
                    exc_info=True,
                )

    def flush(self) -> None:
        """立即派发 buffer 中所有事件。"""
        with self._lock:
            if not self._buffer:
                return
            events = list(self._buffer)
            self._buffer.clear()
        # 在锁外派发，避免 emit 失败导致 buffer 锁死。
        # _bypass_batcher=True 防止事件回环：flush 调用 _emit_task_event 时
        # 若不绕过 batcher，事件会重新进 batcher.add（被 _closed 拒绝而丢失）。
        # 当提供了 render（如 Runtime 绑定的生命周期事件渲染器）时，改由 render
        # 负责在正确的运行时上下文中 emit；未提供时走默认 _emit_task_event。
        for kind, task_id, workflow_id, kwargs in events:
            try:
                if self._render is not None:
                    self._render(kind, task_id, workflow_id, kwargs)
                else:
                    _emit_task_event(
                        kind,
                        task_id,
                        workflow_id,
                        on_error="log",
                        _bypass_batcher=True,
                        **kwargs,
                    )
            except Exception:
                _logger.debug(
                    "EventBatcher: emit event kind=%s task=%s raised",
                    kind,
                    task_id,
                    exc_info=True,
                )

    def close(self) -> None:
        """关闭 batcher：停止后台线程并派发剩余事件。

        显式 join 后台 flush 线程（带 5s 超时），确保：

        - flush 线程不会在调用方返回后继续访问已释放的资源。
        - 进程正常退出路径下，所有缓冲事件已被 flush 线程处理完毕。
        - 异常场景（flush 线程卡在 emit 上）下，5s 超时避免无限阻塞调用方。

        join 之后再执行一次 ``flush()``：flush 线程退出前可能已将事件从 buffer
        取出但尚未完成 emit，此处的 flush 处理 flush 线程未来得及处理的新增事件
        （close 期间若有 add 调用会被 ``_closed`` 拒绝，但 close 前已入队的
        事件可能在 flush 线程退出与本次 flush 之间的竞争窗口中遗留）。
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
        # 唤醒 flush 线程使其看到 _closed=True 并退出。
        self._wakeup.set()
        # 显式 join：带超时防止 flush 线程卡死导致调用方永久阻塞。
        # 超时后放弃 join——daemon 线程会在主进程退出时由 OS 回收。
        if self._flush_thread is not None and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=_EVENT_BATCHER_CLOSE_JOIN_TIMEOUT)
            if self._flush_thread.is_alive():
                _logger.warning(
                    "EventBatcher: flush thread did not exit within %ss; "
                    "abandoning join (daemon thread will be reaped on process exit)",
                    _EVENT_BATCHER_CLOSE_JOIN_TIMEOUT,
                )
            self._flush_thread = None
        # 派发剩余事件（包括 flush 线程未处理完的）。
        self.flush()


# 线程局部：当前活跃的 EventBatcher（由 _EventBatcherScope 设置）。
# None 表示不启用 batching，_emit_task_event 走同步路径。
_event_batcher_local = threading.local()


def _get_event_batcher() -> _EventBatcher | None:
    """返回当前线程活跃的 EventBatcher，未启用返回 None。"""
    return getattr(_event_batcher_local, "batcher", None)


class _EventBatcherScope:
    """``with`` 上下文：在作用域内启用事件 batching。

    进入时创建并设置线程局部 batcher；退出时关闭 batcher 并派发剩余事件。
    可重入：嵌套 scope 重用外层 batcher（避免双重缓冲）。

    用法::

        with _EventBatcherScope(flush_interval_ms=1, flush_threshold=100):
            for i in range(1000):
                task.submit(i)  # 内部 emit 走 batcher
        # 退出时自动 flush 剩余事件
    """

    def __init__(
        self,
        *,
        flush_interval_ms: int = 1,
        flush_threshold: int = 100,
    ) -> None:
        self._flush_interval_ms = flush_interval_ms
        self._flush_threshold = flush_threshold
        self._batcher: _EventBatcher | None = None
        self._owns_batcher = False

    def __enter__(self) -> _EventBatcher:
        prev: _EventBatcher | None = getattr(_event_batcher_local, "batcher", None)
        if prev is not None:
            # 可重入：重用外层 batcher，避免双重缓冲。
            self._batcher = prev
            self._owns_batcher = False
            return prev
        batcher = _EventBatcher(
            flush_interval_ms=self._flush_interval_ms,
            flush_threshold=self._flush_threshold,
        )
        _event_batcher_local.batcher = batcher
        self._batcher = batcher
        self._owns_batcher = True
        return batcher

    def __exit__(self, *exc: object) -> None:
        if self._owns_batcher and self._batcher is not None:
            self._batcher.close()
            _event_batcher_local.batcher = None
        self._batcher = None
        self._owns_batcher = False


# 进程级复用的 asyncio 事件循环：worker 子进程主线程首个 async 任务懒创建，
# 后续任务复用，避免每任务 new_event_loop + close 的重复开销。
_worker_reuse_loop: asyncio.AbstractEventLoop | None = None


def _get_worker_reuse_loop() -> asyncio.AbstractEventLoop:
    """返回 worker 进程级复用的 asyncio 事件循环（懒创建）。"""
    global _worker_reuse_loop
    if _worker_reuse_loop is None:
        _worker_reuse_loop = asyncio.new_event_loop()
    return _worker_reuse_loop


def _cancel_pending_loop_tasks(loop: asyncio.AbstractEventLoop) -> None:
    """取消 loop 中遗留的 pending 任务并等待其结束，供下次复用前清理。

    复用 loop 前必须取消宿主 ``run_until_complete`` 未能 await 的后台任务，
    否则它们会残留到下一次执行，造成 "Task was destroyed but it is pending"
    告警或意外的并发。
    """
    try:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    except Exception:
        # 清理失败不应向任务执行路径传播（coro 本身的结果/异常优先），但不可静默。
        _logger.debug(
            "_cancel_pending_loop_tasks: cleanup raised",
            exc_info=True,
        )


def _is_coroutine_function(func: Any) -> bool:
    """检测 ``func`` 是否为 ``async def`` 函数。

    使用 ``asyncio.iscoroutinefunction``，覆盖原生 coroutine 与
    ``functools.partial`` 包裹的 coroutine（后者需手动 unwrap）。
    """

    # asyncio.iscoroutinefunction 检测原生 async def
    if asyncio.iscoroutinefunction(func):
        return True
    # unwrap functools.partial / decorate 包装层
    unwrapped = inspect.unwrap(func) if hasattr(func, "__wrapped__") else func
    if unwrapped is not func and asyncio.iscoroutinefunction(unwrapped):
        return True
    # functools.partial 包装的 coroutine function
    if isinstance(func, functools.partial):
        return asyncio.iscoroutinefunction(func.func)
    return False


def _run_coroutine_on_worker_thread(coro: Any) -> Any:
    """在 worker 子进程主线程上同步执行 coroutine，返回结果或抛出异常。

    worker 子进程是纯 Python 解释器，主线程无运行中的 asyncio event loop，
    因此可创建临时 loop 并 ``run_until_complete``。

    **性能：** 标准路径复用一条懒创建的进程级 loop（``_get_worker_reuse_loop``），
    避免每个 async 任务都 ``new_event_loop`` + 清理 + ``close`` 的重复开销；
    每次执行后由 ``_cancel_pending_loop_tasks`` 取消宿主 task 遗留的后台任务，
    保证下次复用前 loop 干净。

    若当前线程已有运行中的 loop（防御性处理，worker 主线程正常不会发生），
    在独立线程中执行 coroutine，避免嵌套 ``run_until_complete`` 报错。

    Args:
        coro: 已创建的 coroutine 对象（如 ``async_func(*args)`` 的返回值）。

    Returns:
        coroutine 的 return 值。

    Raises:
        coroutine 内部抛出的任何异常原样向上传播。
    """
    # 检测当前线程是否已有运行中的 loop（如嵌套 dispatch 场景）
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if running_loop is None:
        # 标准路径：worker 主线程无运行 loop，复用懒创建的进程级 loop。
        loop = _get_worker_reuse_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            # 取消宿主 task 遗留的后台任务并等待其结束，避免污染下一次复用。
            _cancel_pending_loop_tasks(loop)
    else:
        # 已有运行 loop：在独立线程中执行，避免嵌套 run_until_complete
        # 此路径不应在 worker 主线程触发，仅为防御性处理
        result_box: list[Any] = [None]
        error_box: list[BaseException] = []

        def _runner() -> None:
            new_loop = asyncio.new_event_loop()
            try:
                result_box[0] = new_loop.run_until_complete(coro)
            except BaseException as e:
                error_box.append(e)
            finally:
                new_loop.close()

        t = threading.Thread(target=_runner, name="actant-async-task-runner")
        t.start()
        t.join()
        if error_box:
            raise error_box[0]
        return result_box[0]


def _run_with_timeout(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    timeout_ms: int,  # 保留以兼容调用方签名；超时由 Rust 侧强制执行。
) -> Any:
    """在 worker 子进程主线程同步执行 ``func``，仅承担协作取消检查点。

    执行模型：
      - 本函数 **不** 在 Python 侧创建子线程或实施任何超时。``func`` 直接在当前
        worker 子进程的主线程中同步执行。
      - 硬超时由 Rust 进程池强制：``ProcessTaskDispatcher.dispatch(...,
        effective_timeout)`` 以 ``effective_timeout`` 为硬上限，超时后立即
        ``terminate()``/``kill()`` 对应的 worker 子进程并回收并发槽位，返回
        ``Err(ActantError::Timeout)``。worker 侧不再套内层超时。
      - Python 无法被强制中断，因此 ``func`` 内部应在长循环处调用
        ``get_task_context().is_cancelled()`` 或使用 ``_interruptible_sleep``
        实现协作式取消，以便在硬杀到来前干净退出。

    本函数的职责只是 **协作检查点**：
      1. 执行前检查取消标志——若已取消（例如 dispatch 启动前上层已取消），
         立即抛出 ``TaskCancelledError``，避免无效工作。
      2. 执行后再次检查——若 ``func`` 期间收到取消，将结果丢弃并抛出
         ``TaskCancelledError``，防止被取消任务返回"成功"结果。

    **async def 支持**：若 ``func`` 是 coroutine function（``async def``），
    在当前 worker 主线程上复用懒创建的进程级 event loop（
    ``_get_worker_reuse_loop``）执行 coroutine。worker 主线程无运行中的
    asyncio loop，因此 ``run_until_complete`` 可安全使用；loop 执行完毕后
    **不关闭**（供后续任务复用），仅由 ``_cancel_pending_loop_tasks`` 清理
    遗留的 pending 任务。本函数不做任何超时实施——硬超时统一由 Rust
    进程池对 worker 子进程强杀完成。

    Args:
        func: 待执行的业务函数（已反序列化）。可以是普通函数或 ``async def``。
        args: 位置参数。
        kwargs: 关键字参数。
        timeout_ms: **未使用**。硬超时由进程池经 ``effective_timeout`` 强制执行。
            此参数仅为保持调用方签名稳定而保留。

    Returns:
        ``func`` 的返回值（对于 ``async def``，是 coroutine 的 return 值，
        而非 coroutine 对象本身）。

    Raises:
        TaskCancelledError: 执行前或执行后检测到取消标志。
        Exception: ``func`` 自身抛出的任何异常原样向上传播。
    """
    ctx = get_task_context()
    if ctx is not None and ctx.is_cancelled():
        raise TaskCancelledError(f"task {ctx.task_id!r} was cancelled before start")
    if _is_coroutine_function(func):
        # async def 函数：复用进程级 event loop 在当前 worker 主线程执行（见
        # _run_coroutine_on_worker_thread）；loop 不关闭、本函数不做超时，
        # 硬超时由 Rust 进程池强杀实施。
        coro = func(*args, **kwargs)
        result = _run_coroutine_on_worker_thread(coro)
    else:
        result = func(*args, **kwargs)
    if ctx is not None and ctx.is_cancelled():
        raise TaskCancelledError(f"task {ctx.task_id!r} was cancelled during execution")
    return result


def _interruptible_sleep(
    duration: float,
    cancel_token: Any,
    *,
    interval: float = 0.05,
) -> None:
    """可中断的 sleep：在 duration 期间分段轮询 cancel_token。

    若 ``cancel_token`` 报告已取消，立即返回，使调用方能在下次循环顶部
    的取消检查点退出。避免重试延迟期间无法响应取消请求。

    Args:
        duration: 总睡眠时长（秒）。
        cancel_token: 拥有 ``is_cancelled() -> bool`` 方法的取消令牌。
        interval: 轮询间隔（秒），默认 50ms。
    """
    if duration <= 0:
        return
    deadline = time.monotonic() + duration
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        if cancel_token is not None and cancel_token.is_cancelled():
            return
        time.sleep(min(interval, remaining))


def _execute_with_retries(
    func: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    timeout_ms: int,
    retries: int,
    retry_delay_ms: int,
    task_id: str,
    workflow_id: str,
    cancel_token: Any,
    *,
    silent: bool = False,
) -> tuple[bool, Any]:
    """执行任务函数并处理重试/取消/超时。

    重试循环：每次尝试前检查取消，调用 ``_run_with_timeout`` 执行函数。
    失败时若仍有重试次数，则等待可中断的 ``retry_delay_ms`` 后重试；
    否则返回异常对象。成功时返回结果对象。

    执行位置：worker 子进程主线程（``actant.task._worker`` 的唯一执行路径）。

    Args:
        silent: ``True`` 时跳过所有 TaskLifecycle 事件发布。

    Returns:
        ``(success, payload)`` 元组：
        - 成功：``(True, result_obj)`` —— result_obj 是任务返回值（未序列化）。
        - 失败：``(False, exc_obj)`` —— exc_obj 是异常对象（已通过
          ``_ensure_picklable`` 处理，确保可序列化）。

        调用方负责序列化（``cloudpickle.dumps``）以跨 Rust 边界传递。
    """
    attempt = 0
    while True:
        # 协作式取消检查
        if cancel_token.is_cancelled():
            cancel_exc = TaskCancelledError(f"task {task_id!r} was cancelled")
            _emit_task_event(
                "cancelled",
                task_id,
                workflow_id,
                attempt=attempt,
                error=f"{type(cancel_exc).__name__}: {cancel_exc}",
                silent=silent,
            )
            return False, _ensure_picklable(cancel_exc)
        try:
            result = _run_with_timeout(func, args, kwargs, timeout_ms)
        except TaskCancelledError as exc:
            _logger.warning("task %s: cancelled", task_id)
            _emit_task_event(
                "cancelled",
                task_id,
                workflow_id,
                attempt=attempt,
                error=f"{type(exc).__name__}: {exc}",
                silent=silent,
            )
            return False, _ensure_picklable(exc)
        except Exception as exc:
            if attempt < retries:
                _logger.warning(
                    "task %s: attempt %d failed (%s: %s), retrying",
                    task_id,
                    attempt,
                    type(exc).__name__,
                    exc,
                )
                _emit_task_event(
                    "retried",
                    task_id,
                    workflow_id,
                    attempt=attempt,
                    next_attempt=attempt + 1,
                    error=f"{type(exc).__name__}: {exc}",
                    silent=silent,
                )
                if retry_delay_ms > 0:
                    # 可中断的重试延迟：分段轮询 cancel_token，
                    # 避免重试延迟期间无法响应取消请求。
                    _interruptible_sleep(
                        retry_delay_ms / 1000,
                        cancel_token,
                        interval=0.05,
                    )
                attempt += 1
                continue
            _logger.error(
                "task %s: failed after %d attempt(s)", task_id, attempt + 1, exc_info=True
            )
            _emit_task_event(
                "failed",
                task_id,
                workflow_id,
                attempt=attempt,
                error=f"{type(exc).__name__}: {exc}",
                silent=silent,
            )
            return False, _ensure_picklable(exc)
        _emit_task_event("completed", task_id, workflow_id, attempt=attempt, silent=silent)
        return True, result


def _build_v2_envelope(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    options: dict[str, Any],
    task_id: str,
) -> bytes:
    """构建 v2 Dispatch 正文：紧凑控制头部 + ``cloudpickle(func, args, kwargs)``。

    头部布局（小端，与 ``actant/task/_worker.py::_parse_dispatch_payload`` 一致）：:

        u8   version   = ``_PAYLOAD_VERSION`` (0x02)
        u32  retries
        u32  retry_delay_ms
        u16  task_id_len ; N 字节 task_id (utf-8)
        u16  workflow_id_len ; N 字节 workflow_id (utf-8)
        其余 = ``cloudpickle(func, args, kwargs)``

    把控制元数据迁出 cloudpickle 载荷：worker 侧用 ``struct`` 单遍解析头部，只需对
    ``(func,args,kwargs)`` 反序列化，省去每任务 ``options`` dict 的编解码与字典构造。
    ``timeout_ms`` 为死参数（硬超时由 Rust 进程池强杀负责）不进入头部。
    """
    retries = int(options.get("retries", 0))
    retry_delay_ms = int(options.get("retry_delay_ms", 0))
    tid = str(options.get("task_id") or task_id).encode("utf-8")
    wid = str(options.get("workflow_id") or "").encode("utf-8")
    header = b"".join(
        [
            struct.pack("<BIIH", _PAYLOAD_VERSION, retries, retry_delay_ms, len(tid)),
            tid,
            struct.pack("<H", len(wid)),
            wid,
        ]
    )
    triplet = cast(bytes, cloudpickle.dumps((func, args, kwargs)))
    return header + triplet


def _safe_serialize(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    options: dict[str, Any],
    *,
    task_id: str,
) -> bytes:
    """序列化任务 payload，失败时定位不可序列化的参数。

    cloudpickle 失败时原始错误消息（如 ``"can't pickle _thread.lock object"``）
    不指明是哪个参数。此函数逐个尝试序列化 func/各 args/各 kwargs values，
    在错误消息中包含参数索引/名称与 repr，帮助用户定位问题。

    返回 v2 envelope：``(_build_v2_envelope)``。控制元数据（retries/retry_delay_ms/
    task_id/workflow_id）内联为紧凑头部，仅 ``(func,args,kwargs)`` 走 cloudpickle。

    envelope 超过 ``MAX_FRAME_BYTES``（worker 帧协议上限）时抛 ``SerializationError``：
    超限载荷会被 worker 以协议损坏拒绝并触发无意义的 crash-failover 重试，在提交侧
    快速失败并指明上限更可诊断。大载荷应经 ``Ref``/``ValueStore`` 值引用传递
    （0.3.2 R2/R3；超过 ``REF_INLINE_THRESHOLD`` 的参数已自动降级，见
    ``actant/task/_ref.py``）。
    """
    try:
        envelope = _build_v2_envelope(func, args, kwargs, options, task_id)
    except (TypeError, AttributeError, ValueError, RecursionError) as overall:
        # 整体失败：逐个定位不可序列化的项
        bad: list[str] = []
        try:
            cloudpickle.dumps(func)
        except (TypeError, AttributeError, ValueError, RecursionError) as e:
            bad.append(f"func {func!r}: {e}")
        for i, a in enumerate(args):
            try:
                cloudpickle.dumps(a)
            except (TypeError, AttributeError, ValueError, RecursionError) as e:
                bad.append(f"args[{i}]={a!r}: {e}")
        for k, v in kwargs.items():
            try:
                cloudpickle.dumps(v)
            except (TypeError, AttributeError, ValueError, RecursionError) as e:
                bad.append(f"kwargs[{k!r}]={v!r}: {e}")
        detail = "; ".join(bad) if bad else f"unknown component: {overall}"
        raise SerializationError(
            f"task {task_id!r}: cannot serialize payload. Unserializable components: {detail}"
        ) from overall
    if len(envelope) > MAX_FRAME_BYTES:
        raise SerializationError(
            f"task {task_id!r}: serialized payload is {len(envelope)} bytes, exceeding the "
            f"{MAX_FRAME_BYTES} worker frame limit. Pass large data by reference "
            "(path/URL/object-store key) instead of embedding it in the payload."
        )
    return envelope


def _pickle_exception(exc: BaseException) -> bytes:
    """序列化异常，不可序列化时退化为携带类型与消息的 RuntimeError。"""
    try:
        return cast(bytes, cloudpickle.dumps(exc))
    except Exception:
        return cast(bytes, cloudpickle.dumps(RuntimeError(f"{type(exc).__name__}: {exc}")))


def _ensure_picklable(exc: BaseException) -> BaseException:
    """确保异常可序列化，不可序列化时退化为携带类型与消息的 RuntimeError。

    与 ``_pickle_exception`` 配套：``_pickle_exception`` 返回 bytes（用于
    直接存入 _error_payload），``_ensure_picklable`` 返回对象（用于
    ``_execute_with_retries`` 的 ``(False, exc_obj)`` 返回值，由调用方
    统一序列化跨 Rust 边界）。
    """
    try:
        cloudpickle.dumps(exc)
        return exc
    except Exception:
        return RuntimeError(f"{type(exc).__name__}: {exc}")


def _invoke_callback(
    fn: Callable[..., Any],
    *args: Any,
    label: str,
    policy: CallbackErrorPolicy = "log",
) -> Exception | None:
    """统一调用用户回调，按 ``policy`` 处理异常。

    - ``log``（默认）：记录 debug 日志并返回异常，不向上抛出。
    - ``raise``：立即向上抛出异常。
    - ``collect``：记录并返回异常，由调用方决定是否聚合抛出。

    返回捕获到的异常（若有）。
    """
    try:
        fn(*args)
    except Exception as e:
        if policy == "raise":
            raise
        _logger.debug("callback %s raised", label, exc_info=True)
        if policy == "collect":
            return e
    return None


def _invoke_callbacks(
    callbacks: list[Callable[..., Any]],
    *args: Any,
    label: str,
    policy: CallbackErrorPolicy = "log",
) -> list[Exception]:
    """批量调用回调，按 ``policy`` 收集/处理异常。

    ``collect`` 策略下收集所有异常并返回；``raise`` 策略下遇到首个异常即抛出；
    ``log`` 策略下记录并继续执行后续回调。
    """
    errors: list[Exception] = []
    for fn in callbacks:
        caught = _invoke_callback(
            fn, *args, label=f"{label}/{getattr(fn, '__name__', fn)}", policy=policy
        )
        if caught is not None:
            errors.append(caught)
    if policy == "collect" and errors:
        raise ActantError(f"{label}: {len(errors)} callback(s) failed; first: {errors[0]!r}")
    return errors


class _suppress_pickle_errors:
    """cloudpickle 反序列化失败时静默返回（用于 exception() 的 best-effort 解码）。

    仅抑制 pickle 反序列化相关的异常类型（``pickle.UnpicklingError`` /
    ``TypeError`` / ``AttributeError`` / ``ValueError``——cloudpickle.loads 对
    损坏载荷、不可解析的全局引用与错误码流抛出的类型族），其余异常照常抛出，
    不抑制 ``KeyboardInterrupt`` / ``SystemExit`` 等 ``BaseException``。
    """

    _SUPPRESSED = (
        pickle.UnpicklingError,
        TypeError,
        AttributeError,
        ValueError,
    )

    def __enter__(self) -> _suppress_pickle_errors:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        return exc_type is not None and issubclass(exc_type, self._SUPPRESSED)


def _emit_task_event(
    kind: str,
    task_id: str,
    workflow_id: str,
    *,
    attempt: int = 0,
    next_attempt: int = 0,
    error: str = "",
    on_error: CallbackErrorPolicy = "log",
    silent: bool = False,
    _bypass_batcher: bool = False,
) -> Exception | None:
    """广播 ``TaskLifecycle`` 事件，失败时按 ``on_error`` 策略处理。

    generic handler 在工作线程中执行，``emit`` 需要活跃 Runtime——已由
    ``Task.submit`` 的 ``use_runtime(rt)`` 传播。若 Runtime 未注册
    ``TaskLifecycle`` handler，emit 静默失败（``_dispatch_emit`` 对空 handler
    列表 + Rust-backed capability 会调用 Rust emit，无 Python handler 时不报错）。

    Args:
        on_error: ``log``（默认）仅记录 warning；``raise`` 立即抛出；
            ``collect`` 记录并返回异常。
        silent: ``True`` 时跳过事件发布（既不进 batcher 也不直接 emit）。
            用于 ``@task(silent=True)`` 等场景，避免每个 task 产生
            started/completed 事件造成 event_bus 噪声。批量提交/低优先级
            任务可启用此选项以提升吞吐。
        _bypass_batcher: 内部参数。``True`` 时绕过当前线程的 batcher 直接 emit，
            用于 batcher.flush() 避免事件回环（flush 调用 emit 时若不绕过，
            事件会重新进 batcher.add 被 _closed 拒绝而丢失）。

    Returns:
        捕获到的异常（仅当 ``on_error="collect"`` 时）。
    """
    if silent:
        return None
    # 若当前线程启用了 EventBatcher，路由到 batcher 而非直接 emit。
    # batcher 内部最终会回调本函数（_bypass_batcher=True）走直接 emit。
    if not _bypass_batcher:
        batcher = _get_event_batcher()
        if batcher is not None:
            batcher.add(
                kind,
                task_id,
                workflow_id,
                attempt=attempt,
                next_attempt=next_attempt,
                error=error,
            )
            return None
    try:
        from actant._effects import emit as _emit
        from actant.capabilities import TASK_LIFECYCLE, TaskEvent

        _emit(
            TASK_LIFECYCLE,
            TaskEvent(
                kind=kind,  # type: ignore[arg-type]
                task_id=task_id,
                workflow_id=workflow_id,
                attempt=attempt,
                next_attempt=next_attempt,
                error=error,
            ),
        )
    except Exception as e:
        if on_error == "raise":
            raise
        _logger.warning(
            "task %s: TaskLifecycle emit (%s) failed",
            task_id,
            kind,
            exc_info=True,
        )
        if on_error == "collect":
            return e
    return None


__all__ = [
    "CallbackErrorPolicy",
    "ExecuteCtx",
    "ExecuteOutcome",
    "_EventBatcher",
    "_EventBatcherScope",
    "_emit_task_event",
    "_ensure_picklable",
    "_execute_with_retries",
    "_interruptible_sleep",
    "_invoke_callback",
    "_invoke_callbacks",
    "_pickle_exception",
    "_run_with_timeout",
    "_safe_serialize",
    "_suppress_pickle_errors",
]
