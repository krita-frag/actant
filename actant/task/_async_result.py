"""``AsyncResult``：异步任务结果句柄与任务依赖解析。

依赖 ``_context``（TaskContext / TaskState）与 ``_helpers``
（``_suppress_pickle_errors``）。``_resolve_value`` 因紧耦合 ``AsyncResult``
亦置于本模块，供 ``Task.submit`` / ``gather`` 使用。

## Intrusively-linked Futures 设计

为支持 ``gather`` 高效等待多个 handle，``AsyncResult`` 内部维护一个
``_CompletionFuture``（基于 ``threading.Condition``）。所有等待同一 handle
的消费者共享同一个 future 实例：

- ``result()`` / ``wait()`` 通过 future 的 ``wait()`` 一次性阻塞等待，
  避免轮询。
- ``add_done_callback`` 注册的回调以 *intrusive linked-list* 形式串接
  在 future 上，完成时仅触发一次链表遍历。这使 ``gather(N)`` 的总调度
  开销从 O(N) 次独立 Event wait 降到 1 次 future wait + O(N) 次回调派发。

跨 handle 等待（``gather``）则借助一个 *共享* future：将所有 handle
的完成信号汇聚到单个 ``threading.Event``，``gather`` 仅等待该 event
一次，避免 N 次 ``wait_for`` 累加延迟。

## 结果内部形态（0.3.2 R4 统一）

``_result`` 单字段两态：小结果为对象缓存（``result()`` 直接返回）；大结果
（结果帧 > ``REF_INLINE_THRESHOLD``）为 ``Ref``（``result()`` 经 ValueStore
透明拉取反序列化）。访问路径只有 ``result()`` 一个方法；``ref()`` 暴露大
结果的 ``Ref`` 句柄。``__await__`` 经完成回调直通 event loop（无等待线程）。
"""

from __future__ import annotations

import asyncio
import logging
import pickle
import threading
from collections.abc import Callable
from typing import Any, cast

import cloudpickle

from actant._runtime import get_current_runtime
from actant.exceptions import ActantError, ActantTimeoutError, TaskCancelledError
from actant.task._context import TaskContext, TaskState
from actant.task._helpers import (
    _emit_task_event,
    _invoke_callback,
    _invoke_callbacks,
    _suppress_pickle_errors,
)
from actant.task._ref import Ref

_logger = logging.getLogger("actant.task")

# 任务终态集合：完成协议在持锁时先置终态、锁外才 set future，因此判断"是否
# 已完成"必须以 _state 终态为准而非 _future.is_set()（后者存在窗口）。
_TERMINAL_STATES = frozenset(("completed", "failed", "cancelled"))


class _CompletionFuture:
    """``AsyncResult`` 的底层完成信号。

    基于 ``threading.Condition`` 实现，支持：
    - ``wait(timeout)``：阻塞直到完成或超时。
    - ``set()``：标记完成并唤醒所有等待者。
    - ``is_set()``：非阻塞查询。

    与 ``threading.Event`` 的区别：``Condition`` 允许我们在同一锁下原子地
    设置完成状态并触发 intrusive callback 链，避免 Event + Lock 两次锁切换。
    所有 ``add_done_callback`` 注册的回调以 singly-linked list 形式 intrusive
    串接在 future 上——节点本身持有 next 指针，避免维护单独的 list 容器。
    """

    __slots__ = ("_callback_head", "_cond", "_done", "_lock")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._done = False
        # Intrusive linked list head: 每个节点是 (callback, next_node)。
        # None 表示链表为空。新回调插入头部（O(1)）。
        self._callback_head: tuple[Callable[[], None], Any] | None = None

    def is_set(self) -> bool:
        with self._lock:
            return self._done

    def wait(self, timeout: float | None) -> bool:
        """阻塞直到完成或超时。返回 ``True`` 若已完成。"""
        with self._cond:
            if self._done:
                return True
            self._cond.wait(timeout=timeout)
            return self._done

    def add_callback(self, cb: Callable[[], None]) -> None:
        """注册一个完成回调。若已完成，立即同步调用。"""
        invoke_now = False
        with self._lock:
            if self._done:
                invoke_now = True
            else:
                # 头插法：intrusive linked list。
                self._callback_head = (cb, self._callback_head)
        if invoke_now:
            cb()

    def set(self) -> None:
        """标记完成并唤醒所有等待者，触发 intrusive callback 链。"""
        with self._lock:
            if self._done:
                return
            self._done = True
            head = self._callback_head
            self._callback_head = None
            self._cond.notify_all()
        # 在锁外顺序触发回调，避免回调内再次获取锁导致死锁。
        while head is not None:
            cb, next_node = head
            head = next_node
            try:
                cb()
            except Exception:
                _logger.debug(
                    "CompletionFuture: callback %r raised",
                    getattr(cb, "__name__", cb),
                    exc_info=True,
                )


class AsyncResult:
    """异步任务结果句柄。

    基于 ``_CompletionFuture`` 实现，由 Rust event_bus 回调
    （``Runtime._on_task_result``）在任务完成时设置结果。

    分布式语义：任务由 Worker 执行，结果通过 P2P 网络回传后触发回调。

    ## 性能特性

    - ``result()`` / ``wait()``：通过 ``Condition`` 一次性等待，无轮询。
    - ``add_done_callback``：intrusive linked-list 注册，O(1) 插入。
    - ``gather`` 跨 handle 等待：所有 handle 完成信号汇聚到单个共享
      ``threading.Event``，``gather`` 仅等待该 event 一次。
    - ``__await__``：完成回调直通 event loop（``call_soon_threadsafe``），
      无每次等待的守护线程。
    """

    __slots__ = (
        "_callbacks",
        "_context",
        "_error_payload",
        "_future",
        "_lock",
        "_result",
        "_state",
        "_workflow_id",
        "task_id",
    )

    def __init__(
        self,
        task_id: str,
        *,
        context: TaskContext | None = None,
        workflow_id: str = "",
    ) -> None:
        self.task_id = task_id
        self._context = context
        self._workflow_id = workflow_id
        self._future = _CompletionFuture()
        # _result 单字段两态（0.3.2 R4 统一，见模块 docstring）：
        # - 小结果：结果对象本身（worker 结果帧反序列化产物，含 bytes 返回值）。
        # - 大结果：Ref（结果帧字节已落本节点 blob store）。
        self._result: Any = None
        self._error_payload: bytes = b""
        self._state: TaskState = "pending"
        self._callbacks: list[Callable[[AsyncResult], None]] = []
        self._lock = threading.Lock()

    @property
    def state(self) -> TaskState:
        """任务状态。

        - ``"pending"``：已提交但 Worker 尚未开始执行。
        - ``"running"``：Worker 正在执行。
        - ``"completed"``：成功完成，``result()`` 可立即返回。
        - ``"failed"``：执行失败，``result()`` 会重新抛出任务异常。
        - ``"cancelled"``：被取消。
        """
        with self._lock:
            return self._state

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    def done(self) -> bool:
        """任务是否已完成（成功/失败/取消）。"""
        return self._future.is_set()

    def result(self, timeout: float | None = None) -> Any:
        """阻塞等待任务结果。

        Args:
            timeout: 最大等待秒数，``None`` 表示无限等待。

        Returns:
            任务的返回值。大结果（内部为 :class:`Ref`）时透明经 ValueStore
            拉取反序列化，首次调用后缓存。

        Raises:
            ActantTimeoutError: 等待超时。
            ActantError: 任务执行失败（重新抛出序列化的异常）。
            TaskCancelledError: 任务被取消。
        """
        if not self._future.wait(timeout=timeout):
            raise ActantTimeoutError(f"task {self.task_id!r} did not complete within {timeout}s")
        with self._lock:
            state = self._state
            error_payload = self._error_payload
            result = self._result
        if state == "cancelled":
            raise TaskCancelledError(f"task {self.task_id!r} was cancelled")
        if error_payload:
            try:
                exc = cloudpickle.loads(error_payload)
            except (pickle.UnpicklingError, ValueError, TypeError) as e:
                raise ActantError(
                    f"task {self.task_id!r} failed (error payload undecodable)"
                ) from e
            raise exc
        # 单一访问路径：内部两态（对象缓存 / Ref）在此收敛。
        if isinstance(result, Ref):
            return result.result()
        return result

    def ref(self) -> Ref | None:
        """返回本结果的 :class:`Ref` 句柄（大结果时），小结果返回 ``None``。

        结果帧超过 ``REF_INLINE_THRESHOLD`` 时结果已落本节点 blob store，
        ``Ref`` 可直接作为下游 ``submit`` 参数（提交方自动解析），或经
        ``Ref.result()`` 显式拉取。
        """
        with self._lock:
            result = self._result
        return result if isinstance(result, Ref) else None

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """阻塞等待并返回任务异常，无异常返回 ``None``。

        取消语义对齐。任务被取消时抛 ``TaskCancelledError``（与
        ``result()`` 行为一致），而非返回 ``None``。这样调用方可以通过
        ``try: result.exception() except TaskCancelledError: ...`` 统一处理取消。

        Returns:
            任务执行中抛出的异常对象；任务成功完成时返回 ``None``。

        Raises:
            ActantTimeoutError: 等待超时。
            TaskCancelledError: 任务被取消。
        """
        if not self._future.wait(timeout=timeout):
            raise ActantTimeoutError(f"task {self.task_id!r} did not complete within {timeout}s")
        with self._lock:
            error_payload = self._error_payload
            state = self._state
        if state == "cancelled":
            raise TaskCancelledError(f"task {self.task_id!r} was cancelled")
        if error_payload:
            with _suppress_pickle_errors():
                return cast(BaseException, cloudpickle.loads(error_payload))
        return None

    def cancel(
        self,
        *,
        propagate: bool = False,
        force_after: float | None = None,
    ) -> bool:
        """尝试取消任务。

        Args:
            propagate: ``True`` 时级联取消同一 ``workflow_id`` 下的其他任务，
                并通过 P2P 广播到远端 Worker。
            force_after: 取消后若任务未在 ``force_after`` 秒内协作退出，
                强制调用其清理钩子。

        Returns:
            ``True`` 表示取消请求已提交（或任务已被取消，幂等语义）；
            ``False`` 表示任务已完成（成功/失败），无法取消。
        """
        # 0. 已完成任务不可取消（成功/失败状态）。
        # 已取消任务返回 True（幂等）。
        with self._lock:
            state = self._state
        if state in ("completed", "failed"):
            return False
        if state == "cancelled":
            return True

        # 1. 协作式取消上下文
        if self._context is not None:
            self._context._cancel(force_after=force_after)

        # 1b. 通知 Rust Worker 设置 cancel_flag（若任务正在运行），
        # 并标记预取消（若任务仍在排队）。直接操作避免递归调用 handle.cancel()。
        runtime = get_current_runtime()
        if runtime is not None:
            runtime._mark_task_cancelled(self.task_id)
            if runtime._rust_core is not None:
                # 取消请求是尽力而为语义：失败不阻止本地级联取消，但不可静默。
                try:
                    runtime._rust_core.cancel_task(self.task_id)
                except Exception:
                    _logger.warning(
                        "cancel_task failed for %s",
                        self.task_id,
                        exc_info=True,
                    )

        # 2. 本地广播：emit TaskLifecycle cancelled
        if self._context is not None and self._context.is_cancelled():
            _emit_task_event("cancelled", self.task_id, self._workflow_id)

        # 3. 跨节点 P2P 广播取消
        if self._workflow_id:
            runtime = get_current_runtime()
            if runtime is not None and runtime._rust_core is not None:
                try:
                    runtime._rust_core.broadcast_cancel(self.task_id, self._workflow_id)
                except Exception:
                    # P2P 广播失败不应阻止本地级联取消，记录 warning 后继续。
                    _logger.warning(
                        "task %s: broadcast_cancel failed",
                        self.task_id,
                        exc_info=True,
                    )

        # 4. 级联传播到同一 workflow 的其他任务
        # 通过 runtime.cancel_task() 走完整路径：设置预取消标记 + Rust cancel_flag +
        # handle.cancel()，确保排队中和运行中的任务都能被取消。
        if propagate and self._workflow_id:
            runtime = get_current_runtime()
            if runtime is not None:
                for tid in runtime.list_tasks():
                    if tid == self.task_id:
                        continue
                    other = runtime.get_task(tid)
                    if other is not None and other.workflow_id == self._workflow_id:
                        runtime.cancel_task(tid)

        # 取消请求已提交（无论 _context 是否存在，Rust cancel_task /
        # broadcast_cancel / 预取消标记均已设置）。返回 True 表示请求已提交，
        # 与 docstring 语义一致。已完成的任务在前面的 early return 中处理。
        return True

    def wait(self, timeout: float | None = None) -> bool:
        """等待任务完成，不取结果。

        Returns:
            ``True`` 若任务在超时前完成，``False`` 若超时。
        """
        return self._future.wait(timeout=timeout)

    def add_done_callback(self, fn: Callable[[AsyncResult], None]) -> None:
        """注册回调，任务完成（成功/失败/取消）后调用。

        回调接收此 ``AsyncResult`` 作为参数。若任务已完成，回调立即同步调用。

        完成判定以锁内 ``_state`` 终态为准（而非 ``_future.is_set()``）：
        ``_set_*`` 在持锁时先置终态、释放锁后才 ``set`` future，若检查
        ``is_set()`` 会在该窗口内把回调 append 进已被清空的 ``_callbacks``
        而永久丢失——回调注册必须与完成协议共用同一终态判定源。
        """
        with self._lock:
            if self._state not in _TERMINAL_STATES:
                self._callbacks.append(fn)
                return
        # 任务已完成，立即调用
        _invoke_callback(
            fn,
            self,
            label=f"AsyncResult {self.task_id}: done callback {getattr(fn, '__name__', fn)!r}",
        )

    # ------------------------------------------------------------------
    # 内部方法：由 Runtime._on_task_result 调用
    # ------------------------------------------------------------------

    def _set_running(self) -> None:
        """标记任务为运行中（由 Worker TaskStarted 事件触发）。"""
        with self._lock:
            if self._state == "pending":
                self._state = "running"

    def _set_result(self, result_obj: Any) -> None:
        """设置任务成功结果（对象缓存态）并触发回调。

        Args:
            result_obj: 任务返回值（worker 结果帧反序列化产物，任意类型，
                包括 bytes）。
        """
        with self._lock:
            if self._future.is_set():
                return
            self._result = result_obj
            self._state = "completed"
            callbacks = list(self._callbacks)
            self._callbacks.clear()
        # 在锁外触发 future.set()，使 Condition.notify_all 与 callback 链
        # 在不持有 self._lock 的状态下执行（避免回调内调用 result() 死锁）。
        self._future.set()
        _invoke_callbacks(
            callbacks,
            self,
            label=f"AsyncResult {self.task_id}: done callback",
        )

    def _set_result_ref(self, ref_bytes: bytes) -> None:
        """设置任务成功结果为 ``Ref``（大结果态，0.3.2 R4）并触发回调。

        Args:
            ref_bytes: 结果帧字节落 blob 后的 BlobRef wire 编码。
        """
        with self._lock:
            if self._future.is_set():
                return
            self._result = Ref(ref_bytes)
            self._state = "completed"
            callbacks = list(self._callbacks)
            self._callbacks.clear()
        self._future.set()
        _invoke_callbacks(
            callbacks,
            self,
            label=f"AsyncResult {self.task_id}: done callback",
        )

    def _set_error(self, error_payload_or_msg: bytes | str | BaseException) -> None:
        """设置任务失败结果并触发回调。

        Args:
            error_payload_or_msg: 失败信息。可为：
                - ``bytes``：已序列化的异常字节（跨节点传播路径）。
                - ``str``：错误消息字符串，包装为 ``ActantError`` 再序列化。
                - ``BaseException``：异常对象（P2-9 优化路径，本地 dispatch），
                  序列化后存入 ``_error_payload`` 供 ``result()`` 重新抛出。
        """
        with self._lock:
            if self._future.is_set():
                return
            if isinstance(error_payload_or_msg, bytes):
                self._error_payload = error_payload_or_msg
            elif isinstance(error_payload_or_msg, BaseException):
                self._error_payload = cloudpickle.dumps(error_payload_or_msg)
            else:
                self._error_payload = cloudpickle.dumps(ActantError(error_payload_or_msg))
            self._state = "failed"
            callbacks = list(self._callbacks)
            self._callbacks.clear()
        self._future.set()
        _invoke_callbacks(
            callbacks,
            self,
            label=f"AsyncResult {self.task_id}: done callback",
        )

    def _set_cancelled(self) -> None:
        """标记任务为已取消并触发回调。"""
        with self._lock:
            if self._future.is_set():
                return
            self._state = "cancelled"
            callbacks = list(self._callbacks)
            self._callbacks.clear()
        self._future.set()
        _invoke_callbacks(
            callbacks,
            self,
            label=f"AsyncResult {self.task_id}: done callback",
        )

    def _export_outcome(self) -> tuple[bool, bytes]:
        """导出任务终态结果字节，供 Orchestrator 状态回灌（``complete_workflow``）。

        Returns:
            ``(success, result_bytes)``：
            - 成功（对象缓存态）：``(True, cloudpickle.dumps(返回值))``。
            - 成功（Ref 态，大结果）：``(True, BlobRef wire 编码)``——回灌引用
              而非值字节，避免大值在 Orchestrator 持久化与查询面二次膨胀；
              消费方经 ``Ref.result()`` 解析。
            - 失败：``(False, 错误消息 UTF-8 字节)``——Orchestrator ``FAIL_TASK``
              期望错误字符串，而非序列化异常。
            - 取消/其他非成功终态：``(False, b"task cancelled")``。

        由 ``FlowDAG.record_outcome`` 的完成回调调用；调用方保证任务已终态。
        """
        with self._lock:
            state = self._state
            if state == "completed":
                result = self._result
            elif state == "failed":
                error_payload = self._error_payload
            else:
                # cancelled / 其它非成功终态：以失败回灌（Phase 1 近似，任务级
                # Cancelled 状态回灌由后续阶段细化）。
                return False, b"task cancelled"
        if state == "completed":
            if isinstance(result, Ref):
                return True, result._ref_bytes
            return True, cloudpickle.dumps(result)
        # failed：错误负载是序列化异常，提取消息字符串（解码失败时给通用错误）。
        try:
            exc = cloudpickle.loads(error_payload)
            msg = str(exc)
        except Exception:
            _logger.warning(
                "task %s: failed to decode error payload for outcome export",
                self.task_id,
                exc_info=True,
            )
            msg = "unknown task failure"
        return False, msg.encode("utf-8")

    def __repr__(self) -> str:
        return f"AsyncResult(task_id={self.task_id!r}, state={self.state!r})"

    def _bridge_to_loop(self, loop: asyncio.AbstractEventLoop) -> asyncio.Future[Any]:
        """把任务完成信号桥接为 ``loop`` 上的 ``asyncio.Future``（0.3.2 R5）。

        经 ``add_done_callback`` 注册回调：回调在完成触发线程（Rust 事件回调
        线程）中取值并 ``call_soon_threadsafe`` 直通 event loop——无每次等待
        的守护线程、不阻塞循环。反序列化（含大结果 unpickle）发生在回调线程
        而非 loop 线程。loop 已关闭时投递失败由完成回调的错误策略记录。
        """
        aio_future: asyncio.Future[Any] = loop.create_future()

        if self.done():
            # Fast path：已完成，直接设置结果（同步，无需跨线程投递）。
            try:
                aio_future.set_result(self.result(timeout=0))
            except BaseException as exc:
                aio_future.set_exception(exc)
            return aio_future

        def _on_done(_handle: AsyncResult) -> None:
            try:
                value = self.result(timeout=0)
            except BaseException as exc:
                loop.call_soon_threadsafe(_set_aio_result, aio_future, None, exc)
            else:
                loop.call_soon_threadsafe(_set_aio_result, aio_future, value, None)

        self.add_done_callback(_on_done)
        return aio_future

    def __await__(self) -> Any:
        """使 ``AsyncResult`` 可在 ``async def`` 中直接 ``await``。

        完成回调直通 event loop（见 :meth:`_bridge_to_loop`）：任务完成时在
        回调线程取值，经 ``call_soon_threadsafe`` 唤醒 await 方，无需守护线程。

        用法::

            async def my_flow():
                handle = my_task.submit(x)
                result = await handle  # 等价于 await handle.result()

        Returns:
            任务的返回值（与 ``result()`` 一致）。

        Raises:
            ActantError: 任务执行失败。
            TaskCancelledError: 任务被取消。
        """
        return self._bridge_to_loop(asyncio.get_running_loop()).__await__()


def _set_aio_result(future: Any, value: Any, exc: Any) -> None:
    """在 event loop 线程中安全设置 asyncio Future 的结果或异常。"""
    if future.done():
        return
    if exc is not None:
        future.set_exception(exc)
    else:
        future.set_result(value)


def _collect_dep_ids(value: Any, seen: set[str], ids: list[str]) -> Any:
    """单遍遍历 value：解析 ``AsyncResult`` 为其结果值，同时去重保序收集上游 task_id。

    遍历规则与 ``_resolve_value`` 一致（list / tuple / dict 递归），但把"解析
    结果"与"收集依赖 id"合并到一次遍历，避免两遍遍历的重复递归与规则漂移。
    ``seen``/``ids`` 由调用方持有，保证跨 ``args``/``kwargs`` 全局去重。

    大结果（内部为 ``Ref``）**不在此处取值**：``result()`` 会把大值反序列化到
    提交方内存再随 envelope 重新序列化；保留 ``Ref`` 由 ``_submit`` 解析为
    ``_RefArg`` 帧内联字节（0.3.2 R3b/R6，见 plans/REF_DESIGN.md）。

    两态判定必须先等终态：``Ref`` 只在结果抵达回调（``_set_result_ref``）中
    才存在。eager flow 里下游 ``submit`` 通常早于上游完成，此时 ``ref()``
    恒为 ``None``，若直接落 ``result()`` 会把大值整体反序列化进提交方并触发
    ``_degrade_large_values`` 二次落 blob——正是 R6 要消除的"钉内存"残余。
    """
    if isinstance(value, AsyncResult):
        if value.task_id not in seen:
            seen.add(value.task_id)
            ids.append(value.task_id)
        value.wait()
        ref = value.ref()
        if ref is not None:
            return ref
        return value.result()
    if isinstance(value, list):
        return [_collect_dep_ids(v, seen, ids) for v in value]
    if isinstance(value, tuple):
        return tuple(_collect_dep_ids(v, seen, ids) for v in value)
    if isinstance(value, dict):
        return {k: _collect_dep_ids(v, seen, ids) for k, v in value.items()}
    return value


def _resolve_args_with_deps(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[tuple[Any, ...], dict[str, Any], list[str]]:
    """单遍解析 ``submit`` 参数：解析上游 ``AsyncResult`` 并收集依赖 id。

    供 ``Task.submit`` / ``submit_batch`` 在构建 ``FlowDAG`` 依赖边时使用。
    相比"先 ``_collect_async_result_ids`` 再 ``_resolve_value``"两遍遍历，
    这里合并为一遍，同一规则不外泄、不漂移。

    Returns:
        ``(resolved_args, resolved_kwargs, upstream_ids)``：
        - resolved_args / resolved_kwargs：参数中 ``AsyncResult`` 已替换为
          其结果值（嵌套容器递归处理）。
        - upstream_ids：本批参数中所有上游 ``task_id``，保持出现顺序并去重。
    """
    seen: set[str] = set()
    ids: list[str] = []
    resolved_args = tuple(_collect_dep_ids(a, seen, ids) for a in args)
    resolved_kwargs = {k: _collect_dep_ids(v, seen, ids) for k, v in kwargs.items()}
    return resolved_args, resolved_kwargs, ids


def _resolve_value(value: Any) -> Any:
    """若 value 是 ``AsyncResult``，阻塞等待并返回其结果；否则原样返回。

    递归处理 list / tuple / dict 容器内的 ``AsyncResult``，支持嵌套依赖。
    """
    if isinstance(value, AsyncResult):
        return value.result()
    if isinstance(value, list):
        return [_resolve_value(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_resolve_value(v) for v in value)
    if isinstance(value, dict):
        return {k: _resolve_value(v) for k, v in value.items()}
    return value


__all__ = ["AsyncResult", "_resolve_value"]
