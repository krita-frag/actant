"""工作流驱动实现。

驱动层把「驱动哪些工作流」与「以什么方式执行重放」分开：
:func:`actant.host_flows` 负责扫描 journal 并裁决工作流级超时，驱动
只决定执行策略（当前线程、线程池或事件循环）。

- :class:`BlockingDriver`：当前线程同步执行，适合单流程或短流程；
- :class:`ThreadedDriver`：每个工作流一个线程，千级并发；
- :class:`AsyncDriver`：单个 event loop 承载万级 ``@aio.flow``。
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import inspect
import logging
import threading
from collections.abc import Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from types import TracebackType
from typing import Any, Protocol, cast

from actant.exceptions import ActantTimeoutError, InvalidStateError

_logger = logging.getLogger("actant.driver")

#: ``AsyncDriver`` 中同步 flow 的线程池上限。async 驱动的主路径不建线程；
#: 仅当同一 journal 混有 ``@flow`` 定义时才需要执行器。
DEFAULT_SYNC_WORKERS = 32

#: ``ThreadedDriver`` 默认并发上限。线程数与内存随并发 flow 数线性增长，
#: 千级是线程模型的实际上界；万级并发应使用 async 驱动而不是放大该值。
DEFAULT_MAX_WORKERS = 512


class FlowDriver(Protocol):
    """工作流重放的执行策略。

    扫描待驱动工作流、读取 invocation、跳过 ``continue_as_new`` 后代与裁决
    工作流级超时都属于 :func:`actant.host_flows`；驱动只执行给定的重放
    闭包，因此 journal 与重放语义不随驱动实现变化。
    """

    def spawn(self, workflow_id: str, run: Callable[[], Any]) -> bool:
        """执行或调度一次 ``workflow_id`` 的重放。

        Args:
            workflow_id: 本次重放的工作流 id，供实现排队、去重与诊断。
            run: 已绑定目标 Runtime 与 flow 定义的无参闭包；其异常按同步
                执行语义向调用方传播。``@aio.flow`` 的闭包是协程函数，
                调用后返回 awaitable——驱动按 ``inspect.iscoroutinefunction``
                选择 event loop 或工作线程。

        Returns:
            ``True`` 表示本次调用接管了重放；``False`` 表示该工作流已在执行
            中（host loop 会重复扫描到未完成工作流，驱动负责去重）。

        Notes:
            实现可在当前线程同步执行 ``run``，也可交给其他线程或事件循环。
            异步实现须保证 :func:`actant.host_flows` 以 ``once=True``
            返回前本轮 ``run`` 已派发；调用方若要在同一 Runtime 上继续等待
            结果，必须调用 :meth:`join`。
        """
        ...

    def join(self) -> None:
        """等待当前在飞的重放全部结束，并重抛首个未被宿主裁决的失败。

        同步驱动没有在飞工作，是空操作。工作流级超时（``ActantTimeoutError``）
        由 :func:`actant.host_flows` 裁决为“跳过该工作流”，异步实现可
        自行吞掉；其余异常在此重抛，保持与同步驱动一致的错误传播。
        """
        ...


class BlockingDriver:
    """在调用线程同步执行每次重放。

    flow 在等待点上 park 时会占住 host 线程，因此该实现适合单流程或短流程
    场景；大量并发 parked flow 需要 threaded / async 驱动。

    ``@aio.flow`` 的重放闭包返回协程时，本驱动为每次重放创建一个临时
    event loop 并同步跑完——语义仍是"一次一个"，不是并发驱动。
    """

    def spawn(self, workflow_id: str, run: Callable[[], Any]) -> bool:
        """同步执行 ``run``；异常原样传播给 host loop。"""
        result = run()
        if inspect.isawaitable(result):
            # ``@aio.flow`` 的闭包返回协程；临时 loop 同步跑完。
            asyncio.run(cast("Coroutine[Any, Any, Any]", result))
        return True

    def join(self) -> None:
        """没有在飞工作，无需等待。"""


class ThreadedDriver:
    """每个工作流一个守护线程，同一工作流不会并发重复驱动。

    flow 在等待点上 park 时只占用自己的线程，其他工作流继续推进。代价是
    线程数与内存随并发数线性增长，因此超过 :attr:`max_workers` 时显式报错，
    而不是静默排队或无限建线程。
    """

    def __init__(self, max_workers: int = DEFAULT_MAX_WORKERS) -> None:
        if max_workers < 1:
            raise ValueError(f"ThreadedDriver: max_workers must be >= 1, got {max_workers}")
        self._max_workers = max_workers
        self._lock = threading.Lock()
        self._active: dict[str, threading.Thread] = {}
        self._error: BaseException | None = None

    def spawn(self, workflow_id: str, run: Callable[[], Any]) -> bool:
        """在独立线程中执行 ``run``；重复或超额时见 :class:`FlowDriver`。"""
        with self._lock:
            if workflow_id in self._active:
                return False
            if len(self._active) >= self._max_workers:
                raise InvalidStateError(
                    f"ThreadedDriver: {self._max_workers} workflows are already "
                    "running; increase max_workers or use an async driver"
                )
            thread = threading.Thread(
                target=self._run,
                args=(workflow_id, run),
                name=f"actant-flow-{workflow_id}",
                daemon=True,
            )
            self._active[workflow_id] = thread
        try:
            thread.start()
        except BaseException:
            with self._lock:
                self._active.pop(workflow_id, None)
            raise
        return True

    def _run(self, workflow_id: str, run: Callable[[], Any]) -> None:
        try:
            result = run()
            if inspect.isawaitable(result):
                # async flow 的闭包：本线程即其 event loop 线程。一个 flow
                # 一个 loop 的代价可接受——万级并发应改用 AsyncDriver。
                asyncio.run(cast("Coroutine[Any, Any, Any]", result))
        except ActantTimeoutError:
            # 工作流级 deadline：orchestrator 已置终态。host loop 对同步驱动
            # 的同类异常也是“跳过该工作流”，线程内不能让它中止 host 进程。
            _logger.warning("workflow %s exceeded its deadline during replay", workflow_id)
        except BaseException as exc:
            with self._lock:
                if self._error is None:
                    self._error = exc
            _logger.error("workflow %s replay failed", workflow_id, exc_info=exc)
        finally:
            with self._lock:
                self._active.pop(workflow_id, None)

    def join(self) -> None:
        """等待全部在飞工作结束；若有重放失败，首个异常在此重抛。"""
        while True:
            with self._lock:
                threads = list(self._active.values())
            if not threads:
                break
            for thread in threads:
                thread.join()
        with self._lock:
            error, self._error = self._error, None
        if error is not None:
            raise error


class AsyncDriver:
    """单个 event loop 承载全部 ``@aio.flow`` 重放。

    flow 在等待点 ``await`` 时只挂起协程，不占线程，因此内存与调度开销随
    并发数近似线性、但线程数恒定。同步 ``@flow`` 定义无法在 loop 上阻塞执行，
    故交给有界线程池，保持与本类接管前的行为一致。

    生命周期：

    - 首次 :meth:`spawn` 时惰性创建 loop 线程（名为 ``actant-async-driver``）；
    - :meth:`join` 等待在飞重放结束并重抛首个失败（工作流级超时由 host loop
      裁决，此处吞掉并记日志）；
    - :meth:`close` 先 :meth:`join`，再停止 loop 并关闭同步执行器。停止
      Runtime 前必须调用，或使用 ``with AsyncDriver() as driver:``。

    一个实例只应服务一个 Runtime 的 host loop：loop 线程的线程局部 Runtime
    在首次重放时绑定，驱动不负责在多个 Runtime 之间切换。
    """

    def __init__(self, *, max_sync_workers: int = DEFAULT_SYNC_WORKERS) -> None:
        if max_sync_workers < 1:
            raise ValueError(f"AsyncDriver: max_sync_workers must be >= 1, got {max_sync_workers}")
        self._max_sync_workers = max_sync_workers
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._active: dict[str, concurrent.futures.Future[Any]] = {}
        self._error: BaseException | None = None
        self._closed = False
        self._executor = ThreadPoolExecutor(
            max_workers=max_sync_workers,
            thread_name_prefix="actant-async-sync",
        )

    def __enter__(self) -> AsyncDriver:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def spawn(self, workflow_id: str, run: Callable[[], Any]) -> bool:
        """把 ``run`` 提交到 loop；重复的 ``workflow_id`` 返回 ``False``。

        协程函数直接 ``await``；普通可调用对象交给线程池，避免阻塞 loop。
        """
        loop = self._ensure_loop()
        with self._lock:
            if workflow_id in self._active:
                return False
            future = asyncio.run_coroutine_threadsafe(self._execute(run), loop)
            self._active[workflow_id] = future
        future.add_done_callback(partial(self._on_done, workflow_id))
        return True

    def join(self) -> None:
        """等待全部在飞重放结束；若有失败，首个异常在此重抛。"""
        while True:
            with self._lock:
                pending = list(self._active.items())
            if not pending:
                break
            for workflow_id, future in pending:
                try:
                    future.result()
                except ActantTimeoutError:
                    # 工作流级 deadline：orchestrator 已置终态，host loop 对
                    # 同步驱动的同类异常也是“跳过该工作流”。
                    _logger.warning(
                        "workflow %s exceeded its deadline during replay",
                        workflow_id,
                    )
                except BaseException as exc:
                    with self._lock:
                        if self._error is None:
                            self._error = exc
                    _logger.error("workflow %s replay failed", workflow_id, exc_info=exc)
                with self._lock:
                    if self._active.get(workflow_id) is future:
                        self._active.pop(workflow_id, None)
        with self._lock:
            error, self._error = self._error, None
        if error is not None:
            raise error

    def close(self) -> None:
        """等待在飞重放、停止 loop、关闭同步执行器。重复调用为 no-op。

        ``join()`` 重抛首个重放失败时仍会完成清理：否则事件循环线程与
        线程池会泄漏。
        """
        try:
            self.join()
        finally:
            with self._lock:
                loop, thread = self._loop, self._loop_thread
                self._loop = None
                self._loop_thread = None
                self._closed = True
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
            if thread is not None:
                thread.join(timeout=5.0)
            self._executor.shutdown(wait=True)

    async def _execute(self, run: Callable[[], Any]) -> Any:
        if inspect.iscoroutinefunction(run):
            return await run()
        return await asyncio.get_running_loop().run_in_executor(self._executor, run)

    def _on_done(
        self,
        workflow_id: str,
        future: concurrent.futures.Future[Any],
    ) -> None:
        with self._lock:
            if self._active.get(workflow_id) is future:
                self._active.pop(workflow_id, None)
        # 取出异常避免被静默丢弃；真正的传播发生在 join()。
        with contextlib.suppress(concurrent.futures.CancelledError):
            future.exception()

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._closed:
                raise InvalidStateError("AsyncDriver is closed")
            if self._loop is not None:
                return self._loop
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=self._loop_main,
                args=(loop,),
                name="actant-async-driver",
                daemon=True,
            )
            self._loop = loop
            self._loop_thread = thread
            thread.start()
            return loop

    @staticmethod
    def _loop_main(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()


__all__ = ["AsyncDriver", "BlockingDriver", "FlowDriver", "ThreadedDriver"]
