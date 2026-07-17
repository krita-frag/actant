"""``AsyncResult``：异步任务结果句柄与任务依赖解析。

依赖 ``_context``（TaskContext / TaskState）与 ``_helpers``
（``_suppress_pickle_errors``）。``_resolve_value`` 因紧耦合 ``AsyncResult``
亦置于本模块，供 ``Task.submit`` / ``gather`` 使用。
"""

from __future__ import annotations

import logging
import pickle
import threading
from collections.abc import Callable
from contextlib import suppress
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

_logger = logging.getLogger("actant.task")


class AsyncResult:
    """异步任务结果句柄。

    基于 ``threading.Event`` 实现，由 Rust event_bus 回调
    （``Runtime._on_task_result``）在任务完成时设置结果。

    分布式语义：任务由 Worker 执行，结果通过 P2P 网络回传后触发回调。
    """

    __slots__ = (
        "_callbacks", "_context", "_error_payload", "_event", "_lock",
        "_result_payload", "_state", "_workflow_id", "task_id",
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
        self._event = threading.Event()
        self._result_payload: bytes = b""
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
        return self._event.is_set()

    def result(self, timeout: float | None = None) -> Any:
        """阻塞等待任务结果。

        Args:
            timeout: 最大等待秒数，``None`` 表示无限等待。

        Returns:
            任务的返回值（经 cloudpickle 反序列化）。

        Raises:
            ActantTimeoutError: 等待超时。
            ActantError: 任务执行失败（重新抛出序列化的异常）。
            TaskCancelledError: 任务被取消。
        """
        if not self._event.wait(timeout=timeout):
            raise ActantTimeoutError(
                f"task {self.task_id!r} did not complete within {timeout}s"
            )
        with self._lock:
            state = self._state
            error_payload = self._error_payload
            result_payload = self._result_payload
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
        return cloudpickle.loads(result_payload)

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
        if not self._event.wait(timeout=timeout):
            raise ActantTimeoutError(
                f"task {self.task_id!r} did not complete within {timeout}s"
            )
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
                with suppress(Exception):
                    runtime._rust_core.cancel_task(self.task_id)

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
                        "task %s: broadcast_cancel failed", self.task_id, exc_info=True,
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
        return self._event.wait(timeout=timeout)

    def add_done_callback(self, fn: Callable[[AsyncResult], None]) -> None:
        """注册回调，任务完成（成功/失败/取消）后调用。

        回调接收此 ``AsyncResult`` 作为参数。若任务已完成，回调立即同步调用。
        """
        with self._lock:
            if not self._event.is_set():
                self._callbacks.append(fn)
                return
        # 任务已完成，立即调用
        _invoke_callback(
            fn, self,
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

    def _set_result(self, result_payload: bytes) -> None:
        """设置任务成功结果并触发回调。"""
        with self._lock:
            if self._event.is_set():
                return
            self._result_payload = result_payload
            self._state = "completed"
            self._event.set()
            callbacks = list(self._callbacks)
            self._callbacks.clear()
        _invoke_callbacks(
            callbacks, self,
            label=f"AsyncResult {self.task_id}: done callback",
        )

    def _set_error(self, error_payload_or_msg: bytes | str) -> None:
        """设置任务失败结果并触发回调。"""
        with self._lock:
            if self._event.is_set():
                return
            if isinstance(error_payload_or_msg, bytes):
                self._error_payload = error_payload_or_msg
            else:
                self._error_payload = cloudpickle.dumps(
                    ActantError(error_payload_or_msg)
                )
            self._state = "failed"
            self._event.set()
            callbacks = list(self._callbacks)
            self._callbacks.clear()
        _invoke_callbacks(
            callbacks, self,
            label=f"AsyncResult {self.task_id}: done callback",
        )

    def _set_cancelled(self) -> None:
        """标记任务为已取消并触发回调。"""
        with self._lock:
            if self._event.is_set():
                return
            self._state = "cancelled"
            self._event.set()
            callbacks = list(self._callbacks)
            self._callbacks.clear()
        _invoke_callbacks(
            callbacks, self,
            label=f"AsyncResult {self.task_id}: done callback",
        )

    def __repr__(self) -> str:
        return f"AsyncResult(task_id={self.task_id!r}, state={self.state!r})"


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
