"""任务执行上下文：协作式取消检查点与清理钩子。

本模块是 ``actant.task`` 包的底层依赖，不引用包内其他子模块，避免循环导入。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Literal

from actant.exceptions import TaskCancelledError

_logger = logging.getLogger("actant.task")

#: 任务状态字面量类型。
TaskState = Literal["pending", "running", "completed", "failed", "cancelled"]

# 线程局部：当前正在执行任务的 TaskContext，供 ``get_task_context()`` 读取（取消系统）。
_task_context_local = threading.local()


class TaskContext:
    """任务执行上下文：协作式取消检查点与清理钩子。

    每个通过 ``Task.submit()`` 提交的任务都关联一个 ``TaskContext``，
    任务函数内可通过 ``actant.get_task_context()`` 获取。它提供：

    - ``is_cancelled()``：协作式检查点，任务应定期调用并优雅退出。
    - ``on_cancel(callback)``：注册清理钩子，取消时自动调用。
    - ``force_after``：取消后若任务未在指定时间内退出，强制调用清理并标记完成。

    注意：Python 线程无法被强制中断，因此"取消"是协作式的；任务必须主动检查
    ``is_cancelled()`` 并在为 ``True`` 时退出。``force_after`` 仅保证清理钩子
     eventually 被调用，不能立即停止 CPU 密集型代码。
    """

    def __init__(self, task_id: str, workflow_id: str = "") -> None:
        self._task_id = task_id
        self._workflow_id = workflow_id
        self._cancelled = threading.Event()
        self._callbacks: list[tuple[int, Callable[[], None]]] = []
        self._called: set[int] = set()
        self._callback_lock = threading.Lock()
        self._force_timer: threading.Timer | None = None
        # 记录已设置的 force_after 秒数，用于 context 迁移时在新 context 上
        # 重启定时器（避免旧 context 的定时器触发时回调已迁移走而漏执行）。
        self._force_after: float | None = None
        self._callback_counter = 0

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    def is_cancelled(self) -> bool:
        """返回任务是否收到取消请求。任务函数应定期调用此检查点。"""
        return self._cancelled.is_set()

    def raise_if_cancelled(self) -> None:
        """若已取消，抛出 ``TaskCancelledError``（便于任务函数快速退出）。"""
        if self._cancelled.is_set():
            raise TaskCancelledError(f"task {self._task_id!r} was cancelled")

    def on_cancel(self, callback: Callable[[], None], *, force_after: float | None = None) -> None:
        """注册取消清理钩子。

        若任务已被取消，回调会立即执行一次（幂等：同一回调不会被重复调用）。

        Args:
            callback: 取消时调用的无参函数。应只包含清理逻辑（释放资源、回滚状态等），
                不应抛出异常；若抛出会被 log 吞掉，避免影响其他清理钩子。
            force_after: 取消后若任务未在 ``force_after`` 秒内退出，强制调用
                callback。``None`` 表示不强制（仅在收到取消信号或注册时调用一次）。
        """
        start_timer = False
        invoke_now = False
        # 在锁内捕获具名值，使 mypy 跨锁收窄 force_after 类型（避免 float | None 传入）。
        timer_after: float | None = None
        with self._callback_lock:
            callback_id = self._callback_counter
            self._callback_counter += 1
            self._callbacks.append((callback_id, callback))
            invoke_now = self._cancelled.is_set()
            if force_after is not None and self._force_timer is None:
                start_timer = True
                timer_after = force_after
        if invoke_now:
            self._invoke_callbacks()
        if start_timer and timer_after is not None:
            self._start_force_timer(timer_after)

    def _cancel(self, *, force_after: float | None = None) -> bool:
        """内部方法：标记取消并触发回调。返回是否成功设置（已为 True 返回 False）。"""
        if self._cancelled.is_set():
            # 已取消状态下仍可启动新的 force_after 定时器
            if force_after is not None and self._force_timer is None:
                self._start_force_timer(force_after)
            return False
        self._cancelled.set()
        self._invoke_callbacks()
        if force_after is not None and self._force_timer is None:
            self._start_force_timer(force_after)
        return True

    def _start_force_timer(self, force_after: float) -> None:
        """启动 force_after 定时器并记录秒数，供 context 迁移时重启。

        必须在调用方确认 ``_force_timer is None`` 后调用（避免重复启动）。
        """
        self._force_after = force_after
        timer = threading.Timer(force_after, self._force_cleanup)
        timer.daemon = True
        self._force_timer = timer
        timer.start()

    def _invoke_callbacks(self) -> None:
        """顺序调用所有未执行过的清理钩子，错误被捕获并 log。

        在锁内标记 ``_called`` 后释放锁执行回调，确保并发调用时
        每个 callback 仅执行一次（标记与执行原子分离）。
        """
        with self._callback_lock:
            pending = [
                (cid, cb)
                for cid, cb in self._callbacks
                if cid not in self._called
            ]
            # 在锁内预先标记，确保并发 _invoke_callbacks 不会重复执行同一 callback。
            for cid, _ in pending:
                self._called.add(cid)
        for _cid, cb in pending:
            try:
                cb()
            except Exception:
                _logger.warning(
                    "task %s: cancel callback %r raised",
                    self._task_id, getattr(cb, "__name__", cb),
                    exc_info=True,
                )

    def _migrate_callbacks_to(self, other: TaskContext) -> None:
        """将本 context 上已注册的回调迁移到 ``other``，用于 worker 线程启动时
        把 submit 阶段预注册的回调迁移到 ``_DispatchTaskContext``。

        已执行过的回调不会重复迁移；若本 context 已被取消，迁移后立即触发
        ``other`` 的回调（保证预取消阶段设置的回调仍然执行）。

        force_after 定时器状态也一并迁移：若本 context 已启动 force_after
        定时器，在 ``other`` 上以相同的 force_after 秒数重启定时器。这避免了
        竞态场景下旧 context 的定时器触发时回调已迁移走而漏执行。
        """
        with self._callback_lock:
            callbacks = list(self._callbacks)
            cancelled = self._cancelled.is_set()
            force_after = self._force_after
        if callbacks:
            with other._callback_lock:
                for _, cb in callbacks:
                    other._callbacks.append((other._callback_counter, cb))
                    other._callback_counter += 1
        if cancelled:
            # 迁移取消状态：若旧 context 已设置 force_after，在新 context 上
            # 也启动 force_after 定时器（_cancel 内部会检查 _force_timer is None）。
            other._cancel(force_after=force_after)

    def _force_cleanup(self) -> None:
        """force_after 超时后强制调用所有未执行的清理钩子。"""
        _logger.warning(
            "task %s: force_after reached, invoking cancel callbacks",
            self._task_id,
        )
        self._invoke_callbacks()

    def __repr__(self) -> str:
        return f"TaskContext(task_id={self._task_id!r}, cancelled={self.is_cancelled()})"


def get_task_context() -> TaskContext | None:
    """返回当前线程正在执行任务的 ``TaskContext``，不在任务中返回 ``None``。

    典型用法::

        @actant.task
        def long_running():
            ctx = actant.get_task_context()
            for i in range(100):
                if ctx is not None and ctx.is_cancelled():
                    return "cancelled"
                time.sleep(0.1)
            return "done"
    """
    return getattr(_task_context_local, "context", None)


@contextmanager
def _task_context_scope(ctx: TaskContext) -> Iterator[None]:
    """在 with 块内设置当前线程的 ``TaskContext``。"""
    prev = getattr(_task_context_local, "context", None)
    _task_context_local.context = ctx
    try:
        yield
    finally:
        _task_context_local.context = prev


class _DispatchTaskContext(TaskContext):
    """Dispatch handler 专用 TaskContext：将 Rust ``cancel_token`` 桥接到 Python 取消系统。

    ``is_cancelled()`` 委托给 Rust ``cancel_token``，首次检测到取消时设置
    Python 端 ``_cancelled`` 事件并触发 ``on_cancel`` 注册的清理钩子。
    """

    def __init__(self, task_id: str, workflow_id: str, cancel_token: Any) -> None:
        super().__init__(task_id, workflow_id=workflow_id)
        self._cancel_token = cancel_token
        self._propagated = False

    def is_cancelled(self) -> bool:
        token_cancelled = self._cancel_token.is_cancelled()
        if token_cancelled:
            if not self._propagated:
                self._propagated = True
                self._cancelled.set()
                self._invoke_callbacks()
            return True
        # 预取消场景：submit 阶段已通过旧 context 触发 _cancelled.set()，
        # 但 Rust cancel_token 可能尚未/不会设置。必须让任务函数能检测到。
        return self._cancelled.is_set()
