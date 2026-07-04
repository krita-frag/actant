"""任务执行上下文：管理协作式取消信号。

当 Rust 层的 `tokio::time::timeout` 触发后，Python 任务无法被强制中断
（spawn_blocking 线程无法被 abort）。协作式取消允许任务在长时间运行
的操作中定期检查取消状态并主动退出。

机制：
    Rust 侧为每个 dispatch 创建 `Arc<AtomicBool>` 取消标志，直接包装为
    `CancelToken` PyO3 对象传递给 Python handler。超时后 Rust 设置
    AtomicBool 为 true，Python 侧通过 `token.is_cancelled()` 检查。

用法：
    import actant

    @actant.task
    def long_running_task(_payload=None):
        for item in process_large_dataset():
            if actant.is_cancelled():
                raise TaskCancelled("task cancelled by timeout")
            process(item)
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from actant.exceptions import TaskCancelledError

if TYPE_CHECKING:
    from actant.actant import CancelToken

_local: threading.local = threading.local()


class TaskCancelled(TaskCancelledError):
    """任务被协作式取消时抛出的异常。

    继承自 :class:`actant.exceptions.TaskCancelledError`（即 ``ActantError``），
    使得取消异常被纳入 Actant 统一异常层次，便于编排循环、监控、用户代码
    使用 ``except ActantError`` 捕获而不需额外分支。
    """


def _set_cancel_token(token: CancelToken) -> None:
    """设置当前线程的取消令牌。"""
    _local.cancel_token = token


def _clear_context() -> None:
    """清理当前线程的任务上下文。"""
    _local.cancel_token = None


def is_cancelled() -> bool:
    """检查当前任务是否已被取消。

    在任务上下文之外调用时返回 ``False``。
    """
    token: CancelToken | None = getattr(_local, "cancel_token", None)
    if token is None:
        return False
    return token.is_cancelled()
