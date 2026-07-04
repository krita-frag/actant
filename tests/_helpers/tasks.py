"""任务/TaskRef 构造辅助函数。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from actant.task import Task, TaskRef


def make_task(
    name: str,
    func: Callable[..., Any] | None = None,
    **options: Any,
) -> Task:
    """构造一个仅供测试使用的 Task。"""
    return Task(name, func=func, **options)


def make_task_ref(
    name: str,
    *args: Any,
    **kwargs: Any,
) -> TaskRef:
    """构造一个仅供测试使用的 TaskRef。"""
    return TaskRef(name, args=args, kwargs=kwargs or None)
