"""全局事件订阅系统。

提供统一的 ``@actant.on`` 装饰器，让用户无需子类化 ABC 即可监听
任务、Actor、Worker 等生命周期事件。

设计要点
--------

- **多订阅者**：同一主题可有多个处理器，并发执行、异常隔离。
- **同步 + 异步**：``async`` 处理器并发执行；同步处理器在事件循环
  线程直接调用（应快速返回，否则会阻塞循环）。
- **全局注册表**：进程内单例，所有 ``_Node`` 共享。多 ``_Node`` 场景
  用户应在处理器内自行区分来源。
- **无返回值**：事件订阅仅用于副作用（告警、日志、清理），
  需要返回值的决策点（路由、调度）仍走 trait/ABC。

事件主题
--------

主题命名约定：``<对象>.<动作>``。当前可用主题：

- ``task.started`` —— 任务被路由后入队。事件对象含
  ``workflow_id``、``task_name``。
- ``task.completed`` —— 任务成功完成。事件对象为 ``_TaskCompletion``。
- ``task.failed`` —— 任务失败。事件对象为 ``_TaskCompletion``。
- ``actor.started`` / ``actor.stopped`` / ``actor.failed`` —— Actor
  生命周期事件。事件对象含 ``event_type``、``actor_id``、``error``。
- ``worker.drained`` —— Worker 任务排空。事件对象为 orchestration event。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Coroutine
from typing import Any, TypeAlias, cast

logger = logging.getLogger("actant.events")

# 异步处理器类型：接收任意事件对象，返回 None。
AsyncHandler: TypeAlias = Callable[[Any], Coroutine[Any, Any, None]]
# 同步处理器类型：接收任意事件对象，返回 None。
SyncHandler: TypeAlias = Callable[[Any], None]
# 任意处理器。
Handler: TypeAlias = AsyncHandler | SyncHandler

# 全局订阅者注册表：topic -> list of handlers。
# 用 dict[str, list] 而非 defaultdict，避免读取时创建空列表副作用。
_subscribers: dict[str, list[Handler]] = {}


def subscribe(topic: str, handler: Handler | None = None) -> Callable[[Handler], Handler] | Handler:
    """注册一个事件处理器。

    可作为装饰器或普通函数调用::

        @actant.on("task.failed")
        async def alert(event):
            send_alert(event.task_name)

        # 或
        actant.on("task.failed", alert)

    Args:
        topic: 事件主题，如 ``"task.completed"``、``"actor.failed"``。
            完整列表见模块文档字符串。
        handler: 事件处理器。``async`` 函数会被 ``await``；普通函数
            会在事件循环线程中同步调用（应快速返回）。

    Returns:
        装饰器函数（若 ``handler`` 为 None）或原 handler。
    """
    if handler is not None:
        _subscribers.setdefault(topic, []).append(handler)
        return handler

    def decorator(fn: Handler) -> Handler:
        _subscribers.setdefault(topic, []).append(fn)
        return fn

    return decorator


async def dispatch(topic: str, event: Any) -> None:
    """分发事件到所有订阅者。

    - ``async`` 处理器并发执行（``asyncio.gather``），异常隔离（单失败不影响其他）。
    - 同步处理器在事件循环线程直接调用（应快速返回，否则会阻塞循环）。

    本函数不会抛出异常：所有处理器异常被记录后吞掉，
    确保事件分发不阻塞调用方（如 OrchestrationLoop）。

    Args:
        topic: 事件主题。
        event: 事件对象，类型取决于主题（如 ``_TaskCompletion``、自定义 dataclass）。
    """
    handlers = _subscribers.get(topic)
    if not handlers:
        return

    async_handlers: list[AsyncHandler] = []
    sync_handlers: list[SyncHandler] = []

    for h in handlers:
        if inspect.iscoroutinefunction(h):
            async_handlers.append(cast(AsyncHandler, h))
        else:
            sync_handlers.append(cast(SyncHandler, h))

    # 同步处理器先执行（快速、确定性顺序）
    for h in sync_handlers:
        try:
            h(event)
        except Exception:
            logger.exception(
                "sync event handler %s failed for topic %s", getattr(h, "__name__", h), topic
            )

    # 异步处理器并发执行
    if async_handlers:
        results = await asyncio.gather(
            *(h(event) for h in async_handlers), return_exceptions=True
        )
        for h, result in zip(async_handlers, results, strict=True):
            if isinstance(result, BaseException):
                logger.exception(
                    "async event handler %s failed for topic %s",
                    getattr(h, "__name__", h),
                    topic,
                    exc_info=result if isinstance(result, Exception) else None,
                )


def clear(topic: str | None = None) -> None:
    """清除订阅者。

    主要用于测试隔离。生产代码不应调用。

    Args:
        topic: 指定主题则只清除该主题；None 清除所有主题。
    """
    if topic is None:
        _subscribers.clear()
    else:
        _subscribers.pop(topic, None)


def subscriber_count(topic: str) -> int:
    """返回指定主题的订阅者数量（主要用于测试断言）。"""
    return len(_subscribers.get(topic, []))
