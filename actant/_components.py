"""可组合的内部组件接口。

这些接口用于把 `_Node` 的硬编码行为拆分为可替换的策略组件。
它们不是公共 API，但未来可能稳定为公共扩展点。

设计原则
--------

决策型扩展点（必须返回值、同步决策）使用 ABC：
- ``TaskRouter``：返回目标 node_id
- ``CapacityProvider``：返回容量快照
- ``PayloadSerializer``：返回 bytes

反应型扩展点（副作用驱动、可多订阅者、无返回值）走事件订阅：
见 ``actant._events`` 和 ``actant.on``。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class CapacityProvider(ABC):
    """对等节点容量缓存策略。"""

    @abstractmethod
    def update(
        self,
        node_id: str,
        available: int,
        max_capacity: int,
        *,
        endpoint_addr: str | None = None,
    ) -> None:
        """更新或添加一个对等节点的容量快照。"""

    @abstractmethod
    def snapshot(
        self,
        local_node_id: str,
        local_capabilities: dict[str, Any],
        local_tasks: list[str],
    ) -> dict[str, Any]:
        """返回包含本地节点在内的所有已知节点容量快照。"""

    @abstractmethod
    def endpoint_addr(self, node_id: str) -> str | None:
        """返回节点已知的 endpoint 地址。"""


class EventContext:
    """编排循环内部上下文。

    仅包含决策点依赖（router / serializer / capacity_provider），
    不再持有事件回调字段 —— 反应点通过 ``actant._events.dispatch``
    分发给全局订阅者。

    通过 dataclass 风格的属性把运行时依赖注入 ``DefaultOrchestrationEventHandler``，
    避免 handler 直接依赖 ``_Node`` 的具体实现。
    """

    def __init__(
        self,
        *,
        node_id: str,
        runtime: Any,
        router: Any,
        serializer: Any,
        capacity_provider: CapacityProvider,
        local_tasks: list[str] | None = None,
        condition_evaluators: dict[str, Any] | None = None,
    ) -> None:
        self.node_id = node_id
        self.runtime = runtime
        self.router = router
        self.serializer = serializer
        self.capacity_provider = capacity_provider
        self.local_tasks = local_tasks or []
        self.condition_evaluators = (
            condition_evaluators if condition_evaluators is not None else {}
        )
