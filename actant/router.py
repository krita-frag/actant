"""分布式调度任务路由策略。

TaskRouter 根据集群范围的容量信息决定哪个节点应该执行任务。
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import Any


class NodeCapacity:
    """节点资源容量的快照。"""

    __slots__ = ("available", "capabilities", "endpoint_addr", "max_capacity")

    def __init__(
        self,
        available: int,
        max_capacity: int,
        *,
        capabilities: dict[str, Any] | None = None,
        endpoint_addr: str | None = None,
    ) -> None:
        self.available = available
        self.max_capacity = max_capacity
        self.capabilities: dict[str, Any] = capabilities or {}
        self.endpoint_addr: str | None = endpoint_addr

    def __repr__(self) -> str:
        cap_str = f", capabilities={self.capabilities}" if self.capabilities else ""
        return (
            f"NodeCapacity(available={self.available}, "
            f"max_capacity={self.max_capacity}{cap_str})"
        )


class TaskRouter(ABC):
    """任务路由策略的基类。

    子类化并实现 ``route`` 方法以定义自定义路由逻辑。
    """

    @abstractmethod
    def route(
        self,
        local_node: str,
        node_key: str,
        task_meta: dict[str, Any],
        peer_capacities: dict[str, NodeCapacity],
    ) -> str | None:
        """返回应该执行此任务的节点 ID。

        Args:
            local_node: 本地（提交）节点的 ID。
            node_key: DAG 节点索引的字符串形式（如 "0"、"1"），用于标识
                本次提交上下文中的节点。注意：非全局 TaskId。
            task_meta: 任务元数据字典，包含以下键：
                - name: 任务名称
                - tags: 标签字符串列表（可能为空）
                - priority: 优先级（int/str/None）
            peer_capacities: 所有已知对等节点的 node_id -> NodeCapacity 映射。
                每个 NodeCapacity 包含一个 ``capabilities`` 字典，
                用于基于亲和性的路由。

        Returns:
            节点 ID 字符串，或 None 让运行时决定（默认为本地节点）。
        """
        ...


class LeastLoadedRouter(TaskRouter):
    """将任务路由到可用容量最大的节点。

    当多个节点具有相同的可用容量时，随机选择一个，
    以避免负载集中在单个节点上。

    如果没有对等节点有容量，则回退到本地节点。
    """

    def route(
        self,
        local_node: str,
        node_key: str,
        task_meta: dict[str, Any],
        peer_capacities: dict[str, NodeCapacity],
    ) -> str | None:
        if not peer_capacities:
            return local_node

        task_name = (task_meta or {}).get("name", "")
        tags = (task_meta or {}).get("tags", [])

        # 单次遍历：同时过滤符合条件的节点并跟踪负载最低的节点
        eligible: list[str] = []
        best_available = -1
        best_nodes: list[str] = []

        for node_id, cap in peer_capacities.items():
            if cap.available < 0:
                continue
            registered_tasks = cap.capabilities.get("tasks")
            if registered_tasks is not None and task_name and task_name not in registered_tasks:
                continue

            eligible.append(node_id)
            if cap.available > best_available:
                best_available = cap.available
                best_nodes = [node_id]
            elif cap.available == best_available:
                best_nodes.append(node_id)

        if not eligible:
            return local_node

        # 基于标签的亲和性：优先选择具有匹配能力的节点
        if tags:
            affinity_nodes = [
                nid for nid in eligible
                if any(t in peer_capacities[nid].capabilities for t in tags)
            ]
            if affinity_nodes:
                return random.choice(affinity_nodes)

        return random.choice(best_nodes)
