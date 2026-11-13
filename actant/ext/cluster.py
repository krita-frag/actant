"""集群视图与成员差分。

``ClusterView`` 只读封装 ``Runtime.peers()``；``watch()`` 通过轮询产生
join/leave/capacity 变化。它是纯 Python 便利层，不新增成员协议，也不改变
心跳与故障判定语义。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from actant.ext.types import NodeView


@dataclass(frozen=True)
class ClusterChange:
    """一次轮询窗口内的成员变化。"""

    joined: tuple[NodeView, ...] = ()
    left: tuple[NodeView, ...] = ()
    changed: tuple[NodeView, ...] = ()


class ClusterView:
    """对 ``Runtime.peers()`` 的只读视图。"""

    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime

    def nodes(self) -> list[NodeView]:
        """返回当前在线节点，按 ``node_id`` 排序。"""
        return sorted(
            (NodeView.from_peer(peer) for peer in self._runtime.peers()),
            key=lambda node: node.node_id,
        )

    def snapshot(self) -> dict[str, NodeView]:
        """返回 ``node_id -> NodeView`` 快照。"""
        return {node.node_id: node for node in self.nodes()}

    def watch(
        self,
        *,
        interval_s: float = 1.0,
        stop_event: threading.Event | None = None,
    ) -> Iterator[ClusterChange]:
        """轮询并产出成员变化。

        - 只报告 join/leave 与容量/标签/角色变化；``last_heartbeat_ms`` 单独变化
          不视为 change，避免每次心跳都产生事件。
        - 生成器关闭或 ``stop_event`` 置位后停止。
        """
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        previous = self.snapshot()
        while stop_event is None or not stop_event.is_set():
            time.sleep(interval_s)
            if stop_event is not None and stop_event.is_set():
                return
            current = self.snapshot()
            joined = tuple(current[node_id] for node_id in sorted(current.keys() - previous.keys()))
            left = tuple(previous[node_id] for node_id in sorted(previous.keys() - current.keys()))
            changed = tuple(
                current[node_id]
                for node_id in sorted(current.keys() & previous.keys())
                if _change_key(current[node_id]) != _change_key(previous[node_id])
            )
            previous = current
            if joined or left or changed:
                yield ClusterChange(joined=joined, left=left, changed=changed)


def _change_key(node: NodeView) -> tuple[Any, ...]:
    """参与变化判定的字段；排除纯心跳时间。"""
    return (
        node.available_slots,
        node.max_slots,
        node.active_workflows,
        node.labels,
        node.available_quotas,
        node.role,
        node.draining,
        node.platform,
    )
