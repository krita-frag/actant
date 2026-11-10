"""拓扑：集群连接关系的显式描述与动态变更。

拓扑只描述"哪些节点之间应该存在连接"，不负责拉起节点。``TestCluster``
在启动、动态加入节点或应用新拓扑时按本模块的边集拨号/断开。

所有边都是**有向边** ``(source, target)``，语义为 ``source`` 能单向看到
``target``。预置拓扑（mesh/ring/line/star）默认双向；``custom`` 默认保留
调用方给出的方向，便于构造非对称网络与单向分区。

注意：Actant 的 gossip 会把**可达**节点的信息传播给同一连通分量内的其他
节点，因此"两个节点之间没有直连边"不等于"彼此不可见"。要制造真正的隔离，
必须切断跨组全部链路（``TestCluster.split()`` / ``isolate()``），使连通分量
本身分裂。
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any, Literal

TopologyKind = Literal["mesh", "ring", "line", "star", "custom"]


@dataclass(frozen=True)
class Topology:
    """不可变有向图拓扑。

    Args:
        nodes: 节点逻辑名（顺序决定确定性遍历/报告顺序）。
        edges: 有向边集合 ``(source, target)``。
        name: 拓扑名称（报告/断言用）。
    """

    nodes: tuple[str, ...]
    edges: frozenset[tuple[str, str]]
    name: str = "custom"

    def __post_init__(self) -> None:
        if not self.nodes:
            raise ValueError("topology requires at least one node")
        if len(set(self.nodes)) != len(self.nodes):
            raise ValueError("topology nodes must be unique")
        allowed = set(self.nodes)
        for source, target in self.edges:
            if source not in allowed or target not in allowed:
                raise ValueError(f"edge ({source!r}, {target!r}) references unknown node")
            if source == target:
                raise ValueError(f"self edge not allowed: {source!r}")

    # ------------------------------------------------------------------
    # 预置拓扑
    # ------------------------------------------------------------------

    @classmethod
    def mesh(cls, nodes: Iterable[str]) -> Topology:
        """全互联（每个有序对双向可见）。"""
        ordered = tuple(nodes)
        edges = frozenset(
            (a, b) for a in ordered for b in ordered if a != b
        )
        return cls(ordered, edges, name="mesh")

    @classmethod
    def ring(cls, nodes: Iterable[str]) -> Topology:
        """环：``a-b-c-a`` 双向。"""
        ordered = tuple(nodes)
        if len(ordered) < 2:
            raise ValueError("ring requires at least two nodes")
        edges: set[tuple[str, str]] = set()
        for i, node in enumerate(ordered):
            nxt = ordered[(i + 1) % len(ordered)]
            if node != nxt:
                edges.add((node, nxt))
                edges.add((nxt, node))
        return cls(ordered, frozenset(edges), name="ring")

    @classmethod
    def line(cls, nodes: Iterable[str]) -> Topology:
        """链：相邻节点双向连接。"""
        ordered = tuple(nodes)
        if len(ordered) < 2:
            raise ValueError("line requires at least two nodes")
        edges: set[tuple[str, str]] = set()
        for left, right in pairwise(ordered):
            edges.add((left, right))
            edges.add((right, left))
        return cls(ordered, frozenset(edges), name="line")

    @classmethod
    def star(cls, center: str, leaves: Iterable[str]) -> Topology:
        """星型：中心与每个叶子双向连接，叶子之间不直连。"""
        leaf_list = tuple(leaves)
        if not leaf_list:
            raise ValueError("star requires at least one leaf")
        if center in leaf_list:
            raise ValueError("center must not also be a leaf")
        ordered = (center, *leaf_list)
        edges: set[tuple[str, str]] = set()
        for leaf in leaf_list:
            edges.add((center, leaf))
            edges.add((leaf, center))
        return cls(ordered, frozenset(edges), name="star")

    @classmethod
    def custom(
        cls,
        nodes: Iterable[str],
        edges: Iterable[tuple[str, str]],
        *,
        bidirectional: bool = False,
        name: str = "custom",
    ) -> Topology:
        """自定义图；``bidirectional=True`` 时自动补反向边。"""
        expanded: set[tuple[str, str]] = set()
        for source, target in edges:
            expanded.add((source, target))
            if bidirectional:
                expanded.add((target, source))
        return cls(tuple(nodes), frozenset(expanded), name=name)

    @classmethod
    def from_name(cls, name: TopologyKind, nodes: Iterable[str], **kwargs: Any) -> Topology:
        """按名称构造预置拓扑（``star`` 需传 ``center=``）。"""
        ordered = tuple(nodes)
        if name == "mesh":
            return cls.mesh(ordered)
        if name == "ring":
            return cls.ring(ordered)
        if name == "line":
            return cls.line(ordered)
        if name == "star":
            center = kwargs.pop("center", None)
            if center is None:
                center = ordered[0]
            if center not in ordered:
                raise ValueError(f"star center {center!r} is not in nodes")
            leaves = [n for n in ordered if n != center]
            return cls.star(center, leaves)
        raise ValueError(f"unknown topology preset: {name!r}")

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def neighbours(self, node: str) -> set[str]:
        """``node`` 单向可见的目标节点集合。"""
        return {target for source, target in self.edges if source == node}

    def out_edges(self, node: str) -> set[tuple[str, str]]:
        return {(source, target) for source, target in self.edges if source == node}

    def in_edges(self, node: str) -> set[tuple[str, str]]:
        return {(source, target) for source, target in self.edges if target == node}

    def undirected_edges(self) -> frozenset[tuple[str, str]]:
        """规范化无向边（端点按字典序排序）。"""
        return frozenset(
            (source, target) if source < target else (target, source)
            for source, target in self.edges
        )

    def is_connected(self) -> bool:
        """无向连通性（忽略方向）。"""
        if len(self.nodes) <= 1:
            return True
        adjacency: dict[str, set[str]] = {node: set() for node in self.nodes}
        for source, target in self.edges:
            adjacency[source].add(target)
            adjacency[target].add(source)
        seen = {self.nodes[0]}
        queue = deque([self.nodes[0]])
        while queue:
            current = queue.popleft()
            for nxt in adjacency[current]:
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        return len(seen) == len(self.nodes)

    def components(self) -> list[tuple[str, ...]]:
        """无向连通分量（按节点声明顺序输出）。"""
        adjacency: dict[str, set[str]] = {node: set() for node in self.nodes}
        for source, target in self.edges:
            adjacency[source].add(target)
            adjacency[target].add(source)
        seen: set[str] = set()
        result: list[tuple[str, ...]] = []
        for node in self.nodes:
            if node in seen:
                continue
            group: list[str] = []
            queue = deque([node])
            seen.add(node)
            while queue:
                current = queue.popleft()
                group.append(current)
                for nxt in sorted(adjacency[current]):
                    if nxt not in seen:
                        seen.add(nxt)
                        queue.append(nxt)
            result.append(tuple(group))
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "nodes": list(self.nodes),
            "edges": sorted(self.edges),
            "connected": self.is_connected(),
            "components": [list(c) for c in self.components()],
        }

    def with_nodes(self, nodes: Iterable[str]) -> Topology:
        """替换节点集合并丢弃指向未知节点的边。"""
        ordered = tuple(nodes)
        allowed = set(ordered)
        edges = {e for e in self.edges if e[0] in allowed and e[1] in allowed}
        return Topology(ordered, frozenset(edges), name=self.name)


@dataclass
class TopologyChange:
    """一次拓扑变更的差分（供报告与断言使用）。"""

    added_edges: frozenset[tuple[str, str]] = field(default_factory=frozenset)
    removed_edges: frozenset[tuple[str, str]] = field(default_factory=frozenset)

    def to_dict(self) -> dict[str, Any]:
        return {
            "added_edges": sorted(self.added_edges),
            "removed_edges": sorted(self.removed_edges),
        }


def diff_topology(before: Topology, after: Topology) -> TopologyChange:
    """计算两个拓扑的边差分。"""
    return TopologyChange(
        added_edges=after.edges - before.edges,
        removed_edges=before.edges - after.edges,
    )
