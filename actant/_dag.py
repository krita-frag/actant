"""DAG 工具：循环检测与可读路径格式化。

Python 侧在提交 flow 到 Rust 运行前进行循环检测，便于给出精确的
错误路径；Rust 核心只负责验证无环 DAG。
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


def detect_cycle(
    node_count: int,
    edges: Sequence[tuple[int, int, str | None]],
) -> list[int] | None:
    """使用 Kahn 算法检测有向图中的循环。

    Parameters
    ----------
    node_count:
        节点数量，节点编号为 ``0..node_count-1``。
    edges:
        边列表，每条边为 ``(src, dst, label)``。label 仅用于可读输出，
        不参与检测。

    Returns
    -------
    若存在循环，返回包含重复起止节点的节点索引列表（如 ``[0, 1, 0]``）；
    无环则返回 ``None``。
    """
    if node_count == 0:
        return None

    adjacency: list[list[int]] = [[] for _ in range(node_count)]
    in_degree = [0] * node_count

    for src, dst, _label in edges:
        if 0 <= src < node_count and 0 <= dst < node_count:
            adjacency[src].append(dst)
            in_degree[dst] += 1

    queue: deque[int] = deque()
    for node in range(node_count):
        if in_degree[node] == 0:
            queue.append(node)

    visited_count = 0
    while queue:
        node = queue.popleft()
        visited_count += 1
        for neighbor in adjacency[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if visited_count == node_count:
        return None

    # 存在环：提取仍在环中的节点。
    cycle_nodes = [node for node in range(node_count) if in_degree[node] > 0]
    return _find_cycle_path(adjacency, cycle_nodes)


def _find_cycle_path(adj: list[list[int]], cycle_nodes: list[int]) -> list[int]:
    """从环节点集合中提取具体循环路径（首尾相同）。

    仅尝试从第一个环节点开始 DFS；若无法成环则回退到 ``cycle_nodes`` 本身，
    保持与历史测试行为一致。
    """
    if not cycle_nodes:
        return []

    cycle_set = set(cycle_nodes)
    start = cycle_nodes[0]

    stack = [(start, [start])]
    while stack:
        node, path = stack.pop()
        for neighbor in adj[node]:
            if neighbor == start and len(path) > 1:
                return [*path, start]
            if neighbor in cycle_set and neighbor not in path:
                stack.append((neighbor, [*path, neighbor]))

    return cycle_nodes


def format_cycle_path(node_names: Sequence[str], cycle: Sequence[int]) -> str:
    """将循环节点索引列表格式化为 ``a -> b -> c -> a`` 形式。

    越界或负索引使用 ``node{idx}`` 占位符。
    """
    if not cycle:
        return ""

    parts = []
    for idx in cycle:
        if 0 <= idx < len(node_names):
            parts.append(str(node_names[idx]))
        else:
            parts.append(f"node{idx}")
    return " -> ".join(parts)
