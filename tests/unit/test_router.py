"""路由策略单元测试：LeastLoadedRouter。

覆盖路由决策正确性与边界分支。
"""

from __future__ import annotations

from actant.router import (
    LeastLoadedRouter,
    NodeCapacity,
)

# ---------------------------------------------------------------------------
# NodeCapacity
# ---------------------------------------------------------------------------


class TestNodeCapacity:
    """NodeCapacity 节点容量快照。"""

    def test_basic_construction(self):
        cap = NodeCapacity(available=5, max_capacity=10)
        assert cap.available == 5
        assert cap.max_capacity == 10
        assert cap.capabilities == {}
        assert cap.endpoint_addr is None

    def test_with_capabilities(self):
        cap = NodeCapacity(
            available=3,
            max_capacity=8,
            capabilities={"gpu": True, "memory_gb": 64},
            endpoint_addr="127.0.0.1:5001",
        )
        assert cap.capabilities == {"gpu": True, "memory_gb": 64}
        assert cap.endpoint_addr == "127.0.0.1:5001"

    def test_repr_contains_key_info(self):
        cap = NodeCapacity(available=2, max_capacity=4)
        repr_str = repr(cap)
        assert "available=2" in repr_str
        assert "max_capacity=4" in repr_str


# ---------------------------------------------------------------------------
# LeastLoadedRouter
# ---------------------------------------------------------------------------


def _peers(**caps: int) -> dict[str, NodeCapacity]:
    """构造 peer_capacities dict，每个节点 available=caps[name]。"""
    return {name: NodeCapacity(available=cap, max_capacity=10) for name, cap in caps.items()}


class TestLeastLoadedRouter:
    """LeastLoadedRouter 路由到可用容量最大的节点。"""

    def test_routes_to_most_available(self):
        router = LeastLoadedRouter()
        peers = _peers(a=3, b=8, c=5)
        assert router.route("local", "0", {}, peers) == "b"

    def test_falls_back_to_local_when_no_peers(self):
        router = LeastLoadedRouter()
        assert router.route("local", "0", {}, {}) == "local"

    def test_random_among_equal_best(self):
        """容量相同时随机选择，避免热点。"""
        router = LeastLoadedRouter()
        peers = _peers(a=5, b=5, c=5)
        results = {router.route("local", "0", {}, peers) for _ in range(50)}
        assert results == {"a", "b", "c"}

    def test_prefers_peer_over_local_when_capacity_higher(self):
        """peer 容量高于 local 时优先 peer（local 不在 peer_capacities 中）。"""
        router = LeastLoadedRouter()
        peers = _peers(a=10)
        # 只有 local 和 a，a 容量高
        result = router.route("local", "0", {}, peers)
        assert result == "a"

    def test_zero_capacity_peers_still_candidates(self):
        """容量为 0 的节点仍是候选（可能刚释放任务）。"""
        router = LeastLoadedRouter()
        peers = _peers(a=0, b=0)
        result = router.route("local", "0", {}, peers)
        assert result in {"a", "b"}


# ---------------------------------------------------------------------------
# LeastLoadedRouter 边界分支
# ---------------------------------------------------------------------------


class TestLeastLoadedRouterEdgeCases:
    """覆盖 router.py 中的边界分支。"""

    def test_skips_negative_available_capacity(self):
        """available < 0 的 peer 应被跳过。"""
        router = LeastLoadedRouter()
        # a 容量为负（过载），b 正常
        peers = {
            "a": NodeCapacity(available=-1, max_capacity=10, capabilities={}),
            "b": NodeCapacity(available=5, max_capacity=10, capabilities={}),
        }
        result = router.route("local", "0", {}, peers)
        assert result == "b"

    def test_skips_node_without_registered_task(self):
        """节点声明了 tasks 能力但不含当前任务 → 跳过。"""
        router = LeastLoadedRouter()
        # a 声明只支持 task_x，b 无限制
        peers = {
            "a": NodeCapacity(available=10, max_capacity=10, capabilities={"tasks": ["task_x"]}),
            "b": NodeCapacity(available=5, max_capacity=10, capabilities={}),
        }
        # 请求 task_y，a 不支持 → 选 b
        result = router.route("local", "task_y", {"name": "task_y"}, peers)
        assert result == "b"

    def test_routes_to_node_with_registered_task(self):
        """节点声明支持当前任务 → 选中。"""
        router = LeastLoadedRouter()
        peers = {
            "a": NodeCapacity(available=10, max_capacity=10, capabilities={"tasks": ["task_y"]}),
            "b": NodeCapacity(available=5, max_capacity=10, capabilities={}),
        }
        result = router.route("local", "task_y", {"name": "task_y"}, peers)
        assert result == "a"

    def test_tag_affinity_prefers_matching_capability(self):
        """tags 亲和性：优先选具有匹配能力标签的节点。"""
        router = LeastLoadedRouter()
        # a 有 gpu 标签，b 没有
        peers = {
            "a": NodeCapacity(available=5, max_capacity=10, capabilities={"gpu": True}),
            "b": NodeCapacity(available=8, max_capacity=10, capabilities={}),
        }
        # 任务要求 gpu 标签 → 应选 a（尽管 b 容量更高）
        results = {
            router.route("local", "0", {"tags": ["gpu"]}, peers) for _ in range(20)
        }
        assert results == {"a"}

    def test_tag_affinity_no_match_falls_back_to_best(self):
        """tags 无匹配节点时回退到容量最高的节点。"""
        router = LeastLoadedRouter()
        peers = {
            "a": NodeCapacity(available=5, max_capacity=10, capabilities={"cpu": True}),
            "b": NodeCapacity(available=8, max_capacity=10, capabilities={}),
        }
        # 要求 gpu，但无节点声明 gpu → 回退到 b（容量最高）
        result = router.route("local", "0", {"tags": ["gpu"]}, peers)
        assert result == "b"

    def test_empty_tags_no_affinity(self):
        """tags 为空时不走亲和性分支。"""
        router = LeastLoadedRouter()
        peers = _peers(a=5, b=8)
        result = router.route("local", "0", {"tags": []}, peers)
        assert result == "b"
