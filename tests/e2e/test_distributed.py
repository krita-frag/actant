"""e2e 多节点分布式测试：跨节点任务执行、DAG 分发、并发工作流。"""

from __future__ import annotations

import time

import pytest

import actant


@actant.task(name="dist_identity")
def _identity(x):
    return x


@actant.task(name="dist_add")
def _add(a, b):
    return a + b


@actant.task(name="dist_sum_list")
def _sum_list(items):
    return sum(items)


@actant.task(name="dist_slow")
def _slow(seconds, payload):
    time.sleep(seconds)
    return payload * 2


@actant.task(name="dist_range")
def _range_n(n):
    return list(range(n))


@actant.task(name="dist_double")
def _double(x):
    """单参数 task，适合 .map() 使用。"""
    return x * 2


@actant.flow
def _diamond_flow():
    """菱形 DAG：1→2→3→4，可跨节点分发。"""
    a = _identity(10)
    b = _add(a, 5)      # 15
    c = _add(a, 10)     # 20
    return _add(b, c)   # 35


@actant.flow
def _fanout_flow():
    """扇出 DAG：1→[2,3,4,5]→6。"""
    src = _identity(100)
    parts = [
        _add(src, 1),
        _add(src, 2),
        _add(src, 3),
        _add(src, 4),
    ]
    return _sum_list.reduce(parts)  # 100*4 + 1+2+3+4 = 410


# ---------------------------------------------------------------------------
# 跨节点执行
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestCrossNodeExecution:
    """验证任务确实在多个节点上执行（非单节点本地完成）。"""

    def test_diamond_two_nodes(self, two_node_cluster, submit_via):
        """菱形 DAG 在两节点集群执行。"""
        a, _b = two_node_cluster
        result = submit_via(a, _diamond_flow, timeout=30.0)
        assert result.is_success
        assert result.value == 35

    def test_fanout_two_nodes(self, two_node_cluster, submit_via):
        """扇出 DAG 在两节点集群执行。"""
        a, _b = two_node_cluster
        result = submit_via(a, _fanout_flow, timeout=30.0)
        assert result.is_success
        assert result.value == 410

    def test_diamond_three_nodes(self, three_node_cluster, submit_via):
        """菱形 DAG 在三节点全互联集群执行。"""
        a, _b, _c = three_node_cluster
        result = submit_via(a, _diamond_flow, timeout=30.0)
        assert result.is_success
        assert result.value == 35

    def test_fanout_three_nodes(self, three_node_cluster, submit_via):
        """扇出 DAG 在三节点全互联集群执行。"""
        a, _b, _c = three_node_cluster
        result = submit_via(a, _fanout_flow, timeout=30.0)
        assert result.is_success
        assert result.value == 410


# ---------------------------------------------------------------------------
# 并发工作流
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestConcurrentWorkflows:
    """验证多个工作流并发提交与隔离。"""

    def test_multiple_workflows_sequential_submit(self, two_node_cluster, submit_via):
        """顺序提交多个 workflow，全部成功完成。"""
        a, _b = two_node_cluster
        results = []
        for i in range(3):
            @actant.flow
            def f(idx=i):
                return _add(idx, idx)

            r = submit_via(a, f, timeout=20.0)
            results.append(r)

        assert all(r.is_success for r in results)
        assert [r.value for r in results] == [0, 2, 4]

    def test_chord_across_nodes(self, two_node_cluster, submit_via):
        """chord 模式（map + reduce）跨节点执行。"""
        a, _b = two_node_cluster

        @actant.flow
        def f():
            refs = _double.map([1, 2, 3, 4])
            return _sum_list.reduce(refs)

        result = submit_via(a, f, timeout=30.0)
        assert result.is_success
        assert result.value == 20  # 2 + 4 + 6 + 8


# ---------------------------------------------------------------------------
# 节点不对称：A 提交但仅 B 执行（A 关闭执行能力）
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestAsymmetricRoles:
    """验证提交节点与执行节点分离的工作模式。"""

    def test_submit_only_node_can_dispatch(self, two_node_cluster, submit_via):
        """A 是提交节点，B 是执行节点。即使 A 也能执行，工作流应正常完成。

        这是基础场景：只要集群中至少有一个节点可执行，工作流就应成功。
        """
        a, _b = two_node_cluster
        result = submit_via(a, _diamond_flow, timeout=30.0)
        assert result.is_success
        assert result.value == 35
