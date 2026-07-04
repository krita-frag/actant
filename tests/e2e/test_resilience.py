"""e2e 故障转移、持久化、取消测试。"""

from __future__ import annotations

import time
from typing import Any

import pytest

import actant
from actant.exceptions import WorkflowCancelledError, WorkflowFailedError


@actant.task(name="resil_identity")
def _identity(x):
    return x


@actant.task(name="resil_add")
def _add(a, b):
    return a + b


@actant.task(name="resil_slow")
def _slow(seconds, marker):
    """慢任务：用于测试取消与故障转移时机。"""
    time.sleep(seconds)
    return marker


@actant.task(name="resil_sum_list")
def _sum_list(items):
    return sum(items)


@actant.task(name="resil_double")
def _double(x):
    return x * 2


# ---------------------------------------------------------------------------
# 取消
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestWorkflowCancellation:
    """工作流取消语义验证。"""

    def test_cancel_running_workflow(self, two_node_cluster):
        """取消正在执行慢任务的工作流，state 应转为 Cancelled。"""
        a, _b = two_node_cluster

        @actant.flow
        def f():
            return _slow(5.0, "should-be-cancelled")

        result = a.submit(f)
        # 等任务真正开始执行
        time.sleep(0.5)
        a.cancel(result.workflow_id)

        # 取消后 get_sync 应抛 WorkflowCancelledError 或返回非 success
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if result.ready():
                break
            time.sleep(0.1)

        assert result.ready()
        # state 应为 Cancelled（或非 Completed 的终态）
        state = result.state()
        assert state in ("Cancelled", "Failed", "CancellationRequested"), (
            f"unexpected state after cancel: {state}"
        )
        # get_sync 应抛 WorkflowCancelledError
        with pytest.raises((WorkflowCancelledError, WorkflowFailedError)):
            result.get_sync(timeout=2.0)

    def test_cancel_completed_workflow_is_noop(self, two_node_cluster, submit_via):
        """对已完成的工作流调用 cancel 应是 no-op（不抛异常）。"""
        a, _b = two_node_cluster

        @actant.flow
        def f():
            return _identity(42)

        result = submit_via(a, f, timeout=10.0)
        assert result.is_success
        # 已完成 workflow 的 cancel 应安全（幂等）
        a.cancel(result.workflow_id)


# ---------------------------------------------------------------------------
# 节点故障与恢复
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestNodeFailure:
    """节点关闭后的集群行为。"""

    def test_workflow_completes_after_peer_shutdown(
        self, three_node_cluster, submit_via
    ):
        """三节点集群：提交后关闭一个非提交节点，工作流仍应完成。

        验证：剩余节点（提交节点 + 一个执行节点）足以完成 workflow。
        """
        a, _b, c = three_node_cluster

        @actant.flow
        def f():
            x = _identity(7)
            return _add(x, 8)  # 15

        result = a.submit(f)
        # 等任务开始
        time.sleep(0.3)
        # 关闭 c（非提交节点）
        c.shutdown(timeout=3.0)
        # workflow 仍应完成（A 和 B 还活着）
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            if result.ready():
                break
            time.sleep(0.1)
        assert result.ready()
        r = result.get_sync(timeout=2.0)
        assert r.is_success
        assert r.value == 15

    def test_submit_after_peer_loss(self, two_node_cluster, submit_via):
        """关闭一个节点后，剩余节点仍能接受新 workflow。"""
        a, b = two_node_cluster

        # 关闭 b
        b.shutdown(timeout=3.0)
        time.sleep(0.5)  # 等待网络感知

        # 在 a 上提交新 workflow
        @actant.flow
        def f():
            return _identity(99)

        result = submit_via(a, f, timeout=15.0)
        assert result.is_success
        assert result.value == 99


# ---------------------------------------------------------------------------
# 持久化（使用 data_dir 的节点）
# ---------------------------------------------------------------------------


def _make_persistent_node(name: str, data_dir: Any) -> actant._node._Node:
    """创建带持久化的节点。"""
    from actant._node import _Node
    from actant.config import NetworkConfig

    return _Node(
        name=name,
        _executing=True,
        network=NetworkConfig(preset="local"),
        port=0,
        data_dir=str(data_dir),
        signing_key="test-key",
    )


@pytest.mark.e2e
class TestPersistence:
    """节点持久化与重启恢复。"""

    def test_workflow_history_survives_restart(self, tmp_path, submit_via):
        """带 data_dir 的节点重启后，已完成 workflow 状态应可查询。"""
        from tests._helpers.network import run_node_in_thread

        # 第一次启动：提交并完成 workflow
        node1 = _make_persistent_node("persist-a", tmp_path)
        ready = __import__("threading").Event()
        t1 = run_node_in_thread(node1, ready_event=ready, timeout_s=30.0)

        try:
            @actant.flow
            def f():
                return _identity(123)

            result = submit_via(node1, f, timeout=15.0)
            assert result.is_success
            assert result.value == 123
            wf_id = result.workflow_id
        finally:
            node1.shutdown(timeout=5.0)
            t1.join(timeout=5.0)

        # 第二次启动：同一 data_dir，应能查询到历史 workflow
        node2 = _make_persistent_node("persist-a-restart", tmp_path)
        ready2 = __import__("threading").Event()
        t2 = run_node_in_thread(node2, ready_event=ready2, timeout_s=30.0)

        try:
            # 查询历史 workflow 状态（持久化恢复后应可查到）
            state = node2.workflow_state(wf_id)
            assert state is not None, (
                f"workflow {wf_id} 状态未持久化；list_workflows={node2.list_workflows()}"
            )
            # 已完成的 workflow 重启后状态应保持 Completed（或类似终态）
            assert state in ("Completed", "completed"), (
                f"workflow 重启后状态异常: {state}"
            )
        finally:
            node2.shutdown(timeout=5.0)
            t2.join(timeout=5.0)
