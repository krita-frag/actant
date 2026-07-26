"""跨节点 e2e 测试：双节点 P2P 发现、任务路由、gossip 收敛。

验证 Actant 的真实分布式能力（非线程池模拟）：
1. 两节点通过 dial/add_gossip_peer 建立连接
2. gossip 网络收敛后双向可见
3. 跨节点任务提交与执行

这些测试使用真实 iroh P2P 网络（``discovery=none`` + 手动 dial），
不依赖中心化服务器，验证 Actant 的分布式架构完整性。
"""

from __future__ import annotations

import time

import pytest

import actant
from actant import Runtime
from actant.task import task
from tests.python._helpers import connect_peers

# 模块级状态：用于跟踪 flaky 任务在 worker 线程的调用次数
# （cloudpickle 序列化后闭包变量不共享，需用模块级状态）
_flaky_call_count: int = 0


@pytest.fixture
def two_nodes():
    """启动两个独立 Runtime 节点，手动 dial 连接。

    使用 ``discovery=none`` 避免自动发现干扰，手动 dial 建立连接。
    每个 Runtime 用独立 data_dir 避免持久化冲突。
    """
    import tempfile

    dir_a = tempfile.mkdtemp(prefix="actant-e2e-a-")
    dir_b = tempfile.mkdtemp(prefix="actant-e2e-b-")

    rt_a = Runtime.with_defaults(name="node-a", data_dir=dir_a)
    rt_b = Runtime.with_defaults(name="node-b", data_dir=dir_b)
    rt_a.start()
    rt_b.start()

    try:
        connected = connect_peers(rt_a, rt_b, timeout_s=15.0)
        if not connected:
            pytest.skip("P2P connection not established within timeout (network env)")
        yield rt_a, rt_b
    finally:
        rt_b.stop()
        rt_a.stop()
        # 清理临时目录
        import shutil

        shutil.rmtree(dir_a, ignore_errors=True)
        shutil.rmtree(dir_b, ignore_errors=True)


@pytest.fixture
def submit_only_and_worker_nodes():
    """启动一个仅提交节点和一个可执行 Worker 节点。"""
    import shutil
    import tempfile

    dir_a = tempfile.mkdtemp(prefix="actant-e2e-submit-")
    dir_b = tempfile.mkdtemp(prefix="actant-e2e-worker-")

    rt_a = Runtime.with_defaults(
        name="submit-node",
        data_dir=dir_a,
        max_concurrent_tasks=0,
    )
    rt_b = Runtime.with_defaults(
        name="worker-node",
        data_dir=dir_b,
        max_concurrent_tasks=1,
    )
    rt_a.start()
    rt_b.start()

    try:
        connected = connect_peers(rt_a, rt_b, timeout_s=15.0)
        if not connected:
            pytest.skip("P2P connection not established within timeout (network env)")
        yield rt_a, rt_b
    finally:
        rt_b.stop()
        rt_a.stop()
        shutil.rmtree(dir_a, ignore_errors=True)
        shutil.rmtree(dir_b, ignore_errors=True)


class TestP2PDiscovery:
    """双节点 P2P 发现与 gossip 收敛。"""

    def test_two_nodes_discover_each_other(self, two_nodes) -> None:
        """dial + add_gossip_peer 后，双方互相可见。"""
        rt_a, rt_b = two_nodes
        # connect_peers 已等待双向发现，这里再次验证状态稳定
        peers_a = rt_a.discover_peers()
        peers_b = rt_b.discover_peers()
        assert rt_b.peer_id in peers_a, (
            f"node-a should see node-b peer_id ({rt_b.peer_id}), "
            f"got peers: {peers_a}"
        )
        assert rt_a.peer_id in peers_b, (
            f"node-b should see node-a peer_id ({rt_a.peer_id}), "
            f"got peers: {peers_b}"
        )

    def test_node_ids_are_distinct(self, two_nodes) -> None:
        """两个节点的 node_id 不同（真实独立节点）。"""
        rt_a, rt_b = two_nodes
        assert rt_a.node_id != rt_b.node_id, (
            "distinct nodes must have distinct node_id"
        )
        assert rt_a.peer_id != rt_b.peer_id, (
            "distinct nodes must have distinct peer_id"
        )

    def test_listen_addresses_contain_endpoint_addr(self, two_nodes) -> None:
        """listen_addresses 返回有效的 endpoint_addr（非空 hex 字符串）。"""
        rt_a, _ = two_nodes
        addrs = rt_a.listen_addresses()
        assert addrs["endpoint_addr"], "endpoint_addr must not be empty"
        assert addrs["endpoint_id"], "endpoint_id must not be empty"
        # endpoint_addr 是 hex 编码，长度应为偶数
        assert len(addrs["endpoint_addr"]) % 2 == 0, (
            "endpoint_addr should be hex-encoded (even length)"
        )

    def test_peer_id_matches_endpoint_id(self, two_nodes) -> None:
        """peer_id 与 listen_addresses 的 endpoint_id 一致。"""
        rt_a, _ = two_nodes
        addrs = rt_a.listen_addresses()
        assert rt_a.peer_id == addrs["endpoint_id"], (
            f"peer_id ({rt_a.peer_id}) should match endpoint_id ({addrs['endpoint_id']})"
        )


class TestCrossNodeTaskExecution:
    """跨节点任务提交与执行。"""

    def test_local_task_executes_on_node_a(self, two_nodes) -> None:
        """node-a 上的任务在本地执行并返回结果。"""
        rt_a, _ = two_nodes

        @task
        def add(x: int, y: int) -> int:
            return x + y

        with actant.use_runtime(rt_a):
            handle = add.submit(3, 4)
            result = handle.result(timeout=10.0)
            assert result == 7

    def test_local_task_executes_on_node_b(self, two_nodes) -> None:
        """node-b 上的任务在本地执行并返回结果。"""
        _, rt_b = two_nodes

        @task
        def multiply(x: int, y: int) -> int:
            return x * y

        with actant.use_runtime(rt_b):
            handle = multiply.submit(5, 6)
            result = handle.result(timeout=10.0)
            assert result == 30

    def test_submit_only_node_dispatches_task_to_remote_worker(
        self, submit_only_and_worker_nodes
    ) -> None:
        """node-a 不允许本地执行时，普通 @task 可在 node-b 远程执行并回传结果。"""
        rt_a, rt_b = submit_only_and_worker_nodes

        @task
        def add(x: int, y: int) -> int:
            return x + y

        with actant.use_runtime(rt_a):
            handle = add.submit_to(
                rt_b.node_id,
                20,
                22,
                endpoint_addr=rt_b.listen_addresses()["endpoint_addr"],
            )
            result = handle.result(timeout=30.0)
            assert result == 42

    def test_task_with_retries_succeeds(self, two_nodes) -> None:
        """带重试的任务在临时失败后最终成功。"""
        rt_a, _ = two_nodes

        @task(retries=2, retry_delay_ms=10)
        def flaky() -> str:
            # 用模块级状态跟踪调用次数（cloudpickle 序列化后闭包变量不共享）
            import tests.python.e2e.test_multi_node as mod
            mod._flaky_call_count += 1
            if mod._flaky_call_count < 2:
                raise RuntimeError("temporary failure")
            return "ok"

        # 重置模块级计数器
        import tests.python.e2e.test_multi_node as mod
        mod._flaky_call_count = 0

        with actant.use_runtime(rt_a):
            handle = flaky.submit()
            result = handle.result(timeout=30.0)
            assert result == "ok"
            assert mod._flaky_call_count >= 2, (
                f"flaky should be called at least 2 times, got {mod._flaky_call_count}"
            )

    def test_task_failure_propagates(self, two_nodes) -> None:
        """任务异常通过 cloudpickle 序列化传播回调用方。"""
        rt_a, _ = two_nodes

        @task
        def fail() -> None:
            raise ValueError("intentional failure")

        with actant.use_runtime(rt_a):
            handle = fail.submit()
            with pytest.raises(ValueError, match="intentional failure"):
                handle.result(timeout=10.0)


class TestGossipConvergence:
    """gossip 网络状态收敛。"""

    def test_peer_count_stable_after_discovery(self, two_nodes) -> None:
        """发现后 peer 数量稳定（不抖动）。"""
        rt_a, rt_b = two_nodes
        # 多次采样，peer 列表应稳定
        for _ in range(3):
            peers_a = rt_a.discover_peers()
            peers_b = rt_b.discover_peers()
            assert len(peers_a) >= 1, "node-a should have at least 1 peer"
            assert len(peers_b) >= 1, "node-b should have at least 1 peer"
            time.sleep(0.1)

    def test_discovered_peer_id_matches(self, two_nodes) -> None:
        """发现的 peer_id 与对端 peer_id 一致。"""
        rt_a, rt_b = two_nodes
        peers_a = rt_a.discover_peers()
        # node-a 看到的 peer 列表应包含 node-b 的 peer_id
        assert rt_b.peer_id in peers_a
        peers_b = rt_b.discover_peers()
        assert rt_a.peer_id in peers_b
