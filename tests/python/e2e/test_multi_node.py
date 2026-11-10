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
from actant import Runtime, testkit
from actant.tasks import task
from tests.python._helpers import connect_peers, wait_until

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

    rt_a = Runtime(name="node-a", data_dir=dir_a)
    rt_b = Runtime(name="node-b", data_dir=dir_b)
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

    rt_a = Runtime(
        name="submit-node",
        data_dir=dir_a,
        max_concurrent_tasks=0,
    )
    rt_b = Runtime(
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

    def test_task_with_retries_succeeds(self, two_nodes, tmp_path) -> None:
        """带重试的任务在临时失败后最终成功。"""
        rt_a, _ = two_nodes
        marker = tmp_path / "retry_attempts.txt"

        @task(retries=2, retry_delay_ms=10)
        def flaky(marker_path: str) -> str:
            # 重试在同一个 worker 子进程内进行，每次尝试把标记写入临时文件，
            # 供父进程统计尝试次数（模块级状态无法跨进程共享）。
            import tests.python.e2e.test_multi_node as mod
            mod._flaky_call_count += 1
            with open(marker_path, "a") as f:
                f.write("attempt\n")
                f.flush()
            if mod._flaky_call_count < 2:
                raise RuntimeError("temporary failure")
            return "ok"

        # 重置模块级计数器（worker 进程内独立副本，父进程仅用于验证结果）
        import tests.python.e2e.test_multi_node as mod
        mod._flaky_call_count = 0

        with actant.use_runtime(rt_a):
            handle = flaky.submit(str(marker))
            result = handle.result(timeout=30.0)
            assert result == "ok"
            # worker 子进程在重试期间把每次尝试写入标记文件
            assert marker.read_text().count("attempt") >= 2, (
                f"flaky should be called at least 2 times, "
                f"got {marker.read_text().count('attempt')}"
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


class TestNodeVisibility:
    """节点可见性：peers() 返回对端 slots/labels。"""

    @pytest.fixture
    def two_nodes_with_labels(self):
        """node-b 携带自定义标签与并发配置启动的双节点拓扑。"""
        import tempfile

        from actant.actant import _ActantConfig, _NetworkConfig

        dir_a = tempfile.mkdtemp(prefix="actant-e2e-vis-a-")
        dir_b = tempfile.mkdtemp(prefix="actant-e2e-vis-b-")

        rt_a = Runtime(name="vis-node-a", data_dir=dir_a)
        config_b = _ActantConfig(
            payload_signing_key="",
            network=_NetworkConfig(preset="local"),
            max_concurrent_tasks=3,
            node_labels={"role": "worker", "zone": "az-1"},
        )
        rt_b = Runtime(
            name="vis-node-b", data_dir=dir_b, config=config_b
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
            import shutil

            shutil.rmtree(dir_a, ignore_errors=True)
            shutil.rmtree(dir_b, ignore_errors=True)

    def test_peers_returns_remote_slots_and_labels(self, two_nodes_with_labels) -> None:
        rt_a, rt_b = two_nodes_with_labels
        node_b_id = rt_b.node_id

        deadline = time.time() + 15
        peer = None
        while time.time() < deadline:
            peers = rt_a.peers()
            peer = next((p for p in peers if p["node_id"] == node_b_id), None)
            if peer is not None:
                break
            time.sleep(0.2)

        assert peer is not None, f"node-b not visible in peers(): {rt_a.peers()}"
        # slots 来自心跳容量视图（node-b 配置 max_concurrent_tasks=3）
        assert peer["max_slots"] == 3
        assert peer["available_slots"] == 3
        # labels 为用户自定义标签
        assert peer["labels"] == {"role": "worker", "zone": "az-1"}
        # platform 携带核心自动填充的平台三要素
        assert peer["platform"] is not None
        assert peer["platform"]["os"]
        assert peer["platform"]["arch"]
        assert peer["platform"]["actant_version"]
        assert peer["last_heartbeat_ms"] > 0

    def test_peers_excludes_unknown_nodes(self, two_nodes_with_labels) -> None:
        rt_a, _ = two_nodes_with_labels
        for p in rt_a.peers():
            assert p["node_id"] != ""


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


class TestFaultInjection:
    """M2 故障注入钩子：close_peer / reopen_peer 改变本节点 peer 视图。"""

    def test_close_peer_removes_and_reopen_restores(self, two_nodes) -> None:
        rt_a, rt_b = two_nodes
        testkit.close_peer(rt_a, rt_b.node_id)
        assert all(p["node_id"] != rt_b.node_id for p in rt_a.peers())

        testkit.reopen_peer(rt_a, rt_b.node_id)
        deadline = time.time() + 10
        while time.time() < deadline:
            if any(p["node_id"] == rt_b.node_id for p in rt_a.peers()):
                return
            time.sleep(0.2)
        pytest.fail("peer did not reappear after reopen_peer")


class TestLabelPlacement:
    """M1 放置契约：任务 tags 只落到标签匹配的节点；无匹配则 Defer 后 Failed。"""

    @staticmethod
    def _wait_peer_labels(rt_a: Runtime, node_id: str, expected: dict[str, str]) -> None:
        deadline = time.time() + 15
        while time.time() < deadline:
            peer = next((p for p in rt_a.peers() if p["node_id"] == node_id), None)
            if peer is not None and peer["labels"] == expected:
                return
            time.sleep(0.2)
        pytest.skip("remote peer labels not visible within timeout (network env)")

    @pytest.fixture
    def placement_nodes(self):
        """提交节点（0 槽位）+ 执行节点（1 槽位、可配置标签）。"""
        import shutil
        import tempfile

        from actant.actant import _ActantConfig, _NetworkConfig

        dirs: list[str] = []
        runtimes: list[Runtime] = []

        def start(worker_labels: dict[str, str], starvation_ms: int):
            dir_a = tempfile.mkdtemp(prefix="actant-e2e-place-a-")
            dir_b = tempfile.mkdtemp(prefix="actant-e2e-place-b-")
            dirs.extend([dir_a, dir_b])
            cfg_a = _ActantConfig(
                payload_signing_key="",
                network=_NetworkConfig(preset="local"),
                max_concurrent_tasks=0,
                starvation_timeout_ms=starvation_ms,
            )
            cfg_b = _ActantConfig(
                payload_signing_key="",
                network=_NetworkConfig(preset="local"),
                max_concurrent_tasks=1,
                node_labels=worker_labels,
            )
            rt_a = Runtime(name="place-submit", data_dir=dir_a, config=cfg_a)
            rt_b = Runtime(name="place-worker", data_dir=dir_b, config=cfg_b)
            rt_a.start()
            rt_b.start()
            runtimes.extend([rt_a, rt_b])
            connected = connect_peers(rt_a, rt_b, timeout_s=15.0)
            if not connected:
                pytest.skip("P2P connection not established within timeout (network env)")
            return rt_a, rt_b

        try:
            yield start
        finally:
            for rt in reversed(runtimes):
                rt.stop()
            for d in dirs:
                shutil.rmtree(d, ignore_errors=True)

    def test_tagged_task_runs_on_matching_node(self, placement_nodes) -> None:
        rt_a, rt_b = placement_nodes({"engine": "a"}, starvation_ms=30_000)
        self._wait_peer_labels(rt_a, rt_b.node_id, {"engine": "a"})

        @task(tags=["engine=a"], timeout_ms=30_000)
        def tagged() -> str:
            return "ran-on-engine-a"

        with actant.use_runtime(rt_a):
            assert tagged.submit().result(timeout=30) == "ran-on-engine-a"

    def test_tagged_task_defers_then_fails_when_no_node_matches(self, placement_nodes) -> None:
        rt_a, rt_b = placement_nodes({"engine": "b"}, starvation_ms=2_000)
        self._wait_peer_labels(rt_a, rt_b.node_id, {"engine": "b"})

        @task(tags=["engine=a"], timeout_ms=30_000)
        def tagged() -> str:
            return "should-not-run"

        with actant.use_runtime(rt_a):
            handle = tagged.submit()
            with pytest.raises(Exception, match="no node satisfies required labels"):
                handle.result(timeout=30)


def _peer_visible(rt: Runtime, node_id: str) -> bool:
    """对端节点是否已出现在本节点的心跳视图（``peers()``）中。"""
    return any(p["node_id"] == node_id for p in rt.peers())


@pytest.fixture
def quota_placement_nodes():
    """双节点配额拓扑工厂：可配置两侧槽位/配额与 B 侧的延后参数。

    A 为提交侧（标签 ``node=a``），B 为配额执行侧（标签 ``node=b``）。
    测试可调用 ``start(...)`` 后通过 ``testkit.delay_heartbeat(rt_b, ...)`` 冻结 B 的对外
    心跳，让 A 侧看到的 B 配额/槽位停留在空闲快照——这正是分布式配额视图
    最终一致的真实窗口：路由据此把任务发往 B，而 B 本地扣减时可能已耗尽。
    """
    import shutil
    import tempfile

    from actant.actant import _ActantConfig, _FailoverConfig, _NetworkConfig

    dirs: list[str] = []
    runtimes: list[Runtime] = []

    def start(
        *,
        a_slots: int = 1,
        a_quotas: dict[str, int] | None = None,
        b_slots: int = 2,
        b_quotas: dict[str, int] | None = None,
        b_starvation_ms: int = 12_000,
        b_fallback_ms: int = 500,
    ) -> tuple[Runtime, Runtime]:
        dir_a = tempfile.mkdtemp(prefix="actant-e2e-quota-a-")
        dir_b = tempfile.mkdtemp(prefix="actant-e2e-quota-b-")
        dirs.extend([dir_a, dir_b])
        cfg_a = _ActantConfig(
            payload_signing_key="",
            network=_NetworkConfig(preset="local"),
            max_concurrent_tasks=a_slots,
            num_worker_processes=a_slots,
            # B 心跳被冻结后其条目仍需在 A 的路由视图内保持心跳新鲜。
            failover=_FailoverConfig(
                failure_timeout_ms=30_000,
                lease_duration_ms=60_000,
            ),
            node_labels={"node": "a"},
            node_quotas=a_quotas or {},
            starvation_timeout_ms=30_000,
        )
        cfg_b = _ActantConfig(
            payload_signing_key="",
            network=_NetworkConfig(preset="local"),
            max_concurrent_tasks=b_slots,
            num_worker_processes=b_slots,
            node_labels={"node": "b"},
            node_quotas=b_quotas or {},
            starvation_timeout_ms=b_starvation_ms,
            remote_fallback_delay_ms=b_fallback_ms,
        )
        rt_a = Runtime(name="quota-a", data_dir=dir_a, config=cfg_a)
        rt_b = Runtime(name="quota-b", data_dir=dir_b, config=cfg_b)
        rt_a.start()
        rt_b.start()
        runtimes.extend([rt_a, rt_b])
        if not connect_peers(rt_a, rt_b, timeout_s=15.0):
            pytest.skip("P2P connection not established within timeout (network env)")
        # 双向心跳可见后才继续：B 需要 A 的心跳视图才能把任务重路由回来。
        if not wait_until(
            lambda: _peer_visible(rt_a, rt_b.node_id)
            and _peer_visible(rt_b, rt_a.node_id),
            timeout_s=15.0,
        ):
            pytest.skip("peer heartbeat view not converged within timeout (network env)")
        return rt_a, rt_b

    try:
        yield start
    finally:
        for rt in reversed(runtimes):
            rt.stop()
        for d in dirs:
            shutil.rmtree(d, ignore_errors=True)


class TestQuotaPlacement:
    """多节点配额互斥：路由目标配额耗尽后重路由 + 配额归还唤醒延后任务。"""

    def test_routed_target_quota_exhausted_reroutes_to_peer(
        self, quota_placement_nodes, tmp_path
    ) -> None:
        """任务被路由到 B 后 B 的配额已耗尽，必须清空路由目标并重路由到 A。

        构造（A 侧看到的是 B 冻结心跳前的快照）：
        1. A 用标签任务占满唯一槽位，使 A 暂时无法本地执行；
        2. B 上以显式 pin 的任务长时间占用 gpu 配额（A 的视图仍显示可用）；
        3. A 提交 ``quota="gpu"`` 的任务 → 路由到 B → B 本地扣减失败 → 清空目标；
        4. A 槽位释放后，B 的重路由把任务转回 A 并成功执行。

        若路由目标未被释放，任务会一直钉在 B 轮询，直到 starvation 失败。
        """
        rt_a, rt_b = quota_placement_nodes(
            a_slots=1,
            a_quotas={"gpu": 1},
            b_slots=2,
            b_quotas={"gpu": 1},
            b_starvation_ms=12_000,
        )
        occupy_started = tmp_path / "occupy_started"
        hold_started = tmp_path / "hold_started"
        hold_abort = tmp_path / "hold_abort"
        compute_started = tmp_path / "compute_started"

        @task(tags=["node=a"], timeout_ms=20_000)
        def occupy_a(path: str) -> None:
            import time as _time
            from pathlib import Path

            Path(path).write_text("started", encoding="utf-8")
            _time.sleep(6.5)

        @task(quota="gpu", timeout_ms=25_000)
        def hold_gpu(started: str, abort: str) -> str:
            import time as _time
            from pathlib import Path

            Path(started).write_text("held", encoding="utf-8")
            deadline = _time.monotonic() + 25.0
            while _time.monotonic() < deadline and not Path(abort).exists():
                _time.sleep(0.05)
            return "released"

        @task(quota="gpu", timeout_ms=25_000)
        def compute(path: str) -> str:
            from pathlib import Path

            Path(path).write_text("ran", encoding="utf-8")
            return "rerouted"

        with actant.use_runtime(rt_a):
            occupy_handle = occupy_a.submit(str(occupy_started))
            assert wait_until(lambda: occupy_started.exists(), timeout_s=10.0), (
                "occupy_a did not start on node-a"
            )

            # 冻结 B 的对外心跳：A 侧看到的 B 配额与槽位停在空闲快照。
            testkit.delay_heartbeat(rt_b, 60_000)

            hold_handle = hold_gpu.submit_to(
                rt_b.node_id,
                str(hold_started),
                str(hold_abort),
                endpoint_addr=rt_b.listen_addresses()["endpoint_addr"],
            )
            assert wait_until(lambda: hold_started.exists(), timeout_s=10.0), (
                "hold_gpu did not start on node-b"
            )

            # A 的唯一槽位仍被 occupy_a 占用 → 路由只能选择 B（视图显示 gpu 空闲）。
            compute_handle = compute.submit(str(compute_started))
            assert compute_handle.result(timeout=20.0) == "rerouted"
            assert compute_started.exists(), "compute ran without recording its start"
            assert occupy_handle.result(timeout=10.0) is None

            hold_abort.write_text("stop", encoding="utf-8")
            assert hold_handle.result(timeout=15.0) == "released"

    def test_quota_release_wakes_deferred_task(
        self, quota_placement_nodes, tmp_path
    ) -> None:
        """配额归还时延后的任务被立即唤醒，而非等固定的重入队间隔。

        B 的 ``remote_fallback_delay_ms=5000`` 放大固定轮询间隔：没有归还
        信号时，任务要等首个 5s 兜底计时到期才重新入队；有信号时应在配额
        释放后立刻重新入队执行。
        """
        rt_a, rt_b = quota_placement_nodes(
            a_slots=1,
            a_quotas={},
            b_slots=2,
            b_quotas={"gpu": 1},
            b_starvation_ms=30_000,
            b_fallback_ms=5_000,
        )
        hold_started = tmp_path / "hold_started"
        hold_end = tmp_path / "hold_end"
        compute_started = tmp_path / "compute_started"

        @task(quota="gpu", timeout_ms=20_000)
        def hold_gpu(started: str, ended: str) -> None:
            import time as _time
            from pathlib import Path

            Path(started).write_text("held", encoding="utf-8")
            _time.sleep(1.5)
            Path(ended).write_text(str(_time.monotonic()), encoding="utf-8")

        @task(quota="gpu", timeout_ms=20_000)
        def compute(started: str) -> str:
            import time as _time
            from pathlib import Path

            Path(started).write_text(str(_time.monotonic()), encoding="utf-8")
            return "woken"

        with actant.use_runtime(rt_a):
            # A 未声明 gpu：路由只能转发；冻结 B 心跳使转发目标稳定为 B。
            testkit.delay_heartbeat(rt_b, 60_000)
            hold_handle = hold_gpu.submit_to(
                rt_b.node_id,
                str(hold_started),
                str(hold_end),
                endpoint_addr=rt_b.listen_addresses()["endpoint_addr"],
            )
            assert wait_until(lambda: hold_started.exists(), timeout_s=10.0), (
                "hold_gpu did not start on node-b"
            )

            # hold 持有唯一配额期间提交：任务在 B 上扣减失败 → Defer。
            compute_handle = compute.submit(str(compute_started))
            assert hold_handle.result(timeout=10.0) is None
            assert compute_handle.result(timeout=20.0) == "woken"

        gap = float(compute_started.read_text(encoding="utf-8")) - float(
            hold_end.read_text(encoding="utf-8")
        )
        assert gap < 2.0, (
            f"quota release did not wake the deferred task promptly: {gap:.3f}s "
            f"(fixed fallback delay is 5s)"
        )
