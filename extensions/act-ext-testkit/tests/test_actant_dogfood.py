"""用测试框架测试 Actant 自身（dogfood）。

覆盖框架主张的分层能力，每一层都驱动**真实 Actant P2P 运行时**：

1. 一致性：并发任务全部完成且生命周期无重复/无缺失终态
2. 容错恢复：Worker 崩溃后由存活 Worker 继续服务
3. 拓扑动态：运行中动态加入节点并按标签路由
4. 混沌 + 恢复：故障脚本执行后集群仍可执行任务，报告可追溯
5. 信任域：启用 payload 签名后，直接任务仍可跨节点执行
6. 资源隔离：命名配额任务只会落到声明了对应容量的节点
7. 性能：延迟/吞吐采样 + 报告落盘 round-trip

所有测试使用 ``TestCluster`` 的独立 ``data_dir`` / 沙箱命名空间，
不修改 Actant 核心语义。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from act_ext_testkit import (
    ClockDrift,
    FaultScript,
    Kill,
    NodeConfig,
    PauseHeartbeat,
    PerformanceSampler,
    TestCluster,
    TraceRecorder,
    assert_all_terminal,
    assert_eventually,
    assert_lifecycle_valid,
    assert_no_duplicate_execution,
    generate_signing_key,
)

import actant
from actant.tasks import gather, task

# 集群统一参数：心跳/失联检测调快，避免测试等太久。
_CLUSTER_KWARGS: dict[str, Any] = {
    "heartbeat_interval_ms": 300,
    "failure_timeout_ms": 1_500,
    "starvation_timeout_ms": 4_000,
}


def _start_cluster(namespace: str, *, workers: int = 2) -> TestCluster:
    """提交节点 + ``workers`` 个带 ``engine=worker`` 标签的 Worker 节点。"""
    cluster = TestCluster(namespace, seed=7, **_CLUSTER_KWARGS)
    cluster.add_node("submit", labels={"role": "submit"}, max_concurrent_tasks=0)
    for index in range(workers):
        cluster.add_node(
            f"w{index}",
            labels={"engine": "worker"},
            max_concurrent_tasks=1,
        )
    cluster.start(topology="mesh")
    return cluster


def test_signed_payloads_execute_across_nodes() -> None:
    """信任域层：启用 payload 签名后，直接 ``@task`` 提交仍可跨节点执行。"""
    key = generate_signing_key()

    with TestCluster("dogfood-signed", seed=7, **_CLUSTER_KWARGS) as cluster:
        cluster.add_node(
            "submit",
            labels={"role": "submit"},
            max_concurrent_tasks=0,
            config=NodeConfig(
                labels={"role": "submit"},
                max_concurrent_tasks=0,
                signing_key=key,
                require_payload_signing=True,
            ),
        )
        cluster.add_node(
            "worker",
            labels={"engine": "worker"},
            config=NodeConfig(
                labels={"engine": "worker"},
                signing_key=key,
                require_payload_signing=True,
            ),
        )
        cluster.start(topology="mesh")

        @task(tags=["engine=worker"], timeout_ms=20_000)
        def add(x: int, y: int) -> int:
            return x + y

        with actant.use_runtime(cluster.node("submit")):
            handle = add.submit(20, 22)
            assert handle.result(timeout=30) == 42

        # 提交节点无本地执行能力，成功即证明签名载荷通过了 Worker 校验。
        assert cluster.report.manifest["namespace"] == "dogfood-signed"


def test_quota_placement_routes_to_capable_node() -> None:
    """资源隔离层：带命名配额的任务只会落到声明了该容量的节点。"""
    with TestCluster("dogfood-quota", seed=7, **_CLUSTER_KWARGS) as cluster:
        cluster.add_node("submit", labels={"role": "submit"}, max_concurrent_tasks=0)
        cluster.add_node(
            "gpu",
            labels={"engine": "worker"},
            config=NodeConfig(
                labels={"engine": "worker"},
                max_concurrent_tasks=2,
                node_quotas={"gpu": 1},
            ),
        )
        cluster.add_node(
            "cpu",
            labels={"engine": "worker"},
            config=NodeConfig(labels={"engine": "worker"}, max_concurrent_tasks=2),
        )
        cluster.start(topology="mesh")

        @task(tags=["engine=worker"], quota="gpu", timeout_ms=20_000)
        def use_gpu(value: int) -> int:
            return value

        with actant.use_runtime(cluster.node("submit")):
            first = use_gpu.submit(1)
            second = use_gpu.submit(2)
            assert first.result(timeout=30) == 1
            assert second.result(timeout=30) == 2


def test_concurrent_tasks_keep_lifecycle_invariants() -> None:
    """一致性层：并发任务全部完成，trace 无重复执行、无缺失终态。"""
    with _start_cluster("dogfood-consistency") as cluster:
        trace = TraceRecorder(partial=True)
        trace.attach_cluster(cluster)

        @task(tags=["engine=worker"], timeout_ms=20_000)
        def square(value: int) -> int:
            return value * value

        with actant.use_runtime(cluster.node("submit")):
            handles = [square.submit(value) for value in range(8)]
            results = gather(*handles, timeout=60)
            assert results == [value * value for value in range(8)]

        task_ids = [handle.task_id for handle in handles]
        assert_eventually(
            lambda: all(trace.terminal_state(task_id) is not None for task_id in task_ids),
            timeout_s=10,
            description="all task terminal events observed",
        )
        assert_all_terminal(trace, task_ids)
        assert_no_duplicate_execution(trace)
        assert_lifecycle_valid(trace)


def test_worker_crash_survivor_keeps_serving() -> None:
    """容错恢复层：一个 Worker 崩溃后，存活 Worker 继续执行同标签任务。"""
    with _start_cluster("dogfood-crash") as cluster:
        cluster.kill("w1")
        cluster.wait_peer_lost("submit", "w1", timeout_s=15)

        @task(tags=["engine=worker"], timeout_ms=20_000)
        def identity(value: str) -> str:
            return value

        with actant.use_runtime(cluster.node("submit")):
            handle = identity.submit("survivor")
            assert handle.result(timeout=30) == "survivor"

        snapshot = cluster.report.snapshot()
        assert any(event["kind"] == "node_killed" and event["node"] == "w1" for event in snapshot["events"])


def test_dynamic_worker_join_is_routable() -> None:
    """拓扑动态层：运行中 join 的新节点立即可被标签选择器路由。"""
    with _start_cluster("dogfood-join", workers=1) as cluster:
        cluster.join("wc", labels={"engine": "late"}, max_concurrent_tasks=1)
        cluster.wait_peer_visible("submit", "wc", timeout_s=15)

        @task(tags=["engine=late"], timeout_ms=20_000)
        def whoami() -> str:
            return "wc"

        with actant.use_runtime(cluster.node("submit")):
            handle = whoami.submit()
            assert handle.result(timeout=30) == "wc"

        assert "wc" in cluster.node_ids()


def test_chaos_script_then_cluster_recovers() -> None:
    """混沌层：崩溃 + 心跳静默 + 时钟漂移后，集群仍能恢复执行任务。"""
    with _start_cluster("dogfood-chaos") as cluster:
        script = FaultScript(seed=7)
        script.add(Kill("w1", at_s=0.0))
        script.add(PauseHeartbeat("w0", 300, at_s=0.1))
        script.add(ClockDrift("submit", 120, at_s=0.2))
        run = script.run(cluster, speed=1.0)
        applied = run.wait(timeout_s=10)
        assert len(applied) == 3

        cluster.wait_peer_lost("submit", "w1", timeout_s=15)

        @task(tags=["engine=worker"], timeout_ms=20_000)
        def add(x: int, y: int) -> int:
            return x + y

        with actant.use_runtime(cluster.node("submit")):
            handle = add.submit(1, 2)
            assert handle.result(timeout=30) == 3

        snapshot = cluster.report.snapshot()
        assert len(snapshot["faults"]) == 3
        assert all(fault["applied"] for fault in snapshot["faults"])


def test_performance_sampling_and_report_roundtrip(tmp_path: Path) -> None:
    """性能层：延迟/吞吐采样可读，报告可落盘并原样读回。"""
    with _start_cluster("dogfood-perf", workers=2) as cluster:
        trace = TraceRecorder(partial=True)
        trace.attach_cluster(cluster)
        sampler = PerformanceSampler(interval_s=0.05).start()

        @task(tags=["engine=worker"], timeout_ms=20_000)
        def sleep_and_return(ms: int) -> int:
            import time

            time.sleep(ms / 1000.0)
            return ms

        with actant.use_runtime(cluster.node("submit")):
            handles = []
            for _ in range(6):
                with sampler.measure():
                    handle = sleep_and_return.submit(10)
                    handles.append(handle)
                    assert handle.result(timeout=30) == 10

        task_ids = [handle.task_id for handle in handles]
        assert_eventually(
            lambda: all(trace.terminal_state(task_id) is not None for task_id in task_ids),
            timeout_s=10,
            description="all performance task terminal events observed",
        )

        summary = sampler.stop().summary()
        assert summary["latency_ms"]["count"] == 6
        assert summary["latency_ms"]["p95"] >= summary["latency_ms"]["p50"]
        assert summary["throughput_per_s"] >= 0
        assert summary["rss_bytes"]["peak"] > 0

        cluster.report.attach_trace(trace)
        cluster.report.attach_metrics(summary)
        path = cluster.report.dump(tmp_path / "dogfood-report.json")
        loaded = type(cluster.report).load(path)
        assert loaded["namespace"] == "dogfood-perf"
        assert loaded["trace"]["event_count"] >= 6
        assert loaded["metrics"]["latency_ms"]["count"] == 6


@pytest.mark.slow
def test_process_node_crash_is_observed_by_cluster() -> None:
    """进程隔离层：进程外节点被 SIGKILL 后，其余节点观测到失联。"""
    with TestCluster("dogfood-process", seed=7, **_CLUSTER_KWARGS) as cluster:
        cluster.add_node("submit", labels={"role": "submit"}, max_concurrent_tasks=0)
        cluster.add_node("wp", labels={"engine": "process"}, mode="process")
        # 进程外节点冷启动较慢；全量套件并发运行时放宽组网上限。
        cluster.start(topology="mesh", connect_timeout_s=45)

        cluster.kill("wp")
        cluster.wait_peer_lost("submit", "wp", timeout_s=15)

        snapshot = cluster.report.snapshot()
        killed = [event for event in snapshot["events"] if event["kind"] == "node_killed"]
        assert killed and killed[0]["node"] == "wp"
