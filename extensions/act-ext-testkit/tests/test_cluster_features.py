"""集群级集成测试：拓扑变更、动态成员、混沌脚本、追踪断言、进程隔离。

这些测试会真实拉起 Runtime（可能还有 worker 子进程），标记为 ``slow``。
"""

from __future__ import annotations

import time
from typing import Any, cast

import pytest
from act_ext_testkit import (
    ClockDrift,
    FaultScript,
    PauseHeartbeat,
    TestCluster,
    Topology,
    TraceRecorder,
    assert_all_terminal,
    assert_lifecycle_valid,
    assert_no_duplicate_execution,
)

import actant
from actant import task

pytestmark = pytest.mark.slow


def _sees(cluster: TestCluster, observer: str, peer: str) -> bool:
    peer_id = cluster.node_ids()[peer]
    return any(item["node_id"] == peer_id for item in cluster.handle(observer).peers())


def _make(
    factory: Any,
    namespace: str,
    names: list[str],
    *,
    topology: Any = "mesh",
    submitter: str | None = None,
    process: str | None = None,
    **kwargs: Any,
) -> TestCluster:
    nodes: list[dict[str, Any]] = []
    for name in names:
        # 每个节点声明 engines=<name> 标签，供标签选择器定向路由。
        node: dict[str, Any] = {"name": name, "labels": {"engine": name}}
        if name == submitter:
            node["max_concurrent_tasks"] = 0
        if name == process:
            node["mode"] = "process"
        nodes.append(node)
    return cast(
        TestCluster,
        factory(
            namespace,
            nodes,
            topology=topology,
            heartbeat_interval_ms=300,
            failure_timeout_ms=1_500,
            # 进程外节点冷启动较慢；全量套件并发运行时放宽组网上限。
            connect_timeout_s=45.0 if process else 20.0,
            **kwargs,
        ),
    )


def test_topology_change_split_and_heal(cluster_factory: Any) -> None:
    cluster = _make(cluster_factory, "topo", ["a", "b", "c"], topology="line")

    assert _sees(cluster, "a", "b")
    assert _sees(cluster, "b", "a") and _sees(cluster, "b", "c")

    # 拓扑描述的是连接意图；gossip 仍可能传播非直连节点，因此这里只验证
    # 新边被建立（差分），不假设"非邻居不可见"。
    change = cluster.apply_topology(Topology.ring(["a", "b", "c"]))
    assert ("a", "c") in change.added_edges
    assert _sees(cluster, "a", "c") and _sees(cluster, "c", "a")

    cluster.split([["a"], ["b", "c"]])
    cluster.wait_peer_lost("a", "b", timeout_s=10)
    cluster.wait_peer_lost("a", "c", timeout_s=10)

    cluster.heal_all(timeout_s=20)
    assert _sees(cluster, "a", "b") and _sees(cluster, "a", "c")


def test_dynamic_join_and_leave(cluster_factory: Any) -> None:
    cluster = _make(cluster_factory, "join", ["a", "b"])
    handle = cluster.join("c", labels={"role": "late"}, connect_timeout_s=20)
    assert handle.name == "c"
    assert _sees(cluster, "a", "c") and _sees(cluster, "c", "a")
    assert cluster.topology is not None
    assert "c" in cluster.topology.nodes

    cluster.leave("c")
    cluster.wait_peer_lost("a", "c", timeout_s=10)
    assert "c" not in (cluster.topology.nodes if cluster.topology else ())


def test_fault_script_applies_and_is_reported(cluster_factory: Any) -> None:
    cluster = _make(cluster_factory, "chaos", ["a", "b", "c"])
    script = FaultScript(seed=11)
    script.flap("a", "c", at_s=0.1, times=1, period_s=0.1, down_s=0.05)
    script.add(PauseHeartbeat("b", 200, at_s=0.1))
    script.add(ClockDrift("a", 100, at_s=0.15))
    run = script.run(cluster, speed=10.0)
    applied = run.wait(timeout_s=10.0)
    assert run.errors == []
    assert len(applied) == 4
    assert len(cluster.report.snapshot()["faults"]) == 4
    cluster.wait_peer_visible("a", "c", timeout_s=20)


def test_trace_driven_assertions_for_remote_task(cluster_factory: Any) -> None:
    cluster = _make(
        cluster_factory,
        "trace",
        ["a", "b"],
        topology="mesh",
        submitter="a",
    )
    # 观察者视角：远端任务只回流终态，started 在执行节点内部。
    trace = TraceRecorder(partial=True)
    trace.attach_cluster(cluster)

    @task(tags=["engine=b"], timeout_ms=30_000)
    def compute() -> int:
        return 42

    with actant.use_runtime(cluster.node("a")):
        handle = compute.submit()
        assert handle.result(timeout=30) == 42

    completed = trace.wait_for(
        lambda event: event.kind == "completed" and bool(event.task_id),
        timeout_s=20,
        description="task completed event",
    )
    # 任务结果回到提交方，TaskLifecycle 由持有 AsyncResult 的 Runtime（a）发布；
    # 远端执行位置由 a 的 max_concurrent_tasks=0 间接证明（a 无法本地执行）。
    assert completed.node == "a"
    task_ids = trace.tasks()
    assert task_ids
    assert_all_terminal(trace, task_ids)
    assert_lifecycle_valid(trace)
    assert_no_duplicate_execution(trace)


def test_process_node_executes_freezes_and_crashes(cluster_factory: Any) -> None:
    cluster = _make(
        cluster_factory,
        "process",
        ["a", "b"],
        topology="mesh",
        submitter="a",
        process="b",
    )
    trace = TraceRecorder(partial=True)
    trace.attach_cluster(cluster)

    @task(tags=["engine=b"], timeout_ms=30_000)
    def compute() -> str:
        return "from-process"

    with actant.use_runtime(cluster.node("a")):
        handle = compute.submit()
        assert handle.result(timeout=30) == "from-process"

    completed = trace.wait_for(
        lambda event: event.kind == "completed" and bool(event.task_id),
        timeout_s=20,
        description="process node completed event",
    )
    assert completed.node == "a"

    cluster.freeze("b")
    cluster.wait_peer_lost("a", "b", timeout_s=20)
    cluster.thaw("b")
    cluster.heal_all(timeout_s=25)
    cluster.wait_peer_visible("a", "b", timeout_s=25)

    cluster.kill("b")
    cluster.wait_peer_lost("a", "b", timeout_s=15)
    assert not cluster.handle("b").alive


def test_eventually_and_metrics_helpers(cluster_factory: Any) -> None:
    from act_ext_testkit import PerformanceSampler, assert_eventually

    cluster = _make(cluster_factory, "metrics", ["a", "b"])
    sampler = PerformanceSampler(interval_s=0.05).start()
    try:
        with sampler.measure():
            time.sleep(0.01)
        assert_eventually(
            lambda: len(cluster.handle("a").peers()) >= 1,
            timeout_s=5,
            description="a sees at least one peer",
        )
    finally:
        sampler.stop(metrics_text=cluster.metrics_text("a"))
    summary = sampler.summary()
    assert summary["latency_ms"]["count"] == 1
    assert summary["metrics_text_chars"] >= 0
