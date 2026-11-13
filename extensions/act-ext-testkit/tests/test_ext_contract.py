"""``actant.ext`` 契约在真实多节点集群上的接线测试。"""

from __future__ import annotations

import time

from act_ext_testkit import TestCluster

import actant
from actant import task
from actant.ext import ClusterView, EventAdapter, EventEnvelope


@task(tags=["pool=ext"], timeout_ms=20_000)
def echo(value: int) -> int:
    return value


def test_event_adapter_and_cluster_view_on_cluster() -> None:
    received: list[EventEnvelope] = []
    with TestCluster("ext-contract", seed=7, discovery="none") as cluster:
        cluster.add_node(
            "n0",
            labels={"pool": "ext"},
            max_concurrent_tasks=2,
            mode="in-process",
        )
        cluster.add_node(
            "n1",
            labels={"pool": "ext"},
            max_concurrent_tasks=2,
            mode="in-process",
        )
        cluster.start(topology="mesh")
        runtime = cluster.node("n0")
        EventAdapter(source="n0").subscribe(runtime, received.append)

        with actant.use_runtime(runtime):
            assert echo.submit(42).result(timeout=20) == 42

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not any(
            envelope.kind == "task.completed" for envelope in received
        ):
            time.sleep(0.05)

        assert any(envelope.kind == "task.completed" for envelope in received)
        assert all(envelope.schema_version == 1 for envelope in received)

        nodes = ClusterView(runtime).nodes()
        assert nodes
        assert all("pool" in node.labels for node in nodes)
