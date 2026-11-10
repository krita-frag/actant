"""Dogfooding：用 act-ext-testkit 测试 actant 自身的 P2P 行为。

覆盖 M1 放置契约 + M2 故障注入，证明 testkit 能真实驱动核心（而非 mock）。
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from act_ext_testkit import TestCluster

import actant
from actant import task


@pytest.fixture
def three_nodes() -> Iterator[TestCluster]:
    c = TestCluster(
        "dogfood",
        heartbeat_interval_ms=300,
        failure_timeout_ms=1_500,
    )
    c.add_node("a", labels={"role": "submit"}, max_concurrent_tasks=0)
    c.add_node("b", labels={"engine": "a"})
    c.add_node("c", labels={"engine": "b"})
    c.start(connect_timeout_s=20)
    try:
        yield c
    finally:
        c.stop()


def test_three_nodes_converge_with_labels(three_nodes: TestCluster) -> None:
    three_nodes.wait_converged(
        timeout_s=20,
        expected_labels={
            "a": {"role": "submit"},
            "b": {"engine": "a"},
            "c": {"engine": "b"},
        },
    )


def test_partition_is_detected_and_heals(three_nodes: TestCluster) -> None:
    three_nodes.partition("a", "c")
    three_nodes.wait_peer_lost("a", "c", timeout_s=2)

    three_nodes.heal("a", "c")
    three_nodes.wait_peer_visible("a", "c", timeout_s=10)


def test_killed_node_disappears_from_peer_view(three_nodes: TestCluster) -> None:
    three_nodes.kill("c")
    three_nodes.wait_peer_lost("a", "c", timeout_s=10)
    three_nodes.wait_peer_lost("b", "c", timeout_s=10)


def test_tagged_task_runs_only_on_matching_node(three_nodes: TestCluster) -> None:
    three_nodes.wait_converged(
        timeout_s=20,
        expected_labels={
            "a": {"role": "submit"},
            "b": {"engine": "a"},
            "c": {"engine": "b"},
        },
    )

    @task(tags=["engine=a"], timeout_ms=30_000)
    def render() -> str:
        return "engine-a-result"

    with actant.use_runtime(three_nodes.node("a")):
        assert render.submit().result(timeout=30) == "engine-a-result"


def test_no_matching_node_fails_after_starvation_timeout() -> None:
    c = TestCluster(
        "dogfood-no-match",
        heartbeat_interval_ms=300,
        failure_timeout_ms=1_500,
        starvation_timeout_ms=1_500,
    )
    c.add_node("submit", labels={"role": "submit"}, max_concurrent_tasks=0)
    c.add_node("worker", labels={"engine": "b"})
    c.start(connect_timeout_s=20)
    try:
        @task(tags=["engine=a"], timeout_ms=30_000)
        def render() -> str:
            return "should-not-run"

        with actant.use_runtime(c.node("submit")):
            handle = render.submit()
            with pytest.raises(Exception, match="no node satisfies required labels"):
                handle.result(timeout=30)
    finally:
        c.stop()
