"""确定性故障脚本单元测试（无 Runtime）。"""

from __future__ import annotations

from typing import Any

import pytest
from act_ext_testkit import (
    ChaosPlan,
    ClockDrift,
    FaultError,
    FaultScript,
    Heal,
    Partition,
    PauseHeartbeat,
)
from act_ext_testkit.report import ClusterReport


class RecordingCluster:
    """记录控制面调用的最小集群替身（仅测试用）。"""

    __test__ = False

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.report = ClusterReport(namespace="fake")

    def partition(self, source: str, target: str) -> None:
        self.calls.append(("partition", (source, target)))

    def heal(self, source: str, target: str) -> None:
        self.calls.append(("heal", (source, target)))

    def kill(self, node: str) -> None:
        self.calls.append(("kill", (node,)))

    def pause_heartbeat(self, node: str, ms: int) -> None:
        self.calls.append(("pause_heartbeat", (node, ms)))

    def drift_clock(self, node: str, ms: int) -> None:
        self.calls.append(("drift_clock", (node, ms)))

    def isolate(self, node: str) -> None:
        self.calls.append(("isolate", (node,)))


def test_plan_is_sorted_by_time_then_insertion() -> None:
    script = FaultScript(seed=1)
    script.add(PauseHeartbeat("a", 500, at_s=1.0))
    script.add(Partition("a", "b", at_s=0.5))
    script.add(Heal("a", "b", at_s=0.5))
    plan = script.plan()
    assert [item.at_s for item in plan] == [0.5, 0.5, 1.0]
    assert [item.order for item in plan] == [1, 2, 0]


def test_script_executes_in_order_and_records_report() -> None:
    cluster = RecordingCluster()
    script = FaultScript(seed=3)
    script.add(Partition("a", "b", at_s=0.05))
    script.add(PauseHeartbeat("a", 300, at_s=0.05))
    script.add(Heal("a", "b", at_s=0.1))
    run = script.run(cluster, speed=20.0)
    run.wait(timeout_s=5.0)
    assert run.errors == []
    assert cluster.calls == [
        ("partition", ("a", "b")),
        ("pause_heartbeat", ("a", 300)),
        ("heal", ("a", "b")),
    ]
    faults = cluster.report.snapshot()["faults"]
    assert len(faults) == 3
    assert all(fault["applied"] for fault in faults)


def test_flap_expands_to_partition_heal_pairs() -> None:
    script = FaultScript(seed=2).flap("a", "b", at_s=0.0, times=3, period_s=0.1, down_s=0.05)
    assert len(script) == 6
    plan = script.plan()
    assert [item.fault.to_dict()["kind"] for item in plan] == [
        "partition",
        "heal",
        "partition",
        "heal",
        "partition",
        "heal",
    ]


def test_strict_mode_surfaces_application_error() -> None:
    class BrokenCluster(RecordingCluster):
        def partition(self, source: str, target: str) -> None:
            raise RuntimeError("boom")

    script = FaultScript(seed=0).add(Partition("a", "b"))
    run = script.run(BrokenCluster(), speed=100.0, strict=True)
    with pytest.raises(FaultError, match="boom"):
        run.wait(timeout_s=5.0)


def test_chaos_plan_is_reproducible_for_same_seed() -> None:
    kwargs: dict[str, Any] = {
        "nodes": ["a", "b", "c"],
        "seed": 42,
        "duration_s": 10.0,
        "tick_s": 1.0,
        "partition_prob": 0.4,
        "pause_prob": 0.4,
        "drift_prob": 0.4,
        "kill_prob": 0.1,
    }
    first = ChaosPlan(**kwargs).build()
    second = ChaosPlan(**kwargs).build()
    assert first.fingerprint() == second.fingerprint()
    assert [f.to_dict() for f in first.plan()] == [f.to_dict() for f in second.plan()]


def test_chaos_plan_differs_across_seeds() -> None:
    base: dict[str, Any] = {"nodes": ["a", "b", "c"], "duration_s": 10.0, "partition_prob": 0.8}
    assert ChaosPlan(**base, seed=1).fingerprint() != ChaosPlan(**base, seed=2).fingerprint()


def test_chaos_plan_keeps_min_alive() -> None:
    script = ChaosPlan(
        nodes=["a", "b"],
        seed=5,
        duration_s=20.0,
        tick_s=1.0,
        partition_prob=0.0,
        pause_prob=0.0,
        drift_prob=0.0,
        kill_prob=1.0,
        min_alive=1,
    ).build()
    kills = [item for item in script.plan() if item.fault.to_dict()["kind"] == "kill"]
    assert len(kills) == 1


def test_fault_validation_rejects_negative_time() -> None:
    with pytest.raises(ValueError):
        FaultScript().add(Partition("a", "b"), at_s=-1.0)
    with pytest.raises(ValueError):
        ChaosPlan(nodes=["a"], partition_prob=2.0).build()


def test_custom_fault_actions_can_be_scheduled() -> None:
    cluster = RecordingCluster()
    script = FaultScript().add(ClockDrift("a", 250))
    run = script.run(cluster, speed=100.0)
    run.wait(timeout_s=5.0)
    assert cluster.calls == [("drift_clock", ("a", 250))]
