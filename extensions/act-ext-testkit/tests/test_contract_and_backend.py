"""契约层与后端抽象的通用性测试（不依赖 Actant 运行时）。"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
from act_ext_testkit import (
    BackendCapabilities,
    CapabilityError,
    ClockMode,
    CommandFabric,
    Crash,
    DnctlFabric,
    FabricError,
    FaultKind,
    FaultScript,
    InvariantViolation,
    IsolationLevel,
    NetworkFidelity,
    NetworkProfile,
    PerformanceSampler,
    ResourceLimits,
    Sandbox,
    ScenarioRequirements,
    SemanticNetem,
    TestCluster,
    TracePolicy,
    TraceRecorder,
    assert_single_execution,
    check_cross_node_duplicates,
    check_trace,
    get_backend,
    list_backends,
    register_backend,
)
from act_ext_testkit.contract import LaunchContext, NodeSpec
from act_ext_testkit.node import NodeError, _validate_limits


class FakeCodec:
    def lifecycle(self, raw: Any) -> dict[str, Any] | None:
        return dict(raw) if isinstance(raw, dict) else None

    def log(self, raw: Any) -> dict[str, Any] | None:
        return {"kind": "log", "task_id": str(raw.get("task_id", ""))}

    def worker(self, raw: Any) -> dict[str, Any] | None:
        return {"kind": "worker", "data": dict(raw)}


class FakeNode:
    mode = "process"

    def __init__(self, name: str) -> None:
        self.name = name
        self._alive = True
        self._peers: dict[str, dict[str, str]] = {}
        self._sinks: list[Any] = []

    @property
    def node_id(self) -> str:
        return self.name

    @property
    def peer_id(self) -> str:
        return self.name

    @property
    def alive(self) -> bool:
        return self._alive

    @property
    def runtime(self) -> Any | None:
        return None

    def listen_addresses(self) -> dict[str, Any]:
        return {"endpoint_addr": f"fake://{self.name}"}

    def peers(self) -> list[dict[str, Any]]:
        return [{"node_id": node, "labels": labels} for node, labels in self._peers.items()]

    def dial(self, addr: str) -> None:
        if addr.startswith("fake://"):
            self._peers[addr.removeprefix("fake://")] = {}

    def add_gossip_peer(self, peer_id: str) -> None:
        self._peers.setdefault(peer_id, {})

    def close_peer(self, node_id: str) -> None:
        self._peers.pop(node_id, None)

    def reopen_peer(self, node_id: str) -> None:
        self._peers.setdefault(node_id, {})

    def delay_heartbeat(self, ms: int) -> None:
        self.heartbeat_delay = ms

    def inject_hlc_drift(self, ms: int) -> None:
        self.clock_drift = ms

    def set_max_concurrent_tasks(self, new_max: int) -> None:
        self.max_concurrent = new_max

    def metrics_text(self) -> str:
        return ""

    def stop(self, *, timeout_s: float = 30.0) -> None:
        self._alive = False

    def kill(self) -> None:
        self._alive = False

    def crash(self) -> None:
        self._alive = False

    def freeze(self) -> None:
        self.frozen = True

    def thaw(self) -> None:
        self.frozen = False

    def subscribe(self, sink: Any) -> None:
        self._sinks.append(sink)

    def describe(self) -> dict[str, Any]:
        return {"name": self.name, "mode": self.mode, "node_id": self.node_id, "alive": self.alive}


class FakeBackend:
    name = "fake"
    capabilities = BackendCapabilities(
        isolation=IsolationLevel.PROCESS,
        network=NetworkFidelity.SEMANTIC,
        clock=ClockMode.HLC,
        faults=frozenset(
            {
                FaultKind.KILL,
                FaultKind.CRASH,
                FaultKind.FREEZE,
                FaultKind.THAW,
                FaultKind.PARTITION,
                FaultKind.HEAL,
                FaultKind.SPLIT,
                FaultKind.ISOLATE,
                FaultKind.PAUSE_HEARTBEAT,
                FaultKind.CLOCK_DRIFT,
            }
        ),
        resources=True,
        black_box=True,
    )

    def __init__(self) -> None:
        self.launched: list[tuple[NodeSpec, LaunchContext]] = []

    def launch(self, spec: NodeSpec, context: LaunchContext) -> FakeNode:
        self.launched.append((spec, context))
        return FakeNode(spec.name)

    def codec(self) -> FakeCodec:
        return FakeCodec()


class WeakBackend(FakeBackend):
    name = "weak"
    capabilities = BackendCapabilities(
        isolation=IsolationLevel.NONE,
        network=NetworkFidelity.SEMANTIC,
        clock=ClockMode.SHARED,
        faults=frozenset({FaultKind.KILL}),
    )


def test_cluster_runs_against_non_actant_backend(tmp_path: Path) -> None:
    backend = FakeBackend()
    cluster = TestCluster(
        "fake-cluster",
        backend=backend,
        profile="ci",
        data_root=tmp_path / "cluster",
    )
    try:
        for name in ("a", "b", "c"):
            cluster.add_node(name, mode="process", labels={"role": name})
        cluster.start(topology="line")
        assert [spec.name for spec, _ in backend.launched] == ["a", "b", "c"]
        assert cluster.capabilities.isolation is IsolationLevel.PROCESS

        cluster.partition("a", "b")
        assert all(p["node_id"] != "b" for p in cluster.handle("a").peers())
        cluster.heal("a", "b")
        assert any(p["node_id"] == "b" for p in cluster.handle("a").peers())

        cluster.crash("c")
        assert not cluster.handle("c").alive

        manifest = cluster.manifest()
        assert manifest["backend"] == "fake"
        assert manifest["fidelity"]["isolation"] == "process"
        assert manifest["fidelity"]["node_modes"] == ["process"]
    finally:
        cluster.stop()


def test_profile_and_requirements_fail_explicitly(tmp_path: Path) -> None:
    profiled = TestCluster(
        "weak-profile",
        backend=WeakBackend(),
        profile="ci",
        data_root=tmp_path / "p",
    )
    try:
        profiled.add_node("a", mode="process")
        with pytest.raises(CapabilityError):
            profiled.start()
    finally:
        profiled.stop()

    cluster = TestCluster("weak-req", backend=WeakBackend(), data_root=tmp_path / "r")
    try:
        cluster.add_node("a")
        with pytest.raises(CapabilityError):
            cluster.require(ScenarioRequirements(faults=frozenset({FaultKind.CRASH})))
        cluster.require(ScenarioRequirements(isolation=IsolationLevel.NONE))
    finally:
        cluster.stop()


def test_fault_script_rejects_unsupported_capability(tmp_path: Path) -> None:
    cluster = TestCluster("fault-cap", backend=WeakBackend(), data_root=tmp_path / "f")
    try:
        cluster.add_node("a")
        script = FaultScript(seed=1).add(Crash("a"))
        with pytest.raises(CapabilityError):
            script.run(cluster, background=False, strict_capabilities=True)
    finally:
        cluster.stop()


def test_strict_env_does_not_leak_host_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TESTKIT_SECRET_TOKEN", "leak-me")
    box = Sandbox("strict-env", strict_env=True, env_passthrough=("PATH",))
    try:
        env = box.child_env()
        assert "TESTKIT_SECRET_TOKEN" not in env
        assert env["HOME"] == str(box.home_dir)
        assert env["XDG_CACHE_HOME"] == str(box.cache_dir)
        assert env["PYTHONPATH"]
        manifest = box.manifest()
        assert manifest["strict_env"] is True
        assert "TESTKIT_SECRET_TOKEN" not in manifest["env_passthrough"]
    finally:
        box.cleanup()


def test_resource_limit_validation_rejects_unsupported_platform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _validate_limits(ResourceLimits())
    monkeypatch.setattr(os, "name", "nt")
    with pytest.raises(NodeError):
        _validate_limits(ResourceLimits(memory_mb=64))


def test_command_fabric_dry_run_records_commands() -> None:
    fabric = CommandFabric(
        apply_commands=lambda profile: [
            ["tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", f"{profile.latency_ms}ms"]
        ],
        dry_run=True,
        unsupported={"bandwidth_kbps"},
    )
    profile = NetworkProfile(latency_ms=120, jitter_ms=10, loss=0.05)
    assert fabric.supports(profile) == set()
    fabric.apply(profile)
    assert fabric.commands == [
        ["tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", "120ms"]
    ]
    assert fabric.supports(NetworkProfile(bandwidth_kbps=512)) == {"bandwidth_kbps"}
    with pytest.raises(FabricError):
        fabric.partition("a", "b")


def test_dnctl_fabric_dry_run_shapes_and_partitions() -> None:
    fabric = DnctlFabric({"a": 40001, "b": 40002}, dry_run=True)
    profile = NetworkProfile(latency_ms=120, loss=0.05, bandwidth_kbps=512)
    assert fabric.supports(profile) == set()

    fabric.apply(profile)
    assert fabric.commands[0] == [
        "dnctl",
        "pipe",
        "39000",
        "config",
        "bw",
        "512Kbit/s",
        "delay",
        "120",
        "plr",
        "0.0500",
    ]
    assert fabric.commands[1] == ["pfctl", "-E"]
    assert fabric.commands[2] == ["pfctl", "-a", "com.apple/actant-testkit", "-f", "-"]
    assert fabric.rules == [
        "dummynet out quick proto udp from any port 40001 to any pipe 39000",
        "dummynet out quick proto udp from any port 40002 to any pipe 39000",
    ]

    fabric.partition("b", "a")
    assert fabric.rules[:2] == [
        "block drop out quick proto udp from any port 40001 to any port 40002",
        "block drop out quick proto udp from any port 40002 to any port 40001",
    ]

    fabric.heal("a", "b")
    assert all(not rule.startswith("block") for rule in fabric.rules)

    fabric.clear()
    assert fabric.rules == []
    assert fabric.commands[-1] == ["dnctl", "pipe", "delete", "39000"]


def test_dnctl_fabric_rejects_invalid_topology() -> None:
    with pytest.raises(FabricError):
        DnctlFabric({})
    with pytest.raises(FabricError):
        DnctlFabric({"a": 40001, "b": 40001})
    with pytest.raises(FabricError):
        DnctlFabric({"a": 0})

    fabric = DnctlFabric({"a": 40001}, dry_run=True)
    with pytest.raises(FabricError):
        fabric.partition("a", "a")
    with pytest.raises(FabricError):
        fabric.partition("a", "missing")


def test_dnctl_fabric_reports_unsupported_profile_fields() -> None:
    fabric = DnctlFabric({"a": 40001}, dry_run=True)
    assert fabric.supports(NetworkProfile(jitter_ms=10)) == {"jitter_ms"}
    assert fabric.supports(NetworkProfile(duplicate=0.1)) == {"duplicate"}
    assert fabric.supports(NetworkProfile(reorder=0.1)) == {"reorder"}
    with pytest.raises(FabricError):
        fabric.apply(NetworkProfile(duplicate=0.1))


@pytest.mark.skipif(
    platform.system() != "Darwin" or shutil.which("pfctl") is None,
    reason="pfctl is only available on macOS",
)
def test_dnctl_fabric_rules_parse_with_pfctl() -> None:
    """用系统 pfctl 的 parse-only 模式验证真实规则语法（不需要 root）。"""
    fabric = DnctlFabric({"a": 40001, "b": 40002}, dry_run=True)
    fabric.apply(NetworkProfile(latency_ms=50, loss=0.01))
    fabric.partition("a", "b")

    result = subprocess.run(
        ["pfctl", "-n", "-f", "-"],
        input="\n".join(fabric.rules) + "\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_trace_policy_supports_custom_terminal_semantics() -> None:
    policy = TracePolicy(
        terminal_kinds=frozenset({"done"}),
        lifecycle_kinds=frozenset({"started", "done"}),
    )
    trace = TraceRecorder(policy=policy)
    assert check_trace(trace) == []
    trace.record("node-a", "started", task_id="t1")
    assert check_trace(trace) == []
    trace.record("node-a", "done", task_id="t1")
    assert check_trace(trace) == []
    trace.record("node-a", "done", task_id="t1")
    codes = {violation.code for violation in check_trace(trace)}
    assert "terminal_duplicate" in codes


def test_cross_node_duplicate_execution_is_detected() -> None:
    left = TraceRecorder()
    right = TraceRecorder()
    left.record("a", "started", task_id="t1")
    right.record("b", "started", task_id="t1")
    codes = {violation.code for violation in check_cross_node_duplicates([left, right])}
    assert "duplicate_execution" in codes
    with pytest.raises(InvariantViolation):
        assert_single_execution([left, right])

    retried = TraceRecorder()
    retried.record("a", "started", task_id="t2")
    retried.record("a", "retried", task_id="t2", attempt=1, next_attempt=2)
    retried.record("a", "started", task_id="t2", attempt=2)
    assert check_cross_node_duplicates([retried]) == []
    assert_single_execution([retried])


def test_semantic_fabric_partition_does_not_recurse(tmp_path: Path) -> None:
    cluster = TestCluster("semantic", backend=FakeBackend(), data_root=tmp_path / "s")
    try:
        cluster.add_node("a", mode="process")
        cluster.add_node("b", mode="process")
        cluster.start(topology="line")
        fabric = SemanticNetem(cluster, NetworkProfile(), nodes=["a", "b"], seed=1)
        cluster.use_fabric(fabric)
        cluster.partition("a", "b")
        assert all(p["node_id"] != "b" for p in cluster.handle("a").peers())
        cluster.heal("a", "b")
        assert any(p["node_id"] == "b" for p in cluster.handle("a").peers())
        assert cluster.fabric is fabric
    finally:
        cluster.stop()


def test_backend_registry_accepts_instances() -> None:
    backend = FakeBackend()
    register_backend("fake-registry", backend)
    assert "fake-registry" in list_backends()
    assert get_backend("fake-registry") is backend


def test_performance_sampler_tracks_process_rss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "act_ext_testkit.metrics._process_rss_bytes",
        lambda pid: 4096,
    )
    sampler = PerformanceSampler(interval_s=0.01).start()
    sampler.watch_process(os.getpid(), label="self")
    with sampler.measure():
        time.sleep(0.02)
    summary = sampler.stop().summary()
    assert summary["processes"]["self"]["samples"] >= 1
    assert summary["processes"]["self"]["peak"] == 4096
