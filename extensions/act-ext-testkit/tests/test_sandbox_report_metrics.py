"""沙箱、报告与性能采样器单元测试。"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from act_ext_testkit import (
    ClusterReport,
    PerformanceError,
    PerformanceSampler,
    Sandbox,
    SandboxError,
)
from act_ext_testkit.sandbox import PACKAGE_ROOT


def test_sandbox_isolates_paths_and_child_env() -> None:
    box = Sandbox("unit", env={"ACTANT_FOO": "bar"})
    try:
        node_dir = box.node_dir("node-a")
        assert node_dir.is_dir()
        assert box.root in node_dir.parents
        log = box.log_path("node-a")
        assert log.parent == box.log_dir
        env = box.child_env({"EXTRA": "1"})
        assert env["TMPDIR"] == str(box.tmp_dir)
        assert env["TEMP"] == str(box.tmp_dir)
        assert env["TMP"] == str(box.tmp_dir)
        assert env["ACTANT_FOO"] == "bar"
        assert env["EXTRA"] == "1"
        assert str(PACKAGE_ROOT) in env["PYTHONPATH"].split(":")
        manifest = box.manifest()
        assert manifest["name"] == "unit"
        assert str(node_dir) in manifest["node_dirs"]
    finally:
        root = box.root
        box.cleanup()
    assert not root.exists()
    box.assert_clean()


def test_sandbox_runs_cleanups_in_reverse_order() -> None:
    box = Sandbox("cleanup")
    calls: list[str] = []
    box.track_cleanup(lambda: calls.append("first"))
    box.track_cleanup(lambda: calls.append("second"))
    box.cleanup()
    assert calls == ["second", "first"]
    box.cleanup()
    with pytest.raises(SandboxError):
        box.track_cleanup(lambda: None)


def test_sandbox_keep_preserves_root(tmp_path: Path) -> None:
    root = tmp_path / "kept"
    box = Sandbox("kept", root=root, keep=True)
    (root / "marker").write_text("x")
    box.cleanup()
    assert root.exists()
    assert box.leftovers() == []


def test_sandbox_external_root_is_not_owned(tmp_path: Path) -> None:
    root = tmp_path / "external"
    box = Sandbox("external", root=root)
    box.node_dir("a")
    box.cleanup()
    assert root.exists()
    assert box.leftovers() == []
    box.assert_clean()


def test_report_round_trip(tmp_path: Path) -> None:
    report = ClusterReport(namespace="r", seed=7)
    report.set_manifest({"topology": {"name": "mesh"}})
    report.record("node_started", node="a")
    report.record_fault({"kind": "partition", "source": "a", "target": "b"})
    report.attach_trace({"event_count": 2, "tasks": ["t1"]})
    report.attach_metrics({"latency_ms": {"p50": 1.0}})
    target = report.dump(tmp_path / "report.json")
    loaded = ClusterReport.load(target)
    assert loaded["namespace"] == "r"
    assert loaded["seed"] == 7
    assert loaded["manifest"]["topology"]["name"] == "mesh"
    assert loaded["events"][0]["kind"] == "node_started"
    assert loaded["faults"][0]["kind"] == "partition"
    assert loaded["trace"]["event_count"] == 2
    assert loaded["metrics"]["latency_ms"]["p50"] == 1.0


def test_report_snapshot_is_json_serialisable() -> None:
    report = ClusterReport(namespace="json")
    report.record("x", value=1)
    json.dumps(report.snapshot())


def test_performance_sampler_records_latency_and_rss() -> None:
    sampler = PerformanceSampler(interval_s=0.01).start()
    try:
        for value in (10.0, 20.0, 30.0, 40.0, 100.0):
            sampler.record_latency_ms(value)
        with sampler.measure():
            time.sleep(0.005)
        time.sleep(0.03)
    finally:
        sampler.stop()
    summary = sampler.summary()
    assert summary["latency_ms"]["count"] == 6
    assert summary["latency_ms"]["p50"] > 0
    assert summary["latency_ms"]["p99"] >= summary["latency_ms"]["p50"]
    assert summary["completed"] == 1
    assert summary["throughput_per_s"] > 0
    assert summary["rss_bytes"]["peak"] > 0


def test_performance_sampler_lifecycle_errors() -> None:
    sampler = PerformanceSampler(interval_s=0.01).start()
    with pytest.raises(PerformanceError):
        sampler.start()
    sampler.stop()
    sampler.stop()
    with pytest.raises(ValueError):
        sampler.record_latency_ms(-1.0)
