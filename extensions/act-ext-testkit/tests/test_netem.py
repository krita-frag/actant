"""弱网语义模拟单元测试（无 Runtime）。"""

from __future__ import annotations

import sys

import pytest
from act_ext_testkit import (
    NetemUnsupported,
    NetworkProfile,
    build_schedule,
    schedule_fingerprint,
    shaping_supported,
)


def test_profile_validation() -> None:
    with pytest.raises(ValueError):
        NetworkProfile(loss=1.5)
    with pytest.raises(ValueError):
        NetworkProfile(latency_ms=-1)


def test_unsupported_capabilities_raise_instead_of_noop() -> None:
    with pytest.raises(NetemUnsupported, match="duplicate"):
        NetworkProfile(duplicate=0.1).ensure_semantic()
    with pytest.raises(NetemUnsupported, match="bandwidth_kbps"):
        NetworkProfile(bandwidth_kbps=100).ensure_semantic()
    with pytest.raises(NetemUnsupported):
        build_schedule(NetworkProfile(bandwidth_kbps=10), ["a"], duration_s=1.0)


def test_schedule_is_deterministic() -> None:
    profile = NetworkProfile(latency_ms=100, jitter_ms=20, loss=0.3, reorder=0.1)
    nodes = ["a", "b", "c"]
    first = build_schedule(profile, nodes, duration_s=4.0, tick_s=0.5, seed=9)
    second = build_schedule(profile, nodes, duration_s=4.0, tick_s=0.5, seed=9)
    assert first == second
    assert schedule_fingerprint(first) == schedule_fingerprint(second)
    assert first


def test_schedule_contains_expected_action_kinds() -> None:
    profile = NetworkProfile(latency_ms=50, loss=1.0)
    actions = build_schedule(profile, ["a", "b"], duration_s=2.0, tick_s=0.5, seed=1)
    kinds = {action.kind for action in actions}
    assert "heartbeat_delay" in kinds
    assert "partition" in kinds
    assert "heal" in kinds
    partitions = [a for a in actions if a.kind == "partition"]
    assert {(a.source, a.target) for a in partitions} == {("a", "b"), ("b", "a")}


def test_reorder_is_documented_as_drop_approximation() -> None:
    profile = NetworkProfile(reorder=0.25)
    assert profile.effective_drop == pytest.approx(0.25)


def test_shaping_reports_platform_capability() -> None:
    supported, reason = shaping_supported()
    if sys.platform == "linux":
        assert supported is True or reason
    elif sys.platform == "darwin":
        assert supported is True or "dummynet" in reason
    else:
        assert supported is False
        assert "Linux" in reason or "macOS" in reason


def test_empty_nodes_rejected() -> None:
    with pytest.raises(ValueError):
        build_schedule(NetworkProfile(), [], duration_s=1.0)
