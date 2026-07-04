"""config 模块单元测试：优先级、失败策略、网络配置、Workflow 状态。"""

from __future__ import annotations

import dataclasses

import pytest

from actant.config import (
    FailureStrategy,
    NetworkConfig,
    TaskPriority,
    WorkflowState,
    normalize_priority,
)

# ---------------------------------------------------------------------------
# TaskPriority
# ---------------------------------------------------------------------------


class TestTaskPriority:
    """TaskPriority IntEnum。"""

    def test_values(self):
        assert int(TaskPriority.LOW) == -10
        assert int(TaskPriority.NORMAL) == 0
        assert int(TaskPriority.HIGH) == 10
        assert int(TaskPriority.CRITICAL) == 20

    def test_ordering(self):
        """优先级数值越大越紧急。"""
        assert TaskPriority.CRITICAL > TaskPriority.HIGH
        assert TaskPriority.HIGH > TaskPriority.NORMAL
        assert TaskPriority.NORMAL > TaskPriority.LOW

    def test_is_int(self):
        """IntEnum 成员本身就是 int，可参与算术。"""
        assert TaskPriority.HIGH + 5 == 15
        assert TaskPriority.HIGH - TaskPriority.NORMAL == 10


class TestNormalizePriority:
    """normalize_priority 接受多种输入并归一化为整数。"""

    def test_none_returns_normal(self):
        assert normalize_priority(None) == 0

    def test_int_member(self):
        assert normalize_priority(TaskPriority.HIGH) == 10

    def test_int_value(self):
        assert normalize_priority(15) == 15
        assert normalize_priority(-5) == -5

    def test_string_lowercase(self):
        assert normalize_priority("low") == -10
        assert normalize_priority("normal") == 0
        assert normalize_priority("high") == 10
        assert normalize_priority("critical") == 20

    def test_string_uppercase(self):
        """字符串大小写不敏感。"""
        assert normalize_priority("HIGH") == 10
        assert normalize_priority("Critical") == 20

    def test_invalid_string_raises_value_error(self):
        with pytest.raises(ValueError, match="invalid priority"):
            normalize_priority("invalid")

    def test_invalid_type_raises_type_error(self):
        with pytest.raises(TypeError, match="priority must be"):
            normalize_priority(3.14)  # type: ignore[arg-type]

    @pytest.mark.parametrize("v", [-1000, -10, 0, 10, 15, 100, 1000])
    def test_arbitrary_int_round_trip(self, v):
        """任意整数应原样返回。"""
        assert normalize_priority(v) == v


# ---------------------------------------------------------------------------
# WorkflowState
# ---------------------------------------------------------------------------


class TestWorkflowState:
    """WorkflowState 状态常量。"""

    def test_all_states_defined(self):
        assert WorkflowState.PENDING == "Pending"
        assert WorkflowState.RUNNING == "Running"
        assert WorkflowState.COMPLETED == "Completed"
        assert WorkflowState.FAILED == "Failed"
        assert WorkflowState.CANCELLED == "Cancelled"
        assert WorkflowState.TIMEOUT == "Timeout"

    def test_terminal_set_excludes_pending_and_running(self):
        assert WorkflowState.PENDING not in WorkflowState.TERMINAL
        assert WorkflowState.RUNNING not in WorkflowState.TERMINAL

    def test_terminal_set_includes_all_terminal(self):
        for s in (WorkflowState.COMPLETED, WorkflowState.FAILED,
                  WorkflowState.CANCELLED, WorkflowState.TIMEOUT):
            assert s in WorkflowState.TERMINAL

    def test_terminal_set_is_frozen(self):
        """TERMINAL 是 frozenset，不可变。"""
        assert isinstance(WorkflowState.TERMINAL, frozenset)
        with pytest.raises(AttributeError):
            WorkflowState.TERMINAL.add("X")  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# FailureStrategy
# ---------------------------------------------------------------------------


class TestFailureStrategy:
    """FailureStrategy.normalize。"""

    def test_none_returns_fail_fast(self):
        assert FailureStrategy.normalize(None) == "fail_fast"

    def test_string_returned_as_is(self):
        assert FailureStrategy.normalize("fail_fast") == "fail_fast"
        assert FailureStrategy.normalize("continue_on_failure") == "continue_on_failure"

    def test_custom_string_passes_through(self):
        """自定义策略字符串透传。"""
        assert FailureStrategy.normalize("my_custom") == "my_custom"

    def test_empty_string_passes_through(self):
        assert FailureStrategy.normalize("") == ""

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError, match="failure_strategy must be"):
            FailureStrategy.normalize(123)  # type: ignore[arg-type]

    def test_constants(self):
        assert FailureStrategy.FAIL_FAST == "fail_fast"
        assert FailureStrategy.CONTINUE_ON_FAILURE == "continue_on_failure"


# ---------------------------------------------------------------------------
# NetworkConfig
# ---------------------------------------------------------------------------


class TestNetworkConfig:
    """NetworkConfig frozen dataclass。"""

    def test_defaults(self):
        cfg = NetworkConfig()
        assert cfg.preset == "local"
        assert cfg.bootstrap_nodes == ()
        assert cfg.max_message_size == 16 * 1024 * 1024
        assert cfg.allowed_peer_ids == ()
        assert cfg.gossip_bootstrap_peers == ()

    def test_empty_preset_raises_value_error(self):
        """preset 为空字符串应抛 ValueError。"""
        with pytest.raises(ValueError, match="network preset must not be empty"):
            NetworkConfig(preset="")

    def test_frozen(self):
        """NetworkConfig 是 frozen dataclass，不可变。"""
        cfg = NetworkConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.preset = "mdns"  # type: ignore[misc]

    def test_bootstrap_nodes_accepts_list(self):
        """构造时接受 list，自动归一化为 tuple。"""
        cfg = NetworkConfig(bootstrap_nodes=["a", "b"])
        assert cfg.bootstrap_nodes == ("a", "b")
        assert isinstance(cfg.bootstrap_nodes, tuple)

    def test_bootstrap_nodes_accepts_tuple(self):
        cfg = NetworkConfig(bootstrap_nodes=("a", "b"))
        assert cfg.bootstrap_nodes == ("a", "b")

    def test_bootstrap_nodes_default_is_empty_tuple(self):
        cfg = NetworkConfig()
        assert cfg.bootstrap_nodes == ()
        assert isinstance(cfg.bootstrap_nodes, tuple)

    def test_custom_preset(self):
        cfg = NetworkConfig(preset="mdns")
        assert cfg.preset == "mdns"

    def test_custom_max_message_size(self):
        cfg = NetworkConfig(max_message_size=1024)
        assert cfg.max_message_size == 1024

    def test_allowed_peer_ids(self):
        cfg = NetworkConfig(allowed_peer_ids=["peer1", "peer2"])
        assert cfg.allowed_peer_ids == ("peer1", "peer2")

    def test_gossip_bootstrap_peers(self):
        cfg = NetworkConfig(gossip_bootstrap_peers=["g1"])
        assert cfg.gossip_bootstrap_peers == ("g1",)

    def test_direct_request_timeout_default(self):
        cfg = NetworkConfig()
        assert cfg.direct_request_timeout_ms == 30_000

    def test_direct_request_timeout_custom(self):
        cfg = NetworkConfig(direct_request_timeout_ms=5_000)
        assert cfg.direct_request_timeout_ms == 5_000

    def test_two_instances_independent(self):
        """两个 NetworkConfig 实例的 default tuple 独立。"""
        a = NetworkConfig()
        b = NetworkConfig()
        _ = a.bootstrap_nodes  # access but should not affect b
        assert b.bootstrap_nodes == ()
