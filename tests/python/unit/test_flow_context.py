"""``actant.flow`` 上下文辅助函数单元测试（不依赖 Runtime）。"""
from __future__ import annotations

from actant.flow import _deadline_failure, _FlowState, current_workflow_id


def test_current_workflow_id_outside_flow() -> None:
    assert current_workflow_id() is None


def test_flow_state_hands_off_deadline_without_local_judgement() -> None:
    """``timeout_ms`` 只是被转交给 orchestrator 的 deadline。

    Python 侧不得再持有计时设施（``cancel_event`` / ``is_cancelled``）——超时的
    判定与强还原都归 orchestrator 的超时 watcher，本地复制一份就是"双源"。
    """
    state = _FlowState("wf-1", timeout_ms=1500)
    assert state.workflow_id == "wf-1"
    assert state.timeout_ms == 1500
    assert not hasattr(state, "cancel_event")
    assert not hasattr(state, "is_cancelled")


def test_current_workflow_id_reads_local_state() -> None:
    import importlib

    flow_mod = importlib.import_module("actant.flow")
    state = _FlowState("wf-2")
    flow_mod._flow_local.state = state
    try:
        assert current_workflow_id() == "wf-2"
    finally:
        del flow_mod._flow_local.state


class TestDeadlineFailurePredicate:
    """唯一的超时判定式：只看 orchestrator 写下的事实，不复算计时器。"""

    def test_true_only_for_failed_with_timeout_error(self) -> None:
        assert _deadline_failure(
            {"state": "Failed", "error": "workflow timeout exceeded"}
        )
        assert not _deadline_failure({"state": "Failed", "error": "boom"})
        assert not _deadline_failure({"state": "Completed", "error": None})
        assert not _deadline_failure({"state": "Cancelled", "error": None})
        assert not _deadline_failure(None)

    def test_missing_or_empty_error_is_not_a_timeout(self) -> None:
        assert not _deadline_failure({"state": "Failed", "error": ""})
        assert not _deadline_failure({"state": "Failed"})
