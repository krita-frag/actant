"""``actant.flow`` 上下文辅助函数单元测试（不依赖 Runtime）。"""
from __future__ import annotations

from actant.flow import _FlowState, current_workflow_id, is_flow_cancelled


def test_current_workflow_id_outside_flow() -> None:
    assert current_workflow_id() is None


def test_is_flow_cancelled_outside_flow() -> None:
    assert is_flow_cancelled() is False


def test_flow_state_tracks_cancellation() -> None:
    state = _FlowState("wf-1")
    assert state.workflow_id == "wf-1"
    assert not state.is_cancelled()
    state.cancel_event.set()
    assert state.is_cancelled()


def test_current_workflow_id_reads_local_state() -> None:
    import importlib

    flow_mod = importlib.import_module("actant.flow")
    state = _FlowState("wf-2")
    flow_mod._flow_local.state = state
    try:
        assert current_workflow_id() == "wf-2"
        assert is_flow_cancelled() is False
    finally:
        del flow_mod._flow_local.state
