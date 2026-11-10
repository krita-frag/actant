"""追踪记录与生命周期不变量单元测试。"""

from __future__ import annotations

import pytest
from act_ext_testkit import (
    InvariantViolation,
    TraceRecorder,
    Violation,
    assert_all_terminal,
    assert_lifecycle_valid,
    assert_no_duplicate_execution,
    assert_trace_order,
    check_trace,
)


def test_valid_lifecycle_has_no_violations() -> None:
    trace = TraceRecorder()
    trace.record("worker", "started", task_id="t1", attempt=1)
    trace.record("worker", "completed", task_id="t1", attempt=1)
    assert check_trace(trace) == []
    assert_lifecycle_valid(trace)
    assert_all_terminal(trace)
    assert_no_duplicate_execution(trace)
    assert_trace_order(trace)
    assert trace.terminal_state("t1") == "completed"


def test_duplicate_terminal_is_reported() -> None:
    trace = TraceRecorder()
    trace.record("worker", "started", task_id="t1")
    trace.record("worker", "completed", task_id="t1")
    trace.record("worker", "failed", task_id="t1")
    violations = check_trace(trace)
    assert any(v.code == "terminal_duplicate" for v in violations)
    with pytest.raises(InvariantViolation):
        assert_lifecycle_valid(trace)


def test_terminal_without_start_is_reported() -> None:
    trace = TraceRecorder()
    trace.record("worker", "failed", task_id="t1", error="boom")
    violations = check_trace(trace)
    assert {v.code for v in violations} == {"terminal_without_start"}


def test_partial_trace_allows_terminal_without_start() -> None:
    """观察者视角的 trace 允许远端任务只留下终态。"""
    trace = TraceRecorder(partial=True)
    trace.record("a", "completed", task_id="t1")
    assert check_trace(trace) == []
    assert_lifecycle_valid(trace)
    assert_trace_order(trace)
    assert any(
        v.code == "terminal_without_start"
        for v in check_trace(trace, allow_partial=False)
    )


def test_duplicate_execution_without_retry_is_reported() -> None:
    trace = TraceRecorder()
    trace.record("worker", "started", task_id="t1")
    trace.record("worker", "started", task_id="t1")
    trace.record("worker", "completed", task_id="t1")
    assert any(v.code == "duplicate_execution" for v in check_trace(trace))
    with pytest.raises(InvariantViolation):
        assert_no_duplicate_execution(trace)


def test_retry_explains_multiple_starts() -> None:
    trace = TraceRecorder()
    trace.record("worker", "started", task_id="t1", attempt=0)
    trace.record("worker", "retried", task_id="t1", attempt=0, next_attempt=1)
    trace.record("worker", "started", task_id="t1", attempt=1)
    trace.record("worker", "completed", task_id="t1", attempt=1)
    assert check_trace(trace) == []
    assert_no_duplicate_execution(trace)


def test_bad_retry_order_is_reported() -> None:
    trace = TraceRecorder()
    trace.record("worker", "started", task_id="t1", attempt=2)
    trace.record("worker", "retried", task_id="t1", attempt=2, next_attempt=2)
    violations = check_trace(trace)
    assert any(v.code == "retry_order" for v in violations)


def test_incomplete_task_can_be_allowed_or_rejected() -> None:
    trace = TraceRecorder()
    trace.record("worker", "started", task_id="t1")
    assert check_trace(trace, allow_incomplete=True) == []
    assert any(v.code == "not_terminal" for v in check_trace(trace, allow_incomplete=False))


def test_assert_all_terminal_reports_missing_and_duplicate() -> None:
    trace = TraceRecorder()
    trace.record("a", "started", task_id="t1")
    trace.record("a", "completed", task_id="t1")
    trace.record("a", "started", task_id="t2")
    with pytest.raises(AssertionError, match="t2: no terminal event"):
        assert_all_terminal(trace, ["t1", "t2"])


def test_trace_order_detects_terminal_before_start() -> None:
    trace = TraceRecorder()
    trace.record("a", "completed", task_id="t1")
    trace.record("a", "started", task_id="t1")
    with pytest.raises(InvariantViolation, match="terminal_before_start"):
        assert_trace_order(trace)


def test_query_and_summary() -> None:
    trace = TraceRecorder()
    trace.record("a", "started", task_id="t1", workflow_id="w1")
    trace.record("a", "log", task_id="t1", data={"message": "hi"})
    trace.record("b", "worker", data={"state": "running"})
    assert trace.query(kind="started")[0].task_id == "t1"
    assert len(trace.query(workflow_id="w1")) == 1
    assert trace.tasks() == ["t1"]
    summary = trace.summary()
    assert summary["event_count"] == 3
    assert summary["task_count"] == 1
    assert summary["by_node"] == {"a": 2, "b": 1}


def test_wait_for_returns_matching_event() -> None:
    trace = TraceRecorder()
    trace.record("a", "started", task_id="t1")
    event = trace.wait_for(
        lambda e: e.kind == "started" and e.task_id == "t1",
        timeout_s=0.5,
        description="started event",
    )
    assert event.node == "a"
    with pytest.raises(TimeoutError):
        trace.wait_for(lambda e: e.kind == "failed", timeout_s=0.1)


def test_violation_string_contains_context() -> None:
    violation = Violation("x", "broken", task_id="t", node="n", seq=3)
    text = str(violation)
    assert "x" in text and "task=t" in text and "node=n" in text
