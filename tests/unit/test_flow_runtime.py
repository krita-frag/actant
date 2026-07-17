"""``actant.flow`` 运行时单元测试（依赖 Runtime）。"""
from __future__ import annotations

import pytest

from actant import Runtime, flow, task
from actant.exceptions import ActantTimeoutError, InvalidStateError


@task
def _add_one(x: int) -> int:
    return x + 1


@task
def _slow_task() -> str:
    import time

    time.sleep(2)
    return "done"


_flow_retry_count: dict[str, int] = {"count": 0}


def _bump_flow_retry() -> None:
    _flow_retry_count["count"] += 1


def test_flow_requires_runtime() -> None:
    @flow
    def my_flow() -> None:
        return None

    with pytest.raises(InvalidStateError):
        my_flow()


def test_flow_success() -> None:
    @flow
    def my_flow(x: int) -> int:
        return _add_one.submit(x).result()  # type: ignore[no-any-return]

    with Runtime.with_defaults() as rt:
        events: list[str] = []
        rt.chain("WorkflowLifecycle", lambda e: events.append(e.kind))
        assert my_flow(5) == 6
    assert "submitted" in events
    assert "started" in events
    assert "completed" in events


def test_flow_failure() -> None:
    @flow
    def my_flow() -> None:
        raise ValueError("boom")

    with Runtime.with_defaults() as rt:
        events: list[str] = []
        rt.chain("WorkflowLifecycle", lambda e: events.append(e.kind))
        with pytest.raises(ValueError, match="boom"):
            my_flow()
    assert "failed" in events


def test_flow_retry_then_success() -> None:
    _flow_retry_count["count"] = 0

    @flow(retries=2, retry_delay_ms=0)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        _bump_flow_retry()
        if _flow_retry_count["count"] < 3:
            raise RuntimeError("not yet")
        return "ok"

    with Runtime.with_defaults():
        assert my_flow() == "ok"
        assert _flow_retry_count["count"] == 3


def test_flow_timeout() -> None:
    @flow(timeout_ms=100)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        return _slow_task().result()  # type: ignore[no-any-return]

    with Runtime.with_defaults(), pytest.raises(ActantTimeoutError):
        my_flow()


def test_flow_with_name() -> None:
    @flow(name="custom-flow")  # type: ignore[untyped-decorator]
    def my_flow() -> int:
        return 42

    with Runtime.with_defaults():
        assert my_flow() == 42


def test_flow_no_parentheses() -> None:
    @flow
    def my_flow() -> int:
        return 42

    with Runtime.with_defaults():
        assert my_flow() == 42


def test_flow_invalid_retries() -> None:
    with pytest.raises(ValueError, match="retries"):
        @flow(retries=-1)  # type: ignore[untyped-decorator]
        def my_flow() -> None:
            return None


def test_flow_workflow_id_in_context() -> None:
    from actant.flow import current_workflow_id

    @flow
    def my_flow() -> str | None:
        return current_workflow_id()

    with Runtime.with_defaults():
        wid = my_flow()
        assert wid is not None
        assert "my_flow" in wid
