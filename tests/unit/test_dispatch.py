"""``actant.task._dispatch`` 单元测试（不依赖 Runtime）。"""
from __future__ import annotations

import cloudpickle
import pytest

from actant.capabilities import ExecuteCtx
from actant.exceptions import ActantError, TaskCancelledError
from actant.task._dispatch import (
    _execute_with_retries,
    _generic_execute_handler,
)


class _FakeCancelToken:
    def __init__(self, cancelled: bool = False) -> None:
        self._cancelled = cancelled

    def is_cancelled(self) -> bool:
        return self._cancelled


def _deserialize_outcome(payload: bytes) -> tuple[bool, object]:
    return cloudpickle.loads(payload)


def test_execute_with_retries_success() -> None:
    payload = _execute_with_retries(
        lambda: 42,
        (),
        {},
        timeout_ms=1000,
        retries=0,
        retry_delay_ms=0,
        task_id="t1",
        workflow_id="wf1",
        cancel_token=_FakeCancelToken(),
    )
    ok, result_bytes = _deserialize_outcome(payload)
    assert ok is True
    assert cloudpickle.loads(result_bytes) == 42


def test_execute_with_retries_exhausted() -> None:
    """重试次数耗尽后返回编码的异常。"""
    payload = _execute_with_retries(
        lambda: (_ for _ in ()).throw(ValueError("boom")),
        (),
        {},
        timeout_ms=1000,
        retries=2,
        retry_delay_ms=0,
        task_id="t1",
        workflow_id="wf1",
        cancel_token=_FakeCancelToken(),
    )
    ok, error_bytes = _deserialize_outcome(payload)
    assert ok is False
    exc = cloudpickle.loads(error_bytes)
    assert isinstance(exc, ValueError)
    assert str(exc) == "boom"


def test_execute_with_retries_cancelled_before_attempt() -> None:
    """cancel_token 已取消时直接返回 TaskCancelledError。"""
    payload = _execute_with_retries(
        lambda: 42,
        (),
        {},
        timeout_ms=1000,
        retries=0,
        retry_delay_ms=0,
        task_id="t1",
        workflow_id="wf1",
        cancel_token=_FakeCancelToken(cancelled=True),
    )
    ok, error_bytes = _deserialize_outcome(payload)
    assert ok is False
    exc = cloudpickle.loads(error_bytes)
    assert isinstance(exc, TaskCancelledError)


def test_execute_with_retries_cancel_during_retry() -> None:
    """重试延迟期间被取消应返回 TaskCancelledError。"""
    token = _FakeCancelToken()

    def _cancel_after() -> None:
        import time

        time.sleep(0.02)
        token._cancelled = True

    import threading

    threading.Thread(target=_cancel_after, daemon=True).start()
    payload = _execute_with_retries(
        lambda: (_ for _ in ()).throw(ValueError("boom")),
        (),
        {},
        timeout_ms=1000,
        retries=5,
        retry_delay_ms=500,
        task_id="t1",
        workflow_id="wf1",
        cancel_token=token,
    )
    ok, error_bytes = _deserialize_outcome(payload)
    assert ok is False
    exc = cloudpickle.loads(error_bytes)
    assert isinstance(exc, TaskCancelledError)


def test_generic_execute_handler_deserialization_failure() -> None:
    """payload 无法反序列化时应抛出 ActantError serialization。"""
    ctx = ExecuteCtx(
        task_id="t1",
        workflow_id="wf1",
        payload=b"not cloudpickle data",
        timeout_ms=1000,
    )
    with pytest.raises(ActantError, match="failed to deserialize payload"):
        _generic_execute_handler(ctx)
