"""``actant.task._helpers._execute_with_retries`` 单元测试（不依赖 Runtime）。"""
from __future__ import annotations

import cloudpickle

from actant.exceptions import TaskCancelledError
from actant.task._helpers import (
    _execute_with_retries,
)


class _FakeCancelToken:
    def __init__(self, cancelled: bool = False) -> None:
        self._cancelled = cancelled

    def is_cancelled(self) -> bool:
        return self._cancelled


def _deserialize_outcome(payload: bytes) -> tuple[bool, object]:
    return cloudpickle.loads(payload)


def test_execute_with_retries_success() -> None:
    # P2-9 优化后 _execute_with_retries 直接返回 (success, payload_obj) 元组，
    # 不再序列化。payload_obj 是 result 对象本身。
    ok, result_obj = _execute_with_retries(
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
    assert ok is True
    assert result_obj == 42


def test_execute_with_retries_exhausted() -> None:
    """重试次数耗尽后返回异常对象（P2-9：不再序列化为 bytes）。"""
    ok, exc_obj = _execute_with_retries(
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
    assert ok is False
    assert isinstance(exc_obj, ValueError)
    assert str(exc_obj) == "boom"


def test_execute_with_retries_cancelled_before_attempt() -> None:
    """cancel_token 已取消时直接返回 TaskCancelledError 对象。"""
    ok, exc_obj = _execute_with_retries(
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
    assert ok is False
    assert isinstance(exc_obj, TaskCancelledError)


def test_execute_with_retries_cancel_during_retry() -> None:
    """重试延迟期间被取消应返回 TaskCancelledError 对象。"""
    token = _FakeCancelToken()

    def _cancel_after() -> None:
        import time

        time.sleep(0.02)
        token._cancelled = True

    import threading

    threading.Thread(target=_cancel_after, daemon=True).start()
    ok, exc_obj = _execute_with_retries(
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
    assert ok is False
    assert isinstance(exc_obj, TaskCancelledError)
