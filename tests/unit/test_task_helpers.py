"""``actant.task._helpers`` 单元测试（不依赖 Runtime）。"""
from __future__ import annotations

import threading
import time

import cloudpickle
import pytest

from actant.exceptions import SerializationError, TaskCancelledError
from actant.task._context import TaskContext, _task_context_scope
from actant.task._helpers import (
    _emit_task_event,
    _interruptible_sleep,
    _pickle_exception,
    _run_with_timeout,
    _safe_serialize,
    _suppress_pickle_errors,
)


class _FakeCancelToken:
    """模拟 Rust cancel_token。"""

    def __init__(self) -> None:
        self._event = threading.Event()

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        self._event.set()


def test_interruptible_sleep_returns_on_cancel() -> None:
    token = _FakeCancelToken()
    t0 = time.monotonic()

    def _cancel_after() -> None:
        time.sleep(0.05)
        token.cancel()

    threading.Thread(target=_cancel_after, daemon=True).start()
    _interruptible_sleep(10.0, token, interval=0.01)
    elapsed = time.monotonic() - t0
    assert elapsed < 0.2


def test_interruptible_sleep_zero_duration_returns_immediately() -> None:
    token = _FakeCancelToken()
    _interruptible_sleep(0.0, token)


def test_interruptible_sleep_already_cancelled_returns_immediately() -> None:
    token = _FakeCancelToken()
    token.cancel()
    _interruptible_sleep(10.0, token)


def test_run_with_timeout_returns_value() -> None:
    def add(a: int, b: int) -> int:
        return a + b

    assert _run_with_timeout(add, (1, 2), {}, 1000) == 3


def test_run_with_timeout_propagates_exception() -> None:
    def boom() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        _run_with_timeout(boom, (), {}, 1000)


def test_run_with_timeout_checks_cancel_before_start() -> None:
    ctx = TaskContext("t-cancel")
    ctx._cancel()
    with _task_context_scope(ctx), pytest.raises(TaskCancelledError):
        _run_with_timeout(lambda: None, (), {}, 1000)


def test_pickle_exception_round_trip() -> None:
    exc = ValueError("round")
    raw = _pickle_exception(exc)
    out = cloudpickle.loads(raw)
    assert isinstance(out, ValueError)
    assert str(out) == "round"


def test_pickle_exception_falls_back_for_unpicklable() -> None:
    """不可序列化的异常应退化为携带类型与消息的 RuntimeError。"""

    class _Unpicklable(Exception):
        def __reduce__(self) -> None:
            raise TypeError("cannot pickle")

    raw = _pickle_exception(_Unpicklable("secret"))
    out = cloudpickle.loads(raw)
    assert isinstance(out, RuntimeError)
    assert "_Unpicklable" in str(out)
    assert "secret" in str(out)


def test_safe_serialize_round_trip() -> None:
    def add(a: int, b: int) -> int:
        return a + b

    raw = _safe_serialize(add, (1, 2), {"c": 3}, {"timeout_ms": 100}, task_id="t1")
    loaded = cloudpickle.loads(raw)
    assert loaded[1] == (1, 2)
    assert loaded[2] == {"c": 3}
    assert loaded[3] == {"timeout_ms": 100}
    assert loaded[0](1, 2) == 3


def test_safe_serialize_fails_for_unserializable() -> None:
    def func(x: object) -> object:
        return x

    with pytest.raises(SerializationError):
        _safe_serialize(func, (threading.Lock(),), {}, {}, task_id="t1")


def test_task_context_scope_sets_and_restores() -> None:
    from actant.task._context import get_task_context

    outer = TaskContext("outer")
    inner = TaskContext("inner")
    with _task_context_scope(outer):
        assert get_task_context() is outer
        with _task_context_scope(inner):
            assert get_task_context() is inner
        assert get_task_context() is outer
    assert get_task_context() is None


def test_task_context_scope_restores_on_exception() -> None:
    from actant.task._context import get_task_context

    outer = TaskContext("outer")
    with _task_context_scope(outer):
        try:
            with _task_context_scope(TaskContext("inner")):
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert get_task_context() is outer


def test_suppress_pickle_errors_swallows_exception() -> None:
    class _Bad:
        def __reduce__(self) -> None:
            raise RuntimeError("bad")

    with _suppress_pickle_errors():
        cloudpickle.dumps(_Bad())


def test_suppress_pickle_errors_no_exception() -> None:
    with _suppress_pickle_errors() as mgr:
        assert isinstance(mgr, _suppress_pickle_errors)


def test_fake_cancel_token() -> None:
    token = _FakeCancelToken()
    assert not token.is_cancelled()
    token.cancel()
    assert token.is_cancelled()


class _EmitFaker:
    """用于模拟 ``emit`` 调用失败的 callable。"""

    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc

    def __call__(self, *args: object, **kwargs: object) -> None:
        if self.exc is not None:
            raise self.exc


def test_emit_task_event_on_error_log_swallows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("actant._effects.emit", _EmitFaker(RuntimeError("emit boom")))
    result = _emit_task_event("started", "t1", "wf1", on_error="log")
    assert result is None


def test_emit_task_event_on_error_raise_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    exc = RuntimeError("emit boom")
    monkeypatch.setattr("actant._effects.emit", _EmitFaker(exc))
    with pytest.raises(RuntimeError, match="emit boom"):
        _emit_task_event("started", "t1", "wf1", on_error="raise")


def test_emit_task_event_on_error_collect_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    exc = RuntimeError("emit boom")
    monkeypatch.setattr("actant._effects.emit", _EmitFaker(exc))
    result = _emit_task_event("started", "t1", "wf1", on_error="collect")
    assert result is exc
