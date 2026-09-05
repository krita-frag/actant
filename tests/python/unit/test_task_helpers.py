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
    """v2 envelope 往返：头部 + ``(func, args, kwargs)`` 可经 canonical 解析还原。"""
    def add(a: int, b: int) -> int:
        return a + b

    from actant.task._helpers import _PAYLOAD_VERSION
    from actant.task._worker import _parse_dispatch_payload

    raw = _safe_serialize(add, (1, 2), {"c": 3}, {"timeout_ms": 100}, task_id="t1")
    # 首字节即版本字节。
    assert raw[0] == _PAYLOAD_VERSION
    retries, retry_delay_ms, task_id, workflow_id, func_payload = _parse_dispatch_payload(raw)
    assert retries == 0
    assert retry_delay_ms == 0
    assert task_id == "t1"  # options 未给 task_id → 回退到 task_id 参数
    assert workflow_id == ""
    func, args, kwargs = cloudpickle.loads(func_payload)
    assert args == (1, 2)
    assert kwargs == {"c": 3}
    assert func(1, 2) == 3


def test_safe_serialize_v2_header_fields() -> None:
    """options 中的控制字段迁入头部；``timeout_ms`` 为死参数不进头部。"""
    def noop() -> None:
        return None

    from actant.task._worker import _parse_dispatch_payload

    raw = _safe_serialize(
        noop,
        (),
        {},
        {
            "retries": 3,
            "retry_delay_ms": 50,
            "task_id": "tid-1",
            "workflow_id": "wf-7",
            "timeout_ms": 999,
        },
        task_id="ignored-name",
    )
    retries, retry_delay_ms, task_id, workflow_id, func_payload = _parse_dispatch_payload(raw)
    assert retries == 3
    assert retry_delay_ms == 50
    assert task_id == "tid-1"
    assert workflow_id == "wf-7"
    # func_payload 仅含 (func, args, kwargs)。cloudpickle 经名重建对象，校验语义。
    func, args, kwargs = cloudpickle.loads(func_payload)
    assert func.__name__ == "noop"
    assert args == ()
    assert kwargs == {}


def test_parse_dispatch_payload_rejects_bad_version() -> None:
    """非 v2 版本字节或头部截断应显式抛错（协议损坏走防御分支）。"""
    import struct

    from actant.task._worker import _parse_dispatch_payload

    # v1 风格（版本字节 0x01）→ 版本不匹配。
    body = struct.pack("<BIIH", 0x01, 0, 0, 0)
    with pytest.raises(ValueError, match="version"):
        _parse_dispatch_payload(body)

    # 头部截断。
    with pytest.raises(ValueError, match="too short"):
        _parse_dispatch_payload(b"\x02\x00")


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


def test_suppress_pickle_errors_swallows_pickle_related_exception() -> None:
    """抑制范围限定为 pickle 反序列化相关异常族（UnpicklingError/TypeError/
    AttributeError/ValueError），族内异常静默。"""

    class _Bad:
        def __reduce__(self) -> None:
            raise ValueError("bad")

    with _suppress_pickle_errors():
        cloudpickle.dumps(_Bad())


def test_suppress_pickle_errors_reraises_other_exceptions() -> None:
    """pickle 相关异常族之外的异常（如 RuntimeError）照常向上抛出，不静默。"""

    class _Bad:
        def __reduce__(self) -> None:
            raise RuntimeError("bad")

    with pytest.raises(RuntimeError, match="bad"), _suppress_pickle_errors():
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


# ───────────────────────── EventBatcher.close() 显式 join 测试（H5）─────────────────────────


def test_event_batcher_close_joins_flush_thread() -> None:
    """close() 应显式 join 后台 flush 线程，避免线程泄漏。"""
    from actant.task._helpers import _EventBatcher

    batcher = _EventBatcher(flush_interval_ms=1, flush_threshold=100)
    flush_thread = batcher._flush_thread
    assert flush_thread is not None
    assert flush_thread.is_alive()

    batcher.close()

    # flush 线程应已退出（不再是 alive）。
    assert not flush_thread.is_alive(), "flush thread should be joined after close()"
    # 重复 close 应为 no-op（不阻塞、不抛异常）。
    batcher.close()
    assert batcher._flush_thread is None


def test_event_batcher_close_flushes_pending_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """close() 应派发 close 前入队但未被后台线程处理的事件。"""
    from actant.task._helpers import _EventBatcher

    emitted: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "actant._effects.emit",
        lambda *args, **kwargs: emitted.append((args[0] if args else "", args[1] if len(args) > 1 else "")),
    )

    # 仅按 threshold flush（interval=0 不启动后台线程），确保事件滞留 buffer。
    batcher = _EventBatcher(flush_interval_ms=0, flush_threshold=100)
    batcher.add("started", "t1", "wf1")
    batcher.add("completed", "t1", "wf1")
    assert emitted == [], "no threshold flush should have fired yet"

    batcher.close()
    # close 应派发所有滞留事件。
    assert len(emitted) == 2, f"expected 2 events flushed on close, got {len(emitted)}"


def test_event_batcher_close_aborts_join_on_stuck_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    """close() 在 flush 线程卡死时应放弃 join 并记录 warning（按 join 超时）。"""
    from actant.task import _helpers as helpers_module
    from actant.task._helpers import _EventBatcher

    # 缩短 join 超时到 0.3s，使测试快速验证超时分支。
    monkeypatch.setattr(
        helpers_module, "_EVENT_BATCHER_CLOSE_JOIN_TIMEOUT", 0.3
    )

    # 永久阻塞的 emit：flush 线程进入 emit 后不会自行退出，
    # close 的 join 必然超时。
    blocker = threading.Event()

    def _stuck_emit(*args: object, **kwargs: object) -> None:
        # 永久等待直到测试结束释放 blocker（或进程退出）。
        blocker.wait(timeout=30.0)

    monkeypatch.setattr("actant._effects.emit", _stuck_emit)

    batcher = _EventBatcher(flush_interval_ms=1, flush_threshold=100)
    flush_thread = batcher._flush_thread
    assert flush_thread is not None
    # 等待后台线程进入第一次 flush（卡在 emit 上）。
    batcher.add("started", "t1", "wf1")
    time.sleep(0.05)

    # close 应在 ~0.3s 超时后放弃 join 并返回（不抛异常）。
    start = time.monotonic()
    batcher.close()
    elapsed = time.monotonic() - start
    # 实际等待应接近 0.3s（join 超时）；放宽下限避免抖动。
    assert elapsed >= 0.25, f"close should wait ~0.3s for stuck flush, got {elapsed:.2f}s"
    assert elapsed < 2.0, f"close should not block much longer than 0.3s, got {elapsed:.2f}s"

    # 释放阻塞，让后台线程退出（避免泄漏到其他测试）。
    blocker.set()
    flush_thread.join(timeout=2.0)
