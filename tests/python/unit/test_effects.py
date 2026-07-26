"""``actant._effects`` 单元测试（构造 Runtime 但不 start Rust）。"""
from __future__ import annotations

import pytest

import actant
from actant import Runtime
from actant.exceptions import InvalidStateError


def _routing_ask(name: str) -> str:
    return f"{name}?"


def test_ask_without_runtime_raises() -> None:
    with pytest.raises(InvalidStateError):
        actant.ask("Routing", "x")


def test_perform_without_runtime_raises() -> None:
    with pytest.raises(InvalidStateError):
        actant.perform("Routing", "x")


def test_emit_without_runtime_raises() -> None:
    with pytest.raises(InvalidStateError):
        actant.emit("Routing", "x")


def test_ask_routing_basic() -> None:
    rt = Runtime()
    rt.layer("Routing", kind="ask").chain(_routing_ask)
    with actant.use_runtime(rt):
        result = actant.ask("Routing", "hello")
    assert result == "hello?"


def test_ask_returns_first_non_none() -> None:
    rt = Runtime()
    rt.layer("MyDecision", kind="ask").chain(lambda x: None)
    rt.layer("MyDecision", kind="ask").chain(lambda x: f"second:{x}")
    rt.layer("MyDecision", kind="ask").chain(lambda x: f"third:{x}")
    with actant.use_runtime(rt):
        result = actant.ask("MyDecision", "x")
    # 后注册的 handler 优先级更高（逆序调用），因此第三个 handler 决定结果。
    assert result == "third:x"


def test_ask_returns_none_when_all_abstain() -> None:
    rt = Runtime()
    rt.layer("MyDecision", kind="ask").chain(lambda x: None)
    with actant.use_runtime(rt):
        assert actant.ask("MyDecision", "x") is None


def test_perform_uses_last_handler() -> None:
    rt = Runtime.with_defaults()
    rt.chain("Serialization", lambda x: "first")
    rt.chain("Serialization", lambda x: "last")
    with actant.use_runtime(rt):
        assert actant.perform("Serialization", None) == "last"


def test_perform_without_handlers_raises() -> None:
    rt = Runtime()
    with actant.use_runtime(rt), pytest.raises(actant.NotFoundError):
        actant.perform("Serialization", None)


def test_emit_calls_all_handlers() -> None:
    rt = Runtime.with_defaults()
    called: list[str] = []
    rt.chain("TaskLifecycle", lambda e: called.append("a"))
    rt.chain("TaskLifecycle", lambda e: called.append("b"))
    with actant.use_runtime(rt):
        actant.emit("TaskLifecycle", "event")
    assert called == ["a", "b"]


def test_emit_on_error_raise() -> None:
    rt = Runtime.with_defaults()
    rt.chain("TaskLifecycle", lambda e: (_ for _ in ()).throw(ValueError("boom")))
    with actant.use_runtime(rt), pytest.raises(ValueError, match="boom"):
        actant.emit("TaskLifecycle", "event", on_error="raise")


def test_emit_on_error_collect() -> None:
    rt = Runtime.with_defaults()
    rt.chain("TaskLifecycle", lambda e: (_ for _ in ()).throw(ValueError("a")))
    rt.chain("TaskLifecycle", lambda e: (_ for _ in ()).throw(ValueError("b")))
    with actant.use_runtime(rt), pytest.raises(actant.ActantError):
        actant.emit("TaskLifecycle", "event", on_error="collect")


def test_emit_invalid_on_error() -> None:
    with pytest.raises(ValueError, match="on_error"):
        actant.emit("TaskLifecycle", "event", on_error="bad")  # type: ignore[arg-type]


def test_ask_kind_mismatch() -> None:
    rt = Runtime()
    rt.layer("MyPerform", kind="perform").chain(lambda x: x)
    with actant.use_runtime(rt), pytest.raises(InvalidStateError, match="not 'ask'"):
        actant.ask("MyPerform", "x")


def test_perform_kind_mismatch() -> None:
    rt = Runtime()
    rt.layer("MyAsk", kind="ask").chain(lambda x: x)
    with actant.use_runtime(rt), pytest.raises(InvalidStateError, match="not 'perform'"):
        actant.perform("MyAsk", "x")


def test_emit_kind_mismatch() -> None:
    rt = Runtime()
    rt.layer("MyAsk", kind="ask").chain(lambda x: x)
    with actant.use_runtime(rt), pytest.raises(InvalidStateError, match="not 'emit'"):
        actant.emit("MyAsk", "x")


def test_effect_dispatcher_perform_and_emit() -> None:
    rt = Runtime()
    rt.layer("MyPerform", kind="perform").chain(lambda x: f"performed:{x}")
    called: list[str] = []
    rt.layer("MyEmit", kind="emit").chain(lambda e: called.append(e))
    with actant.use_runtime(rt):
        assert actant.effect("MyPerform", "perform", "x") == "performed:x"
        assert actant.effect("MyEmit", "emit", "event") is None
    assert called == ["event"]


def test_effect_dispatcher() -> None:
    rt = Runtime()
    rt.layer("Routing", kind="ask").chain(_routing_ask)
    with actant.use_runtime(rt):
        assert actant.effect("Routing", "ask", "x") == "x?"


def test_effect_dispatcher_invalid_kind() -> None:
    with pytest.raises(ValueError, match="kind"):
        actant.effect("MyEffect", "bad", "x")  # type: ignore[arg-type]


def test_impossible_raises() -> None:
    with pytest.raises(actant.InternalError, match="impossible"):
        actant.impossible("unreachable")
