"""集成测试：Python Runtime 与 Rust CapabilityRuntime 的回退语义。

验证统一运行时的核心约定（按 AGENTS.md 分层）：
- `Routing` / `Scheduling` / `RetryPolicy`：纯 Python 策略，无 Rust fallback。
  `Runtime()` 无 handler 时 ask 返回 None；`Runtime.with_defaults()` 注册 Python 默认 handler。
- `Serialization` 等 Rust-backed capability：Python handler 缺失时回退到 Rust 内置 handler。
- 自定义 capability 没有 Rust 实现，仍按原语义报错/返回 None。
"""

from __future__ import annotations

import pytest

import actant
from actant import Runtime
from actant.capabilities import (
    RouteCtx,
    ScheduleCtx,
    SerializationReq,
    TaskEvent,
)


class TestPythonOnlyAskNoFallback:
    """Routing / Scheduling / RetryPolicy 是纯 Python 策略，无 Rust fallback。"""

    def test_routing_ask_returns_none_without_handler(self) -> None:
        rt = Runtime()
        with rt:
            result = actant.ask("Routing", RouteCtx(task_name="t", local_node="n"))
        assert result is None

    def test_python_handler_provides_routing(self) -> None:
        rt = Runtime()
        rt.layer("Routing").chain(lambda ctx: "python-node")
        with rt:
            result = actant.ask("Routing", RouteCtx(task_name="t", local_node="rust-node"))
        assert result == "python-node"

    def test_abstain_then_none_without_defaults(self) -> None:
        rt = Runtime()
        rt.layer("Routing").chain(lambda ctx: None)
        with rt:
            result = actant.ask("Routing", RouteCtx(task_name="t", local_node="n"))
        assert result is None

    def test_scheduling_ask_returns_none_without_handler(self) -> None:
        rt = Runtime()
        with rt:
            result = actant.ask(
                "Scheduling",
                ScheduleCtx(workflow_id="wf", pending=["a", "b"]),
            )
        assert result is None


class TestWithDefaultsPythonHandlers:
    """Runtime.with_defaults() 注册 Python 策略层默认 handler。"""

    def test_routing_default_returns_local_node_without_peers(self) -> None:
        with Runtime.with_defaults():
            result = actant.ask("Routing", RouteCtx(task_name="t", local_node="me"))
        assert result == "me"

    def test_routing_default_round_robin_with_peers(self) -> None:
        with Runtime.with_defaults():
            ctx = RouteCtx(task_name="t", local_node="me", peers=["p1", "p2"])
            result = actant.ask("Routing", ctx)
        assert result in ("p1", "p2")

    def test_scheduling_default_returns_first_pending(self) -> None:
        with Runtime.with_defaults():
            result = actant.ask(
                "Scheduling",
                ScheduleCtx(workflow_id="wf", pending=["a", "b"]),
            )
        assert result == "a"

    def test_custom_handler_overrides_default(self) -> None:
        rt = Runtime.with_defaults()
        rt.layer("Routing").chain(lambda ctx: "override")
        with rt:
            result = actant.ask("Routing", RouteCtx(task_name="t", local_node="me"))
        assert result == "override"


class TestPerformFallback:
    """Serialization 等 Rust-backed capability 回退到 Rust。"""

    def test_serialization_perform_falls_back_to_rust(self) -> None:
        rt = Runtime()
        with rt:
            req = SerializationReq(op="dump", data=b"rust-payload")
            result = actant.perform("Serialization", req)
        assert result == b"rust-payload"

    def test_python_handler_overrides_rust_perform(self) -> None:
        rt = Runtime()
        rt.layer("Serialization").chain(lambda req: b"python-payload")
        with rt:
            req = SerializationReq(op="dump", data=b"rust-payload")
            result = actant.perform("Serialization", req)
        assert result == b"python-payload"


class TestEmitFallback:
    def test_task_lifecycle_emit_falls_back_to_rust(self) -> None:
        rt = Runtime()
        with rt:
            actant.emit(
                "TaskLifecycle",
                TaskEvent(kind="started", task_id="t", workflow_id="wf"),
            )


class TestCustomCapabilityNoFallback:
    def test_custom_perform_still_raises_without_handler(self) -> None:
        rt = Runtime()
        rt.layer("CustomPerform", "perform")
        with pytest.raises(RuntimeError, match="has no handlers"), rt:
            actant.perform("CustomPerform", "x")
