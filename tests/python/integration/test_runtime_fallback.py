"""集成测试：Python Runtime 与 Rust CapabilityRuntime 的回退语义。

验证统一运行时的核心约定（按 AGENTS.md 分层）：

- 自定义 capability 没有 Rust 实现：无 handler 时 ``ask`` 返回 ``None``、
  ``perform`` 抛错。
- ``Store`` 是唯一有 Rust 内置 handler 的 perform capability：Python handler
  缺失时回退到 ``StoreHandler``，注册后由 Python handler 接管。
- 放置类策略（路由/调度/重试）由 Rust ``RoutePolicy`` / ``Scheduler`` 或第 3 层
  外部调度器承担，``Runtime()`` 不在 Python 层注册它们。
"""

from __future__ import annotations

import pytest

import actant
from actant import Runtime
from actant.capabilities import StoreReq, TaskEvent
from actant.effects import ask


class TestCustomAskNoFallback:
    """自定义 ask capability 无 Rust 实现。"""

    def test_custom_ask_returns_none_without_handler(self) -> None:
        rt = Runtime()
        rt.layer("MyRouter", "ask")
        with rt:
            result = ask("MyRouter", {"task_name": "t"})
        assert result is None

    def test_python_handler_provides_decision(self) -> None:
        rt = Runtime()
        rt.layer("MyRouter", "ask").chain(lambda ctx: "python-node")
        with rt:
            result = ask("MyRouter", {"task_name": "t"})
        assert result == "python-node"

    def test_abstain_then_none(self) -> None:
        rt = Runtime()
        rt.layer("MyRouter", "ask").chain(lambda ctx: None)
        with rt:
            result = ask("MyRouter", {"task_name": "t"})
        assert result is None


class TestWithDefaultsRegistersNoPolicyHandlers:
    """``Runtime()`` 不注册 Python 策略层 handler。"""

    def test_no_routing_scheduling_retry_handlers(self) -> None:
        with Runtime() as rt:
            for name in ("Routing", "Scheduling", "RetryPolicy"):
                assert name not in rt._layers


class TestPerformFallback:
    """Rust-backed ``Store`` 的回退与覆盖语义。"""

    def test_store_perform_without_python_handler_uses_rust(self) -> None:
        rt = Runtime()
        with rt:
            actant.perform("Store", StoreReq(op="put", key=b"k", value=b"v"))
            assert actant.perform("Store", StoreReq(op="get", key=b"k")) == b"v"

    def test_python_handler_overrides_rust_perform(self) -> None:
        rt = Runtime()
        rt.layer("Store").chain(lambda req: b"python-payload")
        with rt:
            result = actant.perform("Store", StoreReq(op="get", key=b"k"))
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
