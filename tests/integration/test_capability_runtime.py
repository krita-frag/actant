"""集成测试：Rust CapabilityRuntime 经 PyO3 暴露给 Python。

这些测试直接操作 `actant.actant._CapabilityRuntime`，验证：
1. 内置 capability 元数据可见（与 Rust `builtin_capabilities()` 一致）。
2. `_CapabilityRuntime()` 无参构造时不注册任何 handler——`register_defaults`
   只注册 codec，具体 handler 由 `RuntimeBuilder` 在 `_RuntimeCore` 中注入。
3. 不支持的 capability 会抛出清晰错误（`Routing`/`Scheduling`/`RetryPolicy`
   不在 Rust `capability_registry!` 中，是纯 Python 策略）。

它们不依赖 Python `Runtime` 的 handler 注册表，因此是 Rust-Python
边界的端到端测试。
"""

from __future__ import annotations

import pytest

from actant.actant import _CapabilityRuntime
from actant.capabilities import RouteCtx, SerializationReq, TaskEvent


@pytest.fixture
def cap_rt() -> _CapabilityRuntime:
    """每个测试使用独立的 Rust CapabilityRuntime（无 handler 注册）。"""
    return _CapabilityRuntime()


class TestCapabilityMetadata:
    """Rust `builtin_capabilities()` 返回 10 个 capability（不含策略型）。"""

    def test_builtin_capabilities_excludes_python_only(self, cap_rt: _CapabilityRuntime) -> None:
        """Rust 不暴露 Routing/Scheduling/RetryPolicy——它们是纯 Python 策略。"""
        names = {name for name, _ in cap_rt.builtin_capabilities()}
        # Rust 暴露的 10 个 capability
        expected = {
            "Serialization",
            "Transport",
            "Store",
            "Execute",
            "ActorMessaging",
            "ActorSupervision",
            "TaskLifecycle",
            "WorkflowLifecycle",
            "NodeLifecycle",
            "ActorLifecycle",
        }
        assert names == expected
        # 策略型不在 Rust 暴露集合中
        assert "Routing" not in names
        assert "Scheduling" not in names
        assert "RetryPolicy" not in names

    def test_capability_count_zero_without_runtime_core(self, cap_rt: _CapabilityRuntime) -> None:
        """`_CapabilityRuntime()` 无参构造注册 codec 与空 layer。

        `register_defaults` 调用 `register_codec` 与 `ensure_layer`，使所有内置
        capability 都有 layer entry（无 handler），确保 `bind_actor_system` 能为
        它们 spawn CapabilityActor。因此 count 等于内置 capability 数量。
        """
        assert cap_rt.capability_count == 10

    def test_registered_capabilities_empty_without_handlers(
        self, cap_rt: _CapabilityRuntime
    ) -> None:
        """无 handler 注册时，registered_capabilities 返回所有内置 capability 名称。"""
        names = set(cap_rt.registered_capabilities())
        assert "Serialization" in names
        assert "TaskLifecycle" in names


class TestAskDispatch:
    """Rust 仅支持 ActorSupervision ask；Routing/Scheduling 不支持。"""

    def test_routing_not_supported_by_rust(self, cap_rt: _CapabilityRuntime) -> None:
        """Routing 是纯 Python 策略，Rust 抛 ValueError。"""
        ctx = RouteCtx(task_name="task-1", local_node="node-a")
        with pytest.raises(ValueError, match="ask capability Routing not supported"):
            cap_rt.ask("Routing", ctx)

    def test_unsupported_ask_raises(self, cap_rt: _CapabilityRuntime) -> None:
        with pytest.raises(ValueError, match="ask capability Unknown not supported"):
            cap_rt.ask("Unknown", RouteCtx(task_name="task-1"))


class TestPerformDispatch:
    """Serialization perform 在无 handler 时返回错误（capability 未绑定到 actor system）。

    `_CapabilityRuntime()` 无参构造不经过 `RuntimeBuilder`，因此 Store/Execute 等
    handler 未注册。perform 调用会返回 "not bound to actor system" 错误。
    """

    def test_serialization_perform_without_handler_raises(self, cap_rt: _CapabilityRuntime) -> None:
        """无 handler 时 perform 抛 RuntimeError（内部错误）。"""
        req = SerializationReq(op="dump", data=b"payload")
        with pytest.raises(RuntimeError, match=r"not bound|no handler|internal"):
            cap_rt.perform("Serialization", req)

    def test_unsupported_perform_raises(self, cap_rt: _CapabilityRuntime) -> None:
        with pytest.raises(ValueError, match="perform capability Unknown not supported"):
            cap_rt.perform("Unknown", SerializationReq(op="dump", data=b"x"))


class TestEmitDispatch:
    def test_task_lifecycle_emit_without_handler_raises(
        self, cap_rt: _CapabilityRuntime
    ) -> None:
        """无 handler 时 emit 抛 RuntimeError（内部错误）。"""
        event = TaskEvent(kind="started", task_id="task-1", workflow_id="wf-1")
        with pytest.raises(RuntimeError, match=r"not bound|no handler|internal"):
            cap_rt.emit("TaskLifecycle", event)

    def test_unsupported_emit_raises(self, cap_rt: _CapabilityRuntime) -> None:
        with pytest.raises(ValueError, match="emit capability Unknown not supported"):
            cap_rt.emit("Unknown", TaskEvent(kind="started", task_id="task-1"))
