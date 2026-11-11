"""集成测试：Rust CapabilityRuntime 经 PyO3 暴露给 Python。

这些测试直接操作 `actant.actant._CapabilityRuntime`，验证：
1. 内置 capability 元数据可见（与 Rust `builtin_capabilities()` 一致）。
2. `_CapabilityRuntime()` 无参构造时不注册任何 handler——`register_defaults`
   只注册 codec，具体 handler 由 `RuntimeBuilder` 在 `_RuntimeCore` 中注入。
3. 不支持的 capability 会抛出清晰错误（自定义 ask capability 不在 Rust
   `capability_registry!` 中）。

它们不依赖 Python `Runtime` 的 handler 注册表，因此是 Rust-Python
边界的端到端测试。
"""

from __future__ import annotations

import pytest

from actant.actant import _CapabilityRuntime
from actant.capabilities import StoreReq, TaskEvent


@pytest.fixture
def cap_rt() -> _CapabilityRuntime:
    """每个测试使用独立的 Rust CapabilityRuntime（无 handler 注册）。"""
    return _CapabilityRuntime()


class TestCapabilityMetadata:
    """Rust `builtin_capabilities()` 只暴露引擎接线的数据面/观测面 capability。"""

    def test_builtin_capabilities_excludes_python_only(self, cap_rt: _CapabilityRuntime) -> None:
        names = {name for name, _ in cap_rt.builtin_capabilities()}
        expected = {
            "Store",
            "TaskLifecycle",
            "WorkflowLifecycle",
        }
        assert names == expected
        # Python-only 与放置类策略都不在 Rust 暴露集合中
        assert "ValueStore" not in names
        assert "Routing" not in names
        assert "Scheduling" not in names
        assert "RetryPolicy" not in names

    def test_registered_capabilities_without_runtime_core(
        self, cap_rt: _CapabilityRuntime
    ) -> None:
        """`register_defaults` 为每个内置 capability 建 layer entry（无 handler）。

        无参构造不经过 `RuntimeBuilder`，因此 `StoreHandler` 未注册，且未绑定
        ActorSystem；注册集合等于内置 capability 集合。
        """
        assert len(cap_rt.registered_capabilities()) == 3

    def test_registered_capabilities_lists_builtins(
        self, cap_rt: _CapabilityRuntime
    ) -> None:
        names = set(cap_rt.registered_capabilities())
        assert names == {"Store", "TaskLifecycle", "WorkflowLifecycle"}


class TestAskDispatch:
    """Rust 不支持任何 ask capability（ask 注册表为空）。"""

    def test_custom_ask_not_supported_by_rust(self, cap_rt: _CapabilityRuntime) -> None:
        with pytest.raises(ValueError, match="ask capability MyRouter not supported"):
            cap_rt.ask("MyRouter", {"task_name": "task-1"})

    def test_unsupported_ask_raises(self, cap_rt: _CapabilityRuntime) -> None:
        with pytest.raises(ValueError, match="ask capability Unknown not supported"):
            cap_rt.ask("Unknown", {"task_name": "task-1"})


class TestPerformDispatch:
    """Store perform 在未绑定 ActorSystem 时返回清晰错误。"""

    def test_store_perform_without_binding_raises(self, cap_rt: _CapabilityRuntime) -> None:
        with pytest.raises(RuntimeError, match=r"not bound|no handler|internal"):
            cap_rt.perform("Store", StoreReq(op="get", key=b"k"))

    def test_unsupported_perform_raises(self, cap_rt: _CapabilityRuntime) -> None:
        with pytest.raises(ValueError, match="perform capability Unknown not supported"):
            cap_rt.perform("Unknown", StoreReq(op="get", key=b"k"))


class TestEmitDispatch:
    def test_task_lifecycle_emit_without_binding_raises(
        self, cap_rt: _CapabilityRuntime
    ) -> None:
        event = TaskEvent(kind="started", task_id="task-1", workflow_id="wf-1")
        with pytest.raises(RuntimeError, match=r"not bound|no handler|internal"):
            cap_rt.emit("TaskLifecycle", event)

    def test_unsupported_emit_raises(self, cap_rt: _CapabilityRuntime) -> None:
        with pytest.raises(ValueError, match="emit capability Unknown not supported"):
            cap_rt.emit("Unknown", TaskEvent(kind="started", task_id="task-1"))
