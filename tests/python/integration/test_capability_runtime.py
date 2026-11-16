"""集成测试：Rust CapabilityRuntime 经 PyO3 暴露给 Python。

这些测试直接操作 `actant.actant._CapabilityRuntime`，验证：
1. 内置 capability 元数据可见（与 Rust `builtin_capabilities()` 一致）。
2. `_CapabilityRuntime()` 无参构造时不注册任何 handler——`register_defaults`
   只注册 codec，具体 handler 由 `RuntimeBuilder` 在 `_RuntimeCore` 中注入。
3. Rust 桥不设 ask 分发入口（ask 是纯 Python 决策语义，见
   `TestAskDispatch`）。

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
    """Rust 桥不设 ask 分发入口（设计决定，K12 确认）。

    ask 是纯 Python 决策语义：Python 侧 `_ask_chain` handler 链逆序决策，
    Rust 无 ask 型内置 handler，`CapabilityRuntime` 的 Rust ask 执行链已随
    K12 删除。此负向契约防止将来给桥加 ask 入口而不改 Python 侧——那会
    造成同一 capability 两条决策路径漂移。
    """

    def test_bridge_has_no_ask_entry(self, cap_rt: _CapabilityRuntime) -> None:
        assert not hasattr(cap_rt, "ask")

    def test_python_ask_does_not_reach_rust_bridge(self, cap_rt: _CapabilityRuntime) -> None:
        # Rust 桥只有 perform / emit 入口（与 capability_registry! 宏一致）。
        assert hasattr(cap_rt, "perform")
        assert hasattr(cap_rt, "perform_async")
        assert hasattr(cap_rt, "perform_batch_async")
        assert hasattr(cap_rt, "emit")
        assert not hasattr(cap_rt, "ask")


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
