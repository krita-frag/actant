from __future__ import annotations

import os
import shutil
import tempfile

import pytest

import actant
from actant import Runtime
from actant._runtime import get_current_runtime, use_runtime
from actant.capabilities import RouteCtx, SerializationReq, TaskEvent


class TestRuntimeLifecycle:
    def test_runtime_starts_and_stops(self):
        rt = Runtime()
        assert not rt._started
        rt.start()
        assert rt._started
        assert get_current_runtime() is rt
        rt.stop()
        assert not rt._started
        assert get_current_runtime() is None

    def test_runtime_context_manager(self):
        with Runtime() as rt:
            assert get_current_runtime() is rt
        assert get_current_runtime() is None

    def test_use_runtime_restores_previous(self):
        rt1 = Runtime().start()
        rt2 = Runtime()
        with use_runtime(rt2):
            assert get_current_runtime() is rt2
        assert get_current_runtime() is rt1
        rt1.stop()

    def test_runtime_accepts_worker_convenience_params(self):
        """验证 worker 便捷参数能构造 _ActantConfig。"""
        rt = Runtime(
            max_concurrent_tasks=4,
            default_task_timeout_ms=60000,
            drain_timeout_secs=60,
            remote_fallback_delay_ms=1000,
            scheduler="fifo",
        )
        assert rt._config is not None
        assert rt._config.max_concurrent_tasks == 4
        assert rt._config.default_task_timeout_ms == 60000
        assert rt._config.drain_timeout_secs == 60
        assert rt._config.remote_fallback_delay_ms == 1000
        assert rt._config.scheduler == "fifo"

    def test_runtime_config_param_takes_precedence(self):
        """提供 config 时便捷参数被忽略。"""
        from actant.actant import _ActantConfig

        cfg = _ActantConfig(payload_signing_key="", max_concurrent_tasks=8)
        rt = Runtime(config=cfg, max_concurrent_tasks=2)
        assert rt._config is cfg
        assert rt._config.max_concurrent_tasks == 8

    def test_runtime_no_params_uses_test_config(self):
        rt = Runtime()
        # 无参数时 _build_config 仍会构造一个 _ActantConfig，临时 data_dir
        # 使用 preset="none" 避免测试受 P2P 发现抖动影响。
        assert rt._config is not None
        assert rt._config.network.preset == "none"


class TestLayerRegistration:
    def test_builtin_layer_empty_by_default(self):
        rt = Runtime()
        assert rt.handler_count("Routing") == 0

    def test_chain_appends_handler(self):
        rt = Runtime()
        rt.layer("Routing").chain(lambda ctx: "node-a")
        assert rt.handler_count("Routing") == 1

    def test_custom_capability_requires_kind(self):
        rt = Runtime()
        with pytest.raises(ValueError, match="requires explicit kind"):
            rt.layer("MyCap")

    def test_custom_capability_register_and_call(self):
        rt = Runtime()
        rt.layer("MyCap", "perform").chain(lambda req: f"handled:{req}")
        with rt:
            assert actant.perform("MyCap", "input") == "handled:input"


class TestAskSemantics:
    def test_ask_returns_first_non_none_in_reverse_order(self):
        rt = Runtime()
        rt.layer("Routing").chain(lambda ctx: None)
        rt.layer("Routing").chain(lambda ctx: "low-priority")
        rt.layer("Routing").chain(lambda ctx: "high-priority")
        with rt:
            result = actant.ask("Routing", RouteCtx(task_name="t", local_node="local"))
            assert result == "high-priority"

    def test_ask_returns_none_when_all_abstain_on_custom_capability(self):
        rt = Runtime()
        rt.layer("CustomAsk", "ask").chain(lambda ctx: None)
        with rt:
            assert actant.ask("CustomAsk", "input") is None

    def test_ask_returns_none_when_python_only_capability_abstains(self):
        """Routing 是纯 Python 策略，无 Rust fallback，handler 全弃权时返回 None。"""
        rt = Runtime()
        rt.layer("Routing").chain(lambda ctx: None)
        with rt:
            assert actant.ask("Routing", RouteCtx(task_name="t", local_node="fallback")) is None

    def test_builtin_routing_default_local_node(self):
        rt = Runtime.with_defaults()
        with rt:
            assert actant.ask("Routing", RouteCtx(task_name="t", local_node="me")) == "me"


class TestPerformSemantics:
    def test_perform_calls_last_handler(self):
        rt = Runtime()
        rt.layer("Serialization").chain(lambda req: b"first")
        rt.layer("Serialization").chain(lambda req: b"last")
        with rt:
            assert actant.perform("Serialization", SerializationReq(op="dump", data=b"x")) == b"last"

    def test_perform_raises_when_no_handlers_on_custom_capability(self):
        rt = Runtime()
        rt.layer("CustomPerform", "perform")  # 注册 capability，但不附加 handler
        with pytest.raises(RuntimeError, match="has no handlers"), rt:
            actant.perform("CustomPerform", "x")

    def test_perform_falls_back_to_rust_when_no_python_handlers(self):
        rt = Runtime()
        with rt:
            req = SerializationReq(op="dump", data=b"payload")
            assert actant.perform("Serialization", req) == b"payload"

    def test_builtin_serialization_passthrough(self):
        rt = Runtime.with_defaults()
        with rt:
            req = SerializationReq(op="dump", data=b"payload")
            assert actant.perform("Serialization", req) == b"payload"


class TestEmitSemantics:
    def test_emit_calls_all_handlers(self):
        rt = Runtime()
        called: list = []
        rt.layer("TaskLifecycle").chain(lambda e: called.append(1))
        rt.layer("TaskLifecycle").chain(lambda e: called.append(2))
        with rt:
            actant.emit("TaskLifecycle", TaskEvent(kind="started", task_id="t"))
        assert called == [1, 2]

    def test_emit_continues_after_handler_failure(self):
        rt = Runtime()
        called: list = []
        rt.layer("TaskLifecycle").chain(lambda e: (_ for _ in ()).throw(RuntimeError("boom")))
        rt.layer("TaskLifecycle").chain(lambda e: called.append("ok"))
        with rt:
            actant.emit("TaskLifecycle", TaskEvent(kind="started", task_id="t"))
        assert called == ["ok"]


class TestEffectKindValidation:
    def test_ask_rejects_non_ask_capability(self):
        rt = Runtime()
        rt.layer("CustomPerform", "perform").chain(lambda x: x)
        with pytest.raises(RuntimeError, match="not 'ask'"), rt:
            actant.ask("CustomPerform", "x")

    def test_perform_rejects_non_perform_capability(self):
        rt = Runtime()
        rt.layer("CustomAsk", "ask").chain(lambda x: x)
        with pytest.raises(RuntimeError, match="not 'perform'"), rt:
            actant.perform("CustomAsk", "x")


class TestRuntimeCleanup:
    def test_stop_is_idempotent_for_temp_data_dir(self):
        """重复 stop() 不应因临时 data_dir 已清理而报错。"""
        rt = Runtime()
        data_dir = rt._data_dir
        assert data_dir is not None
        rt.start()
        rt.stop()
        assert not rt._started
        assert not os.path.exists(data_dir)
        rt.stop()  # 幂等，不应抛 FileNotFoundError
        assert not os.path.exists(data_dir)

    def test_user_data_dir_not_cleaned(self):
        """用户显式传入的 data_dir 不应被 Runtime 清理。"""
        data_dir = tempfile.mkdtemp()
        try:
            rt = Runtime(data_dir=data_dir)
            rt.start()
            rt.stop()
            assert os.path.exists(data_dir)
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)
