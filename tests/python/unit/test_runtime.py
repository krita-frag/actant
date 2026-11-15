from __future__ import annotations

import os
import shutil
import tempfile

import pytest

import actant
from actant import Runtime
from actant._internal.runtime import get_current_runtime, use_runtime
from actant._internal.tasks.async_result import AsyncResult
from actant.actant import _NetworkConfig
from actant.capabilities import TaskEvent
from actant.effects import ask
from actant.exceptions import ConfigError


def _endpoint_id() -> str:
    """生成一个语法合法的 iroh endpoint id（用自动临时目录的独立节点）。"""
    rt = Runtime()
    rt.start()
    try:
        return rt.peer_id
    finally:
        rt.stop()


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

    def test_default_node_id_is_endpoint_id(self):
        """未显式命名时 node_id 取持久身份 endpoint id，避免集群内重名。"""
        rt = Runtime()
        rt.start()
        try:
            assert rt.node_id == rt.peer_id
        finally:
            rt.stop()

    def test_explicit_name_is_kept(self):
        rt = Runtime(name="node-a")
        rt.start()
        try:
            assert rt.node_id == "node-a"
        finally:
            rt.stop()

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
        assert len(rt.handlers("Store")) == 0

    def test_chain_appends_handler(self):
        rt = Runtime()
        rt.layer("MyRouter", kind="ask").chain(lambda ctx: "node-a")
        assert len(rt.handlers("MyRouter")) == 1

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
        rt.layer("MyRouter", "ask").chain(lambda ctx: None)
        rt.layer("MyRouter").chain(lambda ctx: "low-priority")
        rt.layer("MyRouter").chain(lambda ctx: "high-priority")
        with rt:
            result = ask("MyRouter", {"task_name": "t", "local_node": "local"})
            assert result == "high-priority"

    def test_ask_returns_none_when_all_abstain_on_custom_capability(self):
        rt = Runtime()
        rt.layer("CustomAsk", "ask").chain(lambda ctx: None)
        with rt:
            assert ask("CustomAsk", "input") is None

    def test_ask_returns_none_when_python_only_capability_abstains(self):
        """自定义 ask capability 无 Rust fallback，handler 全弃权时返回 None。"""
        rt = Runtime()
        rt.layer("MyRouter", "ask").chain(lambda ctx: None)
        with rt:
            assert ask("MyRouter", {"task_name": "t", "local_node": "fallback"}) is None

    def test_default_constructor_does_not_register_policy_handlers(self):
        rt = Runtime()
        assert "Routing" not in rt.capabilities


class TestPerformSemantics:
    def test_perform_calls_last_handler(self):
        rt = Runtime()
        rt.layer("Codec", "perform").chain(lambda req: b"first")
        rt.chain("Codec", lambda req: b"last")
        with rt:
            assert actant.perform("Codec", {"op": "dump"}) == b"last"

    def test_perform_raises_when_no_handlers_on_custom_capability(self):
        rt = Runtime()
        rt.layer("CustomPerform", "perform")  # 注册 capability，但不附加 handler
        with pytest.raises(RuntimeError, match="has no handlers"), rt:
            actant.perform("CustomPerform", "x")


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
            ask("CustomPerform", "x")

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

    def test_temp_data_dir_cleaned_even_when_shutdown_fails(self):
        """shutdown 失败不得留下 Runtime 自建的临时 data_dir。

        清理是 stop() 的既定职责；失败路径正是最需要清理的场景（否则每次
        异常退出都在临时目录里留一份 LMDB）。
        """
        rt = Runtime()
        data_dir = rt._data_dir
        rt.start()

        class _Boom:
            def shutdown(self, _timeout_ms: int) -> None:
                raise RuntimeError("shutdown boom")

        rt._rust_core = _Boom()
        with pytest.raises(RuntimeError, match="shutdown boom"):
            rt.stop()

        assert not rt._started
        assert not os.path.exists(data_dir)


class TestRuntimeStartRollback:
    """start() 在 serve() 之后失败时必须整体回滚。"""

    def test_failed_serve_does_not_leave_started_state(self, monkeypatch):
        rt = Runtime()
        data_dir = rt._data_dir

        def boom(_self):
            raise RuntimeError("serve boom")

        monkeypatch.setattr(Runtime, "serve", boom)
        with pytest.raises(RuntimeError, match="serve boom"):
            rt.start()

        assert not rt._started, "启动失败不能留下 _started=True"
        assert rt._rust_core is None
        assert rt._event_batcher is None
        assert rt._terminal is None
        assert get_current_runtime() is None

        # 回滚后重新启动必须干净成功（后台线程不得残留）。
        monkeypatch.undo()
        rt.start()
        try:
            assert rt._started
        finally:
            rt.stop()
        assert not os.path.exists(data_dir)


class TestRuntimeProduction:
    """生产级 Runtime 工厂的契约测试。"""

    def test_production_rejects_empty_signing_key(self):
        with pytest.raises(ValueError, match="payload_signing_key"):
            Runtime.production(payload_signing_key="", data_dir="/tmp/unused")

    def test_production_rejects_empty_data_dir(self):
        with pytest.raises(ValueError, match="data_dir"):
            Runtime.production(payload_signing_key="secret", data_dir="")

    def test_production_rejects_empty_allowlist(self):
        """空 allowlist（开放成员身份）在生产语义下必须拒启。"""
        with pytest.raises(ValueError, match="allowed_peer_ids"):
            Runtime.production(
                payload_signing_key="cluster-shared-secret",
                data_dir=tempfile.mkdtemp(),
            )

    def test_production_enforces_require_payload_signing(self):
        """production() 必须设置 require_payload_signing=True。"""
        data_dir = tempfile.mkdtemp()
        try:
            rt = Runtime.production(
                payload_signing_key="cluster-shared-secret",
                data_dir=data_dir,
                network=_NetworkConfig(allowed_peer_ids=[_endpoint_id()]),
            )
            assert rt._config is not None
            assert rt._config.require_payload_signing is True
            assert rt._config.payload_signing_key == "cluster-shared-secret"
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_production_enforces_signed_heartbeats(self):
        """production() 必须强制心跳节点记录签名。"""
        data_dir = tempfile.mkdtemp()
        try:
            rt = Runtime.production(
                payload_signing_key="cluster-shared-secret",
                data_dir=data_dir,
                network=_NetworkConfig(allowed_peer_ids=[_endpoint_id()]),
            )
            assert rt._config is not None
            assert rt._config.network.require_signed_records is True
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_production_does_not_mutate_caller_network_config(self):
        """强制签名必须作用在副本上。

        同一份 ``_NetworkConfig`` 可能被多个 Runtime 共用；就地置位会把它永久
        改掉，后续以非生产方式启动的实例也会被带上强制签名。
        """
        data_dir = tempfile.mkdtemp()
        network = _NetworkConfig(allowed_peer_ids=[_endpoint_id()])
        assert network.require_signed_records is False
        try:
            rt = Runtime.production(
                payload_signing_key="cluster-shared-secret",
                data_dir=data_dir,
                network=network,
            )
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)

        assert network.require_signed_records is False, "调用方传入的配置被就地改写"
        assert rt._config.network.require_signed_records is True

    def test_production_rejects_full_endpoint_addr_in_allowlist(self):
        """白名单必须是 endpoint id；写成完整地址会永远匹配不上，启动即失败。"""
        data_dir = tempfile.mkdtemp()
        rt = Runtime.production(
            payload_signing_key="cluster-shared-secret",
            data_dir=data_dir,
            network=_NetworkConfig(allowed_peer_ids=["deadbeef"]),
        )
        try:
            with pytest.raises(ConfigError, match="allowed_peer_ids"):
                rt.start()
        finally:
            rt.stop()
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_production_rejects_custom_name(self):
        """生产模式要求 node_id == endpoint id：显式 name 启动即失败。"""
        data_dir = tempfile.mkdtemp()
        rt = Runtime.production(
            payload_signing_key="cluster-shared-secret",
            data_dir=data_dir,
            name="node-a",
            network=_NetworkConfig(allowed_peer_ids=[_endpoint_id()]),
        )
        try:
            with pytest.raises(ConfigError, match="require_signed_records"):
                rt.start()
        finally:
            rt.stop()
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_production_persists_identity_key(self):
        """节点身份密钥持久化于 data_dir/identity.key，endpoint id 跨重启稳定。"""
        data_dir = tempfile.mkdtemp()
        try:
            with Runtime.production(
                payload_signing_key="cluster-shared-secret",
                data_dir=data_dir,
                network=_NetworkConfig(allowed_peer_ids=[_endpoint_id()]),
            ) as rt:
                assert rt._started
                identity_path = os.path.join(data_dir, "identity.key")
                assert os.path.exists(identity_path)
                peer_id_first = rt.peer_id
            with Runtime.production(
                payload_signing_key="cluster-shared-secret",
                data_dir=data_dir,
                network=_NetworkConfig(allowed_peer_ids=[_endpoint_id()]),
            ) as rt2:
                # 同一 data_dir 重启 → 同一 endpoint id（白名单因此可维护）
                assert rt2.peer_id == peer_id_first
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_production_starts_and_stops_cleanly(self):
        """production() 创建的 Runtime 应能正常 start/stop。"""
        data_dir = tempfile.mkdtemp()
        try:
            with Runtime.production(
                payload_signing_key="cluster-shared-secret",
                data_dir=data_dir,
                network=_NetworkConfig(allowed_peer_ids=[_endpoint_id()]),
            ) as rt:
                assert rt._started
                assert get_current_runtime() is rt
            assert get_current_runtime() is None
            # 用户显式 data_dir 不应被清理
            assert os.path.exists(data_dir)
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)


class TestNetworkPresetSelection:
    """网络 preset 选择：仅内部自动生成的临时 data_dir 才禁用 P2P。"""

    def test_auto_data_dir_uses_none_preset(self):
        rt = Runtime()
        assert rt._owns_data_dir is True
        assert rt._config is not None
        assert rt._config.network.preset == "none"

    def test_explicit_temp_data_dir_keeps_local_preset(self, tmp_path):
        """显式传入系统临时目录下的 data_dir 不再被嗅探改写为 preset="none"。"""
        data_dir = tmp_path / "actant-data"
        rt = Runtime(data_dir=str(data_dir))
        assert rt._owns_data_dir is False
        assert rt._config is not None
        assert rt._config.network.preset == "local"

    def test_explicit_data_dir_outside_tempdir_keeps_local_preset(self, tmp_path):
        rt = Runtime(data_dir=str(tmp_path / "prod-data"))
        assert rt._config is not None
        assert rt._config.network.preset == "local"


class TestCancelTask:
    """``Runtime.cancel_task`` 与 ``AsyncResult.cancel`` 共用同一份取消协议。"""

    def test_cancel_task_logs_warning_when_rust_cancel_fails(self, caplog):
        import logging

        class _BoomCore:
            def cancel_task(self, task_id: str) -> None:
                raise RuntimeError("rust cancel exploded")

        rt = Runtime()
        rt._rust_core = _BoomCore()
        handle = AsyncResult("t-1")
        rt._tasks["t-1"] = handle
        with caplog.at_level(logging.WARNING, logger="actant.runtime"):
            assert rt.cancel_task("t-1") is True
        assert any(
            "cancel_task failed for" in rec.message for rec in caplog.records
        )
        # 取消失败不阻断其余步骤：预取消标记与占位终态照常生效。
        assert rt._is_cancelled("t-1")
        assert handle.state == "cancelled"

    def test_cancel_task_on_flow_task_broadcasts_over_p2p(self):
        """按 id 取消 flow 任务必须广播 P2P，否则远端 Worker 会继续执行。"""

        class _RecordingCore:
            def __init__(self) -> None:
                self.cancelled: list[str] = []
                self.broadcast: list[tuple[str, str]] = []

            def cancel_task(self, task_id: str) -> None:
                self.cancelled.append(task_id)

            def broadcast_cancel(self, task_id: str, workflow_id: str) -> None:
                self.broadcast.append((task_id, workflow_id))

        rt = Runtime()
        core = _RecordingCore()
        rt._rust_core = core
        rt._tasks["t-1"] = AsyncResult("t-1", workflow_id="wf-1")
        assert rt.cancel_task("t-1") is True
        assert core.cancelled == ["t-1"]
        assert core.broadcast == [("t-1", "wf-1")]

    def test_cancel_task_unknown_id_returns_false(self):
        """不存在的任务不触碰 Rust：没有句柄就没有取消语义。"""
        rt = Runtime()
        assert rt.cancel_task("missing") is False
        assert not rt._is_cancelled("missing")


def test_stop_waits_for_auto_resume_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    """``stop()`` 必须等后台 auto_resume 重放结束再释放 Rust core。

    重放会执行用户 flow 代码并重新派发任务；若 stop() 不 join，线程仍持有
    Runtime 引用，可能在 core 关停后继续提交任务。
    """
    import time

    rt = Runtime()

    def _slow_host_flows(*args: object, **kwargs: object) -> list[object]:
        time.sleep(0.4)
        return []

    monkeypatch.setattr(rt, "host_flows", _slow_host_flows)
    rt.start(auto_resume=True)
    thread = rt._auto_resume_thread
    assert thread is not None
    # 确认线程已进入重放（避免 stop 抢在线程启动前返回造成的假绿）。
    deadline = time.monotonic() + 2.0
    while not thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)
    rt.stop()
    assert not thread.is_alive(), (
        "stop() 返回后 auto_resume 线程仍在跑，可能访问已释放的 Rust core"
    )
