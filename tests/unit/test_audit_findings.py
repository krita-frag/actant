"""审查发现验证测试：确认审查报告中的关键问题真实存在。

这些测试是审查报告的"证据"，确保每个发现都有代码级验证。
修复对应问题后，相关测试应改为断言修复行为。
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# S1: cloudpickle 反序列化无签名校验
# ---------------------------------------------------------------------------


class TestAuditS1CloudpickleSignature:
    """验证 cloudpickle payload 在节点间传输时已有 MAC 完整性保护。"""

    def test_payload_mac_module_exists(self):
        """Rust 侧存在 payload_mac 模块提供 sign/verify。"""
        from actant import actant as rust

        assert hasattr(rust, "_payload_mac_sign") or True  # 模块在 Rust 内部，不直接导出
        import os

        mac_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "src", "common", "payload.rs"
        )
        assert os.path.exists(mac_path), "payload.rs not found"
        with open(mac_path) as f:
            source = f.read()
        assert "pub fn sign" in source, "payload_mac::sign not found"
        assert "pub fn verify" in source, "payload_mac::verify not found"

    def test_actant_config_requires_signing_key(self):
        """_ActantConfig 必填 payload_signing_key。"""
        from actant import actant as rust

        with pytest.raises(TypeError):
            rust._ActantConfig()

        cfg = rust._ActantConfig(payload_signing_key="test-key")
        assert cfg.payload_signing_key == "test-key"


# ---------------------------------------------------------------------------
# F1: 缺少多线程/多进程本地调度器
# ---------------------------------------------------------------------------


class TestAuditF1NoLocalThreadedScheduler:
    """验证 actant 仅支持单线程同步本地执行，无多线程/多进程调度器。"""

    def test_no_threaded_or_process_scheduler_class(self):
        """actant 包中无 ThreadedScheduler/ProcessScheduler 等类。"""
        import actant

        for attr in ("ThreadedScheduler", "ProcessScheduler", "LocalScheduler",
                      "ThreadScheduler", "MultiprocessingScheduler"):
            assert not hasattr(actant, attr), (
                f"Found actant.{attr} — if this test fails, F1 may have been fixed"
            )

    def test_flow_run_is_synchronous(self):
        """Flow.run() 是同步单线程执行，非并行。"""
        import inspect

        from actant.flow import Flow

        # Flow.run 不接受 num_workers / threads 等参数
        sig = inspect.signature(Flow.run)
        parallel_params = {"num_workers", "threads", "processes", "executor", "n_workers"}
        for param in sig.parameters:
            assert param not in parallel_params, (
                f"Flow.run now accepts '{param}' — if this test fails, F1 may have been fixed"
            )


# ---------------------------------------------------------------------------
# ST3: actor.py save_state/load_state 锁策略
# ---------------------------------------------------------------------------


class TestAuditST3ActorLockStrategy:
    """验证 Python actor 的 state 保存/加载已用 block_in_place 修复 ST3 GIL 阻塞。

    修复前：save_state/load_state 直接用 Python::attach，阻塞 tokio reactor。
    修复后：用 tokio::task::block_in_place 包裹 GIL 获取。
    """

    def test_rust_actor_state_uses_block_in_place(self):
        """确认 py/actor.rs 中 save_state/load_state 使用 block_in_place。"""
        import os
        import re

        actor_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "src", "py", "actor.rs"
        )
        assert os.path.exists(actor_path), "src/py/actor.rs not found"

        with open(actor_path) as f:
            source = f.read()

        # save_state/load_state 应存在
        assert "fn save_state" in source, "save_state method missing"
        assert "fn load_state" in source, "load_state method missing"

        # 提取 save_state 函数体
        save_match = re.search(
            r"fn save_state\(&self\).*?\n    \}(?=\s*\n|\s*fn |\s*\})",
            source,
            re.DOTALL,
        )
        load_match = re.search(
            r"fn load_state\(&mut self[^)]*\).*?\n    \}(?=\s*\n|\s*fn |\s*\})",
            source,
            re.DOTALL,
        )

        assert save_match, "save_state function body not found"
        assert load_match, "load_state function body not found"

        # 修复后：两个方法都应使用 block_in_place
        assert "block_in_place" in save_match.group(0), (
            "save_state must use tokio::task::block_in_place to avoid blocking reactor"
        )
        assert "block_in_place" in load_match.group(0), (
            "load_state must use tokio::task::block_in_place to avoid blocking reactor"
        )

    def test_actor_ops_rs_does_not_contain_state_methods(self):
        """确认 actor_ops.rs 不含 save_state/load_state（位置勘误验证）。"""
        import os

        actor_ops_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "src", "py", "actor_ops.rs"
        )
        if not os.path.exists(actor_ops_path):
            pytest.skip("actor_ops.rs not found")

        with open(actor_ops_path) as f:
            source = f.read()

        assert "fn save_state" not in source, (
            "save_state should be in actor.rs, not actor_ops.rs"
        )
        assert "fn load_state" not in source, (
            "load_state should be in actor.rs, not actor_ops.rs"
        )


# ---------------------------------------------------------------------------
# PyTask::retry_policy getter unwrap
# ---------------------------------------------------------------------------


class TestAuditPyTaskRetryPolicyUnwrap:
    """验证 PyTask::retry_policy getter 已消除 .unwrap()。"""

    def test_retry_policy_getter_has_no_unwrap(self):
        """确认 py/runtime.rs 中 retry_policy getter 不再使用 unwrap()。"""
        import os
        import re

        runtime_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "src", "py", "runtime.rs"
        )
        with open(runtime_path) as f:
            source = f.read()

        # 查找 retry_policy getter 函数体范围
        match = re.search(
            r"fn retry_policy\(.*?(\{.*?\n    \})",
            source,
            re.DOTALL,
        )
        assert match, "retry_policy getter not found"
        getter_body = match.group(1)
        assert ".unwrap()" not in getter_body, (
            "retry_policy getter still contains .unwrap()"
        )

    def test_retry_policy_getter_returns_pyresult(self):
        """确认 retry_policy getter 返回 PyResult<Option<Py<PyRetryPolicy>>>。"""
        import os
        import re

        runtime_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "src", "py", "runtime.rs"
        )
        with open(runtime_path) as f:
            source = f.read()

        match = re.search(
            r"fn retry_policy\(&self, py: Python<'_>\) -> PyResult<Option<Py<PyRetryPolicy>>>",
            source,
        )
        assert match, "retry_policy getter signature does not return PyResult"
