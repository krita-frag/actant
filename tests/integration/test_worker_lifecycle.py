"""Worker 生命周期集成测试：serve() → stop() 全流程。

验证：
1. Runtime.start() + serve() 启动 worker 守护循环
2. Runtime.stop() 优雅关闭，无 "Endpoint dropped" 警告
3. 重复 stop() 幂等
"""

from __future__ import annotations

import subprocess
import sys
import time

import pytest

import actant


class TestWorkerLifecycle:
    """单进程 worker 生命周期测试。"""

    def test_start_serve_stop_completes_cleanly(self) -> None:
        """start → serve → stop 全流程无异常且无 endpoint 警告。"""
        rt = actant.Runtime.with_defaults()
        rt.start()
        rt.serve()
        # 给 worker 守护循环一点时间启动
        time.sleep(0.5)
        # stop 应在 5s 内完成（PyRuntimeCore.shutdown 默认 timeout_ms=5000）
        t0 = time.monotonic()
        rt.stop()
        elapsed = time.monotonic() - t0
        assert elapsed < 10.0, f"shutdown took {elapsed:.1f}s, expected <10s"

    def test_stop_is_idempotent(self) -> None:
        """重复调用 stop() 不抛异常。"""
        rt = actant.Runtime.with_defaults()
        rt.start()
        rt.serve()
        time.sleep(0.3)
        rt.stop()
        rt.stop()  # 幂等：第二次不抛

    def test_context_manager_cleans_up(self) -> None:
        """with 块退出时自动 stop()。"""
        with actant.Runtime.with_defaults() as rt:
            rt.serve()
            time.sleep(0.3)
        # 退出 with 块后 _rust_core 应为 None
        assert rt._rust_core is None

    def test_cli_worker_shutdown_via_sigterm(self) -> None:
        """`actant worker` CLI 进程在 SIGTERM 下干净退出（exit code 0）。

        验证 iroh endpoint 被正确 close，无 "Endpoint dropped" 警告。
        """
        proc = subprocess.Popen(
            [sys.executable, "-m", "actant.cli", "worker"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={"ACTANT_DISCOVERY": "none", "PATH": ""},
        )
        try:
            # 等待 worker 启动
            time.sleep(3.0)
            proc.terminate()
            _stdout, stderr = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            pytest.fail("worker did not shutdown within 15s after SIGTERM")

        stderr_text = stderr.decode("utf-8", errors="replace")
        # SIGTERM 触发优雅退出，exit code 应为 0
        assert proc.returncode == 0, (
            f"expected exit code 0, got {proc.returncode}\nstderr:\n{stderr_text}"
        )
        # 不应出现 iroh endpoint 未关闭警告
        assert "Endpoint dropped" not in stderr_text, (
            f"iroh endpoint not cleanly closed\nstderr:\n{stderr_text}"
        )
        assert "worker stopped" in stderr_text, (
            f"worker did not log 'worker stopped'\nstderr:\n{stderr_text}"
        )
