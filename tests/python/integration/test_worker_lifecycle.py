"""Worker 生命周期集成测试：serve() → stop() 全流程。

验证：
1. Runtime.start() + serve() 启动 worker 守护循环
2. Runtime.stop() 优雅关闭，无 "Endpoint dropped" 警告
3. 重复 stop() 幂等
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

import pytest

import actant
from actant.exceptions import InvalidStateError, TaskCancelledError


class TestWorkerLifecycle:
    """单进程 worker 生命周期测试。"""

    def test_start_serve_stop_completes_cleanly(self) -> None:
        """start → serve → stop 全流程无异常且无 endpoint 警告。"""
        rt = actant.Runtime()
        rt.start()
        rt.serve()
        # 给 worker 守护循环一点时间启动
        time.sleep(0.5)
        # stop 应在 5s 内完成（PyRuntimeCore.shutdown 默认 timeout_ms=5000）
        t0 = time.monotonic()
        rt.stop()
        elapsed = time.monotonic() - t0
        assert elapsed < 10.0, f"shutdown took {elapsed:.1f}s, expected <10s"

    def test_serve_is_idempotent(self) -> None:
        """重复 serve() 不得再 spawn 一个执行循环。

        ``start()`` 已经拉起循环；CLI 早先又显式调了一次，导致两个
        ``worker.run()`` 并行消费同一队列、旧句柄泄漏。
        """
        rt = actant.Runtime()
        rt.start()
        rt.serve()
        rt.serve()
        t0 = time.monotonic()
        rt.stop()
        assert time.monotonic() - t0 < 10.0

    def test_stop_is_idempotent(self) -> None:
        """重复调用 stop() 不抛异常。"""
        rt = actant.Runtime()
        rt.start()
        rt.serve()
        time.sleep(0.3)
        rt.stop()
        rt.stop()  # 幂等：第二次不抛

    def test_context_manager_cleans_up(self) -> None:
        """with 块退出时自动 stop()。"""
        with actant.Runtime() as rt:
            rt.serve()
            time.sleep(0.3)
        # 退出 with 块后 _rust_core 应为 None
        assert rt._rust_core is None

    def test_cli_worker_shutdown_via_sigterm(self) -> None:
        """`actant worker` CLI 进程在 SIGTERM 下干净退出（exit code 0）。

        验证 iroh endpoint 被正确 close，无 "Endpoint dropped" 警告。
        """
        # 基于 os.environ.copy() 继承父进程环境变量，仅覆盖必要项，
        # 避免 LANG/HOME 等缺失引发子进程异常。
        env = os.environ.copy()
        env["ACTANT_DISCOVERY"] = "none"
        proc = subprocess.Popen(
            [sys.executable, "-m", "actant.cli", "worker"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
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


class TestWorkerStateCallback:
    """``on_worker_state`` 的注册与生命周期校验。"""

    def test_requires_started_runtime(self) -> None:
        rt = actant.Runtime()
        with pytest.raises(InvalidStateError):
            rt.on_worker_state(lambda event: None)

    def test_registers_after_start(self) -> None:
        rt = actant.Runtime.test()
        rt.start()
        try:
            rt.on_worker_state(lambda event: None)
        finally:
            rt.stop()

    def test_stop_delivers_stopped_state(self) -> None:
        """``stop()`` 关闭 worker 时必须回调 ``stopped``。

        事件订阅 task 早先会在 shutdown 前被 abort，导致 ``stopped`` 永远
        送不到 Python 回调；本用例真实驱动 start → serve → stop 生产路径。
        """
        rt = actant.Runtime.test()
        rt.start()
        states: list[str] = []
        stopped = threading.Event()

        def on_state(event: dict[str, str]) -> None:
            states.append(event["state"])
            if event["state"] == "stopped":
                stopped.set()

        try:
            rt.on_worker_state(on_state)
            rt.stop()
        finally:
            assert stopped.wait(timeout=5), f"no stopped event, got {states!r}"

        assert states[-1] == "stopped"


@actant.task(name="wl_slow")
def _slow_task(seconds: float) -> str:
    """停留在执行中，供 stop() 时仍有未结算句柄的用例使用。"""
    time.sleep(seconds)
    return "done"


class TestStopSettlesPendingHandles:
    """stop() 之后在册句柄必须有终态，不能永远停在 running。"""

    def test_stop_settles_inflight_handle_as_cancelled(self) -> None:
        rt = actant.Runtime()
        rt.start()
        handle = _slow_task.submit(30.0)
        # 等它离开 pending，确保是「在途」而不是「还没投递」。
        deadline = time.monotonic() + 10
        while handle.state == "pending" and time.monotonic() < deadline:
            time.sleep(0.05)
        assert handle.state in {"pending", "running"}

        rt.stop(timeout=0)

        assert handle.state == "cancelled", (
            f"stop 后句柄应结算为 cancelled，实际 {handle.state!r}"
        )
        with pytest.raises(TaskCancelledError):
            handle.result(timeout=1)
