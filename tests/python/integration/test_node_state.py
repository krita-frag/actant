"""P3 运行时节点状态与进程隔离集成测试。

覆盖：
- 运行时更新 labels / quotas 后路由立即变化，无需重启节点；
- 软 draining 停止接纳新任务，在途任务继续完成，恢复后可再次执行；
- 超限标签被拒绝（配置错误）；
- 任务超时强杀时整个进程组被终止，孙进程不残留；
- 同一 worker 复用时不继承前一个任务的 cwd / 环境变量。
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from actant import Runtime
from actant.actant import _ActantConfig, _NetworkConfig
from actant.exceptions import ActantError, ConfigError
from actant.tasks import task


def _config(
    *,
    labels: dict[str, str] | None = None,
    quotas: dict[str, int] | None = None,
    starvation_ms: int = 50,
    workers: int = 2,
) -> _ActantConfig:
    return _ActantConfig(
        payload_signing_key="",
        network=_NetworkConfig(preset="none"),
        max_concurrent_tasks=workers,
        num_worker_processes=workers,
        starvation_timeout_ms=starvation_ms,
        node_labels=labels,
        node_quotas=quotas,
    )


def _wait_until(predicate, timeout: float = 10.0) -> None:
    """轮询等待条件成立；超时抛 AssertionError。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met before timeout")


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


class TestRuntimeNodeState:
    def test_labels_update_routes_without_restart(self) -> None:
        rt = Runtime(config=_config())
        rt.start()
        try:

            @task(tags=["engine=gpu"])
            def placed(x: int) -> int:
                return x

            with pytest.raises(ActantError, match="required labels"):
                placed.submit(1).result(timeout=30)

            rt.update_node_state(labels={"engine": "gpu"})
            assert placed.submit(2).result(timeout=30) == 2

            rt.update_node_state(labels={})
            with pytest.raises(ActantError, match="required labels"):
                placed.submit(3).result(timeout=30)
        finally:
            rt.stop()

    def test_quota_capacity_update_without_restart(self) -> None:
        rt = Runtime(config=_config(quotas={}))
        rt.start()
        try:

            @task(quota="gpu")
            def gpu_job(x: int) -> int:
                return x

            with pytest.raises(ActantError, match="quota"):
                gpu_job.submit(1).result(timeout=30)

            rt.update_node_state(quotas={"gpu": 1})
            assert gpu_job.submit(2).result(timeout=30) == 2
        finally:
            rt.stop()

    def test_draining_stops_new_tasks_but_lets_inflight_finish(
        self, tmp_path: Path
    ) -> None:
        started = tmp_path / "started"
        rt = Runtime(config=_config(starvation_ms=50))
        rt.start()
        try:

            @task
            def slow(marker: str) -> str:
                Path(marker).write_text("started", encoding="utf-8")
                time.sleep(0.5)
                return "done"

            @task
            def quick(x: int) -> int:
                return x

            inflight = slow.submit(str(started))
            _wait_until(started.exists)

            rt.update_node_state(draining=True)
            with pytest.raises(ActantError, match="draining"):
                quick.submit(1).result(timeout=30)

            # 在途任务不受影响。
            assert inflight.result(timeout=30) == "done"

            rt.update_node_state(draining=False)
            assert quick.submit(2).result(timeout=30) == 2
        finally:
            rt.stop()

    def test_oversized_labels_raise_config_error(self) -> None:
        rt = Runtime(config=_config())
        rt.start()
        try:
            with pytest.raises(ConfigError, match="labels"):
                rt.update_node_state(labels={"big": "x" * 8192})
        finally:
            rt.stop()


class TestWorkerProcessIsolation:
    def test_timeout_kills_whole_process_tree(self, tmp_path: Path) -> None:
        pid_file = tmp_path / "child.pid"
        rt = Runtime(config=_config(workers=1, starvation_ms=30_000))
        rt.start()
        try:

            @task(timeout_ms=300)
            def leaky(marker: str) -> None:
                child = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(60)"]
                )
                Path(marker).write_text(str(child.pid), encoding="utf-8")
                time.sleep(60)

            with pytest.raises(ActantError):
                leaky.submit(str(pid_file)).result(timeout=30)

            _wait_until(pid_file.exists)
            child_pid = int(pid_file.read_text(encoding="utf-8"))
            _wait_until(lambda: not _process_alive(child_pid))
        finally:
            rt.stop()

    def test_worker_restores_cwd_and_env_between_tasks(self, tmp_path: Path) -> None:
        rt = Runtime(config=_config(workers=1, starvation_ms=30_000))
        rt.start()
        try:

            @task
            def pollute(target: str) -> None:
                os.chdir(target)
                os.environ["ACTANT_LEAK"] = "1"

            @task
            def inspect() -> tuple[str, str | None]:
                return os.getcwd(), os.environ.get("ACTANT_LEAK")

            pollute.submit(str(tmp_path)).result(timeout=30)
            cwd, leak = inspect.submit().result(timeout=30)

            assert cwd != str(tmp_path)
            assert leak is None
        finally:
            rt.stop()
