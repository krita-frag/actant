"""任务级日志流集成测试。

链路：worker 子进程（logging/print → stderr ``actant_log:`` 行协议）→
Rust ``drain_stderr`` 解析 → ``BusEvent::TaskLog``（tap 语义）→ py
``on_task_log`` 回调。

任务内 ``print`` 被捕获进日志边带，不会直写 fd 1 损坏 stdout 帧流，
任务正常完成。
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

import actant
from actant import Runtime
from actant.tasks import task


def _collect_log_events(rt: Runtime, bucket: list[dict], lock: threading.Lock) -> None:
    rt.on_task_log(lambda event: (lock.acquire(), bucket.append(event), lock.release()))


class TestTaskLogStream:
    def test_print_and_logging_reach_task_log_callback(self) -> None:
        rt = Runtime.test()
        rt.start()
        events: list[dict] = []
        lock = threading.Lock()
        try:
            rt.on_task_log(lambda e: (lock.acquire(), events.append(e), lock.release())[1])

            @task
            def noisy_task() -> str:
                print("hello from task stdout")
                logging.getLogger("myapp.worker").warning("careful: %s", "thing")
                return "done"

            result = noisy_task.submit().result(timeout=30)
            assert result == "done"

            # 事件经 stderr 边带异步到达：轮询等待，deadline 内必有 stdout 行
            # 与 logging 行。
            deadline = time.time() + 10
            expected_stdout = {"task_id": None}
            while time.time() < deadline:
                messages = [e["message"] for e in events]
                if any("hello from task stdout" in m for m in messages) and any(
                    "careful: thing" in m for m in messages
                ):
                    break
                time.sleep(0.1)

            messages = [e["message"] for e in events]
            assert any(
                "hello from task stdout" in m for m in messages
            ), f"print line missing in task log events: {events}"
            assert any(
                "careful: thing" in m for m in messages
            ), f"logging line missing in task log events: {events}"

            # 载荷形状：stdout 行 level=STDOUT，logging 行级别名 WARNING。
            stdout_event = next(e for e in events if "hello from task stdout" in e["message"])
            assert stdout_event["level"] == "STDOUT"
            log_event = next(e for e in events if "careful: thing" in e["message"])
            assert log_event["level"] == "WARNING"
            # task_id 非空（任务上下文随行协议携带）。
            assert log_event["task_id"]
            assert expected_stdout is not None
            assert all(isinstance(e["task_id"], str) for e in events)
        finally:
            rt.stop()

    def test_per_task_subscription_only_receives_that_task(self, tmp_path: Path) -> None:
        """逐任务通道只投递目标任务的日志，不混入其它任务。"""
        rt = Runtime.test()
        rt.start()
        events: list[dict] = []
        lock = threading.Lock()
        try:

            @task
            def gated_log(directory: str, message: str) -> str:
                import os
                import time as _time

                gate = os.path.join(directory, "go")
                deadline = _time.time() + 20
                while not os.path.exists(gate):
                    if _time.time() > deadline:
                        return "gate-timeout"
                    _time.sleep(0.05)
                print(message)
                return "done"

            first = gated_log.submit(str(tmp_path), "first-task-line")
            second = gated_log.submit(str(tmp_path), "second-task-line")
            # 两个任务都在等待 gate，订阅一定先于日志产生。
            rt.on_task_log(
                lambda e: (lock.acquire(), events.append(e), lock.release()),
                task_id=first.task_id,
            )
            (tmp_path / "go").write_text("go", encoding="utf-8")

            assert first.result(timeout=30) == "done"
            assert second.result(timeout=30) == "done"

            deadline = time.time() + 10
            while time.time() < deadline:
                with lock:
                    if any("first-task-line" in e["message"] for e in events):
                        break
                time.sleep(0.1)

            # 给 stderr 边带留出投递时间，确保「不混入」断言不是抢先通过。
            time.sleep(1.0)
            with lock:
                assert any("first-task-line" in e["message"] for e in events)
                assert all(e["task_id"] == first.task_id for e in events)
                assert not any("second-task-line" in e["message"] for e in events)
        finally:
            rt.stop()

    def test_task_completes_despite_print(self) -> None:
        """回归：print 不再损坏 stdout 帧流，任务结果完整回传。"""
        rt = Runtime.test()
        rt.start()
        try:

            @task
            def many_prints() -> int:
                for i in range(50):
                    print(f"line-{i}")
                return 42

            assert many_prints.submit().result(timeout=30) == 42
        finally:
            rt.stop()

    def test_on_task_log_requires_started_runtime(self) -> None:
        rt = Runtime.test()
        try:
            import pytest

            from actant.exceptions import InvalidStateError

            with pytest.raises(InvalidStateError):
                rt.on_task_log(lambda e: None)
        finally:
            rt.stop()


# 避免 actant 未直接使用告警（Runtime.test 经 actant 命名空间导入）。
_ = actant
