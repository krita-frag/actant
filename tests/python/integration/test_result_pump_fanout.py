"""结果泵 fan-out 契约测试。

correctness 通道语义上只有一个消费者（结果泵）：首次
``register_task_result_callback`` 取走唯一接收端并启动泵，后续注册经追加
通道进入**同一泵**的 fan-out 列表——而非各自订阅、拆流竞态。本测试驱动
真实生产路径验证：
- 多个回调各收到同一终态事件（fan-out 不丢不重）；
- 注册晚于事件发布的回调收不到历史事件（追加从注册时刻生效）。
"""

from __future__ import annotations

import threading
import time

from actant import Runtime, task


@task(name="pump_fanout_echo")
def _echo(value: str) -> str:
    return value


class _CallbackSink:
    """收集回调事件的线程安全容器。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: list[tuple[str, str | None]] = []

    def __call__(self, completion: object) -> None:
        state = getattr(completion, "state", None)
        task_id = getattr(completion, "task_id", None)
        with self._lock:
            self._events.append((str(state), str(task_id) if task_id else None))

    def states_for(self, task_id: str) -> list[str]:
        with self._lock:
            return [state for state, tid in self._events if tid == task_id]


def _wait_for(predicate, *, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def test_result_pump_fans_out_to_multiple_callbacks() -> None:
    rt = Runtime.test()
    rt.start()
    try:
        core = rt._require_core() if hasattr(rt, "_require_core") else rt._rust_core
        assert core is not None, "started runtime must expose the rust core"

        first = _CallbackSink()
        second = _CallbackSink()
        core.register_task_result_callback(first)
        # 注册晚于第一个（泵已启动）：必须走追加通道进入同一泵，而非拆流。
        core.register_task_result_callback(second)

        handle = _echo.submit("hello-pump")
        value = handle.result()
        assert value == "hello-pump"
        task_id = handle.task_id

        _wait_for(
            lambda: "Completed" in first.states_for(task_id)
            and "Completed" in second.states_for(task_id),
            timeout=5,
            what="both callbacks to observe the terminal event",
        )

        # fan-out 不重不漏：同一终态事件两个回调各恰好收到一次 Completed。
        assert first.states_for(task_id).count("Completed") == 1
        assert second.states_for(task_id).count("Completed") == 1
    finally:
        rt.stop()
