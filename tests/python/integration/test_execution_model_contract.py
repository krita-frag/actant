"""执行模型契约：跨重启的信号缓冲与已完成工作流的重放幂等。

已有契约由下列测试覆盖，本文件只补两个容易在驱动重构中回退的边界：

- 信号先于等待点抵达，且**中途重启**：缓冲随 journal / snapshot 存活。
- 已完成的 workflow 再次被 ``host_flows`` 扫描：不重放、不重复副作用。

执行模型全文见 ``docs/architecture/execution-model.md``。
"""

from __future__ import annotations

import queue
import threading
import time
from pathlib import Path

from actant import Runtime, flow, host_flows, task, use_runtime
from actant.workflow import current_workflow_id, wait_signal


@task(name="contract_marker")
def _append_marker(path: str) -> int:
    """写一次副作用，返回确定性结果；重放命中时不重复写。"""
    with open(path, "a", encoding="utf-8") as fh:
        fh.write("x")
    return 1


def _marker_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8"))


def _wait_until(predicate, *, timeout: float = 15.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def test_buffered_signal_survives_restart_before_waitpoint_registration(tmp_path) -> None:
    """等待点注册前抵达的信号，重启后仍能命中，且已完成节点不重跑。

    阶段 A：flow 完成节点 1 后进入纯睡眠段（等待点尚未注册），主线程递交
    一次信号（返回 ``None`` 表示入缓冲），随后在睡眠段内强停 Runtime。
    阶段 B：同一 ``data_dir`` 重启并 ``host_flows``。若缓冲丢失，flow 会等到
    ``timeout_ms=5000``；若节点重跑，marker 会变成 2。
    """
    data_dir = str(tmp_path / "data")
    marker = tmp_path / "marker.txt"
    ready: queue.Queue[str] = queue.Queue()
    leftovers: list[BaseException] = []

    @flow(name="contract-buffered-restart")
    def pipeline() -> str:
        _append_marker.submit(str(marker)).result()
        ready.put(current_workflow_id())
        time.sleep(1.5)  # 纯睡眠段：等待点尚未注册
        payload = wait_signal("go", timeout_ms=5000)
        return payload.decode() if payload else "timeout"

    rt = Runtime(name="contract-a", data_dir=data_dir)
    rt.start()
    try:

        def _run() -> None:
            try:
                with use_runtime(rt):
                    pipeline()
            except BaseException as exc:  # 强停后的残留执行体预期会报错
                leftovers.append(exc)

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        workflow_id = ready.get(timeout=15)
        node = f"contract_marker-1-{workflow_id}"
        _wait_until(
            lambda: (
                ((rt.get_workflow_state(workflow_id) or {}).get("tasks", {}) or {})
                .get(node, {})
                .get("state")
                == "Completed"
            ),
            what="node1 Completed",
        )
        # 此刻仍在睡眠段内，等待点未注册；信号应入缓冲。
        assert rt.signal_wait_point(workflow_id, "go", b"early") is None
        time.sleep(0.2)
    finally:
        rt.stop()
        thread.join(timeout=15)

    assert leftovers, "被强停的残留执行体应已中止（否则测试前提不成立）"
    assert _marker_count(marker) == 1

    rt2 = Runtime(name="contract-b", data_dir=data_dir)
    rt2.start()
    started = time.monotonic()
    try:
        with use_runtime(rt2):
            host_flows(runtime=rt2, once=True)
        _wait_until(
            lambda: ((rt2.get_workflow_state(workflow_id) or {}).get("state") == "Completed"),
            what="workflow Completed after restart",
        )
        elapsed = time.monotonic() - started
        assert rt2.get_workflow_result(workflow_id) == "early"
    finally:
        rt2.stop()

    # 1.5s 重放睡眠 + 缓冲命中；若缓冲丢失则是 1.5s + 5s 上界。
    assert elapsed < 4.0, f"跨重启缓冲未命中（{elapsed:.2f}s）"
    assert _marker_count(marker) == 1, "已完成节点在重放时被重复执行"


def test_replaying_completed_workflow_does_not_reexecute_side_effects(tmp_path) -> None:
    """已完成的 workflow 再被 ``host_flows`` 扫描时是 no-op。"""
    data_dir = str(tmp_path / "data")
    marker = tmp_path / "marker.txt"

    @flow(name="contract-completed-replay")
    def pipeline() -> str:
        _append_marker.submit(str(marker)).result()
        return "done"

    with (
        Runtime(name="contract-c", data_dir=data_dir) as rt,
        use_runtime(rt),
    ):
        assert pipeline() == "done"
    assert _marker_count(marker) == 1

    with (
        Runtime(name="contract-d", data_dir=data_dir) as rt2,
        use_runtime(rt2),
    ):
        driven = host_flows(runtime=rt2, once=True)

    assert driven == [], driven
    assert _marker_count(marker) == 1, "已完成 workflow 被重复驱动"
