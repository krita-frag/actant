"""恢复闭环测试：``Runtime.host_flows(once=True)`` 与 ``start(auto_resume=True)``。

崩溃重启后，Orchestrator 只恢复**已登记节点**；崩溃点之后尚未提交的节点
不在 DAG 中，必须重放 flow 体重建。本文件固定三条契约：

1. 显式入口 ``Runtime.host_flows(once=True)`` 阻塞重放未终态工作流，缺口节点补提交。
2. ``start(auto_resume=True)`` 在后台线程完成同样的重放，不阻塞启动。
3. ``auto_resume`` 默认关闭——重放会执行用户 flow 代码，必须显式选择。
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from actant import Runtime, flow, task, use_runtime
from actant.exceptions import InvalidStateError
from actant.workflow import wait_signal


@task(name="ar_stage")
def _stage(marker: str) -> None:
    with open(marker, "a") as fh:
        fh.write("s")


@task(name="ar_finish")
def _finish(marker: str, out: str) -> None:
    with open(out, "a") as fh:
        fh.write("done\n")


@flow(name="ar-flow")
def _pipeline(marker: str, out: str) -> None:
    _stage.submit(marker).result()
    wait_signal("go")
    _finish.submit(marker, out)


class _Events:
    def __init__(self) -> None:
        self.items: list[dict[str, str]] = []

    def __call__(self, e) -> None:  # type: ignore[no-untyped-def]
        self.items.append({"workflow_id": e.workflow_id, "kind": e.kind})

    def await_workflow_id(self, timeout: float = 15.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.items:
                return self.items[0]["workflow_id"]
            time.sleep(0.01)
        raise AssertionError("no WorkflowLifecycle event observed")


def _wait_until(predicate, *, timeout: float = 30.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def _signal_until_delivered(rt: Runtime, workflow_id: str, name: str) -> None:
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if rt.signal_wait_point(workflow_id, name) is not None:
            return
        time.sleep(0.01)
    raise AssertionError(f"wait point {name!r} was never registered")


def _executions(marker: str) -> int:
    path = Path(marker)
    return len(path.read_text()) if path.exists() else 0


def _run_until_stage_completed(data_dir: str, marker: str, out: str) -> str:
    """阶段 A：跑到节点 1 完成、flow 停在等待点，然后强停。返回 workflow_id。"""
    events = _Events()
    rt = Runtime(name="ar-node", data_dir=data_dir)
    rt.start()
    leftovers: list[BaseException] = []

    def _run() -> None:
        try:
            with use_runtime(rt):
                _pipeline(marker, out)
        except BaseException as exc:
            leftovers.append(exc)

    try:
        rt.layer("WorkflowLifecycle", "emit").chain(events)
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        workflow_id = events.await_workflow_id()
        node1 = f"ar_stage-1-{workflow_id}"
        _wait_until(
            lambda: (
                ((rt.get_workflow_state(workflow_id) or {}).get("tasks", {}) or {})
                .get(node1, {})
                .get("state")
                == "Completed"
            ),
            what="node1 Completed",
        )
    finally:
        rt.stop()
        thread.join(timeout=15)

    assert leftovers, "被强停的残留执行体应已中止（否则测试前提不成立）"
    return workflow_id


def _state(rt: Runtime, workflow_id: str) -> str:
    return (rt.get_workflow_state(workflow_id) or {}).get("state", "")


class TestRecoveryClosure:
    def test_host_flows_method_completes_workflow(self, tmp_path) -> None:
        """显式 ``Runtime.host_flows(once=True)``：补提交缺口节点并到达终态。"""
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "stage.txt")
        out = str(tmp_path / "out.txt")
        workflow_id = _run_until_stage_completed(data_dir, marker, out)

        rt = Runtime(name="ar-node", data_dir=data_dir)
        rt.start()
        failures: list[BaseException] = []
        resumed: list[str] = []
        try:
            def _resume() -> None:
                try:
                    resumed.extend(rt.host_flows(once=True))
                except BaseException as exc:
                    failures.append(exc)

            driver = threading.Thread(target=_resume, daemon=True)
            driver.start()
            _signal_until_delivered(rt, workflow_id, "go")
            driver.join(timeout=30)
            assert not driver.is_alive(), (
                f"host_flows 未收敛; state={rt.get_workflow_state(workflow_id)}"
            )
            assert failures == []
            assert resumed == [workflow_id]
            assert _state(rt, workflow_id) == "Completed"
        finally:
            rt.stop()

        assert _executions(marker) == 1, "已完成节点不得在恢复时重跑"
        assert Path(out).read_text().splitlines() == ["done"]

    def test_start_auto_resume_recovers_in_background(self, tmp_path) -> None:
        """`start(auto_resume=True)`：后台恢复，不阻塞启动。"""
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "stage.txt")
        out = str(tmp_path / "out.txt")
        workflow_id = _run_until_stage_completed(data_dir, marker, out)

        rt = Runtime(name="ar-node", data_dir=data_dir)
        begun = time.monotonic()
        rt.start(auto_resume=True)
        startup_elapsed = time.monotonic() - begun
        try:
            assert startup_elapsed < 10.0, (
                f"auto_resume 不应阻塞启动，实际耗时 {startup_elapsed:.1f}s"
            )
            _signal_until_delivered(rt, workflow_id, "go")
            _wait_until(
                lambda: _state(rt, workflow_id) == "Completed",
                what="workflow Completed via auto_resume",
            )
        finally:
            rt.stop()

        assert _executions(marker) == 1
        assert Path(out).read_text().splitlines() == ["done"]

    def test_auto_resume_is_off_by_default(self, tmp_path) -> None:
        """默认不自动恢复：等待点保持挂起，缺口节点不被提交。"""
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "stage.txt")
        out = str(tmp_path / "out.txt")
        workflow_id = _run_until_stage_completed(data_dir, marker, out)

        rt = Runtime(name="ar-node", data_dir=data_dir)
        rt.start()
        try:
            time.sleep(1.0)
            assert _state(rt, workflow_id) != "Completed"
        finally:
            rt.stop()

        assert not Path(out).exists(), "默认启动不得自动执行 pending 节点"

    def test_host_flows_requires_started_runtime(self) -> None:
        rt = Runtime(name="ar-not-started")
        with pytest.raises(InvalidStateError):
            rt.host_flows(once=True)
        rt.stop()
