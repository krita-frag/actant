"""确定性上下文值集成测试：``actant.workflow.flow_ctx`` 的跨重放稳定性。

覆盖两条契约：

1. **重放返回记录值**：flow 体在挂起点前调用 ``now_ms`` / ``uuid`` /
   ``random``，重启续跑时同一序号返回首次记录值，提交给下游任务的载荷因此
   完全一致（节点指纹命中，不重跑）。
2. **调用序列漂移显式失败**：同 ``ctx-{n}`` 以不同 ``kind`` 重放抛
   :class:`FlowReplayError`，不静默替换为另一个值。
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from actant import Runtime, flow, host_flows, task, use_runtime
from actant.exceptions import FlowReplayError
from actant.workflow import flow_ctx, wait_signal


@task(name="ctx_write")
def _write(line: str) -> str:
    """把一行业务载荷追加到文件（用于断言重放载荷一致）。"""
    path, _, payload = line.partition(":")
    with open(path, "a") as fh:
        fh.write(payload + "\n")
    return "ok"


@flow(name="ctx-flow")
def _pipeline(marker: str) -> None:
    """先记录一组上下文值 → 挂起 → 再记录一组。

    第一组在重启重放时必须逐字一致（来自历史记录）；第二组在阶段 B 才
    首次生成，故是新值。
    """
    first = "|".join(
        [
            str(flow_ctx.now_ms()),
            flow_ctx.uuid(),
            f"{flow_ctx.random():.17g}",
        ]
    )
    _write.submit(f"{marker}:{first}")
    wait_signal("go")
    second = "|".join(
        [
            str(flow_ctx.now_ms()),
            flow_ctx.uuid(),
            f"{flow_ctx.random():.17g}",
        ]
    )
    _write.submit(f"{marker}:{second}")


class _Events:
    """收集 WorkflowLifecycle 事件（用于取 workflow_id）。"""

    def __init__(self) -> None:
        self.items: list[str] = []

    def __call__(self, e) -> None:  # type: ignore[no-untyped-def]
        self.items.append(e.workflow_id)

    def await_workflow_id(self, timeout: float = 15.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.items:
                return self.items[0]
            time.sleep(0.01)
        raise AssertionError("no WorkflowLifecycle event observed")


def _lines(marker: str) -> list[str]:
    path = Path(marker)
    return path.read_text().splitlines() if path.exists() else []


def _wait_until(predicate, *, timeout: float = 30.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def _signal_until_registered(rt, workflow_id: str, name: str, *, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        delivered = rt.signal_wait_point(workflow_id, name)
        if delivered is not None:
            return delivered
        time.sleep(0.01)
    raise AssertionError(f"wait point {name!r} was never registered")


class TestFlowContextReplay:
    def test_recorded_values_survive_restart_replay(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "values.txt")

        # ---- 阶段 A：生成第一组上下文值后强停 ----
        events = _Events()
        rt_a = Runtime(name="ctx-node", data_dir=data_dir)
        rt_a.start()
        leftovers: list[BaseException] = []

        def _run_a() -> None:
            try:
                with use_runtime(rt_a):
                    _pipeline(marker)
            except BaseException as exc:
                leftovers.append(exc)

        thread = threading.Thread(target=_run_a, daemon=True)
        try:
            rt_a.layer("WorkflowLifecycle", "emit").chain(events)
            thread.start()
            wf_id = events.await_workflow_id()
            node1 = f"ctx_write-1-{wf_id}"
            _wait_until(
                lambda: (
                    (rt_a.get_workflow_state(wf_id) or {})
                    .get("tasks", {})
                    .get(node1, {})
                    .get("state")
                    == "Completed"
                ),
                what="node1 Completed",
            )
        finally:
            rt_a.stop()
            thread.join(timeout=15)

        phase_a = _lines(marker)
        assert len(phase_a) == 1, phase_a
        assert leftovers, "被强停的残留执行体应已中止（否则测试前提不成立）"

        # ---- 阶段 B：同 data_dir 重启 → 重放并补完 ----
        rt_b = Runtime(name="ctx-node", data_dir=data_dir)
        rt_b.start()
        try:
            assert wf_id in rt_b.list_workflows()

            failures: list[BaseException] = []

            def _resume() -> None:
                try:
                    host_flows(runtime=rt_b, once=True)
                except BaseException as exc:
                    failures.append(exc)

            driver = threading.Thread(target=_resume, daemon=True)
            driver.start()
            assert _signal_until_registered(rt_b, wf_id, "go") is not None
            driver.join(timeout=30)
            assert not driver.is_alive(), (
                f"host_flows 未收敛; state={rt_b.get_workflow_state(wf_id)} "
                f"failures={failures}"
            )
            assert failures == []

            final = rt_b.get_workflow_state(wf_id)
            assert final is not None
            assert final["state"] == "Completed", final
            assert final["succeeded_count"] == 2, final
        finally:
            rt_b.stop()

        phase_all = _lines(marker)
        assert len(phase_all) == 2, phase_all
        # 核心断言：重放体的第一组 ctx 调用返回历史记录值，载荷逐字一致。
        assert phase_all[0] == phase_a[0], (
            f"context values changed across replay: before={phase_a[0]!r} "
            f"after={phase_all[0]!r}"
        )
        assert phase_all[1] != phase_all[0]


class TestFlowContextContract:
    def test_kind_mismatch_raises_replay_error(self) -> None:
        rt = Runtime(name="ctx-mismatch")
        rt.start()
        try:
            wf_id = "ctx-mismatch-wf"
            rt.submit_workflow(wf_id)

            # 首次记录；重复调用返回记录值而非新候选值。
            assert (
                rt.resolve_workflow_context_value(wf_id, "ctx-1", "now_ms", b"first")
                == b"first"
            )
            assert (
                rt.resolve_workflow_context_value(wf_id, "ctx-1", "now_ms", b"second")
                == b"first"
            )
            with pytest.raises(FlowReplayError):
                rt.resolve_workflow_context_value(wf_id, "ctx-1", "uuid", b"x")
        finally:
            rt.stop()
