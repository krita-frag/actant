"""flow 版本与分支决策集成测试：``@flow(version=...)`` + ``patched()``。

覆盖三条契约：

1. **决策跨重启稳定**：``patched("change-id")`` 首次执行记录的决策随工作流
   快照落盘；重启重放时即使代码默认值已翻转，也返回记录值（新分支不被
   旧工作流采用），提交序列因此不漂移。
2. **版本随工作流持久化**：``@flow(version=N)`` 在惰性创建时写入历史。
3. **回滚显式失败**：重放的工作流记录版本高于当前代码版本时抛
   :class:`FlowReplayError`，不静默以旧代码重放新历史。
"""

from __future__ import annotations

import struct
import threading
import time
from pathlib import Path

import cloudpickle
import pytest

from actant import Runtime, flow, host_flows, task, use_runtime
from actant.exceptions import FlowReplayError, InvalidStateError
from actant.workflow import FLOW_REGISTRY, FlowDefinition, patched, wait_signal


@task(name="ver_write")
def _write(line: str) -> str:
    """把一行业务载荷追加到文件（用于断言重放走了哪个分支）。"""
    path, _, payload = line.partition(":")
    with open(path, "a") as fh:
        fh.write(payload + "\n")
    return "ok"


#: ``patched()`` 的默认决策。阶段 A/B 取不同值，模拟"代码升级后的默认分支"。
_patched_default = True


@flow(name="ver-flow", version=3)
def _versioned_pipeline(marker: str) -> None:
    branch = patched("branch", enabled=_patched_default)
    _write.submit(f"{marker}:{'new' if branch else 'old'}")
    wait_signal("go")
    _write.submit(f"{marker}:done")


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


class TestPatchedReplay:
    def test_recorded_branch_decision_survives_restart(self, tmp_path) -> None:
        global _patched_default
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "branch.txt")

        # ---- 阶段 A：默认分支 = new，记录决策后强停 ----
        _patched_default = True
        events = _Events()
        rt_a = Runtime(name="ver-node", data_dir=data_dir)
        rt_a.start()
        leftovers: list[BaseException] = []

        def _run_a() -> None:
            try:
                with use_runtime(rt_a):
                    _versioned_pipeline(marker)
            except BaseException as exc:
                leftovers.append(exc)

        thread = threading.Thread(target=_run_a, daemon=True)
        try:
            rt_a.layer("WorkflowLifecycle", "emit").chain(events)
            thread.start()
            wf_id = events.await_workflow_id()
            node1 = f"ver_write-1-{wf_id}"
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
        assert phase_a == ["new"], phase_a
        assert leftovers, "被强停的残留执行体应已中止（否则测试前提不成立）"

        # ---- 阶段 B：代码默认分支翻转为 old，重放必须沿用记录决策 new ----
        _patched_default = False
        rt_b = Runtime(name="ver-node", data_dir=data_dir)
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
            # 节点身份与阶段 A 完全相同：分支决策未漂移（否则节点 1 载荷指纹
            # 不一致，重放会直接抛 FlowReplayError）。
            assert set(final["tasks"]) == {
                f"ver_write-1-{wf_id}",
                f"ver_write-2-{wf_id}",
            }, final

            # 记录值优先于候选值：重放（以及任何再次解析）拿到的都是 True。
            assert (
                rt_b.resolve_workflow_context_value(
                    wf_id, "patch:branch", "patched", b"\x00"
                )
                == b"\x01"
            )
            # 版本随工作流持久化。
            raw_version = rt_b.resolve_workflow_context_value(
                wf_id, "@flow_version", "workflow_version", struct.pack(">q", 0)
            )
            assert struct.unpack(">q", raw_version)[0] == 3
        finally:
            rt_b.stop()

        # 阶段 A 的一行 + 阶段 B 补完的一行；第一行仍是阶段 A 的 new 分支。
        assert _lines(marker) == ["new", "done"], _lines(marker)


@flow(name="ver-mismatch", version=5)
def _mismatch_pipeline(marker: str) -> None:
    _write.submit(f"{marker}:x")


class TestVersionMismatch:
    def test_newer_recorded_version_rejected_on_replay(self) -> None:
        rt = Runtime(name="ver-mismatch-node")
        rt.start()
        try:
            wf_id = "ver-mismatch-deadbeef"
            # 模拟"由 version=5 的代码提交、且 invocation 已落盘"的工作流。
            rt.submit_workflow(
                wf_id,
                name="ver-mismatch",
                version=5,
                args=cloudpickle.dumps((("/tmp/unused",), {})),
            )
            rt.resolve_workflow_context_value(
                wf_id, "@flow_version", "workflow_version", struct.pack(">q", 5)
            )
            # 当前代码回滚到 version=1：无法消费历史里的分支决策。
            original = FLOW_REGISTRY.get("ver-mismatch")
            assert original is not None
            FLOW_REGISTRY.register(
                FlowDefinition(
                    name="ver-mismatch", func=original.func, version=1
                )
            )
            try:
                with pytest.raises(FlowReplayError):
                    host_flows(runtime=rt, once=True)
            finally:
                FLOW_REGISTRY.register(original)
        finally:
            rt.stop()

    def test_negative_version_rejected_at_decoration(self) -> None:
        with pytest.raises(ValueError):
            flow(version=-1)(lambda: None)


@flow(name="ver-conflict", version=1)
def _conflicting_pipeline() -> None:
    patched("x", enabled=True)
    patched("x", enabled=False)


class TestPatchedContract:
    def test_conflicting_defaults_in_one_run_rejected(self) -> None:
        rt = Runtime(name="ver-conflict-node")
        rt.start()
        try:
            with use_runtime(rt), pytest.raises(FlowReplayError):
                _conflicting_pipeline()
        finally:
            rt.stop()

    def test_patched_outside_flow_rejected(self) -> None:
        rt = Runtime(name="ver-outside-node")
        rt.start()
        try:
            with use_runtime(rt), pytest.raises(InvalidStateError):
                patched("x")
        finally:
            rt.stop()
