"""``continue_as_new`` 集成测试：分代续跑、携带参数持久化、重放幂等。

覆盖：

- 一次调用内链式续跑多代（每代一个独立工作流，各自到达终态）；
- 携带参数随新工作流落盘，恢复时优先于 ``Submitted.invocation`` 的上一代入参；
- 旧代重放时复用记录的下一代 id（续跑只发生一次），且同一驱动不会把
  后继代重复执行两遍；
- ``max_history_events`` 配置项可经 ``_ActantConfig`` 设置并读回。
"""

from __future__ import annotations

import time
from pathlib import Path

import cloudpickle
import pytest

from actant import Runtime, flow, host_flows, task, use_runtime
from actant.workflow import continue_as_new


@task(name="can_write")
def _write(line: str) -> str:
    path, _, payload = line.partition(":")
    with open(path, "a") as fh:
        fh.write(payload + "\n")
    return "ok"


@flow(name="can-flow", version=1)
def _chain_pipeline(marker: str, generation: int) -> None:
    _write.submit(f"{marker}:gen{generation}")
    if generation < 2:
        # 携带参数必须显式交给下一代：函数体会从开头重新执行。
        continue_as_new(marker, generation + 1)


def _lines(marker: str) -> list[str]:
    path = Path(marker)
    return path.read_text().splitlines() if path.exists() else []


def _wait_terminal(rt: Runtime, workflow_id: str, *, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = rt.get_workflow_state(workflow_id)
        if state is not None and state.get("state") in {"Completed", "Failed", "Cancelled"}:
            return state
        time.sleep(0.02)
    raise AssertionError(f"workflow {workflow_id} did not reach a terminal state")


class _Events:
    def __init__(self) -> None:
        self.workflow_ids: list[str] = []

    def __call__(self, e) -> None:  # type: ignore[no-untyped-def]
        if e.workflow_id not in self.workflow_ids:
            self.workflow_ids.append(e.workflow_id)


class TestContinueAsNew:
    def test_chains_generations_in_one_call(self, tmp_path) -> None:
        marker = str(tmp_path / "chain.txt")
        events = _Events()
        rt = Runtime(name="can-node")
        with rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)
            with use_runtime(rt):
                _chain_pipeline(marker, 0)

            assert _lines(marker) == ["gen0", "gen1", "gen2"], _lines(marker)
            assert len(events.workflow_ids) == 3, events.workflow_ids
            for workflow_id in events.workflow_ids:
                state = _wait_terminal(rt, workflow_id)
                assert state["state"] == "Completed", state
                assert state["succeeded_count"] == 1, state
                assert set(state["tasks"]) == {f"can_write-1-{workflow_id}"}

    def test_resume_uses_persisted_carry_args(self, tmp_path) -> None:
        marker = str(tmp_path / "carry.txt")
        rt = Runtime(name="can-node")
        with rt:
            workflow_id = "can-flow-cafe1234"
            # 模拟"上一代已创建新代外壳与携带参数，但新代函数体尚未执行"的崩溃现场。
            rt.submit_workflow(
                workflow_id,
                name="can-flow",
                version=1,
                args=cloudpickle.dumps(((marker, 0), {})),
            )
            rt.resolve_workflow_context_value(
                workflow_id,
                "@continue_args",
                "continue_args",
                cloudpickle.dumps(((marker, 7), {})),
            )
            with use_runtime(rt):
                resumed = host_flows(runtime=rt, once=True)

            assert resumed == [workflow_id]
            assert _lines(marker) == ["gen7"], _lines(marker)
            state = _wait_terminal(rt, workflow_id)
            assert state["state"] == "Completed", state

    def test_replayed_continuation_is_idempotent(self, tmp_path) -> None:
        marker = str(tmp_path / "idem.txt")
        old_id = "can-flow-aaaa1111"
        new_id = "can-flow-bbbb2222"
        rt = Runtime(name="can-node")
        with rt:
            # 旧代：已记录"下一代 id"，尚未封口。
            rt.submit_workflow(
                old_id,
                name="can-flow",
                version=1,
                args=cloudpickle.dumps(((marker, 0), {})),
            )
            rt.resolve_workflow_context_value(
                old_id, "@continued_to", "continued_to", new_id.encode()
            )
            # 新代：外壳与携带参数已落盘，函数体未执行（generation=1）。
            rt.submit_workflow(
                new_id,
                name="can-flow",
                version=1,
                args=cloudpickle.dumps(((marker, 1), {})),
            )
            rt.resolve_workflow_context_value(
                new_id,
                "@continue_args",
                "continue_args",
                cloudpickle.dumps(((marker, 1), {})),
            )
            with use_runtime(rt):
                host_flows(runtime=rt, once=True)

            # 旧代重放到 continue_as_new 时复用记录的新代 id（不分裂第三代），
            # 新代由旧代在同一线程内续跑；驱动随后跳过它，故只写一次。
            assert _lines(marker) == ["gen0", "gen1", "gen2"], _lines(marker)
            assert old_id != new_id
            assert _wait_terminal(rt, old_id)["state"] == "Completed"
            state = _wait_terminal(rt, new_id)
            assert state["state"] == "Completed", state

    def test_max_history_events_config_roundtrip(self) -> None:
        from actant.actant import _ActantConfig, _NetworkConfig

        cfg = _ActantConfig(
            payload_signing_key="",
            network=_NetworkConfig(preset="none"),
            max_history_events=64,
        )
        assert cfg.max_history_events == 64

    def test_continue_as_new_outside_flow_rejected(self, tmp_path) -> None:
        from actant.exceptions import InvalidStateError

        rt = Runtime(name="can-node")
        with rt, use_runtime(rt), pytest.raises(InvalidStateError):
            continue_as_new(str(tmp_path / "x.txt"), 1)
