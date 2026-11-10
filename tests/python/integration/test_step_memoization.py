"""step memoization 契约测试：节点结果一旦记录，重放命中不重跑、不改写结果。

契约（与 ``flow.py`` 的重放模型一致）：

1. 节点身份 = ``{任务名}-{提交序号}-{workflow_id}``；重放执行到同一序号时命中历史。
2. 命中**已完成**节点 → 返回记录结果，不再派发执行（副作用次数不变）。
3. 命中节点但定义（payload / 依赖 / 超时 / 重试 / 优先级）不一致 →
   :class:`FlowReplayError`，不静默错位。
4. 重试是**执行期**语义：每次尝试都是一次真实执行；重试耗尽后节点 Failed，
   已完成节点的记录结果不受影响。

范围声明：本文件不覆盖"崩溃时正在运行"的节点（重认领是独立缺口），
阶段 A 一律在节点已完成、flow 停在等待点时强制停机。
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from actant import Runtime, flow, host_flows, task, use_runtime
from actant.exceptions import FlowReplayError
from actant.workflow import FLOW_REGISTRY, FlowDefinition, wait_signal


@task(name="memo_stage")
def _stage(marker: str) -> str:
    """写一次副作用并返回确定性结果（用于断言"命中记录结果而非重跑"）。"""
    with open(marker, "a") as fh:
        fh.write("s")
    return "recorded-value"


@task(name="memo_capture")
def _capture(value: str, out: str) -> str:
    """把上游拿到的值落盘：验证下游收到的是**记录结果**。"""
    with open(out, "a") as fh:
        fh.write(f"{value}\n")
    return value


@task(name="memo_boom", retries=1, retry_delay_ms=10)
def _boom(marker: str) -> None:
    """每次尝试都写一次副作用再失败（用于断言重试次数）。"""
    with open(marker, "a") as fh:
        fh.write("b")
    raise RuntimeError("memo boom")


@flow(name="memo-flow")
def _memo_pipeline(marker: str, out: str) -> None:
    """节点 1 → 等待点 → 节点 2；提交序列只依赖入参，跨重启稳定。"""
    value = _stage.submit(marker).result()
    wait_signal("go")
    _capture.submit(value, out)


@flow(name="memo-retry-flow")
def _retry_pipeline(marker: str) -> None:
    _boom.submit(marker).result()


def _memo_pipeline_drifted(marker: str, out: str) -> None:
    """``_memo_pipeline`` 的"定义漂移"版本：同节点位置、不同 payload。

    不注册为 ``@flow``，只在漂移测试内临时替换，避免污染其他用例。
    """
    value = _stage.submit(marker + "-drift").result()
    wait_signal("go")
    _capture.submit(value, out)


class _Events:
    """收集 WorkflowLifecycle 事件（用于取 workflow_id）。"""

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


def _signal_until_registered(
    rt: Runtime, workflow_id: str, name: str, *, timeout: float = 15.0
) -> bytes:
    """循环递交信号直到等待点注册（注册前的信号由缓冲承接，不丢失）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        delivered = rt.signal_wait_point(workflow_id, name)
        if delivered is not None:
            return delivered
        time.sleep(0.01)
    raise AssertionError(f"wait point {name!r} was never registered")


def _executions(marker: str) -> int:
    path = Path(marker)
    return len(path.read_text()) if path.exists() else 0


def _captured(out: str) -> list[str]:
    path = Path(out)
    return path.read_text().splitlines() if path.exists() else []


def _run_until_stage_completed(data_dir: str, marker: str, out: str) -> str:
    """阶段 A：跑到节点 1 完成、flow 停在等待点，然后强停。返回 workflow_id。"""
    events = _Events()
    rt = Runtime(name="memo-node", data_dir=data_dir)
    rt.start()
    leftovers: list[BaseException] = []

    def _run() -> None:
        # 子线程需显式继承 Runtime 上下文（threading.local 不跨线程传播）。
        try:
            with use_runtime(rt):
                _memo_pipeline(marker, out)
        except BaseException as exc:
            leftovers.append(exc)

    try:
        rt.layer("WorkflowLifecycle", "emit").chain(events)
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        workflow_id = events.await_workflow_id()
        node1 = f"memo_stage-1-{workflow_id}"
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


class TestStepMemoization:
    def test_replay_reuses_recorded_result_without_reexecution(self, tmp_path) -> None:
        """重放命中：节点 1 不重跑，节点 2 收到的是记录结果。"""
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "stage.txt")
        out = str(tmp_path / "capture.txt")

        workflow_id = _run_until_stage_completed(data_dir, marker, out)
        assert _executions(marker) == 1, "阶段 A 应恰好执行一次节点 1"

        rt = Runtime(name="memo-node", data_dir=data_dir)
        rt.start()
        resumed: list[str] = []
        failures: list[BaseException] = []
        try:
            state = rt.get_workflow_state(workflow_id)
            assert state is not None
            assert state["state"] != "Completed"

            def _resume() -> None:
                try:
                    resumed.extend(host_flows(runtime=rt, once=True))
                except BaseException as exc:
                    failures.append(exc)

            driver = threading.Thread(target=_resume, daemon=True)
            driver.start()
            assert _signal_until_registered(rt, workflow_id, "go") is not None
            driver.join(timeout=30)
            assert not driver.is_alive(), (
                f"host_flows 未收敛; state={rt.get_workflow_state(workflow_id)} "
                f"resumed={resumed} failures={failures}"
            )
            assert failures == []

            final = rt.get_workflow_state(workflow_id)
            assert final is not None
            assert final["state"] == "Completed", final
            assert resumed == [workflow_id]
        finally:
            rt.stop()

        assert _executions(marker) == 1, (
            f"已完成节点在重放中被重跑了（executions={_executions(marker)}）"
        )
        assert _captured(out) == ["recorded-value"], (
            f"下游未收到记录结果: {_captured(out)}"
        )

    def test_definition_drift_raises_flow_replay_error(self, tmp_path) -> None:
        """同一节点标识、不同 payload（定义漂移）→ FlowReplayError。"""
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "stage.txt")
        out = str(tmp_path / "capture.txt")

        _run_until_stage_completed(data_dir, marker, out)
        assert _executions(marker) == 1

        rt = Runtime(name="memo-node", data_dir=data_dir)
        rt.start()
        try:
            # 模拟代码漂移：同一 flow 名、同一节点位置，但提交 payload 不同。
            # 入参来自 invocation（不可变），故漂移只能来自函数体变更。
            original = FLOW_REGISTRY.get("memo-flow")
            assert original is not None
            FLOW_REGISTRY.register(
                FlowDefinition(name="memo-flow", func=_memo_pipeline_drifted)
            )
            try:
                with pytest.raises(FlowReplayError):
                    host_flows(runtime=rt, once=True)
            finally:
                FLOW_REGISTRY.register(original)
        finally:
            rt.stop()

        assert _executions(marker) == 1, "定义漂移必须在派发前失败，不得重跑"

    def test_retry_attempts_execute_effect_and_exhaust(self, tmp_path) -> None:
        """重试是执行期语义：retries=1 → 共执行 2 次，耗尽后工作流 Failed。"""
        marker = str(tmp_path / "boom.txt")
        events = _Events()

        with Runtime(name="memo-retry-node") as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)
            with pytest.raises(RuntimeError):
                _retry_pipeline(marker)

            assert _executions(marker) == 2, (
                f"retries=1 应执行 2 次（首次 + 1 次重试），实际 {_executions(marker)}"
            )
            workflow_id = events.items[-1]["workflow_id"]
            state = rt.get_workflow_state(workflow_id)
            assert state is not None
            assert state["state"] == "Failed", state
