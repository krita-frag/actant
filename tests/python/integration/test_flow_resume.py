"""续跑驱动集成测试：节点重启后重放未终态 flow。

这是「kill -9 重启续跑不重跑」的可执行版本，且刻意与等待点
联动：崩溃点落在**flow 挂在等待点上**的时刻，因此同时验证了两件事——

1. 已完成的节点在重放中**不重跑**（副作用计数不变）；
2. 悬挂中的等待点在重启后仍可被递交，重放体继续往下补提交缺口节点。

范围声明：崩溃时处于 ``Running`` 的节点不在本测试覆盖内——``recover`` 只重建
Pending 任务（``build_redispatches_recovered_pending_tasks_only``），在途节点
的重认领是已知缺口。
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from actant import (
    Runtime,
    flow,
    register_flow_recovery,
    resume_flows,
    task,
    use_runtime,
    wait_signal,
)


@task(name="rs_touch")
def _touch(marker: str) -> str:
    """副作用：追加一个字符。用于断言"已完成节点不重跑"。"""
    with open(marker, "a") as fh:
        fh.write("x")
    return "ok"


@task(name="rs_second")
def _second(v: int) -> int:
    return v + 1


def _executions(marker: str) -> int:
    path = Path(marker)
    return len(path.read_text()) if path.exists() else 0


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


def _signal_until_registered(rt, workflow_id: str, name: str, *, timeout: float = 15.0):
    """循环递交信号，直到等待点已注册并确认接收（注册前的信号由缓冲承接，不丢失）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        delivered = rt.signal_wait_point(workflow_id, name)
        if delivered is not None:
            return delivered
        time.sleep(0.01)
    raise AssertionError(f"wait point {name!r} was never registered")


@flow(name="resume-flow")
def _pipeline(marker: str) -> None:
    """节点 1 → 挂起点 → 节点 2。

    确定性契约：提交序列只依赖入参 ``marker``，不依赖 wall-clock / 随机数，
    故重启后重放能命中同一节点标识。
    """
    _touch.submit(marker)
    wait_signal("go")
    _second.submit(1)


class TestFlowResume:
    def test_restart_replays_flow_without_rerunning_completed_nodes(
        self, tmp_path
    ) -> None:
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "runs.txt")

        # ---- 阶段 A：跑一半后强制停掉（模拟节点死亡）----
        events = _Events()
        rt_a = Runtime.with_defaults(name="resume-node", data_dir=data_dir)
        rt_a.start()
        leftovers: list[BaseException] = []

        def _run_a() -> None:
            try:
                # 子线程需显式继承 Runtime 上下文（threading.local 不跨线程传播）。
                with use_runtime(rt_a):
                    _pipeline(marker)
            except BaseException as exc:
                leftovers.append(exc)

        try:
            rt_a.layer("WorkflowLifecycle", "emit").chain(events)
            thread = threading.Thread(target=_run_a, daemon=True)
            thread.start()
            wf_id = events.await_workflow_id()
            node1 = f"rs_touch-1-{wf_id}"

            _wait_until(
                lambda: (
                    (rt_a.get_workflow_state(wf_id) or {}).get("tasks", {})
                    .get(node1, {})
                    .get("state")
                    == "Completed"
                ),
                what="node1 Completed",
            )
        finally:
            # 强停：挂起中的 flow 线程随之终止（其异常已被捕获，不入 store）。
            rt_a.stop()
            thread.join(timeout=15)

        assert _executions(marker) == 1, "阶段 A 应恰好执行一次节点 1"
        assert leftovers, "被强停的残留执行体应已中止（否则测试前提不成立）"

        # ---- 阶段 B：同 data_dir 重启 → 续跑 ----
        rt_b = Runtime.with_defaults(name="resume-node", data_dir=data_dir)
        rt_b.start()
        try:
            assert wf_id in rt_b.list_workflows(), "非终态工作流必须存活于 store"

            state = rt_b.get_workflow_state(wf_id)
            assert state is not None
            assert state["tasks"][node1]["state"] == "Completed", state
            assert state["state"] != "Completed"

            register_flow_recovery("resume-flow", _pipeline, args=(marker,))
            resumed: list[str] = []
            failures: list[BaseException] = []

            def _resume() -> None:
                try:
                    resumed.extend(resume_flows(runtime=rt_b))
                except BaseException as exc:
                    failures.append(exc)

            driver = threading.Thread(target=_resume, daemon=True)
            driver.start()
            # 重放体在挂起点再次 park：悬挂的等待点跨重启仍可被递交。
            assert _signal_until_registered(rt_b, wf_id, "go") is not None
            driver.join(timeout=30)
            assert not driver.is_alive(), (
                f"resume_flows 未收敛; state={rt_b.get_workflow_state(wf_id)} "
                f"resumed={resumed} failures={failures}"
            )
            assert failures == []

            final = rt_b.get_workflow_state(wf_id)
            assert final is not None
            assert final["state"] == "Completed", final
            assert final["succeeded_count"] == 2, final
            assert resumed == [wf_id], resumed
        finally:
            rt_b.stop()

        # 核心断言：已完成节点没有因为重放而重跑。
        assert _executions(marker) == 1, (
            f"已完成节点在重放中被重跑了（executions={_executions(marker)}）"
        )
