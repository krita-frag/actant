"""当前 Actant 能力面的 dogfood 测试。

覆盖通用化开发计划 P0–P4 引入、且与分布式/生命周期相关的行为：

1. batch 提交 + 逐项进度（``submit_batch`` / ``batch_progress``）；
2. 远端 Worker 的任务日志边带（``on_task_log``）；
3. flow 等待点（``wait_signal``）跨节点唤醒；
4. ``continue_as_new`` 续跑链与最终代返回值查询；
5. worker 进程崩溃后的 crash failover 重路由；
6. 幂等键与任务终态记录跨 Runtime 重启存活。

所有用例都通过 testkit 的 ``TestCluster`` / ``Sandbox`` 驱动真实 Actant
运行时，不 mock 核心组件。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from act_ext_testkit import NodeConfig, Sandbox, TestCluster

from actant import Runtime, flow, task, use_runtime
from actant.exceptions import AlreadyExistsError
from actant.workflow import continue_as_new, wait_signal

_CLUSTER_KWARGS: dict[str, Any] = {
    "heartbeat_interval_ms": 300,
    "failure_timeout_ms": 1_500,
    "starvation_timeout_ms": 4_000,
}


def _cluster(
    namespace: str,
    *,
    workers: int = 2,
    submit_route_policy: str | None = None,
) -> TestCluster:
    cluster = TestCluster(namespace, seed=11, **_CLUSTER_KWARGS)
    cluster.add_node(
        "submit",
        labels={"role": "submit"},
        max_concurrent_tasks=0,
        config=NodeConfig(
            labels={"role": "submit"},
            max_concurrent_tasks=0,
            route_policy=submit_route_policy,
        ),
    )
    for index in range(workers):
        cluster.add_node(
            f"w{index}",
            labels={"engine": "worker"},
            config=NodeConfig(labels={"engine": "worker"}, max_concurrent_tasks=1),
        )
    return cluster


def _wait_until(predicate: Callable[[], bool], *, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition was not met before deadline")


class _Events:
    """收集 WorkflowLifecycle 事件（用于取 workflow_id）。"""

    def __init__(self) -> None:
        self.items: list[dict[str, str]] = []
        self._lock = threading.Lock()

    def __call__(self, event: Any) -> None:
        with self._lock:
            self.items.append({"workflow_id": event.workflow_id, "kind": event.kind})

    def await_workflow_id(self, timeout: float = 15.0) -> str:
        _wait_until(lambda: bool(self.items), timeout=timeout)
        return self.items[0]["workflow_id"]


class TestBatchProgressDogfood:
    def test_submit_batch_reports_finished_progress(self) -> None:
        """批量提交的每个任务都结算，进度快照收敛到 finished。"""
        cluster = _cluster("current-batch", submit_route_policy="label_affinity")
        cluster.start(topology="mesh")
        with cluster:
            @task(tags=["engine=worker"], timeout_ms=20_000)
            def double(value: int) -> int:
                return value * 2

            submit_rt = cluster.node("submit")
            with use_runtime(submit_rt):
                handles = double.submit_batch(list(range(8)), batch_id="batch-8")

            assert [handle.result(timeout=30) for handle in handles] == [
                value * 2 for value in range(8)
            ]
            progress = submit_rt.batch_progress("batch-8")
            assert progress is not None
            assert progress.total == 8
            assert progress.completed == 8
            assert progress.pending == 0
            assert progress.finished


class TestTaskLogDogfood:
    def test_remote_worker_log_reaches_owner_runtime(self) -> None:
        """远端节点执行的 ``print`` 日志回流到执行节点 Runtime 的 tap。"""
        cluster = _cluster("current-log", workers=1)
        cluster.start(topology="mesh")
        with cluster:
            @task(tags=["engine=worker"], timeout_ms=20_000)
            def noisy(value: int) -> int:
                print(f"current-log-marker-{value}")
                return value

            worker_rt = cluster.node("w0")
            events: list[dict[str, Any]] = []
            lock = threading.Lock()

            def _collect(event: dict[str, Any]) -> None:
                with lock:
                    events.append(event)

            worker_rt.on_task_log(_collect)

            with use_runtime(cluster.node("submit")):
                assert noisy.submit(7).result(timeout=30) == 7

            _wait_until(
                lambda: any("current-log-marker-7" in event["message"] for event in events)
            )
            matched = next(
                event for event in events if "current-log-marker-7" in event["message"]
            )
            assert matched["level"] == "STDOUT"
            assert matched["task_id"]


class TestFlowDogfood:
    def test_wait_signal_unblocks_flow_across_nodes(self) -> None:
        """flow 在提交节点挂起，外部信号唤醒后由远端 Worker 的任务收尾。"""
        cluster = _cluster("current-flow-signal", workers=1)
        cluster.start(topology="mesh")
        with cluster:
            @task(tags=["engine=worker"], timeout_ms=20_000)
            def increment(value: int) -> int:
                return value + 1

            @flow(name="current-signal-flow")
            def pipeline() -> str:
                increment.submit(1).result()
                payload = wait_signal("go")
                return payload.decode() if payload is not None else "timeout"

            submit_rt = cluster.node("submit")
            events = _Events()
            submit_rt.layer("WorkflowLifecycle", "emit").chain(events)
            outcome: list[str] = []

            def _run() -> None:
                # threading.local 不跨线程；子线程需显式继承 Runtime 上下文。
                with use_runtime(submit_rt):
                    outcome.append(pipeline())

            thread = threading.Thread(target=_run, daemon=True)
            thread.start()
            workflow_id = events.await_workflow_id()
            submit_rt.signal_wait_point(workflow_id, "go", b"signalled")
            thread.join(timeout=30)

            assert not thread.is_alive(), "flow did not resume after signal"
            assert outcome == ["signalled"]

    def test_continue_as_new_resolves_final_generation_result(self) -> None:
        """续跑链只在最终代记录返回值，用首代 id 查询可沿链取到。"""
        cluster = _cluster("current-continue", workers=1)
        cluster.start(topology="mesh")
        with cluster:
            @task(tags=["engine=worker"], timeout_ms=20_000)
            def identity(value: int) -> int:
                return value

            @flow(name="current-continue-flow")
            def pipeline(round_no: int) -> int:
                identity.submit(round_no).result()
                if round_no == 0:
                    continue_as_new(round_no + 1)
                return 100 + round_no

            submit_rt = cluster.node("submit")
            events = _Events()
            submit_rt.layer("WorkflowLifecycle", "emit").chain(events)
            with use_runtime(submit_rt):
                assert pipeline(0) == 101

            first_id = events.await_workflow_id()
            assert submit_rt.get_workflow_result(first_id) == 101


class TestCrashFailoverDogfood:
    def test_worker_crash_is_rerouted_and_task_recovers(self, tmp_path: Path) -> None:
        """worker 进程崩溃属基础设施失败：任务重路由后成功，副作用恰好两次。"""
        marker = tmp_path / "crash_once.marker"
        cluster = _cluster("current-crash", workers=2)
        cluster.start(topology="mesh")
        with cluster:
            @task(tags=["engine=worker"], timeout_ms=30_000)
            def crash_once(marker_path: str) -> str:
                import os
                import signal

                with open(marker_path, "a", encoding="utf-8") as handle:
                    handle.write("attempt\n")
                with open(marker_path, encoding="utf-8") as handle:
                    attempts = handle.read().count("attempt")
                if attempts == 1:
                    os.kill(os.getpid(), signal.SIGKILL)
                return "recovered"

            with use_runtime(cluster.node("submit")):
                assert crash_once.submit(str(marker)).result(timeout=60) == "recovered"

        assert marker.read_text(encoding="utf-8").count("attempt") == 2


class TestPersistenceDogfood:
    def test_idempotency_and_result_survive_runtime_restart(self) -> None:
        """幂等键与任务终态记录写入同一 ``data_dir``，重启后仍可查询/拦截。"""
        with Sandbox("current-persistence") as box:
            data_dir = str(box.node_dir("single"))

            @task(name="current_persist_echo")
            def echo(value: dict[str, int]) -> dict[str, int]:
                return value

            payload = {"v": 42}
            rt = Runtime(name="persist-a", data_dir=data_dir)
            with rt, use_runtime(rt):
                handle = echo.submit(payload, idempotency_key="job-current")
                assert handle.result(timeout=30) == payload
                task_id = handle.task_id
                assert rt.get_task_result(task_id) == payload

            rt2 = Runtime(name="persist-b", data_dir=data_dir)
            with rt2:
                assert rt2.get_task_result(task_id) == payload
                with use_runtime(rt2), pytest.raises(AlreadyExistsError):
                    echo.submit(payload, idempotency_key="job-current")
