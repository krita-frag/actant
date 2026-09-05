"""Flow DAG 编排集成测试：任务提交、状态机推进、失败/重试、状态查询。

验证 @flow 从命令式函数体到 Rust Orchestrator 持久化的完整链路：
1. 成功路径：DAG 记录节点与依赖边 → 提交 → 结果回灌 → 状态机 Completed
2. 生命周期事件 submitted/started/completed/failed 与状态一致
3. 失败/重试：任务重试耗尽后 DAG 部分回灌 → 状态机 Failed
4. flow 级重试与超时语义
"""

from __future__ import annotations

import time

import pytest

from actant import Runtime, flow, task


class _Events:
    """收集 WorkflowLifecycle 事件。"""

    def __init__(self) -> None:
        self.items: list[dict] = []

    def __call__(self, e) -> None:  # type: ignore[no-untyped-def]
        self.items.append({"workflow_id": e.workflow_id, "kind": e.kind})


@task(name="it_fetch")
def _fetch(src: int) -> int:
    return src


@task(name="it_transform")
def _transform(x: int) -> int:
    return x * 2 + 1


@task(name="it_analyze")
def _analyze(x: int) -> int:
    return x + 10


@task(name="it_partition", retries=1, retry_delay_ms=10)
def _partition(a: int, b: int, *, fail: bool = False) -> str:
    if fail:
        raise ValueError("boom")
    return f"ok:{a}:{b}"


@task(name="it_store")
def _store(payload: str) -> str:
    return f"stored[{payload}]"


class TestFlowOrchestration:
    """@flow → Orchestrator 完整链路。"""

    def test_success_submits_dag_reaches_completed(self) -> None:
        """成功路径：DAG 提交、状态机 Completed、任务计数正确。"""

        @flow(name="wf-success")
        def pipeline(src: int) -> str:
            raw = _fetch.submit(src)
            transformed = _transform.submit(raw)
            analyzed = _analyze.submit(raw)
            routed = _partition.submit(transformed, analyzed)
            return _store.submit(routed).result()

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)
            result = pipeline(5)

        assert result == "stored[ok:11:15]", result
        wf_ids = {i["workflow_id"] for i in events.items}
        assert len(wf_ids) == 1, wf_ids
        kinds = [i["kind"] for i in events.items]
        assert kinds == ["submitted", "started", "completed"], kinds

    def test_fail_fast_marks_workflow_failed(self) -> None:
        """失败路径：任务重试耗尽，状态机 Failed，失败事件与状态一致。"""

        @flow(name="wf-fail")
        def pipeline(src: int) -> str:
            raw = _fetch.submit(src)
            transformed = _transform.submit(raw)
            analyzed = _analyze.submit(raw)
            routed = _partition.submit(transformed, analyzed, fail=True)
            return _store.submit(routed).result()

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)
            with pytest.raises(ValueError):
                pipeline(5)

            wf_id = events.items[-1]["workflow_id"]
            state = rt.get_workflow_state(wf_id)
            assert state is not None
            assert state["state"] == "Failed", state["state"]
            assert state["succeeded_count"] == 3, state
            assert state["total_count"] == 4, state
            # partition 任务应标记 Failed，其余已完成。
            by_state = {t["state"] for t in state["tasks"].values()}
            assert "Failed" in by_state and "Completed" in by_state, by_state

        kinds = [i["kind"] for i in events.items]
        assert kinds == ["submitted", "started", "failed"], kinds

    def test_query_state_by_id_after_terminal(self) -> None:
        """终态后可按 ID 查询：状态/计数/tasks 明细可读。"""

        @flow(name="wf-query")
        def pipeline(src: int) -> str:
            raw = _fetch.submit(src)
            return _store.submit(_transform.submit(raw)).result()

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)
            result = pipeline(3)
            # _transform(3) = 7 → _store("ok-ish") 实际值以函数实现为准。
            assert result.startswith("stored["), result
            wf_id = events.items[-1]["workflow_id"]
            state = rt.get_workflow_state(wf_id)
            assert state is not None
            assert state["state"] == "Completed"
            assert state["succeeded_count"] == state["total_count"] == 3
            for ts in state["tasks"].values():
                assert ts["state"] == "Completed"
                assert not ts["error"]

    def test_flow_retry_re_runs_until_success(self) -> None:
        """flow 级重试：函数体中途异常，重试后成功返回。"""

        attempts = {"n": 0}

        # 异常直接在 flow 函数体中抛出（而非经 Task.submit 派发）：flow 函数体
        # 在进程内直接执行，其闭包计数器在 flow 级重试之间是共享的；而经 submit
        # 派发的任务载荷会被序列化，闭包计数器在每次派发间不共享，无法观测重试。
        @flow(name="wf-retry", retries=1, retry_delay_ms=10)
        def pipeline(src: int) -> int:
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("first attempt fails")
            return src * 2

        with Runtime.with_defaults():
            result = pipeline(4)

        assert result == 8
        assert attempts["n"] == 2

    def test_flow_soft_timeout_raises(self) -> None:
        """flow 软超时：超过 timeout_ms 抛 ActantTimeoutError。"""
        from actant.exceptions import ActantTimeoutError

        @task(name="it_slow")
        def slow() -> None:
            time.sleep(10)

        @flow(name="wf-timeout", timeout_ms=200)
        def pipeline() -> None:
            slow.submit().result()

        with Runtime.with_defaults(), pytest.raises(ActantTimeoutError):
            pipeline()
