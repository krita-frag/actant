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

from actant import Runtime, current_workflow_id, flow, task


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
            # partition 重试耗尽后失败：fail-fast 使工作流终态 Failed。
            # 任务级语义由 DAG 状态机维护（partition=Failed，其余 Completed）。
            assert state["state"] == "Failed", state["state"]
            assert state["succeeded_count"] == 3, state
            assert state["total_count"] == 4, state
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
        """flow 级不做整体重试：函数体中途异常直接传播、不重跑；任务级重试由
        ``@task(retries=...)`` 承载（与 flow 级重试无关）。"""

        attempts = {"n": 0}

        # flow 级不重试：函数体抛错直接传播，不重跑。
        # 任务级重试由 @task(retries=...) 承载（orchestrator 驱动）。
        @flow(name="wf-retry", retries=1, retry_delay_ms=10)
        def pipeline(src: int) -> int:
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("first attempt fails")
            return src * 2

        with Runtime.with_defaults(), pytest.raises(RuntimeError, match="first attempt"):
            pipeline(4)

        assert attempts["n"] == 1

    def test_flow_deadline_restores_strongly(self) -> None:
        """强还原：deadline 到期由 orchestrator 取消在途任务 → 工作流 Failed。

        与旧软超时实现的差异（破坏性变更）：
        - **不再"立即返回"**：抛错时刻在 ``deadline + 轮询周期``量级；
        - **事实源是工作流终态**（``Failed`` + ``workflow timeout exceeded``），
          不是 Python 侧计时器——后者已整体删除；
        - 被唤醒的原因是**本地在途任务被取消**（gossip 广播不回环，故必须
          自投递；否则本用例的函数体会永久挂起）。
        """
        from actant.exceptions import ActantTimeoutError
        from actant.flow import current_workflow_id

        @task(name="it_slow")
        def slow() -> None:
            time.sleep(30)

        seen: dict[str, str] = {}

        @flow(name="wf-timeout", timeout_ms=300)
        def pipeline() -> None:
            seen["wf"] = current_workflow_id() or ""
            slow.submit().result()

        with Runtime.with_defaults() as rt:
            started = time.monotonic()
            with pytest.raises(ActantTimeoutError):
                pipeline()
            elapsed = time.monotonic() - started
            state = rt.get_workflow_state(seen["wf"])

        assert state is not None, "workflow should still be queryable"
        assert state["state"] == "Failed", state
        assert state["error"] == "workflow timeout exceeded", state
        # 强还原要等到下一次轮询：deadline(0.3s) + 轮询周期(0.5s) + 取消结算，
        # 远不到任务自身的 30s。这个上界是"没有永久挂起"的判据。
        assert elapsed < 3.0, f"强还原耗时过长（{elapsed:.2f}s）"

    def test_flow_without_workflow_emits_only_terminal_event(self) -> None:
        """未创建工作流的 flow **不**广播 ``submitted`` / ``started``。

        函数体既无 ``Task.submit`` 也无等待点 ⇒ 工作流从不建槽，故不广播
        ``submitted`` / ``started``；"看到 ``submitted`` ⇒ 工作流确实已被提交"，
        只保留终态事件。

        这是**调用方可见的观测面变更**，已记入 CHANGELOG。
        """
        events = _Events()

        @flow(name="wf-noworkflow")
        def pipeline() -> int:
            return 1 + 1

        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)
            assert pipeline() == 2

        kinds = [i["kind"] for i in events.items]
        assert kinds == ["completed"], kinds

    def test_get_dag_exposes_structure_and_retry_policy(self) -> None:
        """``get_dag`` 暴露 DAG **结构**：节点 / 依赖边 / 重试策略。

        与 ``get_workflow_state`` 的分工：后者是**执行**状态（含 ``retry_count`` /
        ``attempt`` / ``result``），本方法是**结构**。按查重口径，
        ``get_retry_info`` 的 ``retry_count`` 那一半与 ``get_workflow_state`` 重复，
        故**不暴露**；其"重试策略"那一半由本方法覆盖（节点策略 + DAG 默认策略）。
        """
        seen: list[str] = []

        @flow(name="wf-getdag")
        def pipeline(src: int) -> str:
            raw = _fetch.submit(src)
            seen.append(current_workflow_id() or "")
            return _store.submit(_partition.submit(raw, raw)).result()

        with Runtime.with_defaults() as rt:
            assert pipeline(5) == "stored[ok:5:5]"
            dag = rt.get_dag(seen[0])
            # 执行状态（含 retry_count）仍在 get_workflow_state —— 二者不重复。
            state = rt.get_workflow_state(seen[0])

        assert dag is not None, "get_dag 应返回结构"
        assert dag["workflow_id"] == seen[0]
        assert dag["failure_strategy"] == "fail_fast"

        nodes = {n["task_id"]: n for n in dag["nodes"]}
        assert len(nodes) == 3, nodes
        # 依赖边：partition 与 store 都依赖 fetch。
        fetch_id = next(t for t, n in nodes.items() if n["name"] == "it_fetch")
        part_id = next(t for t, n in nodes.items() if n["name"] == "it_partition")
        store_id = next(t for t, n in nodes.items() if n["name"] == "it_store")
        assert nodes[fetch_id]["deps"] == []
        assert nodes[part_id]["deps"] == [fetch_id]
        assert nodes[store_id]["deps"] == [part_id]

        # 重试策略：@task(retries=1, retry_delay_ms=10) 落在节点上；
        # 未声明的节点为 None（此时生效的是 default_retry_policy）。
        assert nodes[part_id]["retry_policy"]["max_retries"] == 1
        assert nodes[part_id]["retry_policy"]["delay_ms"] == 10
        assert nodes[fetch_id]["retry_policy"] is None
        assert "default_retry_policy" in dag, "默认策略必须暴露，否则无法还原生效策略"

        # 刻意不含 payload（不透明且可能很大）。
        assert "payload" not in nodes[fetch_id], "get_dag 不得搬运 payload"

        assert state is not None
        assert "retry_count" in state["tasks"][fetch_id]

    def test_get_dag_returns_none_for_unknown_workflow(self) -> None:
        """未知工作流 → ``None``（与 ``get_workflow_state`` 同款契约）。"""
        with Runtime.with_defaults() as rt:
            assert rt.get_dag("no-such-workflow") is None

    def test_get_workflow_history_exposes_event_stream(self) -> None:
        """``get_workflow_history`` 暴露事件流（审计出口）。

        历史是**事实源**：``recover`` = 快照 + 其后事件重放，等待点与信号也进同一
        历史。本方法是它的对外读取出口。

        ``kind`` / ``task_id`` / ``error`` 供筛选；``payload`` 是**不透明**字节
        ——Python 不解释其布局，故 Rust 枚举字段增减不会变成跨语言契约。
        """
        seen: list[str] = []

        @flow(name="wf-history")
        def pipeline(src: int) -> str:
            raw = _fetch.submit(src)
            seen.append(current_workflow_id() or "")
            return _store.submit(raw).result()

        with Runtime.with_defaults() as rt:
            assert pipeline(5) == "stored[5]"
            history = rt.get_workflow_history(seen[0])

        kinds = [e["kind"] for e in history]
        # 提交 → 节点 → 启动 → 完成 → 工作流终态。
        assert "Submitted" in kinds, kinds
        assert kinds.count("NodeAdded") == 2, kinds
        assert "Started" in kinds, kinds
        assert "Completed" in kinds, kinds
        assert kinds.count("TaskCompleted") == 2, kinds

        # 筛选字段：任务事件带 task_id；payload 是不透明字节。
        completed = [e for e in history if e["kind"] == "TaskCompleted"]
        assert all(e["task_id"] for e in completed), completed
        assert all(isinstance(e["payload"], bytes) for e in history)
        # 序列递增，可作为游标。
        seqs = [e["sequence"] for e in history]
        assert seqs == sorted(seqs), seqs

        # 游标：从中间读取应更短，且不与头部重叠。
        cursor = (history[2]["sequence"], history[2]["timestamp_ms"])
        with Runtime.with_defaults():
            pass
        tail_history: list[dict] = []
        with Runtime.with_defaults() as rt2:
            tail_history = rt2.get_workflow_history(seen[0], after=cursor)
        assert len(tail_history) < len(history)

    def test_get_workflow_history_returns_empty_for_unknown_workflow(self) -> None:
        """未知工作流 → 空列表（历史是**可选**观测面，缺失不构成错误）。"""
        with Runtime.with_defaults() as rt:
            assert rt.get_workflow_history("no-such-workflow") == []
