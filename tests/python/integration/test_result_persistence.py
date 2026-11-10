"""终态结果持久化契约：独立任务结果与 workflow 返回值跨重启可查。

- 独立任务：成功结果（小结果内联帧字节 / 大结果只存 Ref）与失败文本写入
  ``data_dir`` 的 Store，``Runtime.get_task_result(task_id)`` 查询。
- workflow：函数体返回值在正常结束时写入 Store，
  ``Runtime.get_workflow_result(workflow_id)`` 查询。
- 未知 id → ``NotFoundError``；失败任务 → 重抛记录的错误（保留 kind）。
"""

from __future__ import annotations

import time

import pytest

from actant import Runtime, flow, task, use_runtime
from actant.exceptions import NotFoundError
from actant.workflow import continue_as_new

_LARGE_SIZE = 2 * 1024 * 1024


@task(name="rp_echo")
def _echo(value):  # type: ignore[no-untyped-def]
    return value


@task(name="rp_boom")
def _boom() -> None:
    raise ValueError("boom persisted")


@flow(name="rp-value-flow")
def _value_flow() -> dict[str, int]:
    _echo.submit(1).result()
    return {"answer": 42}


@flow(name="rp-chain-flow")
def _chain_flow(round_no: int) -> int:
    _echo.submit(round_no).result()
    if round_no == 0:
        continue_as_new(1)
    return 100 + round_no


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


class TestTaskResultPersistence:
    def test_small_result_survives_restart(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        value = {"value": [1, 2, 3]}

        rt = Runtime(name="rp-task", data_dir=data_dir)
        with rt, use_runtime(rt):
            handle = _echo.submit(value)
            assert handle.result() == value
            task_id = handle.task_id
            # 同进程查询：result() 返回后立即可读（记录先于唤醒写入）。
            assert rt.get_task_result(task_id) == value

        rt2 = Runtime(name="rp-task-b", data_dir=data_dir)
        with rt2:
            assert rt2.get_task_result(task_id) == value

    def test_large_result_survives_restart(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        value = b"z" * _LARGE_SIZE

        rt = Runtime(name="rp-large", data_dir=data_dir)
        with rt, use_runtime(rt):
            handle = _echo.submit(value)
            assert handle.result() == value
            task_id = handle.task_id
            assert rt.get_task_result(task_id) == value

        rt2 = Runtime(name="rp-large-b", data_dir=data_dir)
        with rt2:
            assert rt2.get_task_result(task_id) == value

    def test_failure_is_recorded_and_unknown_task_is_distinct(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")

        rt = Runtime(name="rp-fail", data_dir=data_dir)
        with rt, use_runtime(rt):
            handle = _boom.submit()
            # 同进程结果通道保留原始异常对象。
            with pytest.raises(ValueError, match="boom persisted"):
                handle.result()
            task_id = handle.task_id
            # 持久化记录保存异常对象本身，类型与同进程 result() 一致。
            with pytest.raises(ValueError, match="boom persisted"):
                rt.get_task_result(task_id)

        rt2 = Runtime(name="rp-fail-b", data_dir=data_dir)
        with rt2:
            # 已记录但失败 → 重抛同一异常类型，与"从没存在过"的 NotFoundError 区分。
            with pytest.raises(ValueError, match="boom persisted"):
                rt2.get_task_result(task_id)
            with pytest.raises(NotFoundError):
                rt2.get_task_result("task-that-never-existed")


class TestWorkflowResultPersistence:
    def test_return_value_survives_restart(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        events = _Events()

        rt = Runtime(name="rp-flow", data_dir=data_dir)
        rt.layer("WorkflowLifecycle", "emit").chain(events)
        with rt, use_runtime(rt):
            assert _value_flow() == {"answer": 42}
            workflow_id = events.await_workflow_id()
            assert rt.get_workflow_result(workflow_id) == {"answer": 42}

        rt2 = Runtime(name="rp-flow-b", data_dir=data_dir)
        with rt2:
            assert rt2.get_workflow_result(workflow_id) == {"answer": 42}
            with pytest.raises(NotFoundError):
                rt2.get_workflow_result("workflow-that-never-existed")

    def test_completed_event_can_read_result_immediately(self, tmp_path) -> None:
        """``completed`` 处理器应能立即查到返回值（返回值先落盘、再广播）。"""
        data_dir = str(tmp_path / "data")
        observed: dict[str, object] = {}

        rt = Runtime(name="rp-order", data_dir=data_dir)

        def _on_event(e) -> None:  # type: ignore[no-untyped-def]
            if e.kind != "completed":
                return
            try:
                observed["value"] = rt.get_workflow_result(e.workflow_id)
            except Exception as exc:
                observed["error"] = exc

        rt.layer("WorkflowLifecycle", "emit").chain(_on_event)
        with rt, use_runtime(rt):
            assert _value_flow() == {"answer": 42}

        assert "error" not in observed, observed
        assert observed.get("value") == {"answer": 42}

    def test_continue_as_new_chain_resolves_from_any_generation(self, tmp_path) -> None:
        """续跑链只在最终代记录返回值；用首代 id 查询应沿链取到同一结果。"""
        data_dir = str(tmp_path / "data")
        events = _Events()

        rt = Runtime(name="rp-chain", data_dir=data_dir)
        rt.layer("WorkflowLifecycle", "emit").chain(events)
        with rt, use_runtime(rt):
            assert _chain_flow(0) == 101
            first_id = events.await_workflow_id()
            assert rt.get_workflow_result(first_id) == 101

        rt2 = Runtime(name="rp-chain-b", data_dir=data_dir)
        with rt2:
            assert rt2.get_workflow_result(first_id) == 101


class TestBatchedPersistence:
    def test_many_results_are_persisted(self, tmp_path) -> None:
        """合并提交路径下，一批独立任务的记录全部可见（同进程与重启后）。"""
        data_dir = str(tmp_path / "data")
        values = list(range(12))

        rt = Runtime(name="rp-batch", data_dir=data_dir)
        with rt, use_runtime(rt):
            handles = [_echo.submit(value) for value in values]
            for value, handle in zip(values, handles, strict=True):
                assert handle.result() == value
            task_ids = [handle.task_id for handle in handles]

        rt2 = Runtime(name="rp-batch-b", data_dir=data_dir)
        with rt2:
            assert [rt2.get_task_result(task_id) for task_id in task_ids] == values


class TestRecordRetention:
    def test_forget_task_removes_record(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        rt = Runtime(name="rp-forget-task", data_dir=data_dir)
        with rt, use_runtime(rt):
            handle = _echo.submit("value")
            assert handle.result() == "value"
            task_id = handle.task_id
            assert rt.get_task_result(task_id) == "value"
            rt.forget("task", task_id)
            with pytest.raises(NotFoundError):
                rt.get_task_result(task_id)
            # 幂等：重复删除不报错。
            rt.forget("task", task_id)

    def test_forget_workflow_result_removes_record(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        events = _Events()

        rt = Runtime(name="rp-forget-flow", data_dir=data_dir)
        rt.layer("WorkflowLifecycle", "emit").chain(events)
        with rt, use_runtime(rt):
            assert _value_flow() == {"answer": 42}
            workflow_id = events.await_workflow_id()
            assert rt.get_workflow_result(workflow_id) == {"answer": 42}
            rt.forget("workflow_result", workflow_id)
            with pytest.raises(NotFoundError):
                rt.get_workflow_result(workflow_id)
