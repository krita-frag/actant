"""续跑驱动单元测试：flow 名恢复、恢复登记、``_replay_node_outcome`` 四分支。

这些是"纯映射逻辑"的测试，与真实重放解耦：真实重放（已完成节点不重跑、
缺口节点补执行）由 ``tests/python/integration/test_flow_resume.py`` 覆盖。
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

from actant import Runtime, flow, register_flow_recovery, resume_flows
from actant.exceptions import InvalidStateError
from actant.flow import _FLOW_RECOVERY, _flow_name_from_id, _replay_node_outcome

#: ``actant.flow`` 这个**属性**在包命名空间里被同名的 ``flow`` 装饰器遮蔽，
#: 故必须经 ``sys.modules`` 拿模块对象（否则 ``import actant.flow as m`` 会绑到函数）。
_FLOW_MODULE = sys.modules["actant.flow"]


class _FakeRuntime:
    """记录 ``_on_task_result`` 调用的假 Runtime（``_replay_node_outcome`` 的出参）。"""

    def __init__(self) -> None:
        self.results: list[Any] = []

    def _on_task_result(self, event: Any) -> None:
        self.results.append(event)


class _FakeHandle:
    """记录 ``_set_error`` 的假句柄。"""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.error: str | None = None

    def _set_error(self, message: str) -> None:
        self.error = message


class TestFlowNameFromId:
    """``{flow_name}-{uuid8}`` → flow 名。"""

    @pytest.mark.parametrize(
        ("workflow_id", "expected"),
        [
            ("etl-1a2b3c4d", "etl"),
            ("my-flow-1a2b3c4d", "my-flow"),
            ("a-00000000", "a"),
        ],
    )
    def test_recovers_flow_name(self, workflow_id: str, expected: str) -> None:
        assert _flow_name_from_id(workflow_id) == expected

    @pytest.mark.parametrize(
        "workflow_id",
        [
            "wp-restart-wf",  # 尾段非 8 位
            "no-uuid-zzzzzzzz",  # 尾段非十六进制
            "nodash",
            "--",
        ],
    )
    def test_unrecognized_returns_none(self, workflow_id: str) -> None:
        assert _flow_name_from_id(workflow_id) is None


class TestRecoveryRegistry:
    """``@flow`` 导入登记 + 显式参数登记。"""

    def test_flow_registers_its_body(self) -> None:
        @flow(name="unit-registered")
        def pipeline() -> None: ...

        entry = _FLOW_RECOVERY["unit-registered"]
        assert entry.args == ()
        assert entry.kwargs == {}

    def test_register_flow_recovery_supplies_args(self) -> None:
        @flow(name="unit-with-args")
        def pipeline(src: str) -> None: ...

        register_flow_recovery("unit-with-args", pipeline, args=("data.txt",))
        entry = _FLOW_RECOVERY["unit-with-args"]
        assert entry.func.__name__ == "pipeline"
        assert entry.args == ("data.txt",)

    def test_register_flow_recovery_unwraps_decorated_flow(self) -> None:
        """传 ``@flow`` 后的对象必须被解包到原函数。

        若不解包，重放会再次进入包装器并生成**新的 workflow_id**——"重放"静默
        退化成"新建工作流从头跑"，已完成节点全部重跑。这是真实踩过的坑。
        """

        @flow(name="unit-unwrap")
        def pipeline() -> None: ...

        # 装饰后的对象带标记；原函数不带。
        assert getattr(pipeline, "__actant_flow_name__", None) == "unit-unwrap"

        register_flow_recovery("unit-unwrap", pipeline)
        entry = _FLOW_RECOVERY["unit-unwrap"]
        assert getattr(entry.func, "__actant_flow_name__", None) is None, (
            "恢复登记里的函数体必须是原函数，不能是 @flow 包装器"
        )
        assert entry.func.__name__ == "pipeline"

    def test_resume_flows_requires_runtime(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_FLOW_MODULE, "get_current_runtime", lambda: None)
        with pytest.raises(InvalidStateError):
            resume_flows()

    def test_resume_flows_skips_unregistered(self) -> None:
        """无恢复登记的非终态工作流：跳过 + 不抛错（warning 记录在日志）。"""
        with Runtime.with_defaults() as rt:
            # 尾段 8 位十六进制 → 可解析出 flow 名 "orphan"，但无登记。
            rt.submit_workflow("orphan-abcdef12")
            assert resume_flows(runtime=rt) == []


class TestReplayNodeOutcome:
    """``add_workflow_node`` 返回 ``created=False`` 时的四分支句柄重建。"""

    def test_completed_replays_result_through_ingest(self) -> None:
        rt = _FakeRuntime()
        handle = _FakeHandle("t-1")
        _replay_node_outcome(rt, handle, {"state": "Completed", "result": b"payload"})
        assert len(rt.results) == 1
        assert rt.results[0].state == "Completed"
        assert rt.results[0].result == b"payload"
        assert handle.error is None

    def test_failed_carries_history_error(self) -> None:
        rt = _FakeRuntime()
        handle = _FakeHandle("t-2")
        _replay_node_outcome(rt, handle, {"state": "Failed", "error": "boom"})
        assert rt.results[0].state == "Failed"
        assert rt.results[0].error == "boom"

    def test_failed_without_history_error_uses_placeholder(self) -> None:
        rt = _FakeRuntime()
        handle = _FakeHandle("t-2b")
        _replay_node_outcome(rt, handle, {"state": "Failed"})
        assert rt.results[0].error, "failed 分支必须带非空错误信息"

    def test_cancelled_marks_cancelled(self) -> None:
        rt = _FakeRuntime()
        handle = _FakeHandle("t-3")
        _replay_node_outcome(rt, handle, {"state": "Cancelled"})
        assert rt.results[0].state == "Cancelled"

    def test_skipped_sets_handle_error_only(self) -> None:
        rt = _FakeRuntime()
        handle = _FakeHandle("t-4")
        _replay_node_outcome(rt, handle, {"state": "Skipped"})
        assert rt.results == [], "Skipped 不经结果回灌路径"
        assert handle.error == "task skipped"

    @pytest.mark.parametrize("state", ["Pending", "Running"])
    def test_in_flight_leaves_handle_waiting(self, state: str) -> None:
        """在途/排队节点：不重建终态，保持等待（结果经事件回灌）。"""
        rt = _FakeRuntime()
        handle = _FakeHandle("t-5")
        _replay_node_outcome(rt, handle, {"state": state})
        assert rt.results == []
        assert handle.error is None


def test_replay_node_outcome_accepts_simplenamespace_shape() -> None:
    """``outcome`` 实际来自 Rust dict → Python 侧为 dict；这里锁定字段名契约。"""
    rt = _FakeRuntime()
    handle = _FakeHandle("t-6")
    outcome = SimpleNamespace(state="Completed", result=b"x", error=None)
    # dict 形态（生产路径）与 Mapping 形态都应被接受。
    _replay_node_outcome(rt, handle, {"state": outcome.state, "result": outcome.result})
    assert rt.results[0].result == b"x"
