"""result.py 单元测试：WorkflowResult / AsyncResult。

覆盖目标：100% 行覆盖 + 分支覆盖。
重点测试 AsyncResult 的同步/异步访问。
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from actant.result import (
    AsyncResult,
    WorkflowResult,
)

# ---------------------------------------------------------------------------
# WorkflowResult
# ---------------------------------------------------------------------------


class TestWorkflowResult:
    """WorkflowResult 包装类的基础行为。"""

    def test_construction_and_properties(self):
        wr = WorkflowResult(value=42, state="Completed", workflow_id="wf-1")
        assert wr.value == 42
        assert wr.state == "Completed"
        assert wr.workflow_id == "wf-1"
        assert wr.is_success is True

    def test_is_success_false_for_non_completed(self):
        wr = WorkflowResult(value=None, state="Failed", workflow_id="wf-2")
        assert wr.is_success is False

    def test_repr_format(self):
        wr = WorkflowResult(value=1, state="Completed", workflow_id="abc")
        assert repr(wr) == "<WorkflowResult id=abc state=Completed>"

    def test_bool_true_when_success(self):
        wr = WorkflowResult(value=None, state="Completed", workflow_id="x")
        assert bool(wr) is True

    def test_bool_false_when_failed(self):
        wr = WorkflowResult(value=None, state="Failed", workflow_id="x")
        assert bool(wr) is False


# ---------------------------------------------------------------------------
# AsyncResult — get_sync 路径
# ---------------------------------------------------------------------------


def _make_mock_core(raw: Any, workflow_id: str = "wf-1") -> MagicMock:
    """构造 mock _AsyncResultCore。"""
    core = MagicMock()
    core.workflow_id = workflow_id
    core.ready.return_value = True
    core.state.return_value = "Completed"
    # get 是 async 方法
    core.get = AsyncMock(return_value=raw)
    return core


class TestAsyncResultGetSync:
    """AsyncResult.get_sync 的三种事件循环场景。"""

    def test_get_sync_no_running_loop(self):
        """无运行中的事件循环 → asyncio.run()。"""
        from actant._serialization import dumps

        raw = {
            "state": "Completed",
            "results": [("t1", dumps(42))],
        }
        core = _make_mock_core(raw)
        ar = AsyncResult(core)

        result = ar.get_sync(timeout=1.0)
        assert result.value == 42
        assert result.state == "Completed"
        assert result.workflow_id == "wf-1"
        assert result.is_success is True

    def test_get_sync_with_running_loop_in_thread(self):
        """有运行中的事件循环 → 在独立线程中 asyncio.run()。"""
        from actant._serialization import dumps

        raw = {
            "state": "Completed",
            "results": [("t1", dumps("hello"))],
        }
        core = _make_mock_core(raw)
        ar = AsyncResult(core)

        async def driver():
            # 在已有运行 loop 的上下文中调用 get_sync
            return ar.get_sync(timeout=1.0)

        result = asyncio.run(driver())
        assert result.value == "hello"

    def test_get_sync_with_non_running_loop(self):
        """有事件循环但未运行 → loop.run_until_complete()。

        通过 mock get_running_loop 返回一个未运行的 loop，
        触发 295 行的 loop.run_until_complete 分支。
        """
        from unittest.mock import patch

        from actant._serialization import dumps

        raw = {
            "state": "Completed",
            "results": [("t1", dumps(99))],
        }
        core = _make_mock_core(raw)
        ar = AsyncResult(core)

        # 创建新 loop 但不运行
        loop = asyncio.new_event_loop()
        try:
            # mock get_running_loop 返回未运行的 loop（正常情况会抛 RuntimeError）
            with patch("asyncio.get_running_loop", return_value=loop):
                result = ar.get_sync(timeout=1.0)
                assert result.value == 99
        finally:
            loop.close()


# ---------------------------------------------------------------------------
# AsyncResult — get() 路径
# ---------------------------------------------------------------------------


class TestAsyncResultGet:
    """AsyncResult.get() 的结果解析。"""

    @pytest.mark.asyncio
    async def test_get_non_dict_raw(self):
        """raw 非 dict → 直接包装为 WorkflowResult。"""
        core = _make_mock_core("plain-string")
        ar = AsyncResult(core)
        result = await ar.get(timeout=1.0)
        assert result.value == "plain-string"
        assert result.state == "Completed"

    @pytest.mark.asyncio
    async def test_get_empty_results(self):
        """results 为空 → value=None。"""
        raw = {"state": "Completed", "results": []}
        core = _make_mock_core(raw)
        ar = AsyncResult(core)
        result = await ar.get(timeout=1.0)
        assert result.value is None

    @pytest.mark.asyncio
    async def test_get_single_result(self):
        """单个结果 → 直接返回值。"""
        from actant._serialization import dumps

        raw = {"state": "Completed", "results": [("t1", dumps(42))]}
        core = _make_mock_core(raw)
        ar = AsyncResult(core)
        result = await ar.get(timeout=1.0)
        assert result.value == 42

    @pytest.mark.asyncio
    async def test_get_multiple_results_as_list(self):
        """多个结果 → 返回列表。"""
        from actant._serialization import dumps

        raw = {
            "state": "Completed",
            "results": [("t1", dumps(1)), ("t2", dumps(2))],
        }
        core = _make_mock_core(raw)
        ar = AsyncResult(core)
        result = await ar.get(timeout=1.0)
        assert result.value == [1, 2]

    @pytest.mark.asyncio
    async def test_get_failed_state_raises(self):
        """Failed 状态应抛出异常。"""
        from actant.exceptions import WorkflowFailedError

        raw = {
            "state": "Failed",
            "error": "task crashed",
            "failed_tasks": [["t1", "add", "boom"]],
        }
        core = _make_mock_core(raw)
        ar = AsyncResult(core)
        with pytest.raises(WorkflowFailedError):
            await ar.get(timeout=1.0)

    @pytest.mark.asyncio
    async def test_get_cancelled_state_raises(self):
        """Cancelled 状态应抛出异常。"""
        from actant.exceptions import WorkflowCancelledError

        raw = {"state": "Cancelled", "error": "user cancelled"}
        core = _make_mock_core(raw)
        ar = AsyncResult(core)
        with pytest.raises(WorkflowCancelledError):
            await ar.get(timeout=1.0)

    @pytest.mark.asyncio
    async def test_get_timeout_state_raises(self):
        """Timeout 状态应抛出异常。"""
        from actant.exceptions import ActantTimeoutError

        raw = {"state": "Timeout", "error": "timed out"}
        core = _make_mock_core(raw)
        ar = AsyncResult(core)
        with pytest.raises(ActantTimeoutError):
            await ar.get(timeout=1.0)

    @pytest.mark.asyncio
    async def test_get_state_defaults_to_completed(self):
        """raw 无 state 字段 → 默认 Completed。"""
        from actant._serialization import dumps

        raw = {"results": [("t1", dumps(7))]}
        core = _make_mock_core(raw)
        ar = AsyncResult(core)
        result = await ar.get(timeout=1.0)
        assert result.state == "Completed"
        assert result.value == 7


# ---------------------------------------------------------------------------
# AsyncResult — 属性与 repr
# ---------------------------------------------------------------------------


class TestAsyncResultProperties:
    def test_workflow_id_property(self):
        core = _make_mock_core({}, workflow_id="wf-xyz")
        ar = AsyncResult(core)
        assert ar.workflow_id == "wf-xyz"

    def test_ready_delegates_to_core(self):
        core = _make_mock_core({})
        core.ready.return_value = False
        ar = AsyncResult(core)
        assert ar.ready() is False

    def test_state_delegates_to_core(self):
        core = _make_mock_core({})
        core.state.return_value = "Running"
        ar = AsyncResult(core)
        assert ar.state() == "Running"

    def test_repr_format(self):
        core = _make_mock_core({}, workflow_id="abc-123")
        ar = AsyncResult(core)
        assert repr(ar) == "<AsyncResult abc-123>"
