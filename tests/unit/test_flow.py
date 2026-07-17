"""``actant.flow`` 私有辅助函数单元测试（不依赖 Runtime）。"""
from __future__ import annotations

import importlib

import pytest

from actant.flow import _safe_emit

_flow_module = importlib.import_module("actant.flow")


class _EmitFaker:
    """用于模拟 ``emit`` 调用失败的 callable。"""

    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc

    def __call__(self, *args: object, **kwargs: object) -> None:
        if self.exc is not None:
            raise self.exc


def test_safe_emit_on_error_log_swallows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_flow_module, "emit", _EmitFaker(RuntimeError("emit boom")))
    result = _safe_emit("wf1", "started", on_error="log")
    assert result is None


def test_safe_emit_on_error_raise_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    exc = RuntimeError("emit boom")
    monkeypatch.setattr(_flow_module, "emit", _EmitFaker(exc))
    with pytest.raises(RuntimeError, match="emit boom"):
        _safe_emit("wf1", "started", on_error="raise")


def test_safe_emit_on_error_collect_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    exc = RuntimeError("emit boom")
    monkeypatch.setattr(_flow_module, "emit", _EmitFaker(exc))
    result = _safe_emit("wf1", "started", on_error="collect")
    assert result is exc


class _FakeHandle:
    def __init__(self, workflow_id: str, fail: bool = False) -> None:
        self.workflow_id = workflow_id
        self.fail = fail
        self.cancelled = False

    def cancel(self, *, propagate: bool = False) -> None:
        if self.fail:
            raise RuntimeError("cancel boom")
        self.cancelled = True


class _FakeRuntime:
    def __init__(self, handles: dict[str, _FakeHandle]) -> None:
        self._handles = handles

    def list_tasks(self) -> list[str]:
        return list(self._handles.keys())

    def get_task(self, task_id: str) -> _FakeHandle | None:
        return self._handles.get(task_id)


def test_cancel_flow_tasks_continues_on_single_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """单个任务取消失败不应阻止同 workflow 其他任务被取消。"""
    import actant._runtime

    handles = {
        "t1": _FakeHandle("wf1"),
        "t2": _FakeHandle("wf1", fail=True),
        "t3": _FakeHandle("wf1"),
        "t4": _FakeHandle("wf2"),
    }
    monkeypatch.setattr(
        actant._runtime, "get_current_runtime", lambda: _FakeRuntime(handles)
    )

    _flow_module._cancel_flow_tasks("wf1")

    assert handles["t1"].cancelled is True
    assert handles["t2"].cancelled is False  # 失败的任务未被标记
    assert handles["t3"].cancelled is True  # 失败后仍继续
    assert handles["t4"].cancelled is False  # 不同 workflow 不受影响


def test_cancel_flow_tasks_no_runtime_returns() -> None:
    """无活跃 Runtime 时直接返回。"""
    import actant._runtime

    monkeypatch = pytest.MonkeyPatch()
    with monkeypatch.context() as m:
        m.setattr(actant._runtime, "get_current_runtime", lambda: None)
        _flow_module._cancel_flow_tasks("wf1")
