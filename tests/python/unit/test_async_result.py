"""``actant.task._async_result`` 单元测试（不依赖 Runtime）。"""
from __future__ import annotations

import importlib

import cloudpickle
import pytest

from actant.exceptions import ActantError, ActantTimeoutError
from actant.task._async_result import AsyncResult, _resolve_value
from actant.task._context import TaskContext

_async_result_module = importlib.import_module("actant.task._async_result")


class _FakeRustCore:
    def __init__(self, *, broadcast_cancel_fails: bool = False) -> None:
        self.broadcast_cancel_fails = broadcast_cancel_fails
        self.broadcast_cancel_calls: list[tuple[str, str]] = []
        self.cancel_task_calls: list[str] = []
        self.mark_task_cancelled_calls: list[str] = []

    def cancel_task(self, task_id: str) -> None:
        self.cancel_task_calls.append(task_id)

    def broadcast_cancel(self, task_id: str, workflow_id: str) -> None:
        self.broadcast_cancel_calls.append((task_id, workflow_id))
        if self.broadcast_cancel_fails:
            raise RuntimeError("broadcast boom")


class _FakeRuntime:
    def __init__(self, *, broadcast_cancel_fails: bool = False) -> None:
        self._rust_core = _FakeRustCore(broadcast_cancel_fails=broadcast_cancel_fails)
        self._tasks: dict[str, AsyncResult] = {}
        self._cancelled: set[str] = set()

    def _mark_task_cancelled(self, task_id: str) -> None:
        self._cancelled.add(task_id)

    def list_tasks(self) -> list[str]:
        return list(self._tasks.keys())

    def get_task(self, task_id: str) -> AsyncResult | None:
        return self._tasks.get(task_id)

    def cancel_task(self, task_id: str) -> None:
        handle = self._tasks.get(task_id)
        if handle is not None:
            handle.cancel(propagate=False)

    def register(self, handle: AsyncResult) -> None:
        self._tasks[handle.task_id] = handle


def test_initial_state() -> None:
    h = AsyncResult("t1")
    assert h.state == "pending"
    assert not h.done()
    assert h.wait(timeout=0) is False


def test_set_running_transitions_from_pending() -> None:
    h = AsyncResult("t1")
    h._set_running()
    assert h.state == "running"
    assert not h.done()


def test_set_result() -> None:
    h = AsyncResult("t1")
    h._set_result(42)
    assert h.state == "completed"
    assert h.done()
    assert h.result(timeout=0) == 42


def test_result_raises_on_timeout() -> None:
    h = AsyncResult("t1")
    with pytest.raises(ActantTimeoutError):
        h.result(timeout=0.01)


def test_set_error_bytes() -> None:
    h = AsyncResult("t1")
    exc = ValueError("boom")
    h._set_error(cloudpickle.dumps(exc))
    assert h.state == "failed"
    with pytest.raises(ValueError, match="boom"):
        h.result(timeout=0)


def test_set_error_string() -> None:
    h = AsyncResult("t1")
    h._set_error("plain error")
    assert h.state == "failed"
    with pytest.raises(ActantError, match="plain error"):
        h.result(timeout=0)


def test_cancel_sets_context() -> None:
    ctx = TaskContext("t1")
    h = AsyncResult("t1", context=ctx)
    assert h.cancel() is True
    assert ctx.is_cancelled()


def test_cancel_idempotent() -> None:
    ctx = TaskContext("t1")
    h = AsyncResult("t1", context=ctx)
    assert h.cancel() is True
    assert h.cancel() is True
    assert ctx.is_cancelled()


def test_cancel_notifies_rust_core(monkeypatch: pytest.MonkeyPatch) -> None:
    h = AsyncResult("t1", workflow_id="wf1")
    runtime = _FakeRuntime()

    monkeypatch.setattr(_async_result_module, "get_current_runtime", lambda: runtime)
    assert h.cancel() is True
    assert "t1" in runtime._rust_core.cancel_task_calls
    assert ("t1", "wf1") in runtime._rust_core.broadcast_cancel_calls


def test_cancel_rust_core_failure_logged_but_continues(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """cancel_task 失败不静默：记录 warning，且本地取消流程继续（尽力而为语义）。"""

    class _FailingCancelCore(_FakeRustCore):
        def cancel_task(self, task_id: str) -> None:
            raise RuntimeError("rust core gone")

    h = AsyncResult("t1", workflow_id="wf1")
    runtime = _FakeRuntime()
    runtime._rust_core = _FailingCancelCore()

    monkeypatch.setattr(_async_result_module, "get_current_runtime", lambda: runtime)
    with caplog.at_level("WARNING", logger="actant.task"):
        assert h.cancel() is True
    assert any(
        "cancel_task failed for" in rec.getMessage() for rec in caplog.records
    )


def test_cancel_completed_returns_false() -> None:
    h = AsyncResult("t1")
    h._set_result(42)
    assert h.cancel() is False


def test_cancel_broadcast_failure_continues_cascade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2P broadcast_cancel 失败不应阻止本地级联取消。"""
    ctx1 = TaskContext("t1", workflow_id="wf1")
    ctx2 = TaskContext("t2", workflow_id="wf1")
    ctx3 = TaskContext("t3", workflow_id="wf2")
    h1 = AsyncResult("t1", workflow_id="wf1", context=ctx1)
    h2 = AsyncResult("t2", workflow_id="wf1", context=ctx2)
    h3 = AsyncResult("t3", workflow_id="wf2", context=ctx3)

    runtime = _FakeRuntime(broadcast_cancel_fails=True)
    runtime.register(h1)
    runtime.register(h2)
    runtime.register(h3)

    monkeypatch.setattr(_async_result_module, "get_current_runtime", lambda: runtime)
    assert h1.cancel(propagate=True) is True

    # broadcast 失败被记录并继续，h2 仍应被级联取消
    assert ctx2.is_cancelled()
    # 不同 workflow 不受影响
    assert not ctx3.is_cancelled()


def test_wait_timeout() -> None:
    h = AsyncResult("t1")
    assert h.wait(timeout=0.01) is False


def test_wait_until_done() -> None:
    h = AsyncResult("t1")

    def _resolve_later() -> None:
        import time

        time.sleep(0.05)
        h._set_result(42)

    import threading

    threading.Thread(target=_resolve_later, daemon=True).start()
    assert h.wait(timeout=1.0) is True
    assert h.result(timeout=0) == 42


def test_resolve_value_asyncresult() -> None:
    h = AsyncResult("t1")
    h._set_result(42)
    assert _resolve_value(h) == 42


def test_resolve_value_nested_containers() -> None:
    h1 = AsyncResult("t1")
    h2 = AsyncResult("t2")
    h1._set_result(1)
    h2._set_result(2)
    value = _resolve_value({"items": [h1, (h2,)]})
    assert value == {"items": [1, (2,)]}


def test_repr() -> None:
    h = AsyncResult("t1")
    assert "t1" in repr(h)
    assert "pending" in repr(h)


# ───────────────────────── P0-6 并发回归 ─────────────────────────


def test_add_done_callback_not_lost_when_racing_completion() -> None:
    """P0-6 回归：``add_done_callback`` 与 ``_set_result`` 并发时回调不得丢失。

    完成协议在锁内置终态、锁外 set future；若 ``add_done_callback`` 以
    ``is_set()`` 判定完成，窗口内新回调会 append 进已清空的列表而永久丢失。
    此测试循环多轮，每轮多线程经 barrier 同时注册回调与完成任务，
    断言每个回调恰好被调用一次。
    """
    for round_index in range(1000):
        _race_round(round_index)


def _race_round(round_index: int) -> None:
    """单轮竞态：2 个回调注册线程与 1 个完成线程同时起跑，回调各恰好触发一次。"""
    import threading

    adders = 2
    h = AsyncResult(f"race-{round_index}")
    counts = [0] * adders
    counts_lock = threading.Lock()
    barrier = threading.Barrier(adders + 1)

    def _bump(i: int) -> None:
        with counts_lock:
            counts[i] += 1

    def _adder(i: int) -> None:
        barrier.wait()
        h.add_done_callback(lambda _h, i=i: _bump(i))

    def _completer() -> None:
        barrier.wait()
        h._set_result(round_index)

    threads = [
        threading.Thread(target=_adder, args=(i,), daemon=True)
        for i in range(adders)
    ]
    threads.append(threading.Thread(target=_completer, daemon=True))
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)
        assert not t.is_alive(), f"round {round_index}: thread did not finish"

    assert all(c == 1 for c in counts), (
        f"round {round_index}: callback counts {counts} (lost or duplicated)"
    )
    assert h.done()
    assert h.result(timeout=0) == round_index
