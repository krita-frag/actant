"""``actant.task._context`` 单元测试（不依赖 Runtime）。"""
from __future__ import annotations

import threading
import time

import pytest

from actant.exceptions import TaskCancelledError
from actant.task._context import TaskContext, _task_context_scope


def test_task_context_initially_not_cancelled() -> None:
    ctx = TaskContext("t1")
    assert not ctx.is_cancelled()
    assert ctx.task_id == "t1"
    assert ctx.workflow_id == ""


def test_task_context_cancel_sets_event() -> None:
    ctx = TaskContext("t2")
    assert ctx._cancel()
    assert ctx.is_cancelled()
    assert not ctx._cancel()  # 幂等


def test_task_context_raise_if_cancelled() -> None:
    ctx = TaskContext("t3")
    ctx._cancel()
    with pytest.raises(TaskCancelledError):
        ctx.raise_if_cancelled()


def test_task_context_on_cancel_invoked_immediately_if_already_cancelled() -> None:
    ctx = TaskContext("t4")
    called = threading.Event()
    ctx._cancel()
    ctx.on_cancel(lambda: called.set())
    assert called.wait(timeout=0.5)


def test_task_context_on_cancel_invoked_later() -> None:
    ctx = TaskContext("t5")
    called = threading.Event()
    ctx.on_cancel(lambda: called.set())
    assert not called.is_set()
    ctx._cancel()
    assert called.wait(timeout=0.5)


def test_task_context_callback_only_once() -> None:
    ctx = TaskContext("t6")
    count = [0]

    def inc() -> None:
        count[0] += 1

    ctx.on_cancel(inc)
    ctx._cancel()
    ctx._cancel()
    # 给第二次 _invoke_callbacks 一点时间（如果有的话）
    time.sleep(0.05)
    assert count[0] == 1


def test_task_context_force_after_triggers_callback() -> None:
    ctx = TaskContext("t7")
    called = threading.Event()
    ctx.on_cancel(lambda: called.set(), force_after=0.05)
    assert not called.is_set()
    assert called.wait(timeout=0.3)


def test_task_context_callbacks_are_sequential_and_swallow_exceptions() -> None:
    ctx = TaskContext("t8")
    order: list[int] = []

    def bad() -> None:
        order.append(1)
        raise RuntimeError("oops")

    def good() -> None:
        order.append(2)

    ctx.on_cancel(bad)
    ctx.on_cancel(good)
    ctx._cancel()
    assert order == [1, 2]


def test_task_context_scope_sets_and_restores() -> None:
    from actant.task._context import get_task_context

    prev = TaskContext("prev")
    next_ctx = TaskContext("next")
    with _task_context_scope(prev):
        assert get_task_context() is prev
        with _task_context_scope(next_ctx):
            assert get_task_context() is next_ctx
        assert get_task_context() is prev


def test_get_task_context_outside_scope_returns_none() -> None:
    from actant.task._context import get_task_context

    assert get_task_context() is None


def test_migrate_callbacks_to_copies_callbacks_and_cancel_state() -> None:
    old = TaskContext("old")
    new = TaskContext("new")
    called = threading.Event()
    old.on_cancel(lambda: called.set())
    old._cancel()

    old._migrate_callbacks_to(new)
    assert called.wait(timeout=0.5)
    assert new.is_cancelled()
