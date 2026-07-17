"""``actant.task._dispatch._generic_execute_handler`` 直接单元测试。"""
from __future__ import annotations

from typing import Any, cast

import cloudpickle
import pytest

from actant.capabilities import ExecuteCtx, ExecuteOutcome
from actant.exceptions import ActantError, TaskCancelledError
from actant.task._dispatch import _generic_execute_handler


def _ok_fn(x: int) -> int:
    return x * 2


def _fail_fn() -> None:
    raise ValueError("boom")


def _payload(func: object, args: tuple[Any, ...], kwargs: dict[str, Any], options: dict[str, Any]) -> bytes:
    return cast(bytes, cloudpickle.dumps((func, args, kwargs, options)))


def test_generic_success() -> None:
    payload = _payload(_ok_fn, (21,), {}, {"retries": 0, "timeout_ms": 0})
    ctx = ExecuteCtx(task_id="t1", workflow_id="", payload=payload)
    outcome = _generic_execute_handler(ctx)
    assert isinstance(outcome, ExecuteOutcome)
    assert cloudpickle.loads(outcome.result_payload) == 42
    assert outcome.error_payload == b""


def test_generic_failure() -> None:
    payload = _payload(_fail_fn, (), {}, {"retries": 0, "timeout_ms": 0})
    ctx = ExecuteCtx(task_id="t2", workflow_id="", payload=payload)
    outcome = _generic_execute_handler(ctx)
    assert outcome.result_payload == b""
    exc = cloudpickle.loads(outcome.error_payload)
    assert isinstance(exc, ValueError)
    assert str(exc) == "boom"


_flaky_counter: dict[str, int] = {"count": 0}


def _flaky_fn() -> str:
    import sys

    mod = sys.modules[__name__]
    mod._flaky_counter["count"] += 1
    if mod._flaky_counter["count"] < 3:
        raise RuntimeError("not yet")
    return "ok"


def test_generic_retry_then_success() -> None:
    _flaky_counter["count"] = 0
    payload = _payload(_flaky_fn, (), {}, {"retries": 3, "retry_delay_ms": 0, "timeout_ms": 0})
    ctx = ExecuteCtx(task_id="t3", workflow_id="", payload=payload)
    outcome = _generic_execute_handler(ctx)
    assert cloudpickle.loads(outcome.result_payload) == "ok"
    assert _flaky_counter["count"] == 3


def test_generic_retry_exhausted() -> None:
    payload = _payload(_fail_fn, (), {}, {"retries": 2, "retry_delay_ms": 0, "timeout_ms": 0})
    ctx = ExecuteCtx(task_id="t4", workflow_id="", payload=payload)
    outcome = _generic_execute_handler(ctx)
    exc = cloudpickle.loads(outcome.error_payload)
    assert isinstance(exc, ValueError)


def test_generic_timeout_ignored_by_python_handler() -> None:
    # Python 侧 _run_with_timeout 不再实现超时（由 Rust Worker 控制），
    # 因此 timeout_ms 仅作为参数透传；短时间任务不会触发异常。
    payload = _payload(_ok_fn, (1,), {}, {"retries": 0, "timeout_ms": 100})
    ctx = ExecuteCtx(task_id="t5", workflow_id="", payload=payload)
    outcome = _generic_execute_handler(ctx)
    assert cloudpickle.loads(outcome.result_payload) == 2


def test_generic_deserialization_error() -> None:
    ctx = ExecuteCtx(task_id="t6", workflow_id="", payload=b"not-pickle")
    with pytest.raises(ActantError, match="deserialize"):
        _generic_execute_handler(ctx)


def test_generic_cancel_before_task() -> None:
    from actant.task._context import TaskContext, _task_context_scope

    ctx_obj = TaskContext("t7")
    ctx_obj._cancel()
    payload = _payload(_ok_fn, (1,), {}, {"retries": 0, "timeout_ms": 0})
    # 注意 _generic_execute_handler 内部通过 get_task_context() 检查取消，
    # 所以先设置线程局部 context。
    with _task_context_scope(ctx_obj):
        out_ctx = ExecuteCtx(task_id="t7", workflow_id="", payload=payload)
        outcome = _generic_execute_handler(out_ctx)
    assert outcome.result_payload == b""
    exc = cloudpickle.loads(outcome.error_payload)
    assert isinstance(exc, TaskCancelledError)
