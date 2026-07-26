"""``actant.task._dispatch._bind_dispatch_handler`` 直接单元测试。"""
from __future__ import annotations

from typing import Any, cast
from unittest.mock import Mock

import cloudpickle

from actant.exceptions import TaskCancelledError
from actant.task import AsyncResult
from actant.task._dispatch import _bind_dispatch_handler


class _FakeToken:
    def __init__(self) -> None:
        self._cancelled = False

    def is_cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True


def _runtime_with_task(task_id: str, cancelled: bool = False) -> Mock:
    rt = Mock()
    handle = AsyncResult(task_id)
    if cancelled:
        handle._set_cancelled()
    rt.get_task.return_value = handle
    rt.is_cancelled.return_value = cancelled
    return rt


def _payload_bytes(func: object, args: tuple[Any, ...], kwargs: dict[str, Any], options: dict[str, Any]) -> bytes:
    return cast(bytes, cloudpickle.dumps((func, args, kwargs, options)))


def test_bound_handler_success() -> None:
    rt = _runtime_with_task("t1")
    handler = _bind_dispatch_handler(rt)
    payload = _payload_bytes(lambda x: x + 1, (1,), {}, {"task_id": "t1", "retries": 0})
    raw = handler(payload, _FakeToken())
    # P2-9 优化后 _handler 返回 cloudpickle.dumps((success, payload_obj))，
    # payload_obj 是对象本身（非 bytes），省去双层 dumps/loads。
    success, result_obj = cloudpickle.loads(raw)
    assert success is True
    assert result_obj == 2


def test_bound_handler_failure() -> None:
    rt = _runtime_with_task("t2")
    handler = _bind_dispatch_handler(rt)

    def boom() -> None:
        raise ValueError("boom")

    payload = _payload_bytes(boom, (), {}, {"task_id": "t2", "retries": 0})
    raw = handler(payload, _FakeToken())
    success, exc_obj = cloudpickle.loads(raw)
    assert success is False
    assert isinstance(exc_obj, ValueError)


def test_bound_handler_pre_cancelled() -> None:
    rt = _runtime_with_task("t3", cancelled=True)
    handler = _bind_dispatch_handler(rt)
    payload = _payload_bytes(lambda: 1, (), {}, {"task_id": "t3", "retries": 0})
    raw = handler(payload, _FakeToken())
    success, exc_obj = cloudpickle.loads(raw)
    assert success is False
    assert isinstance(exc_obj, TaskCancelledError)


def test_bound_handler_deserialization_error() -> None:
    rt = _runtime_with_task("t4")
    handler = _bind_dispatch_handler(rt)
    raw = handler(b"bad-payload", _FakeToken())
    success, exc_obj = cloudpickle.loads(raw)
    assert success is False
    assert isinstance(exc_obj, Exception)


def test_bound_handler_runtime_none() -> None:
    handler = _bind_dispatch_handler(None)
    payload = _payload_bytes(lambda: 42, (), {}, {"task_id": "t5", "retries": 0})
    raw = handler(payload, _FakeToken())
    success, result_obj = cloudpickle.loads(raw)
    assert success is True
    assert result_obj == 42


def test_bound_handler_cancel_token_during_execution() -> None:
    rt = _runtime_with_task("t6")
    handler = _bind_dispatch_handler(rt)
    token = _FakeToken()

    def check_cancel() -> str:
        for _ in range(100):
            if token.is_cancelled():
                return "cancelled"
            import time

            time.sleep(0.01)
        return "done"

    # 先启动 handler（会进入 sleep），然后在另一个线程取消 token。
    import threading

    def do_cancel() -> None:
        import time

        time.sleep(0.05)
        token.cancel()

    threading.Thread(target=do_cancel, daemon=True).start()
    payload = _payload_bytes(check_cancel, (), {}, {"task_id": "t6", "retries": 0, "timeout_ms": 0})
    raw = handler(payload, token)
    success, exc_obj = cloudpickle.loads(raw)
    assert success is False
    assert isinstance(exc_obj, TaskCancelledError)
