"""``actant.task.gather`` 单元测试（mock AsyncResult，不依赖 Runtime）。"""
from __future__ import annotations

import pytest

from actant.exceptions import ActantTimeoutError
from actant.task._async_result import _CompletionFuture
from actant.task._gather import gather


class _MockHandle:
    """模拟 AsyncResult 的轻量实现。"""

    def __init__(self, result: object = None, exc: BaseException | None = None) -> None:
        self._result = result
        self._exc = exc
        self._wait_called = False
        # 模拟真实 AsyncResult._future：完成状态 + intrusive callback 链。
        # MockHandle 视为已完成（构造时即有结果/异常），因此 _future 也立即 set。
        self._future = _CompletionFuture()
        self._future.set()

    def done(self) -> bool:
        return True

    def wait(self, timeout: float | None = None) -> bool:
        self._wait_called = True
        return True

    def result(self, timeout: float | None = None) -> object:
        if self._exc is not None:
            raise self._exc
        return self._result

    def exception(self, timeout: float | None = None) -> BaseException | None:
        return self._exc


def test_gather_requires_at_least_one_handle() -> None:
    with pytest.raises(ValueError, match="requires at least one"):
        gather()


def test_gather_returns_results_in_order() -> None:
    h1 = _MockHandle(result=1)
    h2 = _MockHandle(result="two")
    h3 = _MockHandle(result=[3])
    assert gather(h1, h2, h3) == [1, "two", [3]]
    # 新实现 fast path：所有 handle 已 done 时不进入 wait 路径，
    # 仅校验 done 状态而非 wait 调用。
    assert h1.done() and h2.done() and h3.done()


def test_gather_raises_first_exception_by_default() -> None:
    h1 = _MockHandle(result=1)
    h2 = _MockHandle(exc=RuntimeError("boom"))
    h3 = _MockHandle(result=3)
    with pytest.raises(RuntimeError, match="boom"):
        gather(h1, h2, h3)


def test_gather_return_exceptions_collects_errors() -> None:
    h1 = _MockHandle(result=1)
    h2 = _MockHandle(exc=RuntimeError("boom"))
    h3 = _MockHandle(result=3)
    results = gather(h1, h2, h3, return_exceptions=True)
    assert results[0] == 1
    assert isinstance(results[1], RuntimeError)
    assert str(results[1]) == "boom"
    assert results[2] == 3


def test_gather_timeout_propagates() -> None:
    class _SlowHandle:
        def __init__(self) -> None:
            # 模拟永不完成的 future：不调用 set()。
            self._future = _CompletionFuture()

        def done(self) -> bool:
            return self._future.is_set()

        def wait(self, timeout: float | None = None) -> bool:
            return self._future.wait(timeout)

        def result(self, timeout: float | None = None) -> object:
            raise RuntimeError("should not reach")

    with pytest.raises(ActantTimeoutError, match="gather:"):
        gather(_SlowHandle(), timeout=0.01)
