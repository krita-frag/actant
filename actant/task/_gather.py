"""并行等待原语 ``gather``。"""

from __future__ import annotations

import time
from typing import Any

from actant.exceptions import ActantTimeoutError, TaskCancelledError
from actant.task._async_result import AsyncResult


def gather(
    *handles: AsyncResult,
    timeout: float | None = None,
    return_exceptions: bool = False,
) -> list[Any]:
    """并行等待多个 ``AsyncResult`` 完成，返回结果列表。

    与 ``asyncio.gather`` / ``ray.get`` 语义一致：所有任务并行执行，
    等待全部完成后返回结果。与顺序 ``result()`` 不同，``gather`` 等待的是
    **最慢**的任务而非所有任务时长之和。

    Args:
        *handles: 一个或多个 ``AsyncResult``。
        timeout: 整体最大等待秒数，``None`` 表示无限等待。超时抛 ``ActantTimeoutError``。
        return_exceptions: ``True`` 时，失败任务的结果为异常对象而非抛出；
            ``False``（默认）时，任一任务失败立即抛出该异常（其他任务不受影响）。

    Returns:
        结果列表，顺序与输入 ``handles`` 一致。

    Raises:
        ActantTimeoutError: 等待超时。
        ActantError: 任一任务失败（``return_exceptions=False`` 时）。
        ValueError: 无 handles。

    用法::

        a = task1.submit(x)
        b = task2.submit(y)
        c = task3.submit(z)
        results = actant.gather(a, b, c)  # 并行等待，返回 [ra, rb, rc]
    """
    if not handles:
        raise ValueError("gather() requires at least one AsyncResult")
    # 等待所有 handle 完成，超时检查
    deadline = None if timeout is None else time.monotonic() + timeout
    for h in handles:
        remaining = None if deadline is None else max(0, deadline - time.monotonic())
        if not h.wait(timeout=remaining):
            raise ActantTimeoutError(
                f"gather: at least one handle did not complete within {timeout}s"
            )

    results: list[Any] = []
    for h in handles:
        if return_exceptions:
            # exception() 对已完成的 handle 立即返回异常对象；成功时返回 None。
            # 因此成功任务需要再调用 result() 获取实际返回值。
            try:
                exc = h.exception(timeout=0)
            except TaskCancelledError:
                # 已取消任务：作为异常收集（与 asyncio.gather 的 return_exceptions
                # 语义一致，取消也视为异常）。
                results.append(TaskCancelledError(f"task {h.task_id!r} was cancelled"))
                continue
            if exc is None:
                results.append(h.result(timeout=0))
            else:
                results.append(exc)
        else:
            # result() 对已完成的 handle 立即返回或抛出异常。
            results.append(h.result(timeout=0))
    return results


__all__ = ["gather"]
