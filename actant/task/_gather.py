"""并行等待原语 ``gather``。"""

from __future__ import annotations

import threading
import asyncio
from typing import Any

from actant.exceptions import ActantTimeoutError, TaskCancelledError
from actant.task._async_result import AsyncResult, _set_aio_result


def gather(
    *handles: AsyncResult,
    timeout: float | None = None,
    return_exceptions: bool = False,
) -> list[Any]:
    """并行等待多个 ``AsyncResult`` 完成，返回结果列表。

    与 ``asyncio.gather`` / ``ray.get`` 语义一致：所有任务并行执行，
    等待全部完成后返回结果。与顺序 ``result()`` 不同，``gather`` 等待的是
    **最慢**的任务而非所有任务时长之和。

    ## 实现细节

    利用 ``AsyncResult._future`` 的 intrusive callback 链：在所有 handle
    上注册一个共享 ``threading.Event`` 的 set 操作，主线程仅等待该 event
    一次。当最慢的 handle 完成时，event 被 set，主线程立即唤醒——
    无需 N 次 ``wait_for`` 累加，避免超时检查的 N 倍开销。

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

    # Fast path: 若所有 handle 已完成，跳过共享 event 设置。
    pending = [h for h in handles if not h.done()]
    if pending:
        # 共享 event：任一 handle 完成时通过 intrusive callback set 它。
        # 主线程仅在此 event 上等待一次，避免 N 次 wait_for 累加延迟。
        shared_event = threading.Event()
        remaining_lock = threading.Lock()
        # 使用 list 包装剩余计数以便闭包内 mutable 访问。
        remaining: list[int] = [len(pending)]

        def _on_done() -> None:
            with remaining_lock:
                remaining[0] -= 1
                last = remaining[0] == 0
            if last:
                shared_event.set()

        for h in pending:
            # 注册 intrusive callback 到 handle 的 CompletionFuture。
            # 回调本身轻量（仅原子递减 + 条件 set），不阻塞 handle 完成路径。
            h._future.add_callback(_on_done)

        # 主线程等待：所有 handle 完成或超时。
        # 共享 event 仅在最慢 handle 完成时被 set，因此 wait() 实际等待时长
        # 等于 max(handle_i 完成时间)，而非 sum。
        if not shared_event.wait(timeout=timeout):
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


async def gather_async(
    *handles: AsyncResult,
    timeout: float | None = None,
    return_exceptions: bool = False,
) -> list[Any]:
    """异步并行等待多个 ``AsyncResult`` 完成。

    与 ``gather`` 语义一致，但返回 coroutine，可在 ``async def`` 函数中
    ``await``。在单个独立守护线程中执行同步 ``gather``，通过
    ``call_soon_threadsafe`` 将结果投递回 event loop。该线程在
    ``Condition.wait`` / ``Event.wait`` 中阻塞时释放 GIL，使 Rust worker
    线程能获取 GIL 执行 dispatch handler。event loop 线程不被阻塞。

    使用单线程执行 ``gather``（而非每个 handle 一个线程）避免多线程
    ``Condition.wait`` 之间的 GIL 竞争死锁。

    Args:
        *handles: 一个或多个 ``AsyncResult``。
        timeout: 整体最大等待秒数，``None`` 表示无限等待。
        return_exceptions: ``True`` 时失败任务的结果为异常对象而非抛出。

    Returns:
        结果列表，顺序与输入 ``handles`` 一致。

    Raises:
        ActantTimeoutError: 等待超时。
        ActantError: 任一任务失败（``return_exceptions=False`` 时）。
        ValueError: 无 handles。

    用法::

        async def my_flow():
            a = task1.submit(x)
            b = task2.submit(y)
            results = await actant.gather_async(a, b)
    """
    if not handles:
        raise ValueError("gather_async() requires at least one AsyncResult")

    loop = asyncio.get_running_loop()
    aio_future: asyncio.Future[list[Any]] = loop.create_future()

    # Fast path：若所有 handle 已完成，直接返回结果。
    if all(h.done() for h in handles):
        return gather(*handles, timeout=0, return_exceptions=return_exceptions)

    def _worker() -> None:
        """在独立线程中执行同步 gather，投递结果到 event loop。"""
        try:
            value = gather(
                *handles,
                timeout=timeout,
                return_exceptions=return_exceptions,
            )
            loop.call_soon_threadsafe(_set_aio_result, aio_future, value, None)
        except BaseException as exc:
            loop.call_soon_threadsafe(_set_aio_result, aio_future, None, exc)

    # 单线程执行 gather，避免多线程 GIL 竞争。
    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return await aio_future


__all__ = ["gather", "gather_async"]
