"""异步面：与同步面同名，靠命名空间区分。

同步 API 只保留"默认名"，异步 API 集中在本模块：

===================  ==========================  ==========================
语义                 同步（``actant`` 等）        异步（``actant.aio``）
===================  ==========================  ==========================
流程定义             ``@actant.flow``              ``@aio.flow``
决策型 effect        ``actant.ask``                ``await aio.ask``
副作用型 effect      ``actant.perform``            ``await aio.perform``
批量副作用           —                             ``await aio.perform_batch``
等待任务结果         ``handle.result()``           ``await handle``
并行等待             ``actant.gather``             ``await aio.gather``
===================  ==========================  ==========================

``aio.perform_batch`` 是异步面独有的批量原语；同步侧逐个调用
``actant.perform``（批量**任务**提交用 ``Task.submit_batch``，语义不同）。

异步 flow 由 :class:`aio.AsyncDriver` 驱动：
``actant.host_flows(driver=aio.AsyncDriver())``。

这样"混用"在阅读 import 时即可识别：异步动词只从 ``actant.aio`` 进入，
``actant`` 顶层保留同步名。跨边界的对象仍只有
:class:`~actant.tasks.AsyncResult` 一个——同步读 ``result()``，异步
``await``，阻塞调用在 event loop 中会立即报错而不是冻结循环。
"""

from __future__ import annotations

import asyncio
from typing import Any

from actant._internal.flow_async import AsyncFlowContext, flow
from actant._internal.runtime import get_current_runtime
from actant.driver import AsyncDriver
from actant.effects import _resolve_by_kind
from actant.exceptions import ActantTimeoutError, InvalidStateError
from actant.tasks import AsyncResult
from actant.tasks import gather as _gather


async def ask(name: str, request: Any) -> Any | None:
    """异步决策型 effect：``await`` 后得到第一个非 ``None`` 的 handler 结果。

    与同步 ``actant.ask`` 语义一致，多个 ask 可通过 ``aio.gather`` 并发。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中、capability kind 不匹配，
            或当前线程无运行中的 asyncio 事件循环。
        KeyError: capability 未注册。
    """
    runtime = _resolve_by_kind(name, "ask", "aio.ask")
    runtime._record_effect(name, "ask")
    return await runtime._dispatch_ask_async(name, request)


async def perform(name: str, request: Any) -> Any:
    """异步副作用型 effect：``await`` 后得到 handler 返回值。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中、capability kind 不匹配，
            或当前线程无运行中的 asyncio 事件循环。
        KeyError: capability 未注册。
    """
    runtime = _resolve_by_kind(name, "perform", "aio.perform")
    runtime._record_effect(name, "perform")
    return await runtime._dispatch_perform_async(name, request)


async def perform_batch(items: list[tuple[str, Any]]) -> list[Any]:
    """异步批量 perform：``await`` 后得到结果列表，顺序与输入一致。

    全部条目由 Rust 内置 handler 处理时走单次边界穿越（Rust 侧 ``join_all``
    并发执行，总延迟 ≈ max(单次)）；含 Python handler 时逐条在默认 executor
    中并发执行。单个 perform 失败不中断批量——失败项在结果列表对应位置为
    异常实例。空列表是合法 no-op，直接返回 ``[]``，不要求活跃 Runtime。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中，或当前线程无运行的 loop。
        TypeError: ``items`` 不是 ``list[tuple[str, Any]]``。
    """
    if not isinstance(items, list):
        raise TypeError(f"aio.perform_batch: items must be a list, got {type(items).__name__}")
    if not items:
        return []
    runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            "aio.perform_batch: no active Runtime; "
            "wrap your code in `with actant.Runtime() as rt:`"
        )
    for i, item in enumerate(items):
        if not (isinstance(item, tuple) and len(item) == 2):
            raise TypeError(
                f"aio.perform_batch: items[{i}] must be a (name, request) tuple, "
                f"got {type(item).__name__}"
            )
    runtime._record_effects([(name, "perform") for name, _ in items])
    # 快照：调度与执行之间调用方仍可能修改原列表。
    results: list[Any] = await runtime._dispatch_perform_batch_async(list(items))
    return results


async def gather(
    *handles: AsyncResult,
    timeout: float | None = None,
    return_exceptions: bool = False,
) -> list[Any]:
    """异步并行等待多个 ``AsyncResult`` 完成。

    与同步 ``actant.gather`` 语义一致，但等待经 ``AsyncResult._bridge_to_loop``
    的完成回调直通 event loop，不阻塞线程。

    Args:
        *handles: 一个或多个 ``AsyncResult``。
        timeout: 整体最大等待秒数，``None`` 表示无限等待。
        return_exceptions: ``True`` 时失败任务的结果为异常对象而非抛出。

    Raises:
        ActantTimeoutError: 等待超时。
        ActantError: 任一任务失败（``return_exceptions=False`` 时）。
        ValueError: 无 handles。
        TypeError: 某个 handle 不是 ``AsyncResult``。
    """
    if not handles:
        raise ValueError("aio.gather() requires at least one AsyncResult")
    for i, handle in enumerate(handles):
        if not isinstance(handle, AsyncResult):
            raise TypeError(
                f"aio.gather: handles[{i}] must be an AsyncResult, "
                f"got {type(handle).__name__}"
            )

    loop = asyncio.get_running_loop()
    if all(h.done() for h in handles):
        return _gather(*handles, timeout=0, return_exceptions=return_exceptions)

    futures = [h._bridge_to_loop(loop) for h in handles]
    try:
        done, pending = await asyncio.wait(futures, timeout=timeout)
    finally:
        # 调用方取消时同样释放桥接引用；迟到完成由 _set_aio_result 处理为 no-op。
        for f in futures:
            if not f.done():
                f.cancel()
    if pending:
        raise ActantTimeoutError(
            f"aio.gather: at least one handle did not complete within {timeout}s"
        )
    # 消费已完成桥接 future 上的异常，避免 "never retrieved" 告警；
    # 真实失败语义由下方 gather(timeout=0) 统一抛出。
    for f in done:
        if f.done() and not f.cancelled():
            f.exception()
    return _gather(*handles, timeout=0, return_exceptions=return_exceptions)


__all__ = [
    "AsyncDriver",
    "AsyncFlowContext",
    "ask",
    "flow",
    "gather",
    "perform",
    "perform_batch",
]
