"""Effect 解释器：`ask` / `perform` / `emit`。

用户在 handler 中通过这些函数请求 capability。当前实现为同步（基于已注册的 handler 列表），
后续可扩展为支持异步 handler（`async def`）与 generator-based algebraic effects。

# 设计

- `ask(name, request)`：在 handler 链上逆序调用（后注册=高优先级），第一个返回非 `None` 的决定结果。
- `perform(name, request)`：调用最后注册的 handler（高优先级），结果直接返回。
- `emit(name, request)`：所有 handler 顺序执行，无返回值。

Handler 通过 `Runtime.layer(name, kind).chain(handler)` 注册。
"""

from __future__ import annotations

from typing import Any, Literal, NoReturn

from actant._runtime import Runtime, get_current_runtime
from actant.capabilities import CapabilityMeta
from actant.exceptions import InternalError, InvalidStateError


def _resolve_meta(name: str) -> tuple[Runtime, CapabilityMeta]:
    """从当前 Runtime 解析 capability 元数据。"""
    runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            f"effect {name!r}: no active Runtime; wrap your code in `with actant.Runtime() as rt:`"
        )
    return runtime, runtime.capability_meta(name)


def ask(name: str, request: Any) -> Any | None:
    """决策型 effect：请求 capability，返回第一个非 `None` 的 handler 结果。

    Args:
        name: capability 名称（如 `"Routing"`）。
        request: 请求 payload（类型由 capability 定义）。

    Returns:
        第一个返回非 `None` 的 handler 的结果；若所有 handler 都返回 `None`，返回 `None`。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中，或 capability kind 不匹配。
        KeyError: capability 未注册。
    """
    runtime, meta = _resolve_meta(name)
    if meta.kind != "ask":
        raise InvalidStateError(
            f"ask: capability {name!r} is {meta.kind!r}, not 'ask'"
        )
    return runtime._dispatch_ask(name, request)


def perform(name: str, request: Any) -> Any:
    """副作用型 effect：请求 capability，执行单 handler 并返回结果。

    Args:
        name: capability 名称。
        request: 请求 payload。

    Returns:
        handler 的返回值。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中，或 capability kind 不匹配。
        KeyError: capability 未注册。
    """
    runtime, meta = _resolve_meta(name)
    if meta.kind != "perform":
        raise InvalidStateError(
            f"perform: capability {name!r} is {meta.kind!r}, not 'perform'"
        )
    return runtime._dispatch_perform(name, request)


def emit(
    name: str,
    request: Any,
    *,
    on_error: Literal["log", "raise", "collect"] = "log",
) -> None:
    """反应型 effect：触发所有订阅该 capability 的 handler。

    Args:
        name: capability 名称。
        request: 事件 payload。
        on_error: 错误处理策略。``"log"``（默认）记录 warning 继续执行；
            ``"raise"`` 首个失败立即抛出；``"collect"`` 聚合所有错误后抛出。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中，或 capability kind 不匹配。
        KeyError: capability 未注册。
        ValueError: ``on_error`` 不是合法值（动态构造时仍校验）。
    """
    # Literal 仅供静态检查；动态构造（on_error 来自变量）时仍需运行时校验，
    # 以防拼写错误延迟到首个 handler 失败时才暴露。
    if on_error not in ("log", "raise", "collect"):
        raise ValueError(
            f"on_error must be 'log'/'raise'/'collect', got {on_error!r}"
        )
    runtime, meta = _resolve_meta(name)
    if meta.kind != "emit":
        raise InvalidStateError(
            f"emit: capability {name!r} is {meta.kind!r}, not 'emit'"
        )
    runtime._dispatch_emit(name, request, on_error=on_error)


def effect(
    name: str,
    kind: Literal["ask", "perform", "emit"],
    request: Any,
) -> Any:
    """通用 effect 调度：根据 kind 分发到 ask/perform/emit。

    Args:
        name: capability 名称。
        kind: `"ask"` / `"perform"` / `"emit"`。
        request: 请求 payload。

    Returns:
        ask 返回 `Optional[Any]`；perform 返回 `Any`；emit 返回 `None`。
    """
    if kind == "ask":
        return ask(name, request)
    if kind == "perform":
        return perform(name, request)
    if kind == "emit":
        emit(name, request)
        return None
    raise ValueError(f"kind must be 'ask'/'perform'/'emit', got {kind!r}")


def impossible(detail: str = "unreachable") -> NoReturn:
    """标记不应到达的代码路径。

    用于 handler 中表达"此 effect 必须有 handler 处理，否则是编程错误"。
    """
    raise InternalError(f"impossible: {detail}")


# ──────────────────────────────────────────────────────────────────
# 异步 effect：在 asyncio 上下文中并发执行多个 effect，
# 避免同步 `ask`/`perform` 阻塞 event loop。
#
# - Rust-backed capability 且无 Python handler 命中时，转发到 Rust
#   ``ask_async``/``perform_async``，结果通过 ``asyncio.Future`` 异步返回。
# - Python handler 命中时，在默认 executor 线程池中执行，避免 GIL 阻塞 loop。
# ──────────────────────────────────────────────────────────────────

def ask_async(name: str, request: Any) -> Any:
    """异步决策型 effect：返回 ``asyncio.Future``。

    与 ``ask`` 的区别：返回 awaitable 而非同步结果，使调用方在
    ``async def`` 函数中可并发执行多个 ask。

    Returns:
        ``asyncio.Future``，``await`` 后得到 handler 结果或 ``None``。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中，或 capability kind 不匹配，
            或当前线程无运行中的 asyncio 事件循环。
        KeyError: capability 未注册。

    用法::

        async def handler():
            r1 = await ask_async("Routing", ctx1)
            r2 = await ask_async("Routing", ctx2)
            # 或并发：
            r1, r2 = await asyncio.gather(
                ask_async("Routing", ctx1),
                ask_async("Routing", ctx2),
            )
    """
    runtime, meta = _resolve_meta(name)
    if meta.kind != "ask":
        raise InvalidStateError(
            f"ask_async: capability {name!r} is {meta.kind!r}, not 'ask'"
        )
    return runtime._dispatch_ask_async(name, request)


def perform_async(name: str, request: Any) -> Any:
    """异步副作用型 effect：返回 ``asyncio.Future``。

    与 ``perform`` 的区别：返回 awaitable 而非同步结果，使调用方在
    ``async def`` 函数中可并发执行多个 perform，避免 GIL 阻塞 event loop。

    Returns:
        ``asyncio.Future``，``await`` 后得到 handler 结果。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中，或 capability kind 不匹配，
            或当前线程无运行中的 asyncio 事件循环。
        KeyError: capability 未注册。

    用法::

        async def handler():
            # 并发执行 3 个 Store put
            await asyncio.gather(
                perform_async("Store", {"op": "put", "key": b"k1", "value": b"v1"}),
                perform_async("Store", {"op": "put", "key": b"k2", "value": b"v2"}),
                perform_async("Store", {"op": "put", "key": b"k3", "value": b"v3"}),
            )
    """
    runtime, meta = _resolve_meta(name)
    if meta.kind != "perform":
        raise InvalidStateError(
            f"perform_async: capability {name!r} is {meta.kind!r}, not 'perform'"
        )
    return runtime._dispatch_perform_async(name, request)


def perform_batch_async(
    items: list[tuple[str, Any]],
) -> Any:
    """异步批量副作用型 effect：返回 ``asyncio.Future``。

    接收 ``(name, request)`` 元组列表，并发执行所有 perform，返回结果列表。

    与循环 ``await perform_async`` 相比，单次边界穿越（Rust-backed 路径），
    总延迟 ≈ max(单次) 而非 sum(单次)。

    单个 perform 失败不中断批量——失败项在结果列表对应位置为 ``Exception`` 实例。

    Args:
        items: ``(capability_name, request)`` 元组列表。

    Returns:
        ``asyncio.Future``，``await`` 后得到结果列表，顺序与输入一致。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中，或当前线程无运行中的
            asyncio 事件循环。

    用法::

        async def handler():
            results = await perform_batch_async([
                ("Store", {"op": "put", "key": b"k1", "value": b"v1"}),
                ("Store", {"op": "put", "key": b"k2", "value": b"v2"}),
            ])
    """
    if not isinstance(items, list):
        raise TypeError(
            f"perform_batch_async: items must be a list, got {type(items).__name__}"
        )
    runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            "perform_batch_async: no active Runtime; "
            "wrap your code in `with actant.Runtime() as rt:`"
        )
    # 校验每个 item 是 (name, request) 元组
    for i, item in enumerate(items):
        if not (isinstance(item, tuple) and len(item) == 2):
            raise TypeError(
                f"perform_batch_async: items[{i}] must be a (name, request) tuple, "
                f"got {type(item).__name__}"
            )
    return runtime._dispatch_perform_batch_async(items)


__all__ = [
    "ask",
    "ask_async",
    "effect",
    "emit",
    "impossible",
    "perform",
    "perform_async",
    "perform_batch_async",
]
