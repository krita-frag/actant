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


__all__ = [
    "ask",
    "effect",
    "emit",
    "impossible",
    "perform",
]
