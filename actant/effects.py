"""Effect 解释器：`ask` / `perform` / `emit`。

用户在 handler 中通过这些函数请求 capability。本模块只提供同步动词；异步上下文
使用 ``actant.aio`` 中的同名函数。

# 设计

- `ask(name, request)`：在 handler 链上逆序调用（后注册=高优先级），第一个返回非 `None` 的决定结果。
- `perform(name, request)`：调用最后注册的 handler（高优先级），结果直接返回。
- `emit(name, request)`：所有 handler 顺序执行，无返回值。

Handler 通过 `Runtime.layer(name, kind).chain(handler)` 注册。
"""

from __future__ import annotations

from typing import Any, Literal, NoReturn

from actant._internal.runtime import Runtime, get_current_runtime
from actant.capabilities import CapabilityMeta, EffectKind
from actant.exceptions import InternalError, InvalidStateError


def _resolve_meta(name: str) -> tuple[Runtime, CapabilityMeta]:
    """从当前 Runtime 解析 capability 元数据。"""
    runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            f"effect {name!r}: no active Runtime; wrap your code in `with actant.Runtime() as rt:`"
        )
    return runtime, runtime.capability_meta(name)


def _resolve_by_kind(name: str, kind: EffectKind, effect_name: str) -> Runtime:
    """解析 capability 元数据并校验 kind，返回其所属 Runtime。

    校验失败抛 ``InvalidStateError``；成功则返回 Runtime 供随后的 effect 分发。
    """
    runtime, meta = _resolve_meta(name)
    if meta.kind != kind:
        raise InvalidStateError(
            f"{effect_name}: capability {name!r} is {meta.kind!r}, not {kind!r}"
        )
    return runtime


def ask(name: str, request: Any) -> Any | None:
    """决策型 effect：请求 capability，返回第一个非 `None` 的 handler 结果。

    Args:
        name: capability 名称（如自定义的 `"MyRouter"`）。
        request: 请求 payload（类型由 capability 定义）。

    Returns:
        第一个返回非 `None` 的 handler 的结果；若所有 handler 都返回 `None`，返回 `None`。

    Raises:
        InvalidStateError: 当前未在 Runtime 上下文中，或 capability kind 不匹配。
        KeyError: capability 未注册。
    """
    runtime = _resolve_by_kind(name, "ask", "ask")
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
    runtime = _resolve_by_kind(name, "perform", "perform")
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
    runtime = _resolve_by_kind(name, "emit", "emit")
    runtime._dispatch_emit(name, request, on_error=on_error)


def impossible(detail: str = "unreachable") -> NoReturn:
    """标记不应到达的代码路径。

    用于 handler 中表达"此 effect 必须有 handler 处理，否则是编程错误"。
    """
    raise InternalError(f"impossible: {detail}")


__all__ = [
    "ask",
    "emit",
    "impossible",
    "perform",
]
