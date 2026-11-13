"""放置策略适配器。

把类型化的 :class:`~actant.ext.types.PlacementPolicy` 适配为
``Runtime(route_policy=...)`` 接受的 callable。适配只发生在调用边界，
不改变 Rust 路由语义；策略异常或返回非法值时仍由 Rust 回退到内置策略。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Protocol

from actant.ext.types import NodeView, PlacementContext, PlacementDecision

RouteReply = str | tuple[str, str] | None


class PlacementPolicy(Protocol):
    """类型化放置策略。

    实现者接收稳定的 :class:`PlacementContext` 与 :class:`NodeView` 序列，
    返回 :class:`PlacementDecision` 或 ``None``（等价于 ``auto``）。

    正确性不应依赖本策略：标签与配额是 Rust 侧的基础约束，本策略只做优化。
    """

    def __call__(
        self, ctx: PlacementContext, nodes: Sequence[NodeView]
    ) -> PlacementDecision | None: ...


def bind_placement_policy(
    policy: PlacementPolicy,
) -> Callable[[dict[str, Any], list[dict[str, Any]]], RouteReply]:
    """把类型化策略绑定为 ``route_policy`` callable。

    用法::

        rt = Runtime(route_policy=bind_placement_policy(my_policy))

    返回的 callable 只做类型转换；不做重试、不捕获策略异常，也不保证被调用
    （Rust 热路径拿不到 GIL 时会回退内置策略）。
    """

    if not callable(policy):
        raise TypeError("placement policy must be callable")

    def _route(ctx: dict[str, Any], candidates: list[dict[str, Any]]) -> RouteReply:
        context = PlacementContext.from_route_context(ctx)
        nodes = [NodeView.from_route_candidate(candidate) for candidate in candidates]
        decision = policy(context, nodes)
        if decision is None:
            return None
        if not isinstance(decision, PlacementDecision):
            raise TypeError(
                "placement policy must return PlacementDecision or None, "
                f"got {type(decision).__name__}"
            )
        return decision.to_route_reply()

    return _route
