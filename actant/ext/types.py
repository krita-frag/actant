"""通用扩展契约的不可变类型。

本模块只提供类型适配，不改变 Runtime 行为：

- :class:`NodeView`：把 ``Runtime.peers()`` / ``route_policy`` 候选字典归一化为
  稳定视图；
- :class:`PlacementContext`：把 ``route_policy`` 的上下文字典归一化；
- :class:`PlacementDecision`：把放置结果转换为 Rust ``RoutePolicy`` 能接受的
  返回形状。

这些类型刻意不包含任何业务领域名词；具体策略由调用方实现。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal


def _freeze_str_map(value: Mapping[str, Any] | None) -> Mapping[str, str]:
    """复制并冻结字符串映射，避免外部修改影响视图。"""
    return MappingProxyType({str(key): str(item) for key, item in (value or {}).items()})


def _freeze_int_map(value: Mapping[str, Any] | None) -> Mapping[str, int]:
    """复制并冻结整数映射。"""
    return MappingProxyType({str(key): int(item) for key, item in (value or {}).items()})


@dataclass(frozen=True)
class NodeView:
    """节点视图。

    字段是 ``Runtime.peers()`` 与 ``route_policy`` 候选的并集；缺失字段使用
    中性默认值。``available_quotas`` 表示剩余容量，不是总容量。
    """

    node_id: str
    labels: Mapping[str, str] = field(default_factory=dict)
    available_slots: int = 0
    max_slots: int = 0
    active_workflows: tuple[str, ...] = ()
    platform: str = ""
    last_heartbeat_ms: int = 0
    endpoint: str | None = None
    role: str = ""
    draining: bool = False
    available_quotas: Mapping[str, int] = field(default_factory=dict)

    @classmethod
    def from_peer(cls, peer: Mapping[str, Any]) -> NodeView:
        """从 ``Runtime.peers()`` 的一项构造视图。"""
        return cls(
            node_id=str(peer.get("node_id", "")),
            labels=_freeze_str_map(peer.get("labels")),
            available_slots=int(peer.get("available_slots", 0)),
            max_slots=int(peer.get("max_slots", 0)),
            active_workflows=_active_workflows(peer.get("active_workflows")),
            platform=str(peer.get("platform", "")),
            last_heartbeat_ms=int(peer.get("last_heartbeat_ms", 0)),
            endpoint=_optional_str(peer.get("endpoint")),
        )

    @classmethod
    def from_route_candidate(cls, candidate: Mapping[str, Any]) -> NodeView:
        """从 ``route_policy`` 的候选字典构造视图。"""
        return cls(
            node_id=str(candidate.get("node_id", "")),
            labels=_freeze_str_map(candidate.get("labels")),
            available_slots=int(candidate.get("available_slots", 0)),
            max_slots=int(candidate.get("max_slots", 0)),
            last_heartbeat_ms=int(candidate.get("last_heartbeat_ms", 0)),
            endpoint=_optional_str(candidate.get("endpoint_addr")),
            role=str(candidate.get("role", "")),
            draining=bool(candidate.get("draining", False)),
            available_quotas=_freeze_int_map(candidate.get("available_quotas")),
        )


@dataclass(frozen=True)
class PlacementContext:
    """放置上下文的稳定视图，对应 ``route_policy`` 的第一个参数。"""

    local_role: str
    local_draining: bool
    local_available_slots: int
    heartbeat_ttl_ms: int
    local_labels: Mapping[str, str] = field(default_factory=dict)
    local_available_quotas: Mapping[str, int] = field(default_factory=dict)
    required_tags: tuple[str, ...] = ()
    required_quota: str | None = None

    @classmethod
    def from_route_context(cls, ctx: Mapping[str, Any]) -> PlacementContext:
        """从 ``route_policy`` 的上下文字典构造视图。"""
        return cls(
            local_role=str(ctx.get("local_role", "")),
            local_draining=bool(ctx.get("local_draining", False)),
            local_available_slots=int(ctx.get("local_available_slots", 0)),
            heartbeat_ttl_ms=int(ctx.get("heartbeat_ttl_ms", 0)),
            local_labels=_freeze_str_map(ctx.get("local_labels")),
            local_available_quotas=_freeze_int_map(ctx.get("local_available_quotas")),
            required_tags=tuple(str(tag) for tag in (ctx.get("required_tags") or ())),
            required_quota=_optional_str(ctx.get("required_quota")),
        )


@dataclass(frozen=True)
class PlacementDecision:
    """放置决策。

    ``kind`` 的语义与 Rust ``RoutePolicy`` 对齐：

    - ``"auto"``：弃权，交回内置策略；
    - ``"local"``：选择本节点；
    - ``"remote"``：选择候选中的某个节点，必须提供 ``node_id``；
    - ``"defer"``：暂不放置，``reason`` 会进入 Rust 的 Defer 原因。
    """

    kind: Literal["auto", "local", "remote", "defer"]
    node_id: str | None = None
    reason: str = ""

    def __post_init__(self) -> None:
        if self.kind not in {"auto", "local", "remote", "defer"}:
            raise ValueError(f"unknown placement decision kind: {self.kind!r}")
        if self.kind == "remote" and not self.node_id:
            raise ValueError("remote placement decision requires node_id")

    @classmethod
    def auto(cls) -> PlacementDecision:
        """弃权，交回内置策略。"""
        return cls("auto")

    @classmethod
    def local(cls) -> PlacementDecision:
        """选择本节点。"""
        return cls("local")

    @classmethod
    def remote(cls, node_id: str) -> PlacementDecision:
        """选择指定候选节点。"""
        return cls("remote", node_id=node_id)

    @classmethod
    def defer(cls, reason: str = "") -> PlacementDecision:
        """暂不放置。"""
        return cls("defer", reason=reason)

    def to_route_reply(self) -> str | tuple[str, str] | None:
        """转换为 ``route_policy`` callable 的返回形状。"""
        if self.kind == "auto":
            return None
        if self.kind == "local":
            return "local"
        if self.kind == "remote":
            return ("remote", str(self.node_id))
        return ("defer", self.reason or "deferred by extension")


def _optional_str(value: Any) -> str | None:
    """把可选值转为 ``str | None``，空字符串视为无值。"""
    if value is None:
        return None
    text = str(value)
    return text or None


def _active_workflows(value: Any) -> tuple[str, ...]:
    """归一化 ``active_workflows``：源字段可能是列表或单值。"""
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(str(item) for item in value)
    return (str(value),)
