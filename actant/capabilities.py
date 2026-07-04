"""内置 Capability 的 Python 声明。

每个 Capability 是一个 `Protocol`，声明其 handler 的请求/响应类型。
用户通过实现对应 Protocol 来提供 handler。

Effect 类型：
- `ask`：决策型，handler 返回 `Optional[T]`，第一个非 `None` 决定结果
- `perform`：副作用型，handler 返回 `T`（或抛出异常）
- `emit`：反应型，handler 返回 `None`，所有 handler 顺序执行
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Protocol, runtime_checkable

# ============================================================================
# Effect 类型
# ============================================================================

EffectKind = Literal["ask", "perform", "emit"]


@dataclass
class CapabilityMeta:
    """Capability 的元数据。"""

    name: str
    kind: EffectKind


# ============================================================================
# 请求/响应数据类
# ============================================================================


@dataclass
class RouteCtx:
    """`Routing` capability 的请求上下文。"""

    task_name: str
    peers: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    local_node: str = ""


@dataclass
class ScheduleCtx:
    """`Scheduling` capability 的请求上下文。"""

    workflow_id: str
    pending: list[str] = field(default_factory=list)
    max_concurrent: int = 4


@dataclass
class RetryCtx:
    """`RetryPolicy` capability 的请求上下文。"""

    task_id: str
    attempt: int
    last_error: str
    max_retries: int


@dataclass
class SerializationReq:
    """`Serialization` capability 的请求。"""

    op: Literal["dump", "load"]
    data: bytes


@dataclass
class TransportReq:
    """`Transport` capability 的请求。"""

    op: Literal["send_task", "send_actor_message", "broadcast_heartbeat"]
    target: str
    payload: bytes


@dataclass
class StoreReq:
    """`Store` capability 的请求。"""

    op: Literal["put", "get", "delete"]
    key: bytes
    value: bytes = b""


@dataclass
class ExecuteCtx:
    """`Execute` capability 的请求上下文。"""

    task_id: str
    workflow_id: str
    payload: bytes
    timeout_ms: int = 0


@dataclass
class ExecuteOutcome:
    """`Execute` capability 的执行结果。"""

    task_id: str
    result_payload: bytes


# ============================================================================
# 事件类型（emit capability 的请求）
# ============================================================================


@dataclass
class TaskEvent:
    """`TaskLifecycle` capability 的事件。"""

    kind: Literal["started", "completed", "failed", "retried", "cancelled"]
    task_id: str
    workflow_id: str = ""
    result_payload: bytes = b""
    error: str = ""
    attempt: int = 0
    next_attempt: int = 0


@dataclass
class WorkflowEvent:
    """`WorkflowLifecycle` capability 的事件。"""

    kind: Literal["submitted", "started", "completed", "failed", "cancelled"]
    workflow_id: str
    error: str = ""


@dataclass
class NodeEvent:
    """`NodeLifecycle` capability 的事件。"""

    kind: Literal["started", "stopped", "peer_joined", "peer_left", "heartbeat"]
    node_id: str
    peer_id: str = ""
    timestamp_ms: int = 0


# ============================================================================
# Capability Protocol 声明
# ============================================================================


@runtime_checkable
class RoutingHandler(Protocol):
    """决策型：任务路由。返回目标节点 ID，`None` 表示放弃决策。"""

    def __call__(self, ctx: RouteCtx) -> Optional[str]: ...


@runtime_checkable
class SchedulingHandler(Protocol):
    """决策型：从待调度任务中选下一个。返回任务 ID，`None` 表示放弃。"""

    def __call__(self, ctx: ScheduleCtx) -> Optional[str]: ...


@runtime_checkable
class RetryPolicyHandler(Protocol):
    """决策型：决定是否重试。返回 `True` 重试，`None` 放弃。"""

    def __call__(self, ctx: RetryCtx) -> Optional[bool]: ...


@runtime_checkable
class SerializationHandler(Protocol):
    """副作用型：序列化/反序列化。"""

    def __call__(self, req: SerializationReq) -> bytes: ...


@runtime_checkable
class TransportHandler(Protocol):
    """副作用型：网络传输。"""

    def __call__(self, req: TransportReq) -> None: ...


@runtime_checkable
class StoreHandler(Protocol):
    """副作用型：持久化存储。返回 `Optional[bytes]`（get 返回值，put/delete 返回 None）。"""

    def __call__(self, req: StoreReq) -> Optional[bytes]: ...


@runtime_checkable
class ExecuteHandler(Protocol):
    """副作用型：执行任务。"""

    def __call__(self, ctx: ExecuteCtx) -> ExecuteOutcome: ...


@runtime_checkable
class TaskLifecycleHandler(Protocol):
    """反应型：任务生命周期事件订阅。"""

    def __call__(self, event: TaskEvent) -> None: ...


@runtime_checkable
class WorkflowLifecycleHandler(Protocol):
    """反应型：工作流生命周期事件订阅。"""

    def __call__(self, event: WorkflowEvent) -> None: ...


@runtime_checkable
class NodeLifecycleHandler(Protocol):
    """反应型：节点生命周期事件订阅。"""

    def __call__(self, event: NodeEvent) -> None: ...


# ============================================================================
# 内置 Capability 注册表
# ============================================================================

#: 所有内置 capability 的元数据。
BUILTIN_CAPABILITIES: dict[str, CapabilityMeta] = {
    "Routing": CapabilityMeta("Routing", "ask"),
    "Scheduling": CapabilityMeta("Scheduling", "ask"),
    "RetryPolicy": CapabilityMeta("RetryPolicy", "ask"),
    "Serialization": CapabilityMeta("Serialization", "perform"),
    "Transport": CapabilityMeta("Transport", "perform"),
    "Store": CapabilityMeta("Store", "perform"),
    "Execute": CapabilityMeta("Execute", "perform"),
    "TaskLifecycle": CapabilityMeta("TaskLifecycle", "emit"),
    "WorkflowLifecycle": CapabilityMeta("WorkflowLifecycle", "emit"),
    "NodeLifecycle": CapabilityMeta("NodeLifecycle", "emit"),
}


def get_capability_meta(name: str) -> CapabilityMeta:
    """返回指定 capability 的元数据。

    Args:
        name: capability 名称（如 `"Routing"`）。

    Raises:
        KeyError: capability 不存在。
    """
    if name not in BUILTIN_CAPABILITIES:
        raise KeyError(
            f"unknown capability: {name!r}; available: {list(BUILTIN_CAPABILITIES)}"
        )
    return BUILTIN_CAPABILITIES[name]


# 用户自定义 capability 也用 CapabilityMeta 表示，但不在 BUILTIN_CAPABILITIES 中。
# 用户通过 `actant.capability(name, kind)` 声明自定义 capability。


def capability(name: str, kind: EffectKind = "perform") -> CapabilityMeta:
    """声明一个自定义 capability。

    Args:
        name: capability 名称。必须是唯一的（不能与内置重复）。
        kind: effect 类型，`"ask"` / `"perform"` / `"emit"`。

    Returns:
        该 capability 的元数据。注册到 Runtime 时使用。
    """
    if name in BUILTIN_CAPABILITIES:
        raise ValueError(
            f"capability name {name!r} conflicts with builtin; use a different name"
        )
    if kind not in ("ask", "perform", "emit"):
        raise ValueError(f"kind must be 'ask'/'perform'/'emit', got {kind!r}")
    return CapabilityMeta(name, kind)


# ============================================================================
# 请求类型映射（用于运行时校验）
# ============================================================================

#: 每个 capability 对应的请求类型（用于运行时 isinstance 校验）。
REQUEST_TYPES: dict[str, type] = {
    "Routing": RouteCtx,
    "Scheduling": ScheduleCtx,
    "RetryPolicy": RetryCtx,
    "Serialization": SerializationReq,
    "Transport": TransportReq,
    "Store": StoreReq,
    "Execute": ExecuteCtx,
    "TaskLifecycle": TaskEvent,
    "WorkflowLifecycle": WorkflowEvent,
    "NodeLifecycle": NodeEvent,
}


__all__ = [
    "BUILTIN_CAPABILITIES",
    "CapabilityMeta",
    "EffectKind",
    "ExecuteCtx",
    "ExecuteHandler",
    "ExecuteOutcome",
    "NodeEvent",
    "NodeLifecycleHandler",
    "REQUEST_TYPES",
    "RetryCtx",
    "RetryPolicyHandler",
    "RouteCtx",
    "RoutingHandler",
    "ScheduleCtx",
    "SchedulingHandler",
    "SerializationHandler",
    "SerializationReq",
    "StoreHandler",
    "StoreReq",
    "TaskEvent",
    "TaskLifecycleHandler",
    "TransportHandler",
    "TransportReq",
    "WorkflowEvent",
    "WorkflowLifecycleHandler",
    "capability",
    "get_capability_meta",
]
