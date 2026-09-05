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
from typing import Literal, Protocol, runtime_checkable

EffectKind = Literal["ask", "perform", "emit"]

# 这些常量用于避免在 ``ask``/``perform``/``emit`` 调用中硬编码魔法字符串，
# 提供 IDE 自动补全与编译期拼写检查。值与 ``BUILTIN_CAPABILITIES`` 的键一致。
# 用法：``ask(actant.ROUTING, RouteCtx(...))`` 而非 ``ask("Routing", ...)``。

#: 策略型（纯 Python，无 Rust fallback）
ROUTING = "Routing"
SCHEDULING = "Scheduling"
RETRY_POLICY = "RetryPolicy"

#: 副作用型（Rust 提供默认 handler，Python 可覆盖）
SERIALIZATION = "Serialization"
TRANSPORT = "Transport"
STORE = "Store"
EXECUTE = "Execute"

#: 反应型（Rust 事件总线广播，Python 可订阅）
TASK_LIFECYCLE = "TaskLifecycle"
WORKFLOW_LIFECYCLE = "WorkflowLifecycle"
NODE_LIFECYCLE = "NodeLifecycle"


@dataclass
class CapabilityMeta:
    """Capability 的元数据。"""

    name: str
    kind: EffectKind

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
    """`Execute` capability 的请求上下文。

    ``timeout_ms`` 与 ``@task(timeout_ms=...)`` 语义对齐：``0`` = 无超时
    （Rust 侧 ``ExecuteHandler`` 映射为远期硬超时），非 ``0`` 为毫秒级硬超时。
    """

    task_id: str
    workflow_id: str
    payload: bytes
    timeout_ms: int = 0


@dataclass
class ExecuteOutcome:
    """`Execute` capability 的执行结果。

    成功时 ``result_payload`` 携带 cloudpickle 序列化的返回值，``error_payload`` 为空。
    失败时 ``error_payload`` 携带 cloudpickle 序列化的异常实例，``result_payload`` 为空；
    调用方（如 ``AsyncResult.result``）据此重新抛出异常。此设计使任务失败可跨节点传播，
    而非依赖 ``perform`` 抛出异常（后者在跨节点时无法保证异常类型可序列化）。
    """

    task_id: str
    result_payload: bytes
    error_payload: bytes = b""

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


@runtime_checkable
class RoutingHandler(Protocol):
    """决策型：任务路由。返回目标节点 ID，`None` 表示放弃决策。"""

    def __call__(self, ctx: RouteCtx) -> str | None: ...


@runtime_checkable
class SchedulingHandler(Protocol):
    """决策型：从待调度任务中选下一个。返回任务 ID，`None` 表示放弃。"""

    def __call__(self, ctx: ScheduleCtx) -> str | None: ...


@runtime_checkable
class RetryPolicyHandler(Protocol):
    """决策型：决定是否重试。返回 `True` 重试，`None` 放弃。"""

    def __call__(self, ctx: RetryCtx) -> bool | None: ...


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

    def __call__(self, req: StoreReq) -> bytes | None: ...


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


#: 所有内置 capability 的元数据。
#:
#: 注意分层（见AGENTS.md）：
#: - `Routing` / `Scheduling` / `RetryPolicy`：策略型，**仅 Python 层**实现，
#:   Rust 核心不提供默认 handler。用户必须通过 `rt.layer(name).chain(handler)`
#:   或 `Runtime.with_defaults()` 注册 Python handler，否则 ask 返回 None。
#: - 其余 7 个：Rust 核心提供 codec（`register_defaults`），具体 handler 由
#:   `RuntimeBuilder` 注入（如 StoreHandler / ExecuteHandler）或 Python 层覆盖。
BUILTIN_CAPABILITIES: dict[str, CapabilityMeta] = {
    # ── 策略型（Python-only，无 Rust fallback）──
    "Routing": CapabilityMeta("Routing", "ask"),
    "Scheduling": CapabilityMeta("Scheduling", "ask"),
    "RetryPolicy": CapabilityMeta("RetryPolicy", "ask"),
    # ── 副作用型（Rust 提供默认 handler，Python 可覆盖）──
    "Serialization": CapabilityMeta("Serialization", "perform"),
    "Transport": CapabilityMeta("Transport", "perform"),
    "Store": CapabilityMeta("Store", "perform"),
    "Execute": CapabilityMeta("Execute", "perform"),
    # ── 反应型（Rust 事件总线广播，Python 可订阅）──
    "TaskLifecycle": CapabilityMeta("TaskLifecycle", "emit"),
    "WorkflowLifecycle": CapabilityMeta("WorkflowLifecycle", "emit"),
    "NodeLifecycle": CapabilityMeta("NodeLifecycle", "emit"),
}

#: 仅 Python 层实现的策略型 capability（无 Rust fallback）。
#: 单点维护：``RUST_BACKED_CAPABILITIES`` 从 ``BUILTIN_CAPABILITIES`` 与此集合派生。
PYTHON_ONLY_CAPABILITIES: frozenset[str] = frozenset({
    "Routing",
    "Scheduling",
    "RetryPolicy",
})

#: Rust 核心通过 `capability_registry!` 注册的 capability 名称集合。
#: 仅这些 capability 在 Python handler 缺失/弃权时可回退到 Rust 内置 handler。
#: 从 ``BUILTIN_CAPABILITIES`` 键集合中减去 ``PYTHON_ONLY_CAPABILITIES`` 派生，
#: 避免新增 Rust-backed capability 时遗漏更新此集合。
RUST_BACKED_CAPABILITIES: frozenset[str] = frozenset(
    BUILTIN_CAPABILITIES.keys() - PYTHON_ONLY_CAPABILITIES
)


def get_builtin_capability_meta(name: str) -> CapabilityMeta:
    """返回指定**内置** capability 的元数据。

    仅查 ``BUILTIN_CAPABILITIES``。如需查询 Runtime 已注册（含自定义）的
    capability，用 ``Runtime.capability_meta(name)``。

    Args:
        name: capability 名称（如 ``ROUTING``）。

    Raises:
        KeyError: capability 不存在。
    """
    if name not in BUILTIN_CAPABILITIES:
        raise KeyError(
            f"unknown capability: {name!r}; available: {list(BUILTIN_CAPABILITIES)}"
        )
    return BUILTIN_CAPABILITIES[name]


# 用户自定义 capability 通过 `rt.layer(name, kind)` 直接注册，
# layer() 会自动创建 CapabilityMeta 并存入 Runtime._metas。


__all__ = [
    "BUILTIN_CAPABILITIES",
    "EXECUTE",
    "NODE_LIFECYCLE",
    "PYTHON_ONLY_CAPABILITIES",
    "RETRY_POLICY",
    "ROUTING",
    "RUST_BACKED_CAPABILITIES",
    "SCHEDULING",
    "SERIALIZATION",
    "STORE",
    "TASK_LIFECYCLE",
    "TRANSPORT",
    "WORKFLOW_LIFECYCLE",
    "CapabilityMeta",
    "EffectKind",
    "ExecuteCtx",
    "ExecuteHandler",
    "ExecuteOutcome",
    "NodeEvent",
    "NodeLifecycleHandler",
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
    "get_builtin_capability_meta",
]
