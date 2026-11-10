"""内置 Capability 的 Python 声明。

只声明引擎真正接线的 capability：

- 数据面 ``Store`` / ``ValueStore``：引擎侧 KV 与 blob 助手会 ``perform``；
- 观测面 ``TaskLifecycle`` / ``WorkflowLifecycle``：引擎侧有 ``emit`` 发射点，
  用户以 handler 订阅。

放置类策略（路由/调度/重试）不属于 ERH 数据面：它们由 Rust ``RoutePolicy`` /
``Scheduler`` 或第 3 层外部调度器承担；进程内 Python handler 无法跟随
workflow failover。

Effect 类型：
- `ask`：决策型，handler 返回 `Optional[T]`，第一个非 `None` 决定结果
- `perform`：副作用型，handler 返回 `T`（或抛出异常）
- `emit`：反应型，handler 返回 `None`，所有 handler 顺序执行
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol, runtime_checkable

EffectKind = Literal["ask", "perform", "emit"]

# 这些常量用于避免在 ``perform``/``emit`` 调用中硬编码魔法字符串，
# 提供 IDE 自动补全与编译期拼写检查。值与 ``BUILTIN_CAPABILITIES`` 的键一致。
# 这些是实现侧常量，不在 ``__all__`` 中，避免公开面随内置 capability 增长。

#: 副作用型（默认 handler 为 Python→Rust blob 桥，Python 可覆盖，如 S3 后端）
VALUE_STORE = "ValueStore"

#: 反应型（引擎在 Python 侧发射，Python handler 订阅）
TASK_LIFECYCLE = "TaskLifecycle"
WORKFLOW_LIFECYCLE = "WorkflowLifecycle"


@dataclass
class CapabilityMeta:
    """Capability 的元数据。"""

    name: str
    kind: EffectKind


@dataclass
class RouteCtx:
    """用户自定义路由 capability 的 payload 助手。

    非内置 capability：引擎不会自动调用它。用户需显式
    ``rt.layer("MyRouter", "ask").chain(...)`` 并自行 ``ask``。
    """

    task_name: str
    peers: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    local_node: str = ""


@dataclass
class ScheduleCtx:
    """用户自定义调度 capability 的 payload 助手（非内置）。"""

    workflow_id: str
    pending: list[str] = field(default_factory=list)
    max_concurrent: int = 4


@dataclass
class RetryCtx:
    """用户自定义重试 capability 的 payload 助手（非内置）。"""

    task_id: str
    attempt: int
    last_error: str
    max_retries: int


@dataclass
class StoreReq:
    """`Store` capability 的请求。

    ``put_batch`` 在单个事务内写入 ``entries`` 中的全部键值对；``key`` /
    ``value`` 仅对 ``put`` / ``get`` / ``delete`` 有意义。
    """

    op: Literal["put", "get", "delete", "put_batch"]
    key: bytes = b""
    value: bytes = b""
    entries: list[tuple[bytes, bytes]] = field(default_factory=list)


@dataclass
class ValueStoreReq:
    """`ValueStore` capability 的请求（值引用原语）。

    内容寻址存储：同一次 ``store`` 的数据字节决定 ``ref``（BlobRef wire 编码，
    内含 blake3 hash 与来源节点）；``fetch`` 按 ``ref`` 取回原始字节，跨节点时
    经 blob 协议流式拉取（逐块校验）。
    """

    op: Literal["store", "fetch"]
    data: bytes = b""
    ref: bytes = b""


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


@runtime_checkable
class RoutingHandler(Protocol):
    """用户自定义路由 handler 的签名助手（非内置 capability）。"""

    def __call__(self, ctx: RouteCtx) -> str | None: ...


@runtime_checkable
class SchedulingHandler(Protocol):
    """用户自定义调度 handler 的签名助手（非内置 capability）。"""

    def __call__(self, ctx: ScheduleCtx) -> str | None: ...


@runtime_checkable
class RetryPolicyHandler(Protocol):
    """用户自定义重试 handler 的签名助手（非内置 capability）。"""

    def __call__(self, ctx: RetryCtx) -> bool | None: ...


@runtime_checkable
class StoreHandler(Protocol):
    """副作用型：持久化存储。返回 `Optional[bytes]`（get 返回值，put/delete 返回 None）。"""

    def __call__(self, req: StoreReq) -> bytes | None: ...


@runtime_checkable
class ValueStoreHandler(Protocol):
    """副作用型：值引用存取。

    ``op="store"`` 返回 BlobRef wire 编码字节（``ref``）；``op="fetch"`` 返回
    取回的原始字节。默认 handler 走本节点 Rust blob 桥（`_RuntimeCore.
    value_store` / `value_fetch`），可覆盖为 S3 等外部对象存储后端。
    """

    def __call__(self, req: ValueStoreReq) -> bytes: ...


@runtime_checkable
class TaskLifecycleHandler(Protocol):
    """反应型：任务生命周期事件订阅。"""

    def __call__(self, event: TaskEvent) -> None: ...


@runtime_checkable
class WorkflowLifecycleHandler(Protocol):
    """反应型：工作流生命周期事件订阅。"""

    def __call__(self, event: WorkflowEvent) -> None: ...


#: 所有内置 capability 的元数据。每一项都必须有生产发射点或默认 handler，
#: 由 ``tests/python/unit/test_capability_wiring.py`` 的 AST 扫描门禁校验。
BUILTIN_CAPABILITIES: dict[str, CapabilityMeta] = {
    # 数据面：引擎侧 KV 助手 perform；无 Python handler 时回退 Rust StoreHandler。
    "Store": CapabilityMeta("Store", "perform"),
    # 数据面：Python 默认 handler 是 Python→Rust blob 桥，库内由 Ref 使用。
    "ValueStore": CapabilityMeta("ValueStore", "perform"),
    # 观测面：引擎在 Python 侧 emit，用户以 handler 订阅；无订阅时静默。
    "TaskLifecycle": CapabilityMeta("TaskLifecycle", "emit"),
    "WorkflowLifecycle": CapabilityMeta("WorkflowLifecycle", "emit"),
}

#: 已登记的 capability 清单。登记与接线由门禁共同约束，新增内置 capability
#: 必须同时在 ``actant/`` 生产代码里提供 effect 发射点或默认 handler。
WIRED_CAPABILITIES: frozenset[str] = frozenset(BUILTIN_CAPABILITIES)

#: 仅 Python 层实现的 capability（无 Rust fallback）。
#: 默认 handler 是 Python→Rust blob 桥（``_DefaultValueStoreHandler``），
#: Rust capability registry 不含该 capability。
PYTHON_ONLY_CAPABILITIES: frozenset[str] = frozenset({"ValueStore"})

#: Rust 核心注册了 codec 的 capability 名称集合。Python handler 缺失/弃权时
#: 转发到 Rust capability runtime。从 ``BUILTIN_CAPABILITIES`` 键集合减去
#: ``PYTHON_ONLY_CAPABILITIES`` 派生，避免新增 Rust-backed capability 时遗漏。
RUST_BACKED_CAPABILITIES: frozenset[str] = frozenset(
    BUILTIN_CAPABILITIES.keys() - PYTHON_ONLY_CAPABILITIES
)

# 用户自定义 capability 通过 `rt.layer(name, kind)` 直接注册，
# layer() 会自动创建 CapabilityMeta 并存入 Runtime._metas。

__all__ = [
    "EffectKind",
    "RetryCtx",
    "RetryPolicyHandler",
    "RouteCtx",
    "RoutingHandler",
    "ScheduleCtx",
    "SchedulingHandler",
    "StoreHandler",
    "StoreReq",
    "TaskEvent",
    "TaskLifecycleHandler",
    "ValueStoreHandler",
    "ValueStoreReq",
    "WorkflowEvent",
    "WorkflowLifecycleHandler",
]
