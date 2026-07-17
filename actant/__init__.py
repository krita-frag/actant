"""Actant - 基于 Actor 模型的跨平台通用分布式任务编排引擎。

0.2.0+ 采用 Effect-Resource-Handler 统一扩展架构（ADR 0001）。
所有扩展点（Routing、Scheduling、Transport、Store、Actor、Lifecycle）统一为
`Capability`（能力声明）+ `Handler`（能力实现）+ `Layer`（handler 组合）+ `Effect`（请求能力）。

典型用法::

    import actant

    rt = actant.Runtime()
    rt.layer("Routing").chain(my_router)
    rt.layer("TaskLifecycle").chain(lambda e: print(f"task {e.kind}: {e.task_id}"))
    rt.start()

    # 在 handler 中请求 capability：
    target = actant.ask("Routing", ctx)
    actant.emit("TaskLifecycle", task_event)

"""

from __future__ import annotations

from actant._effects import ask, effect, emit, impossible, perform
from actant._runtime import Layer, Runtime, get_current_runtime, require_runtime, use_runtime
from actant.actant import get_version
from actant.capabilities import (
    ACTOR_LIFECYCLE,
    ACTOR_MESSAGING,
    ACTOR_SUPERVISION,
    BUILTIN_CAPABILITIES,
    EXECUTE,
    NODE_LIFECYCLE,
    RETRY_POLICY,
    ROUTING,
    SCHEDULING,
    SERIALIZATION,
    STORE,
    TASK_LIFECYCLE,
    TRANSPORT,
    WORKFLOW_LIFECYCLE,
    CapabilityMeta,
    EffectKind,
    ExecuteCtx,
    ExecuteOutcome,
    NodeEvent,
    RetryCtx,
    RouteCtx,
    ScheduleCtx,
    SerializationReq,
    StoreReq,
    TaskEvent,
    TransportReq,
    WorkflowEvent,
    get_builtin_capability_meta,
)
from actant.exceptions import (
    ActantError,
    ActantTimeoutError,
    InternalError,
    InvalidStateError,
    NotFoundError,
    TaskCancelledError,
    WorkflowFailedError,
)
from actant.flow import current_workflow_id, flow
from actant.task import (
    AsyncResult,
    Task,
    TaskContext,
    TaskState,
    gather,
    get_task_context,
    task,
)

__version__ = get_version()

__all__ = [
    "ACTOR_LIFECYCLE",
    "ACTOR_MESSAGING",
    "ACTOR_SUPERVISION",
    "BUILTIN_CAPABILITIES",
    "EXECUTE",
    "NODE_LIFECYCLE",
    "RETRY_POLICY",
    "ROUTING",
    "SCHEDULING",
    "SERIALIZATION",
    "STORE",
    "TASK_LIFECYCLE",
    "TRANSPORT",
    "WORKFLOW_LIFECYCLE",
    "ActantError",
    "ActantTimeoutError",
    "AsyncResult",
    "CapabilityMeta",
    "EffectKind",
    "ExecuteCtx",
    "ExecuteOutcome",
    "InternalError",
    "InvalidStateError",
    "Layer",
    "NodeEvent",
    "NotFoundError",
    "RetryCtx",
    "RouteCtx",
    "Runtime",
    "ScheduleCtx",
    "SerializationReq",
    "StoreReq",
    "Task",
    "TaskCancelledError",
    "TaskContext",
    "TaskEvent",
    "TaskState",
    "TransportReq",
    "WorkflowEvent",
    "WorkflowFailedError",
    "__version__",
    "ask",
    "current_workflow_id",
    "effect",
    "emit",
    "flow",
    "gather",
    "get_builtin_capability_meta",
    "get_current_runtime",
    "get_task_context",
    "impossible",
    "perform",
    "require_runtime",
    "task",
    "use_runtime",
]
