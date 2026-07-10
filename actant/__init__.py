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
from actant._runtime import Layer, Runtime, get_current_runtime, use_runtime
from actant.actant import get_version
from actant.capabilities import (
    BUILTIN_CAPABILITIES,
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
    capability,
    get_capability_meta,
)
from actant.exceptions import ActantError

__version__ = get_version()

__all__ = [
    "BUILTIN_CAPABILITIES",
    "ActantError",
    "CapabilityMeta",
    "EffectKind",
    "ExecuteCtx",
    "ExecuteOutcome",
    "Layer",
    "NodeEvent",
    "RetryCtx",
    "RouteCtx",
    "Runtime",
    "ScheduleCtx",
    "SerializationReq",
    "StoreReq",
    "TaskEvent",
    "TransportReq",
    "WorkflowEvent",
    "__version__",
    "ask",
    "capability",
    "effect",
    "emit",
    "get_capability_meta",
    "get_current_runtime",
    "impossible",
    "perform",
    "use_runtime",
]
