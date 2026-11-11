"""Actant - 基于 Actor 模型的跨平台通用分布式任务编排引擎。

采用 Effect-Resource-Handler 统一扩展架构：数据面与观测面扩展点
（Store、ValueStore、TaskLifecycle、WorkflowLifecycle）统一为 `Capability`
（能力声明）+ `Handler`（能力实现）+ `Layer`（handler 组合）+
`Effect`（请求能力）。

路由、调度、重试等放置类策略不属于 ERH 数据面：它们由 Rust
`RoutePolicy` / `Scheduler` 或第 3 层外部调度器承担，避免进程内 Python
闭包在 workflow failover 后失效。

典型用法::

    import actant

    rt = actant.Runtime()
    rt.layer("TaskLifecycle").chain(lambda e: print(f"task {e.kind}: {e.task_id}"))
    rt.start()

    # 在 handler 中请求 capability：
    actant.emit("TaskLifecycle", task_event)

顶层只保留最常用的入口。其余 API 按职责放在子命名空间：

- ``actant.tasks``：任务对象、上下文与状态；
- ``actant.workflow``：等待点、续代、版本控制与流程定义注册表；
- ``actant.aio``：异步面（与同步面同名，如 ``aio.flow`` / ``aio.ask`` / ``aio.gather``）；
- ``actant.effects``：同步 effect 动词；
- ``actant.capabilities``：能力类型、事件与 handler 协议；
- ``actant.exceptions``：异常层级。

进程边界（HTTP/JSON + SSE 服务协议）不随核心包发布，见 ``extensions/act-service``。
"""

from __future__ import annotations

from actant import aio, testkit, workflow  # noqa: F401  注册子命名空间属性
from actant._internal.flow import FlowHandle, flow, host_flows, start_flow
from actant._internal.runtime import Layer, Runtime, get_current_runtime, use_runtime
from actant.actant import get_version as _get_version
from actant.actant import metrics_text
from actant.effects import ask, emit, perform
from actant.exceptions import ActantError
from actant.tasks import AsyncResult, Ref, gather, task

__version__ = _get_version()

__all__ = [
    "ActantError",
    "AsyncResult",
    "FlowHandle",
    "Layer",
    "Ref",
    "Runtime",
    "__version__",
    "ask",
    "emit",
    "flow",
    "gather",
    "get_current_runtime",
    "host_flows",
    "metrics_text",
    "perform",
    "start_flow",
    "task",
    "use_runtime",
]
