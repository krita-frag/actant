"""工作流公开命名空间。

`@flow` / `@aio.flow` 的函数体里除顶层入口外还需要更细的原语时，从这里导入::

    from actant.workflow import patched, sleep_until, wait_signal

顶层 ``actant`` 只保留 ``flow`` / ``start_flow`` / ``host_flows`` / ``FlowHandle``
四个常用入口；异步面（``aio.flow`` / ``AsyncFlowContext``）见 ``actant.aio``。
等待点、续代、版本控制与定义注册表按职责放在本模块，避免顶层命名空间继续膨胀。
"""

from __future__ import annotations

from actant._internal.flow import (
    FLOW_REGISTRY,
    FlowDefinition,
    FlowHandle,
    continue_as_new,
    current_workflow_id,
    flow,
    host_flows,
    patched,
    sleep_until,
    start_flow,
    suspend,
    wait_signal,
)
from actant._internal.flow import (
    # 扩展层（act-service）仍需要类型与构造入口；不进 __all__、文档与稳定性
    # 分级，因此不构成公开契约。显式同名别名避免被当作未使用导入。
    FlowRegistry as FlowRegistry,
)
from actant._internal.flow import ctx as flow_ctx

__all__ = [
    "FLOW_REGISTRY",
    "FlowDefinition",
    "FlowHandle",
    "continue_as_new",
    "current_workflow_id",
    "flow",
    "flow_ctx",
    "host_flows",
    "patched",
    "sleep_until",
    "start_flow",
    "suspend",
    "wait_signal",
]
