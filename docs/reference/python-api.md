# Python API

本页由源码 docstring 自动生成。公共入口以 `actant.__all__` 为准。

其他参考页：[PyO3 绑定 API](pyo3-api.md) · [配置项](configuration.md) · [CLI](cli.md) · [Rust API](rust-api.md)。

## 顶层与子命名空间

`actant.__all__` 只保留 17 个最常用入口，其余 API 按职责放在子命名空间：

| 用途 | 导入路径 | 名字 |
| --- | --- | --- |
| 运行时 | `actant` | `Runtime` `Layer` `use_runtime` `get_current_runtime` |
| 任务 | `actant` / `actant.tasks` | 顶层 `task` `AsyncResult` `Ref` `gather`；`actant.tasks.Task` `TaskContext` `TaskState` `BatchProgress` `get_task_context` |
| 工作流 | `actant` / `actant.workflow` | 顶层 `flow` `start_flow` `host_flows` `FlowHandle`；`actant.workflow.sleep_until` `wait_signal` `suspend` `continue_as_new` `patched` `current_workflow_id` `flow_ctx` `FlowDefinition` `FLOW_REGISTRY` |
| 异步面 | `actant.aio` | `AsyncDriver` `AsyncFlowContext` `flow` `ask` `perform` `perform_batch` `gather` |
| Effect | `actant` / `actant.effects` | 顶层 `ask` `emit` `perform`；`actant.effects.impossible` |
| 能力与事件 | `actant.capabilities` | `EffectKind` `StoreReq` `ValueStoreReq` `TaskEvent` `WorkflowEvent` `RetryCtx` `RouteCtx` `ScheduleCtx` 及对应 Handler 协议 |
| 顶层异常与版本 | `actant` | `ActantError` `__version__` |
| 异常 | `actant.exceptions` | 全部具名异常与 `raise_for_kind` / `raise_for_state` |
| 观测 | `actant.observability` | `metrics_text` |
| 测试台 | `actant.testkit` | `close_peer` `reopen_peer` `delay_heartbeat` `inject_hlc_drift` `kill_node` |

同步面只保留默认名；异步动词集中在 `actant.aio`，与同步面同名（`aio.flow` /
`aio.ask` / `aio.gather`）。这条命名约定**只约束顶层与 `actant.aio` 的动词面**，
不提供 `aio.flow_async` 之类后缀；个别底层入口仍带 `_async`（如
`Runtime.wait_wait_point_async()` 及 PyO3 绑定层的 `perform_async` / `ask_async`），
它们不属于 `actant.aio` 动词面。跨边界的 `AsyncResult`
同步读 `result()`、异步 `await`；在 event loop 上调用阻塞读会立即抛
`BlockingInEventLoopError`，而不是冻结循环。

```python
from actant import Runtime, flow, task
from actant import aio
from actant.effects import ask
from actant.workflow import patched, wait_signal
```

`from actant import *` 只导入 `actant.__all__` 里的 17 个名字（含 `__version__`：虽以下划线开头，但显式列入 `__all__` 因而同样被导出）；旧路径（如 `from actant import Task`）已移除，不再提供兼容垫片。

::: actant
    options:
      members: true
      show_if_no_docstring: false

## 任务与工作流模块

::: actant.tasks

::: actant.workflow

## 异步面

::: actant.aio

## 扩展与观测

::: actant.effects

::: actant.observability

## 测试台

::: actant.testkit
