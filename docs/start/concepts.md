# 核心概念

## Runtime

`Runtime` 是节点入口：持有 Rust 核心、capability 注册表与任务注册表，提供 `ask` / `perform` / `emit` 三种 effect 执行模型。绝大多数场景用 `Runtime()` 构造。

```python
from actant import Runtime

with Runtime(name="node-a", data_dir="./data") as rt:
    ...
```

## Task 与 AsyncResult

`@task` 把函数包装成 `Task`。直接调用是同步执行；`submit()` 提交到 Rust 调度器并由 worker 子进程执行，返回 `AsyncResult` 句柄。

`AsyncResult` 提供 `result()` / `exception()` / `state` / `done()` / `ref()`。作为下游 `submit()` 参数时会自动等待并解析依赖。

## Flow

`@flow` 是工作流编排装饰器。flow 体内的 `Task.submit` 会被记录为 DAG 节点，由 Rust Orchestrator 调度。工作流是**惰性创建**的：函数体第一次提交任务或进入等待点时创建，因此生命周期事件里的 `workflow_id` 拿到即可用。

## Capability / Handler / Layer / Effect

Actant 的扩展面只有两类：Rust 编译期 trait 缝（经 `RuntimeBuilder` 注入）与用户态 capability handler（经 `rt.layer(...).chain(...)` 登记）。**统一的是词汇**，不是调用路径；按解析时机分为构造期定稿的 `build_time` 域与运行期动态解析的 `runtime` 域，边界见[框架契约](../architecture/framework-contract.md)。

ERH 描述 `runtime` 域的扩展词汇：

- **Capability**：能力声明（名称 + effect 类型）。
- **Handler**：能力实现，可组合。
- **Layer**：同一 capability 的 handler 链。
- **Effect**：请求能力，分 `ask`（决策）/ `perform`（副作用）/ `emit`（广播）。

内置 4 个 capability：`Store` 回退 Rust `StoreHandler`（可用 `rt.layer("Store").chain(...)` 覆盖），`ValueStore` 由 Python 默认 handler 提供，`TaskLifecycle` / `WorkflowLifecycle` 由引擎在生命周期节点发射。自定义 capability 经 `rt.layer(name, kind).chain(...)` 登记，由用户代码驱动。放置类策略（路由/调度/重试）不是 capability，边界见[框架契约](../architecture/framework-contract.md)。

## Worker 与节点

每个 `Runtime` 同时是编排器与执行器。任务经 `ProcessTaskDispatcher` 在 worker 子进程中执行，每进程同一时刻一个任务；超时或取消时强杀对应进程，崩溃只失败当前任务并触发重路由。

## Ref 与大结果

超过 `REF_INLINE_THRESHOLD`（1MB）的结果经 `ValueStore` 落到本节点内容寻址 blob store，`AsyncResult` 内部以 `Ref` 表示，`result()` 透明拉取反序列化。

## 下一步

- [任务](../guide/tasks.md)
- [工作流](../guide/flows.md)
- [能力扩展](../guide/capabilities.md)
- [框架契约](../architecture/framework-contract.md)：哪些缝能扩展、从哪一侧扩展
- [架构总览](../architecture/overview.md)
