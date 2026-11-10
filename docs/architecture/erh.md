# ERH 扩展模型

ERH 描述 `resolution=runtime` 的执行域：数据面与观测面由 Python 侧发起，经 handler 链解析。
Rust 引擎的每任务 / 每消息路径不使用 ERH——它们在构造期定稿为 trait 句柄
（`resolution=build_time`）。统一的是清单、词汇与验收契约，不是分发机制；
全量条目与执行域见[扩展点清单](../reference/extension-points.md)。

数据面与观测面的扩展点统一建模为：

- **Capability**：能力声明。
- **Handler**：能力实现。
- **Layer**：handler 组合。
- **Effect**：请求能力。

## 三种 effect

`ask` / `perform` / `emit` 的语义与调用顺序见[能力扩展指南](../guide/capabilities.md)；后注册的 handler 优先级更高。

## 内置 capability

内置 4 个 capability，均有引擎发射点或默认 handler；登记与接线由 `tests/python/unit/test_capability_wiring.py` 的生产发射点扫描共同约束。权威描述见[核心概念](../start/concepts.md)。

| 名称 | 面 | 实现 | 接线状态 |
| --- | --- | --- | --- |
| `Store` | 数据面 | Rust `StoreHandler` | 开箱即用，可用 `rt.layer("Store").chain(...)` 覆盖 |
| `ValueStore` | 数据面 | 默认 handler 为 Python → Rust blob 桥 | 开箱即用，库内由 `Ref` 使用 |
| `TaskLifecycle` | 观测面 | Python 侧引擎发射 | 用户以 emit handler 订阅 |
| `WorkflowLifecycle` | 观测面 | Python 侧引擎发射 | 用户以 emit handler 订阅 |

`RouteCtx` / `ScheduleCtx` / `RetryCtx` 仅作为用户自定义 capability 的 payload / 签名助手保留。

## 放置类策略不是 capability

路由、调度、重试由 Rust `RoutePolicy` / `Scheduler` 或第 3 层外部调度器承担，避免进程内闭包在 workflow failover 后失效。

## 双分发规则

- Python 侧：先走 Python handler 链；缺失或弃权时，仅 `RUST_BACKED_CAPABILITIES` 回退到 Rust 内置 handler。Python 侧终态记录 writer 的写入也在这一侧。
- Rust 内部：Worker 执行、Orchestrator 状态持久化直接读写 LMDB，不咨询 Python `_layers`。
- `ValueStore` 只有 Python 一条路径。

因此「引擎侧持久化写入经过 Store handler」只指 Python 侧终态记录 writer，
不指 Rust 内部的 LMDB 路径。

要改 Rust 内部行为，只能用 Rust `RuntimeBuilder` 的注入缝，不能靠 `layer().chain()`。

## 相关页面

- [能力扩展指南](../guide/capabilities.md)
- [扩展点清单](../reference/extension-points.md)：全部扩展点与执行域
- [框架契约](framework-contract.md)
- [扩展点维护](../contributing/extensions.md)：新增或修改条目的流程
