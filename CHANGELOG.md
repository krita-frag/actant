# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/)，但 1.0.0 前不保证向后兼容。
每次发布记录破坏性变更、新增功能与缺陷修复。

## [Unreleased]

## [0.2.0] — 2025

### 破坏性变更

- **架构**：引入 Effect-Resource-Handler (ERH) 统一扩展模型，替代分散的通道式扩展点。所有扩展点（Routing、Scheduling、Transport、Store、Actor、Lifecycle）统一为 `Capability` + `Handler` + `Layer` + `Effect`。详见 AGENTS.md 的 "ERH 扩展架构" 章节。
- **重命名**：`NoRetryPolicy` → `DefaultRetryPolicy`。旧名已删除，不再保留兼容别名。
- **移除**：`execution_backend="process"` 参数及对应进程池实现。Runtime 仅保留线程后端，进程后端在 0.2.0 前未稳定且实现存在 GIL 死锁隐患，故直接移除而非降级。`Runtime` / `Runtime.with_defaults` / `cli worker` 上的 `execution_backend` 与 `worker_processes` 参数均已删除。
- **重试 handler**：`RetryPolicy` capability 改为 `ask` 语义（逆序决策，首个非 `None` 决定结果），自定义 handler 现可覆盖默认策略。
- **Worker 任务拉取**：从 sleep 轮询改为基于 `EventBus` 的 `Topic::TaskEnqueued` 事件驱动。`Scheduler` trait 移除 `task_notify_handle()` 方法，外部实现需自行迁移到 EventBus 机制或保留独立通知。
- **Runtime 启动同步**：`Runtime.start()` 不再使用 `time.sleep(0.1)` 等待 Worker 就绪，改为基于 tokio `watch` channel 的事件驱动等待。

### 新增

- **ERH 扩展架构**：13 个内置 capability（`Routing`/`Scheduling`/`RetryPolicy` 为纯 Python，其余由 Rust 核心提供 codec 与默认 handler）。三种 effect 语义：`ask`（决策型，逆序调用）、`perform`（副作用型，最后注册 handler）、`emit`（反应型，顺序调用）。
- **高层 API**：`@task` 装饰器、`AsyncResult` 任务句柄（支持依赖解析递归处理 `list`/`tuple`/`dict`）、`@flow` 工作流编排装饰器（广播 `WorkflowLifecycle` 事件）。
- **`gather` 并行等待原语**：`actant.gather(*async_results)` 批量等待多个任务完成。
- **协作式取消**：`TaskContext` + `CancelToken` 实现 Python 与 Rust 之间的协作式取消信号传递。
- **`EventBus` 内部事件总线**：统一的发布/订阅中枢，支持 `Reliable` 与 `BestEffort` 两种投递保证，订阅者超时自动修剪。
- **可观测性**：`ACTANT_TRACING`、`ACTANT_VIZTRACER`、`ACTANT_TOKIO_CONSOLE` 环境变量开关，无需改动业务代码。
- **CLI**：`actant worker` 命令，支持 `--log-level`、`--max-concurrent-tasks`、`--scheduler`、`--drain-timeout-secs` 等参数。
- **类型存根**：`actant/actant.pyi` 提供 PyO3 模块的类型注解，`actant/py.typed` 标记 PEP 561 兼容。
- **基准测试**：新增 `actor_messaging`、`capability_dispatch`、`event_bus`、`mem_profile` 四个基准测试目标。
- **属性测试**：`cargo test --test property` 验证 payload 编解码与 DAG 拓扑不变量。

### 修复

- 修复 Worker 主循环因 sleep 轮询导致的任务拉取延迟（约 1ms）与 CPU 浪费。
- 修复 `Runtime.start()` 在 Worker 初始化慢于 100ms 时返回未就绪 Runtime 的问题。
- 修复 `_execute_with_retries` 与 `_generic_execute_handler` 之间重复的重试逻辑。

### 内部

- Rust 核心代码禁止 `unwrap()`/`expect()`/`panic!`，统一使用 `Result<T, ActantError>` 传播错误。
- `parking_lot::Mutex` 替代 `std::sync::Mutex`，避免 poison 语义。
- 持久化层基于 heed/LMDB + rkyv + postcard，支持 WAL 与 CRDT 状态合并。

## [0.1.0] — 2024

### 初始发布

- Actor 模型运行时（`ActorSystem`、监督、邮箱、持久化）
- DAG 工作流协议与拓扑计算
- 基于 iroh 的 P2P 网络层（发现、直连、gossip）
- LMDB 持久化（Store、HLC、Checkpoint、WAL）
- PyO3 绑定暴露 Rust 原语为 Python 对象

[Unreleased]: https://github.com/actant/actant/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/actant/actant/releases/tag/v0.2.0
[0.1.0]: https://github.com/actant/actant/releases/tag/v0.1.0
