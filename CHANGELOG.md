# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/)，但 1.0.0 前不保证向后兼容。
每次发布记录破坏性变更、新增功能与缺陷修复。

## [Unreleased]

### 优化

- **EventBus 高并发唤醒优化**：`TaskEnqueued` 唤醒信号从 mpsc 通道订阅改为
  专用 `Arc<Notify>`。此前 1000 并发任务场景下，`TaskEnqueued` 作为 `BestEffort`
  事件因订阅者通道满被大量丢弃，产生 "subscriber is full, dropping best-effort event"
  告警。`Notify::notify_waiters()` 无队列、无丢弃，所有等待的 Worker 立即唤醒。
  相应移除 `BusEvent::TaskEnqueued` 变体与 `Topic::TaskEnqueued`；`SchedulerActor`
  的 `notify_task_enqueued()` 改为同步方法触发 Notify；Worker 的 `wait_for_task` /
  `prefetch_tasks` 改在 `notify.notified().await` 上等待。
- **EventBus 广播并发化**：`broadcast_to_subscribers` 从串行 `for` 循环改为
  `futures::join_all` 并发投递。串行实现下总耗时 = Σ(各订阅者)，并发后 = max(各订阅者)。
  对 `Reliable` 事件尤为关键：一个慢订阅者不再阻塞其他订阅者的投递。
- **EventBus 订阅者深度指标**：新增 `actant.event_bus.subscriber.depth` Gauge，
  按 `topic` 标签记录每次 publish 后各 topic 订阅者通道的最大积压深度，用于预警队列堆积。

## [0.3.0] — 2026

### 破坏性变更

- **公共 API 导出扩充**：`actant.__all__` 现完整导出全部 19 个异常类（此前仅 7 个）、
  全部 13 个 capability `Protocol` handler 类型（`RoutingHandler`/`SchedulingHandler`/…/
  `ActorLifecycleHandler`）、3 个 Actor 请求/事件 dataclass（`ActorEvent`/`ActorFailureCtx`/
  `ActorMessageReq`）以及 capability 分层常量 `PYTHON_ONLY_CAPABILITIES` 与
  `RUST_BACKED_CAPABILITIES`。此前需从 `actant.exceptions` / `actant.capabilities`
  子模块导入的符号现在均可直接从 `actant` 顶层导入。导入这些符号到顶层命名空间的代码
  无需修改；此前依赖 `actant.exceptions.StorageError` 等子模块路径的代码仍可正常工作。

### 新增

- **顶层导出**：见上文"破坏性变更"。用户实现自定义 capability handler 时可直接
  从 `actant` 导入对应 Protocol 类型作为类型约束，无需深入子模块。
- **`PYTHON_ONLY_CAPABILITIES` / `RUST_BACKED_CAPABILITIES` 常量**：表征 capability
  分层（纯 Python 策略型 vs Rust-backed），便于诊断 handler 缺失时的回退行为。

### 修复

- **类型安全**：修复 `TaskContext.on_cancel` 中 `force_after: float | None` 跨锁
  边界类型收窄失效导致的 mypy strict 错误。在锁内捕获具名值 `timer_after`，
  使锁外的 `_start_force_timer(timer_after)` 调用获得 `float` 类型保证。
- **文档与实现一致性**：修正 `max_concurrent_tasks` 默认值的文档。AGENTS.md 与
  `Runtime.with_defaults` docstring 此前声称默认 `1`（与 `WorkerConfig::default` 一致），
  但 PyO3 配置层 `_ActantConfig` 在用户不显式指定时实际取 `num_cpus::get()`（CPU 核数），
  仅纯 Rust 嵌入场景的 `WorkerConfig::default()` 才为 1。文档已修正为反映实际行为。

### 内部

- 修复 `tests/unit/test_task.py` 中 `pytest.raises(..., match="^fail$")` 的 ruff RUF043
  警告（正则字符串应使用 raw string `r"^fail$"`）。
- `actant/__init__.py` 的 `__all__` 经 ruff RUF022 自动排序。

### 已知限制

本节列出 0.3.0 在功能层面的边界条件，帮助用户判断是否满足生产场景需求。
这些限制并非缺陷，而是当前架构的明确取舍；后续版本可能解除。

#### Flow 超时无法强制中断同步代码

- `@flow(timeout_ms=...)` 通过主线程的 `threading.Event.wait(timeout)` 等待子线程，
  超时后设置 `cancel_event`，使子线程中后续的 `Task.submit` 调用抛出
  `ActantTimeoutError`，**阻止 orphan 任务继续创建**。
- 但 Python 无法强制中断正在运行的同步代码——子线程会继续执行直到函数返回或
  抛出异常。flow 线程为 daemon，主线程退出后由 OS 回收。
- 长时间运行的同步函数应在内部轮询 `cancel_event` 或拆分为多个 `Task.submit`，
  以获得及时的取消响应。

#### 任务级 `timeout_ms` 依赖协作式取消

- Rust Worker 通过 `tokio::time::timeout` 在 dispatch future 上强制触发超时，
  超时后设置 `cancel_flag`。但 Python 业务函数仍在 worker 线程中同步执行，
  无法被强制中断。
- `_run_with_timeout` 接收 `timeout_ms` 参数但 **不使用**——超时完全由 Rust
  Worker 直接从 Task spec 读取并强制执行。该函数仅在执行前/后检查 `cancel_flag`，
  函数执行期间不插入检查点。
- 超时后的执行序列：Rust 设置 `cancel_flag` → drop dispatch future（oneshot `rx`
  释放）→ handler 完成时 `tx.send(...)` 返回 `Err`，仅记录 warn 日志。**Python
  函数仍会在 worker 线程中运行到结束**——结果被丢弃，worker 线程被占用直到函数
  真正返回，可能影响后续任务调度。
- 长任务应通过 `get_task_context().is_cancelled()` 主动轮询，或使用
  `_interruptible_sleep` 替代 `time.sleep`。
- 纯 CPU 密集型且无法插入检查点的任务（如大型矩阵运算），超时仅能丢弃结果，
  无法释放计算资源——建议改为外部进程隔离。

#### 信任边界：Payload 签名不验证业务逻辑

- 启用 `payload_signing_key` 后，Actant 验证任务 payload 的 MAC 完整性，
  防止篡改与伪造。但签名 **不** 验证任务的业务语义：
  - 任何持有签名密钥的节点都可以提交任意业务函数（cloudpickle payload）。
  - cloudpickle 反序列化本身具有代码执行风险——**仅在你信任所有 Worker 节点**
    的环境中启用 generic dispatch。
- 在不信任的集群中，应禁用 generic handler 并通过自定义 `TaskDispatcher`
  限制可执行的任务白名单。
- Wire-level MAC（`set_wire_signing_key`）保护 P2P 消息完整性，但同样不验证
  消息内容的安全性。

#### 其他边界

- **Worker drain 不中断执行中任务**：`shutdown` 通过关闭 channel 让空闲 worker
  退出，正在执行的任务会等待完成（受 `drain_timeout_secs` 限制，默认 30s）。
  超时后放弃 join，worker 线程由 OS 在进程退出时回收。
- **EventBatcher 关闭**：`close()` 触发最后一次 flush 并等待 flush 线程退出
  （带 5s 超时）。进程异常退出（SIGKILL/崩溃）时少量在途事件可能丢失；
  正常 `Runtime.stop` 路径下会等待 flush 完成。
- **P2P 发现依赖 iroh**：节点发现使用 iroh 的默认发现机制（Mainnet DNS），
  自定义发现需通过 `discovery_preset` 配置（详见 README）。

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

[Unreleased]: https://github.com/actant/actant/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/actant/actant/releases/tag/v0.3.0
[0.2.0]: https://github.com/actant/actant/releases/tag/v0.2.0
[0.1.0]: https://github.com/actant/actant/releases/tag/v0.1.0
