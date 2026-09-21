# Actant 框架契约（FRAMEWORK）

> 本文是 `actant-core` 作为**可复用编排框架**的契约文档：嵌入方可以依赖哪些面、
> 按什么规则扩展、哪些承诺不做出。0.3.5 F1/X3/X5 交付物。
> 配套验收实验：`examples/rust_embed.rs`（纯 Rust 引擎，不依赖 PyO3）。

## 一、定位与分层

Actant 的身份 = **P2P 无中心 fabric + ERH 能力分发 + 进程级隔离 + 持久化状态机**。
框架化不改变身份：`actant-core` 是语言无关的编排内核，Python 与（未来的）Rust
原生引擎都是它的宿主。

| crate | 内容 | 受众 |
|-------|------|------|
| `actant-common` | 协议、ID、配置、wire message、错误 | 两侧共用 |
| `actant-core` | Actor 运行时、DAG 编排、ERH、iroh 网络、LMDB 持久化 | Rust 嵌入方 |
| `src/`（`actant` 门面） | PyO3 绑定（`py` 模块） | Python 用户 |

依赖方向单向：`common ← core ← 门面`。CI 门禁强制 `core`/`common` 零
`crate::py`（或任何绑定层）引用；`--no-default-features` 构建是一等公民。

## 二、扩展缝（全部是编译期 trait，见"不插件化"）

| 缝 | trait | 注入点 | 内置实现 | 第二实现验证 |
|----|-------|--------|----------|--------------|
| 任务分发 | `TaskDispatcher` | `RuntimeBuilder::with_task_dispatcher` | `ProcessTaskDispatcher`（Python 进程池，绑定层注入） | ✅ `examples/rust_embed.rs` 的 `ClosureDispatcher` |
| 调度策略 | `Scheduler` | `RuntimeBuilder::with_scheduler` | priority / fifo（`SchedulerActor`） | 缝可用，未验证 |
| 节点发现 | `Discovery` | `with_discovery` / `NetworkManager::with_identity` | `none` / `local` / `dns` preset | 缝可用，未验证 |
| 传输层 | `Transport` | `with_transport` | `NetworkManager` | 缝可用，未验证 |
| 事件日志 | `EventLog` | `with_event_log` | LMDB / Memory | Memory 即第二实现 |
| 远端路由 | `RoutePolicy` | `Worker::with_route_policy` | `DefaultRoutePolicy` | 单测覆盖 |
| 能力 | `Capability` + `Handler<C>` | `CapabilityRuntime::register_*` | 6 个内置 | ✅ 双语言多实现 |
| 条件边 | `ConditionEvaluator` | `OrchestratorState::with_condition_evaluator` | 无（策略留空、使用者提供） | **正确形状模板** |

### Rust 嵌入最小清单

1. `RuntimeBuilder::new(node_id, config).with_data_dir(...)`；
2. `with_task_dispatcher(你的实现)`——进程内闭包、shell、任意语言 worker；
3. `with_orchestrator_ingest(true)`——**仅 Rust 嵌入需要**：没有 Python 事件泵时，
   任务完成后的依赖推进（后继入队）与重试裁决由 core 内回灌桥承担；
   Python 绑定层不得开启（双重回灌会产生重试裁决竞态）；
4. `build().await` 后的驱动序列：`orchestrator.submit(wf, dag)` →
   `orchestrator.start(&wf)` 返回 roots → **先 spawn `worker.run()`**
   （就绪门 `worker.is_ready()`）→ `scheduler.enqueue_batch(roots)`；
5. 后继任务由回灌桥在任务完成时自动入队；轮询 `orchestrator.get_results(&wf)` 聚合。

完整可运行示例：`cargo run --no-default-features --example rust_embed`。

## 三、进程内 worker 协议（X3 帧协议版本承诺）

worker 子进程（或任意语言的自定义 worker）与父进程经 **stdio** 通信：

- 帧 = `[4 字节小端长度][1 字节类型][正文]`，长度含类型字节；
- 类型：`Dispatch` = 0x01（Rust→worker），`Cancel` = 0x02，`Shutdown` = 0x03
  （Rust→worker），`Result` = 0x02（worker→Rust）；
- Dispatch 正文 = v2 载荷：`u8 version=0x02 | u32 retries | u32 retry_delay_ms |
  u16 len(task_id) | task_id | u16 len(workflow_id) | workflow_id | payload`
  （`task_id`/`workflow_id` 为 utf-8）；payload 语义由宿主语言定义
  （Python 绑定层为 cloudpickle）；
- **stderr 边带**（非协议通道，绝不写 stdout）：`actant_metric: <name>=<value_ms>`
  与 `actant_log: <ts_ms> <level> <task_id> <message>`（单行）。

**版本承诺**：帧协议版本 = crate 版本，0.x 阶段两端同步升级（不承诺跨版本互通）；
1.0 冻结时加版本字节与握手。Dispatch 载荷自带 `version` 字节（当前 0x02），
核心拒绝未知版本。

## 四、子工作流组合模式（X5）

任务执行体内提交另一个工作流即天然组合：

```python
@task
def parent_step(x):
    child = child_flow.submit(x)      # 在 worker 内经 Runtime 代理提交
    return child.result()
```

- **不加"子工作流节点类型"**：编排组合已可表达，加类型违反减法；
- DAG 不显示嵌套——子工作流是独立工作流，有自己的 id 与历史；
  关联只能靠任务元数据（`metadata` 字段）自行约定；
- 失败传播：子工作流失败经 `AsyncResult.result()` 重抛，父任务按普通任务失败
  进入父工作流的失败策略；父工作流取消**不会**自动取消子工作流
  （需业务层自行监听 `on_worker_state` / 生命周期事件）；
- Rust 嵌入同理：dispatcher 实现内调用 orchestrator 提交 API 即为子工作流。

## 五、不做出的承诺（护栏）

1. **不插件化**：无运行时插件装载、无配置驱动的 handler 加载、无 dylib/WASM
   ABI。扩展 = 编译期实现 trait + builder 注入；
2. **状态面不抽象**：LMDB 内嵌是身份（J6）；"数据面任意，状态面固定 LMDB"。
   `Store` 不是 trait，不出 `StoreBackend` 配置；
3. **0.x 无 API 稳定性**：trait 签名随里程碑演进（CHANGELOG 登记）；
   1.0 冻结时统一承诺；
4. **Rust 嵌入的高层 API**：`@task`/`@flow` 是 Python 语义。Rust 嵌入面对的是
   "DAG + dispatcher 原语层"，不是 DSL；DSL 引擎属第 3 层 crate（不在本仓库）；
5. **测试假件**：`MemoryEventLog`、`MockTransport`、`MockScheduler` 经
   `test-support` feature 导出（`actant-core = { features = ["test-support"] }`），
   仅用于测试，不参与生产语义。
