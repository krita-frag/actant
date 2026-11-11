# Actant 框架契约

> 本文是 `actant-core` 作为**可复用编排框架**的契约文档：嵌入方可以依赖哪些面、  
> 按什么规则扩展、哪些承诺不做出。  
> 配套验收实验：`examples/rust_embed.rs`（纯 Rust 引擎，不依赖 PyO3）。

## 一、定位与分层

Actant 的身份定义见[架构总览](overview.md)。框架化不改变身份：
`actant-core` 是语言无关的编排内核，Python 与（未来的）Rust
原生引擎都是它的宿主。

| crate               | 内容                                    | 受众        |
| ------------------- | ------------------------------------- | --------- |
| `actant-common`     | 协议、ID、配置、wire message、错误              | 两侧共用      |
| `actant-core`       | Actor 运行时、DAG 编排、ERH、iroh 网络、LMDB 持久化 | Rust 嵌入方  |
| `src/`（`actant` 门面） | PyO3 绑定（`py` 模块）                      | Python 用户 |

依赖方向单向：`common ← core ← 门面`。CI 门禁强制 `core`/`common` 零  
`crate::py`（或任何绑定层）引用；`--no-default-features` 构建是一等公民。

## 二、扩展缝（全部是编译期 trait，见"不插件化"）

Actant 没有插件注册表，也没有描述扩展点的清单文件：缝就是 trait（Rust）与
handler 链（Python），注入就是调用一次 builder 方法或 `chain`，其余由用户自己在
Rust / Python 侧拼装。因此「有哪些缝」直接看 `RuntimeBuilder::with_*` 与
`rt.layer(...)`，不存在第二份需要同步的登记表。

| 缝 | 注入点 | 跨节点可用 | 运行期可替换 |
| --- | --- | --- | --- |
| `Scheduler` | `RuntimeBuilder::with_scheduler` | 是 | 否 |
| `RoutePolicy` | `RuntimeBuilder::with_route_policy`（或 `Runtime(route_policy=...)`） | 是 | 否 |
| `TaskDispatcher` | `RuntimeBuilder::with_task_dispatcher` | 是 | 否 |
| `Transport` | `RuntimeBuilder::with_transport` | 是 | 否 |
| `Discovery` | `RuntimeBuilder::with_discovery` | 是 | 否 |
| `EventLog` | `RuntimeBuilder::with_event_log` | 是 | 否 |
| `SubmitBackend` | `Task.submit` 的后端协议（Python 侧） | 否 | 是 |
| 内置 capability handler | `rt.layer(name).chain(handler)` | 否 | 是 |

「跨节点可用」只对 Rust 缝成立：实现随引擎部署，workflow failover 后仍在本节点
可用；Python handler 与 `SubmitBackend` 是进程内对象，不随工作流迁移。

**统一的是词汇，不是调用路径**。所有扩展共用 capability / handler / effect 词汇，
但按解析时机分两个执行域：

- **构造期定稿**：`Arc<dyn Trait>` 句柄在 `Runtime::new` / `RuntimeBuilder::build`
  二选一并冻结，运行期只有一次 vtable 调用、不查注册表、没有 setter。6 条 Rust 缝
  属于此域；`route_policy` 的实现可以是 Python callable，但仍由
  `resolve_route_policy` 在构造期决定并注入，运行期不存在「是否安装」的分支。
- **运行期解析**：`rt.layer(...).chain(...)` 可增删覆盖，用于数据面与观测面。
  `CapabilityRuntime` 的 `ask` / `perform` / `emit` 需经注册表查表、编解码与 Actor
  邮箱往返，只服务 Python 侧发起的 effect，不进入每任务 / 每消息路径。

Rust 侧自行拼装：

```rust
let runtime = RuntimeBuilder::new(node_id, config)
    .with_data_dir(data_dir)
    .with_scheduler(Arc::new(MyScheduler::default()))
    .with_task_dispatcher(Arc::new(MyDispatcher::default()))
    .build()
    .await?;
```

Python 侧自行拼装：

```python
rt = Runtime(route_policy=my_callable)   # 构造期注入，运行期不可换
rt.layer("Store").chain(my_store_handler)  # 运行期可增删覆盖
```

冻结是单向的：构造期定稿后运行期不可替换、不可热更新，换实现必须重建 Runtime；
这与 `crates/actant-core/src/runtime/capability.rs::bind_actor_system` 在 spawn 时
快照 handler、绑定后禁止 `chain` 的语义一致。放置类策略（路由 / 调度 / 重试）不是
capability：它们由 Rust 层的 `RoutePolicy` / `Scheduler` 承担，避免进程内闭包在
failover 后失效。

**性能边界用结构守，不用数值阈值**：`tests/python/unit/test_hot_path_guard.py`
静态扫描热路径白名单文件（路由、调度、任务分发、传输广播、事件日志落盘、上下文），
禁止 `CapabilityRuntime` 三动词调用与 `Python::attach` / `with_gil` 出现在每任务 /
每消息路径。只禁调用形态，不禁类型名与 postcard 序列化——`context.rs` 合法持有
`CapabilityRuntime`。`.github/workflows/bench.yml` 只产出 criterion artifact 供人工
跨 run 对比，不做阈值断言：共享 runner 的 CPU 噪声大于真实回归幅度，不能当门禁。

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

### 自定义 Scheduler / Transport 的契约验收

实现 `Scheduler` 或 `Transport` 时，启用 `test-support` feature 后可直接调用
框架自带的契约测试，把本文与 trait docstring 里的语义承诺跑成断言：

```rust
// Cargo.toml: actant-core = { features = ["test-support"] }
use actant_core::test_support::{assert_scheduler_contract, assert_transport_contract};

assert_scheduler_contract(&my_scheduler).await;
assert_transport_contract(&my_transport, "actant:contract").await;
```

`assert_scheduler_contract` 覆盖：计数自洽、单任务/批量出队、`drain_unrouted`
只取未路由任务、`close` 幂等且拒绝新入队、已排队任务在 `close` 后仍可取、
关闭后 `dequeue` 返回 `None`。`assert_transport_contract` 覆盖：身份非空、
重复 `subscribe` 容忍、已订阅 topic 可广播、地址可读、`discover_peers` 不失败、
重复 `shutdown` 容忍。契约测试不覆盖各实现特有的语义（优先级顺序、真实网络
行为），那部分仍需实现方自己的测试。

### Python 侧 capability 扩展路径

Python 层有两条扩展路径，均经 `rt.layer(...)` 登记 handler，**只作用于 Python 发起的  
`actant.effects.ask` / `actant.perform` / `actant.emit` 调用**：

| 路径               | 入口                                                          | 适用                                                                    |
| ---------------- | ----------------------------------------------------------- | --------------------------------------------------------------------- |
| 覆盖内置 capability  | `rt.layer(name).chain(handler)` 或 `rt.chain(name, handler)` | 替换/追加数据面 handler（如 `Store` 换自建 KV、`ValueStore` 换 S3 后端）；只影响 Python 侧发起的 effect |
| 新增自定义 capability | `rt.layer(name, kind).chain(handler)`                       | 声明新的 `ask`/`perform`/`emit` 扩展点，供自建的第 3 层代码组合                         |

**双分发规则（关键契约）**：Python handler 与 Rust 内部 handler 是两条独立分发路径。

- Python 侧：`_dispatch_ask/perform/emit` 先走 Python handler 链；Python handler  
  缺失或弃权时，**仅** `RUST_BACKED_CAPABILITIES`（`Store` / `TaskLifecycle` /  
  `WorkflowLifecycle`）回退到 Rust capability runtime；其中只有 `Store` 有
  Rust handler，两个 lifecycle 在 Rust 侧是空 layer。
- Rust 内部：Worker 执行、orchestrator 持久化等 Rust 内部分发路径**不咨询**  
  Python `_layers`——Python handler 永不进入 Rust 内部 dispatch。
- `ValueStore` 无 Rust 内部分发参与，只有 Python 这一条路径。
- 放置类策略（路由/调度/重试）不在 Python 层接线：它们由 Rust
  `RoutePolicy` / `Scheduler` 或第 3 层外部调度器承担，避免进程内闭包在
  workflow failover 后失效。

要覆盖 **Rust 内部行为**（任务执行、存储、传输等），只能用 rust 层的
`RuntimeBuilder` 注入缝（见本节表格），不能靠 `layer().chain()`。

`rt.layer("Store").chain(handler)` 是公开的数据面扩展点，边界由
`tests/python/integration/test_store_capability_override.py` 锁定：**Python 侧
终态记录 writer** 的写入经 `_dispatch_perform("Store")` 经过该 handler；
Rust 内部（Worker 执行、Orchestrator 状态持久化）直接读写 LMDB，不咨询
Python `_layers`。两条路径共用 capability 名字，但不共用分发链。

### Python→Rust 扩展范式：`RoutePolicy`

`Runtime(route_policy=callable)` 是当前唯一把 Python 代码接入 Rust 热路径的
扩展面。新增同类扩展照抄以下三点，不要另起一套 ERH：

1. 桥类型见 `src/py/route_policy.rs` 的 `PyRoutePolicy`，解析在
   `resolve_route_policy`（str → 内置名 / callable → 桥 / 其他 → `TypeError`）。
2. **必须 `try_attach`**：`select_target` 在 tokio worker 上同步执行，主线程持
   GIL 等待会与 tokio 互等死锁。
3. 回归只经公共入口驱动，见
   `tests/python/integration/test_route_policy_python.py`：断言 callable 的真实
   调用与任务最终结果，不直接构造 Rust 侧桥类型。

不要桥接的缝及理由：`Scheduler`（8 个 async 在热路径）、`Transport`（返回
`&NodeId` / 独占流）、`Discovery`（入参是 iroh `Builder`）、`EventLog`（需
`LogEntry` codec）、`TaskDispatcher`（与 worker 帧协议语义重复）。

#### 新增 Python→Rust 桥接的判定与模板

新增桥接前先过三道判定，任一不通过就留在 Rust 或下沉到第 3 层：

1. **位置**：该缝是否在每任务 / 每消息路径上？是 → 需要先证明 Python 回调不会
   阻塞热路径，且失败可回退到内置策略。
2. **签名**：trait 是否返回借用（如 `&NodeId`）、独占资源（stream / iroh
   `Builder`）或要求 `async`？是 → 不桥接。
3. **承载**：实现是否需要跨节点随工作流迁移？是 → 必须是 Rust 实现，Python
   闭包在 failover 后不存在。

通过判定的桥接按 `PyRoutePolicy` 的五步模板实现，不要另起一套机制：

```rust
// 1. 桥类型：src/py/<seam>.rs，持有 Python 对象，只做数据转换。
struct PyMySeam { callback: Py<PyAny> }

// 2. 构造期解析：字符串 → 内置实现；callable → 桥；其余 → TypeError。
fn resolve_my_seam(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<...> { ... }

// 3. 热路径回调必须 try_attach：拿不到 GIL 时回退，绝不阻塞等待。
// 4. 回调抛异常 / 返回非法值时记 warning，回退内置策略。
// 5. 回归只经公共入口驱动，见 tests/python/integration/test_route_policy_python.py。
```

模板的验收要求与其它扩展缝一致：注入入口 + 一条真实驱动生产路径的接线测试 +
本节的边界说明；热路径文件仍受 `tests/python/unit/test_hot_path_guard.py` 约束。

### 任务提交接缝

`Task.submit` 经 `actant/_internal/tasks/backend.py` 的 `SubmitBackend` 协议提交，
不直接解析全局 Runtime。默认 `EmbeddedSubmitBackend` 转交进程内 Runtime 与 Rust
调度器；跨进程提交只需在同一协议上提供远程实现，`Task` 与 `@task` 的调用语义
不变。`submit_batch` 仍走内嵌批量路径（批次注册与回滚语义属于内嵌实现）。

**稳定性归属**：`SubmitBackend` 位于 `actant._internal`，分级为 internal——它是
实现接缝而非公开契约，方法签名随核心演进，不承诺跨 0.x 兼容。需要稳定跨进程
提交的调用方应使用 `extensions/act-service` 的 `ServiceClient`，不要基于本协议
自行实现长期依赖。

接缝由线程局部的 `use_submit_backend(backend)` 临时替换；`resolve_submit_backend()`
优先取显式后端，否则按当前 Runtime 构造 `EmbeddedSubmitBackend`，无活跃 Runtime
时返回 `None`（由 `Task._submit` 转成 `InvalidStateError`）。这是 Python 侧唯一的
提交路径替换点，不影响 Rust 内部 dispatch。

## 三、进程内 worker 协议（帧协议版本承诺）

worker 子进程（或任意语言的自定义 worker）与父进程经 **stdio** 通信：

- 帧 = `[4 字节小端长度][1 字节类型][正文]`，长度含类型字节；
- 类型：`Dispatch` = 0x01（Rust→worker），`Cancel` = 0x02，`Shutdown` = 0x03  
  （Rust→worker；协议保留。当前 Rust 关闭时直接强杀空闲 worker，不发送该帧，  
  Python worker 已实现其处理），`Result` = 0x02（worker→Rust）；
- Dispatch 正文 = v2 载荷：`u8 version=0x02 | u32 retries | u32 retry_delay_ms |
  u16 len(task_id) | task_id | u16 len(workflow_id) | workflow_id | payload`  
  （`task_id`/`workflow_id` 为 utf-8）；payload 语义由宿主语言定义  
  （Python 绑定层为 cloudpickle）；
- **stderr 边带**（非协议通道，绝不写 stdout）：`actant_metric: <name>=<value_ms>`  
  与 `actant_log: <ts_ms> <level> <task_id> <message>`（单行）。

**版本承诺**：帧协议版本 = crate 版本，0.x 阶段两端同步升级（不承诺跨版本互通）；  
1.0 冻结时加版本字节与握手。Dispatch 载荷自带 `version` 字节（当前 0x02），  
核心拒绝未知版本。

### 载荷完整性

- Rust 核心**不感知**宿主语言的参数语义（args/kwargs/TaskRef 等）；tag 常量仅用于 `unpack_payload` 内部校验，不构成跨语言契约。
- `pack_group` 由 Python 侧调用构建分组 payload，Rust 视为不透明字节。
- 所有任务 payload 序列化后签名、反序列化前验证，阻止被攻破的节点向集群投递恶意 cloudpickle payload。

## 四、子工作流组合模式

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
2. **状态面不抽象**：LMDB 内嵌是身份；"数据面任意，状态面固定 LMDB"。  
   `Store` 不是 trait，不出 `StoreBackend` 配置；
3. **0.x 无 API 稳定性**：trait 签名随版本演进（CHANGELOG 登记）；  
   1.0 冻结时统一承诺；
4. **Rust 嵌入的高层 API**：`@task`/`@flow` 是 Python 语义。Rust 嵌入面对的是  
   "DAG + dispatcher 原语层"，不是 DSL；DSL 引擎属第 3 层 crate（不在本仓库）；
5. **测试假件**：`MemoryEventLog`、`MockTransport`、`MockScheduler` 经  
   `test-support` feature 导出（`actant-core = { features = ["test-support"] }`），  
   仅用于测试，不参与生产语义。

## 六、语义中性通用原语（M1 / M4）

以下原语只在核心出现 `label / quota / batch / log / node-lifecycle` 词汇，领域语义
（镜头/帧/license/matrix/pipeline）全部留在第 3 层。

| 原语 | 入口 | 语义与边界 |
|---|---|---|
| 自动放置 | `TaskDefinition.tags` + `RoutePolicy -> RouteDecision` | `Local` 前强制校验本地 `node_labels ⊇ tags`；不匹配走 `Remote` 或 `Defer`（退避重入队），超过 `starvation_timeout_ms` 失败。路由只是**放置建议**，执行节点的本地闸是硬约束 |
| batch 进度 | `BatchTracker` / `submit_batch(batch_id=...)` / `Runtime.batch_progress` | 内存态成员索引 + 幂等终态结算；Rust 侧无法区分业务失败（结果正文不透明），终态由能解析 payload 的 Python 结果泵上报；不落盘 |
| 命名配额 | `TaskDefinition.quota` + `WorkerConfig.node_quotas` + `QuotaTracker` | 单维命名容量（`gpu` / license / 环境名），执行节点本地原子扣减、`QuotaGuard` RAII 归还；剩余容量随心跳广播供路由跳过已满节点；**多维资源准入不进核心**（L3 用 label + 配额组合） |
| 逐任务日志 | `EventBus::subscribe_task_logs(task_id)` / `Runtime.on_task_log(task_id=...)` | 与全局 `TaskLog` 话题并存的 tap 通道，满即丢；不承载正确性语义 |
