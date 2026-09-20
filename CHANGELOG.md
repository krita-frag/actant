# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/)，但 1.0.0 前不保证向后兼容。
每次发布记录破坏性变更、新增功能与缺陷修复。

## [Unreleased](https://github.com/actant/actant/compare/v0.3.0...HEAD)

### 破坏性变更

- **Actor 持久化机器整体移除（0.3.4 B1/B2/B3，−1100 行）**：`ActorPersistence`
  （CheckpointManager / WalWriter / WalReader / WalCompactor / ActorSnapshot，state.rs
  同步删除）与 mailbox pending 持久化（`PersistentMessage` / `recover_pending` /
  `ack_message` / delivery-count / 毒消息 bounded-redelivery）删除。`Actor` trait 收缩：
  `save_state` / `load_state` / `supports_state_persistence` 钩子删除；`ActorConfig`
  删除 `wal_compaction_interval_secs` / `checkpoint_retention_count`；`ActorSystem`
  删除 `with_checkpoint` / `with_wal` / `start_compaction_task` / `stop_compaction_task`，
  定位收缩为"系统 actor 专用本地运行时"；`Topic::WalCompacted` 话题与
  `BusEvent::WalCompacted` 变体、`actant.actor.save_state_ms` / `load_state_ms` 指标、
  wire 常量 `store_keys::CHECKPOINT` 一并删除；builder 不再创建 `data_dir/actor` 子目录
  与 `actor.wal`。mailbox 投递语义由 at-least-once 变为**进程内 at-most-once**——工作流
  恢复由 orchestrator 的统一工作流历史（0.3.3 S0）唯一承载，mailbox 重放作为与历史
  重放冲突的第二恢复路径不再存在（鲁棒性净增）。零生产消费者（四个系统 actor 均未启用
  状态持久化），已有数据目录中的 `actor/` / `actor.wal` 残留文件可手动删除。

- **`WorkerConfig.python_path` 删除，worker 拉起规格语言中性化（0.3.4 F3）**：
  字段替换为 `worker_args: Vec<String>` + `worker_env: BTreeMap<String, String>`；
  `ProcessTaskDispatcher` 不再感知 `PYTHONPATH` 与 `-m actant.task._worker`——解释器
  路径、模块入口与环境变量由 Python 绑定层（`src/py/config.rs`）拼装，Rust 纯嵌入
  场景直接配置三要素。`WorkerLaunchSpec::with_python_path` 删除，改 `new(program,
  args, env)`。同时指标边带修复：worker 发射名 `python.handler_ms` → `task.handler_ms`
  （与 Rust 侧 0.3.3 已改名的 `METRIC_TASK_HANDLER_MS` 对齐——此前两者失配，任务
  耗时直方图被静默丢弃）。

- **`Runtime.production()` 空 allowlist 拒启（0.3.4 身份与信任）**：
  `network.allowed_peer_ids` 为空从 warn 升级为 `ValueError`——开放成员身份不属于
  生产语义。同时强制 `require_signed_records=True`（心跳节点记录签名）。原依赖
  "空白名单 + 告警"跑生产的部署需显式登记集群成员 endpoint id（`peer_id` 查询）。

- **SHM ring 传输移除，worker IPC 统一 stdio pipe（0.3.x 减法）**：
  `src/runtime/worker_shm.rs` / `src/runtime/worker_ring.rs` 与 dispatcher、
  `_worker.py` 中的全部共享内存 ring 路径删除——worker 子进程 IPC 回归纯 stdio
  长度前缀二进制帧（``[u32 长度][u8 类型][正文]`` 连续写入 pipe，Windows/POSIX
  行为归一）。`ACTANT_SHM_RING_FD` / `ACTANT_SHM_RING_SIZE` /
  `ACTANT_DISABLE_SHM_RING` 环境变量消失，诊断脚本不得再设置或依赖它们；帧协议
  字节格式与调用方 API 零变化。删除依据：ring 只优化大载荷搬运常数（Windows 上
  本就禁用、pipe 是处处存在的回退路径），而实测瓶颈是 IPC 往返（见
  `docs/PERF_REPORT.md`）；大载荷的正确归宿是 0.3.2 的内容寻址 `Ref` 原语而非
  塞进帧——超过 `MAX_FRAME_BYTES`（256MB）的载荷在提交侧快速失败，大的返回体
  经 pipe 帧传输的吞吐低于原 ring 路径（线性字节搬运 + 内核拷贝）。

- **metrics HTTP server 从核心层外置（0.3.x 减法）**：`Runtime.start_metrics_server()` /
  `stop_metrics_server()` 方法、`Runtime(metrics_bind=...)` 构造参数与内置
  `_MetricsHTTPServer`/`_PrometheusHandler` 类删除——核心 Python 层不再托管 HTTP
  server。`Runtime.metrics_text()` / `actant.metrics_text()`（Prometheus exposition
  format 文本）保留，用户可用标准库 `http.server` 自行托管（样例见
  `examples/metrics_server.py`）；CLI 侧 `actant worker --metrics-port` 与
  `actant metrics` 行为不变（HTTP 托管移入 `actant/cli.py` 自有 handler）。

- **Supervision 观测面移除（0.3.x 减法）**：`SupervisionEvent` / `SupervisionTree`、
  `BusEvent::SupervisionEvent` 变体与 `Topic::Supervision` 话题删除——生产路径中
  这些事件全部发布到无订阅者的总线（虚空发射），零消费方。失败/panic 的可观测性
  由 `tracing::error!` 日志、`ActorLifecycleError` 事件与 `inc_actors_failed` 指标
  承载；actor 启动/停止由 `inc_actors_spawned` / `inc_actors_stopped` 指标与 debug
  日志承载。`ActorConfig.supervision_event_capacity` 配置字段随之删除（serde 兼容
  不保留，0.x）。订阅 `Topic::Supervision` 的观测代码需改订阅 `Topic::ActorLifecycleError`
  或自行消费指标。

- **generic dispatch 机器残留移除（0.3.x 减法）**：`TaskRegistry`（已无生产构造点）、
  `GENERIC_DISPATCH_NAME` 常量及其 PyO3 导出 `actant.__actant_generic_dispatch_name__`、
  `TaskDispatcher::register_handler` no-op 方法与 `TaskHandler` 兼容别名全部删除——
  均为已删除的线程池后端 / `register_python_dispatch_handler` 的残留面。`Execute`
  capability 现直接调用 `TaskDispatcher::dispatch`（进程池），不经过上述任何名称。

- **EventBus 契约收缩（破坏性配置与 API 变更）**：`DeliveryGuarantee` 枚举删除，
  `publish` 从 async 变为同步方法（调用方移除 `.await`）；`EventBusConfig` 删除
  `publish_timeout_ms` / `max_subscriber_timeouts` 字段（serde 兼容不保留，0.x），
  仅保留 `subscriber_capacity`；指标 `actant.event_bus.publish.timeout` 更名为
  `actant.event_bus.publish.dropped`；`BusEvent::Heartbeat/Claim/DagUpdate/
  HeadsExchange` 变体与 `Topic::ClusterHeartbeat/ClusterClaim/DagUpdate/
  HeadsExchange` 话题删除（控制面直连，见「优化」小节）。依赖 EventBus 承载
  控制面语义的调用方需改用直连分发。

- **跨节点 Actor 调用面移除（0.3.1 剪裁 T1）**：`ActorRouter`（Random/RoundRobin/LeastLoaded）、跨节点 `ActorRegistry` gossip、`ActorSystem` 远端调用路径（`call_remote` / `handle_remote_request` / `deliver_reply`）及相关 wire 协议类型已删除。Rust 内部系统 actor（Workflow/Scheduler/Failover/DagGossip）全部本地 spawn，不受影响。受影响的 wire 协议面：`RemoteActorRequest` / `RemoteActorReply` / `RemoteReplyAddress` / `WireMessage::RemoteActorReply` 消息类型、`Topic::actor` / `Topic::actor_reply` 话题（`actant:actor:*` / `actant:actor-reply:*` 前缀）及对应 `TopicRoute::Actor` / `TopicRoute::ActorReply` 路由分支不再存在——0.x 阶段两端同步发版，混合版本集群的跨节点 Actor 消息将被对端丢弃。同步删除配置项：`NetworkConfig.actor_router_strategy`、`actor_registry_gossip_interval_ms`、`ActorConfig.remote_call_timeout_ms` / `remote_call_max_retries` / `remote_call_retry_delay_ms`、`init_actor_system` 的 `network` 参数。

- **CRDT 模块移除（0.3.1 剪裁 T2）**：`src/runtime/state/crdt.rs`（ORSet/GCounter/LWWRegister）为死码，全仓零引用，整文件删除。DAG gossip 状态合并使用 HLC 比较语义，不受影响。

- **Python-facing Actor API 移除（0.3.1 剪裁 T3/T4/T5/T6，capability 13 → 10）**：`_ActorCore`（`spawn_actor`/`call_method` 等全部方法，全仓零调用方）、`PythonActor`、`ActorMessaging`/`ActorSupervision`/`ActorLifecycle` 三个 capability 及其 ctx dataclass 与 Handler Protocol、`_Event.orchestration()`/`_Event.supervision()`（无构造路径）、`_RuntimeCore.retry_policy`/`set_retry_policy`（零调用）、`register_python_dispatch_handler`（no-op）全部删除；`_NetworkConfig.actor_router_strategy`/`actor_registry_gossip_interval_ms` 同步摘除。内置 capability 收敛为 10 个（策略型 Routing/Scheduling/RetryPolicy + Rust-backed Serialization/Transport/Store/Execute/TaskLifecycle/WorkflowLifecycle/NodeLifecycle）。本地 `ActorSystem`（spawn/mailbox/at-least-once/取消/持久化）保留，仍是四类系统 actor 的生产底座；`ActorError` 异常保留（本地 ActorSystem 仍产生 `actor` kind）。另删除 `observability::shutdown()` no-op 与未实现的 relay map 配置字段。

### 新增（0.3.4：身份与信任 + 节点可见性 + 二次开发条件 + API 暴露）

- **身份与信任**：节点身份 = iroh endpoint keypair，随 `data_dir/identity.key` 持久化
  （raw 32 字节 ed25519 seed，unix 0600；无 data_dir 时临时随机）——endpoint id 跨重启
  稳定，`allowed_peer_ids` 白名单因此可维护（同 data_dir 重启 peer_id 不变，测试断言）。
  心跳节点记录签名：`NodeHeartbeat.signature`（serde default）由发送方 endpoint 私钥
  ed25519 签名（签名域 = `signature = None` 的结构序列化），验证公钥来自心跳自带
  `endpoint_addr`（endpoint id 即公钥），无需密钥分发；`NetworkConfig.require_signed_records`
  （默认 false）开启时 `FailoverManager` 拒绝缺签/坏签心跳，`allowed_peer_ids` 非空时
  同时做成员校验（堵 gossip 侧旁路，直连侧另有 ALPN 白名单）。**验收**：未持密钥的
  对端无法提交任务（直连被 ALPN 拒 + wire MAC 验签拒 + 心跳被拒不进路由视图）；
  伪造签名（用自己的密钥签他人的 endpoint 身份）被拒的单元测试。wire per-node
  MAC 注册表保留为消息完整性层（对称完整性 ≠ 节点记录来源认证，见 plans/PLAN.md
  §身份与信任裁决 4）。

- **节点可见性（N1/N2）**：`NodeHeartbeat` 增 `platform: Option<PlatformInfo>`
  （os/arch/actant_version 核心自动填充 + `host_runtime` 绑定层补充，如
  "CPython 3.12.1"）与 `labels: BTreeMap<String,String>`（用户自定义，超 4KB 整体
  丢弃），serde default 向后兼容混版本集群。`FailoverManager::peers()`（心跳新鲜度
  过滤）与 py 桥 `Runtime.peers() -> list[dict]`（node_id/endpoint/slots/
  active_workflows/labels/platform/last_heartbeat）。`ActantConfig` 增
  `node_labels` / `node_host_runtime`；`_ActantConfig` 增 `node_labels` kwarg。
  **验收**：双节点 e2e 中 peers() 返回对端 slots/labels/platform。

- **二次开发条件（G-relay/G-route）**：`NetworkConfig.relay_endpoints: Vec<String>`
  → iroh `RelayMode::Custom`（覆盖 preset relay，discovery 与 relay 正交）；py 层
  `_NetworkConfig(relay_endpoints=[...])`。`RoutePolicy` trait（`workflow/route.rs`）：
  `RouteCandidate`/`RouteContext` + 默认实现 `DefaultRoutePolicy`（复刻原
  `Worker::select_remote_target` 硬编码逻辑：TTL 过滤 + 槽位比较 + 稳定排序），
  `Worker::with_route_policy` 注入自定义策略。

- **API 暴露批（E5-E7）**：`Runtime.delete_workflow(workflow_id)`（运维清理，
  幂等；`evict_workflow` 与其为同一操作，按查重纪律不重复暴露）；
  `_ActantConfig` 透出高级调优字段（暴露前逐字段确认消费点）：
  `store_map_size`/`store_max_dbs`/`store_sync_mode`（"sync"/"group_commit"/"no_sync"
  + `store_flush_interval_ms`）、`prefetch_min`/`prefetch_max`/`worker_cancel_grace_ms`/
  `pending_result_channel_capacity`、`completed_retention_count`/`persist_flush_interval_ms`/
  `state_poll_interval_ms`。死旋钮（`timeout_check_interval_ms` /
  `completion_channel_capacity`，全仓零消费）**不暴露**，留给 T20 删除。

- **量级对照（守则 3）**：0.3.4 全量 diff `+1224 / −2342`（净 −1118，含测试）；
  其中 Actor 精简批 `+83 / −1141`。新增侧（身份/N/G/E）以配置透出与 trait 声明为主。

- **核心 blob 原语（0.3.2 R1，吸收 `spike/0.3.2-iroh-blobs` spike 结论并删除验证代码）**：
  新增 `src/runtime/blobs.rs`，对仓内暴露 store / fetch / hash 三个能力的薄封装，
  不泄漏 iroh-blobs 类型：
  - `NetworkManager::with_blob_store` 在与 gossip / 直连协议同一个 Router 上
    `.accept(iroh_blobs::ALPN, ...)`（spike 已验证多协议共存）；blob 传输走独立
    ALPN 连接，不受直连帧 `max_message_size` 上限约束。
  - `blob_store(data) -> BlobHash`：数据落本地 FsStore（`data_dir/blobs/`，随节点
    持久化，Ref 可能跨重启消费；持久 tag + 默认不回收双重保护）。
  - `blob_fetch(node, hash) -> BlobFetch`：从指定节点流式拉取，逐 blake3 leaf
    （≤16KiB，已 bao 校验）产出，峰值缓冲受通道容量约束，不整块缓冲；取消语义
    按 spike 结论——`Drop` 与显式 `close()` 均立即关闭底层 QUIC 连接，清理从
    idle timeout（30s 级）缩短到即时。
  - 失败路径语义化：provider 无此 hash → `NotFound`，节点不可达 → `Network`，
    未启用 blob 存储 → `Config`，不吞。
  - **依赖**：`iroh-blobs 0.103`（default-features=false，features `fs-store` +
    `rpc` + `hide-proto-docs`）与 `bao-tree 0.16`（fsm 流式路径解构
    `BaoContentItem` 所需，与 iroh-blobs 内部依赖同版本）从 spike optional 转为
    正式依赖，增量 +32 crates（约 +10%，构成见 `plans/SPIKE_0.3.2_BLOBS.md` §四）；
    `spike-blobs` feature 与 `src/runtime/spike_blobs.rs` 删除，验证用例改写为
    正式单测。`rust-version` 声明从 1.75 修正为 1.91（基线 iroh 1.0 实际要求，
    与 spike 结论一致）。
  - **wire**：`common::model::BlobHash`（blake3 32B newtype，hex Display/FromStr）
    与 `common::payload::BlobRef`（32B hash + 来源 NodeId 的 encode/decode，
    roundtrip/篡改/截断测试覆盖）。接入 submit/边传播为 R3/R6，本批不改
    AsyncResult/flow。

### 新增（0.3.2 值引用）

- **核心 blob 原语（R1）**：`BlobStore`（iroh-blobs FsStore 落盘 `data_dir/blobs/`）+
  `BlobFetch`（按 hash 跨节点流式拉取，Drop/显式 close 即时取消）；`BlobHash`/`BlobRef`
  wire 编码；Router accept 链注册 blobs 协议（与 gossip/直连共存）。吸收 0.3.2 spike。
- **ValueStore capability（第 11 个，R2）**：perform 语义 store/fetch，默认 handler 走
  Rust 桥，Python 可覆盖（S3 等）。
- **`Ref` 类型（R3）**：内容寻址值引用句柄；参数 >`REF_INLINE_THRESHOLD`（1MB）自动
  blob 化 + 帧内联哨兵，结果 >1MB 原样落 blob（0 次重序列化）；消费方父进程代取
  （过渡语义，演进见 plans/REF_DESIGN.md）。
- **`AsyncResult` 统一（R4，D5）**：删除 `_result_payload`/`_result_is_obj` 双语义与
  `result()` 三分支；新增 `ref()`。
- **`__await__`/gather 去线程化（R5，D6）**：删除 `_await_slots` 与每次 await 的
  daemon 线程，完成回调直通 event loop。
- **flow 依赖边携带 Ref（R6，D2 地基）**：flow 内大值生产→消费全程字节级搬运。
- **验收**：`tests/python/e2e/test_ref_transfer.py`——100MB 双节点 4.8s、提交方零对象级
  反序列化（`Ref.result` monkeypatch 反向断言）、小结果回归、悬空 Ref 语义化 NotFound。

### 修复（0.3.2 执行中发现）

- **出队路径 4MiB 隐形上限**：`DEQUEUE` 的 Actor 消息回传受 `decode_postcard` 的
  `MAX_DECODE_SIZE`（4MiB）约束——内嵌 ≥4MB 载荷的 `TaskDefinition` 在出队响应解码时
  被静默丢弃（`.ok().flatten()`），任务永久 pending。修复：Worker 直连共享
  `InnerScheduler` 出队（`with_fast_scheduler`），绕过 Actor 消息回传；慢路径解码失败
  由静默改为 `tracing::error!`。
- **大帧派发挂死**：`send_frame` 单次 `write_vectored` 不推进短写——超过 pipe 容量
  （64KB）的派发帧只写出前缀，worker `read_exact` 永久等待。修复：短写显式推进
  （vectored 首写保住小帧快路径 + `write_all` 推进剩余正文）。

### 修复（0.3.3 S6/S7 收尾：取消结算与结果回灌）

- **编排任务取消不结算 DAG，flow 提交方永久挂起**：fail-fast（默认策略）下
  「全部节点被取消」时 `check_workflow_completion` 两条分支都不成立——
  `succeeded_count + skipped_count == total_count` 不满足（`succeeded = 0`），
  而「全部节点终态」判定仅存在于 `Continue` 分支。工作流因此永久停在
  `Running`；S7 的 flow 提交方只轮询 `get_workflow_state` 判定终态（无兜底
  超时），于是无限轮询直到外部超时。修复：封口后**全部节点终态即工作流终态**，
  终态类别优先级 `Failed > Cancelled > Completed`（无结果不得当成功）。
- **取消路径不触发工作流收尾**：`WorkflowExecution::cancel_task` 只改节点状态、
  不重跑终态判定（对比 `skip_task` 有重跑）；`Orchestrator::cancel_task` 只记
  `TaskCancelled` 事件；工作流级 `Orchestrator::cancel` 只做 `put_batch` +
  `notify_terminal`——三者都绕过了 `complete_terminal`，导致持久化、终态事件、
  指标与等待方唤醒全部缺失。修复：统一收口到 `complete_terminal`，新增独立的
  `WorkflowEventPayload::Cancelled` 终态事件（不复用 `Failed`）与
  `actant.workflows.cancelled` 指标；`TaskCancelled` 回放补齐终态落盘
  （对齐 `TaskFailed` 回放）。
- **取消结果被归类为失败，已取消任务被重新入队**：`build_completion_from_dispatch_result`
  没有 `ActantError::Cancelled` 分支——dispatcher 在取消宽限期耗尽后返回的
  `Err(ActantError::Cancelled)` 落入 `Ok(Err(_)) → Failed`，于是取消进入 S6
  重试裁决，**已取消的任务被重新入队执行**（取消被"复活"），且 fail-fast 下把
  工作流误判为 `Failed`。修复：新增 `Cancelled → TaskCompletion::Cancelled` 映射。
- **本地取消被静默丢弃**：`Worker::cancel_task` 只置 `cancel_flags` 中的运行中
  flag；任务尚在调度器队列（主循环未预注册 flag）时取消请求凭空消失，任务照常
  执行到完成（实测两个任务各跑满 10s）。远端 `CancelBroadcast` 路径一直是
  「置 flag + 登记 `cancelled_tasks`」双写，本地路径缺了登记那一半。修复：本地
  路径补齐登记，并关闭「已过 `cancelled_tasks` 检查 → `cancel_flag` 尚未注册」
  的派发窗口（注册后立即采纳窗口期落下的取消）。
- **e2e 用例调用已随 S7 删除的 API**：`test_multi_node_reliability.py::test_node_restart_resume`
  仍调用 `submit_dag` / `complete_workflow`（手工回灌模型），必然
  `AttributeError`。该模型已不可用——`add_workflow_node` 对 ready 节点会真实
  派发，而绕过 `Task.submit` 手工构造 DAG 没有句柄，结果永不回灌 orchestrator。
  已按新语义重写：节点由 `@flow` 经 `add_workflow_node` 真实派发、结果经
  `report_task_result` 单入口回灌，用例改为验证「工作流状态跨重启全量恢复 +
  已完成任务不重跑」。

### 新增（0.3.3 S3：flow 续跑驱动接线）

- **`actant.resume_flows(*, runtime=None, timeout_ms=0)`**：把 `_replay_flow` 从
  "有实现无调用者"变成可用的恢复路径。扫描 store 中**未终态**工作流
  （`list_workflows()` 即该集合，`active_workflow_ids()` 已过滤终态），按
  `workflow_id` 前缀恢复 flow 名 → 查恢复登记 → 重放：**已完成节点不重跑**、
  缺口节点补提交、悬挂等待点"已唤醒的直接返回"。无登记的工作流记录 warning
  后跳过（不静默——否则"重启后什么都没发生"是最难排查的失败形态）。
- **`actant.register_flow_recovery(name, func, *, args, kwargs)`**：补齐重放所需的
  函数体与调用参数。`@flow` 已在导入时登记"flow 名 → 原函数"（`workflow_id` 只
  编码名字，函数体必须由导入提供）；**参数无法反推**，故由调用方显式给出——
  框架不持久化任意 Python 对象（那会退化成函数体快照）。传 `@flow` 装饰后的
  对象会被自动解包到原函数。
- **显式驱动，不做启动自动恢复**：自动恢复会在 Runtime 启动线程里执行用户
  Python 代码（可能 ImportError），把启动路径变成不确定路径；归 S4/S5。

### 修复（0.3.3 接线补齐：等待点关停不释放）

- **`Runtime.stop()` 不唤醒 park 中的等待者，无限等待会挂住进程退出**：
  `wait_wait_point(timeout_ms=0)` 是"等到条件满足为止"的语义，等待者可能永不
  返回；关停时若不释放，park 线程（可能是主线程）无法退出。修复：
  `WorkflowActor::on_stop` 释放全部等待点等待者（丢弃 oneshot sender → receiver
  立即收到 `Err`，park 方返回"未唤醒"）。新增 Rust 测试
  `release_all_wait_point_waiters_unblocks_parked_waiters`。

- **重放误用 `@flow` 包装器会静默退化为"从头重跑"**：`register_flow_recovery`
  若收到装饰后的对象，重放会重新进入 `@flow` 包装器并**生成新的 workflow_id**，
  于是"重放"变成"新建工作流"，已完成节点全部重跑（危险且无任何报错）。
  修复：包装器打 `__actant_flow_name__` 标记，登记时自动解包到 `__wrapped__`
  原函数；无法解包时显式抛 `ValueError`。

### 新增（0.3.3 S1 收口 + S2：flow 侧等待点原语）

- **`actant.sleep_until(deadline_ms)` / `actant.wait_signal(name)`**：flow 函数体内的
  持久化挂起原语。注册等待点到 orchestrator（随工作流快照落盘），park 当前线程；
  条件满足（timer 到期 / 外部 signal）时追加唤醒事件进入同一工作流历史。
  `signal` 为**闭锁语义**（同一名字在工作流内只注册一次，重复 await 立即返回），
  这是重放体"已收到 → 直接返回"幂等的前提。
- **`wait_key` 派生**：`signal` 直接用信号名（外部递交只需知道名字：
  `Runtime.signal_wait_point(workflow_id, name)`）；`timer` 用 `timer-{等待序号}`，
  等待序号是**独立于节点提交序号**的计数器——共用会让增删一个等待点平移全部
  节点标识，放大重放指纹冲突面。`deadline_ms` 是 wall-clock 值，**不参与**
  提交序列指纹（跨重放稳定的是"第 n 次等待"，不是具体时刻）。
- **park 在 actor 之外实现**：actor 消息处理是单线程顺序执行的，在其中阻塞会让
  整个 WorkflowActor（全部工作流）停摆。故新增 `Runtime::orchestrator_handle()`
  （builder 注入的编排器句柄，与 actor 共享同一 `Arc<OrchestratorState>`），
  由 PyO3 侧在 actor 之外阻塞并释放 GIL。副作用：`register_wait_point_waiter`
  从"有实现无消费者"变为完整闭链。
- **异常契约**：未显式指定上界而 flow 自身 deadline 到期 → 抛
  `ActantTimeoutError`（未请求的提前返回会破坏确定性契约）；显式给定
  `timeout_ms` 的有界等待超时 → 返回 `None`。

### 修复（0.3.3 接线补齐：定时等待点永不唤醒）

- **`Timer` 类等待点永不自动到期，挂起的 flow 线程永久阻塞**：S1 的等待点原语
  （`register_wait_point` / `signal_wait_point` / `poll_expired_timers` /
  `register_wait_point_waiter`）在 Rust 侧实现完整并有单元测试覆盖，但
  **`poll_expired_timers` 在生产路径上没有任何调用者**——全仓唯一调用方是测试。
  `Orchestrator::start_timeout_watcher`（工作流级硬超时监控）虽已是现成的周期
  轮询循环，却只做超时工作流处理，从未扫描等待点。后果：`Timer` 条件注册后
  `deadline_ms` 到期无人推进，等待者 oneshot 永不触发。
  修复：把等待点扫描挂入该 watcher 的同一轮询周期，复用已测试的
  `poll_expired_timers` 实现（避免在闭包内重写判定逻辑造成行为漂移）。
  **等待点唤醒延迟上界 = `workflow.state_poll_interval_ms`（默认 500ms）**。
  新增接线回归测试 `timeout_watcher_fires_expired_timer_wait_point_without_explicit_poll`
  ——该测试刻意**不直接调用** `poll_expired_timers`，只依赖 watcher 驱动，
  失败即代表接线再次断裂（而非实现回归）。

### 修复（0.3.3 P0-3：执行节点失联让在途任务句柄永久挂起）

- **死亡执行器上的在途任务无人处置**：故障转移的失联扫描只处理"**死亡编排器**
  的孤儿 workflow"（依据 peer 心跳携带的 `active_workflows`），而**死亡执行器**
  不编排任何 workflow → 该集合为空 → `detect_and_claim_failed_nodes` 的
  `if !is_failed || info.active_workflows.is_empty() { continue; }` 直接跳过。
  后果：`task.submit_to(node, ...)` 这类**在本节点提交、转发到远端执行**的任务，
  若远端节点被强杀，结果字节永不回来，`AsyncResult` 永久挂起（提交方只能靠
  自身超时兜底）。修复为两腿：
  1. **在途转发登记**：Worker 转发成功后把 `task_id → (workflow_id, task_name,
     target_node, target_endpoint_addr, forwarded_at)` 记入
     `FailoverManager::outbound`（内存表，不落盘——丢失只退化为"无恢复"，
     不会错误恢复）；远端结果回来（`NetworkRouter` 收到 `TaskResult`）即清除。
  2. **失联扫描新增处置腿**：peer 被判失联时，把登记表中目标为该节点的在途任务
     终结为 `TaskCompletion::Failed`（error 带 `[actant:worker]` 前缀，Python 侧
     还原为 `WorkerError`），经 **event_bus 发布**——与远端结果回灌走同一条路，
     提交方句柄因此终止，编排任务则由 Python 事件泵照常回灌 orchestrator。
- **心跳盲区：节点在首个可观测心跳前失联**：上述第 2 腿依赖 `peers` 视图，而
  心跳每 2s 一次——节点若在首个心跳送达前就被杀（`peers` 里完全没有它、
  也没有可用于超时判定的时间基线），失联扫描无从触发。这类目标只有**主动探测**
  能判定，故新增直连存活探测：
  - wire 协议新增 `DirectRequest::Ping` / `DirectResponse::Pong`（无副作用，
    用于区分"对端仍在线"与"对端已失联"）；
  - 每轮失联扫描对**不在新鲜心跳窗口内**的在途目标发一次 `Ping`
    （单次上界 2s，避免拖住扫描；正常拓扑下通常零探测）；
  - **探测成功即保留任务**（不误杀健康对端），仅探测失败/超时才终结。
    新增依赖：无。
- **验收**：`tests/python/e2e/test_multi_node_reliability.py::test_node_kill_inflight_task_terminal_state`
  去 `xfail` 转正——**5.7s** 通过（原为 30s 轮询窗口耗尽后 xfail）。
  Rust 侧新增 6 个回归测试（第 2 腿的登记/清除/终结、心跳盲区探测的两种裁决、
  Ping/Pong 编解码往返）。

### 破坏性变更（0.3.3 S5：`@flow(timeout_ms=)` 语义收敛为工作流 deadline）

- **flow 超时不再"立即返回"，也不再由 Python 侧计时**。`@flow(timeout_ms=N)` 的
  唯一决策者改为 orchestrator 的超时 watcher（工作流级 deadline，此前已存在）：
  到期把工作流标为 `Failed`（error=`workflow timeout exceeded`）并取消全部运行中
  任务。调用方可见的差异：

  | | 0.3.2 及更早 | 0.3.3 起 |
  |---|---|---|
  | 抛错时刻 | 超时瞬间立即返回 | `deadline + 轮询周期`（默认 `state_poll_interval_ms`=500ms）量级 |
  | 事实源 | Python 子线程 + `cancel_event` | 工作流终态（`Failed` + 上述 error） |
  | 孤儿工作 | 子线程继续跑（"软超时"） | 运行中任务被真正取消（worker 在协作检查点退出） |
  | 纯 CPU 段 | 调用方已返回，函数体仍在后台跑 | **不可中断**，函数体跑完后调用方仍抛 `ActantTimeoutError` |

- **删除清单**（守则 3）：`_run_body_with_timeout` 的子线程/`done_event`/
  `cancel_event` 路径、`_FlowState.cancel_event` 与 `is_cancelled()`、
  `is_flow_cancelled()`、`Task.submit` 的 flow 取消守卫、`_FlowContext(cancel_event=)`、
  `Runtime.register_flow_thread` / `unregister_flow_thread` / `_flow_threads` 及
  `stop()` 的 flow 线程 join、`_wait_terminal_and_emit` 的 `timeout_ms` 上界。
  依赖这些内部名字的代码需改用公开语义（工作流终态 / `ActantTimeoutError`）。
- **无 `Task.submit` 的 flow 不产生编排外壳 ⇒ deadline 无宿主、不生效**：空 flow
  沿用既有"惰性创建工作流"语义。这在 0.3.2 由 Python 软超时兜住，现在报不出来
  ——task-free flow 本就无编排可超时，属**文档化约束**而非回归。

### 新增（0.3.3 S5：flow 超时强还原）

- **超时取消的本地腿（`Transport::inject_local_event`）**：工作流 deadline 到期时，
  超时 watcher 除 gossip 广播 `CancelBroadcast`（覆盖远端执行的任务）外，还把
  **同一份 postcard 字节**自投递回本节点事件通道，经
  `NetworkEventRouter::handle_message` 既有的 `TopicRoute::Cancel` 分支处理
  （置 `cancel_flag` + 登记 `cancelled_tasks` + 发布 `BusEvent::TaskCancelled`）。
  原因：**gossip 广播只投递给邻居、不发回发送者**。修复前"广播取消"只覆盖远端，
  本节点自己执行的运行中任务不会被取消——工作流已 `Failed`，而阻塞在任务等待上
  的 flow 体**永久挂起**（实测：本地 `AsyncResult` 在 10s 观察窗内始终 `running`）。
  自投递复用远端同一段路由代码，避免本地/远端行为漂移。
- **`@flow` 调用方语义归一**：函数体被强还原唤醒时，任务级终态是 `Cancelled`
  （worker 确实被取消了），对外归一为 `ActantTimeoutError`；函数体若已正常返回，
  则终态为 deadline 失败时**同样抛错**（返回值不得被当作成功）。
- **接线证据**（不接受"单元测试全绿"作为完成标准）：Rust
  `workflow_timeout_self_delivers_cancel_locally` 断言广播与自投递成对出现且字节
  一致；Python 集成 `test_flow_deadline_restores_strongly` 断言端到端——在
  `deadline + 轮询周期` 内抛错、工作流终态为 `Failed` 且 error 为
  `workflow timeout exceeded`（对比任务自身 30s 的睡眠）。
- **唤醒延迟基线**：实测 `deadline=100/300ms → 0.47s`、`deadline=800ms → 0.98s`，
  即**上界 = deadline 之后的第一个轮询 tick**（对应
  `state_poll_interval_ms`，默认 500ms）。与等待点 timer 同一周期。

### 修复（0.3.3 S5 附带：raw-postcard 话题的恒定误报）

- **每条合法取消广播都打一条"dropping message"的 WARN**：
  `NetworkEventRouter::handle_message` 对**所有**入站消息无条件尝试
  `WireEnvelope::decode`，但 `Cancel` / `CapabilityGossip` 话题的载荷是裸
  postcard——解码必然失败并 `tracing::warn!("dropping message: failed to
  deserialize WireEnvelope")`，而消息随后被对应分支正常处理。修复：按
  `Topic::classify()` 的结果跳过这两个话题的解包。改前该告警只出现在多节点
  拓扑，S5 的本地自投递让它在单机路径上也出现，会让人误判"消息被丢弃"。

### 修复（0.3.3 S4：工作流终态到不了 park 中的 flow 函数体）

- **abort / fail / deadline 只改工作流状态，不解阻塞等待点 park**：等待点 park
  （`sleep_until` / `wait_signal` / `suspend`）是独立于 `AsyncResult` 的**第二条
  阻塞原语**，此前唯一的释放点是运行时关停（`release_all_wait_point_waiters`）。
  `complete_terminal` 进入终态时只 fire 终态 oneshot（那是给 `AsyncResult` 等待
  用的），park 的等待者不被释放 ⇒ **工作流已是 `Cancelled`/`Failed`，flow 函数体
  仍永久挂起**。实测：abort 一个 park 在 `wait_signal` 上的 flow，8 秒观察窗内
  函数体纹丝不动（`seen=['parking']`）。修复：`notify_terminal` 增加
  `OrchestratorState::release_wait_waiters`——**只移除等待者，不动 `waitpoints`
  表**（等待点条目是历史与快照的一部分，仍需供重放与查询）；三个终态入口
  （`fail_task` / `complete_terminal` / `mark_workflow_failed`）共用该方法，
  故单点释放即可覆盖 cancel / fail / deadline 三条路径。
- **park 被释放后被误当成"信号返回了空 payload"**：终态释放唤醒 park 时
  `wait_wait_point` 返回 `None`，与"上界超时"**同形**。不区分 ⇒ abort 被伪装成
  一个空 payload，函数体在**已死的工作流上继续跑到正常返回**（其后续 `submit`
  才会被权威层以 `InvalidState` 拒绝，且报错与真实原因无关）。修复：park 收敛到
  **单一入口** `_park_payload`，返回 `None` 时调 `_raise_if_workflow_terminal`
  查工作流终态并抛对应异常——`Failed` + `workflow timeout exceeded` →
  `ActantTimeoutError`；`Failed` → `WorkflowFailedError`；`Cancelled` →
  `WorkflowCancelledError`；工作流已不存在 → `WorkflowCancelledError`。事实源是
  **工作流终态**，Python 侧不复算任何计时器（与 S5 的 `_deadline_failure` 同一
  模式）。`wait_signal` 此前直接调 `wait_wait_point`、绕过该判定，本次一并收敛
  ——单一入口就是为了让这条判定不可能再次漂移。

### 新增（0.3.3 S4：suspend / resume / abort）

- `actant.flow.suspend()`：注册一个**挂起条件**等待点（`kind="suspend"`，键取
  `suspend-{等待序号}`，故同一 flow 内多次挂起各自独立）后 park，由
  `Runtime.resume_suspended(workflow_id)` 唤醒。上界语义与 `sleep_until` 一致
  （`timeout_ms=0` = 取 flow 自身 deadline，仍为 0 = 无限等待）。
- `Runtime.resume_suspended(workflow_id) -> int`：唤醒该工作流所有 `Waiting` 的
  `Suspend` 等待点并返回被唤醒数量。**只唤醒挂起条件，不冒充业务信号**——
  `Signal` 条件的等待点保持 `Waiting`，且 resume 不伪造 payload。幂等：已唤醒的
  等待点重复 resume 返回 `0`；未知工作流返回 `0`（不是错误）。按 `workflow_id`
  而非按键唤醒，故调用方无需知道 flow 内部给挂起点分配了什么键。
- **abort 复用既有取消通道**：`Runtime.cancel_workflow(workflow_id)`。本项
  **不新增** `abort_workflow`——它本就是 cancel，再造一个入口属于净扩张。
  命名区分是刻意的：单数 `resume_suspended(wf_id)` = 恢复**挂起中**的工作流；
  复数 `resume_flows()` = 重放**未终态**工作流（S3 续跑驱动），二者语义不同、
  入口不同（前者按 id，后者按扫描）。
- **新增协议变体 `WaitCondition::Suspend`**（追加在枚举末尾：postcard 的变体判别
  按声明顺序，追加不改变既有值的编码）。它与 `Signal` 的区别是**语义来源**
  （signal 等一个具名业务事件，suspend 等操作员的恢复指令），二者共用
  `SignalReceived` 作为唤醒事件（其载荷语义本就是"等待点被外部满足"），由
  `condition` 字段区分，故**不新增事件变体**。
- **接线证据**：`tests/python/integration/test_flow_suspend.py`（abort 唤醒 park
  中的 flow 并抛 `WorkflowCancelledError`、suspend→resume 往返、多次
  suspend/resume、deadline 到期唤醒 park 中的函数体）；Rust 侧
  `terminal_state_release_unparks_wait_point_waiter` /
  `resume_suspended_wakes_only_suspend_condition` /
  `resume_suspended_unknown_workflow_returns_zero`。
- **删除清单：本项删除量 = 0，如实记录（守则 3）**。S4 是"一处行为修复 + 一组新
  原语"，没有删除。0.3.3 的净删除量由 S5（软超时 / 孤儿线程）与 S7（eager /
  回灌路径）承担；本项使新增侧变大，S7 的删除量须相应兑现对称性。
- **不做（记录在案，偏离设计文档 §四之二·3 的"自动恢复归 S4"）**：启动自动恢复。
  `resume_flows()` 就是这个原语，用户在 `start()` 后一行即可调用；唯一自然的形态
  是 `Runtime(...)` 构参或 `start()` 开关，两者都是**净 API 扩张**，买到的只是
  "省略一行调用"。且原文拒绝它的理由（启动路径执行用户 Python 代码会变成不确定
  路径）不因搬到 S4 而消失——真做还需逐工作流异常隔离（当前 `resume_flows` 只捕获
  `ActantTimeoutError`，`ImportError` 会穿透启动路径）。

### 新增（0.3.3 P1-4：信号缓冲——等待点注册前抵达的信号不再被丢弃）

- **洞**：`signal_wait_point` 在等待点尚未注册时**静默丢弃**信号（返回 `None`、
  不写历史、不补发），递交方只能按返回值重试。实测探针（不重试、只递交一次）：
  flow 先 `submit` 建工作流，进 1.5s 纯睡眠段（等待点未注册），再
  `wait_signal("go", timeout_ms=3000)`；主线程在 0.3s 递交一次 →
  `delivered=None`，flow **白等满 3s 上界后仍拿到 `None`**，总耗时 **4.62s**
  （缓冲生效时应为 ~1.5s）。既有测试靠 `_signal_until_registered`
  重试轮询掩盖该洞——那个辅助函数本身就是"信号不缓冲"的证据，**已删除**，
  改为单次递交。修复后同一探针：**1.61s** 且正常返回。
- **缓冲**：无匹配等待点时追加**独立** `SignalReceived` 历史条目（满足
  `DURABLE_FLOW_DESIGN` §三/§四"查历史——已收到 → 直接返回 payload"的前置要求：
  `SignalReceived` 现在可以作为早于任何等待点的历史事件存在）+ 写入缓冲表；
  `register_wait_point` 注册时命中缓冲 → 等待点**直接生成为已唤醒态**，park 方
  随即返回。缓冲与等待点快照同批落盘（`orch:sigbuf:{id}` 与 `orch:wait:{id}`），
  故**跨重启存活**。
  **注**：落盘的必要性不因"事件重放已存在"而消失——重放的 `SignalReceived`
  分支只在等待点**已存在**时改写它（`persistence.rs:497`），而缓冲场景里
  `SignalReceived` **早于** `WaitPointRegistered`，重放会把它丢掉。故仍需要
  `orch:sigbuf:` 快照兜底（已就此项另立待办：让重放也缓冲"先到的信号"）。
- **缓冲是闩锁**：同一 `wait_key` 已有缓冲时重复递交**不再追加历史、不覆盖**，
  直接返回 `None`。这让"按返回值重试"变安全（重试是 no-op）。当前 payload 恒为
  空；S2 携带数据后须重审"首到者胜出 vs 后到者覆盖"（已记入设计文档）。
- **只有 `Signal` 条件的等待点消费缓冲**：`Suspend`（操作员指令）/`Timer`（时钟
  事件）不得被业务信号顶替。
- **未消费的信号不参与工作流终态判定**：信号是带外输入，不是工作流依赖；若计入，
  一个拼错 key 的悬挂信号就能永久阻塞一个全部节点已终态的工作流。
- **接线证据**：`tests/python/integration/test_flow_waitpoints.py`
  （`test_signal_arrives_before_wait_point_is_registered` 单次递交即唤醒、
  `test_signal_retry_after_consumption_is_safe`、`test_signal_to_unknown_workflow_raises`）；
  Rust 侧 `signal_before_registration_is_buffered_and_consumed` /
  `repeated_signal_while_buffered_is_a_no_op` /
  `buffered_signal_is_not_consumed_by_suspend_condition` /
  `signal_to_unknown_workflow_is_not_found`。
- **删除清单（守则 3）：产品代码删除量 = 0**。仅删除测试辅助
  `_signal_until_registered`（对缺陷的 workaround）。

### 破坏性变更（0.3.3 P1-4：`signal_wait_point` 的递交错误不再静默）

| 情形 | 0.3.2 | 0.3.3 |
|---|---|---|
| 工作流不存在（id 写错） | `None` | **抛 `NotFoundError`** |
| 工作流已终态 | `None` | `None`（**不报错**，见下） |
| 等待点不存在 | `None`（丢弃） | `None`（**入缓冲**） |

递错 id 原先与"递交成功"同形于 `None`——这是 P1-1/P1-2 反复出现的**同一类病**
（`None` 承载了太多含义）。

**实测改判：终态不报错。** 第一版设计为"已终态 ⇒ 抛 `InvalidStateError`"，被集成
测试推翻——时序是「首次递交入缓冲 → flow 注册命中 → 函数体跑完 → 工作流终态 →
递交方重试 → 报错」，**信号其实已经送达并被消费**。故终态递交仍返回 `None`
（入缓冲、随工作流移除而清除），保证重试安全。

**误伤窗口（已于 P1-5 修复，保留根因说明）**：`@flow` 的 wrapper 在**函数体开始
前**就广播 `submitted` / `started`，而工作流是**惰性创建**的，故"从生命周期事件
拿到 id"与"该 id 可被递交"之间存在真实窗口（实测 1/5 复现
`not found: workflow ... not found`）。P1-4 当时未改 emit 顺序
（`test_flow_orchestration.py` 已锁死 `submitted → started → completed` 的
事件序列），P1-5 已把 emit 移到真正建槽之后，**该窗口消失**——详见下文
「修复（0.3.3 P1-5）」。

### 修复（0.3.3 P1-5：生命周期事件不再抢跑于工作流创建）

- **`@flow` 曾广播一个此刻还不存在的 `workflow_id`**：wrapper 在**函数体开始前**
  就 emit `submitted` / `started`，而工作流是**惰性创建**的（要等函数体第一次
  `Task.submit` 或进入等待点才建槽）。于是"从生命周期事件拿到 id"与"该 id 可被
  递交"之间存在真实窗口，外部控制器按**我们广播的 id** 立即调用
  `Runtime.signal_wait_point` / `resume_suspended` 会撞 `NotFoundError`
  （实测 1/5 复现：`not found: workflow wf-latch-c3029338 not found`）。
  这不只是测试脆弱——**发出去的 id 在那一刻不可用**，等于把内部时序泄露给调用方。
- **修复**：事件改在真正建槽**之后**广播，并抽出**唯一**创建入口
  `_ensure_workflow_created`（`Task.submit` 的编排路径与等待点路径共用——顺带消除
  两处重复的创建块）。事件与状态因此同源：**看到 `submitted` ⇒ 工作流已存在**。
  探针 8/8 无 `NotFound`（修复前 1/5 抛错）；`tests/python/integration/
  test_flow_waitpoints.py` 连跑 8 轮 × 10 用例全绿。
- **未采用"wrapper 预先建槽"**：那会推翻 P1-1 已定并有用例锁定的约束——空 flow
  不产生工作流，故 `timeout_ms` 无宿主、deadline 不生效
  （`test_task_free_flow_deadline_has_no_host`）。预先建槽是**语义变更**而非时序
  修正，故只动事件发射点。
- **`_replay_flow` 不重复广播**：重放体走同一条惰性创建路径，靠 `_FlowState` 的
  `announce` 标志关闭广播，保持其既有约定（原始执行已广播）。

### 破坏性变更（0.3.3 P1-5：无工作流创建的 flow 不再广播 `submitted`/`started`）

- 若函数体**从未**创建工作流（无 `Task.submit`、无等待点、无 `suspend`），
  则**不再产生 `submitted` / `started`**——只保留终态事件（`completed` /
  `failed`）。
- 理由：`submitted`/`started` 原本是对一个**不存在的 id** 广播的状态，本就是失真
  信息。新语义是"`submitted` 意味着工作流确实已被提交"。
- **不引入替代事件**：flow 级别的"开始/结束"不是本项目的编排事实源，工作流才是；
  需要感知 flow 生命周期的调用方应在 flow 体内自行 emit——观测面不该由框架替用户
  编造事实。
- 迁移：依赖这两个事件做"flow 已启动"判断的调用方，改为判断 `submitted` 是否出现
  （不出现即该 flow 未产生编排数据），或在函数体内自行广播。

### 新增（0.3.3 P1-3：`Runtime.get_dag` 暴露 DAG 结构，E3）

- **`Runtime.get_dag(workflow_id) -> dict | None`**：查询工作流的 DAG **结构**。
  与 `get_workflow_state`（**执行**状态：每任务 `state`/`result`/`error`/
  `retry_count`/`attempt`）分工——本方法是**结构**：

  - `nodes[]`：`task_id` / `name` / `deps`（前驱 task_id 列表）/ `timeout_ms` /
    `priority` / `metadata` / `retry_policy`
  - `edges[]`：`from` / `to` / `condition`
  - `default_retry_policy`：DAG 级默认重试策略。**必须暴露**——节点自身
    `retry_policy` 为 `None` 时生效的是它（对应 Rust `Dag::effective_retry_policy`），
    缺了它就无法在 Python 侧还原"生效策略"。
  - `workflow_id` / `failure_strategy`

  **不含任务 payload**：它是签名的 cloudpickle 字节，对 Rust 不透明且可能很大；
  需要任务结果用 `get_workflow_state()["tasks"][...]["result"]`。
  未知工作流返回 `None`（与 `get_workflow_state` 同款契约）。
- **接线而非实现**：Rust 侧 `Orchestrator::get_dag`（`queries.rs:20`）早已存在，
  但只有 1 个测试调用者、Python 未接线。本次新增 `dag_snapshot`（不含 payload
  的可序列化形态）+ `workflow_methods::GET_DAG` 分支 + PyO3 方法 + Python 包装。
- **接线证据**：`test_get_dag_exposes_structure_and_retry_policy`（真实调用
  `Runtime.get_dag`，断言依赖边、`@task(retries=...)` 落成的节点策略、无 payload、
  与 `get_workflow_state` 不重复）、`test_get_dag_returns_none_for_unknown_workflow`；
  Rust 侧 `dag_snapshot_exposes_deps_and_retry_policies`。

### API 暴露面审计（0.3.3 P1-3：E1–E4 查重结果，**四项并非全暴露**）

按 ASSESSMENT §8.5「避免同一数据两个出口」逐一查重的结果：

| 项 | 裁决 | 依据 |
|---|---|---|
| E1 `cancel_workflow` | **已暴露**（无需动作） | 早已有 Python / PyO3 出口 |
| E2 `get_workflow_history` | **推迟，与 P2-1 同批** | 它是 S0 的审计出口。**注**：初稿写的"`recover` 不重放事件、事件日志只写不读"是**错误论断**（一次 BSD grep `\|` 静默失败导致的误查），重放早已实现。推迟的真实理由是：还没有配套的**读取 API**，且历史**无裁剪/留存策略**（只增不删，`fmt_version` 未用于迁移）——先补出口与策略再公开 |
| E3 `get_dag` | **暴露** | 无任何 Python 出口，且数据不在 `get_workflow_state` 里（后者只有执行状态） |
| E4 `get_retry_info` | **不暴露** | 其 `retry_count` 已在 `get_workflow_state` 的 `tasks[*]` 中 → 重复出口；其"重试策略"那一半由 `get_dag` 覆盖 |

**E4 不暴露的详细理由**：Rust `Orchestrator::get_retry_info`（`queries.rs:189`）返回
`(retry_count, RetryPolicy, delay_ms)`。`retry_count` 与 `get_workflow_state` 重复；
`RetryPolicy` 确实没有出口，但节点策略 + `default_retry_policy` 已由 `get_dag`
给出；`delay_ms` 是 `compute_retry_delay` 的派生值，调用方可按策略自算。
故**用一个 API 覆盖两个需求**，不再开第二个出口。该函数**保留**为 Rust 内部原语
（是 S6 的潜在调用点），不因"未暴露"而删除。

### 修复（0.3.3 P2-1：重放路径上"先到的信号"不再被丢弃）

- **重放（S0）早已落地，本项订正先前一处错误论断并补残留。** 此前记录称
  "`recover` 不重放事件、事件日志只写不读"——**该论断错误**：实现在
  `persistence.rs:292`（`replay_events_after_watermarks`）与 `:380`
  （`apply_replayed_event`），覆盖 `Started` / `NodeAdded` / `TaskCompleted` /
  `TaskFailed` / `TaskCancelled` / 等待点三类事件，终态同步落盘。
  探针实测：快照里 t2 仍是 `Pending`、事件历史已含其 `TaskCompleted`，
  `recover` 后 t2 = `Completed(r2)`。
  该错误论断源于一次 `grep "read_after\|apply_event"`（BSD grep 不支持 `\|`，
  **静默返回空**），曾扩散到设计文档 / CHANGELOG / `flow.py` 用户可见 warning /
  `_runtime.py` / `waitpoint.rs` / 记忆 / 技能共 **8 处**，本轮全部订正。
- **真正的残留（已修）**：重放的 `SignalReceived` 分支原先只在等待点**已存在**时
  改写它，而"信号先于等待点抵达"这一场景里 `SignalReceived` **早于**
  `WaitPointRegistered` —— 重放会把信号丢掉，等于 P1-4 的缓冲**跨重启失效**。
  修复：重放遇到无等待点的 `SignalReceived` 时**入缓冲**，并在后续
  `WaitPointRegistered` 重放时消费（与 `register_wait_point` 同款）。
  `orch:sigbuf:` 快照因此从"唯一兜底"降为"纵深防御"。
- **接线证据**：`replay_buffers_signal_that_precedes_registration`
  （崩溃前 Signaled → 重放后仍 Signaled，而不是被丢弃）。

### 新增（0.3.3 P2-1：`Runtime.get_workflow_history` 事件历史读取出口，E2）

- **`Runtime.get_workflow_history(workflow_id, *, after=None) -> list[dict]`**：
  事件历史的对外读取出口。历史是**事实源**（`recover` = 快照 + 其后事件重放，
  等待点与信号也进同一历史），此前只能被 `recover` 内部消费。
- **返回形态的取舍**：每项 `{sequence, timestamp_ms, kind, task_id, error, payload}`。
  `kind` / `task_id` / `error` 由 Rust 侧解出供**筛选**（`kind` 取值如
  `"Submitted"` / `"NodeAdded"` / `"TaskCompleted"` / `"WaitPointRegistered"` …）；
  `payload` 是原始编码字节，**不透明**——Python 不解释其布局，故 Rust 枚举的字段
  增减不会变成跨语言契约。
- `after` 为 `(sequence, timestamp_ms)` 游标（取上一项同名字段），`None` 从头读取。
  无 event_log 或工作流不存在时返回**空列表**（历史是可选观测面，缺失不构成错误）。
- **接线证据**：`test_get_workflow_history_exposes_event_stream`（断言事件序列、
  `task_id` 筛选字段、`payload` 为 bytes、序列可作游标）、
  `test_get_workflow_history_returns_empty_for_unknown_workflow`；Rust 侧
  `workflow_history_returns_events_and_empty_without_log`。

### 新增（0.3.3 P2-1：事件历史留存策略）

- **问题**：`LmdbEventLog` **只增不删**，长跑工作流的事件历史会无限增长。
- **关键约束**：**不能按"保留最近 N 条"裁剪**——那会删掉水位**之后**的事件，而
  `replay_events_after_watermarks` 读的正是那一段，裁剪即破坏恢复。
- **裁决**：裁剪上界 = **持久化水位**。只删除 `id <= 水位` 的条目中最旧的若干条，
  使该区间至多保留 `event_log_max_events_per_workflow` 条；**水位之后的事件永不
  裁剪**。这是 S0 语义的自然推论：进了快照的事件，历史只是审计用。
- 新增 `EventLog::trim_absorbed(topic, up_to, keep)`（`LmdbEventLog` 与
  `MemoryEventLog` 均实现），在 **flush 成功、水位已落盘之后**调用；两条 flush
  路径（`start_persist_flush` / `flush_dirty`）共用同一辅助，避免语义漂移。
- **默认 0 = 不裁剪**（保守默认：不静默改变既有留存行为）。
- **接线证据**：`history_trim_only_touches_events_absorbed_by_watermark`
  ——断言裁剪后"水位之后的事件一条不少"，且总量不增加。

### 内部（0.3.3 P2-2：文档双源同步）

- `ROADMAP.md` 与 `PLAN.md` 的 0.3.3 状态列按**实查**刷新，消除「同一事实两个源」：
  **S1 / S3 / S6 / S7 的代码部分实为已落地**（此前状态列写"待做"）；
  S2 标注为"原语与出口已落地、capability 形态未做"；收尾项拆分出已完成与未做。
- 仍遗留并已明确记录：**X1 `fmt_version` 未用于版本迁移**（事件格式变更时靠
  "解码失败跳过"兜底，会静默丢事件）；**S7 的"删除量 > 新增量"对称性未结**。

### 删除（0.3.3 代码审查：守则 3 的删除清单）

- **终态 oneshot 等待者整条链**：`TerminalWaiterRegistry::register` 与
  `OrchestratorState::register_terminal_waiter`（`types.rs`）零生产调用者——
  Python 侧的终态等待已被 `_wait_terminal_and_emit` 轮询取代，`fire_terminal_oneshot`
  因此恒为 no-op。连带删除 2 个只测该链的用例。
  （保留 `fire` 与其调用点是为了不牵动 `notify_terminal` 的结构；若要彻底清除，
  应连同 `TerminalWaiterRegistry` 与 `fire_terminal_oneshot` 一起删。）
- `is_workflow_failed`（`dag.rs`）：语义已被 `Terminal::is_terminal()` 取代。
- `state_handle`（`orchestrator/state.rs`）：`state` 字段本就是 `Arc`。
- `get_expired_workflow_ids`（`queries.rs`）：纯转发，watcher 直调 state 层。
- `ok_response` / `opt_string_response` / `PyCancelToken::flag`（`py/types.rs`）：
  无调用者的 PyO3 辅助（`#[pymethods]` 里另有 `is_cancelled`）。

### 文档修正（0.3.3 代码审查：三处描述了未接线/不存在的路径）

- `actant/_runtime.py`：`start()` 的文档原称"提交的任务由 Rust `ExecuteHandler` →
  `ProcessTaskDispatcher` 分发"——**实际路径是 `submit_task` → channel →
  `scheduler.enqueue()` → dispatcher，不经过 `Execute` capability**
  （`register_execute_handler` 是给 Rust 嵌入场景按需调用的公开 API，默认不注册）。
- `src/py/handler.rs` 模块头：原称 Python handler 经 `Runtime::chain` 挂到 Rust
  capability 链末尾、"消除双分发"——**该桥接当前未接线**（`Layer.chain()` 只登记在
  Python 侧），双分发仍然存在。同理修正 `_runtime.py` `layer()` 的
  "start 前 chain 即可覆盖 Rust 内部 dispatch"指引——该路径 start 前后都不生效。
- `src/common/payload.rs` `pack_single`：原注释称"由 Python 侧调用构建
  default_payload"，该调用方已随 `actant/_serialization.py` 移除（仓库里只剩过期
  .pyc）。函数**保留**（`pub` 公开 API + 属性测试在用），仅修正注释。

### 文档整理（0.3.3：注释与用户可见文档的一致性收尾）

- **`actant.pyi`**：`signal_wait_point` 的存根原写"未知工作流 / **终态工作流抛错**"
  ——终态**不**报错（递交方重试会撞上刚跑完的终态，报错等于"明明送到了却报错"，
  见 §四之三·6）。已改为只标注"未知工作流抛 `NotFoundError`"。
- **`actant/flow.py`**：模块 docstring 原称"`submitted`/`started` 在函数体执行前
  实时广播"——P1-5 后这两个事件在工作流**真正建槽之后**才广播，且无工作流创建的
  flow 只有终态事件。已改。另修正 `wait_signal` 关于缓冲持久性的表述。
- **`actant/task/_async_result.py`**：把已删除的 "eager flow" 旧称改为"增量提交"。
- **`README.md`**：P2P preset 的配置示例**跑不通**（`Runtime.__init__` 是纯关键字
  参数且 `_ActantConfig.payload_signing_key` 必填）。已改为可运行的写法并补注
  `with_defaults` 的 preset 取值规则。
- **`AGENTS.md`**：三处修正——① `@task`/`@flow` 并非基于 `Execute` capability
  （提交走 scheduler → dispatcher）；② 开发期 P2P preset 默认为 `none`（未给
  `data_dir`）而非 `local`；③ Python 层**不能**通过 `Execute` capability 覆盖
  执行行为（`Layer.chain()` 不参与 Rust 内部 dispatch）。
- **`DURABLE_FLOW_DESIGN.md` §二**：加注说明该节是"改造前 vs 改造后"的**历史记录**，
  "现状"指 2026-09 改造前，避免被当成当下状态误读。

### 内部（0.3.3：注释合规清理——计划标签、死链交叉引用、过时断言）

依 `AGENTS.md` 的注释原则（**禁止在注释中记录变更历史、及与当前代码逻辑无关的内容**），
全面清理 `src/`、`actant/`、`tests/` 的注释与 docstring（约 300 处）：

- **移除计划里程碑标签**：`S0`–`S8`、`P0-x`/`P1-x`/`P2-x`、`E1`–`E4`、`R1`–`R6`、
  `X1`、`H1`、`J4` 等。这些编号属于开发计划，读代码的人无从对照。
  ⚠️ **`S3` 有歧义**：多处指 **Amazon S3 对象存储**（`ValueStore` 可覆盖为外部对象
  存储后端），逐处判断后**原样保留**。
- **删除指向 `plans/` 的死链交叉引用**：`plans/` 被 gitignore，注释里写
  "见 `DURABLE_FLOW_DESIGN` §四之三·2" 对任何读者都打不开。共清 20+ 处
  （`DURABLE_FLOW_DESIGN` / `REF_DESIGN` / `ROADMAP` / `§章节号`），**保留其想表达的
  机制本身**，只去掉引用指针。
- **去掉变更历史叙述**（"0.3.2 及更早版本…"、"早期版本会在此…"、"此处的 *eager*
  旧称已随 S7 删除"），改为只陈述当前行为。
- **订正 4 处过时/错误断言**（均打开源码核实后改）：
  1. `py/runtime.rs` 的 `signal_wait_point` 原写"事件重放尚未落地"——**错**：重放早已在
     `persistence.rs` 落地（`replay_events_after_watermarks` / `apply_replayed_event`），
     信号缓冲也确实跨重启存活；
  2. `actor.rs` 的 `on_task_result` 原写"`FailureScope::TaskOnly` 由重试路径内部使用"
     ——**错**：编排内部一律传 `WorkflowLevel`，`TaskOnly` 只由直接调用 `fail_task`
     的调用方（含测试）选择；已与 `execution.rs` 的说明对齐；
  3. `flow.py` 的 `_replay_flow` 原写"S4/S5 **将**以此为重放原语；当前由测试与上层恢复
     逻辑显式调用"——它不是待落地的原语，已由 `resume_flows` 实际驱动，改为陈述现状；
  4. `execution.rs` 的重试日志 `"orchestrator-driven retry scheduled (S6)"` 去掉标签
     （先确认无测试与文档引用该串）。
- 另修 `flow.py` 中一条**面向用户的 warning**：原文指向 `plans/DURABLE_FLOW_DESIGN.md`
  （用户同样打不开）并叙述"0.3.3 已删除"，改为直接说明"`retries` 参数不生效，请改用
  `@task(retries=...)`"。
- **存活文档里的同类死链一并清掉（8 处）**：`AGENTS.md`（裁决编号 `J4` +
  `plans/ROADMAP.md`）、`docs/SLA_BASELINE.md`（3 处 `plans/PLAN.md`/`plans/ROADMAP.md`）、
  `.github/pull_request_template.md`（3 处）与 `.github/workflows/ci.yml`（1 处）。
  其中 PR 模板指向的 **`docs/ROADMAP.md` 在仓库里根本不存在**——贡献者按它去找只会扑空；
  "减法守则"此前也**没有任何仓库内成文出处**，故改为把规则本身写全、去掉指针。
- **35 个孤儿 `.pyc` 移出工作树**（源码已随 0.3.x 减法删除，字节码留在
  `__pycache__/` 里）。它们被 `.gitignore` 挡住、不会入库，但会**误导检索**——
  实际发生过：审查时据 `_serialization.cpython-311.pyc` 误判 `pack_single` "有调用者"。
  移出前先冒烟验证（`import actant` + 单测 362 passed）。

### 缺陷修复（0.3.3 提交就绪度：取消"在途标记"的竞态与契约）

- **`Runtime.is_cancelled` 的契约未写下，导致用例把内部瞬时状态当契约**。
  该方法是"取消已请求、尚未被消费"的**在途标记**，任务一到终态就随注册表回收
  （`unregister_task` → `_clear_task_cancelled`）清除，故对**已取消完成**的任务
  返回 `False`。名字读起来像"任务是否已取消"，语义却是"取消在途"——这个落差
  先骗到了测试：`test_cancel_queued_task` 在 `cancel_task()` 返回后**紧接**断言
  标记为 `True`，而终态可能在两者之间抵达（**单跑 6 次挂 2 次**，失败点固定在
  `test_task.py:546`）。实测 8 次探针确认失败 100% 属于"终态先到 + 已被回收"
  （`get_task() is None`），**不是**"Rust 侧取消注册表与句柄不一致"，
  **`is_cancelled` 本身无 bug**。
- **修复（无 API 语义变化）**：① `is_cancelled` 的 docstring 补明上述契约，并写清
  三项查询的分工（能否取消看 `cancel_task` 返回值、最终结果看 `handle.result()`）；
  ② 用例改断言**可观测契约**（`handle.result()` 抛 `TaskCancelledError` +
  `handle.state == "cancelled"`），清理性断言改以 `get_task() is None` 为等待条件
  ——`unregister_task` **先清标记、后删句柄**，故句柄消失 ⇒ 标记必已清除，无竞态。
  修复后单跑 **10/10 通过**。
- **4 个测试文件此前未被版本控制**（`test_flow_resume.py` / `test_flow_suspend.py` /
  `test_flow_waitpoints.py` / `test_flow_replay.py`，合 1061 行）。它们是 S1/S2/S3/S4
  的接线证据，其中 `test_flow_resume.py` 即 0.3.3 验收「kill -9 重启续跑不重跑」的
  可执行版本——漏掉等于**验收标准在仓库里没有可复现证据**。已纳入索引（40 passed）。

### 语义澄清（0.3.3 P0-3：执行节点失联不自动重跑）

- **执行节点失联 ⇒ 在途任务 fail-fast，不自动重跑**。源节点没有"该任务尚未执行完"
  的持久凭据（结果可能已产出而字节丢失），盲目重派发会**静默重复副作用**；
  因此 0.3.3 明确选择快速失败而非 at-least-once 重跑。需要重跑请使用显式策略：
  编排任务经 S6 重试裁决（`@task(retries=...)` / `RetryPolicy`）重派发，直提任务
  由提交方重新提交。**这是用户可感知的语义承诺**，与"不永久挂起"的 SLA 配套。
- **已知限制（不在本项范围）**：远端节点的结果投递重试耗尽时只在**远端本地**
  发布 `TaskFailed`（`result_delivery.rs`），源节点的在途登记不会被清除；
  该缺口与"节点失联"无关，属结果投递链的独立问题，已记入开发计划。

### 基座硬化（0.3.1）

- **毒消息 bounded-redelivery**：mailbox pending 记录增加 `delivery_count`，`recover_pending` 重投时递增回写；超过 `MAX_PENDING_REDELIVERIES`（5）的确定性失败消息删除记录并 `tracing::error!`（供后续 DLQ capability 消费），不再无限重投、不再随重启全量重放。
- **结果接受 attempt fencing**：故障转移重派发/重试递增 `TaskState.attempt` 并随派发携带；结果接受侧校验代际，过期执行的迟到结果丢弃（wire 协议本版未携带代际的路径保持兼容放行）。
- **租约仲裁裁决落地**：`expire_leases` 恢复"本节点活跃 workflow 即续租"（消除活跃租约周期性 claim 广播写放大与输掉选举后租约无人持有的窗口）；反双主依赖已接线的 `handle_claim`（收到远端 claim 即 `remove_active_workflow`，此后不再续租）。
- **安全最低限**：metrics 端点默认绑定 `127.0.0.1`（暴露所有网卡需显式传 `metrics_bind="0.0.0.0"`）；wire 签名密钥注册表按 node 隔离，同进程多个不同密钥的 Runtime 互不干扰（无来源节点字段的消息退化为 primary 密钥，已文档化）；`validate_data_dir` 从精确匹配改为 canonicalize + 祖先目录判断（`/etc/foo`、`/usr/local/x` 等系统目录子路径被拒）。
- **杂项**：`ExecuteCtx.timeout_ms=0` 映射为无超时（原为立即硬超时陷阱）；emit 聚合错误经 `ActantError::kind_str()` 保留首个失败的 kind（`[actant:KIND]` 前缀，Python `raise_for_kind` 可重建）；crash-failover 回归测试改轮询断言去时序 flake；D8/EventBatcher 裁决：保留后台 flush 线程（移除后低吞吐场景事件滞留，及时性回归 > 收益）。
- **测试与 CI**：新增多节点可靠性矩阵（杀节点/重启续跑/乱序结果/worker 杀死隔离）与混沌压力基线（打满队列零丢失/大 fan-out/慢消费者，观测数据见 `docs/SLA_BASELINE.md`）；从零建立 GitHub Actions（Linux 全量门禁 + Windows 编译与 pipe 路径冒烟 + nightly benches 记录，不做阈值断言为有意取舍）与 PR 模板（删加对称检查）。

- **任务执行后端：线程池 → 进程池**：任务改为在 Rust 管理的 **worker 子进程**（`python -m actant.task._worker`）中执行，原同进程线程池执行器已移除，进程池为唯一执行后端。此前依赖线程内共享状态（模块全局变量、`threading.Event`、进程内对象）在任务间进行通信的代码不再生效——不同任务运行在不同的 worker 进程，不共享可变全局状态。此变更带来崩溃隔离（任务 segfault / `os._exit` 仅失败该任务，节点存活）、GIL 真并行与硬超时强杀。

- **任务超时语义：软 → 硬**：`@task(timeout_ms=...)` 超时后 Rust 会对对应 worker 进程 `terminate()`/`kill()`，**真正终止任务并释放计算资源**，槽位即时回收并自动拉起替补进程。此前超时只能协作式取消、占用的线程会运行到结束。`WorkerConfig` 新增 `num_worker_processes`（进程池大小，默认 CPU 核数）与 `crash_failover_max_attempts`（崩溃重路由上限，默认 3）；`execution_backend` 相关残留参数不再生效。

- **`@flow`** **弃用参数移除**：`mode="dag"` 与 `compiled=True` 参数及 `actant._flow_compiled` 模块已删除。`@flow` 现为动态 DAG 语义——执行期经 `FlowDAG` 记录器捕获节点与依赖边，函数体返回后提交 Rust Orchestrator 持久化并按结果回灌驱动状态机；生命周期事件改由持久化状态驱动而非手写 emit。

- **崩溃任务自动重路由**：worker 进程崩溃（非逻辑失败、非硬超时）时，任务清空目标节点重新入队，由路由器重选本地或远端节点执行，受 `crash_failover_max_attempts` 上限约束；达到上限才降级为正常失败路径。

### 优化

- **EventBus 高并发唤醒优化**：`TaskEnqueued` 唤醒信号从 mpsc 通道订阅改为
  专用 `Arc<Notify>`。此前 1000 并发任务场景下，`TaskEnqueued` 作为 `BestEffort`
  事件因订阅者通道满被大量丢弃，产生 "subscriber is full, dropping best-effort event"
  告警。`Notify::notify_waiters()` 无队列、无丢弃，所有等待的 Worker 立即唤醒。
  相应移除 `BusEvent::TaskEnqueued` 变体与 `Topic::TaskEnqueued`；`SchedulerActor`
  的 `notify_task_enqueued()` 改为同步方法触发 Notify；Worker 的 `wait_for_task` /
  `prefetch_tasks` 改在 `notify.notified().await` 上等待。

- **EventBus 降格为纯非阻塞观测 tap**：`publish` 对所有订阅者一律 `try_send`，
  通道满即丢弃该事件（记 `actant.event_bus.publish.dropped` 计数 +
  `tracing::debug!`——tap 无修剪机制，慢消费者会持续满载，更高级别会刷屏），
  **观测慢/卡死绝不反压生产者热路径**。此前 `Backpressured` 投递的
  `send_timeout` 内联 await 在 worker 执行循环上（TaskStarted/Completed 等热路径），
  慢订阅者可拖慢任务执行。相应删除 `DeliveryGuarantee` 投递保证枚举、订阅者
  连续超时计数与静默修剪机制（被修剪后无重订阅路径，属缺陷设计而非特性）；
  `publish` 变为同步方法（无 await 点）。**控制面消息改走直连分发**：心跳/claim
  /DAG 状态更新/Heads 交换四类跨节点指令由 `NetworkEventRouter` 在 wire 消息
  解码后直接调用 `FailoverManager` / `DagGossipActor`（照抄
  `handle_workflow_state_event` 既有直连模式），不再经过 EventBus——观测 tap
  有损可丢，控制面投递必须无损点对点；builder 的 `spawn_inbound_cluster_events`
  订阅分发随之删除。`Topic::ClusterHeartbeat`/`ClusterClaim`/`DagUpdate`/
  `HeadsExchange`（EventBus 话题）与对应 `BusEvent` 变体删除；wire 层
  `TopicRoute::Heartbeat`/`Failover`/`DagState`/`Heads` 分类保留（wire 协议面
  不变）。观测指标 `actant.event_bus.publish.timeout` 更名为
  `actant.event_bus.publish.dropped`。

- **EventBus 订阅者深度指标**：新增 `actant.event_bus.subscriber.depth` Gauge，
  按 `topic` 标签记录每次 publish 后各 topic 订阅者通道的最大积压深度，用于预警队列堆积。

- **父进程侧 TaskLifecycle 事件批量派发**：`Runtime` 现在持有 `_EventBatcher`，
  `_on_task_result` 的 started/completed/failed 事件经 batcher 累积后由后台线程批量 emit，
  替代进程池后端下每任务 2 次同步 emit。`_EventBatcher` 新增 `render` 回调，使其后台
  flush 线程能把事件绑定到所属 `Runtime` 上下文后再派发；`stop()` 时 close 并派发剩余事件。

- **worker 复用单条 asyncio 事件循环**：`_run_coroutine_on_worker_thread` 改为懒创建并
  复用进程级 event loop，替代每个 async 任务 `new_event_loop` + 清理 + `close` 的重复开销，
  执行后由 `_cancel_pending_loop_tasks` 取消遗留后台任务以保证下次复用干净。

- **TaskLifecycle 零消费者自动静默**：`_emit_batch` 在 `_layers[TASK_LIFECYCLE]` 为空时
  直接跳过 emit（该 capability 为纯可观测事件，任务执行由 worker 结果回调驱动不受影响），
  注册 handler 后动态恢复派发；消除了无消费者时每任务的跨边界 publish 开销。

- **基准** **`op_time`** **口径修正**：`OPS_PER_CALL` 让批量内核（gather/concurrency/events/flow）
  的 `per_op_us` 按内部任务数折算，新增 `call_ms` / `op/call` 列，避免把 sample 耗时当
  单任务耗时误读。

- **worker 轻量协议 v2（控制头部化 + 序列化器复用）**：Dispatch 载荷把控制元数据
  （retries / retry\_delay\_ms / task\_id / workflow\_id）迁出 cloudpickle 的 `options` dict，
  内联为紧凑二进制头部（版本字节 + 定长字段 + 变长字符串），载荷仅序列化 `(func, args,
  kwargs)`；worker 侧 `struct` 单遍解析头部、只反序列化函数载荷，移除每任务 options dict
  的编解码。`timeout_ms` 为死参数（硬超时由 Rust 进程池强杀负责）不再传递。worker 侧复用
  cloudpickle Pickler（每 dump 前 `clear_memo()` 防串扰）与输入 `BytesIO`，避免每任务新建
  序列化器/缓冲。帧外壳不变（Rust 仍把 payload 当不透明字节校验搬运），结果帧格式不变。

- **worker IPC 热路径三项优化（A+B+C）**：

  - **A 取消写端去 Mutex 竞争**：dispatcher hot-path `send_frame` 从 `Arc<tokio::sync::Mutex<ChildStdin>>`
    改为 `WorkerProc` 独占持有，取消轮询通过 `dup(2)` 出独立 fd 并发写入 Cancel 帧（5 字节
    < PIPE\_BUF，Unix 内核保证 write 原子）；hot-path 全程无 async Mutex、无 Arc clone 竞争。
    非 Unix 降级为 dispatcher 侧 `terminate_and_replace` 兜底发 Cancel（语义等价）。

  - **B vectored I/O + 头缓冲复用**：Rust `send_frame` 改为 `write_vectored` 提交
    `[header_buf, body]` 两段 iovec 给内核，去掉原先拼接 `Vec` 的分配 + memcpy；header\_buf
    是 per-worker 持久栈上固定 5 字节数组。Python `_write_frame` 优先 `os.writev` 对称处理。

  - **C Python** **`readinto`/`memoryview`**：`_read_frame` 头读用复用的 `bytearray` +
    `readinto`，去除每帧 header 小 bytes 分配；`_read_exact` 从 chunk list + join 改为
    `bytearray(n)` 单次分配 + `memoryview` 切片读，去掉中间 bytes 拼接复制与 chunk list 增长。

- **worker IPC 共享内存 Ring Buffer（D 项落地）**：dispatcher↔worker 的正文搬运从
  `pipe(2)` 4 次内核拷贝改为**共享内存 + pipe 门铃**。每 worker 一条 256KB 共享内存段，
  内含 p2c/c2p 两条 SPSC ring（`src/runtime/worker_shm.rs` 创建映射、`worker_ring.rs`
  实现 ring）；每帧在 pipe 上只传 5 字节帧头（`[u32 长度][u8 类型]`）作跨进程门铃与
  happens-before，正文经 mmap 零内核拷贝读写。正文超过 ring 数据区 1/4 或 ring 不可用时
  走 pipe 直传（帧头类型置 `0x80` 大正文标记），shm 创建失败自动降级纯 pipe，帧语义与
  调用方 API 零变化。平台：Linux `memfd_create`、macOS 短名 `shm_open` + 立即 `unlink`、
  Windows 命名 MMF（UUID + 会话命名空间）。设计详见
  `docs/DESIGN_worker_shm_ring_buffer.md`。

- **ring 传输正确性修复与复用回归**：修复 `_worker.py::_write_frame` 在 ring 路径把正文
  残留写入 pipe 门铃的缺陷——读端按 CLEAN 类型字节从 ring 读正文、pipe 上多余正文不被
  消费、腐蚀下一帧头，导致「复用一个 worker 的第二个任务起必触发 crash failover、替补
  worker 才成功」。新增 20 任务 worker 复用回归测试与崩溃/超时/fd 泄漏正确性注入
  （`tests/python/integration/test_worker_ring_correctness.py`）。**实测收益**：同一进程池
  复用分发与纯 pipe 持平（约 6ms/op，正文 1KB~1MB 亦持平），单任务延迟由 pipe 门铃内核
  唤醒 + 调度器往返主导，正文零拷贝非该量级瓶颈；诊断开关 `ACTANT_DISABLE_SHM_RING=1`
  可强制 pipe 降级以作同机 A/B 与排障。

### 内部

- **移除死代码** **`actant.task._dispatch`** **模块**：`_bind_dispatch_handler` 在进程池后端
  下生产路径从未被调用（`register_python_dispatch_handler` 已是 no-op），连同其单测
  `test_dispatch_bound.py` 一并删除。`_execute_with_retries` 归入 `_helpers.py`，
  由 worker 子进程直接复用；`_worker.py` 与 `test_dispatch.py` 改从 `_helpers` 导入。
  任务执行路径收敛为"worker 子进程独占"单一语义。

- **归一执行模型叙述**：`_run_with_timeout` / `_run_coroutine_on_worker_thread` 的
  docstring、`task/__init__.py`、`_runtime.py` 中关于"Rust tokio 线程池 worker"的陈旧措辞
  全部改写为进程池模型（硬超时由 `ProcessTaskDispatcher` 强杀 worker；worker 为纯 Python
  子进程，无运行中的 asyncio loop）。

- **合并依赖解析遍历**：`_collect_async_result_ids` 删除，与 `_resolve_value` 统一为
  `_resolve_args_with_deps` 单遍解析（同一次遍历内解析上游 `AsyncResult` 并去重保序收集
  依赖 id）。`Task.submit` / `submit_batch` 由"先收集再解析"两遍遍历改为一遍。

- **补齐** **`ruff check benches/`**：修复 `bench_events.py` / `run_bench.py` 的 5 处
  F401/F841/UP035/RUF100 告警，使 `ruff check actant tests/python benches/` 全绿。
  `mypy actant` 源文件统计 16 → 15（移除 `_dispatch.py`）。

### 缺陷修复（代码质量审查，详见 docs/CODE_QUALITY_REPORT.md）

**正确性（P0）**

- **HLC merge 时间戳回退**：`HybridLogicalClock::merge` 原"本地主导清零 logical、远端并列取 remote+1"
  的分支在 gossip 常态下可产生落后于本地历史的时间戳，导致新更新被判 stale 丢弃。按 Kulkarni
  标准算法重写（并列取 `max(c_local, c_remote) + 1`），新增内联单调性测试与 3 节点乱序 merge
  属性测试（`tests/rust/property/hlc.rs`）。
- **迟到完成复活终态工作流**：`mark_task_completed` 幂等守卫只挡 `Completed`，取消/失败后到达的
  迟到完成回传会把任务改写为 Completed 并翻转工作流终态。新增集中式守卫
  `WorkflowExecution::can_transition_task`，`mark_task_completed`/`fail_task`/
  `check_workflow_completion`/`complete_task` 统一口径：工作流或任务已终态一律拒绝推进。
- **入站集群事件接线断裂**：心跳、DAG 状态更新、heads 交换、claim 四类入站事件此前发布到
  EventBus 后无任何消费者——peer 视图恒为空（跨节点任务转发与故障检测整体失效）、gossip 收敛
  只有广播没有落地、claim 双主防护不可达。`RuntimeBuilder` 现订阅相应 topic 并分发至
  `FailoverManager` / `DagGossipActor`，新增装配接线测试（`tests/rust/unit/runtime/workflow/wiring.rs`）。
- **节点重启后恢复的任务不重派发**：`recover_ready_tasks` 此前无调用方且过滤不看任务状态
  （已完成任务会被重复派发）。修正为仅重建 `Pending` 任务，并接入 builder 恢复路径
  （recover 后经 `SchedulerActor::enqueue_batch` 重派发），新增端到端恢复测试。
- **`AsyncResult.add_done_callback` 竞态丢失回调**：完成协议"锁内拷贝回调清空列表 → 锁外置位
  future"的窗口内注册的回调会被永久丢弃。`add_done_callback` 改为锁内检查终态，新增千轮并发
  回归测试。
- **Windows 下 worker 进程启动即死**：`_worker._init_ring_transport` 对 ring fd 环境变量无条件
  `int()`，而 Windows 上 Rust 侧写入的是 MMF 名称字符串。现在仅纯整数值走 fd mmap，其余
  （含 Windows 名称）记日志后降级纯 pipe 传输。

**可靠性与竞态（P1）**

- 取消轮询器 TOCTOU：结果臂释放 worker 前必须经 `stop_cancel_poller`（abort + 等待退出），
  过期 Cancel 帧不再可能滞留复用 worker 的 stdin 误杀下一任务；`maybe_written` 契约以 pipe
  压测回归固化。
- 故障转移 fencing：`expire_leases` 对已过期租约不再无条件自续（过期即失效走重新选举）；
  故障检测候选集先剔除失联节点；心跳时间戳改用接收方本地时钟，消除跨节点时钟偏差侵蚀检测窗口。
- gossip 去重标记移至 apply 成功之后：apply 失败的更新可被重传重放，不再永久丢失。
- workflow runtime：本地无法执行且无远端可用、远端转发失败两条重入队路径增加每任务弹跳上限
  （超限转 Failed 并通知 origin）；drain 丢弃排队/inflight 任务时逐个发布 Cancelled 完成事件；
  远端结果投递重试改为每结果独立重试任务（消除队头阻塞），超限发布 TaskFailed 补偿事件。
- 条件边求值失败不再丢失已就绪后继（deferred 边交还调用方，重试不卡死）；`submit_with_timeout`
  的 deadline 立即 mark_dirty 持久化（重启后工作流级超时不再失效）；序列化失败记录错误并重新
  标脏（不再静默跳过落盘）；工作流超时路径补写 Failed 事件。
- Actor 子系统：消息级失败/panic 不再误减 `active_actors`（仅 cleanup 扣减）；
  `on_start` 失败回滚类型注册（不再残留幽灵条目）；同类型多实例选择改为真 round-robin
  （AtomicU64 计数器，原实现按指针地址取模恒选同一实例）。
- `MailboxRegistry` 待发消息改为**处理成功（ack）后**删除 pending 记录，模块文档如实声明
  at-least-once 语义（原实现入队即删，崩溃窗口内消息丢失且 ack 为无效调用）。
- 网络直连：请求超尺寸、事件通道满、读帧/解码失败三类丢弃路径经 `DirectResponseChannel::send_error`
  回错，对端快速失败（原先空等 30s 超时）；`NetworkManager::subscribe` 的 check-then-insert
  改写锁内一次完成。
- Python 层：flow 超时触发的重试前先 join 孤儿执行线程（join 超时则放弃重试直接失败），
  消除"两个线程并发执行同一 flow 体"的副作用重复；`cancel_task` 失败不再 `suppress` 静默
  （warning 日志）；`_runtime` 对显式传入 tmp 目录下的 `data_dir` 不再嗅探禁用 P2P 发现。

**语义与文档修正**

- `DeliveryGuarantee::Reliable` 更名 `Backpressured`：原实现超时即丢弃事件，与"关键事件不可丢失"
  注释不符（orchestrator 主路径经专用 completion 通道，不依赖该保证）。
- `StoreConfig` 接入 `RuntimeBuilder` 主构建路径（此前用户配置的 `sync_mode`/`map_size`/`max_dbs`
  静默失效）；`data_dir` 构建时执行系统目录黑名单校验（`validate_data_dir` 原为零调用的死代码）。
- `WriteBatcher::Drop` 改为排空等待最终 flush（原 `abort()` 使已接受写入随任务取消丢失）；
  `flush_batch` 中 delete 失败不再被 `let _` 吞掉（ heed 错误照常传播，仅忽略 bool 返回值）；
  `Store::flush`（GroupCommit）以提交计数 + Condvar 保证"调用时刻前入队条目已提交"
  （原 `sleep(1ms)` 近似）。
- WAL 读取对损坏长度字段设 64MiB 上界（防巨型分配），checksum 错误日志记录实际出错位置。
- emit（ErasedHandler 层）注释澄清 `Handler::handle` 返回 `None` 为"handler 无意见"而非失败；
  CapabilityActor 的 emit 改为顺序调用所有 handler 并聚合失败统一回报（不再首个错误即中断）。
- `PyGossipConfig` 默认值改为委托 `GossipConfig::default()` 单一来源；
  `python_executable`/`python_sys_path` 提取失败记录 error 日志（原静默降级为空值）；
  `register_python_dispatch_handler` 保持兼容签名但显式 warning 声明 no-op。

**配置与接口**

- `_ActantConfig` 新增 `num_worker_processes`、`crash_failover_max_attempts`、
  `workflow_default_timeout_ms` 可选参数（此前文档承诺可配、实际硬编码）；`@flow` 新增
  `failure_strategy` 参数（原 `"fail_fast"` 硬编码不可覆盖）；`WorkerConfig` 新增
  `prefetch_min`/`prefetch_max`（原 `clamp(16, 64)` 魔法数）。
- `actant.pyi` 补齐漂移：`_ActorCore.spawn_actor`、`_RuntimeCore.actor_core`/
  `submit_tasks_batch`、`_NetworkConfig.dns_origin_domain`、`_ActantConfig` 新参数。
- `actant task list` 无运行时注入时输出明确提示而非新建空 Runtime 永远返回空列表。
- 修复变参 `fcntl` 手工 extern 声明在当前 nightly aarch64 工具链上丢失第三实参的问题
  （F_SETFL 静默失效），生产与测试统一改用 `libc::fcntl`。
- `TraceScopeGuard` Drop 真正恢复前值；`backoff::new` 文档与实现对齐（base=0 返回 ZERO 不 panic）；
  `workflow/scheduler` 状态字符串、metrics 绑定地址（新增 `metrics_bind` 参数）等小项见报告。

**修复复审追加（第二轮）**

- **shutdown 后 worker 替补泄漏**：`ensure_replacement` 在 shutdown 后不再 spawn 替补，
  `release_worker` 在 shutdown 后直接终止并回收释放的 worker——此前在途任务结束时新拉起的
  worker 会滞留在空闲队列无人回收（父进程存活期间进程泄漏）。
- **mailbox send 失败回滚 pending 记录**：投递失败（actor 已停止）时清理已持久化的 pending
  记录，防止同 id actor 重新注册后 `recover_pending` 重投"调用方已确认失败"的消息。
- **`fail_task` 纳入集中终态守卫**：迟到的失败事件不再把已取消任务改写为 Failed 并参与
  fail-fast 计数（守卫口径与 `mark_task_completed` 统一）。
- **result_delivery 重投失败补发补偿**：重试任务 re-enqueue 失败（通道满/已关闭）时该结果
  永远无法再进入重试队列，现直接发布 TaskFailed 补偿事件（原实现静默丢弃且注释论证有误）。
- **`submit_dag` 非法 `failure_strategy` 报错**：显式传入无法解析的值抛 `ValueError`，
  不再静默落到默认 FailFast。
- **`actant worker` P2P 提示诚实化**：未给 `--data-dir` 时实际以 "none" preset 单进程运行，
  提示语相应改为 "P2P disabled"。
- **提交侧帧上限校验**：`_safe_serialize` 对超过 `MAX_FRAME_BYTES`（256MB，单点定义于
  `_helpers`）的载荷抛 `SerializationError` 并提示按引用传递大对象——此前超限载荷会送抵
  worker 被拒，触发 3 次无意义的 crash-failover 重试。
- **await/gather 线程创建失败释放槽位**：`Thread.start()` 抛异常时释放 `_await_slots`
  信号量，防止 32 槽被永久耗尽。
- 清理 `DeliveryGuarantee` 改名残留（日志文案/测试名/AGENTS.md）；修正
  `recover_ready_tasks` 过时的"接线缺口"文档与每次重启触发的 warn（已接线，降为 debug）。

## [0.3.0](https://github.com/actant/actant/releases/tag/v0.3.0) — 2026

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

- **`PYTHON_ONLY_CAPABILITIES`** **/** **`RUST_BACKED_CAPABILITIES`** **常量**：表征 capability
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

## [0.2.0](https://github.com/actant/actant/releases/tag/v0.2.0) — 2025

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

- **`gather`** **并行等待原语**：`actant.gather(*async_results)` 批量等待多个任务完成。

- **协作式取消**：`TaskContext` + `CancelToken` 实现 Python 与 Rust 之间的协作式取消信号传递。

- **`EventBus`** **内部事件总线**：统一的发布/订阅中枢，支持 `Reliable` 与 `BestEffort` 两种投递保证，订阅者超时自动修剪。

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

## [0.1.0](https://github.com/actant/actant/releases/tag/v0.1.0) — 2024

### 初始发布

- Actor 模型运行时（`ActorSystem`、监督、邮箱、持久化）

- DAG 工作流协议与拓扑计算

- 基于 iroh 的 P2P 网络层（发现、直连、gossip）

- LMDB 持久化（Store、HLC、Checkpoint、WAL）

- PyO3 绑定暴露 Rust 原语为 Python 对象

