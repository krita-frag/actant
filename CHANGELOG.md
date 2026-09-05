# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/)，但 1.0.0 前不保证向后兼容。
每次发布记录破坏性变更、新增功能与缺陷修复。

## [Unreleased](https://github.com/actant/actant/compare/v0.3.0...HEAD)

### 破坏性变更

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

