//! 运行时构建器。
//!
//! 负责根据配置顺序构造 network → store → actor → workflow → capability 各子系统。
//! 此处只包含不依赖 PyO3 的纯 Rust 初始化逻辑；Python callable 的注册保留在 `src/py/`。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::common::scheduler_kind;
use crate::common::{ActantConfig, ActantError, ActorId, NodeId, Topic};
use crate::runtime::actor::ActorSystem;
use crate::runtime::dispatcher::{ProcessTaskDispatcher, TaskDispatcher};
use crate::runtime::event_bus::EventBus;
use crate::runtime::network::Transport;
use crate::runtime::state::{CheckpointManager, LmdbStore, Store, WalWriter};
use crate::runtime::workflow::orchestrator::Orchestrator;
use crate::runtime::workflow::Worker;
use crate::runtime::workflow::{ActorScheduler, FailoverManager, SchedulerActor};

const FORBIDDEN_SYSTEM_DIRS: &[&str] = &[
    "/",
    "/etc",
    "/bin",
    "/sbin",
    "/usr",
    "/lib",
    "/lib64",
    "/dev",
    "/proc",
    "/sys",
    "/boot",
    "/var/log",
    "C:\\Windows",
    "C:\\Program Files",
    "C:\\Program Files (x86)",
    "C:\\ProgramData",
];

/// 校验 operator 提供的 `data_dir` 配置。
///
/// 黑名单语义：黑名单目录是 `data_dir` 的**祖先（或相等）即拒绝**，而非仅
/// 精确匹配——防止 `/etc/actant`、`/usr/local/x` 等系统目录子路径被当作数据目录。
/// 唯一例外是 `/`：它是一切绝对路径的祖先，只做精确匹配（否则所有合法目录
/// 都会被拒绝）。
///
/// 路径归一化：`data_dir` 在首次启动时通常尚不存在，无法直接 canonicalize。
/// 实现为从候选路径逐级向上找到第一个已存在的祖先做 canonicalize（消除
/// symlink 差异，如 macOS `/var` → `/private/var`），再把其余字面量组件拼回
/// 后比对；黑名单项同样 canonicalize 后比对。
pub fn validate_data_dir(data_dir: &str) -> Result<(), ActantError> {
    let trimmed = data_dir.trim();
    if trimmed.is_empty() {
        return Err(ActantError::Config("data_dir must not be empty".into()));
    }
    let candidate = Path::new(trimmed);

    // 逐级向上找第一个已存在的祖先做 canonicalize；全部不存在（如相对路径
    // 组件耗尽）时跳过归一化——此时无法解析 symlink，维持宽松放行。
    let mut probe = candidate.to_path_buf();
    let canonical = loop {
        match probe.canonicalize() {
            Ok(base) => {
                // probe 是 candidate 的路径前缀（由重复 parent() 得到），
                // strip_prefix 必然成功；unwrap_or 兜底空路径。
                let suffix = candidate
                    .strip_prefix(&probe)
                    .unwrap_or_else(|_| Path::new(""));
                break base.join(suffix);
            }
            Err(_) => match probe.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => probe = parent.to_path_buf(),
                _ => return Ok(()),
            },
        }
    };

    for forbidden in FORBIDDEN_SYSTEM_DIRS {
        let Ok(canon_forbidden) = Path::new(forbidden).canonicalize() else {
            continue;
        };
        let hit = if *forbidden == "/" {
            canonical == canon_forbidden
        } else {
            // starts_with 为按组件的前缀比较：/etc/actant starts_with /etc，
            // 且不会误匹配 /etcfoo 这类共享字符前缀的路径。
            canonical.starts_with(&canon_forbidden)
        };
        if hit {
            return Err(ActantError::Config(format!(
                "data_dir must not point at system directory {}",
                forbidden
            )));
        }
    }
    Ok(())
}

/// 初始化 actor 系统，可选持久化。
pub fn init_actor_system(
    data_dir: Option<&str>,
    node_id: &NodeId,
    event_bus: &EventBus,
    config: &crate::common::ActantConfig,
) -> Result<Arc<ActorSystem>, ActantError> {
    let base = ActorSystem::new()
        .with_node_id(node_id.clone())
        .with_event_bus(event_bus.clone());
    let system = if let Some(dir) = data_dir {
        let db_path = Path::new(dir).join("actor");
        // actor 子存储与主存储使用同一 StoreConfig（map_size / max_dbs /
        // sync_mode），保证持久化语义一致。open_with_config 内部创建目录。
        let store = LmdbStore::open_with_config(&db_path, &config.store)
            .map_err(|e| ActantError::Storage(format!("failed to open actor store: {}", e)))?;
        let checkpoint = CheckpointManager::new(store.clone());
        let wal_path = Path::new(dir).join("actor.wal");
        let wal_writer = WalWriter::open_with_sync(&wal_path, true)
            .map_err(|e| ActantError::Storage(format!("failed to open actor WAL: {}", e)))?;
        Arc::new(base.with_wal(wal_writer, store).with_checkpoint(checkpoint))
    } else {
        Arc::new(base)
    };
    Ok(system)
}

/// 初始化 orchestrator，可选持久化。
pub async fn init_orchestrator(
    data_dir: Option<&str>,
    node_id: &NodeId,
    config: &ActantConfig,
) -> Result<Arc<Orchestrator>, ActantError> {
    if let Some(dir) = data_dir {
        let db_path = Path::new(dir).join("orchestrator");
        // orchestrator 子存储与主存储使用同一 StoreConfig（map_size / max_dbs /
        // sync_mode），保证持久化语义一致。
        let store = LmdbStore::open_with_config(&db_path, &config.store)
            .map_err(|e| ActantError::Storage(format!("failed to open store: {}", e)))?;
        let async_store = Store::new(store.clone());
        let event_log = Arc::new(crate::runtime::state::event_log::LmdbEventLog::new(
            store.clone(),
        ));
        Ok(Arc::new(
            Orchestrator::recover(async_store, config.clone(), Some(event_log.clone()))
                .await
                .map_err(|e| ActantError::Workflow(format!("orchestrator recover failed: {}", e)))?
                .with_node_id(node_id.clone())
                .with_event_log(event_log),
        ))
    } else {
        Ok(Arc::new(
            Orchestrator::new()
                .with_node_id(node_id.clone())
                .with_config(config.clone()),
        ))
    }
}

/// [`init_worker`] 的参数。
pub struct WorkerInitParams<'a> {
    pub node_id: &'a NodeId,
    pub network: &'a Arc<dyn Transport>,
    pub event_bus: EventBus,
    pub scheduler_kind: &'a str,
    pub worker_config: &'a crate::common::WorkerConfig,
    pub actor_system: Arc<ActorSystem>,
    pub task_dispatcher: Arc<dyn TaskDispatcher>,
    pub failover: &'a Arc<FailoverManager>,
    pub tokio_handle: tokio::runtime::Handle,
    /// 本地 WorkflowActor 的 id。用于接收远程任务结果。
    pub workflow_actor_id: Option<crate::common::ActorId>,
    /// 本地 DagGossipActor 的 id。用于处理工作流状态请求/响应主题。
    pub dag_gossip_actor_id: Option<crate::common::ActorId>,
    /// Capability gossip 处理器。用于接收 `TOPIC_CAPABILITY_GOSSIP` 上的远端 capability 广播。
    pub capability_gossip: Option<Arc<crate::runtime::capability::gossip::CapabilityGossipActor>>,
}

/// 初始化 worker runtime。
///
/// 调度器被封装为 [`SchedulerActor`] 并在 `actor_system` 中 spawn；
/// `Worker` 通过 [`ActorScheduler`] 客户端与 Actor 交互，
/// 调度状态由 Actor 持有。
pub async fn init_worker(params: WorkerInitParams<'_>) -> Result<Worker, ActantError> {
    let scheduler_actor_id = ActorId::scheduler(params.node_id);
    // 注入 EventBus 到 SchedulerActor：Actor 在 enqueue 后触发
    // notify_task_enqueued()，Worker 通过 Notify 信号实现事件驱动唤醒。
    let scheduler_actor = match params.scheduler_kind {
        scheduler_kind::FIFO => SchedulerActor::with_event_bus(params.event_bus.clone()),
        scheduler_kind::PRIORITY => {
            SchedulerActor::with_event_bus(params.event_bus.clone()).with_priority()?
        }
        other => {
            return Err(ActantError::Config(format!(
                "unknown scheduler kind '{}': expected one of: {}, {}",
                other,
                scheduler_kind::FIFO,
                scheduler_kind::PRIORITY
            )))
        }
    };
    // 在 spawn 前提取共享内部状态引用，用于 ActorScheduler 的 enqueue 快路径。
    // shared_inner 返回 Arc<InnerScheduler>，Actor 持有同一 Arc 的克隆。
    // 此后 ActorScheduler::enqueue 直接调用 InnerScheduler::enqueue 同步方法，
    // 绕过 Actor 消息往返（postcard 编解码 + 邮箱调度 + 响应通道）。
    let fast_inner = scheduler_actor.shared_inner();
    params
        .actor_system
        .spawn(scheduler_actor_id.clone(), scheduler_actor)
        .await?;

    let actor_scheduler = Arc::new(ActorScheduler::with_fast_path(
        scheduler_actor_id.clone(),
        params.actor_system.clone(),
        fast_inner,
    ));

    let failover_cb = params.failover.clone();
    let worker = Worker::new(
        params.node_id.clone(),
        params.network.clone(),
        params.event_bus,
        actor_scheduler,
        params.task_dispatcher,
        params.worker_config,
        params.tokio_handle,
    )
    .with_actor_system(params.actor_system.clone())
    .with_scheduler_actor_id(scheduler_actor_id)
    .with_capacity_callback(Arc::new(move |available, max| {
        failover_cb.update_local_capacity(available, max);
    }));

    let worker = match params.workflow_actor_id {
        Some(id) => worker.with_workflow_actor_id(id),
        None => worker,
    };
    let worker = match params.dag_gossip_actor_id {
        Some(id) => worker.with_dag_gossip_actor_id(id),
        None => worker,
    };
    let worker = match params.capability_gossip {
        Some(g) => worker.with_capability_gossip(g),
        None => worker,
    };
    Ok(worker)
}

/// 构建 [`crate::runtime::Runtime`] 的 builder。
///
/// 顺序构造：network → store → actor → workflow → capability。
pub struct RuntimeBuilder {
    node_id: NodeId,
    config: ActantConfig,
    data_dir: Option<String>,
}

impl RuntimeBuilder {
    pub fn new(node_id: NodeId, config: ActantConfig) -> Self {
        Self {
            node_id,
            config,
            data_dir: None,
        }
    }

    pub fn with_data_dir(mut self, data_dir: String) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    pub async fn build(self) -> Result<Arc<crate::runtime::Runtime>, ActantError> {
        use crate::runtime::capability::gossip::CapabilityGossipActor;
        use crate::runtime::capability::register_defaults;
        use crate::runtime::capability::register_serialization_handler;
        use crate::runtime::capability::register_store_handler;
        use crate::runtime::capability::CapabilityRuntime;
        use crate::runtime::context::Runtime as ActantRuntime;
        use crate::runtime::network::NetworkManager;
        use crate::runtime::workflow::DagGossip;
        use crate::runtime::workflow::DagGossipActor;
        use crate::runtime::workflow::FailoverActor;
        use crate::runtime::workflow::FailoverManager;

        let _build_span = tracing::info_span!("runtime.build", node = %self.node_id).entered();

        // 启动前校验所有配置约束：未知 discovery/scheduler、Failover 时序、
        // payload 签名硬失败（require_payload_signing=true 且 key 为空时直接报错）。
        self.config.validate()?;

        // SE2: 空 payload_signing_key 会禁用 payload 完整性验证，仅适用于开发/测试。
        // 生产环境必须配置非空密钥，否则恶意节点可投递伪造 cloudpickle payload。
        // require_payload_signing=true 时已由 validate 拒绝；此处仅对未强制签名的
        // 开发场景输出 warn，提醒用户此为不安全配置。
        if self.config.payload_signing_key.is_empty() {
            tracing::warn!(
                "payload_signing_key is empty — payload integrity protection DISABLED. \
                 This is unsafe for production: configure a non-empty shared secret \
                 or set require_payload_signing=true to enforce."
            );
        }

        // D2：将 payload 签名密钥同时作为 wire message 签名密钥。
        // 设计：单一密钥承担双重职责（payload + wire），简化运维配置。
        // 空密钥 = 禁用 wire 签名验证，向后兼容 0.2（无 mac 字段）。
        // 非空密钥 = 集群内所有节点必须共享同一密钥；任一节点密钥不匹配
        // 将导致其跨节点消息被对端丢弃，提供端到端集群身份认证。
        // H6.2：按 node 注册——同进程多 Runtime 各自密钥互不覆盖，
        // 出站签名按消息来源节点选择（wire 模块的调用点在冻结的
        // workflow/ 子树中，密钥无法参数穿透，故保留进程级注册表）。
        crate::common::register_wire_signing_key(
            &self.node_id,
            self.config.payload_signing_key.clone(),
        );

        let data_dir = self
            .data_dir
            .clone()
            .ok_or_else(|| ActantError::Config("RuntimeBuilder requires data_dir".into()))?;
        // 拒绝指向系统目录等危险路径，尽早失败（blob 存储也落在此目录下）。
        validate_data_dir(&data_dir)?;
        // blob 原语（0.3.2 R1）：FsStore 落盘 data_dir/blobs，随节点持久化。
        let blob_dir = Path::new(&data_dir).join("blobs");
        let blob_store = Arc::new(crate::runtime::blobs::BlobStore::open(&blob_dir).await?);

        tracing::info!("build: NetworkManager::new enter");
        let network: Arc<dyn Transport> = Arc::new(
            NetworkManager::with_blob_store(
                self.node_id.clone(),
                self.config.network.clone(),
                blob_store.clone(),
            )
            .await
            .map_err(|e| ActantError::Network(format!("network init failed: {}", e)))?,
        );
        tracing::info!("build: NetworkManager::new done");

        let event_bus = EventBus::new();

        tracing::info!("build: init_actor_system enter");
        let actor_system = init_actor_system(
            self.data_dir.as_deref(),
            &self.node_id,
            &event_bus,
            &self.config,
        )?;
        tracing::info!("build: init_actor_system done");

        // 有持久化存储时启动 WAL compaction 后台任务，由 Runtime::shutdown 停止。
        if self.data_dir.is_some() {
            actor_system.start_compaction_task();
        }

        let store_path = Path::new(&data_dir).join("store");
        // 主存储使用配置中的 StoreConfig（map_size / max_dbs / sync_mode），
        // 不再隐式退回默认配置。open_with_config 内部创建目录。
        let lmdb_store = LmdbStore::open_with_config(&store_path, &self.config.store)
            .map_err(|e| ActantError::Storage(format!("failed to open store: {}", e)))?;
        let store = Store::new(lmdb_store.clone());

        let task_dispatcher: Arc<dyn TaskDispatcher> = Arc::new(
            ProcessTaskDispatcher::new(
                self.config.worker.num_worker_processes.max(1),
                self.config.worker.worker_program.clone(),
                self.config.worker.worker_cancel_grace_ms,
                self.config.payload_signing_key.clone(),
                self.config.worker.python_path.clone(),
            )
            .map_err(|e| ActantError::Config(format!("failed to create task dispatcher: {}", e)))?,
        );

        // ── Capability 注册 ────────────────────────────────────────────
        // 关键顺序：先 register_defaults + register_store_handler（chain 追加），
        // 再 bind_actor_system，确保 CapabilityActor spawn 时持有最新 handlers。
        tracing::info!("build: capability register enter");
        let capability = Arc::new(CapabilityRuntime::new());
        register_defaults(&capability);
        register_serialization_handler(&capability)?;
        register_store_handler(&capability, lmdb_store.clone())?;
        Arc::clone(&capability)
            .bind_actor_system(actor_system.clone())
            .await;
        tracing::info!("build: capability register done");

        // ── WorkflowActor ──────────────────────────────────────────────
        // Orchestrator 状态由 WorkflowActor 独占；on_start 启动 timeout/persist 循环。
        tracing::info!("build: workflow actor spawn enter");
        let workflow_actor_id = crate::common::ActorId::workflow(&self.node_id);
        let orchestrator =
            init_orchestrator(self.data_dir.as_deref(), &self.node_id, &self.config).await?;
        // P0-5 接线：recover 完成后立即重建"Pending 且依赖已满足"的任务。
        // 此处 WorkflowActor 尚未 spawn，Orchestrator 状态仍为独占引用，读取安全；
        // 返回的任务在 init_worker 产出调度器后重新入队（见下方 enqueue_batch）。
        // 若不接线，重启前处于可执行状态的任务会永久滞留。
        let recovered_ready_tasks = orchestrator.recover_ready_tasks();
        // init_orchestrator 返回的 Arc 仅此一处引用，try_unwrap 必成功。
        let orchestrator = match Arc::try_unwrap(orchestrator) {
            Ok(o) => o,
            Err(_) => {
                return Err(ActantError::Internal(
                    "init_orchestrator returned shared Arc".into(),
                ))
            }
        };
        // B2：注入网络传输层，使工作流级硬超时监控可主动广播 CancelBroadcast。
        let orchestrator = orchestrator.with_network(network.clone());
        actor_system
            .spawn(
                workflow_actor_id.clone(),
                crate::runtime::workflow::WorkflowActor::new(orchestrator),
            )
            .await?;
        tracing::info!("build: workflow actor spawned");

        // ── FailoverActor ──────────────────────────────────────────────
        // 接管心跳、故障检测、租约维护。start_background_loops 启动后台循环。
        tracing::info!("build: failover actor spawn enter");
        let failover = Arc::new(FailoverManager::new(
            self.node_id.clone(),
            network.clone(),
            actor_system.clone(),
            workflow_actor_id.clone(),
        ));
        let failover_actor_id = ActorId::failover(&self.node_id);
        actor_system
            .spawn(
                failover_actor_id.clone(),
                FailoverActor::new(failover.clone()),
            )
            .await?;
        tracing::info!("build: failover actor spawned");

        // ── DagGossipActor ─────────────────────────────────────────────
        // 跨节点 DAG 状态反熵同步。
        tracing::info!("build: dag gossip actor spawn enter");
        let gossip = DagGossip::new(
            network.clone(),
            actor_system.clone(),
            workflow_actor_id.clone(),
            self.config.gossip.clone(),
        );
        let dag_gossip_actor_id = ActorId::dag_gossip(&self.node_id);
        actor_system
            .spawn(dag_gossip_actor_id.clone(), DagGossipActor::new(gossip))
            .await?;
        tracing::info!("build: dag gossip actor spawned");

        // ── 构造 Runtime 主体 ──────────────────────────────────────────
        let mut runtime = Arc::new(ActantRuntime::new(
            self.node_id.clone(),
            self.config.clone(),
            network.clone(),
            store.clone(),
            actor_system.clone(),
            workflow_actor_id.clone(),
            None, // worker 稍后注入
            capability.clone(),
            event_bus.clone(),
            task_dispatcher.clone(),
            Some(blob_store),
        ));
        runtime.subscribe_cancel().await?;
        failover.subscribe_topics().await?;

        // ── CapabilityGossip ──────────────────────────────────────────
        // 跨节点 capability 元信息扩散。直接启动后台广播循环，
        // 不通过 ActorSystem（无需消息路由）。cancel handle 注册到 Runtime，
        // 保证 shutdown 时停止。
        // 必须先订阅 topic 再启动后台循环，确保 NetworkEventRouter
        // 接收侧就绪后 gossip 消息才到达。
        tracing::info!("build: capability gossip subscribe enter");
        let cap_gossip_topic = Topic::from(crate::common::wire::constants::TOPIC_CAPABILITY_GOSSIP);
        network
            .subscribe(cap_gossip_topic.as_str())
            .await
            .map_err(|e| {
                ActantError::Network(format!(
                    "failed to subscribe to capability gossip topic: {}",
                    e
                ))
            })?;
        tracing::info!("build: capability gossip subscribed");
        let cap_gossip = Arc::new(
            CapabilityGossipActor::new(self.node_id.clone(), capability.clone(), network.clone())
                .with_broadcast_interval(Duration::from_millis(
                    self.config.network.capability_gossip_interval_ms,
                )),
        );
        runtime.register_background_loop_cancel(cap_gossip.clone().start_background_loop());
        tracing::info!("build: capability gossip started");

        // ── Worker（SchedulerActor + 任务消费循环）─────────────────────
        // 需要 failover 引用以便 capacity callback。tokio handle 取当前运行时。
        tracing::info!("build: init_worker enter");
        let tokio_handle = tokio::runtime::Handle::current();
        let worker = init_worker(WorkerInitParams {
            node_id: &self.node_id,
            network: &network,
            event_bus: event_bus.clone(),
            scheduler_kind: self.config.worker.scheduler_kind.as_str(),
            worker_config: &self.config.worker,
            actor_system: actor_system.clone(),
            task_dispatcher: task_dispatcher.clone(),
            failover: &failover,
            tokio_handle,
            workflow_actor_id: Some(workflow_actor_id.clone()),
            dag_gossip_actor_id: Some(dag_gossip_actor_id.clone()),
            capability_gossip: Some(cap_gossip.clone()),
        })
        .await?;
        tracing::info!("build: init_worker done");
        // Worker 持有 Arc<dyn Scheduler>，让 failover 也能拿到 scheduler 引用。
        let worker = Arc::new(worker.with_failover_manager(failover.clone()));
        // 注入 worker 到 runtime。此处 runtime 仍是唯一 Arc 引用
        //（register_background_loop_cancel 只借用 &self，未 clone Arc），
        // 故 Arc::get_mut 可直接获取可变引用，无需克隆（克隆会
        // 引用计数变为 2，get_mut 永远返回 None → worker 从未注入。
        if let Some(rt) = Arc::get_mut(&mut runtime) {
            rt.set_worker(worker.clone());
        } else {
            tracing::warn!("build: runtime Arc has multiple owners, worker not injected");
        }
        // failover 拿到 scheduler（worker 的 actor_scheduler 句柄）。
        failover.set_scheduler(worker.scheduler_clone());
        // P0-5 接线（续）：把恢复出的 ready 任务重新入队。经 SchedulerActor 的
        // enqueue 快路径进入调度器，Worker 主循环随后按正常流程执行；
        // 终态任务不在此列（recover_ready_tasks 仅返回 Pending 且依赖已满足者）。
        if !recovered_ready_tasks.is_empty() {
            let count = recovered_ready_tasks.len();
            let scheduler = worker.scheduler_clone();
            if let Err(e) = scheduler.enqueue_batch(recovered_ready_tasks).await {
                tracing::error!(
                    count,
                    error = %e,
                    "failed to re-enqueue recovered ready tasks; they will remain pending"
                );
            } else {
                tracing::info!(count, "re-enqueued recovered ready tasks after restart");
            }
        }
        // 所有依赖就绪后再启动心跳/故障检测后台循环（消耗 failover Arc）。
        // cancel handle 注册到 Runtime，保证 shutdown 时停止。
        tracing::info!("build: failover start_background_loops enter");
        runtime.register_background_loop_cancel(failover.start_background_loops());
        tracing::info!("build: failover start_background_loops done");

        tracing::info!("build: returning runtime");
        Ok(runtime)
    }
}

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/builder.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/workflow/wiring.rs"]
mod wiring_tests;
