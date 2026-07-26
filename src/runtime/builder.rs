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
use crate::runtime::dispatcher::{TaskDispatcher, TaskRegistry};
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
pub fn validate_data_dir(data_dir: &str) -> Result<(), ActantError> {
    let trimmed = data_dir.trim();
    if trimmed.is_empty() {
        return Err(ActantError::Config("data_dir must not be empty".into()));
    }
    let candidate = Path::new(trimmed);
    if let Ok(canon) = candidate.canonicalize() {
        for forbidden in FORBIDDEN_SYSTEM_DIRS {
            if let Ok(canon_forbidden) = Path::new(forbidden).canonicalize() {
                if canon == canon_forbidden {
                    return Err(ActantError::Config(format!(
                        "data_dir must not point at system directory {}",
                        forbidden
                    )));
                }
            }
        }
    }
    Ok(())
}

/// `init_actor_system` 的返回值：actor 系统 + A2 跨节点路由组件。
///
/// 元组拆开使用，避免在每个调用点重复长类型签名。
pub type ActorSystemWithRouter = (
    Arc<ActorSystem>,
    Arc<crate::runtime::actor::router::ActorRegistry>,
    Arc<dyn crate::runtime::actor::router::ActorRouter>,
);

/// 初始化 actor 系统，可选持久化。
///
/// 注入 A2 跨节点路由组件：[`ActorRegistry`] 与 [`ActorRouter`]。
/// `actor_router_strategy` 配置项决定具体策略实现（默认 `round-robin`）。
/// Registry 在 `ActorSystem::spawn` 时自动注册本地 actor 类型，由调用方
/// （`RuntimeBuilder::build`）启动 [`ActorRegistryGossipActor`] 后台广播。
pub fn init_actor_system(
    data_dir: Option<&str>,
    node_id: &NodeId,
    network: &Arc<dyn Transport>,
    event_bus: &EventBus,
    config: &crate::common::ActantConfig,
) -> Result<ActorSystemWithRouter, ActantError> {
    // A2：构造跨节点 actor 注册表 + 路由器。registry 在 spawn 时自动登记本地 actor 类型。
    let registry = Arc::new(
        crate::runtime::actor::router::ActorRegistry::new().with_local_node_id(node_id.clone()),
    );
    let strategy = crate::runtime::actor::router::RouterStrategy::parse(
        &config.network.actor_router_strategy,
    )?;
    let router = crate::runtime::actor::router::make_router(strategy, registry.clone());

    let base = ActorSystem::new()
        .with_node_id(node_id.clone())
        .with_network(network.clone())
        .with_event_bus(event_bus.clone())
        .with_actor_registry(registry.clone())
        .with_actor_router(router.clone());
    let system = if let Some(dir) = data_dir {
        let db_path = Path::new(dir).join("actor");
        std::fs::create_dir_all(&db_path).map_err(ActantError::StorageIo)?;
        let store = LmdbStore::open(&db_path)
            .map_err(|e| ActantError::Storage(format!("failed to open actor store: {}", e)))?;
        let checkpoint = CheckpointManager::new(store.clone());
        let wal_path = Path::new(dir).join("actor.wal");
        let wal_writer = WalWriter::open_with_sync(&wal_path, true)
            .map_err(|e| ActantError::Storage(format!("failed to open actor WAL: {}", e)))?;
        Arc::new(base.with_wal(wal_writer, store).with_checkpoint(checkpoint))
    } else {
        Arc::new(base)
    };
    Ok((system, registry, router))
}

/// 初始化 orchestrator，可选持久化。
pub async fn init_orchestrator(
    data_dir: Option<&str>,
    node_id: &NodeId,
    config: &ActantConfig,
) -> Result<Arc<Orchestrator>, ActantError> {
    if let Some(dir) = data_dir {
        let db_path = Path::new(dir).join("orchestrator");
        std::fs::create_dir_all(&db_path).map_err(ActantError::StorageIo)?;
        let store = LmdbStore::open(&db_path)
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
    /// Actor 注册表 gossip 处理器（A2）。用于接收 `TOPIC_ACTOR_REGISTRY` 上的远端注册表广播。
    pub actor_registry_gossip: Option<Arc<crate::runtime::actor::router::ActorRegistryGossipActor>>,
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
            SchedulerActor::with_event_bus(params.event_bus.clone()).with_priority()
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
    let worker = match params.actor_registry_gossip {
        Some(g) => worker.with_actor_registry_gossip(g),
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
        crate::common::set_wire_signing_key(self.config.payload_signing_key.clone());

        tracing::info!("build: NetworkManager::new enter");
        let network: Arc<dyn Transport> = Arc::new(
            NetworkManager::new(self.node_id.clone(), self.config.network.clone())
                .await
                .map_err(|e| ActantError::Network(format!("network init failed: {}", e)))?,
        );
        tracing::info!("build: NetworkManager::new done");

        let event_bus = EventBus::new();

        tracing::info!("build: init_actor_system enter");
        let (actor_system, actor_registry, _actor_router) = init_actor_system(
            self.data_dir.as_deref(),
            &self.node_id,
            &network,
            &event_bus,
            &self.config,
        )?;
        tracing::info!("build: init_actor_system done");

        // 有持久化存储时启动 WAL compaction 后台任务，由 Runtime::shutdown 停止。
        if self.data_dir.is_some() {
            actor_system.start_compaction_task();
        }

        let data_dir = self
            .data_dir
            .clone()
            .ok_or_else(|| ActantError::Config("RuntimeBuilder requires data_dir".into()))?;
        let store_path = Path::new(&data_dir).join("store");
        std::fs::create_dir_all(&store_path).map_err(ActantError::StorageIo)?;
        let lmdb_store = LmdbStore::open(&store_path)
            .map_err(|e| ActantError::Storage(format!("failed to open store: {}", e)))?;
        let store = Store::new(lmdb_store.clone());

        let task_dispatcher: Arc<dyn TaskDispatcher> = TaskRegistry::with_drain_timeout(
            self.config.worker.task_thread_pool_workers.max(1),
            self.config.worker.task_thread_pool_channel_capacity.max(1),
            self.config.payload_signing_key.clone(),
            std::time::Duration::from_secs(self.config.worker.drain_timeout_secs.max(1)),
        )
        .map_err(|e| ActantError::Config(format!("failed to create task dispatcher: {}", e)))?
        .into_dispatcher();

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
        ));
        runtime.subscribe_cancel().await?;
        failover.subscribe_topics().await?;

        // ── CapabilityGossip ──────────────────────────────────────────
        // 跨节点 capability 元信息扩散。直接启动后台广播循环，
        // 不通过 ActorSystem（无需消息路由）。cancel handle 注册到 Runtime，
        // 保证 shutdown 时停止。
        // 必须先订阅 topic 再启动后台循环（与 ActorRegistryGossip 一致），
        // 确保 NetworkEventRouter 接收侧就绪后 gossip 消息才到达。
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

        // ── ActorRegistryGossip（A2）──────────────────────────────────
        // 跨节点 actor 注册表同步：广播本地 actor 类型集合，接收远端注册表。
        // gossip actor 通过 NetworkEventRouter 接收 `TOPIC_ACTOR_REGISTRY` 上的消息，
        // 因此必须先订阅此 topic，再启动后台广播循环（保证接收侧就绪）。
        tracing::info!("build: actor registry gossip subscribe enter");
        let actor_registry_topic = Topic::actor_registry();
        network
            .subscribe(actor_registry_topic.as_str())
            .await
            .map_err(|e| {
                ActantError::Network(format!(
                    "failed to subscribe to actor registry topic: {}",
                    e
                ))
            })?;
        tracing::info!("build: actor registry gossip subscribed");
        let actor_registry_gossip = Arc::new(
            crate::runtime::actor::router::ActorRegistryGossipActor::new(
                self.node_id.clone(),
                actor_registry.clone(),
                network.clone(),
            )
            .with_broadcast_interval(Duration::from_millis(
                self.config.network.actor_registry_gossip_interval_ms,
            )),
        );
        // 启动时立即广播一次：让对端尽快发现本节点承载的 actor 类型，
        // 不必等待第一个 tick（默认 30s）。
        if let Err(e) = actor_registry_gossip.broadcast_registry().await {
            tracing::warn!(error = %e, "initial actor registry broadcast failed");
        }
        runtime
            .register_background_loop_cancel(actor_registry_gossip.clone().start_background_loop());
        tracing::info!("build: actor registry gossip started");

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
            actor_registry_gossip: Some(actor_registry_gossip.clone()),
            capability_gossip: Some(cap_gossip.clone()),
        })
        .await?;
        tracing::info!("build: init_worker done");
        // Worker 持有 Arc<dyn Scheduler>，让 failover 也能拿到 scheduler 引用。
        let worker = Arc::new(worker.with_failover_manager(failover.clone()));
        // 注入 worker 到 runtime。此处 runtime 仍是唯一 Arc 引用
        //（register_background_loop_cancel 只借用 &self，未 clone Arc），
        // 故 Arc::get_mut 可直接获取可变引用。此前用 runtime.clone() 会使
        // 引用计数变为 2，get_mut 永远返回 None → worker 从未注入。
        if let Some(rt) = Arc::get_mut(&mut runtime) {
            rt.set_worker(worker.clone());
        } else {
            tracing::warn!("build: runtime Arc has multiple owners, worker not injected");
        }
        // failover 拿到 scheduler（worker 的 actor_scheduler 句柄）。
        failover.set_scheduler(worker.scheduler_clone());
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
