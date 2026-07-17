//! 运行时构建器。
//!
//! 负责根据配置顺序构造 network → store → actor → workflow → capability 各子系统。
//! 此处只包含不依赖 PyO3 的纯 Rust 初始化逻辑；Python callable 的注册保留在 `src/py/`。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::common::scheduler_kind;
use crate::common::{ActantConfig, ActantError, ActorId, NodeId};
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

/// 初始化 actor 系统，可选持久化。
pub fn init_actor_system(
    data_dir: Option<&str>,
    node_id: &NodeId,
    network: &Arc<dyn Transport>,
    event_bus: &EventBus,
) -> Result<Arc<ActorSystem>, ActantError> {
    if let Some(dir) = data_dir {
        let db_path = Path::new(dir).join("actor");
        std::fs::create_dir_all(&db_path).map_err(ActantError::StorageIo)?;
        let store = LmdbStore::open(&db_path)
            .map_err(|e| ActantError::Storage(format!("failed to open actor store: {}", e)))?;
        let checkpoint = CheckpointManager::new(store.clone());
        let wal_path = Path::new(dir).join("actor.wal");
        let wal_writer = WalWriter::open_with_sync(&wal_path, true)
            .map_err(|e| ActantError::Storage(format!("failed to open actor WAL: {}", e)))?;
        Ok(Arc::new(
            ActorSystem::new()
                .with_node_id(node_id.clone())
                .with_network(network.clone())
                .with_event_bus(event_bus.clone())
                .with_wal(wal_writer, store)
                .with_checkpoint(checkpoint),
        ))
    } else {
        Ok(Arc::new(
            ActorSystem::new()
                .with_node_id(node_id.clone())
                .with_network(network.clone())
                .with_event_bus(event_bus.clone()),
        ))
    }
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
}

/// 初始化 worker runtime。
///
/// 调度器被封装为 [`SchedulerActor`] 并在 `actor_system` 中 spawn；
/// `Worker` 通过 [`ActorScheduler`] 客户端与 Actor 交互，
/// 调度状态由 Actor 持有。
pub async fn init_worker(params: WorkerInitParams<'_>) -> Result<Worker, ActantError> {
    let scheduler_actor_id = ActorId::scheduler(params.node_id);
    // 注入 EventBus 到 SchedulerActor：Actor 在 enqueue 后发布 TaskEnqueued 事件，
    // Worker 订阅该 topic 实现事件驱动唤醒。
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
    params
        .actor_system
        .spawn(scheduler_actor_id.clone(), scheduler_actor)
        .await?;

    let actor_scheduler = Arc::new(ActorScheduler::new(
        scheduler_actor_id.clone(),
        params.actor_system.clone(),
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
    Ok(match params.dag_gossip_actor_id {
        Some(id) => worker.with_dag_gossip_actor_id(id),
        None => worker,
    })
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

        // SE2: 空 payload_signing_key 会禁用 payload 完整性验证，仅适用于开发/测试。
        // 生产环境必须配置非空密钥，否则恶意节点可投递伪造 cloudpickle payload。
        if self.config.payload_signing_key.is_empty() {
            tracing::warn!(
                "payload_signing_key is empty — payload integrity protection DISABLED. \
                 This is unsafe for production: configure a non-empty shared secret."
            );
        }

        tracing::info!("build: NetworkManager::new enter");
        let network: Arc<dyn Transport> = Arc::new(
            NetworkManager::new(self.node_id.clone(), self.config.network.clone())
                .await
                .map_err(|e| ActantError::Network(format!("network init failed: {}", e)))?,
        );
        tracing::info!("build: NetworkManager::new done");

        let event_bus = EventBus::new();

        tracing::info!("build: init_actor_system enter");
        let actor_system = init_actor_system(
            self.data_dir.as_deref(),
            &self.node_id,
            &network,
            &event_bus,
        )?;
        tracing::info!("build: init_actor_system done");

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
        tracing::info!("build: capability gossip start enter");
        let cap_gossip = Arc::new(
            CapabilityGossipActor::new(self.node_id.clone(), capability.clone(), network.clone())
                .with_broadcast_interval(Duration::from_millis(
                    self.config.network.capability_gossip_interval_ms,
                )),
        );
        runtime.register_background_loop_cancel(cap_gossip.start_background_loop());
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
