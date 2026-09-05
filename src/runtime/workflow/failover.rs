//! 基于心跳与租约的工作流故障转移。
//!
//! [`FailoverManager`] 维护远端节点心跳、可用槽位、活跃 workflow 列表和本地租约。
//! 当节点被判定失效时，所有健康节点使用一致性哈希
//! [`should_claim_workflow`] 对 workflow 归属做
//! 确定性选择，只有获选节点会 claim 并重新调度该 workflow 的运行中任务。
//!
//! ## 时间参数
//!
//! [`FailoverConfig`] 要求
//! `heartbeat_interval_ms < failure_timeout_ms < lease_duration_ms`。这个关系保证：
//! - 节点有足够心跳机会，不会因为单次延迟就被误判；
//! - 故障判定完成前旧租约不会先过期；
//! - claim 后的租约有明确过期时间，避免永久双主。
//!
//! ## 持久化
//!
//! 新 claim 的租约同时记录墙钟时间和单调 deadline；从 store 恢复的租约没有
//! 单调基线，只能回退到墙钟过期判断。
//!
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::common::{
    should_claim_workflow, ActorId, FailoverConfig, NodeHeartbeat, NodeId, OrchestratorClaim,
    Result, WireEnvelope, WireMessage, WorkflowId, STORE_KEY_LEASE, TOPIC_FAILOVER, TOPIC_HEADS,
    TOPIC_HEARTBEAT,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::state::{HybridLogicalClock, LmdbStore};
use crate::runtime::workflow::actor::workflow_methods;
use crate::runtime::workflow::messaging;

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
struct PersistedLease {
    node_id: NodeId,
    claimed_at_ms: u64,
    expires_at_ms: u64,
}

struct PeerState {
    last_heartbeat_ms: u64,
    active_workflows: HashSet<WorkflowId>,
    available_slots: u32,
    max_slots: u32,
    endpoint_addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub last_heartbeat_ms: u64,
    pub active_workflows: HashSet<WorkflowId>,
    pub available_slots: u32,
    pub max_slots: u32,
    pub endpoint_addr: Option<String>,
}

impl From<&PeerState> for PeerInfo {
    fn from(state: &PeerState) -> Self {
        Self {
            last_heartbeat_ms: state.last_heartbeat_ms,
            active_workflows: state.active_workflows.clone(),
            available_slots: state.available_slots,
            max_slots: state.max_slots,
            endpoint_addr: state.endpoint_addr.clone(),
        }
    }
}

struct LeaseEntry {
    node_id: NodeId,
    claimed_at_ms: u64,
    expires_at_ms: u64,
    /// 单调时钟租约到期时刻。``Some`` 时优先用于过期判定，避免 NTP 时钟跳变
    /// 导致误判（M5-2 改进）。``None`` 表示租约从持久化恢复（无单调基线），
    /// 回退到墙钟 ``expires_at_ms`` 比较。
    deadline: Option<std::time::Instant>,
}

impl LeaseEntry {
    /// 租约是否仍有效。
    ///
    /// 优先使用单调时钟 ``deadline``（若存在），否则回退到墙钟比较。
    /// ``now_ms`` 为当前墙钟毫秒，``now_monotonic`` 为当前单调时刻。
    fn is_valid(&self, now_ms: u64, now_monotonic: std::time::Instant) -> bool {
        match self.deadline {
            Some(deadline) => now_monotonic < deadline,
            None => now_ms < self.expires_at_ms,
        }
    }

    /// 构造一个带单调 deadline 的新租约（用于本进程新 claim 的租约）。
    fn new_with_monotonic(
        node_id: NodeId,
        claimed_at_ms: u64,
        expires_at_ms: u64,
        lease_duration_ms: u64,
    ) -> Self {
        Self {
            node_id,
            claimed_at_ms,
            expires_at_ms,
            deadline: std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(lease_duration_ms)),
        }
    }

    /// 构造一个从持久化恢复的租约（无单调基线，回退墙钟比较）。
    fn restored(node_id: NodeId, claimed_at_ms: u64, expires_at_ms: u64) -> Self {
        Self {
            node_id,
            claimed_at_ms,
            expires_at_ms,
            deadline: None,
        }
    }
}

impl Clone for LeaseEntry {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            claimed_at_ms: self.claimed_at_ms,
            expires_at_ms: self.expires_at_ms,
            deadline: self.deadline,
        }
    }
}

/// 维护节点健康状态、workflow 租约和故障接管决策。
///
/// `FailoverManager` 通过 heartbeat topic 收集 peer 的活跃 workflow 与容量视图；
/// 当 peer 超过 `failure_timeout_ms` 未更新时，本节点会按一致性哈希判断自己是否
/// 应接管该 peer 的每个 workflow。接管流程先通知本地 `WorkflowActor` adopt 状态，
/// 再持久化租约并广播 claim。
pub struct FailoverManager {
    node_id: NodeId,
    network: Arc<dyn crate::runtime::network::Transport>,
    actor_system: Arc<ActorSystem>,
    workflow_actor_id: ActorId,
    scheduler: parking_lot::Mutex<Option<Arc<dyn crate::runtime::workflow::Scheduler>>>,
    peers: Arc<DashMap<NodeId, PeerState>>,
    heartbeat_interval_ms: u64,
    failure_timeout_ms: u64,
    lease_duration_ms: u64,
    lease_expiry_check_interval_secs: u64,
    leases: Arc<DashMap<WorkflowId, LeaseEntry>>,
    store: Option<LmdbStore>,
    clock: Arc<HybridLogicalClock>,
    /// Local node's available task capacity (updated by the worker).
    local_available_capacity: Arc<AtomicU32>,
    /// Local node's maximum task capacity.
    local_max_capacity: Arc<AtomicU32>,
}

impl Drop for FailoverManager {
    fn drop(&mut self) {
        tracing::debug!("FailoverManager::drop");
    }
}

impl FailoverManager {
    /// 使用默认 failover 配置创建 manager。
    pub fn new(
        node_id: NodeId,
        network: Arc<dyn crate::runtime::network::Transport>,
        actor_system: Arc<ActorSystem>,
        workflow_actor_id: ActorId,
    ) -> Self {
        let config = FailoverConfig::default();
        Self::with_config(
            node_id,
            network,
            actor_system,
            workflow_actor_id,
            config,
            None,
        )
    }

    pub(crate) fn with_config(
        node_id: NodeId,
        network: Arc<dyn crate::runtime::network::Transport>,
        actor_system: Arc<ActorSystem>,
        workflow_actor_id: ActorId,
        config: FailoverConfig,
        store: Option<LmdbStore>,
    ) -> Self {
        let fm = Self {
            node_id,
            network,
            actor_system,
            workflow_actor_id,
            scheduler: parking_lot::Mutex::new(None),
            peers: Arc::new(DashMap::new()),
            heartbeat_interval_ms: config.heartbeat_interval_ms,
            failure_timeout_ms: config.failure_timeout_ms,
            lease_duration_ms: config.lease_duration_ms,
            lease_expiry_check_interval_secs: config.lease_expiry_check_interval_secs,
            leases: Arc::new(DashMap::new()),
            store,
            clock: Arc::new(HybridLogicalClock::new()),
            local_available_capacity: Arc::new(AtomicU32::new(0)),
            local_max_capacity: Arc::new(AtomicU32::new(0)),
        };
        fm.recover_leases_from_store();
        fm
    }

    /// 向 WorkflowActor 发起调用。
    async fn call_workflow<T: serde::Serialize>(
        &self,
        method: &str,
        payload: T,
    ) -> crate::common::Result<crate::common::ActorMessageResult> {
        let bytes = messaging::encode(&payload)?;
        self.actor_system
            .call(&self.workflow_actor_id, method, bytes)
            .await
            .map_err(|e| crate::common::ActantError::Actor(e.to_string()))
    }

    /// 调用无返回值的 WorkflowActor 方法，仅检查错误。
    async fn call_workflow_void<T: serde::Serialize>(
        &self,
        method: &str,
        payload: T,
    ) -> crate::common::Result<()> {
        let result = self.call_workflow(method, payload).await?;
        if let Some(err) = result.error {
            Err(crate::common::ActantError::from(err))
        } else {
            Ok(())
        }
    }

    /// 获取活跃 workflow ID 列表。
    async fn active_workflow_ids(&self) -> crate::common::Result<Vec<WorkflowId>> {
        let result = self
            .call_workflow(workflow_methods::ACTIVE_WORKFLOW_IDS, ())
            .await?;
        messaging::decode(&result.payload)
    }

    /// 从本地编排器接管指定 workflow。
    async fn adopt_workflow(&self, workflow_id: &WorkflowId) -> crate::common::Result<()> {
        self.call_workflow_void(workflow_methods::ADOPT_WORKFLOW, workflow_id)
            .await
    }

    /// 从本地编排器中移除指定 workflow（仅内存状态）。
    async fn remove_active_workflow(&self, workflow_id: &WorkflowId) -> crate::common::Result<()> {
        self.call_workflow_void(workflow_methods::REMOVE_ACTIVE_WORKFLOW, workflow_id)
            .await
    }

    /// 覆盖心跳发送间隔。
    ///
    /// 主要供测试或特殊部署调参使用；生产配置通常来自
    /// [`FailoverConfig`]。
    pub fn with_heartbeat_interval(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = ms;
        self
    }

    /// 设置本节点当前可用容量快照。
    ///
    /// Worker 会通过容量回调持续更新该值，心跳广播会携带它供远端路由决策使用。
    pub fn with_capacity(self, available: u32, max: u32) -> Self {
        self.local_available_capacity
            .store(available, Ordering::Relaxed);
        self.local_max_capacity.store(max, Ordering::Relaxed);
        self
    }

    /// 注入用于 failover 重调度的 scheduler。
    pub fn set_scheduler(&self, scheduler: Arc<dyn crate::runtime::workflow::Scheduler>) {
        *self.scheduler.lock() = Some(scheduler);
    }

    /// 订阅 failover 相关 gossip topic。
    ///
    /// # Errors
    ///
    /// 如果底层网络订阅任一 topic 失败，返回错误。
    pub async fn subscribe_topics(&self) -> Result<()> {
        self.network.subscribe(TOPIC_HEARTBEAT).await?;
        self.network.subscribe(TOPIC_FAILOVER).await?;
        self.network.subscribe(TOPIC_HEADS).await?;
        self.network
            .subscribe(crate::common::TOPIC_DAG_STATE)
            .await?;
        Ok(())
    }

    /// 广播本节点心跳、活跃 workflow 与可用容量。
    ///
    /// # Errors
    ///
    /// 如果查询本地 workflow 状态、序列化 heartbeat 或网络广播失败，返回错误。
    pub async fn send_heartbeat(&self) -> Result<()> {
        let active_workflows = self.active_workflow_ids().await?;
        let now_ms = crate::common::epoch_millis();
        let endpoint_addr = self
            .network
            .listen_addresses()
            .ok()
            .map(|a| a.endpoint_addr);
        let hb = NodeHeartbeat {
            node_id: self.node_id.clone(),
            active_workflows,
            timestamp_ms: now_ms,
            available_slots: self.local_available_capacity.load(Ordering::Relaxed),
            max_slots: self.local_max_capacity.load(Ordering::Relaxed),
            endpoint_addr,
        };
        let msg = WireMessage::NodeHeartbeat(hb);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg))
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        tracing::debug!(
            "sending heartbeat from {} to topic {}",
            self.node_id.0,
            TOPIC_HEARTBEAT
        );
        let result = self.network.broadcast(TOPIC_HEARTBEAT, data).await;
        if let Err(ref e) = result {
            tracing::warn!("heartbeat broadcast failed: {}", e);
        }
        if result.is_ok() {
            crate::metrics::inc_heartbeats_sent();
        }
        result
    }

    pub fn get_peer_infos(&self) -> HashMap<NodeId, PeerInfo> {
        self.peers
            .iter()
            .map(|ref_multi| (ref_multi.key().clone(), PeerInfo::from(ref_multi.value())))
            .collect()
    }

    pub fn remove_peer(&self, node_id: &NodeId) {
        self.peers.remove(node_id);
    }

    /// Remove peers whose last heartbeat exceeds the failure timeout and return their info.
    /// Also decrements the connected_peers gauge for each removed peer.
    pub fn expire_stale_peers(&self) -> Vec<(NodeId, PeerInfo)> {
        let now_ms = crate::common::epoch_millis();
        let timeout_ms = self.failure_timeout_ms;
        let stale: Vec<NodeId> = self
            .peers
            .iter()
            .filter(|ref_multi| {
                let state = ref_multi.value();
                state.last_heartbeat_ms > 0
                    && now_ms.saturating_sub(state.last_heartbeat_ms) > timeout_ms
            })
            .map(|ref_multi| ref_multi.key().clone())
            .collect();
        let mut removed = Vec::new();
        for node_id in &stale {
            if let Some((_, state)) = self.peers.remove(node_id) {
                crate::metrics::dec_connected_peers();
                removed.push((node_id.clone(), PeerInfo::from(&state)));
            }
        }
        removed
    }

    /// 声明本节点接管指定 workflow。
    ///
    /// # Errors
    ///
    /// 如果本地 `WorkflowActor` adopt 失败、租约持久化失败、claim 序列化失败
    /// 或网络广播失败，返回错误。
    pub async fn claim_workflow(&self, workflow_id: &WorkflowId) -> Result<()> {
        let now_ms = crate::common::epoch_millis();
        let now_monotonic = std::time::Instant::now();
        let lease_duration_ms = self.lease_duration_ms;
        if let Some(existing) = self.leases.get(workflow_id) {
            if existing.is_valid(now_ms, now_monotonic) {
                if existing.node_id == self.node_id {
                    return Ok(());
                }
                // 租约仍有效且不属于本节点：直接退让，不通过字典序抢占。
                // 故障转移的仲裁统一由调用方 `should_claim_workflow`（一致性哈希）决定，
                // 避免两种策略互相矛盾导致脑裂。
                tracing::info!(
                    "workflow {} already claimed by {} with valid lease, deferring",
                    workflow_id.0,
                    existing.node_id.0
                );
                return Ok(());
            }
        }
        let lease = LeaseEntry::new_with_monotonic(
            self.node_id.clone(),
            now_ms,
            now_ms + lease_duration_ms,
            lease_duration_ms,
        );

        // 先 adopt workflow，成功后再持久化 lease，避免 adopt 失败但 lease 已写入
        self.adopt_workflow(workflow_id).await?;

        self.persist_lease(workflow_id, &lease)?;
        self.leases.insert(workflow_id.clone(), lease);

        crate::metrics::inc_failover_claims();

        let claim = OrchestratorClaim {
            node_id: self.node_id.clone(),
            workflow_id: workflow_id.clone(),
            timestamp_ms: now_ms,
        };
        let msg = WireMessage::OrchestratorClaim(claim);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg))
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        self.network.broadcast(TOPIC_FAILOVER, data).await
    }

    /// 处理远端节点广播的 workflow claim。
    ///
    /// 远端 claim 会更新本地租约表；如果 claim 不属于本节点，本地会移除该 workflow
    /// 的 active 状态以避免双主推进。
    pub async fn handle_claim(&self, claim: &OrchestratorClaim) {
        let lease_duration_ms = self.lease_duration_ms;

        // 使用 claimant 的时间戳作为基准，使租期时长
        // 在所有节点上一致，不受时钟偏差 / 网络
        // latency 影响。若使用接收方本地时间，
        // 会因网络传输时间而缩短租期。
        // 单调 deadline 以本节点接收时刻为起点，避免 NTP 跳变误判（M5-2）。
        let lease = LeaseEntry::new_with_monotonic(
            claim.node_id.clone(),
            claim.timestamp_ms,
            claim.timestamp_ms + lease_duration_ms,
            lease_duration_ms,
        );
        if let Err(e) = self.persist_lease(&claim.workflow_id, &lease) {
            tracing::error!("failed to persist lease for {}: {}", claim.workflow_id.0, e);
        }
        self.leases.insert(claim.workflow_id.clone(), lease);

        if claim.node_id != self.node_id {
            if let Err(e) = self.remove_active_workflow(&claim.workflow_id).await {
                tracing::warn!(error = %e, workflow_id = %claim.workflow_id.as_str(), "failover: failed to remove active workflow");
            }
            tracing::info!(
                "node {} claimed workflow {}, removed from local active set",
                claim.node_id.0,
                claim.workflow_id.0
            );
        }
    }

    pub async fn reschedule_workflow_tasks(&self, workflow_id: &WorkflowId) -> Result<()> {
        let result = self
            .call_workflow(workflow_methods::RESCHEDULE_RUNNING_TASKS, workflow_id)
            .await?;
        let tasks: Vec<crate::common::TaskDefinition> = messaging::decode(&result.payload)?;

        for task_def in &tasks {
            crate::metrics::inc_failover_reschedules();

            let hlc_ts = self.clock.tick();
            let update = crate::common::WireDagStateUpdate {
                workflow_id: workflow_id.clone(),
                task_id: task_def.id.clone(),
                task_state: crate::common::WireTaskState::Failed {
                    error: "original orchestrator failed, rescheduling".into(),
                },
                hlc_timestamp: hlc_ts,
                origin_node: self.node_id.clone(),
            };
            let msg = WireMessage::DagStateUpdate(update);
            if let Ok(data) = postcard::to_allocvec(&WireEnvelope::wrap(msg)) {
                if self
                    .network
                    .broadcast(crate::common::TOPIC_DAG_STATE, data)
                    .await
                    .is_err()
                {
                    tracing::warn!("failed to broadcast dag state update");
                }
            }

            // 通过 scheduler 入队；不可用时记录警告
            let sched_opt = self.scheduler.lock().clone();
            if let Some(sched) = sched_opt {
                if let Err(e) = sched.enqueue(task_def.clone()).await {
                    tracing::warn!(
                        "scheduler rejected rescheduled task {}/{}: {}",
                        workflow_id.0,
                        task_def.id.0,
                        e
                    );
                }
            } else {
                tracing::warn!(
                    "no scheduler set on FailoverManager, cannot enqueue rescheduled task {}/{}",
                    workflow_id.0,
                    task_def.id.0
                );
            }
        }

        Ok(())
    }

    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.heartbeat_interval_ms
    }

    pub fn failure_timeout_ms(&self) -> u64 {
        self.failure_timeout_ms
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// 返回所有活跃租约（workflow_id, node_id, claimed_at_ms, expires_at_ms）。
    pub fn active_leases(&self) -> Vec<(String, String, u64, u64)> {
        self.leases
            .iter()
            .map(|ref_multi| {
                let wf_id = ref_multi.key();
                let lease = ref_multi.value();
                (
                    wf_id.0.clone(),
                    lease.node_id.0.clone(),
                    lease.claimed_at_ms,
                    lease.expires_at_ms,
                )
            })
            .collect()
    }

    /// 更新本地节点的容量信息（由工作线程调用，当运行任务数量变化时）。
    pub fn update_local_capacity(&self, available: u32, max: u32) {
        self.local_available_capacity
            .store(available, Ordering::Relaxed);
        self.local_max_capacity.store(max, Ordering::Relaxed);
    }

    /// Update a peer's capacity snapshot.
    pub fn update_peer_capacity(&self, node_id: NodeId, available: u32, max: u32) {
        if let Some(mut peer) = self.peers.get_mut(&node_id) {
            peer.available_slots = available;
            peer.max_slots = max;
        }
    }

    /// Returns a snapshot of all peer capacities for task routing.
    pub fn get_peer_capacities(
        &self,
    ) -> std::collections::HashMap<NodeId, (u32, u32, Option<String>)> {
        self.peers
            .iter()
            .map(|ref_multi| {
                (
                    ref_multi.key().clone(),
                    (
                        ref_multi.value().available_slots,
                        ref_multi.value().max_slots,
                        ref_multi.value().endpoint_addr.clone(),
                    ),
                )
            })
            .collect()
    }

    /// 启动心跳发送和故障检测循环。
    /// 返回一个取消发送器；发送一个 `true` 会停止两个循环。
    ///
    /// 接收 `Arc<Self>` 以便后台任务持有共享引用。
    pub fn start_background_loops(self: Arc<Self>) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        // Heartbeat 循环
        let failover_hb = self.clone();
        let mut hb_cancel = cancel_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(
                failover_hb.heartbeat_interval_ms,
            ));
            let mut hb_count: u64 = 0;
            loop {
                tokio::select! {
                    _ = hb_cancel.changed() => break,
                    _ = interval.tick() => {
                        hb_count += 1;
                        if let Err(e) = failover_hb.send_heartbeat().await {
                            tracing::warn!("background heartbeat #{} failed: {}", hb_count, e);
                        }
                    }
                }
            }
        });

        // Failover 检测循环
        let failover_fd = self.clone();
        let mut fd_cancel = cancel_rx.clone();
        let lease_check_interval_secs = failover_fd.lease_expiry_check_interval_secs;
        tokio::spawn(async move {
            let check_interval = failover_fd.failure_timeout_ms / 2;
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(check_interval));
            let mut lease_interval =
                tokio::time::interval(std::time::Duration::from_secs(lease_check_interval_secs));
            loop {
                tokio::select! {
                    _ = fd_cancel.changed() => break,
                    _ = interval.tick() => {
                        failover_fd.detect_and_claim_failed_nodes().await;
                    }
                    _ = lease_interval.tick() => {
                        failover_fd.expire_leases().await;
                    }
                }
            }
        });

        cancel_tx
    }

    /// 检测失效 peer 并按一致性哈希接管 orphan workflow。
    ///
    /// 先把失联 peer 清出视图并取得其最后快照：孤儿 workflow 列表来自快照，
    /// 而接管选举的候选集合只包含存活节点（活跃 peer + 本节点）——否则孤儿
    /// workflow 可能被哈希给已失联的节点，永远无人接管。
    ///
    /// 单个 workflow 接管或重调度失败只记录错误并继续处理其他 workflow，避免一个
    /// 损坏状态阻塞整批故障恢复。
    pub async fn detect_and_claim_failed_nodes(&self) {
        let stale = self.expire_stale_peers();
        if !stale.is_empty() {
            tracing::info!(
                removed = stale.len(),
                "expired stale peers before failure detection"
            );
        }
        let now_ms = crate::common::epoch_millis();
        let timeout_ms = self.failure_timeout_ms();
        let my_id = &self.node_id;
        // 选举候选集合：仅存活节点（expire 后视图中的 peer + 本节点）。
        let candidate_ids: Vec<String> = {
            let mut ids: Vec<_> = self.peers.iter().map(|e| e.key().0.clone()).collect();
            ids.push(my_id.0.clone());
            ids
        };

        // 待检测集合：失联快照 + 当前视图（覆盖边界时序下仍超时的 peer）。
        let mut to_check: Vec<(NodeId, PeerInfo)> = stale;
        for (node_id, info) in self.get_peer_infos() {
            to_check.push((node_id, info));
        }

        for (node_id, info) in &to_check {
            let is_failed = info.last_heartbeat_ms > 0
                && now_ms.saturating_sub(info.last_heartbeat_ms) > timeout_ms;
            if !is_failed || info.active_workflows.is_empty() {
                continue;
            }
            tracing::warn!(
                "detected failed node: {}, orphaned workflows: {:?}",
                node_id.0,
                info.active_workflows
                    .iter()
                    .map(|w| w.0.clone())
                    .collect::<Vec<_>>()
            );

            // 使用 per-workflow 一致性哈希将 claim 均匀分布到
            // 存活节点，而非将所有 workflow 发往
            // ID 最低的单个节点。
            for wf_id in &info.active_workflows {
                if should_claim_workflow(&wf_id.0, &my_id.0, candidate_ids.clone()) {
                    if let Err(e) = self.claim_workflow(wf_id).await {
                        tracing::error!("failed to claim workflow {}: {}", wf_id.0, e);
                        continue;
                    }
                    if let Err(e) = self.reschedule_workflow_tasks(wf_id).await {
                        tracing::error!(
                            "failed to reschedule tasks for workflow {}: {}",
                            wf_id.0,
                            e
                        );
                    }
                }
            }
        }
    }

    /// 处理远端心跳并更新 peer 视图。
    ///
    /// `last_heartbeat_ms` 记录**接收方本地时钟**的接收时刻而非发送方
    /// `timestamp_ms`：故障检测窗口由接收方度量，若使用发送方时钟，
    /// 跨节点时钟偏差会直接侵蚀/放大检测窗口（偏差大时误判失联或漏判）。
    pub fn handle_heartbeat(&self, hb: &NodeHeartbeat) {
        if hb.node_id != self.node_id {
            tracing::debug!(
                "received heartbeat from {} with {} active workflows",
                hb.node_id.0,
                hb.active_workflows.len()
            );
            let is_new = !self.peers.contains_key(&hb.node_id);
            let received_at_ms = crate::common::epoch_millis();
            let mut peer = self.peers.entry(hb.node_id.clone()).or_insert(PeerState {
                last_heartbeat_ms: 0,
                active_workflows: HashSet::new(),
                available_slots: 0,
                max_slots: 0,
                endpoint_addr: None,
            });
            peer.last_heartbeat_ms = received_at_ms;
            peer.active_workflows = hb.active_workflows.iter().cloned().collect();
            peer.available_slots = hb.available_slots;
            peer.max_slots = hb.max_slots;
            peer.endpoint_addr = hb.endpoint_addr.clone();
            if is_new {
                crate::metrics::inc_connected_peers();
            }
        }
    }

    pub async fn expire_leases(&self) {
        let now_ms = crate::common::epoch_millis();
        let now_monotonic = std::time::Instant::now();
        let active = match self.active_workflow_ids().await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = %e, "failover: failed to get active workflow ids");
                return;
            }
        };
        let active_set: HashSet<WorkflowId> = active.into_iter().collect();

        // 租约仲裁语义：本节点活跃 workflow 即续租。
        //
        // - 本节点持有、workflow 仍在本节点 active_set：**无条件续租**——本地
        //   延长到期时间并持久化，不走 claim→adopt→广播→全 peer persist 的
        //   重选路径，消除每个 lease_duration 周期的写放大；也避免过期后参与
        //   接管选举、输给新加入 peer 时出现租约无人持有的窗口（分区自愈）。
        //
        //   反双主依赖 handle_claim 的时序契约：远端节点 claim 成功后广播
        //   claim，本节点收到即 remove_active_workflow，workflow 退出
        //   active_set，下一轮 expire_leases 不再为其续租，旧主让位。claim
        //   与心跳走同一传输通道；若 claim 通知丢失，claimer 会在后续
        //   detect_and_claim_failed_nodes 循环中重新 claim，窗口由
        //   `lease_duration_ms > failure_timeout_ms` 的配置约束兜底。
        //
        // - 其余过期租约（非本节点持有，或 workflow 已不活跃）：移除。
        //   过期失效路径仅对非活跃 workflow 生效。
        let mut lapsed_own_active: Vec<WorkflowId> = Vec::new();
        let mut expired: Vec<WorkflowId> = Vec::new();
        for entry in self.leases.iter() {
            let (wf_id, lease) = (entry.key(), entry.value());
            if lease.is_valid(now_ms, now_monotonic) {
                continue;
            }
            if lease.node_id == self.node_id && active_set.contains(wf_id) {
                lapsed_own_active.push(wf_id.clone());
            } else {
                expired.push(wf_id.clone());
            }
        }

        for wf_id in &expired {
            self.remove_lease(wf_id);
        }

        for wf_id in &lapsed_own_active {
            let renewed = LeaseEntry::new_with_monotonic(
                self.node_id.clone(),
                now_ms,
                now_ms + self.lease_duration_ms,
                self.lease_duration_ms,
            );
            // 持久化失败保留旧租约记录，下一轮 expire_leases 重试续租。
            if let Err(e) = self.persist_lease(wf_id, &renewed) {
                tracing::warn!(
                    workflow = %wf_id.0,
                    error = %e,
                    "failed to persist renewed lease for active workflow"
                );
                continue;
            }
            self.leases.insert(wf_id.clone(), renewed);
            tracing::debug!(
                workflow = %wf_id.0,
                "renewed lapsed lease for active workflow owned by this node"
            );
        }
    }

    /// 从内存与持久化存储中移除租约。
    fn remove_lease(&self, wf_id: &WorkflowId) {
        self.leases.remove(wf_id);
        if let Some(ref store) = self.store {
            let key = format!("{}{}", STORE_KEY_LEASE, wf_id.0);
            if let Err(e) = store.delete(&key) {
                tracing::warn!(
                    "failed to delete expired lease for workflow {}: {}",
                    wf_id.0,
                    e
                );
            }
        }
    }

    fn persist_lease(&self, workflow_id: &WorkflowId, lease: &LeaseEntry) -> Result<()> {
        if let Some(ref store) = self.store {
            let persisted = PersistedLease {
                node_id: lease.node_id.clone(),
                claimed_at_ms: lease.claimed_at_ms,
                expires_at_ms: lease.expires_at_ms,
            };
            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&persisted)
                .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
            let key = format!("{}{}", STORE_KEY_LEASE, workflow_id.0);
            store.put(&key, &data)?;
        }
        Ok(())
    }

    fn recover_leases_from_store(&self) {
        if let Some(ref store) = self.store {
            let entries = match store.scan_prefix(STORE_KEY_LEASE) {
                Ok(e) => e,
                Err(e) => {
                    tracing::error!("failed to scan leases from store: {}", e);
                    return;
                }
            };
            let now_ms = crate::common::epoch_millis();
            for (key, data) in entries {
                let wf_id_str = key.strip_prefix(STORE_KEY_LEASE).unwrap_or(&key);
                let wf_id = WorkflowId(wf_id_str.to_string());
                match rkyv::from_bytes::<PersistedLease, rkyv::rancor::Error>(&data) {
                    Ok(persisted) => {
                        if now_ms < persisted.expires_at_ms {
                            // 从持久化恢复的租约无单调基线，回退墙钟比较。
                            self.leases.insert(
                                wf_id,
                                LeaseEntry::restored(
                                    persisted.node_id,
                                    persisted.claimed_at_ms,
                                    persisted.expires_at_ms,
                                ),
                            );
                        } else if let Err(e) = store.delete(&key) {
                            tracing::warn!("failed to delete expired lease {}: {}", wf_id_str, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("corrupt lease entry {}: {:?}", wf_id_str, e);
                        if let Err(e) = store.delete(&key) {
                            tracing::warn!("failed to delete corrupt lease {}: {}", wf_id_str, e);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/rust/unit/runtime/workflow/failover.rs"]
mod tests;
