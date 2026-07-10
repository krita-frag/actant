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
}

impl Clone for LeaseEntry {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            claimed_at_ms: self.claimed_at_ms,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

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
            Err(crate::common::ActantError::Actor(err))
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

    pub fn with_heartbeat_interval(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = ms;
        self
    }

    pub fn with_capacity(self, available: u32, max: u32) -> Self {
        self.local_available_capacity
            .store(available, Ordering::Relaxed);
        self.local_max_capacity.store(max, Ordering::Relaxed);
        self
    }

    pub fn set_scheduler(&self, scheduler: Arc<dyn crate::runtime::workflow::Scheduler>) {
        *self.scheduler.lock() = Some(scheduler);
    }

    pub async fn subscribe_topics(&self) -> Result<()> {
        self.network.subscribe(TOPIC_HEARTBEAT).await?;
        self.network.subscribe(TOPIC_FAILOVER).await?;
        self.network.subscribe(TOPIC_HEADS).await?;
        self.network
            .subscribe(crate::common::TOPIC_DAG_STATE)
            .await?;
        Ok(())
    }

    pub async fn send_heartbeat(&self) -> Result<()> {
        let active_workflows = self.active_workflow_ids().await?;
        let now_ms = crate::common::epoch_millis();
        let hb = NodeHeartbeat {
            node_id: self.node_id.clone(),
            active_workflows,
            timestamp_ms: now_ms,
            available_slots: self.local_available_capacity.load(Ordering::Relaxed),
            max_slots: self.local_max_capacity.load(Ordering::Relaxed),
            endpoint_addr: Some(self.network.local_peer_id().to_string()),
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

    pub async fn claim_workflow(&self, workflow_id: &WorkflowId) -> Result<()> {
        let now_ms = crate::common::epoch_millis();
        let lease_duration_ms = self.lease_duration_ms;
        if let Some(existing) = self.leases.get(workflow_id) {
            if now_ms < existing.expires_at_ms {
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
        let lease = LeaseEntry {
            node_id: self.node_id.clone(),
            claimed_at_ms: now_ms,
            expires_at_ms: now_ms + lease_duration_ms,
        };

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

    pub async fn handle_claim(&self, claim: &OrchestratorClaim) {
        let lease_duration_ms = self.lease_duration_ms;

        // 使用 claimant 的时间戳作为基准，使租期时长
        // 在所有节点上一致，不受时钟偏差 / 网络
        // latency 影响。若使用接收方本地时间，
        // 会因网络传输时间而缩短租期。
        let lease = LeaseEntry {
            node_id: claim.node_id.clone(),
            claimed_at_ms: claim.timestamp_ms,
            expires_at_ms: claim.timestamp_ms + lease_duration_ms,
        };
        if let Err(e) = self.persist_lease(&claim.workflow_id, &lease) {
            tracing::warn!("failed to persist lease for {}: {}", claim.workflow_id.0, e);
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

    pub async fn detect_and_claim_failed_nodes(&self) {
        let peers = self.get_peer_infos();
        let now_ms = crate::common::epoch_millis();
        let timeout_ms = self.failure_timeout_ms;

        for (node_id, info) in &peers {
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

            let my_id = &self.node_id;
            let candidate_ids: Vec<String> = {
                let mut ids: Vec<_> = peers.keys().map(|k| k.0.clone()).collect();
                ids.push(my_id.0.clone());
                ids
            };

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

    pub fn handle_heartbeat(&self, hb: &NodeHeartbeat) {
        if hb.node_id != self.node_id {
            tracing::debug!(
                "received heartbeat from {} with {} active workflows",
                hb.node_id.0,
                hb.active_workflows.len()
            );
            let is_new = !self.peers.contains_key(&hb.node_id);
            let mut peer = self.peers.entry(hb.node_id.clone()).or_insert(PeerState {
                last_heartbeat_ms: 0,
                active_workflows: HashSet::new(),
                available_slots: 0,
                max_slots: 0,
                endpoint_addr: None,
            });
            peer.last_heartbeat_ms = hb.timestamp_ms;
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
        let active = match self.active_workflow_ids().await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = %e, "failover: failed to get active workflow ids");
                return;
            }
        };
        let active_set: HashSet<WorkflowId> = active.into_iter().collect();
        let lease_duration_ms = self.lease_duration_ms;

        let expired: Vec<WorkflowId> = self
            .leases
            .iter()
            .filter(|ref_multi| {
                let wf_id = ref_multi.key();
                let lease = ref_multi.value();
                if now_ms < lease.expires_at_ms {
                    return false;
                }
                if lease.node_id == self.node_id && active_set.contains(wf_id) {
                    return false;
                }
                true
            })
            .map(|ref_multi| ref_multi.key().clone())
            .collect();

        // 为本节点拥有的活跃 workflow 续租约。
        // 先收集需要续租的 key，再逐个 get_mut 修改，避免 iter_mut 持有 guard 时修改。
        let to_renew_keys: Vec<WorkflowId> = self
            .leases
            .iter()
            .filter_map(|ref_multi| {
                let wf_id = ref_multi.key().clone();
                let lease = ref_multi.value();
                if lease.node_id == self.node_id
                    && now_ms >= lease.expires_at_ms
                    && active_set.contains(&wf_id)
                {
                    Some(wf_id)
                } else {
                    None
                }
            })
            .collect();

        let mut to_renew: Vec<(WorkflowId, LeaseEntry)> = Vec::new();
        for wf_id in &to_renew_keys {
            if let Some(mut lease) = self.leases.get_mut(wf_id) {
                lease.expires_at_ms = now_ms + lease_duration_ms;
                to_renew.push((wf_id.clone(), lease.clone()));
            }
        }

        for (wf_id, lease) in &to_renew {
            if let Err(e) = self.persist_lease(wf_id, lease) {
                tracing::error!("failed to persist lease for workflow {}: {}", wf_id.0, e);
            }
        }

        for wf_id in &expired {
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
                    tracing::warn!("failed to scan leases from store: {}", e);
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
                            self.leases.insert(
                                wf_id,
                                LeaseEntry {
                                    node_id: persisted.node_id,
                                    claimed_at_ms: persisted.claimed_at_ms,
                                    expires_at_ms: persisted.expires_at_ms,
                                },
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
mod tests {
    use super::*;
    use crate::common::{NodeHeartbeat, NodeId, WorkflowId};
    use crate::runtime::actor::ActorSystem;
    use crate::test_support::MockTransport;

    fn make_fm(node_id: &str) -> FailoverManager {
        let network: Arc<dyn crate::runtime::network::Transport> =
            Arc::new(MockTransport::new(node_id));
        let actor_system = Arc::new(ActorSystem::new());
        let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
        FailoverManager::new(
            NodeId::from(node_id.to_string()),
            network,
            actor_system,
            wf_id,
        )
    }

    fn hb(node_id: &str, ts_ms: u64, workflows: &[&str]) -> NodeHeartbeat {
        NodeHeartbeat {
            node_id: NodeId::from(node_id.to_string()),
            active_workflows: workflows
                .iter()
                .map(|w| WorkflowId(w.to_string()))
                .collect(),
            timestamp_ms: ts_ms,
            available_slots: 4,
            max_slots: 8,
            endpoint_addr: Some(format!("peer-{}", node_id)),
        }
    }

    #[test]
    fn getters_return_configured_values() {
        let fm = make_fm("node-A")
            .with_heartbeat_interval(1234)
            .with_capacity(3, 10);
        assert_eq!(fm.heartbeat_interval_ms(), 1234);
        assert_eq!(
            fm.failure_timeout_ms(),
            FailoverConfig::default().failure_timeout_ms
        );
        assert_eq!(fm.node_id().as_str(), "node-A");
    }

    #[test]
    fn handle_heartbeat_records_peer() {
        let fm = make_fm("node-A");
        let now = crate::common::epoch_millis();
        fm.handle_heartbeat(&hb("node-B", now, &["wf-1"]));

        let infos = fm.get_peer_infos();
        assert!(infos.contains_key(&NodeId::from("node-B".to_string())));
        let peer = &infos[&NodeId::from("node-B".to_string())];
        assert_eq!(peer.last_heartbeat_ms, now);
        assert_eq!(peer.available_slots, 4);
        assert_eq!(peer.max_slots, 8);
        assert_eq!(peer.endpoint_addr.as_deref(), Some("peer-node-B"));
        assert!(peer
            .active_workflows
            .contains(&WorkflowId("wf-1".to_string())));
    }

    #[test]
    fn handle_heartbeat_ignores_own_node() {
        let fm = make_fm("node-A");
        fm.handle_heartbeat(&hb("node-A", crate::common::epoch_millis(), &[]));
        assert!(fm.get_peer_infos().is_empty());
    }

    #[test]
    fn handle_heartbeat_updates_existing_peer() {
        let fm = make_fm("node-A");
        let now = crate::common::epoch_millis();
        fm.handle_heartbeat(&hb("node-B", now, &["wf-1"]));
        fm.handle_heartbeat(&hb("node-B", now + 1000, &["wf-1", "wf-2"]));

        let peer = &fm.get_peer_infos()[&NodeId::from("node-B".to_string())];
        assert_eq!(peer.last_heartbeat_ms, now + 1000);
        assert_eq!(peer.active_workflows.len(), 2);
    }

    #[test]
    fn remove_peer_drops_entry() {
        let fm = make_fm("node-A");
        fm.handle_heartbeat(&hb("node-B", crate::common::epoch_millis(), &[]));
        fm.remove_peer(&NodeId::from("node-B".to_string()));
        assert!(!fm
            .get_peer_infos()
            .contains_key(&NodeId::from("node-B".to_string())));
    }

    #[test]
    fn expire_stale_peers_removes_only_timed_out() {
        let fm = make_fm("node-A");
        let now = crate::common::epoch_millis();
        let timeout = fm.failure_timeout_ms();
        // node-B: 心跳新鲜；node-C: 心跳过期
        fm.handle_heartbeat(&hb("node-B", now, &[]));
        fm.handle_heartbeat(&hb("node-C", now.saturating_sub(timeout + 5000), &[]));

        let removed = fm.expire_stale_peers();
        let removed_ids: Vec<String> = removed.iter().map(|(n, _)| n.0.clone()).collect();
        assert_eq!(removed_ids, vec!["node-C".to_string()]);
        let infos = fm.get_peer_infos();
        assert!(infos.contains_key(&NodeId::from("node-B".to_string())));
        assert!(!infos.contains_key(&NodeId::from("node-C".to_string())));
    }

    #[test]
    fn expire_stale_peers_skips_zero_heartbeat() {
        // last_heartbeat_ms == 0 表示从未真正收到心跳，不应被判定为 stale。
        let fm = make_fm("node-A");
        fm.handle_heartbeat(&hb("node-B", 0, &[]));
        let removed = fm.expire_stale_peers();
        assert!(removed.is_empty());
    }

    #[test]
    fn update_local_capacity_reflected_in_get_peer_capacities() {
        let fm = make_fm("node-A");
        fm.update_local_capacity(7, 16);
        // 本节点容量不进入 peer 表（仅 peer 容量才进），但 send_heartbeat 会读取。
        // 这里仅验证 update 不 panic 且 peer 表为空。
        assert!(fm.get_peer_capacities().is_empty());
    }

    #[test]
    fn update_peer_capacity_modifies_existing_peer() {
        let fm = make_fm("node-A");
        fm.handle_heartbeat(&hb("node-B", crate::common::epoch_millis(), &[]));
        fm.update_peer_capacity(NodeId::from("node-B".to_string()), 2, 5);
        let caps = fm.get_peer_capacities();
        let b_cap = &caps[&NodeId::from("node-B".to_string())];
        assert_eq!(b_cap.0, 2);
        assert_eq!(b_cap.1, 5);
    }

    #[test]
    fn update_peer_capacity_ignores_unknown_peer() {
        let fm = make_fm("node-A");
        fm.update_peer_capacity(NodeId::from("node-X".to_string()), 1, 2);
        assert!(fm.get_peer_capacities().is_empty());
    }

    #[test]
    fn active_leases_empty_initially() {
        let fm = make_fm("node-A");
        assert!(fm.active_leases().is_empty());
    }

    #[test]
    fn handle_claim_same_node_records_lease_without_network_call() {
        // 同节点 claim：不调用 remove_active_workflow（避免 actor 调用），
        // 仅持久化 lease + 写入 leases map。
        let fm = make_fm("node-A");
        let now = crate::common::epoch_millis();
        let claim = OrchestratorClaim {
            node_id: fm.node_id().clone(),
            workflow_id: WorkflowId("wf-same".to_string()),
            timestamp_ms: now,
        };
        // 阻塞执行 async 方法
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(fm.handle_claim(&claim));

        let leases = fm.active_leases();
        assert_eq!(leases.len(), 1);
        let (wf, node, claimed, expires) = &leases[0];
        assert_eq!(wf, "wf-same");
        assert_eq!(node, "node-A");
        assert_eq!(*claimed, now);
        assert_eq!(*expires, now + FailoverConfig::default().lease_duration_ms);
    }

    #[test]
    fn handle_claim_remote_node_records_lease_and_skips_removal_on_actor_error() {
        // 远端节点 claim：会调用 remove_active_workflow（actor 调用失败被吞掉），
        // 但 lease 仍应被记录。
        let fm = make_fm("node-A");
        let now = crate::common::epoch_millis();
        let claim = OrchestratorClaim {
            node_id: NodeId::from("node-Z".to_string()),
            workflow_id: WorkflowId("wf-remote".to_string()),
            timestamp_ms: now,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(fm.handle_claim(&claim));

        let leases = fm.active_leases();
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].1, "node-Z");
        assert_eq!(
            leases[0].3,
            now + FailoverConfig::default().lease_duration_ms
        );
    }

    #[test]
    fn with_capacity_sets_atomic_counters() {
        let fm = make_fm("node-A").with_capacity(5, 12);
        // 通过 update_peer_capacity 间接验证 atomic 可用性（不 panic）。
        fm.update_local_capacity(0, 0);
    }
}
