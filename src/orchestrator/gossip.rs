use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crate::common::{
    GossipConfig, HeadsExchange, NodeId, TaskCompletion, TaskId, WireDagStateUpdate, WireEnvelope,
    WireMessage, WireTaskState, WorkflowHead, WorkflowId, WorkflowStateRequest,
    WorkflowStateResponse, TOPIC_DAG_STATE, TOPIC_HEADS, TOPIC_WORKFLOW_STATE_REQ,
};
use crate::network::Transport;
use crate::orchestrator::Orchestrator;
use crate::store::hlc::HybridLogicalClock;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct SeenKey(WorkflowId, TaskId);

#[derive(Clone)]
struct SeenEntry {
    inserted_at: std::time::Instant,
    hlc_timestamp: crate::store::hlc::HlcTimestamp,
    state_priority: u8,
}

impl SeenEntry {
    /// 如果传入的更新应该覆盖当前条目，则返回 true。
    fn is_superseded_by(
        &self,
        incoming_hlc: &crate::store::hlc::HlcTimestamp,
        incoming_prio: u8,
    ) -> bool {
        *incoming_hlc > self.hlc_timestamp
            || (*incoming_hlc == self.hlc_timestamp && incoming_prio >= self.state_priority)
    }
}

/// DAG 状态 gossip 协调器。
///
/// 持有一个类型擦除的 [`Transport`] (`Arc<dyn Transport>`)，使得同一个
/// `DagGossip` 类型与具体的传输实现解耦。
/// 运行时在启动时一次性选择具体的传输实现；下游代码
/// （orchestrator、worker、failover）将其视为黑盒。
#[derive(Clone)]
pub struct DagGossip {
    network: Arc<dyn Transport>,
    orchestrator: Arc<Orchestrator>,
    seen: Arc<DashMap<SeenKey, SeenEntry>>,
    clock: Arc<HybridLogicalClock>,
    node_id: NodeId,
    dedup_window_size: usize,
    dedup_ttl_secs: u64,
    retry_attempts: usize,
    retry_base_delay: Duration,
}

impl Drop for DagGossip {
    fn drop(&mut self) {
        tracing::debug!("DagGossip::drop");
    }
}

impl DagGossip {
    /// 从任意 [`Transport`] 实现创建一个 `DagGossip`。
    ///
    /// 运行时在启动时一次性选择具体的传输实现；下游代码
    /// （orchestrator、worker、failover）将其视为黑盒。
    pub fn new(
        network: Arc<dyn Transport>,
        orchestrator: Arc<Orchestrator>,
        config: GossipConfig,
    ) -> Self {
        let node_id = network.node_id().clone();
        Self {
            network,
            orchestrator,
            seen: Arc::new(DashMap::new()),
            clock: Arc::new(HybridLogicalClock::new()),
            node_id,
            dedup_window_size: config.dedup_window_size,
            dedup_ttl_secs: config.dedup_ttl_secs,
            retry_attempts: config.retry_attempts,
            retry_base_delay: Duration::from_millis(config.retry_base_delay_ms),
        }
    }

    pub fn clock(&self) -> &Arc<HybridLogicalClock> {
        &self.clock
    }

    /// 返回当前跟踪的去重条目数量。
    pub fn seen_count(&self) -> usize {
        self.seen.len()
    }

    /// 返回去重统计信息。
    pub fn dedup_stats(&self) -> (usize, usize, u64, u64) {
        (
            self.seen.len(),
            self.dedup_window_size,
            self.dedup_ttl_secs,
            self.retry_attempts as u64,
        )
    }

    pub async fn broadcast_state_update(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        completion: &TaskCompletion,
    ) -> crate::common::Result<()> {
        let task_state = match completion {
            TaskCompletion::Completed { result, .. } => WireTaskState::Completed {
                result: result.clone(),
            },
            TaskCompletion::Failed { error, .. } => WireTaskState::Failed {
                error: error.clone(),
            },
            TaskCompletion::Cancelled { .. } => WireTaskState::Cancelled,
            TaskCompletion::Skipped { .. } => WireTaskState::Skipped,
        };

        self.broadcast_update(workflow_id, task_id, task_state)
            .await
    }

    /// 当任务开始执行时广播一个 Running 状态更新。
    pub async fn broadcast_task_running(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> crate::common::Result<()> {
        self.broadcast_update(workflow_id, task_id, WireTaskState::Running)
            .await
    }

    pub async fn broadcast_update(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        task_state: WireTaskState,
    ) -> crate::common::Result<()> {
        // 在 move task_state 到 update 之前检查终态
        let is_terminal = matches!(
            task_state,
            WireTaskState::Completed { .. }
                | WireTaskState::Failed { .. }
                | WireTaskState::Cancelled
                | WireTaskState::Skipped
        );
        tracing::trace!(
            wf = ?workflow_id,
            task = ?task_id,
            terminal = is_terminal,
            "gossip broadcast_update"
        );

        let hlc_timestamp = self.clock.tick();

        let update = WireDagStateUpdate {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
            task_state,
            hlc_timestamp,
            origin_node: self.node_id.clone(),
        };

        let dedup_key = SeenKey(workflow_id.clone(), task_id.clone());
        let incoming_prio = state_priority(&update.task_state);
        let entry = SeenEntry {
            inserted_at: std::time::Instant::now(),
            hlc_timestamp,
            state_priority: incoming_prio,
        };
        self.seen
            .entry(dedup_key)
            .and_modify(|existing| {
                if existing.is_superseded_by(&hlc_timestamp, incoming_prio) {
                    *existing = entry.clone();
                }
            })
            .or_insert_with(|| entry);

        let msg = WireMessage::DagStateUpdate(update);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg))
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;

        crate::metrics::inc_gossip_updates_sent();

        // 终态（Completed/Failed）对 workflow 推进至关重要。
        // 使用有限次重试以降低 broadcast 丢失时
        // 状态发散的概率。Running 状态非关键 — 单次
        // 尝试即可，因为终态更新会紧随其后。
        if is_terminal {
            self.broadcast_with_retry(TOPIC_DAG_STATE, data).await
        } else {
            self.network.broadcast(TOPIC_DAG_STATE, data).await
        }
    }

    pub async fn apply_remote_update(
        &self,
        update: WireDagStateUpdate,
    ) -> crate::common::Result<()> {
        let dedup_key = SeenKey(update.workflow_id.clone(), update.task_id.clone());

        // CRDT merge: check if the incoming update is superseded by the
        // existing entry for this (workflow, task) pair.
        let incoming_priority = state_priority(&update.task_state);

        if let Some(existing) = self.seen.get(&dedup_key) {
            if !existing.is_superseded_by(&update.hlc_timestamp, incoming_priority) {
                tracing::debug!(
                    "dropping stale gossip update for {}/{}: incoming hlc {:?} prio {} vs seen hlc {:?} prio {}",
                    update.workflow_id.as_str(), update.task_id.as_str(),
                    update.hlc_timestamp, incoming_priority,
                    existing.hlc_timestamp, existing.state_priority
                );
                crate::metrics::inc_gossip_updates_dropped();
                return Ok(());
            }
        }

        self.clock.merge(&update.hlc_timestamp);

        crate::metrics::inc_gossip_updates_received();

        let entry = SeenEntry {
            inserted_at: std::time::Instant::now(),
            hlc_timestamp: update.hlc_timestamp,
            state_priority: incoming_priority,
        };
        self.seen
            .entry(dedup_key)
            .and_modify(|existing| {
                if existing.is_superseded_by(&update.hlc_timestamp, incoming_priority) {
                    *existing = entry.clone();
                }
            })
            .or_insert_with(|| entry);

        self.evict_seen();

        match update.task_state {
            WireTaskState::Running => {
                if let Err(e) = self
                    .orchestrator
                    .mark_task_running(&update.workflow_id, &update.task_id)
                    .await
                {
                    // 本节点未托管该 workflow — 静默忽略未知 workflow 的 gossip
                    if !matches!(e, crate::common::ActantError::NotFound(_)) {
                        return Err(e);
                    }
                    tracing::debug!(
                        "ignoring gossip Running for unknown workflow {}",
                        update.workflow_id.as_str()
                    );
                }
            }
            WireTaskState::Completed { result } => {
                if let Err(e) = self
                    .orchestrator
                    .on_task_completed(&update.workflow_id, &update.task_id, result)
                    .await
                    .map(|(tasks, _cond)| tasks)
                {
                    if !matches!(e, crate::common::ActantError::NotFound(_)) {
                        return Err(e);
                    }
                    tracing::debug!(
                        "ignoring gossip Completed for unknown workflow {}",
                        update.workflow_id.as_str()
                    );
                }
            }
            WireTaskState::Failed { error } => {
                if let Err(e) = self
                    .orchestrator
                    .fail_task(
                        &update.workflow_id,
                        &update.task_id,
                        error,
                        crate::orchestrator::FailureScope::WorkflowLevel,
                    )
                    .await
                {
                    if !matches!(e, crate::common::ActantError::NotFound(_)) {
                        return Err(e);
                    }
                    tracing::debug!(
                        "ignoring gossip Failed for unknown workflow {}",
                        update.workflow_id.as_str()
                    );
                }
            }
            WireTaskState::Cancelled => {
                if let Err(e) = self
                    .orchestrator
                    .cancel_task(&update.workflow_id, &update.task_id)
                    .await
                {
                    if !matches!(e, crate::common::ActantError::NotFound(_)) {
                        return Err(e);
                    }
                    tracing::debug!(
                        "ignoring gossip Cancelled for unknown workflow {}",
                        update.workflow_id.as_str()
                    );
                }
            }
            WireTaskState::Skipped => {
                if let Err(e) = self
                    .orchestrator
                    .skip_conditional_branch(&update.workflow_id, &update.task_id)
                    .await
                {
                    if !matches!(e, crate::common::ActantError::NotFound(_)) {
                        return Err(e);
                    }
                    tracing::debug!(
                        "ignoring gossip Skipped for unknown workflow {}",
                        update.workflow_id.as_str()
                    );
                }
            }
        }
        Ok(())
    }

    pub async fn broadcast_heads(&self) -> crate::common::Result<()> {
        let active_ids = self.orchestrator.active_workflow_ids().await;
        let mut heads = Vec::with_capacity(active_ids.len());

        for wf_id in &active_ids {
            if let Some(exec) = self.orchestrator.get_state(wf_id).await {
                let hlc_ts = self.clock.tick();
                heads.push(WorkflowHead {
                    workflow_id: wf_id.clone(),
                    succeeded_count: exec.succeeded_count(),
                    total_count: exec.total_count(),
                    hlc_timestamp: hlc_ts,
                });
            }
        }

        let exchange = HeadsExchange {
            node_id: self.node_id.clone(),
            heads,
        };
        let msg = WireMessage::HeadsExchange(exchange);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg))
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;

        self.network.broadcast(TOPIC_HEADS, data).await
    }

    pub async fn handle_heads_exchange(
        &self,
        exchange: &HeadsExchange,
    ) -> crate::common::Result<()> {
        if exchange.node_id == self.node_id {
            return Ok(());
        }

        for remote_head in &exchange.heads {
            let local_exec = self.orchestrator.get_state(&remote_head.workflow_id).await;

            match local_exec {
                None => {
                    tracing::info!(
                        "discovered unknown workflow {} from node {}, requesting full state",
                        remote_head.workflow_id.as_str(),
                        exchange.node_id.as_str()
                    );
                    self.orchestrator
                        .adopt_workflow(&remote_head.workflow_id)
                        .await?;
                    self.request_workflow_state(&remote_head.workflow_id, &exchange.node_id)
                        .await?;
                }
                Some(local) => {
                    if remote_head.succeeded_count > local.succeeded_count() {
                        tracing::info!(
                            "workflow {} behind: local succeeded {} < remote succeeded {}, requesting state",
                            remote_head.workflow_id.as_str(), local.succeeded_count(), remote_head.succeeded_count
                        );
                        self.request_workflow_state(&remote_head.workflow_id, &exchange.node_id)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// 请求从远程节点获取完整的 DAG 状态（DAG +执行）。
    pub async fn request_workflow_state(
        &self,
        workflow_id: &WorkflowId,
        target_node: &NodeId,
    ) -> crate::common::Result<()> {
        let request = WorkflowStateRequest {
            workflow_id: workflow_id.clone(),
            requesting_node: self.node_id.clone(),
        };
        let msg = WireMessage::WorkflowStateRequest(request);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg))
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        let topic = format!("{}{}", TOPIC_WORKFLOW_STATE_REQ, target_node.as_str());
        self.network.broadcast(&topic, data).await
    }

    /// 处理从远程节点收到的 DAG 状态请求。
    pub async fn handle_workflow_state_request(
        &self,
        request: &WorkflowStateRequest,
    ) -> crate::common::Result<()> {
        let (dag_bytes, exec_bytes, pending_bytes) = {
            let state = self.orchestrator.state_handle();
            let slot = state.get_slot(&request.workflow_id);
            match slot {
                Some(ref slot) => {
                    let dag_bytes = crate::common::serialization::serialize_rkyv(&slot.dag).ok();
                    let exec_bytes =
                        crate::common::serialization::serialize_rkyv(&slot.execution).ok();
                    let pending_bytes =
                        crate::common::serialization::serialize_rkyv(&slot.pending).ok();
                    (dag_bytes, exec_bytes, pending_bytes)
                }
                None => {
                    // 检查持久化 store
                    if let Some(ref store) = self.orchestrator.store() {
                        let dag_key = format!(
                            "{}{}",
                            crate::common::STORE_KEY_DAG,
                            request.workflow_id.as_str()
                        );
                        let dag_bytes = store.get(&dag_key).ok().flatten();
                        let exec_key = format!(
                            "{}{}",
                            crate::common::STORE_KEY_EXEC,
                            request.workflow_id.as_str()
                        );
                        let exec_bytes = store.get(&exec_key).ok().flatten();
                        let pending_key = format!(
                            "{}{}",
                            crate::common::STORE_KEY_PENDING,
                            request.workflow_id.as_str()
                        );
                        let pending_bytes = store.get(&pending_key).ok().flatten();
                        (dag_bytes, exec_bytes, pending_bytes)
                    } else {
                        (None, None, None)
                    }
                }
            }
        };

        if dag_bytes.is_none() && exec_bytes.is_none() {
            tracing::debug!(
                "no state available for workflow {} requested by {}",
                request.workflow_id.as_str(),
                request.requesting_node.as_str()
            );
            return Ok(());
        }

        let response = WorkflowStateResponse {
            workflow_id: request.workflow_id.clone(),
            dag: dag_bytes,
            execution: exec_bytes,
            pending: pending_bytes,
        };
        let msg = WireMessage::WorkflowStateResponse(response);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg))
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        let topic = crate::common::Topic::workflow_state_resp(&request.requesting_node);
        self.network.broadcast(topic.as_str(), data).await
    }

    /// 处理从远程节点收到的 DAG 状态响应。
    pub async fn handle_workflow_state_response(
        &self,
        response: &WorkflowStateResponse,
    ) -> crate::common::Result<()> {
        use crate::orchestrator::{Dag, WorkflowExecution};

        let dag: Option<Dag> = response
            .dag
            .as_ref()
            .and_then(|data| rkyv::from_bytes::<Dag, rkyv::rancor::Error>(data).ok());
        let execution: Option<WorkflowExecution> = response
            .execution
            .as_ref()
            .and_then(|data| rkyv::from_bytes::<WorkflowExecution, rkyv::rancor::Error>(data).ok());
        let pending: Option<std::collections::HashMap<TaskId, usize>> =
            response.pending.as_ref().and_then(|data| {
                rkyv::from_bytes::<std::collections::HashMap<TaskId, usize>, rkyv::rancor::Error>(
                    data,
                )
                .ok()
            });

        match (dag, execution) {
            (Some(dag), Some(mut exec)) => {
                // 将 running 的 task 重置为 Pending，便于重新调度
                let task_ids: Vec<TaskId> = exec.tasks.keys().cloned().collect();
                for tid in &task_ids {
                    exec.reset_task(tid, false, true);
                }

                let pending = pending.unwrap_or_else(|| {
                    let mut p = std::collections::HashMap::new();
                    for node in dag.nodes() {
                        let pred_count = dag.predecessor_count(&node.task_id);
                        p.insert(node.task_id.clone(), pred_count);
                    }
                    p
                });

                self.orchestrator
                    .restore_workflow(&response.workflow_id, dag, exec, pending)
                    .await;

                tracing::info!(
                    "restored workflow {} from remote state response",
                    response.workflow_id.as_str()
                );
            }
            (Some(dag), None) => {
                let task_ids: Vec<TaskId> = dag.nodes().map(|n| n.task_id.clone()).collect();
                let exec = WorkflowExecution::new(response.workflow_id.clone(), task_ids)
                    .with_failure_strategy(dag.failure_strategy);
                let mut pending = std::collections::HashMap::new();
                for node in dag.nodes() {
                    let pred_count = dag.predecessor_count(&node.task_id);
                    pending.insert(node.task_id.clone(), pred_count);
                }
                self.orchestrator
                    .restore_workflow(&response.workflow_id, dag, exec, pending)
                    .await;
                tracing::info!(
                    "restored workflow {} from remote DAG (no execution state)",
                    response.workflow_id.as_str()
                );
            }
            _ => {
                tracing::warn!(
                    "incomplete state response for workflow {}, cannot restore",
                    response.workflow_id.as_str()
                );
            }
        }

        Ok(())
    }

    /// 以有限重试次数广播状态更新。
    /// 使用指数退避策略。
    async fn broadcast_with_retry(&self, topic: &str, data: Vec<u8>) -> crate::common::Result<()> {
        let mut last_err = None;
        for attempt in 0..self.retry_attempts {
            match self.network.broadcast(topic, data.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt + 1 < self.retry_attempts {
                        tracing::warn!(
                            "gossip broadcast to {} failed (attempt {}/{}): {}, retrying",
                            topic,
                            attempt + 1,
                            self.retry_attempts,
                            e
                        );
                        tokio::time::sleep(self.retry_base_delay * (attempt as u32 + 1)).await;
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            crate::common::ActantError::Internal(
                "broadcast_with_retry: no attempts made (retry_attempts=0)".into(),
            )
        }))
    }
}

/// 任务状态优先级。
/// 当 HLC 时间戳相同时，值较高的状态优先。
/// 终端状态（Completed、Failed、Cancelled、Skipped）始终覆盖 Running 状态，确保终端状态不会被过期更新覆盖。
fn state_priority(state: &WireTaskState) -> u8 {
    match state {
        WireTaskState::Completed { .. } => 5,
        WireTaskState::Failed { .. } => 4,
        WireTaskState::Cancelled => 3,
        WireTaskState::Skipped => 2,
        WireTaskState::Running => 1,
    }
}

impl DagGossip {
    fn evict_seen(&self) {
        let seen = &self.seen;
        if seen.len() <= self.dedup_window_size * 2 {
            return;
        }
        let now = std::time::Instant::now();
        let ttl = std::time::Duration::from_secs(self.dedup_ttl_secs);

        // 移除超过 TTL 的条目
        seen.retain(|_, entry| now.duration_since(entry.inserted_at) < ttl);

        if seen.len() > self.dedup_window_size {
            // 移除最旧条目以保持窗口大小
            let mut key_ages: Vec<_> = seen
                .iter()
                .map(|e| (e.key().clone(), e.value().inserted_at))
                .collect();
            key_ages.sort_by_key(|(_, t)| *t);
            let to_remove: Vec<_> = key_ages
                .iter()
                .take(seen.len() - self.dedup_window_size)
                .map(|(k, _)| k.clone())
                .collect();
            for key in to_remove {
                seen.remove(&key);
            }
        }
    }
}
