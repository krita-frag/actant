use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crate::common::{
    ActorId, GossipConfig, HeadsExchange, NodeId, TaskCompletion, TaskId, WireDagStateUpdate,
    WireEnvelope, WireMessage, WireTaskState, WorkflowHead, WorkflowId, WorkflowStateRequest,
    WorkflowStateResponse, TOPIC_DAG_STATE, TOPIC_HEADS, TOPIC_WORKFLOW_STATE_REQ,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::network::Transport;
use crate::runtime::state::HybridLogicalClock;
use crate::runtime::workflow::actor::workflow_methods;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct SeenKey(WorkflowId, TaskId);

#[derive(Clone)]
struct SeenEntry {
    inserted_at: std::time::Instant,
    hlc_timestamp: crate::runtime::state::HlcTimestamp,
    state_priority: u8,
}

impl SeenEntry {
    /// 如果传入的更新应该覆盖当前条目，则返回 true。
    fn is_superseded_by(
        &self,
        incoming_hlc: &crate::runtime::state::HlcTimestamp,
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
    actor_system: Arc<ActorSystem>,
    workflow_actor_id: ActorId,
    seen: Arc<DashMap<SeenKey, SeenEntry>>,
    clock: Arc<HybridLogicalClock>,
    node_id: NodeId,
    dedup_window_size: usize,
    dedup_ttl_secs: u64,
    retry_attempts: usize,
    retry_base_delay: Duration,
    heads_broadcast_interval: Duration,
}

impl Drop for DagGossip {
    fn drop(&mut self) {
        tracing::debug!("DagGossip::drop");
    }
}

impl DagGossip {
    /// 从任意 [`Transport`] 实现创建一个 `DagGossip`。
    ///
    /// `DagGossip` 不再直接持有 [`Orchestrator`]，而是通过 `actor_system`
    /// 向 `WorkflowActor` 发送消息，使编排器状态完全由 Actor 独占持有。
    pub fn new(
        network: Arc<dyn Transport>,
        actor_system: Arc<ActorSystem>,
        workflow_actor_id: ActorId,
        config: GossipConfig,
    ) -> Self {
        let node_id = network.node_id().clone();
        Self {
            network,
            actor_system,
            workflow_actor_id,
            seen: Arc::new(DashMap::new()),
            clock: Arc::new(HybridLogicalClock::new()),
            node_id,
            dedup_window_size: config.dedup_window_size,
            dedup_ttl_secs: config.dedup_ttl_secs,
            retry_attempts: config.retry_attempts,
            retry_base_delay: Duration::from_millis(config.retry_base_delay_ms),
            heads_broadcast_interval: Duration::from_millis(config.heads_broadcast_interval_ms),
        }
    }

    pub fn heads_broadcast_interval(&self) -> Duration {
        self.heads_broadcast_interval
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn actor_system(&self) -> &Arc<ActorSystem> {
        &self.actor_system
    }

    /// 向 WorkflowActor 发起调用并反序列化返回结果。
    async fn call_workflow<T: serde::Serialize>(
        &self,
        method: &str,
        payload: T,
    ) -> crate::common::Result<crate::common::ActorMessageResult> {
        let bytes = crate::runtime::workflow::messaging::encode(&payload)?;
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

        // 广播路径同样需要淘汰，否则只发不收的节点（如 workflow owner）
        // 的 seen 表会随不同 workflow/task 无限增长。
        self.evict_seen();

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
                    .call_workflow_void(
                        workflow_methods::MARK_TASK_RUNNING,
                        (&update.workflow_id, &update.task_id),
                    )
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
                    .call_workflow_void(
                        workflow_methods::COMPLETE_TASK,
                        (&update.workflow_id, &update.task_id, result),
                    )
                    .await
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
                    .call_workflow_void(
                        workflow_methods::FAIL_TASK,
                        (
                            &update.workflow_id,
                            &update.task_id,
                            error,
                            crate::runtime::workflow::FailureScope::WorkflowLevel,
                        ),
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
                    .call_workflow_void(
                        workflow_methods::CANCEL_TASK,
                        (&update.workflow_id, &update.task_id),
                    )
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
                    .call_workflow_void(
                        workflow_methods::SKIP_CONDITIONAL_BRANCH,
                        (&update.workflow_id, &update.task_id),
                    )
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
        let active_ids: Vec<WorkflowId> = {
            let result = self
                .call_workflow(workflow_methods::ACTIVE_WORKFLOW_IDS, ())
                .await?;
            crate::runtime::workflow::messaging::decode(&result.payload)?
        };
        let mut heads = Vec::with_capacity(active_ids.len());

        for wf_id in &active_ids {
            let result = self
                .call_workflow(workflow_methods::GET_STATE, wf_id)
                .await?;
            let exec: Option<crate::runtime::workflow::WorkflowExecution> =
                crate::runtime::workflow::messaging::decode(&result.payload)?;
            if let Some(exec) = exec {
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
            let result = self
                .call_workflow(workflow_methods::GET_STATE, &remote_head.workflow_id)
                .await?;
            let local_exec: Option<crate::runtime::workflow::WorkflowExecution> =
                crate::runtime::workflow::messaging::decode(&result.payload)?;

            match local_exec {
                None => {
                    tracing::info!(
                        "discovered unknown workflow {} from node {}, requesting full state",
                        remote_head.workflow_id.as_str(),
                        exchange.node_id.as_str()
                    );
                    self.call_workflow_void(
                        workflow_methods::ADOPT_WORKFLOW,
                        &remote_head.workflow_id,
                    )
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
            let result = self
                .call_workflow(
                    workflow_methods::GET_WORKFLOW_STATE_BYTES,
                    &request.workflow_id,
                )
                .await?;
            crate::runtime::workflow::messaging::decode::<Option<(Vec<u8>, Vec<u8>, Vec<u8>)>>(
                &result.payload,
            )?
            .unwrap_or((Vec::new(), Vec::new(), Vec::new()))
        };

        if dag_bytes.is_empty() && exec_bytes.is_empty() {
            tracing::debug!(
                "no state available for workflow {} requested by {}",
                request.workflow_id.as_str(),
                request.requesting_node.as_str()
            );
            return Ok(());
        }

        let response = WorkflowStateResponse {
            workflow_id: request.workflow_id.clone(),
            dag: if dag_bytes.is_empty() {
                None
            } else {
                Some(dag_bytes)
            },
            execution: if exec_bytes.is_empty() {
                None
            } else {
                Some(exec_bytes)
            },
            pending: if pending_bytes.is_empty() {
                None
            } else {
                Some(pending_bytes)
            },
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
        if response.dag.is_none() && response.execution.is_none() {
            tracing::warn!(
                "incomplete state response for workflow {}, cannot restore",
                response.workflow_id.as_str()
            );
            return Ok(());
        }

        self.call_workflow_void(
            workflow_methods::APPLY_FULL_STATE,
            (
                &response.workflow_id,
                response.dag.clone(),
                response.execution.clone(),
                response.pending.clone(),
            ),
        )
        .await?;

        tracing::info!(
            "restored workflow {} from remote state response",
            response.workflow_id.as_str()
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{GossipConfig, NodeId, TaskId, WireTaskState, WorkflowId};
    use crate::runtime::actor::ActorSystem;
    use crate::runtime::state::HlcTimestamp;
    use crate::test_support::MockTransport;

    fn make_gossip(node_id: &str, config: GossipConfig) -> DagGossip {
        let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
        let actor_system = Arc::new(ActorSystem::new());
        let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
        DagGossip::new(network, actor_system, wf_id, config)
    }

    fn make_entry(ts: u64, prio: u8) -> SeenEntry {
        SeenEntry {
            inserted_at: std::time::Instant::now(),
            hlc_timestamp: HlcTimestamp::from_parts(ts, 0),
            state_priority: prio,
        }
    }

    #[test]
    fn state_priority_orders_terminal_above_running() {
        assert_eq!(state_priority(&WireTaskState::Running), 1);
        assert_eq!(state_priority(&WireTaskState::Skipped), 2);
        assert_eq!(state_priority(&WireTaskState::Cancelled), 3);
        assert_eq!(
            state_priority(&WireTaskState::Failed { error: "e".into() }),
            4
        );
        assert_eq!(
            state_priority(&WireTaskState::Completed { result: Vec::new() }),
            5
        );
    }

    #[test]
    fn seen_entry_superseded_by_higher_hlc() {
        let entry = make_entry(10, 3);
        assert!(entry.is_superseded_by(&HlcTimestamp::from_parts(20, 0), 1));
    }

    #[test]
    fn seen_entry_not_superseded_by_lower_hlc() {
        let entry = make_entry(20, 1);
        assert!(!entry.is_superseded_by(&HlcTimestamp::from_parts(10, 0), 5));
    }

    #[test]
    fn seen_entry_superseded_by_equal_hlc_and_higher_or_equal_prio() {
        let entry = make_entry(10, 3);
        // 同 HLC、更高 prio → 覆盖
        assert!(entry.is_superseded_by(&HlcTimestamp::from_parts(10, 0), 4));
        // 同 HLC、同 prio → 覆盖（>=）
        assert!(entry.is_superseded_by(&HlcTimestamp::from_parts(10, 0), 3));
        // 同 HLC、更低 prio → 不覆盖
        assert!(!entry.is_superseded_by(&HlcTimestamp::from_parts(10, 0), 2));
    }

    #[test]
    fn getters_return_configured_values() {
        let cfg = GossipConfig {
            dedup_window_size: 50,
            dedup_ttl_secs: 30,
            retry_attempts: 4,
            retry_base_delay_ms: 200,
            heads_broadcast_interval_ms: 1500,
        };
        let g = make_gossip("node-A", cfg);
        assert_eq!(g.node_id().as_str(), "node-A");
        assert_eq!(g.heads_broadcast_interval(), Duration::from_millis(1500));
        let (seen, window, ttl, retries) = g.dedup_stats();
        assert_eq!(seen, 0);
        assert_eq!(window, 50);
        assert_eq!(ttl, 30);
        assert_eq!(retries, 4);
    }

    #[test]
    fn seen_count_tracks_inserts() {
        let g = make_gossip("node-A", GossipConfig::default());
        assert_eq!(g.seen_count(), 0);
        let key = SeenKey(
            WorkflowId("wf-1".to_string()),
            TaskId::from("t-1".to_string()),
        );
        g.seen.insert(key, make_entry(5, 1));
        assert_eq!(g.seen_count(), 1);
    }

    #[test]
    fn evict_seen_noop_below_window_threshold() {
        // dedup_window_size * 2 是触发阈值；小于该值不应清理。
        let cfg = GossipConfig {
            dedup_window_size: 10,
            dedup_ttl_secs: 60,
            retry_attempts: 1,
            retry_base_delay_ms: 1,
            heads_broadcast_interval_ms: 1000,
        };
        let g = make_gossip("node-A", cfg);
        for i in 0..15 {
            let key = SeenKey(
                WorkflowId(format!("wf-{}", i)),
                TaskId::from(format!("t-{}", i)),
            );
            g.seen.insert(key, make_entry(i as u64, 1));
        }
        g.evict_seen();
        // 15 < 10*2=20，不应触发清理
        assert_eq!(g.seen_count(), 15);
    }

    #[test]
    fn evict_seen_trims_to_window_size_when_exceeding_double() {
        let cfg = GossipConfig {
            dedup_window_size: 5,
            dedup_ttl_secs: 60,
            retry_attempts: 1,
            retry_base_delay_ms: 1,
            heads_broadcast_interval_ms: 1000,
        };
        let g = make_gossip("node-A", cfg);
        // 插入 12 个条目（> 5*2=10），触发清理。
        for i in 0..12 {
            let key = SeenKey(
                WorkflowId(format!("wf-{}", i)),
                TaskId::from(format!("t-{}", i)),
            );
            g.seen.insert(
                key,
                SeenEntry {
                    // 递增 inserted_at，使最旧的被先驱逐
                    inserted_at: std::time::Instant::now()
                        - std::time::Duration::from_millis(100 - i as u64),
                    hlc_timestamp: HlcTimestamp::from_parts(i as u64, 0),
                    state_priority: 1,
                },
            );
        }
        g.evict_seen();
        assert!(
            g.seen_count() <= 5,
            "expected <= 5 after eviction, got {}",
            g.seen_count()
        );
    }

    #[test]
    fn apply_remote_update_drops_stale_lower_hlc() {
        // 先写入高 HLC 的终态，再尝试用低 HLC 的 Running 覆盖 — 应被丢弃。
        let g = make_gossip("node-A", GossipConfig::default());
        let wf = WorkflowId("wf-1".to_string());
        let task = TaskId::from("t-1".to_string());

        let newer = WireDagStateUpdate {
            workflow_id: wf.clone(),
            task_id: task.clone(),
            task_state: WireTaskState::Completed {
                result: vec![1, 2, 3],
            },
            hlc_timestamp: HlcTimestamp::from_parts(100, 5),
            origin_node: NodeId::from("node-B".to_string()),
        };
        // 直接操作 seen 表验证 CRDT 逻辑（不调用 actor）。
        let key = SeenKey(wf.clone(), task.clone());
        g.seen.insert(
            key.clone(),
            SeenEntry {
                inserted_at: std::time::Instant::now(),
                hlc_timestamp: newer.hlc_timestamp,
                state_priority: state_priority(&newer.task_state),
            },
        );
        assert_eq!(g.seen_count(), 1);

        // 构造一个更老的 Running 更新，应被丢弃（不增加 seen，CRDT 保留高优先级）。
        let older = WireDagStateUpdate {
            workflow_id: wf,
            task_id: task,
            task_state: WireTaskState::Running,
            hlc_timestamp: HlcTimestamp::from_parts(50, 0),
            origin_node: NodeId::from("node-C".to_string()),
        };
        // 直接复用 apply_remote_update 的 dedup 判定逻辑（不调用 actor）：
        let existing = g.seen.get(&key).unwrap();
        assert!(!existing.is_superseded_by(&older.hlc_timestamp, state_priority(&older.task_state)));
        drop(existing);
        assert_eq!(g.seen_count(), 1);
        // 保留旧值不被覆盖
        let entry = g.seen.get(&key).unwrap();
        assert_eq!(entry.state_priority, 5);
    }

    #[test]
    fn clock_tick_monotonic() {
        let g = make_gossip("node-A", GossipConfig::default());
        let t1 = g.clock().tick();
        let t2 = g.clock().tick();
        assert!(t2 > t1, "HLC tick must be monotonic: {:?} -> {:?}", t1, t2);
    }

    #[test]
    fn clock_merges_remote_timestamp() {
        let g = make_gossip("node-A", GossipConfig::default());
        let remote = HlcTimestamp::from_parts(999, 9);
        g.clock().merge(&remote);
        let local = g.clock().tick();
        assert!(local > remote, "after merge, local tick must exceed remote");
    }
}
