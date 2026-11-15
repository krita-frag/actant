use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crate::common::{
    ActorId, GossipConfig, HeadsExchange, NodeId, TaskCompletion, TaskId, WireDagStateUpdate,
    WireEnvelope, WireMessage, WireTaskState, WorkflowHead, WorkflowId, WorkflowStateRequest,
    WorkflowStateResponse, TOPIC_DAG_STATE, TOPIC_HEADS,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::network::Transport;
use crate::runtime::state::HybridLogicalClock;
use crate::runtime::workflow::actor::workflow_methods;
use crate::runtime::workflow::actor::{ResultSource, TaskResultOutcome};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct SeenKey(WorkflowId, TaskId);

#[derive(Clone)]
struct SeenEntry {
    inserted_at: std::time::Instant,
    hlc_timestamp: crate::runtime::state::HlcTimestamp,
    state_priority: u8,
}

/// 未完成的全量状态请求：用于重试与过期清理。
struct StateRequestState {
    target: NodeId,
    sent_at: std::time::Instant,
    attempts: u32,
}

const STATE_REQUEST_RETRY_AFTER: Duration = Duration::from_secs(3);
const STATE_REQUEST_MAX_ATTEMPTS: u32 = 5;

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
///
/// # 网络分区（split-brain）语义
///
/// Actant 使用 **最终一致性** 模型处理网络分区，不追求线性一致：
///
/// 1. **分区期间**：各分区的节点独立处理本地任务，DAG 状态更新通过 HLC
///    （混合逻辑时钟）打时间戳。分区期间 gossip 消息无法跨分区传递，
///    各分区维护各自的 `seen` 去重表与状态视图。
/// 2. **分区恢复后**：gossip 重新连通，节点间交换 `WireDagStateUpdate`。
///    `SeenEntry::is_superseded_by` 基于 HLC 时间戳与 `state_priority`
///    进行 CRDT 风格合并——HLC 较大者胜出；HLC 相同时 priority 高者胜出。
///    这保证了同一 task 的状态最终收敛到单一值，但**不保证**收敛到
///    "全局最新"的状态（取决于 HLC 与 priority 的组合）。
/// 3. **冲突任务结果**：若分区期间两个分区都执行了同一 task（failover
///    误判导致），恢复后两个结果都会通过 gossip 传播。先到达的结果被
///    `seen` 表记录，后到达的被视为重复而丢弃。**最终保留的结果取决于
///    到达顺序，而非结果正确性**——这是 Actant 选择最终一致性的代价。
/// 4. **工作流租约**：`FailoverConfig` 强制 `lease_duration_ms >
///    failure_timeout_ms`，确保故障检测完成前旧持有者的租约未过期，
///    避免分区期间双主同时操作同一工作流。分区恢复后，租约过期的节点
///    通过 `should_claim_workflow` 一致性哈希重新选举唯一持有者。
///
/// **用户责任**：对强一致性要求的任务（如金融交易），用户应在 task 内
/// 添加幂等性保护或使用外部协调器（如数据库事务），不应依赖 Actant 的
/// gossip 合并保证唯一执行。
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
    state_requests: Arc<DashMap<WorkflowId, StateRequestState>>,
}

impl Drop for DagGossip {
    fn drop(&mut self) {
        tracing::debug!("DagGossip::drop");
    }
}

impl DagGossip {
    /// 从任意 [`Transport`] 实现创建一个 `DagGossip`。
    ///
    /// `DagGossip` 不直接持有 `Orchestrator`，而是通过 `actor_system`
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
            state_requests: Arc::new(DashMap::new()),
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
    ///
    /// 若 WorkflowActor 返回 `NotFound` 错误（本节点未托管该 workflow），
    /// 则保留原始 `ActantError::NotFound` 类型，使调用方可通过
    /// `matches!(e, ActantError::NotFound(_))` 判定。
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
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg)?)
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

        // 去重登记放在 apply 成功之后：若 apply 失败（如 WorkflowActor 暂时
        // 不可用、序列化错误），不登记 seen，重传的更新不会被误判为重复，
        // 更新得以重放。`NotFound`（本节点未托管该 workflow）视为已处理。
        match self.apply_to_workflow(&update).await {
            Ok(()) => {
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
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// 将远端 DAG 状态更新落地到本地 WorkflowActor。
    ///
    /// 终态结果（Completed / Failed / Cancelled）统一经 `ON_TASK_RESULT`
    /// 单入口回灌——本方法只做结果同步通道，失败语义（FailureScope）与
    /// attempt fencing 均由入口裁决，apply 侧不做任何 scope 决策。
    ///
    /// `NotFound`（本节点未托管该 workflow）被规范化为 `Ok`：gossip 是广播
    /// 语义，节点不持有某 workflow 属正常情况。其他错误向上传播。
    async fn apply_to_workflow(&self, update: &WireDagStateUpdate) -> crate::common::Result<()> {
        match &update.task_state {
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
                let payload = (
                    &update.workflow_id,
                    &update.task_id,
                    TaskResultOutcome::Completed(result.clone()),
                    None::<u32>,
                    ResultSource::Gossip,
                );
                if let Err(e) = self
                    .call_workflow_void(workflow_methods::ON_TASK_RESULT, payload)
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
                let payload = (
                    &update.workflow_id,
                    &update.task_id,
                    TaskResultOutcome::Failed(error.clone()),
                    None::<u32>,
                    ResultSource::Gossip,
                );
                if let Err(e) = self
                    .call_workflow_void(workflow_methods::ON_TASK_RESULT, payload)
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
                let payload = (
                    &update.workflow_id,
                    &update.task_id,
                    TaskResultOutcome::Cancelled,
                    None::<u32>,
                    ResultSource::Gossip,
                );
                if let Err(e) = self
                    .call_workflow_void(workflow_methods::ON_TASK_RESULT, payload)
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
            // heads 只需要计数标量：GET_STATE 全量克隆结果字节（大结果工作流
            // 会超出 actor 消息 4MiB 上限），必须走 summary。单工作流查询失败
            // 只跳过该工作流——`?` 中止会让所有工作流的落后检测整体停摆。
            let result = match self
                .call_workflow(workflow_methods::GET_STATE_SUMMARY, wf_id)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id.as_str(),
                        error = %e,
                        "failed to fetch workflow summary for heads exchange; skipping"
                    );
                    continue;
                }
            };
            let summary: Option<
                crate::runtime::workflow::orchestrator::queries::WorkflowStateSummary,
            > = match crate::runtime::workflow::messaging::decode(&result.payload) {
                Ok(summary) => summary,
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id.as_str(),
                        error = %e,
                        "failed to decode workflow summary for heads exchange; skipping"
                    );
                    continue;
                }
            };
            if let Some(summary) = summary {
                let hlc_ts = self.clock.tick();
                heads.push(WorkflowHead {
                    workflow_id: wf_id.clone(),
                    succeeded_count: summary.succeeded_count,
                    total_count: summary.total_count,
                    hlc_timestamp: hlc_ts,
                });
            }
        }

        let exchange = HeadsExchange {
            node_id: self.node_id.clone(),
            heads,
        };
        let msg = WireMessage::HeadsExchange(exchange);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg)?)
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
            // 与发送侧同口径：比较只需计数标量，走 summary 避免 全量克隆 与
            // 4MiB 解码上限；失败按「未知工作流」不处理（下一轮 heads 重试）。
            let result = match self
                .call_workflow(
                    workflow_methods::GET_STATE_SUMMARY,
                    &remote_head.workflow_id,
                )
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    tracing::warn!(
                        workflow = %remote_head.workflow_id.as_str(),
                        error = %e,
                        "failed to fetch local summary for heads comparison; skipping"
                    );
                    continue;
                }
            };
            let local_summary: Option<
                crate::runtime::workflow::orchestrator::queries::WorkflowStateSummary,
            > = match crate::runtime::workflow::messaging::decode(&result.payload) {
                Ok(summary) => summary,
                Err(e) => {
                    tracing::warn!(
                        workflow = %remote_head.workflow_id.as_str(),
                        error = %e,
                        "failed to decode local summary for heads comparison; skipping"
                    );
                    continue;
                }
            };

            match local_summary {
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
                    self.ensure_state_request(&remote_head.workflow_id, &exchange.node_id)
                        .await?;
                }
                Some(local) => {
                    // 落后判据必须同时看节点数：flow 增量提交的新节点在它**成功**
                    // 之前不会改变 succeeded_count，只看成功数会让"仍在 Running
                    // 的任务"永远复制不到对端——编排节点失联后接管方手里没有可
                    // 重调度的任务，工作流停摆。
                    if remote_head.total_count > local.total_count
                        || remote_head.succeeded_count > local.succeeded_count
                    {
                        tracing::info!(
                            "workflow {} behind: local {}/{} < remote {}/{}, requesting state",
                            remote_head.workflow_id.as_str(),
                            local.succeeded_count,
                            local.total_count,
                            remote_head.succeeded_count,
                            remote_head.total_count
                        );
                        self.ensure_state_request(&remote_head.workflow_id, &exchange.node_id)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// 请求从远程节点获取完整的 DAG 状态（DAG +执行）。
    ///
    /// 请求走全局话题广播，目标节点由载荷里的 [`WorkflowStateRequest::target_node`]
    /// 指定。刻意不用「每节点一个话题」：运行期新建的话题拿不到已连接的邻居，
    /// `broadcast` 会返回成功却无人接收（接管方永远等不到状态副本）。
    pub async fn request_workflow_state(
        &self,
        workflow_id: &WorkflowId,
        target_node: &NodeId,
    ) -> crate::common::Result<()> {
        let request = WorkflowStateRequest {
            workflow_id: workflow_id.clone(),
            requesting_node: self.node_id.clone(),
            target_node: target_node.clone(),
        };
        let msg = WireMessage::WorkflowStateRequest(request);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg)?)
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        let topic = crate::common::Topic::workflow_state_req();
        let sent = self.network.broadcast(topic.as_str(), data).await;
        let attempts = self
            .state_requests
            .get(workflow_id)
            .map(|entry| entry.attempts.saturating_add(1))
            .unwrap_or(1);
        self.state_requests.insert(
            workflow_id.clone(),
            StateRequestState {
                target: target_node.clone(),
                sent_at: std::time::Instant::now(),
                attempts,
            },
        );
        // 控制面稀有事件（仅在发现未知/落后的工作流时触发），info 级即可观测，
        // 否则跨节点同步一旦静默失败，唯一线索是"本地副本永远不更新"。
        tracing::info!(
            workflow = %workflow_id.as_str(),
            target = %target_node.as_str(),
            ok = sent.is_ok(),
            "sent workflow state request"
        );
        sent
    }

    /// 有界地请求全量状态：同一目标在重试间隔内不重复发送；超过次数后
    /// 清理仍为空的占位符，避免 workflow 永久停在 Loading。
    async fn ensure_state_request(
        &self,
        workflow_id: &WorkflowId,
        target_node: &NodeId,
    ) -> crate::common::Result<()> {
        let now = std::time::Instant::now();
        let should_retry = match self.state_requests.get(workflow_id) {
            Some(entry)
                if entry.target == *target_node
                    && now.duration_since(entry.sent_at) < STATE_REQUEST_RETRY_AFTER =>
            {
                None
            }
            Some(entry) if entry.attempts >= STATE_REQUEST_MAX_ATTEMPTS => Some(false),
            _ => Some(true),
        };
        match should_retry {
            None => Ok(()),
            Some(true) => self.request_workflow_state(workflow_id, target_node).await,
            Some(false) => {
                self.state_requests.remove(workflow_id);
                self.clear_empty_placeholder(workflow_id).await;
                Ok(())
            }
        }
    }

    /// 若 workflow 仍无状态（仅占位符），删除它以便后续重新发现。
    async fn clear_empty_placeholder(&self, workflow_id: &WorkflowId) {
        let empty = match self
            .call_workflow(workflow_methods::GET_STATE, workflow_id)
            .await
        {
            Ok(result) => matches!(
                crate::runtime::workflow::messaging::decode::<
                    Option<crate::runtime::workflow::WorkflowExecution>,
                >(&result.payload),
                Ok(None)
            ),
            Err(_) => false,
        };
        if empty {
            if let Err(e) = self
                .call_workflow_void(workflow_methods::DELETE_WORKFLOW, workflow_id)
                .await
            {
                tracing::warn!(
                    workflow = %workflow_id.as_str(),
                    error = %e,
                    "failed to clear exhausted state-request placeholder"
                );
            }
        }
    }

    /// 周期性重试已超时但未耗尽次数的全量状态请求。
    pub async fn retry_pending_state_requests(&self) {
        let now = std::time::Instant::now();
        let due: Vec<(WorkflowId, NodeId)> = self
            .state_requests
            .iter()
            .filter(|entry| {
                entry.attempts < STATE_REQUEST_MAX_ATTEMPTS
                    && now.duration_since(entry.sent_at) >= STATE_REQUEST_RETRY_AFTER
            })
            .map(|entry| (entry.key().clone(), entry.target.clone()))
            .collect();
        for (workflow_id, target) in due {
            if let Err(e) = self.request_workflow_state(&workflow_id, &target).await {
                tracing::warn!(
                    workflow = %workflow_id.as_str(),
                    error = %e,
                    "failed to retry workflow state request"
                );
            }
        }
    }
    /// 处理从远程节点收到的 DAG 状态请求。
    ///
    /// 全局话题上的请求只有目标节点需要响应，其余节点直接丢弃。
    pub async fn handle_workflow_state_request(
        &self,
        request: &WorkflowStateRequest,
    ) -> crate::common::Result<()> {
        if request.target_node != self.node_id {
            return Ok(());
        }
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
            tracing::warn!(
                workflow = %request.workflow_id.as_str(),
                requester = %request.requesting_node.as_str(),
                "no state available for requested workflow; replying nothing"
            );
            return Ok(());
        }
        tracing::info!(
            workflow = %request.workflow_id.as_str(),
            requester = %request.requesting_node.as_str(),
            dag_bytes = dag_bytes.len(),
            exec_bytes = exec_bytes.len(),
            "replying with workflow state snapshot"
        );

        let response = WorkflowStateResponse {
            workflow_id: request.workflow_id.clone(),
            target_node: request.requesting_node.clone(),
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
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg)?)
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        let topic = crate::common::Topic::workflow_state_resp();
        let sent = self.network.broadcast(topic.as_str(), data).await;
        tracing::info!(
            workflow = %request.workflow_id.as_str(),
            target = %request.requesting_node.as_str(),
            ok = sent.is_ok(),
            "sent workflow state response"
        );
        sent
    }

    /// 处理从远程节点收到的 DAG 状态响应。
    ///
    /// 全局话题上的响应只有目标节点需要应用，其余节点直接丢弃
    /// （响应可能携带完整的 DAG/执行快照，不应落到无关节点上）。
    pub async fn handle_workflow_state_response(
        &self,
        response: &WorkflowStateResponse,
    ) -> crate::common::Result<()> {
        if response.target_node != self.node_id {
            return Ok(());
        }
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
        self.state_requests.remove(&response.workflow_id);

        tracing::info!(
            "restored workflow {} from remote state response",
            response.workflow_id.as_str()
        );
        Ok(())
    }

    /// 以有限重试次数广播状态更新。
    /// 使用指数退避策略。
    async fn broadcast_with_retry(&self, topic: &str, data: Vec<u8>) -> crate::common::Result<()> {
        let backoff = crate::common::backoff::ExponentialBackoff::new(
            self.retry_base_delay,
            crate::common::REMOTE_CALL_MAX_RETRY_DELAY,
        );
        let mut last_err = None;
        for attempt in 0..self.retry_attempts {
            match self.network.broadcast(topic, data.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt + 1 < self.retry_attempts {
                        let delay = backoff.delay_for(attempt as u32);
                        tracing::warn!(
                            "gossip broadcast to {} failed (attempt {}/{}): {}, retrying after {:?}",
                            topic,
                            attempt + 1,
                            self.retry_attempts,
                            e,
                            delay
                        );
                        tokio::time::sleep(delay).await;
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
/// 终端状态（Completed、Failed、Cancelled）始终覆盖 Running 状态，确保终端状态不会被过期更新覆盖。
fn state_priority(state: &WireTaskState) -> u8 {
    match state {
        WireTaskState::Completed { .. } => 4,
        WireTaskState::Failed { .. } => 3,
        WireTaskState::Cancelled => 2,
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
#[path = "../../../../../tests/rust/unit/runtime/workflow/gossip.rs"]
mod tests;
