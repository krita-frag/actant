use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crate::common::{
    ActorId, GossipConfig, HeadsExchange, NodeId, WireEnvelope, WireMessage, WorkflowHead,
    WorkflowId, WorkflowStateRequest, WorkflowStateResponse, TOPIC_HEADS,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::network::Transport;
use crate::runtime::workflow::actor::workflow_methods;

/// 未完成的全量状态请求：用于重试与过期清理。
struct StateRequestState {
    target: NodeId,
    sent_at: std::time::Instant,
    attempts: u32,
}

const STATE_REQUEST_RETRY_AFTER: Duration = Duration::from_secs(3);
const STATE_REQUEST_MAX_ATTEMPTS: u32 = 5;

/// 工作流状态复制协调器。
///
/// 持有一个类型擦除的 [`Transport`] (`Arc<dyn Transport>`)，使得同一个
/// `DagGossip` 类型与具体的传输实现解耦。
/// 运行时在启动时一次性选择具体的传输实现；下游代码
/// （orchestrator、worker、failover）将其视为黑盒。
///
/// # 复制模型
///
/// 只复制**快照**，不复制逐任务状态：`DagGossipActor` 周期性广播各活跃
/// 工作流的 heads（`workflow_id` + 总数/成功数），对端发现本地缺失或计数
/// 落后时，经全局 `actant:wf-state-req` / `actant:wf-state-resp` 请求并落地
/// 一份完整快照（DAG + 执行状态 + pending）。任务结果本身走直连
/// `DirectRequest::TaskResult`（带派发代数），不经过 gossip——否则同一份
/// 结果会有两条权威来源。
///
/// 快照比较只看计数标量：`total_count` 覆盖「新增但尚未成功的节点」，
/// `succeeded_count` 覆盖「已完成的推进」，两者取或即为落后判据。
///
/// # 网络分区（split-brain）语义
///
/// 1. **分区期间**：各分区的节点独立处理本地任务，互不可见。
/// 2. **分区恢复后**：heads 交换重新连通，落后方按计数差异拉取快照。
///    合并语义见 `Orchestrator::restore_workflow`：本地终态不回退，双方
///    都非终态时保留本地（避免旧快照把 Running 打回 Pending）。
/// 3. **工作流租约**：`FailoverConfig` 强制 `lease_duration_ms >
///    failure_timeout_ms`，确保故障检测完成前旧持有者的租约未过期。
///    分区恢复后，租约过期的节点通过 `should_claim_workflow` 一致性哈希
///    重新选举唯一持有者。
///
/// **用户责任**：对强一致性要求的任务（如金融交易），用户应在 task 内
/// 添加幂等性保护或使用外部协调器（如数据库事务），不应依赖 Actant 的
/// 最终一致复制保证唯一执行。
#[derive(Clone)]
pub struct DagGossip {
    network: Arc<dyn Transport>,
    actor_system: Arc<ActorSystem>,
    workflow_actor_id: ActorId,
    node_id: NodeId,
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
            node_id,
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
                heads.push(WorkflowHead {
                    workflow_id: wf_id.clone(),
                    succeeded_count: summary.succeeded_count,
                    total_count: summary.total_count,
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
}

#[cfg(test)]
#[path = "../../../../../tests/rust/unit/runtime/workflow/gossip.rs"]
mod tests;
