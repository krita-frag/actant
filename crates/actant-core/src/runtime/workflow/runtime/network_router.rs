//! Worker 网络事件路由器。
//!
//! Worker 主循环只负责执行任务；所有 topic 分类、wire message 解码、直连请求
//! 分发和事件总线转发都集中在本模块。这样可以把“网络输入如何变成 runtime 事件”
//! 与“任务如何执行”分开阅读和测试。
//!
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::common::{
    NodeId, TaskCompletion, TaskId, Topic, WireEnvelope, WireMessage, WireTaskOutcome, WorkflowId,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::dispatcher::CancelFlag;
use crate::runtime::events::{EventTap, TapEvent, TaskLifecycleEvent, TaskResultFeed};
use crate::runtime::network::{DirectResponseChannel, NetworkEvent, Transport};
use crate::runtime::workflow::actor::{ResultSource, TaskResultOutcome};
use crate::runtime::workflow::messaging::{decode, encode};
use crate::runtime::workflow::Scheduler;
use tracing::Instrument;

/// 将传入的网络事件路由到适当的处理程序。
///
/// 封装了主题匹配和反序列化逻辑，使运行时主循环专注于任务执行。
pub(crate) struct NetworkEventRouterConfig {
    pub(crate) network: Arc<dyn Transport>,
    /// 观测 tap：对端连接事件。
    pub(crate) tap: EventTap,
    /// 任务结果 correctness 通道：远端取消 / 远端结果回灌。
    pub(crate) result_feed: TaskResultFeed,
    pub(crate) scheduler: Arc<dyn Scheduler>,
    pub(crate) actor_system: Option<Arc<ActorSystem>>,
    /// 本地 WorkflowActor 的 id。若存在，远程 TaskResult 会直接路由给它。
    pub(crate) workflow_actor_id: Option<crate::common::ActorId>,
    /// 本地 DagGossipActor 的 id。若存在，工作流状态请求/响应会直接路由给它。
    pub(crate) dag_gossip_actor_id: Option<crate::common::ActorId>,
    /// 故障转移管理器。若存在，入站 Heartbeat / Claim wire 消息会直连分发到
    /// `handle_heartbeat` / `handle_claim`（控制面指令不经过事件通道）。
    pub(crate) failover: Option<Arc<crate::runtime::workflow::FailoverManager>>,
    pub(crate) cancel_flags: Arc<parking_lot::Mutex<HashMap<String, CancelFlag>>>,
    pub(crate) cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
}

pub(crate) struct NetworkEventRouter {
    network: Arc<dyn Transport>,
    tap: EventTap,
    result_feed: TaskResultFeed,
    scheduler: Arc<dyn Scheduler>,
    actor_system: Option<Arc<ActorSystem>>,
    workflow_actor_id: Option<crate::common::ActorId>,
    dag_gossip_actor_id: Option<crate::common::ActorId>,
    failover: Option<Arc<crate::runtime::workflow::FailoverManager>>,
    cancel_flags: Arc<parking_lot::Mutex<HashMap<String, CancelFlag>>>,
    cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
}

impl NetworkEventRouter {
    pub(crate) fn new(cfg: NetworkEventRouterConfig) -> Self {
        Self {
            network: cfg.network,
            tap: cfg.tap,
            result_feed: cfg.result_feed,
            scheduler: cfg.scheduler,
            actor_system: cfg.actor_system,
            workflow_actor_id: cfg.workflow_actor_id,
            dag_gossip_actor_id: cfg.dag_gossip_actor_id,
            failover: cfg.failover,
            cancel_flags: cfg.cancel_flags,
            cancelled_tasks: cfg.cancelled_tasks,
        }
    }

    /// 将单个 `NetworkEvent` 分发给适当的处理程序。
    pub(crate) async fn handle(&self, event: NetworkEvent) {
        match event {
            NetworkEvent::Message(msg) => {
                self.handle_message(&msg.topic, &msg.data).await;
            }
            NetworkEvent::PeerConnected { peer_id } => {
                tracing::info!("peer connected: {}", peer_id);
                crate::metrics::inc_connected_peers();
                self.tap
                    .publish(TapEvent::PeerConnected(NodeId(peer_id.clone())));
            }
            NetworkEvent::PeerDisconnected { peer_id } => {
                tracing::info!("peer disconnected: {}", peer_id);
                crate::metrics::dec_connected_peers();
                self.tap
                    .publish(TapEvent::PeerDisconnected(NodeId(peer_id)));
            }
            NetworkEvent::DirectRequest {
                peer_id,
                request,
                channel,
            } => {
                self.handle_direct_request(peer_id, *request, channel).await;
            }
        }
    }

    pub(crate) async fn handle_message(&self, topic_str: &str, payload: &[u8]) {
        let topic = Topic::from(topic_str);
        // 解码一次后在整个分发过程中复用。失败时直接返回（避免每个分支重复 decode）。
        // `Cancel` 话题同样走 `WireEnvelope`（签名集群下随 MAC 验证）：取消广播
        // 若绕过 MAC，任意 mesh 成员可伪造 `(task_id, workflow_id)` 取消任意任务。
        let decoded = WireEnvelope::decode(payload);
        let traceparent = decoded.as_ref().and_then(|(_, tp)| tp.clone());
        // 解析入站 W3C traceparent，若成功则：
        //   1. 创建 `wire.recv` span，把 traceparent 字符串与解析出的 trace-id/span-id
        //      记录为 span field，便于日志检索与 OTLP 桥接；
        //   2. 通过 `current_trace_scope` 把入站 TraceContext 压入 thread-local，
        //      使本 span 内（同步代码路径）调用的 `WireEnvelope::wrap()` 能生成
        //      child traceparent（继承 trace-id、生成新 span-id），实现多跳链路
        //      trace-id 延续。
        //
        // 失败时（旧版本节点、不支持 trace 传播或解析错误）创建独立 span，
        // thread-local 保持空，wrap() 退化为生成 root trace。
        let parsed_ctx = traceparent
            .as_ref()
            .and_then(|tp| crate::common::wire::TraceContext::parse(tp));

        let span = tracing::info_span!(
            "wire.recv",
            topic = %topic_str,
            wire.traceparent = tracing::field::Empty,
            wire.trace_id = tracing::field::Empty,
            wire.span_id = tracing::field::Empty,
        );
        if let Some(ref tp) = traceparent {
            span.record("wire.traceparent", tp);
        }
        if let Some(ref ctx) = parsed_ctx {
            span.record(
                "wire.trace_id",
                tracing::field::display(crate::common::wire::traceparent::HexDisplay(
                    &ctx.trace_id,
                )),
            );
            span.record(
                "wire.span_id",
                tracing::field::display(crate::common::wire::traceparent::HexDisplay(&ctx.span_id)),
            );
        }
        // 不使用 `span.enter()`：其 guard 若跨 await 持有，await 期间同一线程上
        // 轮询的其他 task 会被错误地归入本 span。改用 `Instrument`：span 只在
        // 分发 future 轮询期间生效，await 挂起时自动退出。
        self.dispatch_topic(&topic, decoded, payload, parsed_ctx)
            .instrument(span)
            .await;
    }

    /// 按话题分类分发已解码的 wire message。
    ///
    /// 在调用方 `wire.recv` span 的 `Instrument` 作用域内执行；`parsed_ctx`
    /// 在此同步压入 thread-local（guard 生命周期等同本 future），
    /// 使分发路径中的同步 `WireEnvelope::wrap()` 调用延续入站 trace。
    async fn dispatch_topic(
        &self,
        topic: &Topic,
        decoded: Option<(WireMessage, Option<String>)>,
        payload: &[u8],
        parsed_ctx: Option<crate::common::wire::TraceContext>,
    ) {
        let _scope = parsed_ctx.map(crate::common::wire::current_trace_scope);

        match topic.classify() {
            crate::common::TopicRoute::Task(_) => {
                if let Some((WireMessage::TaskDispatch(task), _)) = decoded {
                    // task 已按值 move 出消息，无需克隆整份 payload 入队；
                    // 先留下标识再交出所有权——入队失败即任务被丢弃，必须以
                    // error 级别留下任务标识，否则任务在日志中完全消失（提交方
                    // 只能观察到永久等待）。
                    tracing::debug!(task_id = %task.id.as_str(), "enqueuing gossip-dispatched task");
                    if let Err(e) = self.scheduler.enqueue(task).await {
                        tracing::error!(error = %e, "scheduler rejected task dispatch; task dropped");
                    }
                }
            }
            crate::common::TopicRoute::Heartbeat => {
                if let Some((WireMessage::NodeHeartbeat(hb), _)) = decoded {
                    match &self.failover {
                        Some(failover) => {
                            // 控制面直连：handle_heartbeat 更新 peer 表并随心跳
                            // 刷新容量视图（available/max slots、endpoint），
                            // 驱动远端转发与故障检测/接管选举。
                            failover.handle_heartbeat(&hb);
                        }
                        None => {
                            tracing::warn!(
                                heartbeat_node = %hb.node_id.0,
                                "no failover manager configured; dropping inbound heartbeat"
                            );
                        }
                    }
                } else {
                    tracing::warn!(
                        "heartbeat topic but failed to unwrap envelope, payload len={}",
                        payload.len()
                    );
                }
            }
            crate::common::TopicRoute::Failover => {
                match decoded {
                    Some((WireMessage::SignedOrchestratorClaim(signed), _)) => {
                        match &self.failover {
                            Some(failover) => failover.handle_signed_claim(&signed).await,
                            None => tracing::warn!(
                                claim_node = %signed.claim.node_id.0,
                                workflow_id = %signed.claim.workflow_id.as_str(),
                                "no failover manager configured; dropping inbound claim"
                            ),
                        }
                    }
                    Some((WireMessage::OrchestratorClaim(claim), _)) => {
                        match &self.failover {
                            Some(failover) => {
                                // 控制面直连：更新本地租约表，非本节点 claim 时移除
                                // 本地 active 状态，构成双主防护。
                                failover.handle_claim(&claim).await;
                            }
                            None => tracing::warn!(
                                claim_node = %claim.node_id.0,
                                workflow_id = %claim.workflow_id.as_str(),
                                "no failover manager configured; dropping inbound claim"
                            ),
                        }
                    }
                    _ => tracing::warn!("failover topic but failed to unwrap claim envelope"),
                }
            }
            crate::common::TopicRoute::Heads => {
                if let Some((WireMessage::HeadsExchange(exchange), _)) = decoded {
                    // 控制面直连：workflow 发现 / adopt / 按需请求全量状态。
                    self.handle_workflow_state_event(
                        crate::runtime::workflow::gossip_methods::HANDLE_HEADS_EXCHANGE,
                        &exchange,
                    )
                    .await;
                }
            }
            crate::common::TopicRoute::WorkflowStateReq => {
                if let Some((WireMessage::WorkflowStateRequest(request), _)) = decoded {
                    self.handle_workflow_state_event(
                        crate::runtime::workflow::gossip_methods::HANDLE_WORKFLOW_STATE_REQUEST,
                        &request,
                    )
                    .await;
                }
            }
            crate::common::TopicRoute::WorkflowStateResp => {
                if let Some((WireMessage::WorkflowStateResponse(response), _)) = decoded {
                    self.handle_workflow_state_event(
                        crate::runtime::workflow::gossip_methods::HANDLE_WORKFLOW_STATE_RESPONSE,
                        &response,
                    )
                    .await;
                }
            }
            crate::common::TopicRoute::Cancel => {
                // 经 envelope 解出（MAC 已在 decode 中校验）；裸 postcard 载荷
                // 是旧旁路格式，一律丢弃。
                let cancel_msg = decoded.as_ref().and_then(|(msg, _)| match msg {
                    WireMessage::CancelBroadcast(m) => Some(m.clone()),
                    _ => None,
                });
                if let Some(msg) = cancel_msg {
                    tracing::info!(
                        task_id = %msg.task_id,
                        workflow_id = %msg.workflow_id,
                        "received remote cancel broadcast"
                    );
                    {
                        let flags = self.cancel_flags.lock();
                        if let Some(flag) = flags.get(msg.task_id.as_str()) {
                            flag.store(true, std::sync::atomic::Ordering::Release);
                        }
                    }
                    {
                        let mut cancelled_tasks = self.cancelled_tasks.lock();
                        // 仅在新插入时增加计数；已存在的条目为重复取消，不重复计数。
                        // 口径与本地 cancel_task 一致：确认的取消（此处 = 新登记）。
                        if cancelled_tasks
                            .insert(msg.task_id.to_string(), Instant::now())
                            .is_none()
                        {
                            crate::metrics::inc_cancelled_tasks_pending();
                            crate::metrics::inc_tasks_cancelled();
                        }
                    }
                    // 请求腿到此为止：取消的**终态结算**由各执行路径统一承担——
                    // 在途任务经 cancel_flag 协作检查点走 publish_task_completion
                    //（远端直连回传 origin / 本地 correctness 通道），排队任务经
                    // cancelled_tasks 注册表被派发前短路后同样补发终态。此处再发
                    // 一条 TaskCancelled 会产生重复终态事件，且 CancelBroadcast
                    // 不携带 task_name（塞 task_id 顶替会污染下游日志）。
                }
            }
            crate::common::TopicRoute::Unknown => {}
        }
    }

    async fn handle_direct_request(
        &self,
        _peer_id: String,
        request: crate::runtime::network::DirectRequest,
        channel: DirectResponseChannel,
    ) {
        match request {
            crate::runtime::network::DirectRequest::DispatchTask { task } => {
                // 先留标识再交出所有权（无需为日志克隆整份 payload）。
                let (task_id_dbg, task_name_dbg) =
                    (task.id.as_str().to_string(), task.name.clone());
                let accepted = match self.scheduler.enqueue(task).await {
                    Ok(()) => true,
                    Err(e) => {
                        // 直连派发被拒即任务丢弃：以 error 级别记录任务标识，
                        // 便于与提交方的"任务永久等待"告警关联排查。
                        tracing::error!(
                            task_id = %task_id_dbg,
                            task_name = %task_name_dbg,
                            error = %e,
                            "scheduler rejected direct task dispatch; task dropped"
                        );
                        false
                    }
                };
                let response = crate::runtime::network::DirectResponse::DispatchAck { accepted };
                if let Err(e) = self.network.send_direct_response(channel, response).await {
                    tracing::warn!("failed to send DispatchAck: {}", e);
                }
            }
            crate::runtime::network::DirectRequest::TaskResult {
                workflow_id,
                task_id,
                task_name,
                outcome,
                worker_node,
                attempt,
            } => {
                let accepted = self
                    .handle_task_result(
                        workflow_id,
                        task_id,
                        task_name,
                        outcome,
                        worker_node,
                        attempt,
                    )
                    .await;
                let response = crate::runtime::network::DirectResponse::TaskResultAck { accepted };
                if let Err(e) = self.network.send_direct_response(channel, response).await {
                    tracing::warn!("failed to send TaskResultAck: {}", e);
                }
            }
            crate::runtime::network::DirectRequest::Ping => {
                // 存活探测：无副作用，唯一作用是让探测方区分"对端仍在线"与
                // "对端已失联"。供故障转移的在途转发腿使用（心跳视图不可用时）。
                if let Err(e) = self
                    .network
                    .send_direct_response(channel, crate::runtime::network::DirectResponse::Pong)
                    .await
                {
                    tracing::warn!("failed to send Pong: {}", e);
                }
            }
        }
    }

    /// 将工作流状态请求/响应事件路由到本地 DagGossipActor。
    async fn handle_workflow_state_event<T: serde::Serialize>(&self, method: &str, value: &T) {
        let Some(ref actor_system) = self.actor_system else {
            tracing::warn!("no actor system available to handle workflow state event");
            return;
        };
        let Some(ref dag_gossip_actor_id) = self.dag_gossip_actor_id else {
            tracing::warn!("no dag gossip actor configured to handle workflow state event");
            return;
        };

        let payload = match encode(value) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("failed to encode workflow state event: {}", e);
                return;
            }
        };

        if let Err(e) = actor_system
            .call(dag_gossip_actor_id, method, payload)
            .await
        {
            tracing::error!(
                method = %method,
                error = %e,
                "failed to dispatch workflow state event to dag gossip actor"
            );
        }
    }

    /// 将远程 TaskResult 路由到本地 WorkflowActor。
    ///
    /// 统一经 `ON_TASK_RESULT` 单入口回灌：失败语义（FailureScope）与
    /// attempt fencing 与失败语义均由 `WorkflowActor::on_task_result` 裁决，
    /// 本路径不做 scope 决策。返回 `true` 表示已成功提交给 WorkflowActor（不保证
    /// workflow 状态更新成功）。
    pub(super) async fn handle_task_result(
        &self,
        workflow_id: WorkflowId,
        task_id: TaskId,
        task_name: String,
        outcome: WireTaskOutcome,
        worker_node: NodeId,
        attempt: u32,
    ) -> bool {
        if workflow_id.as_str().is_empty() {
            // 直提任务的结果按 attempt fencing：比登记更旧的迟到结果不得被接受，
            // 也不得移除新 attempt 的在途登记。执行节点崩溃转移会自行推进 attempt，
            // 那种「比登记更新」的结果是当前有效结果，必须接受。
            if let Some(failover) = self.failover.as_ref() {
                if failover
                    .take_outbound_if_not_stale(task_id.as_str(), attempt)
                    .is_none()
                {
                    tracing::warn!(
                        task_id = %task_id.as_str(),
                        attempt,
                        worker = %worker_node.as_str(),
                        "rejecting stale standalone task result"
                    );
                    // 旧 attempt 已作废：确认收到以停止 executor 重试，
                    // 同时保留当前 attempt 的在途登记（不在此清除）。
                    return true;
                }
            }
            self.publish_remote_completion(
                &workflow_id,
                &task_id,
                &task_name,
                &outcome,
                &worker_node,
                attempt,
            )
            .await;
            return true;
        }

        // 远端结果按 attempt fencing：早于当前在途登记的旧结果直接确认丢弃
        // （返回 true 以停止 executor 重试），且不得清除当前 attempt 的登记。
        if let Some(failover) = self.failover.as_ref() {
            if failover
                .outbound_attempt(task_id.as_str())
                .is_some_and(|current| current > attempt)
            {
                tracing::warn!(
                    task_id = %task_id.as_str(),
                    attempt,
                    "dropping stale workflow task result (newer attempt in flight)"
                );
                return true;
            }
        }

        let Some(ref actor_system) = self.actor_system else {
            tracing::debug!("no actor system; publishing remote task result to result feed");
            self.publish_remote_completion(
                &workflow_id,
                &task_id,
                &task_name,
                &outcome,
                &worker_node,
                attempt,
            )
            .await;
            return true;
        };
        let Some(ref workflow_actor_id) = self.workflow_actor_id else {
            tracing::debug!("no workflow actor; publishing remote task result to result feed");
            self.publish_remote_completion(
                &workflow_id,
                &task_id,
                &task_name,
                &outcome,
                &worker_node,
                attempt,
            )
            .await;
            return true;
        };

        let outcome_wire = outcome.clone();
        let outcome = match outcome {
            WireTaskOutcome::Completed(result_payload) => {
                TaskResultOutcome::Completed(result_payload)
            }
            WireTaskOutcome::Failed(error) => TaskResultOutcome::Failed(error),
            WireTaskOutcome::Cancelled => TaskResultOutcome::Cancelled,
        };
        let payload = match encode(&(
            workflow_id.clone(),
            task_id.clone(),
            outcome,
            Some(attempt),
            ResultSource::Remote,
        )) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("failed to encode ON_TASK_RESULT message: {}", e);
                return false;
            }
        };
        let call_result = actor_system
            .call(
                workflow_actor_id,
                crate::runtime::workflow::workflow_methods::ON_TASK_RESULT,
                payload,
            )
            .await;

        match call_result {
            Ok(result) => {
                if let Some(error) = result.error {
                    tracing::error!(
                        workflow_id = %workflow_id.as_str(),
                        task_id = %task_id.as_str(),
                        error = %error,
                        "workflow actor rejected task result"
                    );
                    return false;
                }
                // 结果已被编排入口接受：现在才清除在途登记。若 actor 调用
                // 失败（下方 Err 分支），登记保留，执行节点失联时仍可结算。
                if let Some(failover) = self.failover.as_ref() {
                    failover.take_outbound_if_not_stale(task_id.as_str(), attempt);
                }
                // 三态裁决：orchestrator 判定重试时随响应返回重派发任务，由
                // 本节点调度器延迟入队（远端执行的 flow 任务其编排者在本节点）；
                // 终局结果发布到 correctness 通道，使提交方 `AsyncResult` 解析（远端
                // 执行路径若无事件，flow 句柄无法跨节点解析）。结果回灌是
                // at-least-once：Failed 的 `Ignored` / `Settled` 是迟到重复或
                // 过期代数，不得发布（否则提交方句柄在重试在途时被错误置为
                // 失败）；Completed / Cancelled 的 `Settled` 仍须发布——事件
                // 发布是远端结果抵达提交方的唯一通道。
                let verdict: Option<crate::runtime::workflow::actor::TaskResultVerdict> =
                    if result.payload.is_empty() {
                        tracing::warn!(
                            workflow_id = %workflow_id.as_str(),
                            task_id = %task_id.as_str(),
                            "empty ON_TASK_RESULT response payload; treating verdict as ignored"
                        );
                        None
                    } else {
                        decode(&result.payload).ok()
                    };
                let is_failure = matches!(outcome_wire, WireTaskOutcome::Failed(_));
                match verdict {
                    Some(crate::runtime::workflow::actor::TaskResultVerdict::Retry(
                        task,
                        delay_ms,
                    )) => {
                        let scheduler = self.scheduler.clone();
                        tokio::spawn(async move {
                            if delay_ms > 0 {
                                tokio::time::sleep(std::time::Duration::from_millis(delay_ms))
                                    .await;
                            }
                            const MAX_ENQUEUE_ATTEMPTS: usize = 3;
                            let mut backoff = std::time::Duration::from_millis(100);
                            for attempt in 1..=MAX_ENQUEUE_ATTEMPTS {
                                match scheduler.enqueue((*task).clone()).await {
                                    Ok(()) => return,
                                    Err(e) if attempt < MAX_ENQUEUE_ATTEMPTS => {
                                        tracing::warn!(
                                            attempt,
                                            error = %e,
                                            "failed to enqueue orchestrator-driven retry task; retrying"
                                        );
                                        tokio::time::sleep(backoff).await;
                                        backoff = backoff.saturating_mul(2);
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            attempt,
                                            error = %e,
                                            "failed to enqueue orchestrator-driven retry task after retries; workflow may stall"
                                        );
                                        return;
                                    }
                                }
                            }
                        });
                    }
                    Some(crate::runtime::workflow::actor::TaskResultVerdict::FinalFailure) => {
                        self.publish_remote_completion(
                            &workflow_id,
                            &task_id,
                            &task_name,
                            &outcome_wire,
                            &worker_node,
                            attempt,
                        )
                        .await;
                    }
                    Some(crate::runtime::workflow::actor::TaskResultVerdict::Settled) => {
                        // 失败被守卫丢弃（过期代数 / 淘汰工作流）→ 不发布；
                        // 完成 / 取消入账 → 事件发布是提交方解析的唯一通道。
                        if !is_failure {
                            self.publish_remote_completion(
                                &workflow_id,
                                &task_id,
                                &task_name,
                                &outcome_wire,
                                &worker_node,
                                attempt,
                            )
                            .await;
                        }
                    }
                    // 迟到重复失败（重试在途 / 已终态）或裁决解码失败：
                    // 不发布终态——重投仍可能到达，真实终态随后会来。
                    other => {
                        tracing::debug!(
                            workflow_id = %workflow_id.as_str(),
                            task_id = %task_id.as_str(),
                            verdict = ?other.map(|v| match v {
                                crate::runtime::workflow::actor::TaskResultVerdict::Retry(..) => "retry",
                                crate::runtime::workflow::actor::TaskResultVerdict::FinalFailure => "final",
                                crate::runtime::workflow::actor::TaskResultVerdict::Settled => "settled",
                                crate::runtime::workflow::actor::TaskResultVerdict::Ignored => "ignored",
                            }),
                            "ignored duplicate failure report; not publishing terminal event"
                        );
                    }
                }
                true
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %workflow_id.as_str(),
                    task_id = %task_id.as_str(),
                    error = %e,
                    "failed to dispatch task result to workflow actor"
                );
                false
            }
        }
    }

    /// 将远程任务结果转换为 `TaskCompletion` 并发布到 correctness 通道。
    ///
    /// 当本地无 `WorkflowActor`（如 Python `@task` 无工作流）时，
    /// 远程任务结果通过事件总线投递给 Python 层订阅者。
    async fn publish_remote_completion(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        task_name: &str,
        outcome: &WireTaskOutcome,
        worker_node: &NodeId,
        attempt: u32,
    ) {
        let completion = match outcome {
            WireTaskOutcome::Completed(result) => TaskCompletion::Completed {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                result: result.clone(),
                target_node: Some(worker_node.clone()),
                attempt,
            },
            WireTaskOutcome::Failed(error) => TaskCompletion::Failed {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                error: error.clone(),
                target_node: Some(worker_node.clone()),
                attempt,
            },
            WireTaskOutcome::Cancelled => TaskCompletion::Cancelled {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                target_node: Some(worker_node.clone()),
                attempt,
            },
        };
        let lifecycle_event = match completion {
            TaskCompletion::Failed { .. } => TaskLifecycleEvent::TaskFailed(completion),
            TaskCompletion::Cancelled { .. } => TaskLifecycleEvent::TaskCancelled(completion),
            TaskCompletion::Completed { .. } => TaskLifecycleEvent::TaskCompleted(completion),
        };
        self.result_feed.send(lifecycle_event);
    }
}

#[cfg(test)]
#[path = "../../../../../../tests/rust/unit/runtime/workflow/runtime/network_router.rs"]
mod tests;
