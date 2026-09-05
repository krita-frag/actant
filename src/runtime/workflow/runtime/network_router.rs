//! Worker 网络事件路由器。
//!
//! Worker 主循环只负责执行任务；所有 topic 分类、wire message 解码、直连请求
//! 分发和事件总线转发都集中在本模块。这样可以把“网络输入如何变成 runtime 事件”
//! 与“任务如何执行”分开阅读和测试。
//!
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::common::wire::CancelBroadcast;
use crate::common::{
    NodeId, TaskCompletion, TaskId, Topic, WireEnvelope, WireMessage, WireTaskOutcome, WorkflowId,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::dispatcher::CancelFlag;
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::network::{DirectResponseChannel, NetworkEvent, Transport};
use crate::runtime::workflow::actor::{ResultSource, TaskResultOutcome};
use crate::runtime::workflow::messaging::encode;
use crate::runtime::workflow::Scheduler;
use tracing::Instrument;

/// 将传入的网络事件路由到适当的处理程序。
///
/// 封装了主题匹配和反序列化逻辑，使运行时主循环专注于任务执行。
pub(crate) struct NetworkEventRouterConfig {
    pub(crate) network: Arc<dyn Transport>,
    pub(crate) event_bus: EventBus,
    pub(crate) scheduler: Arc<dyn Scheduler>,
    pub(crate) actor_system: Option<Arc<ActorSystem>>,
    /// 本地 WorkflowActor 的 id。若存在，远程 TaskResult 会直接路由给它。
    pub(crate) workflow_actor_id: Option<crate::common::ActorId>,
    /// 本地 DagGossipActor 的 id。若存在，工作流状态请求/响应会直接路由给它。
    pub(crate) dag_gossip_actor_id: Option<crate::common::ActorId>,
    /// Capability gossip 处理器。若存在，`TOPIC_CAPABILITY_GOSSIP` 上的
    /// gossip 消息会通过 `handle_gossip` 更新本地 capability 视图。
    pub(crate) capability_gossip:
        Option<Arc<crate::runtime::capability::gossip::CapabilityGossipActor>>,
    /// 故障转移管理器。若存在，入站 Heartbeat / Claim wire 消息会直连分发到
    /// `handle_heartbeat` / `handle_claim`（控制面指令不经过 EventBus）。
    pub(crate) failover: Option<Arc<crate::runtime::workflow::FailoverManager>>,
    pub(crate) cancel_flags: Arc<parking_lot::Mutex<HashMap<String, CancelFlag>>>,
    pub(crate) cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
}

pub(crate) struct NetworkEventRouter {
    network: Arc<dyn Transport>,
    event_bus: EventBus,
    scheduler: Arc<dyn Scheduler>,
    actor_system: Option<Arc<ActorSystem>>,
    workflow_actor_id: Option<crate::common::ActorId>,
    dag_gossip_actor_id: Option<crate::common::ActorId>,
    capability_gossip: Option<Arc<crate::runtime::capability::gossip::CapabilityGossipActor>>,
    failover: Option<Arc<crate::runtime::workflow::FailoverManager>>,
    cancel_flags: Arc<parking_lot::Mutex<HashMap<String, CancelFlag>>>,
    cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
}

impl NetworkEventRouter {
    pub(crate) fn new(cfg: NetworkEventRouterConfig) -> Self {
        Self {
            network: cfg.network,
            event_bus: cfg.event_bus,
            scheduler: cfg.scheduler,
            actor_system: cfg.actor_system,
            workflow_actor_id: cfg.workflow_actor_id,
            dag_gossip_actor_id: cfg.dag_gossip_actor_id,
            capability_gossip: cfg.capability_gossip,
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
                self.event_bus
                    .publish(BusEvent::PeerConnected(NodeId(peer_id.clone())));
            }
            NetworkEvent::PeerDisconnected { peer_id } => {
                tracing::info!("peer disconnected: {}", peer_id);
                crate::metrics::dec_connected_peers();
                self.event_bus
                    .publish(BusEvent::PeerDisconnected(NodeId(peer_id)));
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
        let decoded = WireEnvelope::decode(payload);
        let traceparent = decoded.as_ref().and_then(|(_, tp)| tp.clone());
        // C3：解析入站 W3C traceparent，若成功则：
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
    /// 在此同步压入 thread-local（guard 生命周期等同本 future，与原先行为一致），
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
                    if let Err(e) = self.scheduler.enqueue(task.clone()).await {
                        // 入队失败即任务被丢弃：必须以 error 级别留下任务标识，
                        // 否则任务在日志中完全消失（提交方只能观察到永久等待）。
                        tracing::error!(
                            task_id = %task.id.as_str(),
                            task_name = %task.name,
                            error = %e,
                            "scheduler rejected task dispatch; task dropped"
                        );
                    }
                }
            }
            crate::common::TopicRoute::DagState => {
                if let Some((WireMessage::DagStateUpdate(update), _)) = decoded {
                    // 控制面直连：DAG 状态 CRDT 更新直接分发给 DagGossipActor，
                    // 不经 EventBus（观测 tap 有损，不承载正确性语义）。
                    self.handle_workflow_state_event(
                        crate::runtime::workflow::gossip_methods::APPLY_REMOTE_UPDATE,
                        &update,
                    )
                    .await;
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
                if let Some((WireMessage::OrchestratorClaim(claim), _)) = decoded {
                    match &self.failover {
                        Some(failover) => {
                            // 控制面直连：更新本地租约表，非本节点 claim 时移除
                            // 本地 active 状态，构成双主防护。
                            failover.handle_claim(&claim).await;
                        }
                        None => {
                            tracing::warn!(
                                claim_node = %claim.node_id.0,
                                workflow_id = %claim.workflow_id.as_str(),
                                "no failover manager configured; dropping inbound claim"
                            );
                        }
                    }
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
            crate::common::TopicRoute::WorkflowStateReq(_) => {
                if let Some((WireMessage::WorkflowStateRequest(request), _)) = decoded {
                    self.handle_workflow_state_event(
                        crate::runtime::workflow::gossip_methods::HANDLE_WORKFLOW_STATE_REQUEST,
                        &request,
                    )
                    .await;
                }
            }
            crate::common::TopicRoute::WorkflowStateResp(_) => {
                if let Some((WireMessage::WorkflowStateResponse(response), _)) = decoded {
                    self.handle_workflow_state_event(
                        crate::runtime::workflow::gossip_methods::HANDLE_WORKFLOW_STATE_RESPONSE,
                        &response,
                    )
                    .await;
                }
            }
            crate::common::TopicRoute::Cancel => {
                if let Ok(msg) = crate::common::decode_postcard::<CancelBroadcast>(payload) {
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
                        // 仅在新插入时增加 pending 计数；已存在的条目为重复取消，不重复计数。
                        if cancelled_tasks
                            .insert(msg.task_id.to_string(), Instant::now())
                            .is_none()
                        {
                            crate::metrics::inc_cancelled_tasks_pending();
                        }
                    }
                    crate::metrics::inc_tasks_cancelled();
                    let tid = msg.task_id.clone();
                    self.event_bus
                        .publish(BusEvent::TaskCancelled(TaskCompletion::Cancelled {
                            workflow_id: msg.workflow_id,
                            task_id: tid.clone(),
                            task_name: tid.0,
                            target_node: None,
                        }));
                }
            }
            crate::common::TopicRoute::CapabilityGossip => {
                // Capability 元信息 gossip。payload 是裸 postcard 编码的
                // `CapabilityGossipMsg`，不包裹在 `WireEnvelope` 中。
                if let Some(ref gossip) = self.capability_gossip {
                    gossip.handle_gossip(payload);
                } else {
                    tracing::debug!("received capability gossip but no gossip handler configured");
                }
            }
            crate::common::TopicRoute::Unknown => {}
        }
    }

    async fn handle_direct_request(
        &self,
        peer_id: String,
        request: crate::runtime::network::DirectRequest,
        channel: DirectResponseChannel,
    ) {
        match request {
            crate::runtime::network::DirectRequest::DispatchTask { task } => {
                let accepted = match self.scheduler.enqueue(task.clone()).await {
                    Ok(()) => true,
                    Err(e) => {
                        // 直连派发被拒即任务丢弃：以 error 级别记录任务标识，
                        // 便于与提交方的"任务永久等待"告警关联排查。
                        tracing::error!(
                            task_id = %task.id.as_str(),
                            task_name = %task.name,
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
            } => {
                let accepted = self
                    .handle_task_result(workflow_id, task_id, task_name, outcome, worker_node)
                    .await;
                let response = crate::runtime::network::DirectResponse::TaskResultAck { accepted };
                if let Err(e) = self.network.send_direct_response(channel, response).await {
                    tracing::warn!("failed to send TaskResultAck: {}", e);
                }
            }
            other => {
                // 点对点请求-响应不走 EventBus：直接由接收方处理或回送 Error 响应，
                // 避免独占投递分支在无订阅者时让调用方永久阻塞。
                // 未识别的 DirectRequest 变体直接回送 Error，让调用方立即
                // 收到明确错误，依赖其自身超时 fallback。
                tracing::warn!(
                    peer = %peer_id,
                    request = ?other,
                    "no handler for DirectRequest variant, returning DirectResponse::Error",
                );
                let response = crate::runtime::network::DirectResponse::Error {
                    message: format!(
                        "no handler for DirectRequest variant on this node: {:?}",
                        other
                    ),
                };
                if let Err(e) = self.network.send_direct_response(channel, response).await {
                    tracing::warn!(
                        peer = %peer_id,
                        error = %e,
                        "failed to send DirectResponse::Error for unhandled DirectRequest",
                    );
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
    ) -> bool {
        if workflow_id.as_str().is_empty() {
            self.publish_remote_completion(
                &workflow_id,
                &task_id,
                &task_name,
                &outcome,
                &worker_node,
            )
            .await;
            return true;
        }

        let Some(ref actor_system) = self.actor_system else {
            tracing::debug!("no actor system; publishing remote task result to event_bus");
            self.publish_remote_completion(
                &workflow_id,
                &task_id,
                &task_name,
                &outcome,
                &worker_node,
            )
            .await;
            return true;
        };
        let Some(ref workflow_actor_id) = self.workflow_actor_id else {
            tracing::debug!("no workflow actor; publishing remote task result to event_bus");
            self.publish_remote_completion(
                &workflow_id,
                &task_id,
                &task_name,
                &outcome,
                &worker_node,
            )
            .await;
            return true;
        };

        // wire 协议尚未携带派发代数，attempt 传 `None`（入口 fencing 放行）。
        let outcome = match outcome {
            WireTaskOutcome::Completed(result_payload) => {
                TaskResultOutcome::Completed(result_payload)
            }
            WireTaskOutcome::Failed(error) => TaskResultOutcome::Failed(error),
            WireTaskOutcome::Cancelled => TaskResultOutcome::Cancelled,
            WireTaskOutcome::Skipped => {
                tracing::warn!(
                    workflow_id = %workflow_id.as_str(),
                    task_id = %task_id.as_str(),
                    "unexpected Skipped outcome in remote task result; ignoring"
                );
                return false;
            }
        };
        let payload = match encode(&(
            workflow_id.clone(),
            task_id.clone(),
            outcome,
            None::<u32>,
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
                    false
                } else {
                    true
                }
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

    /// 将远程任务结果转换为 `TaskCompletion` 并发布到 `event_bus`。
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
    ) {
        let completion = match outcome {
            WireTaskOutcome::Completed(result) => TaskCompletion::Completed {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                result: result.clone(),
                target_node: Some(worker_node.clone()),
            },
            WireTaskOutcome::Failed(error) => TaskCompletion::Failed {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                error: error.clone(),
                target_node: Some(worker_node.clone()),
            },
            WireTaskOutcome::Cancelled => TaskCompletion::Cancelled {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                target_node: Some(worker_node.clone()),
            },
            WireTaskOutcome::Skipped => TaskCompletion::Skipped {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                task_name: task_name.to_string(),
                target_node: Some(worker_node.clone()),
            },
        };
        let bus_event = match &completion {
            TaskCompletion::Failed { .. } => BusEvent::TaskFailed(completion),
            TaskCompletion::Cancelled { .. } => BusEvent::TaskCancelled(completion),
            TaskCompletion::Skipped { .. } => BusEvent::TaskSkipped(completion),
            TaskCompletion::Completed { .. } => BusEvent::TaskCompleted(completion),
        };
        self.event_bus.publish(bus_event);
    }
}

#[cfg(test)]
#[path = "../../../../tests/rust/unit/runtime/workflow/runtime/network_router.rs"]
mod tests;
