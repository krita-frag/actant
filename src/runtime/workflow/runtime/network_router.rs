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
use crate::runtime::workflow::messaging::encode;
use crate::runtime::workflow::Scheduler;

/// 将传入的网络事件路由到适当的处理程序。
///
/// 封装了主题匹配和反序列化逻辑，使运行时主循环专注于任务执行。
pub(super) struct NetworkEventRouterConfig {
    pub(super) network: Arc<dyn Transport>,
    pub(super) event_bus: EventBus,
    pub(super) scheduler: Arc<dyn Scheduler>,
    pub(super) actor_system: Option<Arc<ActorSystem>>,
    /// 本地 WorkflowActor 的 id。若存在，远程 TaskResult 会直接路由给它。
    pub(super) workflow_actor_id: Option<crate::common::ActorId>,
    /// 本地 DagGossipActor 的 id。若存在，工作流状态请求/响应会直接路由给它。
    pub(super) dag_gossip_actor_id: Option<crate::common::ActorId>,
    /// Actor 注册表 gossip 处理器（A2）。若存在，`TOPIC_ACTOR_REGISTRY` 上的
    /// gossip 消息会通过 `handle_gossip` 更新本地注册表。
    pub(super) actor_registry_gossip:
        Option<Arc<crate::runtime::actor::router::ActorRegistryGossipActor>>,
    /// Capability gossip 处理器。若存在，`TOPIC_CAPABILITY_GOSSIP` 上的
    /// gossip 消息会通过 `handle_gossip` 更新本地 capability 视图。
    pub(super) capability_gossip:
        Option<Arc<crate::runtime::capability::gossip::CapabilityGossipActor>>,
    pub(super) cancel_flags: Arc<parking_lot::Mutex<HashMap<String, CancelFlag>>>,
    pub(super) cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
}

pub(super) struct NetworkEventRouter {
    network: Arc<dyn Transport>,
    event_bus: EventBus,
    scheduler: Arc<dyn Scheduler>,
    actor_system: Option<Arc<ActorSystem>>,
    workflow_actor_id: Option<crate::common::ActorId>,
    dag_gossip_actor_id: Option<crate::common::ActorId>,
    actor_registry_gossip: Option<Arc<crate::runtime::actor::router::ActorRegistryGossipActor>>,
    capability_gossip: Option<Arc<crate::runtime::capability::gossip::CapabilityGossipActor>>,
    cancel_flags: Arc<parking_lot::Mutex<HashMap<String, CancelFlag>>>,
    cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
}

impl NetworkEventRouter {
    pub(super) fn new(cfg: NetworkEventRouterConfig) -> Self {
        Self {
            network: cfg.network,
            event_bus: cfg.event_bus,
            scheduler: cfg.scheduler,
            actor_system: cfg.actor_system,
            workflow_actor_id: cfg.workflow_actor_id,
            dag_gossip_actor_id: cfg.dag_gossip_actor_id,
            actor_registry_gossip: cfg.actor_registry_gossip,
            capability_gossip: cfg.capability_gossip,
            cancel_flags: cfg.cancel_flags,
            cancelled_tasks: cfg.cancelled_tasks,
        }
    }

    /// 将单个 `NetworkEvent` 分发给适当的处理程序。
    pub(super) async fn handle(&self, event: NetworkEvent) {
        match event {
            NetworkEvent::Message(msg) => {
                self.handle_message(&msg.topic, &msg.data).await;
            }
            NetworkEvent::PeerConnected { peer_id } => {
                tracing::info!("peer connected: {}", peer_id);
                crate::metrics::inc_connected_peers();
                self.event_bus
                    .publish(BusEvent::PeerConnected(NodeId(peer_id.clone())))
                    .await;
            }
            NetworkEvent::PeerDisconnected { peer_id } => {
                tracing::info!("peer disconnected: {}", peer_id);
                crate::metrics::dec_connected_peers();
                self.event_bus
                    .publish(BusEvent::PeerDisconnected(NodeId(peer_id)))
                    .await;
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

    pub(super) async fn handle_message(&self, topic_str: &str, payload: &[u8]) {
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
        let _enter = span.enter();
        // 设置 thread-local scope：guard 在同步代码块结束时 drop，恢复前值。
        // 注意：guard 不能跨 await，因此 await 必须发生在 guard 仍然活跃的同步
        // 代码块内。当前实现：所有 await 都在 match 体内，guard 在 match 之前
        // drop（因为 _scope 在 match 之前结束），因此多跳传播仅对同步代码路径
        // 生效。这是当前实现的折中——若需跨 await 传播，应改用 tokio::task_local。
        let _scope = parsed_ctx.map(crate::common::wire::current_trace_scope);

        match topic.classify() {
            crate::common::TopicRoute::Task(_) => {
                if let Some((WireMessage::TaskDispatch(task), _)) = decoded {
                    if let Err(e) = self.scheduler.enqueue(task).await {
                        tracing::warn!("scheduler rejected task (drain mode?): {}", e);
                    }
                }
            }
            crate::common::TopicRoute::Actor(_) => {
                if let Some(ref sys) = self.actor_system {
                    if let Ok(remote_req) =
                        crate::common::decode_postcard::<crate::common::RemoteActorRequest>(payload)
                    {
                        sys.handle_remote_request(remote_req).await;
                    }
                }
            }
            crate::common::TopicRoute::ActorReply(_) => {
                if let Some(ref sys) = self.actor_system {
                    if let Some((WireMessage::RemoteActorReply(reply), _)) = decoded {
                        sys.deliver_reply(reply);
                    }
                }
            }
            crate::common::TopicRoute::DagState => {
                if let Some((WireMessage::DagStateUpdate(update), _)) = decoded {
                    self.event_bus.publish(BusEvent::DagUpdate(update)).await;
                }
            }
            crate::common::TopicRoute::Heartbeat => {
                if let Some((WireMessage::NodeHeartbeat(hb), _)) = decoded {
                    tracing::debug!("publishing heartbeat from {} to event_bus", hb.node_id.0);
                    self.event_bus.publish(BusEvent::Heartbeat(hb)).await;
                } else {
                    tracing::warn!(
                        "heartbeat topic but failed to unwrap envelope, payload len={}",
                        payload.len()
                    );
                }
            }
            crate::common::TopicRoute::Failover => {
                if let Some((WireMessage::OrchestratorClaim(claim), _)) = decoded {
                    self.event_bus.publish(BusEvent::Claim(claim)).await;
                }
            }
            crate::common::TopicRoute::Heads => {
                if let Some((WireMessage::HeadsExchange(exchange), _)) = decoded {
                    self.event_bus
                        .publish(BusEvent::HeadsExchange(exchange))
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
                        }))
                        .await;
                }
            }
            crate::common::TopicRoute::ActorRegistry => {
                // A2：跨节点 actor 注册表 gossip。注意 payload 是裸 postcard 编码的
                // `ActorRegistryGossipMsg`，不包裹在 `WireEnvelope` 中
                // （与 `CapabilityGossipActor` 一致，保持 gossip 消息轻量）。
                if let Some(ref gossip) = self.actor_registry_gossip {
                    gossip.handle_gossip(payload);
                } else {
                    tracing::debug!(
                        "received actor registry gossip but no gossip handler configured"
                    );
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
                let accepted = match self.scheduler.enqueue(task).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!("scheduler rejected direct dispatch (drain mode?): {}", e);
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
            crate::runtime::network::DirectRequest::ActorCallByType {
                actor_type,
                method,
                payload,
                reply_to,
            } => {
                // A2：按 actor 类型在本节点选择 actor 实例并调用。
                // 若本节点无此类型 actor，返回 Error 响应（避免调用方永久阻塞）。
                let response = match self.actor_system.as_ref() {
                    Some(sys) => match sys.find_local_actor_by_type(&actor_type) {
                        Some(actor_id) => {
                            self.handle_actor_call_by_type(sys, actor_id, method, payload, reply_to)
                                .await
                        }
                        None => {
                            tracing::warn!(
                                actor_type = %actor_type,
                                "no local actor of requested type"
                            );
                            crate::runtime::network::DirectResponse::Error {
                                message: format!(
                                    "no local actor of type '{}' on this node",
                                    actor_type
                                ),
                            }
                        }
                    },
                    None => crate::runtime::network::DirectResponse::Error {
                        message: "no actor system configured".into(),
                    },
                };
                if let Err(e) = self.network.send_direct_response(channel, response).await {
                    tracing::warn!("failed to send ActorCallByType response: {}", e);
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

    /// 处理远端按 actor 类型发起的调用（A2 接收侧）。
    ///
    /// 在本地 ActorSystem 上调用选中的 actor 实例，将结果序列化为
    /// `DirectResponse::ActorCallResult`。`reply_to` 参数当前未使用
    /// （响应通过 DirectResponse channel 返回，与 `ActorCall` 一致），
    /// 保留用于未来可能的异步通知路径。
    async fn handle_actor_call_by_type(
        &self,
        actor_system: &Arc<ActorSystem>,
        actor_id: crate::common::ActorId,
        method: String,
        payload: Vec<u8>,
        _reply_to: crate::common::RemoteReplyAddress,
    ) -> crate::runtime::network::DirectResponse {
        match actor_system.call(&actor_id, &method, payload).await {
            Ok(result) => match crate::common::encode_postcard(&result) {
                Ok(bytes) => {
                    crate::runtime::network::DirectResponse::ActorCallResult { result: bytes }
                }
                Err(e) => {
                    tracing::error!(
                        actor_id = %actor_id.as_str(),
                        method = %method,
                        error = %e,
                        "failed to encode ActorCallByType result"
                    );
                    crate::runtime::network::DirectResponse::Error {
                        message: format!("failed to encode actor result: {}", e),
                    }
                }
            },
            Err(e) => {
                tracing::warn!(
                    actor_id = %actor_id.as_str(),
                    method = %method,
                    error = %e,
                    "ActorCallByType local call failed"
                );
                crate::runtime::network::DirectResponse::Error {
                    message: format!("actor call failed: {}", e),
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
    /// 根据 outcome 类型分别调用 COMPLETE_TASK / FAIL_TASK / CANCEL_TASK。
    /// 返回 `true` 表示已成功提交给 WorkflowActor（不保证 workflow 状态更新成功）。
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

        let call_result = match outcome {
            WireTaskOutcome::Completed(result_payload) => {
                let payload = match encode(&(workflow_id.clone(), task_id.clone(), result_payload))
                {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("failed to encode COMPLETE_TASK message: {}", e);
                        return false;
                    }
                };
                actor_system
                    .call(
                        workflow_actor_id,
                        crate::runtime::workflow::workflow_methods::COMPLETE_TASK,
                        payload,
                    )
                    .await
            }
            WireTaskOutcome::Failed(error) => {
                let scope = crate::runtime::workflow::dag::FailureScope::TaskOnly;
                let payload = match encode(&(workflow_id.clone(), task_id.clone(), error, scope)) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("failed to encode FAIL_TASK message: {}", e);
                        return false;
                    }
                };
                actor_system
                    .call(
                        workflow_actor_id,
                        crate::runtime::workflow::workflow_methods::FAIL_TASK,
                        payload,
                    )
                    .await
            }
            WireTaskOutcome::Cancelled => {
                let payload = match encode(&(workflow_id.clone(), task_id.clone())) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("failed to encode CANCEL_TASK message: {}", e);
                        return false;
                    }
                };
                actor_system
                    .call(
                        workflow_actor_id,
                        crate::runtime::workflow::workflow_methods::CANCEL_TASK,
                        payload,
                    )
                    .await
            }
            WireTaskOutcome::Skipped => {
                tracing::warn!(
                    workflow_id = %workflow_id.as_str(),
                    task_id = %task_id.as_str(),
                    "unexpected Skipped outcome in remote task result; ignoring"
                );
                return false;
            }
        };

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
        self.event_bus.publish(bus_event).await;
    }
}

#[cfg(test)]
#[path = "../../../../tests/rust/unit/runtime/workflow/runtime/network_router.rs"]
mod tests;
