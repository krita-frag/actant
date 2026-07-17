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
        let trace_id = decoded.as_ref().and_then(|(_, tid)| tid.clone());
        // 为本次跨节点消息接收创建子 span，串联发送方的 trace context。
        // 即使 trace_id 为 None（旧版本节点或 decode 失败），也创建一个无名 span
        // 保持代码结构一致。
        let span = tracing::info_span!(
            "wire.recv",
            topic = %topic_str,
            wire.trace_id = tracing::field::Empty,
        );
        if let Some(ref tid) = trace_id {
            span.record("wire.trace_id", tid);
        }
        let _enter = span.enter();

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
            other => {
                self.event_bus
                    .publish(BusEvent::DirectRequest {
                        peer_id,
                        request: Box::new(other),
                        channel,
                    })
                    .await;
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
