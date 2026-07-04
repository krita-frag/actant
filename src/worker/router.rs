use std::sync::Arc;

use crate::actor::system::ActorSystem;
use crate::common::{NodeId, Topic, WireEnvelope, WireMessage};
use crate::event_bus::{BusEvent, EventBus};
use crate::network::{DirectResponseChannel, NetworkEvent, Transport};
use crate::orchestrator::Scheduler;

/// 将传入的网络事件路由到适当的处理程序。
///
/// 封装了主题匹配和反序列化逻辑，这些逻辑之前
/// 内联在 `WorkerRuntime::run()` 中，使运行时主循环专注于任务执行。
pub(crate) struct NetworkEventRouter {
    network: Arc<dyn Transport>,
    event_bus: EventBus,
    scheduler: Arc<dyn Scheduler>,
    actor_system: Option<Arc<ActorSystem>>,
}

impl NetworkEventRouter {
    pub fn new(
        network: Arc<dyn Transport>,
        event_bus: EventBus,
        scheduler: Arc<dyn Scheduler>,
        actor_system: Option<Arc<ActorSystem>>,
    ) -> Self {
        Self {
            network,
            event_bus,
            scheduler,
            actor_system,
        }
    }

    /// 将单个 `NetworkEvent` 分发给适当的处理程序。
    pub async fn handle(&self, event: NetworkEvent) {
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

    async fn handle_message(&self, topic_str: &str, payload: &[u8]) {
        let topic = Topic::from(topic_str);
        match topic.classify() {
            crate::common::TopicRoute::Task(_) => {
                if let Some(WireMessage::TaskDispatch(task)) = WireEnvelope::decode(payload) {
                    if let Err(e) = self.scheduler.enqueue(task).await {
                        tracing::warn!("scheduler rejected task (drain mode?): {}", e);
                    }
                }
            }
            crate::common::TopicRoute::Actor(_) => {
                if let Some(ref sys) = self.actor_system {
                    if let Ok(remote_req) =
                        postcard::from_bytes::<crate::common::RemoteActorRequest>(payload)
                    {
                        sys.handle_remote_request(remote_req).await;
                    }
                }
            }
            crate::common::TopicRoute::ActorReply(_) => {
                if let Some(ref sys) = self.actor_system {
                    if let Some(WireMessage::RemoteActorReply(reply)) =
                        WireEnvelope::decode(payload)
                    {
                        sys.deliver_reply(reply);
                    }
                }
            }
            crate::common::TopicRoute::DagState => {
                if let Some(WireMessage::DagStateUpdate(update)) = WireEnvelope::decode(payload) {
                    self.event_bus.publish(BusEvent::DagUpdate(update)).await;
                }
            }
            crate::common::TopicRoute::Heartbeat => {
                if let Some(WireMessage::NodeHeartbeat(hb)) = WireEnvelope::decode(payload) {
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
                if let Some(WireMessage::OrchestratorClaim(claim)) = WireEnvelope::decode(payload) {
                    self.event_bus.publish(BusEvent::Claim(claim)).await;
                }
            }
            crate::common::TopicRoute::Heads => {
                if let Some(WireMessage::HeadsExchange(exchange)) = WireEnvelope::decode(payload) {
                    self.event_bus
                        .publish(BusEvent::HeadsExchange(exchange))
                        .await;
                }
            }
            // Workflow state 请求/响应以及未知 topic 在其他地方
            // （gossip 层）处理或丢弃 — 此处不处理。
            crate::common::TopicRoute::WorkflowStateReq(_)
            | crate::common::TopicRoute::WorkflowStateResp(_)
            | crate::common::TopicRoute::Unknown => {}
        }
    }

    async fn handle_direct_request(
        &self,
        peer_id: String,
        request: crate::network::protocol::DirectRequest,
        channel: DirectResponseChannel,
    ) {
        if let crate::network::protocol::DirectRequest::DispatchTask { task } = request {
            let accepted = match self.scheduler.enqueue(task).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!("scheduler rejected direct dispatch (drain mode?): {}", e);
                    false
                }
            };
            let response = crate::network::protocol::DirectResponse::DispatchAck { accepted };
            if let Err(e) = self.network.send_direct_response(channel, response).await {
                tracing::warn!("failed to send DispatchAck: {}", e);
            }
        } else {
            self.event_bus
                .publish(BusEvent::DirectRequest {
                    peer_id,
                    request: Box::new(request),
                    channel,
                })
                .await;
        }
    }
}
