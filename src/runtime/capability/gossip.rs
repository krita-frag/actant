//! Capability Gossip 与反熵协议。
//!
//! - `CapabilityGossip` 消息描述节点上已注册的 capability 元信息。
//! - `CapabilityGossipActor` 周期性广播本地 capability，并监听邻居广播。
//! - 收集到的远端 capability 元信息写入 `CapabilityRuntime`，供
//!   `CapabilityRuntime::ask` / `perform` / `emit` 在本地无 handler 时进行远程路由。

use std::sync::Arc;
use std::time::Duration;

use postcard;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::common::{ActantError, NodeId};
use crate::runtime::capability::{CapabilityRuntime, GossipCapabilityMeta};
use crate::runtime::network::Transport;

/// Capability 广播消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityGossipMsg {
    pub node_id: NodeId,
    pub capabilities: Vec<GossipCapabilityMeta>,
    pub sequence: u64,
}

impl CapabilityGossipMsg {
    pub fn new(node_id: NodeId, capabilities: Vec<GossipCapabilityMeta>, sequence: u64) -> Self {
        Self {
            node_id,
            capabilities,
            sequence,
        }
    }

    pub fn topic() -> &'static str {
        crate::common::wire::constants::TOPIC_CAPABILITY_GOSSIP
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ActantError> {
        postcard::to_allocvec(self).map_err(|e| ActantError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ActantError> {
        // 远端 gossip 输入：先校验大小上限。
        crate::common::decode_postcard(bytes)
    }
}

/// Capability gossip 子系统：周期性广播本地 capability 元信息，
/// 并处理远端广播。
///
/// 不实现 `Actor` trait：它的职责是单一后台循环 + 网络层被动接收，
/// 不需要 ActorSystem 的消息路由能力。直接由 builder spawn 后台任务。
pub struct CapabilityGossipActor {
    node_id: NodeId,
    capability: Arc<CapabilityRuntime>,
    network: Arc<dyn Transport>,
    sequence: std::sync::atomic::AtomicU64,
    seen: dashmap::DashMap<NodeId, u64>,
    broadcast_interval: Duration,
}

impl CapabilityGossipActor {
    pub fn new(
        node_id: NodeId,
        capability: Arc<CapabilityRuntime>,
        network: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            capability,
            network,
            sequence: std::sync::atomic::AtomicU64::new(0),
            seen: dashmap::DashMap::new(),
            broadcast_interval: Duration::from_secs(60),
        }
    }

    pub fn with_broadcast_interval(mut self, interval: Duration) -> Self {
        self.broadcast_interval = interval;
        self
    }

    pub async fn broadcast_capabilities(&self) -> Result<(), ActantError> {
        let metas: Vec<GossipCapabilityMeta> = self
            .capability
            .capabilities()
            .into_iter()
            .map(GossipCapabilityMeta::from)
            .collect();
        if metas.is_empty() {
            return Ok(());
        }
        let sequence = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let gossip = CapabilityGossipMsg::new(self.node_id.clone(), metas, sequence);
        let bytes = gossip.to_bytes()?;
        self.network
            .broadcast(CapabilityGossipMsg::topic(), bytes)
            .await
            .map_err(|e| ActantError::Network(e.to_string()))?;
        Ok(())
    }

    pub fn handle_gossip(&self, payload: &[u8]) {
        match CapabilityGossipMsg::from_bytes(payload) {
            Ok(gossip) => {
                if gossip.node_id == self.node_id {
                    return;
                }
                if let Some(prev) = self.seen.get(&gossip.node_id) {
                    if *prev >= gossip.sequence {
                        return;
                    }
                }
                self.seen.insert(gossip.node_id.clone(), gossip.sequence);
                tracing::debug!(
                    from = %gossip.node_id,
                    caps = gossip.capabilities.len(),
                    seq = gossip.sequence,
                    "received capability gossip"
                );
                self.capability
                    .update_peer_capabilities(gossip.node_id.clone(), gossip.capabilities);
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to decode capability gossip");
            }
        }
    }

    /// 启动周期性广播的后台循环。返回 cancel 句柄，发送 `true` 即可停止。
    pub fn start_background_loop(self: Arc<Self>) -> watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.broadcast_interval);
            // 第一次 tick 立即触发，让 capability 元信息尽快扩散。
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = ticker.tick() => {
                        if let Err(e) = self.broadcast_capabilities().await {
                            tracing::warn!(error = %e, "capability gossip broadcast failed");
                        }
                    }
                }
            }
            tracing::debug!("capability gossip background loop stopped");
        });
        cancel_tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::MAX_DECODE_SIZE;
    use crate::runtime::capability::{register_defaults, CapabilityRuntime};
    use crate::runtime::network::{
        DirectRequest, DirectResponse, DirectResponseChannel, ListenAddresses, NetworkEvent, PeerId,
    };
    use std::sync::Mutex;

    #[test]
    fn capability_gossip_roundtrip() {
        let cap = Arc::new(CapabilityRuntime::new());
        register_defaults(&cap);
        let gossip = CapabilityGossipMsg::new(
            NodeId::new("node-a".to_string()),
            cap.capabilities()
                .into_iter()
                .map(GossipCapabilityMeta::from)
                .collect(),
            1,
        );
        let bytes = gossip.to_bytes().unwrap();
        let decoded = CapabilityGossipMsg::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.node_id, gossip.node_id);
        assert_eq!(decoded.sequence, gossip.sequence);
    }

    #[test]
    fn from_bytes_rejects_oversized_payload() {
        // 远端输入校验：超出大小上限应返回错误而非 panic。
        let big = vec![0u8; MAX_DECODE_SIZE + 1];
        assert!(CapabilityGossipMsg::from_bytes(&big).is_err());
    }

    #[test]
    fn from_bytes_rejects_garbage() {
        assert!(CapabilityGossipMsg::from_bytes(b"not postcard").is_err());
    }

    #[test]
    fn topic_is_stable_constant() {
        // topic 用于订阅匹配，必须是稳定常量。
        assert_eq!(
            CapabilityGossipMsg::topic(),
            crate::common::wire::constants::TOPIC_CAPABILITY_GOSSIP
        );
    }

    /// 记录所有 broadcast 调用的 Transport 桩，用于验证广播行为。
    struct RecordingTransport {
        node_id: NodeId,
        broadcasts: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl RecordingTransport {
        fn new(node_id: NodeId) -> Self {
            Self {
                node_id,
                broadcasts: Mutex::new(Vec::new()),
            }
        }

        fn broadcasts(&self) -> Vec<(String, Vec<u8>)> {
            self.broadcasts.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Transport for RecordingTransport {
        fn node_id(&self) -> &NodeId {
            &self.node_id
        }
        fn local_peer_id(&self) -> &str {
            "recording-peer"
        }
        async fn broadcast(&self, topic: &str, data: Vec<u8>) -> crate::common::Result<()> {
            self.broadcasts
                .lock()
                .unwrap()
                .push((topic.to_string(), data));
            Ok(())
        }
        async fn subscribe(&self, _topic: &str) -> crate::common::Result<()> {
            Ok(())
        }
        async fn recv_event(&self) -> Option<NetworkEvent> {
            None
        }
        async fn dial(&self, _addr: &str) -> crate::common::Result<()> {
            Ok(())
        }
        async fn add_gossip_peer(&self, _peer_id: &str) -> crate::common::Result<()> {
            Ok(())
        }
        fn listen_addresses(&self) -> crate::common::Result<ListenAddresses> {
            Ok(ListenAddresses {
                endpoint_id: String::new(),
                relay_url: None,
                direct_addrs: Vec::new(),
                endpoint_addr: String::new(),
            })
        }
        async fn send_direct_request(
            &self,
            _peer_id_str: &str,
            _request: DirectRequest,
        ) -> crate::common::Result<DirectResponse> {
            Ok(DirectResponse::Error {
                message: "not implemented in stub".into(),
            })
        }
        async fn send_direct_response(
            &self,
            _channel: DirectResponseChannel,
            _response: DirectResponse,
        ) -> crate::common::Result<()> {
            Ok(())
        }
        async fn discover_peers(&self) -> crate::common::Result<Vec<PeerId>> {
            Ok(Vec::new())
        }
        async fn shutdown(&self) -> crate::common::Result<()> {
            Ok(())
        }
    }

    /// 构造 actor 并保留对 RecordingTransport 的强引用，便于读取广播记录。
    fn make_actor(node_id: &str) -> (CapabilityGossipActor, Arc<RecordingTransport>) {
        let cap = Arc::new(CapabilityRuntime::new());
        register_defaults(&cap);
        let rec = Arc::new(RecordingTransport::new(NodeId::new(node_id.into())));
        let transport: Arc<dyn Transport> = rec.clone();
        let actor = CapabilityGossipActor::new(NodeId::new(node_id.into()), cap, transport);
        (actor, rec)
    }

    #[tokio::test]
    async fn broadcast_capabilities_skips_when_no_capabilities() {
        // register_defaults 注册 codec 与空 layer，capabilities() 返回 10 个
        // 内置 capability。broadcast_capabilities 应广播并递增 sequence。
        let (actor, rec) = make_actor("self-node");
        actor.broadcast_capabilities().await.unwrap();
        assert!(!rec.broadcasts().is_empty(), "应在有 capability 时广播");
        // sequence 应递增。
        assert_eq!(
            actor
                .sequence
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn handle_gossip_ignores_own_messages() {
        let (actor, _rec) = make_actor("self-node");
        let own_msg = CapabilityGossipMsg::new(actor.node_id.clone(), Vec::new(), 5);
        actor.handle_gossip(&own_msg.to_bytes().unwrap());
        // 自身消息不应记录到 seen。
        assert!(actor.seen.is_empty());
    }

    #[tokio::test]
    async fn handle_gossip_dedups_by_sequence() {
        let (actor, _rec) = make_actor("self-node");
        let peer = NodeId::new("peer-node".into());

        // seq=2 应被接受。
        let m2 = CapabilityGossipMsg::new(peer.clone(), Vec::new(), 2);
        actor.handle_gossip(&m2.to_bytes().unwrap());
        assert_eq!(*actor.seen.get(&peer).unwrap(), 2);

        // seq=2 重复（等于已见）应被忽略。
        actor.handle_gossip(&m2.to_bytes().unwrap());
        assert_eq!(*actor.seen.get(&peer).unwrap(), 2);

        // seq=1（小于已见）应被忽略。
        let m1 = CapabilityGossipMsg::new(peer.clone(), Vec::new(), 1);
        actor.handle_gossip(&m1.to_bytes().unwrap());
        assert_eq!(*actor.seen.get(&peer).unwrap(), 2);

        // seq=3（大于已见）应被接受并更新。
        let m3 = CapabilityGossipMsg::new(peer.clone(), Vec::new(), 3);
        actor.handle_gossip(&m3.to_bytes().unwrap());
        assert_eq!(*actor.seen.get(&peer).unwrap(), 3);
    }

    #[tokio::test]
    async fn handle_gossip_decodes_and_updates_peer_capabilities() {
        let (actor, _rec) = make_actor("self-node");
        // 构造带 capability meta 的远端消息。
        let peer = NodeId::new("peer-node".into());
        let metas: Vec<GossipCapabilityMeta> = actor
            .capability
            .capabilities()
            .into_iter()
            .map(GossipCapabilityMeta::from)
            .collect();
        let msg = CapabilityGossipMsg::new(peer.clone(), metas, 1);
        actor.handle_gossip(&msg.to_bytes().unwrap());
        assert_eq!(*actor.seen.get(&peer).unwrap(), 1);
    }

    #[tokio::test]
    async fn handle_gossip_swallows_invalid_payload() {
        let (actor, _rec) = make_actor("self-node");
        // 垃圾 payload 不应 panic，仅记录 warn。
        actor.handle_gossip(b"garbage");
        assert!(actor.seen.is_empty());
    }
}
