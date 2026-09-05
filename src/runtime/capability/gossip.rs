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
            // 默认广播间隔 60s；测试或调优可通过 `with_broadcast_interval` 覆盖。
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
#[path = "../../../tests/rust/unit/runtime/capability/gossip.rs"]
mod tests;
