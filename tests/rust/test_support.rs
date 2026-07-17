//! 测试辅助工具（仅 `#[cfg(test)]` 编译）。
//!
//! 为多个模块的单测提供共享的 mock 基础设施，避免在每个测试模块内
//! 重复实现 `Transport` / 容量跟踪等桩。遵循 AGENTS.md「生产代码禁止
//! 桩函数与 Mock」——本文件只编译进 test 构建产物。

#![cfg(test)]

use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::{ActantError, NodeId, Result};
use crate::runtime::network::{
    DirectRequest, DirectResponse, DirectResponseChannel, ListenAddresses, NetworkEvent, Transport,
};

// Mock transport 广播记录类型。
type BroadcastLog = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// 记录所有 `broadcast` 调用的最小 `Transport` 桩。
///
/// - `broadcasts`：按调用顺序记录 `(topic, data)`。
/// - `subscribed`：记录 `subscribe` 调用过的 topic。
///
/// 其余方法返回默认值或 `None`；足以支撑不依赖真实网络的纯逻辑测试。
#[derive(Clone)]
pub struct MockTransport {
    pub node_id: NodeId,
    pub local_peer_id: String,
    pub broadcasts: BroadcastLog,
    pub subscribed: Arc<Mutex<Vec<String>>>,
}

#[allow(dead_code)]
impl MockTransport {
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: NodeId::from(node_id.to_string()),
            local_peer_id: format!("peer-{}", node_id),
            broadcasts: Arc::new(Mutex::new(Vec::new())),
            subscribed: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 返回至今记录到的广播次数。
    pub fn broadcast_count(&self) -> usize {
        self.broadcasts.lock().len()
    }

    /// 返回最近一次广播的 `(topic, data)`（若有）。
    pub fn last_broadcast(&self) -> Option<(String, Vec<u8>)> {
        self.broadcasts.lock().last().cloned()
    }

    /// 返回是否已订阅给定 topic。
    pub fn subscribed_to(&self, topic: &str) -> bool {
        self.subscribed.lock().iter().any(|t| t == topic)
    }
}

#[async_trait::async_trait]
impl Transport for MockTransport {
    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn local_peer_id(&self) -> &str {
        &self.local_peer_id
    }

    async fn broadcast(&self, topic: &str, data: Vec<u8>) -> Result<()> {
        self.broadcasts.lock().push((topic.to_string(), data));
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<()> {
        self.subscribed.lock().push(topic.to_string());
        Ok(())
    }

    async fn recv_event(&self) -> Option<NetworkEvent> {
        None
    }

    async fn dial(&self, _addr: &str) -> Result<()> {
        Ok(())
    }

    async fn add_gossip_peer(&self, _peer_id: &str) -> Result<()> {
        Ok(())
    }

    fn listen_addresses(&self) -> Result<ListenAddresses> {
        Ok(ListenAddresses {
            endpoint_id: self.local_peer_id.clone(),
            relay_url: None,
            direct_addrs: Vec::new(),
            endpoint_addr: self.local_peer_id.clone(),
        })
    }

    async fn send_direct_request(
        &self,
        _peer_id_str: &str,
        _request: DirectRequest,
    ) -> Result<DirectResponse> {
        Err(ActantError::Internal(
            "MockTransport: send_direct_request not implemented".into(),
        ))
    }

    async fn send_direct_response(
        &self,
        _channel: DirectResponseChannel,
        _response: DirectResponse,
    ) -> Result<()> {
        Ok(())
    }

    async fn discover_peers(&self) -> Result<Vec<crate::runtime::network::PeerId>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// 最小 `Scheduler` 桩：所有操作返回空/默认值，不持有真实队列。
///
/// 适用于不需要真实调度行为的单测（如验证 Worker 的 builder / getter / shutdown）。
/// 遵循 AGENTS.md「生产代码禁止桩函数与 Mock」——本类型仅编译进 test 构建产物。
pub struct MockScheduler {
    closed: Arc<std::sync::atomic::AtomicBool>,
}

#[allow(dead_code)]
impl MockScheduler {
    pub fn new() -> Self {
        Self {
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for MockScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl crate::runtime::workflow::Scheduler for MockScheduler {
    async fn enqueue(
        &self,
        _task: crate::common::TaskDefinition,
    ) -> std::result::Result<(), crate::common::ActantError> {
        Ok(())
    }

    async fn enqueue_batch(
        &self,
        _tasks: Vec<crate::common::TaskDefinition>,
    ) -> std::result::Result<(), crate::common::ActantError> {
        Ok(())
    }

    async fn dequeue(&self) -> Option<crate::common::TaskDefinition> {
        None
    }

    async fn try_dequeue(&self) -> Option<crate::common::TaskDefinition> {
        None
    }

    async fn dequeue_batch(&self, _limit: usize) -> Vec<crate::common::TaskDefinition> {
        Vec::new()
    }

    async fn drain_unrouted(&self) -> Vec<crate::common::TaskDefinition> {
        Vec::new()
    }

    async fn is_empty(&self) -> bool {
        true
    }

    async fn len(&self) -> usize {
        0
    }

    fn total_queued(&self) -> usize {
        0
    }

    fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Relaxed)
    }
}
