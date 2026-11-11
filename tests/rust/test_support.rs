//! 测试辅助工具（仅测试构建或 `test-support` feature 编译）。
//!
//! 为多个模块的单测提供共享的 mock 基础设施，避免在每个测试模块内
//! 重复实现 `Transport` / 容量跟踪等桩。遵循 AGENTS.md「生产代码禁止
//! 桩函数与 Mock」——本文件只在 `cfg(test)` 或 `test-support` feature 下
//! 编译，门禁在 `src/lib.rs`。
//!
//! 本文件同时导出可复用的**契约测试**（`assert_scheduler_contract` /
//! `assert_transport_contract`）：第三方实现 `Scheduler` / `Transport` 时可直接
//! 调用，把文档里的语义承诺跑成断言。

use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::{ActantError, NodeId, Result, TaskDefinition, TaskId};
use crate::runtime::network::{
    DirectRequest, DirectResponse, DirectResponseChannel, ListenAddresses, NetworkEvent, Transport,
};
use crate::runtime::workflow::Scheduler;

// Mock transport 广播记录类型。
type BroadcastLog = Arc<Mutex<Vec<(String, Vec<u8>)>>>;
// Mock transport 直接响应记录类型：(channel, response)。
type DirectResponseLog = Arc<Mutex<Vec<(DirectResponseChannel, DirectResponse)>>>;

/// 记录所有 `broadcast` 调用的最小 `Transport` 桩。
///
/// - `broadcasts`：按调用顺序记录 `(topic, data)`。
/// - `subscribed`：记录 `subscribe` 调用过的 topic。
/// - `direct_responses`：记录 `send_direct_response` 调用过的 `(channel, response)`。
///
/// 其余方法返回默认值或 `None`；足以支撑不依赖真实网络的纯逻辑测试。
#[derive(Clone)]
pub struct MockTransport {
    pub node_id: NodeId,
    pub local_peer_id: String,
    pub broadcasts: BroadcastLog,
    pub subscribed: Arc<Mutex<Vec<String>>>,
    pub direct_responses: DirectResponseLog,
    /// `send_direct_request` 的应答。`None`（默认）表示对端不响应（返回错误），
    /// 用于模拟不可达对端；设为 `Some(Pong)` 即模拟存活对端。
    pub direct_request_response: Arc<Mutex<Option<DirectResponse>>>,
}

#[allow(dead_code)]
impl MockTransport {
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: NodeId::from(node_id.to_string()),
            local_peer_id: format!("peer-{}", node_id),
            broadcasts: Arc::new(Mutex::new(Vec::new())),
            subscribed: Arc::new(Mutex::new(Vec::new())),
            direct_responses: Arc::new(Mutex::new(Vec::new())),
            direct_request_response: Arc::new(Mutex::new(None)),
        }
    }

    /// 设置所有 `send_direct_request` 的应答（用于存活探测等请求-响应测试）。
    pub fn with_direct_request_response(&self, response: DirectResponse) {
        *self.direct_request_response.lock() = Some(response);
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

    /// 取走并返回所有记录到的 `send_direct_response` 调用。
    pub fn take_responses(&self) -> Vec<(DirectResponseChannel, DirectResponse)> {
        std::mem::take(&mut *self.direct_responses.lock())
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
        match self.direct_request_response.lock().clone() {
            Some(response) => Ok(response),
            None => Err(ActantError::Internal(
                "MockTransport: send_direct_request not implemented".into(),
            )),
        }
    }

    async fn send_direct_response(
        &self,
        channel: DirectResponseChannel,
        response: DirectResponse,
    ) -> Result<()> {
        self.direct_responses.lock().push((channel, response));
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

// ── 可复用契约测试 ──

/// 构造契约测试用任务。
///
/// `priority` 仅对 priority 调度器有意义；FIFO 调度器按入队顺序消费。
pub fn contract_task(name: &str, priority: i32) -> TaskDefinition {
    TaskDefinition {
        tags: Vec::new(),
        quota: None,
        pinned_target: false,
        id: TaskId::generate(),
        name: name.to_string(),
        payload: Vec::new(),
        workflow_id: None,
        target_node: None,
        origin_node: None,
        retry_policy: None,
        priority,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
        lease_deadline_ms: 0,
    }
}

/// 断言调度器满足 `Scheduler` 的公共语义契约。
///
/// 覆盖文档承诺的五件事：计数自洽、出队语义、批量出队、只 drain 未路由任务、
/// `close` 的幂等与 drain 行为。适用于生产实现（`ActorScheduler`）与第三方实现；
/// `MockScheduler` 只是空桩，不满足本契约。
///
/// 调用前调度器必须为空且未关闭。
pub async fn assert_scheduler_contract<S: Scheduler + ?Sized>(scheduler: &S) {
    assert!(!scheduler.is_closed(), "scheduler must start open");
    assert!(scheduler.is_empty().await, "scheduler must start empty");
    assert_eq!(scheduler.len().await, 0, "empty scheduler len must be 0");

    let Ok(()) = scheduler.enqueue(contract_task("contract-single", 0)).await else {
        panic!("open scheduler must accept enqueue");
    };
    assert_eq!(scheduler.len().await, 1, "len must track enqueued tasks");
    assert!(
        !scheduler.is_empty().await,
        "is_empty must track queue state"
    );

    let Some(first) = scheduler.try_dequeue().await else {
        panic!("queued task must be dequeueable");
    };
    assert_eq!(first.name, "contract-single");
    assert!(
        scheduler.is_empty().await,
        "queue must be empty after dequeue"
    );

    let Ok(()) = scheduler
        .enqueue_batch(vec![
            contract_task("contract-batch-a", 1),
            contract_task("contract-batch-b", 2),
        ])
        .await
    else {
        panic!("open scheduler must accept enqueue_batch");
    };
    assert_eq!(scheduler.len().await, 2);
    let batch = scheduler.dequeue_batch(10).await;
    assert_eq!(batch.len(), 2, "dequeue_batch must return all queued tasks");
    assert!(scheduler.is_empty().await);

    let Ok(()) = scheduler
        .enqueue(contract_task("contract-unrouted", 0))
        .await
    else {
        panic!("open scheduler must accept enqueue");
    };
    let mut routed = contract_task("contract-routed", 0);
    routed.target_node = Some(NodeId::from("contract-peer".to_string()));
    let Ok(()) = scheduler.enqueue(routed).await else {
        panic!("open scheduler must accept enqueue");
    };
    let drained = scheduler.drain_unrouted().await;
    assert_eq!(drained.len(), 1, "drain_unrouted must skip routed tasks");
    assert_eq!(drained[0].name, "contract-unrouted");
    let Some(remaining) = scheduler.try_dequeue().await else {
        panic!("routed task must remain queued after drain_unrouted");
    };
    assert_eq!(remaining.name, "contract-routed");

    let Ok(()) = scheduler
        .enqueue(contract_task("contract-retained", 0))
        .await
    else {
        panic!("open scheduler must accept enqueue");
    };
    scheduler.close();
    assert!(scheduler.is_closed(), "close must be visible immediately");
    scheduler.close();
    assert!(scheduler.is_closed(), "close must be idempotent");

    assert!(
        scheduler
            .enqueue(contract_task("contract-rejected", 0))
            .await
            .is_err(),
        "closed scheduler must reject enqueue"
    );
    assert!(
        scheduler
            .enqueue_batch(vec![contract_task("contract-rejected-batch", 0)])
            .await
            .is_err(),
        "closed scheduler must reject enqueue_batch"
    );

    let Some(retained) = scheduler.try_dequeue().await else {
        panic!("close must retain already queued tasks");
    };
    assert_eq!(retained.name, "contract-retained");
    let closed_dequeue =
        tokio::time::timeout(std::time::Duration::from_secs(2), scheduler.dequeue()).await;
    assert!(closed_dequeue.is_ok(), "dequeue must wake after close");
    assert!(
        closed_dequeue.unwrap_or_default().is_none(),
        "dequeue must return None once closed and drained"
    );
}

/// 断言传输实现满足 `Transport` 的公共语义契约。
///
/// 覆盖身份、订阅幂等、广播、地址可见性、peer 查询与关闭幂等。测试替身
/// （`MockTransport`）与自定义传输都应满足。
pub async fn assert_transport_contract<T: Transport + ?Sized>(transport: &T, probe_topic: &str) {
    assert!(
        !transport.node_id().as_str().is_empty(),
        "transport must expose a non-empty node id"
    );
    assert!(
        !transport.local_peer_id().is_empty(),
        "transport must expose a non-empty local peer id"
    );

    let Ok(()) = transport.subscribe(probe_topic).await else {
        panic!("transport must accept subscribe");
    };
    let Ok(()) = transport.subscribe(probe_topic).await else {
        panic!("repeated subscribe must be tolerated");
    };
    let Ok(()) = transport.broadcast(probe_topic, b"contract".to_vec()).await else {
        panic!("transport must broadcast on a subscribed topic");
    };

    let Ok(addrs) = transport.listen_addresses() else {
        panic!("transport must expose listen addresses");
    };
    assert!(
        !addrs.endpoint_id.is_empty(),
        "listen addresses must carry an endpoint id"
    );

    if transport.discover_peers().await.is_err() {
        panic!("discover_peers must not fail for a healthy transport");
    }

    let Ok(()) = transport.shutdown().await else {
        panic!("transport must shut down cleanly");
    };
    let Ok(()) = transport.shutdown().await else {
        panic!("repeated shutdown must be tolerated");
    };
}
