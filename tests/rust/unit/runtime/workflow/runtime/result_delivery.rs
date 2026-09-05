//! Unit tests extracted from `src/runtime/workflow/runtime/result_delivery.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::{ActantError, NodeId, Result};
use crate::runtime::event_bus::EventBus;
use crate::runtime::network::{
    DirectRequest, DirectResponse, DirectResponseChannel, ListenAddresses, NetworkEvent, PeerId,
    Transport,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

struct RecordingTransport {
    node_id: NodeId,
    responses: Arc<StdMutex<Vec<DirectResponse>>>,
    request_count: Arc<AtomicUsize>,
}

impl RecordingTransport {
    fn new() -> Self {
        Self {
            node_id: NodeId::from("node-A".to_string()),
            responses: Arc::new(StdMutex::new(Vec::new())),
            request_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn push_response(&self, response: DirectResponse) {
        self.responses.lock().unwrap().push(response);
    }

    fn request_count(&self) -> usize {
        self.request_count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Transport for RecordingTransport {
    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn local_peer_id(&self) -> &str {
        "peer-A"
    }

    async fn broadcast(&self, _topic: &str, _data: Vec<u8>) -> Result<()> {
        Ok(())
    }

    async fn subscribe(&self, _topic: &str) -> Result<()> {
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
        Err(ActantError::Internal("mock".into()))
    }

    async fn send_direct_request(
        &self,
        _peer_id_str: &str,
        _request: DirectRequest,
    ) -> Result<DirectResponse> {
        self.request_count.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.responses.lock().unwrap();
        if guard.is_empty() {
            Err(ActantError::Internal("no more responses".into()))
        } else {
            Ok(guard.remove(0))
        }
    }

    async fn send_direct_response(
        &self,
        _channel: DirectResponseChannel,
        _response: DirectResponse,
    ) -> Result<()> {
        Ok(())
    }

    async fn discover_peers(&self) -> Result<Vec<PeerId>> {
        Ok(vec![])
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

fn sample_request() -> DirectRequest {
    DirectRequest::TaskResult {
        workflow_id: crate::common::WorkflowId::from("wf-1".to_string()),
        task_id: crate::common::TaskId::from("t-1".to_string()),
        task_name: "tn".to_string(),
        outcome: crate::common::WireTaskOutcome::Completed(vec![1]),
        worker_node: NodeId::from("node-A".to_string()),
    }
}

#[tokio::test]
async fn try_enqueue_pending_result_accepts_when_room() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PendingResult>(1);
    let ok = try_enqueue_pending_result(&tx, "target".to_string(), sample_request(), 0, 1).await;
    assert!(ok);
    assert!(rx.recv().await.is_some());
}

#[tokio::test]
async fn try_enqueue_pending_result_returns_false_when_full() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<PendingResult>(1);
    tx.try_send(PendingResult {
        target: "first".to_string(),
        request: sample_request(),
        attempts: 0,
    })
    .unwrap();

    let ok = try_enqueue_pending_result(&tx, "second".to_string(), sample_request(), 0, 1).await;
    assert!(!ok);
}

#[tokio::test]
async fn try_enqueue_pending_result_returns_false_when_closed() {
    let (tx, rx) = tokio::sync::mpsc::channel::<PendingResult>(1);
    drop(rx);

    let ok = try_enqueue_pending_result(&tx, "target".to_string(), sample_request(), 0, 1).await;
    assert!(!ok);
}

#[tokio::test]
async fn pending_result_loop_delivers_and_stops_on_cancel() {
    let transport = Arc::new(RecordingTransport::new());
    transport.push_response(DirectResponse::TaskResultAck { accepted: true });

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let pending_tx = start_pending_result_loop(
        transport.clone(),
        cancel_rx,
        3,
        Duration::from_millis(1),
        10,
        EventBus::new(),
    );

    pending_tx
        .send(PendingResult {
            target: "target".to_string(),
            request: sample_request(),
            attempts: 0,
        })
        .await
        .unwrap();

    // 等待后台任务完成投递（1ms 延迟 + 执行时间）。
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(transport.request_count(), 1);

    // 触发取消，验证循环能正常退出。
    let _ = cancel_tx.send(true);
}

#[tokio::test]
async fn pending_result_loop_retries_on_rejected_and_drops_after_max_attempts() {
    let transport = Arc::new(RecordingTransport::new());
    // 两次 rejected 后无更多响应，第二次重试后 attempts 达到 max_attempts 被丢弃。
    transport.push_response(DirectResponse::TaskResultAck { accepted: false });
    transport.push_response(DirectResponse::TaskResultAck { accepted: false });

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let pending_tx = start_pending_result_loop(
        transport.clone(),
        cancel_rx,
        2,
        Duration::from_millis(1),
        10,
        EventBus::new(),
    );

    pending_tx
        .send(PendingResult {
            target: "target".to_string(),
            request: sample_request(),
            attempts: 0,
        })
        .await
        .unwrap();

    // 等待重试完成：第一次 1ms，第二次 2ms，再加上执行开销。
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 第一次尝试 + 一次重试 = 2 次请求，达到 max_attempts 后不再重试。
    assert_eq!(transport.request_count(), 2);

    let _ = cancel_tx.send(true);
}

#[tokio::test]
async fn pending_result_loop_drops_immediately_when_max_attempts_reached() {
    let transport = Arc::new(RecordingTransport::new());
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let pending_tx = start_pending_result_loop(
        transport.clone(),
        cancel_rx,
        1,
        Duration::from_millis(1),
        10,
        EventBus::new(),
    );

    pending_tx
        .send(PendingResult {
            target: "target".to_string(),
            request: sample_request(),
            attempts: 1,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(transport.request_count(), 0);

    let _ = cancel_tx.send(true);
}
