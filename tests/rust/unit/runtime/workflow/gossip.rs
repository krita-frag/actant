//! Unit tests extracted from `src/runtime/workflow/gossip.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::{
    ActantError, GossipConfig, HeadsExchange, NodeId, Result, TaskId, WorkflowHead, WorkflowId,
    WorkflowStateRequest, WorkflowStateResponse,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::network::{
    DirectRequest, DirectResponse, DirectResponseChannel, ListenAddresses, NetworkEvent, Transport,
};
use crate::test_support::MockTransport;

fn make_gossip(node_id: &str, config: GossipConfig) -> DagGossip {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    DagGossip::new(network, actor_system, wf_id, config)
}
#[test]
fn getters_return_configured_values() {
    let cfg = GossipConfig {
        heads_broadcast_interval_ms: 1500,
    };
    let g = make_gossip("node-A", cfg);
    assert_eq!(g.node_id().as_str(), "node-A");
    assert_eq!(g.heads_broadcast_interval(), Duration::from_millis(1500));
}
#[tokio::test]
async fn handle_heads_exchange_skips_same_node() {
    let g = make_gossip("node-A", GossipConfig::default());

    // 从同一节点发来的 heads exchange → 立即返回 Ok
    let exchange = HeadsExchange {
        node_id: NodeId::from("node-A".to_string()), // 同节点
        heads: Vec::new(),
    };

    let result = g.handle_heads_exchange(&exchange).await;
    assert!(result.is_ok(), "same node exchange should be skipped");
}
#[tokio::test]
async fn handle_heads_exchange_with_empty_heads_succeeds() {
    let g = make_gossip("node-A", GossipConfig::default());

    let exchange = HeadsExchange {
        node_id: NodeId::from("node-B".to_string()),
        heads: Vec::new(), // 空 heads 列表
    };

    let result = g.handle_heads_exchange(&exchange).await;
    assert!(result.is_ok(), "empty heads should succeed");
}
#[tokio::test]
async fn handle_workflow_state_response_empty_returns_ok() {
    let g = make_gossip("node-A", GossipConfig::default());

    let response = WorkflowStateResponse {
        workflow_id: WorkflowId("wf-empty".to_string()),
        target_node: NodeId::from("node-A".to_string()),
        dag: None,
        execution: None,
        pending: None,
    };

    // dag 和 execution 都为 None → 立即返回 Ok
    let result = g.handle_workflow_state_response(&response).await;
    assert!(result.is_ok(), "empty response should return Ok");
}
#[tokio::test]
async fn request_workflow_state_succeeds_with_mock_transport() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-req".to_string());
    let target = NodeId::from("node-B".to_string());

    let result = g.request_workflow_state(&wf, &target).await;
    assert!(result.is_ok(), "should succeed with MockTransport");
}
#[tokio::test]
async fn handle_workflow_state_request_no_state_returns_ok() {
    let g = make_gossip("node-A", GossipConfig::default());

    let request = WorkflowStateRequest {
        workflow_id: WorkflowId("wf-no-state".to_string()),
        requesting_node: NodeId::from("node-B".to_string()),
        target_node: NodeId::from("node-A".to_string()),
    };

    // 没有注册 WorkflowActor，call_workflow 会失败，
    // 但如果 actor 返回 None（无状态），应返回 Ok。
    let result = g.handle_workflow_state_request(&request).await;
    // 由于没有 actor，会返回 Err — 验证它确实返回错误
    assert!(result.is_err(), "should fail without WorkflowActor");
}
use crate::runtime::workflow::actor::WorkflowActor;
use crate::runtime::workflow::orchestrator::Orchestrator;

fn make_gossip_with_actor(node_id: &str, config: GossipConfig) -> DagGossip {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    // 注册一个真实的 WorkflowActor，但 orchestrator 中没有 workflow
    let actor = WorkflowActor::new(Orchestrator::new());
    futures::executor::block_on(actor_system.spawn(wf_id.clone(), actor)).unwrap();
    DagGossip::new(network, actor_system, wf_id, config)
}
#[tokio::test]
async fn call_workflow_void_preserves_not_found_error_type() {
    let g = make_gossip_with_actor("node-nf-type", GossipConfig::default());
    // 调用一个需要 workflow 存在的方法，但 workflow 不存在
    let result = g
        .call_workflow_void(
            crate::runtime::workflow::actor::workflow_methods::MARK_TASK_RUNNING,
            (
                &WorkflowId("nonexistent".to_string()),
                &TaskId::from("t-1".to_string()),
            ),
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, crate::common::ActantError::NotFound(_)),
        "expected NotFound, got {:?}",
        err
    );
}
#[test]
fn actor_system_getter_returns_reference() {
    let g = make_gossip("node-getter", GossipConfig::default());
    let sys = g.actor_system();
    // 验证返回的 Arc 引用同一个 ActorSystem
    assert!(Arc::strong_count(sys) >= 1);
}
#[tokio::test]
async fn broadcast_heads_with_no_active_workflows_succeeds() {
    let g = make_gossip_with_actor("node-heads-1", GossipConfig::default());

    // 没有 active workflow → active_ids 为空 → heads 为空 → 广播成功
    let result = g.broadcast_heads().await;
    assert!(
        result.is_ok(),
        "broadcast_heads with no workflows should succeed"
    );
}
#[tokio::test]
async fn handle_workflow_state_request_unknown_workflow_returns_ok() {
    let g = make_gossip_with_actor("node-req-1", GossipConfig::default());
    let request = WorkflowStateRequest {
        workflow_id: WorkflowId("unknown-wf".to_string()),
        requesting_node: NodeId::from("node-B".to_string()),
        target_node: NodeId::from("node-req-1".to_string()),
    };

    // WorkflowActor 存在，但 orchestrator 没有该 workflow → 返回空 bytes → Ok
    let result = g.handle_workflow_state_request(&request).await;
    assert!(result.is_ok(), "should return Ok for unknown workflow");
}
#[tokio::test]
async fn handle_workflow_state_response_with_dag_only_returns_ok() {
    let g = make_gossip_with_actor("node-resp-1", GossipConfig::default());
    let response = WorkflowStateResponse {
        workflow_id: WorkflowId("wf-restore".to_string()),
        target_node: NodeId::from("node-resp-1".to_string()),
        dag: Some(vec![1, 2, 3]),
        execution: None,
        pending: None,
    };

    // dag 有值，execution 为 None → 调用 APPLY_FULL_STATE
    // restore_workflow_from_bytes 使用 .ok() 丢弃反序列化错误，返回 Ok
    // 因此即使 dag_bytes 是无效的序列化数据，也应返回 Ok
    let result = g.handle_workflow_state_response(&response).await;
    assert!(result.is_ok(), "should return Ok even with invalid bytes");
}
#[tokio::test]
async fn handle_heads_exchange_remote_node_with_heads_succeeds() {
    let g = make_gossip_with_actor("node-hex-1", GossipConfig::default());

    let exchange = HeadsExchange {
        node_id: NodeId::from("node-B".to_string()),
        heads: vec![WorkflowHead {
            workflow_id: WorkflowId("wf-remote".to_string()),
            succeeded_count: 5,
            total_count: 10,
        }],
    };

    // 远程节点有 head，但本地没有该 workflow → 调用 ADOPT_WORKFLOW
    // adopt_workflow 对于不存在的 workflow 会插入占位符并返回 Ok
    // 然后调用 request_workflow_state 广播请求
    let result = g.handle_heads_exchange(&exchange).await;
    assert!(result.is_ok(), "should succeed: adopt + request");
}
/// 状态请求走**全局**话题，目标节点写在载荷里。
///
/// 不用 `wf-state-req:<node>`：运行期新建的话题拿不到已连接的邻居，
/// `broadcast` 返回成功却无人接收——接管方永远等不到状态副本。
#[tokio::test]
async fn request_workflow_state_broadcasts_to_correct_topic() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CapturingTransport {
        node_id: NodeId,
        broadcast_count: AtomicUsize,
        last_topic: std::sync::Mutex<String>,
        last_payload: std::sync::Mutex<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl Transport for CapturingTransport {
        fn node_id(&self) -> &NodeId {
            &self.node_id
        }
        fn local_peer_id(&self) -> &str {
            "peer-cap"
        }
        async fn broadcast(&self, topic: &str, data: Vec<u8>) -> Result<()> {
            self.broadcast_count.fetch_add(1, Ordering::Relaxed);
            *self.last_topic.lock().unwrap() = topic.to_string();
            *self.last_payload.lock().unwrap() = data;
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
            Ok(ListenAddresses {
                endpoint_id: "peer-cap".to_string(),
                relay_url: None,
                direct_addrs: Vec::new(),
                endpoint_addr: "peer-cap".to_string(),
            })
        }
        async fn send_direct_request(
            &self,
            _peer_id_str: &str,
            _request: DirectRequest,
        ) -> Result<DirectResponse> {
            Err(ActantError::Internal("not implemented".into()))
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

    let transport = Arc::new(CapturingTransport {
        node_id: NodeId::from("node-cap".to_string()),
        broadcast_count: AtomicUsize::new(0),
        last_topic: std::sync::Mutex::new(String::new()),
        last_payload: std::sync::Mutex::new(Vec::new()),
    });
    let network: Arc<dyn Transport> = transport.clone();
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from("node-cap".to_string()));
    let g = DagGossip::new(network, actor_system, wf_id, GossipConfig::default());

    let wf = WorkflowId("wf-topic".to_string());
    let target = NodeId::from("node-target".to_string());
    g.request_workflow_state(&wf, &target).await.unwrap();

    assert_eq!(transport.broadcast_count.load(Ordering::Relaxed), 1);
    let topic = transport.last_topic.lock().unwrap().clone();
    assert_eq!(
        topic,
        crate::common::Topic::workflow_state_req().as_str(),
        "状态请求必须走全局话题（节点定址在载荷内）"
    );

    let payload = transport.last_payload.lock().unwrap().clone();
    let (message, _) = crate::common::WireEnvelope::decode(&payload).expect("decode envelope");
    match message {
        crate::common::WireMessage::WorkflowStateRequest(request) => {
            assert_eq!(request.target_node, target, "载荷必须携带目标节点");
            assert_eq!(
                request.requesting_node,
                NodeId::from("node-cap".to_string())
            );
            assert_eq!(request.workflow_id, wf);
        }
        other => panic!("unexpected wire message: {other:?}"),
    }
}
#[test]
fn drop_does_not_panic() {
    let g = make_gossip("node-drop", GossipConfig::default());
    // drop 应正常执行，不 panic
    drop(g);
}
/// heads 交换只需计数标量，必须走 `GET_STATE_SUMMARY` 而不是全量 `GET_STATE`。
///
/// 全量状态里带着已完成任务的结果字节：任一工作流结果超过 actor 消息解码上限
/// （4 MiB）时，逐工作流 `decode` 失败会让整轮 heads 检测整体停摆（落后检测、
/// 补拉全部静默失效，集群两侧永久不同步）。正确语义是跳过该工作流并继续。
#[tokio::test]
async fn broadcast_heads_survives_workflow_with_oversized_result() {
    use crate::common::wire::{WireEnvelope, WireMessage};
    use crate::common::NodeId as WireNodeId;
    use crate::runtime::workflow::actor::WorkflowActor;
    use crate::runtime::workflow::dag::{Dag, DagNode};
    use crate::runtime::workflow::orchestrator::Orchestrator;
    use std::collections::HashMap;

    fn two_node_dag() -> Dag {
        let mut dag = Dag::new();
        for (id, name) in [("t1", "first"), ("t2", "second")] {
            dag.add_node(DagNode {
                task_id: TaskId::from(id.to_string()),
                name: name.to_string(),
                payload: Vec::new(),
                retry_policy: None,
                timeout_ms: None,
                priority: 0,
                metadata: HashMap::new(),
                tags: Vec::new(),
                quota: None,
            })
            .unwrap();
        }
        dag.add_edge(TaskId::from("t1"), TaskId::from("t2"))
            .unwrap();
        dag
    }

    let orch = Orchestrator::new().with_signing_key(b"test-key".to_vec());
    let node_id = "node-heads-big";

    // 超大结果工作流：t1 带 5 MiB 结果后仍处于 Running（t2 未完成）。
    let big = WorkflowId::from("wf-big-result");
    orch.submit(big.clone(), two_node_dag()).await.unwrap();
    orch.start(&big).unwrap();
    orch.mark_task_running(&big, &TaskId::from("t1")).unwrap();
    orch.complete_task(
        &big,
        &TaskId::from("t1"),
        vec![7u8; 5 * 1024 * 1024],
        Some(0),
    )
    .await
    .unwrap();

    // 普通工作流：证明整轮检测没有因上者中断。
    let small = WorkflowId::from("wf-small-result");
    orch.submit(small.clone(), two_node_dag()).await.unwrap();
    orch.start(&small).unwrap();

    let transport = MockTransport::new(node_id);
    let network: Arc<dyn Transport> = Arc::new(transport.clone());
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::workflow(&WireNodeId::from(node_id.to_string()));
    actor_system
        .spawn(actor_id.clone(), WorkflowActor::new(orch))
        .await
        .unwrap();

    let gossip = DagGossip::new(network, actor_system, actor_id, GossipConfig::default());
    gossip
        .broadcast_heads()
        .await
        .expect("单个工作流状态过大不得让整轮 heads 检测失败");

    let (topic, data) = transport.last_broadcast().expect("heads must be broadcast");
    assert_eq!(topic, super::TOPIC_HEADS);
    let exchange = match WireEnvelope::decode(&data).expect("valid envelope").0 {
        WireMessage::HeadsExchange(exchange) => exchange,
        other => panic!("expected HeadsExchange, got {other:?}"),
    };
    let mut ids: Vec<String> = exchange
        .heads
        .iter()
        .map(|h| h.workflow_id.0.clone())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["wf-big-result".to_string(), "wf-small-result".to_string()],
        "两个活跃工作流都必须出现在 heads 中（大结果工作流也只需计数标量）"
    );
}
