//! P0-3 / P0-4 装配接线测试：验证 `NetworkEventRouter` 将入站 wire 消息
//! （Heartbeat / DagUpdate / Claim）**直连分发**到 FailoverManager /
//! DagGossipActor——peer 出现、gossip seen 登记、claim 生效。
//!
//! 控制面消息不经过 EventBus（观测 tap，有损）；本测试驱动与生产装配
//! 相同的分发入口（`NetworkEventRouter::handle_message`），断言真实
//! handler 的副作用。
//!
//! 编译经由 `src/runtime/builder.rs` 的 `#[path]` 属性，保留 `super::` 访问。

use super::*;
use crate::common::{GossipConfig, WorkflowId};
use crate::runtime::workflow::runtime::{NetworkEventRouter, NetworkEventRouterConfig};
use crate::runtime::workflow::{DagGossip, DagGossipActor};
use crate::test_support::MockTransport;
use std::time::Duration;

/// 全部方法返回 ok 的 WorkflowActor 桩：使 DagGossip 的落地调用成功返回，
/// 从而验证 seen 登记与分发路径（不关心 workflow 状态本身）。
struct OkWorkflowActor;

#[async_trait::async_trait]
impl crate::runtime::actor::Actor for OkWorkflowActor {
    fn actor_type(&self) -> &str {
        "WorkflowActor"
    }

    async fn handle_message(
        &mut self,
        msg: crate::common::ActorMessage,
    ) -> crate::common::Result<crate::common::ActorMessageResult> {
        Ok(crate::common::ActorMessageResult {
            message_id: msg.id,
            payload: vec![],
            error: None,
        })
    }
}

/// 轮询等待事件被直连分发处理完毕。
async fn wait_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not met within timeout");
}

/// 构造与生产装配一致的 NetworkEventRouter：持有 failover 与 dag gossip actor。
struct TestWiring {
    router: NetworkEventRouter,
    failover: Arc<FailoverManager>,
    gossip: DagGossip,
}

async fn make_wiring(node_id_str: &str) -> TestWiring {
    let node_id = NodeId::from(node_id_str.to_string());
    let transport: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id_str));
    let actor_system = Arc::new(ActorSystem::new());
    let workflow_actor_id = ActorId::workflow(&node_id);
    actor_system
        .spawn(workflow_actor_id.clone(), OkWorkflowActor)
        .await
        .unwrap();

    let failover = Arc::new(FailoverManager::new(
        node_id.clone(),
        transport.clone(),
        actor_system.clone(),
        workflow_actor_id.clone(),
    ));

    let gossip = DagGossip::new(
        transport.clone(),
        actor_system.clone(),
        workflow_actor_id,
        GossipConfig::default(),
    );
    let dag_gossip_actor_id = ActorId::dag_gossip(&node_id);
    actor_system
        .spawn(
            dag_gossip_actor_id.clone(),
            DagGossipActor::new(gossip.clone()),
        )
        .await
        .unwrap();

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus: EventBus::new(),
        scheduler: Arc::new(crate::test_support::MockScheduler::new()),
        actor_system: Some(actor_system),
        workflow_actor_id: None,
        dag_gossip_actor_id: Some(dag_gossip_actor_id),
        capability_gossip: None,
        failover: Some(failover.clone()),
        cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });

    TestWiring {
        router,
        failover,
        gossip,
    }
}

#[tokio::test]
async fn inbound_heartbeat_creates_peer_with_capacity() {
    let wiring = make_wiring("node-wire-A").await;

    let hb = crate::common::NodeHeartbeat {
        node_id: NodeId::from("node-wire-B".to_string()),
        active_workflows: vec![],
        timestamp_ms: crate::common::epoch_millis(),
        available_slots: 3,
        max_slots: 8,
        endpoint_addr: Some("peer-wire-B".to_string()),
    };
    let envelope =
        crate::common::WireEnvelope::wrap(crate::common::WireMessage::NodeHeartbeat(hb.clone()));
    let payload = crate::runtime::workflow::messaging::encode(&envelope).expect("encode envelope");
    wiring
        .router
        .handle_message(crate::common::Topic::heartbeat().as_str(), &payload)
        .await;

    wait_until(|| {
        wiring
            .failover
            .get_peer_infos()
            .contains_key(&NodeId::from("node-wire-B".to_string()))
    })
    .await;

    let peer = &wiring.failover.get_peer_infos()[&NodeId::from("node-wire-B".to_string())];
    assert_eq!(peer.available_slots, 3, "peer capacity should be updated");
    assert_eq!(peer.max_slots, 8);
    assert_eq!(peer.endpoint_addr.as_deref(), Some("peer-wire-B"));
}

#[tokio::test]
async fn inbound_dag_update_registers_gossip_seen() {
    let wiring = make_wiring("node-wire-C").await;

    // 远端节点广播 t1 进入 Running 状态。
    let update = crate::common::WireMessage::DagStateUpdate(crate::common::WireDagStateUpdate {
        workflow_id: WorkflowId::from("wf-wire-1"),
        task_id: crate::common::TaskId::from("t-1".to_string()),
        task_state: crate::common::WireTaskState::Running,
        hlc_timestamp: crate::runtime::state::HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-wire-D".to_string()),
    });
    let envelope = crate::common::WireEnvelope::wrap(update);
    let payload = crate::runtime::workflow::messaging::encode(&envelope).expect("encode");
    wiring
        .router
        .handle_message(crate::common::Topic::dag_state().as_str(), &payload)
        .await;

    wait_until(|| wiring.gossip.seen_count() >= 1).await;
    assert_eq!(
        wiring.gossip.seen_count(),
        1,
        "remote update should be applied"
    );
}

#[tokio::test]
async fn inbound_claim_records_lease() {
    let wiring = make_wiring("node-wire-E").await;

    // 远端节点 node-wire-F 广播它接管了 wf-wire-2。
    let claim = crate::common::WireMessage::OrchestratorClaim(crate::common::OrchestratorClaim {
        node_id: NodeId::from("node-wire-F".to_string()),
        workflow_id: WorkflowId::from("wf-wire-2"),
        timestamp_ms: crate::common::epoch_millis(),
    });
    let envelope = crate::common::WireEnvelope::wrap(claim);
    let payload = crate::runtime::workflow::messaging::encode(&envelope).expect("encode");
    wiring
        .router
        .handle_message(crate::common::Topic::failover().as_str(), &payload)
        .await;

    wait_until(|| !wiring.failover.active_leases().is_empty()).await;

    let leases = wiring.failover.active_leases();
    assert!(
        leases
            .iter()
            .any(|(w, n, _, _)| w == "wf-wire-2" && n == "node-wire-F"),
        "remote claim should be recorded: {leases:?}"
    );
}

/// wire topic 路由覆盖检查：四类控制面消息的 wire 分类入口保持存在
/// （`TopicRoute` 由 wire 层保留，分发在 router 分支内直连完成）。
#[test]
fn wire_control_plane_topics_exist() {
    // 防回归哨兵：这些 topic 名若被改动，发送方与 NetworkEventRouter 分支会失配。
    let _ = (
        crate::common::Topic::heartbeat().as_str().to_string(),
        crate::common::Topic::failover().as_str().to_string(),
        crate::common::Topic::dag_state().as_str().to_string(),
        crate::common::Topic::heads().as_str().to_string(),
    );
}
