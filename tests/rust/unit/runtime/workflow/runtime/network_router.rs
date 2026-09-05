//! Unit tests extracted from `src/runtime/workflow/runtime/network_router.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::{
    ActorId, ActorMessage, ActorMessageResult, NodeId, TaskCompletion, TaskDefinition, TaskId,
    Topic, WireEnvelope, WireMessage, WireTaskOutcome, WorkflowId,
};
use crate::runtime::actor::{Actor, ActorSystem};
use crate::runtime::dispatcher::CancelFlag;
use crate::runtime::event_bus::{BusEvent, EventBus, Topic as BusTopic};
use crate::runtime::network::{
    DirectRequest, DirectResponse, DirectResponseChannel, ListenAddresses, NetworkEvent,
    NetworkMessage, PeerId, Transport,
};
use crate::runtime::workflow::Scheduler;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

// ───────────────────────── Mock Transport ─────────────────────────

struct MockTransport {
    node_id: NodeId,
    local_peer_id: String,
    responses: StdMutex<Vec<(DirectResponseChannel, DirectResponse)>>,
}

impl MockTransport {
    fn new(node_id: &str, peer_id: &str) -> Self {
        Self {
            node_id: NodeId::from(node_id.to_string()),
            local_peer_id: peer_id.to_string(),
            responses: StdMutex::new(Vec::new()),
        }
    }

    fn take_responses(&self) -> Vec<(DirectResponseChannel, DirectResponse)> {
        std::mem::take(&mut *self.responses.lock().unwrap())
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

    async fn broadcast(&self, _topic: &str, _data: Vec<u8>) -> crate::common::Result<()> {
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
        Err(crate::common::ActantError::Internal("mock".into()))
    }

    async fn send_direct_request(
        &self,
        _peer_id_str: &str,
        _request: DirectRequest,
    ) -> crate::common::Result<DirectResponse> {
        Err(crate::common::ActantError::Internal("mock".into()))
    }

    async fn send_direct_response(
        &self,
        channel: DirectResponseChannel,
        response: DirectResponse,
    ) -> crate::common::Result<()> {
        self.responses.lock().unwrap().push((channel, response));
        Ok(())
    }

    async fn discover_peers(&self) -> crate::common::Result<Vec<PeerId>> {
        Ok(vec![])
    }

    async fn shutdown(&self) -> crate::common::Result<()> {
        Ok(())
    }
}

// ───────────────────────── Mock Scheduler ─────────────────────────

struct MockScheduler {
    enqueued: StdMutex<Vec<TaskDefinition>>,
    closed: AtomicBool,
}

impl MockScheduler {
    fn new() -> Self {
        Self {
            enqueued: StdMutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        }
    }

    fn take_enqueued(&self) -> Vec<TaskDefinition> {
        std::mem::take(&mut *self.enqueued.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl Scheduler for MockScheduler {
    async fn enqueue(&self, task: TaskDefinition) -> crate::common::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::common::ActantError::Internal(
                "scheduler is closed".into(),
            ));
        }
        self.enqueued.lock().unwrap().push(task);
        Ok(())
    }

    async fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> crate::common::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::common::ActantError::Internal(
                "scheduler is closed".into(),
            ));
        }
        self.enqueued.lock().unwrap().extend(tasks);
        Ok(())
    }

    async fn dequeue(&self) -> Option<TaskDefinition> {
        self.enqueued.lock().unwrap().pop()
    }

    async fn try_dequeue(&self) -> Option<TaskDefinition> {
        self.dequeue().await
    }

    async fn dequeue_batch(&self, limit: usize) -> Vec<TaskDefinition> {
        let mut guard = self.enqueued.lock().unwrap();
        let count = guard.len().min(limit);
        guard.drain(..count).collect()
    }

    async fn drain_unrouted(&self) -> Vec<TaskDefinition> {
        let mut guard = self.enqueued.lock().unwrap();
        let mut routed = Vec::new();
        let mut unrouted = Vec::new();
        for task in guard.drain(..) {
            if task.target_node.is_some() {
                routed.push(task);
            } else {
                unrouted.push(task);
            }
        }
        *guard = routed;
        unrouted
    }

    async fn is_empty(&self) -> bool {
        self.enqueued.lock().unwrap().is_empty()
    }

    async fn len(&self) -> usize {
        self.enqueued.lock().unwrap().len()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

// ───────────────────────── Echo Actor ─────────────────────────

struct EchoActor;

#[async_trait::async_trait]
impl Actor for EchoActor {
    fn actor_type(&self) -> &str {
        "echo"
    }

    async fn handle_message(
        &mut self,
        msg: ActorMessage,
    ) -> crate::common::Result<ActorMessageResult> {
        Ok(ActorMessageResult {
            message_id: msg.id,
            payload: vec![],
            error: None,
        })
    }
}

// ───────────────────────── Test Helpers ─────────────────────────

fn make_router(
    network: Arc<dyn Transport>,
    scheduler: Arc<dyn Scheduler>,
    actor_system: Option<Arc<ActorSystem>>,
) -> NetworkEventRouter {
    NetworkEventRouter::new(NetworkEventRouterConfig {
        network,
        event_bus: EventBus::new(),
        scheduler,
        actor_system,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    })
}

fn make_task(name: &str) -> TaskDefinition {
    TaskDefinition {
        id: TaskId::from(format!("task-{}", name)),
        name: name.to_string(),
        payload: Vec::new(),
        workflow_id: None,
        target_node: None,
        origin_node: None,
        retry_policy: None,
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    }
}

// ───────────────────────── Tests ─────────────────────────

#[tokio::test]
async fn handle_task_topic_enqueues_task() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let mock_scheduler = Arc::new(MockScheduler::new());
    let scheduler: Arc<dyn Scheduler> = mock_scheduler.clone();
    let router = make_router(transport.clone(), scheduler, None);

    let task = make_task("t1");
    let envelope = WireEnvelope::wrap(WireMessage::TaskDispatch(task.clone()));
    let payload = postcard::to_allocvec(&envelope).unwrap();

    router
        .handle(NetworkEvent::Message(NetworkMessage {
            topic: Topic::task(&NodeId::from("node-A".to_string()))
                .as_str()
                .to_string(),
            data: payload,
        }))
        .await;

    let enqueued = mock_scheduler.take_enqueued();
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueued[0].name, "t1");
}

#[tokio::test]
async fn handle_direct_dispatch_task_sends_ack() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let router = make_router(transport.clone(), scheduler, None);

    let task = make_task("direct");
    let channel = DirectResponseChannel::test_stub();
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-B".to_string(),
            request: Box::new(DirectRequest::DispatchTask { task: task.clone() }),
            channel,
        })
        .await;

    let responses = transport.take_responses();
    assert_eq!(responses.len(), 1);
    assert!(matches!(
        responses[0].1,
        DirectResponse::DispatchAck { accepted: true }
    ));
}

#[tokio::test]
async fn handle_direct_task_result_without_actor_publishes_event() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let mut rx = event_bus.subscribe(BusTopic::TaskCompleted);
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let channel = DirectResponseChannel::test_stub();
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-B".to_string(),
            request: Box::new(DirectRequest::TaskResult {
                workflow_id: WorkflowId::from("wf-1".to_string()),
                task_id: TaskId::from("t-1".to_string()),
                task_name: "tn".to_string(),
                outcome: WireTaskOutcome::Completed(vec![1, 2]),
                worker_node: NodeId::from("node-B".to_string()),
            }),
            channel,
        })
        .await;

    let event = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
        .await
        .expect("event should be published")
        .expect("event bus should not be closed");
    assert!(matches!(
        event,
        BusEvent::TaskCompleted(TaskCompletion::Completed { .. })
    ));
}

#[tokio::test]
async fn handle_peer_connected_publishes_event() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let mut rx = event_bus.subscribe(BusTopic::NetworkPeer);
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    router
        .handle(NetworkEvent::PeerConnected {
            peer_id: "peer-B".to_string(),
        })
        .await;

    let event = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
        .await
        .expect("event should be published")
        .expect("event bus should not be closed");
    assert!(matches!(event, BusEvent::PeerConnected(NodeId(ref s)) if s == "peer-B"));
}

#[tokio::test]
async fn handle_cancel_broadcast_sets_cancel_flag_and_publishes_event() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let mut rx = event_bus.subscribe(BusTopic::TaskCancelled);
    let cancel_flags: Arc<parking_lot::Mutex<HashMap<String, CancelFlag>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let flag = Arc::new(AtomicBool::new(false));
    cancel_flags
        .lock()
        .insert("t-cancel".to_string(), flag.clone());

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags,
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let broadcast = crate::common::wire::CancelBroadcast {
        task_id: TaskId::from("t-cancel".to_string()),
        workflow_id: WorkflowId::from("wf-1".to_string()),
    };
    let payload = postcard::to_allocvec(&broadcast).unwrap();

    router
        .handle(NetworkEvent::Message(NetworkMessage {
            topic: "actant:cancel".to_string(),
            data: payload,
        }))
        .await;

    assert!(flag.load(Ordering::Acquire));
    let event = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
        .await
        .expect("event should be published")
        .expect("event bus should not be closed");
    assert!(matches!(event, BusEvent::TaskCancelled(_)));
}

#[tokio::test]
async fn handle_workflow_state_request_routes_to_dag_gossip_actor() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let actor_system = Arc::new(ActorSystem::new());
    let dag_gossip_id = ActorId::from("dag-gossip-node-A");
    actor_system
        .spawn(dag_gossip_id.clone(), EchoActor)
        .await
        .unwrap();

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus: EventBus::new(),
        scheduler,
        actor_system: Some(actor_system.clone()),
        workflow_actor_id: None,
        dag_gossip_actor_id: Some(dag_gossip_id.clone()),
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let req = crate::common::wire::WorkflowStateRequest {
        workflow_id: WorkflowId::from("wf-1".to_string()),
        requesting_node: NodeId::from("node-B".to_string()),
    };
    let envelope = WireEnvelope::wrap(WireMessage::WorkflowStateRequest(req));
    let payload = postcard::to_allocvec(&envelope).unwrap();

    router
        .handle(NetworkEvent::Message(NetworkMessage {
            topic: Topic::workflow_state_req(&NodeId::from("node-A".to_string()))
                .as_str()
                .to_string(),
            data: payload,
        }))
        .await;
}

#[tokio::test]
async fn handle_direct_task_result_routes_to_workflow_actor() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let actor_system = Arc::new(ActorSystem::new());
    let workflow_actor_id = ActorId::from("workflow-node-A");
    actor_system
        .spawn(workflow_actor_id.clone(), EchoActor)
        .await
        .unwrap();

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport.clone(),
        event_bus: EventBus::new(),
        scheduler,
        actor_system: Some(actor_system.clone()),
        workflow_actor_id: Some(workflow_actor_id.clone()),
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let channel = DirectResponseChannel::test_stub();
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-B".to_string(),
            request: Box::new(DirectRequest::TaskResult {
                workflow_id: WorkflowId::from("wf-1".to_string()),
                task_id: TaskId::from("t-1".to_string()),
                task_name: "tn".to_string(),
                outcome: WireTaskOutcome::Completed(vec![1, 2]),
                worker_node: NodeId::from("node-B".to_string()),
            }),
            channel,
        })
        .await;

    let responses = transport.take_responses();
    assert_eq!(responses.len(), 1);
    assert!(matches!(
        responses[0].1,
        DirectResponse::TaskResultAck { accepted: true }
    ));
}

#[tokio::test]
async fn handle_peer_disconnected_publishes_event() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let mut rx = event_bus.subscribe(BusTopic::NetworkPeer);
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    router
        .handle(NetworkEvent::PeerDisconnected {
            peer_id: "peer-B".to_string(),
        })
        .await;

    let event = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
        .await
        .expect("event should be published")
        .expect("event bus should not be closed");
    assert!(matches!(event, BusEvent::PeerDisconnected(NodeId(ref s)) if s == "peer-B"));
}

#[tokio::test]
async fn handle_dag_state_topic_dispatches_to_dag_gossip_actor() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let actor_system = Arc::new(ActorSystem::new());
    let node_id = NodeId::from("node-A".to_string());
    let workflow_actor_id = ActorId::workflow(&node_id);
    actor_system
        .spawn(workflow_actor_id.clone(), EchoActor)
        .await
        .unwrap();
    let gossip = crate::runtime::workflow::DagGossip::new(
        transport.clone(),
        actor_system.clone(),
        workflow_actor_id,
        crate::common::GossipConfig::default(),
    );
    let dag_gossip_id = ActorId::dag_gossip(&node_id);
    actor_system
        .spawn(
            dag_gossip_id.clone(),
            crate::runtime::workflow::DagGossipActor::new(gossip.clone()),
        )
        .await
        .unwrap();

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus: EventBus::new(),
        scheduler,
        actor_system: Some(actor_system),
        workflow_actor_id: None,
        dag_gossip_actor_id: Some(dag_gossip_id),
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let update = crate::common::WireDagStateUpdate {
        workflow_id: WorkflowId::from("wf-1".to_string()),
        task_id: TaskId::from("t-1".to_string()),
        task_state: crate::common::WireTaskState::Completed { result: vec![1] },
        hlc_timestamp: crate::runtime::state::HlcTimestamp::from_parts(1, 2),
        origin_node: NodeId::from("node-B".to_string()),
    };
    let envelope = WireEnvelope::wrap(WireMessage::DagStateUpdate(update));
    let payload = postcard::to_allocvec(&envelope).unwrap();

    router
        .handle(NetworkEvent::Message(NetworkMessage {
            topic: Topic::dag_state().as_str().to_string(),
            data: payload,
        }))
        .await;

    // 直连分发：DagGossipActor 应登记远端更新（gossip seen）。
    for _ in 0..50 {
        if gossip.seen_count() >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(gossip.seen_count(), 1, "remote update should be applied");
}

#[tokio::test]
async fn handle_heartbeat_topic_dispatches_to_failover_manager() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let node_id = NodeId::from("node-A".to_string());
    let actor_system = Arc::new(ActorSystem::new());
    let failover = Arc::new(crate::runtime::workflow::FailoverManager::new(
        node_id.clone(),
        transport.clone(),
        actor_system,
        ActorId::workflow(&node_id),
    ));

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus: EventBus::new(),
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: Some(failover.clone()),
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let hb = crate::common::NodeHeartbeat {
        node_id: NodeId::from("node-B".to_string()),
        active_workflows: vec![WorkflowId::from("wf-1".to_string())],
        timestamp_ms: crate::common::epoch_millis(),
        available_slots: 4,
        max_slots: 8,
        endpoint_addr: None,
    };
    let envelope = WireEnvelope::wrap(WireMessage::NodeHeartbeat(hb));
    let payload = postcard::to_allocvec(&envelope).unwrap();

    router
        .handle(NetworkEvent::Message(NetworkMessage {
            topic: Topic::heartbeat().as_str().to_string(),
            data: payload,
        }))
        .await;

    // 直连分发：FailoverManager 应登记 peer 及其容量视图。
    let peers = failover.get_peer_infos();
    let peer = peers
        .get(&NodeId::from("node-B".to_string()))
        .expect("peer should be registered from heartbeat");
    assert_eq!(peer.available_slots, 4);
    assert_eq!(peer.max_slots, 8);
}

#[tokio::test]
async fn handle_failover_topic_dispatches_claim_to_failover_manager() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let node_id = NodeId::from("node-A".to_string());
    let actor_system = Arc::new(ActorSystem::new());
    let failover = Arc::new(crate::runtime::workflow::FailoverManager::new(
        node_id.clone(),
        transport.clone(),
        actor_system,
        ActorId::workflow(&node_id),
    ));

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus: EventBus::new(),
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: Some(failover.clone()),
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let claim = crate::common::OrchestratorClaim {
        node_id: NodeId::from("node-B".to_string()),
        workflow_id: WorkflowId::from("wf-1".to_string()),
        timestamp_ms: crate::common::epoch_millis(),
    };
    let envelope = WireEnvelope::wrap(WireMessage::OrchestratorClaim(claim));
    let payload = postcard::to_allocvec(&envelope).unwrap();

    router
        .handle(NetworkEvent::Message(NetworkMessage {
            topic: Topic::failover().as_str().to_string(),
            data: payload,
        }))
        .await;

    // 直连分发：远端 claim 应记录到本地租约表。
    let leases = failover.active_leases();
    assert!(
        leases
            .iter()
            .any(|(w, n, _, _)| w == "wf-1" && n == "node-B"),
        "remote claim should be recorded: {leases:?}"
    );
}

/// 记录到达方法的 Actor 桩：验证 wire 消息直连分发到了目标 actor 方法。
struct RecordingActor {
    methods: Arc<StdMutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Actor for RecordingActor {
    fn actor_type(&self) -> &str {
        "recording"
    }

    async fn handle_message(
        &mut self,
        msg: ActorMessage,
    ) -> crate::common::Result<ActorMessageResult> {
        self.methods.lock().unwrap().push(msg.method);
        Ok(ActorMessageResult {
            message_id: msg.id,
            payload: vec![],
            error: None,
        })
    }
}

#[tokio::test]
async fn handle_heads_topic_dispatches_exchange_to_dag_gossip_actor() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let actor_system = Arc::new(ActorSystem::new());
    let dag_gossip_id = ActorId::dag_gossip(&NodeId::from("node-A".to_string()));
    let methods = Arc::new(StdMutex::new(Vec::new()));
    actor_system
        .spawn(
            dag_gossip_id.clone(),
            RecordingActor {
                methods: methods.clone(),
            },
        )
        .await
        .unwrap();

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus: EventBus::new(),
        scheduler,
        actor_system: Some(actor_system),
        workflow_actor_id: None,
        dag_gossip_actor_id: Some(dag_gossip_id),
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let exchange = crate::common::HeadsExchange {
        node_id: NodeId::from("node-B".to_string()),
        heads: vec![],
    };
    let envelope = WireEnvelope::wrap(WireMessage::HeadsExchange(exchange));
    let payload = postcard::to_allocvec(&envelope).unwrap();

    router
        .handle(NetworkEvent::Message(NetworkMessage {
            topic: Topic::heads().as_str().to_string(),
            data: payload,
        }))
        .await;

    // 直连分发：HANDLE_HEADS_EXCHANGE 方法应被调用。
    for _ in 0..50 {
        if !methods.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        *methods.lock().unwrap(),
        vec![crate::runtime::workflow::gossip_methods::HANDLE_HEADS_EXCHANGE.to_string()],
        "heads exchange should be dispatched directly to the dag gossip actor"
    );
}

#[tokio::test]
async fn handle_direct_request_other_returns_error_response() {
    // 未识别的 DirectRequest 变体不再走 EventBus，而是直接回送 DirectResponse::Error。
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport.clone(),
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let channel = DirectResponseChannel::test_stub();
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-B".to_string(),
            request: Box::new(DirectRequest::QueryWorkflowState {
                workflow_id: WorkflowId::from("wf-1".to_string()),
                requesting_node: NodeId::from("node-B".to_string()),
            }),
            channel,
        })
        .await;

    let responses = transport.take_responses();
    assert_eq!(responses.len(), 1, "should send exactly one error response");
    match &responses[0].1 {
        DirectResponse::Error { message } => {
            assert!(message.contains("no handler for DirectRequest variant"));
        }
        other => panic!("expected DirectResponse::Error, got {:?}", other),
    }
}

#[tokio::test]
async fn handle_task_result_failed_publishes_event() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let mut rx = event_bus.subscribe(BusTopic::TaskFailed);
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let channel = DirectResponseChannel::test_stub();
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-B".to_string(),
            request: Box::new(DirectRequest::TaskResult {
                workflow_id: WorkflowId::from("wf-1".to_string()),
                task_id: TaskId::from("t-1".to_string()),
                task_name: "tn".to_string(),
                outcome: WireTaskOutcome::Failed("boom".to_string()),
                worker_node: NodeId::from("node-B".to_string()),
            }),
            channel,
        })
        .await;

    let event = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
        .await
        .expect("event should be published")
        .expect("event bus should not be closed");
    assert!(matches!(event, BusEvent::TaskFailed(_)));
}

#[tokio::test]
async fn handle_task_result_cancelled_publishes_event() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let mut rx = event_bus.subscribe(BusTopic::TaskCancelled);
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport,
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let channel = DirectResponseChannel::test_stub();
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-B".to_string(),
            request: Box::new(DirectRequest::TaskResult {
                workflow_id: WorkflowId::from("wf-1".to_string()),
                task_id: TaskId::from("t-1".to_string()),
                task_name: "tn".to_string(),
                outcome: WireTaskOutcome::Cancelled,
                worker_node: NodeId::from("node-B".to_string()),
            }),
            channel,
        })
        .await;

    let event = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
        .await
        .expect("event should be published")
        .expect("event bus should not be closed");
    assert!(matches!(event, BusEvent::TaskCancelled(_)));
}

#[tokio::test]
async fn handle_task_result_skipped_returns_false() {
    let transport = Arc::new(MockTransport::new("node-A", "peer-A"));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let actor_system = Arc::new(ActorSystem::new());
    let workflow_actor_id = ActorId::from("workflow-node-A");
    actor_system
        .spawn(workflow_actor_id.clone(), EchoActor)
        .await
        .unwrap();

    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: transport.clone(),
        event_bus: EventBus::new(),
        scheduler,
        actor_system: Some(actor_system.clone()),
        workflow_actor_id: Some(workflow_actor_id.clone()),
        dag_gossip_actor_id: None,
        capability_gossip: None,
        failover: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    });

    let channel = DirectResponseChannel::test_stub();
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-B".to_string(),
            request: Box::new(DirectRequest::TaskResult {
                workflow_id: WorkflowId::from("wf-1".to_string()),
                task_id: TaskId::from("t-1".to_string()),
                task_name: "tn".to_string(),
                outcome: WireTaskOutcome::Skipped,
                worker_node: NodeId::from("node-B".to_string()),
            }),
            channel,
        })
        .await;

    let responses = transport.take_responses();
    assert_eq!(responses.len(), 1);
    assert!(matches!(
        responses[0].1,
        DirectResponse::TaskResultAck { accepted: false }
    ));
}
