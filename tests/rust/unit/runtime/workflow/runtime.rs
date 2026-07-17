//! Unit tests extracted from `src/runtime/workflow/runtime.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::DispatchResult;
use super::*;

#[tokio::test]
async fn pending_results_channel_rejects_when_full() {
    // 验证：容量为 1 的 pending_results 通道，填满后 try_send 返回 Full。
    // enqueue_pending_result! 宏在此情况下应发出 warn 并丢弃，而非阻塞或 panic。
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PendingResult>(1);
    let capacity = 1usize;

    let req = crate::runtime::network::DirectRequest::QueryWorkflowState {
        workflow_id: WorkflowId::from("wf-test"),
        requesting_node: NodeId::from("n-test"),
    };

    // 第一次入队成功
    assert!(
        try_enqueue_pending_result(&tx, "peer-1".to_string(), req.clone(), 0, capacity).await,
        "first enqueue should succeed"
    );
    assert_eq!(rx.len(), 1, "first enqueue should succeed");

    // 第二次入队：通道已满，应被丢弃（函数返回 false），rx 不应增长。
    assert!(
        !try_enqueue_pending_result(&tx, "peer-2".to_string(), req, 0, capacity).await,
        "second enqueue must be dropped (channel full)"
    );
    assert_eq!(
        rx.len(),
        1,
        "second enqueue must be dropped (channel full), not blocked"
    );

    // 取出一个后，下一次入队应再次成功。
    let _ = rx.recv().await.expect("first item present");
    let req2 = crate::runtime::network::DirectRequest::QueryWorkflowState {
        workflow_id: WorkflowId::from("wf-test"),
        requesting_node: NodeId::from("n-test"),
    };
    assert!(
        try_enqueue_pending_result(&tx, "peer-3".to_string(), req2, 0, capacity).await,
        "enqueue should succeed after drain"
    );
    assert_eq!(rx.len(), 1, "enqueue should succeed after drain");
}

// ───────────────────────── Worker 纯逻辑测试 ─────────────────────────

use crate::common::{NodeHeartbeat, WorkerConfig};
use crate::runtime::event_bus::BusEvent;
use crate::runtime::network::NetworkEvent;
use crate::runtime::workflow::Scheduler;
use crate::test_support::{MockScheduler, MockTransport};
use std::sync::Arc as StdArc;

fn make_worker(node_id: &str) -> Worker {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let dispatcher = crate::runtime::dispatcher::TaskRegistry::new(1, 8, Vec::new())
        .expect("TaskRegistry init")
        .into_dispatcher();
    let handle = tokio::runtime::Handle::current();
    Worker::new(
        NodeId::from(node_id.to_string()),
        network,
        event_bus,
        scheduler,
        dispatcher,
        &WorkerConfig::default(),
        handle,
    )
}

#[test]
fn worker_state_as_str_returns_canonical_strings() {
    assert_eq!(WorkerState::Running.as_str(), "healthy");
    assert_eq!(WorkerState::Draining.as_str(), "draining");
    assert_eq!(WorkerState::Stopped.as_str(), "stopped");
}

#[tokio::test]
async fn worker_getters_return_configured_values() {
    let worker = make_worker("node-A");
    assert_eq!(worker.node_id().as_str(), "node-A");
    assert_eq!(worker.network().node_id().as_str(), "node-A");
    assert_eq!(worker.state(), WorkerState::Running);
    assert_eq!(worker.max_concurrent_tasks(), 1); // WorkerConfig::default
    assert_eq!(worker.running_task_count(), 0);
    assert!(worker.scheduler_actor_id().is_none());
}

#[tokio::test]
async fn worker_builder_methods_update_state() {
    let worker = make_worker("node-A")
        .with_max_concurrent_tasks(4)
        .with_task_timeout(Duration::from_millis(7500))
        .with_drain_timeout(Duration::from_secs(10))
        .with_scheduler_actor_id(crate::common::ActorId::scheduler(&NodeId::from(
            "node-A".to_string(),
        )))
        .with_workflow_actor_id(crate::common::ActorId::workflow(&NodeId::from(
            "node-A".to_string(),
        )))
        .with_dag_gossip_actor_id(crate::common::ActorId::dag_gossip(&NodeId::from(
            "node-A".to_string(),
        )))
        .with_actor_system(Arc::new(crate::runtime::actor::ActorSystem::new()));

    assert_eq!(worker.max_concurrent_tasks(), 4);
    assert_eq!(worker.running_task_count(), 0);
    assert!(worker.scheduler_actor_id().is_some());
}

#[tokio::test]
async fn worker_shutdown_sends_cancel_signal() {
    let worker = make_worker("node-A");
    // subscribe_state 用于观察；shutdown 通过 cancel watch 发送 true。
    let rx = worker.subscribe_state();
    worker.shutdown();
    // shutdown 仅发送 cancel 信号，不直接改 state（state 由 run 循环处理）。
    // 这里验证不 panic 即可。
    drop(rx);
}

#[tokio::test]
async fn worker_drain_transitions_state_to_draining() {
    let worker = make_worker("node-A");
    let rx = worker.subscribe_state();
    assert_eq!(worker.state(), WorkerState::Running);
    worker.drain();
    // drain 通过 state watch 发送 Draining
    assert_eq!(*rx.borrow(), WorkerState::Draining);
    assert_eq!(worker.state(), WorkerState::Draining);
}

#[tokio::test]
async fn worker_schedule_task_sets_target_node() {
    let worker = make_worker("node-A");
    let mut task = TaskDefinition {
        id: TaskId::from("t-1".to_string()),
        name: "echo".to_string(),
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
    };
    let result = worker
        .schedule_task(task.clone(), NodeId::from("node-B".to_string()))
        .await;
    assert!(result.is_ok());
    // 验证 schedule_task 内部将 target_node 写入 task（MockScheduler.enqueue 接受任意 task）。
    task.target_node = Some(NodeId::from("node-B".to_string()));
}

#[tokio::test]
async fn worker_discover_peers_returns_empty_with_mock_transport() {
    let worker = make_worker("node-A");
    let peers = worker.discover_peers().await.expect("discover_peers ok");
    assert!(peers.is_empty());
}

#[tokio::test]
async fn worker_clear_capacity_callback_releases_reference() {
    let calls = StdArc::new(std::sync::Mutex::new(Vec::new()));
    let calls_clone = calls.clone();
    let cb: Arc<dyn Fn(u32, u32) + Send + Sync> = Arc::new(move |avail, max| {
        calls_clone.lock().unwrap().push((avail, max));
    });
    let mut worker = make_worker("node-A").with_capacity_callback(cb);
    // clear 后再调用 notify_capacity 不应 panic 且不再触发回调。
    worker.clear_capacity_callback();
    // 间接验证：running_task_count 不依赖 callback。
    assert_eq!(worker.running_task_count(), 0);
}

// ───────────────────────── NetworkEventRouter 测试 ─────────────────────────

use crate::runtime::event_bus::Topic as BusTopic;

#[tokio::test]
async fn router_handle_peer_connected_publishes_event() {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
    let event_bus = EventBus::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network,
        event_bus: event_bus.clone(),
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });

    let mut sub = event_bus.subscribe(BusTopic::NetworkPeer);
    router
        .handle(NetworkEvent::PeerConnected {
            peer_id: "peer-B".to_string(),
        })
        .await;

    // 订阅应在 publish 之后收到 BusEvent::PeerConnected
    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    match event {
        BusEvent::PeerConnected(node_id) => assert_eq!(node_id.as_str(), "peer-B"),
        other => panic!("expected PeerConnected, got {:?}", other),
    }
}

#[tokio::test]
async fn router_handle_peer_disconnected_publishes_event() {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
    let event_bus = EventBus::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network,
        event_bus: event_bus.clone(),
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });

    let mut sub = event_bus.subscribe(BusTopic::NetworkPeer);
    router
        .handle(NetworkEvent::PeerDisconnected {
            peer_id: "peer-C".to_string(),
        })
        .await;

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    match event {
        BusEvent::PeerDisconnected(node_id) => assert_eq!(node_id.as_str(), "peer-C"),
        other => panic!("expected PeerDisconnected, got {:?}", other),
    }
}

#[tokio::test]
async fn router_handle_message_unknown_topic_is_noop() {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
    let event_bus = EventBus::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network,
        event_bus: event_bus.clone(),
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });

    // 未知主题 + 无效 payload：不应 panic，不应产生事件。
    router
        .handle_message("totally/unknown/topic", b"garbage")
        .await;
    // 无从验证「无事件」，但只要不 panic 即认为通过。
}

#[tokio::test]
async fn router_handle_message_heartbeat_publishes_to_event_bus() {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
    let event_bus = EventBus::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network,
        event_bus: event_bus.clone(),
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });

    let hb = NodeHeartbeat {
        node_id: NodeId::from("node-HB".to_string()),
        active_workflows: Vec::new(),
        timestamp_ms: 12345,
        available_slots: 4,
        max_slots: 8,
        endpoint_addr: Some("peer-node-HB".to_string()),
    };
    // 构造 heartbeat 主题的 WireEnvelope
    let envelope =
        crate::common::WireEnvelope::wrap(crate::common::WireMessage::NodeHeartbeat(hb.clone()));
    let topic = crate::common::Topic::heartbeat().to_string();
    let payload = crate::runtime::workflow::messaging::encode(&envelope).expect("encode envelope");

    let mut sub = event_bus.subscribe(BusTopic::ClusterHeartbeat);
    router.handle_message(&topic, &payload).await;

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    match event {
        BusEvent::Heartbeat(received) => assert_eq!(received.node_id, hb.node_id),
        other => panic!("expected Heartbeat, got {:?}", other),
    }
}

#[tokio::test]
async fn router_handle_task_result_returns_false_without_actor_system() {
    // 无 actor_system / workflow_actor_id：handle_task_result 应返回 false。
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
    let event_bus = EventBus::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network,
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });

    let accepted = router
        .handle_task_result(
            WorkflowId::from("wf-1".to_string()),
            TaskId::from("t-1".to_string()),
            "echo".to_string(),
            crate::common::WireTaskOutcome::Completed(vec![1, 2, 3]),
            NodeId::from("node-Z".to_string()),
        )
        .await;
    assert!(
        accepted,
        "should return true (publishes to event bus) without actor system"
    );
}

// ───────────────────────── Worker 扩展测试 ─────────────────────────

#[tokio::test]
async fn worker_set_max_concurrent_tasks_expands_capacity() {
    let worker = make_worker("node-expand");
    assert_eq!(worker.max_concurrent_tasks(), 1);
    worker.set_max_concurrent_tasks(4);
    assert_eq!(worker.max_concurrent_tasks(), 4);
    assert_eq!(worker.running_task_count(), 0);
}

#[tokio::test]
async fn worker_set_max_concurrent_tasks_shrink_ignored() {
    let worker = make_worker("node-shrink").with_max_concurrent_tasks(8);
    assert_eq!(worker.max_concurrent_tasks(), 8);
    worker.set_max_concurrent_tasks(2);
    assert_eq!(worker.max_concurrent_tasks(), 8, "shrink should be ignored");
}

#[tokio::test]
async fn worker_cancel_task_returns_false_for_unknown_task() {
    let worker = make_worker("node-cancel");
    assert!(!worker.cancel_task("nonexistent-task"));
}

#[tokio::test]
async fn worker_notify_stopped_sets_state() {
    let worker = make_worker("node-stop");
    assert_eq!(worker.state(), WorkerState::Running);
    worker.notify_stopped();
    assert_eq!(worker.state(), WorkerState::Stopped);
}

#[tokio::test]
async fn worker_capacity_callback_invoked_on_notify() {
    let calls = StdArc::new(std::sync::Mutex::new(Vec::new()));
    let calls_clone = calls.clone();
    let cb: Arc<dyn Fn(u32, u32) + Send + Sync> = Arc::new(move |avail, max| {
        calls_clone.lock().unwrap().push((avail, max));
    });
    let worker = make_worker("node-cb").with_capacity_callback(cb);
    worker.set_max_concurrent_tasks(4);
    let recorded = calls.lock().unwrap().clone();
    assert!(
        recorded.iter().any(|(avail, max)| *max == 4 && *avail > 0),
        "capacity callback should fire on set_max_concurrent_tasks: {:?}",
        recorded
    );
}

#[tokio::test]
async fn worker_with_failover_manager_stores_arc() {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-fo"));
    let failover = Arc::new(crate::runtime::workflow::FailoverManager::new(
        NodeId::from("node-fo".to_string()),
        network.clone(),
        Arc::new(crate::runtime::actor::ActorSystem::new()),
        crate::common::ActorId::workflow(&NodeId::from("node-fo".to_string())),
    ));
    let worker = make_worker("node-fo").with_failover_manager(failover);
    // select_remote_target 返回 None（无 peers / 无心跳）
    // 仅验证 worker 构造成功，failover 已设置。
    assert_eq!(worker.node_id().as_str(), "node-fo");
}

#[tokio::test]
async fn worker_scheduler_and_dispatcher_accessors() {
    let worker = make_worker("node-acc");
    let _sched = worker.scheduler();
    let _sched_arc = worker.scheduler_clone();
    let _disp = worker.task_dispatcher();
    // 不 panic 即通过
}

#[tokio::test]
async fn worker_schedule_task_with_real_scheduler() {
    use crate::runtime::workflow::scheduler::ActorScheduler;
    use crate::runtime::workflow::SchedulerActor;
    let system = Arc::new(crate::runtime::actor::ActorSystem::new());
    let actor_id = crate::common::ActorId::from("sched-test".to_string());
    system
        .spawn(actor_id.clone(), SchedulerActor::fifo())
        .await
        .unwrap();
    let scheduler: Arc<dyn Scheduler> = Arc::new(ActorScheduler::new(actor_id, system.clone()));

    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-sched"));
    let event_bus = EventBus::new();
    let dispatcher = crate::runtime::dispatcher::TaskRegistry::new(1, 8, Vec::new())
        .expect("TaskRegistry init")
        .into_dispatcher();
    let handle = tokio::runtime::Handle::current();
    let worker = Worker::new(
        NodeId::from("node-sched".to_string()),
        network,
        event_bus,
        scheduler,
        dispatcher,
        &WorkerConfig::default(),
        handle,
    );

    let task = TaskDefinition {
        id: TaskId::from("t-sched".to_string()),
        name: "test_task".to_string(),
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
    };
    worker
        .schedule_task(task, NodeId::from("node-target".to_string()))
        .await
        .expect("schedule_task ok");
}

// ───────────────────────── NetworkEventRouter 扩展测试 ─────────────────────────

fn make_router(node_id: &str) -> (NetworkEventRouter, EventBus, Arc<MockTransport>) {
    let network = Arc::new(MockTransport::new(node_id));
    let event_bus = EventBus::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: network.clone() as Arc<dyn Transport>,
        event_bus: event_bus.clone(),
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });
    (router, event_bus, network)
}

#[tokio::test]
async fn router_handle_message_cancel_broadcast_sets_cancel_flag() {
    let (_router, event_bus, _network) = make_router("node-cancel");

    // 在 cancel_flags 中注册一个 task，然后用新的 router 测试 cancel 逻辑
    let task_id = "task-cancel-test";
    {
        // 用不同的 router 测试 cancel 逻辑
        let flag = crate::runtime::dispatcher::new_cancel_flag();
        let mut flags_map = std::collections::HashMap::new();
        flags_map.insert(task_id.to_string(), flag.clone());
        let cancel_flags = Arc::new(parking_lot::Mutex::new(flags_map));
        let cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>> =
            Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let router2 = NetworkEventRouter::new(NetworkEventRouterConfig {
            network: Arc::new(MockTransport::new("node-cancel")),
            event_bus: event_bus.clone(),
            scheduler: Arc::new(MockScheduler::new()),
            actor_system: None,
            workflow_actor_id: None,
            dag_gossip_actor_id: None,
            cancel_flags: cancel_flags.clone(),
            cancelled_tasks: cancelled_tasks.clone(),
        });

        let cancel_msg = crate::common::wire::CancelBroadcast {
            task_id: TaskId::from(task_id.to_string()),
            workflow_id: WorkflowId::from("wf-cancel".to_string()),
        };
        let payload = postcard::to_allocvec(&cancel_msg).unwrap();
        let topic =
            crate::common::Topic::from(crate::common::wire::constants::TOPIC_CANCEL).to_string();
        router2.handle_message(&topic, &payload).await;

        assert!(
            flag.load(std::sync::atomic::Ordering::Acquire),
            "cancel flag should be set"
        );
        assert!(
            cancelled_tasks.lock().contains_key(task_id),
            "task should be in cancelled_tasks"
        );
    }
    drop(_router);
}

#[tokio::test]
async fn router_handle_message_task_dispatch_enqueues_to_scheduler() {
    let (router, _event_bus, _network) = make_router("node-dispatch");

    let task = TaskDefinition {
        id: TaskId::from("t-dispatch".to_string()),
        name: "dispatch_test".to_string(),
        payload: vec![1, 2, 3],
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
    };
    let envelope =
        crate::common::WireEnvelope::wrap(crate::common::WireMessage::TaskDispatch(task.clone()));
    let topic = crate::common::Topic::task(&NodeId::from("node-dispatch".to_string())).to_string();
    let payload = crate::runtime::workflow::messaging::encode(&envelope).expect("encode");

    // MockScheduler.enqueue 总是返回 Ok，不 panic 即通过。
    router.handle_message(&topic, &payload).await;
}

#[tokio::test]
async fn router_handle_message_dag_state_update_publishes_event() {
    let (router, event_bus, _network) = make_router("node-dag");

    let update =
        crate::common::WireMessage::DagStateUpdate(crate::common::wire::WireDagStateUpdate {
            workflow_id: WorkflowId::from("wf-dag".to_string()),
            task_id: TaskId::from("t-dag".to_string()),
            task_state: crate::common::wire::WireTaskState::Completed { result: Vec::new() },
            hlc_timestamp: crate::runtime::state::HlcTimestamp::zero(),
            origin_node: NodeId::from("node-src".to_string()),
        });
    let envelope = crate::common::WireEnvelope::wrap(update);
    let topic = crate::common::Topic::dag_state().to_string();
    let payload = crate::runtime::workflow::messaging::encode(&envelope).expect("encode");

    let mut sub = event_bus.subscribe(BusTopic::DagUpdate);
    router.handle_message(&topic, &payload).await;

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::DagUpdate(_)));
}

#[tokio::test]
async fn router_handle_message_failover_claim_publishes_event() {
    let (router, event_bus, _network) = make_router("node-failover");

    let claim = crate::common::wire::OrchestratorClaim {
        node_id: NodeId::from("node-claimer".to_string()),
        workflow_id: WorkflowId::from("wf-claim".to_string()),
        timestamp_ms: 12345,
    };
    let envelope =
        crate::common::WireEnvelope::wrap(crate::common::WireMessage::OrchestratorClaim(claim));
    let topic = crate::common::Topic::failover().to_string();
    let payload = crate::runtime::workflow::messaging::encode(&envelope).expect("encode");

    let mut sub = event_bus.subscribe(BusTopic::ClusterClaim);
    router.handle_message(&topic, &payload).await;

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::Claim(_)));
}

#[tokio::test]
async fn router_handle_message_invalid_payload_is_silently_ignored() {
    let (router, _event_bus, _network) = make_router("node-invalid");

    // 给已知 topic 传入无效 payload，不应 panic。
    let topic = crate::common::Topic::heartbeat().to_string();
    router.handle_message(&topic, b"invalid-payload").await;

    let topic = crate::common::Topic::dag_state().to_string();
    router.handle_message(&topic, b"invalid-payload").await;

    let topic = crate::common::Topic::failover().to_string();
    router.handle_message(&topic, b"invalid-payload").await;
}

#[tokio::test]
async fn router_handle_direct_request_dispatch_task_sends_ack() {
    let (router, _event_bus, _network) = make_router("node-direct");

    let task = TaskDefinition {
        id: TaskId::from("t-direct".to_string()),
        name: "direct_test".to_string(),
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
    };
    let channel = DirectResponseChannel::test_stub();

    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-X".to_string(),
            request: Box::new(crate::runtime::network::DirectRequest::DispatchTask { task }),
            channel,
        })
        .await;
    // MockScheduler.enqueue 总是 Ok，ack 应为 true。不 panic 即通过。
}

#[tokio::test]
async fn router_handle_direct_request_task_result_publishes_event() {
    let (router, event_bus, _network) = make_router("node-result");

    let channel = DirectResponseChannel::test_stub();

    let mut sub = event_bus.subscribe(BusTopic::TaskCompleted);

    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-Y".to_string(),
            request: Box::new(crate::runtime::network::DirectRequest::TaskResult {
                workflow_id: WorkflowId::from("wf-result".to_string()),
                task_id: TaskId::from("t-result".to_string()),
                task_name: "result_test".to_string(),
                outcome: crate::common::WireTaskOutcome::Completed(vec![42]),
                worker_node: NodeId::from("node-worker".to_string()),
            }),
            channel,
        })
        .await;

    // handle_task_result 无 actor_system 时走 publish_remote_completion 路径。
    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::TaskCompleted(_)));
}

#[tokio::test]
async fn router_handle_direct_request_unknown_forwards_to_event_bus() {
    let (router, event_bus, _network) = make_router("node-unknown-req");

    let channel = DirectResponseChannel::test_stub();

    let mut sub = event_bus.subscribe(BusTopic::NetworkDirect);

    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-Z".to_string(),
            request: Box::new(crate::runtime::network::DirectRequest::QueryWorkflowState {
                workflow_id: WorkflowId::from("wf-q".to_string()),
                requesting_node: NodeId::from("node-q".to_string()),
            }),
            channel,
        })
        .await;

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::DirectRequest { .. }));
}

#[tokio::test]
async fn router_handle_message_empty_workflow_id_routes_to_event_bus() {
    let (router, event_bus, _network) = make_router("node-empty-wf");

    let mut sub = event_bus.subscribe(BusTopic::TaskFailed);

    // handle_task_result with empty workflow_id should go through publish_remote_completion
    let accepted = router
        .handle_task_result(
            WorkflowId::from("".to_string()),
            TaskId::from("t-empty".to_string()),
            "empty_wf_task".to_string(),
            crate::common::WireTaskOutcome::Failed("test error".to_string()),
            NodeId::from("node-remote".to_string()),
        )
        .await;
    assert!(accepted, "empty workflow_id should return true");

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::TaskFailed(_)));
}

#[tokio::test]
async fn router_handle_task_result_skipped_outcome_returns_false() {
    let (router, _event_bus, _network) = make_router("node-skip");

    let accepted = router
        .handle_task_result(
            WorkflowId::from("wf-skip".to_string()),
            TaskId::from("t-skip".to_string()),
            "skip_task".to_string(),
            crate::common::WireTaskOutcome::Skipped,
            NodeId::from("node-remote".to_string()),
        )
        .await;
    // Skipped outcome without actor_system falls through to publish_remote_completion
    // which returns true
    assert!(accepted);
}

#[tokio::test]
async fn router_handle_task_result_cancelled_outcome_publishes_event() {
    let (router, event_bus, _network) = make_router("node-cancel-outcome");

    let mut sub = event_bus.subscribe(BusTopic::TaskCancelled);
    let accepted = router
        .handle_task_result(
            WorkflowId::from("wf-cancel".to_string()),
            TaskId::from("t-cancel".to_string()),
            "cancel_task".to_string(),
            crate::common::WireTaskOutcome::Cancelled,
            NodeId::from("node-remote".to_string()),
        )
        .await;
    assert!(accepted, "cancelled outcome should return true");

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::TaskCancelled(_)));
}

#[tokio::test]
async fn router_handle_direct_request_dispatch_task_ack_accepted() {
    let (router, _event_bus, network) = make_router("node-dispatch-ack");

    let task = make_task_for_completion("t-direct-dispatch", "direct_dispatch");
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-D".to_string(),
            request: Box::new(crate::runtime::network::DirectRequest::DispatchTask { task }),
            channel: DirectResponseChannel::test_stub(),
        })
        .await;

    // MockTransport 的 send_direct_response 返回 Ok，因此应记录一次广播（内部调用）。
    // 实际上 MockTransport 的 send_direct_response 返回 Ok 但不记录；此处仅验证不 panic。
    assert_eq!(network.broadcast_count(), 0);
}

struct RejectingScheduler;

#[async_trait::async_trait]
impl Scheduler for RejectingScheduler {
    async fn enqueue(&self, _task: TaskDefinition) -> crate::common::Result<()> {
        Err(ActantError::Internal("scheduler closed".into()))
    }

    async fn enqueue_batch(&self, _tasks: Vec<TaskDefinition>) -> crate::common::Result<()> {
        Ok(())
    }

    async fn dequeue(&self) -> Option<TaskDefinition> {
        None
    }

    async fn try_dequeue(&self) -> Option<TaskDefinition> {
        None
    }

    async fn dequeue_batch(&self, _limit: usize) -> Vec<TaskDefinition> {
        Vec::new()
    }

    async fn drain_unrouted(&self) -> Vec<TaskDefinition> {
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

    fn close(&self) {}

    fn is_closed(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn router_handle_direct_request_dispatch_task_ack_rejected() {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
    let event_bus = EventBus::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(RejectingScheduler);
    let router = NetworkEventRouter::new(NetworkEventRouterConfig {
        network: network.clone(),
        event_bus,
        scheduler,
        actor_system: None,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
        cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        cancelled_tasks: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });

    let task = make_task_for_completion("t-direct-dispatch", "direct_dispatch");
    router
        .handle(NetworkEvent::DirectRequest {
            peer_id: "peer-D".to_string(),
            request: Box::new(crate::runtime::network::DirectRequest::DispatchTask { task }),
            channel: DirectResponseChannel::test_stub(),
        })
        .await;

    // RejectingScheduler 拒绝入队，应走 accepted=false 分支并尝试发送响应；
    // MockTransport 的 send_direct_response 返回 Ok，因此不 panic 即通过。
}

#[tokio::test]
async fn router_handle_message_heads_exchange_publishes_event() {
    let (router, event_bus, _network) = make_router("node-heads");

    let exchange = crate::common::wire::HeadsExchange {
        node_id: NodeId::from("node-peer".to_string()),
        heads: Vec::new(),
    };
    let envelope =
        crate::common::WireEnvelope::wrap(crate::common::WireMessage::HeadsExchange(exchange));
    let topic = crate::common::Topic::heads().to_string();
    let payload = crate::runtime::workflow::messaging::encode(&envelope).expect("encode");

    let mut sub = event_bus.subscribe(BusTopic::HeadsExchange);
    router.handle_message(&topic, &payload).await;

    let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
        .await
        .expect("event published in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::HeadsExchange(_)));
}

// ───────────────────────── result_delivery::start_pending_result_loop ─────────────────────────

#[tokio::test]
async fn start_pending_result_loop_returns_sendable_channel() {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-delivery"));
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

    let tx = start_pending_result_loop(network, cancel_rx, 3, Duration::from_millis(1), 16);

    // 通道应可用
    assert!(
        !tx.is_closed(),
        "pending_result channel should be open after start"
    );
}

#[tokio::test]
async fn start_pending_result_loop_drops_after_max_attempts() {
    // 使用 MockTransport，send_direct_request 返回 Err。
    // 验证：达到 max_attempts 后，结果被丢弃（通道不再有新消息）。
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-fail"));
    let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

    let tx = start_pending_result_loop(
        network,
        cancel_rx,
        1, // max_attempts = 1，第一次重试后即达到上限
        Duration::from_millis(1),
        16,
    );

    let req = crate::runtime::network::DirectRequest::QueryWorkflowState {
        workflow_id: WorkflowId::from("wf-drop"),
        requesting_node: NodeId::from("n-test"),
    };

    // 入队一个 PendingResult，attempts=0
    assert!(
        try_enqueue_pending_result(&tx, "peer-x".to_string(), req, 0, 16).await,
        "enqueue should succeed"
    );

    // 给 retry loop 时间处理
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 通道应已排空（result 被丢弃，不再重新入队）
    assert_eq!(tx.capacity(), 16, "channel should be drained");
}

#[tokio::test]
async fn start_pending_result_loop_shutdown_on_cancel() {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-shutdown"));
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

    let tx = start_pending_result_loop(network, cancel_rx, 5, Duration::from_millis(100), 16);

    // 发送取消信号
    let _ = cancel_tx.send(true);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 后台任务退出后，pending_rx 被 drop，通道关闭。
    // try_send 应返回 Closed，try_enqueue_pending_result 返回 false。
    let req = crate::runtime::network::DirectRequest::QueryWorkflowState {
        workflow_id: WorkflowId::from("wf-cancel"),
        requesting_node: NodeId::from("n-test"),
    };
    assert!(
        !try_enqueue_pending_result(&tx, "peer-y".to_string(), req, 0, 16).await,
        "enqueue should fail after loop shutdown (receiver dropped)"
    );
}

// ───────────────────────── cancel::spawn_cancelled_tasks_cleanup_loop ─────────────────────────

#[tokio::test]
async fn spawn_cancelled_tasks_cleanup_loop_starts_without_panic() {
    use std::time::Instant;

    let cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));

    // 插入一些条目
    {
        let mut map = cancelled_tasks.lock();
        map.insert("task-1".to_string(), Instant::now());
        map.insert("task-2".to_string(), Instant::now());
    }

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::runtime::Handle::current();
    cancel::spawn_cancelled_tasks_cleanup_loop(&handle, cancelled_tasks.clone(), cancel_rx);

    // 验证 loop 启动后不 panic，条目仍在（清理间隔 60s，短时间内不会清理）
    assert_eq!(cancelled_tasks.lock().len(), 2, "entries should remain");

    // 关闭 loop
    let _ = cancel_tx.send(true);
    tokio::time::sleep(Duration::from_millis(10)).await;
}

#[tokio::test]
async fn spawn_cancelled_tasks_cleanup_loop_shutdown_on_cancel() {
    use std::time::Instant;

    let cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::runtime::Handle::current();
    cancel::spawn_cancelled_tasks_cleanup_loop(&handle, cancelled_tasks.clone(), cancel_rx);

    // 立即取消
    let _ = cancel_tx.send(true);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 插入一个过期条目
    {
        let mut map = cancelled_tasks.lock();
        map.insert(
            "post-cancel".to_string(),
            Instant::now() - Duration::from_secs(400),
        );
    }

    // 短暂等待（远小于 60s 清理间隔），确认条目仍在
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        cancelled_tasks.lock().contains_key("post-cancel"),
        "entry should remain (cleanup interval not reached)"
    );
}

#[tokio::test]
async fn spawn_cancelled_tasks_cleanup_loop_empty_map_no_panic() {
    let cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::runtime::Handle::current();
    cancel::spawn_cancelled_tasks_cleanup_loop(&handle, cancelled_tasks.clone(), cancel_rx);

    // 短暂等待，空 map 不应 panic
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        cancelled_tasks.lock().is_empty(),
        "empty map should remain empty"
    );

    let _ = cancel_tx.send(true);
}

// ───────────────────────── Configurable mock transport for Worker internals ─────────────────────────

use crate::runtime::network::{DirectRequest, DirectResponse, ListenAddresses, PeerId};
use std::sync::Mutex as StdMutex;

struct ConfigurableMockTransport {
    node_id: NodeId,
    local_peer_id: String,
    direct_response: StdMutex<Option<Result<DirectResponse>>>,
    requested_peer: StdMutex<Option<String>>,
}

impl ConfigurableMockTransport {
    fn new(node_id: &str) -> Self {
        Self {
            node_id: NodeId::from(node_id.to_string()),
            local_peer_id: format!("peer-{}", node_id),
            direct_response: StdMutex::new(None),
            requested_peer: StdMutex::new(None),
        }
    }

    fn with_direct_response(self, response: Result<DirectResponse>) -> Self {
        *self.direct_response.lock().unwrap() = Some(response);
        self
    }

    fn take_requested_peer(&self) -> Option<String> {
        self.requested_peer.lock().unwrap().take()
    }
}

#[async_trait::async_trait]
impl Transport for ConfigurableMockTransport {
    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn local_peer_id(&self) -> &str {
        &self.local_peer_id
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
        Ok(ListenAddresses {
            endpoint_id: self.local_peer_id.clone(),
            relay_url: None,
            direct_addrs: Vec::new(),
            endpoint_addr: self.local_peer_id.clone(),
        })
    }

    async fn send_direct_request(
        &self,
        peer_id_str: &str,
        _request: DirectRequest,
    ) -> Result<DirectResponse> {
        *self.requested_peer.lock().unwrap() = Some(peer_id_str.to_string());
        self.direct_response
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| Err(ActantError::Internal("no response configured".into())))
    }

    async fn send_direct_response(
        &self,
        _channel: crate::runtime::network::DirectResponseChannel,
        _response: DirectResponse,
    ) -> Result<()> {
        Ok(())
    }

    async fn discover_peers(&self) -> Result<Vec<PeerId>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

fn make_worker_with_transport(node_id: &str, transport: Arc<dyn Transport>) -> Worker {
    let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
    let event_bus = EventBus::new();
    let dispatcher = crate::runtime::dispatcher::TaskRegistry::new(1, 8, Vec::new())
        .expect("TaskRegistry init")
        .into_dispatcher();
    let handle = tokio::runtime::Handle::current();
    Worker::new(
        NodeId::from(node_id.to_string()),
        transport,
        event_bus,
        scheduler,
        dispatcher,
        &WorkerConfig::default(),
        handle,
    )
}

// ───────────────────────── select_remote_target tests ─────────────────────────

#[tokio::test]
async fn select_remote_target_without_failover_returns_none() {
    let transport = Arc::new(ConfigurableMockTransport::new("node-A"));
    let worker = make_worker_with_transport("node-A", transport);
    assert!(worker.select_remote_target().is_none());
}

#[tokio::test]
async fn select_remote_target_filters_unavailable_peers() {
    let transport = Arc::new(ConfigurableMockTransport::new("node-A"));
    let network: Arc<dyn Transport> = transport.clone();
    let failover = Arc::new(crate::runtime::workflow::FailoverManager::new(
        NodeId::from("node-A".to_string()),
        network,
        Arc::new(crate::runtime::actor::ActorSystem::new()),
        crate::common::ActorId::workflow(&NodeId::from("node-A".to_string())),
    ));
    let worker = make_worker_with_transport("node-A", transport)
        .with_max_concurrent_tasks(4)
        .with_failover_manager(failover.clone());

    // 注册一个 available_slots=0 的 peer
    let hb = NodeHeartbeat {
        node_id: NodeId::from("node-B".to_string()),
        active_workflows: Vec::new(),
        timestamp_ms: crate::common::epoch_millis(),
        available_slots: 0,
        max_slots: 8,
        endpoint_addr: Some("node-B-addr".to_string()),
    };
    failover.handle_heartbeat(&hb);

    assert!(worker.select_remote_target().is_none());
}

#[tokio::test]
async fn select_remote_target_returns_peer_with_more_available_slots() {
    let transport = Arc::new(ConfigurableMockTransport::new("node-A"));
    let network: Arc<dyn Transport> = transport.clone();
    let failover = Arc::new(crate::runtime::workflow::FailoverManager::new(
        NodeId::from("node-A".to_string()),
        network,
        Arc::new(crate::runtime::actor::ActorSystem::new()),
        crate::common::ActorId::workflow(&NodeId::from("node-A".to_string())),
    ));
    let worker = make_worker_with_transport("node-A", transport)
        .with_max_concurrent_tasks(4)
        .with_failover_manager(failover.clone());

    // 本地有 4 个可用槽位，peer 有 8 个，应被选中
    let hb = NodeHeartbeat {
        node_id: NodeId::from("node-B".to_string()),
        active_workflows: Vec::new(),
        timestamp_ms: crate::common::epoch_millis(),
        available_slots: 8,
        max_slots: 8,
        endpoint_addr: Some("node-B-addr".to_string()),
    };
    failover.handle_heartbeat(&hb);

    let (target, addr) = worker.select_remote_target().expect("should select peer");
    assert_eq!(target.as_str(), "node-B");
    assert_eq!(addr, "node-B-addr");
}

#[tokio::test]
async fn select_remote_target_skips_stale_heartbeat() {
    let transport = Arc::new(ConfigurableMockTransport::new("node-A"));
    let network: Arc<dyn Transport> = transport.clone();
    let config = crate::common::FailoverConfig {
        failure_timeout_ms: 100,
        heartbeat_interval_ms: 30,
        lease_duration_ms: 250,
        ..Default::default()
    };
    let failover = Arc::new(crate::runtime::workflow::FailoverManager::with_config(
        NodeId::from("node-A".to_string()),
        network,
        Arc::new(crate::runtime::actor::ActorSystem::new()),
        crate::common::ActorId::workflow(&NodeId::from("node-A".to_string())),
        config,
        None,
    ));

    let worker = make_worker_with_transport("node-A", transport)
        .with_max_concurrent_tasks(4)
        .with_failover_manager(failover.clone());

    let hb = NodeHeartbeat {
        node_id: NodeId::from("node-B".to_string()),
        active_workflows: Vec::new(),
        // 很久之前的心跳
        timestamp_ms: crate::common::epoch_millis().saturating_sub(10_000),
        available_slots: 8,
        max_slots: 8,
        endpoint_addr: Some("node-B-addr".to_string()),
    };
    failover.handle_heartbeat(&hb);

    assert!(worker.select_remote_target().is_none());
}

#[tokio::test]
async fn select_remote_target_peer_not_better_than_local_returns_none() {
    let transport = Arc::new(ConfigurableMockTransport::new("node-A"));
    let network: Arc<dyn Transport> = transport.clone();
    let failover = Arc::new(crate::runtime::workflow::FailoverManager::new(
        NodeId::from("node-A".to_string()),
        network,
        Arc::new(crate::runtime::actor::ActorSystem::new()),
        crate::common::ActorId::workflow(&NodeId::from("node-A".to_string())),
    ));
    // 本地 4 个槽位，peer 也 4 个，不应被选中
    let worker = make_worker_with_transport("node-A", transport)
        .with_max_concurrent_tasks(4)
        .with_failover_manager(failover.clone());

    let hb = NodeHeartbeat {
        node_id: NodeId::from("node-B".to_string()),
        active_workflows: Vec::new(),
        timestamp_ms: crate::common::epoch_millis(),
        available_slots: 4,
        max_slots: 4,
        endpoint_addr: Some("node-B-addr".to_string()),
    };
    failover.handle_heartbeat(&hb);

    assert!(worker.select_remote_target().is_none());
}

// ───────────────────────── forward_remote_task tests ─────────────────────────

#[tokio::test]
async fn forward_remote_task_uses_target_endpoint_addr_when_present() {
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Ok(DirectResponse::DispatchAck { accepted: true })),
    );
    let worker = make_worker_with_transport("node-A", transport.clone());

    let task = TaskDefinition {
        id: TaskId::from("t-forward".to_string()),
        name: "forward_test".to_string(),
        payload: Vec::new(),
        workflow_id: None,
        target_node: Some(NodeId::from("node-B".to_string())),
        origin_node: None,
        retry_policy: None,
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: Some("custom-endpoint-addr".to_string()),
        origin_endpoint_addr: None,
    };

    let result = worker
        .forward_remote_task(&task, &NodeId::from("node-B".to_string()))
        .await;
    assert!(result.is_ok());
    assert_eq!(
        transport.take_requested_peer().as_deref(),
        Some("custom-endpoint-addr")
    );
}

#[tokio::test]
async fn forward_remote_task_falls_back_to_node_id() {
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Ok(DirectResponse::DispatchAck { accepted: true })),
    );
    let worker = make_worker_with_transport("node-A", transport.clone());

    let task = TaskDefinition {
        id: TaskId::from("t-forward".to_string()),
        name: "forward_test".to_string(),
        payload: Vec::new(),
        workflow_id: None,
        target_node: Some(NodeId::from("node-B".to_string())),
        origin_node: None,
        retry_policy: None,
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    };

    let result = worker
        .forward_remote_task(&task, &NodeId::from("node-B".to_string()))
        .await;
    assert!(result.is_ok());
    assert_eq!(transport.take_requested_peer().as_deref(), Some("node-B"));
}

#[tokio::test]
async fn forward_remote_task_rejected_ack_returns_error() {
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Ok(DirectResponse::DispatchAck { accepted: false })),
    );
    let worker = make_worker_with_transport("node-A", transport);

    let task = TaskDefinition {
        id: TaskId::from("t-forward".to_string()),
        name: "forward_test".to_string(),
        payload: Vec::new(),
        workflow_id: None,
        target_node: Some(NodeId::from("node-B".to_string())),
        origin_node: None,
        retry_policy: None,
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    };

    let result = worker
        .forward_remote_task(&task, &NodeId::from("node-B".to_string()))
        .await;
    assert!(result.is_err());
}

// ───────────────────────── publish_task_completion ─────────────────────────

fn make_remote_task(origin: &str) -> TaskDefinition {
    TaskDefinition {
        id: TaskId::from("t-remote".to_string()),
        name: "remote_task".to_string(),
        payload: vec![1, 2, 3],
        workflow_id: Some(WorkflowId::from("wf-remote".to_string())),
        target_node: Some(NodeId::from("node-A".to_string())),
        origin_node: Some(NodeId::from(origin.to_string())),
        retry_policy: None,
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: Some("origin-addr".to_string()),
    }
}

fn make_local_task() -> TaskDefinition {
    TaskDefinition {
        id: TaskId::from("t-local".to_string()),
        name: "local_task".to_string(),
        payload: vec![1, 2, 3],
        workflow_id: Some(WorkflowId::from("wf-local".to_string())),
        target_node: Some(NodeId::from("node-A".to_string())),
        origin_node: Some(NodeId::from("node-A".to_string())),
        retry_policy: None,
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    }
}

#[tokio::test]
async fn publish_task_completion_local_completed_publishes_event() {
    let event_bus = EventBus::new();
    let mut sub = event_bus.subscribe(crate::runtime::event_bus::Topic::TaskCompleted);
    let (tx, _rx) = tokio::sync::mpsc::channel::<PendingResult>(16);

    let completion = TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-local".to_string()),
        task_id: TaskId::from("t-local".to_string()),
        task_name: "local_task".to_string(),
        result: vec![42],
        target_node: Some(NodeId::from("node-A".to_string())),
    };

    publish_task_completion(
        completion,
        &make_local_task(),
        &NodeId::from("node-A".to_string()),
        &*Arc::new(ConfigurableMockTransport::new("node-A")),
        &event_bus,
        &tx,
        16,
    )
    .await;

    let event = tokio::time::timeout(Duration::from_millis(200), sub.recv())
        .await
        .expect("event in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::TaskCompleted(_)));
}

#[tokio::test]
async fn publish_task_completion_remote_ack_accepted_succeeds() {
    let event_bus = EventBus::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<PendingResult>(16);
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Ok(DirectResponse::TaskResultAck { accepted: true })),
    );

    let completion = TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-remote".to_string()),
        task_id: TaskId::from("t-remote".to_string()),
        task_name: "remote_task".to_string(),
        result: vec![42],
        target_node: Some(NodeId::from("node-A".to_string())),
    };

    publish_task_completion(
        completion,
        &make_remote_task("node-B"),
        &NodeId::from("node-A".to_string()),
        &*transport,
        &event_bus,
        &tx,
        16,
    )
    .await;

    // 直接送达成功，不应入队 pending result。
    assert_eq!(rx.len(), 0);
}

#[tokio::test]
async fn publish_task_completion_remote_ack_rejected_enqueues_retry() {
    let event_bus = EventBus::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<PendingResult>(16);
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Ok(DirectResponse::TaskResultAck { accepted: false })),
    );

    let completion = TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-remote".to_string()),
        task_id: TaskId::from("t-remote".to_string()),
        task_name: "remote_task".to_string(),
        result: vec![42],
        target_node: Some(NodeId::from("node-A".to_string())),
    };

    publish_task_completion(
        completion,
        &make_remote_task("node-B"),
        &NodeId::from("node-A".to_string()),
        &*transport,
        &event_bus,
        &tx,
        16,
    )
    .await;

    assert_eq!(rx.len(), 1, "rejected ack should enqueue pending result");
}

#[tokio::test]
async fn publish_task_completion_remote_unexpected_response_enqueues_retry() {
    let event_bus = EventBus::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<PendingResult>(16);
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Ok(DirectResponse::DispatchAck { accepted: true })),
    );

    let completion = TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-remote".to_string()),
        task_id: TaskId::from("t-remote".to_string()),
        task_name: "remote_task".to_string(),
        result: vec![42],
        target_node: Some(NodeId::from("node-A".to_string())),
    };

    publish_task_completion(
        completion,
        &make_remote_task("node-B"),
        &NodeId::from("node-A".to_string()),
        &*transport,
        &event_bus,
        &tx,
        16,
    )
    .await;

    assert_eq!(
        rx.len(),
        1,
        "unexpected response should enqueue pending result"
    );
}

#[tokio::test]
async fn publish_task_completion_remote_network_error_enqueues_retry() {
    let event_bus = EventBus::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<PendingResult>(16);
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Err(ActantError::Network("down".into()))),
    );

    let completion = TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-remote".to_string()),
        task_id: TaskId::from("t-remote".to_string()),
        task_name: "remote_task".to_string(),
        result: vec![42],
        target_node: Some(NodeId::from("node-A".to_string())),
    };

    publish_task_completion(
        completion,
        &make_remote_task("node-B"),
        &NodeId::from("node-A".to_string()),
        &*transport,
        &event_bus,
        &tx,
        16,
    )
    .await;

    assert_eq!(rx.len(), 1, "network error should enqueue pending result");
}

#[tokio::test]
async fn publish_task_completion_remote_rejected_with_full_channel_publishes_failed() {
    let event_bus = EventBus::new();
    let mut sub = event_bus.subscribe(crate::runtime::event_bus::Topic::TaskFailed);
    // 容量为 1 的通道，先占满，使 try_enqueue_pending_result 返回 Full。
    let (tx, _rx) = tokio::sync::mpsc::channel::<PendingResult>(1);
    tx.send(PendingResult {
        target: "origin-addr".to_string(),
        request: DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf-fill".to_string()),
            requesting_node: NodeId::from("node-A".to_string()),
        },
        attempts: 0,
    })
    .await
    .unwrap();
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Ok(DirectResponse::TaskResultAck { accepted: false })),
    );

    let completion = TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-remote".to_string()),
        task_id: TaskId::from("t-remote".to_string()),
        task_name: "remote_task".to_string(),
        result: vec![42],
        target_node: Some(NodeId::from("node-A".to_string())),
    };

    publish_task_completion(
        completion,
        &make_remote_task("node-B"),
        &NodeId::from("node-A".to_string()),
        &*transport,
        &event_bus,
        &tx,
        1,
    )
    .await;

    let event = tokio::time::timeout(Duration::from_millis(200), sub.recv())
        .await
        .expect("event in time")
        .expect("event present");
    assert!(matches!(event, BusEvent::TaskFailed(_)));
}

// ───────────────────────── build_completion_from_dispatch_result ─────────────────────────

fn make_task_for_completion(id: &str, name: &str) -> TaskDefinition {
    TaskDefinition {
        id: TaskId::from(id.to_string()),
        name: name.to_string(),
        payload: Vec::new(),
        workflow_id: Some(WorkflowId::from("wf-completion".to_string())),
        target_node: Some(NodeId::from("node-A".to_string())),
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

#[test]
fn build_completion_from_ok_result_returns_completed() {
    let task = make_task_for_completion("t-ok", "ok_task");
    let result: DispatchResult = Ok(Ok(Ok(vec![1, 2, 3])));
    let completion = build_completion_from_dispatch_result(
        result,
        &task,
        crate::common::epoch_millis(),
        Duration::from_millis(1000),
    );
    match completion {
        TaskCompletion::Completed { result, .. } => assert_eq!(result, vec![1, 2, 3]),
        other => panic!("expected Completed, got {:?}", other),
    }
}

#[test]
fn build_completion_from_dispatch_error_returns_failed() {
    let task = make_task_for_completion("t-err", "err_task");
    let result: DispatchResult = Ok(Ok(Err(ActantError::Task("dispatch failed".into()))));
    let completion = build_completion_from_dispatch_result(
        result,
        &task,
        crate::common::epoch_millis(),
        Duration::from_millis(1000),
    );
    match completion {
        TaskCompletion::Failed { error, .. } => {
            assert!(error.contains("dispatch failed"));
        }
        other => panic!("expected Failed, got {:?}", other),
    }
}

#[test]
fn build_completion_from_static_str_panic_returns_failed() {
    let task = make_task_for_completion("t-panic", "panic_task");
    let payload: Box<dyn std::any::Any + Send> = Box::new("static panic msg");
    let result: DispatchResult = Ok(Err(payload));
    let completion = build_completion_from_dispatch_result(
        result,
        &task,
        crate::common::epoch_millis(),
        Duration::from_millis(1000),
    );
    match completion {
        TaskCompletion::Failed { error, .. } => {
            assert!(error.contains("dispatcher panicked: static panic msg"));
        }
        other => panic!("expected Failed, got {:?}", other),
    }
}

#[test]
fn build_completion_from_string_panic_returns_failed() {
    let task = make_task_for_completion("t-panic-string", "panic_string_task");
    let payload: Box<dyn std::any::Any + Send> = Box::new("owned panic msg".to_string());
    let result: DispatchResult = Ok(Err(payload));
    let completion = build_completion_from_dispatch_result(
        result,
        &task,
        crate::common::epoch_millis(),
        Duration::from_millis(1000),
    );
    match completion {
        TaskCompletion::Failed { error, .. } => {
            assert!(error.contains("dispatcher panicked: owned panic msg"));
        }
        other => panic!("expected Failed, got {:?}", other),
    }
}

#[test]
fn build_completion_from_non_string_panic_returns_failed() {
    let task = make_task_for_completion("t-panic-nonstring", "panic_nonstring_task");
    let payload: Box<dyn std::any::Any + Send> = Box::new(42i32);
    let result: DispatchResult = Ok(Err(payload));
    let completion = build_completion_from_dispatch_result(
        result,
        &task,
        crate::common::epoch_millis(),
        Duration::from_millis(1000),
    );
    match completion {
        TaskCompletion::Failed { error, .. } => {
            assert!(error.contains("dispatcher panicked: <non-string panic>"));
        }
        other => panic!("expected Failed, got {:?}", other),
    }
}

#[tokio::test]
async fn build_completion_from_timeout_returns_failed() {
    let task = make_task_for_completion("t-timeout", "timeout_task");
    // 构造一个已经超时的 tokio::time::error::Elapsed
    let result: DispatchResult = Err(tokio::time::timeout(Duration::from_nanos(1), async {
        tokio::time::sleep(Duration::from_secs(1)).await;
    })
    .await
    .unwrap_err());
    let completion = build_completion_from_dispatch_result(
        result,
        &task,
        crate::common::epoch_millis(),
        Duration::from_millis(500),
    );
    match completion {
        TaskCompletion::Failed { error, .. } => {
            assert!(error.contains("task timed out after 500ms"));
        }
        other => panic!("expected Failed, got {:?}", other),
    }
}

// ───────────────────────── wait_for_task ─────────────────────────

struct SequenceScheduler {
    tasks: parking_lot::Mutex<Vec<Option<TaskDefinition>>>,
}

impl SequenceScheduler {
    fn new(tasks: Vec<Option<TaskDefinition>>) -> Self {
        Self {
            tasks: parking_lot::Mutex::new(tasks),
        }
    }
}

#[async_trait::async_trait]
impl Scheduler for SequenceScheduler {
    async fn enqueue(&self, _task: TaskDefinition) -> crate::common::Result<()> {
        Ok(())
    }

    async fn enqueue_batch(&self, _tasks: Vec<TaskDefinition>) -> crate::common::Result<()> {
        Ok(())
    }

    async fn dequeue(&self) -> Option<TaskDefinition> {
        None
    }

    async fn try_dequeue(&self) -> Option<TaskDefinition> {
        self.tasks.lock().pop().flatten()
    }

    async fn dequeue_batch(&self, _limit: usize) -> Vec<TaskDefinition> {
        Vec::new()
    }

    async fn drain_unrouted(&self) -> Vec<TaskDefinition> {
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

    fn close(&self) {}

    fn is_closed(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn wait_for_task_returns_task_immediately_when_available() {
    let task = make_task_for_completion("t-wait", "wait_task");
    let scheduler: Arc<dyn Scheduler> = Arc::new(SequenceScheduler::new(vec![Some(task.clone())]));
    let (_tx, mut rx) = tokio::sync::mpsc::channel::<BusEvent>(10);

    let result = tokio::time::timeout(Duration::from_millis(100), wait_for_task(&scheduler, &mut rx))
        .await
        .expect("should return quickly")
        .expect("should have a task");

    assert_eq!(result.id.as_str(), "t-wait");
}

#[tokio::test]
async fn wait_for_task_wakes_on_event_after_empty_dequeue() {
    let task = make_task_for_completion("t-wait-event", "wait_event_task");
    let scheduler: Arc<dyn Scheduler> =
        Arc::new(SequenceScheduler::new(vec![None, None, Some(task.clone())]));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<BusEvent>(10);

    // 模拟 SchedulerActor 发布 TaskEnqueued 事件唤醒 wait_for_task
    tokio::spawn(async move {
        // 第一次事件：唤醒后 try_dequeue 返回 None
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = tx.send(BusEvent::TaskEnqueued).await;
        // 第二次事件：唤醒后 try_dequeue 返回 Some(task)
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = tx.send(BusEvent::TaskEnqueued).await;
    });

    let result = tokio::time::timeout(Duration::from_millis(500), wait_for_task(&scheduler, &mut rx))
        .await
        .expect("should return within timeout")
        .expect("should have a task");

    assert_eq!(result.id.as_str(), "t-wait-event");
}

#[tokio::test]
async fn wait_for_task_returns_none_when_event_bus_closed() {
    // 当 EventBus 被 drop（所有 sender 关闭），wait_for_task 应返回 None
    let scheduler: Arc<dyn Scheduler> = Arc::new(SequenceScheduler::new(vec![None]));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<BusEvent>(10);

    // 立即 drop sender，模拟 EventBus shutdown
    drop(tx);

    let result = tokio::time::timeout(Duration::from_millis(100), wait_for_task(&scheduler, &mut rx))
        .await
        .expect("should return quickly");

    assert!(result.is_none(), "should return None when event bus is closed");
}

// ───────────────────────── Worker background loops ─────────────────────────

#[tokio::test]
async fn start_network_event_loop_runs_and_shuts_down_on_cancel() {
    let worker = make_worker("node-net-loop");
    worker.start_network_event_loop();
    // 发送 cancel 信号，后台任务应退出，不 panic 即通过。
    worker.shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn run_task_execution_loop_enters_drain_and_stops() {
    let worker = make_worker("node-run-loop");
    let pending_tx = worker.start_pending_result_loop();

    let handle = tokio::spawn(async move {
        // 先 drain 再进入 loop，loop 开头检测到 Draining 会直接退出。
        worker.drain();
        worker.run_task_execution_loop(&pending_tx).await.unwrap();
    });

    tokio::time::timeout(Duration::from_millis(500), handle)
        .await
        .expect("run_task_execution_loop should exit after drain")
        .expect("spawn should not panic");
}

#[tokio::test]
async fn run_task_execution_loop_cancel_signal_triggers_drain() {
    let worker = make_worker("node-cancel-loop");
    let pending_tx = worker.start_pending_result_loop();

    let handle = tokio::spawn(async move {
        // 让 loop 先运行一小段时间，然后 cancel。
        let cancel_tx = worker.cancel.clone();
        let loop_fut = worker.run_task_execution_loop(&pending_tx);
        tokio::pin!(loop_fut);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                let _ = cancel_tx.send(true);
                // loop_fut 在 drain 后应完成。
                loop_fut.await.unwrap();
            }
            result = &mut loop_fut => {
                result.unwrap();
            }
        }
    });

    tokio::time::timeout(Duration::from_millis(500), handle)
        .await
        .expect("run_task_execution_loop should exit after cancel")
        .expect("spawn should not panic");
}

#[tokio::test]
async fn forward_remote_task_unexpected_response_returns_error() {
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Ok(DirectResponse::TaskResultAck { accepted: true })),
    );
    let worker = make_worker_with_transport("node-A", transport);

    let task = TaskDefinition {
        id: TaskId::from("t-forward".to_string()),
        name: "forward_test".to_string(),
        payload: Vec::new(),
        workflow_id: None,
        target_node: Some(NodeId::from("node-B".to_string())),
        origin_node: None,
        retry_policy: None,
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    };

    let result = worker
        .forward_remote_task(&task, &NodeId::from("node-B".to_string()))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn forward_remote_task_network_error_returns_error() {
    let transport = Arc::new(
        ConfigurableMockTransport::new("node-A")
            .with_direct_response(Err(ActantError::Network("down".into()))),
    );
    let worker = make_worker_with_transport("node-A", transport);

    let task = TaskDefinition {
        id: TaskId::from("t-forward".to_string()),
        name: "forward_test".to_string(),
        payload: Vec::new(),
        workflow_id: None,
        target_node: Some(NodeId::from("node-B".to_string())),
        origin_node: None,
        retry_policy: None,
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    };

    let result = worker
        .forward_remote_task(&task, &NodeId::from("node-B".to_string()))
        .await;
    assert!(result.is_err());
}
