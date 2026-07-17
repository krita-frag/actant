//! Unit tests extracted from `src/runtime/event_bus.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use std::time::Duration;

#[tokio::test]
async fn subscribe_and_publish_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::TaskStarted);
    bus.publish(BusEvent::TaskStarted {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
    })
    .await;
    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("recv in time")
        .expect("event present");
    match ev {
        BusEvent::TaskStarted {
            workflow_id,
            task_id,
        } => {
            assert_eq!(workflow_id.as_str(), "wf-1");
            assert_eq!(task_id.as_str(), "t-1");
        }
        other => panic!("unexpected event: {:?}", other),
    }
}

#[tokio::test]
async fn publish_without_subscriber_is_noop() {
    let bus = EventBus::new();
    bus.publish(BusEvent::TaskCompleted(TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
        task_name: "tn".to_string(),
        result: vec![1],
        target_node: None,
    }))
    .await;
}

#[tokio::test]
async fn subscriber_timeout_pruning() {
    let bus = EventBus::with_config(EventBusConfig {
        subscriber_capacity: 1,
        publish_timeout_ms: 10,
        max_subscriber_timeouts: 1,
    });
    let _rx = bus.subscribe(Topic::ClusterHeartbeat);
    bus.publish(BusEvent::PeerConnected(NodeId::from("n-1")))
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    bus.publish(BusEvent::PeerConnected(NodeId::from("n-2")))
        .await;
}

#[test]
fn event_bus_config_default() {
    let cfg = EventBusConfig::default();
    assert_eq!(
        cfg.subscriber_capacity,
        EventBusConfig::DEFAULT_SUBSCRIBER_CAPACITY
    );
    assert_eq!(
        cfg.publish_timeout_ms,
        EventBusConfig::DEFAULT_PUBLISH_TIMEOUT_MS
    );
    assert_eq!(
        cfg.max_subscriber_timeouts,
        EventBusConfig::DEFAULT_MAX_SUBSCRIBER_TIMEOUTS
    );
}

#[tokio::test]
async fn multiple_subscribers_receive_same_event() {
    let bus = EventBus::new();
    let mut rx1 = bus.subscribe(Topic::TaskStarted);
    let mut rx2 = bus.subscribe(Topic::TaskStarted);

    bus.publish(BusEvent::TaskStarted {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
    })
    .await;

    let ev1 = tokio::time::timeout(Duration::from_millis(500), rx1.recv())
        .await
        .unwrap()
        .unwrap();
    let ev2 = tokio::time::timeout(Duration::from_millis(500), rx2.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev1, BusEvent::TaskStarted { .. }));
    assert!(matches!(ev2, BusEvent::TaskStarted { .. }));
}

#[tokio::test]
async fn subscriber_for_different_topic_does_not_receive() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::TaskCompleted);

    bus.publish(BusEvent::TaskStarted {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
    })
    .await;

    let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
    assert!(
        result.is_err(),
        "different-topic subscriber should not receive event"
    );
}

#[tokio::test]
async fn bus_clone_shares_subscribers() {
    let bus = EventBus::new();
    let bus2 = bus.clone();
    let mut rx = bus.subscribe(Topic::NetworkPeer);

    bus2.publish(BusEvent::PeerConnected(NodeId::from("n-1")))
        .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::PeerConnected(_)));
}

#[tokio::test]
async fn network_peer_topic_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::NetworkPeer);

    bus.publish(BusEvent::PeerConnected(NodeId::from("n-1")))
        .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::PeerConnected(_)));
}

#[test]
fn bus_event_topic_maps_correctly() {
    assert_eq!(
        BusEvent::TaskStarted {
            workflow_id: WorkflowId::from("wf-1"),
            task_id: TaskId::from("t-1"),
        }
        .topic(),
        Topic::TaskStarted
    );
    assert_eq!(
        BusEvent::PeerConnected(NodeId::from("n-1")).topic(),
        Topic::NetworkPeer
    );
    assert_eq!(
        BusEvent::PeerDisconnected(NodeId::from("n-1")).topic(),
        Topic::NetworkPeer
    );
    assert_eq!(
        BusEvent::WorkerDraining {
            node_id: NodeId::from("n-1")
        }
        .topic(),
        Topic::WorkerLifecycle
    );
}

#[test]
fn cloneable_events_can_be_cloned() {
    let ev = BusEvent::TaskStarted {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
    };
    assert!(ev.is_cloneable());
    assert!(ev.clone_broadcast().is_some());
}

#[tokio::test]
async fn best_effort_topic_drops_when_full() {
    let bus = EventBus::with_config(EventBusConfig {
        subscriber_capacity: 1,
        publish_timeout_ms: 1000,
        max_subscriber_timeouts: 100,
    });
    let _rx = bus.subscribe(Topic::ClusterHeartbeat);

    bus.publish(BusEvent::Heartbeat(NodeHeartbeat {
        node_id: NodeId::from("n-1"),
        active_workflows: Vec::new(),
        timestamp_ms: 0,
        available_slots: 0,
        max_slots: 0,
        endpoint_addr: None,
    }))
    .await;
}

#[test]
fn event_bus_clone_is_independent_for_subscribers() {
    let bus = EventBus::new();
    let _bus2 = bus.clone();
}

// ───────────────────────── topic() for all variants ─────────────────────────

#[test]
fn bus_event_topic_all_variants() {
    let wf = WorkflowId::from("wf");
    let tid = TaskId::from("t");
    let nid = NodeId::from("n");

    assert_eq!(
        BusEvent::TaskFailed(TaskCompletion::Failed {
            workflow_id: wf.clone(),
            task_id: tid.clone(),
            task_name: "n".into(),
            error: "e".into(),
            target_node: None,
        })
        .topic(),
        Topic::TaskFailed
    );
    assert_eq!(
        BusEvent::TaskCancelled(TaskCompletion::Cancelled {
            workflow_id: wf.clone(),
            task_id: tid.clone(),
            task_name: "n".into(),
            target_node: None,
        })
        .topic(),
        Topic::TaskCancelled
    );
    assert_eq!(
        BusEvent::TaskSkipped(TaskCompletion::Skipped {
            workflow_id: wf.clone(),
            task_id: tid.clone(),
            task_name: "n".into(),
            target_node: None,
        })
        .topic(),
        Topic::TaskSkipped
    );
    assert_eq!(
        BusEvent::PeerDisconnected(nid.clone()).topic(),
        Topic::NetworkPeer
    );
    assert_eq!(
        BusEvent::Heartbeat(NodeHeartbeat {
            node_id: nid.clone(),
            active_workflows: vec![],
            timestamp_ms: 0,
            available_slots: 0,
            max_slots: 0,
            endpoint_addr: None,
        })
        .topic(),
        Topic::ClusterHeartbeat
    );
    assert_eq!(
        BusEvent::Claim(OrchestratorClaim {
            node_id: nid.clone(),
            workflow_id: wf.clone(),
            timestamp_ms: 0,
        })
        .topic(),
        Topic::ClusterClaim
    );
    assert_eq!(
        BusEvent::DagUpdate(WireDagStateUpdate {
            workflow_id: wf.clone(),
            task_id: tid.clone(),
            task_state: crate::common::wire::WireTaskState::Running,
            hlc_timestamp: crate::runtime::state::HlcTimestamp::zero(),
            origin_node: nid.clone(),
        })
        .topic(),
        Topic::DagUpdate
    );
    assert_eq!(
        BusEvent::HeadsExchange(crate::common::wire::HeadsExchange {
            node_id: nid.clone(),
            heads: vec![],
        })
        .topic(),
        Topic::HeadsExchange
    );
    assert_eq!(
        BusEvent::WorkerDrained {
            node_id: nid.clone()
        }
        .topic(),
        Topic::WorkerLifecycle
    );
    assert_eq!(
        BusEvent::WorkerStopped {
            node_id: nid.clone()
        }
        .topic(),
        Topic::WorkerLifecycle
    );
}

// ───────────────────────── delivery_guarantee() ─────────────────────────

#[test]
fn delivery_guarantee_reliable_for_critical_events() {
    let wf = WorkflowId::from("wf");
    let tid = TaskId::from("t");
    let completion = TaskCompletion::Completed {
        workflow_id: wf.clone(),
        task_id: tid.clone(),
        task_name: "n".into(),
        result: vec![],
        target_node: None,
    };

    assert_eq!(
        BusEvent::TaskStarted {
            workflow_id: wf.clone(),
            task_id: tid.clone(),
        }
        .delivery_guarantee(),
        DeliveryGuarantee::Reliable
    );
    assert_eq!(
        BusEvent::TaskCompleted(completion).delivery_guarantee(),
        DeliveryGuarantee::Reliable
    );
}

#[test]
fn delivery_guarantee_best_effort_for_periodic_events() {
    let nid = NodeId::from("n");
    assert_eq!(
        BusEvent::PeerConnected(nid.clone()).delivery_guarantee(),
        DeliveryGuarantee::BestEffort
    );
    assert_eq!(
        BusEvent::Heartbeat(NodeHeartbeat {
            node_id: nid.clone(),
            active_workflows: vec![],
            timestamp_ms: 0,
            available_slots: 0,
            max_slots: 0,
            endpoint_addr: None,
        })
        .delivery_guarantee(),
        DeliveryGuarantee::BestEffort
    );
    assert_eq!(
        BusEvent::WorkerDraining {
            node_id: nid.clone()
        }
        .delivery_guarantee(),
        DeliveryGuarantee::BestEffort
    );
}

// ───────────────────────── clone_broadcast() ─────────────────────────

#[test]
fn clone_broadcast_returns_some_for_cloneable_events() {
    let wf = WorkflowId::from("wf");
    let tid = TaskId::from("t");
    let nid = NodeId::from("n");

    let ev = BusEvent::TaskStarted {
        workflow_id: wf.clone(),
        task_id: tid.clone(),
    };
    assert!(ev.clone_broadcast().is_some());

    let ev = BusEvent::PeerConnected(nid.clone());
    assert!(ev.clone_broadcast().is_some());

    let ev = BusEvent::WorkerDrained {
        node_id: nid.clone(),
    };
    assert!(ev.clone_broadcast().is_some());
}

#[test]
fn clone_broadcast_returns_none_for_direct_request() {
    let channel = crate::runtime::network::DirectResponseChannel::test_stub();
    let ev = BusEvent::DirectRequest {
        peer_id: "peer".to_string(),
        request: Box::new(DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf"),
            requesting_node: NodeId::from("n"),
        }),
        channel,
    };
    assert!(!ev.is_cloneable());
    assert!(ev.clone_broadcast().is_none());
}

// ───────────────────────── subscribe_with_capacity ─────────────────────────

#[tokio::test]
async fn subscribe_with_capacity_uses_custom_value() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_with_capacity(Topic::TaskStarted, 5);

    for i in 0..5 {
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId::from(format!("wf-{i}")),
            task_id: TaskId::from(format!("t-{i}")),
        })
        .await;
    }

    for i in 0..5 {
        let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match ev {
            BusEvent::TaskStarted { workflow_id, .. } => {
                assert_eq!(workflow_id.as_str(), format!("wf-{i}"));
            }
            _ => panic!("unexpected event"),
        }
    }
}

// ───────────────────────── publish edge cases ─────────────────────────

#[tokio::test]
async fn publish_reliable_event_to_slow_subscriber_succeeds_within_timeout() {
    let bus = EventBus::with_config(EventBusConfig {
        subscriber_capacity: 1,
        publish_timeout_ms: 500,
        max_subscriber_timeouts: 10,
    });
    let mut rx = bus.subscribe(Topic::TaskCompleted);

    bus.publish(BusEvent::TaskCompleted(TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
        task_name: "n".into(),
        result: vec![],
        target_node: None,
    }))
    .await;

    let _ = rx.recv().await;

    bus.publish(BusEvent::TaskCompleted(TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-2"),
        task_id: TaskId::from("t-2"),
        task_name: "n".into(),
        result: vec![],
        target_node: None,
    }))
    .await;

    let ev = rx.recv().await.unwrap();
    match ev {
        BusEvent::TaskCompleted(c) => assert_eq!(c.workflow_id().as_str(), "wf-2"),
        _ => panic!("unexpected event"),
    }
}

#[tokio::test]
async fn publish_best_effort_event_skips_full_subscriber() {
    let bus = EventBus::with_config(EventBusConfig {
        subscriber_capacity: 1,
        publish_timeout_ms: 1000,
        max_subscriber_timeouts: 100,
    });
    let _rx = bus.subscribe(Topic::NetworkPeer);

    bus.publish(BusEvent::PeerConnected(NodeId::from("n-1")))
        .await;
    bus.publish(BusEvent::PeerDisconnected(NodeId::from("n-2")))
        .await;
}

#[tokio::test]
async fn publish_to_topic_with_no_subscribers_is_noop() {
    let bus = EventBus::new();
    bus.publish(BusEvent::DagUpdate(
        crate::common::wire::WireDagStateUpdate {
            workflow_id: WorkflowId::from("wf-1"),
            task_id: TaskId::from("t-1"),
            task_state: crate::common::wire::WireTaskState::Running,
            hlc_timestamp: crate::runtime::state::HlcTimestamp::zero(),
            origin_node: NodeId::from("n-1"),
        },
    ))
    .await;
}

#[tokio::test]
async fn publish_claim_event_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::ClusterClaim);

    let claim = OrchestratorClaim {
        node_id: NodeId::from("n-1"),
        workflow_id: WorkflowId::from("wf-1"),
        timestamp_ms: 12345,
    };
    bus.publish(BusEvent::Claim(claim)).await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::Claim(_)));
}

#[tokio::test]
async fn publish_dag_update_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::DagUpdate);

    bus.publish(BusEvent::DagUpdate(
        crate::common::wire::WireDagStateUpdate {
            workflow_id: WorkflowId::from("wf-1"),
            task_id: TaskId::from("t-1"),
            task_state: crate::common::wire::WireTaskState::Completed { result: vec![1] },
            hlc_timestamp: crate::runtime::state::HlcTimestamp::zero(),
            origin_node: NodeId::from("n-1"),
        },
    ))
    .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::DagUpdate(_)));
}

#[tokio::test]
async fn publish_supervision_event_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::Supervision);

    bus.publish(BusEvent::SupervisionEvent(SupervisionEvent::ActorStarted {
        actor_id: crate::common::ActorId::workflow(&NodeId::from("n-1")),
    }))
    .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::SupervisionEvent(_)));
}

#[tokio::test]
async fn publish_worker_lifecycle_events() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::WorkerLifecycle);

    let nid = NodeId::from("n-1");
    bus.publish(BusEvent::WorkerDraining {
        node_id: nid.clone(),
    })
    .await;
    bus.publish(BusEvent::WorkerDrained {
        node_id: nid.clone(),
    })
    .await;
    bus.publish(BusEvent::WorkerStopped { node_id: nid }).await;

    assert!(matches!(
        rx.recv().await.unwrap(),
        BusEvent::WorkerDraining { .. }
    ));
    assert!(matches!(
        rx.recv().await.unwrap(),
        BusEvent::WorkerDrained { .. }
    ));
    assert!(matches!(
        rx.recv().await.unwrap(),
        BusEvent::WorkerStopped { .. }
    ));
}

#[tokio::test]
async fn publish_heads_exchange_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::HeadsExchange);

    bus.publish(BusEvent::HeadsExchange(
        crate::common::wire::HeadsExchange {
            node_id: NodeId::from("n-1"),
            heads: vec![],
        },
    ))
    .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::HeadsExchange(_)));
}

#[tokio::test]
async fn publish_task_failed_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::TaskFailed);

    bus.publish(BusEvent::TaskFailed(TaskCompletion::Failed {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
        task_name: "n".into(),
        error: "boom".into(),
        target_node: None,
    }))
    .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    match ev {
        BusEvent::TaskFailed(c) => assert_eq!(c.workflow_id().as_str(), "wf-1"),
        _ => panic!("unexpected event"),
    }
}

#[tokio::test]
async fn publish_task_cancelled_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::TaskCancelled);

    bus.publish(BusEvent::TaskCancelled(TaskCompletion::Cancelled {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
        task_name: "n".into(),
        target_node: None,
    }))
    .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::TaskCancelled(_)));
}

#[tokio::test]
async fn publish_task_skipped_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::TaskSkipped);

    bus.publish(BusEvent::TaskSkipped(TaskCompletion::Skipped {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
        task_name: "n".into(),
        target_node: None,
    }))
    .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::TaskSkipped(_)));
}

#[tokio::test]
async fn subscriber_pruning_after_max_timeouts() {
    let bus = EventBus::with_config(EventBusConfig {
        subscriber_capacity: 1,
        publish_timeout_ms: 10,
        max_subscriber_timeouts: 2,
    });
    let _rx = bus.subscribe(Topic::TaskCompleted);

    for i in 0..5 {
        bus.publish(BusEvent::TaskCompleted(TaskCompletion::Completed {
            workflow_id: WorkflowId::from(format!("wf-{i}")),
            task_id: TaskId::from(format!("t-{i}")),
            task_name: "n".into(),
            result: vec![],
            target_node: None,
        }))
        .await;
    }
}

#[tokio::test]
async fn event_bus_default_creates_valid_instance() {
    let bus = EventBus::default();
    let mut rx = bus.subscribe(Topic::TaskStarted);
    bus.publish(BusEvent::TaskStarted {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
    })
    .await;
    assert!(matches!(
        rx.recv().await,
        Some(BusEvent::TaskStarted { .. })
    ));
}

// ───────────────────────── DirectRequest exclusive dispatch ─────────────────────────

#[tokio::test]
async fn direct_request_no_subscriber_sends_error_response() {
    let bus = EventBus::new();
    let channel = crate::runtime::network::DirectResponseChannel::test_stub();

    bus.publish(BusEvent::DirectRequest {
        peer_id: "peer-B".to_string(),
        request: Box::new(DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf-1"),
            requesting_node: NodeId::from("n-1"),
        }),
        channel,
    })
    .await;
}

#[tokio::test]
async fn direct_request_with_subscriber_dispatches_exclusively() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::NetworkDirect);
    let channel = crate::runtime::network::DirectResponseChannel::test_stub();

    bus.publish(BusEvent::DirectRequest {
        peer_id: "peer-B".to_string(),
        request: Box::new(DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf-1"),
            requesting_node: NodeId::from("n-1"),
        }),
        channel,
    })
    .await;

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::DirectRequest { .. }));
}

#[tokio::test]
async fn closed_subscriber_is_pruned_on_publish() {
    let bus = EventBus::new();
    let rx = bus.subscribe(Topic::TaskStarted);
    drop(rx);

    bus.publish(BusEvent::TaskStarted {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
    })
    .await;

    let mut rx2 = bus.subscribe(Topic::TaskStarted);
    let result = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await;
    assert!(
        result.is_err(),
        "pruned subscriber should not receive stale events"
    );
}
