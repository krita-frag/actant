//! Unit tests extracted from `src/runtime/event_bus.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.
//!
//! EventBus 是纯非阻塞观测 tap：`publish` 对所有订阅者 `try_send`，满即丢，
//! 无 await 点。dropped 计数（`actant.event_bus.publish.dropped`）的全局指标
//! 断言在 `tests/rust/unit/metrics.rs`（需全局 provider 串行化），此处通过
//! 「事件未送达 + publish 即时返回」验证丢弃行为本身。

use super::*;
use std::time::{Duration, Instant};

fn task_started(wf: &str) -> BusEvent {
    BusEvent::TaskStarted {
        workflow_id: WorkflowId::from(wf),
        task_id: TaskId::from("t-1"),
    }
}

#[tokio::test]
async fn subscribe_and_publish_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::TaskStarted);
    bus.publish(task_started("wf-1"));
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
    }));
}

#[tokio::test]
async fn publish_to_full_channel_returns_immediately_and_drops() {
    let bus = EventBus::with_config(EventBusConfig {
        subscriber_capacity: 1,
    });
    let mut rx = bus.subscribe(Topic::TaskStarted);

    // 第一条占满通道。
    bus.publish(task_started("wf-keep"));
    // 第二条：通道满，必须即时返回（不阻塞、不等待），事件被丢弃。
    let start = Instant::now();
    bus.publish(task_started("wf-dropped"));
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(50),
        "publish to full channel must not block, took {:?}",
        elapsed
    );

    // 被丢弃的事件不会在订阅者腾出空间后重投。
    let ev = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("first event should be delivered")
        .expect("channel open");
    match ev {
        BusEvent::TaskStarted { workflow_id, .. } => {
            assert_eq!(workflow_id.as_str(), "wf-keep");
        }
        other => panic!("unexpected event: {:?}", other),
    }
    let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
    assert!(
        result.is_err(),
        "dropped event must not be redelivered after subscriber catches up"
    );
}

#[tokio::test]
async fn closed_subscriber_is_pruned_on_publish() {
    let bus = EventBus::new();
    let rx = bus.subscribe(Topic::TaskStarted);
    drop(rx);

    bus.publish(task_started("wf-1"));

    let mut rx2 = bus.subscribe(Topic::TaskStarted);
    bus.publish(task_started("wf-2"));
    let ev = tokio::time::timeout(Duration::from_millis(500), rx2.recv())
        .await
        .expect("recv in time")
        .expect("event present");
    assert!(matches!(ev, BusEvent::TaskStarted { .. }));
}

#[test]
fn event_bus_config_default() {
    let cfg = EventBusConfig::default();
    assert_eq!(
        cfg.subscriber_capacity,
        EventBusConfig::DEFAULT_SUBSCRIBER_CAPACITY
    );
}

#[tokio::test]
async fn multiple_subscribers_receive_same_event() {
    let bus = EventBus::new();
    let mut rx1 = bus.subscribe(Topic::TaskStarted);
    let mut rx2 = bus.subscribe(Topic::TaskStarted);

    bus.publish(task_started("wf-1"));

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

    bus.publish(task_started("wf-1"));

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

    bus2.publish(BusEvent::PeerConnected(NodeId::from("n-1")));

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

    bus.publish(BusEvent::PeerConnected(NodeId::from("n-1")));

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::PeerConnected(_)));
}

#[test]
fn bus_event_topic_maps_correctly() {
    assert_eq!(task_started("wf-1").topic(), Topic::TaskStarted);
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
fn bus_event_is_clone() {
    let ev = task_started("wf-1");
    let cloned = ev.clone();
    assert!(matches!(cloned, BusEvent::TaskStarted { .. }));
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
    assert_eq!(
        BusEvent::TaskDequeued {
            workflow_id: wf,
            task_id: tid,
        }
        .topic(),
        Topic::TaskDequeued
    );
    assert_eq!(
        BusEvent::ActorLifecycleError {
            actor_id: crate::common::ActorId::from("a"),
            error: "boom".into(),
        }
        .topic(),
        Topic::ActorLifecycleError
    );
    assert_eq!(
        BusEvent::WalCompacted {
            node_id: nid,
            retained_events: 0,
        }
        .topic(),
        Topic::WalCompacted
    );
}

// ───────────────────────── subscribe_with_capacity ─────────────────────────

#[tokio::test]
async fn subscribe_with_capacity_uses_custom_value() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_with_capacity(Topic::TaskStarted, 5);

    for i in 0..5 {
        bus.publish(task_started(&format!("wf-{i}")));
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
async fn publish_worker_lifecycle_events() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::WorkerLifecycle);

    let nid = NodeId::from("n-1");
    bus.publish(BusEvent::WorkerDraining {
        node_id: nid.clone(),
    });
    bus.publish(BusEvent::WorkerDrained {
        node_id: nid.clone(),
    });
    bus.publish(BusEvent::WorkerStopped { node_id: nid });

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
async fn publish_task_failed_roundtrip() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::TaskFailed);

    bus.publish(BusEvent::TaskFailed(TaskCompletion::Failed {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: TaskId::from("t-1"),
        task_name: "n".into(),
        error: "boom".into(),
        target_node: None,
    }));

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
    }));

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
    }));

    let ev = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, BusEvent::TaskSkipped(_)));
}

#[tokio::test]
async fn event_bus_default_creates_valid_instance() {
    let bus = EventBus::default();
    let mut rx = bus.subscribe(Topic::TaskStarted);
    bus.publish(task_started("wf-1"));
    assert!(matches!(
        rx.recv().await,
        Some(BusEvent::TaskStarted { .. })
    ));
}

// ───────────────────────── DirectRequest exclusive dispatch ─────────────────────────
//
// DirectRequest 不走 EventBus（参见 src/runtime/event_bus.rs 顶部注释与
// `NetworkEventRouter::handle_direct_request` 的 `other` 分支）：点对点请求-响应
// 由接收方直接处理或回送 Error，无独占投递路径，故此处无对应测试用例。
