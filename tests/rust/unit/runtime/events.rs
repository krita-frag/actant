//! Unit tests extracted from `src/runtime/events.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.
//!
//! 两个原语各自的投递契约：feed（FIFO 保序、单消费者、永不丢）与 tap
//! （broadcast、Lagged 跳过、无订阅者零开销）。

use super::*;
use crate::common::{NodeId, TaskCompletion, WorkflowId};

fn completed(task: &str) -> TaskLifecycleEvent {
    TaskLifecycleEvent::TaskCompleted(TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: crate::common::TaskId::from(task),
        task_name: "tn".to_string(),
        result: vec![1],
        target_node: None,
    })
}

#[test]
fn feed_fifo_preserves_order_across_event_kinds() {
    let (feed, mut rx) = TaskResultFeed::new();
    feed.send(TaskLifecycleEvent::TaskStarted {
        workflow_id: WorkflowId::from("wf-1"),
        task_id: crate::common::TaskId::from("t-1"),
    });
    feed.send(completed("t-1"));

    let first = rx.try_recv().expect("started first");
    let TaskLifecycleEvent::TaskStarted { task_id, .. } = first else {
        panic!("expected Started, got {first:?}");
    };
    assert_eq!(task_id.as_str(), "t-1");
    let second = rx.try_recv().expect("terminal second");
    assert_eq!(second.task_id().as_str(), "t-1");
}

#[test]
fn feed_send_after_receiver_drop_is_quiet() {
    let (feed, rx) = TaskResultFeed::new();
    drop(rx);
    // 关闭后发布不 panic、不传播错误（shutdown 竞态合法路径）。
    feed.send(completed("t-1"));
    feed.send(completed("t-2"));
}

#[test]
fn feed_empty_try_recv_is_pending() {
    let (_feed, mut rx) = TaskResultFeed::new();
    // 未发事件时接收端处于 pending（无轮询误导）。
    assert!(rx.try_recv().is_err(), "empty feed yields no event");
}

#[test]
fn tap_publish_without_subscriber_is_noop() {
    let tap = EventTap::new();
    tap.publish(TapEvent::PeerConnected(NodeId::from("p-1".to_string())));
}

#[test]
fn tap_broadcast_delivers_to_all_subscribers() {
    let tap = EventTap::new();
    let mut rx1 = tap.subscribe();
    let mut rx2 = tap.subscribe();

    tap.publish(TapEvent::PeerDisconnected(NodeId::from("p-1".to_string())));

    for rx in [&mut rx1, &mut rx2] {
        let ev = rx.try_recv().expect("each subscriber gets the event");
        assert!(matches!(ev, TapEvent::PeerDisconnected(_)));
    }
}

/// Lagged 订阅者拿到 `RecvError::Lagged` 而不是旧事件——tap 语义内建，
/// 接收端应计数 `actant.tap.lagged_total` 并继续消费新事件。
#[test]
fn tap_lagged_subscriber_skips_and_resumes() {
    let tap = EventTap::new();
    let mut rx = tap.subscribe();

    // 容量 1024：灌入超过容量的事件挤掉最旧记录。
    for i in 0..(TAP_CAPACITY + 8) {
        tap.publish(TapEvent::PeerConnected(NodeId::from(format!("p-{i}"))));
    }

    assert!(
        matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ),
        "overrun subscriber must observe Lagged, not stale events"
    );
    // 跳过之后恢复接收最新事件。
    let ev = rx.try_recv().expect("resumes after lag");
    assert!(matches!(ev, TapEvent::PeerConnected(_)));
}

#[test]
fn tap_event_log_clone_shares_payload() {
    // Arc<str> 载荷在广播克隆时共享底层字符串（引用计数而非深拷贝）。
    let event = TapEvent::TaskLog {
        task_id: crate::common::TaskId::from("t-1"),
        level: std::sync::Arc::from("INFO"),
        message: std::sync::Arc::from("hello"),
    };
    let clone = event.clone();
    let TapEvent::TaskLog { message, .. } = clone else {
        panic!("unexpected variant");
    };
    assert_eq!(message.as_ref(), "hello");
}
