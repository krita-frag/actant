//! Unit tests extracted from `src/runtime/workflow/scheduler.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::{ActorId, NodeId, TaskId};
use crate::runtime::actor::ActorSystem;
use crate::runtime::workflow::SchedulerActor;

fn make_task(name: &str, priority: i32) -> TaskDefinition {
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

#[tokio::test]
async fn actor_scheduler_forwards_through_scheduler_actor() {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-test".to_string());
    actor_system
        .spawn(actor_id.clone(), SchedulerActor::priority())
        .await
        .unwrap();

    let client = ActorScheduler::new(actor_id, actor_system);
    client.enqueue(make_task("low", -10)).await.unwrap();
    client.enqueue(make_task("high", 10)).await.unwrap();
    client.enqueue(make_task("mid", 0)).await.unwrap();

    assert_eq!(client.len().await, 3);
    assert!(!client.is_empty().await);
    assert_eq!(client.try_dequeue().await.unwrap().name, "high");
    assert_eq!(client.try_dequeue().await.unwrap().name, "mid");
    assert_eq!(client.try_dequeue().await.unwrap().name, "low");
    assert!(client.try_dequeue().await.is_none());

    client.close();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(client.is_closed());
}

#[tokio::test]
async fn enqueue_batch_and_dequeue_batch_roundtrip() {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-batch".to_string());
    actor_system
        .spawn(actor_id.clone(), SchedulerActor::priority())
        .await
        .unwrap();

    let client = ActorScheduler::new(actor_id, actor_system);
    client
        .enqueue_batch(vec![make_task("a", 1), make_task("b", 2)])
        .await
        .unwrap();

    let batch = client.dequeue_batch(10).await;
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].name, "b");
    assert_eq!(batch[1].name, "a");
}

#[tokio::test]
async fn fifo_scheduler_preserves_insertion_order() {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-fifo".to_string());
    actor_system
        .spawn(actor_id.clone(), SchedulerActor::fifo())
        .await
        .unwrap();

    let client = ActorScheduler::new(actor_id, actor_system);
    client.enqueue(make_task("first", 0)).await.unwrap();
    client.enqueue(make_task("second", 0)).await.unwrap();

    assert_eq!(client.dequeue().await.unwrap().name, "first");
    assert_eq!(client.dequeue().await.unwrap().name, "second");
}

#[tokio::test]
async fn closed_scheduler_rejects_new_tasks() {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-closed".to_string());
    actor_system
        .spawn(actor_id.clone(), SchedulerActor::priority())
        .await
        .unwrap();

    let client = ActorScheduler::new(actor_id, actor_system);
    client.close();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let result = client.enqueue(make_task("x", 0)).await;
    assert!(
        result.is_err(),
        "closed scheduler should reject enqueue: {:?}",
        result
    );
}

#[tokio::test]
async fn drain_unrouted_keeps_routed_tasks() {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-drain".to_string());
    actor_system
        .spawn(actor_id.clone(), SchedulerActor::priority())
        .await
        .unwrap();

    let client = ActorScheduler::new(actor_id, actor_system);
    let mut routed = make_task("routed", 0);
    routed.target_node = Some(NodeId::from("node-1".to_string()));
    let unrouted = make_task("unrouted", 0);

    client.enqueue(routed).await.unwrap();
    client.enqueue(unrouted).await.unwrap();

    let drained = client.drain_unrouted().await;
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].name, "unrouted");
    assert_eq!(client.len().await, 1);
}

// --- Scheduler trait default methods ---

struct MockScheduler;

#[async_trait]
impl Scheduler for MockScheduler {
    async fn enqueue(&self, _task: TaskDefinition) -> Result<(), crate::common::ActantError> {
        Ok(())
    }

    async fn enqueue_batch(
        &self,
        _tasks: Vec<TaskDefinition>,
    ) -> Result<(), crate::common::ActantError> {
        Ok(())
    }

    async fn dequeue(&self) -> Option<TaskDefinition> {
        None
    }

    async fn try_dequeue(&self) -> Option<TaskDefinition> {
        None
    }

    async fn dequeue_batch(&self, _limit: usize) -> Vec<TaskDefinition> {
        vec![]
    }

    async fn drain_unrouted(&self) -> Vec<TaskDefinition> {
        vec![]
    }

    async fn is_empty(&self) -> bool {
        true
    }

    async fn len(&self) -> usize {
        0
    }
}

#[test]
fn scheduler_trait_default_methods_return_expected_values() {
    let s = MockScheduler;
    assert_eq!(s.total_queued(), 0);
    assert!(!s.is_closed());
    s.close(); // default no-op
    assert!(!s.is_closed());
}

#[tokio::test]
async fn slow_path_scheduler_rejects_enqueue_after_close() {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-slow-close".to_string());
    actor_system
        .spawn(actor_id.clone(), SchedulerActor::fifo())
        .await
        .unwrap();

    let client = ActorScheduler::new(actor_id, actor_system);
    client.close();

    assert!(client.enqueue(make_task("late", 0)).await.is_err());
    assert!(client
        .enqueue_batch(vec![make_task("late-batch", 0)])
        .await
        .is_err());
}

// --- Scheduler 契约测试（供第三方实现复用） ---

#[tokio::test]
async fn priority_scheduler_satisfies_contract() {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-contract-priority".to_string());
    let scheduler = spawn_fast_path_scheduler(actor_id, actor_system, SchedulerActor::priority())
        .await
        .expect("fast-path priority scheduler must spawn");

    crate::test_support::assert_scheduler_contract(&scheduler).await;
}

#[tokio::test]
async fn fifo_scheduler_satisfies_contract() {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-contract-fifo".to_string());
    let scheduler = spawn_fast_path_scheduler(actor_id, actor_system, SchedulerActor::fifo())
        .await
        .expect("fast-path fifo scheduler must spawn");

    crate::test_support::assert_scheduler_contract(&scheduler).await;
}
