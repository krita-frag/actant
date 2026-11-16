//! Unit tests extracted from `src/runtime/workflow/scheduler.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::{ActorId, TaskId};
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

/// 批量入队必须与单条入队共用「软上限」计量。
///
/// 上限只在单条路径检查时，批量路径可多批提交无限突破上限：队列无界增长，
/// 且 drain 期被拒的任务没有任何终态通知。本用例先把队列恰好填满，再验证
/// 批与单条两路都被拒。
#[tokio::test]
async fn enqueue_batch_respects_queue_soft_limit() {
    use crate::runtime::workflow::actor::DEFAULT_MAX_QUEUED_TASKS;

    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-batch-cap".to_string());
    actor_system
        .spawn(actor_id.clone(), SchedulerActor::fifo())
        .await
        .unwrap();
    let client = ActorScheduler::new(actor_id, actor_system);

    let full: Vec<TaskDefinition> = (0..DEFAULT_MAX_QUEUED_TASKS)
        .map(|i| make_task(&format!("cap-{i}"), 0))
        .collect();
    client
        .enqueue_batch(full)
        .await
        .expect("恰好填满软上限的批次必须被接受");

    let err = client
        .enqueue_batch(vec![make_task("over-limit", 0)])
        .await
        .expect_err("超出软上限的批次必须被拒绝");
    assert!(
        matches!(err, crate::common::ActantError::Worker(_)),
        "队列满应返回 Worker 错误，实际: {err:?}"
    );
    assert!(
        client
            .enqueue(make_task("over-limit-single", 0))
            .await
            .is_err(),
        "单条入队必须与批量共用同一上限"
    );
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

/// 外部调度器的 async 方法必须在 Actor 任务内直接 await。
///
/// 在 current_thread runtime 上，旧实现的 `futures::executor::block_on`
/// 会阻塞唯一线程，使 tokio timer 无法推进而永久挂起；本测试同时覆盖
/// len / is_empty 的 External 分支。
#[tokio::test(flavor = "current_thread")]
async fn external_scheduler_awaits_without_blocking_runtime() {
    struct TimerScheduler;

    #[async_trait]
    impl Scheduler for TimerScheduler {
        async fn enqueue(&self, _task: TaskDefinition) -> Result<(), crate::common::ActantError> {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            Ok(())
        }

        async fn enqueue_batch(
            &self,
            _tasks: Vec<TaskDefinition>,
        ) -> Result<(), crate::common::ActantError> {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
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

        async fn remove_workflow_tasks(&self, _workflow_id: &crate::common::WorkflowId) -> usize {
            0
        }
    }

    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("scheduler-external-timer".to_string());
    let actor = SchedulerActor::fifo()
        .with_scheduler(Arc::new(TimerScheduler))
        .unwrap();
    actor_system.spawn(actor_id.clone(), actor).await.unwrap();
    let client = ActorScheduler::new(actor_id, actor_system);

    client.enqueue(make_task("x", 0)).await.unwrap();
    client.enqueue_batch(vec![make_task("y", 0)]).await.unwrap();
    assert!(client.try_dequeue().await.is_none());
    assert!(client.dequeue_batch(10).await.is_empty());
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

/// 注入外部调度器后，enqueue 必须唤醒共享的 `Notify`。
///
/// 注入缝只接管「队列」，唤醒语义仍由 `InnerScheduler` 负责：外部实现拿不到
/// Worker 等待的那个 `Notify`。修复前 `with_scheduler` 把 `with_notify` 建的
/// `Notify` 连同 FIFO 变体一起丢弃，External 分支也不发信号——入队成功但
/// Worker 只能靠主循环 1s 的租约检查轮询，吞吐被压到 1 次/秒。
#[tokio::test]
async fn injected_scheduler_signals_shared_notify_on_enqueue() {
    struct NullScheduler;

    #[async_trait]
    impl Scheduler for NullScheduler {
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
            Vec::new()
        }

        async fn remove_workflow_tasks(&self, _workflow_id: &crate::common::WorkflowId) -> usize {
            0
        }
    }

    let notify = Arc::new(tokio::sync::Notify::new());
    let actor = SchedulerActor::with_notify(notify.clone())
        .with_scheduler(Arc::new(NullScheduler))
        .expect("inject external scheduler");
    let inner = actor.shared_inner();

    // 先挂起一个等待者，再入队：等待者必须在超时前被唤醒。
    let waiter = {
        let notify = notify.clone();
        tokio::spawn(async move { notify.notified().await })
    };
    tokio::task::yield_now().await;

    inner
        .enqueue(make_task("wake", 0))
        .await
        .expect("external enqueue must succeed");

    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("enqueue must signal the shared notify for injected schedulers")
        .expect("waiter task must not panic");
}

/// 撤销某工作流排队任务：只过滤目标任务，不影响其他工作流与直提任务。
///
/// 两条实现路径都要覆盖——Actor 消息（`ActorScheduler::new`）与共享队列快路径
/// （`with_fast_path`，生产装配用的那条）。让位时 failover 走的是同一个 trait
/// 方法，路径不一致会让「撤销了但没真撤」静默发生。
#[tokio::test]
async fn remove_workflow_tasks_filters_only_target_workflow() {
    let wf_a = WorkflowId::from("wf-a");
    let wf_b = WorkflowId::from("wf-b");

    for use_fast_path in [false, true] {
        let actor_system = Arc::new(ActorSystem::new());
        let actor_id = ActorId::new(format!("scheduler-purge-{use_fast_path}"));
        let actor = SchedulerActor::priority();
        let client = if use_fast_path {
            let inner = actor.shared_inner();
            actor_system.spawn(actor_id.clone(), actor).await.unwrap();
            ActorScheduler::with_fast_path(actor_id, actor_system.clone(), inner)
        } else {
            actor_system.spawn(actor_id.clone(), actor).await.unwrap();
            ActorScheduler::new(actor_id, actor_system.clone())
        };

        let mut a1 = make_task("a1", 0);
        a1.workflow_id = Some(wf_a.clone());
        let mut a2 = make_task("a2", 0);
        a2.workflow_id = Some(wf_a.clone());
        let mut b1 = make_task("b1", 0);
        b1.workflow_id = Some(wf_b.clone());
        let standalone = make_task("solo", 0);

        client
            .enqueue_batch(vec![a1, a2, b1, standalone])
            .await
            .unwrap();

        let removed = client.remove_workflow_tasks(&wf_a).await;
        assert_eq!(
            removed, 2,
            "fast_path={use_fast_path}: 只应移除 wf-a 的两条"
        );

        let mut left = Vec::new();
        while let Some(task) = client.try_dequeue().await {
            left.push(task.name);
        }
        left.sort();
        assert_eq!(
            left,
            vec!["b1".to_string(), "solo".to_string()],
            "fast_path={use_fast_path}: 其他工作流与直提任务必须保留"
        );

        // 再撤一次：已无该工作流任务，返回 0 且不误删。
        assert_eq!(client.remove_workflow_tasks(&wf_a).await, 0);
        client.close();
    }
}
