use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::common::scheduler_kind;
use crate::common::{ActantError, TaskDefinition};

/// 任务调度器抽象。
///
/// 此 trait 将 DAG 编排器与具体调度策略解耦。默认实现 [`PriorityScheduler`]
/// 基于 `BTreeMap<Reverse<i32>, VecDeque>` 提供优先级 + FIFO 语义。
///
/// # 公共扩展点
///
/// 此 trait 是 Rust 核心的公共扩展点。外部 Rust 用户可实现此 trait 以替换调度策略
/// （例如公平调度、加权轮询、截止时间优先等）。实现只需满足 `Send + Sync`。
///
/// # 0.1.0 限制
///
/// Python 层**无法注入**自定义 `Scheduler` 实现。Python 用户仅通过 `_ActantConfig.scheduler`
/// 字符串选择内置调度器（当前仅 `"priority"`）。自定义 `Scheduler` 实现目前仅适用于
/// 纯 Rust 嵌入场景（Python 绑定暂未暴露注入入口）；0.2 计划通过 PyO3 暴露注入入口。
#[async_trait]
pub trait Scheduler: Send + Sync {
    /// Enqueue a task. Returns `Err` if the scheduler is closed (drain mode).
    async fn enqueue(&self, task: TaskDefinition) -> Result<(), ActantError>;
    /// Enqueue a batch of tasks. Returns `Err` if the scheduler is closed.
    async fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> Result<(), ActantError>;
    async fn dequeue(&self) -> Option<TaskDefinition>;
    /// Non-blocking dequeue: returns immediately with a task or `None`.
    async fn try_dequeue(&self) -> Option<TaskDefinition>;
    /// Dequeue up to `limit` tasks, respecting priority ordering.
    /// Returns fewer than `limit` if the queue doesn't have enough tasks.
    async fn dequeue_batch(&self, limit: usize) -> Vec<TaskDefinition>;
    /// Drain only tasks that have no target_node, leaving routed tasks in place.
    /// This avoids the race condition of drain-all-then-requeue.
    async fn drain_unrouted(&self) -> Vec<TaskDefinition>;
    async fn is_empty(&self) -> bool;
    async fn len(&self) -> usize;
    /// Returns total number of queued tasks across all priority levels.
    fn total_queued(&self) -> usize {
        0
    }
    /// Close the scheduler — reject new enqueues and wake waiters so
    /// `dequeue()` returns `None` once the queue is empty.
    fn close(&self) {}
    /// Returns `true` if the scheduler has been closed.
    fn is_closed(&self) -> bool {
        false
    }
}

/// Priority-based scheduler with FIFO ordering within each priority level.
///
/// Uses a `BTreeMap<Reverse<i32>, VecDeque>` so any integer priority value
/// is supported — the Python layer defines the semantic meaning of specific
/// values. Higher-priority tasks are always dequeued before lower-priority ones.
/// Within the same priority level, tasks are processed in FIFO order.
#[derive(Clone)]
pub struct PriorityScheduler {
    /// Keyed by `Reverse(priority)` so that `BTreeMap` iteration order
    /// yields highest priority first.
    queues: Arc<Mutex<BTreeMap<std::cmp::Reverse<i32>, VecDeque<TaskDefinition>>>>,
    notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl Scheduler for PriorityScheduler {
    async fn enqueue(&self, mut task: TaskDefinition) -> Result<(), ActantError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ActantError::InvalidState(
                "scheduler is closed (drain mode), rejecting task".into(),
            ));
        }
        task.enqueued_at_ms = crate::common::epoch_millis();
        let key = std::cmp::Reverse(task.priority);
        self.queues.lock().entry(key).or_default().push_back(task);
        self.notify.notify_one();
        Ok(())
    }

    async fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> Result<(), ActantError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ActantError::InvalidState(
                "scheduler is closed (drain mode), rejecting tasks".into(),
            ));
        }
        if tasks.is_empty() {
            return Ok(());
        }
        let now_ms = crate::common::epoch_millis();
        let mut queues = self.queues.lock();
        for mut task in tasks {
            task.enqueued_at_ms = now_ms;
            let key = std::cmp::Reverse(task.priority);
            queues.entry(key).or_default().push_back(task);
        }
        drop(queues);
        self.notify.notify_one();
        Ok(())
    }

    async fn dequeue(&self) -> Option<TaskDefinition> {
        loop {
            let task = {
                let mut queues = self.queues.lock();
                // BTreeMap iterates in ascending key order; since keys are
                // Reverse(priority), the first entry has the highest priority.
                if let Some((_, queue)) = queues.iter_mut().next() {
                    queue.pop_front()
                } else {
                    None
                }
            };
            if task.is_some() {
                return task;
            }
            // 若已关闭且为空，唤醒所有等待者并返回 None。
            if self.closed.load(Ordering::Acquire) {
                self.notify.notify_waiters();
                return None;
            }
            self.notify.notified().await;
        }
    }

    async fn try_dequeue(&self) -> Option<TaskDefinition> {
        let mut queues = self.queues.lock();
        let task = queues.iter_mut().next().and_then(|(_, q)| q.pop_front());
        // 清理空队列，避免下次 try_dequeue 命中空队列而误返回 None。
        // dequeue_batch 已有相同清理逻辑；try_dequeue 此前遗漏。
        queues.retain(|_, q| !q.is_empty());
        task
    }

    async fn dequeue_batch(&self, limit: usize) -> Vec<TaskDefinition> {
        let mut result = Vec::with_capacity(limit);
        let mut queues = self.queues.lock();
        while result.len() < limit {
            // BTreeMap 按 Reverse(priority) 升序，首个 key 即最高优先级。
            // 用 keys().next().copied() 取 key，避免 iter_mut 借用阻碍 remove。
            let key = match queues.keys().next().copied() {
                Some(k) => k,
                None => break,
            };
            // 使用 entry API 取代 get_mut().expect()：key 来自 keys().next()，
            // 理论上 entry 必为 Occupied；但以 Vacant 防御性 break 替代 panic，
            // 避免任何并发场景下的运行时崩溃。
            use std::collections::btree_map::Entry;
            match queues.entry(key) {
                Entry::Occupied(mut entry) => match entry.get_mut().pop_front() {
                    Some(task) => result.push(task),
                    // 当前优先级队列已空 — 删除 entry，下次循环取下一个优先级。
                    None => {
                        entry.remove();
                    }
                },
                Entry::Vacant(_) => break,
            }
        }
        result
    }

    async fn drain_unrouted(&self) -> Vec<TaskDefinition> {
        let mut queues = self.queues.lock();
        let mut unrouted = Vec::new();
        for (_, queue) in queues.iter_mut() {
            let old = std::mem::take(queue);
            for task in old {
                if task.target_node.is_none() {
                    unrouted.push(task);
                } else {
                    queue.push_back(task);
                }
            }
        }
        queues.retain(|_, q| !q.is_empty());
        unrouted
    }

    async fn is_empty(&self) -> bool {
        let queues = self.queues.lock();
        queues.values().all(|q| q.is_empty())
    }

    async fn len(&self) -> usize {
        let queues = self.queues.lock();
        queues.values().map(|q| q.len()).sum()
    }

    fn total_queued(&self) -> usize {
        self.queues.lock().values().map(|q| q.len()).sum()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl PriorityScheduler {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(Mutex::new(BTreeMap::new())),
            notify: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn into_scheduler(self) -> Arc<dyn Scheduler> {
        Arc::new(self)
    }

    pub async fn notify_waiters(&self) {
        self.notify.notify_one()
    }
}

impl Default for PriorityScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple FIFO scheduler — tasks are dequeued in arrival order,
/// ignoring priority. Useful when all tasks have equal importance.
#[derive(Clone)]
pub struct FifoScheduler {
    queue: Arc<Mutex<VecDeque<TaskDefinition>>>,
    notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl Scheduler for FifoScheduler {
    async fn enqueue(&self, mut task: TaskDefinition) -> Result<(), ActantError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ActantError::InvalidState(
                "scheduler is closed (drain mode), rejecting task".into(),
            ));
        }
        task.enqueued_at_ms = crate::common::epoch_millis();
        self.queue.lock().push_back(task);
        self.notify.notify_one();
        Ok(())
    }

    async fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> Result<(), ActantError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ActantError::InvalidState(
                "scheduler is closed (drain mode), rejecting tasks".into(),
            ));
        }
        if tasks.is_empty() {
            return Ok(());
        }
        let now_ms = crate::common::epoch_millis();
        let mut queue = self.queue.lock();
        for mut task in tasks {
            task.enqueued_at_ms = now_ms;
            queue.push_back(task);
        }
        drop(queue);
        self.notify.notify_one();
        Ok(())
    }

    async fn dequeue(&self) -> Option<TaskDefinition> {
        loop {
            let task = self.queue.lock().pop_front();
            if task.is_some() {
                return task;
            }
            if self.closed.load(Ordering::Acquire) {
                self.notify.notify_waiters();
                return None;
            }
            self.notify.notified().await;
        }
    }

    async fn try_dequeue(&self) -> Option<TaskDefinition> {
        self.queue.lock().pop_front()
    }

    async fn dequeue_batch(&self, limit: usize) -> Vec<TaskDefinition> {
        let mut queue = self.queue.lock();
        let count = limit.min(queue.len());
        (0..count).filter_map(|_| queue.pop_front()).collect()
    }

    async fn drain_unrouted(&self) -> Vec<TaskDefinition> {
        let mut queue = self.queue.lock();
        let old = std::mem::take(&mut *queue);
        let (unrouted, routed): (Vec<_>, Vec<_>) =
            old.into_iter().partition(|t| t.target_node.is_none());
        *queue = routed.into();
        unrouted
    }

    async fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }

    async fn len(&self) -> usize {
        self.queue.lock().len()
    }

    fn total_queued(&self) -> usize {
        self.queue.lock().len()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl FifoScheduler {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            notify: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn into_scheduler(self) -> Arc<dyn Scheduler> {
        Arc::new(self)
    }
}

impl Default for FifoScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory function type for creating scheduler instances.
pub type SchedulerFactory = Arc<dyn Fn() -> Arc<dyn Scheduler> + Send + Sync>;

/// Global registry mapping scheduler kind names to factory functions.
///
/// New scheduler strategies can be registered at startup via
/// [`register_scheduler`], allowing the Python layer to introduce
/// custom scheduling policies without touching Rust core code.
fn registry() -> &'static parking_lot::RwLock<std::collections::HashMap<String, SchedulerFactory>> {
    static REGISTRY: OnceLock<
        parking_lot::RwLock<std::collections::HashMap<String, SchedulerFactory>>,
    > = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut map = std::collections::HashMap::new();
        map.insert(
            scheduler_kind::PRIORITY.to_string(),
            Arc::new(|| PriorityScheduler::new().into_scheduler()) as SchedulerFactory,
        );
        map.insert(
            scheduler_kind::FIFO.to_string(),
            Arc::new(|| FifoScheduler::new().into_scheduler()) as SchedulerFactory,
        );
        parking_lot::RwLock::new(map)
    })
}

/// Register a custom scheduler strategy under the given name.
///
/// If a strategy with the same name already exists, it is replaced.
/// This allows the Python layer (or any embedding application) to
/// introduce new scheduling policies without modifying Rust core.
#[allow(dead_code)] // Public extension point — used by external/embedding callers
pub fn register_scheduler(name: impl Into<String>, factory: SchedulerFactory) {
    let mut guard = registry().write();
    guard.insert(name.into(), factory);
}

/// Returns `true` if a scheduler strategy is registered under `name`.
///
/// Used by [`crate::common::SchedulerKind::validate`] to reject unknown
/// scheduler names at startup instead of silently falling back.
pub fn is_registered(name: &str) -> bool {
    registry().read().contains_key(name)
}

/// Returns the sorted list of registered scheduler names.
///
/// Used in configuration error messages to enumerate valid options.
pub fn registered_names() -> Vec<String> {
    let mut names: Vec<String> = registry().read().keys().cloned().collect();
    names.sort();
    names
}

/// Create a scheduler based on the configured kind string.
///
/// Looks up the name in the global registry. Returns a
/// [`crate::common::ActantError::Config`] if the name is not registered —
/// no silent fallback. Callers that receive config values from
/// untrusted sources should validate them first via
/// [`crate::common::SchedulerKind::validate`].
pub fn create_scheduler(kind: &str) -> Result<Arc<dyn Scheduler>, crate::common::ActantError> {
    let guard = registry().read();
    if let Some(factory) = guard.get(kind) {
        Ok(factory())
    } else {
        Err(crate::common::ActantError::Config(format!(
            "unknown scheduler kind '{}': expected one of {}",
            kind,
            registered_names().join(", ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{NodeId, TaskId};

    fn make_task(name: &str, priority: i32) -> TaskDefinition {
        TaskDefinition {
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
        }
    }

    fn make_task_with_target(name: &str, priority: i32, target: &str) -> TaskDefinition {
        let mut t = make_task(name, priority);
        t.target_node = Some(NodeId(target.to_string()));
        t
    }

    // ---- PriorityScheduler tests ----

    #[tokio::test]
    async fn priority_enqueue_dequeue_returns_highest_first() {
        let sched = PriorityScheduler::new();
        sched.enqueue(make_task("low", -10)).await.unwrap();
        sched.enqueue(make_task("high", 10)).await.unwrap();
        sched.enqueue(make_task("normal", 0)).await.unwrap();

        let first = sched.try_dequeue().await.unwrap();
        assert_eq!(first.name, "high");
        let second = sched.try_dequeue().await.unwrap();
        assert_eq!(second.name, "normal");
        let third = sched.try_dequeue().await.unwrap();
        assert_eq!(third.name, "low");
    }

    #[tokio::test]
    async fn priority_fifo_within_same_level() {
        let sched = PriorityScheduler::new();
        sched.enqueue(make_task("a", 0)).await.unwrap();
        sched.enqueue(make_task("b", 0)).await.unwrap();
        sched.enqueue(make_task("c", 0)).await.unwrap();

        assert_eq!(sched.try_dequeue().await.unwrap().name, "a");
        assert_eq!(sched.try_dequeue().await.unwrap().name, "b");
        assert_eq!(sched.try_dequeue().await.unwrap().name, "c");
    }

    #[tokio::test]
    async fn priority_enqueue_batch_preserves_ordering() {
        let sched = PriorityScheduler::new();
        let tasks = vec![
            make_task("low", -5),
            make_task("high", 5),
            make_task("mid", 0),
        ];
        sched.enqueue_batch(tasks).await.unwrap();

        assert_eq!(sched.try_dequeue().await.unwrap().name, "high");
        assert_eq!(sched.try_dequeue().await.unwrap().name, "mid");
        assert_eq!(sched.try_dequeue().await.unwrap().name, "low");
    }

    #[tokio::test]
    async fn priority_enqueue_batch_empty_is_noop() {
        let sched = PriorityScheduler::new();
        sched.enqueue_batch(vec![]).await.unwrap();
        assert!(sched.is_empty().await);
    }

    #[tokio::test]
    async fn priority_dequeue_batch_respects_limit_and_priority() {
        let sched = PriorityScheduler::new();
        sched.enqueue(make_task("low", -10)).await.unwrap();
        sched.enqueue(make_task("high1", 10)).await.unwrap();
        sched.enqueue(make_task("high2", 10)).await.unwrap();
        sched.enqueue(make_task("mid", 0)).await.unwrap();

        let batch = sched.dequeue_batch(3).await;
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].name, "high1");
        assert_eq!(batch[1].name, "high2");
        assert_eq!(batch[2].name, "mid");
        assert_eq!(sched.total_queued(), 1);
    }

    #[tokio::test]
    async fn priority_dequeue_batch_returns_empty_when_idle() {
        let sched = PriorityScheduler::new();
        let batch = sched.dequeue_batch(5).await;
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn priority_drain_unrouted_keeps_routed() {
        let sched = PriorityScheduler::new();
        sched.enqueue(make_task("unrouted1", 0)).await.unwrap();
        sched
            .enqueue(make_task_with_target("routed1", 0, "node-a"))
            .await
            .unwrap();
        sched.enqueue(make_task("unrouted2", 5)).await.unwrap();
        sched
            .enqueue(make_task_with_target("routed2", 5, "node-b"))
            .await
            .unwrap();

        let drained = sched.drain_unrouted().await;
        assert_eq!(drained.len(), 2);
        let remaining = sched.total_queued();
        assert_eq!(remaining, 2);
    }

    #[tokio::test]
    async fn priority_is_empty_and_len_track_state() {
        let sched = PriorityScheduler::new();
        assert!(sched.is_empty().await);
        assert_eq!(sched.len().await, 0);

        sched.enqueue(make_task("t1", 0)).await.unwrap();
        sched.enqueue(make_task("t2", 0)).await.unwrap();
        assert!(!sched.is_empty().await);
        assert_eq!(sched.len().await, 2);
        assert_eq!(sched.total_queued(), 2);
    }

    #[tokio::test]
    async fn priority_close_rejects_enqueue_and_returns_none() {
        let sched = PriorityScheduler::new();
        sched.close();
        assert!(sched.is_closed());

        let err = sched.enqueue(make_task("t1", 0)).await.unwrap_err();
        assert!(matches!(err, ActantError::InvalidState(_)));

        // dequeue on closed+empty returns None
        assert!(sched.dequeue().await.is_none());
    }

    #[tokio::test]
    async fn priority_close_rejects_enqueue_batch() {
        let sched = PriorityScheduler::new();
        sched.close();
        let err = sched
            .enqueue_batch(vec![make_task("t1", 0)])
            .await
            .unwrap_err();
        assert!(matches!(err, ActantError::InvalidState(_)));
    }

    #[tokio::test]
    async fn priority_try_dequeue_returns_none_when_empty() {
        let sched = PriorityScheduler::new();
        assert!(sched.try_dequeue().await.is_none());
    }

    // ---- FifoScheduler tests ----

    #[tokio::test]
    async fn fifo_enqueue_dequeue_preserves_arrival_order() {
        let sched = FifoScheduler::new();
        sched.enqueue(make_task("first", -100)).await.unwrap();
        sched.enqueue(make_task("second", 100)).await.unwrap();
        sched.enqueue(make_task("third", 0)).await.unwrap();

        assert_eq!(sched.try_dequeue().await.unwrap().name, "first");
        assert_eq!(sched.try_dequeue().await.unwrap().name, "second");
        assert_eq!(sched.try_dequeue().await.unwrap().name, "third");
    }

    #[tokio::test]
    async fn fifo_enqueue_batch_preserves_order() {
        let sched = FifoScheduler::new();
        let tasks = vec![make_task("a", 10), make_task("b", -10), make_task("c", 0)];
        sched.enqueue_batch(tasks).await.unwrap();

        assert_eq!(sched.try_dequeue().await.unwrap().name, "a");
        assert_eq!(sched.try_dequeue().await.unwrap().name, "b");
        assert_eq!(sched.try_dequeue().await.unwrap().name, "c");
    }

    #[tokio::test]
    async fn fifo_dequeue_batch_respects_limit() {
        let sched = FifoScheduler::new();
        for i in 0..5 {
            sched.enqueue(make_task(&format!("t{i}"), 0)).await.unwrap();
        }
        let batch = sched.dequeue_batch(3).await;
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].name, "t0");
        assert_eq!(batch[2].name, "t2");
        assert_eq!(sched.total_queued(), 2);
    }

    #[tokio::test]
    async fn fifo_drain_unrouted_keeps_routed() {
        let sched = FifoScheduler::new();
        sched.enqueue(make_task("unrouted", 0)).await.unwrap();
        sched
            .enqueue(make_task_with_target("routed", 0, "node-a"))
            .await
            .unwrap();

        let drained = sched.drain_unrouted().await;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].name, "unrouted");
        assert_eq!(sched.total_queued(), 1);
    }

    #[tokio::test]
    async fn fifo_close_rejects_enqueue() {
        let sched = FifoScheduler::new();
        sched.close();
        assert!(sched.is_closed());
        let err = sched.enqueue(make_task("t1", 0)).await.unwrap_err();
        assert!(matches!(err, ActantError::InvalidState(_)));
        assert!(sched.dequeue().await.is_none());
    }

    #[tokio::test]
    async fn fifo_is_empty_and_len() {
        let sched = FifoScheduler::new();
        assert!(sched.is_empty().await);
        sched.enqueue(make_task("t1", 0)).await.unwrap();
        assert!(!sched.is_empty().await);
        assert_eq!(sched.len().await, 1);
    }

    // ---- Factory / registry tests ----

    #[test]
    fn create_scheduler_priority_is_registered() {
        let s = create_scheduler(scheduler_kind::PRIORITY).unwrap();
        assert_eq!(s.total_queued(), 0);
    }

    #[test]
    fn create_scheduler_fifo_is_registered() {
        let s = create_scheduler(scheduler_kind::FIFO).unwrap();
        assert_eq!(s.total_queued(), 0);
    }

    #[test]
    fn create_scheduler_unknown_kind_returns_config_error() {
        match create_scheduler("nonexistent") {
            Err(ActantError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected error for unknown scheduler kind"),
        }
    }

    #[test]
    fn is_registered_recognizes_builtin_kinds() {
        assert!(is_registered(scheduler_kind::PRIORITY));
        assert!(is_registered(scheduler_kind::FIFO));
        assert!(!is_registered("nonexistent"));
    }

    #[test]
    fn registered_names_includes_builtins() {
        let names = registered_names();
        assert!(names.contains(&scheduler_kind::PRIORITY.to_string()));
        assert!(names.contains(&scheduler_kind::FIFO.to_string()));
    }

    #[tokio::test]
    async fn priority_dequeue_blocks_until_task_arrives() {
        let sched = PriorityScheduler::new();
        let sched_clone = sched.clone();
        let handle = tokio::spawn(async move { sched_clone.dequeue().await });
        // Give the dequeue task time to start waiting
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        sched.enqueue(make_task("delayed", 0)).await.unwrap();

        let task = handle.await.unwrap().unwrap();
        assert_eq!(task.name, "delayed");
    }
}
