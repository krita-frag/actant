//! Worker 执行运行时。
//!
//! [`Worker`] 是每个节点上的任务执行循环：它从 [`Scheduler`] 拉取
//! [`TaskDefinition`]，决定本地执行还是转发远端，
//! 然后通过 [`TaskDispatcher`]
//! 调用实际 handler。Worker 不理解任务 payload 的业务语义，只处理签名后的字节、
//! 超时、取消、并发槽位和结果投递。
//!
//! ## 主循环结构
//!
//! 1. 启动网络事件循环，订阅 task / heartbeat / DAG state / cancel 等 topic。
//! 2. 启动远端结果重试循环和取消 TTL 清理循环。
//! 3. 订阅 `Topic::TaskEnqueued` 事件，SchedulerActor 在 `enqueue` 后发布事件
//!    唤醒 Worker 拉取任务。
//! 4. 为每个任务获取 semaphore 槽位，注册 cancel flag，执行或转发任务。
//! 5. 将任务结果投递回 origin 节点或本地 WorkflowActor。
//!
//! ## 关闭语义
//!
//! [`Worker::shutdown`] 只发送取消信号；真正的 drain、任务线程池关闭和 iroh
//! endpoint 关闭由 [`crate::runtime::Runtime::shutdown`] 编排。这样可以保证
//! Actor、dispatcher 和网络按固定顺序退出，避免 Drop 阶段互相等待。
//!
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::{
    ActantError, NodeId, Result, TaskCompletion, TaskDefinition, Topic, WorkerConfig, WorkflowId,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::dispatcher::{new_cancel_flag, CancelFlag, TaskDispatcher};
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::network::Transport;
use crate::runtime::workflow::Scheduler;

mod cancel;
mod network_router;
mod result_delivery;

use network_router::{NetworkEventRouter, NetworkEventRouterConfig};
use result_delivery::{start_pending_result_loop, try_enqueue_pending_result, PendingResult};

#[cfg(test)]
use crate::common::TaskId;
#[cfg(test)]
use crate::runtime::network::DirectResponseChannel;

/// 事件驱动的任务拉取。
///
/// 先 ``try_dequeue`` 拉取已入队任务；无任务时在 EventBus 的
/// ``TaskEnqueued`` 订阅上 await，由 SchedulerActor 在 ``enqueue`` 后
/// 发布事件唤醒。无 sleep、无固定延迟、无 CPU 浪费。
///
/// # 为什么不直接调用 ``dequeue()``
///
/// ``dequeue()`` 会向 SchedulerActor 发送 ``DEQUEUE`` 消息，而
/// SchedulerActor 的 ``handle_message`` 会调用 ``inner.dequeue().await``，
/// 该方法在队列为空时阻塞。由于 Actor 单线程顺序处理消息，阻塞的
/// DEQUEUE 会阻止后续 ENQUEUE 被处理，形成死锁。
/// 通过 EventBus 事件 + 非阻塞 ``try_dequeue`` 组合，Actor 每条消息
/// 都能立即返回，Worker 也能在任务入队时立即被唤醒。
///
/// # 事件丢失安全性
///
/// ``TaskEnqueued`` 是 ``BestEffort`` 投递——通道满时丢弃。这不会导致
/// 任务丢失：Worker 每次被唤醒后会循环 ``try_dequeue`` 直到队列空，
/// 丢弃事件仅意味着"少唤醒一次"，下一次事件或下一次循环仍会拉取到任务。
async fn wait_for_task(
    scheduler: &Arc<dyn Scheduler>,
    event_rx: &mut tokio::sync::mpsc::Receiver<BusEvent>,
) -> Option<TaskDefinition> {
    loop {
        if let Some(task) = scheduler.try_dequeue().await {
            return Some(task);
        }
        // 无任务时等待 EventBus 事件。事件由 SchedulerActor 在
        // enqueue/enqueue_batch/close 后发布。recv() 返回 None 表示
        // EventBus 已 drop（仅在 Runtime shutdown 时发生）——
        // 此时 Worker 的 cancel 信号也会触发，select! 会取 cancel 分支。
        event_rx.recv().await?;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkerState {
    Running,
    Draining,
    Stopped,
}

impl WorkerState {
    /// Stable string representation used across the PyO3 boundary.
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerState::Running => "healthy",
            WorkerState::Draining => "draining",
            WorkerState::Stopped => "stopped",
        }
    }
}

/// 单节点任务执行器。
///
/// Worker 同时处理本地任务执行和跨节点任务转发。它持有 scheduler、dispatcher、
/// 网络传输和运行中任务取消表，是 Python `@task.submit()` 进入 Rust 运行时后的
/// 主要执行面。Worker 自身不拥有 workflow 状态；workflow 推进由
/// `WorkflowActor`/`Orchestrator` 负责。
pub struct Worker {
    node_id: NodeId,
    network: Arc<dyn Transport>,
    scheduler: Arc<dyn Scheduler>,
    /// SchedulerActor 的 id，用于 shutdown 时同步等待 Actor 退出。
    /// `None` 表示 Worker 未绑定 SchedulerActor（仅用于不启动 Actor 系统的测试路径）。
    scheduler_actor_id: Option<crate::common::ActorId>,
    event_bus: EventBus,
    task_dispatcher: Arc<dyn TaskDispatcher>,
    actor_system: Option<Arc<ActorSystem>>,
    /// 本地 WorkflowActor 的 id。用于将远程 task result 路由到编排器。
    workflow_actor_id: Option<crate::common::ActorId>,
    /// 本地 DagGossipActor 的 id。用于处理工作流状态请求/响应主题。
    dag_gossip_actor_id: Option<crate::common::ActorId>,
    /// 远端 peer 容量视图，用于 unrouted task 的自动路由。
    failover: Option<Arc<crate::runtime::workflow::FailoverManager>>,
    /// 最大并发任务数。用 AtomicUsize 支持运行时扩容（`set_max_concurrent_tasks`）。
    max_concurrent_tasks: Arc<std::sync::atomic::AtomicUsize>,
    task_timeout: Duration,
    cancel: Arc<tokio::sync::watch::Sender<bool>>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    drain_timeout: Duration,
    running_tasks: Arc<tokio::sync::Semaphore>,
    state: Arc<tokio::sync::watch::Sender<WorkerState>>,
    broadcast_retry_attempts: usize,
    broadcast_retry_base_delay: Duration,
    remote_fallback_delay: Duration,
    pending_result_channel_capacity: usize,
    /// Callback invoked when running task count changes: (available, max).
    capacity_callback: Option<Arc<dyn Fn(u32, u32) + Send + Sync>>,
    /// Handle to the tokio runtime — used by publish_lifecycle to spawn
    /// tasks even when called from a non-tokio thread (e.g. Python).
    tokio_handle: tokio::runtime::Handle,
    /// 运行中任务的 cancel_flag 注册表：task_id → CancelFlag。
    /// Python 层调用 cancel_task(task_id) 时，通过此表找到对应的 cancel_flag 并置为 true。
    cancel_flags: Arc<parking_lot::Mutex<std::collections::HashMap<String, CancelFlag>>>,
    /// 通过远端取消广播标记的 task_id 集合（带 TTL 时间戳）。
    /// 这只承载“跨节点取消”的可见性，不负责本地 Python 预取消状态。
    /// key: task_id, value: 取消请求注册时间。
    /// 超过 ``CANCELLED_TASKS_TTL`` 的条目由后台清理任务定期移除。
    cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
    /// Worker 就绪信号：``run()`` 完成初始化进入任务执行循环前设为 true。
    /// ``wait_for_ready`` 据此阻塞，事件驱动等待 Worker 就绪。
    /// 用 ``watch`` 而非 ``Notify`` 是因为 watch 的状态是持久的——
    /// 即使 ``wait_for_ready`` 在 ``run()`` 设值后才调用也能立即返回，
    /// 避免 notify 先于 wait 导致的永久阻塞。
    ready: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Worker {
    /// Clear the capacity callback, releasing any captured references.
    pub fn clear_capacity_callback(&mut self) {
        self.capacity_callback = None;
    }

    /// 构造一个未绑定 ActorSystem 的 Worker。
    ///
    /// Builder 随后会通过 `with_*` 方法注入 SchedulerActor id、WorkflowActor id、
    /// FailoverManager 和容量回调。测试也可直接使用该构造器创建最小 Worker。
    pub fn new(
        node_id: NodeId,
        network: Arc<dyn Transport>,
        event_bus: EventBus,
        scheduler: Arc<dyn Scheduler>,
        task_dispatcher: Arc<dyn TaskDispatcher>,
        config: &WorkerConfig,
        tokio_handle: tokio::runtime::Handle,
    ) -> Self {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (state_tx, _) = tokio::sync::watch::channel(WorkerState::Running);
        let max_concurrent = config.max_concurrent_tasks;
        Self {
            node_id,
            network,
            scheduler,
            scheduler_actor_id: None,
            event_bus,
            task_dispatcher,
            actor_system: None,
            workflow_actor_id: None,
            dag_gossip_actor_id: None,
            failover: None,
            max_concurrent_tasks: Arc::new(std::sync::atomic::AtomicUsize::new(max_concurrent)),
            task_timeout: Duration::from_millis(config.default_task_timeout_ms),
            cancel: Arc::new(cancel_tx),
            cancel_rx,
            drain_timeout: Duration::from_secs(config.drain_timeout_secs),
            running_tasks: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            state: Arc::new(state_tx),
            broadcast_retry_attempts: config.broadcast_retry_attempts,
            broadcast_retry_base_delay: Duration::from_millis(config.broadcast_retry_base_delay_ms),
            remote_fallback_delay: Duration::from_millis(config.remote_fallback_delay_ms),
            pending_result_channel_capacity: config.pending_result_channel_capacity,
            capacity_callback: None,
            tokio_handle,
            cancel_flags: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            cancelled_tasks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            // `watch::channel` 返回 (Sender, Receiver)，仅保留 Sender；
            // 等待方通过 `subscribe()` 获取自己的 Receiver。
            ready: Arc::new(tokio::sync::watch::channel(false).0),
        }
    }

    /// 设置底层 SchedulerActor 的 id，供 `Runtime::shutdown` 同步等待 Actor 退出。
    pub fn with_scheduler_actor_id(mut self, id: crate::common::ActorId) -> Self {
        self.scheduler_actor_id = Some(id);
        self
    }

    /// 返回 SchedulerActor 的 id（若使用 Actor-backed scheduler）。
    pub fn scheduler_actor_id(&self) -> Option<&crate::common::ActorId> {
        self.scheduler_actor_id.as_ref()
    }

    pub fn with_max_concurrent_tasks(mut self, max: usize) -> Self {
        self.max_concurrent_tasks = Arc::new(std::sync::atomic::AtomicUsize::new(max));
        self.running_tasks = Arc::new(tokio::sync::Semaphore::new(max));
        self
    }

    /// 运行时调整最大并发任务数。
    ///
    /// **仅支持扩容**（new_max > 当前值）：通过 `Semaphore::add_permits` 增加可用槽位。
    /// 缩容（new_max < 当前值）不受支持——Tokio `Semaphore` 不支持减少 permits，
    /// 已运行的在途任务会继续执行直到完成。若需要缩容，建议重启 Worker。
    ///
    /// 调用后立即生效，后续任务可使用新增的槽位。`capacity_callback`（若设置）
    /// 会被触发以通知 failover 层新的容量。
    pub fn set_max_concurrent_tasks(&self, new_max: usize) {
        use std::sync::atomic::Ordering;
        let current = self.max_concurrent_tasks.load(Ordering::Acquire);
        if new_max <= current {
            tracing::warn!(
                new_max,
                current_max = current,
                "set_max_concurrent_tasks: shrink not supported, ignoring"
            );
            return;
        }
        let diff = new_max - current;
        self.running_tasks.add_permits(diff);
        self.max_concurrent_tasks.store(new_max, Ordering::Release);
        tracing::info!(
            added = diff,
            new_max,
            "max_concurrent_tasks expanded via add_permits"
        );
        self.notify_capacity();
    }

    pub fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.task_timeout = timeout;
        self
    }

    pub fn with_drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    pub(crate) fn with_actor_system(mut self, system: Arc<ActorSystem>) -> Self {
        self.actor_system = Some(system);
        self
    }

    pub(crate) fn with_workflow_actor_id(mut self, id: crate::common::ActorId) -> Self {
        self.workflow_actor_id = Some(id);
        self
    }

    pub(crate) fn with_dag_gossip_actor_id(mut self, id: crate::common::ActorId) -> Self {
        self.dag_gossip_actor_id = Some(id);
        self
    }

    pub(crate) fn with_failover_manager(
        mut self,
        manager: Arc<crate::runtime::workflow::FailoverManager>,
    ) -> Self {
        self.failover = Some(manager);
        self
    }

    pub fn with_capacity_callback(mut self, cb: Arc<dyn Fn(u32, u32) + Send + Sync>) -> Self {
        self.capacity_callback = Some(cb);
        self
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn network(&self) -> &Arc<dyn Transport> {
        &self.network
    }

    pub fn scheduler(&self) -> &dyn Scheduler {
        &*self.scheduler
    }

    /// 返回 scheduler 的 `Arc` 克隆，供其他子系统（如 FailoverManager）共享调度入口。
    pub fn scheduler_clone(&self) -> Arc<dyn Scheduler> {
        self.scheduler.clone()
    }

    /// 返回 task dispatcher 的 Arc 克隆，供外部注册 handler。
    pub fn task_dispatcher(&self) -> &Arc<dyn TaskDispatcher> {
        &self.task_dispatcher
    }

    /// 取消运行中的任务：将对应的 `cancel_flag` 置为 true。
    ///
    /// 这是低层运行时取消入口，仅作用于已进入执行中的任务。
    /// 排队中的任务由 Python 侧预取消状态拦截，避免让 Rust Worker 维护
    /// 两套并行的“待执行取消”语义。
    ///
    /// 返回 `true` 表示找到了运行中的任务并成功提交取消请求；
    /// 返回 `false` 表示任务不存在或尚未进入运行态。
    pub fn cancel_task(&self, task_id: &str) -> bool {
        let flags = self.cancel_flags.lock();
        if let Some(flag) = flags.get(task_id) {
            flag.store(true, std::sync::atomic::Ordering::Release);
            crate::metrics::inc_tasks_cancelled();
            true
        } else {
            false
        }
    }

    /// 返回当前 Worker 状态。
    pub fn state(&self) -> WorkerState {
        *self.state.borrow()
    }

    /// 订阅 Worker 状态变化。
    ///
    /// Runtime shutdown 使用该 watch channel 等待 Worker 从 `Draining` 进入
    /// `Stopped`，避免在任务仍在投递结果时关闭网络。
    pub fn subscribe_state(&self) -> tokio::sync::watch::Receiver<WorkerState> {
        self.state.subscribe()
    }

    /// 将任务标记为目标节点并入队。
    ///
    /// # Errors
    ///
    /// 如果底层 scheduler 已关闭或无法接收任务，返回错误。
    pub async fn schedule_task(&self, mut task: TaskDefinition, target_node: NodeId) -> Result<()> {
        task.target_node = Some(target_node);
        self.scheduler.enqueue(task).await?;
        Ok(())
    }

    /// 查询当前网络视图中的 peer 节点。
    ///
    /// # Errors
    ///
    /// 如果底层传输查询失败，返回网络错误。
    pub async fn discover_peers(&self) -> Result<Vec<NodeId>> {
        let peers = self.network.discover_peers().await?;
        Ok(peers.into_iter().map(|p| NodeId::from(p.0)).collect())
    }

    /// 运行 Worker 主循环直到收到 shutdown 信号。
    ///
    /// 主循环负责网络订阅、远端取消清理、任务出队、并发槽位控制、本地执行、
    /// 远端转发和结果投递。该函数通常由 PyO3 `serve()` 或 runtime 后台任务调用。
    ///
    /// # Errors
    ///
    /// 当前实现会尽量把单任务错误转换为 task lifecycle 事件并继续运行；
    /// 只有主循环初始化或不可恢复的运行时错误才会返回 `Err`。
    pub async fn run(&self) -> Result<()> {
        // P2P 订阅在后台执行，不阻塞本地任务执行循环。
        // 网络订阅可能因 P2P 发现延迟而耗时；任务执行不应等待它。
        self.spawn_subscribe_topics();
        self.notify_capacity();
        self.start_network_event_loop();
        self.start_cancelled_tasks_cleanup_loop();
        let pending_tx = self.start_pending_result_loop();
        // 进入任务执行循环前发送就绪信号：``serve()`` 据此返回。
        // ``send_replace`` 写入持久状态，即使等待者晚于 ``run()`` 启动
        // 也能立即观察到 true。
        let _ = self.ready.send_replace(true);
        self.run_task_execution_loop(&pending_tx).await
    }

    /// 阻塞当前线程直到 Worker 进入任务执行循环（``run()`` 完成初始化）。
    ///
    /// 由 PyO3 ``serve()`` 在 tokio runtime 上 ``block_on`` 调用，
    /// 事件驱动：基于 ``watch`` channel 的状态变更，无轮询、无固定延迟。
    /// 若 ``run()`` 因 spawn 失败或早退未发送就绪信号，
    /// ``watch::Sender`` 在 Worker drop 时被释放，``changed()`` 返回 Err，
    /// 等待立即返回——不会永久阻塞。
    pub async fn wait_for_ready(&self) -> Result<()> {
        let mut rx = self.ready.subscribe();
        if *rx.borrow() {
            return Ok(());
        }
        // `changed()` 在 Sender 被关闭时返回 Err，自然解除阻塞。
        let _ = rx.changed().await;
        Ok(())
    }

    /// 在 tokio 后台 spawn P2P topic 订阅。非阻塞。
    fn spawn_subscribe_topics(&self) {
        let network = self.network.clone();
        let node_id = self.node_id.clone();
        let mut cancel = self.cancel_rx.clone();
        let tokio_handle = self.tokio_handle.clone();
        tokio_handle.spawn(async move {
            let topics = [
                Topic::task(&node_id),
                Topic::actor(&node_id),
                Topic::actor_reply(&node_id),
                Topic::workflow_state_req(&node_id),
                Topic::workflow_state_resp(&node_id),
                Topic::from(crate::common::wire::constants::TOPIC_CANCEL),
            ];
            for topic in &topics {
                tracing::info!("subscribing to topic: {}", topic);
                tokio::select! {
                    _ = cancel.changed() => {
                        tracing::info!("subscribe_topics cancelled during shutdown");
                        return;
                    }
                    r = network.subscribe(topic.as_str()) => {
                        if let Err(e) = r {
                            tracing::warn!(error = %e, "subscribe to topic {} failed", topic);
                        }
                    }
                }
            }
            tracing::info!("subscribe_topics completed");
        });
    }

    fn start_network_event_loop(&self) {
        let router = NetworkEventRouter::new(NetworkEventRouterConfig {
            network: self.network.clone(),
            event_bus: self.event_bus.clone(),
            scheduler: self.scheduler.clone(),
            actor_system: self.actor_system.clone(),
            workflow_actor_id: self.workflow_actor_id.clone(),
            dag_gossip_actor_id: self.dag_gossip_actor_id.clone(),
            cancel_flags: self.cancel_flags.clone(),
            cancelled_tasks: self.cancelled_tasks.clone(),
        });
        let network = self.network.clone();
        let mut event_cancel = self.cancel_rx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = event_cancel.changed() => {
                        tracing::info!("worker event loop shutting down");
                        break;
                    }
                    event = network.recv_event() => {
                        let Some(event) = event else {
                            continue;
                        };
                        router.handle(event).await;
                    }
                }
            }
        });
    }

    fn start_cancelled_tasks_cleanup_loop(&self) {
        cancel::spawn_cancelled_tasks_cleanup_loop(
            &self.tokio_handle,
            self.cancelled_tasks.clone(),
            self.cancel_rx.clone(),
        );
    }

    fn start_pending_result_loop(&self) -> tokio::sync::mpsc::Sender<PendingResult> {
        start_pending_result_loop(
            self.network.clone(),
            self.cancel_rx.clone(),
            self.broadcast_retry_attempts,
            self.broadcast_retry_base_delay,
            self.pending_result_channel_capacity,
        )
    }

    async fn run_task_execution_loop(
        &self,
        pending_tx: &tokio::sync::mpsc::Sender<PendingResult>,
    ) -> Result<()> {
        let node_id = self.node_id.clone();
        let network = self.network.clone();
        let task_dispatcher = self.task_dispatcher.clone();
        let semaphore = self.running_tasks.clone();
        let event_bus = self.event_bus.clone();
        let task_timeout = self.task_timeout;
        let mut task_cancel = self.cancel_rx.clone();
        let state_sender = self.state.clone();
        let drain_timeout = self.drain_timeout;
        let cancel_flags = self.cancel_flags.clone();
        let cancelled_tasks = self.cancelled_tasks.clone();

        // 订阅 TaskEnqueued 事件：SchedulerActor 在 enqueue 后发布事件，
        // Worker 在此事件上 await 唤醒。
        // 订阅在循环外创建，整个 Worker 生命周期复用同一 receiver。
        let mut task_enqueued_rx = self.event_bus.subscribe(crate::runtime::event_bus::Topic::TaskEnqueued);

        loop {
            if *self.state.subscribe().borrow() == WorkerState::Draining {
                // 关闭 scheduler，使新 task 无法入队。
                self.scheduler.close();
                // 排空剩余排队的 task — 由于 scheduler 已关闭，
                // 它们会被拒绝，因此直接丢弃。
                while self.scheduler.try_dequeue().await.is_some() {}
                // 通知子任务（network event loop、retry loop）退出。
                // 否则 rt.shutdown_timeout 会等待完整的 5s。
                let _ = self.cancel.send(true);
                self.publish_lifecycle(BusEvent::WorkerDrained {
                    node_id: self.node_id.clone(),
                });
                let _ = state_sender.send_replace(WorkerState::Stopped);
                return Ok(());
            }

            let task = tokio::select! {
                _ = task_cancel.changed() => {
                    let _ = state_sender.send_replace(WorkerState::Draining);
                    let running = self.running_task_count();
                    tracing::info!(
                        running_tasks = running,
                        drain_timeout_ms = drain_timeout.as_millis(),
                        "worker entering drain mode, waiting for running tasks"
                    );

                    let drain_result = tokio::time::timeout(
                        drain_timeout,
                        async {
                            let sem = semaphore.clone();
                            let permit = sem.acquire_many(self.max_concurrent_tasks.load(std::sync::atomic::Ordering::Acquire) as u32).await;
                            drop(permit);
                        },
                    ).await;

                    if drain_result.is_err() {
                        tracing::warn!("drain timeout exceeded, forcing shutdown");
                    } else {
                        tracing::info!("all running tasks completed, shutting down");
                    }

                    self.publish_lifecycle(BusEvent::WorkerDrained {
                        node_id: self.node_id.clone(),
                    });
                    let _ = state_sender.send_replace(WorkerState::Stopped);
                    return Ok(());
                }
                task = wait_for_task(&self.scheduler, &mut task_enqueued_rx) => task,
            };
            let Some(task) = task else {
                continue;
            };

            let mut task = task;
            if task.target_node.is_none() {
                if let Some((target_node, target_endpoint_addr)) = self.select_remote_target() {
                    tracing::debug!(
                        task_id = %task.id.as_str(),
                        target_node = %target_node.as_str(),
                        "routing unrouted task to remote peer"
                    );
                    task.target_node = Some(target_node);
                    task.target_endpoint_addr = Some(target_endpoint_addr);
                }
            }

            if cancelled_tasks.lock().contains_key(task.id.as_str()) {
                tracing::info!(
                    task_id = %task.id.as_str(),
                    "skipping cancelled task before execution"
                );
                // 记录 cancel latency：从取消注册到任务被跳过的耗时。
                {
                    let registry = cancelled_tasks.lock();
                    if let Some(ts) = registry.get(task.id.as_str()) {
                        let latency_ms = ts.elapsed().as_millis() as u64;
                        crate::metrics::observe_cancel_latency_ms(latency_ms);
                    }
                }
                let completion = TaskCompletion::Cancelled {
                    workflow_id: task
                        .workflow_id
                        .clone()
                        .unwrap_or_else(|| WorkflowId::from(String::new())),
                    task_id: task.id.clone(),
                    task_name: task.name.clone(),
                    target_node: task.target_node.clone(),
                };
                event_bus.publish(BusEvent::TaskCancelled(completion)).await;
                cancelled_tasks.lock().remove(task.id.as_str());
                crate::metrics::dec_cancelled_tasks_pending();
                continue;
            }

            if let Some(ref target) = task.target_node {
                if target != &node_id {
                    match self.forward_remote_task(&task, target).await {
                        Ok(()) => {
                            crate::metrics::inc_task_forward_succeeded();
                            continue;
                        }
                        Err(e) => {
                            crate::metrics::inc_task_forward_failed();
                            crate::metrics::inc_task_forward_reroute();
                            tracing::warn!(
                                "forward task {} to node {} failed ({}), re-enqueueing for re-routing",
                                task.id.as_str(),
                                target.as_str(),
                                e
                            );
                            let mut rerouted_task = task;
                            rerouted_task.target_node = None;
                            if let Err(e) = self.scheduler.enqueue(rerouted_task).await {
                                tracing::warn!("failed to re-enqueue task for re-routing: {}", e);
                            }
                            // 退避阻塞主循环，避免转发失败时 busy-loop。
                            tokio::time::sleep(self.remote_fallback_delay).await;
                            continue;
                        }
                    }
                }
            }

            if self
                .max_concurrent_tasks
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
                && task.target_node.is_none()
            {
                crate::metrics::inc_task_forward_fallback_local();
                tracing::debug!(
                    "submit-only node cannot execute task {} locally, re-enqueueing for re-routing",
                    task.id.as_str()
                );
                if let Err(e) = self.scheduler.enqueue(task).await {
                    tracing::warn!("failed to re-enqueue submit-only task: {}", e);
                }
                // 退避阻塞主循环，避免 submit-only 节点 busy-loop。
                tokio::time::sleep(self.remote_fallback_delay).await;
                continue;
            }

            let event_bus = event_bus.clone();
            let network = network.clone();
            let pending_results = pending_tx.clone();
            let pending_capacity = self.pending_result_channel_capacity;

            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ActantError::Internal("semaphore closed".into()))?;
            let cancel_flags = cancel_flags.clone();

            self.notify_capacity();

            let node_id_for_spawn = node_id.clone();
            let task_name = task.name.clone();
            let task_payload = task.payload.clone();
            let dispatcher = task_dispatcher.clone();
            let dispatch_start_ms = crate::common::epoch_millis();
            let capacity_cb = self.capacity_callback.clone();
            let max_concurrent = self
                .max_concurrent_tasks
                .load(std::sync::atomic::Ordering::Acquire);
            let semaphore_for_cb = semaphore.clone();
            crate::metrics::inc_running_tasks();

            // Publish TaskStarted to EventBus (所有任务，使 Python @task 也能收到 running 状态)
            {
                let wf_id = task
                    .workflow_id
                    .clone()
                    .unwrap_or_else(|| WorkflowId::from("".to_string()));
                event_bus
                    .publish(BusEvent::TaskStarted {
                        workflow_id: wf_id,
                        task_id: task.id.clone(),
                    })
                    .await;
            }

            if task.enqueued_at_ms > 0 {
                let latency = dispatch_start_ms.saturating_sub(task.enqueued_at_ms);
                crate::metrics::observe_scheduling_latency_ms(latency);
                tracing::trace!(task = ?task.id, latency_ms = latency, "scheduling latency");
            }

            let cancelled_tasks_for_spawn = cancelled_tasks.clone();

            tokio::spawn(async move {
                let _permit = permit;
                let task_id_dbg = task.id.clone();
                tracing::debug!(task = ?task_id_dbg, name = %task_name, "task executing");

                // Rust Worker 超时：timeout_ms 由 Python @task 装饰器传入。
                // 超时后设置 cancel_flag，dispatch handler 在协作检查点退出。
                // 超时结果为 TaskCompletion::Failed，Python 层映射为 ActantError。
                let effective_timeout = task
                    .timeout_ms
                    .map(std::time::Duration::from_millis)
                    .unwrap_or(task_timeout);

                let cancel_flag = new_cancel_flag();
                // 注册到 cancel_flags 表，使 Python cancel_task(task_id) 能找到此 flag。
                {
                    let mut flags = cancel_flags.lock();
                    flags.insert(task.id.to_string(), cancel_flag.clone());
                }
                // 任务完成后从注册表移除（无论成功/失败/取消）。
                let task_id_for_cleanup = task.id.clone();
                let cancel_flags_for_cleanup = cancel_flags.clone();
                let cancelled_tasks_for_cleanup = cancelled_tasks_for_spawn.clone();
                let cleanup = || {
                    let mut flags = cancel_flags_for_cleanup.lock();
                    flags.remove(&task_id_for_cleanup.to_string());
                    cancelled_tasks_for_cleanup
                        .lock()
                        .remove(task_id_for_cleanup.as_str());
                };

                tracing::debug!(task_id = ?task.id, "dispatching task");

                // 用 catch_unwind 包裹 dispatcher future，避免 spawn 句柄被丢弃后
                // panic 让 workflow 永久挂起。panic 时降级为 TaskCompletion::Failed。
                use futures::future::FutureExt as _;
                let dispatched = std::panic::AssertUnwindSafe(dispatcher.dispatch(
                    &task_name,
                    task_payload,
                    cancel_flag.clone(),
                ))
                .catch_unwind();

                let dispatch_result = tokio::time::timeout(effective_timeout, dispatched).await;

                if dispatch_result.is_err() {
                    tracing::warn!(task_id = ?task.id, "task timed out, signalling cancel");
                    cancel_flag.store(true, std::sync::atomic::Ordering::Release);
                }

                // 三种结果：Ok(Ok(Ok(output))) | Ok(Ok(Err(e))) | Ok(Err(panic)) | Err(timeout)
                let completion = build_completion_from_dispatch_result(
                    dispatch_result,
                    &task,
                    dispatch_start_ms,
                    effective_timeout,
                );

                publish_task_completion(
                    completion,
                    &task,
                    &node_id_for_spawn,
                    network.as_ref(),
                    &event_bus,
                    &pending_results,
                    pending_capacity,
                )
                .await;

                drop(_permit);

                // 任务完成：从 cancel_flags 注册表移除。
                cleanup();

                if let Some(ref cb) = capacity_cb {
                    let available = semaphore_for_cb.available_permits();
                    cb(available as u32, max_concurrent as u32);
                }
            });
        }
    }

    /// 请求 Worker 进入 drain 并停止主循环。
    ///
    /// 此方法只广播取消信号，不等待任务完成。等待顺序由
    /// [`crate::runtime::Runtime::shutdown`] 统一处理。
    pub fn shutdown(&self) {
        let _ = self.state.send_replace(WorkerState::Draining);
        if let Err(e) = self.cancel.send(true) {
            tracing::warn!("failed to send shutdown signal: {}", e);
        }
        // 关闭任务线程池并等待所有任务线程退出。
        // 确保在 Runtime 关闭前所有任务线程已退出，避免访问已释放的资源。
        self.task_dispatcher.shutdown();
    }

    /// 强制将 worker 状态设为 Stopped。
    ///
    /// 用于 `worker.run()` 异常退出（如 subscribe_topics 被 cancel）后，
    /// 确保 `Runtime::shutdown()` 中等待 `WorkerState::Stopped` 不会超时。
    pub fn notify_stopped(&self) {
        let _ = self.state.send_replace(WorkerState::Stopped);
    }

    pub fn drain(&self) {
        let _ = self.state.send_replace(WorkerState::Draining);
        self.scheduler.close();
        if let Some(ref cb) = self.capacity_callback {
            cb(0, 0);
        }
        self.publish_lifecycle(BusEvent::WorkerDraining {
            node_id: self.node_id.clone(),
        });
        tracing::info!("node {} entering drain mode", self.node_id.as_str());
    }

    /// Publish a worker lifecycle event asynchronously.
    ///
    /// Uses the stored tokio handle so this works even when called
    /// from a non-tokio thread (e.g. Python GIL thread).
    fn publish_lifecycle(&self, event: BusEvent) {
        let event_bus = self.event_bus.clone();
        self.tokio_handle.spawn(async move {
            event_bus.publish(event).await;
        });
    }

    pub fn running_task_count(&self) -> usize {
        self.max_concurrent_tasks
            .load(std::sync::atomic::Ordering::Acquire)
            .saturating_sub(self.running_tasks.available_permits())
    }

    pub fn max_concurrent_tasks(&self) -> usize {
        self.max_concurrent_tasks
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn notify_capacity(&self) {
        if let Some(ref cb) = self.capacity_callback {
            let available = self.running_tasks.available_permits();
            cb(
                available as u32,
                self.max_concurrent_tasks
                    .load(std::sync::atomic::Ordering::Acquire) as u32,
            );
        }
    }

    fn select_remote_target(&self) -> Option<(NodeId, String)> {
        let failover = self.failover.as_ref()?;
        let local_available = self.running_tasks.available_permits() as u32;
        // 心跳 TTL 过滤：排除心跳超时的 peer，避免将任务路由到
        // 已失联的节点。使用 failover 的 failure_timeout_ms 作为 TTL 阈值。
        let now_ms = crate::common::epoch_millis();
        let heartbeat_ttl_ms = failover.failure_timeout_ms();
        let mut peers: Vec<_> = failover
            .get_peer_infos()
            .into_iter()
            .filter(|(_, info)| {
                // available > 0 且 max > 0
                if info.available_slots == 0 || info.max_slots == 0 {
                    return false;
                }
                // 心跳 TTL：last_heartbeat_ms == 0 表示从未收到心跳，跳过；
                // 若距上次心跳超过 failure_timeout_ms，视为失联，跳过。
                if info.last_heartbeat_ms == 0 {
                    return false;
                }
                now_ms.saturating_sub(info.last_heartbeat_ms) <= heartbeat_ttl_ms
            })
            .collect();
        if peers.is_empty() {
            return None;
        }
        peers.sort_by(|(a_id, a_info), (b_id, b_info)| {
            b_info
                .available_slots
                .cmp(&a_info.available_slots)
                .then_with(|| b_info.max_slots.cmp(&a_info.max_slots))
                .then_with(|| a_id.as_str().cmp(b_id.as_str()))
        });
        let (node_id, info) = peers.into_iter().next()?;
        if info.available_slots <= local_available {
            return None;
        }
        Some((
            node_id.clone(),
            info.endpoint_addr
                .unwrap_or_else(|| node_id.as_str().to_string()),
        ))
    }

    async fn forward_remote_task(&self, task: &TaskDefinition, target: &NodeId) -> Result<()> {
        // 优先使用 target_endpoint_addr（iroh 公钥），否则回退到 node_id
        let target_addr = task
            .target_endpoint_addr
            .as_deref()
            .unwrap_or(target.as_str());
        let request = crate::runtime::network::DirectRequest::DispatchTask { task: task.clone() };
        match self.network.send_direct_request(target_addr, request).await {
            Ok(crate::runtime::network::DirectResponse::DispatchAck { accepted: true }) => Ok(()),
            Ok(crate::runtime::network::DirectResponse::DispatchAck { accepted: false }) => Err(
                ActantError::Network(format!("worker {} rejected task dispatch", target_addr)),
            ),
            Ok(_) => Err(ActantError::Network(format!(
                "unexpected response from worker {}",
                target_addr
            ))),
            Err(e) => Err(ActantError::Network(format!(
                "forward to node {} failed: {}",
                target_addr, e
            ))),
        }
    }
}

/// dispatcher.dispatch 的结果类型别名，避免在 spawn 与测试中长期写嵌套 Result。
type PanicPayload = Box<dyn std::any::Any + Send>;
type DispatchResult = std::result::Result<
    std::result::Result<std::result::Result<Vec<u8>, ActantError>, PanicPayload>,
    tokio::time::error::Elapsed,
>;

/// 将任务派发结果转换为 ``TaskCompletion``。
///
/// 抽出为纯函数以便覆盖四种结果分支（成功、失败、panic、超时），
/// 无需构造完整 Worker 与后台执行循环。
fn build_completion_from_dispatch_result(
    dispatch_result: DispatchResult,
    task: &TaskDefinition,
    dispatch_start_ms: u64,
    effective_timeout: Duration,
) -> TaskCompletion {
    let workflow_id = task
        .workflow_id
        .clone()
        .unwrap_or_else(|| WorkflowId::from("".to_string()));
    match dispatch_result {
        Ok(Ok(Ok(result))) => {
            crate::metrics::inc_tasks_completed();
            crate::metrics::dec_running_tasks();
            crate::metrics::observe_task_duration_ms(
                crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
            );
            TaskCompletion::Completed {
                workflow_id,
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                result,
                target_node: task.target_node.clone(),
            }
        }
        Ok(Ok(Err(e))) => {
            crate::metrics::inc_tasks_failed();
            crate::metrics::dec_running_tasks();
            crate::metrics::observe_task_duration_ms(
                crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
            );
            TaskCompletion::Failed {
                workflow_id,
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                error: e.to_string(),
                target_node: task.target_node.clone(),
            }
        }
        Ok(Err(panic_payload)) => {
            // dispatcher panic：提取 panic 消息，降级为 Failed，
            // 让 workflow 能继续推进而非永久挂起。
            crate::metrics::inc_tasks_failed();
            crate::metrics::dec_running_tasks();
            crate::metrics::observe_task_duration_ms(
                crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
            );
            let panic_msg = panic_payload
                .downcast_ref::<&'static str>()
                .copied()
                .map(String::from)
                .or_else(|| {
                    panic_payload
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .map(String::from)
                })
                .unwrap_or_else(|| "<non-string panic>".to_string());
            tracing::error!(
                task_id = ?task.id,
                panic = %panic_msg,
                "dispatcher panicked; emitting TaskCompletion::Failed"
            );
            TaskCompletion::Failed {
                workflow_id,
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                error: format!("dispatcher panicked: {panic_msg}"),
                target_node: task.target_node.clone(),
            }
        }
        Err(_) => {
            crate::metrics::inc_tasks_timeout();
            crate::metrics::dec_running_tasks();
            crate::metrics::observe_task_duration_ms(
                crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
            );
            TaskCompletion::Failed {
                workflow_id,
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                error: format!("task timed out after {}ms", effective_timeout.as_millis()),
                target_node: task.target_node.clone(),
            }
        }
    }
}

/// 将任务完成结果发布到事件总线或回传给远端 orchestrator。
///
/// 抽出为独立 async 函数，覆盖本地发布、远程 ``TaskResultAck`` 接受/拒绝、
/// 异常响应、网络错误等路径，无需构造完整 Worker 执行循环。
async fn publish_task_completion(
    completion: TaskCompletion,
    task: &TaskDefinition,
    node_id: &NodeId,
    network: &dyn crate::runtime::network::Transport,
    event_bus: &EventBus,
    pending_results: &tokio::sync::mpsc::Sender<PendingResult>,
    pending_capacity: usize,
) {
    let is_remote_task = task.origin_node.as_ref().is_some_and(|o| o != node_id);

    if is_remote_task {
        let workflow_id = task
            .workflow_id
            .clone()
            .unwrap_or_else(|| WorkflowId::from(String::new()));
        let wire_result = completion.to_wire_result(workflow_id.clone());

        if let Some(ref origin) = task.origin_node {
            // 优先使用 origin_endpoint_addr（iroh 公钥），否则回退到 node_id
            let origin_addr = task
                .origin_endpoint_addr
                .as_deref()
                .unwrap_or(origin.as_str());
            let request = crate::runtime::network::DirectRequest::TaskResult {
                workflow_id: workflow_id.clone(),
                task_id: wire_result.task_id.clone(),
                task_name: wire_result.task_name.clone(),
                outcome: wire_result.outcome.clone(),
                worker_node: node_id.clone(),
            };
            // 尝试一次；失败则入队异步重试
            match network
                .send_direct_request(origin_addr, request.clone())
                .await
            {
                Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: true }) => {
                    tracing::debug!(
                        "task result delivered directly to orchestrator {}",
                        origin_addr
                    );
                }
                Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: false }) => {
                    tracing::warn!(
                        "orchestrator {} rejected task result, enqueuing for retry",
                        origin_addr
                    );
                    if !try_enqueue_pending_result(
                        pending_results,
                        origin_addr.to_string(),
                        request,
                        0,
                        pending_capacity,
                    )
                    .await
                    {
                        // 通道满：降级为 TaskFailed 事件，避免结果静默丢失。
                        let failed = TaskCompletion::Failed {
                            workflow_id: workflow_id.clone(),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: format!(
                                "result delivery to orchestrator {} rejected and retry queue full",
                                origin_addr
                            ),
                            target_node: task.target_node.clone(),
                        };
                        event_bus.publish(BusEvent::TaskFailed(failed)).await;
                    }
                }
                Ok(_) => {
                    tracing::warn!(
                        "unexpected response from {}, enqueuing result for retry",
                        origin_addr
                    );
                    if !try_enqueue_pending_result(
                        pending_results,
                        origin_addr.to_string(),
                        request,
                        0,
                        pending_capacity,
                    )
                    .await
                    {
                        let failed = TaskCompletion::Failed {
                            workflow_id: workflow_id.clone(),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: format!(
                                "unexpected response from {} and retry queue full",
                                origin_addr
                            ),
                            target_node: task.target_node.clone(),
                        };
                        event_bus.publish(BusEvent::TaskFailed(failed)).await;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "direct result delivery to {} failed: {}, enqueuing for retry",
                        origin_addr,
                        e
                    );
                    if !try_enqueue_pending_result(
                        pending_results,
                        origin_addr.to_string(),
                        request,
                        0,
                        pending_capacity,
                    )
                    .await
                    {
                        let failed = TaskCompletion::Failed {
                            workflow_id: workflow_id.clone(),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: format!(
                                "delivery to {} failed: {} and retry queue full",
                                origin_addr, e
                            ),
                            target_node: task.target_node.clone(),
                        };
                        event_bus.publish(BusEvent::TaskFailed(failed)).await;
                    }
                }
            }
        }
    } else {
        // 本地 task 完成：始终发布到 EventBus（即使无 workflow_id），
        // 使 Python @task 等非工作流任务也能通过事件总线获取结果。
        let bus_event = match completion {
            TaskCompletion::Failed { .. } => BusEvent::TaskFailed(completion),
            TaskCompletion::Cancelled { .. } => BusEvent::TaskCancelled(completion),
            TaskCompletion::Skipped { .. } => BusEvent::TaskSkipped(completion),
            TaskCompletion::Completed { .. } => BusEvent::TaskCompleted(completion),
        };
        event_bus.publish(bus_event).await;
    }
}

#[cfg(test)]
#[path = "../../../tests/rust/unit/runtime/workflow/runtime.rs"]
mod tests;
