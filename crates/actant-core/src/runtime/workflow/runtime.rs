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
//! 3. 获取 `TaskEnqueued` 专用 `Notify` 信号，SchedulerActor 在 `enqueue` 后
//!    触发 `notify_task_enqueued()` 唤醒 Worker 拉取任务。
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
    format_error_kind, ActantError, NodeId, Result, TaskCompletion, TaskDefinition, TaskId, Topic,
    WorkerConfig, WorkflowId,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::dispatcher::{new_cancel_flag, CancelFlag, TaskDispatcher};
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::network::Transport;
use crate::runtime::workflow::actor::{InnerScheduler, ResultSource, TaskResultOutcome};
use crate::runtime::workflow::messaging::{decode, encode};
use crate::runtime::workflow::workflow_methods;
use crate::runtime::workflow::Scheduler;

mod cancel;
mod network_router;
mod result_delivery;

pub(crate) use network_router::{NetworkEventRouter, NetworkEventRouterConfig};
use result_delivery::{start_pending_result_loop, PendingResult};

#[cfg(test)]
use crate::runtime::network::DirectResponseChannel;

/// 信号驱动的任务拉取。
///
/// 先 ``try_dequeue`` 拉取已入队任务；无任务时在 EventBus 的
/// ``TaskEnqueued`` 专用 ``Notify`` 信号上 await，由 SchedulerActor 在
/// ``enqueue`` 后调用 ``notify_task_enqueued()`` 唤醒。
/// 无 sleep、无固定延迟、无 CPU 浪费。
///
/// # 为什么不直接调用 ``dequeue()``
///
/// ``dequeue()`` 会向 SchedulerActor 发送 ``DEQUEUE`` 消息，而
/// SchedulerActor 的 ``handle_message`` 会调用 ``inner.dequeue().await``，
/// 该方法在队列为空时阻塞。由于 Actor 单线程顺序处理消息，阻塞的
/// DEQUEUE 会阻止后续 ENQUEUE 被处理，形成死锁。
/// 通过 Notify 信号 + 非阻塞 ``try_dequeue`` 组合，Actor 每条消息
/// 都能立即返回，Worker 也能在任务入队时立即被唤醒。
///
/// # 信号丢失安全性
///
/// ``Notify::notify_waiters()`` 仅唤醒当前正在等待的 Future，不存储信号。
/// 若 notify 时无等待者，信号丢失。但这不影响正确性：Worker 每次被唤醒后
/// 会循环 ``try_dequeue`` 直到队列空。关键场景是 Worker 正在 ``notified().await``
/// 时收到 notify——此时立即唤醒。Worker 在处理任务期间（未 await）收到的
/// notify 会丢失，但 Worker 完成当前任务后会再次 ``try_dequeue``，拉取到
/// 期间入队的任务。
///
/// # 订阅竞态安全性
///
/// ``Notify`` 引用在 ``run()`` 的 ``ready`` 信号前获取（见 ``run`` 注释），
/// 因此 ``serve()`` 返回后 ``submit_task`` 触发的 notify 不会被错过——
/// Worker 进入循环后立即在 notify 上 await。
async fn wait_for_task(
    scheduler: &Arc<dyn Scheduler>,
    fast: Option<&Arc<InnerScheduler>>,
    notify: &Arc<tokio::sync::Notify>,
) -> TaskDefinition {
    loop {
        // 直连路径：与 SchedulerActor 共享同一 InnerScheduler，出队不经过
        // Actor 消息回传（大载荷 TaskDefinition 会超出响应解码上限）。
        if let Some(inner) = fast {
            if let Some(task) = inner.dequeue().await {
                return task;
            }
        }
        let dequeue_result = scheduler.try_dequeue().await;
        if let Some(task) = dequeue_result {
            return task;
        }
        // 无任务时等待 Notify 信号。信号由 SchedulerActor 在
        // enqueue/enqueue_batch 后通过 notify_task_enqueued() 触发。
        // notified().await 在收到 notify_waiters() 时返回，否则永久阻塞——
        // shutdown 时 select! 的 cancel 分支会取消此 future。
        //
        // Notify 引用在 ready 信号前获取，不会因竞态错过信号。
        // notify_waiters() 仅唤醒当前等待者，不存储许可——
        // Worker 处理任务期间收到的 notify 会丢失，但下次 try_dequeue
        // 仍会拉取已入队任务（见上方"信号丢失安全性"）。
        notify.notified().await;
    }
}

/// 批量 prefetch 任务：一次拉取最多 ``limit`` 个任务到本地 inflight 队列。
///
/// 与 ``wait_for_task`` 的单任务拉取相比，批量 prefetch 有两个性能优势：
///
/// 1. **减少 dequeue 调用次数**：每次 Actor 调用涉及消息编码/解码/通道往返，
///    批量拉取将 N 次调用降为 1 次（当队列中有积压任务时）。
/// 2. **解耦 dequeue 与 dispatch**：prefetch 后主循环可立即开始 dispatch，
///    不必等待下一次 dequeue；同时 dispatcher 线程池能更快填满，提升并发度。
///
/// 队列空时退化为 ``wait_for_task`` 的等待语义：先 dequeue_batch 一次，
/// 无任务则等待 Notify 信号唤醒。这避免了空队列时的忙轮询。
///
/// 返回值：始终返回非空 Vec（队列为空时阻塞直到有任务）。
async fn prefetch_tasks(
    scheduler: &Arc<dyn Scheduler>,
    fast: Option<&Arc<InnerScheduler>>,
    notify: &Arc<tokio::sync::Notify>,
    limit: usize,
) -> Vec<TaskDefinition> {
    if limit == 0 {
        // limit=0 退化为单任务模式：拉一个返回。
        return vec![wait_for_task(scheduler, fast, notify).await];
    }
    loop {
        // 先尝试批量拉取：若队列有积压，一次拉满 limit。
        // dequeue_batch 在队列为空时返回空 Vec（不阻塞），故需配合信号等待。
        let batch = if let Some(inner) = fast {
            inner.dequeue_batch(limit)
        } else {
            scheduler.dequeue_batch(limit).await
        };
        if !batch.is_empty() {
            return batch;
        }
        // 队列空：等待 Notify 信号唤醒，避免忙轮询。
        // 收到信号后下一轮 loop 重新 dequeue_batch。
        notify.notified().await;
    }
}

/// 远端转发失败 / submit-only 回退重入队的每任务弹跳上限。
///
/// 与 [`WorkerConfig::crash_failover_max_attempts`]（进程崩溃故障转移）相互独立：
/// 本上限针对"转发被对端拒绝"或"本地无并发且无远端可用"导致的重入队弹跳。
/// 超限后任务转为 `TaskCompletion::Failed` 并走既有完成事件投递路径，
/// 避免任务在调度器与主循环之间无限弹跳占用资源。
const MAX_REROUTE_BOUNCES: u32 = 5;

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
    /// 调度器共享内部状态（与 SchedulerActor 同一 Arc）。出队直连路径：
    /// `TaskDefinition` 内嵌任务载荷，经 Actor 消息回传受 `decode_postcard`
    /// 4MiB 上限约束，超限任务的出队响应会解码失败——直连共享队列绕过该
    /// 限制。`None`（无 fast path 的测试桩调度器）时退化为 Actor 消息协议。
    fast_scheduler: Option<Arc<InnerScheduler>>,
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
    /// Capability gossip 处理器。用于接收 `TOPIC_CAPABILITY_GOSSIP` 上的远端 capability 广播，
    /// 通过 `NetworkEventRouter` 路由到 `handle_gossip`。
    capability_gossip: Option<Arc<crate::runtime::capability::gossip::CapabilityGossipActor>>,
    /// 远端 peer 容量视图，用于 unrouted task 的自动路由。
    failover: Option<Arc<crate::runtime::workflow::FailoverManager>>,
    /// 远端路由策略（G-route）：默认内置实现，可经 `with_route_policy` 注入。
    route_policy: Arc<dyn crate::runtime::workflow::RoutePolicy>,
    /// 本地编排回灌桥开关（X2）：仅 Rust 嵌入（无 Python 事件泵）开启。
    /// Python 绑定层保持关闭——事件泵已承担同职责，双重回灌会产生重试
    /// 裁决竞态。
    orchestrator_ingest: bool,
    /// 最大并发任务数。用 AtomicUsize 支持运行时扩容（`set_max_concurrent_tasks`）。
    max_concurrent_tasks: Arc<std::sync::atomic::AtomicUsize>,
    task_timeout: Duration,
    /// 进程池 worker 崩溃后任务重路由的最大执行次数（含首次）。见 [`WorkerConfig::crash_failover_max_attempts`]。
    crash_failover_max_attempts: u32,
    cancel: Arc<tokio::sync::watch::Sender<bool>>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    drain_timeout: Duration,
    running_tasks: Arc<tokio::sync::Semaphore>,
    state: Arc<tokio::sync::watch::Sender<WorkerState>>,
    broadcast_retry_attempts: usize,
    broadcast_retry_base_delay: Duration,
    remote_fallback_delay: Duration,
    pending_result_channel_capacity: usize,
    /// 批量 prefetch 的最小/最大批量。由 [`WorkerConfig::prefetch_min`] /
    /// [`WorkerConfig::prefetch_max`] 配置，构造时归一化保证 `min <= max`。
    prefetch_min: usize,
    prefetch_max: usize,
    /// 远端转发失败 / submit-only 回退重入队的每任务计数。
    ///
    /// 两条重入队路径若目标节点持续不可用，任务会在调度器与主循环之间无限
    /// 弹跳。此表记录每个任务的弹跳次数，超过 [`MAX_REROUTE_BOUNCES`] 后
    /// 任务转为 Failed 并发布 TaskFailed 完成事件（复用既有失败投递路径）。
    /// 任务成功转发、开始本地执行或因超限转 Failed 时移除对应条目。
    reroute_counts: Arc<parking_lot::Mutex<HashMap<String, u32>>>,
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

/// [`Worker::report_task_result`] 的返回值（回传 Python 事件泵的重试裁决）。
#[derive(Debug, Clone, Copy, Default)]
pub struct TaskReportResponse {
    /// orchestrator 是否裁决为「重试」。为 `true` 时本节点已按 `delay_ms`
    /// 延迟把重试任务重新入队调度器，提交方句柄应保持等待。
    pub retry: bool,
    /// 重试前的延迟（毫秒）。`retry == false` 时为 0。
    pub delay_ms: u64,
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
            fast_scheduler: None,
            scheduler_actor_id: None,
            event_bus,
            task_dispatcher,
            actor_system: None,
            workflow_actor_id: None,
            dag_gossip_actor_id: None,
            capability_gossip: None,
            failover: None,
            route_policy: Arc::new(crate::runtime::workflow::DefaultRoutePolicy),
            orchestrator_ingest: false,
            max_concurrent_tasks: Arc::new(std::sync::atomic::AtomicUsize::new(max_concurrent)),
            task_timeout: Duration::from_millis(config.default_task_timeout_ms),
            crash_failover_max_attempts: config.crash_failover_max_attempts,
            cancel: Arc::new(cancel_tx),
            cancel_rx,
            drain_timeout: Duration::from_secs(config.drain_timeout_secs),
            running_tasks: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            state: Arc::new(state_tx),
            broadcast_retry_attempts: config.broadcast_retry_attempts,
            broadcast_retry_base_delay: Duration::from_millis(config.broadcast_retry_base_delay_ms),
            remote_fallback_delay: Duration::from_millis(config.remote_fallback_delay_ms),
            pending_result_channel_capacity: config.pending_result_channel_capacity,
            prefetch_min: config.prefetch_min.max(1),
            prefetch_max: config.prefetch_max.max(config.prefetch_min.max(1)),
            reroute_counts: Arc::new(parking_lot::Mutex::new(HashMap::new())),
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

    /// 覆盖崩溃故障转移的最大执行次数（测试用）。
    pub fn with_crash_failover_max_attempts(mut self, max: u32) -> Self {
        self.crash_failover_max_attempts = max;
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

    /// 注入 Capability gossip 处理器。
    ///
    /// `NetworkEventRouter` 在收到 `TOPIC_CAPABILITY_GOSSIP` gossip 消息时，
    /// 通过此 handler 的 `handle_gossip` 方法更新本地 capability 视图。
    pub(crate) fn with_capability_gossip(
        mut self,
        gossip: Arc<crate::runtime::capability::gossip::CapabilityGossipActor>,
    ) -> Self {
        self.capability_gossip = Some(gossip);
        self
    }

    /// 返回 failover 管理器句柄（节点可见性 N2：`peers()` 数据源）。
    pub fn failover_manager(&self) -> Option<Arc<crate::runtime::workflow::FailoverManager>> {
        self.failover.clone()
    }

    /// 开关本地编排回灌桥（X2）：仅 Rust 嵌入（无 Python 事件泵）开启。
    /// 详见 [`RuntimeBuilder::with_orchestrator_ingest`]。
    pub fn with_orchestrator_ingest(mut self, enabled: bool) -> Self {
        self.orchestrator_ingest = enabled;
        self
    }

    /// 注入自定义远端路由策略（G-route）。未注入时使用内置
    /// [`DefaultRoutePolicy`]（心跳新鲜度过滤 + 槽位比较）。
    pub fn with_route_policy(
        mut self,
        policy: Arc<dyn crate::runtime::workflow::RoutePolicy>,
    ) -> Self {
        self.route_policy = policy;
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
    /// drain/关闭路径的出队：优先直连共享队列（大载荷安全），否则退化 trait。
    async fn try_dequeue_drain(&self) -> Option<TaskDefinition> {
        if let Some(inner) = &self.fast_scheduler {
            return inner.try_dequeue();
        }
        self.scheduler.try_dequeue().await
    }

    /// 注入调度器共享内部状态（大载荷直连出队路径，见 [`Worker::fast_scheduler`]）。
    pub(crate) fn with_fast_scheduler(mut self, inner: Arc<InnerScheduler>) -> Self {
        self.fast_scheduler = Some(inner);
        self
    }

    pub fn scheduler_clone(&self) -> Arc<dyn Scheduler> {
        self.scheduler.clone()
    }

    /// 返回 task dispatcher 的 Arc 克隆，供外部注册 handler。
    pub fn task_dispatcher(&self) -> &Arc<dyn TaskDispatcher> {
        &self.task_dispatcher
    }

    /// 取消任务：运行中则置位其 `cancel_flag`；尚未进入执行则登记「派发前取消」。
    ///
    /// 这是低层运行时取消入口。任务尚在调度器队列中（主循环尚未预注册
    /// `cancel_flag`）时，把请求登记到「派发前取消」注册表（`cancelled_tasks`）：
    /// 主循环取出该任务时据此短路为 `TaskCompletion::Cancelled`，不执行任务体。
    ///
    /// **为什么必须双写**：远端的 `CancelBroadcast` 路径一直是「置 flag +
    /// 登记注册表」双写（见 `runtime/network_router.rs` 的 `TopicRoute::Cancel`），
    /// 而本地路径此前只置 flag。未进执行的任务没有 flag 可置，本地取消请求因此
    /// 被静默丢弃——任务照常执行到完成，`propagate=True` 的级联取消形同虚设。
    ///
    /// 返回 `true` 表示存在运行中的任务且已置位取消标志；返回 `false` 表示没有
    /// 运行中的任务（请求可能已登记待派发前拦截，也可能该 task_id 不存在——
    /// worker 侧不持有队列所有权，无法区分；登记条目由 TTL 清理循环兜底）。
    pub fn cancel_task(&self, task_id: &str) -> bool {
        let running = {
            let flags = self.cancel_flags.lock();
            match flags.get(task_id) {
                Some(flag) => {
                    flag.store(true, std::sync::atomic::Ordering::Release);
                    true
                }
                None => false,
            }
        };
        if !running {
            let mut pending = self.cancelled_tasks.lock();
            // 仅新插入时增加 pending 计数；重复取消不重复计数（与远端路径一致）。
            if pending
                .insert(task_id.to_string(), Instant::now())
                .is_none()
            {
                crate::metrics::inc_cancelled_tasks_pending();
            }
        }
        crate::metrics::inc_tasks_cancelled();
        running
    }

    /// 记录一次重入队弹跳；超过 [`MAX_REROUTE_BOUNCES`] 时清除计数并返回 `true`。
    ///
    /// 返回 `true` 表示调用方应将任务转为 Failed（不再重入队）。
    fn record_reroute_bounce(&self, task_id: &str) -> bool {
        let mut counts = self.reroute_counts.lock();
        let entry = counts.entry(task_id.to_string()).or_insert(0);
        *entry += 1;
        if *entry > MAX_REROUTE_BOUNCES {
            counts.remove(task_id);
            return true;
        }
        false
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

    /// 上报本地 flow 任务的终态结果（Rust 侧桥）。
    ///
    /// # 为什么需要这条桥
    ///
    /// 本地 worker 的结果帧正文**对 Rust 不透明**：业务失败由 worker 编码进
    /// 正文（`dumps((False, exc))`），`ProcessTaskDispatcher` 对成功与失败都返回
    /// `Ok(Ok(body))`。因此只有 Python 侧能区分二者，编排推进与重试裁决必须由
    /// Python 事件泵解析后经本方法发起（见 `settle_local_completion` 的说明）。
    ///
    /// # 行为
    ///
    /// 结果经 `ON_TASK_RESULT` 回灌本地 `WorkflowActor`，随后落实
    /// orchestrator 的裁决：
    /// - 裁决为「重试」→ 按 `delay_ms` 延迟把重试任务入队本节点调度器，返回
    ///   `retry = true`（Python 侧据此保持提交方句柄等待，不发布失败事件）；
    /// - 终局（完成 / 重试耗尽 / 取消 / fencing 拒绝）→ 返回 `retry = false`。
    ///
    /// `workflow_id` 为空（独立 `@task` 直调，非编排任务）时不做任何事。
    ///
    /// # 降级
    ///
    /// 未绑定 `ActorSystem` / `WorkflowActor`（不启动 Actor 系统的 Worker 测试
    /// 路径）时返回无裁决（`retry = false`）：编排是可选能力，缺失不应让任务
    /// 本身失败——与本地事件发布路径的降级语义一致。
    ///
    /// # Errors
    ///
    /// 结果编码失败、Actor 调用失败或 workflow 拒绝结果时返回错误，由调用方
    /// （Python 事件泵）记录并继续——句柄侧仍有事件路径兜底。
    pub async fn report_task_result(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        outcome: TaskResultOutcome,
    ) -> Result<TaskReportResponse> {
        if workflow_id.as_str().is_empty() {
            return Ok(TaskReportResponse::default());
        }
        let (Some(actor_system), Some(workflow_actor_id)) =
            (self.actor_system.as_ref(), self.workflow_actor_id.as_ref())
        else {
            tracing::debug!(
                task_id = %task_id.as_str(),
                "report_task_result: no workflow actor bound; skipping orchestration ingest"
            );
            return Ok(TaskReportResponse::default());
        };
        // wire 协议尚未携带派发代数，attempt 传 `None`（入口 fencing 放行）；
        // DAG 写入方法内部的 fencing 校验仍是最终防线。
        let payload = encode(&(
            workflow_id.clone(),
            task_id.clone(),
            outcome,
            None::<u32>,
            ResultSource::Local,
        ))?;
        let result = actor_system
            .call(workflow_actor_id, workflow_methods::ON_TASK_RESULT, payload)
            .await?;
        if let Some(error) = result.error {
            return Err(ActantError::from(error));
        }
        // 响应载荷是编码后的 `Option<(TaskDefinition, u64)>`：`None`
        // 表示终局（无重试任务），`Some` 才是 orchestrator 的重试裁决。注意
        // postcard 下 `None` 仍占一个判别字节（`[0x00]`）而非空 payload，
        // 因此必须按 `Option` 解码——直接解码成二元组会读到判别字节当结构体
        // 头部而报 "Hit the end of buffer"。
        let verdict: Option<(TaskDefinition, u64)> = if result.payload.is_empty() {
            None
        } else {
            decode(&result.payload)?
        };
        let Some((task, delay_ms)) = verdict else {
            return Ok(TaskReportResponse::default());
        };
        let scheduler = self.scheduler.clone();
        // 延迟入队由独立 task 承担：调用方（Python 事件泵）不阻塞，句柄等待
        // 语义与旧 worker 层重试一致——重试耗尽前提交方见不到失败。
        self.tokio_handle.spawn(async move {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            if let Err(e) = scheduler.enqueue(task).await {
                tracing::error!(
                    error = %e,
                    "failed to enqueue orchestrator-driven retry task (local result report)"
                );
            }
        });
        Ok(TaskReportResponse {
            retry: true,
            delay_ms,
        })
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
        // 获取 TaskEnqueued 唤醒信号的 Arc<Notify> 引用。
        // Notify 无队列、无丢弃：notify_waiters() 唤醒所有正在
        // .notified().await 的 Worker。ready 信号前获取确保：
        // serve() 返回后 Python submit() 触发的 notify 不会错过——
        // Worker 进入 run_task_execution_loop 后立即在 notify 上 await。
        let task_enqueued_notify = self.event_bus.task_enqueued_notify();
        // 进入任务执行循环前发送就绪信号：``serve()`` 据此返回。
        // ``send_replace`` 写入持久状态，即使等待者晚于 ``run()`` 启动
        // 也能立即观察到 true。返回的旧值无意义（始终是 false）。
        let _ = self.ready.send_replace(true);
        let result = self
            .run_task_execution_loop(&pending_tx, &task_enqueued_notify)
            .await;
        result
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
        // Err 路径表示 run() 在就绪前已退出（spawn 失败等），返回 Ok
        // 让 serve() 完成；后续 submit 会因 worker 未就绪而显式失败。
        let _ = rx.changed().await;
        Ok(())
    }

    /// 诊断方法：检查 worker 是否已就绪（ready=true）。
    pub fn is_ready(&self) -> bool {
        *self.ready.borrow()
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
            capability_gossip: self.capability_gossip.clone(),
            failover: self.failover.clone(),
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
            self.event_bus.clone(),
        )
    }

    async fn run_task_execution_loop(
        &self,
        pending_tx: &tokio::sync::mpsc::Sender<PendingResult>,
        task_enqueued_notify: &Arc<tokio::sync::Notify>,
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

        // Prefetch 批量大小：取 max_concurrent_tasks 与 [prefetch_min, prefetch_max]
        // 区间的钳制值（默认 [16, 64]）。一次拉取多个任务到本地 inflight，
        // 避免每任务一次 dequeue 调用；上限防止过度 prefetch 占用调度器内存。
        // prefetch_max 在构造时已归一化为 >= prefetch_min，clamp 不会 panic。
        let prefetch_limit = {
            let mc = self
                .max_concurrent_tasks
                .load(std::sync::atomic::Ordering::Acquire);
            mc.clamp(self.prefetch_min, self.prefetch_max)
        };

        // 本地 inflight 队列：prefetch 拉取的任务暂存于此，主循环从中取任务 dispatch。
        // 当队列为空时触发下一次 prefetch。这使 dequeue 与 dispatch 解耦：
        // dispatch 一个任务的同时，下一批任务已在 inflight 中等待。
        let mut inflight: std::collections::VecDeque<TaskDefinition> =
            std::collections::VecDeque::with_capacity(prefetch_limit);

        loop {
            if *self.state.subscribe().borrow() == WorkerState::Draining {
                // 关闭 scheduler，使新 task 无法入队。
                self.scheduler.close();
                // 排空剩余排队 task — 由于 scheduler 已关闭，新入队会被拒绝，
                // 已排队任务直接丢弃。每个被丢弃任务必须发布 Cancelled 完成事件：
                // 排队任务不会被执行，若不通知 origin，远端提交方会永久等待结果。
                while let Some(dropped) = self.try_dequeue_drain().await {
                    publish_drained_task_cancellation(
                        dropped,
                        &DrainNotifyCtx {
                            node_id: &node_id,
                            network: network.as_ref(),
                            event_bus: &event_bus,
                            pending_results: pending_tx,
                            pending_capacity: self.pending_result_channel_capacity,
                        },
                    )
                    .await;
                }
                // inflight 中已 prefetch 但未 dispatch 的任务同样被丢弃，逐个通知。
                for dropped in inflight.drain(..) {
                    publish_drained_task_cancellation(
                        dropped,
                        &DrainNotifyCtx {
                            node_id: &node_id,
                            network: network.as_ref(),
                            event_bus: &event_bus,
                            pending_results: pending_tx,
                            pending_capacity: self.pending_result_channel_capacity,
                        },
                    )
                    .await;
                }
                // 通知子任务（network event loop、retry loop）退出。
                // 否则 rt.shutdown_timeout 会等待完整的 5s。
                // cancel.send 失败仅当所有 receiver 已 drop（子任务已退出）。
                let _ = self.cancel.send(true);
                self.publish_lifecycle(BusEvent::WorkerDrained {
                    node_id: self.node_id.clone(),
                });
                // state_sender.send_replace 返回旧值无意义。
                let _ = state_sender.send_replace(WorkerState::Stopped);
                return Ok(());
            }

            // 取下一个任务：优先从 inflight 队列取（避免 dequeue 调用）；
            // inflight 空时触发 prefetch 批量拉取。
            // 每次 select! 同时监听 cancel 信号，确保 drain 命令即时响应。
            let task_opt: Option<TaskDefinition> = if let Some(t) = inflight.pop_front() {
                Some(t)
            } else {
                let prefetch_result = tokio::select! {
                    _ = task_cancel.changed() => {
                        // state_sender.send_replace 返回旧值无意义。
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
                                // max_concurrent_tasks 理论上为 usize，semaphore::acquire_many
                                // 接收 u32。实际值受 CPU 核数限制不会溢出，但显式截断
                                // 比 `as u32` 静默截断更安全——溢出时取 u32::MAX 仍能
                                // 正确 drain（等待所有 permit）。
                                let max = u32::try_from(
                                    self.max_concurrent_tasks.load(std::sync::atomic::Ordering::Acquire)
                                ).unwrap_or(u32::MAX);
                                let permit = sem.acquire_many(max).await;
                                drop(permit);
                            },
                        ).await;

                        if drain_result.is_err() {
                            tracing::warn!("drain timeout exceeded, forcing shutdown");
                        } else {
                            tracing::info!("all running tasks completed, shutting down");
                        }

                        // 与 Draining 主循环分支一致：关闭 scheduler 并排空残留
                        // 排队/inflight 任务，逐个发布 Cancelled 完成事件通知 origin。
                        self.scheduler.close();
                        while let Some(dropped) = self.try_dequeue_drain().await {
                            publish_drained_task_cancellation(
                                dropped,
                                &DrainNotifyCtx {
                                    node_id: &node_id,
                                    network: network.as_ref(),
                                    event_bus: &event_bus,
                                    pending_results: pending_tx,
                                    pending_capacity: self.pending_result_channel_capacity,
                                },
                            )
                            .await;
                        }
                        for dropped in inflight.drain(..) {
                            publish_drained_task_cancellation(
                                dropped,
                                &DrainNotifyCtx {
                                    node_id: &node_id,
                                    network: network.as_ref(),
                                    event_bus: &event_bus,
                                    pending_results: pending_tx,
                                    pending_capacity: self.pending_result_channel_capacity,
                                },
                            )
                            .await;
                        }
                        self.publish_lifecycle(BusEvent::WorkerDrained {
                            node_id: self.node_id.clone(),
                        });
                        // state_sender.send_replace 返回旧值无意义。
                        let _ = state_sender.send_replace(WorkerState::Stopped);
                        return Ok(());
                    }
                    batch = prefetch_tasks(&self.scheduler, self.fast_scheduler.as_ref(), task_enqueued_notify, prefetch_limit) => batch,
                };
                // prefetch_tasks 返回非空 batch（内部保证）。
                // 第一个任务作为当前任务，剩余入 inflight 供后续循环快速取用。
                let mut batch = prefetch_result;
                let first = batch.remove(0);
                if !batch.is_empty() {
                    inflight.extend(batch);
                }
                Some(first)
            };
            let Some(task) = task_opt else {
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
                event_bus.publish(BusEvent::TaskCancelled(completion));
                cancelled_tasks.lock().remove(task.id.as_str());
                crate::metrics::dec_cancelled_tasks_pending();
                continue;
            }

            if let Some(ref target) = task.target_node {
                if target != &node_id {
                    match self.forward_remote_task(&task, target).await {
                        Ok(()) => {
                            crate::metrics::inc_task_forward_succeeded();
                            // 转发成功：清零弹跳计数，后续失败重新从 1 计。
                            self.reroute_counts.lock().remove(task.id.as_str());
                            // 登记"已转发、结果未回"。若目标节点失联，
                            // failover 据此终结该任务，避免提交方永久挂起。
                            if let Some(failover) = self.failover.as_ref() {
                                failover.record_outbound(
                                    &task.id,
                                    target,
                                    task.target_endpoint_addr.as_deref(),
                                    task.workflow_id
                                        .clone()
                                        .unwrap_or_else(|| WorkflowId::from(String::new())),
                                    &task.name,
                                );
                            }
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
                            // 弹跳上限：目标节点持续不可用（转发被拒/网络失败）时
                            // 任务会无限重入队。超过 MAX_REROUTE_BOUNCES 次后转
                            // Failed 并走既有完成投递路径，通知 origin 终止等待。
                            if self.record_reroute_bounce(task.id.as_str()) {
                                crate::metrics::inc_tasks_failed();
                                let completion = TaskCompletion::Failed {
                                    workflow_id: task
                                        .workflow_id
                                        .clone()
                                        .unwrap_or_else(|| WorkflowId::from(String::new())),
                                    task_id: task.id.clone(),
                                    task_name: task.name.clone(),
                                    error: format_error_kind(
                                        "network",
                                        &format!(
                                            "task forwarding failed after {} reroute attempts, last error: {}",
                                            MAX_REROUTE_BOUNCES, e
                                        ),
                                    ),
                                    target_node: task.target_node.clone(),
                                };
                                publish_task_completion(
                                    completion,
                                    &task,
                                    &node_id,
                                    network.as_ref(),
                                    &event_bus,
                                    pending_tx,
                                    self.pending_result_channel_capacity,
                                )
                                .await;
                                continue;
                            }
                            // 退避由独立 spawn 触发：延迟 remote_fallback_delay 后
                            // 再 enqueue，主循环不阻塞，可继续处理其他任务。
                            // target_node 已清空，重新入队后由路由器选择新目标。
                            let scheduler = self.scheduler.clone();
                            let delay = self.remote_fallback_delay;
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                let mut rerouted = task;
                                rerouted.target_node = None;
                                if let Err(e) = scheduler.enqueue(rerouted).await {
                                    tracing::warn!(
                                        "failed to re-enqueue task for re-routing: {}",
                                        e
                                    );
                                }
                            });
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
                // 弹跳上限：submit-only 节点在无远端可用时任务会无限重入队，
                // 处理方式与转发失败路径一致，超过上限转 Failed。
                if self.record_reroute_bounce(task.id.as_str()) {
                    crate::metrics::inc_tasks_failed();
                    let completion = TaskCompletion::Failed {
                        workflow_id: task
                            .workflow_id
                            .clone()
                            .unwrap_or_else(|| WorkflowId::from(String::new())),
                        task_id: task.id.clone(),
                        task_name: task.name.clone(),
                        error: format_error_kind(
                            "network",
                            &format!(
                                "no local capacity and no remote peer available after {} reroute attempts",
                                MAX_REROUTE_BOUNCES
                            ),
                        ),
                        target_node: task.target_node.clone(),
                    };
                    publish_task_completion(
                        completion,
                        &task,
                        &node_id,
                        network.as_ref(),
                        &event_bus,
                        pending_tx,
                        self.pending_result_channel_capacity,
                    )
                    .await;
                    continue;
                }
                // 同上：退避由独立 spawn 触发，延迟 enqueue 避免主循环阻塞。
                let scheduler = self.scheduler.clone();
                let delay = self.remote_fallback_delay;
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    if let Err(e) = scheduler.enqueue(task).await {
                        tracing::warn!("failed to re-enqueue submit-only task: {}", e);
                    }
                });
                continue;
            }

            // 任务被接受本地执行：清零弹跳计数。
            self.reroute_counts.lock().remove(task.id.as_str());

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

            // 崩溃故障转移用捕获：scheduler（重入队）、崩溃重路由上限与延迟。
            let scheduler_for_failover = self.scheduler.clone();
            // X2 本地编排回灌桥：仅当开关开启且绑定了 workflow actor。
            // 缺省关闭——Python 绑定层的事件泵承担同职责，双重回灌会产生
            // 重试裁决竞态。
            let orchestrator_bridge = if self.orchestrator_ingest {
                match (self.actor_system.clone(), self.workflow_actor_id.clone()) {
                    (Some(actor_system), Some(workflow_actor_id)) => Some(OrchestratorBridge {
                        actor_system,
                        workflow_actor_id,
                        scheduler: self.scheduler.clone(),
                    }),
                    _ => None,
                }
            } else {
                None
            };
            let crash_failover_max = self.crash_failover_max_attempts;
            let failover_delay = self.remote_fallback_delay;

            // Publish TaskStarted to EventBus (所有任务，使 Python @task 也能收到 running 状态)
            {
                let wf_id = task
                    .workflow_id
                    .clone()
                    .unwrap_or_else(|| WorkflowId::from("".to_string()));
                event_bus.publish(BusEvent::TaskStarted {
                    workflow_id: wf_id,
                    task_id: task.id.clone(),
                });
            }

            // 本地派发状态推进（Running）。
            //
            // 远端任务经 gossip 的 `MARK_TASK_RUNNING` 进入 Running；本地路径
            // 此前只记录 `TaskDispatched` 历史而**无状态迁移**，任务在整个执行
            // 期间仍是 `Pending`。这会让本地任务的真实失败与「编排合成的、从未
            // 派发的失败」无法区分——`handle_task_failure` 的迟到守卫据此丢弃
            // 前者，重试永不触发、任务永远停在 Pending。
            //
            // 必须是同步 `call`（而非 fire-and-forget `send`）：状态先于结果
            // 回灌生效，否则极快任务的结果帧可能先于本次迁移到达，被守卫误判。
            if let Some(running_wf) = task.workflow_id.clone() {
                if let (Some(system), Some(actor_id)) =
                    (self.actor_system.clone(), self.workflow_actor_id.clone())
                {
                    match encode(&(running_wf, task.id.clone())) {
                        Ok(running_payload) => {
                            match system
                                .call(
                                    &actor_id,
                                    crate::runtime::workflow::workflow_methods::MARK_TASK_RUNNING,
                                    running_payload,
                                )
                                .await
                            {
                                Ok(result) => {
                                    if let Some(err) = result.error {
                                        // 工作流已终态 / 已被淘汰时拒绝推进属正常，
                                        // 不影响任务本身的执行。
                                        tracing::debug!(
                                            task_id = ?task.id,
                                            error = %err,
                                            "workflow actor declined running transition"
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        task_id = ?task.id,
                                        error = %e,
                                        "failed to mark task running in workflow actor"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                task_id = ?task.id,
                                error = %e,
                                "failed to encode task running report"
                            );
                        }
                    }
                }
            }

            // 派发事件。任务被本地接受执行时经 WorkflowActor 追加
            // TaskDispatched 到工作流统一历史；fire-and-forget，失败仅告警。
            if let Some(dispatched_wf) = task.workflow_id.clone() {
                if let (Some(system), Some(actor_id)) =
                    (self.actor_system.clone(), self.workflow_actor_id.clone())
                {
                    match encode(&(dispatched_wf, task.id.clone())) {
                        Ok(payload) => {
                            let msg = crate::common::ActorMessage::new(
                                actor_id.clone(),
                                crate::runtime::workflow::workflow_methods::TASK_DISPATCHED
                                    .to_string(),
                                payload,
                            );
                            if let Err(e) = system.send(&actor_id, msg).await {
                                tracing::warn!(
                                    task_id = ?task.id,
                                    error = %e,
                                    "failed to report task dispatch to workflow actor"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                task_id = ?task.id,
                                error = %e,
                                "failed to encode task dispatch report"
                            );
                        }
                    }
                }
            }

            if task.enqueued_at_ms > 0 {
                let latency = dispatch_start_ms.saturating_sub(task.enqueued_at_ms);
                crate::metrics::observe_scheduling_latency_ms(latency);
                tracing::trace!(task = ?task.id, latency_ms = latency, "scheduling latency");
            }

            let cancelled_tasks_for_spawn = cancelled_tasks.clone();

            // 预注册 cancel_flag，消除 cancel_task 与 spawn 闭包内注册之间的竞态窗口。
            // cancel_flag 预注册到主循环：消除「主循环检查通过 → 闭包内注册」之间存在的
            // 窗口：cancel_task 查 cancel_flags 返回 false，取消请求丢失，任务继续执行
            // 直到自身超时或正常完成。预注册后 cancel_task 立即生效；若 permit 等待
            // 期间任务被取消，spawn 闭包开始后会检测到并直接走取消完成流程。
            let cancel_flag = new_cancel_flag();
            cancel_flags
                .lock()
                .insert(task.id.to_string(), cancel_flag.clone());

            // 采纳窗口期到达的本地取消：主循环的「派发前取消」检查（见上方
            // `cancelled_tasks` 短路块）发生在取出任务之初，而 cancel_flag 直到
            // 此处才注册。落在两者之间的取消既碰不到检查、也找不到 flag——
            // `Worker::cancel_task` 会把请求登记到 `cancelled_tasks`，此处补一次
            // 采纳：置位刚注册的 flag，spawn 闭包起始处的检查随即走取消完成流程。
            if cancelled_tasks.lock().remove(task.id.as_str()).is_some() {
                cancel_flag.store(true, std::sync::atomic::Ordering::Release);
                crate::metrics::dec_cancelled_tasks_pending();
                tracing::info!(
                    task_id = %task.id.as_str(),
                    "adopted pre-dispatch cancellation registered during dispatch window"
                );
            }

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

                // cancel_flag 已在 spawn 前预注册到 cancel_flags，此处直接使用。
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

                // 检查是否在 permit 等待期间被取消（预注册使 cancel_task 能立即生效）。
                if cancel_flag.load(std::sync::atomic::Ordering::Acquire) {
                    tracing::info!(task_id = ?task.id, "task cancelled before dispatch");
                    let completion = TaskCompletion::Cancelled {
                        workflow_id: task
                            .workflow_id
                            .clone()
                            .unwrap_or_else(|| WorkflowId::from(String::new())),
                        task_id: task.id.clone(),
                        task_name: task.name.clone(),
                        target_node: task.target_node.clone(),
                    };
                    settle_local_completion(
                        completion,
                        &task,
                        &node_id_for_spawn,
                        network.as_ref(),
                        &event_bus,
                        &pending_results,
                        pending_capacity,
                        orchestrator_bridge.as_ref(),
                    )
                    .await;
                    drop(_permit);
                    cleanup();
                    if let Some(ref cb) = capacity_cb {
                        let available = semaphore_for_cb.available_permits();
                        cb(
                            u32::try_from(available).unwrap_or(u32::MAX),
                            u32::try_from(max_concurrent).unwrap_or(u32::MAX),
                        );
                    }
                    return;
                }

                tracing::debug!(task_id = ?task.id, "dispatching task");

                // 硬超时由 ProcessTaskDispatcher 内部执行：dispatch 以
                // `effective_timeout` 为硬上限，超时后立即强杀对应 worker 进程
                // 并回收并发槽位（kill_and_replace，不等待取消宽限），返回
                // `Err(ActantError::Timeout)`。Worker 侧不再套内层 tokio 超时。
                // 用 catch_unwind 包裹 dispatcher future，避免 spawn 句柄被丢弃后
                // panic 让 workflow 永久挂起。panic 时降级为 TaskCompletion::Failed。
                use futures::future::FutureExt as _;
                let dispatched = std::panic::AssertUnwindSafe(dispatcher.dispatch(
                    &task_name,
                    task_payload,
                    cancel_flag.clone(),
                    effective_timeout,
                ))
                .catch_unwind();

                let dispatch_result = dispatched.await;

                // 崩溃故障转移：worker 进程崩溃（非逻辑失败、非硬超时）属于基础设施级失败，
                // 将任务清空 target_node 重新入队，由路由器重选本地或远端节点执行。
                // 受 `crash_failover_max_attempts` 上限约束，防止持久性崩溃无限重路由：
                // 仅在 `attempt + 1 < max` 时重路由（首次执行 attempt=0）；达到上限后
                // 才降级为 TaskCompletion::Failed，让 workflow 按正常失败路径推进。
                if is_worker_crash(&dispatch_result) && task.attempt + 1 < crash_failover_max {
                    crate::metrics::inc_task_forward_reroute();
                    tracing::warn!(
                        task_id = ?task.id,
                        next_attempt = task.attempt + 1,
                        max_attempts = crash_failover_max,
                        "worker crashed while executing task; re-enqueueing for crash failover"
                    );
                    drop(_permit);
                    cleanup();
                    if let Some(ref cb) = capacity_cb {
                        let available = semaphore_for_cb.available_permits();
                        cb(
                            u32::try_from(available).unwrap_or(u32::MAX),
                            u32::try_from(max_concurrent).unwrap_or(u32::MAX),
                        );
                    }
                    // 退避由独立 spawn 触发，主循环不阻塞。target_node 已清空，
                    // 重新入队后由路由器选择新的执行节点（本地或远端）。
                    let scheduler = scheduler_for_failover.clone();
                    let delay = failover_delay;
                    let mut rerouted = task;
                    rerouted.attempt += 1;
                    rerouted.target_node = None;
                    let rerouted_id = rerouted.id.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        if let Err(e) = scheduler.enqueue(rerouted).await {
                            tracing::warn!(
                                task_id = %rerouted_id.as_str(),
                                "failed to re-enqueue crashed task for failover: {e}"
                            );
                        }
                    });
                    return;
                }

                // 三种结果：Ok(Ok(Ok(output))) | Ok(Ok(Err(e))) | Ok(Err(panic)) | Err(timeout)
                let completion = build_completion_from_dispatch_result(
                    dispatch_result,
                    &task,
                    dispatch_start_ms,
                    effective_timeout,
                );

                settle_local_completion(
                    completion,
                    &task,
                    &node_id_for_spawn,
                    network.as_ref(),
                    &event_bus,
                    &pending_results,
                    pending_capacity,
                    orchestrator_bridge.as_ref(),
                )
                .await;

                drop(_permit);

                // 任务完成：从 cancel_flags 注册表移除。
                cleanup();

                if let Some(ref cb) = capacity_cb {
                    let available = semaphore_for_cb.available_permits();
                    cb(
                        u32::try_from(available).unwrap_or(u32::MAX),
                        u32::try_from(max_concurrent).unwrap_or(u32::MAX),
                    );
                }
            });
        }
    }

    /// 请求 Worker 进入 drain 并停止主循环。
    ///
    /// 除广播取消信号外，本方法**还会**关闭任务派发器
    /// （`TaskDispatcher::shutdown` 强杀空闲 worker 进程并回收子进程）。
    /// 它**不**等待在途任务完成——drain 与超时等待由
    /// [`crate::runtime::Runtime::shutdown`] 编排。
    ///
    /// 幂等：`Runtime::shutdown` 之后再次调用是 no-op（`free_workers` 已被取空）。
    pub fn shutdown(&self) {
        // state_sender.send_replace 返回旧值无意义。
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
        // state_sender.send_replace 返回旧值无意义。
        let _ = self.state.send_replace(WorkerState::Stopped);
    }

    pub fn drain(&self) {
        // state_sender.send_replace 返回旧值无意义。
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

    /// Publish a worker lifecycle event.
    ///
    /// `publish` 是同步非阻塞的，可在任意线程（含 Python GIL 线程）直接调用。
    fn publish_lifecycle(&self, event: BusEvent) {
        self.event_bus.publish(event);
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
                u32::try_from(available).unwrap_or(u32::MAX),
                u32::try_from(
                    self.max_concurrent_tasks
                        .load(std::sync::atomic::Ordering::Acquire),
                )
                .unwrap_or(u32::MAX),
            );
        }
    }

    fn select_remote_target(&self) -> Option<(NodeId, String)> {
        let failover = self.failover.as_ref()?;
        let local_available =
            u32::try_from(self.running_tasks.available_permits()).unwrap_or(u32::MAX);
        let ctx = crate::runtime::workflow::RouteContext {
            local_available_slots: local_available,
            heartbeat_ttl_ms: failover.failure_timeout_ms(),
        };
        let candidates: Vec<crate::runtime::workflow::RouteCandidate> = failover
            .get_peer_infos()
            .into_iter()
            .map(|(node_id, info)| crate::runtime::workflow::RouteCandidate {
                node_id,
                endpoint_addr: info.endpoint_addr,
                available_slots: info.available_slots,
                max_slots: info.max_slots,
                last_heartbeat_ms: info.last_heartbeat_ms,
            })
            .collect();
        self.route_policy.select_target(&ctx, &candidates)
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
///
/// 无 `Elapsed` 外层：硬超时由 `ProcessTaskDispatcher` 内部强杀 worker 后
/// 返回 `Err(ActantError::Timeout)`，不再依赖外层 tokio 超时。
/// 判定一次派发是否为**进程崩溃**（worker 子进程异常退出），而非业务失败或硬超时。
///
/// `Ok(Err(ActantError::Worker(_)))`：dispatcher 在读取结果帧时读到 EOF / 写入失败 /
/// worker 被强杀，即 worker 进程本身死亡。这是基础设施级失败，区别于
/// `Ok(Err(ActantError::Timeout))`（硬超时）与 Python 内抛出的业务异常（同样以
/// `Ok(Err(_))` 返回但非 `Worker` 变体）。崩溃是该类唯一需要触发"故障转移重路由"的信号，
/// 超时与业务失败应保持原有失败语义。
mod completion;
#[allow(unused_imports)]
pub(crate) use completion::DispatchResult;
use completion::{
    build_completion_from_dispatch_result, is_worker_crash, settle_local_completion,
    OrchestratorBridge,
};
use completion::{publish_drained_task_cancellation, publish_task_completion, DrainNotifyCtx};

#[cfg(test)]
#[path = "../../../../../tests/rust/unit/runtime/workflow/runtime.rs"]
mod tests;
