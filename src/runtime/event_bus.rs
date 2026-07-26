//! 基于话题的事件总线，用于内部解耦通信。
//!
//! 单一的发布/订阅中枢将事件发布到话题，订阅者只接收自己关心的话题。
//!
//! ## 投递保证
//!
//! 事件按 [DeliveryGuarantee] 分类：
//! - **Reliable**：发布方为每个订阅者等待容量，最长等待 `publish_timeout_ms`。
//!   若超时，订阅者的连续超时计数器递增。达到 `max_subscriber_timeouts` 次
//!   连续超时后，该订阅者将被修剪（移除），防止某个卡住的消费者阻塞整个系统。
//!   关键事件（任务生命周期、集群管理）使用此保证。
//! - **BestEffort**：发布方使用非阻塞 `try_send`。若订阅者通道已满，事件被丢弃并告警。
//!   适用于周期性或被覆盖的事件（心跳、对端状态）。

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use std::time::Duration;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::common::{
    NodeHeartbeat, NodeId, OrchestratorClaim, TaskCompletion, TaskId, WireDagStateUpdate,
    WorkflowId,
};
use crate::runtime::actor::SupervisionEvent;

/// 标识总线上的事件类别。
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum Topic {
    /// 任务已在此 worker 上开始执行。
    TaskStarted,
    /// 任务已从调度器出队（Worker 即将执行）。
    ///
    /// 由 `SchedulerActor` 在 `try_dequeue` 成功返回任务后发布。
    /// 用于外部观测：订阅者可统计实际出队速率，区分入队堆积与
    /// 真正消费速度。事件携带 workflow_id 与 task_id 便于关联。
    TaskDequeued,
    /// 任务成功完成。
    TaskCompleted,
    /// 任务失败。
    TaskFailed,
    /// 任务被取消。
    TaskCancelled,
    /// 任务被跳过（条件分支未命中）。
    TaskSkipped,
    /// 对端连接 / 断开。
    NetworkPeer,
    /// 远端节点的集群心跳。
    ClusterHeartbeat,
    /// 编排器声明公告。
    ClusterClaim,
    /// 来自 gossip 的 DAG 状态更新。
    DagUpdate,
    /// 用于 CRDT 同步的 heads 交换。
    HeadsExchange,
    /// Actor 监管事件（启动、失败、停止）。
    Supervision,
    /// Actor 生命周期中的不可恢复错误（与 `Supervision` 的区别：
    /// 后者描述常规生命周期信号；本 topic 专门用于 Worker/Actor 系统
    /// 拦截到 panic / 状态机非法转换 / 持久化失败等需要外部介入的错误）。
    ActorLifecycleError,
    /// WAL 压缩完成公告。订阅者可据此触发检查点清理、监控告警或
    /// 外部一致性校验。
    WalCompacted,
    /// Worker 生命周期事件（排空中、已排空、已停止）。
    WorkerLifecycle,
}

impl Topic {
    /// 返回话题的字符串标识，用于指标标签。
    pub fn as_str(&self) -> &'static str {
        match self {
            Topic::TaskStarted => "TaskStarted",
            Topic::TaskDequeued => "TaskDequeued",
            Topic::TaskCompleted => "TaskCompleted",
            Topic::TaskFailed => "TaskFailed",
            Topic::TaskCancelled => "TaskCancelled",
            Topic::TaskSkipped => "TaskSkipped",
            Topic::NetworkPeer => "NetworkPeer",
            Topic::ClusterHeartbeat => "ClusterHeartbeat",
            Topic::ClusterClaim => "ClusterClaim",
            Topic::DagUpdate => "DagUpdate",
            Topic::HeadsExchange => "HeadsExchange",
            Topic::Supervision => "Supervision",
            Topic::ActorLifecycleError => "ActorLifecycleError",
            Topic::WalCompacted => "WalCompacted",
            Topic::WorkerLifecycle => "WorkerLifecycle",
        }
    }
}

/// 发布到总线的事件投递保证。
///
/// - `Reliable`：发布方等待订阅者容量，最长 `publish_timeout_ms`。
///   超时后订阅者的连续超时计数器递增。达到 `max_subscriber_timeouts` 次
///   连续超时后，该订阅者将被修剪。
/// - `BestEffort`：发布方使用非阻塞 `try_send`。若订阅者通道已满，事件被丢弃并告警。
///   适用于周期性或被覆盖的事件（心跳、对端状态）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    Reliable,
    BestEffort,
}

/// 流经总线的统一事件类型。
///
/// 所有事件均可克隆并广播到该话题的所有订阅者。点对点请求-响应
/// （如 `DirectRequest`）不走 EventBus，而是由 `NetworkEventRouter`
/// 直接处理或回送 `DirectResponse::Error`，避免总线承载一次性通道
/// 而引入独占投递分支。
#[derive(Debug)]
#[non_exhaustive]
pub enum BusEvent {
    // -- 任务生命周期（可克隆）--
    TaskStarted {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    /// 任务已从调度器出队。
    ///
    /// 由 `SchedulerActor::try_dequeue` 在返回 Some(task) 后立即发布。
    /// 携带 workflow_id 与 task_id 便于订阅者关联任务上下文（如统计
    /// 出队速率、关联日志）。事件丢失不影响 Worker 主流程，仅为观测信号。
    TaskDequeued {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    TaskCompleted(TaskCompletion),
    TaskFailed(TaskCompletion),
    TaskCancelled(TaskCompletion),
    TaskSkipped(TaskCompletion),

    // -- 网络 --
    PeerConnected(NodeId),
    PeerDisconnected(NodeId),

    // -- 集群管理（可克隆）--
    Heartbeat(NodeHeartbeat),
    Claim(OrchestratorClaim),
    DagUpdate(WireDagStateUpdate),
    HeadsExchange(crate::common::HeadsExchange),

    // -- Actor 监管（可克隆）--
    SupervisionEvent(SupervisionEvent),
    /// Actor 生命周期中的不可恢复错误。
    ///
    /// 与 `SupervisionEvent::ActorFailed` 的区别：前者是常规失败信号
    /// （驱动重启策略），本事件描述系统层拦截到的 panic / 状态机
    /// 非法转换 / 持久化失败等需要外部介入的错误。携带 actor_id
    /// 与错误描述，便于运维订阅并触发告警。
    ActorLifecycleError {
        actor_id: crate::common::ActorId,
        error: String,
    },

    // -- 持久化公告 --
    /// WAL 压缩完成。携带节点 id 与压缩后保留的事件序号上限，
    /// 便于订阅者触发检查点清理或一致性校验。WAL 是 per-ActorSystem
    /// 一个文件（非 per-actor），故载荷使用 node_id 而非 actor_id。
    WalCompacted {
        node_id: NodeId,
        retained_events: u64,
    },

    // -- Worker 生命周期（可克隆）--
    /// Worker 已进入排空模式 — 不再接受新任务。
    WorkerDraining {
        node_id: NodeId,
    },
    /// Worker 已完成排空 — 所有运行中任务完成。
    WorkerDrained {
        node_id: NodeId,
    },
    /// Worker 已完全停止。
    WorkerStopped {
        node_id: NodeId,
    },
}

impl BusEvent {
    /// 返回此事件所属的话题。
    pub fn topic(&self) -> Topic {
        match self {
            BusEvent::TaskStarted { .. } => Topic::TaskStarted,
            BusEvent::TaskDequeued { .. } => Topic::TaskDequeued,
            BusEvent::TaskCompleted(_) => Topic::TaskCompleted,
            BusEvent::TaskFailed(_) => Topic::TaskFailed,
            BusEvent::TaskCancelled(_) => Topic::TaskCancelled,
            BusEvent::TaskSkipped(_) => Topic::TaskSkipped,
            BusEvent::PeerConnected(_) | BusEvent::PeerDisconnected(_) => Topic::NetworkPeer,
            BusEvent::Heartbeat(_) => Topic::ClusterHeartbeat,
            BusEvent::Claim(_) => Topic::ClusterClaim,
            BusEvent::DagUpdate(_) => Topic::DagUpdate,
            BusEvent::HeadsExchange(_) => Topic::HeadsExchange,
            BusEvent::SupervisionEvent(_) => Topic::Supervision,
            BusEvent::ActorLifecycleError { .. } => Topic::ActorLifecycleError,
            BusEvent::WalCompacted { .. } => Topic::WalCompacted,
            BusEvent::WorkerDraining { .. }
            | BusEvent::WorkerDrained { .. }
            | BusEvent::WorkerStopped { .. } => Topic::WorkerLifecycle,
        }
    }

    /// 克隆此事件用于广播给多个订阅者。
    ///
    /// 所有 BusEvent 变体均可克隆；本方法返回 `Some`。保留 `Option` 返回值
    /// 是为了未来可能引入不可克隆变体时的前向兼容，同时让 `broadcast_to_subscribers`
    /// 在克隆失败时有明确的防御性跳过路径。
    fn clone_broadcast(&self) -> Option<BusEvent> {
        Some(match self {
            BusEvent::TaskStarted {
                workflow_id,
                task_id,
            } => BusEvent::TaskStarted {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
            },
            BusEvent::TaskDequeued {
                workflow_id,
                task_id,
            } => BusEvent::TaskDequeued {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
            },
            BusEvent::TaskCompleted(c) => BusEvent::TaskCompleted(c.clone()),
            BusEvent::TaskFailed(c) => BusEvent::TaskFailed(c.clone()),
            BusEvent::TaskCancelled(c) => BusEvent::TaskCancelled(c.clone()),
            BusEvent::TaskSkipped(c) => BusEvent::TaskSkipped(c.clone()),
            BusEvent::PeerConnected(id) => BusEvent::PeerConnected(id.clone()),
            BusEvent::PeerDisconnected(id) => BusEvent::PeerDisconnected(id.clone()),
            BusEvent::Heartbeat(hb) => BusEvent::Heartbeat(hb.clone()),
            BusEvent::Claim(c) => BusEvent::Claim(c.clone()),
            BusEvent::DagUpdate(u) => BusEvent::DagUpdate(u.clone()),
            BusEvent::HeadsExchange(e) => BusEvent::HeadsExchange(e.clone()),
            BusEvent::SupervisionEvent(e) => BusEvent::SupervisionEvent(e.clone()),
            BusEvent::ActorLifecycleError { actor_id, error } => BusEvent::ActorLifecycleError {
                actor_id: actor_id.clone(),
                error: error.clone(),
            },
            BusEvent::WalCompacted {
                node_id,
                retained_events,
            } => BusEvent::WalCompacted {
                node_id: node_id.clone(),
                retained_events: *retained_events,
            },
            BusEvent::WorkerDraining { node_id } => BusEvent::WorkerDraining {
                node_id: node_id.clone(),
            },
            BusEvent::WorkerDrained { node_id } => BusEvent::WorkerDrained {
                node_id: node_id.clone(),
            },
            BusEvent::WorkerStopped { node_id } => BusEvent::WorkerStopped {
                node_id: node_id.clone(),
            },
        })
    }

    /// 返回此事件类型的投递保证。
    ///
    /// 关键事件（任务生命周期、集群管理）使用 [DeliveryGuarantee::Reliable]，
    /// 会等待订阅者容量。周期性或被覆盖的事件使用 [DeliveryGuarantee::BestEffort]，
    /// 通道满时丢弃。
    pub fn delivery_guarantee(&self) -> DeliveryGuarantee {
        match self {
            // 关键：不可丢失 — 任务结果驱动 DAG 推进，声明驱动故障转移，
            // DAG 更新驱动状态复制。ActorLifecycleError 与 WalCompacted
            // 也归 Reliable：前者驱动外部告警/介入，后者驱动检查点清理，
            // 丢失会导致运维盲区或检查点堆积。
            BusEvent::TaskStarted { .. }
            | BusEvent::TaskCompleted(_)
            | BusEvent::TaskFailed(_)
            | BusEvent::TaskCancelled(_)
            | BusEvent::TaskSkipped(_)
            | BusEvent::Claim(_)
            | BusEvent::DagUpdate(_)
            | BusEvent::ActorLifecycleError { .. }
            | BusEvent::WalCompacted { .. } => DeliveryGuarantee::Reliable,

            // 周期性/被覆盖：下一次心跳或对端事件会覆盖当前事件，丢弃可接受。
            // TaskDequeued 同理——事件仅为观测信号，丢弃后 Worker 下次
            // try_dequeue 仍会拉取已入队任务。
            BusEvent::TaskDequeued { .. }
            | BusEvent::PeerConnected(_)
            | BusEvent::PeerDisconnected(_)
            | BusEvent::Heartbeat(_)
            | BusEvent::HeadsExchange(_)
            | BusEvent::SupervisionEvent(_)
            | BusEvent::WorkerDraining { .. }
            | BusEvent::WorkerDrained { .. }
            | BusEvent::WorkerStopped { .. } => DeliveryGuarantee::BestEffort,
        }
    }
}

/// 跟踪投递健康的订阅者条目。
///
/// 包装 `mpsc::Sender` 和共享的连续超时计数器。
/// 当可靠事件投递超时，计数器递增。投递成功时计数器重置为零。
/// 超过 `max_subscriber_timeouts` 次连续超时的订阅者将被修剪，
/// 防止某个卡住的消费者拖累整个系统。
struct SubscriberSlot {
    sender: mpsc::Sender<BusEvent>,
    /// 通道的总容量（创建时确定），用于计算当前积压深度。
    /// tokio mpsc 不暴露当前队列长度，需 `capacity - sender.capacity()` 间接求得。
    capacity: usize,
    /// 共享的连续超时计数器。Arc 允许在 DashMap guard 释放后通过克隆的 slot 更新。
    consecutive_timeouts: Arc<AtomicU32>,
}

impl SubscriberSlot {
    fn new(sender: mpsc::Sender<BusEvent>, capacity: usize) -> Self {
        Self {
            sender,
            capacity,
            consecutive_timeouts: Arc::new(AtomicU32::new(0)),
        }
    }

    /// 返回当前积压深度（已入队但未被消费的事件数）。
    /// `sender.capacity()` 返回剩余可用容量，故 `capacity - capacity()` 即积压数。
    fn depth(&self) -> usize {
        self.capacity.saturating_sub(self.sender.capacity())
    }

    /// 记录成功投递 — 重置连续超时计数器。
    fn record_success(&self) {
        self.consecutive_timeouts.store(0, Ordering::Relaxed);
    }

    /// 记录投递超时 — 计数器递增，返回新值。
    fn record_timeout(&self) -> u32 {
        self.consecutive_timeouts.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// 返回当前连续超时计数。
    fn timeout_count(&self) -> u32 {
        self.consecutive_timeouts.load(Ordering::Relaxed)
    }
}

impl Clone for SubscriberSlot {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            capacity: self.capacity,
            consecutive_timeouts: self.consecutive_timeouts.clone(),
        }
    }
}

/// EventBus 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusConfig {
    /// 每个订阅者的默认通道容量。
    #[serde(default = "default_subscriber_capacity")]
    pub subscriber_capacity: usize,
    /// 可靠事件投递超时（毫秒）。
    /// 若订阅者通道已满，发布方最多等待此时长以获得容量，超时则记录错误。
    #[serde(default = "default_publish_timeout_ms")]
    pub publish_timeout_ms: u64,
    /// 修剪订阅者前的最大连续投递超时次数。
    /// 反复无法在超时内消费可靠事件的卡住订阅者将被移除，
    /// 防止级联延迟。设为 0 禁用修剪。
    #[serde(default = "default_max_subscriber_timeouts")]
    pub max_subscriber_timeouts: u32,
}

impl EventBusConfig {
    /// 默认订阅者通道容量。
    pub const DEFAULT_SUBSCRIBER_CAPACITY: usize = 256;
    /// 默认可靠事件投递超时（5s）。
    pub const DEFAULT_PUBLISH_TIMEOUT_MS: u64 = 5_000;
    /// 默认订阅者最大连续投递超时次数。
    pub const DEFAULT_MAX_SUBSCRIBER_TIMEOUTS: u32 = 5;
}

fn default_subscriber_capacity() -> usize {
    EventBusConfig::DEFAULT_SUBSCRIBER_CAPACITY
}
fn default_publish_timeout_ms() -> u64 {
    EventBusConfig::DEFAULT_PUBLISH_TIMEOUT_MS
}
fn default_max_subscriber_timeouts() -> u32 {
    EventBusConfig::DEFAULT_MAX_SUBSCRIBER_TIMEOUTS
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            subscriber_capacity: EventBusConfig::DEFAULT_SUBSCRIBER_CAPACITY,
            publish_timeout_ms: EventBusConfig::DEFAULT_PUBLISH_TIMEOUT_MS,
            max_subscriber_timeouts: EventBusConfig::DEFAULT_MAX_SUBSCRIBER_TIMEOUTS,
        }
    }
}

/// 基于话题的发布/订阅事件总线。
///
/// 使用 `DashMap` 实现订阅者列表的无锁并发访问。
/// 发布方调用 `publish()` 扇出到对应话题的订阅者。订阅者通过 `subscribe()` 获取 `Receiver<BusEvent>`。
///
/// ## 卡住订阅者保护
///
/// 当可靠事件投递超时，订阅者的连续超时计数器递增。达到 `max_subscriber_timeouts` 次
/// 连续超时后，该订阅者将被修剪（移除）。这可防止一个卡住的消费者阻塞整个事件管道。
/// 投递成功时计数器重置。
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<DashMap<Topic, Vec<SubscriberSlot>>>,
    /// 新订阅者的默认容量。
    default_capacity: usize,
    /// 可靠事件投递超时。
    publish_timeout: Duration,
    /// 修剪订阅者前的最大连续超时次数。
    max_subscriber_timeouts: u32,
    /// `TaskEnqueued` 专用唤醒信号。
    ///
    /// `notify_waiters()` 无队列、无丢弃，所有等待的 Worker 立即唤醒。
    /// `Arc` 允许 Worker 持有引用，无需 subscribe。
    task_enqueued_notify: Arc<tokio::sync::Notify>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::with_config(EventBusConfig::default())
    }
}

impl EventBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// 使用给定配置创建 EventBus。
    pub fn with_config(config: EventBusConfig) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            default_capacity: config.subscriber_capacity,
            publish_timeout: Duration::from_millis(config.publish_timeout_ms),
            max_subscriber_timeouts: config.max_subscriber_timeouts,
            task_enqueued_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// 返回 `TaskEnqueued` 唤醒信号的 `Arc<Notify>` 引用。
    ///
    /// Worker 持有此引用并 `notify.notified().await` 等待任务入队信号。
    /// `notify_waiters()` 唤醒所有等待者，支持多 Worker；无队列容量限制。
    pub fn task_enqueued_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.task_enqueued_notify)
    }

    /// 触发 `TaskEnqueued` 唤醒信号。
    ///
    /// 由 `SchedulerActor` 在 `enqueue` / `enqueue_batch` 后调用。
    /// `notify_waiters()` 允许所有正在 `.notified().await` 的 Worker 继续，
    /// 若无等待者则不存储信号（下次 `try_dequeue` 仍会拉取已入队任务）。
    pub fn notify_task_enqueued(&self) {
        self.task_enqueued_notify.notify_waiters();
    }

    /// 订阅话题。返回仅接收该话题 `BusEvent` 的接收器。
    pub fn subscribe(&self, topic: Topic) -> mpsc::Receiver<BusEvent> {
        self.subscribe_with_capacity(topic, self.default_capacity)
    }

    /// 使用自定义通道容量订阅话题。
    pub fn subscribe_with_capacity(
        &self,
        topic: Topic,
        capacity: usize,
    ) -> mpsc::Receiver<BusEvent> {
        let (tx, rx) = mpsc::channel(capacity);
        self.inner
            .entry(topic)
            .or_default()
            .value_mut()
            .push(SubscriberSlot::new(tx, capacity));
        rx
    }

    /// 异步发布事件。所有事件均可克隆并广播给该话题的所有订阅者。
    ///
    /// 对于 [DeliveryGuarantee::Reliable] 事件，发布方为每个订阅者等待容量，
    /// 最长 `publish_timeout_ms`。对于 [DeliveryGuarantee::BestEffort] 事件，
    /// 通道满的订阅者被跳过并告警。
    pub async fn publish(&self, event: BusEvent) {
        let topic = event.topic();
        let guarantee = event.delivery_guarantee();
        Self::broadcast_to_subscribers(
            &self.inner,
            topic,
            &event,
            guarantee,
            self.publish_timeout,
            self.max_subscriber_timeouts,
        )
        .await;
    }

    /// 将可克隆事件广播给话题的所有订阅者。
    ///
    /// 对于可靠事件，使用 `send_timeout` 等待容量。
    /// 对于尽力投递事件，使用 `try_send`（非阻塞）。
    /// 超过 `max_subscriber_timeouts` 次连续超时的订阅者将被修剪。
    async fn broadcast_to_subscribers(
        inner: &DashMap<Topic, Vec<SubscriberSlot>>,
        topic: Topic,
        event: &BusEvent,
        guarantee: DeliveryGuarantee,
        timeout: Duration,
        max_subscriber_timeouts: u32,
    ) {
        // 克隆 slots 以便在 await 前释放 DashMap guard。
        // 每个 slot 内的 Arc<AtomicU32> 与原始条目共享，
        // 因此通过克隆的更新在修剪时可见。
        let slots: Vec<SubscriberSlot> = inner
            .get(&topic)
            .map(|subs| subs.value().clone())
            .unwrap_or_default();

        if slots.is_empty() {
            return;
        }

        // 并发投递：所有订阅者的 send 同时进行。
        // 串行实现下总耗时 = Σ(各订阅者)，并发后 = max(各订阅者)。
        // 对 Reliable 事件尤为关键：一个慢订阅者不再阻塞其他订阅者的投递。
        // 使用 FuturesUnordered 流式推进：结果一旦就绪即处理，无需一次性
        // 分配 Vec<future> + Vec<result>，也便于后续按完成顺序触发修剪。
        // 每个 future 返回 bool（true = 该订阅者需修剪）。
        let mut futures = FuturesUnordered::new();
        for slot in slots {
            let event_ref = event;
            futures.push(async move {
                if slot.sender.is_closed() {
                    return true;
                }
                match guarantee {
                    DeliveryGuarantee::Reliable => {
                        // 所有 BusEvent 变体均可克隆；clone_broadcast() 返回 None
                        // 仅在防御性路径触发——日志后跳过该订阅者而非 panic。
                        let Some(cloned) = event_ref.clone_broadcast() else {
                            tracing::error!(
                                "EventBus: clone_broadcast returned None for {:?}; skipping subscriber",
                                topic
                            );
                            return false;
                        };
                        match slot.sender.send_timeout(cloned, timeout).await {
                            Ok(()) => {
                                slot.record_success();
                                false
                            }
                            Err(mpsc::error::SendTimeoutError::Closed(_)) => true,
                            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                                let count = slot.record_timeout();
                                crate::metrics::inc_event_bus_publish_timeout();
                                if max_subscriber_timeouts > 0
                                    && count >= max_subscriber_timeouts
                                {
                                    tracing::error!(
                                        "EventBus: pruning {:?} subscriber after {} consecutive timeouts",
                                        topic, count,
                                    );
                                    crate::metrics::inc_event_bus_subscriber_pruned();
                                    true
                                } else {
                                    tracing::error!(
                                        "EventBus: timeout delivering reliable event to {:?} subscriber \
                                         (waited {}ms, consecutive timeouts={}), subscriber may be stuck",
                                        topic,
                                        timeout.as_millis(),
                                        count,
                                    );
                                    false
                                }
                            }
                        }
                    }
                    DeliveryGuarantee::BestEffort => {
                        let Some(cloned) = event_ref.clone_broadcast() else {
                            tracing::error!(
                                "EventBus: clone_broadcast returned None for {:?}; skipping subscriber",
                                topic
                            );
                            return false;
                        };
                        match slot.sender.try_send(cloned) {
                            Ok(()) => {
                                slot.record_success();
                                false
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "EventBus: subscriber for {:?} is full, dropping best-effort event",
                                    topic
                                );
                                crate::metrics::inc_event_bus_dropped_events();
                                false
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => true,
                        }
                    }
                }
            });
        }

        let mut needs_prune = false;
        while let Some(prune) = futures.next().await {
            if prune {
                needs_prune = true;
            }
        }

        if needs_prune {
            if let Some(mut subs) = inner.get_mut(&topic) {
                subs.value_mut().retain(|s| {
                    if s.sender.is_closed() {
                        return false;
                    }
                    if max_subscriber_timeouts > 0 && s.timeout_count() >= max_subscriber_timeouts {
                        return false;
                    }
                    true
                });
            }
        }

        // 采样订阅者队列深度：取该 topic 所有订阅者当前积压深度的最大值。
        // 每次 publish 后采样，记录到 metrics gauge，用于预警队列堆积。
        // 采样时刻可能略晚于投递（订阅者已消费部分消息），但足以反映趋势。
        let max_depth = inner
            .get(&topic)
            .map(|subs| subs.value().iter().map(|s| s.depth()).max().unwrap_or(0))
            .unwrap_or(0) as u64;
        crate::metrics::set_event_bus_subscriber_depth(topic.as_str(), max_depth);
    }
}

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/event_bus.rs"]
mod tests;
