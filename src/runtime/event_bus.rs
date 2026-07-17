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
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::common::{
    NodeHeartbeat, NodeId, OrchestratorClaim, TaskCompletion, TaskId, WireDagStateUpdate,
    WorkflowId,
};
use crate::runtime::actor::SupervisionEvent;
use crate::runtime::network::DirectRequest;
use crate::runtime::network::DirectResponseChannel;

/// 标识总线上的事件类别。
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum Topic {
    /// 任务已在此 worker 上开始执行。
    TaskStarted,
    /// 任务已入队到调度器（唤醒 Worker 拉取）。
    ///
    /// 由 `SchedulerActor` 在 `enqueue` / `enqueue_batch` / `close` 后发布。
    /// 事件本身不携带任务数据——仅为唤醒信号，Worker 收到后通过
    /// `try_dequeue` 拉取实际任务。
    TaskEnqueued,
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
    /// 来自远端节点的直连请求。
    NetworkDirect,
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
    /// Worker 生命周期事件（排空中、已排空、已停止）。
    WorkerLifecycle,
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
/// 事件分为两类：
/// - **独占型**事件（含通道）只能投递给一个订阅者。
/// - **可克隆型**事件广播到该话题的所有订阅者。
#[derive(Debug)]
#[non_exhaustive]
pub enum BusEvent {
    // -- 任务生命周期（可克隆）--
    TaskStarted {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    /// 任务已入队到调度器（仅作唤醒信号，不携带任务数据）。
    ///
    /// 由 `SchedulerActor` 在 `enqueue` / `enqueue_batch` / `close` 后发布。
    /// Worker 订阅 `Topic::TaskEnqueued` 后在此事件上 await。
    /// 事件被丢弃（通道满）是安全的——Worker 已在前一次唤醒中
    /// 处理任务，下次 `try_dequeue` 会拉取剩余任务。
    TaskEnqueued,
    TaskCompleted(TaskCompletion),
    TaskFailed(TaskCompletion),
    TaskCancelled(TaskCompletion),
    TaskSkipped(TaskCompletion),

    // -- 网络 --
    PeerConnected(NodeId),
    PeerDisconnected(NodeId),
    /// 独占：包含无法克隆的响应通道。
    /// 总是只投递给一个订阅者。
    DirectRequest {
        peer_id: String,
        request: Box<DirectRequest>,
        channel: DirectResponseChannel,
    },

    // -- 集群管理（可克隆）--
    Heartbeat(NodeHeartbeat),
    Claim(OrchestratorClaim),
    DagUpdate(WireDagStateUpdate),
    HeadsExchange(crate::common::HeadsExchange),

    // -- Actor 监管（可克隆）--
    SupervisionEvent(SupervisionEvent),

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
            BusEvent::TaskEnqueued => Topic::TaskEnqueued,
            BusEvent::TaskCompleted(_) => Topic::TaskCompleted,
            BusEvent::TaskFailed(_) => Topic::TaskFailed,
            BusEvent::TaskCancelled(_) => Topic::TaskCancelled,
            BusEvent::TaskSkipped(_) => Topic::TaskSkipped,
            BusEvent::PeerConnected(_) | BusEvent::PeerDisconnected(_) => Topic::NetworkPeer,
            BusEvent::DirectRequest { .. } => Topic::NetworkDirect,
            BusEvent::Heartbeat(_) => Topic::ClusterHeartbeat,
            BusEvent::Claim(_) => Topic::ClusterClaim,
            BusEvent::DagUpdate(_) => Topic::DagUpdate,
            BusEvent::HeadsExchange(_) => Topic::HeadsExchange,
            BusEvent::SupervisionEvent(_) => Topic::Supervision,
            BusEvent::WorkerDraining { .. }
            | BusEvent::WorkerDrained { .. }
            | BusEvent::WorkerStopped { .. } => Topic::WorkerLifecycle,
        }
    }

    /// 返回此事件是否可克隆并广播给所有订阅者。
    /// 仅 `DirectRequest` 是独占型（包含一次性通道）。
    fn is_cloneable(&self) -> bool {
        !matches!(self, BusEvent::DirectRequest { .. })
    }

    /// 克隆此事件用于广播给多个订阅者。
    ///
    /// 仅对可广播事件返回 `Some`。`DirectRequest` 包含一次性响应通道，
    /// 无法克隆，返回 `None`。
    ///
    /// 通过将"独占事件不可克隆"这一不变式表达为类型安全的 `Option` 返回值，
    /// 避免在 `DirectRequest` 上实现 `Clone` 引发运行时 panic。
    /// 调用方（`broadcast_to_subscribers`）由 `is_cloneable()` 守卫，
    /// 确保 `None` 分支在实践中不会触发；即便因 bug 触发，
    /// 也会被防御性地跳过而非崩溃。
    fn clone_broadcast(&self) -> Option<BusEvent> {
        Some(match self {
            BusEvent::TaskStarted {
                workflow_id,
                task_id,
            } => BusEvent::TaskStarted {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
            },
            BusEvent::TaskEnqueued => BusEvent::TaskEnqueued,
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
            BusEvent::WorkerDraining { node_id } => BusEvent::WorkerDraining {
                node_id: node_id.clone(),
            },
            BusEvent::WorkerDrained { node_id } => BusEvent::WorkerDrained {
                node_id: node_id.clone(),
            },
            BusEvent::WorkerStopped { node_id } => BusEvent::WorkerStopped {
                node_id: node_id.clone(),
            },
            BusEvent::DirectRequest { .. } => return None,
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
            // DAG 更新驱动状态复制，DirectRequest 包含调用方等待的响应通道。
            BusEvent::TaskStarted { .. }
            | BusEvent::TaskCompleted(_)
            | BusEvent::TaskFailed(_)
            | BusEvent::TaskCancelled(_)
            | BusEvent::TaskSkipped(_)
            | BusEvent::DirectRequest { .. }
            | BusEvent::Claim(_)
            | BusEvent::DagUpdate(_) => DeliveryGuarantee::Reliable,

            // 周期性/被覆盖：下一次心跳或对端事件会覆盖当前事件，丢弃可接受。
            // TaskEnqueued 同理——事件仅为唤醒信号，丢弃后 Worker 下次
            // try_dequeue 仍会拉取已入队任务。
            BusEvent::TaskEnqueued
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
    /// 共享的连续超时计数器。Arc 允许在 DashMap guard 释放后通过克隆的 slot 更新。
    consecutive_timeouts: Arc<AtomicU32>,
}

impl SubscriberSlot {
    fn new(sender: mpsc::Sender<BusEvent>) -> Self {
        Self {
            sender,
            consecutive_timeouts: Arc::new(AtomicU32::new(0)),
        }
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
        }
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
            .push(SubscriberSlot::new(tx));
        rx
    }

    /// 异步发布事件。可克隆事件广播给所有订阅者；独占事件只投递给一个。
    ///
    /// 对于 [DeliveryGuarantee::Reliable] 事件，发布方为每个订阅者等待容量，
    /// 最长 `publish_timeout_ms`。对于 [DeliveryGuarantee::BestEffort] 事件，
    /// 通道满的订阅者被跳过并告警。
    pub async fn publish(&self, event: BusEvent) {
        let topic = event.topic();
        let guarantee = event.delivery_guarantee();
        if event.is_cloneable() {
            Self::broadcast_to_subscribers(
                &self.inner,
                topic,
                &event,
                guarantee,
                self.publish_timeout,
                self.max_subscriber_timeouts,
            )
            .await;
        } else {
            Self::dispatch_exclusive(
                &self.inner,
                topic,
                event,
                guarantee,
                self.publish_timeout,
                self.max_subscriber_timeouts,
            )
            .await;
        }
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

        let mut needs_prune = false;

        for slot in &slots {
            if slot.sender.is_closed() {
                needs_prune = true;
                continue;
            }

            match guarantee {
                DeliveryGuarantee::Reliable => {
                    // 由 publish() 中的 is_cloneable() 守卫，此处必为可广播事件；
                    // clone_broadcast() 返回 None 仅在 DirectRequest 上发生，
                    // 防御性跳过而非 panic。
                    let Some(event) = event.clone_broadcast() else {
                        tracing::error!(
                            "EventBus: clone_broadcast returned None for {:?}; skipping subscriber",
                            topic
                        );
                        continue;
                    };
                    match slot.sender.send_timeout(event, timeout).await {
                        Ok(()) => {
                            slot.record_success();
                        }
                        Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                            needs_prune = true;
                        }
                        Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                            let count = slot.record_timeout();
                            if max_subscriber_timeouts > 0 && count >= max_subscriber_timeouts {
                                tracing::error!(
                                    "EventBus: pruning {:?} subscriber after {} consecutive timeouts",
                                    topic, count,
                                );
                                crate::metrics::inc_event_bus_subscriber_pruned();
                                needs_prune = true;
                            } else {
                                tracing::error!(
                                    "EventBus: timeout delivering reliable event to {:?} subscriber \
                                     (waited {}ms, consecutive timeouts={}), subscriber may be stuck",
                                    topic,
                                    timeout.as_millis(),
                                    count,
                                );
                            }
                            crate::metrics::inc_event_bus_publish_timeout();
                        }
                    }
                }
                DeliveryGuarantee::BestEffort => {
                    let Some(event) = event.clone_broadcast() else {
                        tracing::error!(
                            "EventBus: clone_broadcast returned None for {:?}; skipping subscriber",
                            topic
                        );
                        continue;
                    };
                    match slot.sender.try_send(event) {
                        Ok(()) => {
                            slot.record_success();
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            tracing::warn!(
                                "EventBus: subscriber for {:?} is full, dropping best-effort event",
                                topic
                            );
                            crate::metrics::inc_event_bus_dropped_events();
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            needs_prune = true;
                        }
                    }
                }
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
    }

    /// 将独占（不可克隆）事件投递给第一个有容量的订阅者。
    ///
    /// 对于可靠事件，使用 `send_timeout` 等待容量。
    /// 若某个订阅者超时，尝试下一个订阅者。
    /// 对于尽力投递事件，使用 `try_send`（非阻塞）。
    async fn dispatch_exclusive(
        inner: &DashMap<Topic, Vec<SubscriberSlot>>,
        topic: Topic,
        event: BusEvent,
        guarantee: DeliveryGuarantee,
        timeout: Duration,
        max_subscriber_timeouts: u32,
    ) {
        let slots: Vec<SubscriberSlot> = inner
            .get(&topic)
            .map(|subs| subs.value().clone())
            .unwrap_or_default();

        if slots.is_empty() {
            // 无订阅者：DirectRequest 必须主动回送错误响应，否则调用方永久阻塞。
            if let BusEvent::DirectRequest {
                peer_id, channel, ..
            } = event
            {
                tracing::warn!(
                    peer = %peer_id,
                    "EventBus: no subscriber for DirectRequest on {:?}, returning error",
                    topic,
                );
                crate::metrics::inc_event_bus_dropped_events();
                channel
                    .send_error(format!(
                        "no subscriber registered on topic {:?} for DirectRequest",
                        topic
                    ))
                    .await;
            }
            return;
        }

        let mut event_opt = Some(event);
        let mut needs_prune = false;

        for slot in &slots {
            if slot.sender.is_closed() {
                needs_prune = true;
                continue;
            }

            let Some(ev) = event_opt.take() else {
                break;
            };

            match guarantee {
                DeliveryGuarantee::Reliable => match slot.sender.send_timeout(ev, timeout).await {
                    Ok(()) => {
                        slot.record_success();
                        break;
                    }
                    Err(mpsc::error::SendTimeoutError::Closed(ev)) => {
                        event_opt = Some(ev);
                        needs_prune = true;
                    }
                    Err(mpsc::error::SendTimeoutError::Timeout(ev)) => {
                        event_opt = Some(ev);
                        let count = slot.record_timeout();
                        if max_subscriber_timeouts > 0 && count >= max_subscriber_timeouts {
                            tracing::error!(
                                "EventBus: pruning {:?} subscriber after {} consecutive timeouts",
                                topic,
                                count,
                            );
                            crate::metrics::inc_event_bus_subscriber_pruned();
                            needs_prune = true;
                        } else {
                            tracing::error!(
                                    "EventBus: timeout delivering exclusive reliable event on {:?} \
                                     (waited {}ms, consecutive timeouts={}), trying next subscriber",
                                    topic,
                                    timeout.as_millis(),
                                    count,
                                );
                        }
                        crate::metrics::inc_event_bus_publish_timeout();
                    }
                },
                DeliveryGuarantee::BestEffort => match slot.sender.try_send(ev) {
                    Ok(()) => {
                        slot.record_success();
                        break;
                    }
                    Err(mpsc::error::TrySendError::Full(ev)) => {
                        event_opt = Some(ev);
                    }
                    Err(mpsc::error::TrySendError::Closed(ev)) => {
                        event_opt = Some(ev);
                        needs_prune = true;
                    }
                },
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

        if let Some(undelivered) = event_opt {
            tracing::error!(
                "EventBus: no subscriber could accept event on {:?}, event dropped",
                topic
            );
            crate::metrics::inc_event_bus_dropped_events();
            // DirectRequest 包含调用方等待的响应通道。若直接丢弃，调用方将永久
            // 阻塞在 `await` 上等待响应。通过 channel 主动回送 `DirectResponse::Error`，
            // 让调用方能立即收到明确错误并 fallback 到自身超时。
            if let BusEvent::DirectRequest {
                peer_id, channel, ..
            } = undelivered
            {
                tracing::warn!(
                    peer = %peer_id,
                    "EventBus: returning DirectResponse::Error for undeliverable DirectRequest",
                );
                channel
                    .send_error(format!(
                        "no subscriber available on topic {:?} to handle DirectRequest",
                        topic
                    ))
                    .await;
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/event_bus.rs"]
mod tests;
