//! 基于话题的事件总线，用于内部解耦通信。
//!
//! 用单一的发布/订阅中枢取代分散的通道（completion_tx、worker_event_tx、
//! event_forward、orch_tx）。模块将事件发布到话题，订阅者只接收自己关心的话题。
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

/// 每次发布事件后调用的回调。
type OnPublishFn = Arc<dyn Fn(&BusEvent) + Send + Sync>;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::actor::supervision::SupervisionEvent;
use crate::common::{
    NodeHeartbeat, NodeId, OrchestratorClaim, TaskCompletion, TaskId, WireDagStateUpdate,
    WorkflowId,
};
use crate::network::protocol::DirectRequest;
use crate::network::DirectResponseChannel;

// ---------------------------------------------------------------------------
// Topic
// ---------------------------------------------------------------------------

/// 标识总线上的事件类别。
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum Topic {
    /// 任务已在此 worker 上开始执行。
    TaskStarted,
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

// ---------------------------------------------------------------------------
// DeliveryGuarantee
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// BusEvent
// ---------------------------------------------------------------------------

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
    /// 此方法取代了原本会在 `DirectRequest` 上 panic 的 `Clone` 实现，
    /// 将"独占事件不可克隆"这一不变式从运行时 panic 提升为类型安全的
    /// `Option` 返回值。调用方（`broadcast_to_subscribers`）由
    /// `is_cloneable()` 守卫，确保 `None` 分支在实践中不会触发；
    /// 即便因 bug 触发，也会被防御性地跳过而非崩溃。
    fn clone_broadcast(&self) -> Option<BusEvent> {
        Some(match self {
            BusEvent::TaskStarted {
                workflow_id,
                task_id,
            } => BusEvent::TaskStarted {
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
            BusEvent::PeerConnected(_)
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

// ---------------------------------------------------------------------------
// SubscriberSlot
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// EventBusConfig
// ---------------------------------------------------------------------------

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

fn default_subscriber_capacity() -> usize {
    256
}
fn default_publish_timeout_ms() -> u64 {
    5000
}
fn default_max_subscriber_timeouts() -> u32 {
    5
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            subscriber_capacity: default_subscriber_capacity(),
            publish_timeout_ms: default_publish_timeout_ms(),
            max_subscriber_timeouts: default_max_subscriber_timeouts(),
        }
    }
}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

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
///
/// 可设置可选的 `on_publish` 回调，在事件分发时通知外部等待者（如 Python 桥接）。
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<DashMap<Topic, Vec<SubscriberSlot>>>,
    /// 新订阅者的默认容量。
    default_capacity: usize,
    /// 可靠事件投递超时。
    publish_timeout: Duration,
    /// 修剪订阅者前的最大连续超时次数。
    max_subscriber_timeouts: u32,
    /// 每次发布后调用的可选回调。
    /// 接收已发布事件的引用，使订阅者可直接处理而无需轮询。
    on_publish: Option<OnPublishFn>,
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
            on_publish: None,
        }
    }

    /// 设置每次 `publish()` 后调用的回调。
    /// 接收已发布事件的引用。
    pub fn with_on_publish(mut self, cb: Arc<dyn Fn(&BusEvent) + Send + Sync>) -> Self {
        self.on_publish = Some(cb);
        self
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

    /// 订阅多个话题。返回单个接收器，接收所有指定话题的事件。
    pub fn subscribe_many(&self, topics: &[Topic]) -> mpsc::Receiver<BusEvent> {
        self.subscribe_many_with_capacity(topics, self.default_capacity)
    }

    /// 使用自定义通道容量订阅多个话题。
    pub fn subscribe_many_with_capacity(
        &self,
        topics: &[Topic],
        capacity: usize,
    ) -> mpsc::Receiver<BusEvent> {
        let (tx, rx) = mpsc::channel(capacity);
        let slot = SubscriberSlot::new(tx);
        for &topic in topics {
            self.inner
                .entry(topic)
                .or_default()
                .value_mut()
                .push(slot.clone());
        }
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
        // 在分发前触发 on_publish，确保无论分发是否接管所有权（DirectRequest），
        // 回调都能获得事件。
        if let Some(ref cb) = self.on_publish {
            cb(&event);
        }
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

    /// 将事件发布到指定话题（覆盖事件自然所属话题）。
    pub async fn publish_to(&self, topic: Topic, event: BusEvent) {
        let guarantee = event.delivery_guarantee();
        if let Some(ref cb) = self.on_publish {
            cb(&event);
        }
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

        if event_opt.is_some() {
            tracing::error!(
                "EventBus: no subscriber could accept event on {:?}, event dropped",
                topic
            );
            crate::metrics::inc_event_bus_dropped_events();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_publish_subscribe() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe(Topic::TaskStarted);

        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        let event = rx.try_recv().unwrap();
        assert!(matches!(event, BusEvent::TaskStarted { .. }));
    }

    #[tokio::test]
    async fn test_subscribe_many() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe_many(&[Topic::TaskCompleted, Topic::TaskFailed]);

        bus.publish(BusEvent::TaskCompleted(TaskCompletion::Completed {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
            task_name: "task1".into(),
            result: vec![],
            target_node: None,
        }))
        .await;

        let event = rx.try_recv().unwrap();
        assert!(matches!(event, BusEvent::TaskCompleted(_)));
    }

    #[tokio::test]
    async fn test_unrelated_topic_not_received() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe(Topic::TaskStarted);

        bus.publish(BusEvent::Heartbeat(NodeHeartbeat {
            node_id: NodeId("n-1".into()),
            active_workflows: vec![],
            timestamp_ms: 0,
            available_slots: 0,
            max_slots: 0,
            endpoint_addr: None,
        }))
        .await;

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_prune_closed_subscriber() {
        let bus = EventBus::new();
        {
            let _rx = bus.subscribe(Topic::TaskStarted);
            // rx dropped here
        }

        // 不应 panic；已关闭的订阅者会被剪除。
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;
    }

    #[tokio::test]
    async fn test_multiple_subscribers_broadcast() {
        let bus = EventBus::new();
        let mut rx1 = bus.subscribe(Topic::TaskStarted);
        let mut rx2 = bus.subscribe(Topic::TaskStarted);

        // 可克隆事件广播给所有订阅者
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        // 两个订阅者都应收到事件
        let got1 = rx1.try_recv().unwrap();
        let got2 = rx2.try_recv().unwrap();
        assert!(matches!(got1, BusEvent::TaskStarted { .. }));
        assert!(matches!(got2, BusEvent::TaskStarted { .. }));
        let _ = (rx1, rx2);
    }

    #[tokio::test]
    async fn test_subscribe_many_receives_from_all_topics() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe_many(&[Topic::TaskCompleted, Topic::TaskFailed]);

        bus.publish(BusEvent::TaskFailed(TaskCompletion::Failed {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
            task_name: "task1".into(),
            error: "boom".into(),
            target_node: None,
        }))
        .await;

        let event = rx.try_recv().unwrap();
        assert!(matches!(event, BusEvent::TaskFailed(_)));
    }

    #[tokio::test]
    async fn test_reliable_event_waits_for_capacity() {
        // 容量为 1 — 第二次 publish 必须等待订阅者排空。
        let bus = EventBus::with_config(EventBusConfig {
            subscriber_capacity: 1,
            publish_timeout_ms: 5000,
            max_subscriber_timeouts: 5,
        });
        let mut rx = bus.subscribe_with_capacity(Topic::TaskStarted, 1);

        // 第一个事件填满 channel。
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        // 第二个事件：channel 已满，但 Reliable 保证会等待。
        // 启动一个任务在短暂延迟后排空 receiver。
        let rx_drain = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let _ = rx.try_recv().unwrap();
        });

        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-2".into()),
            task_id: TaskId("t-2".into()),
        })
        .await;

        rx_drain.await.unwrap();
    }

    #[tokio::test]
    async fn test_best_effort_event_dropped_when_full() {
        // Heartbeat 是 BestEffort — channel 满时应被丢弃。
        let bus = EventBus::with_config(EventBusConfig {
            subscriber_capacity: 1,
            publish_timeout_ms: 100,
            max_subscriber_timeouts: 5,
        });
        let mut rx = bus.subscribe_with_capacity(Topic::ClusterHeartbeat, 1);

        // 填满 channel。
        bus.publish(BusEvent::Heartbeat(NodeHeartbeat {
            node_id: NodeId("n-1".into()),
            active_workflows: vec![],
            timestamp_ms: 1,
            available_slots: 0,
            max_slots: 0,
            endpoint_addr: None,
        }))
        .await;

        // 第二次 heartbeat 应被丢弃（BestEffort）。
        bus.publish(BusEvent::Heartbeat(NodeHeartbeat {
            node_id: NodeId("n-1".into()),
            active_workflows: vec![],
            timestamp_ms: 2,
            available_slots: 0,
            max_slots: 0,
            endpoint_addr: None,
        }))
        .await;

        // channel 中应仅有一个事件。
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, BusEvent::Heartbeat(_)));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_no_subscribers_event_dropped() {
        let bus = EventBus::new();
        // 向无订阅者的 topic publish 不应 panic
        bus.publish(BusEvent::PeerConnected(NodeId("n-1".into())))
            .await;
    }

    #[tokio::test]
    async fn test_on_publish_callback_fires() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();

        let bus = EventBus::new().with_on_publish(Arc::new(move |_| {
            notify_clone.notify_one();
        }));

        let mut rx = bus.subscribe(Topic::TaskStarted);

        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        // Notify 应已触发
        let notified =
            tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified()).await;
        assert!(
            notified.is_ok(),
            "on_publish callback should have fired Notify"
        );

        // 事件也应能在 receiver 中获取
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, BusEvent::TaskStarted { .. }));
    }

    #[tokio::test]
    async fn test_on_publish_callback_with_no_subscribers() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();

        let bus = EventBus::new().with_on_publish(Arc::new(move |_| {
            notify_clone.notify_one();
        }));

        // 无订阅者时 publish — 回调仍应触发
        bus.publish(BusEvent::PeerConnected(NodeId("n-1".into())))
            .await;

        let notified =
            tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified()).await;
        assert!(
            notified.is_ok(),
            "on_publish should fire even with no subscribers"
        );
    }

    #[tokio::test]
    async fn test_delivery_guarantee_classification() {
        // Reliable 事件
        assert_eq!(
            BusEvent::TaskStarted {
                workflow_id: WorkflowId("w".into()),
                task_id: TaskId("t".into())
            }
            .delivery_guarantee(),
            DeliveryGuarantee::Reliable,
        );
        assert_eq!(
            BusEvent::Claim(OrchestratorClaim {
                node_id: NodeId("n".into()),
                workflow_id: WorkflowId("w".into()),
                timestamp_ms: 0,
            })
            .delivery_guarantee(),
            DeliveryGuarantee::Reliable,
        );

        // BestEffort events
        assert_eq!(
            BusEvent::Heartbeat(NodeHeartbeat {
                node_id: NodeId("n".into()),
                active_workflows: vec![],
                timestamp_ms: 0,
                available_slots: 0,
                max_slots: 0,
                endpoint_addr: None,
            })
            .delivery_guarantee(),
            DeliveryGuarantee::BestEffort,
        );
        assert_eq!(
            BusEvent::PeerConnected(NodeId("n".into())).delivery_guarantee(),
            DeliveryGuarantee::BestEffort,
        );
    }

    #[tokio::test]
    async fn test_stuck_subscriber_pruned_after_consecutive_timeouts() {
        // Configure: very short timeout, prune after 2 consecutive timeouts
        let bus = EventBus::with_config(EventBusConfig {
            subscriber_capacity: 1,
            publish_timeout_ms: 10,
            max_subscriber_timeouts: 2,
        });

        // 订阅容量为 1 但从不排空 — 订阅者卡住
        let _rx = bus.subscribe_with_capacity(Topic::TaskStarted, 1);

        // 第一次 publish 填满 channel
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        // 第二次 publish：超时 #1
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-2".into()),
            task_id: TaskId("t-2".into()),
        })
        .await;

        // 第三次 publish：超时 #2 — 订阅者应被剪除
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-3".into()),
            task_id: TaskId("t-3".into()),
        })
        .await;

        // 剪除后，订阅者列表应为空
        assert!(
            bus.inner
                .get(&Topic::TaskStarted)
                .is_none_or(|s| s.is_empty()),
            "stuck subscriber should have been pruned",
        );
    }

    #[tokio::test]
    async fn test_timeout_counter_resets_on_success() {
        let bus = EventBus::with_config(EventBusConfig {
            subscriber_capacity: 1,
            publish_timeout_ms: 10,
            max_subscriber_timeouts: 3,
        });

        let mut rx = bus.subscribe_with_capacity(Topic::TaskStarted, 1);

        // 填满 channel
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        // 第二次 publish：channel 已满，触发超时 #1
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        // 排空 channel — 下次 publish 应成功并重置计数器
        let _ = rx.try_recv().unwrap();

        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-3".into()),
            task_id: TaskId("t-3".into()),
        })
        .await;

        // 订阅者不应被剪除（计数器已重置）
        assert!(
            bus.inner
                .get(&Topic::TaskStarted)
                .is_some_and(|s| !s.is_empty()),
            "subscriber should still exist after successful delivery reset the counter",
        );
    }

    #[tokio::test]
    async fn test_reliable_event_timeout_drops_event() {
        // 当 reliable 事件超时且订阅者尚未被剪除时，
        // 该事件会被丢弃（不投递）。
        let bus = EventBus::with_config(EventBusConfig {
            subscriber_capacity: 1,
            publish_timeout_ms: 10,
            max_subscriber_timeouts: 100, // high threshold so subscriber is not pruned
        });

        let mut rx = bus.subscribe_with_capacity(Topic::TaskStarted, 1);

        // 填满 channel
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        // 此 reliable 事件将超时并被丢弃
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-2".into()),
            task_id: TaskId("t-2".into()),
        })
        .await;

        // channel 中应只有第一个事件
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, BusEvent::TaskStarted { .. }));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_pruning_disabled_when_max_is_zero() {
        let bus = EventBus::with_config(EventBusConfig {
            subscriber_capacity: 1,
            publish_timeout_ms: 10,
            max_subscriber_timeouts: 0, // pruning disabled
        });

        let _rx = bus.subscribe_with_capacity(Topic::TaskStarted, 1);

        // 填满 channel
        bus.publish(BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        })
        .await;

        // 多次超时 — 订阅者不应被剪除
        for i in 0..5 {
            bus.publish(BusEvent::TaskStarted {
                workflow_id: WorkflowId(format!("wf-{}", i)),
                task_id: TaskId(format!("t-{}", i)),
            })
            .await;
        }

        assert!(
            bus.inner
                .get(&Topic::TaskStarted)
                .is_some_and(|s| !s.is_empty()),
            "subscriber should not be pruned when max_subscriber_timeouts is 0",
        );
    }

    // -----------------------------------------------------------------------
    // DirectRequest 独占投递路径
    //
    // DirectRequest 是唯一不可克隆的事件（包含一次性响应通道）。
    // 这些测试验证：(1) 此前会 panic 的 Clone 实现已移除；(2) 独占投递只
    // 交付给一个订阅者；(3) 无订阅者时不 panic。
    // -----------------------------------------------------------------------

    #[test]
    fn direct_request_is_not_cloneable() {
        // is_cloneable() 守卫：DirectRequest 必须返回 false，
        // 使 publish() 走 dispatch_exclusive 而非 broadcast 路径。
        let event = BusEvent::DirectRequest {
            peer_id: "peer-1".into(),
            request: Box::new(DirectRequest::QueryWorkflowState {
                workflow_id: WorkflowId("wf-1".into()),
                requesting_node: NodeId("n-1".into()),
            }),
            channel: crate::network::DirectResponseChannel::test_stub(),
        };
        assert!(
            !event.is_cloneable(),
            "DirectRequest must not be cloneable (carries one-shot response channel)"
        );
    }

    #[test]
    fn direct_request_clone_broadcast_returns_none() {
        // clone_broadcast() 取代了原会在 DirectRequest 上 panic 的 Clone 实现。
        // 对 DirectRequest 必须返回 None，而非触发 unreachable!。
        let event = BusEvent::DirectRequest {
            peer_id: "peer-1".into(),
            request: Box::new(DirectRequest::QueryWorkflowState {
                workflow_id: WorkflowId("wf-1".into()),
                requesting_node: NodeId("n-1".into()),
            }),
            channel: crate::network::DirectResponseChannel::test_stub(),
        };
        assert!(
            event.clone_broadcast().is_none(),
            "clone_broadcast() must return None for DirectRequest, not panic"
        );
    }

    #[test]
    fn direct_request_delivery_guarantee_is_reliable() {
        // DirectRequest 包含调用方等待的响应通道，必须使用 Reliable 投递。
        let event = BusEvent::DirectRequest {
            peer_id: "peer-1".into(),
            request: Box::new(DirectRequest::QueryWorkflowState {
                workflow_id: WorkflowId("wf-1".into()),
                requesting_node: NodeId("n-1".into()),
            }),
            channel: crate::network::DirectResponseChannel::test_stub(),
        };
        assert_eq!(
            event.delivery_guarantee(),
            DeliveryGuarantee::Reliable,
            "DirectRequest must use Reliable delivery (caller awaits response)"
        );
    }

    #[tokio::test]
    async fn direct_request_delivered_to_single_subscriber() {
        // 独占投递：只有一个订阅者应收到 DirectRequest。
        let bus = EventBus::new();
        let mut rx1 = bus.subscribe(Topic::NetworkDirect);
        let mut rx2 = bus.subscribe(Topic::NetworkDirect);

        let request = DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId("wf-1".into()),
            requesting_node: NodeId("n-1".into()),
        };
        bus.publish(BusEvent::DirectRequest {
            peer_id: "peer-1".into(),
            request: Box::new(request),
            channel: crate::network::DirectResponseChannel::test_stub(),
        })
        .await;

        // 第一个订阅者应收到事件
        let event1 = tokio::time::timeout(std::time::Duration::from_millis(100), rx1.recv())
            .await
            .expect("first subscriber should receive DirectRequest")
            .expect("event should be Some");

        assert!(
            matches!(event1, BusEvent::DirectRequest { .. }),
            "first subscriber should receive a DirectRequest event"
        );

        // 第二个订阅者不应收到事件（独占投递）
        let event2 = tokio::time::timeout(std::time::Duration::from_millis(50), rx2.recv()).await;
        assert!(
            event2.is_err(),
            "second subscriber must NOT receive DirectRequest (exclusive dispatch)"
        );
    }

    #[tokio::test]
    async fn direct_request_with_no_subscribers_does_not_panic() {
        // 无订阅者时 publish DirectRequest — 不应 panic（此前 Clone 路径可能触发）。
        let bus = EventBus::new();
        // 故意不订阅 NetworkDirect
        bus.publish(BusEvent::DirectRequest {
            peer_id: "peer-1".into(),
            request: Box::new(DirectRequest::QueryWorkflowState {
                workflow_id: WorkflowId("wf-1".into()),
                requesting_node: NodeId("n-1".into()),
            }),
            channel: crate::network::DirectResponseChannel::test_stub(),
        })
        .await;
        // 到达此行即说明未 panic；事件被丢弃并记录（metrics counter 递增）。
    }

    #[tokio::test]
    async fn direct_request_delivered_even_with_other_topic_subscribers() {
        // 订阅其他话题的订阅者不应接收 DirectRequest。
        let bus = EventBus::new();
        let mut direct_rx = bus.subscribe(Topic::NetworkDirect);
        let mut task_rx = bus.subscribe(Topic::TaskStarted);

        bus.publish(BusEvent::DirectRequest {
            peer_id: "peer-1".into(),
            request: Box::new(DirectRequest::QueryWorkflowState {
                workflow_id: WorkflowId("wf-1".into()),
                requesting_node: NodeId("n-1".into()),
            }),
            channel: crate::network::DirectResponseChannel::test_stub(),
        })
        .await;

        // NetworkDirect 订阅者收到
        let event = tokio::time::timeout(std::time::Duration::from_millis(100), direct_rx.recv())
            .await
            .expect("NetworkDirect subscriber should receive DirectRequest");
        assert!(matches!(event, Some(BusEvent::DirectRequest { .. })));

        // TaskStarted 订阅者不应收到
        let leaked =
            tokio::time::timeout(std::time::Duration::from_millis(50), task_rx.recv()).await;
        assert!(
            leaked.is_err(),
            "DirectRequest must not leak to subscribers of unrelated topics"
        );
    }

    #[tokio::test]
    async fn broadcast_event_clone_broadcast_returns_some_for_cloneable() {
        // 对照测试：可广播事件的 clone_broadcast() 必须返回 Some。
        // 验证 clone_broadcast() 的契约在可克隆事件上正确生效。
        let event = BusEvent::TaskStarted {
            workflow_id: WorkflowId("wf-1".into()),
            task_id: TaskId("t-1".into()),
        };
        assert!(
            event.clone_broadcast().is_some(),
            "cloneable events must return Some from clone_broadcast()"
        );
    }
}
