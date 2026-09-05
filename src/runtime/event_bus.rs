//! 非阻塞观测 tap：基于话题的发布/订阅事件分发。
//!
//! EventBus 是**纯观测通道**：投递尽力而为、可丢，**不承载任何正确性语义**。
//! 订阅者（指标、日志、Python 事件泵等）消费的是任务生命周期、网络对端、
//! Actor 生命周期等观测信号；事件丢失只影响观测完整性，不影响系统正确性。
//!
//! 控制面消息（心跳、claim、DAG gossip、任务结果回传）**不走 EventBus**：
//! 跨节点指令由 `NetworkEventRouter` 在 wire 消息解码后直连分发到目标
//! （FailoverManager / DagGossipActor / WorkflowActor），跨节点任务结果走
//! 专用直连通道（`DirectRequest::TaskResult` + 重试队列）。这保证控制面
//! 投递无损且点对点，观测慢/卡死绝不反压生产者热路径。
//!
//! 投递语义：`publish` 对每个订阅者执行同步 `try_send`，通道满即丢弃该
//! 事件（记 `actant.event_bus.publish.dropped` 计数 + `tracing::debug!`）。
//! 统一使用 debug 级别：tap 没有订阅者修剪机制，慢消费者会持续满载，
//! warn/error 会在热路径上刷屏；丢弃趋势通过 dropped 计数与
//! `actant.event_bus.subscriber.depth` gauge 观测。
//!
//! `publish` 无 await 点，可在同步或异步上下文直接调用。

use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::common::{NodeId, TaskCompletion, TaskId, WorkflowId};

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
    /// Actor 生命周期中的不可恢复错误（panic / 状态机非法转换 /
    /// 持久化失败等需要外部介入的错误）。
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
            Topic::ActorLifecycleError => "ActorLifecycleError",
            Topic::WalCompacted => "WalCompacted",
            Topic::WorkerLifecycle => "WorkerLifecycle",
        }
    }
}

/// 流经总线的统一事件类型。
///
/// 所有变体均可 `Clone`（广播时为每个订阅者克隆一份）。点对点请求-响应
/// （如 `DirectRequest`）不走 EventBus，而是由 `NetworkEventRouter`
/// 直接处理或回送 `DirectResponse::Error`。
#[derive(Debug, Clone)]
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

    // -- Actor 生命周期（可克隆）--
    /// Actor 生命周期中的不可恢复错误。
    ///
    /// 描述系统层拦截到的 panic / 状态机非法转换 / 持久化失败等需要
    /// 外部介入的错误。携带 actor_id 与错误描述，便于运维订阅并触发告警。
    /// 常规消息失败不走本事件，由 `tracing::error!` 与
    /// `inc_actors_failed` 指标承载。
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
            BusEvent::ActorLifecycleError { .. } => Topic::ActorLifecycleError,
            BusEvent::WalCompacted { .. } => Topic::WalCompacted,
            BusEvent::WorkerDraining { .. }
            | BusEvent::WorkerDrained { .. }
            | BusEvent::WorkerStopped { .. } => Topic::WorkerLifecycle,
        }
    }
}

/// 订阅者条目：包装 `mpsc::Sender` 与通道总容量。
struct SubscriberSlot {
    sender: mpsc::Sender<BusEvent>,
    /// 通道的总容量（创建时确定），用于计算当前积压深度。
    /// tokio mpsc 不暴露当前队列长度，需 `capacity - sender.capacity()` 间接求得。
    capacity: usize,
}

impl SubscriberSlot {
    fn new(sender: mpsc::Sender<BusEvent>, capacity: usize) -> Self {
        Self { sender, capacity }
    }

    /// 返回当前积压深度（已入队但未被消费的事件数）。
    /// `sender.capacity()` 返回剩余可用容量，故 `capacity - capacity()` 即积压数。
    fn depth(&self) -> usize {
        self.capacity.saturating_sub(self.sender.capacity())
    }
}

impl Clone for SubscriberSlot {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            capacity: self.capacity,
        }
    }
}

/// EventBus 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusConfig {
    /// 每个订阅者的默认通道容量。
    #[serde(default = "default_subscriber_capacity")]
    pub subscriber_capacity: usize,
}

impl EventBusConfig {
    /// 默认订阅者通道容量。
    pub const DEFAULT_SUBSCRIBER_CAPACITY: usize = 256;
}

fn default_subscriber_capacity() -> usize {
    EventBusConfig::DEFAULT_SUBSCRIBER_CAPACITY
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            subscriber_capacity: EventBusConfig::DEFAULT_SUBSCRIBER_CAPACITY,
        }
    }
}

/// 基于话题的非阻塞观测 tap。
///
/// 使用 `DashMap` 实现订阅者列表的无锁并发访问。
/// 发布方调用 `publish()` 以 `try_send` 扇出到对应话题的订阅者，通道满即丢。
/// 订阅者通过 `subscribe()` 获取 `Receiver<BusEvent>`。
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<DashMap<Topic, Vec<SubscriberSlot>>>,
    /// 新订阅者的默认容量。
    default_capacity: usize,
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

    /// 非阻塞发布事件：对所有订阅者执行 `try_send`，通道满即丢弃。
    ///
    /// 无 await 点，同步或异步上下文均可调用。事件丢失仅影响观测
    /// 完整性；正确性关键路径不依赖本通道（见模块文档）。
    pub fn publish(&self, event: BusEvent) {
        let topic = event.topic();
        Self::broadcast_to_subscribers(&self.inner, topic, &event);
    }

    /// 将事件以 `try_send` 广播给话题的所有订阅者，满即丢。
    ///
    /// 先克隆 slots 释放 DashMap guard 再投递；已关闭的订阅者在投递后
    /// 从表中移除。没有订阅者时为零开销快速返回。
    fn broadcast_to_subscribers(
        inner: &DashMap<Topic, Vec<SubscriberSlot>>,
        topic: Topic,
        event: &BusEvent,
    ) {
        // 克隆 slots 以便投递期间不持有 DashMap 写锁；克隆的 Sender 与
        // 原条目指向同一通道，try_send 结果对后续 prune 判定有效。
        let slots: Vec<SubscriberSlot> = inner
            .get(&topic)
            .map(|subs| subs.value().clone())
            .unwrap_or_default();

        if slots.is_empty() {
            return;
        }

        let mut needs_prune = false;
        for slot in slots {
            match slot.sender.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // tap 语义：慢消费者只丢事件不反压生产者。统一 debug
                    // 级别——没有订阅者修剪机制，慢消费者会持续满载，
                    // 更高级别会在热路径刷屏；趋势看 dropped 计数与
                    // subscriber.depth gauge。
                    tracing::debug!(
                        topic = topic.as_str(),
                        "event bus subscriber channel full, dropping observability event"
                    );
                    crate::metrics::inc_event_bus_publish_dropped();
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    needs_prune = true;
                }
            }
        }

        if needs_prune {
            if let Some(mut subs) = inner.get_mut(&topic) {
                subs.value_mut().retain(|s| !s.sender.is_closed());
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
