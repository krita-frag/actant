//! 进程内事件的两个专用原语。
//!
//! 旧统一 EventBus 把四种互斥语义叠在同一张 DashMap 上（tap 满即丢 / 无损
//! 结果 / 逐任务日志桶 / 入队唤醒信号），本模块按语义拆回标准原语：
//!
//! - [`TaskResultFeed`]：任务生命周期事件的 **correctness 通道**——mpsc 无界
//!   队列、单接收端（绑定层结果泵）、FIFO 保序（Started 与终态同通道，天然
//!   有序）、永不丢弃。终态事件承载 `AsyncResult` 结算 / batch 归零 / flow
//!   推进 / orchestrator 回灌，丢失即句柄永久悬挂。无界是安全取舍：在途
//!   事件数受任务并发上界约束；若绑定层泵所在进程停摆（如 GIL 被长期
//!   占用），那是进程级故障，通道丢弃只会静默吞掉终态。
//! - [`EventTap`]：观测事件的**可丢通道**——tokio broadcast，慢消费者经
//!   `Lagged` 自动跳过最旧事件（计数到 `actant.tap.lagged_total`），绝不
//!   反压生产者热路径。`publish` 一次 send、无逐订阅者深拷贝；无订阅者时
//!   零开销。
//!
//! 入队唤醒信号不属于事件系统：`InnerScheduler` 各变体持有的 `Notify`
//! 直接承担（快路径 enqueue 与 Worker 等待共享同一实例）。

use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use crate::common::{ActorId, NodeId, TaskCompletion, TaskId, WorkflowId};

/// 任务生命周期事件（correctness 通道载荷）。
///
/// Started 与三种终态共用一条 FIFO 通道：接收端不会观察到终态先于 Started
/// 的乱序（旧总线四通道并发投递无此保证）。
#[derive(Debug, Clone)]
pub enum TaskLifecycleEvent {
    /// 任务已在本 worker 开始执行（非终态）。
    TaskStarted {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    TaskCompleted(TaskCompletion),
    TaskFailed(TaskCompletion),
    TaskCancelled(TaskCompletion),
}

impl TaskLifecycleEvent {
    /// 事件所属任务。
    pub fn task_id(&self) -> &TaskId {
        match self {
            Self::TaskStarted { task_id, .. } => task_id,
            Self::TaskCompleted(c) | Self::TaskFailed(c) | Self::TaskCancelled(c) => c.task_id(),
        }
    }
}

/// 任务结果 correctness 通道的发布端。
///
/// `Clone` 廉价（单一 sender 句柄）：Worker / 结果投递 / failover 终结 /
/// 远端取消回灌等全部生产路径共享。接收端仅一个，由 Runtime 持有并经
/// `take_result_receiver` 一次性交给绑定层泵。
#[derive(Debug, Clone)]
pub struct TaskResultFeed {
    tx: mpsc::UnboundedSender<TaskLifecycleEvent>,
}

impl TaskResultFeed {
    /// 创建 feed 及其唯一接收端。
    pub fn new() -> (Self, mpsc::UnboundedReceiver<TaskLifecycleEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    /// 发布生命周期事件：结果字节随事件 move，无深拷贝。
    ///
    /// 通道关闭（泵已退出，shutdown 竞态）时记 debug 并返回——残留终态已无
    /// 消费者，属正常关停路径而非错误。
    pub fn send(&self, event: TaskLifecycleEvent) {
        if let Err(returned) = self.tx.send(event) {
            tracing::debug!(
                task_id = %returned.0.task_id().as_str(),
                "task result feed closed; dropping lifecycle event during shutdown race"
            );
        }
    }
}

/// 观测事件（可丢通道载荷）。
///
/// 载荷字段用 `Arc<str>` 而非 `String`：broadcast 在每个接收端 `recv` 时
/// 克隆一次，Arc 使该克隆为引用计数而非深拷贝。
#[derive(Debug, Clone)]
pub enum TapEvent {
    /// worker 子进程经 stderr 边带上报的任务日志。
    TaskLog {
        task_id: TaskId,
        level: Arc<str>,
        message: Arc<str>,
    },
    /// 对端连接 / 断开（网络事件循环）。
    PeerConnected(NodeId),
    PeerDisconnected(NodeId),
    /// Actor 生命周期中的不可恢复错误（panic / 状态机非法转换）。
    ActorLifecycleError {
        actor_id: ActorId,
        error: Arc<str>,
    },
    /// Worker 进入排空模式 — 不再接受新任务。
    WorkerDraining {
        node_id: NodeId,
    },
    /// Worker 已完成排空 — 所有运行中任务结束。
    WorkerDrained {
        node_id: NodeId,
    },
    /// Worker 已完全停止。
    WorkerStopped {
        node_id: NodeId,
    },
}

/// broadcast 容量：只需覆盖消费者一个处理周期的突发（日志泵逐条回调
/// Python）。落后超过容量的接收端自动 Lagged 跳过——这正是 tap 语义。
const TAP_CAPACITY: usize = 1024;

/// Worker 生命周期专用通道容量：draining/drained/stopped 每次关停各一条，
/// 容量 16 远超所需；独立于通用 tap，使日志洪泛不可能把生命周期事件
/// 挤出缓冲（这两个事件有 Python 侧等待者，丢失维度上不可容忍）。
const LIFECYCLE_CAPACITY: usize = 16;

/// 观测事件的发布端（可多订阅者、可丢、不反压）。
///
/// 内部两条 broadcast 通道：通用 tap（[`Self::publish`]）承载日志/对端/
/// Actor 错误等可丢观测；生命周期通道（[`Self::publish_lifecycle`]）独立
/// 承载 Worker 状态事件——同通道会被高频日志 Lagged 挤掉，而 drained/
/// stopped 有 Python 侧等待者，不可丢。
///
/// `Clone` 廉价；无订阅者时 publish 为零开销 no-op（未注册任何观测回调
/// 是合法常态）。默认构造即可用，ActorSystem 等低频持有者无需注入。
#[derive(Debug, Clone)]
pub struct EventTap {
    tx: broadcast::Sender<TapEvent>,
    lifecycle_tx: broadcast::Sender<TapEvent>,
}

impl Default for EventTap {
    fn default() -> Self {
        Self::new()
    }
}

impl EventTap {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(TAP_CAPACITY);
        let (lifecycle_tx, _) = broadcast::channel(LIFECYCLE_CAPACITY);
        Self { tx, lifecycle_tx }
    }

    /// 发布通用观测事件。
    ///
    /// 无订阅者时 `send` 返回 `Err`——观测事件无消费者是常态，无需处理
    /// （`let _` 在此有注释豁免：这不是被吞的错误，而是"无人在听"）。
    pub fn publish(&self, event: TapEvent) {
        let _ = self.tx.send(event);
    }

    /// 发布 Worker 生命周期事件（独立通道，日志洪泛不影响其送达）。
    pub fn publish_lifecycle(&self, event: TapEvent) {
        let _ = self.lifecycle_tx.send(event);
    }

    /// 订阅通用观测流。每个订阅者独立消费；落后超过容量的订阅者收到
    /// `RecvError::Lagged(n)`（应计数 `actant.tap.lagged_total` 并继续，
    /// 而不是退出）。
    pub fn subscribe(&self) -> broadcast::Receiver<TapEvent> {
        self.tx.subscribe()
    }

    /// 订阅 Worker 生命周期流（与通用 tap 互不影响）。
    pub fn subscribe_lifecycle(&self) -> broadcast::Receiver<TapEvent> {
        self.lifecycle_tx.subscribe()
    }
}

#[cfg(test)]
#[path = "../../../../tests/rust/unit/runtime/events.rs"]
mod tests;
