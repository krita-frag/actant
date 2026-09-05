//! Workflow 子系统的 Actor 化入口。
//!
//! 工作流核心组件（`Orchestrator` / `Scheduler` / `FailoverManager` /
//! `DagGossip`）均以 Actor 形式运行，享有与其它 Actor 一致的监督、持久化、
//! 远程调用能力。
//!
//! Actor 状态分布：
//! - `WorkflowActor` 独占持有 `Orchestrator`；
//! - `SchedulerActor` 直接持有 FIFO / Priority 队列状态；
//! - `FailoverActor` 与 `DagGossipActor` 均通过 `ActorSystem` 向 `WorkflowActor`
//!   发送消息，不直接持有 `Orchestrator`。
//!
//! 外部代码通过 `Runtime::actor_system()` + `Runtime::workflow_actor_id()`
//! 或 `ActorScheduler` 客户端与这些 Actor 交互。

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::common::{
    ActantError, ActorMessage, ActorMessageResult, FailoverConfig, GossipConfig, NodeId,
    OrchestratorClaim, Result, TaskDefinition, TaskId, WorkflowId,
};
use crate::runtime::actor::Actor;
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::network::Transport;
use crate::runtime::workflow::messaging::{decode, encode, ok_result, payload_result};
use crate::runtime::workflow::{Dag, DagGossip, FailoverManager, FailureScope, Orchestrator};

/// 可序列化的任务完成响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompletionResponse {
    pub workflow_terminal: bool,
    pub ready_successors: Vec<TaskDefinition>,
    pub conditional_edges: Vec<(TaskId, String)>,
}

pub const WORKFLOW_ACTOR_TYPE: &str = "WorkflowActor";

/// 任务终态结果载荷：[`WorkflowActor::on_task_result`] 唯一入口的 `outcome` 参数。
///
/// 由三条结果回灌路径统一构造：本地完成通道（legacy `COMPLETE_TASK` /
/// `FAIL_TASK` 消息）、远端 TaskResult 直连（network_router）、gossip 状态
/// 同步（DagGossip apply_remote_update）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskResultOutcome {
    Completed(Vec<u8>),
    Failed(String),
    Cancelled,
}

/// 结果回灌来源。仅用于日志与指标，不参与任何状态语义决策——状态推进
/// 只由 `outcome` 与 orchestrator 内部状态（终态守卫、attempt fencing、
/// failure_strategy）决定。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultSource {
    /// 本节点产生（flow 本地回灌，legacy `COMPLETE_TASK` / `FAIL_TASK` 消息）。
    Local,
    /// 执行节点经 TaskResult 直连回传（network_router）。
    Remote,
    /// gossip DAG 状态同步（DagGossip apply_remote_update）。
    Gossip,
}

/// WorkflowActor 支持的方法名。
pub mod workflow_methods {
    pub const SUBMIT: &str = "submit";
    pub const SUBMIT_WITH_TIMEOUT: &str = "submit_with_timeout";
    pub const START: &str = "start";
    pub const COMPLETE_TASK: &str = "complete_task";
    pub const ACTIVATE_CONDITIONAL: &str = "activate_conditional";
    pub const SKIP_CONDITIONAL_BRANCH: &str = "skip_conditional_branch";
    pub const FAIL_TASK: &str = "fail_task";
    pub const CANCEL_TASK: &str = "cancel_task";
    /// 任务结果回灌唯一入口（远端直连与 gossip 路径统一走此方法）。
    pub const ON_TASK_RESULT: &str = "on_task_result";
    pub const MARK_TASK_RUNNING: &str = "mark_task_running";
    /// 任务被本地 Worker 接受执行（S0 派发事件，Worker fire-and-forget 上报）。
    pub const TASK_DISPATCHED: &str = "task_dispatched";
    pub const GET_STATE: &str = "get_state";
    pub const ACTIVE_WORKFLOW_IDS: &str = "active_workflow_ids";
    pub const ADOPT_WORKFLOW: &str = "adopt_workflow";
    pub const DELETE_WORKFLOW: &str = "delete_workflow";
    pub const REMOVE_ACTIVE_WORKFLOW: &str = "remove_active_workflow";
    pub const GET_WORKFLOW_STATE_BYTES: &str = "get_workflow_state_bytes";
    pub const APPLY_FULL_STATE: &str = "apply_full_state";
    pub const RESCHEDULE_RUNNING_TASKS: &str = "reschedule_running_tasks";
}

/// 封装 `Orchestrator` 的 Actor，负责 DAG 提交、执行推进与状态查询。
///
/// 后台循环（超时监控、定期落盘）由 `on_start` 启动、`on_stop` 取消，
/// Actor 生命周期与后台任务生命周期严格绑定。
pub struct WorkflowActor {
    orchestrator: Orchestrator,
    /// 超时监控的 cancel 句柄。`None` 表示尚未启动或已停止。
    timeout_cancel: Option<tokio::sync::watch::Sender<bool>>,
    /// 定期落盘的 cancel 句柄。
    persist_cancel: Option<tokio::sync::watch::Sender<bool>>,
}

impl WorkflowActor {
    pub fn new(orchestrator: Orchestrator) -> Self {
        Self {
            orchestrator,
            timeout_cancel: None,
            persist_cancel: None,
        }
    }

    /// 任务结果回灌唯一入口（S8 单路化）。
    ///
    /// 三条路径（本地完成通道 / 远端 TaskResult 直连 / gossip 状态同步）全部
    /// 收敛于此：attempt fencing、失败语义（FailureScope）决策、终态推进均在
    /// 此处统一定夺，调用方不再各自决定。
    ///
    /// - **attempt fencing**：`attempt` 为结果所属派发代数。入口先经
    ///   `Orchestrator::result_attempt_accepted` 做唯一接受决策，过期代数的
    ///   结果直接丢弃（返回 `Ok(None)`，不推进状态、不发事件）。wire 协议
    ///   尚未携带派发代数，三条路径当前均传 `None`（fencing 放行），协议
    ///   扩展后无需改动入口签名。DAG 写入方法内部的 fencing 校验保留为防
    ///   绕过的最终防线（recover / 重派发等不经本入口的写入路径）。
    ///
    /// - **失败语义统一裁决**：任务失败一律按工作流级失败语义处理（内部固定
    ///   `FailureScope::WorkflowLevel`），最终效果由 `failure_strategy` 决定：
    ///   FailFast → 首个任务失败即工作流 Failed；Continue → 任务标 Failed，
    ///   待全部任务终态且有失败时工作流 Failed。理由：到达 orchestrator 的
    ///   失败已是重试耗尽后的最终结果（重试发生在 worker / 派发侧，核心不
    ///   存在消费 TaskOnly 状态的重试路径）；FailFast 下 TaskOnly 会让工作流
    ///   悬挂在非终态（只能等工作流 deadline 兜底），与 failure_strategy 的
    ///   文档语义（"任何任务失败都立即标记工作流为失败"）矛盾。
    ///   `FailureScope::TaskOnly` 保留在 DAG 层 API，供后续 orchestrator 驱动
    ///   重试时在入口内部决策使用。
    ///
    /// - `source` 仅用于日志与指标，不影响状态语义。
    ///
    /// 返回 `Some(response)` 仅当 outcome 为 `Completed` 且未被 fencing 拒绝
    /// （legacy `COMPLETE_TASK` 消息的响应载荷）；其余返回 `None`。
    async fn on_task_result(
        &mut self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        outcome: TaskResultOutcome,
        attempt: Option<u32>,
        source: ResultSource,
    ) -> Result<Option<TaskCompletionResponse>> {
        tracing::debug!(
            workflow = %workflow_id.as_str(),
            task = %task_id.as_str(),
            source = ?source,
            attempt = ?attempt,
            "task result ingested via unified on_task_result entry"
        );
        // attempt fencing 唯一决策点：过期代数的结果在此丢弃。
        if !self
            .orchestrator
            .result_attempt_accepted(workflow_id, task_id, attempt)
        {
            return Ok(None);
        }
        match outcome {
            TaskResultOutcome::Completed(result) => {
                let (ready, conditional, workflow_terminal) = self
                    .orchestrator
                    .on_task_completed(workflow_id, task_id, result)
                    .await?;
                Ok(Some(TaskCompletionResponse {
                    workflow_terminal,
                    ready_successors: ready,
                    conditional_edges: conditional,
                }))
            }
            TaskResultOutcome::Failed(error) => {
                // 失败语义在此统一（见方法文档）：所有来源一律 WorkflowLevel。
                self.orchestrator
                    .fail_task(workflow_id, task_id, error, FailureScope::WorkflowLevel)
                    .await?;
                Ok(None)
            }
            TaskResultOutcome::Cancelled => {
                self.orchestrator.cancel_task(workflow_id, task_id)?;
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl Actor for WorkflowActor {
    fn actor_type(&self) -> &str {
        WORKFLOW_ACTOR_TYPE
    }

    async fn on_start(&mut self) -> Result<()> {
        // 启动超时监控与定期落盘循环。
        // 句柄存入 self，on_stop 时发送 cancel 信号。
        self.timeout_cancel = Some(self.orchestrator.start_timeout_watcher());
        self.persist_cancel = Some(self.orchestrator.start_persist_flush());
        Ok(())
    }

    async fn on_stop(&mut self) -> Result<()> {
        if let Some(tx) = self.timeout_cancel.take() {
            // send 失败仅当 receiver 已 drop（子任务已退出），无需通知。
            let _ = tx.send(true);
        }
        if let Some(tx) = self.persist_cancel.take() {
            // 同上：子任务已退出时 send 返回 Err，丢弃合理。
            let _ = tx.send(true);
        }
        // 停止前同步落盘所有脏状态，确保 graceful shutdown 不丢数据。
        if let Err(e) = self.orchestrator.flush_dirty().await {
            let node = self
                .orchestrator
                .node_id()
                .map(|n| n.as_str().to_string())
                .unwrap_or_else(|| "<unknown>".to_string());
            tracing::warn!(
                node = %node,
                error = %e,
                "WorkflowActor on_stop flush_dirty failed"
            );
        }
        Ok(())
    }

    /// WorkflowActor **不接入** ActorSystem 的 checkpoint/WAL 持久化。
    ///
    /// 原因：Orchestrator 持有大量工作流状态（DAG + execution + pending），
    /// 通过 `Store` 独立落盘（见 `start_persist_flush` / `flush_dirty`）。
    /// 若再通过 ActorSystem checkpoint 序列化为单一 `Vec<u8>`，会产生
    /// 双写不一致且性能不可接受。
    ///
    /// 恢复路径：节点重启时由 builder 调用 `Orchestrator::recover(store, config, event_log)`
    /// 从 Store 恢复工作流状态。
    fn supports_state_persistence(&self) -> bool {
        false
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        let msg_id = msg.id.clone();
        match msg.method.as_str() {
            workflow_methods::SUBMIT => {
                let (workflow_id, dag): (WorkflowId, Dag) = decode(&msg.payload)?;
                self.orchestrator.submit(workflow_id, dag).await?;
                Ok(ok_result(msg_id))
            }
            workflow_methods::SUBMIT_WITH_TIMEOUT => {
                let (workflow_id, dag, timeout_ms): (WorkflowId, Dag, u64) = decode(&msg.payload)?;
                self.orchestrator
                    .submit_with_timeout(workflow_id, dag, timeout_ms)
                    .await?;
                Ok(ok_result(msg_id))
            }
            workflow_methods::START => {
                let workflow_id: WorkflowId = decode(&msg.payload)?;
                let roots = self.orchestrator.start(&workflow_id)?;
                Ok(payload_result(msg_id, encode(&roots)?))
            }
            workflow_methods::COMPLETE_TASK => {
                let (workflow_id, task_id, result): (WorkflowId, TaskId, Vec<u8>) =
                    decode(&msg.payload)?;
                // 本地完成通道：经唯一入口回灌。fencing 拒绝或非完成结果时
                // 返回空响应（与拒绝路径的响应形状一致）。
                let response = self
                    .on_task_result(
                        &workflow_id,
                        &task_id,
                        TaskResultOutcome::Completed(result),
                        None,
                        ResultSource::Local,
                    )
                    .await?
                    .unwrap_or(TaskCompletionResponse {
                        workflow_terminal: false,
                        ready_successors: vec![],
                        conditional_edges: vec![],
                    });
                Ok(payload_result(msg_id, encode(&response)?))
            }
            workflow_methods::ACTIVATE_CONDITIONAL => {
                let (workflow_id, task_id): (WorkflowId, TaskId) = decode(&msg.payload)?;
                let task = self
                    .orchestrator
                    .activate_conditional_successor(&workflow_id, &task_id)?;
                Ok(payload_result(msg_id, encode(&task)?))
            }
            workflow_methods::SKIP_CONDITIONAL_BRANCH => {
                let (workflow_id, task_id): (WorkflowId, TaskId) = decode(&msg.payload)?;
                let ready = self
                    .orchestrator
                    .skip_conditional_branch(&workflow_id, &task_id)
                    .await?;
                Ok(payload_result(msg_id, encode(&ready)?))
            }
            workflow_methods::FAIL_TASK => {
                // `scope` 字段仅为 wire 兼容保留（py complete_workflow 载荷）；
                // 失败语义由 on_task_result 入口统一决定，此处不读取该字段。
                let (workflow_id, task_id, error, _scope): (
                    WorkflowId,
                    TaskId,
                    String,
                    FailureScope,
                ) = decode(&msg.payload)?;
                self.on_task_result(
                    &workflow_id,
                    &task_id,
                    TaskResultOutcome::Failed(error),
                    None,
                    ResultSource::Local,
                )
                .await?;
                Ok(ok_result(msg_id))
            }
            workflow_methods::ON_TASK_RESULT => {
                let (workflow_id, task_id, outcome, attempt, source): (
                    WorkflowId,
                    TaskId,
                    TaskResultOutcome,
                    Option<u32>,
                    ResultSource,
                ) = decode(&msg.payload)?;
                self.on_task_result(&workflow_id, &task_id, outcome, attempt, source)
                    .await?;
                Ok(ok_result(msg_id))
            }
            workflow_methods::CANCEL_TASK => {
                // 取消指令路径（用户 / 控制面发起）与结果回灌分离；gossip /
                // 远端的 Cancelled 结果走 ON_TASK_RESULT 单入口，二者最终都
                // 落到 `Orchestrator::cancel_task` 同一写入路径。
                let (workflow_id, task_id): (WorkflowId, TaskId) = decode(&msg.payload)?;
                self.orchestrator.cancel_task(&workflow_id, &task_id)?;
                Ok(ok_result(msg_id))
            }
            workflow_methods::MARK_TASK_RUNNING => {
                let (workflow_id, task_id): (WorkflowId, TaskId) = decode(&msg.payload)?;
                self.orchestrator
                    .mark_task_running(&workflow_id, &task_id)?;
                Ok(ok_result(msg_id))
            }
            workflow_methods::TASK_DISPATCHED => {
                let (workflow_id, task_id): (WorkflowId, TaskId) = decode(&msg.payload)?;
                self.orchestrator
                    .log_task_dispatched(&workflow_id, &task_id);
                Ok(ok_result(msg_id))
            }
            workflow_methods::GET_STATE => {
                let workflow_id: WorkflowId = decode(&msg.payload)?;
                let state = self.orchestrator.get_state(&workflow_id);
                Ok(payload_result(msg_id, encode(&state)?))
            }
            workflow_methods::ACTIVE_WORKFLOW_IDS => {
                let ids = self.orchestrator.active_workflow_ids();
                Ok(payload_result(msg_id, encode(&ids)?))
            }
            workflow_methods::ADOPT_WORKFLOW => {
                let workflow_id: WorkflowId = decode(&msg.payload)?;
                self.orchestrator.adopt_workflow(&workflow_id).await?;
                Ok(ok_result(msg_id))
            }
            workflow_methods::DELETE_WORKFLOW => {
                let workflow_id: WorkflowId = decode(&msg.payload)?;
                self.orchestrator.delete_workflow(&workflow_id).await;
                Ok(ok_result(msg_id))
            }
            workflow_methods::REMOVE_ACTIVE_WORKFLOW => {
                let workflow_id: WorkflowId = decode(&msg.payload)?;
                self.orchestrator.remove_active_workflow(&workflow_id);
                Ok(ok_result(msg_id))
            }
            workflow_methods::GET_WORKFLOW_STATE_BYTES => {
                let workflow_id: WorkflowId = decode(&msg.payload)?;
                let bytes = self
                    .orchestrator
                    .get_workflow_state_bytes(&workflow_id)
                    .await;
                Ok(payload_result(msg_id, encode(&bytes)?))
            }
            workflow_methods::APPLY_FULL_STATE => {
                let (workflow_id, dag_bytes, exec_bytes, pending_bytes): (
                    WorkflowId,
                    Option<Vec<u8>>,
                    Option<Vec<u8>>,
                    Option<Vec<u8>>,
                ) = decode(&msg.payload)?;
                self.orchestrator
                    .restore_workflow_from_bytes(&workflow_id, dag_bytes, exec_bytes, pending_bytes)
                    .await;
                Ok(ok_result(msg_id))
            }
            workflow_methods::RESCHEDULE_RUNNING_TASKS => {
                let workflow_id: WorkflowId = decode(&msg.payload)?;
                let tasks = self.orchestrator.reschedule_running_tasks(&workflow_id)?;
                Ok(payload_result(msg_id, encode(&tasks)?))
            }
            other => Err(ActantError::Actor(format!(
                "WorkflowActor: unknown method {}",
                other
            ))),
        }
    }
}

pub const SCHEDULER_ACTOR_TYPE: &str = "SchedulerActor";

/// SchedulerActor 内阻塞式 `DEQUEUE` 的最大等待时间。
///
/// Actor 消息处理是单线程顺序执行：若 `DEQUEUE` 在空队列上无限期阻塞，
/// 后续 `ENQUEUE` / `CLOSE` 等消息全部滞留邮箱，调度器假死。超时后返回
/// `None`，消费方（Worker 主循环）由 `TaskEnqueued` Notify 信号驱动
/// `try_dequeue`，不依赖阻塞式 dequeue 的长等待语义。
const DEQUEUE_ACTOR_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// SchedulerActor 支持的方法名。
pub mod scheduler_methods {
    pub const ENQUEUE: &str = "enqueue";
    pub const ENQUEUE_BATCH: &str = "enqueue_batch";
    pub const DEQUEUE: &str = "dequeue";
    pub const DEQUEUE_BATCH: &str = "dequeue_batch";
    pub const TRY_DEQUEUE: &str = "try_dequeue";
    pub const DRAIN_UNROUTED: &str = "drain_unrouted";
    pub const LEN: &str = "len";
    pub const IS_EMPTY: &str = "is_empty";
    pub const CLOSE: &str = "close";
    pub const IS_CLOSED: &str = "is_closed";
}

/// 调度器内部队列状态。
///
/// `pub(crate)` 以允许 [`crate::runtime::workflow::ActorScheduler`] 与 Worker
/// 通过 `Arc<InnerScheduler>` 直接访问 `enqueue`/`dequeue` 系列方法，绕过
/// Actor 消息往返。直接出队是大载荷路径的必需品：`TaskDefinition` 内嵌任务
/// 载荷，经 Actor 消息回传时受 `decode_postcard` 的 4MiB 上限约束，超限任务
/// 会在出队响应解码时被丢弃。单消费者语义由「仅 Worker 主循环调用 dequeue」
/// 保证，与消息协议路径一致。
pub(crate) enum InnerScheduler {
    Fifo {
        queue: parking_lot::Mutex<std::collections::VecDeque<TaskDefinition>>,
        notify: Arc<tokio::sync::Notify>,
        closed: std::sync::atomic::AtomicBool,
    },
    Priority {
        queues: parking_lot::Mutex<
            std::collections::BTreeMap<
                std::cmp::Reverse<i32>,
                std::collections::VecDeque<TaskDefinition>,
            >,
        >,
        notify: Arc<tokio::sync::Notify>,
        closed: std::sync::atomic::AtomicBool,
    },
}

impl InnerScheduler {
    fn fifo() -> Self {
        Self::fifo_with_notify(Arc::new(tokio::sync::Notify::new()))
    }

    /// 使用共享的唤醒信号构造 FIFO 调度器。
    ///
    /// `notify` 与 Worker 等待的 `EventBus::task_enqueued_notify` 是同一
    /// `Arc<Notify>`，确保快路径 `enqueue` 的信号能唤醒 Worker。
    fn fifo_with_notify(notify: Arc<tokio::sync::Notify>) -> Self {
        Self::Fifo {
            queue: parking_lot::Mutex::new(std::collections::VecDeque::new()),
            notify,
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn priority() -> Self {
        Self::priority_with_notify(Arc::new(tokio::sync::Notify::new()))
    }

    fn priority_with_notify(notify: Arc<tokio::sync::Notify>) -> Self {
        Self::Priority {
            queues: parking_lot::Mutex::new(std::collections::BTreeMap::new()),
            notify,
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// 原地将 FIFO 切换为优先级队列策略。
    ///
    /// 保留 `closed` 标志（若已关闭）和 `notify` 引用（若有等待者），
    /// 避免丢失与 `EventBus::task_enqueued_notify` 共享的唤醒信号，
    /// 也避免重置 `closed` 导致 drain 状态被静默撤销。
    fn switch_to_priority(&mut self) {
        // 仅在当前为 Fifo 时切换；已是 Priority 则 no-op。
        if matches!(self, Self::Priority { .. }) {
            return;
        }
        let (notify, closed) = match self {
            Self::Fifo { notify, closed, .. } => (Arc::clone(notify), closed),
            Self::Priority { notify, closed, .. } => (Arc::clone(notify), closed),
        };
        let closed =
            std::sync::atomic::AtomicBool::new(closed.load(std::sync::atomic::Ordering::Acquire));
        *self = Self::Priority {
            queues: parking_lot::Mutex::new(std::collections::BTreeMap::new()),
            notify,
            closed,
        };
    }

    pub(crate) fn is_closed(&self) -> bool {
        use std::sync::atomic::Ordering;
        match self {
            Self::Fifo { closed, .. } | Self::Priority { closed, .. } => {
                closed.load(Ordering::Acquire)
            }
        }
    }

    pub(crate) fn close(&self) {
        use std::sync::atomic::Ordering;
        match self {
            Self::Fifo { closed, notify, .. } | Self::Priority { closed, notify, .. } => {
                closed.store(true, Ordering::Release);
                notify.notify_waiters();
            }
        }
    }

    fn check_closed(&self) -> Result<()> {
        if self.is_closed() {
            return Err(ActantError::InvalidState(
                "scheduler is closed (drain mode), rejecting task".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn enqueue(&self, mut task: TaskDefinition) -> Result<()> {
        self.check_closed()?;
        task.enqueued_at_ms = crate::common::epoch_millis();
        match self {
            Self::Fifo { queue, notify, .. } => {
                queue.lock().push_back(task);
                notify.notify_one();
            }
            Self::Priority { queues, notify, .. } => {
                let key = std::cmp::Reverse(task.priority);
                queues.lock().entry(key).or_default().push_back(task);
                notify.notify_one();
            }
        }
        Ok(())
    }

    pub(crate) fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> Result<()> {
        self.check_closed()?;
        if tasks.is_empty() {
            return Ok(());
        }
        let now_ms = crate::common::epoch_millis();
        match self {
            Self::Fifo { queue, notify, .. } => {
                let mut q = queue.lock();
                for mut task in tasks {
                    task.enqueued_at_ms = now_ms;
                    q.push_back(task);
                }
                drop(q);
                notify.notify_one();
            }
            Self::Priority { queues, notify, .. } => {
                let mut qs = queues.lock();
                for mut task in tasks {
                    task.enqueued_at_ms = now_ms;
                    let key = std::cmp::Reverse(task.priority);
                    qs.entry(key).or_default().push_back(task);
                }
                drop(qs);
                notify.notify_one();
            }
        }
        Ok(())
    }

    /// 阻塞式出队：队列空时等待 Notify 信号。
    ///
    /// # Actor 分发约定
    ///
    /// 本方法**不得**在 Actor 消息处理内无限期调用：`SchedulerActor` 的邮箱
    /// 单线程顺序处理，阻塞的 DEQUEUE 会阻止 ENQUEUE/CLOSE 被处理。
    /// `SchedulerActor` 对本方法套用 [`DEQUEUE_ACTOR_TIMEOUT`] 超时保护；
    /// 生产消费路径（Worker 主循环）应使用 [`Self::try_dequeue`] + Notify
    /// 信号驱动，而非依赖本方法的长等待。
    pub(crate) async fn dequeue(&self) -> Option<TaskDefinition> {
        loop {
            let task = match self {
                Self::Fifo { queue, .. } => queue.lock().pop_front(),
                Self::Priority { queues, .. } => {
                    let mut qs = queues.lock();
                    let task = qs.iter_mut().next().and_then(|(_, q)| q.pop_front());
                    qs.retain(|_, q| !q.is_empty());
                    task
                }
            };
            if task.is_some() {
                return task;
            }
            if self.is_closed() {
                match self {
                    Self::Fifo { notify, .. } | Self::Priority { notify, .. } => {
                        notify.notify_waiters();
                    }
                }
                return None;
            }
            match self {
                Self::Fifo { notify, .. } | Self::Priority { notify, .. } => {
                    notify.notified().await;
                }
            }
        }
    }

    pub(crate) fn try_dequeue(&self) -> Option<TaskDefinition> {
        match self {
            Self::Fifo { queue, .. } => queue.lock().pop_front(),
            Self::Priority { queues, .. } => {
                let mut qs = queues.lock();
                let task = qs.iter_mut().next().and_then(|(_, q)| q.pop_front());
                qs.retain(|_, q| !q.is_empty());
                task
            }
        }
    }

    pub(crate) fn dequeue_batch(&self, limit: usize) -> Vec<TaskDefinition> {
        match self {
            Self::Fifo { queue, .. } => {
                let mut q = queue.lock();
                let count = limit.min(q.len());
                (0..count).filter_map(|_| q.pop_front()).collect()
            }
            Self::Priority { queues, .. } => {
                let mut result = Vec::with_capacity(limit);
                let mut qs = queues.lock();
                while result.len() < limit {
                    let key = match qs.keys().next().copied() {
                        Some(k) => k,
                        None => break,
                    };
                    use std::collections::btree_map::Entry;
                    match qs.entry(key) {
                        Entry::Occupied(mut entry) => match entry.get_mut().pop_front() {
                            Some(task) => result.push(task),
                            None => {
                                entry.remove();
                            }
                        },
                        Entry::Vacant(_) => break,
                    }
                }
                result
            }
        }
    }

    fn drain_unrouted(&self) -> Vec<TaskDefinition> {
        match self {
            Self::Fifo { queue, .. } => {
                let mut q = queue.lock();
                let old = std::mem::take(&mut *q);
                let (unrouted, routed): (Vec<_>, Vec<_>) =
                    old.into_iter().partition(|t| t.target_node.is_none());
                *q = routed.into();
                unrouted
            }
            Self::Priority { queues, .. } => {
                let mut qs = queues.lock();
                let mut unrouted = Vec::new();
                for (_, queue) in qs.iter_mut() {
                    let old = std::mem::take(queue);
                    for task in old {
                        if task.target_node.is_none() {
                            unrouted.push(task);
                        } else {
                            queue.push_back(task);
                        }
                    }
                }
                qs.retain(|_, q| !q.is_empty());
                unrouted
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Fifo { queue, .. } => queue.lock().len(),
            Self::Priority { queues, .. } => queues.lock().values().map(|q| q.len()).sum(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Fifo { queue, .. } => queue.lock().is_empty(),
            Self::Priority { queues, .. } => queues.lock().values().all(|q| q.is_empty()),
        }
    }
}

/// 调度状态完全内化的 Actor，负责任务队列的入队/出队与状态维护。
///
/// `inner` 以 `Arc` 持有，使得 [`crate::runtime::workflow::ActorScheduler`]
/// 可获取共享引用实现 `enqueue` 快路径——直接调用 [`InnerScheduler::enqueue`]
/// 的同步方法，绕过 Actor 消息往返（postcard 编解码 + 邮箱调度 + 响应通道）。
/// 快路径在入队后仍通过 [`Self::notify_task_enqueued`] 的等价路径触发 Worker 唤醒：
/// `InnerScheduler::enqueue` 内部调用 `notify.notify_one()`，与 Actor 路径一致。
pub struct SchedulerActor {
    inner: Arc<InnerScheduler>,
    /// 用于触发 `TaskEnqueued` 唤醒信号（`Notify`）的 EventBus。
    /// `None` 时（如纯单元测试）退化为无信号触发，Worker 需自行轮询。
    /// 生产路径由 `init_worker` 注入与 Worker 共享的 EventBus。
    event_bus: Option<EventBus>,
}

impl SchedulerActor {
    pub fn fifo() -> Self {
        Self {
            inner: Arc::new(InnerScheduler::fifo()),
            event_bus: None,
        }
    }

    pub fn priority() -> Self {
        Self {
            inner: Arc::new(InnerScheduler::priority()),
            event_bus: None,
        }
    }

    /// 用给定的 EventBus 构造 FIFO SchedulerActor。
    ///
    /// `enqueue` / `enqueue_batch` / `close` 后会调用
    /// `notify_task_enqueued()` 触发 `Notify` 信号，使正在
    /// `.notified().await` 的 Worker 立即被唤醒。
    /// 通过 Notify 信号 + 非阻塞 `try_dequeue` 组合，
    /// 避免直接调用 `dequeue()` 阻塞 Actor 邮箱导致的死锁。
    pub fn with_event_bus(event_bus: EventBus) -> Self {
        // 用 EventBus 的唤醒信号构造内部调度器：使快路径 enqueue 与 Worker
        // 等待的是同一 Arc<Notify>，保证任务一经入队即唤醒 Worker。
        let notify = event_bus.task_enqueued_notify();
        Self {
            inner: Arc::new(InnerScheduler::fifo_with_notify(notify)),
            event_bus: Some(event_bus),
        }
    }

    /// 设置优先级调度策略并返回 self（builder 风格）。
    ///
    /// 必须在 Actor `spawn` 之前调用：此方法重建内部状态为优先级队列。
    /// 若 `shared_inner()` 已被调用（Arc 已共享），重建会丢弃旧状态——
    /// 因此调用顺序应为 `with_event_bus(...).with_priority()` → `shared_inner()` → `spawn`。
    ///
    /// 返回 `Err` 当 `self.inner` 已被共享（`Arc::get_mut` 返回 `None`），
    /// 即调用顺序违反上述约定。调用方应通过 `?` 传播此错误而非 panic，
    /// 以符合"库代码不 panic"的约定。
    pub fn with_priority(mut self) -> Result<Self> {
        // 链式调用约定：with_priority 在 shared_inner/spawn 之前调用，
        // 此时 self.inner 是唯一 Arc 引用，Arc::get_mut 必然成功。
        // 若因调用顺序错误导致 get_mut 失败，返回 Err 以暴露误用，
        // 而非 panic 或静默丢弃已共享状态。
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            ActantError::Internal(
                "with_priority must be called before shared_inner/spawn \
                 (inner Arc already shared)"
                    .into(),
            )
        })?;
        inner.switch_to_priority();
        Ok(self)
    }

    /// 返回内部调度状态的共享引用，供 [`ActorScheduler`] 实现 enqueue 快路径。
    ///
    /// 调用方（`init_worker`）在 `spawn` Actor 前调用此方法，将 `Arc` 传递给
    /// `ActorScheduler::with_fast_path`。此后 `enqueue` / `enqueue_batch`
    /// 直接操作共享状态，绕过 Actor 消息往返。
    ///
    /// # 安全性
    ///
    /// `InnerScheduler` 的所有可变状态均由 `parking_lot::Mutex` 保护，
    /// `notify` 为 `tokio::sync::Notify`（内部线程安全），`closed` 为
    /// `AtomicBool`。多线程并发访问 `enqueue` 安全——Mutex 保证队列操作互斥，
    /// `notify_one` 唤醒一个等待 Worker，语义与 Actor 路径一致。
    pub(crate) fn shared_inner(&self) -> Arc<InnerScheduler> {
        Arc::clone(&self.inner)
    }

    /// 触发 `TaskEnqueued` 唤醒信号，唤醒等待的 Worker。
    ///
    /// 通过 `Notify::notify_waiters()` 唤醒所有正在 `.notified().await` 的 Worker。
    /// 无队列容量限制、无事件丢弃。无 EventBus（`None`）时为 no-op，
    /// 仅单元测试路径会落入此分支。
    fn notify_task_enqueued(&self) {
        if let Some(ref bus) = self.event_bus {
            bus.notify_task_enqueued();
        }
    }
}

#[async_trait]
impl Actor for SchedulerActor {
    fn actor_type(&self) -> &str {
        SCHEDULER_ACTOR_TYPE
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        let msg_id = msg.id.clone();
        match msg.method.as_str() {
            scheduler_methods::ENQUEUE => {
                let task: TaskDefinition = decode(&msg.payload)?;
                self.inner.enqueue(task)?;
                // 信号驱动唤醒 Worker：通过 Notify 触发唤醒信号，
                // 正在 .notified().await 的 Worker 立即被唤醒拉取任务。
                // notify_waiters() 无队列无丢弃——信号仅唤醒当前等待者，
                // 无等待者时信号丢失但不影响正确性（Worker 下次 try_dequeue 仍会拉取）。
                self.notify_task_enqueued();
                Ok(ok_result(msg_id))
            }
            scheduler_methods::ENQUEUE_BATCH => {
                let tasks: Vec<TaskDefinition> = decode(&msg.payload)?;
                self.inner.enqueue_batch(tasks)?;
                self.notify_task_enqueued();
                Ok(ok_result(msg_id))
            }
            scheduler_methods::DEQUEUE => {
                // 超时保护：不允许 DEQUEUE 在 Actor 消息处理内无限期阻塞
                // （会卡住整个邮箱，见 `DEQUEUE_ACTOR_TIMEOUT` 文档）。
                // Scheduler trait 的实现约定同样如此：经 Actor 分发的阻塞式
                // dequeue 必须有界，生产消费路径应使用 try_dequeue + 唤醒信号。
                let task = tokio::time::timeout(DEQUEUE_ACTOR_TIMEOUT, self.inner.dequeue())
                    .await
                    .unwrap_or(None);
                Ok(payload_result(msg_id, encode(&task)?))
            }
            scheduler_methods::DEQUEUE_BATCH => {
                let count: usize = decode(&msg.payload)?;
                let tasks = self.inner.dequeue_batch(count);
                Ok(payload_result(msg_id, encode(&tasks)?))
            }
            scheduler_methods::TRY_DEQUEUE => {
                let task = self.inner.try_dequeue();
                // 出队成功后发布 TaskDequeued 事件，供外部观测实际消费速率。
                // 事件丢失可接受（观测 tap），仅为观测信号。
                if let Some(ref t) = task {
                    if let Some(ref bus) = self.event_bus {
                        let workflow_id = t
                            .workflow_id
                            .clone()
                            .unwrap_or_else(|| WorkflowId::from(""));
                        bus.publish(BusEvent::TaskDequeued {
                            workflow_id,
                            task_id: t.id.clone(),
                        });
                    }
                }
                Ok(payload_result(msg_id, encode(&task)?))
            }
            scheduler_methods::DRAIN_UNROUTED => {
                let tasks = self.inner.drain_unrouted();
                Ok(payload_result(msg_id, encode(&tasks)?))
            }
            scheduler_methods::LEN => {
                let len = self.inner.len();
                Ok(payload_result(msg_id, encode(&len)?))
            }
            scheduler_methods::IS_EMPTY => {
                let empty = self.inner.is_empty();
                Ok(payload_result(msg_id, encode(&empty)?))
            }
            scheduler_methods::CLOSE => {
                self.inner.close();
                // close 后发布事件，唤醒等待的 Worker 使其 try_dequeue
                // 得到 None 并退出主循环。
                self.notify_task_enqueued();
                Ok(ok_result(msg_id))
            }
            scheduler_methods::IS_CLOSED => {
                let closed = self.inner.is_closed();
                Ok(payload_result(msg_id, encode(&closed)?))
            }
            other => Err(ActantError::Actor(format!(
                "SchedulerActor: unknown method {}",
                other
            ))),
        }
    }
}

/// 创建默认的 FIFO SchedulerActor。
pub fn fifo_scheduler_actor() -> SchedulerActor {
    SchedulerActor::fifo()
}

/// 创建按优先级调度的 SchedulerActor。
pub fn priority_scheduler_actor() -> SchedulerActor {
    SchedulerActor::priority()
}

pub const FAILOVER_ACTOR_TYPE: &str = "FailoverActor";

/// FailoverActor 支持的方法名。
pub mod failover_methods {
    pub const SEND_HEARTBEAT: &str = "send_heartbeat";
    pub const GET_PEER_INFOS: &str = "get_peer_infos";
    pub const CLAIM_WORKFLOW: &str = "claim_workflow";
    pub const HANDLE_CLAIM: &str = "handle_claim";
    pub const EXPIRE_STALE_PEERS: &str = "expire_stale_peers";
    pub const UPDATE_LOCAL_CAPACITY: &str = "update_local_capacity";
}

/// 封装 [`FailoverManager`] 的 Actor，负责心跳、租约与 peer 管理。
///
/// 持有 `Arc<FailoverManager>` 而非 owned，避免 `FailoverManager` 派生 `Clone`
/// （其内部含 `Mutex` 不可 `Clone`）。Builder 与 Actor 共享同一实例。
///
/// 后台循环（心跳、故障检测、租约过期检查）由 runtime 初始化末期启动，
/// 并通过 runtime shutdown 路径统一取消；FailoverActor 本身仅作为消息网关。
pub struct FailoverActor {
    manager: Arc<FailoverManager>,
}

impl FailoverActor {
    pub fn new(manager: Arc<FailoverManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl Actor for FailoverActor {
    fn actor_type(&self) -> &str {
        FAILOVER_ACTOR_TYPE
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        let msg_id = msg.id.clone();
        match msg.method.as_str() {
            failover_methods::SEND_HEARTBEAT => {
                self.manager.send_heartbeat().await?;
                Ok(ok_result(msg_id))
            }
            failover_methods::GET_PEER_INFOS => {
                let peers = self.manager.get_peer_infos();
                Ok(payload_result(msg_id, encode(&peers)?))
            }
            failover_methods::CLAIM_WORKFLOW => {
                let workflow_id: WorkflowId = decode(&msg.payload)?;
                self.manager.claim_workflow(&workflow_id).await?;
                Ok(ok_result(msg_id))
            }
            failover_methods::HANDLE_CLAIM => {
                let claim: OrchestratorClaim = decode(&msg.payload)?;
                self.manager.handle_claim(&claim).await;
                Ok(ok_result(msg_id))
            }
            failover_methods::EXPIRE_STALE_PEERS => {
                let removed = self.manager.expire_stale_peers();
                Ok(payload_result(msg_id, encode(&removed)?))
            }
            failover_methods::UPDATE_LOCAL_CAPACITY => {
                let (available, max): (u32, u32) = decode(&msg.payload)?;
                self.manager.update_local_capacity(available, max);
                Ok(ok_result(msg_id))
            }
            other => Err(ActantError::Actor(format!(
                "FailoverActor: unknown method {}",
                other
            ))),
        }
    }
}

pub const DAG_GOSSIP_ACTOR_TYPE: &str = "DagGossipActor";

/// DagGossipActor 支持的方法名。
pub mod gossip_methods {
    pub const BROADCAST_STATE_UPDATE: &str = "broadcast_state_update";
    pub const BROADCAST_TASK_RUNNING: &str = "broadcast_task_running";
    pub const APPLY_REMOTE_UPDATE: &str = "apply_remote_update";
    pub const BROADCAST_HEADS: &str = "broadcast_heads";
    pub const HANDLE_HEADS_EXCHANGE: &str = "handle_heads_exchange";
    pub const REQUEST_WORKFLOW_STATE: &str = "request_workflow_state";
    pub const HANDLE_WORKFLOW_STATE_REQUEST: &str = "handle_workflow_state_request";
    pub const HANDLE_WORKFLOW_STATE_RESPONSE: &str = "handle_workflow_state_response";
}

/// 封装 [`DagGossip`] 的 Actor，负责 DAG 状态的 Gossip 同步。
pub struct DagGossipActor {
    gossip: DagGossip,
    heads_broadcast_cancel: Option<tokio::sync::watch::Sender<bool>>,
    heads_broadcast_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DagGossipActor {
    pub fn new(gossip: DagGossip) -> Self {
        Self {
            gossip,
            heads_broadcast_cancel: None,
            heads_broadcast_handle: None,
        }
    }
}

#[async_trait]
impl Actor for DagGossipActor {
    fn actor_type(&self) -> &str {
        DAG_GOSSIP_ACTOR_TYPE
    }

    async fn on_start(&mut self) -> Result<()> {
        // 如果之前已经启动过（如 Actor 重启），先清理旧任务。
        if let Some(handle) = self.heads_broadcast_handle.take() {
            handle.abort();
        }

        let interval = self.gossip.heads_broadcast_interval();
        let actor_system = self.gossip.actor_system().clone();
        let actor_id = crate::common::ActorId::dag_gossip(self.gossip.node_id());
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        self.heads_broadcast_cancel = Some(cancel_tx);

        self.heads_broadcast_handle = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // 第一次 tick 立即触发，让 heads 尽快扩散。
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = ticker.tick() => {
                        let msg = crate::common::ActorMessage::new(
                            actor_id.clone(),
                            crate::runtime::workflow::actor::gossip_methods::BROADCAST_HEADS
                                .to_string(),
                            vec![],
                        );
                        if let Err(e) = actor_system.send(&actor_id, msg).await
                        {
                            tracing::warn!(
                                actor_id = %actor_id.as_str(),
                                error = %e,
                                "failed to send periodic heads broadcast"
                            );
                        }
                    }
                }
            }
            tracing::debug!("dag gossip heads broadcast loop stopped");
        }));

        Ok(())
    }

    async fn on_stop(&mut self) -> Result<()> {
        if let Some(tx) = self.heads_broadcast_cancel.take() {
            // send 失败仅当 broadcast task 已退出 drop 了 receiver。
            let _ = tx.send(true);
        }
        if let Some(handle) = self.heads_broadcast_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        let msg_id = msg.id.clone();
        match msg.method.as_str() {
            gossip_methods::BROADCAST_STATE_UPDATE => {
                let (workflow_id, task_id, task_state): (
                    WorkflowId,
                    TaskId,
                    crate::common::WireTaskState,
                ) = decode(&msg.payload)?;
                self.gossip
                    .broadcast_update(&workflow_id, &task_id, task_state)
                    .await?;
                Ok(ok_result(msg_id))
            }
            gossip_methods::BROADCAST_TASK_RUNNING => {
                let (workflow_id, task_id): (WorkflowId, TaskId) = decode(&msg.payload)?;
                self.gossip
                    .broadcast_task_running(&workflow_id, &task_id)
                    .await?;
                Ok(ok_result(msg_id))
            }
            gossip_methods::APPLY_REMOTE_UPDATE => {
                let update: crate::common::WireDagStateUpdate = decode(&msg.payload)?;
                self.gossip.apply_remote_update(update).await?;
                Ok(ok_result(msg_id))
            }
            gossip_methods::BROADCAST_HEADS => {
                self.gossip.broadcast_heads().await?;
                Ok(ok_result(msg_id))
            }
            gossip_methods::HANDLE_HEADS_EXCHANGE => {
                let exchange: crate::common::HeadsExchange = decode(&msg.payload)?;
                self.gossip.handle_heads_exchange(&exchange).await?;
                Ok(ok_result(msg_id))
            }
            gossip_methods::REQUEST_WORKFLOW_STATE => {
                let (workflow_id, target_node): (WorkflowId, NodeId) = decode(&msg.payload)?;
                self.gossip
                    .request_workflow_state(&workflow_id, &target_node)
                    .await?;
                Ok(ok_result(msg_id))
            }
            gossip_methods::HANDLE_WORKFLOW_STATE_REQUEST => {
                let request: crate::common::WorkflowStateRequest = decode(&msg.payload)?;
                self.gossip.handle_workflow_state_request(&request).await?;
                Ok(ok_result(msg_id))
            }
            gossip_methods::HANDLE_WORKFLOW_STATE_RESPONSE => {
                let response: crate::common::WorkflowStateResponse = decode(&msg.payload)?;
                self.gossip
                    .handle_workflow_state_response(&response)
                    .await?;
                Ok(ok_result(msg_id))
            }
            other => Err(ActantError::Actor(format!(
                "DagGossipActor: unknown method {}",
                other
            ))),
        }
    }
}

/// 用给定网络、ActorSystem、WorkflowActor ID 和配置构造 `DagGossipActor`。
pub fn dag_gossip_actor(
    network: Arc<dyn Transport>,
    actor_system: Arc<crate::runtime::actor::ActorSystem>,
    workflow_actor_id: crate::common::ActorId,
    config: GossipConfig,
) -> DagGossipActor {
    DagGossipActor::new(DagGossip::new(
        network,
        actor_system,
        workflow_actor_id,
        config,
    ))
}

/// 用给定依赖构造 `FailoverActor`。
pub async fn failover_actor(
    node_id: NodeId,
    network: Arc<dyn Transport>,
    actor_system: Arc<crate::runtime::actor::ActorSystem>,
    workflow_actor_id: crate::common::ActorId,
    config: Option<FailoverConfig>,
) -> FailoverActor {
    let manager = match config {
        Some(cfg) => FailoverManager::with_config(
            node_id,
            network,
            actor_system,
            workflow_actor_id,
            cfg,
            None,
        ),
        None => FailoverManager::new(node_id, network, actor_system, workflow_actor_id),
    };
    FailoverActor::new(Arc::new(manager))
}

#[cfg(test)]
#[path = "../../../tests/rust/unit/runtime/workflow/actor.rs"]
mod tests;
