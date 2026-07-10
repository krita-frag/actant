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
    pub const MARK_TASK_RUNNING: &str = "mark_task_running";
    pub const GET_STATE: &str = "get_state";
    pub const ACTIVE_WORKFLOW_IDS: &str = "active_workflow_ids";
    pub const ADOPT_WORKFLOW: &str = "adopt_workflow";
    pub const DELETE_WORKFLOW: &str = "delete_workflow";
    pub const REMOVE_ACTIVE_WORKFLOW: &str = "remove_active_workflow";
    pub const GET_WORKFLOW_STATE_BYTES: &str = "get_workflow_state_bytes";
    pub const APPLY_FULL_STATE: &str = "apply_full_state";
    pub const RESCHEDULE_RUNNING_TASKS: &str = "reschedule_running_tasks";
}

/// 封装 [`Orchestrator`] 的 Actor，负责 DAG 提交、执行推进与状态查询。
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
            let _ = tx.send(true);
        }
        if let Some(tx) = self.persist_cancel.take() {
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
                let (ready, conditional, workflow_terminal) = self
                    .orchestrator
                    .on_task_completed(&workflow_id, &task_id, result)
                    .await?;
                let response = TaskCompletionResponse {
                    workflow_terminal,
                    ready_successors: ready,
                    conditional_edges: conditional,
                };
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
                let (workflow_id, task_id, error, scope): (
                    WorkflowId,
                    TaskId,
                    String,
                    FailureScope,
                ) = decode(&msg.payload)?;
                self.orchestrator
                    .fail_task(&workflow_id, &task_id, error, scope)
                    .await?;
                Ok(ok_result(msg_id))
            }
            workflow_methods::CANCEL_TASK => {
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

enum InnerScheduler {
    Fifo {
        queue: parking_lot::Mutex<std::collections::VecDeque<TaskDefinition>>,
        notify: tokio::sync::Notify,
        closed: std::sync::atomic::AtomicBool,
    },
    Priority {
        queues: parking_lot::Mutex<
            std::collections::BTreeMap<
                std::cmp::Reverse<i32>,
                std::collections::VecDeque<TaskDefinition>,
            >,
        >,
        notify: tokio::sync::Notify,
        closed: std::sync::atomic::AtomicBool,
    },
}

impl InnerScheduler {
    fn fifo() -> Self {
        Self::Fifo {
            queue: parking_lot::Mutex::new(std::collections::VecDeque::new()),
            notify: tokio::sync::Notify::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn priority() -> Self {
        Self::Priority {
            queues: parking_lot::Mutex::new(std::collections::BTreeMap::new()),
            notify: tokio::sync::Notify::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn is_closed(&self) -> bool {
        use std::sync::atomic::Ordering;
        match self {
            Self::Fifo { closed, .. } | Self::Priority { closed, .. } => {
                closed.load(Ordering::Acquire)
            }
        }
    }

    fn close(&self) {
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

    fn enqueue(&self, mut task: TaskDefinition) -> Result<()> {
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

    fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> Result<()> {
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

    async fn dequeue(&self) -> Option<TaskDefinition> {
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

    fn try_dequeue(&self) -> Option<TaskDefinition> {
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

    fn dequeue_batch(&self, limit: usize) -> Vec<TaskDefinition> {
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
pub struct SchedulerActor {
    inner: InnerScheduler,
}

impl SchedulerActor {
    pub fn fifo() -> Self {
        Self {
            inner: InnerScheduler::fifo(),
        }
    }

    pub fn priority() -> Self {
        Self {
            inner: InnerScheduler::priority(),
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
                Ok(ok_result(msg_id))
            }
            scheduler_methods::ENQUEUE_BATCH => {
                let tasks: Vec<TaskDefinition> = decode(&msg.payload)?;
                self.inner.enqueue_batch(tasks)?;
                Ok(ok_result(msg_id))
            }
            scheduler_methods::DEQUEUE => {
                let task = self.inner.dequeue().await;
                Ok(payload_result(msg_id, encode(&task)?))
            }
            scheduler_methods::DEQUEUE_BATCH => {
                let count: usize = decode(&msg.payload)?;
                let tasks = self.inner.dequeue_batch(count);
                Ok(payload_result(msg_id, encode(&tasks)?))
            }
            scheduler_methods::TRY_DEQUEUE => {
                let task = self.inner.try_dequeue();
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
/// 后台循环（心跳、故障检测、租约过期检查）由 `Runtime` 在初始化末期启动，
/// 并通过 [`Runtime::shutdown`] 统一取消；FailoverActor 本身仅作为消息网关。
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
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::common::{ActorId, RetryPolicy};
    use crate::runtime::actor::ActorSystem;
    use crate::runtime::workflow::DagNode;

    fn make_node(id: &str, name: &str) -> DagNode {
        DagNode {
            task_id: TaskId::from(id.to_string()),
            name: name.to_string(),
            payload: Vec::new(),
            retry_policy: None,
            timeout_ms: None,
            priority: 0,
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn workflow_actor_submit_and_start_roundtrip() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("wf-actor-1");
        let actor = WorkflowActor::new(Orchestrator::new());
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.add_node(make_node("b", "task_b")).unwrap();
        dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();

        let wf_id = WorkflowId::from("wf-1");
        system
            .call(
                &actor_id,
                &workflow_methods::SUBMIT,
                encode(&(wf_id.clone(), dag)).unwrap(),
            )
            .await
            .unwrap();

        let result = system
            .call(
                &actor_id,
                &workflow_methods::START,
                encode(&wf_id).unwrap(),
            )
            .await
            .unwrap();
        assert!(result.error.is_none(), "{:?}", result.error);
        let roots: Vec<TaskDefinition> = decode(&result.payload).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id.as_str(), "a");

        system.stop(&actor_id).await.unwrap();
    }

    #[tokio::test]
    async fn scheduler_actor_enqueue_and_dequeue() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("sched-actor-1");
        system
            .spawn(actor_id.clone(), fifo_scheduler_actor())
            .await
            .unwrap();

        let tasks = vec![TaskDefinition {
            id: TaskId::from("t1"),
            name: "task1".into(),
            payload: vec![1, 2, 3],
            workflow_id: None,
            target_node: None,
            origin_node: None,
            retry_policy: Some(RetryPolicy::default()),
            priority: 0,
            timeout_ms: None,
            attempt: 0,
            enqueued_at_ms: 0,
            target_endpoint_addr: None,
            origin_endpoint_addr: None,
        }];

        system
            .call(
                &actor_id,
                &scheduler_methods::ENQUEUE_BATCH,
                encode(&tasks).unwrap(),
            )
            .await
            .unwrap();

        let result = system
            .call(
                &actor_id,
                &scheduler_methods::DEQUEUE_BATCH,
                encode(&1usize).unwrap(),
            )
            .await
            .unwrap();
        assert!(result.error.is_none());
        let out: Vec<TaskDefinition> = decode(&result.payload).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id.as_str(), "t1");

        system.stop(&actor_id).await.unwrap();
    }
}
