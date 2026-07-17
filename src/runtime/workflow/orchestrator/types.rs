//! Orchestrator 辅助类型：事件 payload、条件求值器、workflow slot、并发状态。
//!
//! 这些类型是 `Orchestrator` 操作的数据结构，与 `Orchestrator` 的 impl 解耦，
//! 便于独立阅读与测试。

use std::collections::HashMap;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use serde::{Deserialize, Serialize};

use crate::common::{Result, TaskId, WorkflowId};
use crate::runtime::workflow::{Dag, Terminal, WorkflowExecution};

/// 工作流级别事件，写入 `EventLog` 的 `workflow:{workflow_id}` topic。
///
/// 仅记录状态变迁元数据，不携带完整 DAG/Execution，避免事件体积过大。
/// 完整状态仍由 `Store` 持久化。所有变体由 orchestrator 在内部状态变迁时
/// 构造；Python 侧通过 EventLog 读取 API 观测这些事件（不透明字节），
/// 不依赖其 Rust 类型定义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowEventPayload {
    Submitted {
        workflow_id: WorkflowId,
    },
    Started {
        workflow_id: WorkflowId,
    },
    TaskRunning {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    TaskCompleted {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    TaskFailed {
        workflow_id: WorkflowId,
        task_id: TaskId,
        error: String,
    },
    TaskCancelled {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    Completed {
        workflow_id: WorkflowId,
    },
    Failed {
        workflow_id: WorkflowId,
        error: String,
    },
    /// 节点重启后从持久化存储恢复工作流时发出。
    ///
    /// `task_count` 为恢复后 slot 中的任务总数（含已完成、待执行、跳过）。
    /// `corrupt` 为 true 表示此 workflow 因数据损坏被整体移除（slot 已清理）。
    /// 此事件供外部观测恢复进度，不驱动任何核心状态变迁。
    Recovered {
        workflow_id: WorkflowId,
        task_count: usize,
        corrupt: bool,
    },
}

impl WorkflowEventPayload {
    pub fn workflow_id(&self) -> &WorkflowId {
        match self {
            Self::Submitted { workflow_id }
            | Self::Started { workflow_id }
            | Self::TaskRunning { workflow_id, .. }
            | Self::TaskCompleted { workflow_id, .. }
            | Self::TaskFailed { workflow_id, .. }
            | Self::TaskCancelled { workflow_id, .. }
            | Self::Completed { workflow_id }
            | Self::Failed { workflow_id, .. }
            | Self::Recovered { workflow_id, .. } => workflow_id,
        }
    }

    pub fn topic(&self) -> String {
        format!("workflow:{}", self.workflow_id().as_str())
    }
}

/// 条件分支求值器。
///
/// Rust 核心不内置任何求值语义；用户 Rust 扩展或 Python 层注入自定义实现后，
/// `Orchestrator::on_task_completed` 可在完成节点时直接评估条件边，无需外部循环。
#[async_trait]
pub trait ConditionEvaluator: Send + Sync {
    /// 对 `task_id` 出发的条件标签 `condition` 求值。
    ///
    /// 返回 `true` 表示该条件边被激活，对应后继任务进入待执行状态；
    /// 返回 `false` 表示跳过该分支。
    async fn evaluate(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        condition: &str,
    ) -> Result<bool>;
}

#[derive(Debug, Clone)]
pub struct CompletionInfo {
    pub workflow_terminal: bool,
    pub ready_successors: Vec<TaskId>,
    /// Conditional edges from the completed task: (successor_task_id, condition_tag).
    /// The Python orchestration loop evaluates these conditions and activates
    /// the selected branches via `activate_conditional_successor`.
    pub conditional_edges: Vec<(TaskId, String)>,
}

pub struct ReadyResult {
    pub(crate) ready: Vec<TaskId>,
    pub(crate) conditional: Vec<(TaskId, String)>,
}

/// Per-workflow state container stored in a DashMap for fine-grained locking.
/// Each workflow's dag, execution, and pending counts are bundled together
/// so that operations on a single workflow never block other workflows.
pub struct WorkflowSlot {
    pub dag: Dag,
    pub execution: WorkflowExecution,
    pub pending: HashMap<TaskId, usize>,
    /// Slot 生命周期阶段，区分占位符与已加载工作流。
    ///
    /// - `SlotState::Loading`: `adopt_workflow` 在无本地数据时插入的占位符，
    ///   等待 gossip 层通过 `restore_workflow` 填充真实数据。占位符的 `dag` 为空、
    ///   `execution` 无任务。对占位符执行 `start` / `submit` / `on_task_completed`
    ///   等操作会返回 `ActantError::InvalidState`，防止在数据到达前误操作。
    /// - `SlotState::Ready`: 已从本地存储或远程同步加载完成，可正常操作。
    pub state: SlotState,
}

/// 工作流 slot 的生命周期阶段。
///
/// 详见 [`WorkflowSlot::state`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// 占位符：`adopt_workflow` 插入的空 slot，等待远程数据填充。
    Loading,
    /// 已加载：DAG、execution、pending 均就绪，可正常操作。
    Ready,
}

/// 条件前驱不激活时对条件后继的处理决策。
///
/// `Orchestrator::skip_conditional_branch` 在标记任务为 Skipped 前先用
/// `Orchestrator::decrement_conditional_pending` 决定动作：
/// - `PendingRemaining`：仍有其他未完成前驱（pending > 0），不跳过任务
/// - `Ready`：pending 归零且有已完成前驱，任务变为 ready
/// - `Skip`：pending 归零且无已完成前驱，应跳过任务
pub(crate) enum ConditionalSkipDecision {
    PendingRemaining,
    Ready,
    Skip,
}

/// Per-workflow terminal state waiter registry.
///
/// Manages oneshot channels that resolve when a specific workflow reaches
/// terminal state. This follows the same pattern as Actix/Ractor RPC:
/// oneshot channel per request, resolved exactly once.
///
/// 提取自 `OrchestratorState` 以隔离等待者管理职责。所有方法都是 O(1) 操作。
pub(crate) struct TerminalWaiterRegistry {
    waiters: DashMap<WorkflowId, tokio::sync::oneshot::Sender<()>>,
}

impl TerminalWaiterRegistry {
    fn new() -> Self {
        Self {
            waiters: DashMap::new(),
        }
    }

    /// Register a oneshot receiver for a specific workflow's terminal state.
    /// Returns the receiver that will be resolved when the workflow completes.
    ///
    /// **必须由调用方在注册后检查工作流是否已终态**——若已终态，调用方应立即
    /// 触发 `fire`。这种"先注册后检查"的顺序关闭了竞态窗口：若工作流在检查
    /// 与注册之间变为终态，`fire` 会找到我们注册的 sender 并触发它。
    fn register(&self, workflow_id: WorkflowId) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.waiters.insert(workflow_id, tx);
        rx
    }

    /// Fire the oneshot for a workflow that has reached terminal state.
    /// Called from `notify_terminal()` and timeout watcher.
    /// 若无注册等待者，此操作为 no-op。
    fn fire(&self, workflow_id: &WorkflowId) {
        if let Some((_, tx)) = self.waiters.remove(workflow_id) {
            let _ = tx.send(());
        }
    }

    /// 移除等待者但不触发（用于工作流被移除时的清理）。
    fn remove(&self, workflow_id: &WorkflowId) {
        self.waiters.remove(workflow_id);
    }
}

/// 跟踪需要持久化但尚未写入存储的 workflow ID。
///
/// 后台 flush 任务周期性调用 `drain` 批量持久化。
/// 提取自 `OrchestratorState` 以隔离脏标记职责。
pub(crate) struct DirtyTracker {
    dirty: DashSet<WorkflowId>,
}

impl DirtyTracker {
    fn new() -> Self {
        Self {
            dirty: DashSet::new(),
        }
    }

    fn mark(&self, workflow_id: &WorkflowId) {
        self.dirty.insert(workflow_id.clone());
    }

    fn remove(&self, workflow_id: &WorkflowId) {
        self.dirty.remove(workflow_id);
    }

    /// Drain all dirty workflow IDs, returning them for batch persistence.
    fn drain(&self) -> Vec<WorkflowId> {
        let ids: Vec<WorkflowId> = self.dirty.iter().map(|r| r.key().clone()).collect();
        for id in &ids {
            self.dirty.remove(id);
        }
        ids
    }
}

/// Concurrent orchestrator state using per-workflow DashMap shards.
/// Eliminates the global RwLock bottleneck: different workflows can be
/// read and modified concurrently without contention.
///
/// 由三个独立职责的子结构组合而成：
/// - `slots`：workflow → WorkflowSlot 的并发映射
/// - `terminal_waiters`：终态等待者 oneshot 注册表
/// - `dirty_tracker`：脏 workflow 跟踪器
pub struct OrchestratorState {
    pub(crate) slots: DashMap<WorkflowId, WorkflowSlot>,
    pub(crate) terminal_waiters: TerminalWaiterRegistry,
    pub(crate) dirty_tracker: DirtyTracker,
}

impl Default for OrchestratorState {
    fn default() -> Self {
        Self::new()
    }
}

impl OrchestratorState {
    pub fn new() -> Self {
        Self {
            slots: DashMap::new(),
            terminal_waiters: TerminalWaiterRegistry::new(),
            dirty_tracker: DirtyTracker::new(),
        }
    }

    /// Register a oneshot receiver for a specific workflow's terminal state.
    /// Returns the receiver that will be resolved when the workflow completes.
    /// This is the event-driven equivalent of polling `ready()` in a loop.
    ///
    /// Race-free: inserts the waiter FIRST, then checks terminal state. If the
    /// workflow completes between the check and the insert, `fire_terminal_oneshot`
    /// will find our registered sender and fire it. If the workflow was already
    /// terminal at insert time, we resolve immediately and clean up the entry.
    pub fn register_terminal_waiter(
        &self,
        workflow_id: WorkflowId,
    ) -> tokio::sync::oneshot::Receiver<()> {
        // Insert BEFORE checking — this closes the race window. If the workflow
        // becomes terminal between our check and insert, fire_terminal_oneshot
        // will fire our sender instead of being a no-op.
        let rx = self.terminal_waiters.register(workflow_id.clone());

        // 现在检查是否已处于终态 — 若是，触发自身的 waiter。
        if let Some(slot) = self.slots.get(&workflow_id) {
            if slot.execution.is_terminal() {
                drop(slot);
                self.terminal_waiters.fire(&workflow_id);
                return rx;
            }
        }
        rx
    }

    /// Fire the oneshot for a workflow that has reached terminal state.
    /// Called from `notify_terminal()` and timeout watcher.
    pub(super) fn fire_terminal_oneshot(&self, workflow_id: &WorkflowId) {
        self.terminal_waiters.fire(workflow_id);
    }

    pub(crate) fn insert_workflow(
        &self,
        workflow_id: WorkflowId,
        dag: Dag,
        execution: WorkflowExecution,
        pending: HashMap<TaskId, usize>,
    ) {
        self.slots.insert(
            workflow_id,
            WorkflowSlot {
                dag,
                execution,
                pending,
                state: SlotState::Ready,
            },
        );
    }

    /// 插入占位符 slot，标记为 [`SlotState::Loading`]。
    ///
    /// `adopt_workflow` 在无本地数据时调用此方法注册 workflow ID，
    /// 使 gossip 层能通过 `contains_workflow` 发现待恢复的工作流。
    /// 占位符的 `dag` 为空、`execution` 无任务，`restore_workflow` 会用
    /// 真实数据覆盖并通过 `insert_workflow` 将状态设为 `Ready`。
    pub(crate) fn insert_placeholder(&self, workflow_id: WorkflowId) {
        let execution = WorkflowExecution::new(workflow_id.clone(), vec![]);
        self.slots.insert(
            workflow_id,
            WorkflowSlot {
                dag: Dag::new(),
                execution,
                pending: HashMap::new(),
                state: SlotState::Loading,
            },
        );
    }

    /// 返回 slot 是否已就绪（[`SlotState::Ready`]）。
    pub(crate) fn is_ready(&self, workflow_id: &WorkflowId) -> bool {
        self.slots
            .get(workflow_id)
            .is_some_and(|slot| slot.state == SlotState::Ready)
    }

    pub(crate) fn remove_workflow(&self, workflow_id: &WorkflowId) {
        self.slots.remove(workflow_id);
        self.dirty_tracker.remove(workflow_id);
        self.terminal_waiters.remove(workflow_id);
    }

    pub(crate) fn contains_workflow(&self, workflow_id: &WorkflowId) -> bool {
        self.slots.contains_key(workflow_id)
    }

    /// Mark a workflow as needing persistence. The background flush task
    /// will serialize and write it to the store.
    pub(crate) fn mark_dirty(&self, workflow_id: &WorkflowId) {
        self.dirty_tracker.mark(workflow_id);
    }

    /// Drain all dirty workflow IDs, returning them for batch persistence.
    pub(crate) fn drain_dirty(&self) -> Vec<WorkflowId> {
        self.dirty_tracker.drain()
    }

    pub(crate) fn active_workflow_ids(&self) -> Vec<WorkflowId> {
        self.slots
            .iter()
            .filter(|entry| !entry.execution.is_terminal())
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub(crate) fn expired_workflow_ids(&self) -> Vec<WorkflowId> {
        self.slots
            .iter()
            .filter(|entry| !entry.execution.is_terminal() && entry.execution.is_expired())
            .map(|entry| entry.key().clone())
            .collect()
    }
}
