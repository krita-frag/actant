use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use serde::{Deserialize, Serialize};

use crate::common::payload::unpack_payload;
use crate::common::{
    serialization::serialize_rkyv, ActantConfig, NodeId, Result, TaskDefinition, TaskId,
    WorkflowId, STORE_KEY_DAG, STORE_KEY_EXEC, STORE_KEY_PENDING, STORE_KEY_RESULT,
};
use crate::runtime::state::event_log::EventLog;
use crate::runtime::state::{HybridLogicalClock, Store};

use super::*;

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
    ready: Vec<TaskId>,
    conditional: Vec<(TaskId, String)>,
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
enum ConditionalSkipDecision {
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
struct TerminalWaiterRegistry {
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
struct DirtyTracker {
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
    slots: DashMap<WorkflowId, WorkflowSlot>,
    terminal_waiters: TerminalWaiterRegistry,
    dirty_tracker: DirtyTracker,
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
    fn fire_terminal_oneshot(&self, workflow_id: &WorkflowId) {
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

pub struct Orchestrator {
    state: Arc<OrchestratorState>,
    config: ActantConfig,
    store: Option<Store>,
    event_log: Option<Arc<dyn EventLog>>,
    /// 条件分支求值器。`None` 时 `on_task_completed` 将条件边返回给调用方
    ///（如 Python 编排循环）外部评估。
    condition_evaluator: Option<Arc<dyn ConditionEvaluator>>,
    node_id: Option<NodeId>,
    hlc: Arc<HybridLogicalClock>,
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        tracing::debug!(
            "Orchestrator::drop — store is_some = {}",
            self.store.is_some()
        );
    }
}

impl Clone for Orchestrator {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            config: self.config.clone(),
            store: self.store.clone(),
            event_log: self.event_log.clone(),
            condition_evaluator: self.condition_evaluator.clone(),
            node_id: self.node_id.clone(),
            hlc: self.hlc.clone(),
        }
    }
}

impl Orchestrator {
    pub fn new() -> Self {
        Self {
            state: Arc::new(OrchestratorState::new()),
            config: ActantConfig::default(),
            store: None,
            event_log: None,
            condition_evaluator: None,
            node_id: None,
            hlc: Arc::new(HybridLogicalClock::new()),
        }
    }

    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn with_signing_key(mut self, key: Vec<u8>) -> Self {
        self.config.payload_signing_key = key;
        self
    }

    pub fn with_config(mut self, config: ActantConfig) -> Self {
        self.hlc = Arc::new(HybridLogicalClock::with_max_drift_ms(
            config.network.hlc_max_drift_ms,
        ));
        self.config = config;
        self
    }

    pub fn with_store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_event_log(mut self, event_log: Arc<dyn EventLog>) -> Self {
        self.event_log = Some(event_log);
        self
    }

    pub fn with_condition_evaluator(mut self, evaluator: Arc<dyn ConditionEvaluator>) -> Self {
        self.condition_evaluator = Some(evaluator);
        self
    }

    fn log_event(&self, payload: WorkflowEventPayload) {
        if let Some(log) = self.event_log.as_ref() {
            let topic = payload.topic();
            match postcard::to_allocvec(&payload) {
                Ok(bytes) => {
                    if let Err(e) = log.append(&topic, &bytes) {
                        tracing::warn!(error = %e, topic = %topic, "failed to append workflow event");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, topic = %topic, "failed to serialize workflow event");
                }
            }
        }
        // 规则评估在 Python 编排循环中实现：远端订阅者通过
        // WireEnvelope::TaskDispatch / TaskResult 接收事件，业务规则在
        // Python 侧订阅 EventBus 自行处理。
    }

    /// 从持久化 [Store] 恢复 orchestrator 状态。
    ///
    /// 扫描所有已知前缀（dag、exec、pending）重建内存状态。Running 状态的
    /// 任务被重置为 Pending，以便重启后重新调度。
    ///
    /// **事件记录**：每个成功恢复的 workflow 会向 `event_log`（若提供）
    /// 追加一条 [`WorkflowEventPayload::Recovered`] 事件（`corrupt=false`），
    /// 损坏被移除的 workflow 追加 `corrupt=true` 事件。外部订阅者可通过
    /// `workflow:{id}` topic 观测恢复进度。
    ///
    /// **损坏处理**：dag / exec / pending 三类条目对同一 workflow 是绑定的
    /// （三者共同构成 `WorkflowSlot`）。若任一条目反序列化失败，则整个
    /// workflow 视为损坏——已创建的 slot 将被移除，对应的 store 条目也会
    /// 被清理，避免"exec 数据悬空而 dag 缺失"导致后续 `on_task_completed`
    /// 等操作因 `workflow not found` 失败。损坏事件通过 tracing::warn 记录。
    pub async fn recover(
        store: Store,
        config: ActantConfig,
        event_log: Option<Arc<dyn EventLog>>,
    ) -> Result<Self> {
        config.validate()?;
        let state = Arc::new(OrchestratorState::new());

        // 跟踪所有反序列化失败的 workflow ID。任意一类条目损坏即视为整个 workflow 损坏。
        let mut corrupt: HashSet<WorkflowId> = HashSet::new();

        let dag_entries = store.scan_prefix(STORE_KEY_DAG).await?;
        for (key, data) in dag_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_DAG).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match rkyv::from_bytes::<Dag, rkyv::rancor::Error>(&data) {
                Ok(dag) => {
                    let task_ids: Vec<TaskId> = dag.nodes().map(|n| n.task_id.clone()).collect();
                    let mut pending: HashMap<TaskId, usize> = HashMap::new();
                    for node in dag.nodes() {
                        let pred_count = dag.predecessor_count(&node.task_id);
                        pending.insert(node.task_id.clone(), pred_count);
                    }
                    let execution = WorkflowExecution::new(wf_id.clone(), task_ids)
                        .with_failure_strategy(dag.failure_strategy);
                    state.insert_workflow(wf_id, dag, execution, pending);
                }
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt dag entry, marking workflow as corrupt",
                    );
                    corrupt.insert(wf_id);
                }
            }
        }

        let exec_entries = store.scan_prefix(STORE_KEY_EXEC).await?;
        for (key, data) in exec_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_EXEC).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match rkyv::from_bytes::<WorkflowExecution, rkyv::rancor::Error>(&data) {
                Ok(mut execution) => {
                    if !execution.is_terminal() {
                        let task_ids: Vec<TaskId> = execution.tasks.keys().cloned().collect();
                        for tid in &task_ids {
                            execution.reset_task(tid, false, true);
                        }
                    }
                    if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                        slot.execution = execution;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt exec entry, marking workflow as corrupt",
                    );
                    corrupt.insert(wf_id);
                }
            }
        }

        let pending_entries = store.scan_prefix(STORE_KEY_PENDING).await?;
        for (key, data) in pending_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_PENDING).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match rkyv::from_bytes::<HashMap<TaskId, usize>, rkyv::rancor::Error>(&data) {
                Ok(pending) => {
                    if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                        slot.pending = pending;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt pending entry, marking workflow as corrupt",
                    );
                    corrupt.insert(wf_id);
                }
            }
        }

        // 任何条目损坏的 workflow：移除内存 slot，并删除 store 中的所有三类条目，
        // 避免"exec 数据悬空而 dag 缺失"等不一致状态在后续操作中触发 workflow not found。
        for wf_id in &corrupt {
            state.remove_workflow(wf_id);
            // 删除 store 中的所有三类条目（若存在），清理孤儿数据。
            if let Err(e) = store.delete(&dag_key(wf_id)).await {
                tracing::warn!(workflow = %wf_id.as_str(), error = %e, "recover: failed to delete corrupt dag entry");
            }
            if let Err(e) = store.delete(&exec_key(wf_id)).await {
                tracing::warn!(workflow = %wf_id.as_str(), error = %e, "recover: failed to delete corrupt exec entry");
            }
            if let Err(e) = store.delete(&pending_key(wf_id)).await {
                tracing::warn!(workflow = %wf_id.as_str(), error = %e, "recover: failed to delete corrupt pending entry");
            }
            crate::metrics::inc_workflows_recovered_corrupt();
            tracing::warn!(
                workflow = %wf_id.as_str(),
                "recover: workflow removed due to corrupt data; total corrupt={}",
                corrupt.len(),
            );
            // 记录损坏恢复事件（corrupt=true）供外部观测。
            Self::log_recovered_event(event_log.as_deref(), wf_id, 0, true);
        }

        // 为成功恢复的非损坏 workflow 记录恢复事件（corrupt=false）。
        // 遍历 state.slots，跳过已标记为 corrupt 的 workflow。
        for entry in state.slots.iter() {
            let wf_id = entry.key();
            if corrupt.contains(wf_id) {
                continue;
            }
            let task_count = entry.execution.tasks.len();
            Self::log_recovered_event(event_log.as_deref(), wf_id, task_count, false);
        }

        let orchestrator = Self {
            state,
            config: config.clone(),
            store: Some(store),
            event_log,
            condition_evaluator: None,
            node_id: None,
            hlc: Arc::new(HybridLogicalClock::with_max_drift_ms(
                config.network.hlc_max_drift_ms,
            )),
        };

        Ok(orchestrator)
    }

    /// 记录 [`WorkflowEventPayload::Recovered`] 事件到 event_log（若存在）。
    ///
    /// 此为 `recover` 内部的辅助函数——`recover` 是关联函数，构造 `Self` 前
    /// 无法调用实例方法 `log_event`，因此直接操作 `event_log` 引用。
    fn log_recovered_event(
        event_log: Option<&dyn EventLog>,
        workflow_id: &WorkflowId,
        task_count: usize,
        corrupt: bool,
    ) {
        let Some(log) = event_log else {
            return;
        };
        let payload = WorkflowEventPayload::Recovered {
            workflow_id: workflow_id.clone(),
            task_count,
            corrupt,
        };
        let topic = payload.topic();
        match postcard::to_allocvec(&payload) {
            Ok(bytes) => {
                if let Err(e) = log.append(&topic, &bytes) {
                    tracing::warn!(
                        error = %e,
                        topic = %topic,
                        "recover: failed to append Recovered event",
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    topic = %topic,
                    "recover: failed to serialize Recovered event",
                );
            }
        }
    }

    /// Submits a workflow DAG for execution.
    ///
    /// 若 `workflow_id` 已存在且为占位符（[`SlotState::Loading`]），此调用会覆盖
    /// 占位符——这是 `adopt_workflow` 后本地再次提交同一 workflow 的合法路径。
    /// 若已存在且为 [`SlotState::Ready`]，返回 [`ActantError::AlreadyExists`]。
    #[tracing::instrument(level = "debug", skip(self, dag), fields(workflow = %workflow_id, nodes = dag.nodes().count()))]
    pub async fn submit(&self, workflow_id: WorkflowId, dag: Dag) -> Result<()> {
        // 已就绪的工作流不允许重复提交；占位符允许覆盖（adopt 后本地重新提交）。
        if self.state.is_ready(&workflow_id) {
            return Err(crate::common::ActantError::AlreadyExists(format!(
                "workflow {} already submitted",
                workflow_id.as_str()
            )));
        }
        let task_ids: Vec<TaskId> = dag.nodes().map(|n| n.task_id.clone()).collect();

        let mut pending: HashMap<TaskId, usize> = HashMap::new();
        for node in dag.nodes() {
            let pred_count = dag.predecessor_count(&node.task_id);
            pending.insert(node.task_id.clone(), pred_count);
        }

        let execution = WorkflowExecution::new(workflow_id.clone(), task_ids)
            .with_failure_strategy(dag.failure_strategy);

        if let Some(ref store) = self.store {
            let dag_bytes = serialize_rkyv(&dag)?;
            let exec_bytes = serialize_rkyv(&execution)?;
            let pending_bytes = serialize_rkyv(&pending)?;
            store
                .put_batch(&[
                    (dag_key(&workflow_id), dag_bytes),
                    (exec_key(&workflow_id), exec_bytes),
                    (pending_key(&workflow_id), pending_bytes),
                ])
                .await?;
        }

        self.state
            .insert_workflow(workflow_id.clone(), dag, execution, pending);

        self.log_event(WorkflowEventPayload::Submitted { workflow_id });

        crate::metrics::inc_workflows_submitted();
        crate::metrics::inc_active_workflows();
        Ok(())
    }

    pub async fn submit_with_timeout(
        &self,
        workflow_id: WorkflowId,
        dag: Dag,
        timeout_ms: u64,
    ) -> Result<()> {
        self.submit(workflow_id.clone(), dag).await?;
        if let Some(mut slot) = self.state.slots.get_mut(&workflow_id) {
            slot.execution.set_deadline_ms(timeout_ms);
        }
        Ok(())
    }

    /// Starts a workflow by marking it Running and returning root tasks.
    pub fn start(&self, workflow_id: &WorkflowId) -> Result<Vec<TaskDefinition>> {
        let roots = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            if slot.state == SlotState::Loading {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot start",
                    workflow_id.as_str()
                )));
            }
            slot.execution.mark_running();

            let origin_node = self.node_id.clone();
            slot.dag
                .roots()
                .into_iter()
                .map(|node| {
                    let attempt = slot
                        .execution
                        .tasks
                        .get(&node.task_id)
                        .map(|t| t.attempt())
                        .unwrap_or(0);
                    TaskDefinition {
                        id: node.task_id.clone(),
                        name: node.name.clone(),
                        payload: node.payload.clone(),
                        workflow_id: Some(workflow_id.clone()),
                        target_node: None,
                        origin_node: origin_node.clone(),
                        retry_policy: slot.dag.effective_retry_policy(&node.task_id),
                        priority: node.priority,
                        timeout_ms: node.timeout_ms,
                        attempt,
                        enqueued_at_ms: 0,
                        target_endpoint_addr: None,
                        origin_endpoint_addr: None,
                    }
                })
                .collect()
        };

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        self.log_event(WorkflowEventPayload::Started {
            workflow_id: workflow_id.clone(),
        });

        Ok(roots)
    }

    /// Handles a task completion, decrements dependent task counters, and
    /// returns any successor tasks that have become ready.
    ///
    /// 若 `condition_evaluator` 已设置，条件边在 Rust 核心内直接求值并处理，
    /// 返回空的 `conditional_edges`；否则将条件边返回给调用方（如 Python 编排
    /// 循环）外部评估。
    ///
    /// 返回值第三个元素 `workflow_terminal` 是显式的终态标志，**不应**通过
    /// `ready.is_empty() && conditional_edges.is_empty()` 推断——条件求值器
    /// 全部跳过条件后继也会产生空列表，但工作流未必进入终态（其他分支可能
    /// 仍在运行）。调用方（如 `WorkflowActor`）必须使用此标志判断是否触发
    /// 终态通知。
    pub async fn on_task_completed(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        result: Vec<u8>,
    ) -> Result<(Vec<TaskDefinition>, Vec<(TaskId, String)>, bool)> {
        // 阶段 1：更新执行状态、计算 ready 与 conditional_edges、判断终态。
        let info = self.complete_task(workflow_id, task_id, result).await?;
        if info.workflow_terminal {
            crate::metrics::inc_workflows_completed();
            crate::metrics::dec_active_workflows();
            return Ok((vec![], vec![], true));
        }

        // 阶段 2：将 ready_successors 转换为 TaskDefinition。
        let mut ready = self.build_ready_tasks_for(workflow_id, &info.ready_successors)?;

        // 阶段 3：处理条件边——内部求值（若有 evaluator）或返回给调用方外部评估。
        let conditional_edges = self
            .process_conditional_edges(workflow_id, task_id, info.conditional_edges, &mut ready)
            .await?;

        Ok((ready, conditional_edges, false))
    }

    /// 处理条件边：内部求值或返回给调用方。
    ///
    /// 若 `condition_evaluator` 已设置，对每条条件边求值：
    /// - 激活 → 减少后继 pending 计数，可能加入 ready
    /// - 不激活 → 级联跳过该后继分支，可能产生新的 ready
    ///
    /// 求值后返回空列表（所有条件边已在内部处理）。
    ///
    /// 若未设置 evaluator，原样返回条件边列表，由调用方（如 Python 编排循环）外部评估。
    ///
    /// 此方法是 `on_task_completed` 的"阶段 3"，提取自原函数以隔离条件求值逻辑。
    async fn process_conditional_edges(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        conditional_edges: Vec<(TaskId, String)>,
        ready: &mut Vec<TaskDefinition>,
    ) -> Result<Vec<(TaskId, String)>> {
        let mut conditional_edges = conditional_edges;
        if let Some(evaluator) = self.condition_evaluator.as_ref() {
            for (succ_id, condition) in &conditional_edges {
                let activate = evaluator.evaluate(workflow_id, task_id, condition).await?;
                if activate {
                    if let Some(task) = self.activate_conditional_successor(workflow_id, succ_id)? {
                        ready.push(task);
                    }
                } else {
                    let cascade_ready = self.skip_conditional_branch(workflow_id, succ_id).await?;
                    ready.extend(cascade_ready);
                }
            }
            conditional_edges = Vec::new();
        }
        Ok(conditional_edges)
    }

    /// Activate a conditional successor after Python evaluates the condition.
    /// Decrements the pending count and returns the task definition if it
    /// becomes ready (pending count reaches zero).
    pub fn activate_conditional_successor(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Option<TaskDefinition>> {
        let ready = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            let count = slot.pending.get_mut(task_id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "pending count not found for task {}",
                    task_id.as_str()
                ))
            })?;
            if *count == 0 {
                return Ok(None);
            }
            *count -= 1;
            if *count == 0 {
                Some(task_id.clone())
            } else {
                None
            }
        };
        match ready {
            Some(tid) => {
                let tasks = self.build_ready_tasks_for(workflow_id, &[tid])?;
                Ok(tasks.into_iter().next())
            }
            None => Ok(None),
        }
    }

    /// Skip a conditional branch that was not taken.
    ///
    /// 条件前驱完成且条件不激活时，减少 `task_id` 的 pending 计数（对应条件前驱）。
    /// 根据剩余 pending 决定后续行为：
    /// - pending > 0：仍有其他未完成前驱，**不跳过** task_id，仅减少 pending
    /// - pending == 0 且有已完成前驱：task_id 变为 ready（返回）
    /// - pending == 0 且无已完成前驱：标记 task_id 为 Skipped，级联跳过其非条件后继
    ///
    /// 级联跳过后继的逻辑：
    ///   - 后继 pending 归零且有已完成前驱 → ready（返回）
    ///   - 后继 pending 归零且所有前驱均被跳过 → 级联跳过
    ///
    /// 此方法防止 BranchRef consumer 依赖两个分支但只有一个执行时死锁。
    pub async fn skip_conditional_branch(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Vec<TaskDefinition>> {
        // 阶段 0：减少条件前驱对应的 pending 计数，决定是否应跳过 task_id。
        // compute_ready_successors 跳过条件后继的 pending 减少，延迟到此处处理。
        match self.decrement_conditional_pending(workflow_id, task_id)? {
            ConditionalSkipDecision::PendingRemaining => {
                // 仍有其他未完成前驱，不跳过 task_id。
                self.state.mark_dirty(workflow_id);
                return Ok(Vec::new());
            }
            ConditionalSkipDecision::Ready => {
                // pending 归零且有已完成前驱 → task_id 变为 ready。
                self.state.mark_dirty(workflow_id);
                // build_ready_tasks_for 接受 &[TaskId]（owned），无法用 from_ref 消除 clone。
                #[allow(clippy::cloned_ref_to_slice_refs)]
                return self.build_ready_tasks_for(workflow_id, &[task_id.clone()]);
            }
            ConditionalSkipDecision::Skip => {
                // pending 归零且无已完成前驱 → 跳过 task_id，继续级联逻辑。
            }
        }

        // 阶段 1：标记被跳过的任务。若工作流直接进入终态，立即收尾。
        if let Some(execution) = self.mark_skipped_and_check_terminal(workflow_id, task_id)? {
            self.complete_terminal(workflow_id, task_id, &execution)
                .await?;
            return Ok(Vec::new());
        }

        // 阶段 2：级联跳过——沿非条件后继边递归减少 pending，收集 ready 与新增跳过任务。
        let ready_ids = self.cascade_skip(workflow_id, task_id).await?;

        // 阶段 3：级联后若工作流进入终态，完成收尾。
        if let Some(execution) = self.execution_if_terminal(workflow_id)? {
            self.complete_terminal(workflow_id, task_id, &execution)
                .await?;
            return Ok(Vec::new());
        }

        self.state.mark_dirty(workflow_id);
        let ready = self.build_ready_tasks_for(workflow_id, &ready_ids)?;
        Ok(ready)
    }

    /// 减少条件前驱对应的 pending 计数，并决定后续行为。
    ///
    /// `compute_ready_successors` 完成前驱时不减少条件后继的 pending（条件边需求值后才处理）。
    /// 此方法在条件求值返回 false 时调用，减少 pending 一次，对应已完成的条件前驱。
    ///
    /// 决策逻辑：
    /// - 减少后 pending > 0 → `PendingRemaining`（仍有其他前驱未完成）
    /// - 减少后 pending == 0 且有已完成前驱 → `Ready`
    /// - 减少后 pending == 0 且无已完成前驱 → `Skip`
    fn decrement_conditional_pending(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<ConditionalSkipDecision> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;

        let count = slot.pending.get_mut(task_id).ok_or_else(|| {
            crate::common::ActantError::Internal(format!(
                "pending count not found for task {}",
                task_id.as_str()
            ))
        })?;
        if *count > 0 {
            *count -= 1;
        }
        if *count > 0 {
            return Ok(ConditionalSkipDecision::PendingRemaining);
        }

        // pending 归零：检查是否有任意前驱产生了结果。
        let has_result = slot.dag.predecessors_of(task_id).iter().any(|pred| {
            slot.execution
                .tasks
                .get(&pred.task_id)
                .and_then(|t| t.result.as_ref())
                .is_some()
        });
        if has_result {
            Ok(ConditionalSkipDecision::Ready)
        } else {
            Ok(ConditionalSkipDecision::Skip)
        }
    }

    /// 标记 `task_id` 为 `Skipped`。
    ///
    /// 返回 `Some(execution)` 表示工作流因此次标记直接进入终态（如该任务是唯一未完成任务），
    /// 调用方应执行终态收尾。返回 `None` 表示工作流仍在运行，需继续级联跳过逻辑。
    ///
    /// 此方法是 `skip_conditional_branch` 的"阶段 1"，提取自原函数以隔离状态修改与终态判定。
    fn mark_skipped_and_check_terminal(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Option<WorkflowExecution>> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        slot.execution.mark_task_skipped(task_id);
        if slot.execution.is_terminal() {
            Ok(Some(slot.execution.clone()))
        } else {
            Ok(None)
        }
    }

    /// 沿非条件后继边递归减少 pending 计数，处理级联跳过。
    ///
    /// 起点是 `task_id`（已由 `mark_skipped_and_check_terminal` 标记为 Skipped）。
    /// 对每个起点的非条件后继：
    /// - pending 归零且有已完成前驱 → 加入 ready 列表（返回给调用方调度）
    /// - pending 归零且所有前驱均被跳过 → 级联标记为 Skipped，加入 worklist 继续传播
    ///
    /// 返回所有因级联而变为 ready 的任务 ID。
    ///
    /// 此方法是 `skip_conditional_branch` 的"阶段 2"。
    async fn cascade_skip(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Vec<TaskId>> {
        let mut worklist: Vec<TaskId> = vec![task_id.clone()];
        let mut ready_ids: Vec<TaskId> = Vec::new();

        while let Some(skipped_id) = worklist.pop() {
            let (newly_ready, mut newly_skipped) = {
                let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                let successors: Vec<TaskId> = slot.dag.successor_ids(&skipped_id);
                let conditional: Vec<TaskId> = slot
                    .dag
                    .conditional_edges_from(&skipped_id)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect();

                let mut ready = Vec::new();
                let mut cascade = Vec::new();
                for succ_id in &successors {
                    // 条件后继由单独逻辑处理，跳过。
                    if conditional.iter().any(|id| id == succ_id) {
                        continue;
                    }
                    let count = match slot.pending.get_mut(succ_id) {
                        Some(c) if *c > 0 => {
                            *c -= 1;
                            *c
                        }
                        _ => continue,
                    };
                    if count != 0 {
                        continue;
                    }
                    // pending 归零：检查是否有任意前驱产生了结果。
                    let has_result = slot.dag.predecessors_of(succ_id).iter().any(|pred| {
                        slot.execution
                            .tasks
                            .get(&pred.task_id)
                            .and_then(|t| t.result.as_ref())
                            .is_some()
                    });
                    if has_result {
                        ready.push(succ_id.clone());
                    } else {
                        // 所有前驱均被跳过 → 级联跳过此任务。
                        slot.execution.mark_task_skipped(succ_id);
                        cascade.push(succ_id.clone());
                    }
                }
                (ready, cascade)
            };
            ready_ids.extend(newly_ready);
            worklist.append(&mut newly_skipped);
        }

        Ok(ready_ids)
    }

    /// 返回 `Some(execution)` 若工作流当前处于终态，否则 `None`。
    ///
    /// 此方法是 `skip_conditional_branch` 的"阶段 3"的一部分，
    /// 隔离终态判定与终态收尾逻辑，避免在每个调用点重复 NotFound 检查。
    fn execution_if_terminal(&self, workflow_id: &WorkflowId) -> Result<Option<WorkflowExecution>> {
        let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        if slot.execution.is_terminal() {
            Ok(Some(slot.execution.clone()))
        } else {
            Ok(None)
        }
    }

    /// Cancels a running workflow and moves it to a terminal Cancelled state.
    pub async fn cancel(&self, workflow_id: &WorkflowId) -> Result<()> {
        let store_writes = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.mark_cancelled();
            if self.store.is_some() {
                Some(slot.execution.clone())
            } else {
                None
            }
        };

        if let Some(exec_snapshot) = store_writes {
            if let Some(ref store) = self.store {
                let exec_bytes = serialize_rkyv(&exec_snapshot)?;
                store
                    .put_batch(&[(exec_key(workflow_id), exec_bytes)])
                    .await?;
            }
        }

        self.notify_terminal();
        Ok(())
    }

    /// Cancel a single running task within a workflow.
    /// Returns Ok(true) if the task was running and is now cancelled,
    /// Ok(false) if the task was not in a running state.
    pub fn cancel_task(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<bool> {
        let cancelled = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.cancel_task(task_id)
        };
        if cancelled {
            self.log_event(WorkflowEventPayload::TaskCancelled {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
            });
        }
        Ok(cancelled)
    }

    /// Spawns a background task that periodically checks for expired workflows
    /// and marks them failed. Returns a watch sender for shutdown signaling.
    pub fn start_timeout_watcher(&self) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let state = self.state.clone();
        let store = self.store.clone();
        let poll_interval =
            std::time::Duration::from_millis(self.config.workflow.state_poll_interval_ms);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = interval.tick() => {
                        let expired: Vec<WorkflowId> = state.expired_workflow_ids();
                        for wf_id in expired {
                            if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                                if !slot.execution.is_terminal() {
                                    slot.execution.mark_workflow_failed("workflow timeout exceeded".into());
                                    crate::metrics::inc_workflow_timeouts();
                                    crate::metrics::inc_workflows_failed();
                                    crate::metrics::dec_active_workflows();
                                    tracing::warn!("workflow {} timed out, marked as failed", wf_id.as_str());

                                    if let Some(ref store) = store {
                                        if let Ok(exec_bytes) = serialize_rkyv(&slot.execution) {
                                            if let Err(e) = store.put_batch(&[(exec_key(&wf_id), exec_bytes)]).await {
                                                tracing::error!("failed to persist timed-out workflow {}: {}", wf_id.as_str(), e);
                                            }
                                        }
                                    }

                                    state.fire_terminal_oneshot(&wf_id);
                                }
                            }
                        }
                    }
                }
            }
        });

        cancel_tx
    }

    /// Spawns a background task that periodically flushes dirty workflow
    /// execution states to the store. This replaces per-operation persistence
    /// with batched writes, significantly reducing write amplification.
    ///
    /// Terminal states are always persisted immediately by the caller;
    /// this task only handles non-terminal dirty states.
    pub fn start_persist_flush(&self) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let state = self.state.clone();
        let store = self.store.clone();
        let flush_interval =
            std::time::Duration::from_millis(self.config.workflow.persist_flush_interval_ms);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = interval.tick() => {
                        let dirty_ids = state.drain_dirty();
                        if dirty_ids.is_empty() {
                            continue;
                        }
                        let Some(ref store) = store else {
                            continue;
                        };

                        let mut batch: Vec<(String, Vec<u8>)> = Vec::new();
                        for wf_id in &dirty_ids {
                            let Some(slot) = state.slots.get(wf_id) else {
                                continue;
                            };
                            if let Ok(exec_bytes) = serialize_rkyv(&slot.execution) {
                                batch.push((exec_key(wf_id), exec_bytes));
                            }
                            if let Ok(pending_bytes) = serialize_rkyv(&slot.pending) {
                                batch.push((pending_key(wf_id), pending_bytes));
                            }
                        }
                        if !batch.is_empty() {
                            if let Err(e) = store.put_batch(&batch).await {
                                tracing::warn!("persist flush failed: {}", e);
                            }
                        }
                    }
                }
            }
        });

        cancel_tx
    }

    /// Immediately persist all dirty workflow states to the store.
    /// Useful for testing and graceful shutdown.
    pub async fn flush_dirty(&self) -> Result<()> {
        let Some(ref store) = self.store else {
            return Ok(());
        };
        let dirty_ids = self.state.drain_dirty();
        if dirty_ids.is_empty() {
            return Ok(());
        }
        let mut batch: Vec<(String, Vec<u8>)> = Vec::new();
        for wf_id in &dirty_ids {
            let Some(slot) = self.state.slots.get(wf_id) else {
                continue;
            };
            if let Ok(exec_bytes) = serialize_rkyv(&slot.execution) {
                batch.push((exec_key(wf_id), exec_bytes));
            }
            if let Ok(pending_bytes) = serialize_rkyv(&slot.pending) {
                batch.push((pending_key(wf_id), pending_bytes));
            }
        }
        if !batch.is_empty() {
            store.put_batch(&batch).await?;
        }
        Ok(())
    }

    /// Returns a snapshot of the workflow execution state, or `None` if not found.
    pub fn get_state(&self, workflow_id: &WorkflowId) -> Option<WorkflowExecution> {
        self.state
            .slots
            .get(workflow_id)
            .map(|s| s.execution.clone())
    }

    pub fn get_dag(&self, workflow_id: &WorkflowId) -> Option<Dag> {
        self.state.slots.get(workflow_id).map(|s| s.dag.clone())
    }

    fn build_ready_tasks_from_slot(
        &self,
        slot: &WorkflowSlot,
        workflow_id: &WorkflowId,
        ready_ids: &[TaskId],
    ) -> Result<Vec<TaskDefinition>> {
        let mut ready: Vec<TaskDefinition> = Vec::with_capacity(ready_ids.len());
        for succ_id in ready_ids {
            let node = slot.dag.get_node(succ_id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "node {} not found in dag",
                    succ_id.as_str()
                ))
            })?;

            let payload = build_task_payload(
                &slot.dag,
                &slot.execution,
                succ_id,
                &node.payload,
                &self.config.payload_signing_key,
            )?;

            let attempt = slot
                .execution
                .tasks
                .get(succ_id)
                .map(|t| t.attempt())
                .unwrap_or(0);

            ready.push(TaskDefinition {
                id: succ_id.clone(),
                name: node.name.clone(),
                payload,
                workflow_id: Some(workflow_id.clone()),
                target_node: None,
                origin_node: self.node_id.clone(),
                retry_policy: slot.dag.effective_retry_policy(&node.task_id),
                priority: node.priority,
                timeout_ms: node.timeout_ms,
                attempt,
                enqueued_at_ms: 0,
                target_endpoint_addr: None,
                origin_endpoint_addr: None,
            });
        }

        Ok(ready)
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    pub fn store(&self) -> &Option<Store> {
        &self.store
    }

    pub fn state_handle(&self) -> Arc<OrchestratorState> {
        self.state.clone()
    }

    pub fn active_workflow_ids(&self) -> Vec<WorkflowId> {
        self.state.active_workflow_ids()
    }

    pub fn has_workflow(&self, workflow_id: &WorkflowId) -> bool {
        self.state.contains_workflow(workflow_id)
    }

    /// Serialize the current workflow state (dag, execution, pending) as rkyv bytes.
    /// Returns (dag_bytes, exec_bytes, pending_bytes) or None if workflow not found.
    pub async fn get_workflow_state_bytes(
        &self,
        workflow_id: &WorkflowId,
    ) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        if let Some(slot) = self.state.slots.get(workflow_id) {
            let dag_bytes = serialize_rkyv(&slot.dag).ok()?;
            let exec_bytes = serialize_rkyv(&slot.execution).ok()?;
            let pending_bytes = serialize_rkyv(&slot.pending).ok()?;
            return Some((dag_bytes, exec_bytes, pending_bytes));
        }
        if let Some(ref store) = self.store {
            let dag = store.get(&dag_key(workflow_id)).await.ok().flatten()?;
            let exec = store.get(&exec_key(workflow_id)).await.ok().flatten()?;
            let pending = store.get(&pending_key(workflow_id)).await.ok().flatten()?;
            return Some((dag, exec, pending));
        }
        None
    }

    pub fn remove_active_workflow(&self, workflow_id: &WorkflowId) {
        self.state.remove_workflow(workflow_id);
    }

    /// 强制删除一个工作流及其持久化数据。
    pub async fn delete_workflow(&self, workflow_id: &WorkflowId) {
        self.evict_workflow(workflow_id).await;
    }

    pub async fn adopt_workflow(&self, workflow_id: &WorkflowId) -> Result<()> {
        if self.state.contains_workflow(workflow_id) {
            return Ok(());
        }

        if let Some(ref store) = self.store {
            let dag_key = dag_key(workflow_id);
            if let Ok(Some(data)) = store.get(&dag_key).await {
                if let Ok(dag) = rkyv::from_bytes::<Dag, rkyv::rancor::Error>(&data) {
                    let task_ids: Vec<TaskId> = dag.nodes().map(|n| n.task_id.clone()).collect();
                    let mut pending: HashMap<TaskId, usize> = HashMap::new();
                    for node in dag.nodes() {
                        let pred_count = dag.predecessor_count(&node.task_id);
                        pending.insert(node.task_id.clone(), pred_count);
                    }

                    let exec_key = exec_key(workflow_id);
                    let execution = if let Ok(Some(exec_data)) = store.get(&exec_key).await {
                        match rkyv::from_bytes::<WorkflowExecution, rkyv::rancor::Error>(&exec_data)
                        {
                            Ok(mut execution) => {
                                let task_ids: Vec<TaskId> =
                                    execution.tasks.keys().cloned().collect();
                                for tid in &task_ids {
                                    execution.reset_task(tid, false, true);
                                }
                                execution
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "corrupt exec entry for adopted workflow {}: {:?}",
                                    workflow_id.as_str(),
                                    e
                                );
                                WorkflowExecution::new(workflow_id.clone(), task_ids)
                            }
                        }
                    } else {
                        WorkflowExecution::new(workflow_id.clone(), task_ids)
                    };

                    self.state
                        .insert_workflow(workflow_id.clone(), dag, execution, pending);
                    return Ok(());
                }
            }
        }

        // 无本地数据可用 — 插入占位符以注册 workflow ID。
        // gossip 层会向远程节点请求完整状态，并在数据到达后
        // 调用 restore_workflow 覆盖此占位符（状态从 Loading → Ready）。
        self.state.insert_placeholder(workflow_id.clone());
        Ok(())
    }

    /// 从远程同步得到的字节恢复工作流状态。
    pub async fn restore_workflow_from_bytes(
        &self,
        workflow_id: &WorkflowId,
        dag_bytes: Option<Vec<u8>>,
        exec_bytes: Option<Vec<u8>>,
        pending_bytes: Option<Vec<u8>>,
    ) {
        use crate::common::serialization::deserialize_rkyv_value;

        let dag: Option<Dag> = dag_bytes.and_then(|b| deserialize_rkyv_value(&b).ok());
        let execution: Option<WorkflowExecution> =
            exec_bytes.and_then(|b| deserialize_rkyv_value(&b).ok());
        let pending: Option<HashMap<TaskId, usize>> =
            pending_bytes.and_then(|b| deserialize_rkyv_value(&b).ok());

        match (dag, execution) {
            (Some(dag), Some(mut exec)) => {
                let task_ids: Vec<TaskId> = exec.tasks.keys().cloned().collect();
                for tid in &task_ids {
                    exec.reset_task(tid, false, true);
                }
                let pending = pending.unwrap_or_else(|| {
                    let mut p = HashMap::new();
                    for node in dag.nodes() {
                        p.insert(node.task_id.clone(), dag.predecessor_count(&node.task_id));
                    }
                    p
                });
                self.restore_workflow(workflow_id, dag, exec, pending).await;
            }
            (Some(dag), None) => {
                let exec = WorkflowExecution::new(
                    workflow_id.clone(),
                    dag.nodes().map(|n| n.task_id.clone()).collect(),
                )
                .with_failure_strategy(dag.failure_strategy);
                let mut pending = HashMap::new();
                for node in dag.nodes() {
                    pending.insert(node.task_id.clone(), dag.predecessor_count(&node.task_id));
                }
                self.restore_workflow(workflow_id, dag, exec, pending).await;
            }
            _ => {
                tracing::warn!(
                    "incomplete state bytes for workflow {}, cannot restore",
                    workflow_id.as_str()
                );
            }
        }
    }

    /// Restore a workflow from remote state (DAG + execution + pending).
    /// Called by the gossip layer when a WorkflowStateResponse is received.
    /// Overwrites any existing placeholder entry for this workflow.
    pub async fn restore_workflow(
        &self,
        workflow_id: &WorkflowId,
        dag: Dag,
        execution: WorkflowExecution,
        pending: HashMap<TaskId, usize>,
    ) {
        // 若可用，批量持久化到本地 store（单次事务，减少 fsync）
        if let Some(ref store) = self.store {
            let mut batch = Vec::new();
            if let Ok(dag_bytes) = serialize_rkyv(&dag) {
                batch.push((dag_key(workflow_id), dag_bytes));
            }
            if let Ok(exec_bytes) = serialize_rkyv(&execution) {
                batch.push((exec_key(workflow_id), exec_bytes));
            }
            if let Ok(pending_bytes) = serialize_rkyv(&pending) {
                batch.push((pending_key(workflow_id), pending_bytes));
            }
            if !batch.is_empty() {
                if let Err(e) = store.put_batch(&batch).await {
                    tracing::error!(
                        "failed to persist workflow {} state: {}",
                        workflow_id.as_str(),
                        e
                    );
                }
            }
        }

        self.state
            .insert_workflow(workflow_id.clone(), dag, execution, pending);
    }

    pub fn get_running_task_ids(&self, workflow_id: &WorkflowId) -> Vec<TaskId> {
        if let Some(slot) = self.state.slots.get(workflow_id) {
            slot.execution
                .tasks
                .iter()
                .filter(|(_, ts)| ts.state == Phase::Running)
                .map(|(id, _)| id.clone())
                .collect()
        } else {
            vec![]
        }
    }

    pub fn mark_task_pending(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<()> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        slot.execution.reset_task(task_id, false, true);
        Ok(())
    }

    pub fn build_task_for_id(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Option<TaskDefinition>> {
        let slot = match self.state.slots.get(workflow_id) {
            Some(s) => s,
            None => return Ok(None),
        };
        let node = match slot.dag.get_node(task_id) {
            Some(n) => n,
            None => return Ok(None),
        };

        let attempt = slot
            .execution
            .tasks
            .get(task_id)
            .map(|t| t.attempt())
            .unwrap_or(0);

        let payload = build_task_payload(
            &slot.dag,
            &slot.execution,
            task_id,
            &node.payload,
            &self.config.payload_signing_key,
        )?;
        Ok(Some(TaskDefinition {
            id: node.task_id.clone(),
            name: node.name.clone(),
            payload,
            workflow_id: Some(workflow_id.clone()),
            target_node: None,
            origin_node: self.node_id.clone(),
            retry_policy: slot.dag.effective_retry_policy(&node.task_id),
            priority: node.priority,
            timeout_ms: node.timeout_ms,
            attempt,
            enqueued_at_ms: 0,
            target_endpoint_addr: None,
            origin_endpoint_addr: None,
        }))
    }

    pub fn mark_task_running(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<()> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        slot.execution.mark_task_running(task_id);
        self.log_event(WorkflowEventPayload::TaskRunning {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
        });
        Ok(())
    }

    /// Mark a task as failed.
    ///
    /// The `mode` parameter controls the scope:
    /// - `FailureScope::TaskOnly`: Only mark the task as Failed. The workflow
    ///   remains non-terminal, allowing `prepare_retry` to reset the task.
    /// - `FailureScope::WorkflowLevel`: Mark the task as Failed AND apply workflow-level
    ///   failure semantics. If the workflow becomes terminal, metrics are
    ///   updated and the terminal notification is sent.
    pub async fn fail_task(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        error: String,
        mode: FailureScope,
    ) -> Result<()> {
        let error_for_event = error.clone();
        let is_terminal = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.fail_task(task_id, error, mode);
            slot.execution.is_terminal()
        };

        self.log_event(WorkflowEventPayload::TaskFailed {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
            error: error_for_event,
        });

        if is_terminal {
            // Terminal: persist immediately for crash safety
            let exec_snapshot = {
                let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                slot.execution.clone()
            };
            if let Some(ref store) = self.store {
                let exec_bytes = serialize_rkyv(&exec_snapshot)?;
                store
                    .put_batch(&[(exec_key(workflow_id), exec_bytes)])
                    .await?;
            }
            self.log_event(WorkflowEventPayload::Failed {
                workflow_id: workflow_id.clone(),
                error: format!("workflow failed at task {}", task_id.as_str()),
            });
            crate::metrics::inc_workflows_failed();
            crate::metrics::dec_active_workflows();
            self.notify_terminal();
        } else {
            // Non-terminal: defer to background flush
            self.state.mark_dirty(workflow_id);
        }
        Ok(())
    }

    pub(crate) async fn complete_task(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        result: Vec<u8>,
    ) -> Result<CompletionInfo> {
        let is_terminal = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            if slot.state == SlotState::Loading {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot complete task",
                    workflow_id.as_str()
                )));
            }
            slot.execution.mark_task_completed(task_id, result);
            slot.execution.is_terminal()
        };

        if is_terminal {
            let exec_snapshot = {
                let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                slot.execution.clone()
            };
            return self
                .complete_terminal(workflow_id, task_id, &exec_snapshot)
                .await;
        }

        let ready_ids = self.compute_ready_successors(workflow_id, task_id)?;

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        self.log_event(WorkflowEventPayload::TaskCompleted {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
        });

        Ok(CompletionInfo {
            workflow_terminal: false,
            ready_successors: ready_ids.ready,
            conditional_edges: ready_ids.conditional,
        })
    }

    async fn complete_terminal(
        &self,
        workflow_id: &WorkflowId,
        completed_task_id: &TaskId,
        exec_snapshot: &crate::runtime::workflow::dag::WorkflowExecution,
    ) -> Result<CompletionInfo> {
        if let Some(started_at_ms) = exec_snapshot.started_at_ms() {
            let now_ms = crate::common::epoch_millis();
            crate::metrics::observe_workflow_duration_ms(now_ms.saturating_sub(started_at_ms));
        }

        if self.config.workflow.completed_retention_count == 0 {
            self.evict_workflow(workflow_id).await;
        } else if let Some(ref store) = self.store {
            let exec_bytes = serialize_rkyv(exec_snapshot)?;
            let mut batch = vec![(exec_key(workflow_id), exec_bytes)];

            if matches!(exec_snapshot.state, Phase::Completed) {
                let results: Vec<Vec<u8>> = exec_snapshot.collected_results();
                if !results.is_empty() {
                    let result_bytes = crate::common::pack_group(&results);
                    batch.push((result_key(workflow_id), result_bytes));
                }
            }
            store.put_batch(&batch).await?;
        }

        if matches!(exec_snapshot.state, Phase::Completed) {
            self.log_event(WorkflowEventPayload::Completed {
                workflow_id: workflow_id.clone(),
            });
        } else {
            self.log_event(WorkflowEventPayload::Failed {
                workflow_id: workflow_id.clone(),
                error: format!("workflow failed at task {}", completed_task_id.as_str()),
            });
        }

        self.notify_terminal();
        Ok(CompletionInfo {
            workflow_terminal: true,
            ready_successors: vec![],
            conditional_edges: vec![],
        })
    }

    /// Notify waiters that a workflow has reached a terminal state.
    /// Fires the per-workflow oneshot channel for instant wake-up.
    fn notify_terminal(&self) {
        // 查找所有终态 workflow 并触发其 oneshot
        let terminal_ids: Vec<WorkflowId> = self
            .state
            .slots
            .iter()
            .filter(|entry| entry.value().execution.is_terminal())
            .map(|entry| entry.key().clone())
            .collect();
        for id in &terminal_ids {
            self.state.fire_terminal_oneshot(id);
        }
    }

    fn compute_ready_successors(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<ReadyResult> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;

        let successors: Vec<TaskId> = slot.dag.successor_ids(task_id);
        let conditional_edges: Vec<(TaskId, String)> = slot.dag.conditional_edges_from(task_id);

        let mut ready_ids: Vec<TaskId> = Vec::new();
        for succ_id in &successors {
            // 跳过条件后继 — 它们由单独逻辑处理
            if conditional_edges.iter().any(|(id, _)| id == succ_id) {
                continue;
            }
            let count = slot.pending.get_mut(succ_id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "pending count not found for task {}",
                    succ_id.as_str()
                ))
            })?;
            if *count == 0 {
                continue;
            }
            *count -= 1;
            if *count == 0 {
                ready_ids.push(succ_id.clone());
            }
        }
        Ok(ReadyResult {
            ready: ready_ids,
            conditional: conditional_edges,
        })
    }

    pub async fn mark_workflow_failed(
        &self,
        workflow_id: &WorkflowId,
        error: String,
    ) -> Result<()> {
        let store_writes = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.mark_workflow_failed(error);
            if self.store.is_some() {
                Some(slot.execution.clone())
            } else {
                None
            }
        };

        if let Some(exec_snapshot) = store_writes {
            if let Some(ref store) = self.store {
                let exec_bytes = serialize_rkyv(&exec_snapshot)?;
                store
                    .put_batch(&[(exec_key(workflow_id), exec_bytes)])
                    .await?;
            }
        }

        self.notify_terminal();
        Ok(())
    }

    pub fn build_ready_tasks_for(
        &self,
        workflow_id: &WorkflowId,
        task_ids: &[TaskId],
    ) -> Result<Vec<TaskDefinition>> {
        let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        self.build_ready_tasks_from_slot(&slot, workflow_id, task_ids)
    }

    /// After recovery, returns all tasks with pending_count == 0 (ready to run)
    /// for every non-terminal workflow. The caller should enqueue these into
    /// the scheduler so they can be dispatched.
    pub fn recover_ready_tasks(&self) -> Vec<TaskDefinition> {
        let mut all_ready = Vec::new();
        for entry in self.state.slots.iter() {
            let workflow_id = entry.key();
            let slot = entry.value();

            if slot.execution.is_terminal() {
                continue;
            }

            let ready_ids: Vec<TaskId> = slot
                .pending
                .iter()
                .filter(|(_, &count)| count == 0)
                .map(|(tid, _)| tid.clone())
                .collect();

            if ready_ids.is_empty() {
                continue;
            }

            if let Ok(tasks) = self.build_ready_tasks_from_slot(slot, workflow_id, &ready_ids) {
                tracing::info!(
                    "recovered workflow {} with {} ready tasks",
                    workflow_id.as_str(),
                    tasks.len()
                );
                all_ready.extend(tasks);
            }
        }
        all_ready
    }

    pub async fn get_result(&self, workflow_id: &WorkflowId) -> Option<Vec<u8>> {
        if let Some(ref store) = self.store {
            let key = result_key(workflow_id);
            store.get(&key).await.ok().flatten()
        } else {
            // 内存路径：与 store 路径一致，将所有已完成任务的结果打包为 group。
            // 之前用 HashMap::values().last() 取单个结果，顺序未定义且与 store
            // 路径不一致；现在统一使用 collected_results() + pack_group。
            let slot = self.state.slots.get(workflow_id)?;
            let results = slot.execution.collected_results();
            if results.is_empty() {
                None
            } else {
                Some(crate::common::pack_group(&results))
            }
        }
    }

    /// Returns unpacked task results for a completed workflow.
    /// Handles both single-result and group-encoded payloads.
    pub async fn get_results(&self, workflow_id: &WorkflowId) -> Option<Vec<Vec<u8>>> {
        let raw = self.get_result(workflow_id).await?;
        match unpack_payload(&raw) {
            Ok(items) => Some(items),
            Err(_) => Some(vec![raw]),
        }
    }

    pub fn get_retry_info(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Option<(u32, crate::common::RetryPolicy, u64)> {
        let slot = self.state.slots.get(workflow_id)?;
        let policy = slot.dag.effective_retry_policy(task_id)?;
        let task_state = slot.execution.tasks.get(task_id)?;
        let retry_count = task_state.retry_count();
        let delay_ms = Self::compute_retry_delay(retry_count, &policy);
        Some((retry_count, policy.clone(), delay_ms))
    }

    fn compute_retry_delay(retry_count: u32, policy: &crate::common::RetryPolicy) -> u64 {
        let base = policy.delay_ms as f64;
        let multiplier = policy.backoff_multiplier.powi(retry_count as i32);
        (base * multiplier).min(policy.max_delay_ms as f64) as u64
    }

    pub fn prepare_task_retry(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Option<TaskDefinition>> {
        {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;

            let pred_count = slot.dag.predecessor_count(task_id);

            // 普通 task 重试使用 reset_task 并 increment_retry=true。
            // 这同时处理 Failed → Pending 的状态转换。
            slot.execution.reset_task(task_id, true, false);

            // 验证 task 是否确实被重置（可能由于状态不匹配，
            // 例如处于 Completed 等不可重置状态）。
            let task_state = slot.execution.tasks.get(task_id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "task {} state not found",
                    task_id.as_str()
                ))
            })?;
            if task_state.state != Phase::Pending {
                return Ok(None);
            }

            slot.pending.insert(task_id.clone(), pred_count);
        }

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        self.build_task_for_id(workflow_id, task_id)
    }

    pub fn get_expired_workflow_ids(&self) -> Vec<WorkflowId> {
        self.state.expired_workflow_ids()
    }

    pub async fn evict_workflow(&self, old_id: &WorkflowId) {
        self.state.remove_workflow(old_id);
        if let Some(ref s) = self.store {
            for key in [
                dag_key(old_id),
                exec_key(old_id),
                pending_key(old_id),
                result_key(old_id),
            ] {
                if let Err(e) = s.delete(&key).await {
                    tracing::warn!(
                        "failed to delete key during eviction of workflow {}: {}",
                        old_id.as_str(),
                        e
                    );
                }
            }
        }
    }

    /// 将工作流所有 Running 状态的任务重置为 Pending，返回需要重新入队的任务定义。
    ///
    /// 用于故障转移：当原 orchestrator 节点失效、另一节点接管其工作流时调用。
    ///
    /// 本方法负责工作流状态变迁（标记 Pending、构造任务定义）；调用方负责
    /// 广播状态更新并将返回的任务入队到调度器。
    pub fn reschedule_running_tasks(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<TaskDefinition>> {
        let running_task_ids = self.get_running_task_ids(workflow_id);
        let mut tasks_to_reschedule = Vec::with_capacity(running_task_ids.len());

        for task_id in &running_task_ids {
            self.mark_task_pending(workflow_id, task_id)?;

            if let Some(task_def) = self.build_task_for_id(workflow_id, task_id)? {
                tasks_to_reschedule.push(task_def);
            }
        }

        Ok(tasks_to_reschedule)
    }
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}

fn dag_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_DAG, wf_id.as_str())
}

fn exec_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_EXEC, wf_id.as_str())
}

fn pending_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_PENDING, wf_id.as_str())
}

fn result_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_RESULT, wf_id.as_str())
}

fn build_task_payload(
    dag: &Dag,
    execution: &WorkflowExecution,
    task_id: &TaskId,
    default_payload: &[u8],
    signing_key: &[u8],
) -> Result<Vec<u8>> {
    let predecessors = dag.predecessors_of(task_id);
    if predecessors.is_empty() {
        return Ok(default_payload.to_vec());
    }
    // 先验证并解包原始任务 payload，再重新包装上游结果并签名。
    // 这保证了带依赖的任务 payload 仍具有端到端 MAC 保护。
    let raw_payload =
        crate::common::payload::verify(signing_key, default_payload).map_err(|e| {
            crate::common::ActantError::Internal(format!("payload verification: {}", e))
        })?;
    // 收集前驱任务结果（按 DAG 边顺序），统一前置到 default_payload。
    // Rust 核心不感知 default_payload 的 tag 类型 — 参数合并逻辑由 Python dispatcher 处理。
    let upstream_results: Vec<Vec<u8>> = predecessors
        .iter()
        .filter_map(|pred| {
            execution
                .tasks
                .get(&pred.task_id)
                .and_then(|t| t.result.clone())
        })
        .collect();
    let inner = crate::common::payload::pack_upstream_prefix(&upstream_results, &raw_payload);
    crate::common::payload::sign(signing_key, &inner)
        .map_err(|e| crate::common::ActantError::Internal(format!("payload sign: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::should_claim_workflow;
    use crate::common::RetryPolicy;
    use std::collections::HashMap;

    const TEST_SIGNING_KEY: &[u8] = b"test-key";

    fn make_node(id: &str, name: &str) -> DagNode {
        DagNode {
            task_id: TaskId::from(id.to_string()),
            name: name.to_string(),
            payload: crate::common::payload::sign(TEST_SIGNING_KEY, b"").unwrap(),
            retry_policy: None,
            timeout_ms: None,
            priority: 0,
            metadata: HashMap::new(),
        }
    }

    fn make_linear_dag() -> Dag {
        // t1 → t2 → t3
        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "first")).unwrap();
        dag.add_node(make_node("t2", "second")).unwrap();
        dag.add_node(make_node("t3", "third")).unwrap();
        dag.add_edge(TaskId::from("t1"), TaskId::from("t2"))
            .unwrap();
        dag.add_edge(TaskId::from("t2"), TaskId::from("t3"))
            .unwrap();
        dag
    }

    fn make_diamond_dag() -> Dag {
        //     t1
        //    /  \
        //   t2   t3
        //    \  /
        //     t4
        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "root")).unwrap();
        dag.add_node(make_node("t2", "left")).unwrap();
        dag.add_node(make_node("t3", "right")).unwrap();
        dag.add_node(make_node("t4", "join")).unwrap();
        dag.add_edge(TaskId::from("t1"), TaskId::from("t2"))
            .unwrap();
        dag.add_edge(TaskId::from("t1"), TaskId::from("t3"))
            .unwrap();
        dag.add_edge(TaskId::from("t2"), TaskId::from("t4"))
            .unwrap();
        dag.add_edge(TaskId::from("t3"), TaskId::from("t4"))
            .unwrap();
        dag
    }
    #[tokio::test]
    async fn submit_registers_workflow_in_state() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        let dag = make_linear_dag();

        orch.submit(wf.clone(), dag).await.unwrap();

        assert!(orch.has_workflow(&wf));
        let ids = orch.active_workflow_ids();
        assert!(ids.contains(&wf));
    }

    /// `adopt_workflow` 在无本地 store 数据时插入占位符（`SlotState::Loading`）。
    /// 占位符应：`has_workflow` 返回 true，但 `start` / `on_task_completed` 返回
    /// `InvalidState` 错误，防止在数据到达前误操作。
    #[tokio::test]
    async fn adopt_workflow_inserts_placeholder_when_no_local_data() {
        // Orchestrator::new() 无 store，adopt_workflow 必走占位符路径。
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-adopted");

        orch.adopt_workflow(&wf).await.unwrap();

        // 占位符已注册 workflow ID
        assert!(orch.has_workflow(&wf));
        assert!(!orch.state.is_ready(&wf));

        // start 应拒绝占位符
        let err = orch.start(&wf).unwrap_err();
        assert!(
            matches!(err, crate::common::ActantError::InvalidState(_)),
            "start on placeholder should return InvalidState, got {:?}",
            err
        );

        // on_task_completed 也应拒绝占位符
        let err = orch
            .on_task_completed(&wf, &TaskId::from("t1"), b"r".to_vec())
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::common::ActantError::InvalidState(_)),
            "on_task_completed on placeholder should return InvalidState, got {:?}",
            err
        );
    }

    /// `submit` 对已就绪的工作流返回 `AlreadyExists`，但对占位符允许覆盖。
    #[tokio::test]
    async fn submit_rejects_ready_workflow_but_allows_placeholder_override() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-submit");

        // 先 submit 一次，进入 Ready
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        assert!(orch.state.is_ready(&wf));

        // 再次 submit 应返回 AlreadyExists
        let err = orch
            .submit(wf.clone(), make_linear_dag())
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::common::ActantError::AlreadyExists(_)),
            "second submit on Ready workflow should return AlreadyExists, got {:?}",
            err
        );

        // 另一工作流先 adopt 为占位符，再 submit 应成功（覆盖占位符）
        let wf2 = WorkflowId::from("wf-override");
        orch.adopt_workflow(&wf2).await.unwrap();
        assert!(!orch.state.is_ready(&wf2));
        orch.submit(wf2.clone(), make_linear_dag()).await.unwrap();
        assert!(orch.state.is_ready(&wf2));
    }

    #[tokio::test]
    async fn start_returns_root_tasks_and_marks_running() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();

        let roots = orch.start(&wf).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, TaskId::from("t1"));

        let state = orch.get_state(&wf).unwrap();
        assert_eq!(state.state, Phase::Running);
    }

    #[tokio::test]
    async fn start_returns_error_for_unknown_workflow() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let err = orch.start(&WorkflowId::from("nonexistent")).unwrap_err();
        assert!(matches!(err, crate::common::ActantError::NotFound(_)));
    }

    #[tokio::test]
    async fn completing_task_returns_ready_successors() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).unwrap();

        let (ready, _, terminal) = orch
            .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, TaskId::from("t2"));
        assert!(
            !terminal,
            "workflow should not be terminal after completing only the root task"
        );
    }

    #[tokio::test]
    async fn condition_evaluator_automatically_activates_branch() {
        struct AlwaysTrue;
        #[async_trait]
        impl ConditionEvaluator for AlwaysTrue {
            async fn evaluate(
                &self,
                _workflow_id: &WorkflowId,
                _task_id: &TaskId,
                _condition: &str,
            ) -> Result<bool> {
                Ok(true)
            }
        }

        let orch = Orchestrator::new()
            .with_signing_key(TEST_SIGNING_KEY.to_vec())
            .with_condition_evaluator(Arc::new(AlwaysTrue));
        let wf = WorkflowId::from("wf-1");

        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "root")).unwrap();
        dag.add_node(make_node("t2", "left")).unwrap();
        dag.add_conditional_edge(
            TaskId::from("t1"),
            TaskId::from("t2"),
            "go_left".to_string(),
        )
        .unwrap();

        orch.submit(wf.clone(), dag).await.unwrap();
        let roots = orch.start(&wf).unwrap();
        assert_eq!(roots.len(), 1);

        let (ready, conditional, _) = orch
            .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        assert!(
            conditional.is_empty(),
            "conditional edges should be handled internally"
        );
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, TaskId::from("t2"));
    }

    #[tokio::test]
    async fn completing_last_task_signals_workflow_terminal() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).unwrap();

        orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
            .await
            .unwrap();
        let (ready, _, terminal) = orch
            .on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
            .await
            .unwrap();

        // Terminal: explicit flag set, no more ready tasks.
        // 必须使用显式 terminal 标志而非 `ready.is_empty()`——条件求值器全部跳过
        // 后继时 ready 也会为空，但工作流未必终态。
        assert!(terminal, "explicit workflow_terminal flag must be set");
        assert!(ready.is_empty());

        let state = orch.get_state(&wf).unwrap();
        assert!(state.is_terminal());
        assert_eq!(state.state, Phase::Completed);
    }

    #[tokio::test]
    async fn completing_diamond_join_waits_for_both_predecessors() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_diamond_dag()).await.unwrap();
        orch.start(&wf).unwrap();

        // Complete root t1 → t2 and t3 become ready
        let (ready, _, _) = orch
            .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        assert_eq!(ready.len(), 2);

        // Complete t2 → t4 NOT ready (still waiting on t3)
        let (ready, _, _) = orch
            .on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
            .await
            .unwrap();
        assert!(ready.is_empty());

        // Complete t3 → t4 NOW ready
        let (ready, _, _) = orch
            .on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
            .await
            .unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, TaskId::from("t4"));
    }

    #[tokio::test]
    async fn skip_conditional_branch_skips_task_without_failing_workflow() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");

        // t1 → t2 → t3 where t2 is skipped
        let dag = make_linear_dag();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).unwrap();

        orch.skip_conditional_branch(&wf, &TaskId::from("t2"))
            .await
            .unwrap();

        let state = orch.get_state(&wf).unwrap();
        let t2_state = state.tasks.get(&TaskId::from("t2")).unwrap();
        assert_eq!(t2_state.state, Phase::Skipped);
    }

    /// 回归测试：节点同时是某前驱的普通后继和另一前驱的条件后继时，
    /// `skip_conditional_branch` 不得将其错误跳过。
    ///
    /// 场景：
    /// ```text
    ///     t1 ──普通──→ t3
    ///     t2 ──条件──→ t3
    /// ```
    /// t3 的 pending = 2（t1 普通 + t2 条件）。当 t2 的条件分支不激活时，
    /// `skip_conditional_branch(t3)` 应仅减少一次 pending（2→1），不跳过 t3，
    /// 因为 t3 仍等待 t1 的普通完成。t3 的状态应保持 Pending。
    #[tokio::test]
    async fn skip_conditional_branch_does_not_skip_node_also_regular_successor() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-mixed");

        // t1 ──普通──→ t3
        // t2 ──条件──→ t3
        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "regular_pred")).unwrap();
        dag.add_node(make_node("t2", "cond_pred")).unwrap();
        dag.add_node(make_node("t3", "join")).unwrap();
        dag.add_edge(TaskId::from("t1"), TaskId::from("t3"))
            .unwrap();
        dag.add_conditional_edge(TaskId::from("t2"), TaskId::from("t3"), "maybe".to_string())
            .unwrap();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).unwrap();

        // 模拟 t2 的条件分支不激活：调用 skip_conditional_branch(t3)。
        // t3 的 pending 应从 2 减为 1，状态保持 Pending（不跳过）。
        let ready = orch
            .skip_conditional_branch(&wf, &TaskId::from("t3"))
            .await
            .unwrap();

        // 不应返回任何 ready 任务（t3 的 pending 仍为 1）
        assert!(
            ready.is_empty(),
            "t3 should not be ready: it still waits for t1"
        );

        let state = orch.get_state(&wf).unwrap();
        let t3_state = state.tasks.get(&TaskId::from("t3")).unwrap();
        assert_ne!(
            t3_state.state,
            Phase::Skipped,
            "t3 must not be skipped: it is still a regular successor of t1"
        );
        assert_eq!(
            t3_state.state,
            Phase::Pending,
            "t3 should remain Pending while waiting for t1"
        );
    }

    #[tokio::test]
    async fn cancel_marks_workflow_cancelled() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).unwrap();

        orch.cancel(&wf).await.unwrap();

        let state = orch.get_state(&wf).unwrap();
        assert!(state.is_terminal());
        assert_eq!(state.state, Phase::Cancelled);
    }

    #[tokio::test]
    async fn cancel_unknown_workflow_returns_not_found() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let err = orch.cancel(&WorkflowId::from("nope")).await.unwrap_err();
        assert!(matches!(err, crate::common::ActantError::NotFound(_)));
    }

    #[tokio::test]
    async fn terminal_waiter_resolves_after_completion() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).unwrap();

        let rx = orch.state.register_terminal_waiter(wf.clone());

        // Complete all tasks
        orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
            .await
            .unwrap();

        // Waiter should resolve
        tokio::time::timeout(std::time::Duration::from_secs(1), rx)
            .await
            .expect("waiter did not resolve within 1s")
            .expect("waiter was dropped without signaling");
    }

    #[tokio::test]
    async fn terminal_waiter_resolves_immediately_if_already_terminal() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).unwrap();
        orch.cancel(&wf).await.unwrap();

        let rx = orch.state.register_terminal_waiter(wf.clone());
        // Should resolve immediately
        tokio::time::timeout(std::time::Duration::from_millis(100), rx)
            .await
            .expect("waiter did not resolve immediately")
            .expect("waiter was dropped without signaling");
    }

    #[test]
    fn builder_with_node_id_sets_node_id() {
        let orch = Orchestrator::new()
            .with_signing_key(TEST_SIGNING_KEY.to_vec())
            .with_node_id(NodeId::from("node-1"));
        assert_eq!(orch.node_id(), Some(&NodeId::from("node-1")));
    }

    #[test]
    fn new_orchestrator_has_no_store() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        assert!(orch.store().is_none());
    }

    #[tokio::test]
    async fn submit_with_timeout_sets_deadline() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit_with_timeout(wf.clone(), make_linear_dag(), 5000)
            .await
            .unwrap();

        let state = orch.get_state(&wf).unwrap();
        assert!(state.deadline_ms().is_some());
    }

    #[tokio::test]
    async fn get_dag_returns_submitted_dag() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_diamond_dag()).await.unwrap();

        let dag = orch.get_dag(&wf).unwrap();
        assert_eq!(dag.node_count(), 4);
    }

    #[tokio::test]
    async fn get_state_returns_none_for_unknown_workflow() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        assert!(orch.get_state(&WorkflowId::from("nonexistent")).is_none());
    }

    // get_result 返回 pack_group 打包的所有已完成任务结果（与 store 路径一致）。
    // get_results 解包 get_result 的返回值，得到 Vec<Vec<u8>>。
    // 结果按 task_id 升序排序，保证确定性（HashMap 迭代顺序未定义）。

    #[tokio::test]
    async fn get_result_returns_packed_group_after_completion() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");

        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "only")).unwrap();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).unwrap();

        orch.on_task_completed(&wf, &TaskId::from("t1"), b"final".to_vec())
            .await
            .unwrap();

        // get_result 返回 pack_group 编码的字节（与 store 路径一致）。
        let packed = orch.get_result(&wf).await.expect("should have result");
        assert_eq!(packed, crate::common::pack_group(&[b"final".to_vec()]));

        // get_results 解包得到原始任务结果列表。
        let results = orch.get_results(&wf).await.expect("should have results");
        assert_eq!(results, vec![b"final".to_vec()]);
    }

    #[tokio::test]
    async fn get_results_orders_by_task_id_deterministically() {
        // 多任务工作流：验证结果按 task_id 升序排序，而非 HashMap 随机顺序。
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-multi");

        let mut dag = Dag::new();
        // 故意用非字母序的提交顺序，且 task_id 字典序与提交序不同。
        dag.add_node(make_node("t3", "third")).unwrap();
        dag.add_node(make_node("t1", "first")).unwrap();
        dag.add_node(make_node("t2", "second")).unwrap();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).unwrap();

        // 按非字典序完成，排除"提交即排序"的巧合。
        orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();

        let results = orch.get_results(&wf).await.expect("should have results");
        // 期望按 task_id 升序：t1, t2, t3 → r1, r2, r3
        assert_eq!(
            results,
            vec![b"r1".to_vec(), b"r2".to_vec(), b"r3".to_vec()]
        );
    }

    #[tokio::test]
    async fn get_result_returns_none_when_no_completed_tasks() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-empty");

        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "pending")).unwrap();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).unwrap();

        // 任务未完成，无结果。
        assert_eq!(orch.get_result(&wf).await, None);
        assert_eq!(orch.get_results(&wf).await, None);
    }

    #[tokio::test]
    async fn start_propagates_retry_policy_from_dag_node() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");

        let mut dag = Dag::new();
        let mut node = make_node("t1", "retryable");
        node.retry_policy = Some(RetryPolicy {
            max_retries: 5,
            delay_ms: 1000,
            backoff_multiplier: 2.0,
            max_delay_ms: 60000,
        });
        dag.add_node(node).unwrap();

        orch.submit(wf.clone(), dag).await.unwrap();
        let roots = orch.start(&wf).unwrap();

        assert_eq!(roots.len(), 1);
        let policy = roots[0].retry_policy.as_ref().unwrap();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.delay_ms, 1000);
    }

    // ---- 审查验证: should_claim_workflow 一致性哈希 vs claim_workflow 字典序退让 ----

    /// 验证审查发现 ST1: should_claim_workflow (一致性哈希) 与 claim_workflow 内部
    /// 字典序退让逻辑存在设计不一致。当 lease 过期边界情况发生时，
    /// 一致性哈希指定的认领者可能因字典序更大而退让给非指定节点。
    ///
    /// 此测试验证 should_claim_workflow 的决策与 claim_workflow 内部
    /// 退让逻辑（existing.node_id < self.node_id 时退让）在特定场景下矛盾。
    #[test]
    fn failover_claim_strategy_aligned_with_consistent_hash() {
        // should_claim_workflow 使用一致性哈希决定认领权。
        // 修复后 claim_workflow 不再使用字典序退让，因此两种策略不会矛盾：
        // 当租约仍有效且不属于本节点时直接退让，仅当租约过期或归属本节点时才认领。
        let candidates = vec!["node_a".to_string(), "node_b".to_string()];

        // 暴力搜索一个 key 使一致性哈希指向 node_b 而非 node_a
        let mut conflict_key = String::new();
        for i in 0..10000 {
            let key = format!("wf-{}", i);
            if should_claim_workflow(&key, "node_b", candidates.clone())
                && !should_claim_workflow(&key, "node_a", candidates.clone())
            {
                conflict_key = key;
                break;
            }
        }
        assert!(
            !conflict_key.is_empty(),
            "should find a key mapping to node_b via consistent hash"
        );

        assert!(should_claim_workflow(
            &conflict_key,
            "node_b",
            candidates.clone()
        ));
        assert!(!should_claim_workflow(
            &conflict_key,
            "node_a",
            candidates.clone()
        ));
    }

    /// 高扇出 DAG 基准测试：1 root → N children。
    ///
    /// 测量端到端热点路径（submit → start → complete root → complete all children）
    /// 在不同扇出度下的耗时，作为 orchestrator 状态机扩展性能的回归基线。
    /// 不做绝对耗时断言（CI 环境噪声大），仅验证正确性与相对增长趋势。
    #[tokio::test]
    async fn high_fanout_dag_completes_correctly() {
        for &fanout in &[10usize, 100, 500] {
            let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
            let wf = WorkflowId::from(format!("wf-fanout-{fanout}"));

            let mut dag = Dag::new();
            dag.add_node(make_node("root", "root")).unwrap();
            for i in 0..fanout {
                let child_id = format!("c{i}");
                dag.add_node(make_node(&child_id, &child_id)).unwrap();
                dag.add_edge(TaskId::from("root"), TaskId::from(child_id))
                    .unwrap();
            }

            orch.submit(wf.clone(), dag).await.unwrap();
            let roots = orch.start(&wf).unwrap();
            assert_eq!(roots.len(), 1, "fanout={fanout}: single root");

            // 完成 root，应一次性释放全部 fanout 个子任务。
            let (ready, _, terminal) = orch
                .on_task_completed(&wf, &TaskId::from("root"), b"r".to_vec())
                .await
                .unwrap();
            assert!(
                !terminal,
                "fanout={fanout}: workflow not terminal after root"
            );
            assert_eq!(
                ready.len(),
                fanout,
                "fanout={fanout}: all children ready after root"
            );

            // 逐个完成子任务；最后一个应触发 workflow 终态。
            for i in 0..fanout {
                let child_id = TaskId::from(format!("c{i}"));
                let (_, _, terminal) = orch
                    .on_task_completed(&wf, &child_id, b"c".to_vec())
                    .await
                    .unwrap();
                let is_last = i == fanout - 1;
                assert_eq!(
                    terminal, is_last,
                    "fanout={fanout}: terminal flag mismatch at child {i}"
                );
            }

            let state = orch.get_state(&wf).unwrap();
            assert_eq!(
                state.state,
                Phase::Completed,
                "fanout={fanout}: workflow completed"
            );
        }
    }
}
