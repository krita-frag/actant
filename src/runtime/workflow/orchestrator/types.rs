//! Orchestrator 辅助类型：事件 payload、条件求值器、workflow slot、并发状态。
//!
//! 这些类型是 `Orchestrator` 操作的数据结构，与 `Orchestrator` 的 impl 解耦，
//! 便于独立阅读与测试。

use std::collections::HashMap;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use serde::{Deserialize, Serialize};

use crate::common::{Result, TaskId, WorkflowId};
use crate::runtime::workflow::{
    Dag, DagNode, Phase, Terminal, WaitCondition, WaitPoint, WorkflowExecution,
};

/// 工作流级别事件，写入 `EventLog` 的 `workflow:{workflow_id}` topic。
///
/// 记录每次状态迁移，是工作流历史的唯一事实源：recover = 执行快照
/// （重放加速缓存）+ 其后事件重放。除 [`WorkflowEventPayload::TaskCompleted`]
/// 携带结果字节（重放需要恢复结果）与 [`WorkflowEventPayload::NodeAdded`]
/// 携带节点定义（增量提交的节点可由历史重建）外，不携带完整
/// DAG/Execution，避免事件体积过大。所有变体由 orchestrator 在内部状态
/// 变迁时构造；Python 侧通过 EventLog 读取 API 观测这些事件（不透明字节），
/// 不依赖其 Rust 类型定义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowEventPayload {
    Submitted {
        workflow_id: WorkflowId,
    },
    /// 节点进入 DAG。当前唯一变更点是 `submit`（整图提交）；增量提交
    /// 后每个节点在写入点单独追加，携带完整 [`DagNode`] 使历史可重建 DAG。
    NodeAdded {
        workflow_id: WorkflowId,
        node: DagNode,
    },
    /// 任务由 Worker 接受本地执行（派发确认）。
    TaskDispatched {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    Started {
        workflow_id: WorkflowId,
    },
    TaskRunning {
        workflow_id: WorkflowId,
        task_id: TaskId,
    },
    /// 任务完成。携带结果字节：历史是结果的事实源（重放"已完成返回
    /// 记录结果"、回灌均读同一历史）。
    TaskCompleted {
        workflow_id: WorkflowId,
        task_id: TaskId,
        result: Vec<u8>,
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
    /// 工作流以取消收尾（全部节点终态且无失败、存在被取消节点）。
    ///
    /// 与节点级 [`Self::TaskCancelled`] 区分：后者是单个节点的取消记录，
    /// 本变体是工作流终态事件，由 `complete_terminal` 在终态收尾时追加。
    Cancelled {
        workflow_id: WorkflowId,
    },
    /// 等待点注册。幂等：同 wait_key 重复注册不追加此事件。
    WaitPointRegistered {
        workflow_id: WorkflowId,
        wait_key: String,
        condition: WaitCondition,
    },
    /// 等待点被**外部满足**而唤醒。
    ///
    /// 覆盖两种来源，由对应 `WaitPointRegistered` 的 condition 区分：
    /// `Signal` 条件 ← `signal_wait_point` 递交业务信号；`Suspend` 条件 ←
    /// `resume_suspended` 的操作员恢复指令。payload 预留给 Signals
    /// capability 携带信号数据；当前两条路径都递交空 payload。
    SignalReceived {
        workflow_id: WorkflowId,
        wait_key: String,
        payload: Vec<u8>,
    },
    /// 定时等待点到期唤醒。
    TimerFired {
        workflow_id: WorkflowId,
        wait_key: String,
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
            | Self::NodeAdded { workflow_id, .. }
            | Self::TaskDispatched { workflow_id, .. }
            | Self::Started { workflow_id }
            | Self::TaskRunning { workflow_id, .. }
            | Self::TaskCompleted { workflow_id, .. }
            | Self::TaskFailed { workflow_id, .. }
            | Self::TaskCancelled { workflow_id, .. }
            | Self::Completed { workflow_id }
            | Self::Failed { workflow_id, .. }
            | Self::Cancelled { workflow_id }
            | Self::WaitPointRegistered { workflow_id, .. }
            | Self::SignalReceived { workflow_id, .. }
            | Self::TimerFired { workflow_id, .. }
            | Self::Recovered { workflow_id, .. } => workflow_id,
        }
    }

    /// 事件种类名（观测面）。
    ///
    /// 只暴露**名字**而不是整个枚举布局：Python 侧据此筛选历史，但不解释
    /// payload 结构（跨语言 wire 编码，字段增减不致破坏跨语言契约）。
    ///
    /// **按 `python` 特性门控**：唯一消费者是绑定层（`src/py/runtime.rs`），
    /// 纯框架构建下不产生 dead_code 告警。
    #[cfg(feature = "python")]
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Submitted { .. } => "Submitted",
            Self::NodeAdded { .. } => "NodeAdded",
            Self::TaskDispatched { .. } => "TaskDispatched",
            Self::Started { .. } => "Started",
            Self::TaskRunning { .. } => "TaskRunning",
            Self::TaskCompleted { .. } => "TaskCompleted",
            Self::TaskFailed { .. } => "TaskFailed",
            Self::TaskCancelled { .. } => "TaskCancelled",
            Self::Completed { .. } => "Completed",
            Self::Failed { .. } => "Failed",
            Self::Cancelled { .. } => "Cancelled",
            Self::WaitPointRegistered { .. } => "WaitPointRegistered",
            Self::SignalReceived { .. } => "SignalReceived",
            Self::TimerFired { .. } => "TimerFired",
            Self::Recovered { .. } => "Recovered",
        }
    }

    /// 事件关联的任务（若有）。与 [`Self::kind_name`] 同为观测面字段。
    #[cfg(feature = "python")]
    pub fn task_id(&self) -> Option<&TaskId> {
        match self {
            Self::NodeAdded { node, .. } => Some(&node.task_id),
            Self::TaskDispatched { task_id, .. }
            | Self::TaskRunning { task_id, .. }
            | Self::TaskCompleted { task_id, .. }
            | Self::TaskFailed { task_id, .. }
            | Self::TaskCancelled { task_id, .. } => Some(task_id),
            _ => None,
        }
    }

    /// 事件携带的错误串（若有）。
    #[cfg(feature = "python")]
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::TaskFailed { error, .. } | Self::Failed { error, .. } => Some(error.as_str()),
            _ => None,
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

/// `Orchestrator::add_node` 的增量提交结果（flow 重放闭环）。
///
/// 作为 `WorkflowActor` `add_node` 消息的响应载荷跨 PyO3 边界回传：
/// Python 侧 flow 提交路径据此决定返回新建句柄还是从历史重建已完成句柄。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AddNodeOutcome {
    /// 新节点已持久化。`ready` 为依赖已满足、需调用方入队调度器的任务定义
    /// （`None` 表示仍有未终态依赖，由前驱完成事件触发派发）。
    Created {
        ready: Option<Box<crate::common::TaskDefinition>>,
    },
    /// 节点已存在且指纹一致（flow 重放命中历史）：不重新提交、不重跑。
    /// 携带任务当前状态、已完成结果字节与失败信息，供 Python 重建句柄。
    Existing {
        state: Phase,
        result: Option<Vec<u8>>,
        error: Option<String>,
    },
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

    // **当前没有注册侧**：唯一的注册入口 `register_terminal_waiter` 已随审查
    // （2026-09-18）删除——它零生产调用者，终态等待已被 Python 侧的
    // `_wait_terminal_and_emit` 轮询取代（`fire` 因此总是 no-op）。
    // 保留 `fire` 与其调用点是为了不牵动 `notify_terminal` 的既有结构；
    // 若确认不再需要 oneshot 唤醒，应连同本结构体与 `fire_terminal_oneshot`
    // 一起删除（守则 3）。

    /// Fire the oneshot for a workflow that has reached terminal state.
    /// Called from `notify_terminal()` and timeout watcher.
    /// 若无注册等待者，此操作为 no-op。
    fn fire(&self, workflow_id: &WorkflowId) {
        if let Some((_, tx)) = self.waiters.remove(workflow_id) {
            // send 失败仅当等待者已超时放弃（receiver drop），通知无人接收。
            let _ = tx.send(());
        }
    }

    /// 移除等待者但不触发（用于工作流被移除时的清理）。
    fn remove(&self, workflow_id: &WorkflowId) {
        self.waiters.remove(workflow_id);
    }
}

/// Per-(workflow, wait_key) 等待点唤醒句柄注册表。
///
/// 扩展 [`TerminalWaiterRegistry`] 模式：每个等待点一条 oneshot 通道，
/// 精确唤醒一次，`fire` 后条目移除。供后续 flow 线程在等待点 park，
/// signal/timer 到期时被唤醒。
pub(crate) struct WaiterRegistry {
    waiters: DashMap<(WorkflowId, String), tokio::sync::oneshot::Sender<Vec<u8>>>,
}

impl WaiterRegistry {
    fn new() -> Self {
        Self {
            waiters: DashMap::new(),
        }
    }

    /// 注册 oneshot 唤醒句柄，条件满足时收到 payload。
    ///
    /// **必须由调用方在注册后检查条件是否已满足**（与
    /// [`TerminalWaiterRegistry::register`] 相同的"先注册后检查"次序关闭
    /// 竞态窗口）。
    fn register(
        &self,
        workflow_id: WorkflowId,
        wait_key: &str,
    ) -> tokio::sync::oneshot::Receiver<Vec<u8>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.waiters.insert((workflow_id, wait_key.to_string()), tx);
        rx
    }

    /// 唤醒指定等待点的一个等待者并携带 payload；无等待者时 no-op。
    fn fire(&self, workflow_id: &WorkflowId, wait_key: &str, payload: Vec<u8>) {
        if let Some((_, tx)) = self
            .waiters
            .remove(&(workflow_id.clone(), wait_key.to_string()))
        {
            // send 失败仅当等待者已放弃（receiver drop），通知无人接收。
            let _ = tx.send(payload);
        }
    }

    /// 移除某工作流的全部等待点等待者（工作流被移除时的清理）。
    fn remove_workflow(&self, workflow_id: &WorkflowId) {
        self.waiters.retain(|(wf, _), _| wf != workflow_id);
    }

    /// 释放**全部**等待者（运行时关停时调用）。
    ///
    /// 等待点是"无限 park"语义的挂起原语：`wait_wait_point(timeout_ms=0)` 会一直
    /// 阻塞到条件满足。若关停时不释放，park 中的调用方（可能是主线程）永远等不到
    /// 唤醒，进程无法退出。丢弃 sender 使 receiver 立即收到 `Err`，park 方据此
    /// 返回"未唤醒"。
    fn clear(&self) {
        self.waiters.clear();
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
/// 由六个独立职责的子结构组合而成：
/// - `slots`：workflow → WorkflowSlot 的并发映射
/// - `terminal_waiters`：终态等待者 oneshot 注册表
/// - `wait_waiters`：等待点唤醒句柄注册表
/// - `waitpoints`：workflow → 等待点表的并发映射
/// - `pending_signals`：workflow → 等待点注册前抵达的信号缓冲
/// - `dirty_tracker`：脏 workflow 跟踪器
/// - `event_seqs`：workflow → 最近一次追加事件的 `EventId`（事件水位）
pub struct OrchestratorState {
    pub(crate) slots: DashMap<WorkflowId, WorkflowSlot>,
    pub(crate) terminal_waiters: TerminalWaiterRegistry,
    pub(crate) wait_waiters: WaiterRegistry,
    pub(crate) waitpoints: DashMap<WorkflowId, DashMap<String, WaitPoint>>,
    /// 信号缓冲：`workflow → (wait_key → payload)`。等待点**注册前**抵达的信号
    /// 存于此，`register_wait_point` 命中时消费（等待点直接生成为 `Signaled`）。
    ///
    /// 与 `waitpoints` 同寿命：随 `remove_workflow` 清除，故悬挂信号**随其工作流
    /// 消亡**，不会跨工作流累积。
    pub(crate) pending_signals: DashMap<WorkflowId, DashMap<String, Vec<u8>>>,
    pub(crate) dirty_tracker: DirtyTracker,
    pub(crate) event_seqs: DashMap<WorkflowId, crate::runtime::state::event_log::EventId>,
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
            wait_waiters: WaiterRegistry::new(),
            waitpoints: DashMap::new(),
            pending_signals: DashMap::new(),
            dirty_tracker: DirtyTracker::new(),
            event_seqs: DashMap::new(),
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
        self.waitpoints.remove(workflow_id);
        // 信号缓冲与等待点同寿命：工作流移除后，未被消费的信号不再有任何
        // 可能的消费者（注册方已随工作流消失），继续保留只会静默累积
        // （信号缓冲随工作流移除而清除，避免悬挂信号静默累积）。
        self.pending_signals.remove(workflow_id);
        self.event_seqs.remove(workflow_id);
        self.wait_waiters.remove_workflow(workflow_id);
    }

    /// 记录工作流最近一次成功追加的事件 ID（事件水位）。
    ///
    /// flush 在序列化快照**之前**读取水位写入 store：保证任何已计入水位
    /// 的状态变更必然已包含在快照中；反向窗口（变更在水位居捕获后、序列化
    /// 前发生）由重放幂等守卫兜底——该事件重放时被终态守卫拒绝。
    pub(crate) fn record_event_seq(
        &self,
        workflow_id: &WorkflowId,
        id: crate::runtime::state::event_log::EventId,
    ) {
        self.event_seqs.insert(workflow_id.clone(), id);
    }

    /// 返回工作流当前事件水位（flush 时随快照落盘）。
    pub(crate) fn event_seq(
        &self,
        workflow_id: &WorkflowId,
    ) -> Option<crate::runtime::state::event_log::EventId> {
        self.event_seqs.get(workflow_id).map(|e| *e)
    }

    /// 返回工作流等待点表的克隆（flush 时序列化；无等待点返回 `None`）。
    pub(crate) fn clone_waitpoints(
        &self,
        workflow_id: &WorkflowId,
    ) -> Option<std::collections::HashMap<String, WaitPoint>> {
        self.waitpoints.get(workflow_id).map(|table| {
            table
                .iter()
                .map(|wp| (wp.key().clone(), wp.value().clone()))
                .collect()
        })
    }

    /// 克隆工作流的信号缓冲，供快照序列化。
    ///
    /// 与 [`Self::clone_waitpoints`] 成对：二者同批落盘，否则等待点跨重启存活
    /// 而其信号不存活。
    pub(crate) fn clone_pending_signals(
        &self,
        workflow_id: &WorkflowId,
    ) -> Option<std::collections::HashMap<String, Vec<u8>>> {
        self.pending_signals.get(workflow_id).map(|table| {
            table
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect()
        })
    }

    /// 注册等待点唤醒句柄（先注册，调用方随后检查是否已 Signaled）。
    pub(crate) fn register_wait_waiter(
        &self,
        workflow_id: WorkflowId,
        wait_key: &str,
    ) -> tokio::sync::oneshot::Receiver<Vec<u8>> {
        self.wait_waiters.register(workflow_id, wait_key)
    }

    /// 唤醒等待点的等待者（oneshot，携带 payload）；无等待者时 no-op。
    pub(crate) fn fire_wait_waiter(
        &self,
        workflow_id: &WorkflowId,
        wait_key: &str,
        payload: Vec<u8>,
    ) {
        self.wait_waiters.fire(workflow_id, wait_key, payload);
    }

    /// 释放全部等待点等待者（运行时关停时调用）。
    pub(crate) fn clear_wait_waiters(&self) {
        self.wait_waiters.clear();
    }

    /// 释放某工作流的全部等待点等待者（该工作流进入终态时调用）。
    ///
    /// 与 [`Self::clear_wait_waiters`] 的区别是作用域：只影响单个工作流。
    /// 丢弃 sender 使 receiver 立即收到 `Err` → park 方返回"未唤醒"，
    /// 由调用方查工作流终态判定真实原因（cancel / failed / deadline）。
    ///
    /// **不触碰 `waitpoints` 表**：等待点条目是历史与快照的一部分，仍需保留
    /// 以供重放与查询；此处只解阻塞 park 中的调用方。
    pub(crate) fn release_wait_waiters(&self, workflow_id: &WorkflowId) {
        self.wait_waiters.remove_workflow(workflow_id);
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
