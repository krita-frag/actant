//! Orchestrator 辅助类型：事件 payload、workflow slot、并发状态。
//!
//! 这些类型是 `Orchestrator` 操作的数据结构，与 `Orchestrator` 的 impl 解耦，
//! 便于独立阅读与测试。

use std::collections::HashMap;

use dashmap::{DashMap, DashSet};
use serde::{Deserialize, Serialize};

use crate::common::{TaskId, WorkflowId};
use crate::runtime::workflow::{Dag, Phase, Terminal, WaitPoint, WorkflowExecution};

// 事件契约定义在 `actant-common::journal`；这里重导出，使 orchestrator 内部
// 通过 `super::types::*` 的 glob 导入继续使用同一符号。
pub use crate::common::JournalEvent;

#[derive(Debug, Clone)]
pub struct CompletionInfo {
    pub workflow_terminal: bool,
    pub ready_successors: Vec<TaskId>,
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

/// 确定性上下文值的已记录条目。
///
/// `kind` 与 `value` 成对保存：重放请求同 `key` 但不同 `kind` 时据此判定
/// 提交序列漂移（见 [`super::context`]）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextEntry {
    pub kind: String,
    pub value: Vec<u8>,
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

/// Per-(workflow, wait_key) 等待点唤醒句柄注册表。
///
/// 每个等待点一条 oneshot 通道，精确唤醒一次，`fire` 后条目移除。
/// 供 flow 线程在等待点 park，signal/timer 到期时被唤醒。
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
    /// **必须由调用方在注册后检查条件是否已满足**（"先注册后检查"
    /// 的次序用于关闭竞态窗口）。
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
/// - `wait_waiters`：等待点唤醒句柄注册表
/// - `waitpoints`：workflow → 等待点表的并发映射
/// - `pending_signals`：workflow → 等待点注册前抵达的信号缓冲
/// - `dirty_tracker`：脏 workflow 跟踪器
/// - `event_seqs`：workflow → 最近一次追加事件的 `EventId`（事件水位）
pub struct OrchestratorState {
    pub(crate) slots: DashMap<WorkflowId, WorkflowSlot>,
    pub(crate) wait_waiters: WaiterRegistry,
    pub(crate) waitpoints: DashMap<WorkflowId, DashMap<String, WaitPoint>>,
    /// 信号缓冲：`workflow → (wait_key → payload)`。等待点**注册前**抵达的信号
    /// 存于此，`register_wait_point` 命中时消费（等待点直接生成为 `Signaled`）。
    ///
    /// 与 `waitpoints` 同寿命：随 `remove_workflow` 清除，故悬挂信号**随其工作流
    /// 消亡**，不会跨工作流累积。
    pub(crate) pending_signals: DashMap<WorkflowId, DashMap<String, Vec<u8>>>,
    /// 确定性上下文值：`workflow → (ctx key → 条目)`。
    ///
    /// 与等待点同寿命：随 `remove_workflow` 清除。事实源是 `ContextValue`
    /// 事件，本表随快照落盘作为重放加速缓存。
    pub(crate) context_values: DashMap<WorkflowId, DashMap<String, ContextEntry>>,
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
            wait_waiters: WaiterRegistry::new(),
            waitpoints: DashMap::new(),
            pending_signals: DashMap::new(),
            context_values: DashMap::new(),
            dirty_tracker: DirtyTracker::new(),
            event_seqs: DashMap::new(),
        }
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
        self.waitpoints.remove(workflow_id);
        // 信号缓冲与等待点同寿命：工作流移除后，未被消费的信号不再有任何
        // 可能的消费者（注册方已随工作流消失），继续保留只会静默累积
        // （信号缓冲随工作流移除而清除，避免悬挂信号静默累积）。
        self.pending_signals.remove(workflow_id);
        self.context_values.remove(workflow_id);
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

    /// 克隆工作流的确定性上下文值，供快照序列化。
    ///
    /// 与 [`Self::clone_waitpoints`] 同批落盘：上下文值跨重启存活是"重放
    /// 返回同一组值"的前提。
    pub(crate) fn clone_context_values(
        &self,
        workflow_id: &WorkflowId,
    ) -> Option<std::collections::HashMap<String, ContextEntry>> {
        self.context_values.get(workflow_id).map(|table| {
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
