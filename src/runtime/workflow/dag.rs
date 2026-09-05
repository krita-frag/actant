//! DAG 与工作流执行状态模型。
//!
//! 本模块保存 workflow 的纯数据结构，不持有网络、ActorSystem 或持久化句柄。
//! [`Dag`] 负责节点/边校验与拓扑查询；[`WorkflowExecution`] 负责每个 task 的
//! 运行状态、结果、失败策略和超时信息；[`Phase`] 是 Rust/Python 边界共享的
//! 稳定状态枚举。
//!
//! ## 状态推进
//!
//! - `Pending` 表示任务尚未满足依赖或尚未被调度。
//! - `Running` 表示任务已经被 Worker 接收。
//! - `Completed`、`Failed`、`Cancelled`、`Skipped` 是终态。
//! - 条件分支未被激活时使用 `Skipped`，避免把未执行分支误判为失败。
//!
//! ## 设计边界
//!
//! DAG 只表达结构和状态，不触发调度、不写 store、不发网络消息。编排副作用由
//! `Orchestrator` 和 Worker 层处理。
//!
use std::collections::{HashMap, HashSet, VecDeque};

use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::common::{Result, RetryPolicy, TaskId, WorkflowId};

/// [`Phase`] 变体的稳定字符串标识符。
///
/// Rust↔Python 状态映射的唯一真实来源。[`Phase::as_str`] 和 Python 端的
/// 解析都引用这些常量，确保两个方向永远不会出现不一致。
pub mod phase_str {
    pub const PENDING: &str = "Pending";
    pub const RUNNING: &str = "Running";
    pub const COMPLETED: &str = "Completed";
    pub const FAILED: &str = "Failed";
    pub const CANCELLED: &str = "Cancelled";
    pub const SKIPPED: &str = "Skipped";
}

/// 工作流和任务的统一生命周期阶段。
///
/// 单一的 `Phase` 类型为工作流与任务提供共享的生命周期阶段词汇表，
/// 包含它的结构体（`WorkflowExecution` 或 `TaskState`）提供语义上下文。
///
/// 变体名称是稳定的；[`Phase::as_str`] 返回在 PyO3 边界使用的规范字符串
/// 表示，因此重命名变体不会破坏与 Python 的约定。
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Default,
)]
#[rkyv(bytecheck())]
#[non_exhaustive]
pub enum Phase {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    /// 工作流或任务未执行是因为其条件分支未被取。
    Skipped,
}

impl Phase {
    /// PyO3 边界使用的稳定字符串表示。
    ///
    /// 返回 [`phase_str`] 常量中的一个 —— 从不返回字面值字符串。
    pub fn as_str(&self) -> &'static str {
        match self {
            Phase::Pending => phase_str::PENDING,
            Phase::Running => phase_str::RUNNING,
            Phase::Completed => phase_str::COMPLETED,
            Phase::Failed => phase_str::FAILED,
            Phase::Cancelled => phase_str::CANCELLED,
            Phase::Skipped => phase_str::SKIPPED,
        }
    }

    /// 从其稳定字符串表示解析 [`Phase`] 变体。
    ///
    /// 对比 [`phase_str`] 常量进行大小写不敏感的比较。
    /// 返回未知字符串的 `None` 。
    pub fn parse(s: &str) -> Option<Self> {
        let lower = s.to_ascii_lowercase();
        let ok = |c: &'static str| lower == c.to_ascii_lowercase();
        if ok(phase_str::PENDING) {
            Some(Phase::Pending)
        } else if ok(phase_str::RUNNING) {
            Some(Phase::Running)
        } else if ok(phase_str::COMPLETED) {
            Some(Phase::Completed)
        } else if ok(phase_str::FAILED) {
            Some(Phase::Failed)
        } else if ok(phase_str::CANCELLED) {
            Some(Phase::Cancelled)
        } else if ok(phase_str::SKIPPED) {
            Some(Phase::Skipped)
        } else {
            None
        }
    }
}

/// 终端状态类型 trait。
///
/// 中心化 [`Phase`]、 [`TaskState`] 和 [`WorkflowExecution`] 定义的"终端"状态。
pub trait Terminal {
    /// 如果没有进一步的状态转换，则返回 `true` 。
    fn is_terminal(&self) -> bool;
}

impl Terminal for Phase {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Phase::Completed | Phase::Failed | Phase::Cancelled | Phase::Skipped
        )
    }
}

/// 工作流在任务失败时的反应策略。
///
/// 使用枚举使得策略可穷尽匹配，防止拼写错误的字符串比较。
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Default,
)]
#[rkyv(bytecheck())]
#[non_exhaustive]
pub enum FailureStrategy {
    /// 任何任务失败都立即标记工作流为失败。
    #[default]
    FailFast,
    /// 任务失败后，工作流继续执行，直到所有任务都达到终端状态且至少有一个任务失败。
    Continue,
}

impl FailureStrategy {
    /// PyO3 边界使用的稳定字符串表示。
    pub fn as_str(&self) -> &'static str {
        match self {
            FailureStrategy::FailFast => "fail_fast",
            FailureStrategy::Continue => "continue",
        }
    }

    /// 从其稳定字符串表示解析 [`FailureStrategy`] 变体。
    ///
    /// 返回未知字符串的 `None` 。
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "fail_fast" | "failfast" => Some(FailureStrategy::FailFast),
            "continue" | "continue_on_failure" | "best_effort" => Some(FailureStrategy::Continue),
            _ => None,
        }
    }
}

/// 任务失败的范围 —— 决定是仅标记任务失败，还是同时标记工作流失败。
///
/// - [`FailureScope::TaskOnly`]: 仅将任务标记为 Failed。工作流保持非终态，
///   允许 `prepare_retry` 重置该任务以进行重试。
/// - [`FailureScope::WorkflowLevel`]: 将任务标记为 Failed 并应用工作流级别的
///   失败语义（FailFast → 立即将工作流标记为失败；否则 → 检查所有任务是否
///   都已到达终态）。如果工作流变为终态，调用方应通知下游系统。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum FailureScope {
    TaskOnly,
    /// 应用工作流级别的失败语义。
    /// 从 `Terminal` 重命名，以避免与 `Phase::is_terminal` 概念混淆 ——
    /// 此变体表示"将失败传播到工作流级别"。
    WorkflowLevel,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(bytecheck())]
pub struct TaskState {
    pub task_id: TaskId,
    pub state: Phase,
    pub result: Option<Vec<u8>>,
    /// 任务进入 `Failed` 状态时捕获的错误消息。
    #[serde(default)]
    pub error: Option<String>,
    retry_count: u32,
    /// 任务派发代数（dispatch generation）：每次重派发/重试（`reset_task`
    /// 的 `increment_attempt`）递增一次，并随 TaskDefinition 带上派发。
    /// 用于结果接受侧的 attempt fencing——过期代数的结果必须被丢弃。
    attempt: u32,
}

impl TaskState {
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub(crate) fn increment_retry_count(&mut self) {
        self.retry_count += 1;
    }

    pub(crate) fn increment_attempt(&mut self) {
        self.attempt += 1;
    }
}

impl Terminal for TaskState {
    fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub struct WorkflowExecution {
    pub workflow_id: WorkflowId,
    pub state: Phase,
    pub tasks: HashMap<TaskId, TaskState>,
    /// 已成功完成的任务数量。
    succeeded_count: usize,
    /// 由于条件分支而跳过任务数量。
    skipped_count: usize,
    total_count: usize,
    deadline_ms: Option<u64>,
    started_at_ms: Option<u64>,
    /// 工作流在任务失败时的反应策略。
    #[serde(default)]
    pub failure_strategy: FailureStrategy,
    /// 工作流进入 `Failed` 状态时捕获的错误消息。
    #[serde(default)]
    pub error: Option<String>,
}

impl WorkflowExecution {
    /// 已成功完成的任务数量。
    pub fn succeeded_count(&self) -> usize {
        self.succeeded_count
    }

    /// 按任务 ID 升序收集已完成任务的结果。
    ///
    /// `tasks` 是 `HashMap`，迭代顺序未定义；按 `task_id` 排序保证结果顺序
    /// 确定性。store 路径与内存路径共用此方法，确保两条路径返回一致的结果。
    pub fn collected_results(&self) -> Vec<Vec<u8>> {
        let mut entries: Vec<(&str, &Vec<u8>)> = self
            .tasks
            .iter()
            .filter_map(|(id, ts)| ts.result.as_ref().map(|r| (id.0.as_str(), r)))
            .collect();
        entries.sort_by_key(|(id, _)| *id);
        entries.into_iter().map(|(_, r)| r.clone()).collect()
    }

    pub fn total_count(&self) -> usize {
        self.total_count
    }

    pub fn deadline_ms(&self) -> Option<u64> {
        self.deadline_ms
    }

    pub fn started_at_ms(&self) -> Option<u64> {
        self.started_at_ms
    }

    pub(crate) fn set_deadline_ms(&mut self, deadline: u64) {
        self.deadline_ms = Some(deadline);
    }

    pub fn new(workflow_id: WorkflowId, task_ids: Vec<TaskId>) -> Self {
        let total = task_ids.len();
        let tasks = task_ids
            .into_iter()
            .map(|id| {
                (
                    id.clone(),
                    TaskState {
                        task_id: id,
                        state: Phase::Pending,
                        result: None,
                        error: None,
                        retry_count: 0,
                        attempt: 0,
                    },
                )
            })
            .collect();
        Self {
            workflow_id,
            state: Phase::Pending,
            tasks,
            succeeded_count: 0,
            skipped_count: 0,
            total_count: total,
            deadline_ms: None,
            started_at_ms: None,
            failure_strategy: FailureStrategy::default(),
            error: None,
        }
    }

    pub fn with_failure_strategy(mut self, strategy: FailureStrategy) -> Self {
        self.failure_strategy = strategy;
        self
    }

    pub fn mark_running(&mut self) {
        self.state = Phase::Running;
        if self.started_at_ms.is_none() {
            self.started_at_ms = Some(crate::common::epoch_millis());
        }
    }

    pub fn is_expired(&self) -> bool {
        if let (Some(deadline), Some(started)) = (self.deadline_ms, self.started_at_ms) {
            let now_ms = crate::common::epoch_millis();
            now_ms > started + deadline
        } else {
            false
        }
    }

    pub fn mark_task_running(&mut self, task_id: &TaskId) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            task.state = Phase::Running;
        }
    }

    /// 标记任务为 Completed。
    ///
    /// 返回 `true` 表示状态实际发生推进；`false` 表示被守卫拒绝：
    /// - 终态守卫（[`Self::can_transition_task`]）：工作流已终态，或任务已处于
    ///   任意终态——迟到的重复完成结果不得改写 `Cancelled` / `Failed` 等终态，
    ///   也不得把已终态工作流"复活"为 Completed；
    /// - attempt fencing（[`Self::attempt_fencing_passes`]）：结果所属派发代数
    ///   落后于任务当前记录的代数——重派发/重试后旧代在途执行的迟到结果被丢弃。
    ///
    /// `result_attempt` 为 `None` 表示结果通路未携带派发代数（wire 协议尚未
    /// 扩展 `WireTaskResult` / `WireDagStateUpdate`），无法判定代数，fencing
    /// 放行。
    pub fn mark_task_completed(
        &mut self,
        task_id: &TaskId,
        result: Vec<u8>,
        result_attempt: Option<u32>,
    ) -> bool {
        if !self.can_transition_task(task_id)
            || !self.attempt_fencing_passes(task_id, result_attempt)
        {
            return false;
        }
        if let Some(task) = self.tasks.get_mut(task_id) {
            task.state = Phase::Completed;
            task.result = Some(result);
            self.succeeded_count += 1;
        }
        self.check_workflow_completion();
        true
    }

    /// attempt fencing：判定结果所属派发代数是否仍可被接受。
    ///
    /// 结果的 attempt 必须 ≥ 任务当前记录的 attempt（同代重复投递或更新代均
    /// 放行）。派发侧保证每次重派发/重试（`reset_task` 的 `increment_attempt`）
    /// 递增 [`TaskState::attempt`] 并随 TaskDefinition 携带，因此小于当前值的
    /// result_attempt 一定是过期代数在途执行的迟到结果。
    ///
    /// 与 [`Self::can_transition_task`] 互补：fencing 校验派发代际，
    /// 终态守卫校验状态机合法性。任务不存在时放行（NotFound 由上层处理）。
    ///
    /// `pub(crate)`：orchestrator 的 `complete_task` 在推进状态与发事件**之前**
    /// 需要先判定 fencing（被拒结果不得触发 TaskCompleted 事件或后继调度），
    /// 因此单独暴露给同 crate 使用；`mark_task_completed` / `fail_task` 内部
    /// 亦会再次调用，保证任何直接写入路径都经过 fencing。
    pub(crate) fn attempt_fencing_passes(
        &self,
        task_id: &TaskId,
        result_attempt: Option<u32>,
    ) -> bool {
        let Some(result_attempt) = result_attempt else {
            return true;
        };
        let Some(task) = self.tasks.get(task_id) else {
            return true;
        };
        let current_attempt = task.attempt();
        if result_attempt < current_attempt {
            tracing::debug!(
                task = %task_id.as_str(),
                result_attempt,
                current_attempt,
                "dropping stale result: dispatch generation older than current attempt"
            );
            return false;
        }
        true
    }

    /// 集中式终态守卫：判断任务是否允许被推进（Completed / Failed 等写入）。
    ///
    /// 合法迁移要求同时满足：
    /// - 工作流未处于终态（终态工作流不可被任何任务事件改写）；
    /// - 任务存在且未处于终态（终态任务不可被覆盖）。
    ///
    /// 所有任务状态写入路径（`mark_task_completed` / `fail_task` /
    /// orchestrator 的 `complete_task`）都应经由本守卫判断，避免守卫口径
    /// 分散后出现遗漏。
    pub fn can_transition_task(&self, task_id: &TaskId) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        self.tasks
            .get(task_id)
            .is_some_and(|t| !t.state.is_terminal())
    }

    /// 标记任务为已跳过（条件分支未被取）。
    pub fn mark_task_skipped(&mut self, task_id: &TaskId) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            // 幂等性：已处于终态则跳过
            if task.state.is_terminal() {
                return;
            }
            task.state = Phase::Skipped;
            self.skipped_count += 1;
        }
        self.check_workflow_completion();
    }

    /// 检查工作流是否已到达终态。
    fn check_workflow_completion(&mut self) {
        // 终态工作流不可被改写：已终态（如 Cancelled / Failed）后到达的
        // 任务事件不得把状态翻转为 Completed。
        if self.state.is_terminal() {
            return;
        }
        // 所有 task 已完成或被跳过 → workflow 完成
        if self.succeeded_count + self.skipped_count == self.total_count {
            self.state = Phase::Completed;
        } else if self.failure_strategy != FailureStrategy::FailFast {
            // 非 fail-fast：检查是否所有 task 都已到达终态
            let all_terminal = self.tasks.values().all(|t| t.state.is_terminal());
            if all_terminal {
                let any_failed = self.tasks.values().any(|t| t.state == Phase::Failed);
                if any_failed {
                    let msg = self.collect_failed_summary();
                    self.set_failed(msg);
                } else {
                    self.state = Phase::Completed;
                }
            }
        }
    }

    /// 进入工作流 `Failed` 状态并记录错误。
    fn set_failed(&mut self, error: String) {
        self.error = Some(error);
        self.state = Phase::Failed;
    }

    /// Mark a task as failed.
    ///
    /// The `mode` parameter controls the scope:
    /// - `FailureScope::TaskOnly`: Only mark the task as Failed. The workflow
    ///   remains non-terminal, allowing `prepare_retry` to reset the task.
    /// - `FailureScope::WorkflowLevel`: Mark the task as Failed AND apply workflow-level
    ///   failure semantics based on `failure_strategy`:
    ///   - `FailFast`: the entire workflow is marked as failed immediately.
    ///   - `Continue`: the workflow is marked failed only when all
    ///     tasks have reached a terminal state and at least one has failed.
    ///
    /// Idempotency: skips if the task is already Completed or Failed, or if
    /// the workflow is already in a terminal state.
    ///
    /// `result_attempt` 语义与 [`Self::mark_task_completed`] 相同：过期派发
    /// 代数的失败报告同样会被 attempt fencing 丢弃。
    pub fn fail_task(
        &mut self,
        task_id: &TaskId,
        error: String,
        mode: FailureScope,
        result_attempt: Option<u32>,
    ) {
        // 幂等性：workflow 已处于终态、任务已处任意终态（含 Cancelled——迟到的失败
        // 事件不得把已取消任务改写为 Failed 并参与 fail-fast 计数）则跳过。
        if !self.can_transition_task(task_id)
            || !self.attempt_fencing_passes(task_id, result_attempt)
        {
            return;
        }
        if let Some(task) = self.tasks.get_mut(task_id) {
            task.state = Phase::Failed;
            task.error = Some(error.clone());
        }

        match mode {
            FailureScope::TaskOnly => {}
            FailureScope::WorkflowLevel => {
                if self.failure_strategy == FailureStrategy::FailFast {
                    self.set_failed(error);
                } else {
                    let all_terminal = self.tasks.values().all(|t| t.state.is_terminal());
                    if all_terminal {
                        let any_failed = self.tasks.values().any(|t| t.state == Phase::Failed);
                        if any_failed {
                            let msg = self.collect_failed_summary();
                            self.set_failed(msg);
                        }
                    }
                }
            }
        }
    }

    /// 为工作流错误消息构建失败任务摘要。
    fn collect_failed_summary(&self) -> String {
        const MAX_FAILED_SUMMARY_ENTRIES: usize = 10;
        const MAX_FAILED_ERROR_LEN: usize = 200;

        let mut failed: Vec<String> = Vec::new();
        for (id, t) in &self.tasks {
            if t.state == Phase::Failed {
                let e = t.error.as_deref().unwrap_or("");
                let truncated = if e.len() > MAX_FAILED_ERROR_LEN {
                    // 按字节切片会 panic：若第 MAX_FAILED_ERROR_LEN 字节落在多字节
                    // UTF-8 字符（中文/emoji）中间，&e[..MAX_FAILED_ERROR_LEN] 会
                    // 触发 "byte index is not a char boundary"。回退到最近的字符
                    // 边界，保证不 panic 且不超过字节上限。
                    let mut end = MAX_FAILED_ERROR_LEN;
                    while !e.is_char_boundary(end) {
                        end -= 1;
                    }
                    format!("{}…", &e[..end])
                } else {
                    e.to_string()
                };
                failed.push(format!("{}: {}", id.0, truncated));
                if failed.len() >= MAX_FAILED_SUMMARY_ENTRIES {
                    break;
                }
            }
        }
        let total_failed = self
            .tasks
            .values()
            .filter(|t| t.state == Phase::Failed)
            .count();
        if total_failed > MAX_FAILED_SUMMARY_ENTRIES {
            format!(
                "failed tasks ({} of {}): [{}]",
                MAX_FAILED_SUMMARY_ENTRIES,
                total_failed,
                failed.join(", ")
            )
        } else {
            format!("failed tasks: [{}]", failed.join(", "))
        }
    }

    /// 重置任务为 Pending 状态，用于重试或重新调度。
    ///
    /// 只有处于 Running 或 Failed 状态的任务会被重置 —— Completed 或
    /// Cancelled 状态的任务保持不变。
    ///
    /// 当 `increment_retry` 为 true 时，任务的 retry 计数器会递增
    /// （用于失败后的正常任务重试）。
    /// 当 `increment_attempt` 为 true 时，任务的 attempt 计数器会递增
    /// （用于 orchestrator 故障转移后的重新调度）。
    pub fn reset_task(&mut self, task_id: &TaskId, increment_retry: bool, increment_attempt: bool) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            if increment_retry {
                if !matches!(task.state, Phase::Failed) {
                    return;
                }
                task.increment_retry_count();
            } else {
                if !matches!(task.state, Phase::Pending | Phase::Running | Phase::Failed) {
                    return;
                }
            }
            if increment_attempt {
                task.increment_attempt();
            }
            task.state = Phase::Pending;
            task.result = None;
        }
    }

    pub fn is_workflow_failed(&self) -> bool {
        matches!(self.state, Phase::Failed)
    }

    pub fn mark_workflow_failed(&mut self, error: String) {
        for task in self.tasks.values_mut() {
            if task.state == Phase::Running {
                task.state = Phase::Failed;
                task.error = Some(error.clone());
            }
        }
        self.set_failed(error);
    }

    pub fn mark_cancelled(&mut self) {
        for task in self.tasks.values_mut() {
            if task.state == Phase::Running {
                task.state = Phase::Cancelled;
            }
        }
        self.state = Phase::Cancelled;
    }

    /// 取消指定 ID 的任务。如果任务未运行，则返回 false。
    pub fn cancel_task(&mut self, task_id: &TaskId) -> bool {
        if let Some(task) = self.tasks.get_mut(task_id) {
            if matches!(task.state, Phase::Running) {
                task.state = Phase::Cancelled;
                return true;
            }
        }
        false
    }
}

impl Terminal for WorkflowExecution {
    fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

/// 等待点条件（S1 持久化等待点原语）。
///
/// 纯数据结构，与 [`Dag`]/[`Phase`] 同层：不解释条件语义、不触发唤醒，
/// 唤醒由 `Orchestrator` 的等待点 API 追加事件后推进状态机。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum WaitCondition {
    /// 外部信号触发（S2 经 Signals capability 递交）。
    Signal {
        /// 信号名。与 wait_key 相互独立：wait_key 是注册表键，name 是信号语义名。
        name: String,
    },
    /// 定时到期唤醒。`deadline_ms` 为绝对 epoch 毫秒，与工作流级超时
    /// watcher（`is_expired` / `epoch_millis`）使用同一时钟基准。
    Timer { deadline_ms: u64 },
}

/// 等待点当前状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum WaitPointState {
    /// 等待条件满足。
    Waiting,
    /// 已被唤醒（signal 递交或 timer 到期），携带唤醒时附带的 payload。
    Signaled { payload: Vec<u8> },
}

/// 单个持久化等待点：`(workflow_id, wait_key)` 唯一标识，条件 + 状态。
///
/// 事实源是 `workflow:{id}` topic 的等待点事件（WaitPointRegistered /
/// SignalReceived / TimerFired）；随快照落盘的等待点条目（`orch:wait:{id}`）
/// 是重放加速缓存，与 exec/pending 快照同批写入。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitPoint {
    pub condition: WaitCondition,
    pub state: WaitPointState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub struct DagNode {
    pub task_id: TaskId,
    pub name: String,
    pub payload: Vec<u8>,
    pub retry_policy: Option<RetryPolicy>,
    pub timeout_ms: Option<u64>,
    /// 任务优先级（有符号整数）。数值越高越紧急。
    #[serde(default)]
    pub priority: i32,
    /// 任务元数据键值对，由 Python 层添加。
    /// Rust 视为不透明数据，不解释它。
    /// 用于标签、路由提示或任何 Python定义的属性。
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub(crate) struct DagEdge {
    pub from: TaskId,
    pub to: TaskId,
    /// 条件标签，用于条件分支。
    /// 当存在时，仅当条件为 true 时激活此边。
    /// Python 调度循环在运行时评估条件式。
    #[serde(default)]
    pub condition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub struct Dag {
    nodes: HashMap<TaskId, DagNode>,
    edges: Vec<DagEdge>,
    predecessors: HashMap<TaskId, Vec<TaskId>>,
    successors: HashMap<TaskId, Vec<TaskId>>,
    /// 默认重试策略，应用于未指定自己重试策略的任务。
    #[serde(default)]
    pub default_retry_policy: Option<RetryPolicy>,
    /// 如何处理任务失败。
    #[serde(default)]
    pub failure_strategy: FailureStrategy,
}

impl Dag {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
            predecessors: HashMap::new(),
            successors: HashMap::new(),
            default_retry_policy: None,
            failure_strategy: FailureStrategy::default(),
        }
    }

    pub fn add_node(&mut self, node: DagNode) -> Result<()> {
        let id = node.task_id.clone();
        self.nodes.insert(id.clone(), node);
        self.predecessors.entry(id.clone()).or_default();
        self.successors.entry(id).or_default();
        Ok(())
    }

    pub fn add_edge(&mut self, from: TaskId, to: TaskId) -> Result<()> {
        if !self.nodes.contains_key(&from) {
            return Err(crate::common::ActantError::Workflow(format!(
                "node {} not found",
                from.as_str()
            )));
        }
        if !self.nodes.contains_key(&to) {
            return Err(crate::common::ActantError::Workflow(format!(
                "node {} not found",
                to.as_str()
            )));
        }

        if from == to || self.path_exists(&to, &from) {
            return Err(crate::common::ActantError::Workflow(
                "adding edge would create a cycle".into(),
            ));
        }

        self.edges.push(DagEdge {
            from: from.clone(),
            to: to.clone(),
            condition: None,
        });
        self.successors
            .entry(from.clone())
            .or_default()
            .push(to.clone());
        self.predecessors.entry(to).or_default().push(from);

        Ok(())
    }

    /// 添加条件分支边。
    /// 当存在时，仅当条件为 true 时激活此边。
    /// Python 调度循环在运行时评估条件式。
    pub fn add_conditional_edge(
        &mut self,
        from: TaskId,
        to: TaskId,
        condition: String,
    ) -> Result<()> {
        if !self.nodes.contains_key(&from) {
            return Err(crate::common::ActantError::Workflow(format!(
                "node {} not found",
                from.as_str()
            )));
        }
        if !self.nodes.contains_key(&to) {
            return Err(crate::common::ActantError::Workflow(format!(
                "node {} not found",
                to.as_str()
            )));
        }

        if from == to || self.path_exists(&to, &from) {
            return Err(crate::common::ActantError::Workflow(
                "adding conditional edge would create a cycle".into(),
            ));
        }

        self.edges.push(DagEdge {
            from: from.clone(),
            to: to.clone(),
            condition: Some(condition),
        });
        self.successors
            .entry(from.clone())
            .or_default()
            .push(to.clone());
        self.predecessors.entry(to).or_default().push(from);

        Ok(())
    }

    fn path_exists(&self, source: &TaskId, target: &TaskId) -> bool {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(source);
        while let Some(current) = queue.pop_front() {
            if current == target {
                return true;
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            if let Some(succs) = self.successors.get(current) {
                for succ in succs {
                    if !visited.contains(succ) {
                        queue.push_back(succ);
                    }
                }
            }
        }
        false
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn get_node(&self, id: &TaskId) -> Option<&DagNode> {
        self.nodes.get(id)
    }

    pub fn predecessors_of(&self, id: &TaskId) -> Vec<&DagNode> {
        self.predecessors
            .get(id)
            .map(|ids| ids.iter().filter_map(|i| self.nodes.get(i)).collect())
            .unwrap_or_default()
    }

    pub fn predecessor_count(&self, id: &TaskId) -> usize {
        self.predecessors.get(id).map(|p| p.len()).unwrap_or(0)
    }

    pub fn successor_ids(&self, id: &TaskId) -> Vec<TaskId> {
        self.successors.get(id).cloned().unwrap_or_default()
    }

    /// 返回从给定任务出发的条件分支边。
    /// 每个条目为 (后继任务 ID,条件标签)。
    pub fn conditional_edges_from(&self, id: &TaskId) -> Vec<(TaskId, String)> {
        self.edges
            .iter()
            .filter(|e| &e.from == id)
            .filter_map(|e| e.condition.as_ref().map(|c| (e.to.clone(), c.clone())))
            .collect()
    }

    pub fn successors_of(&self, id: &TaskId) -> Vec<&DagNode> {
        self.successors
            .get(id)
            .map(|ids| ids.iter().filter_map(|i| self.nodes.get(i)).collect())
            .unwrap_or_default()
    }

    pub fn roots(&self) -> Vec<&DagNode> {
        self.nodes
            .keys()
            .filter(|id| {
                self.predecessors
                    .get(id)
                    .map(|p| p.is_empty())
                    .unwrap_or(true)
            })
            .filter_map(|id| self.nodes.get(id))
            .collect()
    }

    pub fn sinks(&self) -> Vec<&DagNode> {
        self.nodes
            .keys()
            .filter(|id| {
                self.successors
                    .get(id)
                    .map(|s| s.is_empty())
                    .unwrap_or(true)
            })
            .filter_map(|id| self.nodes.get(id))
            .collect()
    }

    pub fn topological_sort(&self) -> Result<Vec<&DagNode>> {
        let mut in_degree: HashMap<&TaskId, usize> = HashMap::new();
        for id in self.nodes.keys() {
            in_degree.insert(id, self.predecessors.get(id).map(|p| p.len()).unwrap_or(0));
        }

        let mut queue: VecDeque<&TaskId> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(id, _)| *id)
            .collect();

        let mut sorted = Vec::new();

        while let Some(id) = queue.pop_front() {
            let node = self.nodes.get(id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!("node {} not found", id.as_str()))
            })?;
            sorted.push(node);
            if let Some(succs) = self.successors.get(id) {
                for succ in succs {
                    if let Some(deg) = in_degree.get_mut(succ) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(succ);
                        }
                    }
                }
            }
        }

        if sorted.len() != self.nodes.len() {
            return Err(crate::common::ActantError::Workflow(
                "graph has a cycle".into(),
            ));
        }

        Ok(sorted)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &DagNode> {
        self.nodes.values()
    }

    /// 返回节点的有效重试策略：
    /// 如果节点设置了自己的重试策略，则返回该策略；否则返回 DAG 级默认重试策略。
    /// 如果节点不存在或未设置任何重试策略，则返回 None。
    pub fn effective_retry_policy(&self, task_id: &TaskId) -> Option<RetryPolicy> {
        let node = self.nodes.get(task_id)?;
        node.retry_policy
            .clone()
            .or_else(|| self.default_retry_policy.clone())
    }
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "../../../tests/rust/unit/runtime/workflow/dag.rs"]
mod tests;
