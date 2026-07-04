use std::collections::HashMap;

use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::common::{TaskId, WorkflowId};

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
/// 替代了之前的 `WorkflowState` 和 `TaskStateKind` 枚举，这两个枚举具有
/// 相同的变体但名称不同 —— 这是命名歧义的来源。
/// 使用单一的 `Phase` 类型消除了重复，而包含它的结构体
/// （`WorkflowExecution` 或 `TaskState`）提供了语义上下文。
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

impl Terminal for Phase {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Phase::Completed | Phase::Failed | Phase::Cancelled | Phase::Skipped
        )
    }
}

/// 终端状态类型 trait。
///
/// 中心化 [`Phase`]、 [`TaskState`] 和 [`WorkflowExecution`] 定义的“终端”状态。
pub trait Terminal {
    /// 如果没有进一步的状态转换，则返回 `true` 。
    fn is_terminal(&self) -> bool;
}

/// 工作流在任务失败时的反应策略。
///
/// 替换了之前的 `String` + `FAIL_FAST` 常量模式。使用枚举使得策略
/// 可穷尽匹配，防止拼写错误的字符串比较。
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// 仅包括 `Failed` 状态的任务。
    succeeded_count: usize,
    /// 由于条件分支而跳过任务数量。
    skipped_count: usize,
    total_count: usize,
    deadline_ms: Option<u64>,
    started_at_ms: Option<u64>,
    /// 工作流在任务失败时的反应策略。
    /// 替换了之前的 `String` + `FAIL_FAST` 常量模式。
    #[serde(default)]
    pub failure_strategy: FailureStrategy,
    /// 工作流进入 `Failed` 状态时捕获的错误消息。
    /// 与 [`Phase`] 枚举分离，保持保持负载为空。
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

    pub fn mark_task_completed(&mut self, task_id: &TaskId, result: Vec<u8>) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            // 幂等性：已 completed 则跳过
            if matches!(task.state, Phase::Completed) {
                return;
            }
            task.state = Phase::Completed;
            task.result = Some(result);
            self.succeeded_count += 1;
        }
        self.check_workflow_completion();
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
    /// 错误消息存储在 [`WorkflowExecution::error`] 中，保持 [`Phase`] 枚举负载为空。
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
    pub fn fail_task(&mut self, task_id: &TaskId, error: String, mode: FailureScope) {
        // 幂等性：workflow 已处于终态则跳过
        if self.state.is_terminal() {
            return;
        }
        if let Some(task) = self.tasks.get_mut(task_id) {
            // 已 Completed 的 task 跳过 — 永不覆盖成功结果
            if matches!(task.state, Phase::Completed) {
                return;
            }
            // 已 Failed 的 task 跳过（幂等性）
            if matches!(task.state, Phase::Failed) {
                return;
            }
            task.state = Phase::Failed;
            task.error = Some(error.clone());
        }

        match mode {
            FailureScope::TaskOnly => {
                // 不触发 workflow 级别状态转换
            }
            FailureScope::WorkflowLevel => {
                if self.failure_strategy == FailureStrategy::FailFast {
                    self.set_failed(error);
                } else {
                    // 非 fail-fast：检查是否所有 task 都已到达终态
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

    /// 为工作流错误消息构建失败任务摘要摘要。
    ///
    /// 限制输出最多 `MAX_FAILED_SUMMARY_ENTRIES` 个任务
    /// 并将每个错误截断为 `MAX_FAILED_ERROR_LEN` 个字符，以防止无限制增长。
    fn collect_failed_summary(&self) -> String {
        const MAX_FAILED_SUMMARY_ENTRIES: usize = 10;
        const MAX_FAILED_ERROR_LEN: usize = 200;

        let mut failed: Vec<String> = Vec::new();
        for (id, t) in &self.tasks {
            if t.state == Phase::Failed {
                let e = t.error.as_deref().unwrap_or("");
                let truncated = if e.len() > MAX_FAILED_ERROR_LEN {
                    format!("{}…", &e[..MAX_FAILED_ERROR_LEN])
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
    ///
    /// 如果 `increment_retry` 为 true，只有 Failed 状态的任务会被重置 ——
    /// Pending 状态的任务不应增加其 retry 计数（它已经在等待执行）。
    /// 如果 `increment_retry` 为 false（故障转移重新调度），
    /// Pending 状态的任务也会被重置，因为它们可能需要被重新调度。
    pub fn reset_task(&mut self, task_id: &TaskId, increment_retry: bool, increment_attempt: bool) {
        if let Some(task) = self.tasks.get_mut(task_id) {
            if increment_retry {
                // 重试路径：仅从 Failed 状态重置，避免在 task 已是 Pending
                // 时收到重复完成事件导致 retry_count 重复递增。
                if !matches!(task.state, Phase::Failed) {
                    return;
                }
                task.increment_retry_count();
            } else {
                // Failover/reschedule 路径：允许从 Pending、Running 或 Failed 状态重置。
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
        // 同时将所有 running 的 task 标记为 failed
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

    /// 取消指定 ID 的任务。
    /// 如果任务未运行，则返回 false。
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- Phase::as_str / Phase::parse 往返契约（Rust↔Python 边界） ---

    #[test]
    fn phase_as_str_returns_canonical_constants() {
        assert_eq!(Phase::Pending.as_str(), phase_str::PENDING);
        assert_eq!(Phase::Running.as_str(), phase_str::RUNNING);
        assert_eq!(Phase::Completed.as_str(), phase_str::COMPLETED);
        assert_eq!(Phase::Failed.as_str(), phase_str::FAILED);
        assert_eq!(Phase::Cancelled.as_str(), phase_str::CANCELLED);
        assert_eq!(Phase::Skipped.as_str(), phase_str::SKIPPED);
    }

    #[test]
    fn phase_parse_roundtrips_all_variants() {
        for p in [
            Phase::Pending,
            Phase::Running,
            Phase::Completed,
            Phase::Failed,
            Phase::Cancelled,
            Phase::Skipped,
        ] {
            let s = p.as_str();
            assert_eq!(Phase::parse(s), Some(p), "roundtrip failed for {s}");
        }
    }

    #[test]
    fn phase_parse_is_case_insensitive() {
        assert_eq!(Phase::parse("pending"), Some(Phase::Pending));
        assert_eq!(Phase::parse("RUNNING"), Some(Phase::Running));
        assert_eq!(Phase::parse("CoMpLeTeD"), Some(Phase::Completed));
    }

    #[test]
    fn phase_parse_unknown_returns_none() {
        assert_eq!(Phase::parse(""), None);
        assert_eq!(Phase::parse("unknown"), None);
        assert_eq!(Phase::parse("paused"), None);
    }

    // --- Phase::is_terminal 终态语义 ---

    #[test]
    fn phase_terminal_states() {
        assert!(!Phase::Pending.is_terminal());
        assert!(!Phase::Running.is_terminal());
        assert!(Phase::Completed.is_terminal());
        assert!(Phase::Failed.is_terminal());
        assert!(Phase::Cancelled.is_terminal());
        assert!(Phase::Skipped.is_terminal());
    }

    #[test]
    fn phase_default_is_pending() {
        assert_eq!(Phase::default(), Phase::Pending);
    }

    // --- FailureStrategy 边界 ---

    #[test]
    fn failure_strategy_as_str_canonical() {
        assert_eq!(FailureStrategy::FailFast.as_str(), "fail_fast");
        assert_eq!(FailureStrategy::Continue.as_str(), "continue");
    }

    #[test]
    fn failure_strategy_parse_canonical_and_aliases() {
        // 规范名
        assert_eq!(
            FailureStrategy::parse("fail_fast"),
            Some(FailureStrategy::FailFast)
        );
        assert_eq!(
            FailureStrategy::parse("continue"),
            Some(FailureStrategy::Continue)
        );
        // 别名（兼容历史/外部输入）
        assert_eq!(
            FailureStrategy::parse("FailFast"),
            Some(FailureStrategy::FailFast)
        );
        assert_eq!(
            FailureStrategy::parse("continue_on_failure"),
            Some(FailureStrategy::Continue)
        );
        assert_eq!(
            FailureStrategy::parse("best_effort"),
            Some(FailureStrategy::Continue)
        );
    }

    #[test]
    fn failure_strategy_parse_unknown_returns_none() {
        assert_eq!(FailureStrategy::parse(""), None);
        assert_eq!(FailureStrategy::parse("abort"), None);
    }

    #[test]
    fn failure_strategy_default_is_fail_fast() {
        assert_eq!(FailureStrategy::default(), FailureStrategy::FailFast);
    }
}
