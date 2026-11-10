//! Batch/array job 逐项进度原语。
//!
//! 一批任务共享 `batch_id`，核心只维护「每项终态计数」这一语义中性事实，
//! 不解释 batch 的业务含义（帧序列、matrix 行、测试场景都只是 task 集合）。
//!
//! 进度由任务终态完成事件驱动：任务进入 Completed/Failed/Cancelled
//! 之一时调用 [`BatchTracker::record_terminal`]，计数只增不减；同一 `task_id`
//! 的重复终态事件（重试/重放）被幂等忽略。`pending` 由总数减去已结算项推导，
//! 因此不存在「完成数超过总数」的竞态。
//!
//! 该原语不落盘：batch 进度是可重建的观测状态，进程重启后由重新提交方
//! 重新注册。需要持久化审计的部署应由第 3 层消费事件自行落库。

use std::collections::HashSet;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use crate::common::{TaskCompletion, TaskId};

/// 单个 batch 项的终态类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchItemState {
    Completed,
    Failed,
    Cancelled,
}

impl BatchItemState {
    /// 从任务终态完成事件映射；`None` 表示该完成事件不是终态。
    pub fn from_completion(completion: &TaskCompletion) -> Option<Self> {
        match completion {
            TaskCompletion::Completed { .. } => Some(Self::Completed),
            TaskCompletion::Failed { .. } => Some(Self::Failed),
            TaskCompletion::Cancelled { .. } => Some(Self::Cancelled),
        }
    }
}

/// batch 进度快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchProgress {
    pub batch_id: String,
    pub total: u32,
    pub completed: u32,
    pub failed: u32,
    pub cancelled: u32,
    /// 尚未结算的项数（含仍在运行与尚未出队的任务）。
    pub pending: u32,
}

impl BatchProgress {
    /// 所有项均已结算（无论成功或失败）。
    pub fn finished(&self) -> bool {
        self.pending == 0
    }
}

#[derive(Default)]
struct BatchState {
    total: u32,
    completed: u32,
    failed: u32,
    cancelled: u32,
}

/// batch 注册表与进度计数器。
///
/// 由 [`Worker`](crate::runtime::workflow::Worker) 持有，任务终态结算时更新；
/// 绑定层通过 `Worker::batch_tracker` 注册 batch 并查询进度。
#[derive(Default)]
pub struct BatchTracker {
    batches: DashMap<String, BatchState>,
    /// task_id → batch_id。`register` 时写入，终态结算后移除（保证幂等）。
    members: DashMap<TaskId, String>,
}

impl BatchTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一批任务。返回该 batch 的进度快照。
    ///
    /// 同一 `batch_id` 重复注册会累加新成员（用于增量提交）；已在其它
    /// batch 中的 `task_id` 不会被重复计数（记录一条 warning，避免静默丢失）。
    /// 成员写入使用 DashMap 的原子 entry，注册与并发注册之间不存在
    /// check-then-insert 竞态。
    pub fn register<I>(&self, batch_id: &str, task_ids: I) -> BatchProgress
    where
        I: IntoIterator<Item = TaskId>,
    {
        let mut added = 0u32;
        for task_id in task_ids {
            match self.members.entry(task_id) {
                Entry::Vacant(slot) => {
                    slot.insert(batch_id.to_string());
                    added = added.saturating_add(1);
                }
                Entry::Occupied(existing) => {
                    // 同一 batch 的重复注册是合法的幂等操作；跨 batch 冲突才
                    // 值得告警（该任务只会结算到先注册的 batch）。
                    if existing.get() != batch_id {
                        tracing::warn!(
                            task_id = %existing.key().as_str(),
                            batch = batch_id,
                            existing_batch = %existing.get(),
                            "task already belongs to another batch; not re-registered"
                        );
                    }
                }
            }
        }

        let mut state = self.batches.entry(batch_id.to_string()).or_default();
        state.total = state.total.saturating_add(added);
        progress_from(batch_id, &state)
    }

    /// 记录一个任务终态。非 batch 成员或重复终态返回 `None`。
    pub fn record_terminal(
        &self,
        task_id: &TaskId,
        state: BatchItemState,
    ) -> Option<BatchProgress> {
        let batch_id = self.members.remove(task_id)?.1;

        let mut batch = self.batches.get_mut(&batch_id)?;
        match state {
            BatchItemState::Completed => batch.completed = batch.completed.saturating_add(1),
            BatchItemState::Failed => batch.failed = batch.failed.saturating_add(1),
            BatchItemState::Cancelled => batch.cancelled = batch.cancelled.saturating_add(1),
        }
        Some(progress_from(&batch_id, &batch))
    }

    /// 查询 batch 进度；未注册的 batch 返回 `None`。
    pub fn progress(&self, batch_id: &str) -> Option<BatchProgress> {
        self.batches
            .get(batch_id)
            .map(|state| progress_from(batch_id, &state))
    }

    /// 移除 batch 及其成员索引（终态查询完成后清理）。
    pub fn remove(&self, batch_id: &str) -> bool {
        self.members.retain(|_, bid| bid != batch_id);
        self.batches.remove(batch_id).is_some()
    }

    /// 当前跟踪的 batch 数量。
    pub fn len(&self) -> usize {
        self.batches.len()
    }

    /// 是否没有任何 batch。
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// 由计数推导含 `pending` 的快照。
fn progress_from(batch_id: &str, state: &BatchState) -> BatchProgress {
    let settled = state
        .completed
        .saturating_add(state.failed)
        .saturating_add(state.cancelled);
    BatchProgress {
        batch_id: batch_id.to_string(),
        total: state.total,
        completed: state.completed,
        failed: state.failed,
        cancelled: state.cancelled,
        pending: state.total.saturating_sub(settled),
    }
}

/// 成员集合类型别名，供调用方构造注册参数。
pub type BatchMembers = HashSet<TaskId>;

#[cfg(test)]
#[path = "../../../../tests/rust/unit/runtime/batch.rs"]
mod tests;
