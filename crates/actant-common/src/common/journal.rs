//! 工作流 journal：语言无关的事件契约。
//!
//! journal 是工作流历史的**唯一事实源**：恢复 = 快照 + 其后事件重放，导出与
//! 重放读同一份记录。事件按 `workflow:{workflow_id}` topic 追加，载荷用
//! postcard 编码；Rust 之外的语言只需遵守 [`JOURNAL_SCHEMA_VERSION`] 与
//! [`JournalEvent`] 的字段形状，不依赖 Rust 内部类型。
//!
//! 设计约束：
//! - 除 [`JournalEvent::TaskCompleted`] 携带结果字节（重放需要恢复结果）与
//!   [`JournalEvent::NodeAdded`] 携带节点定义（历史可重建 DAG）外，不携带
//!   完整 DAG / Execution，避免事件体积过大。
//! - `payload` / `args` / `result` 一律是上层序列化的不透明字节，Rust 不解析。
//! - 新增变体属于 schema 变更：提升 [`JOURNAL_SCHEMA_VERSION`]；该版本号是
//!   公开契约，非 Rust 消费者据此判断能否解析。

use serde::{Deserialize, Serialize};

use super::model::{DagNode, TaskId, WaitCondition, WorkflowId, WorkflowInvocation};

/// journal 事件 schema 版本。
///
/// 只在**不兼容**的字段/语义变更时递增；新增可选字段不递增。
pub const JOURNAL_SCHEMA_VERSION: u16 = 1;

/// 工作流级别事件，写入 `EventLog` 的 `workflow:{workflow_id}` topic。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEvent {
    /// 工作流被提交。`invocation` 记录定义名、版本与入参，供任意节点重建
    /// “这是什么工作流”；旧记录或非 flow 提交路径可能为 `None`。
    Submitted {
        workflow_id: WorkflowId,
        invocation: Option<WorkflowInvocation>,
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
    /// 任务完成。携带结果字节：历史是结果的事实源（重放“已完成返回
    /// 记录结果”、回灌均读同一历史）。
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
    /// 确定性上下文值（flow 体的 wall-clock / 随机数 / UUID）。
    ///
    /// flow 体重放时按 `key` 取回记录值而不是重新生成，故“第 n 次 ctx 调用”
    /// 必须与历史同序同位；`kind` 参与一致性校验——同 `key` 记录成另一种
    /// `kind` 说明提交序列已漂移，重放显式失败。
    ContextValue {
        workflow_id: WorkflowId,
        key: String,
        kind: String,
        value: Vec<u8>,
    },
    /// 节点重启后从持久化存储恢复工作流时发出。
    ///
    /// `task_count` 为恢复后 slot 中的任务总数（含已完成与待执行）。
    /// `corrupt` 为 true 表示此 workflow 因数据损坏被整体移除（slot 已清理）。
    /// 此事件供外部观测恢复进度，不驱动任何核心状态变迁。
    Recovered {
        workflow_id: WorkflowId,
        task_count: usize,
        corrupt: bool,
    },
}

impl JournalEvent {
    pub fn workflow_id(&self) -> &WorkflowId {
        match self {
            Self::Submitted { workflow_id, .. }
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
            | Self::ContextValue { workflow_id, .. }
            | Self::Recovered { workflow_id, .. } => workflow_id,
        }
    }

    pub fn topic(&self) -> String {
        format!("workflow:{}", self.workflow_id().as_str())
    }

    /// 事件的稳定类型名（导出面 `kind` 字段），与 Rust 变体名一致。
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Submitted { .. } => "submitted",
            Self::NodeAdded { .. } => "node_added",
            Self::TaskDispatched { .. } => "task_dispatched",
            Self::Started { .. } => "started",
            Self::TaskRunning { .. } => "task_running",
            Self::TaskCompleted { .. } => "task_completed",
            Self::TaskFailed { .. } => "task_failed",
            Self::TaskCancelled { .. } => "task_cancelled",
            Self::Completed { .. } => "completed",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
            Self::WaitPointRegistered { .. } => "wait_point_registered",
            Self::SignalReceived { .. } => "signal_received",
            Self::TimerFired { .. } => "timer_fired",
            Self::ContextValue { .. } => "context_value",
            Self::Recovered { .. } => "recovered",
        }
    }
}

/// journal 的一条记录：事件 + 其在 `EventLog` 中的位置。
///
/// `seq` / `timestamp_ms` 来自 `EventId`（HLC），跨节点单调；导出 API 直接
/// 返回本结构，调用方无需理解 `EventLog` 内部游标。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecord {
    pub seq: u64,
    pub timestamp_ms: u64,
    pub event: JournalEvent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_event_kind_and_topic_are_stable() {
        let wf = WorkflowId::new("wf-1".to_string());
        let event = JournalEvent::Started {
            workflow_id: wf.clone(),
        };
        assert_eq!(event.kind(), "started");
        assert_eq!(event.topic(), "workflow:wf-1");
        assert_eq!(event.workflow_id(), &wf);
    }

    #[test]
    fn journal_record_roundtrips_postcard() {
        let record = JournalRecord {
            seq: 7,
            timestamp_ms: 1_700_000_000_000,
            event: JournalEvent::Failed {
                workflow_id: WorkflowId::new("wf-2".to_string()),
                error: "boom".to_string(),
            },
        };
        let bytes = postcard::to_allocvec(&record).expect("encode");
        let decoded: JournalRecord = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.seq, record.seq);
        assert_eq!(decoded.event.kind(), "failed");
    }

    #[test]
    fn invocation_is_carried_by_submitted_event() {
        let invocation = WorkflowInvocation {
            workflow_id: WorkflowId::new("wf-3".to_string()),
            name: "render_frame".to_string(),
            version: 2,
            args: vec![1, 2, 3],
        };
        let event = JournalEvent::Submitted {
            workflow_id: invocation.workflow_id.clone(),
            invocation: Some(invocation.clone()),
        };
        let bytes = postcard::to_allocvec(&event).expect("encode");
        let decoded: JournalEvent = postcard::from_bytes(&bytes).expect("decode");
        match decoded {
            JournalEvent::Submitted {
                invocation: Some(got),
                ..
            } => {
                assert_eq!(got, invocation);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
