//! 工作流 Orchestrator 的聚合入口。
//!
//! `orchestrator` 按职责拆分为：
//! - `state`: 结构体定义与依赖注入
//! - `types`: slot、等待者注册表与事件契约重导出
//! - `keys`: Store key 生成
//! - `persistence`: 恢复（快照 + 事件重放）、落盘、清理、迁移
//! - `execution`: 提交、启动、任务完成、取消、重试、调度（含 `timeout` 子模块）
//! - `queries`: 状态查询与结果读取
//! - `waitpoint`: 持久化等待点原语
//! - `context`: 确定性上下文值
//!
//! 本文件仅保留子模块声明与公共 re-export，保持外部 API 不变。

mod keys;
/// `pub(crate)`：`JournalEvent` 是事件历史的载荷类型，crate 内
/// （含测试）需可解码 `EventLog` 条目；不对外部 crate 暴露。
pub mod types;

mod context;
mod execution;
mod persistence;
pub mod queries;
mod state;
mod waitpoint;

/// Python 暴露面的序列化形态，由 PyO3 边界层构造。
pub use execution::FailureOutcome;
pub use queries::{DagSnapshot, WorkflowStateSummary};
pub use state::Orchestrator;

#[cfg(test)]
#[path = "../../../../../tests/rust/unit/runtime/workflow/orchestrator.rs"]
mod tests;
