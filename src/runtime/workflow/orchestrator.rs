//! 工作流 Orchestrator 的聚合入口。
//!
//! 原 `orchestrator.rs` 已按职责拆分为：
//! - `state`: 结构体定义与依赖注入
//! - `persistence`: 恢复（快照 + 事件重放）、落盘、清理、迁移
//! - `execution`: 提交、启动、任务完成、取消、重试、调度
//! - `queries`: 状态查询与结果读取
//! - `waitpoint`: 持久化等待点原语（S1）
//!
//! 本文件仅保留子模块声明与公共 re-export，保持外部 API 不变。

mod keys;
/// `pub(crate)`：`WorkflowEventPayload` 是事件历史的载荷类型，crate 内
/// （含测试）需可解码 `EventLog` 条目；不对外部 crate 暴露。
pub(crate) mod types;

mod execution;
mod persistence;
mod queries;
mod state;
mod waitpoint;

pub use state::Orchestrator;

#[cfg(test)]
#[path = "../../../tests/rust/unit/runtime/workflow/orchestrator.rs"]
mod tests;
