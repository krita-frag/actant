//! 工作流 Orchestrator 的聚合入口。
//!
//! 原 `orchestrator.rs` 已按职责拆分为：
//! - `state`: 结构体定义与依赖注入
//! - `persistence`: 恢复（快照 + 事件重放）、落盘、清理、迁移
//! - `execution`: 提交、启动、任务完成、取消、重试、调度
//! - `queries`: 状态查询与结果读取
//! - `waitpoint`: 持久化等待点原语
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

/// Python 暴露面的序列化形态（`queries` 模块私有，仅 PyO3 边界层需要构造它）。
///
/// **按 `python` 特性门控**：唯一消费者是绑定层（`src/py/runtime.rs`），
/// 纯框架构建（`--no-default-features`）下不产生 unused 告警
/// （框架构建必须零告警，见 CI 的 `clippy --no-default-features` 门禁；
/// 同款先例见 `workflow.rs` 的 `AddNodeOutcome`）。
#[cfg(feature = "python")]
pub(crate) use queries::DagSnapshot;
pub use state::Orchestrator;

#[cfg(test)]
#[path = "../../../tests/rust/unit/runtime/workflow/orchestrator.rs"]
mod tests;
