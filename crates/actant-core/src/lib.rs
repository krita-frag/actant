//! # Actant Core（框架主体）
//!
//! Actant 的 Rust 框架层：Actor 运行时、DAG 编排状态机、ERH 能力分发、
//! iroh 网络与持久化。**不含任何 Python 语义**——载荷为不透明字节，
//! PyO3 绑定位于 workspace 门面 crate `actant` 的 `src/py` 模块。
//!
//! ## 架构地图
//!
//! | 模块 | 职责 |
//! |------|------|
//! | [`runtime::actor`] | Actor trait、邮箱、ActorSystem facade |
//! | [`runtime::state`] | LMDB store、HLC 与事件日志 |
//! | [`runtime::capability`] | Capability/Handler/Layer 统一扩展模型 |
//! | [`runtime::network`] | iroh 传输、gossip topic、直连请求响应 |
//! | [`runtime::workflow`] | DAG、Orchestrator、Worker、调度、故障转移与 gossip |
//! | [`runtime::builder`] | 将上述子系统按依赖顺序装配成 [`runtime::Runtime`] |
//! | [`metrics`] / [`observability`] | 指标与 tracing 初始化 |
//!
//! ## 嵌入路径
//!
//! Rust 嵌入方从 [`runtime::builder::RuntimeBuilder`] 开始：注入自定义
//! [`runtime::dispatcher::TaskDispatcher`] / [`runtime::workflow::Scheduler`] /
//! [`runtime::network::Discovery`] 等，`with_orchestrator_ingest(true)` 启用
//! core 内的依赖推进（无 Python 事件泵时必需）。验收示例见
//! `examples/rust_embed.rs`；契约文档见 `docs/FRAMEWORK.md`。
//!
//! ## 边界约束
//!
//! - 本 crate 不依赖 PyO3；`src/`（绑定壳）单向依赖本 crate。
//! - 共享类型（协议 / ID / 配置 / wire / 错误）来自 `actant-common`，
//!   本 crate 不反向暴露它们之外的新共享面。
//! - 跨节点消息必须经过 `WireEnvelope` 或 payload signing/verification。

/// 共享类型层 re-export：框架用户经 `actant_core::common` 访问，
/// 模块路径（`common::model` / `common::wire` / ...）与单 crate 时期一致。
pub use actant_common::common;

pub mod metrics;
pub mod observability;
pub mod runtime;

/// 框架测试支撑：内存假件（MockTransport / MockScheduler 等）。
/// 仅在 `test-support` feature（或 crate 内测试）下编译。
#[cfg(any(test, feature = "test-support"))]
#[path = "../../../tests/rust/test_support.rs"]
pub mod test_support;
