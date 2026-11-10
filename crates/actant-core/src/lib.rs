//! # Actant Core（框架主体）
//!
//! Actor 运行时、DAG 编排状态机、ERH 能力分发、iroh 网络与持久化。
//! **不含任何 Python 语义**——载荷为不透明字节，PyO3 绑定位于 workspace
//! 门面 crate `actant` 的 `src/py` 模块。
//!
//! 模块职责表、嵌入路径与边界约束见 `docs/architecture/overview.md`；
//! 注入缝契约见 `docs/architecture/framework-contract.md`；验收示例见
//! `examples/rust_embed.rs`。

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
