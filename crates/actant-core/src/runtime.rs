//! Actant 统一运行时。
//!
//! 本模块是 Rust 核心的主入口，组织 Actor、State、Capability、Network 与
//! Workflow 五个子系统。对外通常只需要 [`Runtime`]；构造过程由
//! [`builder::RuntimeBuilder`] 负责，运行时持有关系由 [`context::Runtime`] 表达。
//!
//! ## 组件关系
//!
//! - [`actor`] 提供所有后台组件的执行容器。
//! - [`state`] 提供持久化与时间戳基础设施。
//! - [`capability`] 提供 ERH 扩展点和 capability actor 化执行。
//! - [`network`] 封装 iroh P2P 传输。
//! - [`blobs`] 提供内容寻址 blob 存取与流式传输原语。
//! - [`workflow`] 在上述基础上实现 DAG 编排、Worker 执行、调度与 failover。
//! - [`dispatcher`] 是 Worker 调用本地任务 handler 的最后一跳。
//! - [`event_bus`] 在运行时内部广播生命周期、网络和 workflow 事件。

pub mod actor;
pub mod blobs;
pub mod builder;
pub mod capability;
pub mod context;
pub mod dispatcher;
pub mod event_bus;
pub mod network;
pub mod state;
pub mod workflow;

pub use context::Runtime;
