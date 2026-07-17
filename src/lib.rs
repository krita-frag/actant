//! # Actant Rust core
//!
//! Actant 的 Rust 层是一个基于 Actor 模型的分布式任务编排核心。它只处理
//! 字节载荷、节点通信、调度、状态持久化和 capability 分发；Python 语义
//! 只存在于 [`py`] 模块边界内。
//!
//! ## 架构地图
//!
//! | 模块 | 职责 |
//! |------|------|
//! | [`common`] | 跨模块共享协议、ID、配置、wire message 与错误类型 |
//! | [`runtime::actor`] | Actor trait、邮箱、监督、ActorSystem facade |
//! | [`runtime::state`] | LMDB store、HLC、checkpoint、WAL 与事件日志 |
//! | [`runtime::capability`] | Capability/Handler/Layer 统一扩展模型 |
//! | [`runtime::network`] | iroh 传输、gossip topic、直连请求响应 |
//! | [`runtime::workflow`] | DAG、Orchestrator、Worker、调度、故障转移与 gossip |
//! | [`runtime::builder`] | 将上述子系统按依赖顺序装配成 [`runtime::Runtime`] |
//! | [`py`] | PyO3 绑定，是 Python 与 Rust 核心的唯一边界 |
//! | [`metrics`] / [`observability`] | 指标与 tracing 初始化 |
//!
//! ## 启动路径
//!
//! 新人阅读运行时装配时，建议从 [`runtime::builder::RuntimeBuilder::build`] 开始。
//! 该函数按 `network -> store -> actor -> workflow -> capability -> worker` 的顺序
//! 创建组件，并解释了 capability 注册、WorkflowActor、FailoverActor、
//! DagGossipActor 与 Worker 之间的依赖关系。
//!
//! ## 任务执行路径
//!
//! Python 的 `@task.submit()` 最终通过 `PyRuntimeCore::submit_task`
//! 把不透明 `Vec<u8>` payload 入队到 [`runtime::workflow::Worker`] 的 scheduler。
//! Worker 只负责调度、路由、签名校验和调用 [`runtime::dispatcher::TaskDispatcher`]；
//! 它不会理解 cloudpickle payload 的语义。
//!
//! ## 边界约束
//!
//! - `src/runtime/**` 和 `src/common/**` 不应依赖 Python 类型或 GIL。
//! - Python handler 只能通过 [`py`] 模块桥接进 capability/dispatcher。
//! - 跨节点消息必须经过 [`common::WireEnvelope`] 或 payload signing/verification。
//! - 关闭顺序由 [`runtime::context::Runtime::shutdown`] 统一控制，避免 Actor、
//!   Worker、任务线程池和 iroh endpoint 互相等待。
//!
pub mod common;
pub mod metrics;
pub mod observability;
pub mod runtime;

#[cfg(feature = "python")]
pub mod py;

#[cfg(test)]
#[path = "../tests/rust/test_support.rs"]
mod test_support;
