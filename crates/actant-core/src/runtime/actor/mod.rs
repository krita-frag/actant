//! Actor 运行时 — Rust 核心四盒之一。
//!
//! Actor 是统一执行引擎：DAG、状态机、事件溯源都是 Actor 特例。
//! 本模块只提供系统 Actor 的本地运行时，不做持久化。
//!
//! 类型职责表、子模块结构与定位约束见 `docs/architecture/overview.md`。

pub mod mailbox;
pub mod runtime;
pub mod system;

pub use mailbox::MailboxRegistry;
pub use runtime::{Actor, ActorContext};
pub use system::ActorSystem;

#[cfg(test)]
#[path = "../../../../../tests/rust/unit/runtime/actor.rs"]
mod tests;
