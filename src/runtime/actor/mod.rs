//! Actor 运行时 — Rust 核心四盒之一。
//!
//! Actor 是统一执行引擎：DAG、状态机、事件溯源都是
//! Actor 特例。本模块是 Actor 运行时的唯一入口，职责边界如下：
//!
//! | 类型 | 职责 |
//! |------|------|
//! | `Actor` trait | 用户/系统 Actor 实现的唯一抽象 |
//! | `ActorContext` | Actor 实例生命周期上下文 |
//! | `MailboxRegistry` | Actor 邮箱注册表与消息持久化 |
//! | `ActorPersistence` | Actor 状态检查点 + WAL |
//! | `ActorSystem` | 对外 facade：spawn / send / call / stop |
//!
//! 子模块结构：
//! - [`runtime`]：`Actor` trait + `ActorContext`
//! - [`mailbox`]：`MailboxRegistry` + 持久化待发消息
//! - [`persistence`]：`ActorPersistence`
//! - [`system`]：`ActorSystem` facade + `RunningActor` 私有循环

pub mod mailbox;
pub mod persistence;
pub mod runtime;
pub mod system;

pub use mailbox::MailboxRegistry;
pub use persistence::ActorPersistence;
pub use runtime::{Actor, ActorContext};
pub use system::ActorSystem;

#[cfg(test)]
#[path = "../../../tests/rust/unit/runtime/actor.rs"]
mod tests;
