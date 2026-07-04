//! Capability-Resource-Handler 统一扩展架构（ADR 0001）。
//!
//! 本模块定义了 actant 的统一扩展抽象：
//! - [`Capability`]：能力的声明（"能做什么"），关联 Request/Response 类型
//! - [`Handler`]：能力的具体实现
//! - [`Layer`]：handler 链，支持多 handler 叠加
//! - [`Runtime`]：注册表 + dispatcher，提供 `ask`/`perform`/`emit` 三种执行模型
//!
//! # 三种 Effect 类型
//!
//! | 类型   | 语义               | 执行模型                                 |
//! |--------|--------------------|------------------------------------------|
//! | `ask`  | 决策型，请求返回值 | chain 中第一个返回 `Some` 的 handler 决定结果 |
//! | `perform` | 副作用型，执行 Rust trait | 单 handler 执行，结果直接返回       |
//! | `emit` | 反应型，触发订阅   | 所有 handler 顺序执行，无返回值          |

use std::any::TypeId;
use std::sync::Arc;

use async_trait::async_trait;

use crate::common::ActantError;

pub mod builtins;
pub mod default_handlers;
pub mod handler;
pub mod layer;
pub mod runtime;

pub use builtins::*;
pub use handler::{Handler, HandlerAdapter, HandlerList, erase_handler};
pub use layer::Layer;
pub use runtime::Runtime;

/// 能力的声明 trait。
///
/// 一个 `Capability` 实现声明"能做什么"，但不包含具体实现。具体实现由 [`Handler`] 提供。
///
/// 关联类型：
/// - `Request`：调用方传递给 handler 的请求 payload
/// - `Response`：handler 返回的响应类型
///
/// # 设计约束
///
/// - `Request` 与 `Response` 必须满足 `Send + Sync + 'static`，以支持跨线程调度。
/// - `Capability` 本身只是类型标签，不需要携带数据；实例化由 `Runtime` 通过 `TypeId` 完成。
///
/// # 用户自定义能力
///
/// 用户可在 Rust 侧或 Python 侧声明新能力。Rust 侧直接 `impl Capability for MyCap`，
/// Python 侧通过 `actant.Capability` Protocol 声明，由 PyO3 trampoline 桥接为 Rust trait object。
pub trait Capability: Send + Sync + 'static {
    /// 请求 payload 类型。
    type Request: Send + Sync + 'static;
    /// 响应类型。反应型能力（emit）应设为 `()`。
    type Response: Send + Sync + 'static;
}

/// 内部辅助 trait：将具体 `Capability` 类型擦除为 `dyn ErasedHandler`。
///
/// 这是 [`Handler<C>`] 的对象安全封装。`Runtime` 通过 `Box<dyn ErasedHandler>`
/// 存储异构 handler 集合，按 `TypeId` 分桶。
///
/// 不直接对外暴露，用户应实现 [`Handler`] trait，由 blanket impl 自动生成 trampoline。
///
/// # 请求共享
///
/// 请求通过 `Arc<dyn Any + Send + Sync>` 共享，使 `emit` 中多个 handler 可访问同一份数据。
/// handler 内部通过 `downcast_ref::<C::Request>()` 取值并 `clone` 后传入 `Handler::handle`。
#[async_trait]
pub trait ErasedHandler: Send + Sync {
    /// 决策型（ask）：返回 `Some(resp)` 表示该 handler 产生决策；返回 `None` 表示放弃决策。
    async fn ask(
        &self,
        req: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Option<Box<dyn std::any::Any + Send + Sync>>;

    /// 副作用型（perform）：单 handler 执行，直接返回结果。
    async fn perform(
        &self,
        req: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync>, ActantError>;

    /// 反应型（emit）：在所有 handler 上执行，无返回值。
    async fn emit(&self, req: Arc<dyn std::any::Any + Send + Sync>) -> Result<(), ActantError>;
}

/// Capability 的元数据（运行时类型信息）。
///
/// 由 `Runtime` 内部使用，用于按 `TypeId` 分桶与查找。
#[derive(Clone)]
pub struct CapabilityMeta {
    /// Capability 的 `TypeId`，作为注册表 key。
    pub type_id: TypeId,
    /// Capability 的可读名称（用于错误信息与日志）。
    pub name: &'static str,
    /// Effect 语义：`"ask"` / `"perform"` / `"emit"`。
    ///
    /// 决定 `Runtime::dispatch` 默认调用 `ErasedHandler` 的哪个方法。
    pub default_kind: EffectKind,
}

/// Effect 执行语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EffectKind {
    /// 决策型：chain 中第一个返回 `Some` 的 handler 决定结果。
    Ask,
    /// 副作用型：单 handler 执行，结果直接返回。
    Perform,
    /// 反应型：所有 handler 顺序执行，无返回值。
    Emit,
}

impl CapabilityMeta {
    /// 构造 capability 元数据。
    pub fn new<C: Capability>(name: &'static str, default_kind: EffectKind) -> Self {
        Self {
            type_id: TypeId::of::<C>(),
            name,
            default_kind,
        }
    }
}
