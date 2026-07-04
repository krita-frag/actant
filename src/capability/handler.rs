//! `Handler` trait：能力的具体实现。
//!
//! 用户通过 `impl Handler<MyCap> for MyHandler` 提供能力实现。
//! `HandlerAdapter` 包装 `Handler<C>` 为 `ErasedHandler`，使 `Runtime` 可统一存储异构 handler。

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use crate::capability::{Capability, ErasedHandler};
use crate::common::ActantError;

/// 用户实现的 trait：处理特定 [`Capability`] 的请求。
///
/// # 决策型能力（`EffectKind::Ask`）
///
/// `handle` 返回 `Some(resp)` 表示该 handler 产生决策，chain 终止；
/// 返回 `None` 表示放弃决策，由后续 handler 处理。
///
/// # 副作用型能力（`EffectKind::Perform`）
///
/// `handle` 必须返回 `Some(resp)`；返回 `None` 视为内部错误。
///
/// # 反应型能力（`EffectKind::Emit`）
///
/// `Response` 应设为 `()`；`handle` 返回 `Some(())` 即可。
#[async_trait]
pub trait Handler<C: Capability>: Send + Sync {
    /// 处理请求，返回响应。
    async fn handle(&self, req: C::Request) -> Option<C::Response>;
}

/// 包装 `Handler<C>` 为 `ErasedHandler` 的适配器。
///
/// 由于 Rust 不允许 `impl<H, C> ErasedHandler for H`（unconstrained type parameter），
/// 用户在注册 handler 时需显式包装：`Arc::new(HandlerAdapter::new(my_handler))`。
///
/// `Layer::chain` 与 `Runtime::chain` 内部会自动包装，用户通常无需直接使用此类型。
pub struct HandlerAdapter<H, C: Capability> {
    pub handler: H,
    _phantom: std::marker::PhantomData<C>,
}

impl<H, C: Capability> HandlerAdapter<H, C> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<H, C> ErasedHandler for HandlerAdapter<H, C>
where
    H: Handler<C> + 'static,
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    C::Response: Any + Send + Sync,
{
    async fn ask(
        &self,
        req: Arc<dyn Any + Send + Sync>,
    ) -> Option<Box<dyn Any + Send + Sync>> {
        let req_ref = req.downcast_ref::<C::Request>()?;
        let resp = Handler::handle(&self.handler, req_ref.clone()).await?;
        Some(Box::new(resp))
    }

    async fn perform(
        &self,
        req: Arc<dyn Any + Send + Sync>,
    ) -> Result<Box<dyn Any + Send + Sync>, ActantError> {
        let req_ref = req
            .downcast_ref::<C::Request>()
            .ok_or_else(|| ActantError::Internal("perform: request type mismatch".into()))?;
        let resp = Handler::handle(&self.handler, req_ref.clone())
            .await
            .ok_or_else(|| ActantError::Internal("perform: handler returned None".into()))?;
        Ok(Box::new(resp))
    }

    async fn emit(&self, req: Arc<dyn Any + Send + Sync>) -> Result<(), ActantError> {
        let req_ref = req
            .downcast_ref::<C::Request>()
            .ok_or_else(|| ActantError::Internal("emit: request type mismatch".into()))?;
        // Clone 出独立的 req 实例传入，避免 handler 内部修改影响后续 handler
        let _ = Handler::handle(&self.handler, req_ref.clone()).await;
        Ok(())
    }
}

/// 把 `Handler<C>` 包装为 `Arc<dyn ErasedHandler>`，便于注册到 `Layer`。
///
/// 等价于 `Arc::new(HandlerAdapter::new(handler))`，但类型推导更友好。
pub fn erase_handler<H, C>(handler: H) -> Arc<dyn ErasedHandler>
where
    H: Handler<C> + 'static,
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    C::Response: Any + Send + Sync,
{
    Arc::new(HandlerAdapter::new(handler))
}

/// 类型擦除的 handler 列表，对应一个 `Capability` 的所有已注册 handler。
///
/// `Runtime` 内部按 `TypeId` 分桶存储 `HandlerList`。
/// 多 handler 顺序由注册顺序决定（`ask` 时第一个返回 `Some` 的终止 chain）。
pub struct HandlerList {
    /// Capability 元数据（用于错误信息）。
    pub meta: super::CapabilityMeta,
    /// 有序 handler 列表。
    pub handlers: Vec<Arc<dyn ErasedHandler>>,
}

impl HandlerList {
    /// 构造空 handler 列表。
    pub fn new(meta: super::CapabilityMeta) -> Self {
        Self {
            meta,
            handlers: Vec::new(),
        }
    }

    /// 追加一个 handler。
    pub fn push(&mut self, handler: Arc<dyn ErasedHandler>) {
        self.handlers.push(handler);
    }

    /// 返回 handler 数量。
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}
