//! `Layer`：handler 链的组合方式。
//!
//! 用户通过 `Layer` 声明一个 capability 的所有 handler，注册到 `Runtime`。
//! `Layer` 支持 `chain` 链式追加（自动用 `HandlerAdapter` 包装），`into_list` 转换为 `HandlerList`。

use std::any::Any;
use std::sync::Arc;

use crate::capability::{
    Capability, CapabilityMeta, EffectKind, ErasedHandler, HandlerAdapter, HandlerList,
};

/// 构建一个 capability 的 handler 链。
///
/// # 示例
///
/// ```ignore
/// use actant::capability::{Layer, builtins::Routing};
/// use async_trait::async_trait;
///
/// struct TagRouter;
/// #[async_trait]
/// impl Handler<Routing> for TagRouter {
///     async fn handle(&self, req: RouteCtx) -> Option<Option<NodeId>> { /* ... */ }
/// }
///
/// let layer = Layer::for_capability::<Routing>("Routing", EffectKind::Ask)
///     .chain(TagRouter);
/// runtime.register(layer);
/// ```
pub struct Layer<C: Capability> {
    meta: CapabilityMeta,
    handlers: Vec<Arc<dyn ErasedHandler>>,
    _phantom: std::marker::PhantomData<C>,
}

impl<C: Capability> Layer<C> {
    /// 构造空 layer，指定 capability 元数据。
    pub fn new(meta: CapabilityMeta) -> Self {
        Self {
            meta,
            handlers: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// 构造空 layer，自动从 `Capability` 类型推导元数据。
    pub fn for_capability(name: &'static str, default_kind: EffectKind) -> Self {
        Self::new(CapabilityMeta::new::<C>(name, default_kind))
    }

    /// 追加一个 `Handler<C>`，自动用 `HandlerAdapter` 包装。
    pub fn chain<H>(mut self, handler: H) -> Self
    where
        H: crate::capability::Handler<C> + 'static,
        C::Request: Any + Send + Sync + Clone,
        C::Response: Any + Send + Sync,
    {
        self.handlers.push(Arc::new(HandlerAdapter::new(handler)));
        self
    }

    /// 追加一个已擦除的 handler（用于 PyO3 trampoline 注入）。
    pub fn chain_erased(mut self, handler: Arc<dyn ErasedHandler>) -> Self {
        self.handlers.push(handler);
        self
    }

    /// 返回 handler 数量。
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// 消费 `Layer`，转换为 `HandlerList`。
    pub fn into_list(self) -> HandlerList {
        HandlerList {
            meta: self.meta,
            handlers: self.handlers,
        }
    }

    /// 返回 capability 元数据。
    pub fn meta(&self) -> &CapabilityMeta {
        &self.meta
    }
}
