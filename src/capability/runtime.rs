//! `Runtime`：注册表 + dispatcher，统一调度所有 capability 的 effect 请求。
//!
//! `Runtime` 提供 `ask` / `perform` / `emit` 三种执行模型，对应 ADR 0001 的三种 Effect 类型。

use std::any::{Any, TypeId};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{debug, warn};

use crate::capability::{
    Capability, CapabilityMeta, ErasedHandler, HandlerList, Layer,
};
use crate::common::ActantError;

/// Capability 注册表 + effect dispatcher。
///
/// `Runtime` 是 actant 的统一运行时入口。所有扩展点（Routing、Scheduling、Transport、
/// Store、Actor、Lifecycle 等）都注册为 `Layer`，由 `Runtime` 按 `TypeId` 分桶存储。
///
/// # 线程安全
///
/// `Runtime` 内部使用 `DashMap` 存储 handler 列表，支持并发注册与查询。
/// `Arc<Runtime>` 可安全共享于多线程。
pub struct Runtime {
    /// `TypeId -> HandlerList` 注册表。
    layers: DashMap<TypeId, HandlerList>,
    /// Capability 元数据缓存（用于错误信息与 PyO3 反射）。
    metas: RwLock<Vec<CapabilityMeta>>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    /// 构造空 Runtime。
    pub fn new() -> Self {
        Self {
            layers: DashMap::new(),
            metas: RwLock::new(Vec::new()),
        }
    }

    /// 注册一个 `Layer`，替换该 capability 的所有 handler。
    ///
    /// 若该 capability 已有 handler，会被完全替换（不追加）。
    /// 若需要追加，请用 `chain`。
    pub fn register<C: Capability>(&self, layer: Layer<C>) {
        let type_id = layer.meta().type_id;
        debug!(
            capability = layer.meta().name,
            handlers = layer.len(),
            "registering capability layer"
        );
        self.metas.write().push(layer.meta().clone());
        self.layers.insert(type_id, layer.into_list());
    }

    /// 向已存在的 capability 追加单个 handler。
    ///
    /// 若 capability 未注册，返回 `Err`。
    pub fn chain<C: Capability>(
        &self,
        handler: Arc<dyn ErasedHandler>,
    ) -> Result<(), ActantError> {
        let type_id = TypeId::of::<C>();
        let mut entry = self.layers.get_mut(&type_id).ok_or_else(|| {
            ActantError::Internal(format!(
                "capability {:?} not registered",
                std::any::type_name::<C>()
            ))
        })?;
        entry.push(handler);
        Ok(())
    }

    /// 决策型 effect（ask）：逆序调用 handler 链（后注册=高优先级），第一个返回 `Some` 的决定结果。
    ///
    /// 语义：后注册的 handler 优先决策，使其能覆盖默认 handler。
    /// 若所有 handler 都返回 `None`，返回 `Ok(None)`。
    /// 若 capability 未注册，返回 `Err`。
    pub async fn ask<C: Capability>(&self, req: C::Request) -> Result<Option<C::Response>, ActantError>
    where
        C::Request: Any + Clone,
        C::Response: Any,
    {
        let type_id = TypeId::of::<C>();
        let entry = self.layers.get(&type_id).ok_or_else(|| {
            ActantError::Internal(format!(
                "ask: capability {:?} not registered",
                std::any::type_name::<C>()
            ))
        })?;
        let handlers: Vec<Arc<dyn ErasedHandler>> = entry.handlers.iter().cloned().collect();
        drop(entry);

        let shared_req: Arc<dyn Any + Send + Sync> = Arc::new(req);
        // 逆序：后注册的 handler 优先决策（用户自定义覆盖默认）
        for handler in handlers.iter().rev() {
            if let Some(resp) = handler.ask(Arc::clone(&shared_req)).await {
                let resp = resp
                    .downcast::<C::Response>()
                    .map_err(|_| ActantError::Internal("ask: response type mismatch".into()))?;
                return Ok(Some(*resp));
            }
        }
        Ok(None)
    }

    /// 副作用型 effect（perform）：调用最后注册的 handler（高优先级），结果直接返回。
    ///
    /// 语义：后注册的 handler 覆盖默认。若 capability 未注册或 handler 返回 `None`，返回 `Err`。
    pub async fn perform<C: Capability>(
        &self,
        req: C::Request,
    ) -> Result<C::Response, ActantError>
    where
        C::Request: Any + Clone,
        C::Response: Any,
    {
        let type_id = TypeId::of::<C>();
        let entry = self.layers.get(&type_id).ok_or_else(|| {
            ActantError::Internal(format!(
                "perform: capability {:?} not registered",
                std::any::type_name::<C>()
            ))
        })?;
        let handlers: Vec<Arc<dyn ErasedHandler>> = entry.handlers.iter().cloned().collect();
        drop(entry);

        if handlers.is_empty() {
            return Err(ActantError::Internal(format!(
                "perform: capability {:?} has no handlers",
                std::any::type_name::<C>()
            )));
        }
        let shared_req: Arc<dyn Any + Send + Sync> = Arc::new(req);
        // 取最后一个 handler（后注册=高优先级，覆盖默认）
        let last = handlers.last().unwrap();
        let resp = last.perform(Arc::clone(&shared_req)).await?;
        let resp = resp
            .downcast::<C::Response>()
            .map_err(|_| ActantError::Internal("perform: response type mismatch".into()))?;
        Ok(*resp)
    }

    /// 反应型 effect（emit）：在所有 handler 上执行，无返回值。
    ///
    /// 若 capability 未注册，返回 `Err`。
    pub async fn emit<C: Capability>(&self, req: C::Request) -> Result<(), ActantError>
    where
        C::Request: Any + Clone,
    {
        let type_id = TypeId::of::<C>();
        let entry = self.layers.get(&type_id).ok_or_else(|| {
            ActantError::Internal(format!(
                "emit: capability {:?} not registered",
                std::any::type_name::<C>()
            ))
        })?;
        let handlers: Vec<Arc<dyn ErasedHandler>> = entry.handlers.iter().cloned().collect();
        drop(entry);

        let shared_req: Arc<dyn Any + Send + Sync> = Arc::new(req);
        // 顺序执行所有 handler；任一失败记录日志但继续执行后续 handler
        // （反应型语义：一个 handler 失败不应阻塞其他订阅者）
        for handler in &handlers {
            if let Err(e) = handler.emit(Arc::clone(&shared_req)).await {
                warn!(error = %e, "emit handler failed, continuing");
            }
        }
        Ok(())
    }

    /// 返回已注册的 capability 数量。
    pub fn capability_count(&self) -> usize {
        self.layers.len()
    }

    /// 返回指定 capability 的 handler 数量。
    pub fn handler_count<C: Capability>(&self) -> usize {
        self.layers
            .get(&TypeId::of::<C>())
            .map(|e| e.handlers.len())
            .unwrap_or(0)
    }

    /// 返回所有已注册 capability 的元数据（用于 PyO3 反射）。
    pub fn capabilities(&self) -> Vec<CapabilityMeta> {
        self.metas.read().clone()
    }
}
