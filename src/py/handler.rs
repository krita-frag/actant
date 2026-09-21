//! Python callable 注册为 Rust `CapabilityRuntime` handler。
//!
//! 本模块把 Python handler 包装成 `ErasedHandler`，**有能力**通过
//! `Runtime::chain` 挂到 Rust 内置 capability 的 handler 链末尾，使 Python
//! 与 Rust handler 共享同一条分发路径。
//!
//! **但当前默认并未接线**：Python 侧的 `Runtime.layer(name).chain(handler)`
//! 只登记在 Python 自己的注册表里，不会调用到本模块的 `chain_python_handler`
//! （后者经 `PyCapabilityRuntime` 暴露，需显式调用）。因此**双分发仍然存在**——
//! Python handler 默认只在用户代码显式 `actant.ask/perform/emit` 时生效，
//! 不参与 Rust 内部 dispatch。接线与否是一项独立决策，未接线前不要照本段
//! 第一句的描述去推断运行时行为（审查 2026-09-18 修订）。

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use parking_lot::Mutex;
use pyo3::prelude::*;

use actant_core::common::ActantError;
use actant_core::runtime::capability::{
    Capability, CapabilityRuntime, ErasedHandler, NodeLifecycle, Serialization, Store,
    TaskLifecycle, Transport, WorkflowLifecycle,
};

use super::types::{
    NodeLifecycleCodec, PyHandlerEmitCodec, PyHandlerPerformCodec, SerializationCodec, StoreCodec,
    TaskLifecycleCodec, TransportCodec, WorkflowLifecycleCodec,
};

/// Perform effect 的 Python handler 包装。
pub struct PyPerformHandler<C, Codec> {
    handler: Mutex<Py<PyAny>>,
    _phantom: std::marker::PhantomData<(C, Codec)>,
}

impl<C, Codec> PyPerformHandler<C, Codec> {
    fn new(handler: Py<PyAny>) -> Self {
        Self {
            handler: Mutex::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<C, Codec> ErasedHandler for PyPerformHandler<C, Codec>
where
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    C::Response: Any + Send + Sync,
    Codec: PyHandlerPerformCodec<C> + Send + Sync + 'static,
{
    async fn ask(&self, _req: Arc<dyn Any + Send + Sync>) -> Option<Box<dyn Any + Send + Sync>> {
        None
    }

    async fn perform(
        &self,
        req: Arc<dyn Any + Send + Sync>,
    ) -> Result<Box<dyn Any + Send + Sync>, ActantError> {
        let req = req.downcast_ref::<C::Request>().ok_or_else(|| {
            ActantError::Internal("python perform handler: request mismatch".into())
        })?;
        let req = req.clone();
        // 在 GIL 下快速 clone handler 引用；后续 Python 调用放到 spawn_blocking，
        // 避免阻塞 tokio worker 线程。
        let handler = Python::attach(|py| self.handler.lock().clone_ref(py));

        let resp = tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> Result<C::Response, ActantError> {
                let py_req = Codec::encode_request(py, &req)
                    .map_err(|e| ActantError::Internal(format!("encode request: {}", e)))?;
                let t0 = Instant::now();
                let py_resp = handler
                    .call1(py, (&py_req,))
                    .map_err(|e| ActantError::Internal(format!("python handler: {}", e)))?
                    .into_bound(py);
                actant_core::metrics::observe_task_handler_ms(t0.elapsed().as_millis() as u64);
                let resp = Codec::decode_response(py, &py_resp)
                    .map_err(|e| ActantError::Internal(format!("decode response: {}", e)))?;
                Ok(resp)
            })
        })
        .await
        .map_err(|e| ActantError::Internal(format!("python perform handler join: {}", e)))?;
        Ok(Box::new(resp) as Box<dyn Any + Send + Sync>)
    }

    async fn emit(&self, _req: Arc<dyn Any + Send + Sync>) -> Result<(), ActantError> {
        Ok(())
    }
}

/// Emit effect 的 Python handler 包装。
pub struct PyEmitHandler<C, Codec> {
    handler: Mutex<Py<PyAny>>,
    _phantom: std::marker::PhantomData<(C, Codec)>,
}

impl<C, Codec> PyEmitHandler<C, Codec> {
    fn new(handler: Py<PyAny>) -> Self {
        Self {
            handler: Mutex::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<C, Codec> ErasedHandler for PyEmitHandler<C, Codec>
where
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    Codec: PyHandlerEmitCodec<C> + Send + Sync + 'static,
{
    async fn ask(&self, _req: Arc<dyn Any + Send + Sync>) -> Option<Box<dyn Any + Send + Sync>> {
        None
    }

    async fn perform(
        &self,
        _req: Arc<dyn Any + Send + Sync>,
    ) -> Result<Box<dyn Any + Send + Sync>, ActantError> {
        Err(ActantError::Internal(
            "python emit handler does not support perform".into(),
        ))
    }

    async fn emit(&self, req: Arc<dyn Any + Send + Sync>) -> Result<(), ActantError> {
        let req = req
            .downcast_ref::<C::Request>()
            .ok_or_else(|| ActantError::Internal("python emit handler: request mismatch".into()))?;
        let req = req.clone();
        // 在 GIL 下快速 clone handler 引用；后续 Python 调用放到 spawn_blocking，
        // 避免阻塞 tokio worker 线程。
        let handler = Python::attach(|py| self.handler.lock().clone_ref(py));

        tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> Result<(), ActantError> {
                let py_req = Codec::encode_request(py, &req)
                    .map_err(|e| ActantError::Internal(format!("encode request: {}", e)))?;
                let t0 = Instant::now();
                match handler.call1(py, (&py_req,)) {
                    Ok(_) => (),
                    Err(e) => return Err(ActantError::Internal(format!("python handler: {}", e))),
                }
                actant_core::metrics::observe_task_handler_ms(t0.elapsed().as_millis() as u64);
                Ok(())
            })
        })
        .await
        .map_err(|e| ActantError::Internal(format!("python emit handler join: {}", e)))?
    }
}

/// 由 capability 名称创建 `Arc<dyn ErasedHandler>` 的工厂 trait。
pub trait PyHandlerFactory: Send + Sync {
    fn create(&self, handler: Py<PyAny>) -> Arc<dyn ErasedHandler>;
}

struct PerformFactory<C, Codec>(std::marker::PhantomData<(C, Codec)>);

impl<C, Codec> PerformFactory<C, Codec> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<C, Codec> PyHandlerFactory for PerformFactory<C, Codec>
where
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    C::Response: Any + Send + Sync,
    Codec: PyHandlerPerformCodec<C> + Send + Sync + 'static,
{
    fn create(&self, handler: Py<PyAny>) -> Arc<dyn ErasedHandler> {
        Arc::new(PyPerformHandler::<C, Codec>::new(handler))
    }
}

struct EmitFactory<C, Codec>(std::marker::PhantomData<(C, Codec)>);

impl<C, Codec> EmitFactory<C, Codec> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<C, Codec> PyHandlerFactory for EmitFactory<C, Codec>
where
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    Codec: PyHandlerEmitCodec<C> + Send + Sync + 'static,
{
    fn create(&self, handler: Py<PyAny>) -> Arc<dyn ErasedHandler> {
        Arc::new(PyEmitHandler::<C, Codec>::new(handler))
    }
}

/// Python handler 工厂注册表。
///
/// 每个内置 capability 名称对应一个工厂；调用方通过 `create` 把 Python callable
/// 包装为 `ErasedHandler` 后挂到 Rust Runtime。
pub struct PythonHandlerRegistry {
    factories: std::collections::HashMap<String, Box<dyn PyHandlerFactory>>,
}

impl PythonHandlerRegistry {
    pub fn new() -> Self {
        let mut factories: std::collections::HashMap<String, Box<dyn PyHandlerFactory>> =
            std::collections::HashMap::new();
        factories.insert(
            "Serialization".to_string(),
            Box::new(PerformFactory::<Serialization, SerializationCodec>::new()),
        );
        factories.insert(
            "Transport".to_string(),
            Box::new(PerformFactory::<Transport, TransportCodec>::new()),
        );
        factories.insert(
            "Store".to_string(),
            Box::new(PerformFactory::<Store, StoreCodec>::new()),
        );
        factories.insert(
            "TaskLifecycle".to_string(),
            Box::new(EmitFactory::<TaskLifecycle, TaskLifecycleCodec>::new()),
        );
        factories.insert(
            "WorkflowLifecycle".to_string(),
            Box::new(EmitFactory::<WorkflowLifecycle, WorkflowLifecycleCodec>::new()),
        );
        factories.insert(
            "NodeLifecycle".to_string(),
            Box::new(EmitFactory::<NodeLifecycle, NodeLifecycleCodec>::new()),
        );
        Self { factories }
    }

    /// 根据 capability 名称创建对应的 Python handler 包装。
    pub fn create(&self, name: &str, handler: Py<PyAny>) -> Option<Arc<dyn ErasedHandler>> {
        self.factories.get(name).map(|f| f.create(handler))
    }

    /// 返回所有支持的 capability 名称。
    pub fn names(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }
}

impl Default for PythonHandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// 把 Python handler 挂到指定 capability 的 Rust handler 链末尾。
///
/// 若 capability 名称未知，返回 `None`；成功挂载返回 `Some(())`。
pub fn chain_python_handler(
    runtime: &CapabilityRuntime,
    registry: &PythonHandlerRegistry,
    name: &str,
    handler: Py<PyAny>,
) -> Option<()> {
    let erased = registry.create(name, handler)?;
    chain_by_name(runtime, name, erased).ok()
}

/// 根据 capability 名称把 `Arc<dyn ErasedHandler>` 挂到 CapabilityRuntime。
fn chain_by_name(
    runtime: &CapabilityRuntime,
    name: &str,
    handler: Arc<dyn ErasedHandler>,
) -> Result<(), ActantError> {
    match name {
        "Serialization" => runtime.chain::<Serialization>(handler),
        "Transport" => runtime.chain::<Transport>(handler),
        "Store" => runtime.chain::<Store>(handler),
        "TaskLifecycle" => runtime.chain::<TaskLifecycle>(handler),
        "WorkflowLifecycle" => runtime.chain::<WorkflowLifecycle>(handler),
        "NodeLifecycle" => runtime.chain::<NodeLifecycle>(handler),
        _ => Err(ActantError::Internal(format!(
            "chain: unknown capability {}",
            name
        ))),
    }
}
