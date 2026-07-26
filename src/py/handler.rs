//! Python callable 注册为 Rust `CapabilityRuntime` handler。
//!
//! 本模块把 Python handler 包装成 `ErasedHandler`，通过 `Runtime::chain`
//! 挂到 Rust 内置 capability 的 handler 链末尾。这样 Python 和 Rust handler
//! 共享同一条分发路径，消除 Python 侧独立维护注册表的双分发。

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use parking_lot::Mutex;
use pyo3::prelude::*;

use crate::common::ActantError;
use crate::runtime::capability::{
    ActorLifecycle, ActorMessaging, ActorSupervision, Capability, CapabilityRuntime, ErasedHandler,
    Execute, NodeLifecycle, Serialization, Store, TaskLifecycle, Transport, WorkflowLifecycle,
};

use super::types::{
    ActorLifecycleCodec, ActorMessagingCodec, ActorSupervisionCodec, ExecuteCodec,
    NodeLifecycleCodec, PyHandlerAskCodec, PyHandlerEmitCodec, PyHandlerPerformCodec,
    SerializationCodec, StoreCodec, TaskLifecycleCodec, TransportCodec, WorkflowLifecycleCodec,
};

/// Ask effect 的 Python handler 包装。
pub struct PyAskHandler<C, Codec> {
    handler: Mutex<Py<PyAny>>,
    _phantom: std::marker::PhantomData<(C, Codec)>,
}

impl<C, Codec> PyAskHandler<C, Codec> {
    fn new(handler: Py<PyAny>) -> Self {
        Self {
            handler: Mutex::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<C, Codec> ErasedHandler for PyAskHandler<C, Codec>
where
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    C::Response: Any + Send + Sync,
    Codec: PyHandlerAskCodec<C> + Send + Sync + 'static,
{
    async fn ask(&self, req: Arc<dyn Any + Send + Sync>) -> Option<Box<dyn Any + Send + Sync>> {
        let req = req.downcast_ref::<C::Request>()?.clone();
        // 在 GIL 下快速 clone handler 引用；后续 Python 调用放到 spawn_blocking，
        // 避免阻塞 tokio worker 线程。
        let handler = Python::attach(|py| self.handler.lock().clone_ref(py));

        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let py_req = Codec::encode_request(py, &req).ok()?;
                let t0 = Instant::now();
                let py_resp = handler.call1(py, (&py_req,)).ok()?.into_bound(py);
                crate::metrics::observe_python_handler_ms(t0.elapsed().as_millis() as u64);
                if py_resp.is_none() {
                    return None;
                }
                let resp = Codec::decode_response(py, &py_resp).ok()??;
                Some(Box::new(resp) as Box<dyn Any + Send + Sync>)
            })
        })
        .await
        .ok()?
    }

    async fn perform(
        &self,
        _req: Arc<dyn Any + Send + Sync>,
    ) -> Result<Box<dyn Any + Send + Sync>, ActantError> {
        Err(ActantError::Internal(
            "python ask handler does not support perform".into(),
        ))
    }

    async fn emit(&self, _req: Arc<dyn Any + Send + Sync>) -> Result<(), ActantError> {
        Ok(())
    }
}

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
                crate::metrics::observe_python_handler_ms(t0.elapsed().as_millis() as u64);
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
                crate::metrics::observe_python_handler_ms(t0.elapsed().as_millis() as u64);
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

/// 把具体 capability 和编解码器映射到工厂实现。
struct AskFactory<C, Codec>(std::marker::PhantomData<(C, Codec)>);

impl<C, Codec> AskFactory<C, Codec> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<C, Codec> PyHandlerFactory for AskFactory<C, Codec>
where
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    C::Response: Any + Send + Sync,
    Codec: PyHandlerAskCodec<C> + Send + Sync + 'static,
{
    fn create(&self, handler: Py<PyAny>) -> Arc<dyn ErasedHandler> {
        Arc::new(PyAskHandler::<C, Codec>::new(handler))
    }
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
            "ActorSupervision".to_string(),
            Box::new(AskFactory::<ActorSupervision, ActorSupervisionCodec>::new()),
        );
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
            "Execute".to_string(),
            Box::new(PerformFactory::<Execute, ExecuteCodec>::new()),
        );
        factories.insert(
            "ActorMessaging".to_string(),
            Box::new(PerformFactory::<ActorMessaging, ActorMessagingCodec>::new()),
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
        factories.insert(
            "ActorLifecycle".to_string(),
            Box::new(EmitFactory::<ActorLifecycle, ActorLifecycleCodec>::new()),
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
        "ActorSupervision" => runtime.chain::<ActorSupervision>(handler),
        "Serialization" => runtime.chain::<Serialization>(handler),
        "Transport" => runtime.chain::<Transport>(handler),
        "Store" => runtime.chain::<Store>(handler),
        "Execute" => runtime.chain::<Execute>(handler),
        "ActorMessaging" => runtime.chain::<ActorMessaging>(handler),
        "TaskLifecycle" => runtime.chain::<TaskLifecycle>(handler),
        "WorkflowLifecycle" => runtime.chain::<WorkflowLifecycle>(handler),
        "NodeLifecycle" => runtime.chain::<NodeLifecycle>(handler),
        "ActorLifecycle" => runtime.chain::<ActorLifecycle>(handler),
        _ => Err(ActantError::Internal(format!(
            "chain: unknown capability {}",
            name
        ))),
    }
}
