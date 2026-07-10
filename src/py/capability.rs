//! PyO3 桥接：将 Rust `CapabilityRuntime` 暴露给 Python。
//!
//! Python 侧通过 `actant._runtime.Runtime` 包装此 `PyCapabilityRuntime`，
//! 并额外维护 Python handler 注册表（Python callable 无法直接实现 Rust `Handler` trait）。
//!
//! # 设计
//!
//! - `PyCapabilityRuntime`：Rust 强类型 `CapabilityRuntime` 的 PyO3 包装，仅处理 Rust 内置 capability
//! - Python handler 注册：在 Python 侧 `actant._runtime.Runtime` 中用 dict 维护
//! - 调用 `ask/perform/emit` 时，Python Runtime 优先查 Python 注册表，未命中再调用此 Rust Runtime

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::Bound;

use crate::runtime::capability::{
    builtin_capabilities, register_defaults, CapabilityRuntime, EffectKind,
};
use crate::runtime::capability::{
    ActorLifecycle, ActorMessaging, ActorSupervision, Execute, NodeLifecycle, Serialization, Store,
    TaskLifecycle, Transport, WorkflowLifecycle,
};

use super::handler::{chain_python_handler, PythonHandlerRegistry};

/// Rust `CapabilityRuntime` 的 PyO3 包装。
///
/// Python 侧通过 `actant._runtime.Runtime` 持有此对象，用于：
/// - 查询已注册的 Rust 内置 capability
/// - 注册 Rust 实现的 handler（内置模块启动时）
/// - 调用 Rust 内置 capability 的 effect
///
/// Python 用户自定义 handler 不经过此对象，而是在 Python 侧的 `Runtime` 中用 dict 维护。
#[pyclass(name = "_CapabilityRuntime")]
pub struct PyCapabilityRuntime {
    pub(crate) inner: Arc<CapabilityRuntime>,
    pub(crate) tokio: Arc<tokio::runtime::Runtime>,
    registry: PythonHandlerRegistry,
}

impl PyCapabilityRuntime {
    /// 从已存在的统一 Runtime 构造 capability 视图。
    pub(crate) fn from_runtime(
        runtime: &crate::runtime::Runtime,
        tokio: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        let inner = runtime.capability().clone();
        register_defaults(&inner);
        Self {
            inner,
            tokio,
            registry: PythonHandlerRegistry::new(),
        }
    }
}

#[pymethods]
impl PyCapabilityRuntime {
    #[new]
    fn new() -> PyResult<Self> {
        let tokio = Arc::new(
            tokio::runtime::Runtime::new().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to create tokio runtime for capability bridge: {e}"
                ))
            })?,
        );
        let inner = Arc::new(CapabilityRuntime::new());
        register_defaults(&inner);
        Ok(Self {
            inner,
            tokio,
            registry: PythonHandlerRegistry::new(),
        })
    }

    /// 返回所有内置 capability 的元数据列表。
    ///
    /// 每个元素是 `(name: str, kind: str)` 元组，`kind` 为 `"ask"` / `"perform"` / `"emit"`。
    fn builtin_capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for meta in builtin_capabilities() {
            let kind = match meta.default_kind {
                EffectKind::Ask => "ask",
                EffectKind::Perform => "perform",
                EffectKind::Emit => "emit",
            };
            let tuple = (meta.name, kind).into_pyobject(py)?;
            list.append(tuple)?;
        }
        Ok(list)
    }

    /// 返回已注册的 capability 数量。
    #[getter]
    fn capability_count(&self) -> usize {
        self.inner.capability_count()
    }

    /// 返回所有已注册 capability 的名称列表。
    fn registered_capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for meta in self.inner.capabilities() {
            list.append(meta.name)?;
        }
        Ok(list)
    }

    /// 返回指定 capability 的 handler 数量。
    fn handler_count(&self, name: &str) -> usize {
        self.inner
            .capabilities()
            .iter()
            .find(|m| m.name == name)
            .and_then(|m| self.inner.handler_count_by_type_id(m.type_id))
            .unwrap_or(0)
    }

    /// 将 Python callable 注册为指定 capability 的 handler。
    ///
    /// Python handler 会被包装为 `ErasedHandler` 并挂到 Rust Runtime 的 handler 链末尾，
    /// 从而与 Rust 内置 handler 共享同一条分发路径。
    #[pyo3(signature = (name, handler))]
    fn chain_python_handler(&mut self, name: &str, handler: &Bound<'_, PyAny>) -> PyResult<()> {
        let handler = handler.clone().unbind();
        chain_python_handler(&self.inner, &self.registry, name, handler).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "chain_python_handler: capability '{}' not supported",
                name
            ))
        })
    }

    /// 决策型 effect 分发（Rust 内置 handler）。
    fn dispatch_ask<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        dispatch_ask(py, name, request, self.inner.clone(), &self.tokio)
    }

    /// 副作用型 effect 分发（Rust 内置 handler）。
    fn dispatch_perform<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        dispatch_perform(py, name, request, self.inner.clone(), &self.tokio)
    }

    /// 反应型 effect 分发（Rust 内置 handler）。
    fn dispatch_emit(
        &self,
        py: Python<'_>,
        name: &str,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        dispatch_emit(py, name, request, self.inner.clone(), &self.tokio)
    }

    /// 决策型 effect 公开入口。
    #[pyo3(signature = (name, request))]
    fn ask<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        self.dispatch_ask(py, name, request)
    }

    /// 副作用型 effect 公开入口。
    #[pyo3(signature = (name, request))]
    fn perform<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.dispatch_perform(py, name, request)
    }

    /// 反应型 effect 公开入口。
    #[pyo3(signature = (name, request))]
    fn emit(&self, py: Python<'_>, name: &str, request: &Bound<'_, PyAny>) -> PyResult<()> {
        self.dispatch_emit(py, name, request)
    }
}

// 由 `capability_registry!` 生成 `dispatch_ask` / `dispatch_perform` / `dispatch_emit`。
crate::capability_registry! {
    ask: {
        "ActorSupervision" => ActorSupervision => super::types::ActorSupervisionCodec,
    }
    perform: {
        "Serialization" => Serialization => super::types::SerializationCodec,
        "Transport" => Transport => super::types::TransportCodec,
        "Store" => Store => super::types::StoreCodec,
        "Execute" => Execute => super::types::ExecuteCodec,
        "ActorMessaging" => ActorMessaging => super::types::ActorMessagingCodec,
    }
    emit: {
        "TaskLifecycle" => TaskLifecycle => super::types::TaskLifecycleCodec,
        "WorkflowLifecycle" => WorkflowLifecycle => super::types::WorkflowLifecycleCodec,
        "NodeLifecycle" => NodeLifecycle => super::types::NodeLifecycleCodec,
        "ActorLifecycle" => ActorLifecycle => super::types::ActorLifecycleCodec,
    }
}

/// 注册 PyO3 类到模块。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCapabilityRuntime>()?;
    Ok(())
}
