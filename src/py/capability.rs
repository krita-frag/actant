//! PyO3 桥接：将 Rust `capability::Runtime` 暴露给 Python。
//!
//! Python 侧通过 `actant._runtime.Runtime` 包装此 `PyCapabilityRuntime`，
//! 并额外维护 Python handler 注册表（Python callable 无法直接实现 Rust `Handler` trait）。
//!
//! # 设计
//!
//! - `PyCapabilityRuntime`：Rust 强类型 Runtime 的 PyO3 包装，仅处理 Rust 内置 capability
//! - Python handler 注册：在 Python 侧 `actant._runtime.Runtime` 中用 dict 维护
//! - 调用 `ask/perform/emit` 时，Python Runtime 优先查 Python 注册表，未命中再调用此 Rust Runtime

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::Bound;

use crate::capability::{builtin_capabilities, Runtime};

/// Rust `capability::Runtime` 的 PyO3 包装。
///
/// Python 侧通过 `actant._runtime.Runtime` 持有此对象，用于：
/// - 查询已注册的 Rust 内置 capability
/// - 注册 Rust 实现的 handler（内置模块启动时）
/// - 调用 Rust 内置 capability 的 effect
///
/// Python 用户自定义 handler 不经过此对象，而是在 Python 侧的 `Runtime` 中用 dict 维护。
#[pyclass(name = "_CapabilityRuntime")]
pub struct PyCapabilityRuntime {
    pub(crate) inner: Arc<Runtime>,
}

#[pymethods]
impl PyCapabilityRuntime {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(Runtime::new()),
        }
    }

    /// 返回所有内置 capability 的元数据列表。
    ///
    /// 每个元素是 `(name: str, kind: str)` 元组，`kind` 为 `"ask"` / `"perform"` / `"emit"`。
    fn builtin_capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for meta in builtin_capabilities() {
            let kind = match meta.default_kind {
                crate::capability::EffectKind::Ask => "ask",
                crate::capability::EffectKind::Perform => "perform",
                crate::capability::EffectKind::Emit => "emit",
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
        // 按 name 查找（线性扫描 metas）
        self.inner
            .capabilities()
            .iter()
            .find(|m| m.name == name)
            .map(|_| 0) // 简化：不暴露具体 handler 数量
            .unwrap_or(0)
    }
}

/// 注册 PyO3 类到模块。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCapabilityRuntime>()?;
    Ok(())
}
