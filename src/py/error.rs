use pyo3::exceptions::{PyBaseException, PyRuntimeError};
use pyo3::prelude::*;
use std::sync::OnceLock;

use crate::common::ActantError;

// 缓存 Python 端异常类对象（在 register_exceptions 中初始化）。
//
// Python 异常类对象经 OnceLock 缓存为单例（而非 create_exception! 派生独立类型），
// 中 Python 定义的类形成两套独立继承链，导致 Rust 抛出的异常无法被
// `except actant.ActantError` 捕获。改为缓存 Python 端的类对象，使
// `impl From<ActantError> for PyErr` 抛出的就是 Python 端的类。
static EXC_ACTANT: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_STORAGE: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_NETWORK: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_SERIALIZATION: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_ACTOR: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_WORKFLOW: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_TASK: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_WORKER: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_CONFIG: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_METRICS: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_NOT_FOUND: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_ALREADY_EXISTS: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_TIMEOUT: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_CANCELLED: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_INVALID_STATE: OnceLock<Py<PyAny>> = OnceLock::new();
static EXC_INTERNAL: OnceLock<Py<PyAny>> = OnceLock::new();

/// 用缓存的 Python 异常类构造 `PyErr`。
///
/// 若缓存未初始化（`register_exceptions` 未调用）或构造失败，
/// 回退到 `PyRuntimeError`，保证不 panic。
fn make_pyerr(cls: &OnceLock<Py<PyAny>>, message: &str) -> PyErr {
    let Some(cls) = cls.get() else {
        return PyRuntimeError::new_err(message.to_string());
    };
    Python::attach(|py| {
        let cls_bound = cls.bind(py);
        match cls_bound.call1((message,)) {
            Ok(instance) => match instance.cast_into::<PyBaseException>() {
                // PyO3 0.29: `cast_into` 消耗 self 返回 owned Bound<PyBaseException>，
                // 通过 `into_any()` upcast 到 Bound<PyAny> 后交给 PyErr::from_value。
                Ok(exc) => PyErr::from_value(exc.into_any()),
                Err(_) => PyRuntimeError::new_err(message.to_string()),
            },
            Err(_) => PyRuntimeError::new_err(message.to_string()),
        }
    })
}

/// 将 Rust `ActantError` 转换为对应的 Python 异常类型。
///
/// 使用 `register_exceptions` 中缓存的 Python 端异常类构造 `PyErr`，
/// 确保 Rust 抛出的异常与 Python 用户 `except` 的类是同一个，
/// 跨语言边界保留错误类型信息。
impl From<ActantError> for PyErr {
    fn from(err: ActantError) -> Self {
        let message = err.to_string();
        match &err {
            ActantError::Storage(_) | ActantError::StorageIo(_) | ActantError::Heed(_) => {
                make_pyerr(&EXC_STORAGE, &message)
            }
            ActantError::Network(_) => make_pyerr(&EXC_NETWORK, &message),
            ActantError::Serialization(_) | ActantError::Postcard(_) => {
                make_pyerr(&EXC_SERIALIZATION, &message)
            }
            ActantError::Actor(_) => make_pyerr(&EXC_ACTOR, &message),
            ActantError::Workflow(_) => make_pyerr(&EXC_WORKFLOW, &message),
            ActantError::Task(_) => make_pyerr(&EXC_TASK, &message),
            ActantError::Worker(_) => make_pyerr(&EXC_WORKER, &message),
            ActantError::Config(_) => make_pyerr(&EXC_CONFIG, &message),
            ActantError::Metrics(_) => make_pyerr(&EXC_METRICS, &message),
            ActantError::NotFound(_) => make_pyerr(&EXC_NOT_FOUND, &message),
            ActantError::AlreadyExists(_) => make_pyerr(&EXC_ALREADY_EXISTS, &message),
            ActantError::Timeout(_) => make_pyerr(&EXC_TIMEOUT, &message),
            ActantError::Cancelled(_) => make_pyerr(&EXC_CANCELLED, &message),
            ActantError::InvalidState(_) => make_pyerr(&EXC_INVALID_STATE, &message),
            ActantError::Internal(_) => make_pyerr(&EXC_INTERNAL, &message),
        }
    }
}

/// 在 Python 模块上注册所有异常类。
///
/// 从 `actant.exceptions` 导入 Python 端定义的异常类（而非用 `create_exception!`
/// 创建独立类），确保 Rust 抛出的异常与 Python 用户 `except` 的类是同一个。
/// 同时缓存类对象供 `impl From<ActantError> for PyErr` 使用。
pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let exc_mod = py.import(pyo3::intern!(py, "actant.exceptions"))?;

    macro_rules! reg {
        ($lock:ident, $name:literal) => {{
            let cls: Py<PyAny> = exc_mod.getattr(pyo3::intern!(py, $name))?.into();
            // PyO3 0.29 中 `Py<T>` 不再直接 impl Clone；通过 `clone_ref(py)`
            // 显式克隆引用，一份给 OnceLock 缓存，一份注册到模块。
            let _ = $lock.set(cls.clone_ref(py));
            m.add($name, cls)?;
        }};
    }

    reg!(EXC_ACTANT, "ActantError");
    reg!(EXC_STORAGE, "StorageError");
    reg!(EXC_NETWORK, "NetworkError");
    reg!(EXC_SERIALIZATION, "SerializationError");
    reg!(EXC_ACTOR, "ActorError");
    reg!(EXC_WORKFLOW, "WorkflowError");
    reg!(EXC_TASK, "TaskError");
    reg!(EXC_WORKER, "WorkerError");
    reg!(EXC_CONFIG, "ConfigError");
    reg!(EXC_METRICS, "MetricsError");
    reg!(EXC_NOT_FOUND, "NotFoundError");
    reg!(EXC_ALREADY_EXISTS, "AlreadyExistsError");
    reg!(EXC_TIMEOUT, "ActantTimeoutError");
    reg!(EXC_CANCELLED, "TaskCancelledError");
    reg!(EXC_INVALID_STATE, "InvalidStateError");
    reg!(EXC_INTERNAL, "InternalError");

    // 额外注册 Python 端独有的异常类（无对应 Rust ActantError 变体）。
    m.add(
        "PayloadTooLargeError",
        exc_mod.getattr(pyo3::intern!(py, "PayloadTooLargeError"))?,
    )?;
    m.add(
        "WorkflowFailedError",
        exc_mod.getattr(pyo3::intern!(py, "WorkflowFailedError"))?,
    )?;
    m.add(
        "WorkflowCancelledError",
        exc_mod.getattr(pyo3::intern!(py, "WorkflowCancelledError"))?,
    )?;

    Ok(())
}
