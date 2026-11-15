use pyo3::exceptions::{PyBaseException, PyRuntimeError};
use pyo3::prelude::*;

use actant_core::common::ActantError;
use parking_lot::RwLock;

// 缓存 Python 端异常类对象（在 register_exceptions 中初始化）。
//
// Python 异常类对象经 RwLock 缓存（而非 create_exception! 派生独立类型），
// 使 Python 定义的类只有一条继承链，Rust 抛出的异常可被
// `except actant.ActantError` 捕获。缓存 Python 端的类对象，使
// `actant_error_to_pyerr` 抛出的就是 Python 端的类。
//
// 用可覆盖的 RwLock 而非 OnceLock：模块被删除后重新导入时，Python 会创建
// 一批新的异常类；此时必须用新类覆盖旧缓存，否则 Rust 抛出的实例与
// `actant.exceptions.X` 不再是同一个类，`except` 会漏接。
static EXC_ACTANT: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_STORAGE: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_NETWORK: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_SERIALIZATION: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_ACTOR: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_WORKFLOW: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_TASK: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_WORKER: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_CONFIG: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_METRICS: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_NOT_FOUND: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_ALREADY_EXISTS: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_TIMEOUT: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_CANCELLED: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_INVALID_STATE: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_INTERNAL: RwLock<Option<Py<PyAny>>> = RwLock::new(None);
static EXC_REPLAY: RwLock<Option<Py<PyAny>>> = RwLock::new(None);

/// 用缓存的 Python 异常类构造 `PyErr`。
///
/// 若缓存未初始化（`register_exceptions` 未调用）或构造失败，
/// 回退到 `PyRuntimeError`，保证不 panic。
fn make_pyerr(cls: &RwLock<Option<Py<PyAny>>>, message: &str) -> PyErr {
    Python::attach(|py| {
        // 在持锁期间只克隆引用，拿到 owned 句柄后立即释放读锁，
        // 不在持锁时执行任何 Python 代码。
        let Some(cls) = cls.read().as_ref().map(|c| c.clone_ref(py)) else {
            return PyRuntimeError::new_err(message.to_string());
        };
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
/// 孤儿规则禁止跨 crate 实现 `From<ActantError> for PyErr`——
/// 改用自由函数；调用点经 `actant_error_to_pyerr(x)` 的全部替换为 `actant_error_to_pyerr(x)`。
pub fn actant_error_to_pyerr(err: ActantError) -> PyErr {
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
        ActantError::Replay(_) => make_pyerr(&EXC_REPLAY, &message),
        ActantError::Internal(_) => make_pyerr(&EXC_INTERNAL, &message),
    }
}

/// 在 Python 模块上注册所有异常类。
///
/// 从 `actant.exceptions` 导入 Python 端定义的异常类（而非用 `create_exception!`
/// 创建独立类），确保 Rust 抛出的异常与 Python 用户 `except` 的类是同一个。
/// 每次调用都会覆盖类缓存，使模块重新导入后缓存与新类保持一致。
pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let exc_mod = py.import(pyo3::intern!(py, "actant.exceptions"))?;

    macro_rules! reg {
        ($lock:ident, $name:literal) => {{
            let cls: Py<PyAny> = exc_mod.getattr(pyo3::intern!(py, $name))?.into();
            // `Py<T>` 不直接 impl Clone；通过 `clone_ref(py)`
            // 显式克隆引用，一份给缓存，一份注册到模块。
            // 写入是覆盖语义：模块重新导入后，新类必须替换旧缓存。
            *$lock.write() = Some(cls.clone_ref(py));
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
    reg!(EXC_REPLAY, "FlowReplayError");
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
