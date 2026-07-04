use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::common::ActantError;

// Python 异常层级，镜像 actant.exceptions。
// 基类 PyActantError 映射 actant.exceptions.ActantError，
// 子类映射各 ActantError 变体。
create_exception!(
    actant,
    PyActantError,
    PyRuntimeError,
    "Actant base exception"
);
create_exception!(actant, PyStorageError, PyActantError, "Storage error");
create_exception!(actant, PyNetworkError, PyActantError, "Network error");
create_exception!(
    actant,
    PySerializationError,
    PyActantError,
    "Serialization error"
);
create_exception!(actant, PyActorError, PyActantError, "Actor error");
create_exception!(actant, PyWorkflowError, PyActantError, "Workflow error");
create_exception!(actant, PyTaskError, PyActantError, "Task error");
create_exception!(actant, PyWorkerError, PyActantError, "Worker error");
create_exception!(actant, PyConfigError, PyActantError, "Configuration error");
create_exception!(
    actant,
    PyMetricsError,
    PyActantError,
    "Metrics pipeline error"
);
create_exception!(actant, PyNotFoundError, PyActantError, "Resource not found");
create_exception!(
    actant,
    PyAlreadyExistsError,
    PyActantError,
    "Resource already exists"
);
create_exception!(actant, PyTimeoutError, PyActantError, "Operation timed out");
create_exception!(
    actant,
    PyCancelledError,
    PyActantError,
    "Operation cancelled"
);
create_exception!(
    actant,
    PyInvalidStateError,
    PyActantError,
    "Invalid state operation"
);
create_exception!(actant, PyInternalError, PyActantError, "Internal error");

/// 将 Rust ActantError 转换为对应的 Python 异常类型。
///
/// 映射：ActantError 变体 → PyXxxError 子类，
/// 跨语言边界保留错误类型信息。
impl From<ActantError> for PyErr {
    fn from(err: ActantError) -> Self {
        match &err {
            ActantError::Storage(_) | ActantError::StorageIo(_) | ActantError::Heed(_) => {
                PyStorageError::new_err(err.to_string())
            }
            ActantError::Network(_) => PyNetworkError::new_err(err.to_string()),
            ActantError::Serialization(_) | ActantError::Postcard(_) => {
                PySerializationError::new_err(err.to_string())
            }
            ActantError::Actor(_) => PyActorError::new_err(err.to_string()),
            ActantError::Workflow(_) => PyWorkflowError::new_err(err.to_string()),
            ActantError::Task(_) => PyTaskError::new_err(err.to_string()),
            ActantError::Worker(_) => PyWorkerError::new_err(err.to_string()),
            ActantError::Config(_) => PyConfigError::new_err(err.to_string()),
            ActantError::Metrics(_) => PyMetricsError::new_err(err.to_string()),
            ActantError::NotFound(_) => PyNotFoundError::new_err(err.to_string()),
            ActantError::AlreadyExists(_) => PyAlreadyExistsError::new_err(err.to_string()),
            ActantError::Timeout(_) => PyTimeoutError::new_err(err.to_string()),
            ActantError::Cancelled(_) => PyCancelledError::new_err(err.to_string()),
            ActantError::InvalidState(_) => PyInvalidStateError::new_err(err.to_string()),
            ActantError::Internal(_) => PyInternalError::new_err(err.to_string()),
        }
    }
}

/// 在 Python 模块上注册所有异常类。
pub fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ActantError", m.py().get_type::<PyActantError>())?;
    m.add("StorageError", m.py().get_type::<PyStorageError>())?;
    m.add("NetworkError", m.py().get_type::<PyNetworkError>())?;
    m.add(
        "SerializationError",
        m.py().get_type::<PySerializationError>(),
    )?;
    m.add("ActorError", m.py().get_type::<PyActorError>())?;
    m.add("WorkflowError", m.py().get_type::<PyWorkflowError>())?;
    m.add("TaskError", m.py().get_type::<PyTaskError>())?;
    m.add("WorkerError", m.py().get_type::<PyWorkerError>())?;
    m.add("ConfigError", m.py().get_type::<PyConfigError>())?;
    m.add("MetricsError", m.py().get_type::<PyMetricsError>())?;
    m.add("NotFoundError", m.py().get_type::<PyNotFoundError>())?;
    m.add(
        "AlreadyExistsError",
        m.py().get_type::<PyAlreadyExistsError>(),
    )?;
    m.add("TimeoutError", m.py().get_type::<PyTimeoutError>())?;
    m.add("CancelledError", m.py().get_type::<PyCancelledError>())?;
    m.add(
        "InvalidStateError",
        m.py().get_type::<PyInvalidStateError>(),
    )?;
    m.add("InternalError", m.py().get_type::<PyInternalError>())?;
    Ok(())
}
