pub mod actor;
pub mod actor_ops;
pub mod awaitable;
pub mod bootstrap;
pub mod cancel;
pub mod config;
pub mod error;
pub mod event;
pub(crate) mod gil_thread;
pub mod runtime;

pub use actor::PythonActor;
pub use actor_ops::PyActorCore;
pub use cancel::PyCancelToken;
pub use config::{
    PyActantConfig, PyFailoverConfig, PyGossipConfig, PyNetworkConfig, PyRetryPolicy,
    PyWorkflowState,
};
pub use error::PyActantError;
pub use event::{PyEvent, PyOrchestrationEvent, PySupervisionEventData, PyTaskCompletion};
pub use runtime::{PyAsyncResultCore, PyRetryInfo, PyRuntimeCore};

use pyo3::prelude::*;

/// 在模块上注册所有 Python 端类和异常。
///
/// 各子模块拥有自己的 `register` 函数（单一职责），
/// 此函数仅分派到它们。新子模块只需在此添加一行，
/// 而非膨胀 `lib.rs`。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    runtime::register(m)?;
    config::register(m)?;
    event::register(m)?;
    actor_ops::register(m)?;
    awaitable::register(m)?;
    cancel::register(m)?;
    error::register_exceptions(m)?;
    Ok(())
}

/// Python 模块入口点。
///
/// 所有 PyO3 定义（`#[pymodule]`、`#[pyfunction]`）集中在 `src/py/` 下，
/// `src/lib.rs` 仅声明模块，不包含任何 PyO3 代码（分层边界）。
#[pymodule]
pub fn actant(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // 安装 pyo3-log 桥接：Rust `log` 记录（包括通过 `log` feature 转发的 `tracing` 事件）
    // 流入 Python 的 `logging` 模块。Python 成为日志级别和输出的唯一真相来源。
    pyo3_log::init();

    register(m)?;
    m.add_function(pyo3::wrap_pyfunction!(get_version, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(refresh_logger, m)?)?;
    Ok(())
}

/// 返回包版本号。
#[pyfunction]
fn get_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// 将 Python 根 logger 级别同步到 Rust `log` crate 的最大级别。
///
/// 在更改 Python 日志级别后调用（如通过 `logging.basicConfig(level=...)` 或
/// `logging.getLogger().setLevel(...)`）。Rust `log` 最大级别过滤器会被设为匹配值，
/// 以便 `tracing` 事件（转发到 `log`）在到达 `pyo3_log` 前不被过滤掉。
#[pyfunction]
fn refresh_logger(py: Python<'_>) -> PyResult<()> {
    use log::LevelFilter;

    let logging = py.import(pyo3::intern!(py, "logging"))?;
    let python_level: usize = logging
        .getattr(pyo3::intern!(py, "getLogger"))?
        .call0()?
        .getattr(pyo3::intern!(py, "level"))?
        .extract()
        .unwrap_or(0);

    let level_filter = match python_level {
        0 => LevelFilter::Off,
        1..=10 => LevelFilter::Debug,
        11..=20 => LevelFilter::Info,
        21..=30 => LevelFilter::Warn,
        31..=40 => LevelFilter::Error,
        _ => LevelFilter::Error,
    };

    log::set_max_level(level_filter);
    Ok(())
}
