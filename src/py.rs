pub mod capability;
pub mod config;
pub mod error;
pub(crate) mod gil_thread;
pub(crate) mod route_policy;
pub mod runtime;
pub mod types;

pub use config::{
    PyActantConfig, PyFailoverConfig, PyGossipConfig, PyNetworkConfig, PyRetryPolicy,
};
pub use runtime::{PyNode, PyRuntimeCore, PyTask};
pub use types::PyTaskCompletion;

use pyo3::prelude::*;

/// 在模块上注册所有 Python 端类和异常。
///
/// 各子模块拥有自己的 `register` 函数（单一职责），
/// 此函数仅分派到它们。新子模块只需在此添加一行，
/// 而非膨胀 `lib.rs`。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    runtime::register(m)?;
    config::register(m)?;
    types::register(m)?;
    capability::register(m)?;
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
    Ok(())
}

/// 返回包版本号。
///
/// Python 公开入口是 ``actant.__version__``；本函数属于 advanced 低层接口，
/// 仅在需要从 PyO3 模块直接读取版本时使用。
#[pyfunction]
fn get_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
