//! 暴露给 Python 的协作式取消令牌。
//!
//! `PyCancelToken` 包装一个在 Rust dispatcher 和 Python 任务处理器之间共享的
//! `Arc<AtomicBool>`。当 dispatcher 超时时，将 flag 设为 `true`；
//! Python 处理器通过 `token.is_cancelled()` 轮询 flag，以协作方式退出长任务。
//!
//! 设计理由：直接传递 `Arc<AtomicBool>`（而非全局注册表中的字符串 key），
//! 消除了注册表、字符串查找和清理生命周期。flag 通过单次
//! `AtomicBool::load` 检查 — 无需通过 `DashMap` 进行 PyO3 往返。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pyo3::prelude::*;

use crate::worker::dispatcher::CancelFlag;

/// 在 Rust 和 Python 之间共享的协作式取消 flag。
///
/// 由 Rust dispatcher 为每次任务分发创建并传给 Python 处理器。
/// Python 代码在长循环内轮询 `token.is_cancelled()`，
/// 以在 dispatcher 发出超时信号时协作退出。
#[pyclass(frozen, name = "CancelToken", module = "actant")]
pub struct PyCancelToken {
    flag: CancelFlag,
}

impl PyCancelToken {
    /// 包装现有 `CancelFlag` 以暴露给 Python。
    pub fn new(flag: CancelFlag) -> Self {
        Self { flag }
    }

    /// 访问底层 flag（用于 Rust 端修改，如超时）。
    pub fn flag(&self) -> &Arc<AtomicBool> {
        &self.flag
    }
}

#[pymethods]
impl PyCancelToken {
    /// 若任务已被取消（超时）则返回 `True`。
    ///
    /// 使用 `Acquire` 顺序以观察 dispatcher 线程的写入。
    fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// `is_cancelled()` 的别名，作为 property 访问。
    #[getter]
    fn cancelled(&self) -> bool {
        self.is_cancelled()
    }

    fn __bool__(&self) -> bool {
        self.is_cancelled()
    }

    fn __repr__(&self) -> String {
        format!("CancelToken(cancelled={})", self.is_cancelled())
    }
}

/// 在 Python 模块上注册 cancel-token 类。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCancelToken>()?;
    Ok(())
}
