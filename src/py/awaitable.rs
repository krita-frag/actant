use pyo3::{exceptions::PyStopIteration, prelude::*};
use std::sync::{atomic, OnceLock};

use super::gil_thread::GilThread;

pub(crate) enum FutureResultToPy {
    Value(Py<PyAny>),
    Err(PyErr),
}

/// 状态机：0 = Pending，1 = Completed
const STATE_PENDING: u8 = 0;
const STATE_COMPLETED: u8 = 1;

#[pyclass(frozen, freelist = 128, module = "actant.actant")]
pub(crate) struct PyAsyncAwaitable {
    state: atomic::AtomicU8,
    result: OnceLock<FutureResultToPy>,
}

impl PyAsyncAwaitable {
    pub(crate) fn new() -> Self {
        Self {
            state: atomic::AtomicU8::new(STATE_PENDING),
            result: OnceLock::new(),
        }
    }

    /// 当 future 完成时由 GIL worker 线程调用。
    /// 将结果设置到 OnceLock — 等待的协程在下一次 __next__ 调用
    /// （asyncio 自动调用）时将看到它。
    pub(crate) fn set_result(pyself: Py<Self>, py: Python, result: FutureResultToPy) {
        let rself = pyself.get();
        let _ = rself.result.set(result);
        rself
            .state
            .store(STATE_COMPLETED, atomic::Ordering::Release);
        // 释放 GIL worker 线程持有的持久引用。
        // 否则 tokio 任务持有的 Py<Self> 会阻止回收，
        // 直到任务被 drop，即使结果已交付。
        pyself.drop_ref(py);
    }
}

#[pymethods]
impl PyAsyncAwaitable {
    fn __await__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __iter__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __next__(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        if self.state.load(atomic::Ordering::Acquire) == STATE_COMPLETED {
            // state 在 result.set() 成功后以 Release 顺序写入，
            // 正常情况下 `get()` 总返回 Some。若返回 None
            // （如 double set_result 导致首次 set 丢失值的逻辑 bug），
            // 转为 Python 异常而非 panic 解释器。
            let result = self.result.get().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "awaitable completed but result is missing (internal invariant violated)",
                )
            })?;
            return match result {
                FutureResultToPy::Value(v) => Err(PyStopIteration::new_err(v.clone_ref(py))),
                FutureResultToPy::Err(e) => Err(e.clone_ref(py)),
            };
        }

        Ok(Some(py.None()))
    }
}

/// 创建 awaitable 并将 future 派生到指定 tokio handle 上。
/// future 完成后，结果发送到 GIL worker 线程（永久持有 GIL）以设置到 awaitable。
///
/// 无 Mutex、无 asyncio.Future 包装、无事件循环依赖 —
/// 使用 OnceLock + AtomicU8 实现无锁结果交付。
pub(crate) fn future_into_py_iter<'py, F>(
    py: Python<'py>,
    handle: tokio::runtime::Handle,
    gil_thread: &GilThread,
    fut: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = FutureResultToPy> + Send + 'static,
{
    let aw = Py::new(py, PyAsyncAwaitable::new())?;
    let py_fut = aw.clone_ref(py);

    let gw = gil_thread.clone();
    handle.spawn(async move {
        let result = fut.await;
        let _ = gw.send(move |py| {
            PyAsyncAwaitable::set_result(aw, py, result);
        });
    });

    Ok(py_fut.into_any().into_bound(py))
}

/// 在 Python 模块上注册所有 awaitable 相关类。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAsyncAwaitable>()?;
    Ok(())
}
