//! PyO3 <-> Rust 类型转换与辅助原语统一入口。
//! 为每个内置 capability 提供实现 `PyAskCodec` / `PyPerformCodec` / `PyEmitCodec`
//! 的 marker 类型。`capability_registry!` 通过声明式映射完成分发。

use std::sync::{atomic, OnceLock};

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::py::gil_thread::GilThread;
use actant_core::common::{TaskId, WorkflowId};
use actant_core::runtime::capability::{
    Store, StoreReq, TaskEvent, TaskLifecycle, WorkflowEvent, WorkflowLifecycle,
};
use actant_core::runtime::state::StorageStats;

/// `ask` effect 的 PyO3 编解码 trait。
///
/// `Runtime::ask` 返回 `Option<C::Response>`（第一个返回 `Some` 的 handler），
/// 因此 `encode_response` 接收的是 `Option<C::Response>`。
pub trait PyAskCodec<C: actant_core::runtime::capability::Capability> {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<C::Request>;
    fn encode_response(
        py: Python<'_>,
        resp: Option<C::Response>,
    ) -> PyResult<Option<Bound<'_, PyAny>>>;
}

/// `perform` effect 的 PyO3 编解码 trait。
pub trait PyPerformCodec<C: actant_core::runtime::capability::Capability> {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<C::Request>;
    fn encode_response(py: Python<'_>, resp: C::Response) -> PyResult<Bound<'_, PyAny>>;
}

/// `emit` effect 的 PyO3 解码 trait（emit 无返回值）。
pub trait PyEmitCodec<C: actant_core::runtime::capability::Capability> {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<C::Request>;
}

/// 缓存高频属性名的 interned PyString，避免每次 getattr 都新建 Python 字符串对象。
fn intern_attr<'py>(py: Python<'py>, name: &str) -> Bound<'py, pyo3::types::PyString> {
    match name {
        "attempt" => pyo3::intern!(py, "attempt").clone(),
        "data" => pyo3::intern!(py, "data").clone(),
        "error" => pyo3::intern!(py, "error").clone(),
        "key" => pyo3::intern!(py, "key").clone(),
        "kind" => pyo3::intern!(py, "kind").clone(),
        "next_attempt" => pyo3::intern!(py, "next_attempt").clone(),
        "node_id" => pyo3::intern!(py, "node_id").clone(),
        "op" => pyo3::intern!(py, "op").clone(),
        "payload" => pyo3::intern!(py, "payload").clone(),
        "peer_id" => pyo3::intern!(py, "peer_id").clone(),
        "result_payload" => pyo3::intern!(py, "result_payload").clone(),
        "target" => pyo3::intern!(py, "target").clone(),
        "task_id" => pyo3::intern!(py, "task_id").clone(),
        "timestamp_ms" => pyo3::intern!(py, "timestamp_ms").clone(),
        "timeout_ms" => pyo3::intern!(py, "timeout_ms").clone(),
        "value" => pyo3::intern!(py, "value").clone(),
        "workflow_id" => pyo3::intern!(py, "workflow_id").clone(),
        _ => pyo3::types::PyString::new(py, name),
    }
}

fn get_string<'py, D>(ob: D, name: &str) -> PyResult<String>
where
    D: AsRef<Bound<'py, PyAny>>,
{
    let ob = ob.as_ref();
    let py = ob.py();
    ob.getattr(intern_attr(py, name))?.extract()
}

fn get_u32<'py, D>(ob: D, name: &str) -> PyResult<u32>
where
    D: AsRef<Bound<'py, PyAny>>,
{
    let ob = ob.as_ref();
    let py = ob.py();
    ob.getattr(intern_attr(py, name))?.extract()
}

fn get_bytes<'py, D>(ob: D, name: &str) -> PyResult<Vec<u8>>
where
    D: AsRef<Bound<'py, PyAny>>,
{
    let ob = ob.as_ref();
    let py = ob.py();
    ob.getattr(intern_attr(py, name))?.extract()
}

fn task_id<'py, D>(ob: D, name: &str) -> PyResult<TaskId>
where
    D: AsRef<Bound<'py, PyAny>>,
{
    let s: String = get_string(&ob, name)?;
    Ok(TaskId::from(s))
}

fn workflow_id<'py, D>(ob: D, name: &str) -> PyResult<WorkflowId>
where
    D: AsRef<Bound<'py, PyAny>>,
{
    let s: String = get_string(&ob, name)?;
    Ok(WorkflowId::from(s))
}

// 放置类策略（路由/调度/重试）由 Rust RoutePolicy/Scheduler 或第 3 层外部
// 调度器承担；Rust 核心不提供对应 Python 编排循环的 marker 类型及其 codec。

pub struct StoreCodec;

impl PyPerformCodec<Store> for StoreCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<StoreReq> {
        extract_storereq(ob)
    }

    fn encode_response(
        py: Python<'_>,
        resp: Result<Option<Vec<u8>>, String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        use pyo3::conversion::IntoPyObject;
        match resp {
            Ok(None) => {
                let none = py.None().into_pyobject(py)?;
                Ok(none.to_owned().into_any())
            }
            Ok(Some(data)) => bytes_response(py, data),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}

pub struct TaskLifecycleCodec;

impl PyEmitCodec<TaskLifecycle> for TaskLifecycleCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<TaskEvent> {
        let kind: String = get_string(ob, "kind")?;
        match kind.as_str() {
            "started" => Ok(TaskEvent::Started {
                task_id: task_id(ob, "task_id")?,
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            "completed" => Ok(TaskEvent::Completed {
                task_id: task_id(ob, "task_id")?,
                result_payload: get_bytes(ob, "result_payload")?,
            }),
            "failed" => Ok(TaskEvent::Failed {
                task_id: task_id(ob, "task_id")?,
                error: get_string(ob, "error")?,
                attempt: get_u32(ob, "attempt")?,
            }),
            "retried" => Ok(TaskEvent::Retried {
                task_id: task_id(ob, "task_id")?,
                next_attempt: get_u32(ob, "next_attempt")?,
            }),
            "cancelled" => Ok(TaskEvent::Cancelled {
                task_id: task_id(ob, "task_id")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown TaskEvent kind: {}",
                kind
            ))),
        }
    }
}

pub struct WorkflowLifecycleCodec;

impl PyEmitCodec<WorkflowLifecycle> for WorkflowLifecycleCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<WorkflowEvent> {
        let kind: String = get_string(ob, "kind")?;
        match kind.as_str() {
            "submitted" => Ok(WorkflowEvent::Submitted {
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            "started" => Ok(WorkflowEvent::Started {
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            "completed" => Ok(WorkflowEvent::Completed {
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            "failed" => Ok(WorkflowEvent::Failed {
                workflow_id: workflow_id(ob, "workflow_id")?,
                error: get_string(ob, "error")?,
            }),
            "cancelled" => Ok(WorkflowEvent::Cancelled {
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown WorkflowEvent kind: {}",
                kind
            ))),
        }
    }
}

pub fn bytes_response<'py>(py: Python<'py>, data: impl AsRef<[u8]>) -> PyResult<Bound<'py, PyAny>> {
    use pyo3::types::PyBytes;
    Ok(PyBytes::new(py, data.as_ref()).into_any())
}

pub fn dict_response<'py>(py: Python<'py>) -> Bound<'py, PyDict> {
    PyDict::new(py)
}

// ---------------------------------------------------------------------------
// Capability 类型 PyO3 自动转换（#[pyclass] 自动派生等效方案）
// ---------------------------------------------------------------------------
//
// 为 Rust 内置 capability 的 Request/Response 类型实现 `FromPyObject` /
// `IntoPyObject`，使 codec 可以一次性 `extract()` / `into_pyobject()` 完成转换，
// 避免逐字段重复 getattr/set_item。Python 侧仍使用 dataclass，无需同步修改。

/// 孤儿规则禁止跨 crate 实现 `FromPyObject`——由 codec 的
/// `decode_request` 直接调用本函数替代 trait 实现。
fn extract_storereq(ob: &Bound<'_, PyAny>) -> PyResult<StoreReq> {
    let op: String = get_string(ob, "op")?;
    match op.as_str() {
        "put" => Ok(StoreReq::Put {
            key: get_bytes(ob, "key")?,
            value: get_bytes(ob, "value")?,
        }),
        "get" => Ok(StoreReq::Get {
            key: get_bytes(ob, "key")?,
        }),
        "delete" => Ok(StoreReq::Delete {
            key: get_bytes(ob, "key")?,
        }),
        "put_batch" => {
            let entries: Vec<(Vec<u8>, Vec<u8>)> = ob.getattr("entries")?.extract()?;
            Ok(StoreReq::PutBatch { entries })
        }
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown StoreReq op: {}",
            op
        ))),
    }
}

pub(crate) enum FutureResultToPy {
    Value(Py<PyAny>),
    Err(PyErr),
}

/// 状态机：0 = Pending，1 = Completed
const STATE_PENDING: u8 = 0;
const STATE_COMPLETED: u8 = 1;

/// 自定义 awaitable，用于没有 asyncio running loop 的同步上下文。
/// 在异步上下文中优先返回标准 asyncio.Future。
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

    pub(crate) fn set_result(pyself: Py<Self>, py: Python, result: FutureResultToPy) {
        let rself = pyself.get();
        match rself.result.set(result) {
            Ok(_) => {}
            Err(_) => {
                tracing::warn!("set_result: result already set (duplicate completion)");
            }
        }
        rself
            .state
            .store(STATE_COMPLETED, atomic::Ordering::Release);
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

/// 进程级缓存的 `asyncio` 模块引用。
///
/// `py.import("asyncio")` 每次调用都会在 `sys.modules` 中做 dict 查找
/// （~200ns-1μs）。在 `future_into_py_iter` 等热路径中缓存 `Py<PyModule>`
/// 可省去 dict 查找，直接 `.bind(py)` 借用（~10ns）。
///
/// `OnceLock` 保证只初始化一次；`Py<PyModule>` 是 `Send + Sync`，
/// 可安全存储在 `static` 中。初始化在持 GIL 状态下完成（由 `Python` token 保证）。
static ASYNCIO_MODULE: OnceLock<Py<pyo3::types::PyModule>> = OnceLock::new();

/// 获取缓存的 `asyncio` 模块（首次调用时 import，后续直接 bind）。
///
/// 返回 `Bound<'py, PyModule>`（owned），需要一次 `clone_ref`（atomic ++）
/// 但省去了 `sys.modules` dict 查找 + `__import__` 路径，净收益在热路径显著。
fn get_asyncio<'py>(py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyModule>> {
    if let Some(cached) = ASYNCIO_MODULE.get() {
        // 已缓存：clone_ref 取 owned Bound。比 py.import 少了 dict 查找。
        return Ok(cached.clone_ref(py).into_bound(py));
    }
    // 首次：import 并缓存。unbind 转为 Py<PyModule>（不持 GIL 生命周期）。
    let module = py.import("asyncio")?;
    let _ = ASYNCIO_MODULE.set(module.clone().unbind());
    Ok(module)
}

pub(crate) fn future_into_py_iter<'py, F>(
    py: Python<'py>,
    handle: tokio::runtime::Handle,
    gil_thread: &GilThread,
    fut: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = FutureResultToPy> + Send + 'static,
{
    let asyncio = get_asyncio(py)?;

    // 优先使用 asyncio.Future：在 async 上下文中有 running loop，
    // 通过 loop.call_soon_threadsafe 从 tokio 任务线程安全设置结果。
    // 失败时回退到自定义 PyAsyncAwaitable，兼容同步/非 asyncio 上下文。
    let loop_obj: Option<Bound<'py, PyAny>> = match asyncio.call_method0("get_running_loop") {
        Ok(loop_obj) => Some(loop_obj),
        Err(e) => {
            tracing::debug!(
                error = %e,
                "no asyncio running loop, falling back to PyAsyncAwaitable"
            );
            None
        }
    };

    if let Some(loop_obj) = loop_obj {
        let py_fut = loop_obj.call_method0("create_future")?;
        let loop_ref = loop_obj.clone().unbind();
        let fut_ref = py_fut.clone().unbind();

        let gw = gil_thread.clone();
        handle.spawn(async move {
            let result = fut.await;
            // 使用 send_or_run 而非 send：bounded channel 满时降级为
            // 当前 tokio 线程直接 Python::attach 同步执行。这里闭包只设置
            // Future 结果（µs 级），降级阻塞可接受；避免无界堆积内存。
            gw.send_or_run(move |py| {
                let loop_obj = loop_ref.bind(py);
                let fut = fut_ref.bind(py);
                let set_result = fut.getattr("set_result").ok();
                let set_exception = fut.getattr("set_exception").ok();
                match result {
                    FutureResultToPy::Value(v) => {
                        if let Some(cb) = set_result {
                            // call_soon_threadsafe 失败通常表示 event loop 已关闭
                            // （Python 解释器关闭中）。此时 Future 永远不会 resolve，
                            // 但程序正在退出，Future 会被 GC 回收。
                            // 记录 warning 使问题可观测，避免静默挂起。
                            if let Err(e) = loop_obj.call_method1("call_soon_threadsafe", (cb, v)) {
                                tracing::warn!(
                                    error = %e,
                                    "call_soon_threadsafe(set_result) failed; \
                                     event loop may be closed; Future will not resolve"
                                );
                            }
                        }
                    }
                    FutureResultToPy::Err(e) => {
                        if let Some(cb) = set_exception {
                            if let Err(err) = loop_obj.call_method1("call_soon_threadsafe", (cb, e))
                            {
                                tracing::warn!(
                                    error = %err,
                                    "call_soon_threadsafe(set_exception) failed; \
                                     event loop may be closed; Future will not resolve"
                                );
                            }
                        }
                    }
                }
            });
        });

        return Ok(py_fut);
    }

    // Fallback：无 running loop 时使用自定义 awaitable。
    let aw = Py::new(py, PyAsyncAwaitable::new())?;
    let py_fut = aw.clone_ref(py);

    let gw = gil_thread.clone();
    handle.spawn(async move {
        let result = fut.await;
        // 同上：send_or_run 在 bounded channel 满时降级同步执行。
        gw.send_or_run(move |py| {
            PyAsyncAwaitable::set_result(aw, py, result);
        });
    });

    Ok(py_fut.into_any().into_bound(py))
}

#[pyclass(name = "_TaskCompletion", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTaskCompletion {
    #[pyo3(get)]
    pub workflow_id: String,
    #[pyo3(get)]
    pub task_id: String,
    #[pyo3(get)]
    pub task_name: String,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub result: Option<Vec<u8>>,
    #[pyo3(get)]
    pub error: Option<String>,
    #[pyo3(get)]
    pub target_node: Option<String>,
}

/// 生成 Rust 内置 capability 的 PyO3 分发函数。
#[macro_export]
macro_rules! capability_registry {
    (
        ask: { $( $ask_name:literal => $ask_cap:ty => $ask_codec:path ),* $(,)? }
        perform: { $( $perf_name:literal => $perf_cap:ty => $perf_codec:path ),* $(,)? }
        emit: { $( $emit_name:literal => $emit_cap:ty => $emit_codec:path ),* $(,)? }
    ) => {
        // ask 注册表为空（Rust 核心当前无 ask 型 capability）时，
        // 分发函数参数不被任何分支消费，故允许未使用变量。
        #[allow(unused_variables)]
        fn dispatch_ask<'py>(
            py: pyo3::Python<'py>,
            name: &str,
            request: &pyo3::Bound<'_, pyo3::PyAny>,
            inner: std::sync::Arc<actant_core::runtime::capability::CapabilityRuntime>,
            tokio: &tokio::runtime::Runtime,
        ) -> pyo3::PyResult<Option<pyo3::Bound<'py, pyo3::PyAny>>> {
            match name {
                $(
                    $ask_name => {
                        use $crate::py::types::PyAskCodec;
                        let _t = std::time::Instant::now();
                        let req = <$ask_codec as PyAskCodec<$ask_cap>>::decode_request(request)?;
                        // 释放 GIL 期间 block_on：避免 tokio worker pyo3_log 回调死锁。
                        let resp = py.detach(move || {
                            tokio.block_on(inner.ask::<$ask_cap>(req))
                        })
                            .map_err($crate::py::error::actant_error_to_pyerr)?;
                        tracing::trace!(cap = $ask_name, ms = _t.elapsed().as_millis() as u64, "dispatch_ask");
                        <$ask_codec as PyAskCodec<$ask_cap>>::encode_response(py, resp)
                    }
                ),*
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "ask capability {} not supported by Rust runtime",
                    name
                ))),
            }
        }

        fn dispatch_perform<'py>(
            py: pyo3::Python<'py>,
            name: &str,
            request: &pyo3::Bound<'_, pyo3::PyAny>,
            inner: std::sync::Arc<actant_core::runtime::capability::CapabilityRuntime>,
            tokio: &tokio::runtime::Runtime,
        ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
            match name {
                $(
                    $perf_name => {
                        use $crate::py::types::PyPerformCodec;
                        let _t = std::time::Instant::now();
                        let req = <$perf_codec as PyPerformCodec<$perf_cap>>::decode_request(request)?;
                        // 释放 GIL 期间 block_on：避免 tokio worker pyo3_log 回调死锁。
                        let resp = py.detach(move || {
                            tokio.block_on(inner.perform::<$perf_cap>(req))
                        })
                            .map_err($crate::py::error::actant_error_to_pyerr)?;
                        tracing::trace!(cap = $perf_name, ms = _t.elapsed().as_millis() as u64, "dispatch_perform");
                        <$perf_codec as PyPerformCodec<$perf_cap>>::encode_response(py, resp)
                    }
                ),*
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "perform capability {} not supported by Rust runtime",
                    name
                ))),
            }
        }

        fn dispatch_emit(
            py: pyo3::Python<'_>,
            name: &str,
            request: &pyo3::Bound<'_, pyo3::PyAny>,
            inner: std::sync::Arc<actant_core::runtime::capability::CapabilityRuntime>,
            tokio: &tokio::runtime::Runtime,
        ) -> pyo3::PyResult<()> {
            match name {
                $(
                    $emit_name => {
                        use $crate::py::types::PyEmitCodec;
                        let _t = std::time::Instant::now();
                        let req = <$emit_codec as PyEmitCodec<$emit_cap>>::decode_request(request)?;
                        // 释放 GIL 期间 block_on：避免 tokio worker pyo3_log 回调死锁。
                        py.detach(move || {
                            tokio.block_on(inner.emit::<$emit_cap>(req))
                        })
                            .map_err($crate::py::error::actant_error_to_pyerr)?;
                        tracing::trace!(cap = $emit_name, ms = _t.elapsed().as_millis() as u64, "dispatch_emit");
                        Ok(())
                    }
                ),*
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "emit capability {} not supported by Rust runtime",
                    name
                ))),
            }
        }

        /// 异步 perform 分发：返回 `asyncio.Future`，不阻塞 Python 主线程。
        ///
        /// 与同步 `dispatch_perform` 的差异：同步实现于 Python 主线程上
        /// `py.detach(|| tokio.block_on(...))` 同步阻塞，无法在 asyncio
        /// 上下文中并发执行多个 perform。异步版本将 Rust Future 通过
        /// `future_into_py_iter` 转为 `asyncio.Future`，在 tokio runtime
        /// 上异步执行 `inner.perform().await`，结果通过 GIL 线程回调到
        /// asyncio loop。
        fn dispatch_perform_async<'py>(
            py: pyo3::Python<'py>,
            name: &str,
            request: &pyo3::Bound<'_, pyo3::PyAny>,
            inner: std::sync::Arc<actant_core::runtime::capability::CapabilityRuntime>,
            tokio_handle: tokio::runtime::Handle,
            gil_thread: &$crate::py::gil_thread::GilThread,
        ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
            match name {
                $(
                    $perf_name => {
                        use $crate::py::types::{PyPerformCodec, future_into_py_iter, FutureResultToPy};
                        // 持 GIL 解码 Python 请求 → Rust 类型。
                        let req = <$perf_codec as PyPerformCodec<$perf_cap>>::decode_request(request)?;
                        // future_into_py_iter 创建 asyncio.Future 并在 tokio 上 spawn
                        // 异步任务。Python 侧 await 此 Future 时不阻塞主线程。
                        future_into_py_iter(
                            py,
                            tokio_handle,
                            gil_thread,
                            async move {
                                let _t = std::time::Instant::now();
                                match inner.perform::<$perf_cap>(req).await {
                                    Ok(resp) => {
                                        let ms = _t.elapsed().as_millis() as u64;
                                        tracing::trace!(cap = $perf_name, ms, "dispatch_perform_async");
                                        // 在 tokio 线程上获取 GIL 编码响应为 Python 对象。
                                        // Python::attach 会获取 GIL，不影响 asyncio loop。
                                        pyo3::Python::attach(|py| {
                                            match <$perf_codec as PyPerformCodec<$perf_cap>>::encode_response(py, resp) {
                                                Ok(v) => FutureResultToPy::Value(v.unbind()),
                                                Err(e) => FutureResultToPy::Err(e),
                                            }
                                        })
                                    }
                                    Err(e) => FutureResultToPy::Err(
                                        $crate::py::error::actant_error_to_pyerr(e)
                                    ),
                                }
                            },
                        )
                    }
                ),*
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "perform_async capability {} not supported by Rust runtime",
                    name
                ))),
            }
        }

        /// 异步 ask 分发：返回 `asyncio.Future`，不阻塞 Python 主线程。
        #[allow(unused_variables)]
        fn dispatch_ask_async<'py>(
            py: pyo3::Python<'py>,
            name: &str,
            request: &pyo3::Bound<'_, pyo3::PyAny>,
            inner: std::sync::Arc<actant_core::runtime::capability::CapabilityRuntime>,
            tokio_handle: tokio::runtime::Handle,
            gil_thread: &$crate::py::gil_thread::GilThread,
        ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
            match name {
                $(
                    $ask_name => {
                        use $crate::py::types::{PyAskCodec, future_into_py_iter, FutureResultToPy};
                        let req = <$ask_codec as PyAskCodec<$ask_cap>>::decode_request(request)?;
                        future_into_py_iter(
                            py,
                            tokio_handle,
                            gil_thread,
                            async move {
                                let _t = std::time::Instant::now();
                                match inner.ask::<$ask_cap>(req).await {
                                    Ok(resp) => {
                                        let ms = _t.elapsed().as_millis() as u64;
                                        tracing::trace!(cap = $ask_name, ms, "dispatch_ask_async");
                                        pyo3::Python::attach(|py| {
                                            match <$ask_codec as PyAskCodec<$ask_cap>>::encode_response(py, resp) {
                                                Ok(Some(v)) => FutureResultToPy::Value(v.unbind()),
                                                Ok(None) => FutureResultToPy::Value(py.None()),
                                                Err(e) => FutureResultToPy::Err(e),
                                            }
                                        })
                                    }
                                    Err(e) => FutureResultToPy::Err(
                                        $crate::py::error::actant_error_to_pyerr(e)
                                    ),
                                }
                            },
                        )
                    }
                ),*
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "ask_async capability {} not supported by Rust runtime",
                    name
                ))),
            }
        }

        /// 批量 perform 异步分发：接收 `(name, request)` 列表，返回单个
        /// `asyncio.Future`，resolve 为结果列表（顺序与输入一致）。
        ///
        /// # 设计
        ///
        /// 所有 perform 在 tokio 上 `join_all` 并发执行——N 个独立 effect
        /// 的总延迟近似于最慢的一个，而非 N × 单次延迟。这对 Python 侧
        /// 批量提交 effect（如循环 perform Store.put）尤为关键：
        /// - 单次 perform：Python→Rust 边界 + tokio block_on + 编解码 ≈ 10-50µs
        /// - 批量 perform：1 次边界穿越 + N 个 tokio task 并发 + 1 次编解码
        ///
        /// # 错误处理
        ///
        /// 单个 perform 失败不影响其他——失败项在结果列表中对应位置为
        /// `Exception` 实例（而非抛出中断整个批量）。调用方可在 Python 侧
        /// 检查每项类型决定重试策略。
        fn dispatch_perform_batch_async<'py>(
            py: pyo3::Python<'py>,
            items: &pyo3::Bound<'py, pyo3::types::PyList>,
            inner: std::sync::Arc<actant_core::runtime::capability::CapabilityRuntime>,
            tokio_handle: tokio::runtime::Handle,
            gil_thread: &$crate::py::gil_thread::GilThread,
        ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
            use std::future::Future;
            use std::pin::Pin;

            // 类型擦除的 perform future——不同 capability 的 Cap 类型不同，
            // 用 Pin<Box<dyn Future>> 擦除 Cap 类型以统一收集。
            type ErasedPerformFut = Pin<Box<dyn Future<Output = $crate::py::types::FutureResultToPy> + Send>>;

            let mut futures: Vec<ErasedPerformFut> = Vec::with_capacity(items.len());

            // 持 GIL 解码所有请求，构造 future。解码失败立即返回错误，
            // 不提交任何 perform（批量语义为「全有或全无」的提交阶段）。
            for item in items.iter() {
                let pair: pyo3::Bound<'_, pyo3::types::PyTuple> = item
                    .extract()
                    .map_err(|_| {
                        pyo3::exceptions::PyTypeError::new_err(
                            "perform_batch_async: each item must be a (name, request) tuple",
                        )
                    })?;
                if pair.len() != 2 {
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "perform_batch_async: each tuple must have exactly 2 elements (name, request)",
                    ));
                }
                let name_obj = pair.get_item(0)?;
                let name: &str = name_obj.extract()?;
                let request = pair.get_item(1)?;

                match name {
                    $(
                        $perf_name => {
                            use $crate::py::types::PyPerformCodec;
                            let req = <$perf_codec as PyPerformCodec<$perf_cap>>::decode_request(&request)?;
                            let inner_clone = inner.clone();
                            let fut: ErasedPerformFut = Box::pin(async move {
                                let _t = std::time::Instant::now();
                                match inner_clone.perform::<$perf_cap>(req).await {
                                    Ok(resp) => {
                                        let ms = _t.elapsed().as_millis() as u64;
                                        tracing::trace!(cap = $perf_name, ms, "dispatch_perform_batch_async item");
                                        pyo3::Python::attach(|py| {
                                            match <$perf_codec as PyPerformCodec<$perf_cap>>::encode_response(py, resp) {
                                                Ok(v) => $crate::py::types::FutureResultToPy::Value(v.unbind()),
                                                Err(e) => $crate::py::types::FutureResultToPy::Err(e),
                                            }
                                        })
                                    }
                                    Err(e) => $crate::py::types::FutureResultToPy::Err(
                                        $crate::py::error::actant_error_to_pyerr(e)
                                    ),
                                }
                            });
                            futures.push(fut);
                        }
                    ),*
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(format!(
                            "perform_batch_async: capability '{}' not supported by Rust runtime",
                            name
                        )));
                    }
                }
            }

            // join_all 并发执行，收集结果（顺序与输入一致）。
            let batch_fut = async move {
                let results: Vec<$crate::py::types::FutureResultToPy> =
                    futures::future::join_all(futures).await;
                pyo3::Python::attach(|py| {
                    let list = pyo3::types::PyList::empty(py);
                    for result in results {
                        match result {
                            $crate::py::types::FutureResultToPy::Value(v) => {
                                if let Err(e) = list.append(v.bind(py)) {
                                    return $crate::py::types::FutureResultToPy::Err(e);
                                }
                            }
                            $crate::py::types::FutureResultToPy::Err(e) => {
                                let exc = e.value(py).clone();
                                if let Err(e2) = list.append(&exc) {
                                    return $crate::py::types::FutureResultToPy::Err(e2);
                                }
                            }
                        }
                    }
                    $crate::py::types::FutureResultToPy::Value(list.into_any().unbind())
                })
            };

            $crate::py::types::future_into_py_iter(
                py,
                tokio_handle,
                gil_thread,
                batch_fut,
            )
        }
    };
}

/// 本地存储容量快照（PyO3 视图，对应 Rust `StorageStats`）。
///
/// 由 `_RuntimeCore.storage_stats()` 返回，供运维监控与容量规划使用。
#[pyclass(name = "_StoreStats", frozen, module = "actant.actant")]
pub struct PyStoreStats {
    /// 配置的 mmap 上限（虚拟地址预留，不是磁盘预分配）。
    #[pyo3(get)]
    pub map_size: u64,
    /// 不含空闲页的实际数据占用。
    #[pyo3(get)]
    pub used_bytes: u64,
    /// 数据文件高水位（含可复用空闲页）；写满判断以此为准。
    #[pyo3(get)]
    pub file_bytes: u64,
    /// 默认子数据库中的条目数。
    #[pyo3(get)]
    pub entries: usize,
    /// 当前活跃读事务槽位数。
    #[pyo3(get)]
    pub readers: u32,
    /// 读事务槽位上限。
    #[pyo3(get)]
    pub max_readers: u32,
}

#[pymethods]
impl PyStoreStats {
    /// 数据文件高水位占 mmap 上限的比例（`file_bytes / map_size`）。
    ///
    /// advanced 观测辅助：供容量巡检使用，不构成稳定性承诺。
    #[getter]
    fn utilization_ratio(&self) -> f64 {
        if self.map_size == 0 {
            0.0
        } else {
            self.file_bytes as f64 / self.map_size as f64
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "_StoreStats(map_size={}, used_bytes={}, file_bytes={}, entries={}, \
             readers={}/{})",
            self.map_size,
            self.used_bytes,
            self.file_bytes,
            self.entries,
            self.readers,
            self.max_readers
        )
    }
}

impl From<StorageStats> for PyStoreStats {
    fn from(s: StorageStats) -> Self {
        Self {
            map_size: s.map_size,
            used_bytes: s.used_bytes,
            file_bytes: s.file_bytes,
            entries: s.entries,
            readers: s.readers,
            max_readers: s.max_readers,
        }
    }
}

/// 在 Python 模块上注册 `types.rs` 中所有 PyO3 类型。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAsyncAwaitable>()?;
    m.add_class::<PyStoreStats>()?;
    m.add_class::<PyTaskCompletion>()?;
    Ok(())
}
