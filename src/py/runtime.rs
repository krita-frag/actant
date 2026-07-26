//! PyO3 runtime 数据类型与统一运行时核心（轻量边界层）。
//!
//! `src/runtime/`，此处保留：
//! - 跨边界传递的纯数据类型（`PyNode`、`PyTask`）
//! - `_RuntimeCore`：对 `runtime::Runtime` 的薄 PyO3 包装，供 Python 层持有

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use pyo3::prelude::*;

use crate::common::{
    ActantConfig, ActantError, NodeId, RetryPolicy, TaskCompletion, TaskDefinition, TaskId,
    WorkflowId,
};
use crate::runtime::builder::RuntimeBuilder;
use crate::runtime::dispatcher::GENERIC_DISPATCH_NAME;
use crate::runtime::event_bus::{BusEvent, Topic as BusTopic};

use super::capability::PyCapabilityRuntime;
use super::config::PyRetryPolicy;
use super::types::{future_into_py_iter, FutureResultToPy, PyTaskCompletion};

/// 进程级共享 tokio runtime。
///
/// 避免一个 Python 进程内创建多个 `_RuntimeCore` 时产生多个 tokio 线程池。
/// 首个 `_RuntimeCore` 初始化时创建；后续实例复用同一 runtime，
/// 直到最后一个 `_RuntimeCore` 调用 `shutdown()` 才关闭。
///
/// 使用 `OnceLock<Result<_, String>>` 而非 `expect()`：`tokio::runtime::Runtime::new()`
/// 在 OS 资源耗尽（线程上限/OOM）时会失败，此时应将错误传播给调用方而非 panic。
/// `String` 实现了 `Clone`，使 `Result::clone()` 可用，保证 `get_or_init` 的并发安全语义。
static GLOBAL_TOKIO: OnceLock<Result<Arc<tokio::runtime::Runtime>, String>> = OnceLock::new();

fn shared_tokio_runtime() -> PyResult<Arc<tokio::runtime::Runtime>> {
    GLOBAL_TOKIO
        .get_or_init(|| {
            tokio::runtime::Runtime::new()
                .map(Arc::new)
                .map_err(|e| format!("failed to create shared tokio runtime: {e}"))
        })
        .clone()
        .map_err(ActantError::Internal)
        .map_err(PyErr::from)
}

// ---------------------------------------------------------------------------
// 跨 PyO3 边界的 typed struct
// ---------------------------------------------------------------------------

/// DAG 节点定义，由 Python 层构造后通过 `submit_dag` 提交。
#[pyclass(name = "_DagNode", from_py_object)]
#[derive(Clone)]
pub struct PyNode {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub payload: Vec<u8>,
    #[pyo3(get, set)]
    pub retry: Option<PyRetryPolicy>,
    #[pyo3(get, set)]
    pub timeout_ms: Option<u64>,
    #[pyo3(get, set)]
    pub priority: Option<i32>,
    #[pyo3(get, set)]
    pub metadata: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyNode {
    #[new]
    #[pyo3(signature = (name, payload, retry=None, timeout_ms=None, priority=None, metadata=None))]
    fn new(
        name: String,
        payload: Vec<u8>,
        retry: Option<PyRetryPolicy>,
        timeout_ms: Option<u64>,
        priority: Option<i32>,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            name,
            payload,
            retry,
            timeout_ms,
            priority,
            metadata,
        }
    }

    fn __repr__(&self) -> String {
        format!("_DagNode(name={:?})", self.name)
    }
}

/// 任务定义 wire 格式，跨边界传递已就绪/路由后的任务。
#[pyclass(name = "_TaskDef", from_py_object)]
#[derive(Clone)]
pub struct PyTask {
    #[pyo3(get, set)]
    pub task_id: String,
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub payload: Vec<u8>,
    #[pyo3(get, set)]
    pub workflow_id: Option<String>,
    #[pyo3(get, set)]
    pub target_node: Option<String>,
    #[pyo3(get, set)]
    pub target_endpoint_addr: Option<String>,
    #[pyo3(get, set)]
    pub timeout_ms: Option<u64>,
    pub retry_policy: Option<RetryPolicy>,
}

#[pymethods]
impl PyTask {
    #[new]
    #[pyo3(signature = (task_id, name, payload, workflow_id=None, target_node=None, target_endpoint_addr=None, timeout_ms=None, retry_policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        task_id: String,
        name: String,
        payload: Vec<u8>,
        workflow_id: Option<String>,
        target_node: Option<String>,
        target_endpoint_addr: Option<String>,
        timeout_ms: Option<u64>,
        retry_policy: Option<PyRetryPolicy>,
    ) -> Self {
        Self {
            task_id,
            name,
            payload,
            workflow_id,
            target_node,
            target_endpoint_addr,
            timeout_ms,
            retry_policy: retry_policy.map(RetryPolicy::from),
        }
    }

    #[getter]
    fn retry_policy(&self, py: Python<'_>) -> PyResult<Option<Py<PyRetryPolicy>>> {
        match self.retry_policy.as_ref() {
            Some(p) => Ok(Some(Py::new(py, PyRetryPolicy::from(p.clone()))?)),
            None => Ok(None),
        }
    }

    #[setter]
    fn set_retry_policy(&mut self, value: Option<PyRetryPolicy>) {
        self.retry_policy = value.map(RetryPolicy::from);
    }

    fn __repr__(&self) -> String {
        format!("_TaskDef(task_id={:?}, name={:?})", self.task_id, self.name)
    }
}

// ---------------------------------------------------------------------------
// 统一 Runtime 核心：Python 层对 Rust `runtime::Runtime` 的薄包装
// ---------------------------------------------------------------------------

/// 节点监听地址信息（PyO3 暴露给 Python）。
///
/// 由 ``_RuntimeCore.listen_addresses()`` 返回，用于跨节点 dial。
#[pyclass(name = "_ListenAddresses")]
pub struct PyListenAddresses {
    #[pyo3(get)]
    pub endpoint_id: String,
    #[pyo3(get)]
    pub relay_url: Option<String>,
    #[pyo3(get)]
    pub direct_addrs: Vec<String>,
    #[pyo3(get)]
    pub endpoint_addr: String,
}

impl From<crate::runtime::network::ListenAddresses> for PyListenAddresses {
    fn from(a: crate::runtime::network::ListenAddresses) -> Self {
        Self {
            endpoint_id: a.endpoint_id,
            relay_url: a.relay_url,
            direct_addrs: a.direct_addrs,
            endpoint_addr: a.endpoint_addr,
        }
    }
}

/// Python 层统一运行时核心。
///
/// 仅持有一个 `Arc<runtime::Runtime>` 与共享的 tokio runtime，所有子系统访问
/// 都通过 Rust 侧 `Runtime` 完成。capability 视图通过 `capability_runtime()` 获取，
/// 保证 `_CapabilityRuntime` 复用同一套运行时句柄。
#[pyclass(name = "_RuntimeCore")]
pub struct PyRuntimeCore {
    /// `Option` 以便 Drop 时 take 出来，在释放 GIL 的状态下显式 drop。
    /// 否则 PyRuntimeCore drop 时 GIL 被持有，iroh router / actor system 的
    /// Drop 可能阻塞等待 tokio worker，而 worker 的 pyo3_log 回调需要 GIL → 死锁。
    runtime: Option<Arc<crate::runtime::Runtime>>,
    tokio: Mutex<Option<Arc<tokio::runtime::Runtime>>>,
    /// `serve()` spawn 的 worker.run() 任务句柄。shutdown 时先 abort 它，
    /// 避免 worker 循环仍在使用 network 时 endpoint.close() 被调用。
    worker_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 缓存的 endpoint_addr：节点启动后 iroh endpoint 不会变，
    /// 避免每次 submit_task 都重新执行 listen_addresses()（hex + postcard 编码）。
    /// 第一次 submit 时 lazy 计算；后续直接 clone。
    endpoint_addr_cache: parking_lot::Mutex<Option<String>>,
    /// submit_task 的后台投递通道：Python submit 只把 TaskDef 推到 channel 立即返回，
    /// 后台 tokio task 拉取并调用 `scheduler.enqueue().await`，避免每次 submit 都
    /// `tokio.block_on` 跨 GIL 同步阻塞（实测单次 block_on ~12ms）。
    /// `None` 表示 runtime 已 shutdown 或尚未 serve。
    submit_tx: parking_lot::Mutex<Option<tokio::sync::mpsc::UnboundedSender<TaskDefinition>>>,
    /// submit_tasks_batch 的后台投递通道。
    /// 与 `submit_tx` 分离避免单条 submit 与批量 submit 互相阻塞；
    /// 后台 task 批量调用 `scheduler.enqueue_batch().await`，减少 actor 往返次数。
    submit_batch_tx:
        parking_lot::Mutex<Option<tokio::sync::mpsc::UnboundedSender<Vec<TaskDefinition>>>>,
    /// submit 后台 task 句柄；shutdown 时先 close channel 再 await 它。
    submit_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// submit_batch 后台 task 句柄；shutdown 时先 close channel 再 await 它。
    submit_batch_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 共享的 GIL worker 线程，用于把异步结果设置到 Python awaitable/Future。
    /// 在 `_RuntimeCore` 生命周期内保持存活，避免每次跨语言调用都重新竞争 GIL。
    gil_thread: super::gil_thread::GilThread,
    /// 共享的 actor dispatcher 映射。
    /// 由 `PyActorCore` 克隆共享，shutdown 时统一清理，避免 Python 可调用对象泄漏。
    dispatchers: super::actor_ops::SharedDispatchers,
}

#[pymethods]
impl PyRuntimeCore {
    /// 启动统一运行时。
    ///
    /// 参数：
    /// - `name`: 节点 ID 字符串，省略时使用临时名称。
    /// - `data_dir`: 持久化目录，省略时使用系统临时目录下的随机子目录。
    /// - `config`: 配置对象（`PyActantConfig`），省略时使用默认配置。
    #[new]
    #[pyo3(signature = (name=None, data_dir=None, config=None))]
    fn new(
        py: Python<'_>,
        name: Option<String>,
        data_dir: Option<String>,
        config: Option<super::config::PyActantConfig>,
    ) -> PyResult<Self> {
        let t0 = std::time::Instant::now();
        tracing::info!("PyRuntimeCore::new: start");

        // 初始化可观测性子系统：tracing subscriber（受 ACTANT_TRACING 控制）
        // 与 metrics 管道（Prometheus exporter + 可选 OTLP）。
        // 两者均为幂等：多次调用（如一个进程创建多个 _RuntimeCore）会替换之前的管道。
        // 失败不阻断 Runtime 构造——metrics/tracing 不可用不应使节点无法启动；
        // 错误经 tracing::error! 上报，调用方可通过 RUST_LOG=actant=error 观察。
        crate::observability::init();
        if let Err(e) = crate::metrics::init() {
            tracing::error!(error = %e, "metrics::init() failed; Prometheus /metrics endpoint will be empty");
        }

        // 使用进程级共享 tokio runtime，避免多个 `_RuntimeCore` 实例产生冗余线程池。
        let tokio = shared_tokio_runtime()?;
        tracing::info!(
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "shared tokio runtime acquired"
        );

        let node_id = NodeId::new(name.unwrap_or_else(|| "python-node".to_string()));
        // 在释放 GIL 前完成 PyActantConfig → ActantConfig 转换（需访问 Python 对象）。
        let rust_config: ActantConfig = match config {
            Some(c) => ActantConfig::try_from(&c)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            None => ActantConfig::default(),
        };
        tracing::info!(
            discovery_mode = %rust_config.network.discovery_mode.as_str(),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "config resolved"
        );
        let data_dir = data_dir.unwrap_or_else(|| {
            std::env::temp_dir()
                .join(format!(
                    "actant-{}-{}-runtime",
                    node_id,
                    uuid::Uuid::new_v4()
                ))
                .to_string_lossy()
                .to_string()
        });

        // 关键：释放 GIL 后再 block_on，否则 tokio worker 线程的 pyo3_log 回调
        // 需要 GIL，而 MainThread 阻塞在 block_on 持有 GIL → 死锁。
        // 这是 pytest 环境 hang 的根因（pytest 激活 DEBUG 日志使回调更频繁）。
        let t_build = std::time::Instant::now();
        tracing::info!("PyRuntimeCore::new: entering block_on(build) [GIL released]");
        let tokio_for_build = tokio.clone();
        let runtime = py
            .detach(move || {
                tokio_for_build.block_on(async {
                    RuntimeBuilder::new(node_id, rust_config)
                        .with_data_dir(data_dir)
                        .build()
                        .await
                })
            })
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        tracing::info!(
            build_ms = t_build.elapsed().as_millis() as u64,
            total_ms = t0.elapsed().as_millis() as u64,
            "PyRuntimeCore::new: block_on(build) returned"
        );

        Ok(Self {
            runtime: Some(runtime),
            tokio: Mutex::new(Some(tokio)),
            worker_handle: Mutex::new(None),
            endpoint_addr_cache: parking_lot::Mutex::new(None),
            submit_tx: parking_lot::Mutex::new(None),
            submit_batch_tx: parking_lot::Mutex::new(None),
            submit_handle: Mutex::new(None),
            submit_batch_handle: Mutex::new(None),
            gil_thread: super::gil_thread::GilThread::new(),
            dispatchers: Arc::new(dashmap::DashMap::new()),
        })
    }

    /// 返回 capability runtime 视图，与当前核心共享 tokio 与 capability 句柄。
    #[tracing::instrument(name = "py.capability_runtime", level = "debug", skip(self))]
    fn capability_runtime(&self) -> PyResult<PyCapabilityRuntime> {
        let tokio = self
            .tokio
            .lock()
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime shut down"))?;
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("runtime already shut down")
        })?;
        let r = PyCapabilityRuntime::from_runtime(runtime, tokio, self.gil_thread.clone());
        Ok(r)
    }

    /// 返回 actor 系统操作视图，用于直接管理 Rust ActorSystem 中的 actor。
    #[tracing::instrument(name = "py.actor_core", level = "info", skip(self))]
    fn actor_core(&self) -> PyResult<super::actor_ops::PyActorCore> {
        let tokio = self
            .tokio
            .lock()
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime shut down"))?;
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("runtime already shut down")
        })?;
        Ok(super::actor_ops::PyActorCore::new(
            runtime.actor_system().clone(),
            tokio.handle().clone(),
            self.gil_thread.clone(),
            self.dispatchers.clone(),
        ))
    }

    /// 返回节点 ID（Actant 内部标识，可能是用户提供的 name）。
    fn node_id(&self) -> PyResult<String> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("runtime already shut down")
        })?;
        Ok(runtime.node_id().to_string())
    }

    /// 返回 iroh P2P peer ID（公钥 hex），用于 `add_gossip_peer` 与跨节点通信。
    ///
    /// 与 ``node_id`` 的区别：``node_id`` 是 Actant 内部标识，
    /// ``peer_id`` 是 iroh 网络层的公钥标识，传给对端 ``add_gossip_peer``。
    fn peer_id(&self) -> PyResult<String> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("runtime already shut down")
        })?;
        Ok(runtime.network().local_peer_id().to_string())
    }

    /// 返回本节点的监听地址（用于其他节点 dial）。
    ///
    /// ``endpoint_addr`` 是完整的 iroh NodeAddr 编码（hex postcard），
    /// 传给对端 ``dial(endpoint_addr)`` 即可建立连接。
    fn listen_addresses(&self) -> PyResult<PyListenAddresses> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("runtime already shut down")
        })?;
        let addrs = runtime
            .network()
            .listen_addresses()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyListenAddresses::from(addrs))
    }

    /// 拨号远端节点建立直连，并自动将其加入 gossip 网络。
    ///
    /// ``addr`` 为对端 ``listen_addresses()["endpoint_addr"]`` 返回的 hex 字符串。
    /// 连接建立后，本节点会自动加入对端的 gossip topic，开始接收 P2P 消息。
    #[tracing::instrument(name = "py.dial", level = "info", skip(self, py))]
    fn dial(&self, py: Python<'_>, addr: String) -> PyResult<()> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let network = runtime.network().clone();
        py.detach(move || {
            tokio
                .block_on(async move { network.dial(&addr).await })
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(())
    }

    /// ``dial`` 的异步版本，返回 ``asyncio.Future``，可在 Python ``async`` 函数中 ``await``。
    ///
    /// 避免在 Python 事件循环中阻塞主线程——网络 I/O 在 tokio 后台执行，
    /// 完成后通过 GIL worker 线程设置 Future 结果。
    #[pyo3(signature = (addr,))]
    fn dial_async(&self, py: Python<'_>, addr: String) -> PyResult<Py<PyAny>> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let network = runtime.network().clone();
        let gil_thread = self.gil_thread.clone();

        future_into_py_iter(py, tokio.handle().clone(), &gil_thread, async move {
            match network.dial(&addr).await {
                Ok(()) => Python::attach(|py| FutureResultToPy::Value(py.None())),
                Err(e) => Python::attach(|_py| {
                    FutureResultToPy::Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                }),
            }
        })
        .map(|b| b.unbind())
    }

    /// 将远端节点加入 gossip 网络（仅 peer_id，不建立直连）。
    ///
    /// 用于 dial() 之后对端尚未自动加入 gossip topic 时的补充。
    /// ``peer_id`` 为对端 ``node_id()`` 返回的字符串。
    #[tracing::instrument(name = "py.add_gossip_peer", level = "info", skip(self, py))]
    fn add_gossip_peer(&self, py: Python<'_>, peer_id: String) -> PyResult<()> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let network = runtime.network().clone();
        py.detach(move || {
            tokio
                .block_on(async move { network.add_gossip_peer(&peer_id).await })
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(())
    }

    /// ``add_gossip_peer`` 的异步版本，返回 ``asyncio.Future``。
    #[pyo3(signature = (peer_id,))]
    fn add_gossip_peer_async(&self, py: Python<'_>, peer_id: String) -> PyResult<Py<PyAny>> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let network = runtime.network().clone();
        let gil_thread = self.gil_thread.clone();

        future_into_py_iter(py, tokio.handle().clone(), &gil_thread, async move {
            match network.add_gossip_peer(&peer_id).await {
                Ok(()) => Python::attach(|py| FutureResultToPy::Value(py.None())),
                Err(e) => Python::attach(|_py| {
                    FutureResultToPy::Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                }),
            }
        })
        .map(|b| b.unbind())
    }

    /// 返回当前已知对等节点列表（已建立 gossip 邻居关系的节点）。
    fn discover_peers(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let network = runtime.network().clone();
        py.detach(move || {
            tokio
                .block_on(async move { network.discover_peers().await })
                .map(|peers| peers.into_iter().map(|p| p.0.to_string()).collect())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// ``discover_peers`` 的异步版本，返回 ``asyncio.Future``，await 后得到 ``list[str]``。
    fn discover_peers_async(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let network = runtime.network().clone();
        let gil_thread = self.gil_thread.clone();

        future_into_py_iter(py, tokio.handle().clone(), &gil_thread, async move {
            match network.discover_peers().await {
                Ok(peers) => Python::attach(|py| {
                    let list: Vec<String> = peers.into_iter().map(|p| p.0.to_string()).collect();
                    // into_pyobject 在内存耗尽等极端情况返回 PyErr；
                    match list.into_pyobject(py) {
                        Ok(bound) => FutureResultToPy::Value(bound.into_any().unbind()),
                        Err(e) => FutureResultToPy::Err(e),
                    }
                }),
                Err(e) => Python::attach(|_py| {
                    FutureResultToPy::Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                }),
            }
        })
        .map(|b| b.unbind())
    }

    /// 广播跨节点任务取消消息（P2P 取消广播发送端）。
    ///
    /// 通过 Iroh Gossip topic ``actant:cancel`` 广播，接收方 Worker 将事件发布到
    /// event_bus。Python 层目前通过监听 ``TaskLifecycle`` capability 的本地事件响应
    /// 取消；远端取消消息的完整回调链将在任务接入 Rust Worker 调度后自然贯通。
    #[tracing::instrument(name = "py.broadcast_cancel", level = "info", skip(self, py))]
    fn broadcast_cancel(
        &self,
        py: Python<'_>,
        task_id: String,
        workflow_id: String,
    ) -> PyResult<()> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let runtime = runtime.clone();
        py.detach(move || {
            // 取消广播是 fire-and-forget：网络层故障不应阻塞 Python 调用方，
            // cancel_flag 已在本地置位，远端任务最终会通过下次心跳感知。
            let _ = tokio.block_on(async move {
                // 幂等：未订阅时先订阅，确保广播可送达。subscribe 失败
                // （如 gossip actor 已 shutdown）则跳过订阅直接广播。
                let _ = runtime.subscribe_cancel().await;
                runtime.broadcast_cancel(&task_id, &workflow_id).await
            });
        });
        Ok(())
    }

    /// 取消指定任务：将运行中任务的 cancel_flag 置为 true。
    ///
    /// 若任务正在运行，dispatch handler 会在下次协作检查点检测到取消并退出；
    /// 若任务尚未入队或已完成，返回 ``false``。
    #[tracing::instrument(name = "py.cancel_task", level = "info", skip(self))]
    fn cancel_task(&self, task_id: String) -> PyResult<bool> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let worker = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?;
        Ok(worker.cancel_task(&task_id))
    }

    /// 运行时调整 Worker 最大并发任务数（仅支持扩容）。
    ///
    /// Tokio Semaphore 不支持减少 permits，因此缩容请求会被忽略并记录警告日志。
    /// 若需缩容，建议重启 Worker。
    fn set_max_concurrent_tasks(&self, new_max: usize) -> PyResult<()> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let worker = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?;
        worker.set_max_concurrent_tasks(new_max);
        Ok(())
    }

    /// 返回 Worker 当前最大并发任务数。
    fn max_concurrent_tasks(&self) -> PyResult<usize> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let worker = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?;
        Ok(worker.max_concurrent_tasks())
    }

    /// 提交任务到本地 Worker 调度器执行（分布式任务提交入口）。
    ///
    /// 将 ``_TaskDef`` 转换为 Rust ``TaskDefinition`` 并入队到 Worker 的调度器。
    /// 任务由 Worker 的执行循环拉取并调用 ``task_dispatcher.dispatch()`` 执行。
    /// 完成结果通过 event_bus 发布，Python 层通过 ``register_task_result_callback``
    /// 注册的回调接收。
    ///
    /// ``origin_node`` 自动设为本节点 ID，使远程结果投递能找到本节点。
    ///
    /// 性能路径：submit 不再 `block_on(scheduler.enqueue())` 跨 GIL 阻塞，
    /// 而是把 `TaskDefinition` 推到后台 task 的 mpsc channel 立即返回。
    /// `endpoint_addr` 在首次调用时 lazy 缓存（iroh endpoint 启动后不变）。
    #[tracing::instrument(name = "py.submit_task", level = "debug", skip(self, py, task), fields(task_id = %task.task_id))]
    fn submit_task(&self, py: Python<'_>, task: PyTask) -> PyResult<()> {
        crate::metrics::inc_tasks_submitted();
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let node_id = runtime.node_id().clone();

        // endpoint_addr lazy 缓存：节点启动后 iroh endpoint 不会变。
        // 首次调用计算并 cache（hex+postcard 编码 ~50μs），后续直接 clone。
        let endpoint_addr = {
            let mut cache = self.endpoint_addr_cache.lock();
            if let Some(addr) = cache.as_ref() {
                Some(addr.clone())
            } else {
                let addrs = runtime
                    .network()
                    .listen_addresses()
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                *cache = Some(addrs.endpoint_addr.clone());
                Some(addrs.endpoint_addr)
            }
        };

        let task_def = TaskDefinition {
            id: TaskId::new(task.task_id),
            name: task.name,
            payload: task.payload,
            workflow_id: task.workflow_id.map(WorkflowId::from),
            target_node: task.target_node.map(NodeId::new),
            origin_node: Some(node_id),
            retry_policy: task.retry_policy,
            priority: 0,
            timeout_ms: task.timeout_ms,
            attempt: 0,
            enqueued_at_ms: 0,
            target_endpoint_addr: task.target_endpoint_addr,
            origin_endpoint_addr: endpoint_addr,
        };

        // 通过 unbounded channel 投递给后台 task，避免 block_on 跨 GIL 同步阻塞。
        // 后台 task 在 serve() 中 spawn，调用 `scheduler.enqueue().await` 完成 actor 往返。
        let tx_guard = self.submit_tx.lock();
        let tx = tx_guard
            .as_ref()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "submit channel not started; call serve() first",
                )
            })?
            .clone();
        // clone sender 后立即释放 lock。
        drop(tx_guard);
        tx.send(task_def).map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "submit channel closed; runtime may have shut down",
            )
        })?;
        // 让出 GIL 一次给后台 tokio worker 处理 enqueue（协作式调度）。
        // 不阻塞等待 enqueue 完成——后台 task 异步处理，Python 立即返回 handle。
        py.detach(move || {
            std::hint::spin_loop();
        });
        Ok(())
    }

    /// 批量提交任务到本地 Worker 调度器（性能优化路径）。
    ///
    /// 一次性投递多个 `TaskDefinition`，比循环调用 `submit_task` 快 10-50×。
    /// 内部通过专用 channel 把 `Vec<TaskDefinition>` 推到后台 task 异步执行
    /// `scheduler.enqueue_batch().await`，避免 `tokio.block_on` 跨 GIL 同步阻塞。
    /// 仅在批量场景下使用（如 `task.map()` / `gather(*handles)`）。
    #[tracing::instrument(name = "py.submit_tasks_batch", level = "debug", skip(self, py, tasks), fields(n = tasks.len()))]
    fn submit_tasks_batch(&self, py: Python<'_>, tasks: Vec<PyTask>) -> PyResult<()> {
        let n = tasks.len();
        crate::metrics::inc_tasks_submitted_by(n as u64);
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let node_id = runtime.node_id().clone();
        let endpoint_addr = {
            let mut cache = self.endpoint_addr_cache.lock();
            if let Some(addr) = cache.as_ref() {
                addr.clone()
            } else {
                let addrs = runtime
                    .network()
                    .listen_addresses()
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                *cache = Some(addrs.endpoint_addr.clone());
                addrs.endpoint_addr
            }
        };
        let task_defs: Vec<TaskDefinition> = tasks
            .into_iter()
            .map(|t| TaskDefinition {
                id: TaskId::new(t.task_id),
                name: t.name,
                payload: t.payload,
                workflow_id: t.workflow_id.map(WorkflowId::from),
                target_node: t.target_node.map(NodeId::new),
                origin_node: Some(node_id.clone()),
                retry_policy: t.retry_policy,
                priority: 0,
                timeout_ms: t.timeout_ms,
                attempt: 0,
                enqueued_at_ms: 0,
                target_endpoint_addr: t.target_endpoint_addr,
                origin_endpoint_addr: Some(endpoint_addr.clone()),
            })
            .collect();
        // 通过 channel 投递给后台 task，避免 block_on 跨 GIL 同步阻塞。
        // 后台 task 在 serve() 中 spawn，调用 `scheduler.enqueue_batch().await`。
        let tx_guard = self.submit_batch_tx.lock();
        let tx = tx_guard
            .as_ref()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "submit batch channel not started; call serve() first",
                )
            })?
            .clone();
        drop(tx_guard);
        tx.send(task_defs).map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "submit batch channel closed; runtime may have shut down",
            )
        })?;
        // 让出 GIL 一次给后台 tokio worker 处理 enqueue_batch。
        py.detach(move || {
            std::hint::spin_loop();
        });
        Ok(())
    }

    /// 注册 Python 任务结果回调。
    ///
    /// 订阅 event_bus 的 ``TaskCompleted`` / ``TaskFailed`` / ``TaskCancelled`` 话题，
    /// 收到事件时调用 ``callback(py_task_completion)``。回调在 tokio 后台线程执行，
    /// 通过 ``Python::attach`` 获取 GIL。
    ///
    /// 用于分布式任务提交后，Python 层接收 Worker 执行完成的通知并解析 ``AsyncResult``。
    #[tracing::instrument(
        name = "py.register_task_result_callback",
        level = "info",
        skip(self, callback)
    )]
    fn register_task_result_callback(&self, callback: Py<PyAny>) -> PyResult<()> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let event_bus = runtime.event_bus().clone();
        let callback = Arc::new(callback);

        // 订阅 4 个任务生命周期话题（Started + Completed/Failed/Cancelled）
        let mut rx_started = event_bus.subscribe(BusTopic::TaskStarted);
        let mut rx_completed = event_bus.subscribe(BusTopic::TaskCompleted);
        let mut rx_failed = event_bus.subscribe(BusTopic::TaskFailed);
        let mut rx_cancelled = event_bus.subscribe(BusTopic::TaskCancelled);

        tokio.spawn(async move {
            loop {
                let event = tokio::select! {
                    ev = rx_started.recv() => ev,
                    ev = rx_completed.recv() => ev,
                    ev = rx_failed.recv() => ev,
                    ev = rx_cancelled.recv() => ev,
                };
                let Some(event) = event else {
                    break;
                };
                let py_completion = match event {
                    BusEvent::TaskStarted {
                        workflow_id,
                        task_id,
                    } => PyTaskCompletion {
                        workflow_id: workflow_id.as_str().to_string(),
                        task_id: task_id.as_str().to_string(),
                        task_name: String::new(),
                        state: "Running".to_string(),
                        result: None,
                        error: None,
                        target_node: None,
                    },
                    BusEvent::TaskCompleted(c) => task_completion_to_py(&c),
                    BusEvent::TaskFailed(c) => task_completion_to_py(&c),
                    BusEvent::TaskCancelled(c) => task_completion_to_py(&c),
                    _ => continue,
                };
                let cb = callback.clone();
                // 把 Python 回调放到 spawn_blocking 执行，避免慢回调阻塞
                // event_bus 消费和 tokio worker 线程。
                tokio::task::spawn_blocking(move || {
                    Python::attach(|py| {
                        let t0 = std::time::Instant::now();
                        let cb_ref = cb.clone_ref(py);
                        if let Err(e) = cb_ref.call1(py, (py_completion,)) {
                            tracing::warn!("task result callback error: {}", e);
                        }
                        crate::metrics::observe_event_bridge_ms(t0.elapsed().as_millis() as u64);
                    });
                });
            }
        });
        Ok(())
    }

    /// 注册 Python 通用任务分发 handler。
    ///
    /// 将 Python callable 注册为 ``__actant_generic__`` handler 到 TaskRegistry。
    /// Worker 执行任务时通过 ``task_dispatcher.dispatch()`` 调用此 handler，
    /// handler 接收 ``(payload_bytes, cancel_token)`` 并返回结果 bytes。
    ///
    /// handler 签名: ``handler(payload: bytes, cancel_token: CancelToken) -> bytes``
    ///
    /// 优化：handler 通过 `Arc<Py<PyAny>>` 共享，每次调用只需 `Arc::clone`
    /// （非原子 ++）而非 `clone_ref`（Python refcount ++ + 可能的 GIL）。
    /// `handler.bind(py)` 直接借用 Arc，无任何 Python 引用计数操作。
    #[tracing::instrument(
        name = "py.register_python_dispatch_handler",
        level = "info",
        skip(self, handler)
    )]
    fn register_python_dispatch_handler(&self, handler: Py<PyAny>) -> PyResult<()> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let dispatcher = runtime.task_dispatcher().clone();
        // 用 Arc<Py<PyAny>> 包装：dispatch 调用频繁（每个任务一次），
        // 旧实现每次 `handler.clone_ref(py)` 触发 Python refcount ++/--。
        // 改用 Arc 后，dispatch 路径仅 Arc::clone（非原子 ++），完全无 GIL 交互。
        let handler = Arc::new(handler);

        let task_handler: crate::runtime::dispatcher::TaskHandler = Arc::new(
            move |payload: Vec<u8>, cancel_flag: crate::runtime::dispatcher::CancelFlag| {
                let handler = handler.clone();
                // Python dispatch handler 在 task thread pool 中同步调用。
                // 把它移到 tokio spawn_blocking 中执行，避免慢任务阻塞整个
                // task thread pool；如果当前不在 tokio runtime 上则退化到同步调用。
                fn run_python_handler(
                    handler: Arc<Py<PyAny>>,
                    payload: Vec<u8>,
                    cancel_flag: crate::runtime::dispatcher::CancelFlag,
                ) -> Result<Vec<u8>, ActantError> {
                    Python::attach(|py| {
                        // 直接 bind Arc<Py<PyAny>> 借用，无需 clone_ref。
                        // Python refcount 不变，省一次 atomic ++/-- + 可能的 GIL。
                        let handler_bound = handler.bind(py);
                        let token = super::types::PyCancelToken::new(cancel_flag);
                        let token_py = Py::new(py, token).map_err(|e| {
                            ActantError::Internal(format!("create CancelToken: {}", e))
                        })?;
                        let payload_py = pyo3::types::PyBytes::new(py, &payload);
                        let result = handler_bound.call1((payload_py, token_py)).map_err(|e| {
                            ActantError::Internal(format!("python dispatch handler: {}", e))
                        })?;
                        // pyo3 0.29: Bound::extract 不再需要 py 参数（已通过 Bound 的生命周期绑定）。
                        let bytes: &[u8] = result.extract::<&[u8]>().map_err(|e| {
                            ActantError::Internal(format!("python dispatch handler return: {}", e))
                        })?;
                        Ok(bytes.to_vec())
                    })
                }

                match tokio::runtime::Handle::try_current() {
                    Ok(handle) => handle
                        .block_on(tokio::task::spawn_blocking(move || {
                            run_python_handler(handler, payload, cancel_flag)
                        }))
                        .map_err(|e| {
                            ActantError::Internal(format!("spawn_blocking join: {}", e))
                        })?,
                    Err(_) => run_python_handler(handler, payload, cancel_flag),
                }
            },
        );

        dispatcher
            .register_handler(GENERIC_DISPATCH_NAME, task_handler)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    /// 启动 Worker 守护循环（订阅 P2P topic + 任务执行循环）。
    ///
    /// 非阻塞：`worker.run()` 在 tokio runtime 后台 spawn，直到 `shutdown()` 取消。
    /// 用于 CLI `actant worker` 命令——使节点作为后台任务执行器常驻。
    /// 若 worker 未初始化则返回错误。
    #[tracing::instrument(name = "py.serve", level = "info", skip(self, py))]
    fn serve(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))?;
        let tokio = self.tokio.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable")
        })?;
        let worker = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?
            .clone();
        let worker_for_wait = worker.clone();
        let handle = tokio.spawn(async move {
            tracing::info!("worker daemon loop started");
            if let Err(e) = worker.run().await {
                tracing::error!(error = %e, "worker daemon loop exited with error");
            }
            // 无论 run() 成功或失败（如 subscribe_topics 被 cancel），
            // 都确保状态设为 Stopped，使 Runtime::shutdown() 的等待不会超时。
            worker.notify_stopped();
            tracing::info!("worker daemon loop exited");
        });
        *self.worker_handle.lock() = Some(handle);

        // 启动 submit_task 后台投递 task。
        //
        // 关键优化：每次 submit_task 不再 `tokio.block_on(scheduler.enqueue())`
        // 跨 GIL 同步阻塞（实测 ~12ms/op），而是把 TaskDefinition 推到
        // unbounded mpsc channel 立即返回。后台 task 在 tokio runtime 上拉取
        // 并调用 `scheduler.enqueue().await`（actor 消息往返）。
        //
        // 错误处理：enqueue 失败（如 worker 已 shutdown）会 log error 并继续，
        // 下次 submit 时 channel 仍能接收（直到 shutdown 关闭 sender）。
        // 上层 Python 通过 AsyncResult.result(timeout=) 超时感知失败。
        let scheduler_for_submit = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?
            .scheduler_clone();
        let (submit_tx, mut submit_rx) = tokio::sync::mpsc::unbounded_channel::<TaskDefinition>();
        let submit_handle = tokio.spawn(async move {
            while let Some(task_def) = submit_rx.recv().await {
                if let Err(e) = scheduler_for_submit.enqueue(task_def).await {
                    tracing::error!(error = %e, "background submit_task enqueue failed");
                }
            }
        });
        *self.submit_tx.lock() = Some(submit_tx);
        *self.submit_handle.lock() = Some(submit_handle);

        // 启动 submit_tasks_batch 后台投递 task。
        //
        // 与单条 submit 分离，避免批量提交阻塞单条路径。
        // 后台 task 在 tokio runtime 上拉取 `Vec<TaskDefinition>` 并调用
        // `scheduler.enqueue_batch().await`（一次 actor 往返处理多任务）。
        let scheduler_for_batch = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?
            .scheduler_clone();
        let (batch_tx, mut batch_rx) =
            tokio::sync::mpsc::unbounded_channel::<Vec<TaskDefinition>>();
        let batch_handle = tokio.spawn(async move {
            while let Some(task_defs) = batch_rx.recv().await {
                if let Err(e) = scheduler_for_batch.enqueue_batch(task_defs).await {
                    tracing::error!(error = %e, "background submit_tasks_batch enqueue failed");
                }
            }
        });
        *self.submit_batch_tx.lock() = Some(batch_tx);
        *self.submit_batch_handle.lock() = Some(batch_handle);

        // 事件驱动等待 Worker 进入任务执行循环（``run()`` 完成初始化）。
        // ``wait_for_ready`` 基于 ``watch`` channel 的状态变更，
        // 无轮询、无固定延迟；当 ``run()`` 设就绪标志后立即返回，
        // spawn 失败时 ``watch::Sender`` drop 使 ``changed()`` 返回 Err 也会解除阻塞。
        // 释放 GIL 以避免与 tokio worker 的 pyo3_log 回调死锁。
        py.detach(move || {
            // wait_for_ready 返回 Err 仅在 Worker spawn 失败导致 watch sender drop，
            // 此时 daemon loop 已 log 错误，serve() 返回 Ok 让调用方继续；后续 submit
            // 会因 worker 未就绪而显式失败，错误路径不会被掩盖。
            let _ = tokio.block_on(worker_for_wait.wait_for_ready());
        });
        Ok(())
    }

    /// 停止运行时，释放 tokio runtime 并触发后台任务优雅退出。
    ///
    /// 关闭顺序：
    /// 1. 关闭 submit/submit_batch channel，等待后台 task 退出；
    /// 2. 清理 dispatchers 映射（M1：避免 Python 可调用对象泄漏）；
    /// 3. 在 tokio runtime 上执行 `Runtime::shutdown()`（停止 Actor 并
    ///    关闭 iroh endpoint，避免 `Endpoint` 被无声 drop 触发 iroh 的
    ///    "Aborting ungracefully" 警告）；
    /// 4. `shutdown_timeout` 关闭 tokio runtime。超时时间内无法完成则强制关闭。
    /// 重复调用为幂等。
    #[pyo3(signature = (timeout_ms = 5000))]
    #[tracing::instrument(name = "py.shutdown", level = "info", skip(self, py), fields(timeout_ms = timeout_ms))]
    fn shutdown(&self, py: Python<'_>, timeout_ms: u64) {
        let t0 = std::time::Instant::now();
        // 先关闭 submit channel：drop sender 让后台 task 退出 recv 循环。
        if let Some(tx) = self.submit_tx.lock().take() {
            drop(tx);
        }
        // 关闭 submit_batch channel：同样 drop sender 让后台 task 退出。
        if let Some(tx) = self.submit_batch_tx.lock().take() {
            drop(tx);
        }
        // 在 tokio runtime 上等待 submit 后台 task 退出（最多 500ms）。
        if let Some(handle) = self.submit_handle.lock().take() {
            let tokio_opt = self.tokio.lock().clone();
            if let Some(tokio) = tokio_opt {
                tokio.block_on(async {
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_millis(500), handle).await;
                });
            }
        }
        // 等待 submit_batch 后台 task 退出（最多 500ms）。
        if let Some(handle) = self.submit_batch_handle.lock().take() {
            let tokio_opt = self.tokio.lock().clone();
            if let Some(tokio) = tokio_opt {
                tokio.block_on(async {
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_millis(500), handle).await;
                });
            }
        }
        // M1：清理 dispatchers 映射。释放所有 Python dispatcher 对象的引用，
        // 避免 actor shutdown 后 Python 可调用对象常驻内存泄漏。
        // 必须在持 GIL 状态下执行（Py<PyAny> drop 触发 Python refcount --）。
        // DashMap::clear 内部逐个 remove，每个 remove 触发 Py<PyAny> drop。
        // 当前线程持 GIL（Python 主线程 shutdown 路径），drop 安全。
        let dispatcher_count = self.dispatchers.len();
        if dispatcher_count > 0 {
            tracing::info!(
                count = dispatcher_count,
                "clearing dispatchers map during shutdown"
            );
            self.dispatchers.clear();
        }
        let mut guard = self.tokio.lock();
        if let Some(tokio) = guard.take() {
            // 1. 在 tokio runtime 上执行 ActantRuntime::shutdown()：停止 Actor
            //    并关闭 iroh endpoint（endpoint.close()）。必须在 tokio runtime
            //    关闭前完成，否则 close 无 reactor 可用 → Endpoint 被无声 drop。
            if let Some(rt) = self.runtime.as_ref() {
                let rt = rt.clone();
                let tokio_for_shutdown = tokio.clone();
                // 释放 GIL：shutdown 内 Actor 停止 / endpoint.close 可能触发
                // tokio worker 的 pyo3_log 回调（需 GIL），MainThread 阻塞在
                // block_on 持有 GIL 会死锁。
                // Runtime::shutdown() 内部会先 worker.shutdown()（发 cancel），
                // 等 worker.run() 循环退出（状态→Stopped）再 network.shutdown()
                // （endpoint.close）。serve() spawn 的 worker.run() 任务会在
                // cancel 信号下优雅退出。给整体设 5s 上限，超时则由后续
                // shutdown_timeout 强制关闭 tokio runtime。
                py.detach(move || {
                    tokio_for_shutdown.block_on(async {
                        // 15s 软超时：超时表示 shutdown 流程卡住（如远端 actor
                        // 未响应或 iroh endpoint.close 阻塞），此时不阻塞 Python
                        // 主线程，由后续 shutdown_timeout 强制 drop tokio runtime 收尾。
                        // 5s 不足以覆盖多 actor stop（每个 500ms）+ network.shutdown()
                        // （endpoint.close 含 QUIC 连接 drain），导致 endpoint 被强 drop
                        // 影响后续测试的 iroh 资源释放。15s 在 loopback 上足够完成
                        // 优雅关闭，慢机/CI 仍有 shutdown_timeout 兜底。
                        // 丢弃 timeout Err 是有意为之：超时路径已 log。
                        let _ =
                            tokio::time::timeout(std::time::Duration::from_secs(15), rt.shutdown())
                                .await;
                    });
                });
            }
            drop(guard);
            // 2. 关闭 tokio runtime（超时强制）。
            // 如果是进程级共享 runtime，GLOBAL_TOKIO 持有强引用，try_unwrap 不会成功，
            // 此时只关闭 ActantRuntime，tokio runtime 随进程结束自然回收。
            if let Ok(runtime) = Arc::try_unwrap(tokio) {
                py.detach(move || {
                    runtime.shutdown_timeout(std::time::Duration::from_millis(timeout_ms));
                });
            } else {
                tracing::info!("shared tokio runtime still in use by other instances, skipping shutdown_timeout");
            }
        }
        tracing::info!(
            shutdown_ms = t0.elapsed().as_millis() as u64,
            "PyRuntimeCore::shutdown done"
        );
    }
}

impl Drop for PyRuntimeCore {
    fn drop(&mut self) {
        tracing::debug!("PyRuntimeCore dropped");
        // 显式 take 出 Arc<Runtime> 并在释放 GIL 的状态下 drop。
        // 否则 iroh router / actor system 的 Drop 会阻塞等待 tokio worker，
        // 而 worker 的 pyo3_log 回调需要 GIL（此时被 Drop 持有）→ 死锁。
        // 这是 pytest 环境 test teardown 阶段 hang 的根因。
        if let Some(runtime) = self.runtime.take() {
            // Python::attach 在 PyO3 pyclass Drop 中安全：若 GIL 已持有则复用，
            // 否则获取。随后 detach 释放 GIL 执行重资源 drop。
            Python::attach(|py| {
                py.detach(move || {
                    drop(runtime);
                });
            });
        }
    }
}

/// 将 Rust ``TaskCompletion`` 转换为 Python ``_TaskCompletion``。
fn task_completion_to_py(completion: &TaskCompletion) -> PyTaskCompletion {
    match completion {
        TaskCompletion::Completed {
            workflow_id,
            task_id,
            task_name,
            result,
            target_node,
        } => PyTaskCompletion {
            workflow_id: workflow_id.as_str().to_string(),
            task_id: task_id.as_str().to_string(),
            task_name: task_name.clone(),
            state: "Completed".to_string(),
            result: Some(result.clone()),
            error: None,
            target_node: target_node.as_ref().map(|n| n.as_str().to_string()),
        },
        TaskCompletion::Failed {
            workflow_id,
            task_id,
            task_name,
            error,
            target_node,
        } => PyTaskCompletion {
            workflow_id: workflow_id.as_str().to_string(),
            task_id: task_id.as_str().to_string(),
            task_name: task_name.clone(),
            state: "Failed".to_string(),
            result: None,
            error: Some(error.clone()),
            target_node: target_node.as_ref().map(|n| n.as_str().to_string()),
        },
        TaskCompletion::Cancelled {
            workflow_id,
            task_id,
            task_name,
            target_node,
        } => PyTaskCompletion {
            workflow_id: workflow_id.as_str().to_string(),
            task_id: task_id.as_str().to_string(),
            task_name: task_name.clone(),
            state: "Cancelled".to_string(),
            result: None,
            error: None,
            target_node: target_node.as_ref().map(|n| n.as_str().to_string()),
        },
        TaskCompletion::Skipped {
            workflow_id,
            task_id,
            task_name,
            target_node,
        } => PyTaskCompletion {
            workflow_id: workflow_id.as_str().to_string(),
            task_id: task_id.as_str().to_string(),
            task_name: task_name.clone(),
            state: "Skipped".to_string(),
            result: None,
            error: None,
            target_node: target_node.as_ref().map(|n| n.as_str().to_string()),
        },
    }
}

/// 在 Python 模块上注册 runtime 数据类型与核心。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyNode>()?;
    m.add_class::<PyTask>()?;
    m.add_class::<PyListenAddresses>()?;
    m.add_class::<PyRuntimeCore>()?;
    m.add_function(pyo3::wrap_pyfunction!(prometheus_text, m)?)?;
    Ok(())
}

/// 返回所有已注册指标的 Prometheus exposition format 文本。
///
/// 用于 `actant worker --metrics-port` 启动的 HTTP 端点抓取，或供
/// 用户在自己的 HTTP 服务器中直接暴露。若未调用 `metrics::init()`
/// （如纯 Python 测试场景），返回空字符串。
#[pyfunction]
fn prometheus_text() -> String {
    crate::metrics::prometheus_text()
}
