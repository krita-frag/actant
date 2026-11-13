//! PyO3 runtime 数据类型与统一运行时核心（轻量边界层）。
//!
//! `src/runtime/`，此处保留：
//! - 跨边界传递的纯数据类型（`PyNode`、`PyTask`）
//! - `_RuntimeCore`：对 `runtime::Runtime` 的薄 PyO3 包装，供 Python 层持有

use super::error::actant_error_to_pyerr;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use pyo3::prelude::*;

use actant_core::common::{
    decode_blob_ref, encode_blob_ref, ActantConfig, ActantError, BlobRef, JournalRecord, NodeId,
    NodeStateUpdate, RetryPolicy, TaskCompletion, TaskDefinition, TaskId, WorkflowId,
    WorkflowInvocation,
};
use actant_core::runtime::builder::RuntimeBuilder;
use actant_core::runtime::event_bus::{BusEvent, Topic as BusTopic};
use actant_core::runtime::workflow::actor::TaskResultOutcome;
use actant_core::runtime::workflow::messaging::{decode, encode};
use actant_core::runtime::workflow::orchestrator::types::JournalEvent;
use actant_core::runtime::workflow::orchestrator::DagSnapshot;
use actant_core::runtime::workflow::{
    workflow_methods, AddNodeOutcome, Dag, DagNode, FailureStrategy, WaitCondition,
    WorkflowExecution,
};

use super::capability::PyCapabilityRuntime;
use super::config::PyRetryPolicy;
use super::route_policy::PyRoutePolicy;
use super::types::{future_into_py_iter, FutureResultToPy, PyStoreStats, PyTaskCompletion};

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
        .map_err(actant_error_to_pyerr)
}

/// 取本节点 FailoverManager（故障注入钩子入口）。
///
/// 返回 `Result` 的 PyO3 方法不能直接返回 `Arc<FailoverManager>`（非 Python 对象），
/// 故以自由函数形式在内部使用。
fn fault_injection_target(
    runtime: Option<&Arc<actant_core::runtime::Runtime>>,
) -> PyResult<Arc<actant_core::runtime::workflow::failover::FailoverManager>> {
    runtime
        .and_then(|rt| rt.failover_manager())
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime has no failover manager"))
}

/// 按名称解析内置远端路由策略。
///
/// `None` / `"default"` 使用 Worker 内置 [`DefaultRoutePolicy`]；
/// `"label_affinity"` 使用 [`LabelAffinityRoutePolicy`]（本地满足诉求即本地执行，
/// 否则转发标签匹配且槽位最多的 peer）。未知名称在启动时拒绝，避免静默回退。
fn route_policy_by_name(
    name: Option<&str>,
) -> PyResult<Option<Arc<dyn actant_core::runtime::workflow::RoutePolicy>>> {
    use actant_core::runtime::workflow::LabelAffinityRoutePolicy;

    match name {
        None | Some("") | Some("default") => Ok(None),
        Some("label_affinity") => Ok(Some(Arc::new(LabelAffinityRoutePolicy))),
        Some(other) => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown route_policy '{other}': expected 'default' or 'label_affinity'"
        ))),
    }
}

/// 解析 `route_policy` 参数：字符串走内置策略名，callable 走 Python 桥。
///
/// 两者都不匹配时报 `TypeError` 而不是静默回退——静默回退会让用户的策略
/// 「看起来生效了但实际没跑」，是路由问题里最难排查的一类。
fn resolve_route_policy(
    py: Python<'_>,
    value: Option<Py<PyAny>>,
) -> PyResult<Option<Arc<dyn actant_core::runtime::workflow::RoutePolicy>>> {
    use actant_core::runtime::workflow::DefaultRoutePolicy;

    let Some(value) = value else {
        return Ok(None);
    };
    let bound = value.bind(py);
    if bound.is_none() {
        return Ok(None);
    }
    if let Ok(name) = bound.extract::<String>() {
        return route_policy_by_name(Some(&name));
    }
    if bound.is_callable() {
        // 回退策略与「不注入」时 Worker 使用的内置默认一致。
        let fallback: Arc<dyn actant_core::runtime::workflow::RoutePolicy> =
            Arc::new(DefaultRoutePolicy);
        return Ok(Some(Arc::new(PyRoutePolicy::new(
            value.clone_ref(py),
            fallback,
        ))));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "route_policy must be a string ('default' / 'label_affinity') \
         or a callable (ctx, candidates) -> decision",
    ))
}

// ---------------------------------------------------------------------------
// 跨 PyO3 边界的 typed struct
// ---------------------------------------------------------------------------

/// DAG 节点定义，由 Python 层构造后通过 `add_workflow_node` 提交。
///
/// `task_id` 是节点在 DAG 中的唯一标识（对应 Orchestrator 侧 `TaskId`）；
/// `name` 为人类可读名称。
#[pyclass(name = "_DagNode", from_py_object)]
#[derive(Clone)]
pub struct PyNode {
    #[pyo3(get, set)]
    pub task_id: String,
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
    /// 任务放置诉求（标签选择器），映射到 `DagNode::tags`。
    #[pyo3(get, set)]
    pub tags: Vec<String>,
    /// 任务所需的命名配额（单维），映射到 `DagNode::quota`。
    #[pyo3(get, set)]
    pub quota: Option<String>,
}

#[pymethods]
impl PyNode {
    #[new]
    #[pyo3(signature = (task_id, name, payload, timeout_ms=None, priority=None, tags=None, quota=None))]
    fn new(
        task_id: String,
        name: String,
        payload: Vec<u8>,
        timeout_ms: Option<u64>,
        priority: Option<i32>,
        tags: Option<Vec<String>>,
        quota: Option<String>,
    ) -> Self {
        Self {
            task_id,
            name,
            payload,
            retry: None,
            timeout_ms,
            priority,
            metadata: None,
            tags: tags.unwrap_or_default(),
            quota,
        }
    }

    fn __repr__(&self) -> String {
        format!("_DagNode(task_id={:?}, name={:?})", self.task_id, self.name)
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
    /// 任务放置诉求（标签选择器），映射到 `TaskDefinition::tags`。
    #[pyo3(get, set)]
    pub tags: Vec<String>,
    /// 任务所需的命名配额（单维），映射到 `TaskDefinition::quota`。
    #[pyo3(get, set)]
    pub quota: Option<String>,
    /// 任务优先级（越大越优先），映射到 `TaskDefinition::priority`。
    #[pyo3(get, set)]
    pub priority: i32,
    /// `target_node` 是否为调用方的持久指定（`submit_to` 路径）。
    ///
    /// 构造时由 `target_node` 推断，只读：运行期修改 `target_node`
    /// （如路由器选择）不应改变调用方的放置语义。
    #[pyo3(get)]
    pub pinned_target: bool,
    pub retry_policy: Option<RetryPolicy>,
}

#[pymethods]
impl PyTask {
    #[new]
    #[pyo3(signature = (task_id, name, payload, workflow_id=None, target_node=None, target_endpoint_addr=None, timeout_ms=None, tags=None, quota=None, priority=None, retry_policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        task_id: String,
        name: String,
        payload: Vec<u8>,
        workflow_id: Option<String>,
        target_node: Option<String>,
        target_endpoint_addr: Option<String>,
        timeout_ms: Option<u64>,
        tags: Option<Vec<String>>,
        quota: Option<String>,
        priority: Option<i32>,
        retry_policy: Option<PyRetryPolicy>,
    ) -> Self {
        // Python 层传入的 `target_node` 只来自 `Task.submit_to` / 显式
        // `target_node` 参数，语义是调用方持久指定；路由器选择的目标不经过
        // 此构造器。
        let pinned_target = target_node.is_some();
        Self {
            task_id,
            name,
            payload,
            workflow_id,
            target_node,
            target_endpoint_addr,
            timeout_ms,
            tags: tags.unwrap_or_default(),
            quota,
            priority: priority.unwrap_or(0),
            pinned_target,
            retry_policy: retry_policy.map(RetryPolicy::from),
        }
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

impl From<actant_core::runtime::network::ListenAddresses> for PyListenAddresses {
    fn from(a: actant_core::runtime::network::ListenAddresses) -> Self {
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
    runtime: Option<Arc<actant_core::runtime::Runtime>>,
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
    /// `register_task_result_callback` spawn 的事件消费 task 句柄列表。
    ///
    /// 每次调用 `register_task_result_callback` 都会 spawn 一个独立的 tokio
    /// task 订阅 event_bus 4 个 task 生命周期 topic 并回调 Python callable。
    /// 句柄必须跟踪至 shutdown：abort + 限时 await，
    /// 1. shutdown 时这些 task 仍持有 `Py<PyAny>` callback 引用，阻止 Python
    ///    端对象 GC，直到 tokio runtime 关闭强制 abort。
    /// 2. 多次注册产生多个孤儿 task 同时回调同一事件，行为难以预测。
    /// 3. event_bus drop 后 recv 返回 None 才退出，但 callback Arc 引用
    ///    在 task 内部循环，无法被释放。
    ///
    /// shutdown 时统一 abort + 短超时 await，确保 callback 引用及时释放。
    task_result_callback_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// 共享的 GIL worker 线程，用于把异步结果设置到 Python awaitable/Future。
    /// 在 `_RuntimeCore` 生命周期内保持存活，避免每次跨语言调用都重新竞争 GIL。
    gil_thread: super::gil_thread::GilThread,
    /// payload 签名密钥（来自 `ActantConfig::payload_signing_key`）。
    ///
    /// 直接 `@task` 提交不经过 orchestrator，须在此处签名，否则
    /// `ProcessTaskDispatcher` 在启用签名时会拒绝未签名载荷。
    payload_signing_key: Vec<u8>,
}

/// 按配置的共享密钥签名任务 payload。
///
/// 空密钥时 `sign` 原样返回（禁用签名，开发/测试场景）。
fn sign_payload(key: &[u8], payload: Vec<u8>) -> PyResult<Vec<u8>> {
    actant_core::common::payload::sign(key, &payload)
        .map_err(|e| actant_error_to_pyerr(ActantError::Internal(format!("payload sign: {e}"))))
}

impl PyRuntimeCore {
    /// 取已启动的运行时；未启动时返回 Python `RuntimeError`。
    fn core(&self) -> PyResult<&Arc<actant_core::runtime::Runtime>> {
        self.runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime not started"))
    }

    /// 取进程级 tokio runtime 句柄；不可用时返回 Python `RuntimeError`。
    fn tokio_handle(&self) -> PyResult<Arc<tokio::runtime::Runtime>> {
        self.tokio
            .lock()
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("tokio runtime unavailable"))
    }
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
    #[pyo3(signature = (name=None, data_dir=None, config=None, route_policy=None))]
    fn new(
        py: Python<'_>,
        name: Option<String>,
        data_dir: Option<String>,
        config: Option<super::config::PyActantConfig>,
        route_policy: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let t0 = std::time::Instant::now();
        tracing::info!("PyRuntimeCore::new: start");

        // 初始化可观测性子系统：tracing subscriber（受 ACTANT_TRACING 控制）
        // 与 metrics 管道（Prometheus exporter + 可选 OTLP）。
        // 两者均为幂等：多次调用（如一个进程创建多个 _RuntimeCore）会替换之前的管道。
        // 失败不阻断 Runtime 构造——metrics/tracing 不可用不应使节点无法启动；
        // 错误经 tracing::error! 上报，调用方可通过 RUST_LOG=actant=error 观察。
        actant_core::observability::init();
        if let Err(e) = actant_core::metrics::init() {
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
        let route_policy = resolve_route_policy(py, route_policy)?;
        let payload_signing_key = rust_config.payload_signing_key.clone();
        // 解析优先级：显式 `data_dir` 参数 > `config.store.data_dir` > 临时目录。
        // 前者是 Runtime 级覆盖，后者让 `_StoreConfig` 可独立承载持久化目录。
        let config_data_dir = rust_config.store.data_dir.clone();
        let data_dir = data_dir.or(config_data_dir).unwrap_or_else(|| {
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
                    let mut builder =
                        RuntimeBuilder::new(node_id, rust_config).with_data_dir(data_dir);
                    if let Some(policy) = route_policy {
                        builder = builder.with_route_policy(policy);
                    }
                    builder.build().await
                })
            })
            .map_err(actant_error_to_pyerr)?;
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
            task_result_callback_handles: Mutex::new(Vec::new()),
            gil_thread: super::gil_thread::GilThread::new(),
            payload_signing_key,
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
            .map_err(actant_error_to_pyerr)?;
        Ok(PyListenAddresses::from(addrs))
    }

    /// 返回本地存储容量与使用情况快照。
    ///
    /// 供运维周期性采样；启动时已同步上报一次 ``actant.store.*`` 指标。
    fn storage_stats(&self, py: Python<'_>) -> PyResult<PyStoreStats> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("runtime already shut down")
        })?;
        let tokio = self
            .tokio
            .lock()
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime shut down"))?;
        let store = runtime.store().clone();
        let stats = py
            .detach(move || tokio.block_on(store.storage_stats()))
            .map_err(actant_error_to_pyerr)?;
        Ok(PyStoreStats::from(stats))
    }

    /// 在线热备份本地存储到指定文件，返回备份文件字节数。
    ///
    /// 备份启用 compaction，会顺带回收空闲页；可在运行中调用。
    fn backup_to(&self, py: Python<'_>, path: String) -> PyResult<u64> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("runtime already shut down")
        })?;
        let tokio = self
            .tokio
            .lock()
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime shut down"))?;
        let store = runtime.store().clone();
        let path = std::path::PathBuf::from(path);
        py.detach(move || tokio.block_on(store.backup_to(path)))
            .map_err(actant_error_to_pyerr)
    }

    /// 拨号远端节点建立直连，并自动将其加入 gossip 网络。
    ///
    /// ``addr`` 为对端 ``listen_addresses()["endpoint_addr"]`` 返回的 hex 字符串。
    /// 连接建立后，本节点会自动加入对端的 gossip topic，开始接收 P2P 消息。
    #[tracing::instrument(name = "py.dial", level = "info", skip(self, py))]
    fn dial(&self, py: Python<'_>, addr: String) -> PyResult<()> {
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let network = runtime.network().clone();
        py.detach(move || {
            tokio
                .block_on(async move { network.dial(&addr).await })
                .map_err(actant_error_to_pyerr)
        })?;
        Ok(())
    }

    /// 将远端节点加入 gossip 网络（仅 peer_id，不建立直连）。
    ///
    /// 用于 dial() 之后对端尚未自动加入 gossip topic 时的补充。
    /// ``peer_id`` 为对端 ``node_id()`` 返回的字符串。
    #[tracing::instrument(name = "py.add_gossip_peer", level = "info", skip(self, py))]
    fn add_gossip_peer(&self, py: Python<'_>, peer_id: String) -> PyResult<()> {
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let network = runtime.network().clone();
        py.detach(move || {
            tokio
                .block_on(async move { network.add_gossip_peer(&peer_id).await })
                .map_err(actant_error_to_pyerr)
        })?;
        Ok(())
    }

    /// 返回当前已知对等节点列表（已建立 gossip 邻居关系的节点）。
    fn discover_peers(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let network = runtime.network().clone();
        py.detach(move || {
            tokio
                .block_on(async move { network.discover_peers().await })
                .map(|peers| peers.into_iter().map(|p| p.0.to_string()).collect())
                .map_err(actant_error_to_pyerr)
        })
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
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let runtime = runtime.clone();
        let task_id_for_log = task_id.clone();
        let workflow_id_for_log = workflow_id.clone();
        py.detach(move || {
            // 取消广播是 fire-and-forget：网络层故障不应阻塞 Python 调用方，
            // cancel_flag 已在本地置位，远端任务最终会通过下次心跳感知。
            // 语义保留，但失败路径必须可观测——否则远端节点收不到取消只能
            // 等待心跳租约过期，排查时无从下手。
            let result = tokio.block_on(async move {
                // 幂等：未订阅时先订阅，确保广播可送达。subscribe 失败
                // （如 gossip actor 已 shutdown）则跳过订阅直接广播。
                if let Err(e) = runtime.subscribe_cancel().await {
                    tracing::debug!(
                        error = %e,
                        task_id = %task_id,
                        "subscribe_cancel failed before cancel broadcast; broadcasting without subscription"
                    );
                }
                runtime.broadcast_cancel(&task_id, &workflow_id).await
            });
            if let Err(e) = result {
                tracing::warn!(
                    error = %e,
                    task_id = %task_id_for_log,
                    workflow_id = %workflow_id_for_log,
                    "cancel broadcast failed; remote nodes will not observe this cancellation until the next heartbeat/lease expiry"
                );
            }
        });
        Ok(())
    }

    /// 取消指定任务：将运行中任务的 cancel_flag 置为 true。
    ///
    /// 若任务正在运行，dispatch handler 会在下次协作检查点检测到取消并退出；
    /// 若任务尚未入队或已完成，返回 ``false``。
    #[tracing::instrument(name = "py.cancel_task", level = "info", skip(self))]
    fn cancel_task(&self, task_id: String) -> PyResult<bool> {
        let runtime = self.core()?;
        let worker = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?;
        Ok(worker.cancel_task(&task_id))
    }

    /// 运行时更新节点状态：标签 / 配额容量 / draining。
    ///
    /// 任一参数为 ``None`` 表示保持当前值。标签随下一次心跳广播给集群；
    /// ``draining=True`` 停止接纳新任务并让远端路由跳过本节点，在途任务
    /// 继续完成；``draining=False`` 恢复。
    #[pyo3(signature = (labels=None, quotas=None, draining=None))]
    fn update_node_state(
        &self,
        labels: Option<BTreeMap<String, String>>,
        quotas: Option<BTreeMap<String, u32>>,
        draining: Option<bool>,
    ) -> PyResult<()> {
        let runtime = self.core()?;
        let update = NodeStateUpdate {
            labels,
            quotas,
            draining,
        };
        runtime
            .update_node_state(&update)
            .map_err(actant_error_to_pyerr)
    }

    /// 运行时调整 Worker 最大并发任务数（仅支持扩容）。
    ///
    /// Tokio Semaphore 不支持减少 permits，因此缩容请求会被忽略并记录警告日志。
    /// 若需缩容，建议重启 Worker。
    fn set_max_concurrent_tasks(&self, new_max: usize) -> PyResult<()> {
        let runtime = self.core()?;
        let worker = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?;
        worker.set_max_concurrent_tasks(new_max);
        Ok(())
    }

    /// 返回 Worker 当前最大并发任务数。
    fn max_concurrent_tasks(&self) -> PyResult<usize> {
        let runtime = self.core()?;
        let worker = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?;
        Ok(worker.max_concurrent_tasks())
    }

    /// 将字节存入本节点内容寻址 blob 存储，返回 `BlobRef` wire 编码。
    ///
    /// `BlobRef.node` 记为本节点 endpoint_addr，供跨节点 [`Self::value_fetch`]
    /// 寻址。经 `ValueStore` capability 的默认 handler 调用。
    #[tracing::instrument(name = "py.value_store", level = "debug", skip(self, py, data))]
    fn value_store(&self, py: Python<'_>, data: Vec<u8>) -> PyResult<Vec<u8>> {
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let network = runtime.network().clone();
        let node = self.local_endpoint_addr(py)?;
        py.detach(move || {
            tokio.block_on(async move {
                let hash = network.blob_store(data).await?;
                encode_blob_ref(&BlobRef {
                    hash,
                    node: NodeId::new(node),
                })
            })
        })
        .map_err(actant_error_to_pyerr)
    }

    /// 按 `BlobRef` wire 编码取回值字节。
    ///
    /// 解析顺序：先本地 blob store（内容寻址下本地命中即真——Ref 在 blob 所属
    /// 节点上解析零网络；本地未命中的 `get_bytes` Err 是预期的未命中探测，
    /// 不是吞错误），未命中再按 `ref.node` 跨节点流式拉取（逐 leaf 已校验）。
    #[tracing::instrument(name = "py.value_fetch", level = "debug", skip(self, py, ref_bytes))]
    fn value_fetch(&self, py: Python<'_>, ref_bytes: Vec<u8>) -> PyResult<Vec<u8>> {
        let r = decode_blob_ref(&ref_bytes).map_err(actant_error_to_pyerr)?;
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let network = runtime.network().clone();
        py.detach(move || {
            tokio.block_on(async move {
                if let Some(store) = network.blobs() {
                    if let Ok(data) = store.get_bytes(&r.hash).await {
                        return Ok(data);
                    }
                }
                let mut fetch = network.blob_fetch(&r.node, r.hash).await?;
                let mut data = Vec::new();
                while let Some(chunk) = fetch.next_chunk().await {
                    match chunk {
                        Ok(bytes) => data.extend_from_slice(&bytes),
                        Err(e) => {
                            fetch.close();
                            return Err(e);
                        }
                    }
                }
                fetch.close();
                Ok(data)
            })
        })
        .map_err(actant_error_to_pyerr)
    }

    /// 解码 `BlobRef` wire 编码为 ``(hash_hex, node)``。
    ///
    /// 供 Python `Ref.hash` / `.node` 展示使用，避免在 Python 侧引入 postcard
    /// 解码器。
    fn value_ref_parts(&self, ref_bytes: Vec<u8>) -> PyResult<(String, String)> {
        let r = decode_blob_ref(&ref_bytes).map_err(actant_error_to_pyerr)?;
        Ok((r.hash.to_string(), r.node.as_str().to_string()))
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
    /// 性能路径：submit 把 `TaskDefinition` 推到后台 task 的 mpsc channel
    /// 后立即返回，避免 `block_on(scheduler.enqueue())` 跨 GIL 阻塞。
    /// `endpoint_addr` 在首次调用时 lazy 缓存（iroh endpoint 启动后不变）。
    #[tracing::instrument(name = "py.submit_task", level = "debug", skip(self, py, task), fields(task_id = %task.task_id))]
    fn submit_task(&self, py: Python<'_>, task: PyTask) -> PyResult<()> {
        actant_core::metrics::inc_tasks_submitted();
        let runtime = self.core()?;
        let node_id = runtime.node_id().clone();

        // endpoint_addr lazy 缓存：节点启动后 iroh endpoint 不会变。
        let endpoint_addr = self.local_endpoint_addr(py)?;
        // 直接任务提交不经过 orchestrator，签名必须在此完成，
        // 否则启用 payload 签名的 Worker 会拒绝未签名载荷。
        let payload = sign_payload(&self.payload_signing_key, task.payload)?;

        let task_def = TaskDefinition {
            id: TaskId::new(task.task_id),
            name: task.name,
            payload,
            workflow_id: task.workflow_id.map(WorkflowId::from),
            target_node: task.target_node.map(NodeId::new),
            origin_node: Some(node_id),
            retry_policy: task.retry_policy,
            priority: task.priority,
            timeout_ms: task.timeout_ms,
            attempt: 0,
            lease_deadline_ms: 0,
            enqueued_at_ms: 0,
            target_endpoint_addr: task.target_endpoint_addr,
            origin_endpoint_addr: Some(endpoint_addr),
            tags: task.tags,
            quota: task.quota,
            pinned_target: task.pinned_target,
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
        actant_core::metrics::inc_tasks_submitted_by(n as u64);
        let runtime = self.core()?;
        let node_id = runtime.node_id().clone();
        let endpoint_addr = self.local_endpoint_addr(py)?;
        let task_defs: Vec<TaskDefinition> = tasks
            .into_iter()
            .map(|t| {
                Ok(TaskDefinition {
                    id: TaskId::new(t.task_id),
                    name: t.name,
                    // 与单条提交一致：直接任务载荷在 PyO3 边界签名。
                    payload: sign_payload(&self.payload_signing_key, t.payload)?,
                    workflow_id: t.workflow_id.map(WorkflowId::from),
                    target_node: t.target_node.map(NodeId::new),
                    origin_node: Some(node_id.clone()),
                    retry_policy: t.retry_policy,
                    priority: t.priority,
                    timeout_ms: t.timeout_ms,
                    attempt: 0,
                    lease_deadline_ms: 0,
                    enqueued_at_ms: 0,
                    target_endpoint_addr: t.target_endpoint_addr,
                    origin_endpoint_addr: Some(endpoint_addr.clone()),
                    tags: t.tags,
                    quota: t.quota,
                    pinned_target: t.pinned_target,
                })
            })
            .collect::<PyResult<Vec<TaskDefinition>>>()?;
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

    /// 创建（或重置为）一个空的持久化工作流外壳（flow 提交路径第一步）。
    ///
    /// flow 函数体的首次 `task.submit()` 前调用：以空 DAG `submit` 持久化
    /// 工作流，后续节点经 [`Self::add_workflow_node`] 增量加入。
    /// ``timeout_ms > 0`` 时经 `submit_with_timeout` 设置工作流级 deadline。
    /// ``failure_strategy`` 为 ``"fail_fast"``（默认）或 ``"continue"``。
    #[pyo3(signature = (workflow_id, failure_strategy=None, timeout_ms=0, name=None, version=0, args=None))]
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        name = "py.submit_workflow",
        level = "info",
        skip(self, py),
        fields(workflow_id = %workflow_id, timeout_ms)
    )]
    fn submit_workflow(
        &self,
        py: Python<'_>,
        workflow_id: String,
        failure_strategy: Option<String>,
        timeout_ms: u64,
        name: Option<String>,
        version: u32,
        args: Option<Vec<u8>>,
    ) -> PyResult<()> {
        let mut dag = Dag::new();
        dag.failure_strategy = match failure_strategy.as_deref() {
            None => FailureStrategy::default(),
            Some(s) => FailureStrategy::parse(s).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid failure_strategy {s:?}: expected \"fail_fast\" or \"continue\""
                ))
            })?,
        };
        let wf = WorkflowId::from(workflow_id);
        actant_core::metrics::inc_workflows_submitted();
        // 只有 flow 提交路径会给出 name；其余路径留空，journal 仍记录
        // “没有调用描述”，而不是伪造一个。
        let invocation = name.map(|name| WorkflowInvocation {
            workflow_id: wf.clone(),
            name,
            version,
            args: args.unwrap_or_default(),
        });
        if timeout_ms > 0 {
            let payload =
                encode(&(wf, dag, timeout_ms, invocation)).map_err(actant_error_to_pyerr)?;
            self.call_workflow_actor::<()>(py, workflow_methods::SUBMIT_WITH_TIMEOUT, payload)?;
        } else {
            let payload = encode(&(wf, dag, invocation)).map_err(actant_error_to_pyerr)?;
            self.call_workflow_actor::<()>(py, workflow_methods::SUBMIT, payload)?;
        }
        Ok(())
    }

    /// 增量加入单个 DAG 节点（flow 提交路径核心）。
    ///
    /// 每次函数体内 `task.submit()` 调用一次：节点在 orchestrator 内登记、
    /// 建边、同步落盘（先持久化再派发），随后做重放裁决与派发裁决：
    /// - 新节点且依赖已满足 → 返回的 `TaskDefinition` 已直接推入本节点的
    ///   后台调度通道（与 `submit_task` 同路，经路由能力决定本地/远端执行）；
    /// - 节点已存在（flow 重放命中历史）→ 返回其当前状态与已完成结果字节，
    ///   Python 层据此重建句柄，不重新提交、不重跑；
    /// - 节点已存在但定义不一致 → 抛 ``FlowReplayError``（提交序列指纹
    ///   fail-fast）。
    ///
    /// 返回 dict：``{"created": bool, "state": str | None, "result": bytes | None,
    /// "error": str | None}``（``created=False`` 时后三项为历史中的任务状态、
    /// 结果与失败信息）。
    #[pyo3(signature = (workflow_id, node, deps))]
    #[tracing::instrument(
        name = "py.add_workflow_node",
        level = "debug",
        skip(self, py, node, deps),
        fields(workflow_id = %workflow_id, task_id = %node.task_id)
    )]
    fn add_workflow_node(
        &self,
        py: Python<'_>,
        workflow_id: String,
        node: PyNode,
        deps: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let dag_node = DagNode {
            task_id: TaskId::new(node.task_id.clone()),
            name: node.name,
            payload: node.payload,
            retry_policy: node.retry.map(RetryPolicy::from),
            timeout_ms: node.timeout_ms,
            priority: node.priority.unwrap_or(0),
            metadata: node.metadata.unwrap_or_default(),
            tags: node.tags,
            quota: node.quota,
        };
        let deps: Vec<TaskId> = deps.into_iter().map(TaskId::new).collect();
        let payload = encode(&(WorkflowId::from(workflow_id), dag_node, deps))
            .map_err(actant_error_to_pyerr)?;
        let outcome: AddNodeOutcome =
            self.call_workflow_actor(py, workflow_methods::ADD_NODE, payload)?;
        let dict = pyo3::types::PyDict::new(py);
        match outcome {
            AddNodeOutcome::Created { ready } => {
                // 依赖已满足的新节点：推入与 submit_task 相同的后台调度通道，
                // 复用路由 / 派发 / 结果回灌全链路。调度通道未启动（未 serve）
                // 时显式失败——节点已持久化但无法派发，静默吞掉会造成 flow
                // 永久悬挂。
                if let Some(task_def) = ready.map(|b| *b) {
                    let tx_guard = self.submit_tx.lock();
                    let tx = tx_guard.as_ref().ok_or_else(|| {
                        pyo3::exceptions::PyRuntimeError::new_err(
                            "submit channel not started; call serve() first",
                        )
                    })?;
                    tx.send(task_def).map_err(|_| {
                        pyo3::exceptions::PyRuntimeError::new_err(
                            "submit channel closed; runtime may have shut down",
                        )
                    })?;
                    drop(tx_guard);
                    py.detach(move || {
                        std::hint::spin_loop();
                    });
                }
                dict.set_item("created", true)?;
                dict.set_item("state", Option::<String>::None)?;
                dict.set_item("result", Option::<Vec<u8>>::None)?;
            }
            AddNodeOutcome::Existing {
                state,
                result,
                error,
            } => {
                dict.set_item("created", false)?;
                dict.set_item("state", state.as_str())?;
                dict.set_item("result", result)?;
                dict.set_item("error", error)?;
            }
        }
        Ok(dict.into_any().unbind())
    }

    /// 取消整个工作流（flow 失败兜底路径）。
    ///
    /// 将工作流及运行中任务置为 Cancelled 终态；已终态的工作流为幂等 no-op。
    #[tracing::instrument(name = "py.cancel_workflow", level = "info", skip(self, py), fields(workflow_id = %workflow_id))]
    fn cancel_workflow(&self, py: Python<'_>, workflow_id: String) -> PyResult<()> {
        let payload = encode(&WorkflowId::from(workflow_id)).map_err(actant_error_to_pyerr)?;
        self.call_workflow_actor::<()>(py, workflow_methods::CANCEL_WORKFLOW, payload)?;
        Ok(())
    }

    /// 删除工作流（运维清理）。
    ///
    /// 从内存与持久化存储中移除该工作流的全部状态（DAG/execution/pending/
    /// 结果/等待点/信号缓冲/事件水位）。与 `completed_retention_count` 的
    /// 自动淘汰互补，供手动清理已完成/孤儿工作流。幂等：工作流不存在时为
    /// no-op。注意：运行中的工作流删除后其任务结果将不可恢复。
    #[tracing::instrument(name = "py.delete_workflow", level = "info", skip(self, py), fields(workflow_id = %workflow_id))]
    fn delete_workflow(&self, py: Python<'_>, workflow_id: String) -> PyResult<()> {
        let payload = encode(&WorkflowId::from(workflow_id)).map_err(actant_error_to_pyerr)?;
        self.call_workflow_actor::<()>(py, workflow_methods::DELETE_WORKFLOW, payload)?;
        Ok(())
    }

    /// 注册持久化等待点。
    ///
    /// 等待点是 orchestrator 状态机的挂起原语：持久化
    /// ``(workflow_id, wait_key, 条件)``，条件满足（信号递交 / 定时到期）时追加
    /// 唤醒事件进入同一工作流历史。
    ///
    /// Args:
    ///     workflow_id: 工作流标识。
    ///     wait_key: 等待点注册表键（同一工作流内唯一）。
    ///     kind: ``"signal"``（外部信号触发）、``"timer"``（定时到期）或
    ///         ``"suspend"``（等待 ``resume_suspended``）。
    ///     name: ``kind="signal"`` 时的信号语义名；``None`` 时退化为 ``wait_key``。
    ///     deadline_ms: ``kind="timer"`` 时的**绝对** epoch 毫秒到期时刻，必须 > 0。
    ///
    /// Raises:
    ///     ValueError: ``kind`` 非法，或 ``kind="timer"`` 时 ``deadline_ms == 0``。
    ///     KeyError/NotFound: 工作流不存在。
    ///
    /// Note:
    ///     幂等：同 ``wait_key`` 重复注册为 no-op（不改写条件、不重复追加事件），
    ///     这是重放体天然幂等的前提。
    #[pyo3(signature = (workflow_id, wait_key, kind, name=None, deadline_ms=0))]
    #[tracing::instrument(
        name = "py.register_wait_point",
        level = "debug",
        skip(self, py),
        fields(workflow_id = %workflow_id, wait_key = %wait_key, kind = %kind)
    )]
    fn register_wait_point(
        &self,
        py: Python<'_>,
        workflow_id: String,
        wait_key: String,
        kind: String,
        name: Option<String>,
        deadline_ms: u64,
    ) -> PyResult<()> {
        let condition = match kind.as_str() {
            "signal" => WaitCondition::Signal {
                name: name.unwrap_or_else(|| wait_key.clone()),
            },
            "timer" => {
                if deadline_ms == 0 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "register_wait_point: kind=\"timer\" requires deadline_ms > 0 \
                         (absolute epoch milliseconds)",
                    ));
                }
                WaitCondition::Timer { deadline_ms }
            }
            // 挂起条件。等待显式恢复（`resume_suspended`），与 `signal` 的区别是
            // 语义来源——signal 等业务事件，suspend 等操作员的恢复指令。
            "suspend" => WaitCondition::Suspend,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "register_wait_point: unknown kind {other:?}: expected \"signal\", \
                     \"timer\" or \"suspend\""
                )))
            }
        };
        let payload = encode(&(WorkflowId::from(workflow_id), wait_key, condition))
            .map_err(actant_error_to_pyerr)?;
        self.call_workflow_actor::<()>(py, workflow_methods::REGISTER_WAIT_POINT, payload)?;
        Ok(())
    }

    /// 递交信号，唤醒 ``wait_key`` 对应的等待点（Signals 出口）。
    ///
    /// **返回值**
    ///
    /// - ``bytes``：本次递交**唤醒了一个等待点**（或该等待点此前已唤醒）。
    ///   返回的是该等待点记录的 payload（首次递交胜出）；重放体"已收到 →
    ///   直接返回"由此路径实现，重复 signal 幂等。
    /// - ``None``：此刻**没有等待点可被唤醒**，信号已入缓冲——将来注册同一
    ///   ``wait_key`` 的等待点会**立即生成为已唤醒态**，无需递交方重试。
    ///
    /// ``payload`` 是可选字节串（默认空），原样随 ``SignalReceived`` 进入工作流
    /// 历史：重放、跨重启恢复与等待点缓冲命中都返回同一份字节。业务语义
    /// （JSON / pickle）由第 2 层解释，核心不解析。
    ///
    /// **抛出**
    ///
    /// - ``NotFoundError``：工作流不存在（递交方 id 写错）。
    /// - ``ValueError``：``wait_key`` 为空。
    /// - 工作流**已终态不报错**：递交方重试时，前一次递交可能已被缓冲命中、
    ///   flow 已跑完并让工作流进入终态，此时拒绝会让"明明送到了"变成报错。
    ///
    /// **注意**
    ///
    /// 缓冲是**闩锁**：同一 ``wait_key`` 已有缓冲时重复递交不再追加历史、
    /// 不覆盖，直接返回 ``None``——这让"按返回值重试"变安全（重试是 no-op）。
    /// 缓冲与等待点快照同批落盘，故跨重启存活；事件重放已在恢复路径落地（recover = 快照 + 其后事件重放），信号缓冲亦随重放重建。
    #[tracing::instrument(
        name = "py.signal_wait_point",
        level = "debug",
        skip(self, py, payload),
        fields(workflow_id = %workflow_id, wait_key = %wait_key)
    )]
    #[pyo3(signature = (workflow_id, wait_key, payload=None))]
    fn signal_wait_point(
        &self,
        py: Python<'_>,
        workflow_id: String,
        wait_key: String,
        payload: Option<Vec<u8>>,
    ) -> PyResult<Option<Vec<u8>>> {
        let frame = encode(&(
            WorkflowId::from(workflow_id),
            wait_key,
            payload.unwrap_or_default(),
        ))
        .map_err(actant_error_to_pyerr)?;
        self.call_workflow_actor(py, workflow_methods::SIGNAL_WAIT_POINT, frame)
    }

    /// 恢复挂起：唤醒该工作流所有 `kind="suspend"` 且仍在等待的等待点。
    ///
    /// Returns:
    ///     ``int``：本次唤醒的挂起点数量；``0`` 表示该工作流当前没有处于挂起中的
    ///     挂起点（幂等——重复调用第二次返回 0）。
    ///
    /// Note:
    ///     **只唤醒 `Suspend` 条件**，不触碰 `signal` / `timer` 等待点：
    ///     `resume` 是操作员的恢复指令，不得冒名顶替一个业务信号
    ///     按 ``workflow_id`` 而非按键
    ///     唤醒，调用方无需知道 flow 内部给挂起点分配了什么键。
    #[tracing::instrument(
        name = "py.resume_suspended",
        level = "info",
        skip(self, py),
        fields(workflow_id = %workflow_id)
    )]
    fn resume_suspended(&self, py: Python<'_>, workflow_id: String) -> PyResult<usize> {
        let payload = encode(&WorkflowId::from(workflow_id)).map_err(actant_error_to_pyerr)?;
        self.call_workflow_actor(py, workflow_methods::RESUME_SUSPENDED, payload)
    }

    /// 解析（首次记录 / 重放命中）确定性上下文值。
    ///
    /// Args:
    ///     workflow_id: 工作流标识。
    ///     key: 上下文值键（由 flow 体的调用序号决定，如 ``ctx-1``）。
    ///     kind: 值种类（``"now_ms"`` / ``"uuid"`` / ``"random"``）。
    ///     candidate: 首次执行时生成的候选值（重放时被忽略）。
    ///
    /// Returns:
    ///     记录值：首次执行等于 ``candidate``，重放时等于历史值。
    ///
    /// Raises:
    ///     FlowReplayError: 同 ``key`` 的历史记录是另一种 ``kind``——flow 体的
    ///         调用序列已漂移。
    #[pyo3(signature = (workflow_id, key, kind, candidate))]
    #[tracing::instrument(
        name = "py.resolve_context_value",
        level = "debug",
        skip(self, py, candidate),
        fields(workflow_id = %workflow_id, key = %key, kind = %kind)
    )]
    fn resolve_context_value(
        &self,
        py: Python<'_>,
        workflow_id: String,
        key: String,
        kind: String,
        candidate: Vec<u8>,
    ) -> PyResult<Vec<u8>> {
        let payload = encode(&(WorkflowId::from(workflow_id), key, kind, candidate))
            .map_err(actant_error_to_pyerr)?;
        self.call_workflow_actor(py, workflow_methods::RESOLVE_CONTEXT_VALUE, payload)
    }

    /// 只读查询工作流已记录的确定性上下文值。
    ///
    /// 与 ``resolve_context_value`` 的区别：未记录时返回 ``None`` 且**不写
    /// 历史**。用于探测"续跑携带参数"等可能不存在的记录。
    ///
    /// Args:
    ///     workflow_id: 工作流标识。
    ///     key: 上下文值键。
    ///
    /// Returns:
    ///     记录值字节串；未记录返回 ``None``。
    #[pyo3(signature = (workflow_id, key))]
    #[tracing::instrument(
        name = "py.get_context_value",
        level = "debug",
        skip(self, py),
        fields(workflow_id = %workflow_id, key = %key)
    )]
    fn get_context_value(
        &self,
        py: Python<'_>,
        workflow_id: String,
        key: String,
    ) -> PyResult<Option<Vec<u8>>> {
        let payload =
            encode(&(WorkflowId::from(workflow_id), key)).map_err(actant_error_to_pyerr)?;
        self.call_workflow_actor(py, workflow_methods::GET_CONTEXT_VALUE, payload)
    }

    /// 阻塞等待等待点条件满足（flow 体 park 原语）。
    ///
    /// 与其它工作流方法不同，本方法**不经 actor 消息循环**：actor 消息处理是
    /// 单线程顺序执行的，在 `handle_message` 内阻塞会让整个 WorkflowActor
    /// （全部工作流）停摆。因此本方法持有编排器只读句柄
    /// （[`actant_core::runtime::context::Runtime::orchestrator_handle`]，与 actor 共享
    /// 同一个 `Arc<OrchestratorState>`）在 actor 之外阻塞。
    ///
    /// **调用方必须先 `register_wait_point`**：注册是将等待点写入历史/快照的
    /// 动作，本方法只负责 park。顺序为"先注册、后 park"，且
    /// `register_wait_point_waiter` 内部"先注册句柄、再检查是否已 Signaled"
    /// 关闭竞态窗口——信号在两步之间到达不会丢失。
    ///
    /// Args:
    ///     workflow_id: 工作流标识。
    ///     wait_key: 等待点注册表键。
    ///     timeout_ms: 等待上界；``0`` 表示无限等待（直到被唤醒或工作流被移除）。
    ///
    /// Returns:
    ///     ``bytes``：条件满足（signal 递交或 timer 到期）时的 payload。
    ///     ``None``：等待超时，或等待期间工作流被移除/编排器未注入。
    #[pyo3(signature = (workflow_id, wait_key, timeout_ms=0))]
    #[tracing::instrument(
        name = "py.wait_wait_point",
        level = "debug",
        skip(self, py),
        fields(workflow_id = %workflow_id, wait_key = %wait_key, timeout_ms)
    )]
    fn wait_wait_point(
        &self,
        py: Python<'_>,
        workflow_id: String,
        wait_key: String,
        timeout_ms: u64,
    ) -> PyResult<Option<Vec<u8>>> {
        let runtime = self.core()?;
        let Some(orchestrator) = runtime.orchestrator_handle().cloned() else {
            return Ok(None);
        };
        let tokio = self.tokio_handle()?;
        let wf = WorkflowId::from(workflow_id);
        let rx = orchestrator.register_wait_point_waiter(wf, &wait_key);
        // 阻塞期间释放 GIL：否则 tokio worker 的 pyo3_log 回调会与持 GIL 的
        // 调用方互等（与 call_workflow_actor / report_task_result 同一考量）。
        let outcome = py.detach(move || {
            tokio.block_on(async move {
                if timeout_ms == 0 {
                    rx.await.ok()
                } else {
                    match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), rx)
                        .await
                    {
                        Ok(Ok(payload)) => Some(payload),
                        // 超时，或等待者被移除（sender 提前 drop）→ 无可交付结果。
                        _ => None,
                    }
                }
            })
        });
        Ok(outcome)
    }

    /// 非阻塞等待等待点条件满足（asyncio 版本）。
    ///
    /// 语义与 :meth:`wait_wait_point` 一致，但返回 ``asyncio.Future``（无 running
    /// loop 时回退为自定义 awaitable），等待期间不占用调用线程、不阻塞 event
    /// loop。等待者仍在 actor 之外注册，故不会拖停其他工作流。
    ///
    /// Args:
    ///     workflow_id: 工作流标识。
    ///     wait_key: 等待点注册表键。
    ///     timeout_ms: 等待上界；``0`` 表示无限等待。
    ///
    /// Returns:
    ///     解析为 ``bytes``（条件满足）或 ``None``（超时 / 等待者被释放 /
    ///     编排器未注入）的 awaitable。
    #[pyo3(signature = (workflow_id, wait_key, timeout_ms=0))]
    #[tracing::instrument(
        name = "py.wait_wait_point_async",
        level = "debug",
        skip(self, py),
        fields(workflow_id = %workflow_id, wait_key = %wait_key, timeout_ms)
    )]
    fn wait_wait_point_async<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        wait_key: String,
        timeout_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let handle = tokio.handle().clone();
        let Some(orchestrator) = runtime.orchestrator_handle().cloned() else {
            return future_into_py_iter(
                py,
                handle,
                &self.gil_thread,
                std::future::ready(FutureResultToPy::Value(py.None())),
            );
        };
        let wf = WorkflowId::from(workflow_id);
        let rx = orchestrator.register_wait_point_waiter(wf, &wait_key);
        let fut = async move {
            let payload = if timeout_ms == 0 {
                rx.await.ok()
            } else {
                match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), rx).await {
                    Ok(Ok(payload)) => Some(payload),
                    // 超时，或等待者被移除（sender 提前 drop）→ 无可交付结果。
                    _ => None,
                }
            };
            match payload {
                Some(bytes) => Python::attach(|py| {
                    FutureResultToPy::Value(
                        pyo3::types::PyBytes::new(py, &bytes).unbind().into_any(),
                    )
                }),
                None => Python::attach(|py| FutureResultToPy::Value(py.None())),
            }
        };
        future_into_py_iter(py, handle, &self.gil_thread, fut)
    }

    /// 封口工作流节点集（flow 函数体返回信号）。
    ///
    /// 封口后 orchestrator 才允许工作流终态判定；全部任务已终态时立即收尾。
    #[tracing::instrument(name = "py.seal_workflow", level = "debug", skip(self, py), fields(workflow_id = %workflow_id))]
    fn seal_workflow(&self, py: Python<'_>, workflow_id: String) -> PyResult<()> {
        let payload = encode(&WorkflowId::from(workflow_id)).map_err(actant_error_to_pyerr)?;
        self.call_workflow_actor::<()>(py, workflow_methods::SEAL_WORKFLOW, payload)?;
        Ok(())
    }

    /// 上报本地 flow 任务的终态结果（由 Python 事件泵调用）。
    ///
    /// worker 结果帧正文对 Rust 不透明（成功与业务失败都表现为 `Ok(Ok(body))`），
    /// 故由 Python 侧解析 `dumps((success, payload))` 后经此上报；Rust 据此经
    /// `ON_TASK_RESULT` 单入口回灌 orchestrator，并落实重试裁决
    /// （裁决为重试时按延迟重新入队本节点调度器）。
    ///
    /// Args:
    ///     workflow_id: 工作流标识。空串表示非编排任务（独立 ``@task``），直接
    ///         返回无裁决。
    ///     task_id: 节点标识。
    ///     state: ``"Completed"`` / ``"Failed"`` / ``"Cancelled"``。
    ///     result: Completed 时的结果字节。
    ///     error: Failed 时的错误信息。
    ///
    /// Returns:
    ///     ``{"retry": bool, "delay_ms": int}``：``retry`` 为真表示 orchestrator
    ///     已排定重试（任务已按 ``delay_ms`` 延迟重新入队），提交方句柄应保持
    ///     等待；为假表示终局，句柄按实际终态解析。
    ///
    /// Raises:
    ///     ValueError: ``state`` 不是已知终态（fail-fast，避免静默错分）。
    ///     RuntimeError: Runtime 未启动或 Worker 未初始化。
    #[pyo3(signature = (workflow_id, task_id, state, result=None, error=None))]
    #[tracing::instrument(
        name = "py.report_task_result",
        level = "debug",
        skip(self, py, result),
        fields(workflow_id = %workflow_id, task_id = %task_id, state = %state)
    )]
    fn report_task_result(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
        state: String,
        result: Option<Vec<u8>>,
        error: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let runtime = self.core()?;
        let worker = runtime
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not initialized"))?
            .clone();
        let tokio = self.tokio_handle()?;
        let outcome = match state.as_str() {
            "Completed" => TaskResultOutcome::Completed(result.unwrap_or_default()),
            "Failed" => TaskResultOutcome::Failed(
                error.unwrap_or_else(|| "task failed (no detail reported)".to_string()),
            ),
            "Cancelled" => TaskResultOutcome::Cancelled,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "report_task_result: unknown task state {other:?}: \
                     expected one of: Completed, Failed, Cancelled"
                )))
            }
        };
        let wf = WorkflowId::from(workflow_id);
        let tid = TaskId::new(task_id);
        // GIL 在 block_on 期间释放（与其它 actor 调用一致），避免 pyo3_log
        // 回调与持有 GIL 的调用方 block_on 互相等待。
        let response = py
            .detach(move || tokio.block_on(worker.report_task_result(&wf, &tid, outcome)))
            .map_err(actant_error_to_pyerr)?;
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("retry", response.retry)?;
        dict.set_item("delay_ms", response.delay_ms)?;
        Ok(dict.into_any().unbind())
    }

    /// 查询指定工作流的持久化执行状态。
    ///
    /// 返回 dict（``state``/``tasks``/``succeeded_count``/``total_count``/
    /// ``failure_strategy``/``error``）或 ``None``（工作流不存在）。
    /// ``tasks`` 为 ``{task_id: {state, result, error, retry_count, attempt}}``。
    #[tracing::instrument(name = "py.get_workflow_state", level = "debug", skip(self, py), fields(workflow_id = %workflow_id))]
    fn get_workflow_state(&self, py: Python<'_>, workflow_id: String) -> PyResult<Py<PyAny>> {
        let payload =
            encode(&WorkflowId::from(workflow_id.clone())).map_err(actant_error_to_pyerr)?;
        let state: Option<WorkflowExecution> =
            self.call_workflow_actor(py, workflow_methods::GET_STATE, payload)?;
        let Some(exec) = state else {
            return Ok(py.None());
        };
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("workflow_id", exec.workflow_id.as_str())?;
        dict.set_item("state", exec.state.as_str())?;
        let tasks = pyo3::types::PyDict::new(py);
        for (tid, ts) in &exec.tasks {
            let task = pyo3::types::PyDict::new(py);
            task.set_item("state", ts.state.as_str())?;
            task.set_item("result", ts.result.clone())?;
            task.set_item("error", ts.error.clone())?;
            task.set_item("retry_count", ts.retry_count())?;
            task.set_item("attempt", ts.attempt())?;
            tasks.set_item(tid.as_str(), task)?;
        }
        dict.set_item("tasks", tasks)?;
        dict.set_item("succeeded_count", exec.succeeded_count())?;
        dict.set_item("total_count", exec.total_count())?;
        dict.set_item("failure_strategy", exec.failure_strategy.as_str())?;
        dict.set_item("error", exec.error.clone())?;
        Ok(dict.into_any().unbind())
    }

    /// 查询指定工作流的 DAG **结构**（暴露面）。
    ///
    /// 返回 ``None``（工作流不存在）或 dict：
    /// ``workflow_id`` / ``failure_strategy`` / ``default_retry_policy`` /
    /// ``nodes`` / ``edges``。``nodes`` 每项为
    /// ``{task_id, name, deps, timeout_ms, priority, metadata, retry_policy}``；
    /// ``edges`` 每项为 ``{from, to}``。
    ///
    /// **不含 payload**：任务载荷是签名的 cloudpickle 字节，对 Rust 不透明且可能
    /// 很大。需要任务**结果**用 :func:`get_workflow_state` 的 ``tasks[*].result``。
    ///
    /// ``default_retry_policy`` 一并暴露，否则节点 ``retry_policy`` 为 ``None``
    /// 时无法在 Python 侧还原生效策略（``Dag::effective_retry_policy`` 的输入）。
    #[tracing::instrument(name = "py.get_dag", level = "debug", skip(self, py), fields(workflow_id = %workflow_id))]
    fn get_dag(&self, py: Python<'_>, workflow_id: String) -> PyResult<Py<PyAny>> {
        let payload = encode(&WorkflowId::from(workflow_id)).map_err(actant_error_to_pyerr)?;
        let snapshot: Option<DagSnapshot> =
            self.call_workflow_actor(py, workflow_methods::GET_DAG, payload)?;
        let Some(snap) = snapshot else {
            return Ok(py.None());
        };
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("workflow_id", snap.workflow_id)?;
        dict.set_item("failure_strategy", snap.failure_strategy)?;
        dict.set_item(
            "default_retry_policy",
            retry_policy_to_py(py, snap.default_retry_policy.as_ref())?,
        )?;
        let nodes = pyo3::types::PyList::new(py, [] as [Py<PyAny>; 0])?;
        for n in &snap.nodes {
            let node = pyo3::types::PyDict::new(py);
            node.set_item("task_id", &n.task_id)?;
            node.set_item("name", &n.name)?;
            node.set_item("deps", &n.deps)?;
            node.set_item("timeout_ms", n.timeout_ms)?;
            node.set_item("priority", n.priority)?;
            let meta = pyo3::types::PyDict::new(py);
            for (k, v) in &n.metadata {
                meta.set_item(k, v)?;
            }
            node.set_item("metadata", meta)?;
            node.set_item(
                "retry_policy",
                retry_policy_to_py(py, n.retry_policy.as_ref())?,
            )?;
            nodes.append(node)?;
        }
        dict.set_item("nodes", nodes)?;
        let edges = pyo3::types::PyList::new(py, [] as [Py<PyAny>; 0])?;
        for e in &snap.edges {
            let edge = pyo3::types::PyDict::new(py);
            edge.set_item("from", &e.from)?;
            edge.set_item("to", &e.to)?;
            edges.append(edge)?;
        }
        dict.set_item("edges", edges)?;
        Ok(dict.into_any().unbind())
    }

    /// 读取工作流的事件历史（审计出口）。
    ///
    /// 返回 list，每项为
    /// ``{sequence, timestamp_ms, kind, task_id, error, payload}``：
    ///
    /// - ``kind`` / ``task_id`` / ``error`` 由 Rust 侧解出，供调用方**筛选**
    ///   （``kind`` 取值如 ``"TaskCompleted"`` / ``"WaitPointRegistered"``…）；
    /// - ``payload`` 是原始 postcard 字节，**不透明**——Python 不解释其布局，
    ///   故 Rust 枚举的字段增减不会变成跨语言契约。
    ///
    /// ``after`` 为 ``(sequence, timestamp_ms)`` 形式的游标（来自上一项的
    /// ``sequence``/``timestamp_ms``）或 ``None``（从头读取）。
    /// 无 event_log 或工作流不存在时返回空列表——历史是**可选**的观测面。
    #[tracing::instrument(name = "py.get_workflow_history", level = "debug", skip(self, py), fields(workflow_id = %workflow_id))]
    // 显式签名：`after` 必须**可选且仅关键字**，与 `actant.pyi` 的声明一致
    // （缺省时 PyO3 会把它变成必填位置参数，存根因此撒谎）。
    #[pyo3(signature = (workflow_id, *, after=None))]
    fn get_workflow_history(
        &self,
        py: Python<'_>,
        workflow_id: String,
        after: Option<(u64, u64)>,
    ) -> PyResult<Py<PyAny>> {
        use actant_core::runtime::state::event_log::EventId;
        use actant_core::runtime::state::HlcTimestamp;

        let cursor = after.map(|(sequence, timestamp_ms)| EventId {
            timestamp: HlcTimestamp::from_parts(timestamp_ms, 0),
            sequence,
        });
        let payload =
            encode(&(WorkflowId::from(workflow_id), cursor)).map_err(actant_error_to_pyerr)?;
        let entries: Vec<(EventId, Vec<u8>)> =
            self.call_workflow_actor(py, workflow_methods::GET_HISTORY, payload)?;

        let list = pyo3::types::PyList::new(py, [] as [Py<PyAny>; 0])?;
        for (id, raw) in &entries {
            let item = pyo3::types::PyDict::new(py);
            item.set_item("sequence", id.sequence)?;
            item.set_item("timestamp_ms", id.timestamp.wall_time())?;
            item.set_item("payload", pyo3::types::PyBytes::new(py, raw))?;
            match postcard::from_bytes::<JournalEvent>(raw) {
                Ok(ev) => {
                    item.set_item("kind", event_payload_kind(&ev))?;
                    item.set_item("task_id", event_payload_task_id(&ev).map(|t| t.to_string()))?;
                    item.set_item("error", event_payload_error(&ev).map(|s| s.to_string()))?;
                }
                Err(_) => {
                    // 解码失败（历史格式变更）仍回传字节，但观测字段为 None。
                    item.set_item("kind", py.None())?;
                    item.set_item("task_id", py.None())?;
                    item.set_item("error", py.None())?;
                }
            }
            list.append(item)?;
        }
        Ok(list.into_any().unbind())
    }

    /// 返回工作流的稳定 journal（跨语言消费面）。
    ///
    /// 每项固定含 ``seq`` / ``timestamp_ms`` / ``kind`` / ``workflow_id`` /
    /// ``task_id`` / ``wait_key`` / ``error`` / ``payload``，并按事件类型附带
    /// ``condition``（等待点）与 ``invocation``（提交描述）。
    ///
    /// - ``kind`` 是稳定的小写事件名（与 Python 侧 lifecycle 事件的 ``kind``
    ///   一致，如 ``"submitted"`` / ``"task_completed"``）；
    /// - ``payload`` 是该事件携带的不透明字节（节点定义入参、任务结果、
    ///   信号载荷、上下文值），可直接 JSON base64 导出；
    /// - 无法解码的历史条目以 ``kind="undecodable"`` 保留位置并带解码错误，
    ///   不静默跳过。
    fn get_workflow_journal(&self, py: Python<'_>, workflow_id: String) -> PyResult<Py<PyAny>> {
        use actant_core::runtime::state::event_log::EventId;

        let payload = encode(&(WorkflowId::from(workflow_id), None::<EventId>))
            .map_err(actant_error_to_pyerr)?;
        let entries: Vec<(EventId, Vec<u8>)> =
            self.call_workflow_actor(py, workflow_methods::GET_HISTORY, payload)?;

        let list = pyo3::types::PyList::new(py, [] as [Py<PyAny>; 0])?;
        for (id, raw) in &entries {
            let item = pyo3::types::PyDict::new(py);
            item.set_item("seq", id.sequence)?;
            item.set_item("timestamp_ms", id.timestamp.wall_time())?;
            match postcard::from_bytes::<JournalEvent>(raw) {
                Ok(ev) => {
                    let record = JournalRecord {
                        seq: id.sequence,
                        timestamp_ms: id.timestamp.wall_time(),
                        event: ev,
                    };
                    let ev = &record.event;
                    item.set_item("kind", ev.kind())?;
                    item.set_item("workflow_id", ev.workflow_id().as_str())?;
                    item.set_item("task_id", event_payload_task_id(ev))?;
                    item.set_item("wait_key", event_payload_wait_key(ev))?;
                    item.set_item("error", event_payload_error(ev))?;
                    item.set_item(
                        "payload",
                        event_payload_bytes(ev).map(|b| pyo3::types::PyBytes::new(py, b)),
                    )?;
                    item.set_item(
                        "condition",
                        event_payload_condition(ev).map(wait_condition_label),
                    )?;
                    match ev {
                        JournalEvent::Submitted {
                            invocation: Some(inv),
                            ..
                        } => item.set_item("invocation", invocation_to_py(py, inv)?)?,
                        _ => item.set_item("invocation", py.None())?,
                    }
                }
                Err(e) => {
                    // 历史格式变更时保留位置但标记不可解码，避免静默跳过。
                    item.set_item("kind", "undecodable")?;
                    item.set_item("workflow_id", py.None())?;
                    item.set_item("task_id", py.None())?;
                    item.set_item("wait_key", py.None())?;
                    item.set_item("error", e.to_string())?;
                    item.set_item("payload", py.None())?;
                    item.set_item("condition", py.None())?;
                    item.set_item("invocation", py.None())?;
                }
            }
            list.append(item)?;
        }
        Ok(list.into_any().unbind())
    }

    /// 返回工作流的提交描述（`JournalEvent::Submitted` 中的 invocation）。
    ///
    /// 无 invocation（旧记录或非 flow 提交）时返回 ``None``。
    fn get_workflow_invocation(&self, py: Python<'_>, workflow_id: String) -> PyResult<Py<PyAny>> {
        use actant_core::runtime::state::event_log::EventId;

        let payload = encode(&(WorkflowId::from(workflow_id), None::<EventId>))
            .map_err(actant_error_to_pyerr)?;
        let entries: Vec<(EventId, Vec<u8>)> =
            self.call_workflow_actor(py, workflow_methods::GET_HISTORY, payload)?;
        for (_, raw) in &entries {
            if let Ok(JournalEvent::Submitted {
                invocation: Some(inv),
                ..
            }) = postcard::from_bytes::<JournalEvent>(raw)
            {
                return invocation_to_py(py, &inv);
            }
        }
        Ok(py.None())
    }

    /// 记录一次 capability effect 调用（Python 分发层内部指标埋点）。
    ///
    /// 仅统计 Python 侧 `actant.ask/perform/emit` 入口；`_` 前缀使其
    /// 停留在 internal 稳定性级别，不进入公开参考页。
    fn _record_capability_invocation(&self, capability: String, effect: String) {
        actant_core::metrics::inc_capability_invocation(&capability, &effect);
    }

    /// 批量记录 capability effect 调用（Python 分发层内部指标埋点）。
    ///
    /// 与 `_record_capability_invocation` 语义相同，但整批只发生一次
    /// Python→Rust 边界穿越，供 `aio.perform_batch` 保持批量快路径的单次穿越特性。
    fn _record_capability_invocations(&self, entries: Vec<(String, String)>) {
        for (capability, effect) in entries {
            actant_core::metrics::inc_capability_invocation(&capability, &effect);
        }
    }

    /// 返回工作流 ID 列表。
    ///
    /// `include_archived=False` 仅返回内存中活跃（已提交未淘汰）的工作流；
    /// `True` 额外遍历存储（``STORE_KEY_DAG`` 前缀），含已完成/历史工作流。
    #[tracing::instrument(name = "py.list_workflows", level = "debug", skip(self, py))]
    #[pyo3(signature = (include_archived = false))]
    fn list_workflows(&self, py: Python<'_>, include_archived: bool) -> PyResult<Vec<String>> {
        let method = if include_archived {
            workflow_methods::ALL_WORKFLOW_IDS
        } else {
            workflow_methods::ACTIVE_WORKFLOW_IDS
        };
        let ids: Vec<WorkflowId> = self.call_workflow_actor(py, method, Vec::new())?;
        Ok(ids.into_iter().map(|id| id.as_str().to_string()).collect())
    }

    /// 枚举当前在线的 peer 节点（节点可见性）。
    ///
    /// 返回 dict 列表，每项为 ``node_id`` / ``endpoint`` / ``available_slots`` /
    /// ``max_slots`` / ``active_workflows`` / ``labels`` / ``platform`` /
    /// ``last_heartbeat_ms``。在线判定复用心跳新鲜度语义；是面板与资源核算类
    /// 第 3 层应用的数据源（核心不内置调度策略）。
    #[tracing::instrument(name = "py.peers", level = "debug", skip(self, py))]
    fn peers(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let Some(worker) = self.runtime.as_ref().and_then(|rt| rt.worker()) else {
            return Ok(Vec::new());
        };
        let Some(failover) = worker.failover_manager() else {
            return Ok(Vec::new());
        };
        let infos = failover.peers();
        let mut out = Vec::with_capacity(infos.len());
        for info in infos {
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("node_id", info.node_id.as_str())?;
            dict.set_item("endpoint", info.endpoint_addr.clone())?;
            dict.set_item("available_slots", info.available_slots)?;
            dict.set_item("max_slots", info.max_slots)?;
            dict.set_item(
                "active_workflows",
                info.active_workflows
                    .iter()
                    .map(|w| w.as_str())
                    .collect::<Vec<_>>(),
            )?;
            dict.set_item("labels", info.labels.clone())?;
            match &info.platform {
                Some(p) => {
                    let pd = pyo3::types::PyDict::new(py);
                    pd.set_item("os", &p.os)?;
                    pd.set_item("arch", &p.arch)?;
                    pd.set_item("actant_version", &p.actant_version)?;
                    pd.set_item("host_runtime", p.host_runtime.clone())?;
                    dict.set_item("platform", pd)?;
                }
                None => dict.set_item("platform", py.None())?,
            }
            dict.set_item("last_heartbeat_ms", info.last_heartbeat_ms)?;
            out.push(dict.into_any().unbind());
        }
        Ok(out)
    }

    /// 测试专用故障注入：模拟本节点到 `target` 的单向网络分区。
    ///
    /// 仅供 `actant.testkit` 使用；以 `_test_` 前缀从公开 API 面隔离。
    fn _test_close_peer(&self, target: String) -> PyResult<()> {
        let failover = fault_injection_target(self.runtime.as_ref())?;
        failover.block_peer(&actant_core::common::NodeId::from(target));
        Ok(())
    }

    /// 测试专用故障注入：解除对 `target` 的分区模拟。
    fn _test_reopen_peer(&self, target: String) -> PyResult<()> {
        let failover = fault_injection_target(self.runtime.as_ref())?;
        failover.unblock_peer(&actant_core::common::NodeId::from(target));
        Ok(())
    }

    /// 测试专用故障注入：在接下来 `ms` 毫秒内不广播心跳（0 立即恢复）。
    fn _test_delay_heartbeat(&self, ms: u64) -> PyResult<()> {
        fault_injection_target(self.runtime.as_ref())?.pause_heartbeat_for_ms(ms);
        Ok(())
    }

    /// 测试专用故障注入：给本节点 HLC 施加时钟偏移（毫秒，可正可负），0 恢复。
    fn _test_inject_hlc_drift(&self, ms: i64) -> PyResult<()> {
        let runtime = self.core()?;
        runtime.inject_hlc_drift_ms(ms);
        Ok(())
    }

    /// 注册一个 batch/array job，返回初始进度快照。
    ///
    /// `batch_id` 由调用方生成（Python 层用 uuid）；`task_ids` 为该批任务的
    /// 提交 id。同一 id 重复注册累加成员；终态由 Python 结果泵在解析 payload
    /// 后经 `record_batch_terminal` 上报——Rust EventBus 无法区分「协议成功」
    /// 与「业务失败」，不能自行结算。
    fn register_batch(
        &self,
        py: Python<'_>,
        batch_id: String,
        task_ids: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let worker = self
            .core()?
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not available"))?;
        let progress = worker
            .batch_tracker()
            .register(&batch_id, task_ids.into_iter().map(TaskId::from));
        batch_progress_to_py(py, &progress)
    }

    /// 查询 batch 进度；未注册返回 `None`。
    fn batch_progress(&self, py: Python<'_>, batch_id: String) -> PyResult<Option<Py<PyAny>>> {
        let worker = self
            .core()?
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not available"))?;
        match worker.batch_tracker().progress(&batch_id) {
            Some(progress) => Ok(Some(batch_progress_to_py(py, &progress)?)),
            None => Ok(None),
        }
    }

    /// 记录一个 batch 成员的终态（由 Python 结果泵在解析 payload 后调用）。
    ///
    /// Rust EventBus 无法区分「协议成功」与「业务失败」（worker 结果正文不透明），
    /// 因此 batch 终态由唯一能解析 payload 的 Python 侧上报；返回更新后的进度快照，
    /// 非 batch 成员或重复上报返回 `None`。
    fn record_batch_terminal(
        &self,
        py: Python<'_>,
        task_id: String,
        state: String,
    ) -> PyResult<Option<Py<PyAny>>> {
        use actant_core::runtime::batch::BatchItemState;

        let worker = self
            .core()?
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not available"))?;
        let state = match state.to_ascii_lowercase().as_str() {
            "completed" => BatchItemState::Completed,
            "failed" => BatchItemState::Failed,
            "cancelled" => BatchItemState::Cancelled,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown batch terminal state '{other}'"
                )))
            }
        };
        match worker
            .batch_tracker()
            .record_terminal(&TaskId::from(task_id), state)
        {
            Some(progress) => Ok(Some(batch_progress_to_py(py, &progress)?)),
            None => Ok(None),
        }
    }

    /// 移除 batch 注册与成员索引。
    fn remove_batch(&self, batch_id: String) -> PyResult<bool> {
        let worker = self
            .core()?
            .worker()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("worker not available"))?;
        Ok(worker.batch_tracker().remove(&batch_id))
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
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let event_bus = runtime.event_bus().clone();
        let callback = Arc::new(callback);

        // 订阅 4 个任务生命周期话题（Started + Completed/Failed/Cancelled）
        let mut rx_started = event_bus.subscribe(BusTopic::TaskStarted);
        let mut rx_completed = event_bus.subscribe(BusTopic::TaskCompleted);
        let mut rx_failed = event_bus.subscribe(BusTopic::TaskFailed);
        let mut rx_cancelled = event_bus.subscribe(BusTopic::TaskCancelled);

        let handle = tokio.spawn(async move {
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
                        actant_core::metrics::observe_event_bridge_ms(
                            t0.elapsed().as_millis() as u64
                        );
                    });
                });
            }
        });
        // 跟踪 spawned task 句柄：shutdown 时 abort + await，避免孤儿 task
        // 持有 Py<PyAny> callback 引用阻止 GC（参见字段文档）。
        self.task_result_callback_handles.lock().push(handle);
        Ok(())
    }

    /// 注册任务日志回调。
    ///
    /// worker 子进程经 stderr 边带上报的任务日志（tap 语义 best-effort 可丢），
    /// 收到事件时调用 ``callback(dict)``，dict 为 ``{task_id, level, message}``。
    /// 指定 ``task_id`` 时走 `EventBus` 的逐任务通道，只接收该任务的日志；
    /// 省略时订阅全局 ``TaskLog`` 话题，接收所有任务日志。
    /// 回调在 tokio 后台线程执行，通过 ``Python::attach`` 获取 GIL。
    #[tracing::instrument(
        name = "py.register_task_log_callback",
        level = "info",
        skip(self, callback)
    )]
    #[pyo3(signature = (callback, task_id=None))]
    fn register_task_log_callback(
        &self,
        callback: Py<PyAny>,
        task_id: Option<String>,
    ) -> PyResult<()> {
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let event_bus = runtime.event_bus().clone();
        let callback = Arc::new(callback);
        let mut rx_log = match task_id {
            Some(task_id) => event_bus.subscribe_task_logs(TaskId::from(task_id)),
            None => event_bus.subscribe(BusTopic::TaskLog),
        };

        let handle = tokio.spawn(async move {
            loop {
                let Some(event) = rx_log.recv().await else {
                    break;
                };
                let BusEvent::TaskLog {
                    task_id,
                    level,
                    message,
                } = event
                else {
                    continue;
                };
                let cb = callback.clone();
                tokio::task::spawn_blocking(move || {
                    Python::attach(|py| {
                        let t0 = std::time::Instant::now();
                        let dict = pyo3::types::PyDict::new(py);
                        if dict.set_item("task_id", task_id.as_str()).is_err()
                            || dict.set_item("level", level).is_err()
                            || dict.set_item("message", message).is_err()
                        {
                            return;
                        }
                        let cb_ref = cb.clone_ref(py);
                        if let Err(e) = cb_ref.call1(py, (dict,)) {
                            tracing::warn!("task log callback error: {}", e);
                        }
                        actant_core::metrics::observe_event_bridge_ms(
                            t0.elapsed().as_millis() as u64
                        );
                    });
                });
            }
        });
        self.task_result_callback_handles.lock().push(handle);
        Ok(())
    }

    /// 注册 Worker 状态回调。
    ///
    /// 订阅 event_bus 的 ``WorkerLifecycle`` 话题（draining/drained/stopped），
    /// 收到事件时调用 ``callback(dict)``，dict 为 ``{state, node_id}``。
    #[tracing::instrument(
        name = "py.register_worker_state_callback",
        level = "info",
        skip(self, callback)
    )]
    fn register_worker_state_callback(&self, callback: Py<PyAny>) -> PyResult<()> {
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let event_bus = runtime.event_bus().clone();
        let callback = Arc::new(callback);
        let mut rx = event_bus.subscribe(BusTopic::WorkerLifecycle);

        let handle = tokio.spawn(async move {
            loop {
                let Some(event) = rx.recv().await else {
                    break;
                };
                let (state, node_id) = match &event {
                    BusEvent::WorkerDraining { node_id } => ("draining", node_id),
                    BusEvent::WorkerDrained { node_id } => ("drained", node_id),
                    BusEvent::WorkerStopped { node_id } => ("stopped", node_id),
                    _ => continue,
                };
                let state = state.to_string();
                let node_id = node_id.clone();
                let cb = callback.clone();
                let task = tokio::task::spawn_blocking(move || {
                    Python::attach(|py| {
                        let t0 = std::time::Instant::now();
                        let dict = pyo3::types::PyDict::new(py);
                        if dict.set_item("state", state).is_err()
                            || dict.set_item("node_id", node_id.as_str()).is_err()
                        {
                            return;
                        }
                        let cb_ref = cb.clone_ref(py);
                        if let Err(e) = cb_ref.call1(py, (dict,)) {
                            tracing::warn!("worker state callback error: {}", e);
                        }
                        actant_core::metrics::observe_event_bridge_ms(
                            t0.elapsed().as_millis() as u64
                        );
                    });
                });
                // 生命周期回调必须按事件顺序送达：广播通道本身保序，但每个事件
                // 各起一个 spawn_blocking 后回调会并发执行，可能出现 stopped
                // 先于 drained 的乱序。这里串行等待，代价是低频事件各等一次。
                if let Err(e) = task.await {
                    tracing::warn!("worker state callback task failed: {}", e);
                }
            }
        });
        self.task_result_callback_handles.lock().push(handle);
        Ok(())
    }

    /// 启动 Worker 守护循环（订阅 P2P topic + 任务执行循环）。
    ///
    /// 非阻塞：`worker.run()` 在 tokio runtime 后台 spawn，直到 `shutdown()` 取消。
    /// 用于 CLI `actant worker` 命令——使节点作为后台任务执行器常驻。
    /// 若 worker 未初始化则返回错误。
    #[tracing::instrument(name = "py.serve", level = "info", skip(self, py))]
    fn serve(&self, py: Python<'_>) -> PyResult<()> {
        // 幂等：每次调用都会 spawn 一个新的 `worker.run()`，两个循环并行消费同一
        // 队列既抢任务又互相覆盖句柄。以「句柄在册」作为已启动判据，`shutdown`
        // 会清空它，因此 stop → start 仍可重新拉起。
        if self.worker_handle.lock().is_some() {
            tracing::debug!("serve() ignored: worker daemon loop already running");
            return Ok(());
        }
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
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
        // 关键优化：submit_task 把 TaskDefinition 推到 unbounded mpsc channel
        // 立即返回，避免 `tokio.block_on(scheduler.enqueue())` 跨 GIL 同步阻塞
        // （实测 ~12ms/op）。后台 task 在 tokio runtime 上拉取
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
    /// 2. 在 tokio runtime 上执行 `Runtime::shutdown()`（停止 Actor 并
    ///    关闭 iroh endpoint，避免 `Endpoint` 被无声 drop 触发 iroh 的
    ///    "Aborting ungracefully" 警告）；
    /// 3. `shutdown_timeout` 关闭 tokio runtime。超时时间内无法完成则强制关闭。
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
        // 清理 register_task_result_callback spawn 的事件消费 task 放到
        // Runtime::shutdown() 之后执行：shutdown 会发布 WorkerStopped 等终态
        // 事件，提前 abort 订阅循环会让这批事件永远送不到 Python 回调。
        let tokio_opt = self.tokio.lock().take();
        if let Some(tokio) = tokio_opt {
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
                        if tokio::time::timeout(std::time::Duration::from_secs(15), rt.shutdown())
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                "runtime shutdown exceeded 15s; forcing tokio runtime shutdown"
                            );
                        }
                    });
                });
            }
            // 清理 register_task_result_callback spawn 的事件消费 task：
            // abort 让 select! 立即退出，短超时 await 确保任务真正结束（释放
            // callback Arc 引用）。timeout 防止回调中阻塞的 spawn_blocking 拖住 shutdown。
            // 放在 Runtime::shutdown() 之后，使 shutdown 期间发布的终态事件
            // （如 WorkerStopped）先送达 Python 回调再停止订阅循环。
            let callback_handles: Vec<_> =
                self.task_result_callback_handles.lock().drain(..).collect();
            if !callback_handles.is_empty() {
                tokio.block_on(async {
                    futures::future::join_all(
                        callback_handles
                            .into_iter()
                            .map(|h| async move {
                                h.abort();
                                let _ =
                                    tokio::time::timeout(std::time::Duration::from_millis(500), h)
                                        .await;
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await;
                });
            }
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
        // 清空 serve 的在册句柄：Runtime::shutdown 已等待 worker 进入 Stopped，
        // 该任务不会再被 join；留着会阻止后续 serve() 重新拉起（幂等判据）。
        drop(self.worker_handle.lock().take());
        tracing::info!(
            shutdown_ms = t0.elapsed().as_millis() as u64,
            "PyRuntimeCore::shutdown done"
        );
    }
}

impl PyRuntimeCore {
    /// 返回本节点的 endpoint_addr（hex postcard 编码），lazy 缓存。
    ///
    /// 节点启动后 iroh endpoint 不会变：首次调用计算并缓存，后续直接 clone。
    fn local_endpoint_addr(&self, _py: Python<'_>) -> PyResult<String> {
        let runtime = self.core()?;
        let mut cache = self.endpoint_addr_cache.lock();
        if let Some(addr) = cache.as_ref() {
            return Ok(addr.clone());
        }
        let addrs = runtime
            .network()
            .listen_addresses()
            .map_err(actant_error_to_pyerr)?;
        *cache = Some(addrs.endpoint_addr.clone());
        Ok(addrs.endpoint_addr)
    }

    /// 调用本地 `WorkflowActor` 的方法并解码返回 payload。
    ///
    /// GIL 在 `block_on` 期间释放（与其它网络/actor 调用一致），避免
    /// tokio worker 的 pyo3_log 回调与主线程在持有 GIL 时 block_on 死锁。
    fn call_workflow_actor<T: serde::de::DeserializeOwned>(
        &self,
        py: Python<'_>,
        method: &str,
        payload: Vec<u8>,
    ) -> PyResult<T> {
        let runtime = self.core()?;
        let tokio = self.tokio_handle()?;
        let system = runtime.actor_system().clone();
        let actor_id = runtime.workflow_actor_id().clone();
        let method = method.to_string();
        let result = py
            .detach(move || {
                tokio.block_on(async move { system.call(&actor_id, &method, payload).await })
            })
            .map_err(actant_error_to_pyerr)?;
        if let Some(err) = result.error {
            return Err(actant_error_to_pyerr(ActantError::from(err)));
        }
        decode(&result.payload).map_err(actant_error_to_pyerr)
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

/// Actor 返回错误时转换为对应的 `ActantError` 异常。
/// `JournalEvent` 的观测访问器在 core 内按 `python` 特性门控
/// （纯框架构建零 dead_code），绑定壳在此自行实现（enum 变体字段天然可见）。
fn event_payload_task_id(ev: &JournalEvent) -> Option<&str> {
    match ev {
        JournalEvent::TaskDispatched { task_id, .. }
        | JournalEvent::TaskRunning { task_id, .. }
        | JournalEvent::TaskCompleted { task_id, .. }
        | JournalEvent::TaskFailed { task_id, .. }
        | JournalEvent::TaskCancelled { task_id, .. } => Some(task_id.as_str()),
        _ => None,
    }
}
fn event_payload_error(ev: &JournalEvent) -> Option<&str> {
    match ev {
        JournalEvent::Failed { error, .. } | JournalEvent::TaskFailed { error, .. } => {
            Some(error.as_str())
        }
        _ => None,
    }
}
fn event_payload_wait_key(ev: &JournalEvent) -> Option<&str> {
    match ev {
        JournalEvent::WaitPointRegistered { wait_key, .. }
        | JournalEvent::SignalReceived { wait_key, .. }
        | JournalEvent::TimerFired { wait_key, .. } => Some(wait_key.as_str()),
        _ => None,
    }
}
/// 事件携带的不透明业务字节：节点入参、任务结果、信号载荷、上下文值。
/// 重放与导出都需要它们，故 journal 视为可消费数据而非仅元数据。
fn event_payload_bytes(ev: &JournalEvent) -> Option<&[u8]> {
    match ev {
        JournalEvent::Submitted {
            invocation: Some(inv),
            ..
        } => Some(&inv.args),
        JournalEvent::NodeAdded { node, .. } => Some(&node.payload),
        JournalEvent::TaskCompleted { result, .. } => Some(result),
        JournalEvent::SignalReceived { payload, .. } => Some(payload),
        JournalEvent::ContextValue { value, .. } => Some(value),
        _ => None,
    }
}
fn event_payload_condition(ev: &JournalEvent) -> Option<&WaitCondition> {
    match ev {
        JournalEvent::WaitPointRegistered { condition, .. } => Some(condition),
        _ => None,
    }
}
/// 等待条件的稳定字符串标签（``signal:<name>`` / ``timer:<deadline_ms>`` /
/// ``suspend``），供导出面跨语言消费；完整条件以 ``JournalEvent`` 为准。
fn wait_condition_label(condition: &WaitCondition) -> String {
    match condition {
        WaitCondition::Signal { name } => format!("signal:{name}"),
        WaitCondition::Timer { deadline_ms } => format!("timer:{deadline_ms}"),
        WaitCondition::Suspend => "suspend".to_string(),
        // 枚举 non_exhaustive：未来变体在旧消费端以显式标签暴露而非静默丢失。
        other => format!("unknown:{other:?}"),
    }
}
fn invocation_to_py(py: Python<'_>, inv: &WorkflowInvocation) -> PyResult<Py<PyAny>> {
    let item = pyo3::types::PyDict::new(py);
    item.set_item("workflow_id", inv.workflow_id.as_str())?;
    item.set_item("name", &inv.name)?;
    item.set_item("version", inv.version)?;
    item.set_item("args", pyo3::types::PyBytes::new(py, &inv.args))?;
    Ok(item.into_any().unbind())
}
fn event_payload_kind(ev: &JournalEvent) -> &'static str {
    match ev {
        JournalEvent::Submitted { .. } => "Submitted",
        JournalEvent::NodeAdded { .. } => "NodeAdded",
        JournalEvent::TaskDispatched { .. } => "TaskDispatched",
        JournalEvent::Started { .. } => "Started",
        JournalEvent::TaskRunning { .. } => "TaskRunning",
        JournalEvent::TaskCompleted { .. } => "TaskCompleted",
        JournalEvent::TaskFailed { .. } => "TaskFailed",
        JournalEvent::TaskCancelled { .. } => "TaskCancelled",
        JournalEvent::Completed { .. } => "Completed",
        JournalEvent::Failed { .. } => "Failed",
        JournalEvent::Cancelled { .. } => "Cancelled",
        JournalEvent::WaitPointRegistered { .. } => "WaitPointRegistered",
        JournalEvent::SignalReceived { .. } => "SignalReceived",
        JournalEvent::TimerFired { .. } => "TimerFired",
        JournalEvent::ContextValue { .. } => "ContextValue",
        JournalEvent::Recovered { .. } => "Recovered",
    }
}
/// "同一结构两处构造"漂移。
fn retry_policy_to_py(py: Python<'_>, policy: Option<&RetryPolicy>) -> PyResult<Py<PyAny>> {
    let Some(p) = policy else {
        return Ok(py.None());
    };
    let d = pyo3::types::PyDict::new(py);
    d.set_item("max_retries", p.max_retries)?;
    d.set_item("delay_ms", p.delay_ms)?;
    d.set_item("backoff_multiplier", p.backoff_multiplier)?;
    d.set_item("max_delay_ms", p.max_delay_ms)?;
    Ok(d.into_any().unbind())
}

fn batch_progress_to_py(
    py: Python<'_>,
    progress: &actant_core::runtime::batch::BatchProgress,
) -> PyResult<Py<PyAny>> {
    let d = pyo3::types::PyDict::new(py);
    d.set_item("batch_id", &progress.batch_id)?;
    d.set_item("total", progress.total)?;
    d.set_item("completed", progress.completed)?;
    d.set_item("failed", progress.failed)?;
    d.set_item("cancelled", progress.cancelled)?;
    d.set_item("pending", progress.pending)?;
    d.set_item("finished", progress.finished())?;
    Ok(d.into_any().unbind())
}

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
    }
}

/// 在 Python 模块上注册 runtime 数据类型与核心。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyNode>()?;
    m.add_class::<PyTask>()?;
    m.add_class::<PyListenAddresses>()?;
    m.add_class::<PyRuntimeCore>()?;
    m.add_function(pyo3::wrap_pyfunction!(metrics_text, m)?)?;
    Ok(())
}

/// 返回所有已注册指标的 Prometheus exposition format 文本。
///
/// 用于 `actant worker --metrics-port` 启动的 HTTP 端点抓取，或供
/// 用户在自己的 HTTP 服务器中直接暴露。若未调用 `metrics::init()`
/// （如纯 Python 测试场景），返回空字符串。
#[pyfunction]
fn metrics_text() -> String {
    actant_core::metrics::prometheus_text()
}
