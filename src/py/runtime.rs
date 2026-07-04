use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::actor::ActorSystem;
use crate::common::{
    ActorId, NetworkConfig, NodeId, RetryPolicy, TaskCompletion, TaskId, WireDagStateUpdate,
    WorkflowId, TOPIC_DAG_STATE,
};
use crate::event_bus::{BusEvent, EventBus, Topic};
use crate::network::{NetworkManager, Transport};
use crate::orchestrator::{
    DagGossip, FailoverManager, FailureScope, FailureStrategy, Orchestrator, Phase, Scheduler,
    Terminal,
};
use crate::worker::{TaskRegistry, WorkerRuntime};

use super::actor::PythonActor;
use super::actor_ops::PyActorCore;
use super::awaitable::{future_into_py_iter, FutureResultToPy};
use super::config::{PyActantConfig, PyRetryPolicy};
use super::event::PyEventBridge;
use super::gil_thread::GilThread;

// ---------------------------------------------------------------------------
// 跨 PyO3 边界的 typed struct — 替代裸 tuple，提升可读性与稳定性
// ---------------------------------------------------------------------------

/// DAG 节点定义，由 Python 层构造后通过 `submit_dag` 提交。
///
/// `priority` 为有符号整数；语义由 Python 层定义。
/// `metadata` 为不透明 key-value 映射，Rust 透传不解释。
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
///
/// 由 Rust 编排器产生（`complete_task_and_broadcast`、`build_ready_tasks` 等），
/// Python 侧编排循环消费并可能修改 `target_node`/`target_endpoint_addr` 后重新入队。
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
    /// 任务级超时（毫秒）。`None` 表示使用 worker 默认值。
    /// 必须在路由/重试过程中保留，否则重试任务会错误地使用 worker 默认超时。
    #[pyo3(get, set)]
    pub timeout_ms: Option<u64>,
    /// 任务级重试策略。`None` 表示使用 DAG 默认策略。
    /// 必须在路由/重试过程中保留，否则重试任务会错误地使用默认无重试。
    /// 使用 Rust 原生 `RetryPolicy` 存储，通过 getter/setter 与 Python 侧
    /// `_RetryPolicy` 互转，避免 `Py<T>` 不实现 `Clone` 的问题。
    pub retry_policy: Option<RetryPolicy>,
}

impl PyTask {
    /// 从内部 `TaskDefinition` 构造 PyO3 边界类型。
    /// 保留 `timeout_ms` 和 `retry_policy` 是为了避免任务在 Python 路由过程中
    /// 丢失这些属性（导致重试任务使用错误的全局默认值）。
    pub(crate) fn from_task_def(t: crate::common::TaskDefinition) -> Self {
        Self {
            task_id: t.id.to_string(),
            name: t.name,
            payload: t.payload,
            workflow_id: t.workflow_id.map(|w| w.to_string()),
            target_node: t.target_node.map(|n| n.to_string()),
            target_endpoint_addr: t.target_endpoint_addr,
            timeout_ms: t.timeout_ms,
            retry_policy: t.retry_policy,
        }
    }
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

    /// Python 侧 getter：将 Rust `RetryPolicy` 转为 `_RetryPolicy`。
    #[getter]
    fn retry_policy(&self, py: Python<'_>) -> PyResult<Option<Py<PyRetryPolicy>>> {
        match self.retry_policy.as_ref() {
            Some(p) => Ok(Some(Py::new(py, PyRetryPolicy::from(p.clone()))?)),
            None => Ok(None),
        }
    }

    /// Python 侧 setter：将 `_RetryPolicy` 转为 Rust `RetryPolicy`。
    #[setter]
    fn set_retry_policy(&mut self, value: Option<PyRetryPolicy>) {
        self.retry_policy = value.map(RetryPolicy::from);
    }

    fn __repr__(&self) -> String {
        format!("_TaskDef(task_id={:?}, name={:?})", self.task_id, self.name)
    }
}

/// Peer 容量信息：可用槽位、最大槽位、endpoint 地址。
#[pyclass(name = "_PeerCapacity", skip_from_py_object)]
#[derive(Clone)]
pub struct PyPeerCapacity {
    #[pyo3(get, set)]
    pub available: u32,
    #[pyo3(get, set)]
    pub max: u32,
    #[pyo3(get, set)]
    pub endpoint_addr: Option<String>,
}

#[pymethods]
impl PyPeerCapacity {
    #[new]
    #[pyo3(signature = (available, max, endpoint_addr=None))]
    fn new(available: u32, max: u32, endpoint_addr: Option<String>) -> Self {
        Self {
            available,
            max,
            endpoint_addr,
        }
    }
}

/// 激活后继任务结果：(new_tasks, broadcast_ids)。
pub type PyActivateResult = (Vec<PyTask>, Vec<(String, String)>);

// ---------------------------------------------------------------------------
// PyRetryInfo — 任务失败后返回给 Python
// ---------------------------------------------------------------------------

#[pyclass(name = "_RetryInfo", skip_from_py_object)]
#[derive(Clone)]
pub struct PyRetryInfo {
    #[pyo3(get)]
    pub current_retry_count: u32,
    #[pyo3(get)]
    pub max_retries: u32,
    #[pyo3(get)]
    pub next_delay_ms: u64,
}

// ---------------------------------------------------------------------------
// RuntimeContext — 持有所有 Arc 引用，shutdown 时 drop
// ---------------------------------------------------------------------------

/// 所有共享 Rust runtime 组件。`start()` 时创建一次，
/// `shutdown()` 时 drop 一次。以单个原子取出的容器替代
/// 逐字段的 `Mutex<Option<Arc<...>>>` 模式。
struct RuntimeContext {
    orchestrator: Arc<Orchestrator>,
    actor_system: Arc<ActorSystem>,
    gossip: DagGossip,
    failover: Arc<FailoverManager>,
    worker: Arc<WorkerRuntime>,
    scheduler: Arc<dyn Scheduler>,
    network: Arc<dyn Transport>,
    default_task_timeout_ms: u64,
    /// Payload 签名密钥。用于签名提交的任务和验证接收的任务。
    payload_signing_key: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Transport 选择
// ---------------------------------------------------------------------------

/// 构建基于 iroh 的 transport。
///
/// 所有 runtime — 生产、集成测试、单元测试 — 使用同一个基于 iroh 的
/// `NetworkManager`。测试选择 `discovery_mode = "none"`（NoDiscovery），
/// 仅绑定 loopback，从不联系任何外部服务；
/// 已验证在沙盒环境中 <200 ms 启动。
///
/// 不再读取 `ACTANT_TRANSPORT` 环境变量 — 只有一个 transport 实现。
/// `ACTANT_DISCOVERY` 仍被采纳以覆盖 discovery 模式
/// （测试套件用于强制 `none`）。
async fn build_transport(
    node_id: NodeId,
    config: NetworkConfig,
) -> crate::common::Result<Arc<dyn Transport>> {
    let _span = tracing::debug_span!("transport.build", node = ?node_id).entered();
    let nm = NetworkManager::new(node_id, config).await?;
    Ok(Arc::new(nm))
}

// ---------------------------------------------------------------------------
// PyRuntimeCore — 唯一的 Python 端 runtime 对象
// ---------------------------------------------------------------------------

#[pyclass(name = "_RuntimeCore", skip_from_py_object)]
pub struct PyRuntimeCore {
    /// 所有重型 Rust 资源。运行时为 `Some`，shutdown 时 `take()`。
    ctx: std::sync::Mutex<Option<RuntimeContext>>,
    event_bridge: Arc<PyEventBridge>,
    gil_thread: GilThread,
    tokio_handle: tokio::runtime::Handle,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    failover_cancel_tx: Option<tokio::sync::watch::Sender<bool>>,
    timeout_cancel_tx: Option<tokio::sync::watch::Sender<bool>>,
    persist_flush_cancel_tx: Option<tokio::sync::watch::Sender<bool>>,
    runtime_thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Python actor 的共享 dispatcher 注册表。
    actor_dispatchers: Arc<dashmap::DashMap<String, Py<PyAny>>>,
}

impl PyRuntimeCore {
    /// 辅助：访问 RuntimeContext 的字段。
    /// 若 runtime 已 shutdown 返回 `PyRuntimeError`。
    fn with_ctx<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&RuntimeContext) -> R,
    {
        let guard = self.ctx.lock().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Actant runtime lock poisoned — this is an internal error. \
                Please report at https://github.com/actant/actant/issues",
            )
        })?;
        guard
            .as_ref()
            .map(f)
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Actant runtime is not running. Call actant.start() first, or use 'with actant.start(...):' context manager.",
                )
            })
    }

    fn orch(&self) -> PyResult<Arc<Orchestrator>> {
        self.with_ctx(|c| c.orchestrator.clone())
    }
    fn gossip(&self) -> PyResult<DagGossip> {
        self.with_ctx(|c| c.gossip.clone())
    }
    fn sched(&self) -> PyResult<Arc<dyn Scheduler>> {
        self.with_ctx(|c| c.scheduler.clone())
    }
    fn worker(&self) -> PyResult<Arc<WorkerRuntime>> {
        self.with_ctx(|c| c.worker.clone())
    }
    fn failover(&self) -> PyResult<Arc<FailoverManager>> {
        self.with_ctx(|c| c.failover.clone())
    }
    fn network(&self) -> PyResult<Arc<dyn Transport>> {
        self.with_ctx(|c| c.network.clone())
    }
    fn actor_system(&self) -> PyResult<Arc<ActorSystem>> {
        self.with_ctx(|c| c.actor_system.clone())
    }
    fn default_timeout(&self) -> PyResult<u64> {
        self.with_ctx(|c| c.default_task_timeout_ms)
    }

    fn signing_key(&self) -> PyResult<Vec<u8>> {
        self.with_ctx(|c| c.payload_signing_key.clone())
    }

    fn spawn_awaitable<'py, F>(&self, py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
    where
        F: std::future::Future<Output = FutureResultToPy> + Send + 'static,
    {
        future_into_py_iter(py, self.tokio_handle.clone(), &self.gil_thread, fut)
    }
}

#[pymethods]
impl PyRuntimeCore {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    #[staticmethod]
    #[pyo3(signature = (name, config=None, node_id=None, tasks=None))]
    fn start(
        py: Python<'_>,
        name: String,
        config: Option<PyActantConfig>,
        node_id: Option<String>,
        tasks: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Self> {
        let config = config.unwrap_or_default();
        Self::start_inner(py, name, node_id, config, tasks.unwrap_or_default())
    }

    // ------------------------------------------------------------------
    // Identity & status
    // ------------------------------------------------------------------

    fn peer_id(&self) -> PyResult<String> {
        Ok(self.network()?.local_peer_id().to_string())
    }

    fn node_id(&self) -> PyResult<String> {
        Ok(self.worker()?.node_id().to_string())
    }

    fn running_task_count(&self) -> PyResult<usize> {
        Ok(self.worker()?.running_task_count())
    }

    fn max_concurrent_tasks(&self) -> PyResult<usize> {
        Ok(self.worker()?.max_concurrent_tasks())
    }

    fn available_capacity(&self) -> PyResult<usize> {
        let worker = self.worker()?;
        let max = worker.max_concurrent_tasks();
        let running = worker.running_task_count();
        Ok(max.saturating_sub(running))
    }

    fn max_capacity(&self) -> PyResult<usize> {
        Ok(self.worker()?.max_concurrent_tasks())
    }

    fn get_health_info(&self, py: Python<'_>) -> PyResult<(String, usize)> {
        let worker = self.worker()?;
        let state = *worker.subscribe_state().borrow();
        let status_str = state.as_str();
        let failover = self.failover()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            let peer_count = handle.block_on(async { failover.get_peer_infos().len() });
            Ok((status_str.to_string(), peer_count))
        })
    }

    fn get_metrics_snapshot(&self, py: Python<'_>) -> PyResult<HashMap<String, u64>> {
        let worker = self.worker()?;
        let failover = self.failover()?;
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            let mut snapshot = HashMap::new();
            snapshot.insert("running_tasks".into(), worker.running_task_count() as u64);
            snapshot.insert(
                "max_concurrent_tasks".into(),
                worker.max_concurrent_tasks() as u64,
            );

            let peer_infos = handle.block_on(async { failover.get_peer_infos() });
            snapshot.insert("connected_peers".into(), peer_infos.len() as u64);

            let active_wfs = handle.block_on(async { orch.active_workflow_ids().await });
            snapshot.insert("active_workflows".into(), active_wfs.len() as u64);

            Ok(snapshot)
        })
    }

    // ------------------------------------------------------------------
    // Peer 容量
    // ------------------------------------------------------------------

    fn get_peer_capacities(&self, py: Python<'_>) -> PyResult<HashMap<String, PyPeerCapacity>> {
        let failover = self.failover()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            Ok(handle.block_on(async {
                failover
                    .get_peer_capacities()
                    .into_iter()
                    .map(|(id, (available, max, endpoint))| {
                        (
                            id.to_string(),
                            PyPeerCapacity {
                                available,
                                max,
                                endpoint_addr: endpoint,
                            },
                        )
                    })
                    .collect()
            }))
        })
    }

    fn _update_peer_capacity(&self, peer_id: String, available: u32, max: u32) -> PyResult<()> {
        self.failover()?
            .update_peer_capacity(NodeId::from(peer_id), available, max);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------

    /// 返回所有已注册指标的 Prometheus 文本展示格式。
    fn prometheus_text(&self) -> String {
        crate::metrics::prometheus_text()
    }

    // ------------------------------------------------------------------
    // 事件桥接
    // ------------------------------------------------------------------

    fn set_event_callback(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<()> {
        let asyncio_mod = py.import("asyncio")?;
        let event_loop = asyncio_mod.call_method0("get_running_loop")?;
        self.event_bridge
            .set_callback(event_loop.unbind(), callback);
        Ok(())
    }

    // ------------------------------------------------------------------
    // DAG submission & task enqueue
    // ------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (nodes, edges, workflow_timeout_ms = None, default_retry_policy = None, target_nodes = None, target_endpoint_addrs = None, failure_strategy = None))]
    fn submit_dag(
        &self,
        py: Python<'_>,
        nodes: Vec<PyNode>,
        edges: Vec<(usize, usize, Option<String>)>,
        workflow_timeout_ms: Option<u64>,
        default_retry_policy: Option<PyRetryPolicy>,
        target_nodes: Option<HashMap<usize, String>>,
        target_endpoint_addrs: Option<HashMap<usize, String>>,
        failure_strategy: Option<String>,
    ) -> PyResult<PyAsyncResultCore> {
        let mut dag = crate::orchestrator::Dag::new();
        dag.default_retry_policy = default_retry_policy.map(RetryPolicy::from);
        if let Some(fs) = failure_strategy {
            dag.failure_strategy = FailureStrategy::parse(&fs).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown failure strategy '{}': expected 'fail_fast' or 'continue'",
                    fs
                ))
            })?;
        } else {
            dag.failure_strategy = FailureStrategy::default();
        }
        let node_count = nodes.len();
        let edge_count = edges.len();
        let mut task_ids: Vec<TaskId> = Vec::with_capacity(node_count);

        let signing_key = self.signing_key()?;

        for node in nodes {
            let retry_policy = node.retry.map(RetryPolicy::from);
            let priority = node.priority.unwrap_or(0);
            let task_id = TaskId::generate();
            let signed_payload = crate::common::payload::sign(&signing_key, &node.payload)
                .map_err(pyo3::exceptions::PyValueError::new_err)?;
            let dag_node = crate::orchestrator::DagNode {
                task_id: task_id.clone(),
                name: node.name,
                payload: signed_payload,
                retry_policy,
                timeout_ms: node.timeout_ms,
                priority,
                metadata: node.metadata.unwrap_or_default(),
            };
            dag.add_node(dag_node)?;
            task_ids.push(task_id);
        }

        for (from_idx, to_idx, condition) in edges {
            if from_idx >= task_ids.len() || to_idx >= task_ids.len() {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "edge index out of range: ({}, {}) for {} nodes",
                    from_idx,
                    to_idx,
                    task_ids.len()
                )));
            }
            if let Some(cond) = condition {
                dag.add_conditional_edge(
                    task_ids[from_idx].clone(),
                    task_ids[to_idx].clone(),
                    cond,
                )?;
            } else {
                dag.add_edge(task_ids[from_idx].clone(), task_ids[to_idx].clone())?;
            }
        }

        let target_nodes_map = target_nodes.unwrap_or_default();
        let target_endpoint_addrs_map = target_endpoint_addrs.unwrap_or_default();

        let target_nodes_by_id: HashMap<String, String> = target_nodes_map
            .into_iter()
            .filter_map(|(idx, target)| task_ids.get(idx).map(|tid| (tid.to_string(), target)))
            .collect();
        let target_endpoint_addrs_by_id: HashMap<String, String> = target_endpoint_addrs_map
            .into_iter()
            .filter_map(|(idx, addr)| task_ids.get(idx).map(|tid| (tid.to_string(), addr)))
            .collect();

        let workflow_id = WorkflowId::from(format!("wf-{}", uuid::Uuid::new_v4()));
        let orchestrator = self.orch()?;
        let scheduler = self.sched()?;
        let worker = self.worker()?;
        let default_timeout = self.default_timeout()?;

        let wf_id = workflow_id.clone();
        let tokio_handle = self.tokio_handle.clone();
        // Release GIL during the async block_on. The orchestrator/worker
        // operations are pure Rust and don't need the GIL; holding it would
        // block any other Python thread (including the runtime thread's
        // tracing events routed via pyo3_log).
        let state = py
            .detach(|| {
                tokio_handle.block_on(async {
                    let _span = tracing::debug_span!(
                        "submit_dag",
                        wf = ?wf_id,
                        nodes = node_count,
                        edges = edge_count
                    )
                    .entered();

                    if let Some(timeout) = workflow_timeout_ms {
                        orchestrator
                            .submit_with_timeout(workflow_id.clone(), dag, timeout)
                            .await?;
                    } else {
                        orchestrator.submit(workflow_id.clone(), dag).await?;
                    }

                    let mut roots = orchestrator.start(&workflow_id).await?;
                    tracing::debug!(roots = roots.len(), "submit_dag: orchestrator.start");

                    let local_node = worker.node_id().clone();
                    for task in &mut roots {
                        if task.origin_node.is_none() {
                            task.origin_node = Some(local_node.clone());
                        }
                        if let Some(target) = target_nodes_by_id.get(task.id.as_str()) {
                            task.target_node = Some(NodeId::from(target.clone()));
                        }
                        if let Some(endpoint) = target_endpoint_addrs_by_id.get(task.id.as_str()) {
                            task.target_endpoint_addr = Some(endpoint.clone());
                        }
                    }

                    let local_tasks = dispatch_tasks(&worker, roots).await;
                    let local_count = local_tasks.len();
                    if !local_tasks.is_empty() {
                        scheduler.enqueue_batch(local_tasks).await?;
                    }
                    tracing::debug!(local = local_count, "submit_dag: dispatch+enqueue");

                    Ok::<_, crate::common::ActantError>(orchestrator.state_handle())
                })
            })
            .map_err(pyo3::PyErr::from)?;

        Ok(PyAsyncResultCore::new(
            wf_id.to_string(),
            state,
            self.tokio_handle.clone(),
            self.gil_thread.clone(),
            default_timeout,
        ))
    }

    #[allow(clippy::type_complexity)]
    fn enqueue_tasks(&self, py: Python<'_>, tasks: Vec<PyTask>) -> PyResult<()> {
        let sched = self.sched()?;
        let handle = self.tokio_handle.clone();
        let orchestrator = self.orch()?;
        let worker = self.worker()?;
        let task_count = tasks.len();
        // 在持有 GIL 时提取字段，避免在 detach 后访问 Python 对象。
        // retry_policy 已是 Rust 原生 RetryPolicy，无需 GIL 即可访问。
        let extracted: Vec<(
            String,
            String,
            Vec<u8>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<u64>,
            Option<RetryPolicy>,
        )> = tasks
            .into_iter()
            .map(|t| {
                (
                    t.task_id,
                    t.name,
                    t.payload,
                    t.workflow_id,
                    t.target_node,
                    t.target_endpoint_addr,
                    t.timeout_ms,
                    t.retry_policy,
                )
            })
            .collect();
        py.detach(|| {
            handle.block_on(async {
                let mut defs = Vec::with_capacity(extracted.len());
                for (
                    task_id,
                    name,
                    payload,
                    workflow_id,
                    target_node,
                    target_endpoint_addr,
                    timeout_ms,
                    retry_policy,
                ) in extracted
                {
                    let enqueued_at = crate::common::epoch_millis();
                    defs.push(crate::common::TaskDefinition {
                        id: TaskId::from(task_id),
                        name,
                        payload,
                        workflow_id: workflow_id.map(WorkflowId::from),
                        target_node: target_node.map(NodeId::from),
                        origin_node: orchestrator.node_id().cloned(),
                        retry_policy,
                        priority: 0,
                        timeout_ms,
                        attempt: 0,
                        enqueued_at_ms: enqueued_at,
                        target_endpoint_addr,
                        origin_endpoint_addr: None,
                    });
                }

                let local_tasks = dispatch_tasks(&worker, defs).await;
                let local_count = local_tasks.len();
                if !local_tasks.is_empty() {
                    sched.enqueue_batch(local_tasks).await?;
                }
                tracing::debug!(
                    total = task_count,
                    local = local_count,
                    "enqueue_tasks: dispatched"
                );
                Ok(())
            })
        })
    }

    fn _drain_unrouted_tasks(&self, py: Python<'_>) -> PyResult<Vec<PyTask>> {
        let sched = self.sched()?;
        let handle = self.tokio_handle.clone();
        let unrouted = py.detach(|| handle.block_on(async { sched.drain_unrouted().await }));
        Ok(unrouted.into_iter().map(PyTask::from_task_def).collect())
    }

    fn scheduler_stats(&self) -> PyResult<usize> {
        let sched = self.sched()?;
        Ok(sched.total_queued())
    }

    // ------------------------------------------------------------------
    // Orchestrator 操作
    // ------------------------------------------------------------------

    fn _mark_failed_and_get_retry_info(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
        error: String,
    ) -> PyResult<Option<PyRetryInfo>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle
                .block_on(async {
                    let wf_id = WorkflowId::from(workflow_id);
                    let tid = TaskId::from(task_id);

                    let retry_info = orch.get_retry_info(&wf_id, &tid).await;

                    if let Some((retry_count, policy, next_delay_ms)) = &retry_info {
                        if *retry_count < policy.max_retries {
                            orch.fail_task(&wf_id, &tid, error, FailureScope::TaskOnly)
                                .await?;
                            return Ok::<_, crate::common::ActantError>(Some(PyRetryInfo {
                                current_retry_count: *retry_count,
                                max_retries: policy.max_retries,
                                next_delay_ms: *next_delay_ms,
                            }));
                        }
                    }

                    orch.fail_task(&wf_id, &tid, error, FailureScope::WorkflowLevel)
                        .await?;
                    Ok(
                        retry_info.map(|(retry_count, policy, next_delay_ms)| PyRetryInfo {
                            current_retry_count: retry_count,
                            max_retries: policy.max_retries,
                            next_delay_ms,
                        }),
                    )
                })
                .map_err(pyo3::PyErr::from)
        })
    }

    #[pyo3(signature = (workflow_id, task_id, result, task_name = String::new()))]
    fn _complete_task_and_broadcast(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
        result: Vec<u8>,
        task_name: String,
    ) -> PyResult<PyActivateResult> {
        let orch = self.orch()?;
        let gossip = self.gossip()?;
        let handle = self.tokio_handle.clone();
        let (next, conditional_edges) = py.detach(|| {
            handle
                .block_on(async {
                    let wf_id = WorkflowId::from(workflow_id);
                    let tid = TaskId::from(task_id);

                    let _span = tracing::debug_span!(
                        "complete_task",
                        wf = ?wf_id,
                        task = ?tid
                    )
                    .entered();

                    let (next, conditional_edges) =
                        orch.on_task_completed(&wf_id, &tid, result.clone()).await?;
                    tracing::debug!(
                        next = next.len(),
                        cond = conditional_edges.len(),
                        "complete_task: on_task_completed"
                    );

                    let completion = TaskCompletion::Completed {
                        workflow_id: wf_id,
                        task_id: tid,
                        task_name,
                        result,
                        target_node: None,
                    };
                    gossip
                        .broadcast_state_update(
                            completion.workflow_id(),
                            completion.task_id(),
                            &completion,
                        )
                        .await?;

                    let cond_edges = conditional_edges
                        .into_iter()
                        .map(|(tid, tag)| (tid.to_string(), tag))
                        .collect::<Vec<_>>();
                    Ok::<_, crate::common::ActantError>((next, cond_edges))
                })
                .map_err(pyo3::PyErr::from)
        })?;
        let py_next = next.into_iter().map(PyTask::from_task_def).collect();
        Ok((py_next, conditional_edges))
    }

    #[pyo3(signature = (workflow_id, task_id))]
    fn _activate_conditional_successor(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
    ) -> PyResult<Option<PyTask>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        let task_def = py.detach(|| {
            handle
                .block_on(async {
                    let wf_id = WorkflowId::from(workflow_id);
                    let tid = TaskId::from(task_id);
                    orch.activate_conditional_successor(&wf_id, &tid).await
                })
                .map_err(pyo3::PyErr::from)
        })?;
        Ok(task_def.map(PyTask::from_task_def))
    }

    #[pyo3(signature = (workflow_id, task_id))]
    fn _skip_conditional_branch(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
    ) -> PyResult<Vec<PyTask>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        let tasks = py.detach(|| {
            handle
                .block_on(async {
                    let wf_id = WorkflowId::from(workflow_id);
                    let tid = TaskId::from(task_id);
                    orch.skip_conditional_branch(&wf_id, &tid).await
                })
                .map_err(pyo3::PyErr::from)
        })?;
        Ok(tasks.into_iter().map(PyTask::from_task_def).collect())
    }

    #[pyo3(signature = (workflow_id, task_id, error, task_name = String::new()))]
    fn _broadcast_failure(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
        error: String,
        task_name: String,
    ) -> PyResult<()> {
        let gossip = self.gossip()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async {
                let wf_id = WorkflowId::from(workflow_id);
                let tid = TaskId::from(task_id);
                let completion = TaskCompletion::Failed {
                    workflow_id: wf_id,
                    task_id: tid,
                    task_name,
                    error,
                    target_node: None,
                };
                gossip
                    .broadcast_state_update(
                        completion.workflow_id(),
                        completion.task_id(),
                        &completion,
                    )
                    .await
            })
        })
        .map_err(pyo3::PyErr::from)
    }

    fn cancel_workflow(&self, py: Python<'_>, workflow_id: String) -> PyResult<()> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| handle.block_on(async { orch.cancel(&WorkflowId::from(workflow_id)).await }))
            .map_err(pyo3::PyErr::from)
    }

    fn cancel_task(&self, py: Python<'_>, workflow_id: String, task_id: String) -> PyResult<bool> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async {
                orch.cancel_task(&WorkflowId::from(workflow_id), &TaskId::from(task_id))
                    .await
            })
        })
        .map_err(pyo3::PyErr::from)
    }

    fn _mark_workflow_failed(
        &self,
        py: Python<'_>,
        workflow_id: String,
        error: String,
    ) -> PyResult<()> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async {
                orch.mark_workflow_failed(&WorkflowId::from(workflow_id), error)
                    .await
            })
        })
        .map_err(pyo3::PyErr::from)
    }

    fn _build_ready_tasks(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_ids: Vec<String>,
    ) -> PyResult<Vec<PyTask>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        let tasks = py.detach(|| {
            handle
                .block_on(async {
                    let ids: Vec<TaskId> = task_ids.into_iter().map(TaskId::from).collect();
                    orch.build_ready_tasks_for(&WorkflowId::from(workflow_id), &ids)
                        .await
                })
                .map_err(pyo3::PyErr::from)
        })?;
        Ok(tasks.into_iter().map(PyTask::from_task_def).collect())
    }

    fn _get_retry_info(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
    ) -> PyResult<Option<PyRetryInfo>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle
                .block_on(async {
                    Ok::<_, crate::common::ActantError>(
                        orch.get_retry_info(&WorkflowId::from(workflow_id), &TaskId::from(task_id))
                            .await
                            .map(|(retry_count, policy, next_delay_ms)| PyRetryInfo {
                                current_retry_count: retry_count,
                                max_retries: policy.max_retries,
                                next_delay_ms,
                            }),
                    )
                })
                .map_err(pyo3::PyErr::from)
        })
    }

    fn _prepare_retry(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
    ) -> PyResult<Option<PyTask>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        let task_def = py.detach(|| {
            handle
                .block_on(async {
                    orch.prepare_task_retry(&WorkflowId::from(workflow_id), &TaskId::from(task_id))
                        .await
                })
                .map_err(pyo3::PyErr::from)
        })?;
        Ok(task_def.map(PyTask::from_task_def))
    }

    fn _mark_task_running(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
    ) -> PyResult<()> {
        let orch = self.orch()?;
        let gossip = self.gossip()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async {
                let wf = WorkflowId::from(workflow_id);
                let tid = TaskId::from(task_id);
                orch.mark_task_running(&wf, &tid).await?;
                gossip.broadcast_task_running(&wf, &tid).await?;
                Ok::<_, crate::common::ActantError>(())
            })
        })
        .map_err(pyo3::PyErr::from)
    }

    fn _apply_dag_state_update(
        &self,
        py: Python<'_>,
        workflow_id: String,
        task_id: String,
        state: String,
        data: Vec<u8>,
    ) -> PyResult<()> {
        let gossip = self.gossip()?;
        let origin_node_id = self
            .orch()?
            .node_id()
            .cloned()
            .unwrap_or(NodeId::from("unknown"));
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async {
                let hlc_timestamp = gossip.clock().tick();
                let task_state = crate::common::WireTaskState::from_python_str(&state, data)
                    .ok_or_else(|| {
                        crate::common::ActantError::Internal(format!(
                            "unknown task state: {}",
                            state
                        ))
                    })?;
                let update = WireDagStateUpdate {
                    workflow_id: WorkflowId::from(workflow_id),
                    task_id: TaskId::from(task_id),
                    task_state,
                    hlc_timestamp,
                    origin_node: origin_node_id,
                };
                gossip.apply_remote_update(update).await
            })
        })
        .map_err(pyo3::PyErr::from)
    }

    fn _handle_heads_exchange(&self, py: Python<'_>, data: Vec<u8>) -> PyResult<()> {
        let gossip = self.gossip()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async {
                let exchange: crate::common::HeadsExchange = postcard::from_bytes(&data)?;
                gossip.handle_heads_exchange(&exchange).await
            })
        })
        .map_err(pyo3::PyErr::from)
    }

    fn gossip_stats(&self) -> PyResult<(usize, usize, u64, u64)> {
        Ok(self.gossip()?.dedup_stats())
    }

    fn get_stored_results(
        &self,
        py: Python<'_>,
        workflow_id: String,
    ) -> PyResult<Option<Vec<Vec<u8>>>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle
                .block_on(async {
                    let wf_id = WorkflowId::from(workflow_id);
                    Ok::<_, crate::common::ActantError>(orch.get_results(&wf_id).await)
                })
                .map_err(pyo3::PyErr::from)
        })
    }

    fn _recoverable_workflows_with_pending(
        &self,
        py: Python<'_>,
    ) -> PyResult<Vec<(String, Vec<String>)>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle
                .block_on(async {
                    let wf_ids = orch.active_workflow_ids().await;
                    let mut result = Vec::new();
                    for wf_id in &wf_ids {
                        if let Some(slot) = orch.state_handle().get_slot(wf_id) {
                            if slot.execution.is_terminal() {
                                continue;
                            }
                            let ready: Vec<String> = slot
                                .pending
                                .iter()
                                .filter(|(_, &count)| count == 0)
                                .map(|(tid, _)| tid.to_string())
                                .collect();
                            if !ready.is_empty() {
                                result.push((wf_id.to_string(), ready));
                            }
                        }
                    }
                    Ok::<_, crate::common::ActantError>(result)
                })
                .map_err(pyo3::PyErr::from)
        })
    }

    fn list_workflows(&self, py: Python<'_>) -> PyResult<Vec<(String, String)>> {
        let orch = self.orch()?;
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle
                .block_on(async {
                    let wf_ids = orch.active_workflow_ids().await;
                    let mut result = Vec::new();
                    for wf_id in &wf_ids {
                        if let Some(slot) = orch.state_handle().get_slot(wf_id) {
                            let state_str = slot.execution.state.as_str().to_string();
                            result.push((wf_id.to_string(), state_str));
                        }
                    }
                    Ok::<_, crate::common::ActantError>(result)
                })
                .map_err(pyo3::PyErr::from)
        })
    }

    /// 返回指定 workflow ID 的 workflow 级状态字符串。
    /// workflow 不存在时返回 None。
    fn workflow_state(&self, py: Python<'_>, workflow_id: String) -> PyResult<Option<String>> {
        let orch = self.orch()?;
        let wf_id = WorkflowId::from(workflow_id);
        py.detach(|| {
            let state_handle = orch.state_handle();
            let slot = state_handle.get_slot(&wf_id);
            Ok(slot.map(|s| s.execution.state.as_str().to_string()))
        })
    }

    /// 返回指定 workflow ID 的任务级状态。
    /// 返回 (task_id, state_str) 列表；workflow 不存在时返回 None。
    fn task_states(
        &self,
        py: Python<'_>,
        workflow_id: String,
    ) -> PyResult<Option<Vec<(String, String)>>> {
        let orch = self.orch()?;
        let wf_id = WorkflowId::from(workflow_id);
        py.detach(|| {
            let state_handle = orch.state_handle();
            let slot = state_handle.get_slot(&wf_id);
            Ok(slot.map(|s| {
                s.execution
                    .tasks
                    .iter()
                    .map(|(id, ts)| (id.to_string(), ts.state.as_str().to_string()))
                    .collect()
            }))
        })
    }

    // ------------------------------------------------------------------
    // Failover 操作
    // ------------------------------------------------------------------

    fn get_peer_infos(&self) -> PyResult<Vec<(String, u64, Vec<String>)>> {
        let peers = self.failover()?.get_peer_infos();
        Ok(peers
            .into_iter()
            .map(|(node_id, info)| {
                (
                    node_id.to_string(),
                    info.last_heartbeat_ms,
                    info.active_workflows
                        .into_iter()
                        .map(|w| w.to_string())
                        .collect(),
                )
            })
            .collect())
    }

    fn _detect_failed_nodes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let failover = self.failover()?;
        self.spawn_awaitable(py, async move {
            let expired = failover.expire_stale_peers();
            let result: Vec<(String, Vec<String>)> = expired
                .into_iter()
                .map(|(node_id, info)| {
                    let wf_ids: Vec<String> = info
                        .active_workflows
                        .iter()
                        .map(|w| w.to_string())
                        .collect();
                    (node_id.to_string(), wf_ids)
                })
                .collect();
            Python::attach(|py| {
                let bound = pyo3::IntoPyObject::into_pyobject(result, py).map_err(|e| {
                    pyo3::PyErr::from(crate::common::ActantError::Internal(e.to_string()))
                })?;
                let obj: pyo3::Py<pyo3::PyAny> = bound.clone().into_any().unbind();
                Ok::<_, pyo3::PyErr>(FutureResultToPy::Value(obj))
            })
            .unwrap_or_else(FutureResultToPy::Err)
        })
        .map(|b| b.unbind())
    }

    fn _should_claim_workflow(
        &self,
        py: Python<'_>,
        workflow_ids: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let failover = self.failover()?;
        self.spawn_awaitable(py, async move {
            let peers = failover.get_peer_infos();
            let my_id = failover.node_id();
            let candidate_ids: Vec<String> = {
                let mut ids: Vec<_> = peers.keys().map(|k| k.to_string()).collect();
                ids.push(my_id.to_string());
                ids
            };

            let should_claim = workflow_ids.iter().any(|wf_id| {
                crate::common::should_claim_workflow(wf_id, my_id.as_str(), candidate_ids.clone())
            });

            Python::attach(|py| {
                let bound = pyo3::IntoPyObject::into_pyobject(should_claim, py).map_err(|e| {
                    pyo3::PyErr::from(crate::common::ActantError::Internal(e.to_string()))
                })?;
                let obj: pyo3::Py<pyo3::PyAny> = pyo3::Bound::<pyo3::types::PyBool>::clone(&bound)
                    .into_any()
                    .unbind();
                Ok::<_, pyo3::PyErr>(FutureResultToPy::Value(obj))
            })
            .unwrap_or_else(FutureResultToPy::Err)
        })
        .map(|b| b.unbind())
    }

    fn _active_leases(&self) -> PyResult<Vec<(String, String, u64, u64)>> {
        Ok(self.failover()?.active_leases())
    }

    // ------------------------------------------------------------------
    // 网络操作
    // ------------------------------------------------------------------

    fn listen_addresses(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let network = self.network()?;
        self.spawn_awaitable(py, async move {
            match network.listen_addresses().await {
                Ok(addrs) => Python::attach(|py| match (|py: Python<'_>| -> PyResult<Py<PyAny>> {
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("endpoint_id", &addrs.endpoint_id)?;
                    dict.set_item("relay_url", &addrs.relay_url)?;
                    dict.set_item("endpoint_addr", &addrs.endpoint_addr)?;
                    let direct_list = pyo3::types::PyList::empty(py);
                    for addr in &addrs.direct_addrs {
                        direct_list.append(addr)?;
                    }
                    dict.set_item("direct_addrs", direct_list)?;
                    Ok(dict.into_any().unbind())
                })(py)
                {
                    Ok(v) => FutureResultToPy::Value(v),
                    Err(e) => FutureResultToPy::Err(e),
                }),
                Err(e) => Python::attach(|_py| FutureResultToPy::Err(pyo3::PyErr::from(e))),
            }
        })
        .map(|b| b.unbind())
    }

    fn dial(&self, py: Python<'_>, addr: String) -> PyResult<Py<PyAny>> {
        let network = self.network()?;
        self.spawn_awaitable(py, async move {
            let result = network.dial(&addr).await;
            match &result {
                Ok(()) => tracing::info!(addr = %addr, "dial succeeded"),
                Err(e) => tracing::warn!(addr = %addr, error = %e, "dial failed"),
            }
            match result {
                Ok(()) => Python::attach(|py| FutureResultToPy::Value(py.None().into_any())),
                Err(e) => Python::attach(|_py| FutureResultToPy::Err(pyo3::PyErr::from(e))),
            }
        })
        .map(|b| b.unbind())
    }

    fn discover_peers(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let network = self.network()?;
        self.spawn_awaitable(py, async move {
            match network.discover_peers().await {
                Ok(peers) => Python::attach(|py| match (|py: Python<'_>| -> PyResult<Py<PyAny>> {
                    let pylist = pyo3::types::PyList::empty(py);
                    for peer in &peers {
                        pylist.append(pyo3::types::PyString::new(py, &peer.0))?;
                    }
                    Ok(pylist.into_any().unbind())
                })(py)
                {
                    Ok(v) => FutureResultToPy::Value(v),
                    Err(e) => FutureResultToPy::Err(e),
                }),
                Err(e) => Python::attach(|_py| FutureResultToPy::Err(pyo3::PyErr::from(e))),
            }
        })
        .map(|b| b.unbind())
    }

    fn _add_gossip_peer(&self, py: Python<'_>, peer_id: String) -> PyResult<Py<PyAny>> {
        let network = self.network()?;
        self.spawn_awaitable(py, async move {
            match network.add_gossip_peer(&peer_id).await {
                Ok(()) => Python::attach(|py| FutureResultToPy::Value(py.None().into_any())),
                Err(e) => Python::attach(|_py| FutureResultToPy::Err(pyo3::PyErr::from(e))),
            }
        })
        .map(|b| b.unbind())
    }

    // ------------------------------------------------------------------
    // Actor 操作
    // ------------------------------------------------------------------

    fn create_actor(
        &self,
        py: Python<'_>,
        name: String,
        dispatcher: Py<PyAny>,
    ) -> PyResult<String> {
        let actor_id = ActorId::generate();
        let actor_id_str = actor_id.to_string();
        let system = self.actor_system()?;
        let disp_clone = Python::attach(|py| dispatcher.clone_ref(py));
        self.actor_dispatchers
            .insert(actor_id_str.clone(), disp_clone);
        let python_actor = PythonActor::new(name, dispatcher);
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async move {
                system
                    .spawn(actor_id, python_actor)
                    .await
                    .map_err(pyo3::PyErr::from)
            })
        })?;
        Ok(actor_id_str)
    }

    fn create_actor_with_id(
        &self,
        py: Python<'_>,
        name: String,
        actor_id: String,
        dispatcher: Py<PyAny>,
    ) -> PyResult<String> {
        let actor_id_obj = ActorId::from(actor_id.clone());
        let system = self.actor_system()?;
        let disp_clone = Python::attach(|py| dispatcher.clone_ref(py));
        self.actor_dispatchers.insert(actor_id.clone(), disp_clone);
        let python_actor = PythonActor::new(name, dispatcher);
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async move {
                system
                    .spawn(actor_id_obj, python_actor)
                    .await
                    .map_err(pyo3::PyErr::from)
            })
        })?;
        Ok(actor_id)
    }

    fn actor_core(&self) -> PyResult<PyActorCore> {
        Ok(PyActorCore::new_with_dispatchers(
            self.actor_system()?,
            self.tokio_handle.clone(),
            self.gil_thread.clone(),
            self.actor_dispatchers.clone(),
        ))
    }

    // ------------------------------------------------------------------
    // Workflow 状态辅助
    // ------------------------------------------------------------------

    fn workflow_state_completed(&self) -> super::config::PyWorkflowState {
        super::config::PyWorkflowState::Completed
    }

    fn workflow_state_failed(&self) -> super::config::PyWorkflowState {
        super::config::PyWorkflowState::Failed
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    fn drain(&self) -> PyResult<()> {
        let _span = tracing::debug_span!("runtime.drain").entered();
        self.worker()?.drain();
        Ok(())
    }

    #[pyo3(signature = (timeout_ms = None))]
    fn shutdown(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<()> {
        let _span = tracing::debug_span!(
            "runtime.shutdown",
            timeout_ms = ?timeout_ms
        )
        .entered();

        tracing::debug!("shutdown: signaling background loops");

        // 1. 通知所有后台任务退出。
        //    先触发 worker 的 cancel_tx，使其子任务（network event
        //    loop、retry loop）在 rt.shutdown_timeout 强制取消前退出。
        //    Python 层已调用 drain()，但 worker 的 run() loop 可能
        //    还未被 poll 到 Draining 状态 — shutdown() 无论如何都会发送 cancel_tx。
        if let Ok(worker) = self.worker() {
            worker.shutdown();
        }
        let _ = self.shutdown_tx.send(true);
        if let Some(ref cancel_tx) = self.failover_cancel_tx {
            let _ = cancel_tx.send(true);
        }
        if let Some(ref cancel_tx) = self.timeout_cancel_tx {
            let _ = cancel_tx.send(true);
        }
        if let Some(ref cancel_tx) = self.persist_flush_cancel_tx {
            let _ = cancel_tx.send(true);
        }

        // 2. Wait for the runtime thread to exit.
        //
        //    CRITICAL: Release the GIL while waiting. The runtime thread's
        //    tokio tasks emit `tracing` events, which flow through `pyo3_log`
        //    → Python `logging` and therefore require the GIL. If we hold
        //    the GIL during `thread::sleep`, those tasks cannot process the
        //    shutdown signal (their logging calls block on GIL acquisition),
        //    causing a ~10-second deadlock until the timeout expires.
        //
        //    `py.detach` releases the GIL for the duration of the wait,
        //    letting the runtime thread's tasks acquire it as needed.
        py.detach(|| {
            let mut guard = self
                .runtime_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(handle) = guard.take() {
                match timeout_ms {
                    Some(ms) => {
                        let deadline =
                            std::time::Instant::now() + std::time::Duration::from_millis(ms);
                        loop {
                            if std::time::Instant::now() >= deadline {
                                tracing::warn!(
                                    "shutdown: runtime thread did not exit within {}ms, detaching",
                                    ms
                                );
                                break;
                            }
                            if handle.is_finished() {
                                let _ = handle.join();
                                tracing::debug!("shutdown: runtime thread joined");
                                break;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(50));
                        }
                    }
                    None => {
                        let _ = handle.join();
                        tracing::debug!("shutdown: runtime thread joined");
                    }
                }
            }
        });

        // 3. Break Python→Rust→Python reference cycle.
        tracing::debug!("shutdown: clearing event bridge callback");
        self.event_bridge.clear_callback();

        // 4. Signal LMDB to prepare for closing. Must be called before
        //    ctx.take() so the closing event is ready to wait on.
        let closing_event = {
            let guard = self.ctx.lock().unwrap_or_else(|e| e.into_inner());
            guard
                .as_ref()
                .and_then(|ctx| ctx.orchestrator.store().as_ref().map(|s| s.prepare_close()))
        };

        // 5. Release all Arc references atomically.
        tracing::debug!("shutdown: releasing runtime context");
        {
            let mut guard = self.ctx.lock().unwrap_or_else(|e| e.into_inner());
            guard.take();
        }

        // 6. Force Python GC to release lingering Python→Rust references
        //    that might keep Arc<Env> alive via the Store chain.
        py.detach(|| {
            Python::attach(|py| {
                let gc_mod = py.import("gc")?;
                gc_mod.call_method0("collect")?;
                Ok::<(), pyo3::PyErr>(())
            })
            .ok();
        });

        // 7. 等待 LMDB 真正关闭（释放 lock.mdb）。
        //    对使用相同 data_dir 重启的测试至关重要。
        //    在等待期间释放 GIL，理由同步骤 2。
        if let Some(event) = closing_event {
            py.detach(|| {
                if !event.wait_timeout(std::time::Duration::from_secs(5)) {
                    tracing::warn!("shutdown: LMDB close event wait timed out (5s)");
                }
            });
        }

        // 8. 写出 profiling 工件（pprof 火焰图等）。
        crate::observability::shutdown();

        tracing::info!("shutdown: complete");
        Ok(())
    }
}

impl Drop for PyRuntimeCore {
    fn drop(&mut self) {
        // 确保 LMDB 和所有 Rust 资源在 shutdown 未被调用时也被释放。
        // 这防止了 LMDB 文件句柄泄漏。
        let _ = self.shutdown_tx.send(true);
        if let Some(ref cancel_tx) = self.failover_cancel_tx {
            let _ = cancel_tx.send(true);
        }
        if let Some(ref cancel_tx) = self.timeout_cancel_tx {
            let _ = cancel_tx.send(true);
        }
        if let Some(ref cancel_tx) = self.persist_flush_cancel_tx {
            let _ = cancel_tx.send(true);
        }
        {
            let mut guard = self
                .runtime_thread
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(handle) = guard.take() {
                // 给 runtime 线程 5 秒完成，超时则 detach 并记录错误
                let (tx, rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let _ = tx.send(handle.join());
                });
                match rx.recv_timeout(std::time::Duration::from_secs(5)) {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::error!("runtime thread panicked during drop: {:?}", e);
                    }
                    Err(_) => {
                        tracing::error!(
                            "runtime thread did not finish within 5s timeout, detaching"
                        );
                    }
                }
            }
        }
        self.event_bridge.clear_callback();
        {
            let mut guard = self.ctx.lock().unwrap_or_else(|e| e.into_inner());
            guard.take();
        }
        tracing::debug!("Drop: _RuntimeCore resources released");
    }
}

// ---------------------------------------------------------------------------
// 任务分发辅助
// ---------------------------------------------------------------------------

/// 任务分发：远程任务通过网络发送，本地任务直接返回。
async fn dispatch_tasks(
    worker: &Arc<WorkerRuntime>,
    tasks: Vec<crate::common::TaskDefinition>,
) -> Vec<crate::common::TaskDefinition> {
    let network = worker.network().clone();
    let local_node_id = worker.node_id().clone();
    let local_endpoint_addr = network.local_peer_id().to_string();
    let mut local_tasks = Vec::new();

    for mut def in tasks {
        if let Some(ref target) = def.target_node {
            if target != &local_node_id {
                if def.origin_endpoint_addr.is_none() {
                    def.origin_endpoint_addr = Some(local_endpoint_addr.clone());
                }
                let target_addr = def
                    .target_endpoint_addr
                    .as_deref()
                    .unwrap_or(target.as_str());
                let request =
                    crate::network::protocol::DirectRequest::DispatchTask { task: def.clone() };
                let dispatched = match network.send_direct_request(target_addr, request).await {
                    Ok(crate::network::protocol::DirectResponse::DispatchAck {
                        accepted: true,
                    }) => true,
                    Ok(crate::network::protocol::DirectResponse::DispatchAck {
                        accepted: false,
                    }) => false,
                    Ok(_) => false,
                    Err(_) => false,
                };
                if dispatched {
                    continue;
                }
                def.target_node = None;
                def.target_endpoint_addr = None;
                local_tasks.push(def);
                continue;
            }
        }
        if def.origin_endpoint_addr.is_none() {
            def.origin_endpoint_addr = Some(local_endpoint_addr.clone());
        }
        local_tasks.push(def);
    }

    local_tasks
}

// ---------------------------------------------------------------------------
// PyRuntimeCore::start_inner — runtime 线程初始化
// ---------------------------------------------------------------------------

/// [`spawn_event_dispatch`] 的参数集合。
///
/// 将 10 个参数聚合为单个结构体，避免 clippy `too_many_arguments` 警告，
/// 同时使调用点更清晰。
struct EventDispatchArgs {
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    dispatch_rx: tokio::sync::mpsc::Receiver<BusEvent>,
    claim_rx: tokio::sync::mpsc::Receiver<BusEvent>,
    dag_rx: tokio::sync::mpsc::Receiver<BusEvent>,
    direct_rx: tokio::sync::mpsc::Receiver<BusEvent>,
    failover: Arc<crate::orchestrator::FailoverManager>,
    gossip: DagGossip,
    orchestrator: Arc<crate::orchestrator::Orchestrator>,
    network: Arc<dyn Transport>,
    event_bus: EventBus,
    actor_system: Arc<ActorSystem>,
}

/// 派生事件分发后台任务，将 bus 事件扇出到
/// failover manager、gossip 协议、orchestrator 和网络回复通道。
/// 从 `start_inner` 抽取，使 runtime 线程闭包聚焦于子系统构造。
async fn spawn_event_dispatch(args: EventDispatchArgs) {
    let EventDispatchArgs {
        mut shutdown_rx,
        mut dispatch_rx,
        mut claim_rx,
        mut dag_rx,
        mut direct_rx,
        failover,
        gossip,
        orchestrator,
        network,
        event_bus,
        actor_system,
    } = args;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    tracing::debug!("event dispatch loop received shutdown signal");
                    break;
                }
                Some(event) = dispatch_rx.recv() => {
                    if let BusEvent::Heartbeat(hb) = &event {
                        failover.handle_heartbeat(hb);
                    }
                }
                Some(event) = claim_rx.recv() => {
                    if let BusEvent::Claim(claim) = &event {
                        failover.handle_claim(claim).await;
                    }
                }
                Some(event) = dag_rx.recv() => {
                    if let BusEvent::DagUpdate(update) = &event {
                        if let Err(e) = gossip.apply_remote_update(update.clone()).await {
                            tracing::warn!("failed to apply remote dag state update: {}", e);
                        }
                    }
                }
                Some(event) = direct_rx.recv() => {
                    if let BusEvent::DirectRequest { request, channel, peer_id: _ } = event {
                        match *request {
                            crate::network::protocol::DirectRequest::TaskResult {
                                workflow_id, task_id, task_name, outcome, worker_node: _
                            } => {
                                let accepted = orchestrator.has_workflow(&workflow_id).await;
                                if accepted {
                                    match &outcome {
                                        crate::common::WireTaskOutcome::Completed(result) => {
                                            let completion = TaskCompletion::Completed {
                                                workflow_id: workflow_id.clone(),
                                                task_id: task_id.clone(),
                                                task_name: task_name.clone(),
                                                result: result.clone(),
                                                target_node: None,
                                            };
                                            event_bus.publish(BusEvent::TaskCompleted(completion)).await;
                                        }
                                        crate::common::WireTaskOutcome::Failed(error) => {
                                            let completion = TaskCompletion::Failed {
                                                workflow_id: workflow_id.clone(),
                                                task_id: task_id.clone(),
                                                task_name: task_name.clone(),
                                                error: error.clone(),
                                                target_node: None,
                                            };
                                            event_bus.publish(BusEvent::TaskFailed(completion)).await;
                                        }
                                        crate::common::WireTaskOutcome::Cancelled => {
                                            let completion = TaskCompletion::Cancelled {
                                                workflow_id: workflow_id.clone(),
                                                task_id: task_id.clone(),
                                                task_name: task_name.clone(),
                                                target_node: None,
                                            };
                                            event_bus.publish(BusEvent::TaskCancelled(completion)).await;
                                        }
                                        crate::common::WireTaskOutcome::Skipped => {
                                            let completion = TaskCompletion::Skipped {
                                                workflow_id: workflow_id.clone(),
                                                task_id: task_id.clone(),
                                                task_name: task_name.clone(),
                                                target_node: None,
                                            };
                                            event_bus.publish(BusEvent::TaskSkipped(completion)).await;
                                        }
                                    }
                                }
                                let response = crate::network::protocol::DirectResponse::TaskResultAck { accepted };
                                if let Err(e) = network.send_direct_response(channel, response).await {
                                    tracing::warn!("failed to send TaskResultAck: {}", e);
                                }
                            }
                            crate::network::protocol::DirectRequest::DispatchTask { .. } => {
                                let response = crate::network::protocol::DirectResponse::DispatchAck { accepted: false };
                                if let Err(e) = network.send_direct_response(channel, response).await {
                                    tracing::warn!("failed to send DispatchAck(rejected): {}", e);
                                }
                            }
                            crate::network::protocol::DirectRequest::QueryWorkflowState { workflow_id, .. } => {
                                let (dag, execution, pending) =
                                    orchestrator.get_workflow_state_bytes(&workflow_id).await
                                    .map(|(d, e, p)| (Some(d), Some(e), Some(p)))
                                    .unwrap_or((None, None, None));
                                let response = crate::network::protocol::DirectResponse::WorkflowState {
                                    dag, execution, pending,
                                };
                                if let Err(e) = network.send_direct_response(channel, response).await {
                                    tracing::warn!("failed to send WorkflowState response: {}", e);
                                }
                            }
                            crate::network::protocol::DirectRequest::ActorCall { target, method, payload, reply_to: _ } => {
                                let result_bytes = match actor_system.call(&target, method.clone(), payload.clone()).await {
                                    Ok(msg_result) => {
                                        postcard::to_allocvec(&msg_result)
                                            .unwrap_or_default()
                                    }
                                    Err(e) => {
                                        tracing::warn!("ActorCall for {}:{} failed: {}", target.as_str(), method, e);
                                        Vec::new()
                                    }
                                };
                                let response = crate::network::protocol::DirectResponse::ActorCallResult {
                                    result: result_bytes,
                                };
                                if let Err(e) = network.send_direct_response(channel, response).await {
                                    tracing::warn!("failed to send ActorCallResult response: {}", e);
                                }
                            }
                        }
                    }
                }
                else => break,
            }
        }
    });
}

/// 派生 task_started 后台任务，将任务标记为运行中
/// 并通过 gossip 广播状态变更。
fn spawn_task_started(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut task_started_rx: tokio::sync::mpsc::Receiver<BusEvent>,
    orchestrator: Arc<crate::orchestrator::Orchestrator>,
    gossip: DagGossip,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    tracing::debug!("task_started loop received shutdown signal");
                    break;
                }
                msg = task_started_rx.recv() => {
                    match msg {
                        Some(BusEvent::TaskStarted { workflow_id, task_id }) => {
                            if orchestrator.has_workflow(&workflow_id).await {
                                if let Err(e) = orchestrator.mark_task_running(&workflow_id, &task_id).await {
                                    tracing::warn!("mark_task_running failed for {}/{}: {}", workflow_id.as_str(), task_id.as_str(), e);
                                }
                                if let Err(e) = gossip.broadcast_task_running(&workflow_id, &task_id).await {
                                    tracing::warn!("broadcast_task_running failed for {}/{}: {}", workflow_id.as_str(), task_id.as_str(), e);
                                }
                            }
                        }
                        Some(_) => {}
                        None => break,
                    }
                }
            }
        }
    });
}

impl PyRuntimeCore {
    fn start_inner(
        py: Python<'_>,
        name: String,
        node_id: Option<String>,
        config: PyActantConfig,
        tasks: HashMap<String, Py<PyAny>>,
    ) -> PyResult<Self> {
        // 在创建任何子系统之前初始化可观测性（tracing subscriber / pprof /
        // tokio-console）。由环境变量驱动，默认不安装 subscriber，不影响
        // pyo3-log 桥接。幂等 — 多次调用安全。
        crate::observability::init();

        let node_id =
            NodeId::from(node_id.unwrap_or_else(|| format!("{}-{}", name, uuid::Uuid::new_v4())));
        let actant_config = crate::common::ActantConfig::try_from(&config)?;
        if config.payload_signing_key.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "payload_signing_key is required for payload MAC signing",
            ));
        }
        // 在构造任何子系统之前，针对实时注册表验证策略名称字段。
        // 未知的发现模式或调度器类型会在此处快速失败并返回 Config 错误 —— 静默回退。
        actant_config.validate()?;
        let scheduler = crate::orchestrator::scheduler::create_scheduler(
            actant_config.worker.scheduler_kind.as_str(),
        )?;

        let default_task_timeout_ms = config.default_task_timeout_ms;
        let payload_signing_key = config.payload_signing_key.as_bytes().to_vec();
        let network_config = NetworkConfig::try_from(&config.network)?;
        let failover_cfg = crate::common::FailoverConfig::from(config.failover.clone());
        let data_dir = config.data_dir.clone();
        // 在配置入口边界校验 data_dir（foot-gun 防护：拒绝空字符串与系统关键目录）。
        if let Some(ref dir) = data_dir {
            crate::py::bootstrap::validate_data_dir(dir)?;
        }

        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            pyo3::PyErr::from(crate::common::ActantError::Internal(format!(
                "failed to create tokio runtime: {}",
                e
            )))
        })?;

        let tokio_handle = rt.handle().clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let tasks_clone: HashMap<String, Py<PyAny>> = tasks
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect();

        let sched_for_rt = scheduler.clone();
        let node_id_for_rt = node_id.clone();
        let actant_config_for_rt = actant_config.clone();
        let network_config_for_rt = network_config;
        let data_dir_for_rt = data_dir.clone();
        let failover_cfg_for_rt = failover_cfg;

        let gil_thread = GilThread::spawn();
        let gil_thread_for_rt = gil_thread.clone();

        let (ready_tx, ready_rx) = crossbeam_channel::bounded::<
            Result<
                (
                    Arc<WorkerRuntime>,
                    Arc<ActorSystem>,
                    Arc<dyn Transport>,
                    Arc<FailoverManager>,
                    Arc<Orchestrator>,
                    DagGossip,
                    Arc<PyEventBridge>,
                    GilThread,
                    tokio::sync::watch::Sender<bool>,
                    tokio::sync::watch::Sender<bool>,
                    tokio::sync::watch::Sender<bool>,
                ),
                String,
            >,
        >(1);

        // 初始化 OpenTelemetry 指标管道，确保在调用 prometheus_text() 之前就绪。
        crate::metrics::init()?;

        let runtime_thread = std::thread::spawn(move || {
            let gw = gil_thread_for_rt.clone();
            let _result: Result<(), PyErr> = rt.block_on(async move {
                tracing::debug!("runtime thread: starting network init");
                let network: Arc<dyn Transport> =
                    build_transport(node_id_for_rt.clone(), network_config_for_rt)
                        .await
                        .map_err(|e| {
                            crate::common::ActantError::Network(format!(
                                "network init failed: {}",
                                e
                            ))
                        })?;
                tracing::debug!("runtime thread: network init done");

                let event_bridge = Arc::new(PyEventBridge::new());
                let event_bus = EventBus::new().with_on_publish(event_bridge.make_on_publish(&gw));

                tracing::debug!("runtime thread: starting actor system init");
                let actor_system = super::bootstrap::init_actor_system(
                    data_dir_for_rt.as_deref(),
                    &node_id_for_rt,
                    &network,
                    &event_bus,
                )
                .map_err(|e: PyErr| {
                    let msg = format!("actor system init failed: {}", e);
                    let _ = ready_tx.send(Err(msg));
                    e
                })?;
                tracing::debug!("runtime thread: actor system init done");

                tracing::debug!("runtime thread: starting orchestrator init");
                let orchestrator = super::bootstrap::init_orchestrator(
                    data_dir_for_rt.as_deref(),
                    &node_id_for_rt,
                    &actant_config_for_rt,
                )
                .await
                .map_err(|e: PyErr| {
                    let msg = format!("orchestrator init failed: {}", e);
                    let _ = ready_tx.send(Err(msg));
                    e
                })?;
                tracing::debug!("runtime thread: orchestrator init done");

                let mut failover = FailoverManager::with_config(
                    node_id_for_rt.clone(),
                    network.clone(),
                    orchestrator.clone(),
                    failover_cfg_for_rt,
                    None,
                )
                .await;
                failover.set_scheduler(sched_for_rt.clone());
                let failover = Arc::new(failover);

                let gossip = DagGossip::new(
                    network.clone(),
                    orchestrator.clone(),
                    actant_config.gossip.clone(),
                );

                let dispatch_rx = event_bus.subscribe(Topic::ClusterHeartbeat);
                let claim_rx = event_bus.subscribe(Topic::ClusterClaim);
                let dag_rx = event_bus.subscribe(Topic::DagUpdate);
                let direct_rx = event_bus.subscribe(Topic::NetworkDirect);
                let task_started_rx = event_bus.subscribe(Topic::TaskStarted);

                // 克隆 shutdown watcher，使后台任务在收到 shutdown 信号时能够优雅退出，
                // 而不是依赖 rt.shutdown_timeout 在 5 秒后强制取消它们。
                let shutdown_rx_for_dispatch = shutdown_rx.clone();
                let shutdown_rx_for_task_started = shutdown_rx.clone();

                spawn_event_dispatch(EventDispatchArgs {
                    shutdown_rx: shutdown_rx_for_dispatch,
                    dispatch_rx,
                    claim_rx,
                    dag_rx,
                    direct_rx,
                    failover: failover.clone(),
                    gossip: gossip.clone(),
                    orchestrator: orchestrator.clone(),
                    network: network.clone(),
                    event_bus: event_bus.clone(),
                    actor_system: actor_system.clone(),
                })
                .await;

                let task_registry = TaskRegistry::new(
                    actant_config_for_rt.worker.task_thread_pool_workers,
                    actant_config_for_rt
                        .worker
                        .task_thread_pool_channel_capacity,
                    actant_config_for_rt.payload_signing_key.clone(),
                )?;
                super::bootstrap::register_python_tasks(&task_registry, tasks_clone);
                let task_dispatcher = task_registry.into_dispatcher();

                let runtime = super::bootstrap::init_worker(super::bootstrap::WorkerInitParams {
                    node_id: &node_id_for_rt,
                    network: &network,
                    event_bus,
                    scheduler: &sched_for_rt,
                    worker_config: &actant_config_for_rt.worker,
                    actor_system: actor_system.clone(),
                    task_dispatcher,
                    failover: &failover,
                    tokio_handle: tokio::runtime::Handle::current(),
                });

                if data_dir_for_rt.is_some() {
                    actor_system.start_compaction_task();
                }

                network.subscribe(TOPIC_DAG_STATE).await.map_err(|e| {
                    crate::common::ActantError::Network(format!(
                        "subscribe dag-state failed: {}",
                        e
                    ))
                })?;
                failover.subscribe_topics().await.map_err(|e| {
                    crate::common::ActantError::Network(format!(
                        "subscribe failover topics failed: {}",
                        e
                    ))
                })?;

                let failover_cancel_tx = failover.start_background_loops();
                let timeout_cancel_tx = orchestrator.start_timeout_watcher();
                let persist_flush_cancel_tx = orchestrator.start_persist_flush();

                spawn_task_started(
                    shutdown_rx_for_task_started,
                    task_started_rx,
                    orchestrator.clone(),
                    gossip.clone(),
                );

                let runtime_arc = Arc::new(runtime);
                let runtime_clone = runtime_arc.clone();

                if let Err(e) = ready_tx.send(Ok((
                    runtime_arc,
                    actor_system.clone(),
                    network.clone(),
                    failover.clone(),
                    orchestrator.clone(),
                    gossip.clone(),
                    event_bridge,
                    gw,
                    failover_cancel_tx,
                    timeout_cancel_tx,
                    persist_flush_cancel_tx,
                ))) {
                    tracing::error!("ready tx send failed: {}", e);
                }

                let recovered = orchestrator.recover_ready_tasks();
                if !recovered.is_empty() {
                    tracing::info!("enqueuing {} recovered ready tasks", recovered.len());
                    for task in recovered {
                        if let Err(e) = sched_for_rt.enqueue(task).await {
                            tracing::warn!("scheduler rejected recovered task: {}", e);
                        }
                    }
                }

                let runtime_for_run = runtime_clone;
                let _bg_worker_run = tokio::spawn(async move {
                    if let Err(e) = runtime_for_run.run().await {
                        tracing::error!("runtime run failed: {}", e);
                    }
                });

                tracing::debug!("runtime thread: entering shutdown wait");
                if let Err(e) = shutdown_rx.changed().await {
                    tracing::error!("shutdown rx changed failed: {}", e);
                }
                tracing::debug!("runtime thread: shutdown signal received");
                // 在释放子系统之前优雅地关闭网络。
                // 这会关闭 iroh 的 QUIC 端点并取消其后台任务，
                // 这样 rt.shutdown_timeout 就不需要等待完整的 5 秒超时来强制取消它们。
                if let Err(e) = network.shutdown().await {
                    tracing::warn!("network shutdown failed: {}", e);
                }
                tracing::debug!("runtime thread: network shutdown done");
                // Drop 子系统，使其后台任务干净退出。
                drop(gossip);
                drop(failover);
                drop(network);
                drop(orchestrator);
                drop(actor_system);
                tracing::debug!("runtime thread: subsystems dropped");
                Ok::<_, PyErr>(())
            });
            rt.shutdown_timeout(std::time::Duration::from_secs(5));
            tracing::debug!("runtime thread: rt.shutdown_timeout completed");
        });

        // 关键：在等待运行时线程发出就绪信号时释放 GIL。
        // 运行时线程使用 `tracing::debug!`，它会通过 `pyo3_log` → Python `logging`，
        // 这需要 GIL。如果主线程在 `recv_timeout` 期间持有 GIL，就会发生死锁：
        // 主线程等待就绪信号（由运行时线程发送），而运行时线程等待 GIL（由主线程持有）。
        // `py.detach` 在等待期间释放 GIL，打破这个循环。
        let inner = py.detach(|| {
            ready_rx
                .recv_timeout(std::time::Duration::from_secs(15))
                .map_err(|e| match e {
                    crossbeam_channel::RecvTimeoutError::Timeout => {
                        pyo3::PyErr::from(crate::common::ActantError::Internal(
                            "background runtime thread timed out after 15s".into(),
                        ))
                    }
                    crossbeam_channel::RecvTimeoutError::Disconnected => {
                        pyo3::PyErr::from(crate::common::ActantError::Internal(
                            "background runtime thread died before ready".into(),
                        ))
                    }
                })
        })?;
        let (
            worker,
            actor_system,
            network,
            failover,
            orchestrator,
            gossip,
            event_bridge,
            gil_thread,
            failover_cancel_tx,
            timeout_cancel_tx,
            persist_flush_cancel_tx,
        ) = inner.map_err(|e| pyo3::PyErr::from(crate::common::ActantError::Internal(e)))?;

        Ok(Self {
            ctx: std::sync::Mutex::new(Some(RuntimeContext {
                orchestrator,
                actor_system,
                gossip,
                failover,
                worker,
                scheduler,
                network,
                default_task_timeout_ms,
                payload_signing_key,
            })),
            event_bridge,
            gil_thread,
            tokio_handle,
            shutdown_tx,
            failover_cancel_tx: Some(failover_cancel_tx),
            timeout_cancel_tx: Some(timeout_cancel_tx),
            persist_flush_cancel_tx: Some(persist_flush_cancel_tx),
            runtime_thread: std::sync::Mutex::new(Some(runtime_thread)),
            actor_dispatchers: Arc::new(dashmap::DashMap::new()),
        })
    }
}

// ---------------------------------------------------------------------------
// PyAsyncResultCore — awaitable workflow result
// ---------------------------------------------------------------------------

#[pyclass(name = "_AsyncResultCore", skip_from_py_object)]
pub struct PyAsyncResultCore {
    #[pyo3(get)]
    pub workflow_id: String,
    state: Arc<crate::orchestrator::OrchestratorState>,
    tokio_handle: tokio::runtime::Handle,
    gil_thread: GilThread,
    default_task_timeout_ms: u64,
}

impl PyAsyncResultCore {
    pub(crate) fn new(
        workflow_id: String,
        state: Arc<crate::orchestrator::OrchestratorState>,
        tokio_handle: tokio::runtime::Handle,
        gil_thread: GilThread,
        default_task_timeout_ms: u64,
    ) -> Self {
        Self {
            workflow_id,
            state,
            tokio_handle,
            gil_thread,
            default_task_timeout_ms,
        }
    }

    fn ready_inner(&self) -> Result<bool, String> {
        let wf_id = WorkflowId::from(self.workflow_id.clone());
        match self.state.get_slot(&wf_id) {
            Some(slot) => Ok(slot.execution.is_terminal()),
            None => Err(format!("workflow {} not found", wf_id.as_str())),
        }
    }

    fn state_inner(&self) -> Result<String, String> {
        let wf_id = WorkflowId::from(self.workflow_id.clone());
        match self.state.get_slot(&wf_id) {
            Some(slot) => Ok(slot.execution.state.as_str().to_string()),
            None => Ok("unknown".to_string()),
        }
    }
}

#[pymethods]
impl PyAsyncResultCore {
    fn ready(&self, py: Python<'_>) -> PyResult<bool> {
        let result = py.detach(|| self.ready_inner());
        result.map_err(|e| pyo3::PyErr::from(crate::common::ActantError::Internal(e)))
    }

    fn state(&self, py: Python<'_>) -> PyResult<String> {
        let result = py.detach(|| self.state_inner());
        result.map_err(|e| pyo3::PyErr::from(crate::common::ActantError::Internal(e)))
    }

    #[pyo3(signature = (timeout_ms = None))]
    fn get(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<Py<PyAny>> {
        let timeout = timeout_ms.unwrap_or(self.default_task_timeout_ms);
        let state = self.state.clone();
        let wf_id = WorkflowId::from(self.workflow_id.clone());

        future_into_py_iter(
            py,
            self.tokio_handle.clone(),
            &self.gil_thread,
            async move {
                let execution =
                    tokio::time::timeout(tokio::time::Duration::from_millis(timeout), async {
                        // 快速路径：已是终态。
                        if let Some(slot) = state.get_slot(&wf_id) {
                            if slot.execution.is_terminal() {
                                tracing::debug!(
                                    wf = ?wf_id,
                                    "result.get: fast path (already terminal)"
                                );
                                return Some(slot.execution.clone());
                            }
                        }
                        // register_terminal_waiter 是无竞态的：它在检查终止状态之前插入等待者，
                        // 因此 oneshot 一定会触发（要么由 fire_terminal_oneshot 触发，要么如果已经终止则立即触发）。
                        // 不需要轮询回退机制。
                        let rx = state.register_terminal_waiter(wf_id.clone());
                        let _ = rx.await;
                        if let Some(slot) = state.get_slot(&wf_id) {
                            if slot.execution.is_terminal() {
                                return Some(slot.execution.clone());
                            }
                            // Slot 存在但未终止 —— 仅当工作流在触发和我们的重新检查之间被逐出时才会发生。
                            return None;
                        }
                        // 工作流已被逐出（没有槽位），将其视为未找到。
                        None
                    })
                    .await;

                match execution {
                    Err(_) => {
                        tracing::warn!(wf = ?wf_id, timeout_ms = timeout, "result.get: timed out");
                        Python::attach(|py| {
                            let dict = pyo3::types::PyDict::new(py);
                            dict.set_item("state", "Timeout")?;
                            dict.set_item("error", format!("timed out after {}ms", timeout))?;
                            Ok::<_, pyo3::PyErr>(FutureResultToPy::Value(dict.into_any().unbind()))
                        })
                        .unwrap_or_else(FutureResultToPy::Err)
                    }
                    Ok(None) => Python::attach(|_py| {
                        FutureResultToPy::Err(pyo3::PyErr::from(
                            crate::common::ActantError::NotFound("workflow not found".into()),
                        ))
                    }),
                    Ok(Some(execution)) => {
                        tracing::debug!(
                            wf = ?wf_id,
                            state = execution.state.as_str(),
                            "result.get: terminal"
                        );
                        let sink_task_ids: Vec<String> = {
                            if let Some(slot) = state.get_slot(&wf_id) {
                                slot.dag
                                    .sinks()
                                    .iter()
                                    .map(|s| s.task_id.to_string())
                                    .collect()
                            } else {
                                Vec::new()
                            }
                        };

                        let all_task_results: Vec<(String, String, Vec<u8>)> = {
                            if let Some(slot) = state.get_slot(&wf_id) {
                                slot.dag
                                    .nodes()
                                    .filter_map(|node| {
                                        let task_state = execution.tasks.get(&node.task_id)?;
                                        let result_bytes = task_state.result.as_ref()?;
                                        Some((
                                            node.task_id.to_string(),
                                            node.name.clone(),
                                            result_bytes.clone(),
                                        ))
                                    })
                                    .collect()
                            } else {
                                Vec::new()
                            }
                        };

                        Python::attach(|py| {
                            let dict = pyo3::types::PyDict::new(py);

                            match &execution.state {
                                Phase::Completed => {
                                    let mut results_list: Vec<(String, Vec<u8>)> = Vec::new();
                                    for tid in &sink_task_ids {
                                        let task_id = TaskId::from(tid.clone());
                                        if let Some(task_state) = execution.tasks.get(&task_id) {
                                            if let Some(ref result_bytes) = task_state.result {
                                                results_list
                                                    .push((tid.clone(), result_bytes.clone()));
                                            }
                                        }
                                    }
                                    dict.set_item("state", Phase::Completed.as_str())?;
                                    dict.set_item("results", results_list)?;
                                    dict.set_item("all_results", all_task_results)?;
                                }
                                Phase::Failed => {
                                    dict.set_item("state", Phase::Failed.as_str())?;
                                    dict.set_item(
                                        "error",
                                        execution.error.clone().unwrap_or_default(),
                                    )?;
                                    // 收集失败任务的结构化信息：
                                    let task_name_map: std::collections::HashMap<String, String> =
                                        if let Some(slot) = state.get_slot(&wf_id) {
                                            slot.dag
                                                .nodes()
                                                .map(|n| (n.task_id.to_string(), n.name.clone()))
                                                .collect()
                                        } else {
                                            std::collections::HashMap::new()
                                        };
                                    let failed_tasks: Vec<Vec<String>> = execution
                                        .tasks
                                        .iter()
                                        .filter_map(|(tid, ts)| {
                                            if ts.state == Phase::Failed {
                                                Some(vec![
                                                    tid.to_string(),
                                                    task_name_map
                                                        .get(tid.as_str())
                                                        .cloned()
                                                        .unwrap_or_default(),
                                                    ts.error.clone().unwrap_or_default(),
                                                ])
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();
                                    dict.set_item("failed_tasks", failed_tasks)?;
                                }
                                Phase::Cancelled => {
                                    dict.set_item("state", Phase::Cancelled.as_str())?;
                                    dict.set_item("error", "workflow was cancelled")?;
                                }
                                other => {
                                    dict.set_item("state", other.as_str())?;
                                    dict.set_item(
                                        "error",
                                        "workflow did not reach terminal state",
                                    )?;
                                }
                            }

                            Ok::<_, pyo3::PyErr>(FutureResultToPy::Value(dict.into_any().unbind()))
                        })
                        .unwrap_or_else(FutureResultToPy::Err)
                    }
                }
            },
        )
        .map(|b| b.unbind())
    }

    #[pyo3(signature = (timeout_ms = None))]
    fn wait_for_completion(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<Py<PyAny>> {
        self.get(py, timeout_ms)
    }
}

/// 在 Python 模块上注册所有 runtime 相关类。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRuntimeCore>()?;
    m.add_class::<PyAsyncResultCore>()?;
    m.add_class::<PyRetryInfo>()?;
    m.add_class::<PyNode>()?;
    m.add_class::<PyTask>()?;
    m.add_class::<PyPeerCapacity>()?;
    Ok(())
}
