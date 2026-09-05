use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::common::{
    ActantConfig, DiscoveryMode, FailoverConfig, GossipConfig, NetworkConfig, RetryPolicy,
};
use crate::runtime::workflow::Phase;

#[pyclass(name = "_WorkflowState", from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyWorkflowState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Skipped,
}

#[pymethods]
impl PyWorkflowState {
    #[classattr]
    const PENDING: PyWorkflowState = PyWorkflowState::Pending;
    #[classattr]
    const RUNNING: PyWorkflowState = PyWorkflowState::Running;
    #[classattr]
    const COMPLETED: PyWorkflowState = PyWorkflowState::Completed;
    #[classattr]
    const FAILED: PyWorkflowState = PyWorkflowState::Failed;
    #[classattr]
    const CANCELLED: PyWorkflowState = PyWorkflowState::Cancelled;
    #[classattr]
    const SKIPPED: PyWorkflowState = PyWorkflowState::Skipped;

    fn __repr__(&self) -> &'static str {
        match self {
            PyWorkflowState::Pending => "_WorkflowState.PENDING",
            PyWorkflowState::Running => "_WorkflowState.RUNNING",
            PyWorkflowState::Completed => "_WorkflowState.COMPLETED",
            PyWorkflowState::Failed => "_WorkflowState.FAILED",
            PyWorkflowState::Cancelled => "_WorkflowState.CANCELLED",
            PyWorkflowState::Skipped => "_WorkflowState.SKIPPED",
        }
    }
}

impl From<Phase> for PyWorkflowState {
    fn from(s: Phase) -> Self {
        match s {
            Phase::Pending => PyWorkflowState::Pending,
            Phase::Running => PyWorkflowState::Running,
            Phase::Completed => PyWorkflowState::Completed,
            Phase::Failed => PyWorkflowState::Failed,
            Phase::Cancelled => PyWorkflowState::Cancelled,
            Phase::Skipped => PyWorkflowState::Skipped,
        }
    }
}

impl From<PyWorkflowState> for Phase {
    fn from(s: PyWorkflowState) -> Self {
        match s {
            PyWorkflowState::Pending => Phase::Pending,
            PyWorkflowState::Running => Phase::Running,
            PyWorkflowState::Completed => Phase::Completed,
            PyWorkflowState::Failed => Phase::Failed,
            PyWorkflowState::Cancelled => Phase::Cancelled,
            PyWorkflowState::Skipped => Phase::Skipped,
        }
    }
}

/// 将用户提供的网络 preset 字符串解析为内部 `DiscoveryMode`。
///
/// 此处接受任何非空字符串；discovery 注册表在启动时通过
/// [`crate::common::DiscoveryMode::validate`] 验证名称，
/// 并以 `Config` 错误拒绝未知名称（无静默回退）。
/// 这允许 Python 层在 runtime 启动前注册自定义发现策略。
///
/// # 环境变量覆盖
///
/// `ACTANT_DISCOVERY` 环境变量设置时优先于配置的 preset。
/// 用于无互联网访问、需避开 iroh 公共 relay（N0 preset）的
/// 测试/CI 环境 — 设置 `ACTANT_DISCOVERY=none` 强制离线
/// `Minimal` preset，使 runtime 无需联系任何外部服务即可立即启动。
/// 该值与任何 preset 一样通过 discovery 注册表验证。
fn discovery_mode_from_preset(preset: &str) -> PyResult<DiscoveryMode> {
    if let Ok(env_override) = std::env::var("ACTANT_DISCOVERY") {
        if !env_override.is_empty() {
            return Ok(DiscoveryMode::new_unchecked(env_override));
        }
    }
    if preset.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "network preset must not be empty",
        ));
    }
    Ok(DiscoveryMode::new_unchecked(preset))
}

#[pyclass(name = "_RetryPolicy", from_py_object)]
#[derive(Clone)]
pub struct PyRetryPolicy {
    #[pyo3(get)]
    pub max_retries: u32,
    #[pyo3(get)]
    pub delay_ms: u64,
    #[pyo3(get)]
    pub backoff_multiplier: f64,
    #[pyo3(get)]
    pub max_delay_ms: u64,
}

#[pymethods]
impl PyRetryPolicy {
    #[new]
    #[pyo3(signature = (max_retries=RetryPolicy::DEFAULT_MAX_RETRIES, delay_ms=RetryPolicy::DEFAULT_DELAY_MS, backoff_multiplier=RetryPolicy::DEFAULT_BACKOFF_MULTIPLIER, max_delay_ms=RetryPolicy::DEFAULT_MAX_DELAY_MS))]
    fn new(max_retries: u32, delay_ms: u64, backoff_multiplier: f64, max_delay_ms: u64) -> Self {
        Self {
            max_retries,
            delay_ms,
            backoff_multiplier,
            max_delay_ms,
        }
    }

    fn to_bytes(&self) -> PyResult<Vec<u8>> {
        let policy = RetryPolicy::from(self.clone());
        postcard::to_allocvec(&policy).map_err(|e| {
            pyo3::PyErr::from(crate::common::ActantError::Serialization(format!(
                "RetryPolicy serialization failed: {}",
                e
            )))
        })
    }

    #[classmethod]
    fn default(_: &Bound<'_, PyType>) -> PyRetryPolicy {
        let policy = RetryPolicy::default();
        PyRetryPolicy::from(policy)
    }
}

impl From<PyRetryPolicy> for RetryPolicy {
    fn from(p: PyRetryPolicy) -> Self {
        Self {
            max_retries: p.max_retries,
            delay_ms: p.delay_ms,
            backoff_multiplier: p.backoff_multiplier,
            max_delay_ms: p.max_delay_ms,
        }
    }
}

impl From<RetryPolicy> for PyRetryPolicy {
    fn from(p: RetryPolicy) -> Self {
        Self {
            max_retries: p.max_retries,
            delay_ms: p.delay_ms,
            backoff_multiplier: p.backoff_multiplier,
            max_delay_ms: p.max_delay_ms,
        }
    }
}

// Compile-time assertion: PyRetryPolicy defaults must match RetryPolicy::default()
const _: () = {
    assert!(RetryPolicy::DEFAULT_MAX_RETRIES == 3);
    assert!(RetryPolicy::DEFAULT_DELAY_MS == 1000);
    assert!(RetryPolicy::DEFAULT_BACKOFF_MULTIPLIER == 2.0);
    assert!(RetryPolicy::DEFAULT_MAX_DELAY_MS == 60000);
};

#[pyclass(name = "_NetworkConfig", from_py_object)]
#[derive(Clone)]
pub struct PyNetworkConfig {
    #[pyo3(get)]
    pub preset: String,
    #[pyo3(get)]
    pub bootstrap_nodes: Vec<String>,
    #[pyo3(get)]
    pub hlc_max_drift_ms: u64,
    #[pyo3(get)]
    pub max_pending_direct_requests: usize,
    #[pyo3(get)]
    pub gossip_bootstrap_peers: Vec<String>,
    #[pyo3(get)]
    pub max_message_size: usize,
    #[pyo3(get)]
    pub allowed_peer_ids: Vec<String>,
    #[pyo3(get)]
    pub direct_request_timeout_ms: u64,
    #[pyo3(get)]
    pub listen_port: u16,
    #[pyo3(get)]
    pub listen_ip: String,
    #[pyo3(get)]
    pub capability_gossip_interval_ms: u64,
    #[pyo3(get)]
    pub event_channel_capacity: usize,
    /// 自定义 DNS 起源域，仅当 `preset = "dns"` 时生效。
    /// 空字符串表示使用 n0 默认 `iroh.link`。
    #[pyo3(get)]
    pub dns_origin_domain: String,
}

#[pymethods]
impl PyNetworkConfig {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (preset=None, bootstrap_nodes=None, hlc_max_drift_ms=crate::common::NetworkConfig::DEFAULT_HLC_MAX_DRIFT_MS, max_pending_direct_requests=crate::common::NetworkConfig::DEFAULT_MAX_PENDING_DIRECT_REQUESTS, gossip_bootstrap_peers=None, max_message_size=crate::common::NetworkConfig::DEFAULT_MAX_MESSAGE_SIZE, allowed_peer_ids=None, direct_request_timeout_ms=crate::common::NetworkConfig::DEFAULT_DIRECT_REQUEST_TIMEOUT_MS, listen_port=0, listen_ip="", capability_gossip_interval_ms=crate::common::NetworkConfig::DEFAULT_CAPABILITY_GOSSIP_INTERVAL_MS, event_channel_capacity=crate::common::NetworkConfig::DEFAULT_EVENT_CHANNEL_CAPACITY, dns_origin_domain=""))]
    fn new(
        preset: Option<String>,
        bootstrap_nodes: Option<Vec<String>>,
        hlc_max_drift_ms: u64,
        max_pending_direct_requests: usize,
        gossip_bootstrap_peers: Option<Vec<String>>,
        max_message_size: usize,
        allowed_peer_ids: Option<Vec<String>>,
        direct_request_timeout_ms: u64,
        listen_port: u16,
        listen_ip: &str,
        capability_gossip_interval_ms: u64,
        event_channel_capacity: usize,
        dns_origin_domain: &str,
    ) -> Self {
        Self {
            preset: preset.unwrap_or_else(|| "local".to_string()),
            bootstrap_nodes: bootstrap_nodes.unwrap_or_default(),
            hlc_max_drift_ms,
            max_pending_direct_requests,
            gossip_bootstrap_peers: gossip_bootstrap_peers.unwrap_or_default(),
            max_message_size,
            allowed_peer_ids: allowed_peer_ids.unwrap_or_default(),
            direct_request_timeout_ms,
            listen_port,
            listen_ip: listen_ip.to_string(),
            capability_gossip_interval_ms,
            event_channel_capacity,
            dns_origin_domain: dns_origin_domain.to_string(),
        }
    }
}

impl Default for PyNetworkConfig {
    fn default() -> Self {
        Self {
            preset: "local".to_string(),
            bootstrap_nodes: Vec::new(),
            hlc_max_drift_ms: crate::common::NetworkConfig::DEFAULT_HLC_MAX_DRIFT_MS,
            max_pending_direct_requests:
                crate::common::NetworkConfig::DEFAULT_MAX_PENDING_DIRECT_REQUESTS,
            gossip_bootstrap_peers: Vec::new(),
            max_message_size: crate::common::NetworkConfig::DEFAULT_MAX_MESSAGE_SIZE,
            allowed_peer_ids: Vec::new(),
            direct_request_timeout_ms:
                crate::common::NetworkConfig::DEFAULT_DIRECT_REQUEST_TIMEOUT_MS,
            listen_port: 0,
            listen_ip: String::new(),
            capability_gossip_interval_ms:
                crate::common::NetworkConfig::DEFAULT_CAPABILITY_GOSSIP_INTERVAL_MS,
            event_channel_capacity: crate::common::NetworkConfig::DEFAULT_EVENT_CHANNEL_CAPACITY,
            dns_origin_domain: String::new(),
        }
    }
}

impl TryFrom<&PyNetworkConfig> for NetworkConfig {
    type Error = PyErr;

    fn try_from(c: &PyNetworkConfig) -> PyResult<Self> {
        Ok(Self {
            discovery_mode: discovery_mode_from_preset(&c.preset)?,
            bootstrap_nodes: c.bootstrap_nodes.clone(),
            hlc_max_drift_ms: c.hlc_max_drift_ms,
            max_pending_direct_requests: c.max_pending_direct_requests,
            gossip_bootstrap_peers: c.gossip_bootstrap_peers.clone(),
            max_message_size: c.max_message_size,
            allowed_peer_ids: c.allowed_peer_ids.clone(),
            direct_request_timeout_ms: c.direct_request_timeout_ms,
            listen_port: c.listen_port,
            listen_ip: c.listen_ip.clone(),
            capability_gossip_interval_ms: c.capability_gossip_interval_ms,
            event_channel_capacity: c.event_channel_capacity,
            dns_origin_domain: c.dns_origin_domain.clone(),
        })
    }
}

#[pyclass(name = "_FailoverConfig", from_py_object)]
#[derive(Clone)]
pub struct PyFailoverConfig {
    #[pyo3(get)]
    pub heartbeat_interval_ms: u64,
    #[pyo3(get)]
    pub failure_timeout_ms: u64,
    #[pyo3(get)]
    pub lease_expiry_check_interval_secs: u64,
    #[pyo3(get)]
    pub lease_duration_ms: u64,
}

#[pymethods]
impl PyFailoverConfig {
    #[new]
    #[pyo3(signature = (heartbeat_interval_ms=None, failure_timeout_ms=None, lease_expiry_check_interval_secs=None, lease_duration_ms=None))]
    fn new(
        heartbeat_interval_ms: Option<u64>,
        failure_timeout_ms: Option<u64>,
        lease_expiry_check_interval_secs: Option<u64>,
        lease_duration_ms: Option<u64>,
    ) -> Self {
        let default = FailoverConfig::default();
        Self {
            heartbeat_interval_ms: heartbeat_interval_ms.unwrap_or(default.heartbeat_interval_ms),
            failure_timeout_ms: failure_timeout_ms.unwrap_or(default.failure_timeout_ms),
            lease_expiry_check_interval_secs: lease_expiry_check_interval_secs
                .unwrap_or(default.lease_expiry_check_interval_secs),
            lease_duration_ms: lease_duration_ms.unwrap_or(default.lease_duration_ms),
        }
    }
}

impl Default for PyFailoverConfig {
    fn default() -> Self {
        let default = FailoverConfig::default();
        Self {
            heartbeat_interval_ms: default.heartbeat_interval_ms,
            failure_timeout_ms: default.failure_timeout_ms,
            lease_expiry_check_interval_secs: default.lease_expiry_check_interval_secs,
            lease_duration_ms: default.lease_duration_ms,
        }
    }
}

impl From<PyFailoverConfig> for FailoverConfig {
    fn from(c: PyFailoverConfig) -> Self {
        Self {
            heartbeat_interval_ms: c.heartbeat_interval_ms,
            failure_timeout_ms: c.failure_timeout_ms,
            lease_expiry_check_interval_secs: c.lease_expiry_check_interval_secs,
            lease_duration_ms: c.lease_duration_ms,
        }
    }
}

#[pyclass(name = "_GossipConfig", from_py_object)]
#[derive(Clone)]
pub struct PyGossipConfig {
    #[pyo3(get)]
    pub dedup_window_size: usize,
    #[pyo3(get)]
    pub dedup_ttl_secs: u64,
    #[pyo3(get)]
    pub retry_attempts: usize,
    #[pyo3(get)]
    pub retry_base_delay_ms: u64,
    #[pyo3(get)]
    pub heads_broadcast_interval_ms: u64,
}

#[pymethods]
impl PyGossipConfig {
    /// 所有字段的默认值取自 Rust [`GossipConfig::default()`]（单一来源），
    /// 此处不重复维护数值，避免 PyO3 与 Rust 侧默认值漂移。
    #[new]
    #[pyo3(signature = (dedup_window_size=None, dedup_ttl_secs=None, retry_attempts=None, retry_base_delay_ms=None, heads_broadcast_interval_ms=None))]
    fn new(
        dedup_window_size: Option<usize>,
        dedup_ttl_secs: Option<u64>,
        retry_attempts: Option<usize>,
        retry_base_delay_ms: Option<u64>,
        heads_broadcast_interval_ms: Option<u64>,
    ) -> Self {
        let default = GossipConfig::default();
        Self {
            dedup_window_size: dedup_window_size.unwrap_or(default.dedup_window_size),
            dedup_ttl_secs: dedup_ttl_secs.unwrap_or(default.dedup_ttl_secs),
            retry_attempts: retry_attempts.unwrap_or(default.retry_attempts),
            retry_base_delay_ms: retry_base_delay_ms.unwrap_or(default.retry_base_delay_ms),
            heads_broadcast_interval_ms: heads_broadcast_interval_ms
                .unwrap_or(default.heads_broadcast_interval_ms),
        }
    }
}

impl Default for PyGossipConfig {
    fn default() -> Self {
        let default = GossipConfig::default();
        Self {
            dedup_window_size: default.dedup_window_size,
            dedup_ttl_secs: default.dedup_ttl_secs,
            retry_attempts: default.retry_attempts,
            retry_base_delay_ms: default.retry_base_delay_ms,
            heads_broadcast_interval_ms: default.heads_broadcast_interval_ms,
        }
    }
}

impl From<PyGossipConfig> for GossipConfig {
    fn from(c: PyGossipConfig) -> Self {
        Self {
            dedup_window_size: c.dedup_window_size,
            dedup_ttl_secs: c.dedup_ttl_secs,
            retry_attempts: c.retry_attempts,
            retry_base_delay_ms: c.retry_base_delay_ms,
            heads_broadcast_interval_ms: c.heads_broadcast_interval_ms,
        }
    }
}

/// 将用户提供的 scheduler kind 字符串解析为内部 `SchedulerKind`。
///
/// 此处接受任何非空字符串；scheduler 注册表在启动时通过
/// [`crate::common::SchedulerKind::validate`] 验证名称，
/// 并以 `Config` 错误拒绝未知名称（无静默回退）。
/// 这允许 Python 层在 runtime 启动前注册自定义调度策略。
fn scheduler_kind_from_str(kind: &str) -> PyResult<crate::common::SchedulerKind> {
    if kind.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "scheduler kind must not be empty",
        ));
    }
    Ok(crate::common::SchedulerKind::new_unchecked(kind))
}

#[pyclass(name = "_ActantConfig", from_py_object)]
#[derive(Clone)]
pub struct PyActantConfig {
    // --- 用户可配置参数 ---
    #[pyo3(get)]
    pub network: PyNetworkConfig,
    #[pyo3(get)]
    pub failover: PyFailoverConfig,
    #[pyo3(get)]
    pub gossip: PyGossipConfig,
    #[pyo3(get)]
    pub max_concurrent_tasks: usize,
    #[pyo3(get)]
    pub default_task_timeout_ms: u64,
    #[pyo3(get)]
    pub data_dir: Option<String>,
    #[pyo3(get)]
    pub drain_timeout_secs: u64,
    #[pyo3(get)]
    pub remote_fallback_delay_ms: u64,
    #[pyo3(get)]
    pub scheduler: String,
    /// Payload 签名密钥（必填）。所有任务 payload 使用 BLAKE3 keyed hash 签名。
    #[pyo3(get)]
    pub payload_signing_key: String,
    /// 强制要求 payload 签名。`true` 时 `payload_signing_key` 为空启动直接报错。
    /// 默认 `false`（向后兼容 0.2 行为，仅 warn 日志）。
    #[pyo3(get)]
    pub require_payload_signing: bool,
    /// worker 子进程数（进程池大小）。每个 worker 同一时刻执行一个任务，
    /// 杀进程即精确终止一个任务。`None` = 跟随 `max_concurrent_tasks`，
    /// 保持信号量与进程池容量一致。
    #[pyo3(get)]
    pub num_worker_processes: usize,
    /// worker 进程崩溃后任务重路由的最大执行次数（含首次）。默认取 Rust
    /// `WorkerConfig::default`（3）。
    #[pyo3(get)]
    pub crash_failover_max_attempts: u32,
    /// 工作流默认超时（毫秒），透传 `WorkflowConfig::default_timeout_ms`。
    /// 未指定时取 Rust `WorkflowConfig::default`（3_600_000）。
    #[pyo3(get)]
    pub workflow_default_timeout_ms: u64,
}

#[pymethods]
impl PyActantConfig {
    /// 构造 `_ActantConfig`。
    ///
    /// Worker 相关参数：
    /// - `num_worker_processes`：worker 子进程数（进程池大小，仅经 `_ActantConfig`
    ///   可配置）。`None`（默认）= 跟随 `max_concurrent_tasks`，保证信号量背压与
    ///   进程池容量一致；显式指定时二者解耦，需自行保证语义一致。
    /// - `crash_failover_max_attempts`：worker 崩溃后任务重路由的最大执行次数
    ///   （含首次）。`None`（默认）= Rust `WorkerConfig::default` 的 3。
    /// - `workflow_default_timeout_ms`：工作流默认超时（毫秒）。`None`（默认）
    ///   = Rust `WorkflowConfig::default` 的 3_600_000。
    #[new]
    #[pyo3(signature = (payload_signing_key, network=None, failover=None, gossip=None, max_concurrent_tasks=None, default_task_timeout_ms=None, data_dir=None, drain_timeout_secs=None, remote_fallback_delay_ms=None, scheduler=None, require_payload_signing=false, num_worker_processes=None, crash_failover_max_attempts=None, workflow_default_timeout_ms=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        payload_signing_key: String,
        network: Option<PyNetworkConfig>,
        failover: Option<PyFailoverConfig>,
        gossip: Option<PyGossipConfig>,
        max_concurrent_tasks: Option<usize>,
        default_task_timeout_ms: Option<u64>,
        data_dir: Option<String>,
        drain_timeout_secs: Option<u64>,
        remote_fallback_delay_ms: Option<u64>,
        scheduler: Option<String>,
        require_payload_signing: bool,
        num_worker_processes: Option<usize>,
        crash_failover_max_attempts: Option<u32>,
        workflow_default_timeout_ms: Option<u64>,
    ) -> Self {
        let default_worker = crate::common::WorkerConfig::default();
        // 默认并发度 = num_cpus：多数 Python 任务为 IO-bound，
        // 用户可显式传 max_concurrent_tasks 覆盖。
        let max_concurrent = max_concurrent_tasks.unwrap_or_else(default_max_concurrent_tasks);
        Self {
            payload_signing_key,
            network: network.unwrap_or_default(),
            failover: failover.unwrap_or_default(),
            gossip: gossip.unwrap_or_default(),
            max_concurrent_tasks: max_concurrent,
            default_task_timeout_ms: default_task_timeout_ms
                .unwrap_or(default_worker.default_task_timeout_ms),
            data_dir,
            drain_timeout_secs: drain_timeout_secs.unwrap_or(default_worker.drain_timeout_secs),
            remote_fallback_delay_ms: remote_fallback_delay_ms
                .unwrap_or(default_worker.remote_fallback_delay_ms),
            scheduler: scheduler.unwrap_or_else(|| "priority".to_string()),
            require_payload_signing,
            // 默认跟随 max_concurrent_tasks：进程池模型中并发信号量须与
            // 进程池容量一致（见 TryFrom<&PyActantConfig>）。
            num_worker_processes: num_worker_processes
                .map(|n| n.max(1))
                .unwrap_or(max_concurrent),
            crash_failover_max_attempts: crash_failover_max_attempts
                .unwrap_or(default_worker.crash_failover_max_attempts),
            workflow_default_timeout_ms: workflow_default_timeout_ms
                .unwrap_or(crate::common::WorkflowConfig::default().default_timeout_ms),
        }
    }
}

/// 默认 Worker 并发度：``num_cpus``。
///
/// 进程池模型中每个 worker 子进程同一时刻执行一个任务，进程数即本地并发上限；
/// 默认取 CPU 核数（与 `WorkerConfig::default` 保持一致）。CPU 密集与 IO 密集
/// 场景均可用；如需更高并发可显式传 ``max_concurrent_tasks``，对应拉起更多进程。
fn default_max_concurrent_tasks() -> usize {
    num_cpus::get().max(1)
}

impl Default for PyActantConfig {
    fn default() -> Self {
        let default_worker = crate::common::WorkerConfig::default();
        let max_concurrent = default_max_concurrent_tasks();
        Self {
            payload_signing_key: String::new(),
            network: PyNetworkConfig::default(),
            failover: PyFailoverConfig::default(),
            gossip: PyGossipConfig::default(),
            max_concurrent_tasks: max_concurrent,
            default_task_timeout_ms: default_worker.default_task_timeout_ms,
            data_dir: None,
            drain_timeout_secs: default_worker.drain_timeout_secs,
            remote_fallback_delay_ms: default_worker.remote_fallback_delay_ms,
            scheduler: "priority".to_string(),
            require_payload_signing: false,
            num_worker_processes: max_concurrent,
            crash_failover_max_attempts: default_worker.crash_failover_max_attempts,
            workflow_default_timeout_ms: crate::common::WorkflowConfig::default()
                .default_timeout_ms,
        }
    }
}

impl TryFrom<&PyActantConfig> for ActantConfig {
    type Error = PyErr;

    fn try_from(c: &PyActantConfig) -> PyResult<Self> {
        let default = ActantConfig::default();
        // 进程池模型：worker 子进程数决定有效本地并发。用户未显式指定
        // num_worker_processes 时其值已在构造期跟随 max_concurrent_tasks，
        // 保证并发背压信号量与进程池容量一致。
        Ok(Self {
            network: NetworkConfig::try_from(&c.network)?,
            failover: FailoverConfig::from(c.failover.clone()),
            gossip: GossipConfig::from(c.gossip.clone()),
            worker: crate::common::WorkerConfig {
                max_concurrent_tasks: c.max_concurrent_tasks.max(1),
                num_worker_processes: c.num_worker_processes.max(1),
                worker_program: python_executable(),
                python_path: python_sys_path(),
                default_task_timeout_ms: c.default_task_timeout_ms,
                drain_timeout_secs: c.drain_timeout_secs,
                remote_fallback_delay_ms: c.remote_fallback_delay_ms,
                scheduler_kind: scheduler_kind_from_str(&c.scheduler)?,
                crash_failover_max_attempts: c.crash_failover_max_attempts,
                ..default.worker
            },
            store: crate::common::StoreConfig {
                data_dir: c.data_dir.clone(),
                ..default.store
            },
            workflow: crate::common::WorkflowConfig {
                default_timeout_ms: c.workflow_default_timeout_ms,
                ..default.workflow
            },
            payload_signing_key: c.payload_signing_key.as_bytes().to_vec(),
            require_payload_signing: c.require_payload_signing,
            ..default
        })
    }
}

/// 当前解释器路径（``sys.executable``），用于拉起 worker 子进程。
///
/// Python 层始终注入运行中的解释器；仅 Rust 纯嵌入场景由嵌入方显式配置
/// `WorkerConfig::worker_program`，此时不经过本函数。
///
/// 提取失败（或得到空值）时返回空字符串并记录 `error` 日志——空
/// `worker_program` 会使 worker 子进程无法 spawn，所有任务将停在
/// 本地执行失败路径，日志必须能指向根因。
fn python_executable() -> String {
    pyo3::Python::attach(|py| {
        let extracted = pyo3::types::PyModule::import(py, "sys")
            .and_then(|sys| sys.getattr("executable"))
            .and_then(|e| e.extract::<String>());
        match extracted {
            Ok(exe) if !exe.is_empty() => exe,
            Ok(exe) => {
                tracing::error!(
                    "sys.executable resolved to an empty string; worker subprocesses \
                     will fail to spawn and tasks will not execute"
                );
                exe
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "failed to read sys.executable; worker subprocesses will fail \
                     to spawn and tasks will not execute"
                );
                String::new()
            }
        }
    })
}

/// 当前解释器的 ``sys.path``，透传给 worker 子进程作为 ``PYTHONPATH``。
///
/// 进程隔离下模块级任务函数需要 by-reference 再导入；Rust 嵌入场景直接配置
/// `WorkerConfig::python_path`，不经过本函数。
///
/// 提取失败时返回空列表并记录 `error` 日志——空 `python_path` 会使 worker
/// 子进程内模块级任务函数导入失败（ModuleNotFoundError）。
fn python_sys_path() -> Vec<String> {
    pyo3::Python::attach(|py| {
        let extracted = pyo3::types::PyModule::import(py, "sys")
            .and_then(|sys| sys.getattr("path"))
            .and_then(|path| path.extract::<Vec<String>>());
        match extracted {
            Ok(path) => path,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "failed to read sys.path; worker subprocesses will run with an \
                     empty PYTHONPATH and module-level task functions may raise \
                     ModuleNotFoundError"
                );
                Vec::new()
            }
        }
    })
}

/// 在 Python 模块上注册所有 config 相关类。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWorkflowState>()?;
    m.add_class::<PyRetryPolicy>()?;
    m.add_class::<PyNetworkConfig>()?;
    m.add_class::<PyFailoverConfig>()?;
    m.add_class::<PyGossipConfig>()?;
    m.add_class::<PyActantConfig>()?;
    Ok(())
}
