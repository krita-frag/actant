use serde::{Deserialize, Serialize};

/// 发现模式 — 已注册的策略名称。
///
/// 包装 `String` 的新类型，构造须经过 [`DiscoveryMode::parse`] 校验。
/// 未知名称在启动时返回 [`crate::common::ActantError::Config`] 而非静默回退默认值。
///
/// 内置名称见 [`discovery_mode`] 模块。可通过 [`crate::network::discovery::register_discovery`] 注册新名称。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DiscoveryMode(pub String);

impl DiscoveryMode {
    /// 不做校验地构造 `DiscoveryMode`。
    ///
    /// 校验构造请用 [`DiscoveryMode::parse`]。此原始构造器供反序列化和测试使用。
    pub fn new_unchecked(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// 校验构造器。
    ///
    /// 仅当名称已在发现注册表中注册时返回 `Ok`，否则返回
    /// [`crate::common::ActantError::Config`] — 不做静默回退。
    pub fn parse(s: &str) -> Result<Self, crate::common::ActantError> {
        if crate::network::discovery::is_registered(s) {
            Ok(Self(s.to_string()))
        } else {
            Err(crate::common::ActantError::Config(format!(
                "unknown discovery mode '{}': expected one of {}",
                s,
                crate::network::discovery::registered_names().join(", ")
            )))
        }
    }

    /// 校验此名称已注册。在启动时调用。
    pub fn validate(&self) -> Result<(), crate::common::ActantError> {
        Self::parse(self.as_str()).map(|_| ())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for DiscoveryMode {
    fn default() -> Self {
        Self(discovery_mode::LOCAL.to_string())
    }
}

impl std::fmt::Display for DiscoveryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 内置发现模式常量。
pub mod discovery_mode {
    /// 无自动发现。须通过 `bootstrap_nodes` 或 `dial()` 显式拨号。用于测试和 CI。
    pub const NONE: &str = "none";
    /// n0 预设：DNS Pkarr + relay 兜底。互联网节点的默认模式。
    pub const LOCAL: &str = "local";
    /// 仅局域网：n0 预设但禁用 relay。
    pub const MDNS: &str = "mdns";
    /// 基于 DNS 的发现，适用于企业 / Kubernetes 部署。
    pub const DNS: &str = "dns";
    /// 仅 relay 发现，适用于跨 NAT 部署。
    pub const RELAY: &str = "relay";
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ActantConfig {
    pub actor: ActorConfig,
    pub worker: WorkerConfig,
    pub network: NetworkConfig,
    pub workflow: WorkflowConfig,
    pub store: StoreConfig,
    pub failover: FailoverConfig,
    pub gossip: GossipConfig,
    pub event_bus: crate::event_bus::EventBusConfig,
    /// 非空时，所有任务 payload 使用 BLAKE3 keyed hash 签名，
    /// 反序列化前验证签名，防止恶意节点投递篡改 payload。
    pub payload_signing_key: Vec<u8>,
}

impl ActantConfig {
    /// 校验所有策略名称字段是否在对应注册表中，并确保关键安全字段有效。
    ///
    /// 在启动时、反序列化或 PyConfig 转换后调用，
    /// 以明确的错误拒绝未知发现模式、调度器类型和空 payload 签名密钥。
    pub fn validate(&self) -> Result<(), crate::common::ActantError> {
        self.worker.scheduler_kind.validate()?;
        self.network.discovery_mode.validate()?;
        if self.payload_signing_key.is_empty() {
            return Err(crate::common::ActantError::Config(
                "payload_signing_key must be non-empty: provide a shared secret to protect task payloads".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorConfig {
    pub mailbox_capacity: usize,
    pub remote_call_timeout_ms: u64,
    /// 远程 Actor 调用最大重试次数。初次尝试不计入重试（0=不重试，1=重试一次）。
    pub remote_call_max_retries: u32,
    /// 远程调用重试间隔（毫秒）。
    pub remote_call_retry_delay_ms: u64,
    pub wal_compaction_interval_secs: u64,
    pub supervision_event_capacity: usize,
    /// WAL 压缩后每个 Actor 保留的最新检查点数量。旧检查点将被清理。默认为 1。
    pub checkpoint_retention_count: usize,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            mailbox_capacity: 1024,
            remote_call_timeout_ms: 30_000,
            remote_call_max_retries: 2,
            remote_call_retry_delay_ms: 500,
            wal_compaction_interval_secs: 60,
            supervision_event_capacity: 256,
            checkpoint_retention_count: 1,
        }
    }
}

/// 调度器类型 — 已注册的策略名称。
///
/// 包装 `String` 的新类型，通过 [`SchedulerKind::parse`] 校验。
/// 未知名称在启动时返回 [`crate::common::ActantError::Config`] 而非静默回退默认值。
///
/// 内置名称见 [`scheduler_kind`] 模块。可通过 [`crate::orchestrator::scheduler::register_scheduler`] 注册新名称。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchedulerKind(pub String);

impl SchedulerKind {
    /// 不做校验地构造 `SchedulerKind`。
    pub fn new_unchecked(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// 校验构造器 — 检查调度器注册表。
    pub fn parse(s: &str) -> Result<Self, crate::common::ActantError> {
        if crate::orchestrator::scheduler::is_registered(s) {
            Ok(Self(s.to_string()))
        } else {
            Err(crate::common::ActantError::Config(format!(
                "unknown scheduler kind '{}': expected one of {}",
                s,
                crate::orchestrator::scheduler::registered_names().join(", ")
            )))
        }
    }

    /// 校验此名称已注册。在启动时调用。
    pub fn validate(&self) -> Result<(), crate::common::ActantError> {
        Self::parse(self.as_str()).map(|_| ())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SchedulerKind {
    fn default() -> Self {
        Self(scheduler_kind::PRIORITY.to_string())
    }
}

impl std::fmt::Display for SchedulerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 内置调度器类型常量。
pub mod scheduler_kind {
    pub const PRIORITY: &str = "priority";
    pub const FIFO: &str = "fifo";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// 调度器类型，内置项见 [`scheduler_kind`]。
    #[serde(default = "default_scheduler_kind")]
    pub scheduler_kind: SchedulerKind,
    pub timeout_check_interval_ms: u64,
    pub default_task_timeout_ms: u64,
    pub max_concurrent_tasks: usize,
    pub completion_channel_capacity: usize,
    pub broadcast_retry_attempts: usize,
    pub broadcast_retry_base_delay_ms: u64,
    pub drain_timeout_secs: u64,
    /// 本地无法执行的任务（仅提交节点或远程转发失败）重新入队前的延迟（毫秒）。
    pub remote_fallback_delay_ms: u64,
    /// 任务执行线程池的工作线程数。默认 `max(4, num_cpus)`。
    pub task_thread_pool_workers: usize,
    /// 任务执行线程池的通道容量。默认 `task_thread_pool_workers * 16`。
    pub task_thread_pool_channel_capacity: usize,
    /// 待处理结果重试队列的通道容量。首次投递失败的结果将入队异步重试。
    pub pending_result_channel_capacity: usize,
}

fn default_scheduler_kind() -> SchedulerKind {
    SchedulerKind::default()
}

const DEFAULT_MAX_CONCURRENT_TASKS: usize = 8;

impl Default for WorkerConfig {
    fn default() -> Self {
        let pool_workers = std::cmp::max(4, num_cpus::get());
        Self {
            scheduler_kind: default_scheduler_kind(),
            timeout_check_interval_ms: 10,
            default_task_timeout_ms: 30000,
            max_concurrent_tasks: DEFAULT_MAX_CONCURRENT_TASKS,
            completion_channel_capacity: 256,
            broadcast_retry_attempts: 3,
            broadcast_retry_base_delay_ms: 100,
            drain_timeout_secs: 30,
            remote_fallback_delay_ms: 500,
            task_thread_pool_workers: pool_workers,
            task_thread_pool_channel_capacity: pool_workers * 16,
            pending_result_channel_capacity: 256,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// 节点发现模式，内置常量见 [`discovery_mode`]。
    #[serde(default = "default_discovery_mode")]
    pub discovery_mode: DiscoveryMode,
    /// 引导节点 endpoint 地址（iroh EndpointAddr 格式）。
    pub bootstrap_nodes: Vec<String>,
    /// 节点间可接受的最大时钟漂移（毫秒）。
    pub hlc_max_drift_ms: u64,
    /// 拒绝新请求前允许的最大待处理直连请求-响应调用数。
    pub max_pending_direct_requests: usize,
    /// 订阅后加入 gossip 话题的对端 endpoint ID 列表。
    /// 建立 gossip 订阅时通过 `GossipSender::join_peers` 自动添加这些对端。
    #[serde(default)]
    pub gossip_bootstrap_peers: Vec<String>,
    /// 单个直连请求消息帧的最大字节数。超过此值将被拒绝，防止畸形或恶意对端导致 OOM。默认 16 MiB。
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    /// P2P 节点认证白名单：允许建立直连请求-响应的 iroh EndpointId 字符串集合。
    ///
    /// 空（默认）= 开放模式，接受任意对端的直连。
    /// 非空 = 仅接受 EndpointId 在此列表中的对端的直连请求；其余连接在 ALPN
    /// `accept` 阶段即被关闭，不读取其请求体。
    ///
    /// EndpointId 由 iroh 在 QUIC/TLS 握手层基于对端密钥对认证，不可伪造，
    /// 因此本字段构成对**入站直连请求**的认证白名单。gossip 广播不在本白名单
    /// 管辖范围（gossip 话题成员由 iroh-gossip 管理，0.1.0 不在此强制）。
    #[serde(default)]
    pub allowed_peer_ids: Vec<String>,
    /// 单次直连请求-响应调用的超时（毫秒）。覆盖 connect + open_bi + 读写全过程。
    /// 超时返回 `ActantError::Timeout`，防止对端故障导致调用方永久阻塞。默认 30s。
    #[serde(default = "default_direct_request_timeout_ms")]
    pub direct_request_timeout_ms: u64,
    /// iroh 绑定的 IPv4 监听端口。0 = 随机端口（默认）。
    #[serde(default)]
    pub listen_port: u16,
    /// iroh 绑定的 IPv4 监听 IP。空字符串或 "0.0.0.0" = 所有接口（默认）。
    #[serde(default)]
    pub listen_ip: String,
}

impl NetworkConfig {
    /// 默认 HLC 最大时钟漂移（毫秒）。
    pub const DEFAULT_HLC_MAX_DRIFT_MS: u64 = 500;
    /// 默认在途直连请求-响应调用上限。
    pub const DEFAULT_MAX_PENDING_DIRECT_REQUESTS: usize = 1024;
    /// 默认直连请求消息帧最大尺寸（16 MiB）。
    pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
    /// 默认直连请求-响应超时（30s）。
    pub const DEFAULT_DIRECT_REQUEST_TIMEOUT_MS: u64 = 30_000;
}

fn default_direct_request_timeout_ms() -> u64 {
    NetworkConfig::DEFAULT_DIRECT_REQUEST_TIMEOUT_MS
}

fn default_discovery_mode() -> DiscoveryMode {
    DiscoveryMode::default()
}

fn default_max_message_size() -> usize {
    NetworkConfig::DEFAULT_MAX_MESSAGE_SIZE
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            discovery_mode: default_discovery_mode(),
            bootstrap_nodes: Vec::new(),
            hlc_max_drift_ms: Self::DEFAULT_HLC_MAX_DRIFT_MS,
            max_pending_direct_requests: Self::DEFAULT_MAX_PENDING_DIRECT_REQUESTS,
            gossip_bootstrap_peers: Vec::new(),
            max_message_size: default_max_message_size(),
            allowed_peer_ids: Vec::new(),
            direct_request_timeout_ms: default_direct_request_timeout_ms(),
            listen_port: 0,
            listen_ip: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowConfig {
    pub state_poll_interval_ms: u64,
    pub completed_retention_count: usize,
    pub default_timeout_ms: u64,
    /// 后台持久化刷新间隔（毫秒）。
    /// 脏工作流执行状态按此间隔批量写入存储，而非每次状态变更都写，减少写放大。
    /// 终态总是立即持久化。
    pub persist_flush_interval_ms: u64,
}

impl Default for WorkflowConfig {
    fn default() -> Self {
        Self {
            state_poll_interval_ms: 500,
            completed_retention_count: 1000,
            default_timeout_ms: 3_600_000,
            persist_flush_interval_ms: 200,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    pub data_dir: Option<String>,
    pub map_size: usize,
    pub max_dbs: u32,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            // LMDB mmap 上限（2 GiB）。须足够支撑生产负载，但不会预分配磁盘空间。
            map_size: 2 * 1024 * 1024 * 1024,
            max_dbs: 16,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    pub heartbeat_interval_ms: u64,
    pub failure_timeout_ms: u64,
    pub lease_expiry_check_interval_secs: u64,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 2000,
            failure_timeout_ms: 8000,
            lease_expiry_check_interval_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipConfig {
    pub dedup_window_size: usize,
    pub dedup_ttl_secs: u64,
    /// 终态更新（Completed/Failed）的广播最大重试次数。非终态更新（Running）仅发送一次不重试。
    pub retry_attempts: usize,
    /// 重试间隔基数（毫秒）。实际延迟采用指数退避：`retry_base_delay_ms * attempt_number`。
    pub retry_base_delay_ms: u64,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            dedup_window_size: 1024,
            dedup_ttl_secs: 300,
            retry_attempts: 3,
            retry_base_delay_ms: 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_empty_signing_key() {
        let mut config = ActantConfig::default();
        config.payload_signing_key = Vec::new();
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("payload_signing_key must be non-empty"),
            "expected empty signing key error, got: {}",
            err
        );
    }

    #[test]
    fn validate_accepts_non_empty_signing_key() {
        let mut config = ActantConfig::default();
        config.payload_signing_key = b"shared-secret".to_vec();
        assert!(config.validate().is_ok());
    }
}
