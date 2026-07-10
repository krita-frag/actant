use serde::{Deserialize, Serialize};

/// 发现模式 — 内置策略名称。
///
/// 包装 `String` 的新类型，构造须经过 [`DiscoveryMode::parse`] 校验。
/// 未知名称在启动时返回 [`crate::common::ActantError::Config`] 而非静默回退默认值。
///
/// 内置名称见 [`discovery_mode`] 模块。自定义发现策略应通过 Rust `Discovery` trait
/// 扩展（纯 Rust 嵌入场景）或后续由 Python 层注入。
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
        if crate::runtime::network::is_registered(s) {
            Ok(Self(s.to_string()))
        } else {
            Err(crate::common::ActantError::Config(format!(
                "unknown discovery mode '{}': expected one of {}",
                s,
                crate::runtime::network::registered_names().join(", ")
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
///
/// 当前仅实现 `none` / `local` / `mdns` 三种策略。`dns` / `relay` 等策略
/// 属于 0.2 预留，待实现后再加入常量，避免用户引用未支持的模式。
pub mod discovery_mode {
    /// 无自动发现。须通过 `bootstrap_nodes` 或 `dial()` 显式拨号。用于测试和 CI。
    pub const NONE: &str = "none";
    /// n0 预设：DNS Pkarr + relay 兜底。互联网节点的默认模式。
    pub const LOCAL: &str = "local";
    /// 仅局域网：n0 预设但禁用 relay。
    pub const MDNS: &str = "mdns";
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
    pub event_bus: crate::runtime::event_bus::EventBusConfig,
    /// 任务 payload 签名密钥。
    ///
    /// - 非空时：所有任务 payload 使用 BLAKE3 keyed hash 签名，反序列化前验证签名，
    ///   防止恶意节点投递篡改 payload（生产环境推荐）。
    /// - 空时：禁用签名验证，payload 直接透传（仅用于开发/测试）。
    pub payload_signing_key: Vec<u8>,
}

impl ActantConfig {
    /// 校验所有策略名称字段是否在对应注册表中。
    ///
    /// 在启动时、反序列化或 PyConfig 转换后调用，以明确的错误拒绝未知发现模式
    /// 和调度器类型。payload 签名密钥允许为空，表示禁用签名验证。
    pub fn validate(&self) -> Result<(), crate::common::ActantError> {
        self.worker.scheduler_kind.validate()?;
        self.network.discovery_mode.validate()?;
        self.failover.validate()?;
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
    /// 单个 Actor stop 超时（毫秒）。超时后放弃等待，使 shutdown 路径总能完成
    /// （如 network.shutdown）。默认 500ms（M1 改进：从硬编码提取为配置）。
    pub stop_timeout_ms: u64,
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
            stop_timeout_ms: 500,
        }
    }
}

/// 调度器类型 — 内置策略名称。
///
/// 包装 `String` 的新类型，通过 [`SchedulerKind::parse`] 校验。
/// 未知名称在启动时返回 [`crate::common::ActantError::Config`] 而非静默回退默认值。
///
/// 内置名称见 [`scheduler_kind`] 模块。自定义调度策略应通过 Rust `Scheduler` trait
/// 扩展（纯 Rust 嵌入场景）或后续由 Python 层注入。
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
        if crate::runtime::workflow::scheduler::is_registered(s) {
            Ok(Self(s.to_string()))
        } else {
            Err(crate::common::ActantError::Config(format!(
                "unknown scheduler kind '{}': expected one of {}",
                s,
                crate::runtime::workflow::scheduler::registered_names().join(", ")
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
    /// Capability gossip 广播间隔（毫秒）。默认 60 秒。
    #[serde(default = "default_capability_gossip_interval_ms")]
    pub capability_gossip_interval_ms: u64,
    /// 网络事件有界通道容量。
    ///
    /// `NetworkManager` 内部使用此容量的 `mpsc::channel` 缓冲 `NetworkEvent`。
    /// 当事件生产速率超过消费速率时，新事件被丢弃（仅记录日志）以避免无界队列导致 OOM。
    /// 高吞吐场景下应适当增大此值。默认 1024。
    #[serde(default = "default_event_channel_capacity")]
    pub event_channel_capacity: usize,
}

fn default_capability_gossip_interval_ms() -> u64 {
    NetworkConfig::DEFAULT_CAPABILITY_GOSSIP_INTERVAL_MS
}

fn default_event_channel_capacity() -> usize {
    NetworkConfig::DEFAULT_EVENT_CHANNEL_CAPACITY
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
    /// 默认 capability gossip 广播间隔（60s）。
    pub const DEFAULT_CAPABILITY_GOSSIP_INTERVAL_MS: u64 = 60_000;
    /// 默认网络事件通道容量。
    pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 1024;
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
            capability_gossip_interval_ms: default_capability_gossip_interval_ms(),
            event_channel_capacity: default_event_channel_capacity(),
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

/// 故障转移相关时序参数。
///
/// # 参数关系
///
/// 四个时序参数共同决定故障检测与工作流接管的行为，必须满足以下关系：
///
/// ```text
/// heartbeat_interval_ms < failure_timeout_ms < lease_duration_ms
///                                          <
///                            lease_expiry_check_interval_secs * 1000
///                                            (建议，非硬性约束)
/// ```
///
/// - **`heartbeat_interval_ms`**：节点发送心跳的间隔。必须远小于
///   `failure_timeout_ms`，以确保在超时窗口内至少有 2-3 次心跳机会，
///   避免网络抖动导致误判。建议 `failure_timeout_ms >= heartbeat_interval_ms * 3`。
///
/// - **`failure_timeout_ms`**：判定节点失联的超时阈值。超过此时间未收到心跳即认为
///   节点故障。必须小于 `lease_duration_ms`，否则旧持有者的租约尚未过期，
///   新节点无法安全接管工作流，可能导致双主。
///
/// - **`lease_duration_ms`**：工作流租约时长。持有者必须在此时间内续约，否则租约过期，
///   其他节点可竞争接管。默认 = `failure_timeout_ms * 2`，确保故障检测（约
///   `failure_timeout_ms`）完成后仍有充足时间让原持有者的租约自然过期。
///
/// - **`lease_expiry_check_interval_secs`**：租约过期扫描周期。此值决定了从租约实际
///   过期到被检测到的延迟。建议 `lease_expiry_check_interval_secs * 1000 <=
///   lease_duration_ms / 2`，避免过期租约长时间未被清理。注意此参数以**秒**为单位，
///   其余三个以**毫秒**为单位，配置时注意单位转换。
///
/// # 故障转移时序示例
///
/// 默认值（`heartbeat=2s, failure=8s, lease=16s, check=30s`）下的典型流程：
///
/// ```text
/// t=0s    节点 A 持有工作流 W 的租约（有效期至 t=16s）
/// t=2s    A 发送心跳
/// t=4s    A 发送心跳
/// t=6s    A 发送心跳
/// t=8s    A 崩溃，停止心跳
/// t=10s   其他节点发现 A 已超过 failure_timeout (8s) 未心跳 → 标记 A 失联
/// t=16s   W 的租约过期
/// t≤46s   下次 lease_expiry_check 扫描发现 W 租约过期 → 触发接管选举
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// 心跳发送间隔（毫秒）。建议 `failure_timeout_ms / 3` 以上。
    pub heartbeat_interval_ms: u64,
    /// 节点失联判定阈值（毫秒）。超过此时间未收到心跳即认为节点故障。
    /// 必须严格大于 `heartbeat_interval_ms`，严格小于 `lease_duration_ms`。
    pub failure_timeout_ms: u64,
    /// 租约过期扫描周期（**秒**）。注意与其他参数的单位差异。
    /// 建议此值（换算为毫秒）不超过 `lease_duration_ms / 2`。
    pub lease_expiry_check_interval_secs: u64,
    /// 工作流租约时长（毫秒）。默认 = `failure_timeout_ms * 2`。
    /// 必须严格大于 `failure_timeout_ms`，否则故障检测完成前租约未过期，
    /// 可能导致双主。
    #[serde(default = "default_lease_duration_ms")]
    pub lease_duration_ms: u64,
}

fn default_lease_duration_ms() -> u64 {
    16_000
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 2000,
            failure_timeout_ms: 8000,
            lease_expiry_check_interval_secs: 30,
            lease_duration_ms: default_lease_duration_ms(),
        }
    }
}

impl FailoverConfig {
    /// 校验时序参数关系满足故障转移安全约束。
    ///
    /// 调用方：[`ActantConfig::validate`]。在启动时调用以尽早发现配置错误。
    ///
    /// # 约束
    ///
    /// 1. `heartbeat_interval_ms > 0`：心跳间隔必须为正。
    /// 2. `failure_timeout_ms > heartbeat_interval_ms`：超时阈值必须大于心跳间隔，
    ///    确保至少一次心跳机会（建议 3 倍以上，但此处仅强制最小约束）。
    /// 3. `lease_duration_ms > failure_timeout_ms`：租约时长必须大于故障检测阈值，
    ///    防止双主。
    /// 4. `lease_expiry_check_interval_secs > 0`：扫描周期必须为正。
    pub fn validate(&self) -> Result<(), crate::common::ActantError> {
        if self.heartbeat_interval_ms == 0 {
            return Err(crate::common::ActantError::Config(format!(
                "failover.heartbeat_interval_ms must be > 0, got {}",
                self.heartbeat_interval_ms
            )));
        }
        if self.failure_timeout_ms <= self.heartbeat_interval_ms {
            return Err(crate::common::ActantError::Config(format!(
                "failover.failure_timeout_ms ({}) must be > heartbeat_interval_ms ({})",
                self.failure_timeout_ms, self.heartbeat_interval_ms
            )));
        }
        if self.lease_duration_ms <= self.failure_timeout_ms {
            return Err(crate::common::ActantError::Config(format!(
                "failover.lease_duration_ms ({}) must be > failure_timeout_ms ({}) to prevent split-brain",
                self.lease_duration_ms, self.failure_timeout_ms
            )));
        }
        if self.lease_expiry_check_interval_secs == 0 {
            return Err(crate::common::ActantError::Config(format!(
                "failover.lease_expiry_check_interval_secs must be > 0, got {}",
                self.lease_expiry_check_interval_secs
            )));
        }
        Ok(())
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
    /// 周期性广播 DAG heads 的间隔（毫秒）。默认 30 秒。
    pub heads_broadcast_interval_ms: u64,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            dedup_window_size: 1024,
            dedup_ttl_secs: 300,
            retry_attempts: 3,
            retry_base_delay_ms: 100,
            heads_broadcast_interval_ms: 30_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_empty_signing_key() {
        let config = ActantConfig {
            payload_signing_key: Vec::new(),
            ..Default::default()
        };
        assert!(
            config.validate().is_ok(),
            "empty signing key should disable signing"
        );
    }

    #[test]
    fn validate_accepts_non_empty_signing_key() {
        let config = ActantConfig {
            payload_signing_key: b"shared-secret".to_vec(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn failover_validate_accepts_default() {
        assert!(FailoverConfig::default().validate().is_ok());
    }

    #[test]
    fn failover_validate_rejects_zero_heartbeat() {
        let cfg = FailoverConfig {
            heartbeat_interval_ms: 0,
            ..FailoverConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(format!("{err}").contains("heartbeat_interval_ms"));
    }

    #[test]
    fn failover_validate_rejects_failure_le_heartbeat() {
        let default = FailoverConfig::default();
        let cfg = FailoverConfig {
            failure_timeout_ms: default.heartbeat_interval_ms,
            ..default
        };
        let err = cfg.validate().unwrap_err();
        assert!(format!("{err}").contains("failure_timeout_ms"));
    }

    #[test]
    fn failover_validate_rejects_lease_le_failure() {
        let default = FailoverConfig::default();
        let cfg = FailoverConfig {
            lease_duration_ms: default.failure_timeout_ms,
            ..default
        };
        let err = cfg.validate().unwrap_err();
        assert!(format!("{err}").contains("split-brain"));
    }

    #[test]
    fn failover_validate_rejects_zero_check_interval() {
        let cfg = FailoverConfig {
            lease_expiry_check_interval_secs: 0,
            ..FailoverConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(format!("{err}").contains("lease_expiry_check_interval_secs"));
    }
}
