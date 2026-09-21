use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 发现模式 — 内置策略名称。
///
/// 包装 `String` 的新类型，构造须经过 [`DiscoveryMode::parse`] 校验。
/// 未知名称在启动时返回 [`crate::ActantError::Config`] 而非静默回退默认值。
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
    /// 仅当名称是内置发现模式时返回 `Ok`，否则返回
    /// [`crate::ActantError::Config`] — 不做静默回退。
    pub fn parse(s: &str) -> Result<Self, crate::ActantError> {
        if matches!(
            s,
            discovery_mode::NONE | discovery_mode::LOCAL | discovery_mode::DNS
        ) {
            Ok(Self(s.to_string()))
        } else {
            Err(crate::ActantError::Config(format!(
                "unknown discovery mode '{}': expected one of {}, {}, {}",
                s,
                discovery_mode::NONE,
                discovery_mode::LOCAL,
                discovery_mode::DNS
            )))
        }
    }

    /// 校验此名称已注册。在启动时调用。
    pub fn validate(&self) -> Result<(), crate::ActantError> {
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
/// - `none`：无自动发现，仅靠 `bootstrap_nodes` 显式拨号。
/// - `local`：n0 预设（DNS + Pkarr 发布 + relay 兜底），适合互联网节点。
/// - `dns`：仅 DNS endpoint 发现（`DnsAddressLookup` + `PkarrPublisher`），无 relay。
///   适合 K8s Headless Service / 自建 DNS 场景：通过 `dns_origin_domain` 指定起源域。
/// - `relay`：强制启用 iroh relay（`RelayMode::Default` + DNS），适合 NAT 穿透场景。
pub mod discovery_mode {
    /// 无自动发现。须通过 `bootstrap_nodes` 或 `dial()` 显式拨号。用于测试和 CI。
    pub const NONE: &str = "none";
    /// n0 预设：DNS Pkarr + relay 兜底。互联网节点的默认模式。
    pub const LOCAL: &str = "local";
    /// 仅局域网：n0 预设但禁用 relay。
    pub const MDNS: &str = "mdns";
    /// 仅 DNS endpoint 发现（无 relay）。配合 `dns_origin_domain` 使用。
    pub const DNS: &str = "dns";
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
    pub event_bus: EventBusConfig,
    /// 任务 payload 签名密钥。
    ///
    /// - 非空时：所有任务 payload 使用 BLAKE3 keyed hash 签名，反序列化前验证签名，
    ///   防止恶意节点投递篡改 payload（生产环境推荐）。
    /// - 空时：禁用签名验证，payload 直接透传（仅用于开发/测试）。
    pub payload_signing_key: Vec<u8>,
    /// 强制要求 payload 签名。生产环境硬约束。
    ///
    /// - `false`（默认）：`payload_signing_key` 为空时仅 `warn` 日志，不阻止启动
    ///   （向后兼容 0.2 行为，用于开发/测试）。
    /// - `true`：`payload_signing_key` 为空时启动直接返回
    ///   [`crate::ActantError::Config`]，防止生产环境静默运行无签名模式。
    ///
    /// 由 `ActantConfig::validate` 在启动时强制检查，RuntimeBuilder 在 build 前
    /// 调用 validate，因此无法绕过。
    #[serde(default)]
    pub require_payload_signing: bool,
    /// 用户自定义节点标签（N2），随心跳广播给集群。
    ///
    /// 总字节量（key + value 长度之和）超过 [`crate::model::NODE_LABELS_MAX_BYTES`]
    /// 时心跳整体置空标签并告警。
    #[serde(default)]
    pub node_labels: BTreeMap<String, String>,
    /// 宿主语言运行时描述（N1），由绑定层填充（如 "CPython 3.12.1"）。
    /// 核心/纯 Rust 嵌入为 `None`。核心自动填充 os/arch/actant_version。
    #[serde(default)]
    pub node_host_runtime: Option<String>,
}

impl ActantConfig {
    /// 校验所有策略名称字段是否在对应注册表中。
    ///
    /// 在启动时、反序列化或 PyConfig 转换后调用，以明确的错误拒绝未知发现模式
    /// 和调度器类型。
    ///
    /// # Payload 签名约束
    ///
    /// 当 `require_payload_signing = true` 时，`payload_signing_key` 必须非空，
    /// 否则返回 [`crate::ActantError::Config`]。这为生产环境提供硬失败
    /// 语义，避免依赖运行时 `warn` 日志被忽视。
    pub fn validate(&self) -> Result<(), crate::ActantError> {
        self.worker.scheduler_kind.validate()?;
        self.network.discovery_mode.validate()?;
        self.failover.validate()?;
        if self.require_payload_signing && self.payload_signing_key.is_empty() {
            return Err(crate::ActantError::Config(
                "require_payload_signing=true but payload_signing_key is empty; \
                 configure a non-empty shared secret or set require_payload_signing=false \
                 for development"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorConfig {
    pub mailbox_capacity: usize,
    /// 单个 Actor stop 超时（毫秒）。超时后放弃等待，使 shutdown 路径总能完成
    /// （如 network.shutdown）。默认 500ms（M1 改进：从硬编码提取为配置）。
    pub stop_timeout_ms: u64,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            mailbox_capacity: 1024,
            stop_timeout_ms: 500,
        }
    }
}

/// 调度器类型 — 内置策略名称。
///
/// 包装 `String` 的新类型，通过 [`SchedulerKind::parse`] 校验。
/// 未知名称在启动时返回 [`crate::ActantError::Config`] 而非静默回退默认值。
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

    /// 校验构造器 — 检查内置调度器种类。
    pub fn parse(s: &str) -> Result<Self, crate::ActantError> {
        if matches!(s, scheduler_kind::FIFO | scheduler_kind::PRIORITY) {
            Ok(Self(s.to_string()))
        } else {
            Err(crate::ActantError::Config(format!(
                "unknown scheduler kind '{}': expected one of {}, {}",
                s,
                scheduler_kind::FIFO,
                scheduler_kind::PRIORITY
            )))
        }
    }

    /// 校验此名称已注册。在启动时调用。
    pub fn validate(&self) -> Result<(), crate::ActantError> {
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
    pub default_task_timeout_ms: u64,
    /// 本地最大并发任务数（信号量背压 + 远端转发判断）。
    ///
    /// 进程池后端的有效本地并发由 worker 子进程数决定；此值应等于
    /// `num_worker_processes`，保持信号量与进程池容量一致。
    pub max_concurrent_tasks: usize,
    pub broadcast_retry_attempts: usize,
    pub broadcast_retry_base_delay_ms: u64,
    pub drain_timeout_secs: u64,
    /// 本地无法执行的任务（仅提交节点或远程转发失败）重新入队前的延迟（毫秒）。
    pub remote_fallback_delay_ms: u64,
    /// worker 子进程数（进程池大小）。每个 worker 进程同一时刻执行一个任务，
    /// 杀进程即精确终止一个任务。默认 `num_cpus`。
    pub num_worker_processes: usize,
    /// 拉起 worker 子进程的可执行文件路径。
    /// 进程池以 `[worker_program, worker_args…]` 启动 worker；参数与环境变量
    /// 由绑定层或嵌入方提供（核心不感知任何语言语义）。Python 绑定层始终注入
    /// 运行中的解释器路径。
    #[serde(default)]
    pub worker_program: String,
    /// 传给 `worker_program` 的参数（不含 program 自身）。Python 绑定层注入
    /// 模块入口形如 `["-m", "actant.task._worker"]`。
    #[serde(default)]
    pub worker_args: Vec<String>,
    /// 注入 worker 子进程的环境变量。空表 = 完全继承父进程环境。
    /// Python 绑定层经此注入模块搜索路径（`PYTHONPATH`）；键唯一，重复注入
    /// 以最后一次为准。
    #[serde(default)]
    pub worker_env: BTreeMap<String, String>,
    /// worker 进程崩溃后任务重新入队重路由的最大执行次数（含首次执行）。
    ///
    /// 进程崩溃（`ActantError::Worker`，worker 进程异常退出）属于基础设施级失败，
    /// 与业务失败、超时不同：任务会被清空 `target_node` 重新入队，由路由器重选
    /// 本地或远端节点重试。该次数即为上限，防止持久性崩溃在无退路时无限重路由。
    /// 默认 3（首次 + 最多 2 次转移）；超时与业务失败不参与此上限，保持原有失败语义。
    pub crash_failover_max_attempts: u32,
    /// 取消/硬超时触发后，向 worker 发送 `Cancel` 帧等待其协作退出的宽限期（毫秒）。
    /// 宽限期过后仍未退出则强杀进程。默认 2000ms。
    pub worker_cancel_grace_ms: u64,
    /// 待处理结果重试队列的通道容量。首次投递失败的结果将入队异步重试。
    pub pending_result_channel_capacity: usize,
    /// Worker 主循环批量 prefetch 的最小批量。
    ///
    /// `max_concurrent_tasks` 低于此值时，prefetch 批量大小仍取此值，
    /// 保证低并发节点也能一次拉取一批任务，减少 dequeue 调用次数。
    #[serde(default = "default_prefetch_min")]
    pub prefetch_min: usize,
    /// Worker 主循环批量 prefetch 的最大批量。
    ///
    /// 上限防止过度 prefetch 占用调度器内存。须 >= `prefetch_min`
    /// （构造时由 `Worker::new` 归一化，避免 `clamp` panic）。
    #[serde(default = "default_prefetch_max")]
    pub prefetch_max: usize,
}

fn default_prefetch_min() -> usize {
    16
}

fn default_prefetch_max() -> usize {
    64
}

fn default_scheduler_kind() -> SchedulerKind {
    SchedulerKind::default()
}

impl Default for WorkerConfig {
    fn default() -> Self {
        // 进程池模型：worker 进程数与并发度默认取 CPU 核数。
        let proc_count = num_cpus::get().max(1);
        Self {
            scheduler_kind: default_scheduler_kind(),
            default_task_timeout_ms: 30000,
            max_concurrent_tasks: proc_count,
            broadcast_retry_attempts: 3,
            broadcast_retry_base_delay_ms: 100,
            drain_timeout_secs: 30,
            remote_fallback_delay_ms: 500,
            num_worker_processes: proc_count,
            crash_failover_max_attempts: 3,
            worker_program: String::new(),
            worker_args: Vec::new(),
            worker_env: BTreeMap::new(),
            worker_cancel_grace_ms: 2000,
            pending_result_channel_capacity: 256,
            prefetch_min: default_prefetch_min(),
            prefetch_max: default_prefetch_max(),
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
    /// 管辖范围（gossip 话题成员由 iroh-gossip 管理，不由此字段控制）。
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
    /// 自定义 DNS 起源域名，仅当 `discovery_mode = "dns"` 时生效。
    ///
    /// - 空字符串（默认）：使用 n0 公共 DNS 服务（`iroh.link`）。
    /// - 非空：使用此域名作为 DNS endpoint 发现的起源域，例如自建 DNS 服务时
    ///   填入 `actant.internal.example.com`。
    ///
    /// 节点会向此域发布 `_iroh.<z32-endpoint-id>.<origin_domain>` TXT 记录，
    /// 其他节点通过相同域查询。
    #[serde(default)]
    pub dns_origin_domain: String,
    /// 自定义 relay 集群 URL 列表（G-relay）。
    ///
    /// 非空时以 `RelayMode::Custom` **覆盖** preset 自带的 relay 配置
    /// （discovery 与 relay 正交：preset 决定发现机制，本字段决定中继）。
    /// 空（默认）= 沿用 preset 的 relay 设置。
    #[serde(default)]
    pub relay_endpoints: Vec<String>,
    /// 强制校验心跳节点记录签名（身份与信任）。
    ///
    /// `true` 时 `FailoverManager` 拒绝缺签或验签失败的 gossip 心跳——
    /// 节点记录必须由其 iroh endpoint 私钥签名，防止伪造他人节点身份。
    /// `false`（默认，向后兼容）跳过校验。`Runtime.production()` 默认开启。
    #[serde(default)]
    pub require_signed_records: bool,
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
            dns_origin_domain: String::new(),
            relay_endpoints: Vec::new(),
            require_signed_records: false,
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
    /// 事件历史留存：每个工作流最多保留多少条**已被快照吸收**的事件
    /// （即 `id <= 持久化水位` 的那一批）。
    ///
    /// `0` = **不裁剪**（默认，保守：不静默改变既有留存行为）。
    /// 水位**之后**的事件永不裁剪——它们是重放所需的增量。见
    /// 事件历史留存策略。
    pub event_log_max_events_per_workflow: usize,
}

impl Default for WorkflowConfig {
    fn default() -> Self {
        Self {
            state_poll_interval_ms: 500,
            completed_retention_count: 1000,
            default_timeout_ms: 3_600_000,
            persist_flush_interval_ms: 200,
            event_log_max_events_per_workflow: 0,
        }
    }
}

/// Store 持久化同步策略。
///
/// 控制单 key 写入路径（`Store::put` / `Store::delete`）的 fsync 行为。
/// 批量写入路径（`Store::put_batch`）始终单事务提交，不受此配置影响。
///
/// # 模式对比
///
/// | 模式 | 单 key 写延迟 | 数据丢失窗口 | 适用场景 |
/// |------|---------------|--------------|----------|
/// | `Sync` | ~2.9 ms（含 fsync） | 0（提交即持久） | 关键状态、低写入速率 |
/// | `GroupCommit(ms)` | ~1-10 µs（仅入队） | `ms` 毫秒 | 高吞吐 event_log / 等待点快照 |
/// | `NoSync` | ~10-50 µs（mmap 写入） | 进程崩溃时未 fsync 部分 | 可重建的缓存型数据 |
///
/// # `GroupCommit` 语义
///
/// `GroupCommit(ms)` 启用 [`crate::runtime::state::WriteBatcher`]：单 key 写入
/// 进入有界通道，后台任务每 `ms` 毫秒或满 `BATCH_FLUSH_THRESHOLD` 条时
/// 合并为单次 LMDB 事务提交（一次 fsync）。崩溃时丢失最近 `ms` 毫秒内的写入。
///
/// 高频写入路径（工作流快照 / 事件水位）由此把多次单 key 写的 fsync
/// 合并为一次事务提交；正确性语义不依赖该合并（崩溃丢失窗口见上表）。
///
/// # `NoSync` 语义
///
/// `NoSync` 在 LMDB 打开时设置 `MDB_NOSYNC`：写事务 commit 时跳过 fsync，
/// 由 OS page cache 异步刷盘。进程崩溃但 OS 正常运行时数据不丢失；
/// OS 崩溃或断电时丢失最近未刷盘的写入。需配合周期性
/// [`crate::runtime::state::Store::sync`] 显式刷盘。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "mode", content = "flush_interval_ms")]
pub enum SyncMode {
    /// 每次写事务 commit 即 fsync（默认，最强持久性）。
    #[default]
    Sync,
    /// 后台合并提交：单 key 写入入队，每 `flush_interval_ms` 毫秒合并提交一次。
    ///
    /// 单位：毫秒。建议 1-10ms；过小退化为 Sync，过大增加丢失窗口。
    GroupCommit(u64),
    /// 跳过 fsync，依赖 OS page cache 异步刷盘。
    ///
    /// 调用方需周期性调用 [`crate::runtime::state::Store::sync`] 显式持久化。
    NoSync,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    pub data_dir: Option<String>,
    pub map_size: usize,
    pub max_dbs: u32,
    /// 单 key 写入路径的同步策略。默认 [`SyncMode::Sync`]。
    ///
    /// 详见 [`SyncMode`] 文档对三种模式语义与权衡的说明。
    #[serde(default)]
    pub sync_mode: SyncMode,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            // LMDB mmap 上限（2 GiB）。须足够支撑生产负载，但不会预分配磁盘空间。
            map_size: 2 * 1024 * 1024 * 1024,
            max_dbs: 16,
            sync_mode: SyncMode::default(),
        }
    }
}

/// EventBus 配置（F5 下移：纯配置结构，与 runtime 无依赖）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusConfig {
    /// 每个订阅者的默认通道容量。
    #[serde(default = "default_subscriber_capacity")]
    pub subscriber_capacity: usize,
}

impl EventBusConfig {
    /// 默认订阅者通道容量。
    pub const DEFAULT_SUBSCRIBER_CAPACITY: usize = 256;
}

fn default_subscriber_capacity() -> usize {
    EventBusConfig::DEFAULT_SUBSCRIBER_CAPACITY
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            subscriber_capacity: EventBusConfig::DEFAULT_SUBSCRIBER_CAPACITY,
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
    pub fn validate(&self) -> Result<(), crate::ActantError> {
        if self.heartbeat_interval_ms == 0 {
            return Err(crate::ActantError::Config(format!(
                "failover.heartbeat_interval_ms must be > 0, got {}",
                self.heartbeat_interval_ms
            )));
        }
        if self.failure_timeout_ms <= self.heartbeat_interval_ms {
            return Err(crate::ActantError::Config(format!(
                "failover.failure_timeout_ms ({}) must be > heartbeat_interval_ms ({})",
                self.failure_timeout_ms, self.heartbeat_interval_ms
            )));
        }
        if self.lease_duration_ms <= self.failure_timeout_ms {
            return Err(crate::ActantError::Config(format!(
                "failover.lease_duration_ms ({}) must be > failure_timeout_ms ({}) to prevent split-brain",
                self.lease_duration_ms, self.failure_timeout_ms
            )));
        }
        if self.lease_expiry_check_interval_secs == 0 {
            return Err(crate::ActantError::Config(format!(
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
    /// 重试间隔基数（毫秒）。实际延迟采用指数退避：`base * 2^attempt`，上限 30s。
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
#[path = "../../../tests/rust/unit/common/config.rs"]
mod tests;
