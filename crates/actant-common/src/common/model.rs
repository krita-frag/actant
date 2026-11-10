use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rkyv::Archive;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 为 ID 新类型生成统一访问器与 trait 实现。
///
/// 内部字符串为 `pub`：core 需直接读取（日志/格式化/测试断言），
/// 跨 crate 的 `pub(crate)` 不可见。构造仍走 `generate()`/`new()`/`From`。
///
/// `#[serde(transparent)]` 与 rkyv derive 保持序列化二进制兼容（与 `pub String` 时期一致）。
macro_rules! impl_id_type {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            Hash,
            Eq,
            PartialEq,
            Serialize,
            Deserialize,
            Archive,
            rkyv::Serialize,
            rkyv::Deserialize,
        )]
        #[serde(transparent)]
        #[rkyv(bytecheck())]
        #[rkyv(derive(Hash, Eq, PartialEq, Debug))]
        pub struct $name(pub String);

        impl $name {
            /// 生成随机 UUID 字符串的新 ID。
            pub fn generate() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            /// 从已有字符串构造 ID（不校验格式，调用方负责合法性）。
            pub fn new(s: String) -> Self {
                Self(s)
            }

            /// 读取内部字符串引用。
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// 消费 ID 返回内部字符串。
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }

        impl std::str::FromStr for $name {
            type Err = std::convert::Infallible;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(s.to_string()))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

impl_id_type!(TaskId);
impl_id_type!(WorkflowId);
impl_id_type!(ActorId);
impl_id_type!(NodeId);
impl_id_type!(MessageId);

/// 内容寻址 blob 标识：blake3 32 字节哈希。
///
/// blob 原语（`runtime::blobs`）与 `BlobRef` wire 编码共用的值引用标识。
/// `#[serde(transparent)]` 使 postcard/wire 编码为裸 32 字节，无额外头部。
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlobHash([u8; 32]);

impl BlobHash {
    /// 从 32 字节原始哈希构造。
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// 读取原始 32 字节哈希。
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for BlobHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&data_encoding::HEXLOWER.encode(&self.0))
    }
}

impl std::str::FromStr for BlobHash {
    type Err = crate::common::ActantError;

    /// 从 64 字符小写 hex 解析。
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = data_encoding::HEXLOWER.decode(s.as_bytes()).map_err(|e| {
            crate::common::ActantError::Serialization(format!("invalid blob hash hex '{s}': {e}"))
        })?;
        let arr: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
            crate::common::ActantError::Serialization(format!(
                "blob hash must be 32 bytes, got {}",
                v.len()
            ))
        })?;
        Ok(Self(arr))
    }
}

impl ActorId {
    /// Workflow orchestrator actor for a node.
    pub fn workflow(node_id: &NodeId) -> Self {
        Self::from(format!("workflow-{}", node_id.as_str()))
    }

    /// Scheduler actor for a node.
    pub fn scheduler(node_id: &NodeId) -> Self {
        Self::from(format!("scheduler-{}", node_id.as_str()))
    }

    /// Failover manager actor for a node.
    pub fn failover(node_id: &NodeId) -> Self {
        Self::from(format!("failover-{}", node_id.as_str()))
    }

    /// DAG gossip actor for a node.
    pub fn dag_gossip(node_id: &NodeId) -> Self {
        Self::from(format!("dag-gossip-{}", node_id.as_str()))
    }

    /// Capability handler actor for a capability name.
    pub fn capability(name: impl AsRef<str>) -> Self {
        Self::from(format!("capability-{}", name.as_ref()))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(bytecheck())]
pub enum ActorStatus {
    Created,
    Running,
    Stopped,
    Failed,
}

impl ActorStatus {
    /// PyO3 边界使用的稳定字符串表示。
    pub fn as_str(&self) -> &'static str {
        match self {
            ActorStatus::Created => "Created",
            ActorStatus::Running => "Running",
            ActorStatus::Stopped => "Stopped",
            ActorStatus::Failed => "Failed",
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(bytecheck())]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub delay_ms: u64,
    pub backoff_multiplier: f64,
    pub max_delay_ms: u64,
}

impl RetryPolicy {
    pub const DEFAULT_MAX_RETRIES: u32 = 3;
    pub const DEFAULT_DELAY_MS: u64 = 1000;
    pub const DEFAULT_BACKOFF_MULTIPLIER: f64 = 2.0;
    pub const DEFAULT_MAX_DELAY_MS: u64 = 60000;
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: Self::DEFAULT_MAX_RETRIES,
            delay_ms: Self::DEFAULT_DELAY_MS,
            backoff_multiplier: Self::DEFAULT_BACKOFF_MULTIPLIER,
            max_delay_ms: Self::DEFAULT_MAX_DELAY_MS,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub struct TaskDefinition {
    pub id: TaskId,
    pub name: String,
    pub payload: Vec<u8>,
    pub workflow_id: Option<WorkflowId>,
    pub target_node: Option<NodeId>,
    pub origin_node: Option<NodeId>,
    pub retry_policy: Option<RetryPolicy>,
    /// 有符号整数优先级。值越大越紧急。
    /// Python 层定义具体值的语义（如 LOW=-10, NORMAL=0, HIGH=10, CRITICAL=20）。
    /// Rust 仅用于调度器中的相对排序。
    #[serde(default)]
    pub priority: i32,
    pub timeout_ms: Option<u64>,
    pub attempt: u32,
    /// 任务执行租约截止时刻（epoch ms）；`0` 表示**无有效租约**。
    ///
    /// 只有一种含义：值非 0 时表示 origin 授予的租约覆盖到该时刻；值为
    /// [`TaskDefinition::NO_LEASE`] 时表示当前没有任何租约覆盖该任务（从未
    /// 租出，或已被 origin 主动放弃）——两种情况对租约检查点等效，均可立即
    /// 取回重投。写入必须经 [`TaskDefinition::relinquish_lease`] 或转发前的
    /// 授予点，判定必须经 [`TaskDefinition::lease_expired_at`]，不要各自
    /// 比较字面量。
    ///
    /// 仅独立任务在转发到远端时由 origin 设置：到期仍未收到结果即重新入队
    /// （`attempt` 递增，旧结果按 attempt fencing 拒绝）。工作流任务不使用
    /// 该字段——编排器已有自己的超时与重试语义。
    #[serde(default)]
    pub lease_deadline_ms: u64,
    /// 任务入队到调度器的时间戳（epoch ms）。用于测量调度延迟。
    #[serde(default)]
    pub enqueued_at_ms: u64,
    /// 目标节点的 Iroh endpoint 地址，用于直连任务分发。
    #[serde(default)]
    pub target_endpoint_addr: Option<String>,
    /// 源节点的 Iroh endpoint 地址，用于直连结果投递。
    #[serde(default)]
    pub origin_endpoint_addr: Option<String>,
    /// 任务放置诉求：标签选择器。
    ///
    /// 每项为 `key`（要求节点具备该标签键）或 `key=value`（要求键值精确匹配）。
    /// 语义中性——核心只做字符串比较，不解释标签的业务含义。空表示无诉求。
    #[serde(default)]
    pub tags: Vec<String>,
    /// 任务执行所需的命名配额（单维）。
    ///
    /// 执行节点按 `ActantConfig.node_quotas` 声明的容量在本地原子扣减；
    /// 耗尽时延后重入队（配额归还时提前唤醒等待者），超过
    /// `starvation_timeout_ms` 仍未获得则转 Failed。`None` 表示不占用配额。
    /// 多维资源不进核心。
    #[serde(default)]
    pub quota: Option<String>,
    /// `target_node` 是否为调用方的持久指定（如 `Task.submit_to`）。
    ///
    /// `false` 表示 `target_node` 可能是路由器的一次性放置决策：配额等节点级
    /// 资源暂不可用时任务可清空目标重新参与路由。`true` 时配额延后不会覆盖
    /// 目标（由本地 starvation 语义兜底）；worker 崩溃等基础设施故障转移仍按
    /// 全局重路由规则处理。
    #[serde(default)]
    pub pinned_target: bool,
}

impl TaskDefinition {
    /// `lease_deadline_ms` 的「无有效租约」取值。
    ///
    /// 选 0 而非 `Option<u64>`：字段随 wire 与 rkyv 归档一起编码，改类型会
    /// 同时破坏线上格式与已落盘快照，而 0 在 epoch ms 语义下本就不可能成为
    /// 合法截止时刻，作为哨兵没有歧义。
    pub const NO_LEASE: u64 = 0;

    /// 放弃当前租约，使任务可被 origin 立即取回。
    ///
    /// 调用方：origin 的失联处置（目标节点死亡，转租约重投）与租约检查点
    /// 自身（重新入队前先解除旧租约，避免下一次判定重复命中）。
    pub fn relinquish_lease(&mut self) {
        self.lease_deadline_ms = Self::NO_LEASE;
    }

    /// 租约在 `now_ms` 时刻是否已失效（可被 origin 取回重投）。
    ///
    /// 无有效租约（[`Self::NO_LEASE`]）恒为已失效——0 早于任何真实时刻。
    pub fn lease_expired_at(&self, now_ms: u64) -> bool {
        now_ms >= self.lease_deadline_ms
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActorMessage {
    pub id: MessageId,
    pub target: ActorId,
    pub method: String,
    pub payload: Vec<u8>,
    #[serde(skip)]
    pub(crate) reply_tx: Option<tokio::sync::oneshot::Sender<ActorMessageResult>>,
}

impl ActorMessage {
    pub fn new(target: ActorId, method: String, payload: Vec<u8>) -> Self {
        Self {
            id: MessageId::generate(),
            target,
            method,
            payload,
            reply_tx: None,
        }
    }

    pub fn with_reply(
        target: ActorId,
        method: String,
        payload: Vec<u8>,
    ) -> (Self, tokio::sync::oneshot::Receiver<ActorMessageResult>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            Self {
                id: MessageId::generate(),
                target,
                method,
                payload,
                reply_tx: Some(tx),
            },
            rx,
        )
    }

    pub fn take_reply_tx(&mut self) -> Option<tokio::sync::oneshot::Sender<ActorMessageResult>> {
        self.reply_tx.take()
    }
}

#[derive(Debug, Clone)]
pub enum TaskCompletion {
    Completed {
        workflow_id: WorkflowId,
        task_id: TaskId,
        task_name: String,
        result: Vec<u8>,
        target_node: Option<NodeId>,
    },
    Failed {
        workflow_id: WorkflowId,
        task_id: TaskId,
        task_name: String,
        error: String,
        target_node: Option<NodeId>,
    },
    Cancelled {
        workflow_id: WorkflowId,
        task_id: TaskId,
        task_name: String,
        target_node: Option<NodeId>,
    },
}

impl TaskCompletion {
    /// 完成类型的稳定字符串表示。
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskCompletion::Completed { .. } => "Completed",
            TaskCompletion::Failed { .. } => "Failed",
            TaskCompletion::Cancelled { .. } => "Cancelled",
        }
    }

    pub fn workflow_id(&self) -> &WorkflowId {
        match self {
            TaskCompletion::Completed { workflow_id, .. }
            | TaskCompletion::Failed { workflow_id, .. }
            | TaskCompletion::Cancelled { workflow_id, .. } => workflow_id,
        }
    }

    pub fn task_id(&self) -> &TaskId {
        match self {
            TaskCompletion::Completed { task_id, .. }
            | TaskCompletion::Failed { task_id, .. }
            | TaskCompletion::Cancelled { task_id, .. } => task_id,
        }
    }

    pub fn task_name(&self) -> &str {
        match self {
            TaskCompletion::Completed { task_name, .. }
            | TaskCompletion::Failed { task_name, .. }
            | TaskCompletion::Cancelled { task_name, .. } => task_name,
        }
    }

    pub fn target_node(&self) -> Option<&NodeId> {
        match self {
            TaskCompletion::Completed { target_node, .. }
            | TaskCompletion::Failed { target_node, .. }
            | TaskCompletion::Cancelled { target_node, .. } => target_node.as_ref(),
        }
    }

    /// 转换为网络传输用的协议结果。
    pub fn to_wire_result(&self, workflow_id: WorkflowId) -> crate::common::WireTaskResult {
        match self {
            TaskCompletion::Completed {
                task_id,
                task_name,
                result,
                ..
            } => crate::common::WireTaskResult {
                workflow_id,
                task_id: task_id.clone(),
                task_name: task_name.clone(),
                outcome: crate::common::WireTaskOutcome::Completed(result.clone()),
            },
            TaskCompletion::Failed {
                task_id,
                task_name,
                error,
                ..
            } => crate::common::WireTaskResult {
                workflow_id,
                task_id: task_id.clone(),
                task_name: task_name.clone(),
                outcome: crate::common::WireTaskOutcome::Failed(error.clone()),
            },
            TaskCompletion::Cancelled {
                task_id, task_name, ..
            } => crate::common::WireTaskResult {
                workflow_id,
                task_id: task_id.clone(),
                task_name: task_name.clone(),
                outcome: crate::common::WireTaskOutcome::Cancelled,
            },
        }
    }
}

/// Actor 间消息返回的结构化错误信封。
///
/// 携带错误种类，使调用方可以按错误种类分支处理
///（如 `NotFound`、`Timeout`），而不必依赖错误字符串前缀匹配。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorErrorEnvelope {
    pub kind: ActorErrorKind,
    pub message: String,
}

impl std::fmt::Display for ActorErrorEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl ActorErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Storage => "storage",
            Self::StorageIo => "storage_io",
            Self::Heed => "heed",
            Self::Network => "network",
            Self::Serialization => "serialization",
            Self::Postcard => "postcard",
            Self::Actor => "actor",
            Self::Workflow => "workflow",
            Self::Task => "task",
            Self::Worker => "worker",
            Self::Config => "config",
            Self::Metrics => "metrics",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::InvalidState => "invalid_state",
            Self::Replay => "replay",
            Self::Internal => "internal",
        }
    }
}

/// Actor 间错误种类。
///
/// 与 [`ActantError`] 的变体一一对应，但专门用于跨 Actor 边界序列化。
/// 使用 `snake_case` 保证 wire 格式稳定可读。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActorErrorKind {
    Storage,
    StorageIo,
    Heed,
    Network,
    Serialization,
    Postcard,
    Actor,
    Workflow,
    Task,
    Worker,
    Config,
    Metrics,
    NotFound,
    AlreadyExists,
    Timeout,
    Cancelled,
    InvalidState,
    Replay,
    Internal,
}

impl From<&crate::common::ActantError> for ActorErrorEnvelope {
    fn from(err: &crate::common::ActantError) -> Self {
        use crate::common::ActantError;
        let (kind, message) = match err {
            ActantError::Storage(m) => (ActorErrorKind::Storage, m.clone()),
            ActantError::StorageIo(e) => (ActorErrorKind::StorageIo, e.to_string()),
            ActantError::Heed(e) => (ActorErrorKind::Heed, e.to_string()),
            ActantError::Network(m) => (ActorErrorKind::Network, m.clone()),
            ActantError::Serialization(m) => (ActorErrorKind::Serialization, m.clone()),
            ActantError::Postcard(e) => (ActorErrorKind::Postcard, e.to_string()),
            ActantError::Actor(m) => (ActorErrorKind::Actor, m.clone()),
            ActantError::Workflow(m) => (ActorErrorKind::Workflow, m.clone()),
            ActantError::Task(m) => (ActorErrorKind::Task, m.clone()),
            ActantError::Worker(m) => (ActorErrorKind::Worker, m.clone()),
            ActantError::Config(m) => (ActorErrorKind::Config, m.clone()),
            ActantError::Metrics(m) => (ActorErrorKind::Metrics, m.clone()),
            ActantError::NotFound(m) => (ActorErrorKind::NotFound, m.clone()),
            ActantError::AlreadyExists(m) => (ActorErrorKind::AlreadyExists, m.clone()),
            ActantError::Timeout(m) => (ActorErrorKind::Timeout, m.clone()),
            ActantError::Cancelled(m) => (ActorErrorKind::Cancelled, m.clone()),
            ActantError::InvalidState(m) => (ActorErrorKind::InvalidState, m.clone()),
            ActantError::Replay(m) => (ActorErrorKind::Replay, m.clone()),
            ActantError::Internal(m) => (ActorErrorKind::Internal, m.clone()),
        };
        Self { kind, message }
    }
}

impl From<crate::common::ActantError> for ActorErrorEnvelope {
    fn from(err: crate::common::ActantError) -> Self {
        Self::from(&err)
    }
}

impl From<ActorErrorEnvelope> for crate::common::ActantError {
    fn from(envelope: ActorErrorEnvelope) -> Self {
        match envelope.kind {
            ActorErrorKind::Storage => Self::Storage(envelope.message),
            ActorErrorKind::StorageIo => Self::StorageIo(std::io::Error::other(envelope.message)),
            ActorErrorKind::Heed => Self::Actor(format!("heed: {}", envelope.message)),
            ActorErrorKind::Network => Self::Network(envelope.message),
            ActorErrorKind::Serialization => Self::Serialization(envelope.message),
            ActorErrorKind::Postcard => Self::Serialization(envelope.message),
            ActorErrorKind::Actor => Self::Actor(envelope.message),
            ActorErrorKind::Workflow => Self::Workflow(envelope.message),
            ActorErrorKind::Task => Self::Task(envelope.message),
            ActorErrorKind::Worker => Self::Worker(envelope.message),
            ActorErrorKind::Config => Self::Config(envelope.message),
            ActorErrorKind::Metrics => Self::Metrics(envelope.message),
            ActorErrorKind::NotFound => Self::NotFound(envelope.message),
            ActorErrorKind::AlreadyExists => Self::AlreadyExists(envelope.message),
            ActorErrorKind::Timeout => Self::Timeout(envelope.message),
            ActorErrorKind::Cancelled => Self::Cancelled(envelope.message),
            ActorErrorKind::InvalidState => Self::InvalidState(envelope.message),
            ActorErrorKind::Replay => Self::Replay(envelope.message),
            ActorErrorKind::Internal => Self::Internal(envelope.message),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorMessageResult {
    pub message_id: MessageId,
    pub payload: Vec<u8>,
    pub error: Option<ActorErrorEnvelope>,
}

/// 节点宿主平台信息，随心跳广播（节点可见性）。
///
/// 核心自动填充 `os`/`arch`/`actant_version`；`host_runtime` 由绑定层补充
/// （如 Python 层填 `platform.python_version()`），核心不感知任何语言语义。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformInfo {
    /// 操作系统（`std::env::consts::OS`）。
    pub os: String,
    /// CPU 架构（`std::env::consts::ARCH`）。
    pub arch: String,
    /// Actant crate 版本。
    pub actant_version: String,
    /// 宿主语言运行时描述（如 "CPython 3.12.1"）；纯 Rust 嵌入为 `None`。
    #[serde(default)]
    pub host_runtime: Option<String>,
}

impl PlatformInfo {
    /// 以本机平台信息构造；`host_runtime` 由调用方（绑定层）按需补充。
    pub fn detect() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            actant_version: env!("CARGO_PKG_VERSION").to_string(),
            host_runtime: None,
        }
    }
}

/// 单个标签集的序列化字节上限（key + value 长度之和）。
///
/// 标签由用户自定义且随心跳周期广播，超限直接整体丢弃并告警，防止
/// 恶意/误用配置把心跳载荷放大成带宽攻击面。
pub const NODE_LABELS_MAX_BYTES: usize = 4096;

/// 校验标签集总字节量是否在 [`NODE_LABELS_MAX_BYTES`] 之内。
pub fn node_labels_within_limit(labels: &std::collections::BTreeMap<String, String>) -> bool {
    labels.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() <= NODE_LABELS_MAX_BYTES
}

/// 运行时可变节点状态更新。
///
/// 每个字段 `None` 表示保持不变；`Some` 覆盖当前值。标签与配额随下一次
/// 心跳广播给集群，`draining` 同时让本地路由与远端候选跳过本节点。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStateUpdate {
    /// 新的标签集（整表覆盖）。
    pub labels: Option<std::collections::BTreeMap<String, String>>,
    /// 新的命名配额容量（整表覆盖；在用量按差值保留）。
    pub quotas: Option<std::collections::BTreeMap<String, u32>>,
    /// `true` 停止接纳新任务并让远端路由跳过；`false` 恢复。
    pub draining: Option<bool>,
}

/// 混合逻辑时钟时间戳（wire 协议依赖此类型）。
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(bytecheck())]
pub struct HlcTimestamp {
    wall_time: u64,
    logical: u32,
}

impl HlcTimestamp {
    pub fn zero() -> Self {
        Self {
            wall_time: 0,
            logical: 0,
        }
    }

    pub fn wall_time(&self) -> u64 {
        self.wall_time
    }

    pub fn logical(&self) -> u32 {
        self.logical
    }

    pub fn from_parts(wall_time: u64, logical: u32) -> Self {
        Self { wall_time, logical }
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.wall_time().cmp(&other.wall_time()) {
            Ordering::Equal => self.logical().cmp(&other.logical()),
            other => other,
        }
    }
}

pub struct HybridLogicalClock {
    inner: Mutex<Inner>,
    max_drift_nanos: u64,
    /// 故障注入：人为施加到物理时钟的偏移（纳秒，可正可负）。
    /// 仅用于测试台模拟时钟漂移，生产默认 0。
    injected_drift_nanos: AtomicI64,
}

struct Inner {
    last_time: u64,
    logical: u32,
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        Self::with_max_drift_ms(500)
    }

    pub fn with_max_drift_ms(max_drift_ms: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                last_time: 0,
                logical: 0,
            }),
            max_drift_nanos: max_drift_ms.saturating_mul(1_000_000),
            injected_drift_nanos: AtomicI64::new(0),
        }
    }

    /// 注入物理时钟偏移（毫秒，可正可负）。传 0 恢复正常时钟。
    ///
    /// 仅影响本时钟后续 `tick`/`merge` 使用的物理时间，用于测试台模拟
    /// 节点时钟漂移；HLC 单调性与 drift 上限语义不变。
    pub fn inject_drift_ms(&self, ms: i64) {
        self.injected_drift_nanos
            .store(ms.saturating_mul(1_000_000), AtomicOrdering::Relaxed);
    }

    /// 应用注入偏移后的物理时间（纳秒）。
    fn physical_now_with_drift(&self) -> u64 {
        let base = Self::physical_now() as i128;
        let offset = i128::from(self.injected_drift_nanos.load(AtomicOrdering::Relaxed));
        u64::try_from((base + offset).max(0)).unwrap_or(u64::MAX)
    }

    fn physical_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    pub fn tick(&self) -> HlcTimestamp {
        let mut inner = self.inner.lock();
        let physical = self.physical_now_with_drift();

        if physical > inner.last_time {
            inner.last_time = physical;
            inner.logical = 0;
        } else {
            inner.logical += 1;
        }

        HlcTimestamp {
            wall_time: inner.last_time,
            logical: inner.logical,
        }
    }

    /// 按 Kulkarni 标准 HLC merge 算法推进时钟（`max(pt_j, l.pt, m.pt)`）。
    ///
    /// 记 `pt` 为本地物理时钟、`l` 为本地上次状态、`m` 为（可能被 drift 上限
    /// 截断的）远端时间戳，新的逻辑计数按三方最大值归属决定：
    /// - 物理时钟严格主导：`c = 0`；
    /// - 仅本地历史并列主导：`c = l.c + 1`；
    /// - 仅远端并列主导：`c = m.c + 1`；
    /// - 本地与远端同时并列主导：`c = max(l.c, m.c) + 1`。
    ///
    /// 每个分支都保证输出严格大于本地此前发出的任何时间戳（单调性）：
    /// wall_time 不小于旧值；wall_time 相等时逻辑计数严格递增。
    /// 远端超出 drift 上限时按 `(cap, m.c)` 参与比较，单调性不受影响。
    pub fn merge(&self, remote: &HlcTimestamp) -> HlcTimestamp {
        let mut inner = self.inner.lock();
        let physical = self.physical_now_with_drift();
        let max_drift = self.max_drift_nanos;

        let capped_wall_time = if remote.wall_time() > physical.saturating_add(max_drift) {
            tracing::warn!(
                "HLC drift detected: remote wall_time {}ns exceeds local physical {}ns by >{}ms, capping",
                remote.wall_time(),
                physical,
                max_drift / 1_000_000,
            );
            physical.saturating_add(max_drift)
        } else {
            remote.wall_time()
        };

        let local_wall = inner.last_time;
        let local_logical = inner.logical;
        let max_wall = physical.max(local_wall).max(capped_wall_time);
        inner.last_time = max_wall;

        let eq_local = max_wall == local_wall;
        let eq_remote = max_wall == capped_wall_time;
        inner.logical = if eq_local && eq_remote {
            // 本地与远端并列主导：取双方逻辑计数的最大值再加一。
            local_logical.max(remote.logical()).saturating_add(1)
        } else if eq_local {
            // 仅本地历史主导：本地物理时钟未前进（tick 语义的 c+1）。
            local_logical.saturating_add(1)
        } else if eq_remote {
            // 仅远端并列主导。
            remote.logical().saturating_add(1)
        } else {
            // 物理时钟严格大于双方：逻辑计数清零。
            0
        };

        HlcTimestamp {
            wall_time: inner.last_time,
            logical: inner.logical,
        }
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

/// 等待点条件（持久化等待点原语）。
///
/// 纯数据结构，不解释条件语义、不触发唤醒；唤醒由 `Orchestrator` 的
/// 等待点 API 追加事件后推进状态机。定义在 common 是因为它同时属于
/// journal 的稳定事件载荷（`JournalEvent::WaitPointRegistered`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum WaitCondition {
    /// 外部信号触发（经 Signals capability 递交）。
    Signal {
        /// 信号名。与 wait_key 相互独立：wait_key 是注册表键，name 是信号语义名。
        name: String,
    },
    /// 定时到期唤醒。`deadline_ms` 为绝对 epoch 毫秒。
    Timer { deadline_ms: u64 },
    /// 挂起等待显式恢复：由 `Orchestrator::resume_suspended` 唤醒。
    ///
    /// 与 [`WaitCondition::Signal`] 的区别是**语义来源**——`Signal` 等待一个具名
    /// 业务事件，`Suspend` 等待操作员的恢复指令。二者共用
    /// `JournalEvent::SignalReceived` 作为唤醒事件，由本字段区分来源。
    Suspend,
}

/// DAG 节点定义：提交与 journal 共用的稳定结构。
///
/// `payload` 是不透明的业务载荷字节，Rust 不解析其语义。
#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub struct DagNode {
    pub task_id: TaskId,
    pub name: String,
    pub payload: Vec<u8>,
    pub retry_policy: Option<RetryPolicy>,
    pub timeout_ms: Option<u64>,
    /// 任务优先级（有符号整数）。数值越高越紧急。
    #[serde(default)]
    pub priority: i32,
    /// 任务元数据键值对，由上层添加。Rust 视为不透明数据，不解释它。
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    /// 任务放置诉求：标签选择器，随节点派发为 `TaskDefinition::tags`。
    ///
    /// 每项为 `key` 或 `key=value`，与执行节点的 `node_labels` 匹配后才允许放置。
    #[serde(default)]
    pub tags: Vec<String>,
    /// 任务执行所需的命名配额（单维），随节点派发为 `TaskDefinition::quota`。
    #[serde(default)]
    pub quota: Option<String>,
}

/// 工作流提交的不可变描述（`JournalEvent::Submitted` 载荷）。
///
/// 任意节点都能据此判断“这是什么工作流、用哪版定义、带什么入参提交”，
/// 而不依赖提交进程的内存状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowInvocation {
    pub workflow_id: WorkflowId,
    /// flow 定义名；未通过定义注册提交时为空串。
    pub name: String,
    /// flow 定义版本；`0` 表示未声明。
    pub version: u32,
    /// 调用参数的序列化字节（上层序列化，Rust 不解析）。
    pub args: Vec<u8>,
}

#[cfg(test)]
#[path = "../../../../tests/rust/unit/common/model.rs"]
mod tests;

/// HLC merge 单调性测试。
///
/// 与外置 `mod tests`（镜像单测文件）分离，专测 merge 在各分支下的
/// 全序递增不变量：任一节点发出的 HLC 时间戳必须严格大于其此前发出
/// 的所有时间戳，无论远端时间戳新旧、是否触发 drift cap。
#[cfg(test)]
mod hlc_merge_tests {
    use super::{HlcTimestamp, HybridLogicalClock};

    /// 断言 `next` 严格大于该节点此前输出的最大时间戳。
    fn assert_advances(last: &mut Option<HlcTimestamp>, next: HlcTimestamp) {
        if let Some(prev) = *last {
            assert!(
                next > prev,
                "HLC regressed: ({}, {}) <= ({}, {})",
                next.wall_time(),
                next.logical(),
                prev.wall_time(),
                prev.logical()
            );
        }
        *last = Some(next);
    }

    #[test]
    fn physical_dominant_resets_logical() {
        let clock = HybridLogicalClock::new();
        // 远端为过去时间戳：物理时钟严格主导，logical 清零（Kulkarni c=0）。
        let stale = HlcTimestamp::from_parts(1, 999);
        let t = clock.merge(&stale);
        assert_eq!(t.logical(), 0);
        assert!(t.wall_time() > 1);
    }

    #[test]
    fn tie_with_remote_takes_max_plus_one() {
        // 远端时间戳取在 1000s 后的未来且 drift 上限极大，保证测试期间
        // 本地物理时钟不可能越过它——排除真实时钟前进带来的不确定性。
        let now = HybridLogicalClock::physical_now();
        let clock = HybridLogicalClock::with_max_drift_ms(u64::MAX / 4_000_000);
        let future_wall = now + 1_000_000_000_000;
        // 本地先吸收远端时间戳，使本地历史与该远端并列。
        let remote = HlcTimestamp::from_parts(future_wall, 7);
        let t1 = clock.merge(&remote);
        assert_eq!(t1.wall_time(), future_wall);
        assert_eq!(t1.logical(), 8);

        // 与远端时间戳并列的再次 merge：max(local, remote) + 1，而非远端 +1。
        let t2 = clock.merge(&HlcTimestamp::from_parts(future_wall, 3));
        assert_eq!(t2.wall_time(), future_wall);
        assert_eq!(t2.logical(), 9);
    }

    #[test]
    fn merge_after_drift_cap_is_monotonic() {
        // 场景：远端超出 drift 上限被 cap 后，
        // 后续携带较旧时间戳的常态 merge 不得造成 (T, 0) 回退。
        let now = HybridLogicalClock::physical_now();
        let clock = HybridLogicalClock::with_max_drift_ms(500);
        let future = HlcTimestamp::from_parts(now + 10_000_000_000, 0);
        let mut last = Some(clock.merge(&future)); // cap 到 now + 500ms

        // 窗口内旧远端（gossip 常态）：全部落入本地主导分支 c+1。
        for i in 0..100u32 {
            let stale = HlcTimestamp::from_parts(now + i as u64, 0);
            let t = clock.merge(&stale);
            assert_advances(&mut last, t);
        }
    }

    #[test]
    fn merge_stale_remotes_never_regress() {
        let now = HybridLogicalClock::physical_now();
        let clock = HybridLogicalClock::with_max_drift_ms(500);
        let mut last = None;
        // 交替吸收"未来远端"与"陈旧远端"，覆盖全部四个分支。
        let inputs = [
            HlcTimestamp::from_parts(now + 100, 5),
            HlcTimestamp::from_parts(now, 0),
            HlcTimestamp::from_parts(now + 100, 50),
            HlcTimestamp::from_parts(now.saturating_sub(1_000_000), 3),
            HlcTimestamp::from_parts(now + 10_000_000_000, 1),
            HlcTimestamp::from_parts(now + 100, 1),
            HlcTimestamp::from_parts(now + 100, 200),
            HlcTimestamp::from_parts(now, 1),
        ];
        for remote in inputs {
            let t = clock.merge(&remote);
            assert_advances(&mut last, t);
        }
    }

    #[test]
    fn tick_after_merge_is_monotonic() {
        let clock = HybridLogicalClock::new();
        let mut last = None;
        for i in 0..50u32 {
            let remote_wall = HybridLogicalClock::physical_now() + (i % 3) as u64 * 1_000;
            let t = clock.merge(&HlcTimestamp::from_parts(remote_wall, i));
            assert_advances(&mut last, t);
            let t = clock.tick();
            assert_advances(&mut last, t);
        }
    }
}
