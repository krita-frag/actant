use rkyv::Archive;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 为 ID 新类型生成私有字段 + 统一访问器与 trait 实现。
///
/// 字段私有化后，外部代码无法直接构造任意 ID 或访问内部字符串，
/// 必须通过 `generate()`/`new()`/`From`/`FromStr` 构造，通过 `as_str()`/`Display` 读取。
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
        pub struct $name(pub(crate) String);

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
#[non_exhaustive]
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

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
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
    /// 任务入队到调度器的时间戳（epoch ms）。用于测量调度延迟。
    #[serde(default)]
    pub enqueued_at_ms: u64,
    /// 目标节点的 Iroh endpoint 地址，用于直连任务分发。
    #[serde(default)]
    pub target_endpoint_addr: Option<String>,
    /// 源节点的 Iroh endpoint 地址，用于直连结果投递。
    #[serde(default)]
    pub origin_endpoint_addr: Option<String>,
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
#[non_exhaustive]
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
    Skipped {
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
            TaskCompletion::Skipped { .. } => "Skipped",
        }
    }

    pub fn workflow_id(&self) -> &WorkflowId {
        match self {
            TaskCompletion::Completed { workflow_id, .. }
            | TaskCompletion::Failed { workflow_id, .. }
            | TaskCompletion::Cancelled { workflow_id, .. }
            | TaskCompletion::Skipped { workflow_id, .. } => workflow_id,
        }
    }

    pub fn task_id(&self) -> &TaskId {
        match self {
            TaskCompletion::Completed { task_id, .. }
            | TaskCompletion::Failed { task_id, .. }
            | TaskCompletion::Cancelled { task_id, .. }
            | TaskCompletion::Skipped { task_id, .. } => task_id,
        }
    }

    pub fn task_name(&self) -> &str {
        match self {
            TaskCompletion::Completed { task_name, .. }
            | TaskCompletion::Failed { task_name, .. }
            | TaskCompletion::Cancelled { task_name, .. }
            | TaskCompletion::Skipped { task_name, .. } => task_name,
        }
    }

    pub fn target_node(&self) -> Option<&NodeId> {
        match self {
            TaskCompletion::Completed { target_node, .. }
            | TaskCompletion::Failed { target_node, .. }
            | TaskCompletion::Cancelled { target_node, .. }
            | TaskCompletion::Skipped { target_node, .. } => target_node.as_ref(),
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
            TaskCompletion::Skipped {
                task_id, task_name, ..
            } => crate::common::WireTaskResult {
                workflow_id,
                task_id: task_id.clone(),
                task_name: task_name.clone(),
                outcome: crate::common::WireTaskOutcome::Skipped,
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

#[cfg(test)]
#[path = "../../tests/rust/unit/common/model.rs"]
mod tests;
