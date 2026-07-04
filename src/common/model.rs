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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorMessageResult {
    pub message_id: MessageId,
    pub payload: Vec<u8>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- 访问器与构造器 ---

    #[test]
    fn test_id_as_str_returns_inner() {
        let id = TaskId::new("task-123".to_string());
        assert_eq!(id.as_str(), "task-123");
    }

    #[test]
    fn test_id_new_constructs_correctly() {
        let id = ActorId::new("actor-abc".to_string());
        assert_eq!(id.as_str(), "actor-abc");
    }

    #[test]
    fn test_id_generate_returns_nonempty_uuid() {
        let id = WorkflowId::generate();
        assert!(!id.as_str().is_empty());
        // UUID v4 格式：8-4-4-4-12
        assert_eq!(id.as_str().len(), 36);
    }

    #[test]
    fn test_id_into_inner_consumes() {
        let id = NodeId::new("node-1".to_string());
        let inner: String = id.into_inner();
        assert_eq!(inner, "node-1");
    }

    // --- From / FromStr / Display ---

    #[test]
    fn test_id_from_string() {
        let id = MessageId::from("msg-1".to_string());
        assert_eq!(id.as_str(), "msg-1");
    }

    #[test]
    fn test_id_from_str_ref() {
        let id = TaskId::from("task-x");
        assert_eq!(id.as_str(), "task-x");
    }

    #[test]
    fn test_id_fromstr_trait() {
        let id: TaskId = "task-parse".parse().unwrap();
        assert_eq!(id.as_str(), "task-parse");
    }

    #[test]
    fn test_id_display_matches_as_str() {
        let id = ActorId::new("actor-d".to_string());
        assert_eq!(format!("{}", id), id.as_str());
    }

    #[test]
    fn test_id_as_ref_str() {
        let id = NodeId::new("node-r".to_string());
        let s: &str = id.as_ref();
        assert_eq!(s, "node-r");
    }

    // --- 序列化往返（serde + postcard）---

    #[test]
    fn test_id_postcard_roundtrip() {
        let original = TaskId::new("task-postcard".to_string());
        let bytes = postcard::to_allocvec::<TaskId>(&original).unwrap();
        let decoded: TaskId = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_id_serde_json_roundtrip() {
        let original = WorkflowId::new("wf-json".to_string());
        let json = serde_json::to_string(&original).unwrap();
        // transparent 序列化为纯字符串，而非 {"0": "..."}
        assert_eq!(json, "\"wf-json\"");
        let decoded: WorkflowId = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_id_serde_transparent_format_unchanged() {
        // 关键：transparent 保证序列化格式与 pub String 时期一致（纯字符串）。
        let id = ActorId::new("actor-fmt".to_string());
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"actor-fmt\"");
    }

    // --- rkyv 往返 ---

    #[test]
    fn test_id_rkyv_roundtrip() {
        let original = NodeId::new("node-rkyv".to_string());
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&original).unwrap();
        let decoded: NodeId = rkyv::from_bytes::<NodeId, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_id_rkyv_archived_hash_eq() {
        // rkyv(derive(Hash, Eq, PartialEq)) 保证归档形式可比较。
        let a = MessageId::new("msg-1".to_string());
        let b = MessageId::new("msg-1".to_string());
        let bytes_a = rkyv::to_bytes::<rkyv::rancor::Error>(&a).unwrap();
        let bytes_b = rkyv::to_bytes::<rkyv::rancor::Error>(&b).unwrap();
        let arch_a: &rkyv::Archived<MessageId> =
            rkyv::access::<rkyv::Archived<MessageId>, rkyv::rancor::Error>(&bytes_a).unwrap();
        let arch_b: &rkyv::Archived<MessageId> =
            rkyv::access::<rkyv::Archived<MessageId>, rkyv::rancor::Error>(&bytes_b).unwrap();
        assert_eq!(arch_a, arch_b);
    }

    // --- 相等性与 Hash ---

    #[test]
    fn test_id_eq_and_hash() {
        let a = TaskId::new("t1".to_string());
        let b = TaskId::new("t1".to_string());
        let c = TaskId::new("t2".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }
}
