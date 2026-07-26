pub(crate) mod backoff;
pub(crate) mod config;
pub(crate) mod error;
pub(crate) mod model;
pub(crate) mod payload;
pub mod serialization;
pub(crate) mod wire;

/// 返回 UNIX 纪元起的当前时间（毫秒）。
pub(crate) fn epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// 基于候选节点 ID 排序列表的一致性哈希，判断本地节点是否应声明给定工作流 ID。
///
/// 此函数是声明决策逻辑的唯一真相来源，由 Rust 侧 `FailoverManager` 和
/// Python 侧 `PyRuntimeCore.should_claim_workflow` 共享。
///
/// 当 `workflow_key` 的一致性哈希映射到 `my_node_id` 时返回 `true`。
pub fn should_claim_workflow(
    workflow_key: &str,
    my_node_id: &str,
    mut candidate_ids: Vec<String>,
) -> bool {
    if candidate_ids.is_empty() {
        return false;
    }
    candidate_ids.sort();

    use std::hash::{Hash, Hasher};
    use xxhash_rust::xxh3::Xxh3;
    let mut hasher = Xxh3::new();
    workflow_key.hash(&mut hasher);
    let idx = hasher.finish() as usize % candidate_ids.len();
    candidate_ids[idx] == my_node_id
}

pub use backoff::{ExponentialBackoff, REMOTE_CALL_MAX_RETRY_DELAY};
pub use config::{
    discovery_mode, scheduler_kind, ActantConfig, ActorConfig, DiscoveryMode, FailoverConfig,
    GossipConfig, NetworkConfig, SchedulerKind, StoreConfig, SyncMode, WorkerConfig,
    WorkflowConfig,
};
pub use error::{format_error_kind, ActantError, Result};
pub use wire::TopicRoute;
// 领域类型 — 公共 API
pub use model::{
    ActorErrorEnvelope, ActorErrorKind, ActorId, ActorMessage, ActorMessageResult, ActorStatus,
    MessageId, NodeId, RetryPolicy, TaskCompletion, TaskDefinition, TaskId, WorkflowId,
};
// 协议类型 — crate 内部
pub(crate) use wire::{
    OrchestratorClaim, RemoteActorRequest, RemoteReplyAddress, Topic, WireTaskResult,
    STORE_KEY_DAG, STORE_KEY_EXEC, STORE_KEY_LEASE, STORE_KEY_PENDING, STORE_KEY_RESULT,
    TOPIC_FAILOVER, TOPIC_HEADS, TOPIC_HEARTBEAT, TOPIC_WORKFLOW_STATE_REQ,
};
// 请求-响应协议所需类型（公共 API）
pub use payload::{
    pack_group, pack_single, pack_upstream_prefix, sign, unpack_payload, verify,
    TAG_UPSTREAM_PREFIX,
};
pub use serialization::MAX_DECODE_SIZE;
pub use serialization::{decode_postcard, deserialize_rkyv_value, encode_postcard, serialize_rkyv};
pub use wire::set_wire_signing_key;
pub use wire::WireTaskOutcome;
pub use wire::{current_trace_scope, TraceContext, TraceScopeGuard};

// 仅供测试使用的内部类型 — 不属于稳定公共 API
#[doc(hidden)]
pub use wire::{
    HeadsExchange, NodeHeartbeat, RemoteActorReply, ReplyRegistry, WireDagStateUpdate,
    WireEnvelope, WireMessage, WireTaskState, WorkflowHead, WorkflowStateRequest,
    WorkflowStateResponse, TOPIC_DAG_STATE, TOPIC_WORKFLOW_STATE_RESP_PREFIX,
    WIRE_PROTOCOL_VERSION,
};
