//! 内置 Capability 定义与默认 handler。
//!
//! 本模块定义了 actant 运行时自带的 capability marker 类型及其
//! Request/Response 消息。这些声明属于业务观测语义，按 AGENTS.md
//! 分层原则可由上层（Python 第 2 层或用户第 3 层）注入 handler。
//!
//! Rust 核心只保留：
//! - `Capability` / `Handler` / `Layer` / `CapabilityRuntime` trait 与运行时
//! - 内置 capability 的序列化 codec 注册（`register_defaults`）
//! - 基于真实子系统的 handler（`StoreHandler` / `ExecuteHandler`）

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::common::{ActantError, ActorId, MessageId, NodeId, TaskId, WorkflowId};
use crate::runtime::dispatcher::TaskDispatcher;
use crate::runtime::state::LmdbStore as StateStore;

use super::{
    erase_handler, Capability, CapabilityMeta, CapabilityRuntime, EffectKind, Handler, Layer,
};
pub struct Serialization;
impl Capability for Serialization {
    type Request = SerializationReq;
    type Response = Result<Vec<u8>, String>;
}
impl Serialization {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Serialization", EffectKind::Perform)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializationReq {
    Dump { payload: Vec<u8> },
    Load { data: Vec<u8> },
}

pub struct Transport;
impl Capability for Transport {
    type Request = TransportReq;
    type Response = Result<(), String>;
}
impl Transport {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Transport", EffectKind::Perform)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransportReq {
    SendTask { target: NodeId, payload: Vec<u8> },
    SendActorMessage { target: NodeId, payload: Vec<u8> },
    BroadcastHeartbeat { payload: Vec<u8> },
}

pub struct Store;
impl Capability for Store {
    type Request = StoreReq;
    type Response = Result<Option<Vec<u8>>, String>;
}
impl Store {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Store", EffectKind::Perform)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreReq {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct Execute;
impl Capability for Execute {
    type Request = ExecuteCtx;
    type Response = Result<ExecuteOutcome, String>;
}
impl Execute {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Execute", EffectKind::Perform)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteCtx {
    pub task_id: TaskId,
    pub workflow_id: WorkflowId,
    pub payload: Vec<u8>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteOutcome {
    pub task_id: TaskId,
    pub result_payload: Vec<u8>,
}

pub struct TaskLifecycle;
impl Capability for TaskLifecycle {
    type Request = TaskEvent;
    type Response = ();
}
impl TaskLifecycle {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("TaskLifecycle", EffectKind::Emit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskEvent {
    Started {
        task_id: TaskId,
        workflow_id: WorkflowId,
    },
    Completed {
        task_id: TaskId,
        result_payload: Vec<u8>,
    },
    Failed {
        task_id: TaskId,
        error: String,
        attempt: u32,
    },
    Retried {
        task_id: TaskId,
        next_attempt: u32,
    },
    Cancelled {
        task_id: TaskId,
    },
}

pub struct WorkflowLifecycle;
impl Capability for WorkflowLifecycle {
    type Request = WorkflowEvent;
    type Response = ();
}
impl WorkflowLifecycle {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("WorkflowLifecycle", EffectKind::Emit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowEvent {
    Submitted {
        workflow_id: WorkflowId,
    },
    Started {
        workflow_id: WorkflowId,
    },
    Completed {
        workflow_id: WorkflowId,
    },
    Failed {
        workflow_id: WorkflowId,
        error: String,
    },
    Cancelled {
        workflow_id: WorkflowId,
    },
}

pub struct NodeLifecycle;
impl Capability for NodeLifecycle {
    type Request = NodeEvent;
    type Response = ();
}
impl NodeLifecycle {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("NodeLifecycle", EffectKind::Emit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeEvent {
    Started { node_id: NodeId },
    Stopped { node_id: NodeId },
    PeerJoined { peer_id: NodeId },
    PeerLeft { peer_id: NodeId },
    Heartbeat { node_id: NodeId, timestamp_ms: u64 },
}

pub struct ActorMessaging;
impl Capability for ActorMessaging {
    type Request = ActorMessageReq;
    type Response = Result<MessageId, String>;
}
impl ActorMessaging {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("ActorMessaging", EffectKind::Perform)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorMessageReq {
    pub target: ActorId,
    pub payload: Vec<u8>,
    pub sender: Option<ActorId>,
}

pub struct ActorSupervision;
impl Capability for ActorSupervision {
    type Request = ActorFailureCtx;
    type Response = Option<SupervisionDecision>;
}
impl ActorSupervision {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("ActorSupervision", EffectKind::Ask)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorFailureCtx {
    pub actor_id: ActorId,
    pub error: String,
    pub restart_count: u32,
    pub max_restarts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SupervisionDecision {
    Restart,
    Stop,
    Resume,
}

pub struct ActorLifecycle;
impl Capability for ActorLifecycle {
    type Request = ActorEvent;
    type Response = ();
}
impl ActorLifecycle {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("ActorLifecycle", EffectKind::Emit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActorEvent {
    Spawned { actor_id: ActorId, name: String },
    Stopped { actor_id: ActorId },
    Failed { actor_id: ActorId, error: String },
    Restarted { actor_id: ActorId, attempt: u32 },
}

/// 内置 Capability 注册表。
pub fn builtin_capabilities() -> Vec<CapabilityMeta> {
    vec![
        Serialization::meta(),
        Transport::meta(),
        Store::meta(),
        Execute::meta(),
        TaskLifecycle::meta(),
        WorkflowLifecycle::meta(),
        NodeLifecycle::meta(),
        ActorMessaging::meta(),
        ActorSupervision::meta(),
        ActorLifecycle::meta(),
    ]
}

/// 注册所有内置 capability 的序列化 codec 与空 layer。
///
/// Rust 核心不预置任何 handler：策略与后端实现由上层注入。
/// 但每个 capability 都需要 layer entry，使 `bind_actor_system` 为其
/// spawn CapabilityActor（即使无 handler），否则 perform/emit 报
/// "not bound to actor system"。
pub fn register_defaults(runtime: &CapabilityRuntime) {
    runtime.register_codec::<Serialization>();
    runtime.register_codec::<Transport>();
    runtime.register_codec::<Store>();
    runtime.register_codec::<Execute>();
    runtime.register_codec::<TaskLifecycle>();
    runtime.register_codec::<WorkflowLifecycle>();
    runtime.register_codec::<NodeLifecycle>();
    runtime.register_codec::<ActorMessaging>();
    runtime.register_codec::<ActorSupervision>();
    runtime.register_codec::<ActorLifecycle>();

    runtime.ensure_layer::<Serialization>(Serialization::meta());
    runtime.ensure_layer::<Transport>(Transport::meta());
    runtime.ensure_layer::<Store>(Store::meta());
    runtime.ensure_layer::<Execute>(Execute::meta());
    runtime.ensure_layer::<TaskLifecycle>(TaskLifecycle::meta());
    runtime.ensure_layer::<WorkflowLifecycle>(WorkflowLifecycle::meta());
    runtime.ensure_layer::<NodeLifecycle>(NodeLifecycle::meta());
    runtime.ensure_layer::<ActorMessaging>(ActorMessaging::meta());
    runtime.ensure_layer::<ActorSupervision>(ActorSupervision::meta());
    runtime.ensure_layer::<ActorLifecycle>(ActorLifecycle::meta());
}

/// `Serialization` capability 的内置 handler。
///
/// `dump` 返回原始 payload，`load` 返回原始 data。
/// 这是一个直通 handler，使 `Serialization` capability 在无 Python handler 时
/// 有合理的默认行为。
#[derive(Clone)]
pub struct SerializationHandler;

#[async_trait]
impl Handler<Serialization> for SerializationHandler {
    async fn handle(&self, req: SerializationReq) -> Option<Result<Vec<u8>, String>> {
        Some(match req {
            SerializationReq::Dump { payload } => Ok(payload),
            SerializationReq::Load { data } => Ok(data),
        })
    }
}

/// 基于真实 `Store` 的 `Store` capability handler。
#[derive(Clone)]
pub(crate) struct StoreHandler {
    store: StateStore,
}

impl StoreHandler {
    pub(crate) fn new(store: StateStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Handler<Store> for StoreHandler {
    async fn handle(&self, req: StoreReq) -> Option<Result<Option<Vec<u8>>, String>> {
        let key = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
        let res = match req {
            StoreReq::Put { key: k, value } => self
                .store
                .put(&key(&k), &value)
                .map(|_| None)
                .map_err(|e| e.to_string()),
            StoreReq::Get { key: k } => self.store.get(&key(&k)).map_err(|e| e.to_string()),
            StoreReq::Delete { key: k } => self
                .store
                .delete(&key(&k))
                .map(|_| None)
                .map_err(|e| e.to_string()),
        };
        Some(res)
    }
}

/// 基于真实 `TaskDispatcher` 的 `Execute` capability handler。
#[derive(Clone)]
pub struct ExecuteHandler {
    dispatcher: Arc<dyn TaskDispatcher>,
    signing_key: Vec<u8>,
}

impl ExecuteHandler {
    pub fn new(dispatcher: Arc<dyn TaskDispatcher>, signing_key: Vec<u8>) -> Self {
        Self {
            dispatcher,
            signing_key,
        }
    }
}

#[async_trait]
impl Handler<Execute> for ExecuteHandler {
    async fn handle(&self, req: ExecuteCtx) -> Option<Result<ExecuteOutcome, String>> {
        let cancel_flag = crate::runtime::dispatcher::new_cancel_flag();
        let payload = crate::common::payload::sign(&self.signing_key, &req.payload)
            .map_err(|e| format!("payload sign: {}", e));
        let payload = match payload {
            Ok(p) => p,
            Err(e) => return Some(Err(e)),
        };
        let result = self
            .dispatcher
            .dispatch(req.task_id.as_ref(), payload, cancel_flag)
            .await;
        Some(match result {
            Ok(result_payload) => Ok(ExecuteOutcome {
                task_id: req.task_id,
                result_payload,
            }),
            Err(e) => Err(e.to_string()),
        })
    }
}

/// 注册真实 `Store` handler。
///
/// Rust 核心不在 `register_defaults` 中注册任何 `Store` handler；
/// 应在 `RuntimeBuilder` 中调用本函数，确保 `Store` capability 路由到真实 LMDB 存储。
pub(crate) fn register_store_handler(
    runtime: &CapabilityRuntime,
    store: StateStore,
) -> Result<(), ActantError> {
    runtime.register(
        Layer::<Store>::new(Store::meta()).chain_erased(erase_handler(StoreHandler::new(store))),
    )
}

/// 注册 `Serialization` 内置 handler（直通：dump/load 返回原始 payload）。
pub fn register_serialization_handler(runtime: &CapabilityRuntime) -> Result<(), ActantError> {
    runtime.register(
        Layer::<Serialization>::new(Serialization::meta())
            .chain_erased(erase_handler(SerializationHandler)),
    )
}

/// 注册真实 `Execute` handler。
///
/// Rust 核心不在 `register_defaults` 中注册任何 `Execute` handler；
/// `dispatcher` 必须已经注册了能够执行实际任务 payload 的 handler（如 Python 侧的
/// `__actant_generic__`），否则 `Execute` capability 调用会失败。
pub fn register_execute_handler(
    runtime: &CapabilityRuntime,
    dispatcher: Arc<dyn TaskDispatcher>,
    signing_key: Vec<u8>,
) -> Result<(), ActantError> {
    runtime.register(
        Layer::<Execute>::new(Execute::meta())
            .chain_erased(erase_handler(ExecuteHandler::new(dispatcher, signing_key))),
    )
}
