//! 内置 Capability 定义与默认 handler。
//!
//! Rust 核心只保留引擎真正接线的 capability：
//! - `Store`：数据面，由 `StoreHandler` 落到真实 LMDB 存储；
//! - `TaskLifecycle` / `WorkflowLifecycle`：观测面，事件由 Python 侧引擎发射，
//!   Rust 侧保留 codec 与 layer entry 以便统一的 effect 分发。
//!
//! `Capability` / `Handler` / `Layer` / `CapabilityRuntime` trait 与运行时定义
//! 在父模块；本模块只负责内置类型的声明与注册。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::common::{ActantError, TaskId, WorkflowId};
use crate::runtime::state::LmdbStore as StateStore;

use super::{
    erase_handler, Capability, CapabilityMeta, CapabilityRuntime, EffectKind, Handler, Layer,
};

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
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Get {
        key: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// 单事务批量写入。用于把多条记录合并为一次 commit（一次 fsync），
    /// 避免逐条写入在高吞吐路径上产生写放大。
    PutBatch {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
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

/// 内置 Capability 注册表。
pub fn builtin_capabilities() -> Vec<CapabilityMeta> {
    vec![
        Store::meta(),
        TaskLifecycle::meta(),
        WorkflowLifecycle::meta(),
    ]
}

/// 注册所有内置 capability 的序列化 codec 与空 layer。
///
/// Rust 核心不预置观测面 handler：策略与后端实现由上层注入。
/// 但每个 capability 都需要 layer entry，使 `bind_actor_system` 为其
/// spawn CapabilityActor（即使无 handler），否则 perform/emit 报
/// "not bound to actor system"。
pub fn register_defaults(runtime: &CapabilityRuntime) {
    runtime.register_codec::<Store>();
    runtime.register_codec::<TaskLifecycle>();
    runtime.register_codec::<WorkflowLifecycle>();

    runtime.ensure_layer::<Store>(Store::meta());
    runtime.ensure_layer::<TaskLifecycle>(TaskLifecycle::meta());
    runtime.ensure_layer::<WorkflowLifecycle>(WorkflowLifecycle::meta());
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
        let store = self.store.clone();
        // LMDB 事务是同步阻塞调用（`Sync` 模式下每次提交都 fsync）。直接在
        // async fn 里做会让一个 tokio worker 全程停在磁盘 IO 上，因此整体挪到
        // 阻塞线程池；用 `spawn_blocking` 而不是自建线程，才能复用池化的
        // 线程与取消语义。
        let outcome = tokio::task::spawn_blocking(move || {
            let key = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
            match req {
                StoreReq::Put { key: k, value } => store
                    .put(&key(&k), &value)
                    .map(|_| None)
                    .map_err(|e| e.to_string()),
                StoreReq::Get { key: k } => store.get(&key(&k)).map_err(|e| e.to_string()),
                StoreReq::Delete { key: k } => store
                    .delete(&key(&k))
                    .map(|_| None)
                    .map_err(|e| e.to_string()),
                StoreReq::PutBatch { entries } => {
                    let entries: Vec<(String, Vec<u8>)> =
                        entries.into_iter().map(|(k, v)| (key(&k), v)).collect();
                    store
                        .put_batch(&entries)
                        .map(|_| None)
                        .map_err(|e| e.to_string())
                }
            }
        })
        .await;

        Some(match outcome {
            Ok(result) => result,
            // 阻塞任务 panic 说明 LMDB 封装层出错，按业务失败上抛而不是让
            // capability 调用悬挂。
            Err(e) => Err(format!("store handler task failed: {e}")),
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
