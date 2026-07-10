//! Actor 运行时 — Rust 核心四盒之一。
//!
//! Actor 是统一执行引擎：DAG、状态机、CRDT、事件溯源都是
//! Actor 特例。本模块是 Actor 运行时的唯一入口，职责边界如下：
//!
//! | 类型 | 职责 |
//! |------|------|
//! | `Actor` trait | 用户/系统 Actor 实现的唯一抽象 |
//! | `ActorContext` | Actor 实例生命周期上下文 |
//! | `SupervisionTree` | 监督事件广播树 |
//! | `MailboxRegistry` | Actor 邮箱注册表与消息持久化 |
//! | `ActorPersistence` | Actor 状态检查点 + WAL |
//! | `ActorRuntime` | 单个 Actor 的执行循环 |
//! | `ActorSystem` | 对外 facade：spawn / send / call / stop |

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::common::{
    ActantError, ActorConfig, ActorId, ActorMessage, ActorMessageResult, ActorStatus, MessageId,
    NodeId, RemoteActorReply, RemoteActorRequest, RemoteReplyAddress, ReplyRegistry, Result,
};
use crate::runtime::event_bus::EventBus;
use crate::runtime::network::DirectRequest;
use crate::runtime::network::DirectResponse;
use crate::runtime::network::Transport;
use crate::runtime::state::{
    ActorSnapshot, CheckpointManager, HybridLogicalClock, LmdbStore, Store, WalEvent, WalReader,
    WalWriter,
};

#[async_trait]
pub trait Actor: Send + Sync + 'static {
    fn actor_type(&self) -> &str;

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult>;

    fn save_state(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn load_state(&mut self, _state: &[u8]) -> Result<()> {
        Ok(())
    }

    fn supports_state_persistence(&self) -> bool {
        false
    }

    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn on_stop(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Actor for Box<dyn Actor> {
    fn actor_type(&self) -> &str {
        (**self).actor_type()
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        (**self).handle_message(msg).await
    }

    fn save_state(&self) -> Result<Vec<u8>> {
        (**self).save_state()
    }

    fn load_state(&mut self, state: &[u8]) -> Result<()> {
        (**self).load_state(state)
    }

    fn supports_state_persistence(&self) -> bool {
        (**self).supports_state_persistence()
    }

    async fn on_start(&mut self) -> Result<()> {
        (**self).on_start().await
    }

    async fn on_stop(&mut self) -> Result<()> {
        (**self).on_stop().await
    }
}

pub struct ActorContext {
    pub actor_id: ActorId,
    pub status: ActorStatus,
}

impl ActorContext {
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            status: ActorStatus::Created,
        }
    }

    pub fn transition(&mut self, new_status: ActorStatus) -> Result<()> {
        let valid = matches!(
            (&self.status, &new_status),
            (ActorStatus::Created, ActorStatus::Running)
                | (ActorStatus::Running, ActorStatus::Failed)
                | (ActorStatus::Failed, ActorStatus::Running)
                | (ActorStatus::Running, ActorStatus::Stopped)
        );

        if !valid {
            return Err(ActantError::Actor(format!(
                "invalid state transition: {:?} -> {:?}",
                self.status, new_status
            )));
        }

        self.status = new_status;
        Ok(())
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum SupervisionEvent {
    ActorStarted { actor_id: ActorId },
    ActorFailed { actor_id: ActorId, error: String },
    ActorStopped { actor_id: ActorId },
}

pub struct SupervisionTree {
    pub(crate) event_tx: tokio::sync::broadcast::Sender<SupervisionEvent>,
}

impl SupervisionTree {
    pub fn with_capacity(capacity: usize) -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(capacity);
        Self { event_tx }
    }

    pub fn emit(&self, event: SupervisionEvent) {
        if let Err(e) = self.event_tx.send(event) {
            tracing::trace!("supervision event send failed (no receivers): {}", e);
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistentMessage {
    id: MessageId,
    target: ActorId,
    method: String,
    payload: Vec<u8>,
}

impl PersistentMessage {
    fn from_message(msg: &ActorMessage) -> Self {
        Self {
            id: msg.id.clone(),
            target: msg.target.clone(),
            method: msg.method.clone(),
            payload: msg.payload.clone(),
        }
    }

    fn to_actor_message(&self) -> ActorMessage {
        ActorMessage {
            id: self.id.clone(),
            target: self.target.clone(),
            method: self.method.clone(),
            payload: self.payload.clone(),
            reply_tx: None,
        }
    }
}

#[derive(Clone)]
struct MailboxInner {
    tx: mpsc::Sender<ActorMessage>,
}

pub struct MailboxRegistry {
    mailboxes: DashMap<ActorId, MailboxInner>,
    store: Option<Store>,
}

impl MailboxRegistry {
    pub fn new() -> Self {
        Self {
            mailboxes: DashMap::new(),
            store: None,
        }
    }

    pub fn with_store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    pub fn register(&self, actor_id: ActorId, tx: mpsc::Sender<ActorMessage>) {
        self.mailboxes.insert(actor_id, MailboxInner { tx });
    }

    pub fn unregister(&self, actor_id: &ActorId) {
        self.mailboxes.remove(actor_id);
    }

    pub async fn send(&self, target: &ActorId, msg: ActorMessage) -> Result<()> {
        let mailbox = self.mailboxes.get(target).ok_or_else(|| {
            ActantError::Actor(format!("actor {} not found in mailbox registry", target.0))
        })?;

        let msg_id = msg.id.clone();
        if let Some(ref store) = self.store {
            let persistent = PersistentMessage::from_message(&msg);
            let key = pending_key(target, &msg.id);
            let data = postcard::to_allocvec(&persistent)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            if let Err(e) = store.put(&key, &data).await {
                tracing::warn!(
                    "failed to persist pending message {} for actor {}: {}",
                    msg_id.0,
                    target.0,
                    e
                );
            }
        }

        mailbox
            .tx
            .send(msg)
            .await
            .map_err(|e| ActantError::Actor(format!("mailbox send failed: {}", e)))?;

        if let Some(ref store) = self.store {
            let key = pending_key(target, &msg_id);
            if let Err(e) = store.delete(&key).await {
                tracing::warn!(
                    "failed to delete pending message {} for actor {} after successful delivery: {}",
                    msg_id.0, target.0, e
                );
            }
        }

        self.drain_pending(target).await;
        Ok(())
    }

    pub async fn ack_message(&self, actor_id: &ActorId, msg_id: &MessageId) -> Result<()> {
        if let Some(ref store) = self.store {
            store.delete(&pending_key(actor_id, msg_id)).await?;
        }
        Ok(())
    }

    async fn drain_pending(&self, actor_id: &ActorId) {
        let Some(ref store) = self.store else {
            return;
        };

        let prefix = pending_prefix(actor_id);
        let entries = match store.scan_prefix(&prefix).await {
            Ok(e) => e,
            Err(_) => return,
        };

        let Some(inner) = self.mailboxes.get(actor_id) else {
            return;
        };

        for (key, data) in entries {
            match postcard::from_bytes::<PersistentMessage>(&data) {
                Ok(persistent) => {
                    let msg = persistent.to_actor_message();
                    match inner.tx.try_send(msg) {
                        Ok(()) => {
                            if let Err(e) = store.delete(&key).await {
                                tracing::warn!(
                                    "failed to delete drained pending message for actor {}: {}",
                                    actor_id.0,
                                    e
                                );
                            }
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => break,
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            if let Err(e) = store.delete(&key).await {
                                tracing::warn!("failed to delete closed mailbox message: {}", e);
                            }
                        }
                    }
                }
                Err(_) => {
                    if let Err(e) = store.delete(&key).await {
                        tracing::warn!("failed to delete corrupt mailbox message: {}", e);
                    }
                }
            }
        }
    }

    pub async fn recover_pending(&self, actor_id: &ActorId) -> Result<usize> {
        let Some(ref store) = self.store else {
            return Ok(0);
        };

        let prefix = pending_prefix(actor_id);
        let entries = store.scan_prefix(&prefix).await?;
        let mut count = 0;
        let mut delete_errors: Vec<String> = Vec::new();

        for (key, data) in entries {
            match postcard::from_bytes::<PersistentMessage>(&data) {
                Ok(persistent) => {
                    if let Some(inner) = self.mailboxes.get(actor_id) {
                        let msg = persistent.to_actor_message();
                        match inner.tx.try_send(msg) {
                            Ok(()) => {
                                if let Err(e) = store.delete(&key).await {
                                    delete_errors.push(format!(
                                        "delete key {:?} for actor {}: {}",
                                        key, actor_id.0, e
                                    ));
                                }
                                count += 1;
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "mailbox full for actor {}, keeping pending message {} in store",
                                    actor_id.0, persistent.id.0
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                tracing::warn!(
                                    "mailbox closed for actor {}, dropping pending message {}",
                                    actor_id.0,
                                    persistent.id.0
                                );
                                if let Err(e) = store.delete(&key).await {
                                    delete_errors.push(format!(
                                        "delete key {:?} for actor {}: {}",
                                        key, actor_id.0, e
                                    ));
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to deserialize pending message for {}: {}",
                        actor_id.0,
                        e
                    );
                    if let Err(e) = store.delete(&key).await {
                        delete_errors.push(format!(
                            "delete key {:?} for actor {}: {}",
                            key, actor_id.0, e
                        ));
                    }
                }
            }
        }

        if !delete_errors.is_empty() {
            tracing::warn!(
                "recover_pending for actor {} had {} delete errors",
                actor_id.0,
                delete_errors.len()
            );
            return Err(ActantError::Storage(format!(
                "recover_pending for actor {}: {} delete errors: {}",
                actor_id.0,
                delete_errors.len(),
                delete_errors.join("; ")
            )));
        }

        if count > 0 {
            tracing::info!(
                "recovered {} pending messages for actor {}",
                count,
                actor_id.0
            );
        }
        Ok(count)
    }
}

impl Default for MailboxRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MailboxRegistry {
    fn clone(&self) -> Self {
        Self {
            mailboxes: self.mailboxes.clone(),
            store: self.store.clone(),
        }
    }
}

fn pending_prefix(actor_id: &ActorId) -> String {
    format!("pending:{}:", actor_id.0)
}

fn pending_key(actor_id: &ActorId, msg_id: &MessageId) -> String {
    format!("pending:{}:{}", actor_id.0, msg_id.0)
}

#[derive(Clone)]
pub struct ActorPersistence {
    wal_writer: Arc<RwLock<Option<WalWriter>>>,
    checkpoint: Arc<Mutex<Option<CheckpointManager>>>,
    sequence: Arc<AtomicU64>,
    hlc: Arc<HybridLogicalClock>,
}

impl ActorPersistence {
    pub fn new() -> Self {
        Self {
            wal_writer: Arc::new(RwLock::new(None)),
            checkpoint: Arc::new(Mutex::new(None)),
            sequence: Arc::new(AtomicU64::new(0)),
            hlc: Arc::new(HybridLogicalClock::new()),
        }
    }

    pub(crate) fn with_wal(&self, wal_writer: WalWriter, store: LmdbStore) -> Self {
        let checkpoint = CheckpointManager::new(store);
        Self {
            wal_writer: Arc::new(RwLock::new(Some(wal_writer))),
            checkpoint: Arc::new(Mutex::new(Some(checkpoint))),
            sequence: Arc::new(AtomicU64::new(0)),
            hlc: Arc::new(HybridLogicalClock::new()),
        }
    }

    pub fn with_checkpoint(&self, checkpoint: CheckpointManager) -> Self {
        let cloned = self.clone();
        *cloned.checkpoint.lock() = Some(checkpoint);
        cloned
    }

    /// 加载最新检查点状态（若存在）。返回 `(state_bytes, wal_offset)`。
    pub async fn load_latest(&self, actor_id: ActorId) -> Option<(Vec<u8>, u64)> {
        let persistence = self.clone();
        tokio::task::spawn_blocking(move || {
            let checkpoint = persistence.checkpoint.lock();
            let checkpoint = checkpoint.as_ref()?;
            let snapshot = checkpoint.load_latest(&actor_id).ok()??;
            Some((snapshot.state, snapshot.wal_offset))
        })
        .await
        .ok()?
    }

    /// 重放检查点之后的全部 WAL 事件，返回该 Actor 的最终状态字节。
    ///
    /// 当前 WAL 事件负载为完整状态快照，因此只需按顺序遍历并取最后一个
    /// 匹配事件即可得到最新状态；若未来改为增量事件，可在此依次应用。
    pub async fn replay_after(&self, actor_id: ActorId, wal_offset: u64) -> Option<Vec<u8>> {
        let persistence = self.clone();
        tokio::task::spawn_blocking(move || {
            let wal_guard = persistence.wal_writer.read();
            let wal = wal_guard.as_ref()?;
            let reader = WalReader::open(wal.path().to_path_buf()).ok()?;
            let events = reader.recover_events(wal_offset).ok()?;
            let mut latest: Option<Vec<u8>> = None;
            for (_, event) in events {
                if event.actor_id == actor_id {
                    latest = Some(event.payload);
                }
            }
            latest
        })
        .await
        .ok()?
    }

    /// 持久化 Actor 状态：先写检查点，再追加 WAL 事件。
    pub async fn persist(&self, actor_id: ActorId, actor_type: String, state: Vec<u8>) {
        if state.is_empty() {
            return;
        }

        let persistence = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let seq = persistence.sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let hlc_ts = persistence.hlc.tick();

            if let Some(ref checkpoint) = *persistence.checkpoint.lock() {
                let wal_offset = {
                    let wal_guard = persistence.wal_writer.read();
                    wal_guard.as_ref().map(|w| w.current_offset()).unwrap_or(0)
                };

                let snapshot = ActorSnapshot {
                    actor_id: actor_id.clone(),
                    actor_type: actor_type.clone(),
                    state: state.clone(),
                    timestamp: hlc_ts,
                    sequence: seq,
                    wal_offset,
                };

                if let Err(e) = checkpoint.save(&snapshot) {
                    tracing::warn!("checkpoint save failed for {}: {}", actor_id.0, e);
                }
            }

            let mut wal_guard = persistence.wal_writer.write();
            if let Some(ref mut wal) = *wal_guard {
                let event = WalEvent {
                    actor_id: actor_id.clone(),
                    sequence: seq,
                    payload: state,
                };
                if let Err(e) = wal.append_event(hlc_ts, &event) {
                    tracing::warn!("WAL append failed for {}: {}", actor_id.0, e);
                }
            }
        })
        .await;

        if let Err(e) = result {
            tracing::error!("actor persistence task panicked: {}", e);
        }
    }

    pub fn start_compaction(
        self: Arc<Self>,
        interval_secs: u64,
        checkpoint_retention_count: usize,
    ) -> watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let persistence = self.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            let checkpoint_opt = persistence.checkpoint.lock().as_ref().cloned();
                            let mut wal_guard = persistence.wal_writer.write();
                            if let (Some(ref mut writer), Some(checkpoint)) = (&mut *wal_guard, checkpoint_opt) {
                                let wal_path = writer.path().to_path_buf();
                                let current_offset = writer.current_offset();
                                let compactor = crate::runtime::state::WalCompactor::new(
                                    wal_path, current_offset, checkpoint, checkpoint_retention_count,
                                );
                                if let Err(e) = compactor.compact() {
                                    tracing::warn!("WAL compaction failed: {}", e);
                                    return;
                                }
                                if let Err(e) = writer.sync() {
                                    tracing::warn!("WAL sync after compaction failed: {}", e);
                                    return;
                                }
                                let path = writer.path().to_path_buf();
                                if let Ok(new_writer) = WalWriter::open(&path) {
                                    *writer = new_writer;
                                }
                            }
                        }).await;
                        if let Err(e) = result {
                            tracing::warn!("WAL compaction task panicked: {}", e);
                        }
                    }
                    Ok(()) = cancel_rx.changed() => break,
                    else => break,
                }
            }
        });

        cancel_tx
    }
}

impl Default for ActorPersistence {
    fn default() -> Self {
        Self::new()
    }
}

struct ActorEntry {
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct RunningActor {
    actor_id: ActorId,
    actor: Box<dyn Actor>,
    ctx: ActorContext,
    rx: mpsc::Receiver<ActorMessage>,
    cancel_rx: watch::Receiver<bool>,
    supervision: Arc<SupervisionTree>,
    event_bus: Option<EventBus>,
    registry: MailboxRegistry,
    persistence: Arc<ActorPersistence>,
}

impl RunningActor {
    fn emit_supervision(&self, event: SupervisionEvent) {
        self.supervision.emit(event.clone());
        if let Some(ref bus) = self.event_bus {
            let bus = bus.clone();
            let ev = event.clone();
            tokio::spawn(async move {
                bus.publish(crate::runtime::event_bus::BusEvent::SupervisionEvent(ev))
                    .await;
            });
        }
    }

    async fn run(mut self) {
        if let Err(e) = self.ctx.transition(ActorStatus::Running) {
            tracing::error!(
                "actor {} failed to transition to Running: {}",
                self.actor_id.0,
                e
            );
            return;
        }

        loop {
            let mut msg = tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(m) => m,
                        None => break,
                    }
                }
                Ok(()) = self.cancel_rx.changed() => {
                    self.handle_stop().await;
                    break;
                }
                else => break,
            };

            let msg_id = msg.id.clone();
            let reply_tx = msg.take_reply_tx();

            let future = self.actor.handle_message(msg);
            let handle_t0 = std::time::Instant::now();
            let result =
                futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(future)).await;
            crate::metrics::observe_actor_handle_message_ms(handle_t0.elapsed().as_millis() as u64);

            match result {
                Ok(Ok(response)) => {
                    if let Some(tx) = reply_tx {
                        if tx.send(response).is_err() {
                            tracing::warn!(
                                actor = %self.actor_id.0,
                                msg_id = %msg_id,
                                "reply channel closed before sending response"
                            );
                        }
                    }
                    if let Err(e) = self.registry.ack_message(&self.actor_id, &msg_id).await {
                        tracing::warn!(
                            actor = %self.actor_id.0,
                            msg_id = %msg_id,
                            error = %e,
                            "failed to ack message"
                        );
                    }
                }
                Ok(Err(e)) => {
                    if let Err(ack_err) = self.registry.ack_message(&self.actor_id, &msg_id).await {
                        tracing::warn!(
                            actor = %self.actor_id.0,
                            msg_id = %msg_id,
                            error = %ack_err,
                            "failed to ack message after error"
                        );
                    }
                    self.handle_message_error(msg_id, reply_tx, e.to_string());
                }
                Err(_panic_payload) => {
                    if let Err(ack_err) = self.registry.ack_message(&self.actor_id, &msg_id).await {
                        tracing::warn!(
                            actor = %self.actor_id.0,
                            msg_id = %msg_id,
                            error = %ack_err,
                            "failed to ack message after panic"
                        );
                    }
                    self.handle_message_panic(msg_id, reply_tx);
                }
            }

            self.persist_state().await;
        }

        self.cleanup();
    }

    fn handle_message_error(
        &mut self,
        msg_id: MessageId,
        reply_tx: Option<oneshot::Sender<ActorMessageResult>>,
        error: String,
    ) {
        tracing::error!("actor {} handle_message error: {}", self.actor_id.0, error);
        self.emit_supervision(SupervisionEvent::ActorFailed {
            actor_id: self.actor_id.clone(),
            error: error.clone(),
        });
        crate::metrics::inc_actors_failed();
        crate::metrics::dec_active_actors();

        if let Some(tx) = reply_tx {
            let _ = tx.send(ActorMessageResult {
                message_id: msg_id,
                payload: vec![],
                error: Some(error),
            });
        }
    }

    fn handle_message_panic(
        &mut self,
        msg_id: MessageId,
        reply_tx: Option<oneshot::Sender<ActorMessageResult>>,
    ) {
        tracing::error!("actor {} panicked in handle_message", self.actor_id.0);
        self.emit_supervision(SupervisionEvent::ActorFailed {
            actor_id: self.actor_id.clone(),
            error: "actor panicked".to_string(),
        });
        crate::metrics::inc_actors_failed();
        crate::metrics::dec_active_actors();

        if let Some(tx) = reply_tx {
            let _ = tx.send(ActorMessageResult {
                message_id: msg_id,
                payload: vec![],
                error: Some("actor panicked".to_string()),
            });
        }
    }

    async fn handle_stop(&mut self) {
        let stop_future = self.actor.on_stop();
        let result =
            futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(stop_future)).await;
        match result {
            Err(_) => {
                tracing::warn!("actor {} panicked in on_stop", self.actor_id.0);
            }
            Ok(Err(e)) => {
                tracing::warn!("actor {} on_stop error: {}", self.actor_id.0, e);
            }
            Ok(Ok(())) => {}
        }
        if let Err(e) = self.ctx.transition(ActorStatus::Stopped) {
            tracing::warn!(
                "actor {} failed to transition to Stopped: {}",
                self.actor_id.0,
                e
            );
        }
    }

    async fn persist_state(&self) {
        if !self.actor.supports_state_persistence() {
            return;
        }

        let save_t0 = std::time::Instant::now();
        let save_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.actor.save_state()));
        crate::metrics::observe_actor_save_state_ms(save_t0.elapsed().as_millis() as u64);

        let state = match save_result {
            Err(_) => {
                tracing::warn!("actor {} panicked in save_state", self.actor_id.0);
                return;
            }
            Ok(Err(e)) => {
                tracing::warn!("actor {} save_state error: {}", self.actor_id.0, e);
                return;
            }
            Ok(Ok(state)) => state,
        };

        self.persistence
            .persist(
                self.actor_id.clone(),
                self.actor.actor_type().to_string(),
                state,
            )
            .await;
    }

    fn cleanup(self) {
        self.registry.unregister(&self.actor_id);
        crate::metrics::inc_actors_stopped();
        crate::metrics::dec_active_actors();
    }
}

pub struct ActorSystem {
    actors: Arc<DashMap<ActorId, ActorEntry>>,
    actor_types: DashMap<ActorId, String>,
    registry: MailboxRegistry,
    supervision: Arc<SupervisionTree>,
    persistence: Arc<ActorPersistence>,
    network: Option<Arc<dyn Transport>>,
    node_id: Option<NodeId>,
    config: ActorConfig,
    compaction_cancel: Arc<Mutex<Option<watch::Sender<bool>>>>,
    pending_replies: Arc<ReplyRegistry>,
    event_bus: Option<EventBus>,
}

impl ActorSystem {
    pub fn new() -> Self {
        Self {
            actors: Arc::new(DashMap::new()),
            actor_types: DashMap::new(),
            registry: MailboxRegistry::new(),
            supervision: Arc::new(SupervisionTree::with_capacity(
                ActorConfig::default().supervision_event_capacity,
            )),
            persistence: Arc::new(ActorPersistence::new()),
            network: None,
            node_id: None,
            config: ActorConfig::default(),
            compaction_cancel: Arc::new(Mutex::new(None)),
            pending_replies: Arc::new(ReplyRegistry::new()),
            event_bus: None,
        }
    }

    pub fn with_config(mut self, config: ActorConfig) -> Self {
        if config.supervision_event_capacity != self.config.supervision_event_capacity {
            self.supervision = Arc::new(SupervisionTree::with_capacity(
                config.supervision_event_capacity,
            ));
        }
        self.config = config;
        self
    }

    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn with_network(mut self, network: Arc<dyn Transport>) -> Self {
        self.network = Some(network);
        self
    }

    pub fn with_event_bus(mut self, bus: EventBus) -> Self {
        self.event_bus = Some(bus);
        self
    }

    pub fn with_checkpoint(mut self, checkpoint: CheckpointManager) -> Self {
        self.persistence = Arc::new(self.persistence.with_checkpoint(checkpoint));
        self
    }

    pub(crate) fn with_wal(mut self, wal_writer: WalWriter, store: LmdbStore) -> Self {
        self.persistence = Arc::new(self.persistence.with_wal(wal_writer, store.clone()));
        self.registry = self.registry.with_store(Store::new(store));
        self
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    pub async fn spawn(&self, actor_id: ActorId, actor: impl Actor) -> Result<()> {
        self.spawn_boxed(actor_id, Box::new(actor)).await
    }

    async fn spawn_boxed(&self, actor_id: ActorId, mut actor: Box<dyn Actor>) -> Result<()> {
        if self.actors.contains_key(&actor_id) {
            return Err(ActantError::AlreadyExists(format!(
                "actor {} already exists",
                actor_id.as_str()
            )));
        }

        let actor_type = actor.actor_type().to_string();
        self.actor_types.insert(actor_id.clone(), actor_type);

        let (tx, rx) = mpsc::channel::<ActorMessage>(self.config.mailbox_capacity);
        self.registry.register(actor_id.clone(), tx);

        if let Err(e) = self.registry.recover_pending(&actor_id).await {
            tracing::warn!(
                "failed to recover pending messages for {}: {}",
                actor_id.as_str(),
                e
            );
        }

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let ctx = ActorContext::new(actor_id.clone());

        self.restore_actor_state(&actor_id, &mut actor).await;
        self.run_actor_on_start(&actor_id, &mut actor).await?;

        self.supervision.emit(SupervisionEvent::ActorStarted {
            actor_id: actor_id.clone(),
        });

        crate::metrics::inc_actors_spawned();
        crate::metrics::inc_active_actors();

        let runtime = RunningActor {
            actor_id: actor_id.clone(),
            actor,
            ctx,
            rx,
            cancel_rx,
            supervision: self.supervision.clone(),
            event_bus: self.event_bus.clone(),
            registry: self.registry.clone(),
            persistence: self.persistence.clone(),
        };

        let task = tokio::spawn(async move {
            runtime.run().await;
        });

        self.actors.insert(
            actor_id,
            ActorEntry {
                cancel: cancel_tx,
                task,
            },
        );

        Ok(())
    }

    async fn restore_actor_state(&self, actor_id: &ActorId, actor: &mut Box<dyn Actor>) {
        let Some((state, wal_offset)) = self.persistence.load_latest(actor_id.clone()).await else {
            return;
        };

        let load_t0 = std::time::Instant::now();
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.load_state(&state)));
        crate::metrics::observe_actor_load_state_ms(load_t0.elapsed().as_millis() as u64);
        match result {
            Err(_) => {
                tracing::warn!("actor {} panicked in load_state", actor_id.as_str());
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "failed to load actor state for {}: {}",
                    actor_id.as_str(),
                    e
                );
            }
            Ok(Ok(())) => {}
        }

        let Some(latest_state) = self
            .persistence
            .replay_after(actor_id.clone(), wal_offset)
            .await
        else {
            return;
        };
        let replay_t0 = std::time::Instant::now();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            actor.load_state(&latest_state)
        }));
        crate::metrics::observe_actor_load_state_ms(replay_t0.elapsed().as_millis() as u64);
        match result {
            Err(_) => {
                tracing::warn!(
                    "actor {} panicked in WAL replay load_state",
                    actor_id.as_str()
                );
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "WAL replay load_state failed for {}: {}",
                    actor_id.as_str(),
                    e
                );
            }
            Ok(Ok(())) => {}
        }
    }

    async fn run_actor_on_start(
        &self,
        actor_id: &ActorId,
        actor: &mut Box<dyn Actor>,
    ) -> Result<()> {
        let start_future = actor.on_start();
        let catch_result =
            futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(start_future)).await;
        match catch_result {
            Err(_) => {
                tracing::warn!("actor {} panicked in on_start", actor_id.as_str());
                Err(ActantError::Actor(format!(
                    "actor {} panicked in on_start",
                    actor_id.as_str()
                )))
            }
            Ok(Err(e)) => {
                tracing::warn!("actor on_start failed for {}: {}", actor_id.as_str(), e);
                Err(ActantError::Actor(format!(
                    "actor {} on_start failed: {}",
                    actor_id.as_str(),
                    e
                )))
            }
            Ok(Ok(())) => Ok(()),
        }
    }

    pub async fn send(&self, actor_id: &ActorId, msg: ActorMessage) -> Result<()> {
        self.registry.send(actor_id, msg).await
    }

    pub async fn call(
        &self,
        actor_id: &ActorId,
        method: &str,
        payload: Vec<u8>,
    ) -> Result<ActorMessageResult> {
        let (msg, rx) = ActorMessage::with_reply(actor_id.clone(), method.to_string(), payload);
        self.registry.send(actor_id, msg).await?;
        rx.await
            .map_err(|e| ActantError::Actor(format!("call failed: {}", e)))
    }

    pub async fn call_remote(
        &self,
        target_node: &NodeId,
        target: ActorId,
        method: String,
        payload: Vec<u8>,
    ) -> Result<ActorMessageResult> {
        let network = self
            .network
            .as_ref()
            .ok_or_else(|| ActantError::Actor("network not configured".into()))?;
        let local_node_id = self
            .node_id
            .as_ref()
            .ok_or_else(|| ActantError::Actor("node_id not configured".into()))?;

        let max_retries = self.config.remote_call_max_retries;
        let base_retry_delay_ms = self.config.remote_call_retry_delay_ms;
        const MAX_RETRY_DELAY_MS: u64 = 30_000;

        let mut attempt = 0u32;
        loop {
            let reply_addr = RemoteReplyAddress {
                node_id: local_node_id.clone(),
                correlation_id: MessageId::generate(),
            };

            let direct_req = DirectRequest::ActorCall {
                target: target.clone(),
                method: method.clone(),
                payload: payload.clone(),
                reply_to: reply_addr,
            };

            match network
                .send_direct_request(target_node.as_str(), direct_req)
                .await
            {
                Ok(DirectResponse::ActorCallResult { result }) => {
                    if result.is_empty() {
                        return Err(ActantError::Actor(
                            "remote actor call returned empty result".into(),
                        ));
                    }
                    let msg_result: ActorMessageResult = crate::common::decode_postcard(&result)?;
                    return Ok(msg_result);
                }
                Ok(_) => {
                    return Err(ActantError::Actor(
                        "remote actor call returned unexpected response type".into(),
                    ));
                }
                Err(ActantError::Timeout(_)) => {
                    if attempt >= max_retries {
                        return Err(ActantError::Actor("remote call timed out".into()));
                    }
                    let delay_ms = std::cmp::min(
                        base_retry_delay_ms.saturating_mul(2u64.saturating_pow(attempt)),
                        MAX_RETRY_DELAY_MS,
                    );
                    attempt += 1;
                    tracing::debug!(
                        "remote call to {}/{} timed out, retry {}/{} after {}ms",
                        target_node.as_str(),
                        target.as_str(),
                        attempt,
                        max_retries,
                        delay_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn deliver_reply(&self, reply: RemoteActorReply) {
        if let Some((_, tx)) = self.pending_replies.remove(&reply.correlation_id) {
            if tx.send(reply.result).is_err() {
                tracing::warn!(
                    correlation_id = %reply.correlation_id,
                    "deliver_reply: reply channel already closed"
                );
            }
        }
    }

    pub async fn handle_remote_request(&self, req: RemoteActorRequest) {
        let target = req.target.clone();
        let reply_to = req.reply_to.clone();

        let (actor_msg, reply_rx) = if reply_to.is_some() {
            let (m, rx) = ActorMessage::with_reply(req.target, req.method, req.payload);
            (m, Some(rx))
        } else {
            (ActorMessage::new(req.target, req.method, req.payload), None)
        };

        if let Err(e) = self.registry.send(&target, actor_msg).await {
            tracing::warn!(
                "failed to deliver remote actor message to {}: {}",
                target.as_str(),
                e
            );
        }

        if let (Some(rx), Some(reply_addr)) = (reply_rx, reply_to) {
            let network = self.network.clone();
            tokio::spawn(async move {
                if let Ok(result) = rx.await {
                    let reply = RemoteActorReply {
                        correlation_id: reply_addr.correlation_id,
                        result,
                    };
                    if let Some(ref network) = network {
                        let reply_topic = crate::common::Topic::actor_reply(&reply_addr.node_id);
                        let msg = crate::common::WireMessage::RemoteActorReply(reply);
                        if let Ok(data) =
                            postcard::to_allocvec(&crate::common::WireEnvelope::wrap(msg))
                        {
                            if let Err(e) = network.broadcast(reply_topic.as_str(), data).await {
                                tracing::warn!(
                                    "actor reply broadcast to {:?} failed: {}",
                                    reply_addr.node_id,
                                    e
                                );
                            }
                        }
                    }
                }
            });
        }
    }

    pub async fn stop(&self, actor_id: &ActorId) -> Result<()> {
        if let Some(entry) = self.actors.get(actor_id) {
            let _ = entry.cancel.send(true);
            drop(entry);
        }
        if let Some((_, entry)) = self.actors.remove(actor_id) {
            let _ = entry.task.await;
        }
        self.cleanup_actor(actor_id);
        Ok(())
    }

    pub fn kill(&self, actor_id: &ActorId) -> Result<()> {
        if let Some((_, entry)) = self.actors.remove(actor_id) {
            entry.task.abort();
        }
        self.cleanup_actor(actor_id);
        Ok(())
    }

    pub async fn stop_timeout(
        &self,
        actor_id: &ActorId,
        timeout: std::time::Duration,
    ) -> Result<()> {
        if let Some(entry) = self.actors.get(actor_id) {
            let _ = entry.cancel.send(true);
            drop(entry);
        }

        if let Some((_, entry)) = self.actors.remove(actor_id) {
            let result = tokio::time::timeout(timeout, entry.task).await;
            if result.is_err() {
                tracing::warn!("actor {} stop timed out, aborting", actor_id.as_str());
            }
        }

        self.cleanup_actor(actor_id);
        Ok(())
    }

    fn cleanup_actor(&self, actor_id: &ActorId) {
        self.registry.unregister(actor_id);
        self.supervision.emit(SupervisionEvent::ActorStopped {
            actor_id: actor_id.clone(),
        });
    }

    pub fn list_actors(&self) -> Vec<ActorId> {
        self.actors.iter().map(|r| r.key().clone()).collect()
    }

    pub fn actor_status(&self, actor_id: &ActorId) -> Option<ActorStatus> {
        self.actors.get(actor_id).map(|e| {
            if e.task.is_finished() {
                ActorStatus::Stopped
            } else {
                ActorStatus::Running
            }
        })
    }

    pub fn stop_compaction_task(&self) {
        let mut cancel = self.compaction_cancel.lock();
        if let Some(tx) = cancel.take() {
            let _ = tx.send(true);
        }
    }

    pub fn start_compaction_task(&self) {
        let cancel_tx = self.persistence.clone().start_compaction(
            self.config.wal_compaction_interval_secs,
            self.config.checkpoint_retention_count,
        );
        let mut cancel = self.compaction_cancel.lock();
        *cancel = Some(cancel_tx);
    }
}

impl Default for ActorSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;

    struct EchoActor {
        received: Arc<StdMutex<Vec<String>>>,
        fail_method: Option<String>,
        panic_method: Option<String>,
    }

    impl EchoActor {
        fn new() -> (Self, Arc<StdMutex<Vec<String>>>) {
            let received = Arc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    received: received.clone(),
                    fail_method: None,
                    panic_method: None,
                },
                received,
            )
        }

        fn with_fail(method: &str) -> (Self, Arc<StdMutex<Vec<String>>>) {
            let (mut actor, received) = Self::new();
            actor.fail_method = Some(method.to_string());
            (actor, received)
        }

        fn with_panic(method: &str) -> (Self, Arc<StdMutex<Vec<String>>>) {
            let (mut actor, received) = Self::new();
            actor.panic_method = Some(method.to_string());
            (actor, received)
        }
    }

    #[async_trait]
    impl Actor for EchoActor {
        fn actor_type(&self) -> &str {
            "echo"
        }

        async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
            self.received.lock().unwrap().push(msg.method.clone());

            if self.panic_method.as_deref() == Some(&msg.method) {
                panic!("test panic in handle_message");
            }
            if self.fail_method.as_deref() == Some(&msg.method) {
                return Err(ActantError::Actor("intentional failure".into()));
            }

            Ok(ActorMessageResult {
                message_id: msg.id,
                payload: msg.payload.clone(),
                error: None,
            })
        }
    }

    #[tokio::test]
    async fn new_context_starts_in_created_state() {
        let ctx = ActorContext::new(ActorId("a1".into()));
        assert_eq!(ctx.status, ActorStatus::Created);
        assert_eq!(ctx.actor_id.0, "a1");
    }

    #[tokio::test]
    async fn spawn_and_send_delivers_message_to_actor() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("echo-1");
        let (actor, received) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let msg = ActorMessage::new(actor_id.clone(), "ping".into(), b"data".to_vec());
        system.send(&actor_id, msg).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let received = received.lock().unwrap();
        assert_eq!(*received, vec!["ping".to_string()]);
    }

    #[tokio::test]
    async fn spawn_duplicate_actor_returns_already_exists() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("dup");
        let (actor, _) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let (actor2, _) = EchoActor::new();
        let err = system.spawn(actor_id.clone(), actor2).await.unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn send_to_unknown_actor_returns_error() {
        let system = ActorSystem::new();
        let target = ActorId::from("ghost");
        let msg = ActorMessage::new(target.clone(), "ping".into(), vec![]);
        let err = system.send(&target, msg).await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn call_returns_reply_from_actor() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("echo-2");
        let (actor, _) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let result = system
            .call(&actor_id, "echo", b"hello".to_vec())
            .await
            .unwrap();

        assert_eq!(result.payload, b"hello");
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn call_returns_error_when_actor_fails() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("fail-1");
        let (actor, _) = EchoActor::with_fail("boom");
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let result = system.call(&actor_id, "boom", vec![]).await.unwrap();
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("intentional failure"));
    }

    #[tokio::test]
    async fn call_returns_error_when_actor_panics() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("panic-1");
        let (actor, _) = EchoActor::with_panic("explode");
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let result = system
            .call(&actor_id, "explode", vec![])
            .await
            .unwrap();
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("panicked"));
    }

    #[tokio::test]
    async fn actor_panic_does_not_crash_other_actors() {
        let system = ActorSystem::new();

        let panic_id = ActorId::from("panic-2");
        let (panic_actor, _) = EchoActor::with_panic("boom");
        system.spawn(panic_id.clone(), panic_actor).await.unwrap();

        let healthy_id = ActorId::from("healthy");
        let (healthy_actor, healthy_received) = EchoActor::new();
        system
            .spawn(healthy_id.clone(), healthy_actor)
            .await
            .unwrap();

        let _ = system.call(&panic_id, "boom", vec![]).await;

        let result = system
            .call(&healthy_id, "ping", b"data".to_vec())
            .await
            .unwrap();
        assert_eq!(result.payload, b"data");

        let received = healthy_received.lock().unwrap();
        assert!(*received == vec!["ping".to_string()]);
    }

    #[tokio::test]
    async fn stop_terminates_actor_gracefully() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("stop-1");
        let (actor, _) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        assert_eq!(system.actor_status(&actor_id), Some(ActorStatus::Running));

        system.stop(&actor_id).await.unwrap();

        let status = system.actor_status(&actor_id);
        assert!(
            status.is_none() || status == Some(ActorStatus::Stopped),
            "expected None or Stopped, got {:?}",
            status
        );

        let msg = ActorMessage::new(actor_id.clone(), "ping".into(), vec![]);
        assert!(system.send(&actor_id, msg).await.is_err());
    }

    #[tokio::test]
    async fn kill_aborts_actor_immediately() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("kill-1");
        let (actor, _) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        system.kill(&actor_id).unwrap();

        let msg = ActorMessage::new(actor_id.clone(), "ping".into(), vec![]);
        assert!(system.send(&actor_id, msg).await.is_err());
    }

    #[tokio::test]
    async fn list_actors_returns_all_spawned_actors() {
        let system = ActorSystem::new();
        let id1 = ActorId::from("list-1");
        let id2 = ActorId::from("list-2");
        let (a1, _) = EchoActor::new();
        let (a2, _) = EchoActor::new();
        system.spawn(id1.clone(), a1).await.unwrap();
        system.spawn(id2.clone(), a2).await.unwrap();

        let mut actors = system.list_actors();
        actors.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        assert_eq!(actors.len(), 2);
        assert_eq!(actors[0].as_str(), "list-1");
        assert_eq!(actors[1].as_str(), "list-2");
    }

    #[tokio::test]
    async fn actor_status_none_for_unknown_actor() {
        let system = ActorSystem::new();
        assert_eq!(system.actor_status(&ActorId::from("ghost")), None);
    }

    #[tokio::test]
    async fn stop_unknown_actor_is_noop() {
        let system = ActorSystem::new();
        system.stop(&ActorId::from("ghost")).await.unwrap();
    }

    #[tokio::test]
    async fn spawn_emits_actor_started_supervision_event() {
        let system = ActorSystem::new();
        let mut rx = system.supervision.event_tx.subscribe();

        let actor_id = ActorId::from("sup-1");
        let (actor, _) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let event = rx.try_recv().expect("should receive ActorStarted event");
        match event {
            SupervisionEvent::ActorStarted { actor_id: id } => {
                assert_eq!(id.as_str(), "sup-1");
            }
            _ => panic!("expected ActorStarted, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn stop_emits_actor_stopped_supervision_event() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("sup-2");
        let (actor, _) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let mut rx = system.supervision.event_tx.subscribe();
        system.stop(&actor_id).await.unwrap();

        let event = rx.try_recv().expect("should receive ActorStopped event");
        assert!(matches!(event, SupervisionEvent::ActorStopped { .. }));
    }

    #[tokio::test]
    async fn actor_failure_emits_actor_failed_supervision_event() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("sup-3");
        let (actor, _) = EchoActor::with_fail("boom");
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let mut rx = system.supervision.event_tx.subscribe();
        let _ = system.call(&actor_id, "boom", vec![]).await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let event = rx.try_recv().expect("should receive ActorFailed event");
        match event {
            SupervisionEvent::ActorFailed {
                actor_id: id,
                error,
            } => {
                assert_eq!(id.as_str(), "sup-3");
                assert!(error.contains("intentional failure"));
            }
            _ => panic!("expected ActorFailed, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn multiple_actors_process_messages_concurrently() {
        let system = ActorSystem::new();

        let id1 = ActorId::from("conc-1");
        let id2 = ActorId::from("conc-2");
        let (a1, r1) = EchoActor::new();
        let (a2, r2) = EchoActor::new();
        system.spawn(id1.clone(), a1).await.unwrap();
        system.spawn(id2.clone(), a2).await.unwrap();

        let s1 = system.send(&id1, ActorMessage::new(id1.clone(), "m1".into(), vec![]));
        let s2 = system.send(&id2, ActorMessage::new(id2.clone(), "m2".into(), vec![]));
        let (send_r1, send_r2) = tokio::join!(s1, s2);
        send_r1.unwrap();
        send_r2.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(*r1.lock().unwrap(), vec!["m1".to_string()]);
        assert_eq!(*r2.lock().unwrap(), vec!["m2".to_string()]);
    }

    #[tokio::test]
    async fn call_remote_without_network_returns_error() {
        let system = ActorSystem::new();
        let target_node = NodeId::from("remote");
        let result = system
            .call_remote(&target_node, ActorId::from("a"), "method".into(), vec![])
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("network not configured"));
    }

    #[tokio::test]
    async fn with_config_sets_mailbox_capacity() {
        let config = ActorConfig {
            mailbox_capacity: 16,
            ..Default::default()
        };
        let system = ActorSystem::new().with_config(config);
        assert_eq!(system.config.mailbox_capacity, 16);
    }

    #[tokio::test]
    async fn with_node_id_stores_node_id() {
        let system = ActorSystem::new().with_node_id(NodeId::from("node-1"));
        assert_eq!(system.node_id().unwrap().as_str(), "node-1");
    }

    #[test]
    fn supervision_delivers_to_subscriber() {
        let tree = SupervisionTree::with_capacity(16);
        let mut rx = tree.event_tx.subscribe();

        tree.emit(SupervisionEvent::ActorStarted {
            actor_id: ActorId("a1".into()),
        });

        let event = rx.try_recv().expect("should receive emitted event");
        match event {
            SupervisionEvent::ActorStarted { actor_id } => {
                assert_eq!(actor_id.0, "a1");
            }
            _ => panic!("expected ActorStarted, got {:?}", event),
        }
    }

    #[test]
    fn supervision_without_subscribers_is_silent_noop() {
        let tree = SupervisionTree::with_capacity(16);
        tree.emit(SupervisionEvent::ActorStarted {
            actor_id: ActorId("a1".into()),
        });
    }

    #[tokio::test]
    async fn mailbox_send_to_unknown_actor_returns_error() {
        let registry = MailboxRegistry::new();
        let target = ActorId("ghost".into());
        let msg = ActorMessage::new(target.clone(), "ping".into(), b"payload".to_vec());
        let err = registry.send(&target, msg).await.unwrap_err();
        assert!(err.to_string().contains("not found in mailbox registry"));
    }

    #[tokio::test]
    async fn replay_after_replays_all_wal_events_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal_writer = WalWriter::open(&wal_path).unwrap();
        let persistence = ActorPersistence::new().with_wal(wal_writer, store);

        let actor_id = ActorId::from("replay-actor");
        let other_id = ActorId::from("other-actor");

        // 手动写入一个较早的检查点，模拟 checkpoint 之后仍有 WAL 事件的场景。
        {
            let checkpoint = persistence.checkpoint.lock();
            let cm = checkpoint.as_ref().unwrap();
            cm.save(&ActorSnapshot {
                actor_id: actor_id.clone(),
                actor_type: "test".to_string(),
                state: b"state-1".to_vec(),
                timestamp: HybridLogicalClock::new().tick(),
                sequence: 1,
                wal_offset: 0,
            })
            .unwrap();
        }

        // 追加多个 WAL 事件，并穿插其他 Actor 的事件。
        persistence
            .persist(actor_id.clone(), "test".to_string(), b"state-2".to_vec())
            .await;
        persistence
            .persist(
                other_id.clone(),
                "test".to_string(),
                b"other-state".to_vec(),
            )
            .await;
        persistence
            .persist(actor_id.clone(), "test".to_string(), b"state-3".to_vec())
            .await;

        // 从检查点 offset 重放，应返回该 Actor 的最终状态。
        let replayed = persistence.replay_after(actor_id.clone(), 0).await.unwrap();
        assert_eq!(replayed, b"state-3");

        // 其他 Actor 的事件应独立返回其最终状态。
        let other_replayed = persistence.replay_after(other_id.clone(), 0).await.unwrap();
        assert_eq!(other_replayed, b"other-state");
    }
}
