use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::actor::handler::{Actor, ActorContext};
use crate::actor::mailbox::MailboxRegistry;
use crate::actor::supervision::{SupervisionEvent, SupervisionState};
use crate::common::{
    ActantError, ActorConfig, ActorId, ActorMessage, ActorMessageResult, ActorStatus, MessageId,
    NodeId, RemoteActorRequest, RemoteReplyAddress, ReplyRegistry, Result,
};
use crate::event_bus::EventBus;
use crate::store::hlc::HybridLogicalClock;
use crate::store::CheckpointManager;

struct ActorEntry {
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct PersistentState {
    /// RwLock 保护 WAL writer 免受压缩竞争：
    /// - persist_state 持有 **读** 锁（多个并发写入无冲突）
    /// - 压缩持有 **写** 锁（排除写入和其他压缩）
    wal_writer: parking_lot::RwLock<Option<crate::store::WalWriter>>,
    checkpoint: parking_lot::Mutex<Option<CheckpointManager>>,
    sequence: std::sync::atomic::AtomicU64,
    hlc: HybridLogicalClock,
}

pub struct ActorSystem {
    actors: Arc<DashMap<ActorId, ActorEntry>>,
    actor_types: DashMap<ActorId, String>,
    registry: MailboxRegistry,
    supervision: Arc<SupervisionState>,
    persistent: Arc<PersistentState>,
    network: Option<Arc<dyn crate::network::Transport>>,
    node_id: Option<NodeId>,
    config: ActorConfig,
    compaction_cancel: Arc<parking_lot::Mutex<Option<watch::Sender<bool>>>>,
    pending_replies: Arc<ReplyRegistry>,
    event_bus: Option<EventBus>,
}

impl ActorSystem {
    pub fn new() -> Self {
        Self {
            actors: Arc::new(DashMap::new()),
            actor_types: DashMap::new(),
            registry: MailboxRegistry::new(),
            supervision: Arc::new(SupervisionState::with_capacity(
                ActorConfig::default().supervision_event_capacity,
            )),
            persistent: Arc::new(PersistentState {
                wal_writer: parking_lot::RwLock::new(None),
                checkpoint: parking_lot::Mutex::new(None),
                sequence: std::sync::atomic::AtomicU64::new(0),
                hlc: HybridLogicalClock::new(),
            }),
            network: None,
            node_id: None,
            config: ActorConfig::default(),
            compaction_cancel: Arc::new(parking_lot::Mutex::new(None)),
            pending_replies: Arc::new(ReplyRegistry::new()),
            event_bus: None,
        }
    }

    pub fn with_config(mut self, config: ActorConfig) -> Self {
        if config.supervision_event_capacity != self.config.supervision_event_capacity {
            self.supervision = Arc::new(SupervisionState::with_capacity(
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

    pub fn with_network(mut self, network: Arc<dyn crate::network::Transport>) -> Self {
        self.network = Some(network);
        self
    }

    pub fn with_event_bus(mut self, bus: EventBus) -> Self {
        self.event_bus = Some(bus);
        self
    }

    pub fn with_checkpoint(self, checkpoint: CheckpointManager) -> Self {
        *self.persistent.checkpoint.lock() = Some(checkpoint);
        self
    }

    pub fn with_wal(
        mut self,
        wal_writer: crate::store::WalWriter,
        store: crate::store::Store,
    ) -> Self {
        let checkpoint = CheckpointManager::new(store.clone());
        self.registry = self.registry.with_store(store);
        *self.persistent.wal_writer.write() = Some(wal_writer);
        *self.persistent.checkpoint.lock() = Some(checkpoint);
        self
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    /// 向广播通道和 EventBus 同时发出监管事件。
    fn emit_supervision(&self, event: SupervisionEvent) {
        self.supervision.emit(event.clone());
        if let Some(ref bus) = self.event_bus {
            let bus = bus.clone();
            let event = event.clone();
            // 派生一个小任务 — publish 是异步的，但 emit 是同步的。
            tokio::spawn(async move {
                bus.publish(crate::event_bus::BusEvent::SupervisionEvent(event))
                    .await;
            });
        }
    }

    pub async fn spawn(&self, actor_id: ActorId, actor: impl Actor) -> Result<()> {
        self.spawn_boxed(actor_id, Box::new(actor)).await
    }

    async fn spawn_boxed(&self, actor_id: ActorId, mut actor: Box<dyn Actor>) -> Result<()> {
        if self.actors.contains_key(&actor_id) {
            return Err(crate::common::ActantError::AlreadyExists(format!(
                "actor {} already exists",
                actor_id.as_str()
            )));
        }

        let actor_type = actor.actor_type().to_string();
        self.actor_types.insert(actor_id.clone(), actor_type);

        let (tx, rx) = mpsc::channel::<ActorMessage>(self.config.mailbox_capacity);
        self.registry.register(actor_id.clone(), tx);

        // 恢复上一 incarnation 遗留的待处理消息
        if let Err(e) = self.registry.recover_pending(&actor_id) {
            tracing::warn!(
                "failed to recover pending messages for {}: {}",
                actor_id.as_str(),
                e
            );
        }

        let (cancel_tx, cancel_rx) = watch::channel(false);

        let ctx = ActorContext::new(actor_id.clone());

        let mut checkpoint_wal_offset: Option<u64> = None;

        if let Some(ref checkpoint) = *self.persistent.checkpoint.lock() {
            if let Ok(Some(snapshot)) = checkpoint.load_latest(&actor_id) {
                checkpoint_wal_offset = Some(snapshot.wal_offset);
                let load_t0 = std::time::Instant::now();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    actor.load_state(&snapshot.state)
                }));
                crate::metrics::observe_actor_load_state_ms(load_t0.elapsed().as_millis() as u64);
                match result {
                    Err(_panic) => {
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
            }
        }

        // 重放检查点之后的 WAL 事件，恢复已持久化到 WAL 但尚未被压缩检查点捕获的状态。
        if let Some(offset) = checkpoint_wal_offset {
            let wal_guard = self.persistent.wal_writer.read();
            if let Some(ref wal) = *wal_guard {
                if let Ok(reader) = crate::store::wal::WalReader::open(wal.path().to_path_buf()) {
                    if let Ok(events) = reader.recover_events(offset) {
                        // 每个 WAL 事件携带完整状态快照。
                        // 应用此 Actor 的最后一个事件以获得最新状态。
                        if let Some((_ts, last_event)) =
                            events.into_iter().rfind(|(_, e)| e.actor_id == actor_id)
                        {
                            let replay_t0 = std::time::Instant::now();
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    actor.load_state(&last_event.payload)
                                }));
                            crate::metrics::observe_actor_load_state_ms(
                                replay_t0.elapsed().as_millis() as u64,
                            );
                            match result {
                                Err(_panic) => {
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
                                Ok(Ok(())) => {
                                    tracing::debug!(
                                        "actor {} replayed WAL events after checkpoint at offset {}",
                                        actor_id.as_str(),
                                        offset
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        {
            let start_future = actor.on_start();
            let catch_result =
                futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(start_future)).await;
            match catch_result {
                Err(_panic) => {
                    tracing::warn!("actor {} panicked in on_start", actor_id.as_str());
                    return Err(ActantError::Actor(format!(
                        "actor {} panicked in on_start",
                        actor_id.as_str()
                    )));
                }
                Ok(Err(e)) => {
                    tracing::warn!("actor on_start failed for {}: {}", actor_id.as_str(), e);
                    return Err(ActantError::Actor(format!(
                        "actor {} on_start failed: {}",
                        actor_id.as_str(),
                        e
                    )));
                }
                Ok(Ok(())) => {}
            }
        }

        self.supervision.emit(SupervisionEvent::ActorStarted {
            actor_id: actor_id.clone(),
        });

        crate::metrics::inc_actors_spawned();
        crate::metrics::inc_active_actors();

        let runtime = ActorRuntime {
            actor_id: actor_id.clone(),
            actor,
            ctx,
            rx,
            cancel_rx,
            supervision: self.supervision.clone(),
            event_bus: self.event_bus.clone(),
            registry: self.registry.clone(),
            persistent: self.persistent.clone(),
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

    pub async fn send(&self, actor_id: &ActorId, msg: ActorMessage) -> Result<()> {
        self.registry.send(actor_id, msg).await
    }

    pub async fn call(
        &self,
        actor_id: &ActorId,
        method: String,
        payload: Vec<u8>,
    ) -> Result<ActorMessageResult> {
        let (msg, rx) = ActorMessage::with_reply(actor_id.clone(), method, payload);
        self.registry.send(actor_id, msg).await?;
        rx.await
            .map_err(|e| crate::common::ActantError::Actor(format!("call failed: {}", e)))
    }

    /// 向远端 Actor 发送请求并通过网络等待回复。
    /// 超时时最多重试 `remote_call_max_retries` 次。
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
            .ok_or_else(|| crate::common::ActantError::Actor("network not configured".into()))?;
        let local_node_id = self
            .node_id
            .as_ref()
            .ok_or_else(|| crate::common::ActantError::Actor("node_id not configured".into()))?;

        let max_retries = self.config.remote_call_max_retries;
        let retry_delay = std::time::Duration::from_millis(self.config.remote_call_retry_delay_ms);

        let mut attempt = 0u32;
        loop {
            let reply_addr = RemoteReplyAddress {
                node_id: local_node_id.clone(),
                correlation_id: MessageId::generate(),
            };

            let direct_req = crate::network::protocol::DirectRequest::ActorCall {
                target: target.clone(),
                method: method.clone(),
                payload: payload.clone(),
                reply_to: reply_addr,
            };

            match network
                .send_direct_request(target_node.as_str(), direct_req)
                .await
            {
                Ok(crate::network::protocol::DirectResponse::ActorCallResult { result }) => {
                    if result.is_empty() {
                        return Err(crate::common::ActantError::Actor(
                            "remote actor call returned empty result".into(),
                        ));
                    }
                    let msg_result: ActorMessageResult = postcard::from_bytes(&result)
                        .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
                    return Ok(msg_result);
                }
                Ok(_) => {
                    return Err(crate::common::ActantError::Actor(
                        "remote actor call returned unexpected response type".into(),
                    ));
                }
                Err(crate::common::ActantError::Timeout(_)) => {
                    if attempt < max_retries {
                        attempt += 1;
                        tracing::debug!(
                            "remote call to {}/{} timed out, retry {}/{}",
                            target_node.as_str(),
                            target.as_str(),
                            attempt,
                            max_retries
                        );
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                    return Err(crate::common::ActantError::Actor(
                        "remote call timed out".into(),
                    ));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 将远端 Actor 回复投递给等待的 oneshot sender。
    /// 由 Worker 事件循环在收到 `RemoteActorReply` 时调用。
    pub fn deliver_reply(&self, reply: crate::common::RemoteActorReply) {
        if let Some((_, tx)) = self.pending_replies.remove(&reply.correlation_id) {
            let _ = tx.send(reply.result);
        }
    }

    /// 处理进入的远端 Actor 请求：投递到本地 Actor 邮箱，
    /// 若需要回复则派生任务将回复通过网络回传。
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
                    let reply = crate::common::RemoteActorReply {
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

    pub async fn kill(&self, actor_id: &ActorId) -> Result<()> {
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
        self.emit_supervision(SupervisionEvent::ActorStopped {
            actor_id: actor_id.clone(),
        });
    }

    pub async fn list_actors(&self) -> Vec<ActorId> {
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

    pub async fn stop_compaction_task(&self) {
        let mut cancel = self.compaction_cancel.lock();
        if let Some(tx) = cancel.take() {
            let _ = tx.send(true);
        }
    }

    pub fn start_compaction_task(&self) {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let persistent = self.persistent.clone();
        let interval_secs = self.config.wal_compaction_interval_secs;
        let checkpoint_retention_count = self.config.checkpoint_retention_count;

        // 立即存储 cancel_tx 以避免竞态
        {
            let mut cancel = self.compaction_cancel.lock();
            *cancel = Some(cancel_tx);
        }

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let checkpoint_opt = persistent.checkpoint.lock().as_ref().cloned();

                        // 压缩期间对 wal_writer 持有 **写** 锁。
                        // 这会阻塞 persist_state（持读锁）追加新事件，
                        // 防止压缩期间写入的数据被截断的竞态。
                        let mut wal_guard = persistent.wal_writer.write();
                        if let (Some(ref mut writer), Some(checkpoint)) = (&mut *wal_guard, checkpoint_opt) {
                            let wal_path = writer.path().to_path_buf();
                            let current_offset = writer.current_offset();
                            let keep_latest = checkpoint_retention_count;
                            let compactor =
                                crate::store::WalCompactor::new(wal_path, current_offset, checkpoint, keep_latest);
                            if let Err(e) = compactor.compact() {
                                tracing::warn!("WAL compaction failed: {}", e);
                            } else {
                                // 压缩成功：在压缩后的文件上同步并重新打开。
                                if let Err(e) = writer.sync() {
                                    tracing::warn!("WAL sync after compaction failed: {}", e);
                                }
                                let path = writer.path().to_path_buf();
                                if let Ok(new_writer) = crate::store::WalWriter::open(&path) {
                                    *writer = new_writer;
                                }
                            }
                        }
                    }
                    Ok(()) = cancel_rx.changed() => {
                        break;
                    }
                    else => break,
                }
            }
        });
    }
}

struct ActorRuntime {
    actor_id: ActorId,
    actor: Box<dyn Actor>,
    ctx: ActorContext,
    rx: mpsc::Receiver<ActorMessage>,
    cancel_rx: watch::Receiver<bool>,
    supervision: Arc<SupervisionState>,
    event_bus: Option<EventBus>,
    registry: MailboxRegistry,
    persistent: Arc<PersistentState>,
}

impl ActorRuntime {
    /// 向广播通道和 EventBus 同时发出监管事件。
    fn emit_supervision(&self, event: SupervisionEvent) {
        self.supervision.emit(event.clone());
        if let Some(ref bus) = self.event_bus {
            let bus = bus.clone();
            let ev = event.clone();
            tokio::spawn(async move {
                bus.publish(crate::event_bus::BusEvent::SupervisionEvent(ev))
                    .await;
            });
        }
    }

    async fn run(mut self) {
        if let Err(e) = self.ctx.transition(ActorStatus::Running) {
            tracing::error!(
                "actor {} failed to transition to Running: {}",
                self.actor_id.as_str(),
                e
            );
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
                        let _ = tx.send(response);
                    }
                    self.registry.ack_message(&self.actor_id, &msg_id).ok();
                }
                Ok(Err(e)) => {
                    self.registry.ack_message(&self.actor_id, &msg_id).ok();
                    self.handle_message_error(msg_id, reply_tx, e.to_string())
                        .await;
                }
                Err(_panic_payload) => {
                    self.registry.ack_message(&self.actor_id, &msg_id).ok();
                    self.handle_message_panic(msg_id, reply_tx).await;
                }
            }

            self.persist_state().await;
        }

        self.cleanup();
    }

    async fn handle_message_error(
        &mut self,
        msg_id: MessageId,
        reply_tx: Option<tokio::sync::oneshot::Sender<ActorMessageResult>>,
        error: String,
    ) {
        tracing::error!(
            "actor {} handle_message error: {}",
            self.actor_id.as_str(),
            error
        );
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

    async fn handle_message_panic(
        &mut self,
        msg_id: MessageId,
        reply_tx: Option<tokio::sync::oneshot::Sender<ActorMessageResult>>,
    ) {
        tracing::error!(
            "actor {} panicked in handle_message",
            self.actor_id.as_str()
        );
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
            Err(_panic) => {
                tracing::warn!("actor {} panicked in on_stop", self.actor_id.as_str());
            }
            Ok(Err(e)) => {
                tracing::warn!("actor {} on_stop error: {}", self.actor_id.as_str(), e);
            }
            Ok(Ok(())) => {}
        }
        if let Err(e) = self.ctx.transition(ActorStatus::Stopped) {
            tracing::warn!(
                "actor {} failed to transition to Stopped: {}",
                self.actor_id.as_str(),
                e
            );
        }
    }

    async fn persist_state(&self) {
        let seq = self
            .persistent
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;

        let save_t0 = std::time::Instant::now();
        let save_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.actor.save_state()));
        crate::metrics::observe_actor_save_state_ms(save_t0.elapsed().as_millis() as u64);

        let state = match save_result {
            Err(_panic) => {
                tracing::warn!("actor {} panicked in save_state", self.actor_id.as_str());
                return;
            }
            Ok(Err(e)) => {
                tracing::warn!("actor {} save_state error: {}", self.actor_id.as_str(), e);
                return;
            }
            Ok(Ok(state)) => state,
        };

        if !self.actor.supports_state_persistence() && state.is_empty() {
            tracing::debug!(
                "actor {} does not support state persistence, skipping checkpoint",
                self.actor_id.as_str()
            );
            return;
        }

        let hlc_ts = self.persistent.hlc.tick();

        if let Some(ref checkpoint) = *self.persistent.checkpoint.lock() {
            let wal_offset = {
                let wal_guard = self.persistent.wal_writer.read();
                wal_guard.as_ref().map(|w| w.current_offset()).unwrap_or(0)
            };

            let snapshot = crate::store::checkpoint::ActorSnapshot {
                actor_id: self.actor_id.clone(),
                actor_type: self.actor.actor_type().to_string(),
                state: state.clone(),
                timestamp: hlc_ts,
                sequence: seq,
                wal_offset,
            };

            if let Err(e) = checkpoint.save(&snapshot) {
                tracing::warn!(
                    "checkpoint save failed for {}: {}",
                    self.actor_id.as_str(),
                    e
                );
            }
        }

        let mut wal_guard = self.persistent.wal_writer.write();
        if let Some(ref mut wal) = *wal_guard {
            let event = crate::store::wal::WalEvent {
                actor_id: self.actor_id.clone(),
                sequence: seq,
                payload: state,
            };
            if let Err(e) = wal.append_event(hlc_ts, &event) {
                tracing::warn!("WAL append failed for {}: {}", self.actor_id.as_str(), e);
            }
        }
    }

    fn cleanup(self) {
        self.registry.unregister(&self.actor_id);
        crate::metrics::inc_actors_stopped();
        crate::metrics::dec_active_actors();
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
    use std::sync::{Arc, Mutex};

    /// 测试用 Actor：记录收到的消息方法名，可配置为 panic/error。
    struct EchoActor {
        received: Arc<Mutex<Vec<String>>>,
        fail_method: Option<String>,
        panic_method: Option<String>,
    }

    impl EchoActor {
        fn new() -> (Self, Arc<Mutex<Vec<String>>>) {
            let received = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    received: received.clone(),
                    fail_method: None,
                    panic_method: None,
                },
                received,
            )
        }

        fn with_fail(method: &str) -> (Self, Arc<Mutex<Vec<String>>>) {
            let (mut actor, received) = Self::new();
            actor.fail_method = Some(method.to_string());
            (actor, received)
        }

        fn with_panic(method: &str) -> (Self, Arc<Mutex<Vec<String>>>) {
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
                return Err(crate::common::ActantError::Actor(
                    "intentional failure".into(),
                ));
            }

            Ok(ActorMessageResult {
                message_id: msg.id,
                payload: msg.payload.clone(),
                error: None,
            })
        }
    }

    #[tokio::test]
    async fn spawn_and_send_delivers_message_to_actor() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("echo-1");
        let (actor, received) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let msg = ActorMessage::new(actor_id.clone(), "ping".into(), b"data".to_vec());
        system.send(&actor_id, msg).await.unwrap();

        // 给 runtime 时间处理消息
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
            .call(&actor_id, "echo".into(), b"hello".to_vec())
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

        let result = system.call(&actor_id, "boom".into(), vec![]).await.unwrap();
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("intentional failure"));
    }

    #[tokio::test]
    async fn call_returns_error_when_actor_panics() {
        // Actor panic 不应崩溃系统，而应返回错误给调用方。
        let system = ActorSystem::new();
        let actor_id = ActorId::from("panic-1");
        let (actor, _) = EchoActor::with_panic("explode");
        system.spawn(actor_id.clone(), actor).await.unwrap();

        let result = system
            .call(&actor_id, "explode".into(), vec![])
            .await
            .unwrap();
        assert!(result.error.is_some());
        assert!(result.error.unwrap().contains("panicked"));
    }

    #[tokio::test]
    async fn actor_panic_does_not_crash_other_actors() {
        // 一个 actor panic 后，另一个 actor 仍能正常处理消息。
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

        // 触发 panic actor
        let _ = system.call(&panic_id, "boom".into(), vec![]).await;

        // healthy actor 仍能正常工作
        let result = system
            .call(&healthy_id, "ping".into(), b"data".to_vec())
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

        // stop 后 status 应为 Stopped 或 None（actor 已从 map 移除）
        let status = system.actor_status(&actor_id);
        assert!(
            status.is_none() || status == Some(ActorStatus::Stopped),
            "expected None or Stopped, got {:?}",
            status
        );

        // 停止后 send 应失败
        let msg = ActorMessage::new(actor_id.clone(), "ping".into(), vec![]);
        assert!(system.send(&actor_id, msg).await.is_err());
    }

    #[tokio::test]
    async fn kill_aborts_actor_immediately() {
        let system = ActorSystem::new();
        let actor_id = ActorId::from("kill-1");
        let (actor, _) = EchoActor::new();
        system.spawn(actor_id.clone(), actor).await.unwrap();

        system.kill(&actor_id).await.unwrap();

        // kill 后 send 应失败
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

        let mut actors = system.list_actors().await;
        actors.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        assert_eq!(actors.len(), 2);
        assert_eq!(actors[0].as_str(), "list-1");
        assert_eq!(actors[1].as_str(), "list-2");
    }

    #[tokio::test]
    async fn list_actors_empty_when_none_spawned() {
        let system = ActorSystem::new();
        let actors = system.list_actors().await;
        assert!(actors.is_empty());
    }

    #[tokio::test]
    async fn actor_status_none_for_unknown_actor() {
        let system = ActorSystem::new();
        assert_eq!(system.actor_status(&ActorId::from("ghost")), None);
    }

    #[tokio::test]
    async fn stop_unknown_actor_is_noop() {
        // stop 对不存在的 actor 应返回 Ok（幂等）。
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
        let _ = system.call(&actor_id, "boom".into(), vec![]).await;

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

        // 并发发送消息到两个 actor
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
        // 未配置网络时 call_remote 应返回错误。
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
}
