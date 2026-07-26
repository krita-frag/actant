//! ActorSystem facade 与单 Actor 运行循环。
//!
//! `RunningActor` 在独立任务中驱动单个 Actor 实例的消息循环、状态持久化
//! 与生命周期钩子；`ActorSystem` 对外提供 spawn/send/call/stop 等 API，
//! 并串联 mailbox、persistence、supervision、网络、跨节点路由等子系统。

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::common::backoff::{ExponentialBackoff, REMOTE_CALL_MAX_RETRY_DELAY};
use crate::common::{
    ActantError, ActorConfig, ActorErrorEnvelope, ActorErrorKind, ActorId, ActorMessage,
    ActorMessageResult, ActorStatus, MessageId, NodeId, RemoteActorReply, RemoteActorRequest,
    RemoteReplyAddress, ReplyRegistry, Result,
};
use crate::runtime::actor::mailbox::MailboxRegistry;
use crate::runtime::actor::persistence::ActorPersistence;
use crate::runtime::actor::router::{ActorRegistry, ActorRouter};
use crate::runtime::actor::runtime::{Actor, ActorContext};
use crate::runtime::actor::supervision::{SupervisionEvent, SupervisionTree};
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::network::{DirectRequest, DirectResponse, Transport};
use crate::runtime::state::{CheckpointManager, LmdbStore, Store, WalWriter};

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
    registry: MailboxRegistry,
    persistence: Arc<ActorPersistence>,
}

impl RunningActor {
    fn emit_supervision(&self, event: SupervisionEvent) {
        // SupervisionTree 内部通过 EventBus 发布，订阅者通过
        // `Topic::Supervision` 统一消费——无需 RunningActor 再走第二条路径。
        self.supervision.emit(event);
    }

    /// 发布不可恢复的 Actor 生命周期错误到 `Topic::ActorLifecycleError`。
    ///
    /// 与 `SupervisionEvent::ActorFailed` 互补：后者是常规失败信号（驱动
    /// 重启策略），本事件描述 panic / 状态机非法转换等需要外部介入的
    /// 错误。两条路径独立：调用方在 panic 路径同时调 emit_supervision
    /// 与本方法。
    fn emit_lifecycle_error(&self, error: String) {
        let bus = self.supervision.event_bus().clone();
        let actor_id = self.actor_id.clone();
        tokio::spawn(async move {
            bus.publish(BusEvent::ActorLifecycleError { actor_id, error })
                .await;
        });
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
                    self.handle_message_error(msg_id, reply_tx, e);
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
        error: ActantError,
    ) {
        tracing::error!("actor {} handle_message error: {}", self.actor_id.0, error);
        self.emit_supervision(SupervisionEvent::ActorFailed {
            actor_id: self.actor_id.clone(),
            error: error.to_string(),
        });
        crate::metrics::inc_actors_failed();
        crate::metrics::dec_active_actors();

        if let Some(tx) = reply_tx {
            // tx.send 失败仅当接收端已 drop（调用方超时放弃等待），错误结果
            // 无处投递，丢弃是合理的。
            let _ = tx.send(ActorMessageResult {
                message_id: msg_id,
                payload: vec![],
                error: Some(ActorErrorEnvelope::from(error)),
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
        // panic 是不可恢复错误，独立公告到 ActorLifecycleError 供外部介入。
        self.emit_lifecycle_error("actor panicked in handle_message".to_string());
        crate::metrics::inc_actors_failed();
        crate::metrics::dec_active_actors();

        if let Some(tx) = reply_tx {
            // tx.send 失败仅当接收端已 drop（调用方超时放弃等待），错误结果
            // 无处投递，丢弃是合理的。
            let _ = tx.send(ActorMessageResult {
                message_id: msg_id,
                payload: vec![],
                error: Some(ActorErrorEnvelope {
                    kind: ActorErrorKind::Internal,
                    message: "actor panicked".to_string(),
                }),
            });
        }
    }

    async fn handle_stop(&mut self) {
        let stop_future = self.actor.on_stop();
        let result =
            futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(stop_future)).await;
        match result {
            Err(_) => {
                tracing::error!("actor {} panicked in on_stop", self.actor_id.0);
                self.emit_lifecycle_error("actor panicked in on_stop".to_string());
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
                tracing::error!("actor {} panicked in save_state", self.actor_id.0);
                self.emit_lifecycle_error("actor panicked in save_state".to_string());
                return;
            }
            Ok(Err(e)) => {
                tracing::error!("actor {} save_state error: {}", self.actor_id.0, e);
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
    pub(crate) supervision: Arc<SupervisionTree>,
    persistence: Arc<ActorPersistence>,
    network: Option<Arc<dyn Transport>>,
    node_id: Option<NodeId>,
    pub(crate) config: ActorConfig,
    compaction_cancel: Arc<Mutex<Option<watch::Sender<bool>>>>,
    pending_replies: Arc<ReplyRegistry>,
    /// 跨节点 Actor 注册表（A2 路由）。可选，未配置时 `call_by_type` 返回错误。
    pub(crate) actor_registry: Option<Arc<ActorRegistry>>,
    /// Actor 路由器（A2）。可选，未配置时 `call_by_type` 返回错误。
    pub(crate) actor_router: Option<Arc<dyn ActorRouter>>,
}

impl ActorSystem {
    pub fn new() -> Self {
        Self {
            actors: Arc::new(DashMap::new()),
            actor_types: DashMap::new(),
            registry: MailboxRegistry::new(),
            supervision: Arc::new(SupervisionTree::with_event_bus(EventBus::new())),
            persistence: Arc::new(ActorPersistence::new()),
            network: None,
            node_id: None,
            config: ActorConfig::default(),
            compaction_cancel: Arc::new(Mutex::new(None)),
            pending_replies: Arc::new(ReplyRegistry::new()),
            actor_registry: None,
            actor_router: None,
        }
    }

    pub fn with_config(mut self, config: ActorConfig) -> Self {
        // SupervisionTree 现持有 EventBus；容量参数由 EventBus 的
        // `subscriber_capacity` 控制，本字段保留为兼容性配置项。
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

    /// 注入共享的 `EventBus`，使监督事件流向系统其他模块。
    ///
    /// 替换 `SupervisionTree` 内部的 EventBus——所有已通过 `subscribe()`
    /// 建立的订阅会随旧 bus 一起失效；调用方应在 `with_event_bus` 之后再订阅。
    pub fn with_event_bus(mut self, bus: EventBus) -> Self {
        self.supervision = Arc::new(SupervisionTree::with_event_bus(bus));
        self
    }

    /// 注入跨节点 Actor 注册表（A2 路由）。
    ///
    /// 一旦注入，`spawn_boxed` 会自动将 actor 类型注册到注册表，
    /// `call_by_type` 可用（前提是同时注入 `actor_router`）。
    pub fn with_actor_registry(mut self, registry: Arc<ActorRegistry>) -> Self {
        if let Some(node_id) = &self.node_id {
            registry.set_local_node_id(node_id.clone());
        }
        self.actor_registry = Some(registry);
        self
    }

    /// 注入 Actor 路由器（A2）。`call_by_type` 使用此路由器选择目标节点。
    pub fn with_actor_router(mut self, router: Arc<dyn ActorRouter>) -> Self {
        self.actor_router = Some(router);
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
        self.actor_types
            .insert(actor_id.clone(), actor_type.clone());

        // 同步到跨节点注册表（A2）：注册成功后下次 gossip 广播会通知对端。
        // 注册表未配置时为 NoOp。
        if let Some(registry) = &self.actor_registry {
            registry.register_local_type(&actor_type);
        }

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
                tracing::error!("actor {} panicked in load_state", actor_id.as_str());
                self.emit_lifecycle_error(
                    actor_id.clone(),
                    "actor panicked in load_state".to_string(),
                );
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
                self.emit_lifecycle_error(
                    actor_id.clone(),
                    "actor panicked in WAL replay load_state".to_string(),
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
                tracing::error!("actor {} panicked in on_start", actor_id.as_str());
                self.emit_lifecycle_error(
                    actor_id.clone(),
                    "actor panicked in on_start".to_string(),
                );
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
        let backoff = ExponentialBackoff::new(
            std::time::Duration::from_millis(self.config.remote_call_retry_delay_ms),
            REMOTE_CALL_MAX_RETRY_DELAY,
        );

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
                    let delay = backoff.delay_for(attempt);
                    attempt += 1;
                    tracing::debug!(
                        "remote call to {}/{} timed out, retry {}/{} after {:?}",
                        target_node.as_str(),
                        target.as_str(),
                        attempt,
                        max_retries,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 返回本地已注册的 actor 类型集合（A2）。
    pub fn local_actor_types(&self) -> std::collections::BTreeSet<String> {
        self.actor_types.iter().map(|e| e.value().clone()).collect()
    }

    /// 返回当前已知的所有 actor 类型（本节点 + 远端节点）（A2）。
    pub fn known_actor_types(&self) -> Vec<String> {
        let mut all: std::collections::BTreeSet<String> = self.local_actor_types();
        if let Some(ref registry) = self.actor_registry {
            for (_node_id, entry) in registry.snapshot_peers() {
                for t in &entry.actor_types {
                    all.insert(t.clone());
                }
            }
        }
        all.into_iter().collect()
    }

    /// 按类型查找本地 actor 实例（A2 接收侧）。
    ///
    /// 若有多个实例匹配同一类型，使用 round-robin 在它们之间选择。
    pub fn find_local_actor_by_type(&self, actor_type: &str) -> Option<ActorId> {
        let matches: Vec<ActorId> = self
            .actor_types
            .iter()
            .filter(|e| e.value() == actor_type)
            .map(|e| e.key().clone())
            .collect();
        if matches.is_empty() {
            return None;
        }
        if matches.len() == 1 {
            return matches.into_iter().next();
        }
        // 多个匹配：round-robin 选择。使用指针地址作为简单的哈希源，
        // 避免为本次查找引入 AtomicU64 状态。
        let idx = (self as *const Self as usize) % matches.len();
        matches.into_iter().nth(idx)
    }

    /// 按 actor 类型发起跨节点调用（A2 路由入口）。
    pub async fn call_by_type(
        &self,
        actor_type: &str,
        method: &str,
        payload: Vec<u8>,
    ) -> Result<ActorMessageResult> {
        let router = self.actor_router.as_ref().ok_or_else(|| {
            ActantError::Actor("actor_router not configured: call_by_type requires router".into())
        })?;
        let local_node_id = self
            .node_id
            .as_ref()
            .ok_or_else(|| ActantError::Actor("node_id not configured".into()))?;

        let target_node = router.select_node(actor_type, None).ok_or_else(|| {
            ActantError::Actor(format!("no node available for actor_type '{}'", actor_type))
        })?;

        // 本地节点直接走本地 call，避免 wire 序列化往返。
        if target_node == *local_node_id {
            if let Some(actor_id) = self.find_local_actor_by_type(actor_type) {
                return self.call(&actor_id, method, payload).await;
            }
            return Err(ActantError::Actor(format!(
                "router selected local node but no local actor of type '{}' found",
                actor_type
            )));
        }

        let network = self
            .network
            .as_ref()
            .ok_or_else(|| ActantError::Actor("network not configured".into()))?;

        router.on_call_start(&target_node);

        let max_retries = self.config.remote_call_max_retries;
        let backoff = ExponentialBackoff::new(
            std::time::Duration::from_millis(self.config.remote_call_retry_delay_ms),
            REMOTE_CALL_MAX_RETRY_DELAY,
        );

        let mut attempt = 0u32;
        let mut excluded: Option<NodeId> = None;
        loop {
            // 重试时排除上次失败的节点，尝试其他节点。
            let selected = if attempt == 0 {
                target_node.clone()
            } else {
                match router.select_node(actor_type, excluded.as_ref()) {
                    Some(n) => n,
                    None => {
                        return Err(ActantError::Actor(format!(
                            "no alternative node available for actor_type '{}' after {} retries",
                            actor_type, attempt
                        )));
                    }
                }
            };

            let reply_addr = RemoteReplyAddress {
                node_id: local_node_id.clone(),
                correlation_id: MessageId::generate(),
            };

            let direct_req = DirectRequest::ActorCallByType {
                actor_type: actor_type.to_string(),
                method: method.to_string(),
                payload: payload.clone(),
                reply_to: reply_addr,
            };

            match network
                .send_direct_request(selected.as_str(), direct_req)
                .await
            {
                Ok(DirectResponse::ActorCallResult { result }) => {
                    if result.is_empty() {
                        return Err(ActantError::Actor(
                            "remote actor call by type returned empty result".into(),
                        ));
                    }
                    let msg_result: ActorMessageResult = crate::common::decode_postcard(&result)?;
                    router.on_call_end(&selected);
                    return Ok(msg_result);
                }
                Ok(_) => {
                    return Err(ActantError::Actor(
                        "remote actor call by type returned unexpected response type".into(),
                    ));
                }
                Err(ActantError::Timeout(_)) => {
                    if attempt >= max_retries {
                        router.on_call_end(&selected);
                        return Err(ActantError::Actor("remote call by type timed out".into()));
                    }
                    let delay = backoff.delay_for(attempt);
                    attempt += 1;
                    excluded = Some(selected.clone());
                    router.on_call_end(&selected);
                    tracing::debug!(
                        "remote call by type to {} for '{}' timed out, retry {}/{} after {:?}",
                        selected.as_str(),
                        actor_type,
                        attempt,
                        max_retries,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Err(e) => {
                    router.on_call_end(&selected);
                    return Err(e);
                }
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
            // cancel.send 失败仅当 task 已完成 drop 了 cancel receiver；
            // 此时 task.await 会立即返回，无需通过 cancel 通知。
            let _ = entry.cancel.send(true);
            drop(entry);
        }
        if let Some((_, entry)) = self.actors.remove(actor_id) {
            // task.await 失败仅当 task 已被 abort/panic；stop 路径下视为正常退出。
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
            // cancel.send 失败仅当 task 已完成 drop 了 cancel receiver；
            // 此时 timeout 包裹的 task.await 会立即返回。
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

    /// 发布不可恢复的 Actor 生命周期错误到 `Topic::ActorLifecycleError`。
    ///
    /// 用于 `restore_actor_state` / `run_actor_on_start` 等 spawn 前路径
    /// 拦截到的 panic——这些路径 RunningActor 尚未构造，无法用其
    /// `emit_lifecycle_error`，由 ActorSystem 直接发布。
    fn emit_lifecycle_error(&self, actor_id: ActorId, error: String) {
        let bus = self.supervision.event_bus().clone();
        tokio::spawn(async move {
            bus.publish(BusEvent::ActorLifecycleError { actor_id, error })
                .await;
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
            // send 失败仅当 compaction task 已自行退出 drop 了 receiver，
            // 此时无需通知。
            let _ = tx.send(true);
        }
    }

    pub fn start_compaction_task(&self) {
        // 通过 SupervisionTree 内的 EventBus 公告 WalCompacted；
        // node_id 用于载荷，让订阅者区分来源节点。
        let event_bus = self.supervision.event_bus().clone();
        let cancel_tx = self.persistence.clone().start_compaction(
            self.config.wal_compaction_interval_secs,
            self.config.checkpoint_retention_count,
            Some(event_bus),
            self.node_id.clone(),
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
