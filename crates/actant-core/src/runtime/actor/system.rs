//! ActorSystem facade 与单 Actor 运行循环。
//!
//! `RunningActor` 在独立任务中驱动单个 Actor 实例的消息循环与生命周期钩子；
//! `ActorSystem` 对外提供 spawn/send/call/stop 等 API，并串联 mailbox、
//! event bus 等子系统。定位为**系统 actor 专用本地运行时**：不做状态持久化，
//! 工作流恢复由 orchestrator 的统一工作流历史唯一承载。

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::common::{
    ActantError, ActorConfig, ActorErrorEnvelope, ActorErrorKind, ActorId, ActorMessage,
    ActorMessageResult, ActorStatus, NodeId, Result,
};
use crate::runtime::actor::mailbox::MailboxRegistry;
use crate::runtime::actor::runtime::{Actor, ActorContext};
use crate::runtime::event_bus::{BusEvent, EventBus};

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
    event_bus: EventBus,
}

impl RunningActor {
    /// 发布不可恢复的 Actor 生命周期错误到 `Topic::ActorLifecycleError`。
    ///
    /// 描述 panic / 状态机非法转换等需要外部介入的错误；常规消息失败不发布
    /// 事件，由 `tracing::error!` 与 `inc_actors_failed` 指标承载可观测性。
    fn emit_lifecycle_error(&self, error: String) {
        let actor_id = self.actor_id.clone();
        self.event_bus
            .publish(BusEvent::ActorLifecycleError { actor_id, error });
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
                }
                Ok(Err(e)) => {
                    self.handle_message_error(msg_id, reply_tx, e);
                }
                Err(_panic_payload) => {
                    self.handle_message_panic(msg_id, reply_tx);
                }
            }
        }

        self.cleanup();
    }

    fn handle_message_error(
        &mut self,
        msg_id: crate::common::MessageId,
        reply_tx: Option<oneshot::Sender<ActorMessageResult>>,
        error: ActantError,
    ) {
        tracing::error!(actor = %self.actor_id.0, error = %error, "actor handle_message failed");
        // 仅计入失败次数，不动 active_actors：消息级失败不终止 actor，
        // active_actors 的扣减只在 cleanup（actor 退出）时发生一次。
        crate::metrics::inc_actors_failed();

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
        msg_id: crate::common::MessageId,
        reply_tx: Option<oneshot::Sender<ActorMessageResult>>,
    ) {
        tracing::error!(actor = %self.actor_id.0, "actor panicked in handle_message");
        // panic 是不可恢复错误，独立公告到 ActorLifecycleError 供外部介入。
        self.emit_lifecycle_error("actor panicked in handle_message".to_string());
        // 仅计入失败次数，不动 active_actors：panic 被 catch_unwind 拦截后
        // actor 继续运行，active_actors 的扣减只在 cleanup 时发生一次。
        crate::metrics::inc_actors_failed();

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

    fn cleanup(self) {
        // registry 的注销由 ActorSystem::cleanup_actor 统一完成（stop/kill
        // 路径都会走到）；此处仅扣减在途计数。
        crate::metrics::inc_actors_stopped();
        crate::metrics::dec_active_actors();
    }
}

pub struct ActorSystem {
    actors: Arc<DashMap<ActorId, ActorEntry>>,
    registry: MailboxRegistry,
    pub(crate) event_bus: EventBus,
    node_id: Option<NodeId>,
    pub(crate) config: ActorConfig,
}

impl ActorSystem {
    pub fn new() -> Self {
        Self {
            actors: Arc::new(DashMap::new()),
            registry: MailboxRegistry::new(),
            event_bus: EventBus::new(),
            node_id: None,
            config: ActorConfig::default(),
        }
    }

    pub fn with_config(mut self, config: ActorConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// 注入共享的 `EventBus`，使 Actor 生命周期事件流向系统其他模块。
    ///
    /// 所有已通过 `subscribe()` 建立的订阅会随旧 bus 一起失效；调用方应在
    /// `with_event_bus` 之后再订阅。
    pub fn with_event_bus(mut self, bus: EventBus) -> Self {
        self.event_bus = bus;
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

        // 生命周期敏感操作按 "失败不留残留" 顺序执行：on_start 在注册
        // mailbox 之前——on_start 失败时 mailbox 尚未建立，下次 spawn 可原样恢复。
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let ctx = ActorContext::new(actor_id.clone());

        self.run_actor_on_start(&actor_id, &mut actor).await?;

        let (tx, rx) = mpsc::channel::<ActorMessage>(self.config.mailbox_capacity);
        self.registry.register(actor_id.clone(), tx);

        tracing::debug!(actor = %actor_id.0, "actor spawned");

        crate::metrics::inc_actors_spawned();
        crate::metrics::inc_active_actors();

        let runtime = RunningActor {
            actor_id: actor_id.clone(),
            actor,
            ctx,
            rx,
            cancel_rx,
            event_bus: self.event_bus.clone(),
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
        tracing::debug!(actor = %actor_id.0, "actor cleaned up");
    }

    /// 发布不可恢复的 Actor 生命周期错误到 `Topic::ActorLifecycleError`。
    ///
    /// 用于 `run_actor_on_start` 等 spawn 前路径拦截到的 panic——这些路径
    /// RunningActor 尚未构造，无法用其 `emit_lifecycle_error`，由 ActorSystem
    /// 直接发布。
    pub(crate) fn emit_lifecycle_error(&self, actor_id: ActorId, error: String) {
        self.event_bus
            .publish(BusEvent::ActorLifecycleError { actor_id, error });
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
}

impl Default for ActorSystem {
    fn default() -> Self {
        Self::new()
    }
}
