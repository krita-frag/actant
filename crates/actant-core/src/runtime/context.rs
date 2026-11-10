//! 统一运行时上下文。
//!
//! 把所有 Rust 子系统句柄聚合到一个结构中，供 PyO3 层和其他第 1 层代码使用。
//! “统一 Runtime + 薄 PyO3 绑定”架构的核心容器。

use std::sync::Arc;

use crate::common::wire::{CancelBroadcast, TOPIC_CANCEL};
use crate::common::{ActantConfig, ActantError, ActorId, NodeId};
use crate::runtime::actor::ActorSystem;
use crate::runtime::blobs::BlobStore;
use crate::runtime::capability::CapabilityRuntime;
use crate::runtime::dispatcher::TaskDispatcher;
use crate::runtime::event_bus::EventBus;
use crate::runtime::network::Transport;
use crate::runtime::state::Store;
use crate::runtime::workflow::{Orchestrator, Worker};

/// 非 Actor 化后台任务的取消句柄集合类型。
///
/// 使用 `parking_lot::Mutex` 而非 `std::sync::Mutex`，避免持锁线程 panic 后
/// 传播 poison 错误导致 `shutdown` 路径也 panic。
type BackgroundCancels = parking_lot::Mutex<Vec<tokio::sync::watch::Sender<bool>>>;

/// Actant 统一运行时。
///
/// 持有所有核心子系统的 `Arc` 句柄；本身廉价且 `Clone`。
#[derive(Clone)]
pub struct Runtime {
    node_id: NodeId,
    config: ActantConfig,
    network: Arc<dyn Transport>,
    store: Store,
    actor_system: Arc<ActorSystem>,
    workflow_actor_id: ActorId,
    worker: Option<Arc<Worker>>,
    capability: Arc<CapabilityRuntime>,
    event_bus: EventBus,
    task_dispatcher: Arc<dyn TaskDispatcher>,
    /// blob 原语 facade；未启用 blob 存储时为 `None`。
    blobs: Option<Arc<BlobStore>>,
    /// 编排器只读句柄（builder 装配后注入）。供绑定层在 actor 之外做
    /// 阻塞式等待点 park，见 [`Runtime::orchestrator_handle`]。
    orchestrator: Option<Orchestrator>,
    /// 非 Actor 化后台任务（如 capability gossip）的取消句柄集合。
    background_loop_cancels: Arc<BackgroundCancels>,
}

impl Runtime {
    /// 故障注入：给本节点编排器 HLC 施加时钟偏移（毫秒，可正可负），0 恢复。
    pub fn inject_hlc_drift_ms(&self, ms: i64) {
        if let Some(orchestrator) = self.orchestrator.as_ref() {
            orchestrator.inject_hlc_drift_ms(ms);
        }
    }

    /// 本节点 Worker 的 FailoverManager（故障注入钩子入口）。
    pub fn failover_manager(
        &self,
    ) -> Option<Arc<crate::runtime::workflow::failover::FailoverManager>> {
        self.worker.as_ref().and_then(|w| w.failover_manager())
    }

    /// 运行时更新节点状态：标签 / 配额容量 / draining。
    ///
    /// 标签与 draining 同时写入本地路由视图和心跳广播视图；配额容量更新
    /// 共享的 `QuotaTracker`。任一字段为 `None` 表示保持当前值。
    pub fn update_node_state(
        &self,
        update: &crate::common::NodeStateUpdate,
    ) -> Result<(), ActantError> {
        let worker = self.worker.as_ref().ok_or_else(|| {
            ActantError::InvalidState("runtime has no worker to update node state".into())
        })?;
        worker.apply_node_state(update)?;
        if let Some(failover) = worker.failover_manager() {
            failover.apply_node_state(update);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        config: ActantConfig,
        network: Arc<dyn Transport>,
        store: Store,
        actor_system: Arc<ActorSystem>,
        workflow_actor_id: ActorId,
        worker: Option<Arc<Worker>>,
        capability: Arc<CapabilityRuntime>,
        event_bus: EventBus,
        task_dispatcher: Arc<dyn TaskDispatcher>,
        blobs: Option<Arc<BlobStore>>,
    ) -> Self {
        Self {
            node_id,
            config,
            network,
            store,
            actor_system,
            workflow_actor_id,
            worker,
            capability,
            event_bus,
            task_dispatcher,
            blobs,
            orchestrator: None,
            background_loop_cancels: Arc::new(BackgroundCancels::new(Vec::new())),
        }
    }

    /// 注册一个非 Actor 化后台任务的取消句柄。
    ///
    /// 在 `Runtime::shutdown` 时会向所有已注册句柄发送 `true`，
    /// 确保后台循环与 Actor 子系统一并停止。
    pub fn register_background_loop_cancel(&self, cancel: tokio::sync::watch::Sender<bool>) {
        self.background_loop_cancels.lock().push(cancel);
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn config(&self) -> &ActantConfig {
        &self.config
    }

    pub fn network(&self) -> &Arc<dyn Transport> {
        &self.network
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn actor_system(&self) -> &Arc<ActorSystem> {
        &self.actor_system
    }

    pub fn workflow_actor_id(&self) -> &ActorId {
        &self.workflow_actor_id
    }

    pub fn worker(&self) -> Option<&Arc<Worker>> {
        self.worker.as_ref()
    }

    pub fn set_worker(&mut self, worker: Arc<Worker>) {
        self.worker = Some(worker);
    }

    /// 编排器只读句柄克隆（`Orchestrator: Clone`，内部状态全为共享句柄）。
    ///
    /// 供绑定层在**不经过 actor 消息循环**的前提下做阻塞式 park：actor 消息
    /// 处理是单线程顺序执行的，若把阻塞等待放进 `handle_message`，整个
    /// WorkflowActor（全部工作流）都会停摆。因此等待点 park 必须由调用方
    /// 持有编排器句柄、在 actor 之外阻塞。
    ///
    /// `None` 表示编排器尚未注入（`Runtime::new` 之后、builder 完成装配之前）。
    pub fn orchestrator_handle(&self) -> Option<&Orchestrator> {
        self.orchestrator.as_ref()
    }

    pub fn set_orchestrator(&mut self, orchestrator: Orchestrator) {
        self.orchestrator = Some(orchestrator);
    }

    pub fn capability(&self) -> &Arc<CapabilityRuntime> {
        &self.capability
    }

    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    pub fn task_dispatcher(&self) -> &Arc<dyn TaskDispatcher> {
        &self.task_dispatcher
    }

    /// blob 原语 facade 句柄；未启用 blob 存储时为 `None`。
    pub fn blobs(&self) -> Option<&Arc<BlobStore>> {
        self.blobs.as_ref()
    }

    /// 订阅跨节点任务取消广播话题。
    ///
    /// 应在 worker 启动前调用，确保 ``broadcast_cancel`` 可立即发送。
    pub async fn subscribe_cancel(&self) -> crate::common::Result<()> {
        self.network.subscribe(TOPIC_CANCEL).await
    }

    /// 广播一条跨节点任务取消消息。
    ///
    /// 接收方应监听 ``TOPIC_CANCEL`` 话题并据此取消本地对应任务。
    pub async fn broadcast_cancel(
        &self,
        task_id: &str,
        workflow_id: &str,
    ) -> crate::common::Result<()> {
        let msg = CancelBroadcast {
            task_id: task_id.into(),
            workflow_id: workflow_id.into(),
        };
        let bytes = postcard::to_allocvec(&msg)
            .map_err(|e| ActantError::Serialization(format!("cancel broadcast encode: {e}")))?;
        self.network.broadcast(TOPIC_CANCEL, bytes).await
    }

    /// 优雅关闭 Runtime：停止所有关键 Actor 并等待其实际退出。
    ///
    /// 关闭顺序：Worker → SchedulerActor → FailoverActor → DagGossipActor
    /// → WorkflowActor → CapabilityActor。每个 `stop` 都会 await Actor 任务
    /// 结束，确保状态落盘与后台循环取消完成。
    pub async fn shutdown(&self) -> crate::common::Result<()> {
        use crate::common::ActorId;

        // 每个 actor stop 设超时，确保 network.shutdown() 总能被执行。
        // actor stop 卡住时（如网络阻塞），放弃等待——endpoint.close() 会使
        // 其网络调用自然失败。超时由 config.actor.stop_timeout_ms 控制。
        let stop_timeout = std::time::Duration::from_millis(self.config.actor.stop_timeout_ms);

        // 0. 先取消非 Actor 化的后台循环（如 capability gossip），
        //    避免在 Actor 系统停止后仍有任务引用 network / capability 句柄。
        let cancels: Vec<tokio::sync::watch::Sender<bool>> = self
            .background_loop_cancels
            .lock()
            .iter()
            .cloned()
            .collect();
        for tx in cancels {
            // send 失败仅当子任务已退出 drop 了 cancel receiver，无需通知。
            let _ = tx.send(true);
        }

        // 1. 停止 Worker（取消消费循环）。
        if let Some(worker) = self.worker.as_ref() {
            worker.shutdown();
            // 等待 worker.run() 退出（状态→Stopped）。subscribe_topics 可能阻塞
            // 导致 cancel 不被处理，故设短超时。超时表示 worker 卡死，强行继续
            // 关闭后续组件（网络/actor），由 shutdown_timeout 兜底 drop tokio。
            let mut state_rx = worker.subscribe_state();
            // timeout Err 在此丢弃：卡死的 worker 最终由 tokio runtime drop 收尾。
            let _ = tokio::time::timeout(stop_timeout, async {
                while *state_rx.borrow() != crate::runtime::workflow::WorkerState::Stopped {
                    if state_rx.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await;
            if let Some(sched_id) = worker.scheduler_actor_id() {
                // 同上：scheduler actor stop 超时由 tokio drop 兜底。
                let _ = tokio::time::timeout(stop_timeout, self.actor_system.stop(sched_id)).await;
            }
        }

        // 2. 停止 FailoverActor（停止心跳/故障检测循环）。
        // 超时丢弃：actor 内部 on_stop 已尝试 flush，超时表示卡死，由 tokio drop 收尾。
        let failover_id = ActorId::failover(&self.node_id);
        let _ = tokio::time::timeout(stop_timeout, self.actor_system.stop(&failover_id)).await;

        // 3. 停止 DagGossipActor。
        // 超时丢弃：gossip actor 无持久化状态，卡死由 tokio drop 收尾。
        let dag_gossip_id = ActorId::dag_gossip(&self.node_id);
        let _ = tokio::time::timeout(stop_timeout, self.actor_system.stop(&dag_gossip_id)).await;

        // 4. 停止 WorkflowActor（on_stop 取消 timeout/persist 循环）。
        // 超时丢弃：on_stop 已尝试 flush_dirty，超时表示 IO 卡死，由 tokio drop 收尾。
        let _ = tokio::time::timeout(
            stop_timeout,
            self.actor_system.stop(&self.workflow_actor_id),
        )
        .await;

        // 5. 停止所有 CapabilityActor（按注册名）。
        // 超时丢弃：capability actor 通常无副作用，卡死由 tokio drop 收尾。
        for meta in self.capability.capabilities() {
            let cap_id = ActorId::capability(meta.name);
            let _ = tokio::time::timeout(stop_timeout, self.actor_system.stop(&cap_id)).await;
        }

        // 6. 关闭任务分发器线程池（等待在途任务完成或超时放弃 join）。
        //    必须在 Actor / 后台循环停止后、网络关闭前执行：线程池中的任务
        //    handler 可能访问 network / capability 等共享资源。
        self.task_dispatcher.shutdown();

        // 7. 关闭网络（iroh endpoint.close()）。必须在所有 Actor / 后台循环停止后
        //    执行：否则 Endpoint 被无声 drop 会触发 iroh 的
        //    "Endpoint dropped without calling Endpoint::close. Aborting ungracefully."
        //    警告，并可能中断在途 gossip/直连请求。
        let network_result = self.network.shutdown().await;

        // 8. 释放进程级 wire 签名注册。同进程后续 Runtime 可能禁用签名；
        //    若本 Runtime 的 primary 密钥残留，会强制它们出站带 MAC，
        //    而对端（尤其是独立进程）无法验证，导致消息被丢弃。
        crate::common::register_wire_signing_key(&self.node_id, Vec::new());

        network_result
            .map_err(|e| ActantError::Network(format!("failed to shutdown network: {e}")))?;

        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../../tests/rust/unit/runtime/context.rs"]
mod tests;
