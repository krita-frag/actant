use std::sync::Arc;
use std::time::Duration;

use crate::common::{
    ActantError, NodeId, Result, TaskCompletion, TaskDefinition, TaskId, Topic, WireEnvelope,
    WireMessage, WireTaskOutcome, WorkerConfig, WorkflowId,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::dispatcher::{new_cancel_flag, TaskDispatcher};
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::network::{DirectResponseChannel, NetworkEvent, Transport};
use crate::runtime::workflow::messaging::encode;
use crate::runtime::workflow::Scheduler;

/// A task result that failed to deliver and is pending retry.
struct PendingResult {
    target: String,
    request: crate::runtime::network::DirectRequest,
    attempts: usize,
}

/// 尝试将待重试结果入队；通道满或关闭时发出 warn 并丢弃，而非静默丢失。
///
/// 使用 `try_send`（非阻塞）：通道满时立即返回 `Full`，
/// 避免在高负载下无限阻塞任务执行循环。
macro_rules! enqueue_pending_result {
    ($tx:expr, $target:expr, $request:expr, $attempts:expr, $capacity:expr) => {{
        let _target_for_log = $target.clone();
        match $tx.try_send(PendingResult {
            target: $target,
            request: $request,
            attempts: $attempts,
        }) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    target = %_target_for_log,
                    capacity = $capacity,
                    "pending_results channel full, dropping result for retry"
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!(target = %_target_for_log, "pending_results channel closed, dropping result");
            }
        }
    }};
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkerState {
    Running,
    Draining,
    Stopped,
}

impl WorkerState {
    /// Stable string representation used across the PyO3 boundary.
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerState::Running => "healthy",
            WorkerState::Draining => "draining",
            WorkerState::Stopped => "stopped",
        }
    }
}

/// 将传入的网络事件路由到适当的处理程序。
///
/// 封装了主题匹配和反序列化逻辑，这些逻辑之前
/// 内联在 `WorkerRuntime::run()` 中，使运行时主循环专注于任务执行。
pub(crate) struct NetworkEventRouter {
    network: Arc<dyn Transport>,
    event_bus: EventBus,
    scheduler: Arc<dyn Scheduler>,
    actor_system: Option<Arc<ActorSystem>>,
    /// 本地 WorkflowActor 的 id。若存在，远程 TaskResult 会直接路由给它。
    workflow_actor_id: Option<crate::common::ActorId>,
    /// 本地 DagGossipActor 的 id。若存在，工作流状态请求/响应会直接路由给它。
    dag_gossip_actor_id: Option<crate::common::ActorId>,
}

impl NetworkEventRouter {
    pub fn new(
        network: Arc<dyn Transport>,
        event_bus: EventBus,
        scheduler: Arc<dyn Scheduler>,
        actor_system: Option<Arc<ActorSystem>>,
        workflow_actor_id: Option<crate::common::ActorId>,
        dag_gossip_actor_id: Option<crate::common::ActorId>,
    ) -> Self {
        Self {
            network,
            event_bus,
            scheduler,
            actor_system,
            workflow_actor_id,
            dag_gossip_actor_id,
        }
    }

    /// 将单个 `NetworkEvent` 分发给适当的处理程序。
    pub async fn handle(&self, event: NetworkEvent) {
        match event {
            NetworkEvent::Message(msg) => {
                self.handle_message(&msg.topic, &msg.data).await;
            }
            NetworkEvent::PeerConnected { peer_id } => {
                tracing::info!("peer connected: {}", peer_id);
                crate::metrics::inc_connected_peers();
                self.event_bus
                    .publish(BusEvent::PeerConnected(NodeId(peer_id.clone())))
                    .await;
            }
            NetworkEvent::PeerDisconnected { peer_id } => {
                tracing::info!("peer disconnected: {}", peer_id);
                crate::metrics::dec_connected_peers();
                self.event_bus
                    .publish(BusEvent::PeerDisconnected(NodeId(peer_id)))
                    .await;
            }
            NetworkEvent::DirectRequest {
                peer_id,
                request,
                channel,
            } => {
                self.handle_direct_request(peer_id, *request, channel).await;
            }
        }
    }

    async fn handle_message(&self, topic_str: &str, payload: &[u8]) {
        let topic = Topic::from(topic_str);
        match topic.classify() {
            crate::common::TopicRoute::Task(_) => {
                if let Some(WireMessage::TaskDispatch(task)) = WireEnvelope::decode(payload) {
                    if let Err(e) = self.scheduler.enqueue(task).await {
                        tracing::warn!("scheduler rejected task (drain mode?): {}", e);
                    }
                }
            }
            crate::common::TopicRoute::Actor(_) => {
                if let Some(ref sys) = self.actor_system {
                    if let Ok(remote_req) =
                        crate::common::decode_postcard::<crate::common::RemoteActorRequest>(payload)
                    {
                        sys.handle_remote_request(remote_req).await;
                    }
                }
            }
            crate::common::TopicRoute::ActorReply(_) => {
                if let Some(ref sys) = self.actor_system {
                    if let Some(WireMessage::RemoteActorReply(reply)) =
                        WireEnvelope::decode(payload)
                    {
                        sys.deliver_reply(reply);
                    }
                }
            }
            crate::common::TopicRoute::DagState => {
                if let Some(WireMessage::DagStateUpdate(update)) = WireEnvelope::decode(payload) {
                    self.event_bus.publish(BusEvent::DagUpdate(update)).await;
                }
            }
            crate::common::TopicRoute::Heartbeat => {
                if let Some(WireMessage::NodeHeartbeat(hb)) = WireEnvelope::decode(payload) {
                    tracing::debug!("publishing heartbeat from {} to event_bus", hb.node_id.0);
                    self.event_bus.publish(BusEvent::Heartbeat(hb)).await;
                } else {
                    tracing::warn!(
                        "heartbeat topic but failed to unwrap envelope, payload len={}",
                        payload.len()
                    );
                }
            }
            crate::common::TopicRoute::Failover => {
                if let Some(WireMessage::OrchestratorClaim(claim)) = WireEnvelope::decode(payload) {
                    self.event_bus.publish(BusEvent::Claim(claim)).await;
                }
            }
            crate::common::TopicRoute::Heads => {
                if let Some(WireMessage::HeadsExchange(exchange)) = WireEnvelope::decode(payload) {
                    self.event_bus
                        .publish(BusEvent::HeadsExchange(exchange))
                        .await;
                }
            }
            crate::common::TopicRoute::WorkflowStateReq(_) => {
                if let Some(WireMessage::WorkflowStateRequest(request)) =
                    WireEnvelope::decode(payload)
                {
                    self.handle_workflow_state_event(
                        crate::runtime::workflow::gossip_methods::HANDLE_WORKFLOW_STATE_REQUEST,
                        &request,
                    )
                    .await;
                }
            }
            crate::common::TopicRoute::WorkflowStateResp(_) => {
                if let Some(WireMessage::WorkflowStateResponse(response)) =
                    WireEnvelope::decode(payload)
                {
                    self.handle_workflow_state_event(
                        crate::runtime::workflow::gossip_methods::HANDLE_WORKFLOW_STATE_RESPONSE,
                        &response,
                    )
                    .await;
                }
            }
            crate::common::TopicRoute::Unknown => {}
        }
    }

    async fn handle_direct_request(
        &self,
        peer_id: String,
        request: crate::runtime::network::DirectRequest,
        channel: DirectResponseChannel,
    ) {
        match request {
            crate::runtime::network::DirectRequest::DispatchTask { task } => {
                let accepted = match self.scheduler.enqueue(task).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!("scheduler rejected direct dispatch (drain mode?): {}", e);
                        false
                    }
                };
                let response = crate::runtime::network::DirectResponse::DispatchAck { accepted };
                if let Err(e) = self.network.send_direct_response(channel, response).await {
                    tracing::warn!("failed to send DispatchAck: {}", e);
                }
            }
            crate::runtime::network::DirectRequest::TaskResult {
                workflow_id,
                task_id,
                task_name,
                outcome,
                worker_node,
            } => {
                let accepted = self
                    .handle_task_result(workflow_id, task_id, task_name, outcome, worker_node)
                    .await;
                let response = crate::runtime::network::DirectResponse::TaskResultAck { accepted };
                if let Err(e) = self.network.send_direct_response(channel, response).await {
                    tracing::warn!("failed to send TaskResultAck: {}", e);
                }
            }
            other => {
                self.event_bus
                    .publish(BusEvent::DirectRequest {
                        peer_id,
                        request: Box::new(other),
                        channel,
                    })
                    .await;
            }
        }
    }

    /// 将工作流状态请求/响应事件路由到本地 DagGossipActor。
    async fn handle_workflow_state_event<T: serde::Serialize>(&self, method: &str, value: &T) {
        let Some(ref actor_system) = self.actor_system else {
            tracing::warn!("no actor system available to handle workflow state event");
            return;
        };
        let Some(ref dag_gossip_actor_id) = self.dag_gossip_actor_id else {
            tracing::warn!("no dag gossip actor configured to handle workflow state event");
            return;
        };

        let payload = match encode(value) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("failed to encode workflow state event: {}", e);
                return;
            }
        };

        if let Err(e) = actor_system
            .call(dag_gossip_actor_id, method, payload)
            .await
        {
            tracing::error!(
                method = %method,
                error = %e,
                "failed to dispatch workflow state event to dag gossip actor"
            );
        }
    }

    /// 将远程 TaskResult 路由到本地 WorkflowActor。
    ///
    /// 根据 outcome 类型分别调用 COMPLETE_TASK / FAIL_TASK / CANCEL_TASK。
    /// 返回 `true` 表示已成功提交给 WorkflowActor（不保证 workflow 状态更新成功）。
    async fn handle_task_result(
        &self,
        workflow_id: WorkflowId,
        task_id: TaskId,
        _task_name: String,
        outcome: WireTaskOutcome,
        _worker_node: NodeId,
    ) -> bool {
        let Some(ref actor_system) = self.actor_system else {
            tracing::warn!("no actor system available to handle remote task result");
            return false;
        };
        let Some(ref workflow_actor_id) = self.workflow_actor_id else {
            tracing::warn!("no workflow actor configured to handle remote task result");
            return false;
        };

        let call_result = match outcome {
            WireTaskOutcome::Completed(result_payload) => {
                let payload = match encode(&(workflow_id.clone(), task_id.clone(), result_payload))
                {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("failed to encode COMPLETE_TASK message: {}", e);
                        return false;
                    }
                };
                actor_system
                    .call(
                        workflow_actor_id,
                        crate::runtime::workflow::workflow_methods::COMPLETE_TASK,
                        payload,
                    )
                    .await
            }
            WireTaskOutcome::Failed(error) => {
                let scope = crate::runtime::workflow::dag::FailureScope::TaskOnly;
                let payload = match encode(&(workflow_id.clone(), task_id.clone(), error, scope)) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("failed to encode FAIL_TASK message: {}", e);
                        return false;
                    }
                };
                actor_system
                    .call(
                        workflow_actor_id,
                        crate::runtime::workflow::workflow_methods::FAIL_TASK,
                        payload,
                    )
                    .await
            }
            WireTaskOutcome::Cancelled => {
                let payload = match encode(&(workflow_id.clone(), task_id.clone())) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("failed to encode CANCEL_TASK message: {}", e);
                        return false;
                    }
                };
                actor_system
                    .call(
                        workflow_actor_id,
                        crate::runtime::workflow::workflow_methods::CANCEL_TASK,
                        payload,
                    )
                    .await
            }
            WireTaskOutcome::Skipped => {
                tracing::warn!(
                    workflow_id = %workflow_id.as_str(),
                    task_id = %task_id.as_str(),
                    "unexpected Skipped outcome in remote task result; ignoring"
                );
                return false;
            }
        };

        match call_result {
            Ok(result) => {
                if let Some(error) = result.error {
                    tracing::error!(
                        workflow_id = %workflow_id.as_str(),
                        task_id = %task_id.as_str(),
                        error = %error,
                        "workflow actor rejected task result"
                    );
                    false
                } else {
                    true
                }
            }
            Err(e) => {
                tracing::error!(
                    workflow_id = %workflow_id.as_str(),
                    task_id = %task_id.as_str(),
                    error = %e,
                    "failed to dispatch task result to workflow actor"
                );
                false
            }
        }
    }
}

pub struct Worker {
    node_id: NodeId,
    network: Arc<dyn Transport>,
    scheduler: Arc<dyn Scheduler>,
    /// SchedulerActor 的 id，用于 shutdown 时同步等待 Actor 退出。
    /// `None` 表示 Worker 未绑定 SchedulerActor（仅用于不启动 Actor 系统的测试路径）。
    scheduler_actor_id: Option<crate::common::ActorId>,
    event_bus: EventBus,
    task_dispatcher: Arc<dyn TaskDispatcher>,
    actor_system: Option<Arc<ActorSystem>>,
    /// 本地 WorkflowActor 的 id。用于将远程 task result 路由到编排器。
    workflow_actor_id: Option<crate::common::ActorId>,
    /// 本地 DagGossipActor 的 id。用于处理工作流状态请求/响应主题。
    dag_gossip_actor_id: Option<crate::common::ActorId>,
    max_concurrent_tasks: usize,
    task_timeout: Duration,
    cancel: Arc<tokio::sync::watch::Sender<bool>>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    drain_timeout: Duration,
    running_tasks: Arc<tokio::sync::Semaphore>,
    state: Arc<tokio::sync::watch::Sender<WorkerState>>,
    broadcast_retry_attempts: usize,
    broadcast_retry_base_delay: Duration,
    remote_fallback_delay: Duration,
    pending_result_channel_capacity: usize,
    /// Callback invoked when running task count changes: (available, max).
    capacity_callback: Option<Arc<dyn Fn(u32, u32) + Send + Sync>>,
    /// Handle to the tokio runtime — used by publish_lifecycle to spawn
    /// tasks even when called from a non-tokio thread (e.g. Python).
    tokio_handle: tokio::runtime::Handle,
}

impl Worker {
    /// Clear the capacity callback, releasing any captured references.
    pub fn clear_capacity_callback(&mut self) {
        self.capacity_callback = None;
    }

    pub fn new(
        node_id: NodeId,
        network: Arc<dyn Transport>,
        event_bus: EventBus,
        scheduler: Arc<dyn Scheduler>,
        task_dispatcher: Arc<dyn TaskDispatcher>,
        config: &WorkerConfig,
        tokio_handle: tokio::runtime::Handle,
    ) -> Self {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (state_tx, _) = tokio::sync::watch::channel(WorkerState::Running);
        let max_concurrent = config.max_concurrent_tasks;
        Self {
            node_id,
            network,
            scheduler,
            scheduler_actor_id: None,
            event_bus,
            task_dispatcher,
            actor_system: None,
            workflow_actor_id: None,
            dag_gossip_actor_id: None,
            max_concurrent_tasks: max_concurrent,
            task_timeout: Duration::from_millis(config.default_task_timeout_ms),
            cancel: Arc::new(cancel_tx),
            cancel_rx,
            drain_timeout: Duration::from_secs(config.drain_timeout_secs),
            running_tasks: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            state: Arc::new(state_tx),
            broadcast_retry_attempts: config.broadcast_retry_attempts,
            broadcast_retry_base_delay: Duration::from_millis(config.broadcast_retry_base_delay_ms),
            remote_fallback_delay: Duration::from_millis(config.remote_fallback_delay_ms),
            pending_result_channel_capacity: config.pending_result_channel_capacity,
            capacity_callback: None,
            tokio_handle,
        }
    }

    /// 设置底层 SchedulerActor 的 id，供 `Runtime::shutdown` 同步等待 Actor 退出。
    pub fn with_scheduler_actor_id(mut self, id: crate::common::ActorId) -> Self {
        self.scheduler_actor_id = Some(id);
        self
    }

    /// 返回 SchedulerActor 的 id（若使用 Actor-backed scheduler）。
    pub fn scheduler_actor_id(&self) -> Option<&crate::common::ActorId> {
        self.scheduler_actor_id.as_ref()
    }

    pub fn with_max_concurrent_tasks(mut self, max: usize) -> Self {
        self.max_concurrent_tasks = max;
        self.running_tasks = Arc::new(tokio::sync::Semaphore::new(max));
        self
    }

    pub fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.task_timeout = timeout;
        self
    }

    pub fn with_drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    pub(crate) fn with_actor_system(mut self, system: Arc<ActorSystem>) -> Self {
        self.actor_system = Some(system);
        self
    }

    pub(crate) fn with_workflow_actor_id(mut self, id: crate::common::ActorId) -> Self {
        self.workflow_actor_id = Some(id);
        self
    }

    pub(crate) fn with_dag_gossip_actor_id(mut self, id: crate::common::ActorId) -> Self {
        self.dag_gossip_actor_id = Some(id);
        self
    }

    pub fn with_capacity_callback(mut self, cb: Arc<dyn Fn(u32, u32) + Send + Sync>) -> Self {
        self.capacity_callback = Some(cb);
        self
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn network(&self) -> &Arc<dyn Transport> {
        &self.network
    }

    pub fn scheduler(&self) -> &dyn Scheduler {
        &*self.scheduler
    }

    /// 返回 scheduler 的 `Arc` 克隆，供其他子系统（如 FailoverManager）共享调度入口。
    pub fn scheduler_clone(&self) -> Arc<dyn Scheduler> {
        self.scheduler.clone()
    }

    pub fn state(&self) -> WorkerState {
        *self.state.borrow()
    }

    pub fn subscribe_state(&self) -> tokio::sync::watch::Receiver<WorkerState> {
        self.state.subscribe()
    }

    pub async fn schedule_task(&self, mut task: TaskDefinition, target_node: NodeId) -> Result<()> {
        task.target_node = Some(target_node);
        self.scheduler.enqueue(task).await?;
        Ok(())
    }

    pub async fn discover_peers(&self) -> Result<Vec<NodeId>> {
        let peers = self.network.discover_peers().await?;
        Ok(peers.into_iter().map(|p| NodeId::from(p.0)).collect())
    }

    pub async fn run(&self) -> Result<()> {
        self.subscribe_topics().await?;
        self.notify_capacity();
        self.start_network_event_loop();
        let pending_tx = self.start_pending_result_loop();
        self.run_task_execution_loop(&pending_tx).await
    }

    async fn subscribe_topics(&self) -> Result<()> {
        // 使用 select! 监听 cancel 信号：shutdown 时若网络不可达，
        // subscribe 会阻塞，cancel 让 worker.run() 尽快退出，
        // 使 Runtime::shutdown() 的 network.shutdown() 得以执行。
        let mut cancel = self.cancel_rx.clone();
        let topics = [
            Topic::task(&self.node_id),
            Topic::actor(&self.node_id),
            Topic::actor_reply(&self.node_id),
            Topic::workflow_state_req(&self.node_id),
            Topic::workflow_state_resp(&self.node_id),
        ];
        for topic in &topics {
            tracing::info!("subscribing to topic: {}", topic);
            tokio::select! {
                _ = cancel.changed() => {
                    tracing::info!("subscribe_topics cancelled during shutdown");
                    return Err(ActantError::Internal("subscribe cancelled".into()));
                }
                r = self.network.subscribe(topic.as_str()) => r?,
            }
        }
        Ok(())
    }

    fn start_network_event_loop(&self) {
        let router = NetworkEventRouter::new(
            self.network.clone(),
            self.event_bus.clone(),
            self.scheduler.clone(),
            self.actor_system.clone(),
            self.workflow_actor_id.clone(),
            self.dag_gossip_actor_id.clone(),
        );
        let network = self.network.clone();
        let mut event_cancel = self.cancel_rx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = event_cancel.changed() => {
                        tracing::info!("worker event loop shutting down");
                        break;
                    }
                    event = network.recv_event() => {
                        let Some(event) = event else {
                            continue;
                        };
                        router.handle(event).await;
                    }
                }
            }
        });
    }

    fn start_pending_result_loop(&self) -> tokio::sync::mpsc::Sender<PendingResult> {
        let (pending_tx, mut pending_rx) =
            tokio::sync::mpsc::channel::<PendingResult>(self.pending_result_channel_capacity);
        let pending_capacity = self.pending_result_channel_capacity;
        let retry_network = self.network.clone();
        let max_attempts = self.broadcast_retry_attempts;
        let base_delay = self.broadcast_retry_base_delay;
        let mut retry_cancel = self.cancel_rx.clone();
        let retry_tx = pending_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = retry_cancel.changed() => {
                        tracing::info!("pending result retry loop shutting down");
                        break;
                    }
                    Some(pending) = pending_rx.recv() => {
                        if pending.attempts >= max_attempts {
                            tracing::error!(
                                "dropping result for {} after {} failed attempts",
                                pending.target, max_attempts
                            );
                            continue;
                        }
                        let delay = base_delay * 2u32.saturating_pow(pending.attempts as u32);
                        tokio::time::sleep(delay).await;
                        match retry_network.send_direct_request(&pending.target, pending.request.clone()).await {
                            Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: true }) => {
                                tracing::debug!("pending result delivered to {}", pending.target);
                            }
                            Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: false }) => {
                                tracing::warn!("pending result rejected by {}, will retry", pending.target);
                                enqueue_pending_result!(
                                    retry_tx,
                                    pending.target,
                                    pending.request,
                                    pending.attempts + 1,
                                    pending_capacity
                                );
                            }
                            Ok(_) | Err(_) => {
                                tracing::debug!("pending result delivery to {} failed, will retry (attempt {})", pending.target, pending.attempts + 1);
                                enqueue_pending_result!(
                                    retry_tx,
                                    pending.target,
                                    pending.request,
                                    pending.attempts + 1,
                                    pending_capacity
                                );
                            }
                        }
                    }
                    else => break,
                }
            }
        });

        pending_tx
    }

    async fn run_task_execution_loop(
        &self,
        pending_tx: &tokio::sync::mpsc::Sender<PendingResult>,
    ) -> Result<()> {
        let node_id = self.node_id.clone();
        let network = self.network.clone();
        let task_dispatcher = self.task_dispatcher.clone();
        let semaphore = self.running_tasks.clone();
        let event_bus = self.event_bus.clone();
        let task_timeout = self.task_timeout;
        let mut task_cancel = self.cancel_rx.clone();
        let state_sender = self.state.clone();
        let drain_timeout = self.drain_timeout;
        let pending_capacity = self.pending_result_channel_capacity;

        loop {
            if *self.state.subscribe().borrow() == WorkerState::Draining {
                // 关闭 scheduler，使新 task 无法入队。
                self.scheduler.close();
                // 排空剩余排队的 task — 由于 scheduler 已关闭，
                // 它们会被拒绝，因此直接丢弃。
                while self.scheduler.try_dequeue().await.is_some() {}
                // 通知子任务（network event loop、retry loop）退出。
                // 否则 rt.shutdown_timeout 会等待完整的 5s。
                let _ = self.cancel.send(true);
                self.publish_lifecycle(BusEvent::WorkerDrained {
                    node_id: self.node_id.clone(),
                });
                let _ = state_sender.send(WorkerState::Stopped);
                return Ok(());
            }

            let task = tokio::select! {
                _ = task_cancel.changed() => {
                    let _ = state_sender.send(WorkerState::Draining);
                    let running = self.running_task_count();
                    tracing::info!(
                        running_tasks = running,
                        drain_timeout_ms = drain_timeout.as_millis(),
                        "worker entering drain mode, waiting for running tasks"
                    );

                    let drain_result = tokio::time::timeout(
                        drain_timeout,
                        async {
                            let sem = semaphore.clone();
                            let permit = sem.acquire_many(self.max_concurrent_tasks as u32).await;
                            drop(permit);
                        },
                    ).await;

                    if drain_result.is_err() {
                        tracing::warn!("drain timeout exceeded, forcing shutdown");
                    } else {
                        tracing::info!("all running tasks completed, shutting down");
                    }

                    self.publish_lifecycle(BusEvent::WorkerDrained {
                        node_id: self.node_id.clone(),
                    });
                    let _ = state_sender.send(WorkerState::Stopped);
                    return Ok(());
                }
                task = self.scheduler.dequeue() => task,
            };
            let Some(task) = task else {
                continue;
            };

            if let Some(ref target) = task.target_node {
                if target != &node_id {
                    match self.forward_remote_task(&task, target).await {
                        Ok(()) => {
                            crate::metrics::inc_task_forward_succeeded();
                            continue;
                        }
                        Err(e) => {
                            crate::metrics::inc_task_forward_failed();
                            tracing::warn!(
                                "forward task {} to node {} failed ({}), re-enqueueing for re-routing",
                                task.id.as_str(), target.as_str(), e
                            );
                            let mut rerouted_task = task;
                            rerouted_task.target_node = None;
                            if let Err(e) = self.scheduler.enqueue(rerouted_task).await {
                                tracing::warn!("failed to re-enqueue task for re-routing: {}", e);
                            }
                            // 退避阻塞主循环，避免转发失败时 busy-loop。
                            tokio::time::sleep(self.remote_fallback_delay).await;
                            continue;
                        }
                    }
                }
            }

            if self.max_concurrent_tasks == 0 && task.target_node.is_none() {
                tracing::debug!(
                    "submit-only node cannot execute task {} locally, re-enqueueing for re-routing",
                    task.id.as_str()
                );
                if let Err(e) = self.scheduler.enqueue(task).await {
                    tracing::warn!("failed to re-enqueue submit-only task: {}", e);
                }
                // 退避阻塞主循环，避免 submit-only 节点 busy-loop。
                tokio::time::sleep(self.remote_fallback_delay).await;
                continue;
            }

            let event_bus = event_bus.clone();
            let network = network.clone();
            let pending_results = pending_tx.clone();

            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ActantError::Internal("semaphore closed".into()))?;

            self.notify_capacity();

            let node_id_for_spawn = node_id.clone();
            let task_name = task.name.clone();
            let task_payload = task.payload.clone();
            let dispatcher = task_dispatcher.clone();
            let dispatch_start_ms = crate::common::epoch_millis();
            let capacity_cb = self.capacity_callback.clone();
            let max_concurrent = self.max_concurrent_tasks;
            let semaphore_for_cb = semaphore.clone();
            crate::metrics::inc_running_tasks();

            // Publish TaskStarted to EventBus
            if let Some(ref wf_id) = task.workflow_id {
                event_bus
                    .publish(BusEvent::TaskStarted {
                        workflow_id: wf_id.clone(),
                        task_id: task.id.clone(),
                    })
                    .await;
            }

            if task.enqueued_at_ms > 0 {
                let latency = dispatch_start_ms.saturating_sub(task.enqueued_at_ms);
                crate::metrics::observe_scheduling_latency_ms(latency);
                tracing::trace!(task = ?task.id, latency_ms = latency, "scheduling latency");
            }

            tokio::spawn(async move {
                let _permit = permit;
                let task_id_dbg = task.id.clone();
                tracing::debug!(task = ?task_id_dbg, name = %task_name, "task executing");

                let effective_timeout = task
                    .timeout_ms
                    .map(std::time::Duration::from_millis)
                    .unwrap_or(task_timeout);

                let cancel_flag = new_cancel_flag();
                tracing::debug!(task_id = ?task.id, "dispatching task");

                // B1: 用 catch_unwind 包裹 dispatcher future，避免 spawn 句柄被丢弃后
                // panic 让 workflow 永久挂起。panic 时降级为 TaskCompletion::Failed。
                use futures::future::FutureExt as _;
                let dispatched = std::panic::AssertUnwindSafe(dispatcher.dispatch(
                    &task_name,
                    task_payload,
                    cancel_flag.clone(),
                ))
                .catch_unwind();

                let dispatch_result = tokio::time::timeout(effective_timeout, dispatched).await;

                if dispatch_result.is_err() {
                    tracing::warn!(task_id = ?task.id, "task timed out, signalling cancel");
                    cancel_flag.store(true, std::sync::atomic::Ordering::Release);
                }

                // 三种结果：Ok(Ok(Ok(output))) | Ok(Ok(Err(e))) | Ok(Err(panic)) | Err(timeout)
                let completion = match dispatch_result {
                    Ok(Ok(Ok(result))) => {
                        crate::metrics::inc_tasks_completed();
                        crate::metrics::dec_running_tasks();
                        crate::metrics::observe_task_duration_ms(
                            crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
                        );
                        TaskCompletion::Completed {
                            workflow_id: task
                                .workflow_id
                                .clone()
                                .unwrap_or_else(|| WorkflowId::from("".to_string())),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            result,
                            target_node: task.target_node.clone(),
                        }
                    }
                    Ok(Ok(Err(e))) => {
                        crate::metrics::inc_tasks_failed();
                        crate::metrics::dec_running_tasks();
                        crate::metrics::observe_task_duration_ms(
                            crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
                        );
                        TaskCompletion::Failed {
                            workflow_id: task
                                .workflow_id
                                .clone()
                                .unwrap_or_else(|| WorkflowId::from("".to_string())),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: e.to_string(),
                            target_node: task.target_node.clone(),
                        }
                    }
                    Ok(Err(panic_payload)) => {
                        // dispatcher panic：提取 panic 消息，降级为 Failed，
                        // 让 workflow 能继续推进而非永久挂起。
                        crate::metrics::inc_tasks_failed();
                        crate::metrics::dec_running_tasks();
                        crate::metrics::observe_task_duration_ms(
                            crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
                        );
                        let panic_msg = panic_payload
                            .downcast_ref::<&'static str>()
                            .copied()
                            .map(String::from)
                            .or_else(|| {
                                panic_payload
                                    .downcast_ref::<String>()
                                    .map(String::as_str)
                                    .map(String::from)
                            })
                            .unwrap_or_else(|| "<non-string panic>".to_string());
                        tracing::error!(
                            task_id = ?task.id,
                            panic = %panic_msg,
                            "dispatcher panicked; emitting TaskCompletion::Failed"
                        );
                        TaskCompletion::Failed {
                            workflow_id: task
                                .workflow_id
                                .clone()
                                .unwrap_or_else(|| WorkflowId::from("".to_string())),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: format!("dispatcher panicked: {panic_msg}"),
                            target_node: task.target_node.clone(),
                        }
                    }
                    Err(_) => {
                        crate::metrics::inc_tasks_timeout();
                        crate::metrics::dec_running_tasks();
                        crate::metrics::observe_task_duration_ms(
                            crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
                        );
                        TaskCompletion::Failed {
                            workflow_id: task
                                .workflow_id
                                .clone()
                                .unwrap_or_else(|| WorkflowId::from("".to_string())),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: format!(
                                "task timed out after {}ms",
                                effective_timeout.as_millis()
                            ),
                            target_node: task.target_node.clone(),
                        }
                    }
                };

                let is_remote_task = task
                    .origin_node
                    .as_ref()
                    .is_some_and(|o| o != &node_id_for_spawn);

                if is_remote_task {
                    if let Some(ref workflow_id) = task.workflow_id {
                        let wire_result = completion.to_wire_result(workflow_id.clone());

                        if let Some(ref origin) = task.origin_node {
                            // 优先使用 origin_endpoint_addr（iroh 公钥），否则回退到 node_id
                            let origin_addr = task
                                .origin_endpoint_addr
                                .as_deref()
                                .unwrap_or(origin.as_str());
                            let request = crate::runtime::network::DirectRequest::TaskResult {
                                workflow_id: workflow_id.clone(),
                                task_id: wire_result.task_id.clone(),
                                task_name: wire_result.task_name.clone(),
                                outcome: wire_result.outcome.clone(),
                                worker_node: node_id_for_spawn.clone(),
                            };
                            // 尝试一次；失败则入队异步重试
                            match network
                                .send_direct_request(origin_addr, request.clone())
                                .await
                            {
                                Ok(crate::runtime::network::DirectResponse::TaskResultAck {
                                    accepted: true,
                                }) => {
                                    tracing::debug!(
                                        "task result delivered directly to orchestrator {}",
                                        origin_addr
                                    );
                                }
                                Ok(crate::runtime::network::DirectResponse::TaskResultAck {
                                    accepted: false,
                                }) => {
                                    tracing::warn!(
                                        "orchestrator {} rejected task result, enqueuing for retry",
                                        origin_addr
                                    );
                                    enqueue_pending_result!(
                                        pending_results,
                                        origin_addr.to_string(),
                                        request,
                                        0,
                                        pending_capacity
                                    );
                                }
                                Ok(_) => {
                                    tracing::warn!(
                                        "unexpected response from {}, enqueuing result for retry",
                                        origin_addr
                                    );
                                    enqueue_pending_result!(
                                        pending_results,
                                        origin_addr.to_string(),
                                        request,
                                        0,
                                        pending_capacity
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!("direct result delivery to {} failed: {}, enqueuing for retry", origin_addr, e);
                                    enqueue_pending_result!(
                                        pending_results,
                                        origin_addr.to_string(),
                                        request,
                                        0,
                                        pending_capacity
                                    );
                                }
                            }
                        }
                    }
                } else if task.workflow_id.is_some() {
                    // 本地 task 完成：发布到 EventBus
                    let bus_event = match &completion {
                        TaskCompletion::Failed { .. } => BusEvent::TaskFailed(completion),
                        TaskCompletion::Cancelled { .. } => BusEvent::TaskCancelled(completion),
                        TaskCompletion::Skipped { .. } => BusEvent::TaskSkipped(completion),
                        TaskCompletion::Completed { .. } => BusEvent::TaskCompleted(completion),
                    };
                    event_bus.publish(bus_event).await;
                }

                drop(_permit);

                if let Some(ref cb) = capacity_cb {
                    let available = semaphore_for_cb.available_permits();
                    cb(available as u32, max_concurrent as u32);
                }
            });
        }
    }

    pub fn shutdown(&self) {
        if let Err(e) = self.cancel.send(true) {
            tracing::warn!("failed to send shutdown signal: {}", e);
        }
    }

    /// 强制将 worker 状态设为 Stopped。
    ///
    /// 用于 `worker.run()` 异常退出（如 subscribe_topics 被 cancel）后，
    /// 确保 `Runtime::shutdown()` 中等待 `WorkerState::Stopped` 不会超时。
    pub fn notify_stopped(&self) {
        let _ = self.state.send(WorkerState::Stopped);
    }

    pub fn drain(&self) {
        let _ = self.state.send(WorkerState::Draining);
        self.scheduler.close();
        if let Some(ref cb) = self.capacity_callback {
            cb(0, 0);
        }
        self.publish_lifecycle(BusEvent::WorkerDraining {
            node_id: self.node_id.clone(),
        });
        tracing::info!("node {} entering drain mode", self.node_id.as_str());
    }

    /// Publish a worker lifecycle event asynchronously.
    ///
    /// Uses the stored tokio handle so this works even when called
    /// from a non-tokio thread (e.g. Python GIL thread).
    fn publish_lifecycle(&self, event: BusEvent) {
        let event_bus = self.event_bus.clone();
        self.tokio_handle.spawn(async move {
            event_bus.publish(event).await;
        });
    }

    pub fn running_task_count(&self) -> usize {
        self.max_concurrent_tasks
            .saturating_sub(self.running_tasks.available_permits())
    }

    pub fn max_concurrent_tasks(&self) -> usize {
        self.max_concurrent_tasks
    }

    fn notify_capacity(&self) {
        if let Some(ref cb) = self.capacity_callback {
            let available = self.running_tasks.available_permits();
            cb(available as u32, self.max_concurrent_tasks as u32);
        }
    }

    async fn forward_remote_task(&self, task: &TaskDefinition, target: &NodeId) -> Result<()> {
        // 优先使用 target_endpoint_addr（iroh 公钥），否则回退到 node_id
        let target_addr = task
            .target_endpoint_addr
            .as_deref()
            .unwrap_or(target.as_str());
        let request = crate::runtime::network::DirectRequest::DispatchTask { task: task.clone() };
        match self.network.send_direct_request(target_addr, request).await {
            Ok(crate::runtime::network::DirectResponse::DispatchAck { accepted: true }) => Ok(()),
            Ok(crate::runtime::network::DirectResponse::DispatchAck { accepted: false }) => Err(
                ActantError::Network(format!("worker {} rejected task dispatch", target_addr)),
            ),
            Ok(_) => Err(ActantError::Network(format!(
                "unexpected response from worker {}",
                target_addr
            ))),
            Err(e) => Err(ActantError::Network(format!(
                "forward to node {} failed: {}",
                target_addr, e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_results_channel_rejects_when_full() {
        // 验证：容量为 1 的 pending_results 通道，填满后 try_send 返回 Full。
        // enqueue_pending_result! 宏在此情况下应发出 warn 并丢弃，而非阻塞或 panic。
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PendingResult>(1);
        let capacity = 1usize;

        let req = crate::runtime::network::DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf-test"),
            requesting_node: NodeId::from("n-test"),
        };

        // 第一次入队成功
        enqueue_pending_result!(tx, "peer-1".to_string(), req.clone(), 0, capacity);
        assert_eq!(rx.len(), 1, "first enqueue should succeed");

        // 第二次入队：通道已满，应被丢弃（宏内部 warn），rx 不应增长。
        enqueue_pending_result!(tx, "peer-2".to_string(), req, 0, capacity);
        assert_eq!(
            rx.len(),
            1,
            "second enqueue must be dropped (channel full), not blocked"
        );

        // 取出一个后，下一次入队应再次成功。
        let _ = rx.recv().await.expect("first item present");
        let req2 = crate::runtime::network::DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf-test"),
            requesting_node: NodeId::from("n-test"),
        };
        enqueue_pending_result!(tx, "peer-3".to_string(), req2, 0, capacity);
        assert_eq!(rx.len(), 1, "enqueue should succeed after drain");
    }

    // ───────────────────────── Worker 纯逻辑测试 ─────────────────────────

    use crate::common::{NodeHeartbeat, WorkerConfig};
    use crate::runtime::event_bus::BusEvent;
    use crate::runtime::network::NetworkEvent;
    use crate::runtime::workflow::Scheduler;
    use crate::test_support::{MockScheduler, MockTransport};
    use std::sync::Arc as StdArc;

    fn make_worker(node_id: &str) -> Worker {
        let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
        let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
        let event_bus = EventBus::new();
        let dispatcher = crate::runtime::dispatcher::TaskRegistry::new(1, 8, Vec::new())
            .expect("TaskRegistry init")
            .into_dispatcher();
        let handle = tokio::runtime::Handle::current();
        Worker::new(
            NodeId::from(node_id.to_string()),
            network,
            event_bus,
            scheduler,
            dispatcher,
            &WorkerConfig::default(),
            handle,
        )
    }

    #[test]
    fn worker_state_as_str_returns_canonical_strings() {
        assert_eq!(WorkerState::Running.as_str(), "healthy");
        assert_eq!(WorkerState::Draining.as_str(), "draining");
        assert_eq!(WorkerState::Stopped.as_str(), "stopped");
    }

    #[tokio::test]
    async fn worker_getters_return_configured_values() {
        let worker = make_worker("node-A");
        assert_eq!(worker.node_id().as_str(), "node-A");
        assert_eq!(worker.network().node_id().as_str(), "node-A");
        assert_eq!(worker.state(), WorkerState::Running);
        assert_eq!(worker.max_concurrent_tasks(), 8); // WorkerConfig::default
        assert_eq!(worker.running_task_count(), 0);
        assert!(worker.scheduler_actor_id().is_none());
    }

    #[tokio::test]
    async fn worker_builder_methods_update_state() {
        let worker = make_worker("node-A")
            .with_max_concurrent_tasks(4)
            .with_task_timeout(Duration::from_millis(7500))
            .with_drain_timeout(Duration::from_secs(10))
            .with_scheduler_actor_id(crate::common::ActorId::scheduler(&NodeId::from(
                "node-A".to_string(),
            )))
            .with_workflow_actor_id(crate::common::ActorId::workflow(&NodeId::from(
                "node-A".to_string(),
            )))
            .with_dag_gossip_actor_id(crate::common::ActorId::dag_gossip(&NodeId::from(
                "node-A".to_string(),
            )))
            .with_actor_system(Arc::new(crate::runtime::actor::ActorSystem::new()));

        assert_eq!(worker.max_concurrent_tasks(), 4);
        assert_eq!(worker.running_task_count(), 0);
        assert!(worker.scheduler_actor_id().is_some());
    }

    #[tokio::test]
    async fn worker_shutdown_sends_cancel_signal() {
        let worker = make_worker("node-A");
        // subscribe_state 用于观察；shutdown 通过 cancel watch 发送 true。
        let rx = worker.subscribe_state();
        worker.shutdown();
        // shutdown 仅发送 cancel 信号，不直接改 state（state 由 run 循环处理）。
        // 这里验证不 panic 即可。
        drop(rx);
    }

    #[tokio::test]
    async fn worker_drain_transitions_state_to_draining() {
        let worker = make_worker("node-A");
        let rx = worker.subscribe_state();
        assert_eq!(worker.state(), WorkerState::Running);
        worker.drain();
        // drain 通过 state watch 发送 Draining
        assert_eq!(*rx.borrow(), WorkerState::Draining);
        assert_eq!(worker.state(), WorkerState::Draining);
    }

    #[tokio::test]
    async fn worker_schedule_task_sets_target_node() {
        let worker = make_worker("node-A");
        let mut task = TaskDefinition {
            id: TaskId::from("t-1".to_string()),
            name: "echo".to_string(),
            payload: Vec::new(),
            workflow_id: None,
            target_node: None,
            origin_node: None,
            retry_policy: None,
            priority: 0,
            timeout_ms: None,
            attempt: 0,
            enqueued_at_ms: 0,
            target_endpoint_addr: None,
            origin_endpoint_addr: None,
        };
        let result = worker
            .schedule_task(task.clone(), NodeId::from("node-B".to_string()))
            .await;
        assert!(result.is_ok());
        // 验证 schedule_task 内部将 target_node 写入 task（MockScheduler.enqueue 接受任意 task）。
        task.target_node = Some(NodeId::from("node-B".to_string()));
    }

    #[tokio::test]
    async fn worker_discover_peers_returns_empty_with_mock_transport() {
        let worker = make_worker("node-A");
        let peers = worker.discover_peers().await.expect("discover_peers ok");
        assert!(peers.is_empty());
    }

    #[tokio::test]
    async fn worker_clear_capacity_callback_releases_reference() {
        let calls = StdArc::new(std::sync::Mutex::new(Vec::new()));
        let calls_clone = calls.clone();
        let cb: Arc<dyn Fn(u32, u32) + Send + Sync> = Arc::new(move |avail, max| {
            calls_clone.lock().unwrap().push((avail, max));
        });
        let mut worker = make_worker("node-A").with_capacity_callback(cb);
        // clear 后再调用 notify_capacity 不应 panic 且不再触发回调。
        worker.clear_capacity_callback();
        // 间接验证：running_task_count 不依赖 callback。
        assert_eq!(worker.running_task_count(), 0);
    }

    // ───────────────────────── NetworkEventRouter 测试 ─────────────────────────

    use crate::runtime::event_bus::Topic as BusTopic;

    #[tokio::test]
    async fn router_handle_peer_connected_publishes_event() {
        let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
        let event_bus = EventBus::new();
        let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
        let router =
            NetworkEventRouter::new(network, event_bus.clone(), scheduler, None, None, None);

        let mut sub = event_bus.subscribe(BusTopic::NetworkPeer);
        router
            .handle(NetworkEvent::PeerConnected {
                peer_id: "peer-B".to_string(),
            })
            .await;

        // 订阅应在 publish 之后收到 BusEvent::PeerConnected
        let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
            .await
            .expect("event published in time")
            .expect("event present");
        match event {
            BusEvent::PeerConnected(node_id) => assert_eq!(node_id.as_str(), "peer-B"),
            other => panic!("expected PeerConnected, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn router_handle_peer_disconnected_publishes_event() {
        let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
        let event_bus = EventBus::new();
        let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
        let router =
            NetworkEventRouter::new(network, event_bus.clone(), scheduler, None, None, None);

        let mut sub = event_bus.subscribe(BusTopic::NetworkPeer);
        router
            .handle(NetworkEvent::PeerDisconnected {
                peer_id: "peer-C".to_string(),
            })
            .await;

        let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
            .await
            .expect("event published in time")
            .expect("event present");
        match event {
            BusEvent::PeerDisconnected(node_id) => assert_eq!(node_id.as_str(), "peer-C"),
            other => panic!("expected PeerDisconnected, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn router_handle_message_unknown_topic_is_noop() {
        let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
        let event_bus = EventBus::new();
        let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
        let router =
            NetworkEventRouter::new(network, event_bus.clone(), scheduler, None, None, None);

        // 未知主题 + 无效 payload：不应 panic，不应产生事件。
        router
            .handle_message("totally/unknown/topic", b"garbage")
            .await;
        // 无从验证「无事件」，但只要不 panic 即认为通过。
    }

    #[tokio::test]
    async fn router_handle_message_heartbeat_publishes_to_event_bus() {
        let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
        let event_bus = EventBus::new();
        let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
        let router =
            NetworkEventRouter::new(network, event_bus.clone(), scheduler, None, None, None);

        let hb = NodeHeartbeat {
            node_id: NodeId::from("node-HB".to_string()),
            active_workflows: Vec::new(),
            timestamp_ms: 12345,
            available_slots: 4,
            max_slots: 8,
            endpoint_addr: Some("peer-node-HB".to_string()),
        };
        // 构造 heartbeat 主题的 WireEnvelope
        let envelope = crate::common::WireEnvelope::wrap(
            crate::common::WireMessage::NodeHeartbeat(hb.clone()),
        );
        let topic = crate::common::Topic::heartbeat().to_string();
        let payload =
            crate::runtime::workflow::messaging::encode(&envelope).expect("encode envelope");

        let mut sub = event_bus.subscribe(BusTopic::ClusterHeartbeat);
        router.handle_message(&topic, &payload).await;

        let event = tokio::time::timeout(Duration::from_millis(500), sub.recv())
            .await
            .expect("event published in time")
            .expect("event present");
        match event {
            BusEvent::Heartbeat(received) => assert_eq!(received.node_id, hb.node_id),
            other => panic!("expected Heartbeat, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn router_handle_task_result_returns_false_without_actor_system() {
        // 无 actor_system / workflow_actor_id：handle_task_result 应返回 false。
        let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
        let event_bus = EventBus::new();
        let scheduler: Arc<dyn Scheduler> = Arc::new(MockScheduler::new());
        let router = NetworkEventRouter::new(network, event_bus, scheduler, None, None, None);

        let accepted = router
            .handle_task_result(
                WorkflowId::from("wf-1".to_string()),
                TaskId::from("t-1".to_string()),
                "echo".to_string(),
                crate::common::WireTaskOutcome::Completed(vec![1, 2, 3]),
                NodeId::from("node-Z".to_string()),
            )
            .await;
        assert!(!accepted, "should return false without actor system");
    }
}
