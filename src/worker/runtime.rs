use std::sync::Arc;
use std::time::Duration;

use super::dispatcher::TaskDispatcher;
use super::router::NetworkEventRouter;
use crate::actor::system::ActorSystem;
use crate::common::{
    ActantError, NodeId, Result, TaskCompletion, TaskDefinition, Topic, WorkerConfig, WorkflowId,
};
use crate::event_bus::{BusEvent, EventBus};
use crate::network::Transport;
use crate::orchestrator::Scheduler;

/// A task result that failed to deliver and is pending retry.
struct PendingResult {
    target: String,
    request: crate::network::protocol::DirectRequest,
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

pub struct WorkerRuntime {
    node_id: NodeId,
    network: Arc<dyn Transport>,
    scheduler: Arc<dyn Scheduler>,
    event_bus: EventBus,
    task_dispatcher: Arc<dyn TaskDispatcher>,
    actor_system: Option<Arc<ActorSystem>>,
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

impl WorkerRuntime {
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
            event_bus,
            task_dispatcher,
            actor_system: None,
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
        let task_topic = Topic::task(&self.node_id);
        tracing::info!("subscribing to task topic: {}", task_topic);
        self.network.subscribe(task_topic.as_str()).await?;

        let actor_topic = Topic::actor(&self.node_id);
        tracing::info!("subscribing to actor topic: {}", actor_topic);
        self.network.subscribe(actor_topic.as_str()).await?;

        let actor_reply_topic = Topic::actor_reply(&self.node_id);
        tracing::info!("subscribing to actor reply topic: {}", actor_reply_topic);
        self.network.subscribe(actor_reply_topic.as_str()).await?;

        // 在 FailoverManager 中初始化容量，确保首次 heartbeat
        // 携带正确的值而不是 0。
        self.notify_capacity();

        // -- Network event loop: route all network events via NetworkEventRouter --
        let router = NetworkEventRouter::new(
            self.network.clone(),
            self.event_bus.clone(),
            self.scheduler.clone(),
            self.actor_system.clone(),
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

        // -- Pending result 投递循环 --
        // 初次投递失败的 result 会被入队并异步重试。
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
                            Ok(crate::network::protocol::DirectResponse::TaskResultAck { accepted: true }) => {
                                tracing::debug!("pending result delivered to {}", pending.target);
                            }
                            Ok(crate::network::protocol::DirectResponse::TaskResultAck { accepted: false }) => {
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

        // -- Task execution loop --
        let node_id = self.node_id.clone();
        let network = self.network.clone();
        let task_dispatcher = self.task_dispatcher.clone();
        let semaphore = self.running_tasks.clone();
        let event_bus = self.event_bus.clone();
        let task_timeout = self.task_timeout;
        let mut task_cancel = self.cancel_rx.clone();
        let state_sender = self.state.clone();
        let drain_timeout = self.drain_timeout;

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
                    tracing::info!("worker entering drain mode, waiting up to {}ms for running tasks", drain_timeout.as_millis());

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
                            // 在后台延迟，不阻塞主循环继续处理其他任务
                            let delay = self.remote_fallback_delay;
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                            });
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
                // 在后台延迟，不阻塞主循环继续处理其他任务
                let delay = self.remote_fallback_delay;
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                });
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

                let cancel_flag = crate::worker::dispatcher::new_cancel_flag();
                tracing::debug!(task_id = ?task.id, "dispatching task");

                let dispatch_result = tokio::time::timeout(
                    effective_timeout,
                    dispatcher.dispatch(&task_name, task_payload, cancel_flag.clone()),
                )
                .await;

                if dispatch_result.is_err() {
                    tracing::warn!(task_id = ?task.id, "task timed out, signalling cancel");
                    cancel_flag.store(true, std::sync::atomic::Ordering::Release);
                }

                let completion = match dispatch_result {
                    Ok(Ok(result)) => {
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
                    Ok(Err(e)) => {
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
                            let request = crate::network::protocol::DirectRequest::TaskResult {
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
                                Ok(crate::network::protocol::DirectResponse::TaskResultAck {
                                    accepted: true,
                                }) => {
                                    tracing::debug!(
                                        "task result delivered directly to orchestrator {}",
                                        origin_addr
                                    );
                                }
                                Ok(crate::network::protocol::DirectResponse::TaskResultAck {
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
        let request = crate::network::protocol::DirectRequest::DispatchTask { task: task.clone() };
        match self.network.send_direct_request(target_addr, request).await {
            Ok(crate::network::protocol::DirectResponse::DispatchAck { accepted: true }) => Ok(()),
            Ok(crate::network::protocol::DirectResponse::DispatchAck { accepted: false }) => Err(
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

        let req = crate::network::protocol::DirectRequest::QueryWorkflowState {
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
        let req2 = crate::network::protocol::DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf-test"),
            requesting_node: NodeId::from("n-test"),
        };
        enqueue_pending_result!(tx, "peer-3".to_string(), req2, 0, capacity);
        assert_eq!(rx.len(), 1, "enqueue should succeed after drain");
    }
}
