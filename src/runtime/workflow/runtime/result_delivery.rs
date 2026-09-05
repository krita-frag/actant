//! 远端任务结果投递重试队列。
//!
//! 当 Worker 无法把任务结果直连投递回 origin 节点时，结果会进入 bounded
//! retry queue。队列满时调用方会降级为发布失败事件，避免任务执行线程在网络
//! 背压下无限阻塞。
//!
//! ## 重试模型
//!
//! 接收循环本身不做投递：取出待重试结果后 spawn **独立重试任务**（经
//! 并发上限信号量背压），按指数退避延迟后投递。这样单条结果的退避等待
//! 不会阻塞队列中其他结果（消除队头阻塞）；并发上限防止目标节点长时间
//! 不可达时重试任务无限堆积。
//!
//! ## 超限补偿
//!
//! 重试次数达到上限后，结果不再重试，而是发布 `BusEvent::TaskFailed`
//! 补偿事件——否则远端提交方会永久等待一个永远不会到达的结果。
//!
use std::sync::Arc;
use std::time::Duration;

/// 同时在途的独立重试任务上限。达到上限后接收循环阻塞在信号量上，
/// 形成对 retry queue 的背压。
const MAX_CONCURRENT_RETRIES: usize = 16;

/// A task result that failed to deliver and is pending retry.
pub(super) struct PendingResult {
    pub(super) target: String,
    pub(super) request: crate::runtime::network::DirectRequest,
    pub(super) attempts: usize,
}

/// 尝试将待重试结果入队。
///
/// 使用 `try_send`（非阻塞）：通道满时立即返回 `false`，让调用方决定降级策略
/// （如发布 TaskFailed 事件）。避免在高负载下无限阻塞任务执行循环。
///
/// 返回 `true` 表示成功入队，`false` 表示通道满或已关闭。
pub(super) async fn try_enqueue_pending_result(
    tx: &tokio::sync::mpsc::Sender<PendingResult>,
    target: String,
    request: crate::runtime::network::DirectRequest,
    attempts: usize,
    capacity: usize,
) -> bool {
    let target_for_log = target.clone();
    match tx.try_send(PendingResult {
        target,
        request,
        attempts,
    }) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(
                target = %target_for_log,
                capacity = capacity,
                "pending_results channel full, result could not be enqueued for retry"
            );
            false
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!(target = %target_for_log, "pending_results channel closed, dropping result for retry");
            false
        }
    }
}

/// 从 `DirectRequest::TaskResult` 提取 (workflow_id, task_id, task_name, outcome)。
///
/// 重试队列中的结果均来自任务完成路径；其他 `DirectRequest` 变体不会入队，
/// 但仍需处理以防万一（返回 `None` 时跳过补偿事件）。
fn extract_task_result(
    request: &crate::runtime::network::DirectRequest,
) -> Option<(
    crate::common::WorkflowId,
    crate::common::TaskId,
    String,
    crate::common::WireTaskOutcome,
)> {
    match request {
        crate::runtime::network::DirectRequest::TaskResult {
            workflow_id,
            task_id,
            task_name,
            outcome,
            ..
        } => Some((
            workflow_id.clone(),
            task_id.clone(),
            task_name.clone(),
            outcome.clone(),
        )),
        _ => None,
    }
}

/// 超过重试上限后发布 TaskFailed 补偿事件。
///
/// 结果本身可能是 Completed（任务在 worker 上成功了，只是结果送不回去），
/// 但对提交方而言它永远等不到结果——以 Failed 终止比永久挂起更好。
fn publish_delivery_failure_compensation(
    event_bus: &crate::runtime::event_bus::EventBus,
    request: &crate::runtime::network::DirectRequest,
    target: &str,
    max_attempts: usize,
) {
    let Some((workflow_id, task_id, task_name, _outcome)) = extract_task_result(request) else {
        return;
    };
    let completion = crate::common::TaskCompletion::Failed {
        workflow_id: workflow_id.clone(),
        task_id,
        task_name,
        error: crate::common::format_error_kind(
            "network",
            &format!(
                "task result could not be delivered to orchestrator {target} \
                 after {max_attempts} retry attempts"
            ),
        ),
        target_node: None,
    };
    event_bus.publish(crate::runtime::event_bus::BusEvent::TaskFailed(completion));
}

pub(super) fn start_pending_result_loop(
    network: std::sync::Arc<dyn crate::runtime::network::Transport>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    max_attempts: usize,
    base_delay: Duration,
    pending_result_channel_capacity: usize,
    event_bus: crate::runtime::event_bus::EventBus,
) -> tokio::sync::mpsc::Sender<PendingResult> {
    let (pending_tx, mut pending_rx) =
        tokio::sync::mpsc::channel::<PendingResult>(pending_result_channel_capacity);
    let retry_tx = pending_tx.clone();
    let mut retry_cancel = cancel_rx.clone();
    let backoff = crate::common::backoff::ExponentialBackoff::new(
        base_delay,
        crate::common::REMOTE_CALL_MAX_RETRY_DELAY,
    );
    let retry_semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_RETRIES));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = retry_cancel.changed() => {
                    tracing::info!("pending result retry loop shutting down");
                    break;
                }
                Some(pending) = pending_rx.recv() => {
                    if pending.attempts >= max_attempts {
                        // 超限：发布 TaskFailed 补偿事件而非静默丢弃，
                        // 让提交方（远端 orchestrator / 本地订阅者）感知终态。
                        tracing::error!(
                            "dropping result for {} after {} failed attempts; publishing compensation event",
                            pending.target, max_attempts
                        );
                        publish_delivery_failure_compensation(
                            &event_bus,
                            &pending.request,
                            &pending.target,
                            max_attempts,
                        );
                        continue;
                    }
                    let delay = backoff.delay_for(pending.attempts as u32);
                    // 并发上限背压：在途重试任务达到上限时阻塞接收循环，
                    // 使 retry queue 逐渐填满并最终触发调用方的降级路径。
                    let Ok(permit) = retry_semaphore.clone().acquire_owned().await else {
                        // 信号量关闭只发生在 loop 退出后，实际不可达。
                        break;
                    };
                    let network = network.clone();
                    let retry_tx = retry_tx.clone();
                    let retry_event_bus = event_bus.clone();
                    let mut retry_cancel = retry_cancel.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        tokio::select! {
                            _ = retry_cancel.changed() => {
                                // shutdown：放弃本次重试（节点在停机，不发补偿）。
                            }
                            _ = tokio::time::sleep(delay) => {
                                // 重投失败（retry channel 满或已关闭）：该结果将
                                // 永远无法再进入重试队列——接收循环见不到它，
                                // "attempts 超限发补偿"的兜底不会触发。必须在
                                // 此处直接发布补偿事件，否则结果静默丢失。
                                async fn reenqueue_or_compensate(
                                    retry_tx: &tokio::sync::mpsc::Sender<PendingResult>,
                                    event_bus: &crate::runtime::event_bus::EventBus,
                                    pending: PendingResult,
                                    capacity: usize,
                                ) {
                                    let attempts = pending.attempts + 1;
                                    let enqueued = try_enqueue_pending_result(
                                        retry_tx,
                                        pending.target.clone(),
                                        pending.request.clone(),
                                        attempts,
                                        capacity,
                                    )
                                    .await;
                                    if !enqueued {
                                        tracing::error!(
                                            target = %pending.target,
                                            attempts,
                                            "retry re-enqueue failed; publishing compensation event for undeliverable result"
                                        );
                                        publish_delivery_failure_compensation(
                                            event_bus,
                                            &pending.request,
                                            &pending.target,
                                            attempts,
                                        );
                                    }
                                }
                                match network.send_direct_request(&pending.target, pending.request.clone()).await {
                                    Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: true }) => {
                                        tracing::debug!("pending result delivered to {}", pending.target);
                                    }
                                    Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: false }) => {
                                        tracing::warn!("pending result rejected by {}, will retry", pending.target);
                                        reenqueue_or_compensate(
                                            &retry_tx,
                                            &retry_event_bus,
                                            pending,
                                            pending_result_channel_capacity,
                                        ).await;
                                    }
                                    Ok(_) | Err(_) => {
                                        tracing::debug!("pending result delivery to {} failed, will retry (attempt {})", pending.target, pending.attempts + 1);
                                        reenqueue_or_compensate(
                                            &retry_tx,
                                            &retry_event_bus,
                                            pending,
                                            pending_result_channel_capacity,
                                        ).await;
                                    }
                                }
                            }
                        }
                    });
                }
                else => break,
            }
        }
    });

    pending_tx
}

#[cfg(test)]
#[path = "../../../../tests/rust/unit/runtime/workflow/runtime/result_delivery.rs"]
mod tests;
