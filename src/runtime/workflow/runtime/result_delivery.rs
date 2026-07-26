//! 远端任务结果投递重试队列。
//!
//! 当 Worker 无法把任务结果直连投递回 origin 节点时，结果会进入 bounded
//! retry queue。队列满时调用方会降级为发布失败事件，避免任务执行线程在网络
//! 背压下无限阻塞。
//!
use std::time::Duration;

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

pub(super) fn start_pending_result_loop(
    network: std::sync::Arc<dyn crate::runtime::network::Transport>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    max_attempts: usize,
    base_delay: Duration,
    pending_result_channel_capacity: usize,
) -> tokio::sync::mpsc::Sender<PendingResult> {
    let (pending_tx, mut pending_rx) =
        tokio::sync::mpsc::channel::<PendingResult>(pending_result_channel_capacity);
    let retry_tx = pending_tx.clone();
    let mut retry_cancel = cancel_rx;
    let backoff = crate::common::backoff::ExponentialBackoff::new(
        base_delay,
        crate::common::REMOTE_CALL_MAX_RETRY_DELAY,
    );

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
                    let delay = backoff.delay_for(pending.attempts as u32);
                    tokio::time::sleep(delay).await;
                    match network.send_direct_request(&pending.target, pending.request.clone()).await {
                        Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: true }) => {
                            tracing::debug!("pending result delivered to {}", pending.target);
                        }
                        Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: false }) => {
                            tracing::warn!("pending result rejected by {}, will retry", pending.target);
                            // 入队失败仅当 retry channel 已满（容量耗尽）或 retry loop
                            // 已退出。前者由 channel 容量限制保护，不再重试避免雪崩；
                            // 后者属 shutdown 路径，结果丢弃可接受。
                            let _ = try_enqueue_pending_result(
                                &retry_tx,
                                pending.target,
                                pending.request,
                                pending.attempts + 1,
                                pending_result_channel_capacity,
                            ).await;
                        }
                        Ok(_) | Err(_) => {
                            tracing::debug!("pending result delivery to {} failed, will retry (attempt {})", pending.target, pending.attempts + 1);
                            // 同上：入队失败属容量耗尽或 shutdown 路径。
                            let _ = try_enqueue_pending_result(
                                &retry_tx,
                                pending.target,
                                pending.request,
                                pending.attempts + 1,
                                pending_result_channel_capacity,
                            ).await;
                        }
                    }
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
