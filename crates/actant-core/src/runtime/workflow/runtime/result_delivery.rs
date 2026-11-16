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
//! **不保序是有意的**：跨结果的投递顺序无保证。乱序正确性不依赖本队列——
//! 结果回灌的终态守卫（`can_transition_task`）与 attempt fencing（按派发
//! 代数裁决）使迟到/重复结果无论先后到达都被一致拒绝；同工作流结算的
//! 串行是结果泵上游的有意设计（见跨片裁定 X17）。
//!
//! ## 超限行为
//!
//! 重试次数达到上限后不做本地补偿（origin 在远端节点，收不到本地事件）：
//! 结果继续按封顶退避重试，直到投递成功或节点停机；停机后由 origin 的
//! 失联接管/租约重投兜底。
//!
use std::sync::Arc;
use std::time::Duration;

/// 同时在途的独立重试任务上限。达到上限后接收循环阻塞在信号量上，
/// 形成对 retry queue 的背压。
const MAX_CONCURRENT_RETRIES: usize = 16;

/// 连续失败多少次后把日志级别提到 warn。
///
/// 这不是放弃阈值——结果重试不设放弃（放弃即丢结果，origin 永久等待）。
/// 它只用来把「单位数重试」的 debug 噪声与「长期投不出去」的告警区分开。
const RESULT_RETRY_WARN_AFTER: usize = 10;

/// A task result that failed to deliver and is pending retry.
pub(super) struct PendingResult {
    pub(super) target: String,
    pub(super) request: crate::runtime::network::DirectRequest,
    pub(super) attempts: usize,
}

/// 有界队列上的阻塞入队：队列满时等待，而不是丢弃结果。
///
/// 远端结果一旦丢弃，origin 可能永久等待；这里用 `send().await` 形成背压。
/// 返回 `false` 只表示重试循环已关闭（节点正在停机），此时由 origin 的
/// 失联接管/租约重投兜底。
pub(super) async fn enqueue_pending_result(
    tx: &tokio::sync::mpsc::Sender<PendingResult>,
    target: String,
    request: crate::runtime::network::DirectRequest,
    attempts: usize,
) -> bool {
    let target_for_log = target.clone();
    match tx
        .send(PendingResult {
            target,
            request,
            attempts,
        })
        .await
    {
        Ok(()) => true,
        Err(_) => {
            tracing::warn!(
                target = %target_for_log,
                "pending_results channel closed during shutdown; origin will re-dispatch"
            );
            false
        }
    }
}

/// 尝试将待重试结果入队（非阻塞，仅测试使用）。
#[cfg(test)]
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

/// 超过重试上限后继续重试：远端结果不能因为本地放弃而永久丢失。
///
/// 队列有界 + 并发上限提供背压；origin 不可达期间结果保留，恢复后补投。
/// 只有节点停机（channel 关闭）才停止，由 origin 的失联接管兜底。
pub(super) fn start_pending_result_loop(
    network: std::sync::Arc<dyn crate::runtime::network::Transport>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    base_delay: Duration,
    pending_result_channel_capacity: usize,
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
                    if pending.attempts >= RESULT_RETRY_WARN_AFTER {
                        // 不做本地补偿（origin 在远端，收不到本地事件）；
                        // 继续按封顶退避重试，直到投递成功或节点停机。
                        tracing::warn!(
                            target = %pending.target,
                            attempts = pending.attempts,
                            "result still undeliverable; keeping for retry"
                        );
                    }
                    let delay = backoff.delay_for(pending.attempts as u32);
                    // 并发上限背压：在途重试任务达到上限时阻塞接收循环，
                    // 使 retry queue 逐渐填满并最终触发调用方的背压路径。
                    let Ok(permit) = retry_semaphore.clone().acquire_owned().await else {
                        break;
                    };
                    let network = network.clone();
                    let retry_tx = retry_tx.clone();
                    let mut retry_cancel = retry_cancel.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        tokio::select! {
                            _ = retry_cancel.changed() => {
                                // shutdown：放弃本次重试（节点在停机，origin 会重投）。
                            }
                            _ = tokio::time::sleep(delay) => {
                                // 重投失败（retry channel 满或已关闭）：队列满时
                                // `enqueue_pending_result` 会等待而不是丢弃；只有
                                // 停机关闭通道才返回 false。
                                async fn reenqueue_or_log(
                                    retry_tx: &tokio::sync::mpsc::Sender<PendingResult>,
                                    pending: PendingResult,
                                ) {
                                    let attempts = pending.attempts + 1;
                                    // 返回值只表示本次是否入队成功；停机关闭通道
                                    // 时无人可收，重投自然终止，无需上报。
                                    let _ = enqueue_pending_result(
                                        retry_tx,
                                        pending.target.clone(),
                                        pending.request.clone(),
                                        attempts,
                                    )
                                    .await;
                                }
                                match network.send_direct_request(&pending.target, pending.request.clone()).await {
                                    Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: true }) => {
                                        tracing::debug!("pending result delivered to {}", pending.target);
                                    }
                                    Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: false }) => {
                                        tracing::warn!("pending result rejected by {}, will retry", pending.target);
                                        reenqueue_or_log(&retry_tx, pending).await;
                                    }
                                    Ok(_) | Err(_) => {
                                        tracing::debug!("pending result delivery to {} failed, will retry (attempt {})", pending.target, pending.attempts + 1);
                                        reenqueue_or_log(&retry_tx, pending).await;
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
#[path = "../../../../../../tests/rust/unit/runtime/workflow/runtime/result_delivery.rs"]
mod tests;
