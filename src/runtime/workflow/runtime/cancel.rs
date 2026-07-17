//! Worker 远端取消标记的 TTL 清理。
//!
//! 本模块只维护“远端 cancel gossip 已到达但任务尚未进入本地运行态”的短期可见性。
//! 运行中任务的协作式取消由 [`CancelFlag`](crate::runtime::dispatcher::CancelFlag)
//! 注册表处理；Python 侧排队任务的预取消状态也不在这里维护。
//!
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 已取消任务的 TTL：超过此时间的条目将被定期清理，防止集合无界增长。
/// 5 分钟足够覆盖任务在调度队列中的最长停留时间（默认超时 30s）。
const CANCELLED_TASKS_TTL: Duration = Duration::from_secs(300);

/// 清理间隔：每 60 秒执行一次 TTL 清理。
const CANCELLED_TASKS_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// 清理过期取消条目，返回移除数量。
///
/// 抽出为独立函数以便单元测试覆盖 TTL 逻辑，不依赖 60 秒清理间隔。
fn cleanup_expired_cancelled_tasks(
    cancelled_tasks: &mut HashMap<String, Instant>,
    now: Instant,
    ttl: Duration,
) -> usize {
    let before = cancelled_tasks.len();
    cancelled_tasks.retain(|_, ts| now.duration_since(*ts) < ttl);
    let removed = before - cancelled_tasks.len();
    if removed > 0 {
        tracing::debug!(
            removed = removed,
            remaining = cancelled_tasks.len(),
            "cleaned up expired cancelled_tasks entries"
        );
        // 同步递减 pending gauge，保持指标与实际集合大小一致。
        crate::metrics::dec_cancelled_tasks_pending_by(removed as i64);
    }
    removed
}

/// 在 tokio 后台 spawn 远端取消标记的 TTL 清理循环。
///
/// 定期移除超过 ``CANCELLED_TASKS_TTL`` 的条目，防止集合无界增长。
/// 清理在 Worker shutdown 时自动退出。
pub(super) fn spawn_cancelled_tasks_cleanup_loop(
    tokio_handle: &tokio::runtime::Handle,
    cancelled_tasks: Arc<parking_lot::Mutex<HashMap<String, Instant>>>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut cleanup_cancel = cancel_rx;

    tokio_handle.spawn(async move {
        loop {
            tokio::select! {
                _ = cleanup_cancel.changed() => {
                    tracing::debug!("cancelled_tasks cleanup loop shutting down");
                    break;
                }
                _ = tokio::time::sleep(CANCELLED_TASKS_CLEANUP_INTERVAL) => {
                    let now = Instant::now();
                    let mut registry = cancelled_tasks.lock();
                    cleanup_expired_cancelled_tasks(&mut registry, now, CANCELLED_TASKS_TTL);
                }
            }
        }
    });
}

#[cfg(test)]
#[path = "../../../../tests/rust/unit/runtime/workflow/runtime/cancel.rs"]
mod tests;
