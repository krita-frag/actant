//! 指数退避策略的统一实现。
//!
//! 项目中多处需要"失败后重试 + 退避"语义：
//! - 远端任务结果投递重试（`result_delivery`）
//! - Gossip 广播重试（`workflow/gossip.rs::broadcast_with_retry`）
//!
//! 本模块提供统一的 [`ExponentialBackoff`] 迭代器，让重试逻辑只关心"重试几次、
//! 基础延迟多少"，而退避计算与上限钳制由本模块保证，避免各调用点自行实现
//! `2u64.saturating_pow(attempt)` + `min(.., MAX)` 样板与 `MAX_RETRY_DELAY_MS`
//! 等上限常量散落。
//!
//! ## 设计约束
//!
//! - **纯计算**：`next_delay()` 不做 `sleep`，调用方决定是 `tokio::time::sleep`
//!   还是同步等待。这使本模块既可用于 async 也可用于 sync 上下文，且便于单元测试。
//! - **无状态**：迭代器内部仅持有 `attempt` 计数，所有配置参数在构造时确定。
//! - **饱和运算**：使用 `saturating_mul` / `saturating_pow` 防止整数溢出，
//!   达到上限后保持 `max_delay` 不再增长。
//! - **不决定何时停止**：是否继续重试由调用方根据 `max_attempts` 或外层条件决定，
//!   本模块只负责"给定第 N 次重试，应等多久"。

use std::time::Duration;

/// 指数退避策略配置。
///
/// 第 `attempt` 次重试（`attempt` 从 0 开始）的延迟为：
/// ```text
/// delay = min(base * 2^attempt, max_delay)
/// ```
///
/// `attempt = 0` 时返回 `base` 本身（即首次重试前等待一个基础延迟）。
#[derive(Debug, Clone, Copy)]
pub struct ExponentialBackoff {
    base: Duration,
    max_delay: Duration,
}

impl ExponentialBackoff {
    /// 创建指数退避策略。
    ///
    /// `base` 为 0 是合法输入：[`Self::delay_for`] 对其直接返回
    /// [`Duration::ZERO`]，等价于"无延迟重试"，不会 panic。若需要真正的
    /// 退避语义，调用方应传入非零 `base`。
    pub const fn new(base: Duration, max_delay: Duration) -> Self {
        Self { base, max_delay }
    }

    /// 返回第 `attempt` 次重试应等待的延迟。
    ///
    /// `attempt = 0` 返回 `base`；后续按 `2^attempt` 指数增长，被 `max_delay` 钳制。
    /// `attempt` 过大导致 `2^attempt` 溢出时，通过 `saturating_mul` 钳制到 `max_delay`。
    pub fn delay_for(&self, attempt: u32) -> Duration {
        if self.base.is_zero() {
            return Duration::ZERO;
        }
        // 2^attempt 计算为 u32（attempt 上限 32），再提升到 u64 与 base 毫秒数相乘。
        // saturating_pow(32) = u32::MAX，不会 panic。
        let multiplier = 2u32.saturating_pow(attempt);
        let base_ms = self.base.as_millis() as u64;
        let delay_ms = base_ms.saturating_mul(multiplier as u64);
        let capped_ms = delay_ms.min(self.max_delay.as_millis() as u64);
        Duration::from_millis(capped_ms)
    }

    /// 返回基础延迟（用于调用方日志或断言）。
    pub const fn base(&self) -> Duration {
        self.base
    }

    /// 返回最大延迟上限（用于调用方日志或断言）。
    pub const fn max_delay(&self) -> Duration {
        self.max_delay
    }
}

/// 跨节点重试类操作的默认最大延迟（30s）。
///
/// 30s 是 P2P 网络下"对端临时不可达但应允许重试"的合理上限：
/// 超过此值通常意味着对端已下线，重试无意义，应让上层超时接管。
pub const REMOTE_CALL_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

#[cfg(test)]
#[path = "../../tests/rust/unit/common/backoff.rs"]
mod tests;
