//! 单维命名配额（分布式信号量）的本地准入器。
//!
//! 每个节点按配置声明若干**命名配额**的容量（如 `gpu: 2`），任务可携带一个
//! 配额名。执行前在本地原子扣减；容量耗尽时任务延后重入队（归还时唤醒等待者，
//! 并保留固定间隔兜底），直到配置的 `starvation_timeout_ms` 耗尽才转 Failed。
//! 心跳广播每个配额的**剩余**容量，路由策略据此避免把任务转发到已满节点。
//!
//! ## 语义边界
//!
//! - 核心只做「命名计数」，不解释配额的业务含义（GPU、许可证、并发连接都只是
//!   字符串键）；多维资源（CPU/mem 等向量）不在核心，属第 3 层。
//! - 准入是**本地硬约束 + 全局最终一致**：心跳视图可能过时，因此执行节点始终
//!   重新扣减；路由只是尽力避免无效转发。
//! - 扣减是原子的 all-or-nothing（当前单键，但实现保留多键形状），
//!   不会出现部分扣减。
//! - 守卫（[`QuotaGuard`]）在 `Drop` 时归还，覆盖成功/失败/取消/panic 路径；
//!   归还同时广播 [`QuotaTracker::wait_for_release`] 信号，唤醒延后的任务。
//! - `starvation_timeout_ms = 0` 表示不设上限（任务可持续等待配额）。

use std::collections::BTreeMap;

use parking_lot::Mutex;

/// 配额准入失败原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaAcquireError {
    /// 本节点未声明该配额（或容量为 0）。
    Unavailable(String),
    /// 该配额剩余容量为 0。
    Exhausted(String),
}

impl QuotaAcquireError {
    /// 供日志/失败信息使用的配额键。
    pub fn key(&self) -> &str {
        match self {
            Self::Unavailable(key) | Self::Exhausted(key) => key,
        }
    }
}

/// 本地命名配额计数器。
#[derive(Debug)]
pub struct QuotaTracker {
    /// 可运行时替换的容量表。运行中扩容/缩容由
    /// [`Self::set_capacities`] 整表覆盖，在用量按差值保留。
    capacities: Mutex<BTreeMap<String, u32>>,
    remaining: Mutex<BTreeMap<String, u32>>,
    /// 配额归还信号：容量耗尽而延后的任务据此提前唤醒，
    /// 而不是固定间隔轮询（容量为 1 时固定轮询会把吞吐压到 `1/delay`）。
    released: tokio::sync::Notify,
}

impl QuotaTracker {
    /// 按容量表构造；容量为 0 的键视为永不提供。
    pub fn new(capacities: BTreeMap<String, u32>) -> Self {
        Self {
            remaining: Mutex::new(capacities.clone()),
            capacities: Mutex::new(capacities),
            released: tokio::sync::Notify::new(),
        }
    }

    /// 等待任一配额单元归还。
    ///
    /// 与 [`Self::try_acquire`] 配合使用：延后的任务在 `select!` 中同时等待
    /// 此信号与兜底超时，释放方无需知道有多少任务在等待。信号是广播语义
    /// （`notify_waiters`），不缓存许可；错过信号的等待者由兜底超时覆盖。
    pub async fn wait_for_release(&self) {
        self.released.notified().await;
    }

    /// 当前每个配额的剩余容量快照（用于心跳广播）。
    pub fn available_map(&self) -> BTreeMap<String, u32> {
        self.remaining.lock().clone()
    }

    /// 本节点声明的配额容量表快照。
    pub fn capacities(&self) -> BTreeMap<String, u32> {
        self.capacities.lock().clone()
    }

    /// 运行时整表替换配额容量。
    ///
    /// 已在用的单元按旧容量与旧剩余量之差保留：扩容立刻增加可用量，缩容
    /// 不会把在用量算成负值（新剩余量饱和到 0）。移除的键从剩余表中消失，
    /// 新增的键从满容量开始。完成后唤醒等待配额的任务。
    ///
    /// 锁顺序固定为 `capacities` → `remaining`，与 [`Self::release`] 一致。
    pub fn set_capacities(&self, capacities: BTreeMap<String, u32>) {
        let mut caps = self.capacities.lock();
        let mut remaining = self.remaining.lock();
        let mut next = BTreeMap::new();
        for (key, capacity) in &capacities {
            let old_capacity = caps.get(key).copied().unwrap_or(0);
            let old_remaining = remaining.get(key).copied().unwrap_or(0);
            let in_use = old_capacity.saturating_sub(old_remaining);
            next.insert(key.clone(), capacity.saturating_sub(in_use));
        }
        *caps = capacities;
        *remaining = next;
        drop(remaining);
        drop(caps);
        self.released.notify_waiters();
    }

    /// 尝试扣减一个配额单元。
    ///
    /// `key` 为 `None` 时不做任何扣减并返回 `Ok(None)`。成功返回的守卫
    /// 在 `Drop` 时归还容量；失败时保持计数不变（all-or-nothing）。
    pub fn try_acquire(
        self: &std::sync::Arc<Self>,
        key: Option<&str>,
    ) -> Result<Option<QuotaGuard>, QuotaAcquireError> {
        let Some(key) = key else {
            return Ok(None);
        };
        let mut remaining = self.remaining.lock();
        match remaining.get_mut(key) {
            None => Err(QuotaAcquireError::Unavailable(key.to_string())),
            Some(0) => Err(QuotaAcquireError::Exhausted(key.to_string())),
            Some(slot) => {
                *slot = slot.saturating_sub(1);
                Ok(Some(QuotaGuard {
                    tracker: std::sync::Arc::clone(self),
                    key: key.to_string(),
                }))
            }
        }
    }

    /// 归还一个配额单元（由 [`QuotaGuard`] 调用）。
    fn release(&self, key: &str) {
        {
            // 与 set_capacities 保持同一锁顺序：先 capacities，后 remaining。
            let capacity = self.capacities.lock().get(key).copied().unwrap_or(u32::MAX);
            let mut remaining = self.remaining.lock();
            if let Some(slot) = remaining.get_mut(key) {
                *slot = slot.saturating_add(1).min(capacity);
            }
        }
        // 锁外唤醒：等待者被唤醒后会再次尝试获取 `remaining`。
        self.released.notify_waiters();
    }
}

/// 配额持有守卫：`Drop` 时自动归还。
#[derive(Debug)]
pub struct QuotaGuard {
    tracker: std::sync::Arc<QuotaTracker>,
    key: String,
}

impl QuotaGuard {
    /// 守卫持有的配额键。
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        self.tracker.release(&self.key);
    }
}

#[cfg(test)]
#[path = "../../../../tests/rust/unit/runtime/quota.rs"]
mod tests;
