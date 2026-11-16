//! 任务调度器的抽象与 Actor 实现。
//!
//! [`Scheduler`] 定义 Worker 与调度策略之间的最小契约；生产路径由
//! [`SchedulerActor`] 持有队列状态，[`ActorScheduler`] 作为客户端转发。
//! 阻塞 / 关闭语义、enqueue 快路径与扩展方式见 `docs/architecture/scheduler.md`。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use crate::common::{ActantError, ActorId, TaskDefinition, WorkflowId};
use crate::runtime::actor::ActorSystem;
use crate::runtime::workflow::actor::{scheduler_methods, InnerScheduler};
use crate::runtime::workflow::messaging::{decode_result, encode, ok_or_error};

/// 任务调度器抽象。
///
/// 此 trait 将 DAG 编排器与具体调度策略解耦。生产实现为
/// [`SchedulerActor`] +
/// [`ActorScheduler`]：调度状态由 Actor 持有，客户端通过 Actor 消息交互。
///
/// # 公共扩展点
///
/// 此 trait 是 Rust 核心的公共扩展点。外部 Rust 用户可实现此 trait 以替换调度策略
/// （例如公平调度、加权轮询、截止时间优先等）。实现只需满足 `Send + Sync`。
///
#[async_trait]
pub trait Scheduler: Send + Sync {
    /// 入队单个任务。调度器已关闭（drain 模式）时返回 `Err`。
    async fn enqueue(&self, task: TaskDefinition) -> Result<(), ActantError>;
    /// 批量入队。调度器已关闭时返回 `Err`。
    async fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> Result<(), ActantError>;
    async fn dequeue(&self) -> Option<TaskDefinition>;
    /// 非阻塞出队：立即返回一个任务或 `None`。
    async fn try_dequeue(&self) -> Option<TaskDefinition>;
    /// 至多出队 `limit` 个任务，按优先级排序返回。队列不足时返回数量少于 `limit`。
    async fn dequeue_batch(&self, limit: usize) -> Vec<TaskDefinition>;
    /// 移除该工作流所有**尚未出队**的任务，返回移除数量。
    ///
    /// 用于「让位」：本节点收到的 claim 表明另一节点已成为工作流的编排者时，
    /// 本地排队但未开始执行的任务必须一并撤销。只摘编排状态（
    /// `remove_active_workflow`）不够——执行层仍会把它们派发出去，副作用会在
    /// 新旧两个主上各发生一次，而完成回灌时 slot 已删、结果被静默丢弃。
    ///
    /// 已经出队（在 `inflight` 或执行中）的任务不在此列，由派发前的
    /// 租约所有权校验拦截。实现必须是无副作用的纯队列操作：不发终态事件、
    /// 不重排、不重试——工作流已归对方，撤销的任务由对方重新派发。
    async fn remove_workflow_tasks(&self, workflow_id: &WorkflowId) -> usize;
    /// 关闭调度器——拒绝新入队并唤醒等待者，使 `dequeue()` 在队列清空后返回 `None`。
    ///
    /// # 语义
    ///
    /// 调用后调度器进入 drain 模式：
    /// 1. **新入队被拒**：后续 `enqueue` / `enqueue_batch` 返回
    ///    `Err(ActantError::InvalidState("scheduler is closed ..."))`。
    /// 2. **等待中的消费者被唤醒**：阻塞在 `dequeue()` 的调用方会立即收到 `None`，
    ///    从而退出消费循环。
    /// 3. **已排队任务保留**：`close` 不清空队列，已入队任务仍可通过 `try_dequeue`
    ///    或 `dequeue_batch` 取出（由调用方决定是否在关闭后继续 drain）。
    ///
    /// # 幂等性
    ///
    /// 重复调用 `close` 安全无副作用。
    ///
    /// # 与 `is_closed` 的关系
    ///
    /// `close` 返回后 `is_closed` 立即返回 `true`（在 `ActorScheduler` 实现中
    /// 通过 `AtomicBool` 保证）。`ActorScheduler` 的 `close` 是 fire-and-forget：
    /// 它仅标记本地状态并异步通知 Actor 进入 drain 模式。
    fn close(&self) {}
    /// 调度器是否已被 [`close`](Self::close)。
    ///
    /// 一旦 [`close`](Self::close) 被调用，此方法立即返回 `true`，并保持到
    /// 调度器被销毁。`ActorScheduler` 实现使用 `AtomicBool` + `Acquire/Release`
    /// 序保证跨线程可见性。
    fn is_closed(&self) -> bool {
        false
    }
}

/// 构造启用 enqueue fast-path 的 [`ActorScheduler`]（供 bench/test 使用）。
///
/// 生产代码用 [`crate::runtime::builder::init_worker`] 装配；此函数仅供
/// 基准/测试直接构造 fast-path 客户端，避免依赖完整 `WorkerInitParams`。
#[doc(hidden)]
pub async fn spawn_fast_path_scheduler(
    actor_id: ActorId,
    actor_system: Arc<ActorSystem>,
    actor: crate::runtime::workflow::SchedulerActor,
) -> Result<ActorScheduler, ActantError> {
    let inner = actor.shared_inner();
    actor_system.spawn(actor_id.clone(), actor).await?;
    Ok(ActorScheduler::with_fast_path(
        actor_id,
        actor_system,
        inner,
    ))
}

/// 通过 [`ActorSystem`] 调用 [`SchedulerActor`] 的客户端。
///
/// 这是 `Worker` 迁移到 Actor 模型的桥接实现：调度状态由
/// `SchedulerActor` 持有，本客户端仅负责序列化请求并通过 actor 消息
/// 协议转发。
///
/// # enqueue 快路径
///
/// 当通过 [`Self::with_fast_path`] 构造时，`enqueue` / `enqueue_batch`
/// 直接调用共享的 [`InnerScheduler::enqueue`] 同步方法，绕过 Actor 消息往返
/// （postcard 编解码 + 邮箱调度 + 响应通道），将单任务入队延迟从 ~10µs
/// 降至 ~100ns 量级。
///
/// 快路径安全性：`InnerScheduler` 的队列由 `parking_lot::Mutex` 保护，
/// `notify` 为线程安全 `tokio::sync::Notify`，`closed` 为 `AtomicBool`。
/// `enqueue` 的 `notify_one()` 唤醒语义与 Actor 路径的
/// `InnerScheduler::enqueue` 内部的 `notify_one()` 一致（Worker 通过 `Notify`
/// 信号驱动 `try_dequeue`）。
///
/// `dequeue` / `try_dequeue` / `close` 等仍走 Actor 消息协议——这些操作
/// 涉及 Actor 侧协调（如 `dequeue` 的阻塞等待、`close` 的 drain 模式），
/// 不适合直接共享状态。
#[derive(Clone)]
pub struct ActorScheduler {
    actor_id: ActorId,
    actor_system: Arc<ActorSystem>,
    closed: Arc<AtomicBool>,
    /// enqueue 快路径共享状态。`None` 时退化为 Actor 消息往返。
    /// 由 [`SchedulerActor::shared_inner`] 在 spawn 前注入。
    fast_inner: Option<Arc<InnerScheduler>>,
}

impl ActorScheduler {
    pub fn new(actor_id: ActorId, actor_system: Arc<ActorSystem>) -> Self {
        Self {
            actor_id,
            actor_system,
            closed: Arc::new(AtomicBool::new(false)),
            fast_inner: None,
        }
    }

    /// 启用 enqueue 快路径：注入与 `SchedulerActor` 共享的内部状态。
    ///
    /// 调用方应在 `ActorSystem::spawn` **之前**调用 `SchedulerActor::shared_inner`
    /// 获取 `Arc<InnerScheduler>`，再通过本方法构造客户端。典型用法见
    /// [`crate::runtime::builder::init_worker`]。
    pub(crate) fn with_fast_path(
        actor_id: ActorId,
        actor_system: Arc<ActorSystem>,
        inner: Arc<InnerScheduler>,
    ) -> Self {
        Self {
            actor_id,
            actor_system,
            closed: Arc::new(AtomicBool::new(false)),
            fast_inner: Some(inner),
        }
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }

    async fn call(
        &self,
        method: &str,
        payload: Vec<u8>,
    ) -> Result<crate::common::ActorMessageResult, ActantError> {
        self.actor_system
            .call(&self.actor_id, method, payload)
            .await
    }
}

#[async_trait]
impl Scheduler for ActorScheduler {
    async fn enqueue(&self, task: TaskDefinition) -> Result<(), ActantError> {
        // 本地 closed 检查对快路径与慢路径都生效：慢路径的 CLOSE 消息是异步
        // 投递的，若不在这里拦截，close 后立刻 enqueue 会抢在 Actor 处理前成功。
        if self.closed.load(Ordering::Acquire) {
            return Err(ActantError::InvalidState(
                "scheduler is closed (drain mode), rejecting task".into(),
            ));
        }
        // 快路径：直接调用共享 InnerScheduler::enqueue，绕过 Actor 消息往返。
        // 适用于高 QPS 单任务入队场景（如 mailbox 持久化后的任务派发）。
        // Worker 唤醒由 InnerScheduler::enqueue 内部的 notify_one() 保证，
        // 语义与 Actor 消息路径一致（两条路径最终都经 InnerScheduler::enqueue
        // 内部的 notify_one() 唤醒 Worker）。
        if let Some(ref inner) = self.fast_inner {
            return inner.enqueue(task).await;
        }
        // 慢路径：无共享状态（如纯单元测试构造的 ActorScheduler::new），
        // 走完整 Actor 消息协议。
        let result = self
            .call(scheduler_methods::ENQUEUE, encode(&task)?)
            .await?;
        ok_or_error(result)
    }

    async fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> Result<(), ActantError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ActantError::InvalidState(
                "scheduler is closed (drain mode), rejecting task".into(),
            ));
        }
        // 快路径：批量入队同样直接操作共享状态。
        if let Some(ref inner) = self.fast_inner {
            return inner.enqueue_batch(tasks).await;
        }
        let result = self
            .call(scheduler_methods::ENQUEUE_BATCH, encode(&tasks)?)
            .await?;
        ok_or_error(result)
    }

    async fn dequeue(&self) -> Option<TaskDefinition> {
        match self.call(scheduler_methods::DEQUEUE, vec![]).await {
            Ok(result) => decode_result::<Option<TaskDefinition>>(result)
                .ok()
                .flatten(),
            Err(e) => {
                tracing::warn!("SchedulerActor dequeue call failed: {}", e);
                None
            }
        }
    }

    async fn try_dequeue(&self) -> Option<TaskDefinition> {
        match self.call(scheduler_methods::TRY_DEQUEUE, vec![]).await {
            Ok(result) => decode_result::<Option<TaskDefinition>>(result)
                .ok()
                .flatten(),
            Err(e) => {
                tracing::warn!("SchedulerActor try_dequeue call failed: {}", e);
                None
            }
        }
    }

    async fn dequeue_batch(&self, limit: usize) -> Vec<TaskDefinition> {
        match self
            .call(
                scheduler_methods::DEQUEUE_BATCH,
                encode(&limit).unwrap_or_default(),
            )
            .await
        {
            Ok(result) => decode_result::<Vec<TaskDefinition>>(result).unwrap_or_default(),
            Err(e) => {
                tracing::warn!("SchedulerActor dequeue_batch call failed: {}", e);
                vec![]
            }
        }
    }

    async fn remove_workflow_tasks(&self, workflow_id: &WorkflowId) -> usize {
        // 快路径：与 enqueue 同理，直接操作共享队列，避免一次 Actor 往返。
        if let Some(ref inner) = self.fast_inner {
            return inner.remove_workflow_tasks(workflow_id).await;
        }
        match self
            .call(
                scheduler_methods::REMOVE_WORKFLOW_TASKS,
                encode(workflow_id).unwrap_or_default(),
            )
            .await
        {
            Ok(result) => decode_result::<usize>(result).unwrap_or(0),
            Err(e) => {
                // 撤队列失败时保守返回 0：调用方据此保留「已撤销」以外的日志口径，
                // 未撤掉的任务由派发前的租约所有权校验兜住。
                tracing::warn!("SchedulerActor remove_workflow_tasks call failed: {}", e);
                0
            }
        }
    }

    /// 关闭调度器客户端：标记本地状态为 closed，并异步通知 `SchedulerActor` 进入 drain 模式。
    ///
    /// # 执行流程
    ///
    /// 1. **本地立即标记**：`closed` 标志通过 `AtomicBool::store(Release)` 设为 `true`，
    ///    此后 `is_closed` 立即返回 `true`，`enqueue` / `enqueue_batch` 拒绝新任务。
    /// 2. **异步通知 Actor**：通过 `tokio::spawn` 向 `SchedulerActor` 发送 `CLOSE` 消息。
    ///    Actor 侧唤醒所有等待 `dequeue` 的消费者，使其返回 `None`。
    /// 3. **fire-and-forget**：`close` 不等待 Actor 响应即返回。即使 Actor 通知失败
    ///    （仅记录 `tracing::warn`），本地状态仍为 closed，客户端侧的入队拒绝语义不受影响。
    ///
    /// # 幂等性
    ///
    /// 重复调用安全：`closed` 标志的 `store` 是幂等的；`CLOSE` 消息会被 Actor 侧
    /// 重复处理但无副作用。
    ///
    /// # 与 Actor 生命周期的关系
    ///
    /// `close` 只负责「不再接单 + 唤醒消费者」。`SchedulerActor` 本身由
    /// `ActorSystem` 的停机流程停止（`Runtime::shutdown` 的 actor 关闭顺序），
    /// 调用方不需要、也没有接口等待它退出。
    ///
    /// # 错误处理
    ///
    /// Actor 通知失败仅记录日志，不返回错误。这是有意为之——即使 Actor 通知失败，
    /// 本地 `closed` 标志已确保客户端不再接受新任务，调度器在 Actor 侧最终也会
    /// 通过其他路径（如 ActorSystem shutdown）停止。
    fn close(&self) {
        // 本地 closed 标志：保证客户端侧 enqueue 立即被拒。
        self.closed.store(true, Ordering::Release);
        // 快路径共享状态 closed 标志：保证直接调用 InnerScheduler::enqueue
        // 的路径（绕过 ActorScheduler::enqueue 的 closed 检查）也被拒绝。
        // InnerScheduler::check_closed 在 enqueue 入口检查此标志。
        if let Some(ref inner) = self.fast_inner {
            inner.close();
        }
        let actor_id = self.actor_id.clone();
        let actor_system = self.actor_system.clone();
        tokio::spawn(async move {
            if let Err(e) = actor_system
                .call(&actor_id, scheduler_methods::CLOSE, vec![])
                .await
            {
                // 停机竞态：shutdown 会先 stop actor，本任务可能在其后被调度。
                // 此时 actor 已注销，而 CLOSE 的目标（不再接单）已由 stop 达成，
                // 因此只在 actor 仍然存在时才视为真实失败。
                if actor_system.list_actors().contains(&actor_id) {
                    tracing::warn!("SchedulerActor close call failed: {}", e);
                } else {
                    tracing::debug!(
                        actor = %actor_id.0,
                        error = %e,
                        "scheduler actor already stopped; close is a no-op"
                    );
                }
            }
        });
    }

    fn is_closed(&self) -> bool {
        // 本地 closed 标志优先：close() 调用后立即生效。
        if self.closed.load(Ordering::Acquire) {
            return true;
        }
        // 快路径共享状态标志：反映 Actor 侧或其他客户端触发的 close。
        if let Some(ref inner) = self.fast_inner {
            return inner.is_closed();
        }
        false
    }
}

#[cfg(test)]
#[path = "../../../../../tests/rust/unit/runtime/workflow/scheduler.rs"]
mod tests;
