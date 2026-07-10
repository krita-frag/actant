use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use crate::common::scheduler_kind;
use crate::common::{ActantError, ActorId, TaskDefinition};
use crate::runtime::actor::ActorSystem;
use crate::runtime::workflow::actor::scheduler_methods;
use crate::runtime::workflow::messaging::{decode_result, encode, ok_or_error};

/// 任务调度器抽象。
///
/// 此 trait 将 DAG 编排器与具体调度策略解耦。生产实现为
/// [`SchedulerActor`](crate::runtime::workflow::SchedulerActor) +
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
    /// 仅取出未指定 `target_node` 的任务，保留已路由任务，避免「先全部取出再重入队」的竞态。
    async fn drain_unrouted(&self) -> Vec<TaskDefinition>;
    async fn is_empty(&self) -> bool;
    async fn len(&self) -> usize;
    /// 返回所有优先级队列中的任务总数。
    fn total_queued(&self) -> usize {
        0
    }
    /// 关闭调度器——拒绝新入队并唤醒等待者，使 `dequeue()` 在队列清空后返回 `None`。
    ///
    /// # 语义
    ///
    /// 调用后调度器进入 drain 模式：
    /// 1. **新入队被拒**：后续 `enqueue` / `enqueue_batch` 返回
    ///    `Err(ActantError::Internal("scheduler is closed"))`。
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
    /// 它仅标记本地状态并异步通知 Actor，调用方若需确保 Actor 完全停止应继续调用
    /// [`ActorScheduler::join`]。
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

/// `name` 是否为内置调度器种类。
///
/// 供 [`crate::common::SchedulerKind::validate`] 在启动时拒绝未知调度器名，
/// 而非静默回退到默认策略。
pub fn is_registered(name: &str) -> bool {
    matches!(name, scheduler_kind::FIFO | scheduler_kind::PRIORITY)
}

/// 返回内置调度器种类名称的排序列表。
///
/// 供配置错误信息枚举合法选项使用。
pub fn registered_names() -> Vec<String> {
    vec![
        scheduler_kind::FIFO.to_string(),
        scheduler_kind::PRIORITY.to_string(),
    ]
}

/// 通过 [`ActorSystem`] 调用 [`SchedulerActor`] 的客户端。
///
/// 这是 `Worker` 迁移到 Actor 模型的桥接实现：调度状态由
/// `SchedulerActor` 持有，本客户端仅负责序列化请求并通过 actor 消息
/// 协议转发。
#[derive(Clone)]
pub struct ActorScheduler {
    actor_id: ActorId,
    actor_system: Arc<ActorSystem>,
    closed: Arc<AtomicBool>,
}

impl ActorScheduler {
    pub fn new(actor_id: ActorId, actor_system: Arc<ActorSystem>) -> Self {
        Self {
            actor_id,
            actor_system,
            closed: Arc::new(AtomicBool::new(false)),
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
        let result = self
            .call(scheduler_methods::ENQUEUE, encode(&task)?)
            .await?;
        ok_or_error(result)
    }

    async fn enqueue_batch(&self, tasks: Vec<TaskDefinition>) -> Result<(), ActantError> {
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

    async fn drain_unrouted(&self) -> Vec<TaskDefinition> {
        match self.call(scheduler_methods::DRAIN_UNROUTED, vec![]).await {
            Ok(result) => decode_result::<Vec<TaskDefinition>>(result).unwrap_or_default(),
            Err(e) => {
                tracing::warn!("SchedulerActor drain_unrouted call failed: {}", e);
                vec![]
            }
        }
    }

    async fn is_empty(&self) -> bool {
        match self.call(scheduler_methods::IS_EMPTY, vec![]).await {
            Ok(result) => decode_result::<bool>(result).unwrap_or(true),
            Err(e) => {
                tracing::warn!("SchedulerActor is_empty call failed: {}", e);
                true
            }
        }
    }

    async fn len(&self) -> usize {
        match self.call(scheduler_methods::LEN, vec![]).await {
            Ok(result) => decode_result::<usize>(result).unwrap_or(0),
            Err(e) => {
                tracing::warn!("SchedulerActor len call failed: {}", e);
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
    /// # 与 [`ActorScheduler::join`] 的关系
    ///
    /// 关闭流程分两步：
    /// ```text
    /// client.close();        // 1. 标记本地 + 异步通知 Actor
    /// client.join().await?;  // 2. 等待 SchedulerActor 实际停止
    /// ```
    /// 若调用方需要确保 Actor 完全退出（例如 shutdown 序列），应在 `close` 之后
    /// 调用 `join`。仅调用 `close` 不足以保证 Actor 已停止。
    ///
    /// # 错误处理
    ///
    /// Actor 通知失败仅记录日志，不返回错误。这是有意为之——即使 Actor 通知失败，
    /// 本地 `closed` 标志已确保客户端不再接受新任务，调度器在 Actor 侧最终也会
    /// 通过其他路径（如 ActorSystem shutdown）停止。
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let actor_id = self.actor_id.clone();
        let actor_system = self.actor_system.clone();
        tokio::spawn(async move {
            if let Err(e) = actor_system
                .call(&actor_id, scheduler_methods::CLOSE, vec![])
                .await
            {
                tracing::warn!("SchedulerActor close call failed: {}", e);
            }
        });
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl ActorScheduler {
    /// 同步等待底层 `SchedulerActor` 实际停止。
    ///
    /// `close()` 是 fire-and-forget：它标记本地状态为 closed 并异步通知 Actor。
    /// 调用方若需要确保 Actor 完全停止（例如 shutdown 流程），应在 `close()`
    /// 之后调用 `join()`。
    pub async fn join(&self) -> Result<(), ActantError> {
        self.actor_system.stop(&self.actor_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{ActorId, TaskId};
    use crate::runtime::actor::ActorSystem;
    use crate::runtime::workflow::SchedulerActor;

    fn make_task(name: &str, priority: i32) -> TaskDefinition {
        TaskDefinition {
            id: TaskId::generate(),
            name: name.to_string(),
            payload: Vec::new(),
            workflow_id: None,
            target_node: None,
            origin_node: None,
            retry_policy: None,
            priority,
            timeout_ms: None,
            attempt: 0,
            enqueued_at_ms: 0,
            target_endpoint_addr: None,
            origin_endpoint_addr: None,
        }
    }

    #[test]
    fn is_registered_recognizes_builtin_kinds() {
        assert!(is_registered(scheduler_kind::PRIORITY));
        assert!(is_registered(scheduler_kind::FIFO));
        assert!(!is_registered("nonexistent"));
    }

    #[test]
    fn registered_names_includes_builtins() {
        let names = registered_names();
        assert!(names.contains(&scheduler_kind::PRIORITY.to_string()));
        assert!(names.contains(&scheduler_kind::FIFO.to_string()));
    }
    
    #[tokio::test]
    async fn actor_scheduler_forwards_through_scheduler_actor() {
        let actor_system = Arc::new(ActorSystem::new());
        let actor_id = ActorId::new("scheduler-test".to_string());
        actor_system
            .spawn(actor_id.clone(), SchedulerActor::priority())
            .await
            .unwrap();

        let client = ActorScheduler::new(actor_id, actor_system);
        client.enqueue(make_task("low", -10)).await.unwrap();
        client.enqueue(make_task("high", 10)).await.unwrap();
        client.enqueue(make_task("mid", 0)).await.unwrap();

        assert_eq!(client.len().await, 3);
        assert!(!client.is_empty().await);
        assert_eq!(client.try_dequeue().await.unwrap().name, "high");
        assert_eq!(client.try_dequeue().await.unwrap().name, "mid");
        assert_eq!(client.try_dequeue().await.unwrap().name, "low");
        assert!(client.try_dequeue().await.is_none());

        client.close();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(client.is_closed());
    }
}
