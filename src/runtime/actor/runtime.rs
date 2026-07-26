//! Actor 抽象与生命周期上下文。
//!
//! `Actor` trait 是用户/系统 Actor 实现的唯一契约；`ActorContext` 跟踪
//! 单个 actor 实例的状态机。两者不依赖 `ActorSystem`，便于在测试中
//! 独立构造。

use async_trait::async_trait;

use crate::common::ActantError;
use crate::common::{ActorId, ActorMessage, ActorMessageResult, ActorStatus, Result};

#[async_trait]
/// Actant Actor 的最小行为契约。
///
/// Actor 由 [`crate::runtime::actor::system::ActorSystem`] 独占驱动：每个 actor
/// 实例在自己的任务中顺序处理 [`ActorMessage`]，因此实现内部通常不需要额外同步。
/// Actor 可以选择实现状态持久化钩子；不支持持久化的 actor 使用默认 no-op 实现即可。
///
/// # Lifecycle
///
/// [`ActorSystem::spawn`] 会先调用 [`Actor::on_start`]，再进入消息循环。
/// [`ActorSystem::stop`] 发送停止信号后调用 [`Actor::on_stop`]。如果 actor panic，
/// runtime 会把 panic 转换为 actor 错误并通过监督事件广播。
pub trait Actor: Send + Sync + 'static {
    /// 返回稳定的 actor 类型名称，用于日志、监督和跨节点 actor 路由。
    fn actor_type(&self) -> &str;

    /// 处理一条消息并返回响应。
    ///
    /// # Errors
    ///
    /// 返回错误表示消息处理失败；调用方会收到该错误，监督树也可观测到失败事件。
    /// 不要用 panic 表示业务错误，panic 只用于不可恢复的实现缺陷。
    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult>;

    /// 序列化 actor 当前状态。
    ///
    /// 默认返回空字节，表示 actor 没有需要持久化的状态。
    ///
    /// # Errors
    ///
    /// 如果状态无法序列化或底层资源不可用，应返回错误。调用方会保留原状态，
    /// 并通过 tracing 记录失败。
    fn save_state(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    /// 从已保存字节恢复 actor 状态。
    ///
    /// # Errors
    ///
    /// 如果字节格式不兼容、损坏或无法应用到当前 actor，应返回错误。
    fn load_state(&mut self, _state: &[u8]) -> Result<()> {
        Ok(())
    }

    /// 返回 actor 是否参与 checkpoint/WAL 恢复。
    fn supports_state_persistence(&self) -> bool {
        false
    }

    /// actor 启动钩子。
    ///
    /// # Errors
    ///
    /// 返回错误会使 spawn 失败，actor 不会进入消息循环。
    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    /// actor 停止钩子。
    ///
    /// # Errors
    ///
    /// 返回错误会传播给停止调用方，并通过 tracing 暴露。
    async fn on_stop(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Actor for Box<dyn Actor> {
    fn actor_type(&self) -> &str {
        (**self).actor_type()
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        (**self).handle_message(msg).await
    }

    fn save_state(&self) -> Result<Vec<u8>> {
        (**self).save_state()
    }

    fn load_state(&mut self, state: &[u8]) -> Result<()> {
        (**self).load_state(state)
    }

    fn supports_state_persistence(&self) -> bool {
        (**self).supports_state_persistence()
    }

    async fn on_start(&mut self) -> Result<()> {
        (**self).on_start().await
    }

    async fn on_stop(&mut self) -> Result<()> {
        (**self).on_stop().await
    }
}

pub struct ActorContext {
    pub actor_id: ActorId,
    pub status: ActorStatus,
}

impl ActorContext {
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            status: ActorStatus::Created,
        }
    }

    pub fn transition(&mut self, new_status: ActorStatus) -> Result<()> {
        let valid = matches!(
            (&self.status, &new_status),
            (ActorStatus::Created, ActorStatus::Running)
                | (ActorStatus::Running, ActorStatus::Failed)
                | (ActorStatus::Failed, ActorStatus::Running)
                | (ActorStatus::Running, ActorStatus::Stopped)
        );

        if !valid {
            return Err(ActantError::Actor(format!(
                "invalid state transition: {:?} -> {:?}",
                self.status, new_status
            )));
        }

        self.status = new_status;
        Ok(())
    }
}
