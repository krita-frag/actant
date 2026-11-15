//! Actor 邮箱注册表。
//!
//! `MailboxRegistry` 维护 ActorId → mpsc::Sender 映射，提供进程内
//! 最多一次（at-most-once）投递：邮箱使用**有界**通道，满时 `send`
//! 阻塞等待（背压），仅在 Actor 已注销或通道关闭时返回错误——不缓存、
//! 不重放。调用方若需要非阻塞投递或超时，应在上层用
//! `tokio::time::timeout` 包裹 [`MailboxRegistry::send`]。
//! 不做任何持久化——跨重启恢复由 orchestrator 的统一工作流历史承载，
//! mailbox 只服务进程内系统 actor 的消息通道。

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::common::{ActantError, ActorId, ActorMessage, Result};

#[derive(Clone)]
struct MailboxInner {
    tx: mpsc::Sender<ActorMessage>,
}

pub struct MailboxRegistry {
    mailboxes: DashMap<ActorId, MailboxInner>,
}

impl MailboxRegistry {
    pub fn new() -> Self {
        Self {
            mailboxes: DashMap::new(),
        }
    }

    pub fn register(&self, actor_id: ActorId, tx: mpsc::Sender<ActorMessage>) {
        self.mailboxes.insert(actor_id, MailboxInner { tx });
    }

    pub fn unregister(&self, actor_id: &ActorId) {
        self.mailboxes.remove(actor_id);
    }

    /// 向目标 Actor 投递消息。
    ///
    /// 邮箱满时等待（有界背压），只有目标已注销或通道关闭才返回
    /// [`ActantError::Actor`]。需要超时语义的调用方在外层包裹
    /// `tokio::time::timeout`。
    pub async fn send(&self, target: &ActorId, msg: ActorMessage) -> Result<()> {
        // 在任何 await 之前克隆 Sender 并立即释放 DashMap read guard。
        // guard 不得跨越 mailbox.tx.send().await：会与 unregister/register
        // 对同一 shard 的写操作互斥，造成 actor 重启/停止路径的延迟尖峰。
        let tx = {
            let mailbox = self.mailboxes.get(target).ok_or_else(|| {
                ActantError::Actor(format!("actor {} not found in mailbox registry", target.0))
            })?;
            mailbox.tx.clone()
        };

        tx.send(msg)
            .await
            .map_err(|e| ActantError::Actor(format!("mailbox send failed: {}", e)))?;
        Ok(())
    }
}

impl Default for MailboxRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MailboxRegistry {
    fn clone(&self) -> Self {
        Self {
            mailboxes: self.mailboxes.clone(),
        }
    }
}
