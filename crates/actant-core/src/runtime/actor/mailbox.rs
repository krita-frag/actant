//! Actor 邮箱注册表。
//!
//! `MailboxRegistry` 维护 ActorId → mpsc::Sender 映射，提供进程内
//! 最多一次（at-most-once）投递：`send` 在邮箱满/关闭时返回错误，
//! 由调用方决定重试或失败。不做任何持久化——跨重启恢复由 orchestrator
//! 的统一工作流历史承载，mailbox 只服务进程内系统 actor 的消息通道。

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
