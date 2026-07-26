//! Actor 邮箱注册表与持久化待发消息。
//!
//! `MailboxRegistry` 维护 ActorId → mpsc::Sender 映射，并通过可选的
//! `Store` 持久化未确认消息，支持崩溃后 `recover_pending` 重投。

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::common::{ActantError, ActorId, ActorMessage, MessageId, Result};
use crate::runtime::state::Store;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistentMessage {
    id: MessageId,
    target: ActorId,
    method: String,
    payload: Vec<u8>,
}

impl PersistentMessage {
    fn from_message(msg: &ActorMessage) -> Self {
        Self {
            id: msg.id.clone(),
            target: msg.target.clone(),
            method: msg.method.clone(),
            payload: msg.payload.clone(),
        }
    }

    fn to_actor_message(&self) -> ActorMessage {
        ActorMessage {
            id: self.id.clone(),
            target: self.target.clone(),
            method: self.method.clone(),
            payload: self.payload.clone(),
            reply_tx: None,
        }
    }
}

#[derive(Clone)]
struct MailboxInner {
    tx: mpsc::Sender<ActorMessage>,
}

pub struct MailboxRegistry {
    mailboxes: DashMap<ActorId, MailboxInner>,
    store: Option<Store>,
}

impl MailboxRegistry {
    pub fn new() -> Self {
        Self {
            mailboxes: DashMap::new(),
            store: None,
        }
    }

    pub fn with_store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    pub fn register(&self, actor_id: ActorId, tx: mpsc::Sender<ActorMessage>) {
        self.mailboxes.insert(actor_id, MailboxInner { tx });
    }

    pub fn unregister(&self, actor_id: &ActorId) {
        self.mailboxes.remove(actor_id);
    }

    pub async fn send(&self, target: &ActorId, msg: ActorMessage) -> Result<()> {
        let mailbox = self.mailboxes.get(target).ok_or_else(|| {
            ActantError::Actor(format!("actor {} not found in mailbox registry", target.0))
        })?;

        let msg_id = msg.id.clone();
        if let Some(ref store) = self.store {
            let persistent = PersistentMessage::from_message(&msg);
            let key = pending_key(target, &msg.id);
            let data = postcard::to_allocvec(&persistent)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            if let Err(e) = store.put(&key, &data).await {
                tracing::warn!(
                    "failed to persist pending message {} for actor {}: {}",
                    msg_id.0,
                    target.0,
                    e
                );
            }
        }

        mailbox
            .tx
            .send(msg)
            .await
            .map_err(|e| ActantError::Actor(format!("mailbox send failed: {}", e)))?;

        if let Some(ref store) = self.store {
            let key = pending_key(target, &msg_id);
            if let Err(e) = store.delete(&key).await {
                tracing::warn!(
                    "failed to delete pending message {} for actor {} after successful delivery: {}",
                    msg_id.0, target.0, e
                );
            }
        }

        Ok(())
    }

    pub async fn ack_message(&self, actor_id: &ActorId, msg_id: &MessageId) -> Result<()> {
        if let Some(ref store) = self.store {
            store.delete(&pending_key(actor_id, msg_id)).await?;
        }
        Ok(())
    }

    pub async fn recover_pending(&self, actor_id: &ActorId) -> Result<usize> {
        let Some(ref store) = self.store else {
            return Ok(0);
        };

        let prefix = pending_prefix(actor_id);
        let entries = store.scan_prefix(&prefix).await?;
        let mut count = 0;
        let mut delete_errors: Vec<String> = Vec::new();

        for (key, data) in entries {
            match postcard::from_bytes::<PersistentMessage>(&data) {
                Ok(persistent) => {
                    if let Some(inner) = self.mailboxes.get(actor_id) {
                        let msg = persistent.to_actor_message();
                        match inner.tx.try_send(msg) {
                            Ok(()) => {
                                if let Err(e) = store.delete(&key).await {
                                    delete_errors.push(format!(
                                        "delete key {:?} for actor {}: {}",
                                        key, actor_id.0, e
                                    ));
                                }
                                count += 1;
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "mailbox full for actor {}, keeping pending message {} in store",
                                    actor_id.0, persistent.id.0
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                tracing::warn!(
                                    "mailbox closed for actor {}, dropping pending message {}",
                                    actor_id.0,
                                    persistent.id.0
                                );
                                if let Err(e) = store.delete(&key).await {
                                    delete_errors.push(format!(
                                        "delete key {:?} for actor {}: {}",
                                        key, actor_id.0, e
                                    ));
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to deserialize pending message for {}: {}",
                        actor_id.0,
                        e
                    );
                    if let Err(e) = store.delete(&key).await {
                        delete_errors.push(format!(
                            "delete key {:?} for actor {}: {}",
                            key, actor_id.0, e
                        ));
                    }
                }
            }
        }

        if !delete_errors.is_empty() {
            tracing::warn!(
                "recover_pending for actor {} had {} delete errors",
                actor_id.0,
                delete_errors.len()
            );
            return Err(ActantError::Storage(format!(
                "recover_pending for actor {}: {} delete errors: {}",
                actor_id.0,
                delete_errors.len(),
                delete_errors.join("; ")
            )));
        }

        if count > 0 {
            tracing::info!(
                "recovered {} pending messages for actor {}",
                count,
                actor_id.0
            );
        }
        Ok(count)
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
            store: self.store.clone(),
        }
    }
}

fn pending_prefix(actor_id: &ActorId) -> String {
    format!("pending:{}:", actor_id.0)
}

fn pending_key(actor_id: &ActorId, msg_id: &MessageId) -> String {
    format!("pending:{}:{}", actor_id.0, msg_id.0)
}
