use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::common::{ActorId, ActorMessage, MessageId, Result};
use crate::store::engine::Store;

/// 可序列化的 Actor 消息表示，用于持久化。排除瞬态的回复通道。
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistentMessage {
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
        // 保留原始 message id，用于崩溃恢复后的追踪与去重。
        // ActorMessage::new 会生成新 id，不适合恢复路径。
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

pub(crate) struct MailboxRegistry {
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
        // 持久化前检查 Actor 是否存在，避免产生孤儿消息
        let mailbox = self.mailboxes.get(target).ok_or_else(|| {
            crate::common::ActantError::Actor(format!(
                "actor {} not found in mailbox registry",
                target.0
            ))
        })?;

        // 若 store 可用，先持久化再投递
        let msg_id = msg.id.clone();
        if let Some(ref store) = self.store {
            let persistent = PersistentMessage::from_message(&msg);
            let key = pending_key(target, &msg.id);
            let data = postcard::to_allocvec(&persistent)
                .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
            store.put(&key, &data)?;
        }

        mailbox.tx.send(msg).await.map_err(|e| {
            crate::common::ActantError::Actor(format!("mailbox send failed: {}", e))
        })?;

        // 消息成功投递后从持久化存储中移除（投递前已持久化以保证崩溃安全）。
        if let Some(ref store) = self.store {
            let key = pending_key(target, &msg_id);
            if let Err(e) = store.delete(&key) {
                tracing::warn!("failed to delete pending message {} for actor {} after successful delivery: {}", msg_id.0, target.0, e);
            }
        }

        // 成功发送后，机会式地排空之前 recover_pending 时因邮箱满而遗留的待处理消息。
        self.drain_pending(target);

        Ok(())
    }

    /// 确认消息已处理，从持久化存储中移除。
    pub fn ack_message(&self, actor_id: &ActorId, msg_id: &MessageId) -> Result<()> {
        if let Some(ref store) = self.store {
            let key = pending_key(actor_id, msg_id);
            store.delete(&key)?;
        }
        Ok(())
    }

    /// 尝试投递存储中仍待处理的消息给指定 Actor。
    /// 与 `recover_pending` 不同，此方法非阻塞，邮箱满时静默跳过 — 将在下一次 `send` 时重试。
    fn drain_pending(&self, actor_id: &ActorId) {
        let Some(ref store) = self.store else {
            return;
        };

        let prefix = pending_prefix(actor_id);
        let entries = match store.scan_prefix(&prefix) {
            Ok(e) => e,
            Err(_) => return,
        };

        let Some(inner) = self.mailboxes.get(actor_id) else {
            return;
        };

        for (key, data) in &entries {
            match postcard::from_bytes::<PersistentMessage>(data) {
                Ok(persistent) => {
                    let msg = persistent.to_actor_message();
                    match inner.tx.try_send(msg) {
                        Ok(()) => {
                            if let Err(e) = store.delete(key) {
                                tracing::warn!(
                                    "failed to delete drained pending message for actor {}: {}",
                                    actor_id.0,
                                    e
                                );
                            }
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            if let Err(e) = store.delete(key) {
                                tracing::warn!("failed to delete closed mailbox message: {}", e);
                            }
                        }
                    }
                }
                Err(_) => {
                    if let Err(e) = store.delete(key) {
                        tracing::warn!("failed to delete corrupt mailbox message: {}", e);
                    }
                }
            }
        }
    }

    /// 从存储中恢复待处理消息并重新投递到 Actor 邮箱。
    /// 应在注册 Actor 邮箱通道后调用。
    pub fn recover_pending(&self, actor_id: &ActorId) -> Result<usize> {
        let Some(ref store) = self.store else {
            return Ok(0);
        };

        let prefix = pending_prefix(actor_id);
        let entries = store.scan_prefix(&prefix)?;
        let mut count = 0;
        let mut delete_errors: Vec<String> = Vec::new();

        for (key, data) in &entries {
            match postcard::from_bytes::<PersistentMessage>(data) {
                Ok(persistent) => {
                    if let Some(inner) = self.mailboxes.get(actor_id) {
                        let msg = persistent.to_actor_message();
                        // 尝试非阻塞发送；若邮箱满则跳过并保留在存储中
                        match inner.tx.try_send(msg) {
                            Ok(()) => {
                                if let Err(e) = store.delete(key) {
                                    delete_errors.push(format!(
                                        "delete key {:?} for actor {}: {}",
                                        key, actor_id.0, e
                                    ));
                                }
                                count += 1;
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "mailbox full for actor {}, keeping pending message {} in store",
                                    actor_id.0, persistent.id.0
                                );
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                tracing::warn!(
                                    "mailbox closed for actor {}, dropping pending message {}",
                                    actor_id.0,
                                    persistent.id.0
                                );
                                if let Err(e) = store.delete(key) {
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
                    if let Err(e) = store.delete(key) {
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
            return Err(crate::common::ActantError::Storage(format!(
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_msg(target: &ActorId, method: &str) -> ActorMessage {
        ActorMessage::new(target.clone(), method.to_string(), b"payload".to_vec())
    }

    #[tokio::test]
    async fn send_to_unknown_actor_returns_error() {
        let registry = MailboxRegistry::new();
        let target = ActorId("ghost".into());
        let msg = make_msg(&target, "ping");
        let err = registry.send(&target, msg).await.unwrap_err();
        assert!(err.to_string().contains("not found in mailbox registry"));
    }

    #[tokio::test]
    async fn send_delivers_message_to_registered_channel() {
        let registry = MailboxRegistry::new();
        let actor_id = ActorId("a1".into());
        let (tx, mut rx) = mpsc::channel(8);
        registry.register(actor_id.clone(), tx);

        let msg = make_msg(&actor_id, "ping");
        registry.send(&actor_id, msg).await.unwrap();

        let received = rx.recv().await.expect("should receive message");
        assert_eq!(received.method, "ping");
        assert_eq!(received.target.0, "a1");
    }

    #[tokio::test]
    async fn send_to_closed_channel_returns_error() {
        let registry = MailboxRegistry::new();
        let actor_id = ActorId("a1".into());
        let (tx, rx) = mpsc::channel(8);
        registry.register(actor_id.clone(), tx);
        drop(rx); // 关闭接收端

        let msg = make_msg(&actor_id, "ping");
        let err = registry.send(&actor_id, msg).await.unwrap_err();
        assert!(err.to_string().contains("mailbox send failed"));
    }

    #[tokio::test]
    async fn unregister_removes_actor_from_registry() {
        let registry = MailboxRegistry::new();
        let actor_id = ActorId("a1".into());
        let (tx, _rx) = mpsc::channel(8);
        registry.register(actor_id.clone(), tx);
        registry.unregister(&actor_id);

        let msg = make_msg(&actor_id, "ping");
        let err = registry.send(&actor_id, msg).await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn send_with_store_persists_then_deletes_on_delivery() {
        // 持久化路径：send 先写入 store，投递成功后删除。
        // 验证投递后 store 中无残留 pending 消息。
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let registry = MailboxRegistry::new().with_store(store.clone());
        let actor_id = ActorId("a1".into());
        let (tx, mut rx) = mpsc::channel(8);
        registry.register(actor_id.clone(), tx);

        let msg = make_msg(&actor_id, "ping");
        let msg_id = msg.id.clone();
        registry.send(&actor_id, msg).await.unwrap();

        // 消息应已投递
        assert!(rx.recv().await.is_some());

        // store 中应无残留 pending 消息
        let pending = store.scan_prefix(&pending_prefix(&actor_id)).unwrap();
        assert!(
            pending.is_empty(),
            "pending store should be empty after delivery"
        );

        // ack_message 对已删除的 key 是幂等的（返回 Ok）
        registry.ack_message(&actor_id, &msg_id).unwrap();
    }

    #[tokio::test]
    async fn recover_pending_redelivers_stored_messages() {
        // 模拟崩溃恢复：消息在 store 中但邮箱未接收。
        // recover_pending 应重新投递这些消息。
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let actor_id = ActorId("a1".into());
        // 直接向 store 写入 pending 消息（模拟崩溃前持久化但未投递）
        let msg = make_msg(&actor_id, "recover-me");
        let persistent = PersistentMessage::from_message(&msg);
        let key = pending_key(&actor_id, &msg.id);
        let data = postcard::to_allocvec(&persistent).unwrap();
        store.put(&key, &data).unwrap();

        // 创建 registry 并注册邮箱
        let registry = MailboxRegistry::new().with_store(store.clone());
        let (tx, mut rx) = mpsc::channel(8);
        registry.register(actor_id.clone(), tx);

        let count = registry.recover_pending(&actor_id).unwrap();
        assert_eq!(count, 1, "should have recovered 1 message");

        let received = rx.recv().await.expect("should receive recovered message");
        assert_eq!(received.method, "recover-me");

        // 恢复后 store 中应无残留
        let pending = store.scan_prefix(&pending_prefix(&actor_id)).unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn recover_pending_without_store_returns_zero() {
        let registry = MailboxRegistry::new();
        let actor_id = ActorId("a1".into());
        let (tx, _rx) = mpsc::channel(8);
        registry.register(actor_id.clone(), tx);

        let count = registry.recover_pending(&actor_id).unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn recover_pending_for_unknown_actor_keeps_messages() {
        // recover_pending 对未注册的 actor：消息保留在 store 中（不删除）。
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let actor_id = ActorId("a1".into());
        let msg = make_msg(&actor_id, "pending");
        let persistent = PersistentMessage::from_message(&msg);
        let key = pending_key(&actor_id, &msg.id);
        let data = postcard::to_allocvec(&persistent).unwrap();
        store.put(&key, &data).unwrap();

        // 不注册邮箱直接 recover
        let registry = MailboxRegistry::new().with_store(store.clone());
        let count = registry.recover_pending(&actor_id).unwrap();
        assert_eq!(count, 0, "no mailbox registered, nothing delivered");

        // 消息应仍在 store 中
        let pending = store.scan_prefix(&pending_prefix(&actor_id)).unwrap();
        assert_eq!(pending.len(), 1, "message should remain in store");
    }

    #[tokio::test]
    async fn drain_pending_opportunistic_on_next_send() {
        // drain_pending 在 send 成功后被调用，尝试排空残留 pending 消息。
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let actor_id = ActorId("a1".into());
        // 写入两条 pending 消息（模拟崩溃前残留）
        for method in &["msg1", "msg2"] {
            let msg = make_msg(&actor_id, method);
            let persistent = PersistentMessage::from_message(&msg);
            let key = pending_key(&actor_id, &msg.id);
            let data = postcard::to_allocvec(&persistent).unwrap();
            store.put(&key, &data).unwrap();
        }

        let registry = MailboxRegistry::new().with_store(store.clone());
        let (tx, mut rx) = mpsc::channel(8);
        registry.register(actor_id.clone(), tx);

        // 发送一条新消息，触发 drain_pending
        let new_msg = make_msg(&actor_id, "new");
        registry.send(&actor_id, new_msg).await.unwrap();

        // 应收到 3 条消息：2 条 drained + 1 条新消息
        let mut methods = Vec::new();
        for _ in 0..3 {
            methods.push(rx.recv().await.expect("should receive").method);
        }
        assert!(methods.contains(&"msg1".to_string()));
        assert!(methods.contains(&"msg2".to_string()));
        assert!(methods.contains(&"new".to_string()));

        // store 应已清空
        let pending = store.scan_prefix(&pending_prefix(&actor_id)).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn persistent_message_roundtrip_preserves_fields() {
        let target = ActorId("a1".into());
        let msg = ActorMessage::new(target.clone(), "method".into(), b"data".to_vec());
        let persistent = PersistentMessage::from_message(&msg);
        let restored = persistent.to_actor_message();

        assert_eq!(restored.id.0, msg.id.0);
        assert_eq!(restored.target.0, msg.target.0);
        assert_eq!(restored.method, msg.method);
        assert_eq!(restored.payload, msg.payload);
        // reply_tx 不持久化（瞬态通道）
        assert!(restored.reply_tx.is_none());
    }

    #[test]
    fn pending_key_format_is_namespaced() {
        let actor_id = ActorId("a1".into());
        let msg_id = MessageId("m1".into());
        let key = pending_key(&actor_id, &msg_id);
        assert_eq!(key, "pending:a1:m1");
    }

    #[test]
    fn pending_prefix_format_is_namespaced() {
        let actor_id = ActorId("a1".into());
        let prefix = pending_prefix(&actor_id);
        assert_eq!(prefix, "pending:a1:");
    }
}
