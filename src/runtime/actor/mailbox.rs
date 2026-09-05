//! Actor 邮箱注册表与持久化待发消息。
//!
//! `MailboxRegistry` 维护 ActorId → mpsc::Sender 映射，并通过可选的
//! `Store` 持久化未确认消息，支持崩溃后 `recover_pending` 重投。
//!
//! ## 投递语义：at-least-once
//!
//! `send` 在入队前将消息持久化为 pending 记录；`ack_message` 在消息被
//! actor **成功处理后**删除该记录。若 actor 在处理前崩溃，或处理失败 /
//! panic（不 ack），重启后 `recover_pending` 会重投该消息。因此消费端
//! handler 必须具备幂等性——同一条消息可能被处理多次。
//!
//! 重投是有界的：连续 [`MAX_PENDING_REDELIVERIES`] 次重投后仍未被 ack 的
//! 消息判定为毒消息，pending 记录被删除（不再无限累积）。

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::common::{ActantError, ActorId, ActorMessage, MessageId, Result};
use crate::runtime::state::Store;

/// 单条 pending 消息允许的最大重投次数。
///
/// 超过该次数仍未被 actor 成功处理（ack）的消息判定为**毒消息**：
/// 确定性失败的消息（如 gossip COMPLETE_TASK 打在已淘汰 workflow 上）
/// 永远不会被 ack，无界重投会让每次 spawn 全量重投且 pending 记录无限
/// 累积。达到上限后删除 pending 记录并记录 error 日志（含 actor_id /
/// msg_id / delivery_count），供未来 DLQ capability 消费。
const MAX_PENDING_REDELIVERIES: u32 = 5;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistentMessage {
    id: MessageId,
    target: ActorId,
    method: String,
    payload: Vec<u8>,
    /// 已重投次数（首次投递为 0）。每次 `recover_pending` 重投成功后
    /// 递增并回写；超过 [`MAX_PENDING_REDELIVERIES`] 时记录被丢弃。
    delivery_count: u32,
}

impl PersistentMessage {
    /// 已重投次数，供测试读取。
    #[cfg(test)]
    pub(crate) fn delivery_count(&self) -> u32 {
        self.delivery_count
    }

    fn from_message(msg: &ActorMessage) -> Self {
        Self {
            id: msg.id.clone(),
            target: msg.target.clone(),
            method: msg.method.clone(),
            payload: msg.payload.clone(),
            delivery_count: 0,
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
        // 在任何 await 之前克隆 Sender 与 Store 引用并立即释放 DashMap read guard。
        // guard 不得跨越任何 await：若跨越 store.put().await / mailbox.tx.send().await /
        // store.delete().await，会与 unregister/register 对同一 shard 的写操作
        // 互斥，造成 actor 重启/停止路径的延迟尖峰。
        let (tx, store) = {
            let mailbox = self.mailboxes.get(target).ok_or_else(|| {
                ActantError::Actor(format!("actor {} not found in mailbox registry", target.0))
            })?;
            (mailbox.tx.clone(), self.store.clone())
        };

        // at-least-once：入队前持久化 pending 记录；仅在 ack_message
        // （actor 成功处理后）删除，入队成功不删除——否则进程崩溃时
        // 已入队但未处理的消息会丢失。
        let msg_id = msg.id.clone();
        let pending_record_key = pending_key(target, &msg.id);
        if let Some(ref store) = store {
            let persistent = PersistentMessage::from_message(&msg);
            let data = postcard::to_allocvec(&persistent)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            if let Err(e) = store.put(&pending_record_key, &data).await {
                tracing::warn!(
                    "failed to persist pending message {} for actor {}: {}",
                    msg_id.0,
                    target.0,
                    e
                );
            }
        }

        tx.send(msg).await.map_err(|e| {
            // 投递失败（actor 已停止/mailbox 关闭）：回滚 pending 记录。调用方
            // 已收到发送失败，若记录残留，同 id actor 重新注册并 recover_pending
            // 时会把这条"调用方确认失败"的消息重新投递。
            if let Some(cleanup_store) = store.clone() {
                let key = pending_record_key.clone();
                tokio::spawn(async move {
                    if let Err(cleanup_err) = cleanup_store.delete(&key).await {
                        tracing::warn!(
                            "failed to clean up pending message {} after send error: {}",
                            msg_id.0,
                            cleanup_err
                        );
                    }
                });
            }
            ActantError::Actor(format!("mailbox send failed: {}", e))
        })?;

        Ok(())
    }

    /// 确认消息已成功处理：删除持久化 pending 记录。
    ///
    /// 仅在 `RunningActor` 处理消息成功后调用；失败 / panic 路径不调用，
    /// 使未确认消息在重启后由 `recover_pending` 重投（at-least-once）。
    pub async fn ack_message(&self, actor_id: &ActorId, msg_id: &MessageId) -> Result<()> {
        if let Some(ref store) = self.store {
            store.delete(&pending_key(actor_id, msg_id)).await?;
        }
        Ok(())
    }

    /// 重投未确认的持久化消息（崩溃恢复）。
    ///
    /// 重投**不**删除 pending 记录：记录仅在 `ack_message`（成功处理后）删除，
    /// 因此未确认消息在每次 spawn 时都会重投，直到被成功处理并确认
    /// （at-least-once，消费端需幂等）。
    ///
    /// 重投是**有界**的：每次重投成功后递增记录的 `delivery_count` 并回写；
    /// 超过 [`MAX_PENDING_REDELIVERIES`] 仍不被 ack 的消息判定为毒消息，
    /// 删除记录并记录 error（供未来 DLQ capability 消费），不再重投。
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
                    // 毒消息判定：本次重投将使计数超过上限时直接丢弃。
                    let delivery_count = persistent.delivery_count + 1;
                    if delivery_count > MAX_PENDING_REDELIVERIES {
                        tracing::error!(
                            actor_id = %actor_id.0,
                            msg_id = %persistent.id.0,
                            delivery_count,
                            "poison message: exceeded max pending redeliveries, dropping record"
                        );
                        if let Err(e) = store.delete(&key).await {
                            delete_errors.push(format!(
                                "delete poison message {} for actor {}: {}",
                                persistent.id.0, actor_id.0, e
                            ));
                        }
                        continue;
                    }
                    if let Some(inner) = self.mailboxes.get(actor_id) {
                        let msg = persistent.to_actor_message();
                        match inner.tx.try_send(msg) {
                            Ok(()) => {
                                // 重投成功但**不**删除 pending 记录：消息尚未被
                                // actor 成功处理，删除会使 ack_message 失去意义
                                // （处理中崩溃即丢消息）。记录由成功处理后的
                                // ack_message 删除（at-least-once）。
                                //
                                // 回写递增后的重投计数；写回失败不丢消息，
                                // 仅使毒消息判定推迟到计数成功回写的那轮。
                                let updated = PersistentMessage {
                                    delivery_count,
                                    ..persistent
                                };
                                match postcard::to_allocvec(&updated) {
                                    Ok(data) => {
                                        if let Err(e) = store.put(&key, &data).await {
                                            tracing::warn!(
                                                "failed to update delivery count for message {} of actor {}: {}",
                                                updated.id.0,
                                                actor_id.0,
                                                e
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "failed to serialize pending message {} for actor {}: {}",
                                            updated.id.0,
                                            actor_id.0,
                                            e
                                        );
                                    }
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
                                // 邮箱已关闭：actor 可能正在退出，保留 pending
                                // 记录，待下次 spawn 时重投（at-least-once）。
                                tracing::warn!(
                                    "mailbox closed for actor {}, keeping pending message {} in store",
                                    actor_id.0,
                                    persistent.id.0
                                );
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
                    // 无法反序列化的记录永远不会被成功处理，删除以免永久堆积。
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

/// pending 记录的存储 key，供测试与诊断使用。
pub(crate) fn pending_key(actor_id: &ActorId, msg_id: &MessageId) -> String {
    format!("pending:{}:{}", actor_id.0, msg_id.0)
}
