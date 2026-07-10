//! Capability 的 Actor 化执行器。
//!
//! 每个已注册且带有序列化 codec 的 capability 都会在 `ActorSystem` 中拥有一个
//! `CapabilityActor`。该 Actor 直接持有 handler 链与 codec，从而：
//! - 享有 Actor 的监督、生命周期、持久化能力；
//! - 保留 capability 请求的强类型语义（通过 codec 在消息边界做序列化）。

use std::sync::Arc;

use async_trait::async_trait;

use crate::common::{ActorMessage, ActorMessageResult, MessageId};
use crate::runtime::actor::Actor;
use crate::runtime::capability::{CapabilityCodec, EffectKind, ErasedHandler};

/// Capability 执行 Actor。
///
/// 关键设计：不持有固定 `kind`，而是根据每条消息的
/// [`CapabilityEnvelope::kind`] 分发。这允许同一个 capability
/// 同时支持 `ask`（决策查询）、`perform`（副作用执行）、`emit`（事件广播）
/// 三种调用语义，由调用方按需选择。
pub struct CapabilityActor {
    handlers: Vec<Arc<dyn ErasedHandler>>,
    codec: Arc<dyn CapabilityCodec>,
}

impl CapabilityActor {
    pub fn new(handlers: Vec<Arc<dyn ErasedHandler>>, codec: Arc<dyn CapabilityCodec>) -> Self {
        Self { handlers, codec }
    }

    fn ok(message_id: MessageId) -> ActorMessageResult {
        ActorMessageResult {
            message_id,
            payload: vec![],
            error: None,
        }
    }

    fn payload(message_id: MessageId, payload: Vec<u8>) -> ActorMessageResult {
        ActorMessageResult {
            message_id,
            payload,
            error: None,
        }
    }

    fn error(message_id: MessageId, err: crate::common::ActantError) -> ActorMessageResult {
        ActorMessageResult {
            message_id,
            payload: vec![],
            error: Some(err.to_string()),
        }
    }
}

#[async_trait]
impl Actor for CapabilityActor {
    fn actor_type(&self) -> &str {
        "CapabilityActor"
    }

    async fn handle_message(
        &mut self,
        msg: ActorMessage,
    ) -> crate::common::Result<ActorMessageResult> {
        let msg_id = msg.id.clone();
        let envelope: crate::runtime::capability::CapabilityEnvelope =
            match crate::common::decode_postcard(&msg.payload) {
                Ok(e) => e,
                Err(e) => {
                    return Ok(Self::error(msg_id, e));
                }
            };

        let req = match self.codec.deserialize_request(&envelope.payload) {
            Ok(r) => r,
            Err(e) => return Ok(Self::error(msg_id, e)),
        };

        // 关键：根据 envelope.kind 分发，而非固定 kind。
        // 这样同一 capability 可同时被 ask/perform/emit 调用。
        match envelope.kind {
            EffectKind::Ask => {
                for handler in self.handlers.iter().rev() {
                    if let Some(resp) = handler.ask(req.clone()).await {
                        let bytes = match self.codec.serialize_response(resp) {
                            Ok(b) => b,
                            Err(e) => return Ok(Self::error(msg_id, e)),
                        };
                        let mut result = vec![1u8];
                        result.extend_from_slice(&bytes);
                        return Ok(Self::payload(msg_id, result));
                    }
                }
                Ok(Self::payload(msg_id, vec![0u8]))
            }
            EffectKind::Perform => {
                if let Some(handler) = self.handlers.last() {
                    match handler.perform(req).await {
                        Ok(resp) => {
                            let bytes = match self.codec.serialize_response(resp) {
                                Ok(b) => b,
                                Err(e) => return Ok(Self::error(msg_id, e)),
                            };
                            Ok(Self::payload(msg_id, bytes))
                        }
                        Err(e) => Ok(Self::error(msg_id, e)),
                    }
                } else {
                    Ok(Self::error(
                        msg_id,
                        crate::common::ActantError::Internal(
                            "perform: no handler registered".into(),
                        ),
                    ))
                }
            }
            EffectKind::Emit => {
                for handler in &self.handlers {
                    if let Err(e) = handler.emit(req.clone()).await {
                        return Ok(Self::error(msg_id, e));
                    }
                }
                Ok(Self::ok(msg_id))
            }
        }
    }
}
