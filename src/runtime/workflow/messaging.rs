//! Actor 消息序列化辅助工具。
//!
//! 提供 `ActorSystem::call` 交互所需的编码/解码/结果构建函数，
//! 供 `ActorScheduler` 及其他 Actor 客户端复用。

use std::time::Instant;

use crate::common::{ActantError, ActorMessageResult, Result};

/// 将值序列化为 postcard 字节。
pub(crate) fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let t0 = Instant::now();
    let result =
        postcard::to_allocvec(value).map_err(|e| ActantError::Serialization(e.to_string()));
    crate::metrics::observe_payload_serialize_ms(t0.elapsed().as_millis() as u64);
    result
}

/// 从 postcard 字节反序列化为目标类型。
///
/// 走 `decode_postcard` 校验大小上限，因为 Actor 消息可能来自远端节点。
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let t0 = Instant::now();
    let result = crate::common::decode_postcard(bytes);
    crate::metrics::observe_payload_deserialize_ms(t0.elapsed().as_millis() as u64);
    result
}

/// 构造成功（空 payload）的 [`ActorMessageResult`]。
pub(crate) fn ok_result(msg_id: crate::common::MessageId) -> ActorMessageResult {
    ActorMessageResult {
        message_id: msg_id,
        payload: vec![],
        error: None,
    }
}

/// 构造带 payload 的 [`ActorMessageResult`]。
pub(crate) fn payload_result(
    msg_id: crate::common::MessageId,
    payload: Vec<u8>,
) -> ActorMessageResult {
    ActorMessageResult {
        message_id: msg_id,
        payload,
        error: None,
    }
}

/// 将 [`ActorMessageResult`] 解码为目标类型；若 Actor 返回错误则转换为 [`ActantError`]。
pub(crate) fn decode_result<T: serde::de::DeserializeOwned>(
    result: ActorMessageResult,
) -> Result<T> {
    if let Some(err) = result.error {
        return Err(ActantError::from(err));
    }
    decode(&result.payload)
}

/// 验证 [`ActorMessageResult`] 为成功（空 payload），否则返回 Actor 错误。
pub(crate) fn ok_or_error(result: ActorMessageResult) -> Result<()> {
    if let Some(err) = result.error {
        return Err(ActantError::from(err));
    }
    Ok(())
}
