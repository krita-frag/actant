use rkyv::rancor::Error as RkyvError;

use super::{ActantError, Result};

/// 反序列化外部输入时的最大字节数。
///
/// 超过此大小的 `postcard::from_bytes` 调用会在解码前直接拒绝，避免恶意
/// 嵌套结构触发 OOM。4 MiB 足够容纳单个 task payload / capability 请求 /
/// 网络协议消息；超出此阈值的请求应走分片或外部 blob 存储。
pub const MAX_DECODE_SIZE: usize = 4 * 1024 * 1024;

/// 反序列化外部输入的字节切片，先校验长度上限再调用 `postcard::from_bytes`。
///
/// 用于所有远端输入边界（网络、capability 远端调用、gossip 等）。
/// 本地持久化数据可使用 `postcard::from_bytes` 直接解码。
pub fn decode_postcard<T>(bytes: &[u8]) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if bytes.len() > MAX_DECODE_SIZE {
        return Err(ActantError::Serialization(format!(
            "decode rejected: {} bytes exceeds MAX_DECODE_SIZE ({} bytes)",
            bytes.len(),
            MAX_DECODE_SIZE
        )));
    }
    postcard::from_bytes(bytes).map_err(|e| ActantError::Serialization(e.to_string()))
}

/// 将值序列化为 rkyv 字节向量。
///
/// 公开以允许嵌入应用与基准测试直接测量序列化开销，
/// 与 Rust 核心内部使用的路径完全一致。
pub fn serialize_rkyv<T>(value: &T) -> Result<Vec<u8>>
where
    T: rkyv::Archive,
    for<'a> T: rkyv::Serialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            RkyvError,
        >,
    >,
{
    rkyv::to_bytes::<RkyvError>(value)
        .map_err(|e| ActantError::Serialization(e.to_string()))
        .map(|v| v.to_vec())
}

/// 从 rkyv 字节反序列化值。
///
/// 公开以允许嵌入应用与基准测试直接测量反序列化开销，
/// 与 Rust 核心内部使用的路径完全一致。
pub fn deserialize_rkyv_value<T>(data: &[u8]) -> Result<T>
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, RkyvError>>,
{
    rkyv::from_bytes::<T, RkyvError>(data).map_err(|e| ActantError::Serialization(e.to_string()))
}

#[cfg(test)]
#[path = "../../tests/rust/unit/common/serialization.rs"]
mod tests;
