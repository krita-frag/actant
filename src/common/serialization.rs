use rkyv::rancor::Error as RkyvError;

use super::{ActantError, Result};

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
