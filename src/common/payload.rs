//! 任务 payload 编解码与完整性保护。
//!
//! ## 设计原则
//!
//! Rust 核心层**不感知** Python 的参数语义（args/kwargs/TaskRef 位置等）。
//! 本模块只提供两个职责：
//!
//! 1. **基础打包**：`pack_single` / `pack_group` — 由 Python 侧调用构建 default_payload，
//!    Rust 视为不透明字节。
//! 2. **上游结果前置**：`pack_upstream_prefix` — 由 Rust orchestrator 调用，
//!    把前驱任务结果机械地前置到 default_payload，**不检查 default_payload 的 tag 类型**。
//!
//! ## Payload 格式
//!
//! ### default_payload（由 Python 构建，Rust 视为不透明）
//!
//! Python 侧定义自己的 tag 系统（TAG_SINGLE/TAG_GROUP/TAG_GENERIC/TAG_POSITIONAL 等），
//! Rust 不需要知道这些 tag 的含义。
//!
//! ### 最终 payload（Rust 构建后）
//!
//! 若任务有前驱，Rust 用 `pack_upstream_prefix` 包装：
//! `[TAG_UPSTREAM_PREFIX, upstream_count(u32), up_len1(u32), up_bytes1, ..., default_payload]`
//!
//! Python dispatcher 收到后先解包 upstream prefix，再把剩余的 default_payload
//! 交给对应的 tag dispatcher 处理。
//!
//! ## Payload 完整性保护
//!
//! 所有任务 payload 在序列化后签名，反序列化前验证。
//! 防止被攻破的节点向集群投递恶意 cloudpickle payload。

/// 位置参数调用标签（仅用于 `unpack_payload` 内部校验）。
const TAG_SINGLE: u8 = 0x00;
/// 组结果调用标签（仅用于 `unpack_payload` 内部校验）。
const TAG_GROUP: u8 = 0x01;
/// 位置+关键字参数调用标签（仅用于 `unpack_payload` 内部校验）。
const TAG_SINGLE_KW: u8 = 0x02;
/// 上游结果前置标签：Rust orchestrator 用此标签包装 default_payload。
///
/// 格式：`[TAG_UPSTREAM_PREFIX, upstream_count(u32 LE), up_len1(u32 LE), up_bytes1, ..., default_payload]`
/// Python dispatcher 先解包此前缀，再把 default_payload 交给对应 tag 的 dispatcher。
pub const TAG_UPSTREAM_PREFIX: u8 = 0x08;

/// 将单个结果打包为 `[TAG_SINGLE, pickle_bytes...]`。
///
/// 由 Python 侧调用构建 default_payload，Rust 不直接调用。
pub fn pack_single(pickle_bytes: Vec<u8>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + pickle_bytes.len());
    buf.push(TAG_SINGLE);
    buf.extend_from_slice(&pickle_bytes);
    buf
}

/// 将多个结果打包为 `[TAG_GROUP, count(u32 LE), len1(u32 LE), data1, ...]`。
///
/// 由 Python 侧调用构建 default_payload，Rust 不直接调用。
pub fn pack_group(results: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + results.len() * 8);
    buf.push(TAG_GROUP);
    buf.extend_from_slice(&(results.len() as u32).to_le_bytes());
    for data in results {
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);
    }
    buf
}

/// 将上游结果前置到 default_payload，生成最终 payload。
///
/// **这是 Rust orchestrator 构建 task payload 的唯一函数**。
/// 它不检查 default_payload 的 tag 类型，只做机械的前置操作。
///
/// 格式：`[TAG_UPSTREAM_PREFIX, upstream_count(u32 LE), up_len1(u32 LE), up_bytes1, ..., default_payload]`
///
/// - `upstream_results`: 前驱任务的结果字节列表（按 DAG 边顺序）
/// - `default_payload`: Python 构建的原始 payload（含 callable + concrete args）
///
/// 若 `upstream_results` 为空，直接返回 `default_payload`（无前驱的叶子任务）。
pub fn pack_upstream_prefix(upstream_results: &[Vec<u8>], default_payload: &[u8]) -> Vec<u8> {
    if upstream_results.is_empty() {
        return default_payload.to_vec();
    }
    let upstream_len: usize = upstream_results.iter().map(|r| 4 + r.len()).sum::<usize>();
    let mut buf = Vec::with_capacity(1 + 4 + upstream_len + default_payload.len());
    buf.push(TAG_UPSTREAM_PREFIX);
    buf.extend_from_slice(&(upstream_results.len() as u32).to_le_bytes());
    for r in upstream_results {
        buf.extend_from_slice(&(r.len() as u32).to_le_bytes());
        buf.extend_from_slice(r);
    }
    buf.extend_from_slice(default_payload);
    buf
}

/// 解包由 `pack_single` 或 `pack_group` 打包的负载。
///
/// 仅用于 Rust 侧测试和内部校验，Python 侧有自己的解包逻辑。
pub(crate) fn unpack_payload(data: &[u8]) -> crate::common::Result<Vec<Vec<u8>>> {
    if data.is_empty() {
        return Err(crate::common::ActantError::Serialization(
            "empty payload".to_string(),
        ));
    }
    match data[0] {
        TAG_SINGLE | TAG_SINGLE_KW => Ok(vec![data[1..].to_vec()]),
        TAG_GROUP => {
            if data.len() < 5 {
                return Err(crate::common::ActantError::Serialization(
                    "group payload too short".to_string(),
                ));
            }
            let count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            let mut results = Vec::with_capacity(count);
            let mut offset = 5;
            for _ in 0..count {
                if offset + 4 > data.len() {
                    return Err(crate::common::ActantError::Serialization(
                        "group payload truncated".to_string(),
                    ));
                }
                let len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err(crate::common::ActantError::Serialization(
                        "group payload truncated".to_string(),
                    ));
                }
                results.push(data[offset..offset + len].to_vec());
                offset += len;
            }
            Ok(results)
        }
        _ => Err(crate::common::ActantError::Serialization(format!(
            "unknown payload tag: 0x{:02x}",
            data[0]
        ))),
    }
}

/// MAC 标签长度（BLAKE3 输出 256 位 = 32 字节）。
const MAC_LEN: usize = 32;

/// MAC 前缀，用于区分签名 payload。
const MAC_PREFIX: &[u8] = b"ACT1";

/// 从用户提供的密钥材料派生 32 字节 BLAKE3 key。
fn derive_key(key: &[u8]) -> [u8; 32] {
    blake3::hash(key).into()
}

/// 使用共享密钥对 payload 签名。
///
/// 返回格式: `[MAC_PREFIX(4) | mac(32) | payload]`。
///
/// # Errors
///
/// 若 `key` 为空返回 `Err`。空 key 不提供任何完整性保护，禁止签名。
/// 正常配置路径下 `ActantConfig::validate` 已拦截空 key，此处为防御性检查，
/// 确保 release 构建中直接调用 `sign` 也无法绕过。
pub fn sign(key: &[u8], payload: &[u8]) -> Result<Vec<u8>, String> {
    if key.is_empty() {
        return Err(
            "empty signing key rejected: cannot sign payload without integrity protection".into(),
        );
    }
    let derived = derive_key(key);
    let mac = blake3::keyed_hash(&derived, payload);
    let mut signed = Vec::with_capacity(MAC_PREFIX.len() + MAC_LEN + payload.len());
    signed.extend_from_slice(MAC_PREFIX);
    signed.extend_from_slice(mac.as_bytes());
    signed.extend_from_slice(payload);
    Ok(signed)
}

/// 验证并提取 payload。签名不匹配时返回 `Err`。
pub fn verify(key: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    if key.is_empty() {
        return Err("empty signing key rejected: cannot verify payload integrity".into());
    }
    if data.len() < MAC_PREFIX.len() + MAC_LEN {
        return Err("payload too short: missing MAC header".into());
    }

    if &data[..MAC_PREFIX.len()] != MAC_PREFIX {
        return Err("payload missing MAC prefix (unsigned payload rejected)".into());
    }

    let mac_bytes = &data[MAC_PREFIX.len()..MAC_PREFIX.len() + MAC_LEN];
    let payload = &data[MAC_PREFIX.len() + MAC_LEN..];

    let derived = derive_key(key);
    let expected = blake3::keyed_hash(&derived, payload);

    // 恒定时间比较，避免时序侧信道泄露 MAC 字节前缀（SE1）。
    // `ct_eq` 要求双方长度相等；mac_bytes 与 expected.as_bytes() 均为 MAC_LEN=32 字节。
    use subtle::ConstantTimeEq;
    if mac_bytes.ct_eq(expected.as_bytes()).into() {
        Ok(payload.to_vec())
    } else {
        Err("payload MAC verification failed: signature mismatch".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_upstream_prefix_empty_returns_default() {
        let default = b"hello".to_vec();
        let result = pack_upstream_prefix(&[], &default);
        assert_eq!(result, default);
    }

    #[test]
    fn pack_upstream_prefix_format() {
        let upstream = vec![b"a".to_vec(), b"bb".to_vec()];
        let default = b"inner".to_vec();
        let result = pack_upstream_prefix(&upstream, &default);

        // [TAG_UPSTREAM_PREFIX, count=2, len1=1, "a", len2=2, "bb", "inner"]
        assert_eq!(result[0], TAG_UPSTREAM_PREFIX);
        assert_eq!(
            u32::from_le_bytes([result[1], result[2], result[3], result[4]]),
            2
        );
        assert_eq!(
            u32::from_le_bytes([result[5], result[6], result[7], result[8]]),
            1
        );
        assert_eq!(&result[9..10], b"a");
        assert_eq!(
            u32::from_le_bytes([result[10], result[11], result[12], result[13]]),
            2
        );
        assert_eq!(&result[14..16], b"bb");
        assert_eq!(&result[16..], b"inner");
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let key = b"test-secret-key";
        let payload = b"hello world";
        let signed_data = sign(key, payload).unwrap();
        let verified = verify(key, &signed_data).unwrap();
        assert_eq!(&verified, payload);
    }

    #[test]
    fn wrong_key_fails() {
        let signed_data = sign(b"correct", b"data").unwrap();
        assert!(verify(b"wrong", &signed_data).is_err());
    }

    #[test]
    fn tampered_payload_fails() {
        let key = b"key";
        let mut signed_data = sign(key, b"original").unwrap();
        let last = signed_data.len() - 1;
        signed_data[last] ^= 0xFF;
        assert!(verify(key, &signed_data).is_err());
    }

    #[test]
    fn unsigned_payload_rejected() {
        assert!(verify(b"key", b"raw payload").is_err());
    }

    #[test]
    fn empty_payload_roundtrip() {
        let key = b"key";
        let signed_data = sign(key, b"").unwrap();
        let verified = verify(key, &signed_data).unwrap();
        assert!(verified.is_empty());
    }

    #[test]
    fn empty_key_rejected_for_verification() {
        let signed = sign(b"non-empty-key", b"payload").unwrap();
        assert!(verify(b"", &signed).is_err());
    }

    /// H1 回归：空 key 签名在 release 构建中也必须被拒绝。
    ///
    /// 旧实现用 `debug_assert!`，release 构建可绕过；新实现返回 `Err`。
    #[test]
    fn empty_key_rejected_for_signing() {
        assert!(sign(b"", b"payload").is_err());
    }

    /// 回归测试（SE1）：所有字节位错的 MAC 都应被拒绝，且不依赖前缀提前返回。
    ///
    /// 旧实现使用 `==`，可能因短路比较而泄露前缀；改用 `subtle::ConstantTimeEq`
    /// 后，无论错位在 MAC 的哪个字节，行为应一致。
    #[test]
    fn mac_constant_time_rejects_all_byte_positions() {
        let key = b"constant-time-key";
        let signed = sign(key, b"verified payload").unwrap();

        // 遍历 MAC 的每个 bit，翻转后必须拒绝。
        // MAC 区间为 [MAC_PREFIX.len() .. MAC_PREFIX.len() + MAC_LEN]。
        let mac_start = MAC_PREFIX.len();
        let mac_end = mac_start + MAC_LEN;
        for i in mac_start..mac_end {
            for bit in 0..8 {
                let mut tampered = signed.clone();
                tampered[i] ^= 1 << bit;
                assert!(
                    verify(key, &tampered).is_err(),
                    "MAC tamper at byte {i} bit {bit} must be rejected"
                );
            }
        }
    }
}
