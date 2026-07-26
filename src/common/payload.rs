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
pub fn unpack_payload(data: &[u8]) -> crate::common::Result<Vec<Vec<u8>>> {
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
/// - 非空密钥：生成 BLAKE3 keyed MAC 并前置到 payload。
/// - 空密钥：直接返回原始 payload，表示禁用签名验证（仅用于开发/测试）。
pub fn sign(key: &[u8], payload: &[u8]) -> Result<Vec<u8>, String> {
    if key.is_empty() {
        return Ok(payload.to_vec());
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
///
/// - 非空密钥：要求数据以 `MAC_PREFIX` 开头并验证 MAC；拒绝未签名 payload。
/// - 空密钥：禁用签名验证，直接返回原始数据；若数据以 `MAC_PREFIX` 开头则报错，
///   避免在禁用签名的节点上误处理本应签名的 payload。
pub fn verify(key: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    if key.is_empty() {
        if data.len() >= MAC_PREFIX.len() && &data[..MAC_PREFIX.len()] == MAC_PREFIX {
            return Err("signing disabled but payload appears signed".into());
        }
        return Ok(data.to_vec());
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

/// Wire message MAC 长度（与 payload MAC 一致，BLAKE3 输出 256 位 = 32 字节）。
pub const WIRE_MAC_LEN: usize = 32;

/// 使用共享密钥为 wire message 字节计算 BLAKE3 keyed MAC。
///
/// 返回固定 32 字节 MAC。空密钥返回 `None`（表示禁用 wire 签名）。
///
/// 与 [`sign`] 共享密钥派生逻辑，但语义独立：wire MAC 防止跨节点消息伪造，
/// payload MAC 防止任务载荷篡改。两者可共用同一密钥（`payload_signing_key`）
/// 也可分别配置。
pub fn wire_mac(key: &[u8], bytes: &[u8]) -> Option<[u8; WIRE_MAC_LEN]> {
    if key.is_empty() {
        return None;
    }
    let derived = derive_key(key);
    let mac = blake3::keyed_hash(&derived, bytes);
    let mut out = [0u8; WIRE_MAC_LEN];
    out.copy_from_slice(mac.as_bytes());
    Some(out)
}

/// 恒定时间校验 wire message MAC。
///
/// `mac_bytes` 长度必须为 [`WIRE_MAC_LEN`]。空密钥返回 `Ok(())`（禁用签名时
/// 一切通过，向后兼容）。MAC 不匹配返回 `Err`，调用方应丢弃该消息。
pub fn verify_wire_mac(key: &[u8], bytes: &[u8], mac_bytes: &[u8]) -> Result<(), String> {
    if key.is_empty() {
        return Ok(());
    }
    let expected =
        wire_mac(key, bytes).ok_or_else(|| "verify with non-empty key failed".to_string())?;
    if mac_bytes.len() != WIRE_MAC_LEN {
        return Err(format!(
            "wire MAC length mismatch: expected {}, got {}",
            WIRE_MAC_LEN,
            mac_bytes.len()
        ));
    }
    use subtle::ConstantTimeEq;
    if mac_bytes.ct_eq(&expected).into() {
        Ok(())
    } else {
        Err("wire MAC verification failed: signature mismatch".into())
    }
}

#[cfg(test)]
#[path = "../../tests/rust/unit/common/payload.rs"]
mod tests;
