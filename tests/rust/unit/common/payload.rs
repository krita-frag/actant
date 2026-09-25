// Unit tests extracted from `src/common/payload.rs`.
// Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

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
fn empty_key_rejects_signed_payload() {
    let signed = sign(b"non-empty-key", b"payload").unwrap();
    assert!(verify(b"", &signed).is_err());
}

/// 空 key 禁用签名：sign 直接返回原始 payload，verify 直接返回原始 data。
#[test]
fn empty_key_disables_signing() {
    let payload = b"plain payload";
    let signed = sign(b"", payload).unwrap();
    assert_eq!(&signed, payload);
    let verified = verify(b"", payload).unwrap();
    assert_eq!(&verified, payload);
}

// --- Wire MAC ---

#[test]
fn wire_mac_roundtrip() {
    let key = b"cluster-secret";
    let bytes = b"some wire message bytes";
    let mac = wire_mac(key, bytes).expect("non-empty key should produce MAC");
    assert_eq!(mac.len(), WIRE_MAC_LEN);
    verify_wire_mac(key, bytes, &mac).expect("valid MAC should verify");
}

#[test]
fn wire_mac_wrong_key_fails() {
    let mac = wire_mac(b"key-a", b"bytes").unwrap();
    assert!(verify_wire_mac(b"key-b", b"bytes", &mac).is_err());
}

#[test]
fn wire_mac_tampered_bytes_fail() {
    let mac = wire_mac(b"key", b"original").unwrap();
    assert!(verify_wire_mac(b"key", b"tampered", &mac).is_err());
}

#[test]
fn wire_mac_tampered_mac_fails() {
    let mut mac = wire_mac(b"key", b"bytes").unwrap();
    mac[0] ^= 0xFF;
    assert!(verify_wire_mac(b"key", b"bytes", &mac).is_err());
}

#[test]
fn wire_mac_empty_key_disables() {
    assert!(wire_mac(b"", b"bytes").is_none());
    // 空 key 视图下任何输入都视为合法（向后兼容 0.2）。
    let bogus = [0u8; WIRE_MAC_LEN];
    verify_wire_mac(b"", b"bytes", &bogus).expect("empty key disables verification");
}

#[test]
fn wire_mac_wrong_length_fails() {
    let short = [0u8; 16];
    assert!(verify_wire_mac(b"key", b"bytes", &short).is_err());
}

/// 回归测试：所有字节位错的 MAC 都应被拒绝，且不依赖前缀提前返回。
///
/// 必须使用 `subtle::ConstantTimeEq` 恒定时间比较：`==` 的短路语义会因前缀不匹配提前返回，泄露时间侧信道
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

#[test]
fn truncated_signed_payload_rejected() {
    let key = b"key";
    let signed = sign(key, b"payload").unwrap();
    for len in 0..signed.len() {
        assert!(
            verify(key, &signed[..len]).is_err(),
            "truncated length {len} must be rejected"
        );
    }
}

#[test]
fn wrong_prefix_rejected() {
    let key = b"key";
    let signed = sign(key, b"payload").unwrap();
    let mut tampered = signed.clone();
    tampered[0] = b'X';
    assert!(verify(key, &tampered).is_err());
}

#[test]
fn empty_data_rejected_with_key() {
    assert!(verify(b"key", b"").is_err());
}

// ───────────────────────── BlobRef（值引用 wire 编码）─────────────────────────

#[test]
fn blob_ref_roundtrip() {
    let r = BlobRef {
        hash: crate::common::model::BlobHash::from_bytes([7u8; 32]),
        node: crate::common::model::NodeId::new("node-a".into()),
    };
    let bytes = encode_blob_ref(&r).unwrap();
    assert_eq!(decode_blob_ref(&bytes).unwrap(), r);
}

#[test]
fn blob_ref_hash_is_raw_32_bytes_on_wire() {
    let r = BlobRef {
        hash: crate::common::model::BlobHash::from_bytes([0xAB; 32]),
        node: crate::common::model::NodeId::new("node-a".into()),
    };
    let bytes = encode_blob_ref(&r).unwrap();
    // postcard 编码 [u8;32] 为裸 32 字节，无长度前缀。
    assert!(bytes.windows(32).any(|w| w == [0xAB; 32]));
}

/// 篡改编码字节的任意 bit 后，要么解码失败，要么解出与原引用不同的值（可检出）。
#[test]
fn blob_ref_tampering_is_detectable() {
    let r = BlobRef {
        hash: crate::common::model::BlobHash::from_bytes([3u8; 32]),
        node: crate::common::model::NodeId::new("node-b".into()),
    };
    let bytes = encode_blob_ref(&r).unwrap();
    for i in 0..bytes.len() {
        for bit in 0..8 {
            let mut tampered = bytes.clone();
            tampered[i] ^= 1 << bit;
            match decode_blob_ref(&tampered) {
                Err(_) => {}
                Ok(decoded) => assert_ne!(
                    decoded, r,
                    "tamper at byte {i} bit {bit} silently decoded to original"
                ),
            }
        }
    }
}

#[test]
fn blob_ref_truncated_rejected() {
    let r = BlobRef {
        hash: crate::common::model::BlobHash::from_bytes([1u8; 32]),
        node: crate::common::model::NodeId::new("node-c".into()),
    };
    let bytes = encode_blob_ref(&r).unwrap();
    for len in 0..bytes.len() {
        assert!(
            decode_blob_ref(&bytes[..len]).is_err(),
            "truncated length {len} must be rejected"
        );
    }
}

#[test]
fn blob_hash_hex_display_and_parse_roundtrip() {
    use std::str::FromStr;
    let hash = crate::common::model::BlobHash::from_bytes([
        0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0xFF,
    ]);
    let text = hash.to_string();
    assert_eq!(text.len(), 64);
    assert_eq!(
        crate::common::model::BlobHash::from_str(&text).unwrap(),
        hash
    );
    // 非 hex / 错误长度均拒绝。
    assert!(crate::common::model::BlobHash::from_str("xyz").is_err());
    assert!(crate::common::model::BlobHash::from_str(&"aa".repeat(31)).is_err());
}

// ───────────────────────── wire_mac / verify_wire_mac 属性测试 ─────────────────────────
//
// `wire_mac` / `verify_wire_mac` 是 `payload.rs` 内的 pub 函数，但未在
// `common.rs` 中 re-export，因此 `tests/rust/property/` 无法访问。这里通过
// `use super::*;` 直接在单元测试模块中运行 proptest 属性测试，覆盖空密钥
// 兼容路径与各种不匹配路径。

use proptest::prelude::*;

proptest! {
    /// wire_mac + verify_wire_mac 往返：非空 key 下应验证通过。
    #[test]
    fn wire_mac_verify_roundtrip(
        key in prop::collection::vec(any::<u8>(), 1..64),
        bytes in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let mac = wire_mac(&key, &bytes).expect("non-empty key should produce MAC");
        prop_assert_eq!(mac.len(), WIRE_MAC_LEN);
        verify_wire_mac(&key, &bytes, &mac).expect("valid MAC should verify");
    }

    /// 不同 key 签名的 wire MAC 互相验证应失败。
    #[test]
    fn wire_mac_different_key_fails(
        key_a in prop::collection::vec(any::<u8>(), 1..64),
        key_b in prop::collection::vec(any::<u8>(), 1..64),
        bytes in prop::collection::vec(any::<u8>(), 1..128)
    ) {
        prop_assume!(key_a != key_b);
        let mac = wire_mac(&key_a, &bytes).unwrap();
        prop_assert!(verify_wire_mac(&key_b, &bytes, &mac).is_err());
    }

    /// 篡改 wire bytes 后 MAC 验证应失败。
    #[test]
    fn wire_mac_tampered_bytes_fail_property(
        key in prop::collection::vec(any::<u8>(), 1..32),
        bytes in prop::collection::vec(any::<u8>(), 1..128),
        byte_idx in 0usize..256,
        bit in 0u8..8,
    ) {
        let mac = wire_mac(&key, &bytes).unwrap();
        prop_assume!(byte_idx < bytes.len());
        let mut tampered = bytes.clone();
        tampered[byte_idx] ^= 1 << bit;
        prop_assert!(verify_wire_mac(&key, &tampered, &mac).is_err());
    }

    /// 篡改 MAC 的任意 bit 后验证应失败（恒定时间比较回归测试）。
    /// 覆盖 `subtle::ConstantTimeEq` 的所有字节位，确保不依赖前缀提前返回。
    #[test]
    fn wire_mac_tampered_mac_fails_all_positions(
        key in prop::collection::vec(any::<u8>(), 1..32),
        bytes in prop::collection::vec(any::<u8>(), 1..128),
        byte_idx in 0usize..WIRE_MAC_LEN,
        bit in 0u8..8,
    ) {
        let mut mac = wire_mac(&key, &bytes).unwrap();
        mac[byte_idx] ^= 1 << bit;
        prop_assert!(verify_wire_mac(&key, &bytes, &mac).is_err());
    }

    /// 空 key 禁用 wire MAC：wire_mac 返回 None，verify_wire_mac 对任意 MAC 返回 Ok。
    #[test]
    fn wire_mac_empty_key_disables_property(
        bytes in prop::collection::vec(any::<u8>(), 0..256),
        mac_len in 0usize..40
    ) {
        prop_assert!(wire_mac(&[], &bytes).is_none());
        let bogus = vec![0u8; mac_len];
        verify_wire_mac(&[], &bytes, &bogus).expect("empty key disables verification");
    }

    /// verify_wire_mac 对错误长度的 MAC 应返回 Err（非空 key 下）。
    #[test]
    fn wire_mac_wrong_length_fails_property(
        key in prop::collection::vec(any::<u8>(), 1..32),
        bytes in prop::collection::vec(any::<u8>(), 0..128),
        mac_len in 0usize..40
    ) {
        prop_assume!(mac_len != WIRE_MAC_LEN);
        let bogus = vec![0u8; mac_len];
        prop_assert!(verify_wire_mac(&key, &bytes, &bogus).is_err());
    }
}

// payload 签名/验证直测（关键路径）。
//
// `sign`/`verify` 是任务载荷完整性防线的唯一实现：提交侧签名（builder 注入
// 密钥）、worker 侧派发前验证（dispatcher）、执行编排侧构造带 MAC 的节点
// 载荷（keys.rs `build_task_payload` 直通 `sign`）。本文件不经过任何上层
// 包装，直接对二进制契约断言：帧布局（MAC_PREFIX | mac | payload）、空密钥
// 降级语义、篡改/截断/伪造拒绝。

use super::{sign, verify, wire_mac, wire_mac_incremental, MAC_LEN, MAC_PREFIX, WIRE_MAC_LEN};

const KEY: &[u8] = b"c5-direct-test-key";
const PAYLOAD: &[u8] = b"cloudpickle-envelope-bytes";

#[test]
fn signed_payload_has_prefix_mac_body_layout() {
    let signed = sign(KEY, PAYLOAD).unwrap();
    // 布局：MAC_PREFIX | 32B mac | 原始 payload。
    assert_eq!(&signed[..MAC_PREFIX.len()], MAC_PREFIX);
    assert_eq!(signed.len(), MAC_PREFIX.len() + MAC_LEN + PAYLOAD.len());
    assert_eq!(&signed[MAC_PREFIX.len() + MAC_LEN..], PAYLOAD);
}

#[test]
fn verify_roundtrip_returns_original_payload() {
    let signed = sign(KEY, PAYLOAD).unwrap();
    let recovered = verify(KEY, &signed).unwrap();
    assert_eq!(recovered, PAYLOAD);
}

#[test]
fn empty_key_bypasses_signing_both_directions() {
    // 空密钥 = 禁用签名：sign 原样返回；verify 接受未签名数据。
    let unsigned = sign(b"", PAYLOAD).unwrap();
    assert_eq!(unsigned, PAYLOAD);
    assert_eq!(verify(b"", PAYLOAD).unwrap(), PAYLOAD);
    // 但禁用签名的节点拒绝"本应签名"的数据（防误处理）。
    let signed = sign(KEY, PAYLOAD).unwrap();
    assert!(verify(b"", &signed).is_err());
}

#[test]
fn tampered_mac_rejected() {
    let mut signed = sign(KEY, PAYLOAD).unwrap();
    let i = MAC_PREFIX.len(); // 翻转 MAC 首字节
    signed[i] ^= 0xFF;
    assert!(
        verify(KEY, &signed).is_err(),
        "tampered MAC must be rejected"
    );
}

#[test]
fn tampered_body_rejected() {
    let mut signed = sign(KEY, PAYLOAD).unwrap();
    let last = signed.len() - 1;
    signed[last] ^= 0x01;
    assert!(
        verify(KEY, &signed).is_err(),
        "tampered payload body must be rejected"
    );
}

#[test]
fn truncated_payload_rejected() {
    let signed = sign(KEY, PAYLOAD).unwrap();
    assert!(verify(KEY, &signed[..MAC_PREFIX.len() + MAC_LEN - 1]).is_err());
    assert!(verify(KEY, &signed[..MAC_PREFIX.len()]).is_err());
}

#[test]
fn wrong_key_rejected() {
    let signed = sign(KEY, PAYLOAD).unwrap();
    assert!(
        verify(b"another-key", &signed).is_err(),
        "foreign key must be rejected"
    );
}

#[test]
fn unsigned_input_rejected_when_key_set() {
    assert!(
        verify(KEY, PAYLOAD).is_err(),
        "unsigned payload must be rejected under signing key"
    );
}

#[test]
fn wire_mac_incremental_matches_buffered() {
    // 字节序不变红线：分段喂 hasher 与单缓冲必须产出同一 MAC。
    let segs: Vec<&[u8]> = vec![b"version-byte", PAYLOAD, b"traceparent", &[0x00]];
    let joined: Vec<u8> = segs.concat();
    assert_eq!(wire_mac_incremental(KEY, &segs), wire_mac(KEY, &joined));
}

#[test]
fn wire_mac_empty_key_returns_none() {
    assert!(wire_mac(b"", b"x").is_none());
    assert!(wire_mac_incremental(b"", &[b"x"]).is_none());
}

#[test]
fn wire_mac_len_is_32() {
    assert_eq!(WIRE_MAC_LEN, 32);
    let m = wire_mac(KEY, b"data").unwrap();
    assert_eq!(m.len(), WIRE_MAC_LEN);
}
