//! Unit tests extracted from `src/common/payload.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

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
