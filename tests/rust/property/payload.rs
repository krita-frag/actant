//! Property-based tests for payload encoding/decoding.
//!
//! 运行: `cargo test --test payload_property`
//! 这些测试使用 `proptest` 验证 payload 编解码的不变量，
//! 覆盖手写单元测试难以穷举的边界情况。

use actant::common::{
    pack_group, pack_single, pack_upstream_prefix, sign, unpack_payload, verify,
    TAG_UPSTREAM_PREFIX,
};
use proptest::prelude::*;

proptest! {
    /// `pack_single` 后 `unpack_payload` 应恢复原始字节。
    #[test]
    fn pack_single_unpack_roundtrip(data in prop::collection::vec(any::<u8>(), 0..256)) {
        let packed = pack_single(data.clone());
        let unpacked = unpack_payload(&packed).unwrap();
        prop_assert_eq!(unpacked, vec![data]);
    }

    /// `pack_group` 后 `unpack_payload` 应恢复原始字节列表。
    #[test]
    fn pack_group_unpack_roundtrip(
        items in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16)
    ) {
        let packed = pack_group(&items).unwrap();
        let unpacked = unpack_payload(&packed).unwrap();
        prop_assert_eq!(unpacked, items);
    }

    /// `pack_upstream_prefix` 空列表返回原 payload。
    #[test]
    fn pack_upstream_prefix_empty_returns_default(
        payload in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let result = pack_upstream_prefix(&[], &payload).unwrap();
        prop_assert_eq!(result, payload);
    }

    /// `pack_upstream_prefix` 非空时以 TAG_UPSTREAM_PREFIX 开头，且 default_payload 在尾部。
    #[test]
    fn pack_upstream_prefix_nonempty_preserves_default_payload(
        upstream in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..32), 1..8),
        default in prop::collection::vec(any::<u8>(), 0..128)
    ) {
        let result = pack_upstream_prefix(&upstream, &default).unwrap();
        prop_assert_eq!(result[0], TAG_UPSTREAM_PREFIX);
        // default_payload 应完整出现在 result 尾部
        let tail_start = result.len() - default.len();
        prop_assert_eq!(&result[tail_start..], &default[..]);
    }

    /// `pack_upstream_prefix` 的 upstream_count 字段应等于输入长度。
    #[test]
    fn pack_upstream_prefix_count_matches(
        upstream in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..32), 1..16)
    ) {
        let result = pack_upstream_prefix(&upstream, b"").unwrap();
        let count = u32::from_le_bytes([result[1], result[2], result[3], result[4]]);
        prop_assert_eq!(count as usize, upstream.len());
    }

    /// sign + verify 往返：非空 key 下应恢复原始 payload。
    #[test]
    fn sign_verify_roundtrip(
        key in prop::collection::vec(any::<u8>(), 1..64),
        payload in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let signed = sign(&key, &payload).unwrap();
        let verified = verify(&key, &signed).unwrap();
        prop_assert_eq!(verified, payload);
    }

    /// 空 key 禁用签名：sign 返回原 payload，verify 返回原 data。
    ///
    /// 跳过以 MAC_PREFIX (`b"ACT1"`) 开头的 payload：空 key 模式下 verify 会
    /// 拒绝此类数据（"signing disabled but payload appears signed"），这是
    /// 防止在禁用签名的节点上误处理本应签名 payload 的安全防护。
    #[test]
    fn empty_key_disables_signing(
        payload in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        const MAC_PREFIX: &[u8] = b"ACT1";
        prop_assume!(
            payload.len() < MAC_PREFIX.len() || &payload[..MAC_PREFIX.len()] != MAC_PREFIX,
            "payloads starting with MAC_PREFIX are rejected when signing is disabled"
        );
        let signed = sign(&[], &payload).unwrap();
        prop_assert_eq!(&signed[..], &payload[..]);
        let verified = verify(&[], &payload).unwrap();
        prop_assert_eq!(verified, payload);
    }

    /// 不同 key 签名的 payload 互相验证应失败。
    #[test]
    fn different_key_verification_fails(
        key_a in prop::collection::vec(any::<u8>(), 1..64),
        key_b in prop::collection::vec(any::<u8>(), 1..64),
        payload in prop::collection::vec(any::<u8>(), 1..128)
    ) {
        // 跳过 key 相同的情况
        prop_assume!(key_a != key_b);
        let signed = sign(&key_a, &payload).unwrap();
        prop_assert!(verify(&key_b, &signed).is_err());
    }

    /// 篡改 signed payload 的任意字节都应导致验证失败。
    #[test]
    fn tampered_signed_payload_fails(
        key in prop::collection::vec(any::<u8>(), 1..32),
        payload in prop::collection::vec(any::<u8>(), 1..128),
        byte_idx in 0usize..256,
        bit in 0u8..8,
    ) {
        let signed = sign(&key, &payload).unwrap();
        prop_assume!(byte_idx < signed.len());
        let mut tampered = signed.clone();
        tampered[byte_idx] ^= 1 << bit;
        prop_assert!(verify(&key, &tampered).is_err());
    }
}
