//! Unit tests for `src/common/serialization.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct Sample {
    id: u64,
    name: String,
}

#[test]
fn decode_postcard_roundtrip() {
    let value = Sample {
        id: 42,
        name: "actant".into(),
    };
    let bytes = postcard::to_allocvec(&value).unwrap();
    let decoded: Sample = decode_postcard(&bytes).unwrap();
    assert_eq!(decoded, value);
}

#[test]
fn decode_postcard_rejects_oversized_input() {
    let huge = vec![0u8; MAX_DECODE_SIZE + 1];
    let result: Result<Sample> = decode_postcard(&huge);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("exceeds MAX_DECODE_SIZE"),
        "error should mention size limit: {err}"
    );
}

#[test]
fn decode_postcard_rejects_truncated_bytes() {
    // 一个合法的 postcard 序列被截断后必须失败。
    let value = Sample {
        id: 42,
        name: "actant".into(),
    };
    let bytes = postcard::to_allocvec(&value).unwrap();
    for len in 0..bytes.len() {
        let result: Result<Sample> = decode_postcard(&bytes[..len]);
        assert!(
            result.is_err(),
            "truncated length {len} should fail decoding"
        );
    }
}

#[test]
fn decode_postcard_rejects_garbage_bytes() {
    let garbage = b"this is not postcard data";
    let result: Result<Sample> = decode_postcard(garbage);
    assert!(result.is_err());
}
