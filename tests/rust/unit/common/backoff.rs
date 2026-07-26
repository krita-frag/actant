//! Unit tests for `src/common/backoff.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use std::time::Duration;

#[test]
fn delay_for_grows_exponentially_until_cap() {
    let bo = ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(30));
    assert_eq!(bo.delay_for(0), Duration::from_millis(100));
    assert_eq!(bo.delay_for(1), Duration::from_millis(200));
    assert_eq!(bo.delay_for(2), Duration::from_millis(400));
    assert_eq!(bo.delay_for(3), Duration::from_millis(800));
    // 2^10 * 100ms = 102400ms = 102.4s，超过 30s 上限被钳制
    assert_eq!(bo.delay_for(10), Duration::from_secs(30));
}

#[test]
fn delay_for_caps_at_max_delay() {
    let bo = ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(1));
    // 2^1 * 500ms = 1000ms = 1s，正好达到上限
    assert_eq!(bo.delay_for(1), Duration::from_secs(1));
    // 更大的 attempt 仍然被钳制为 1s
    assert_eq!(bo.delay_for(5), Duration::from_secs(1));
    assert_eq!(bo.delay_for(50), Duration::from_secs(1));
}

#[test]
fn delay_for_saturates_on_overflow() {
    // 极大 attempt 不应 panic
    let bo = ExponentialBackoff::new(Duration::from_millis(1), Duration::from_secs(60));
    let d = bo.delay_for(u32::MAX);
    assert_eq!(d, Duration::from_secs(60));
}

#[test]
fn zero_base_returns_zero() {
    let bo = ExponentialBackoff::new(Duration::ZERO, Duration::from_secs(10));
    assert_eq!(bo.delay_for(0), Duration::ZERO);
    assert_eq!(bo.delay_for(5), Duration::ZERO);
}

#[test]
fn accessors_return_configured_values() {
    let bo = ExponentialBackoff::new(Duration::from_millis(250), Duration::from_secs(45));
    assert_eq!(bo.base(), Duration::from_millis(250));
    assert_eq!(bo.max_delay(), Duration::from_secs(45));
}

#[test]
fn remote_call_max_retry_delay_is_30s() {
    assert_eq!(REMOTE_CALL_MAX_RETRY_DELAY, Duration::from_secs(30));
}
