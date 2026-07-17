//! Unit tests extracted from `src/runtime/workflow/runtime/cancel.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use std::time::{Duration, Instant};

#[test]
fn cleanup_expired_removes_only_old_entries() {
    let mut map = HashMap::new();
    let now = Instant::now();
    let ttl = Duration::from_secs(60);

    map.insert("fresh".to_string(), now - Duration::from_secs(30));
    map.insert("expired".to_string(), now - Duration::from_secs(61));

    let removed = cleanup_expired_cancelled_tasks(&mut map, now, ttl);
    assert_eq!(removed, 1);
    assert!(map.contains_key("fresh"));
    assert!(!map.contains_key("expired"));
}

#[test]
fn cleanup_expired_with_empty_map_returns_zero() {
    let mut map = HashMap::new();
    let removed =
        cleanup_expired_cancelled_tasks(&mut map, Instant::now(), Duration::from_secs(60));
    assert_eq!(removed, 0);
}

#[test]
fn cleanup_expired_boundary_removes_exactly_ttl_old() {
    let mut map = HashMap::new();
    let now = Instant::now();
    let ttl = Duration::from_secs(60);

    // 条件为 duration < ttl，因此恰好等于 ttl 的条目会被移除。
    map.insert("exact".to_string(), now - ttl);
    let removed = cleanup_expired_cancelled_tasks(&mut map, now, ttl);
    assert_eq!(removed, 1);
    assert!(!map.contains_key("exact"));
}
