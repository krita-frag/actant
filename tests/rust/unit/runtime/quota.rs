//! Unit tests extracted from `src/runtime/quota.rs`.

use super::*;
use std::sync::Arc;

fn tracker(pairs: &[(&str, u32)]) -> Arc<QuotaTracker> {
    Arc::new(QuotaTracker::new(
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
    ))
}

#[test]
fn acquire_and_release_restore_capacity() {
    let t = tracker(&[("gpu", 2)]);
    assert_eq!(t.available_map().get("gpu"), Some(&2));

    let guard = t.try_acquire(Some("gpu")).unwrap().unwrap();
    assert_eq!(t.available_map().get("gpu"), Some(&1));
    drop(guard);
    assert_eq!(t.available_map().get("gpu"), Some(&2));
}

#[test]
fn exhausted_quota_is_rejected_without_change() {
    let t = tracker(&[("gpu", 1)]);
    let _guard = t.try_acquire(Some("gpu")).unwrap().unwrap();
    assert!(matches!(
        t.try_acquire(Some("gpu")),
        Err(QuotaAcquireError::Exhausted(_))
    ));
    assert_eq!(t.available_map().get("gpu"), Some(&0));
}

#[test]
fn unknown_and_zero_capacity_are_unavailable() {
    let t = tracker(&[("licence", 0)]);
    assert!(matches!(
        t.try_acquire(Some("licence")),
        Err(QuotaAcquireError::Exhausted(_))
    ));
    assert!(matches!(
        t.try_acquire(Some("gpu")),
        Err(QuotaAcquireError::Unavailable(_))
    ));
}

#[test]
fn no_quota_key_is_noop() {
    let t = tracker(&[("gpu", 1)]);
    assert!(t.try_acquire(None).unwrap().is_none());
    assert_eq!(t.available_map().get("gpu"), Some(&1));
}

#[test]
fn concurrent_acquires_never_exceed_capacity() {
    let t = tracker(&[("gpu", 2)]);
    let mut held = Vec::new();
    for _ in 0..2 {
        held.push(t.try_acquire(Some("gpu")).unwrap().unwrap());
    }
    assert!(t.try_acquire(Some("gpu")).is_err());
    drop(held);
    assert_eq!(t.available_map().get("gpu"), Some(&2));
}

#[tokio::test]
async fn release_wakes_waiting_task() {
    let t = tracker(&[("gpu", 1)]);
    let guard = t.try_acquire(Some("gpu")).unwrap().unwrap();

    let waiter = {
        let t = Arc::clone(&t);
        tokio::spawn(async move { t.wait_for_release().await })
    };
    // 让等待者完成 `notified()` 注册。
    tokio::task::yield_now().await;
    drop(guard);

    tokio::time::timeout(std::time::Duration::from_millis(500), waiter)
        .await
        .expect("release should wake the waiter")
        .expect("waiter task panicked");
}

#[test]
fn release_never_exceeds_capacity() {
    let t = tracker(&[("gpu", 2)]);
    let guard = t.try_acquire(Some("gpu")).unwrap().unwrap();
    let key = guard.key().to_string();
    // 模拟重复归还：直接调用私有 release 两次。
    guard_key_release_twice(&t, &key);
    assert_eq!(t.available_map().get("gpu"), Some(&2));
}

fn guard_key_release_twice(tracker: &Arc<QuotaTracker>, key: &str) {
    tracker.release(key);
    tracker.release(key);
}

#[test]
fn set_capacities_keeps_in_use_units_on_shrink() {
    let t = tracker(&[("gpu", 4)]);
    let _held = t.try_acquire(Some("gpu")).unwrap().unwrap();
    // 4 容量扣 1 → 3 剩余；缩到 2 时在用的 1 仍占用，剩余应为 1。
    t.set_capacities([("gpu".to_string(), 2)].into_iter().collect());
    assert_eq!(t.capacities().get("gpu"), Some(&2));
    assert_eq!(t.available_map().get("gpu"), Some(&1));
}

#[test]
fn set_capacities_expands_available_units() {
    let t = tracker(&[("gpu", 2)]);
    let _held = t.try_acquire(Some("gpu")).unwrap().unwrap();
    // 2 容量扣 1 → 1 剩余；扩到 5 时在用的 1 保留，剩余应为 4。
    t.set_capacities([("gpu".to_string(), 5)].into_iter().collect());
    assert_eq!(t.available_map().get("gpu"), Some(&4));
}

#[test]
fn set_capacities_saturates_when_shrunk_below_usage() {
    let t = tracker(&[("gpu", 4)]);
    let _a = t.try_acquire(Some("gpu")).unwrap().unwrap();
    let _b = t.try_acquire(Some("gpu")).unwrap().unwrap();
    // 在用量 2，容量缩到 1：剩余饱和为 0，不出现负值。
    t.set_capacities([("gpu".to_string(), 1)].into_iter().collect());
    assert_eq!(t.available_map().get("gpu"), Some(&0));
}

#[test]
fn set_capacities_adds_and_removes_keys() {
    let t = tracker(&[("gpu", 2)]);
    t.set_capacities(
        [("cpu".to_string(), 3)]
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>(),
    );
    assert_eq!(t.available_map().get("gpu"), None);
    assert_eq!(t.available_map().get("cpu"), Some(&3));
}

#[tokio::test]
async fn set_capacities_wakes_waiting_task() {
    let t = tracker(&[("gpu", 1)]);
    let guard = t.try_acquire(Some("gpu")).unwrap().unwrap();
    let waiter = {
        let t = Arc::clone(&t);
        tokio::spawn(async move { t.wait_for_release().await })
    };
    tokio::task::yield_now().await;
    t.set_capacities([("gpu".to_string(), 4)].into_iter().collect());
    tokio::time::timeout(std::time::Duration::from_millis(500), waiter)
        .await
        .expect("capacity update should wake the waiter")
        .expect("waiter task panicked");
    drop(guard);
}
