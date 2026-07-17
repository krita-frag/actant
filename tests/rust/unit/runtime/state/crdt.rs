//! Unit tests extracted from `src/runtime/state/crdt.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::runtime::state::HybridLogicalClock;

#[test]
fn gcounter_increment_and_merge() {
    let mut a = GCounter::new();
    a.increment(NodeId::from("n1"));
    a.increment(NodeId::from("n1"));
    a.increment(NodeId::from("n2"));
    assert_eq!(a.value(), 3);

    let mut b = GCounter::new();
    b.increment(NodeId::from("n1"));
    b.increment(NodeId::from("n3"));

    a.merge(&b);
    assert_eq!(a.value(), 4);
}

#[test]
fn orset_add_and_merge() {
    let mut a: ORSet<String> = ORSet::new();
    a.add(NodeId::from("n1"), "x".to_string());
    a.add(NodeId::from("n2"), "y".to_string());

    let mut b: ORSet<String> = ORSet::new();
    b.add(NodeId::from("n3"), "z".to_string());

    a.merge(&b);
    let values: HashSet<_> = a.values().cloned().collect();
    assert!(values.contains("x"));
    assert!(values.contains("y"));
    assert!(values.contains("z"));
}

#[test]
fn lww_register_last_write_wins() {
    let clock = HybridLogicalClock::new();
    let mut reg: LWWRegister<i32> = LWWRegister::new();

    let t1 = clock.tick();
    reg.set(NodeId::from("n1"), t1, 1);
    assert_eq!(reg.get(), Some(&1));

    let t2 = clock.tick();
    reg.set(NodeId::from("n2"), t2, 2);
    assert_eq!(reg.get(), Some(&2));

    // 旧时间戳不应覆盖新值
    reg.set(NodeId::from("n3"), t1, 3);
    assert_eq!(reg.get(), Some(&2));
}

#[test]
fn lww_register_tie_break_by_node_id() {
    let clock = HybridLogicalClock::new();
    let t = clock.tick();

    // 相同时间戳：节点 ID 字典序大者胜（should_replace 当 existing < incoming）。
    // 先写小节点再写大节点 → 大节点覆盖。
    let mut reg: LWWRegister<i32> = LWWRegister::new();
    reg.set(NodeId::from("a-node"), t, 1);
    reg.set(NodeId::from("b-node"), t, 2);
    assert_eq!(reg.get(), Some(&2), "大节点 b 应覆盖小节点 a");

    // 反向：先写大节点再写小节点 → 小节点不覆盖。
    let mut reg2: LWWRegister<i32> = LWWRegister::new();
    reg2.set(NodeId::from("b-node"), t, 2);
    reg2.set(NodeId::from("a-node"), t, 1);
    assert_eq!(reg2.get(), Some(&2), "小节点 a 不应覆盖大节点 b");
}

#[test]
fn lww_register_tie_break_converges_via_merge() {
    let clock = HybridLogicalClock::new();
    let t = clock.tick();

    // 两个 register 各持有一方写入，时间戳相同。
    let mut a: LWWRegister<i32> = LWWRegister::new();
    a.set(NodeId::from("a-node"), t, 1);
    let mut b: LWWRegister<i32> = LWWRegister::new();
    b.set(NodeId::from("b-node"), t, 2);

    // 双向 merge 后应收敛到大节点 b 的值。
    a.merge(&b);
    assert_eq!(a.get(), Some(&2));
    b.merge(&a);
    assert_eq!(b.get(), Some(&2));

    // 重复 merge 幂等，结果不变。
    a.merge(&b);
    a.merge(&b);
    assert_eq!(a.get(), Some(&2));
}
