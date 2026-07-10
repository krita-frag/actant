//! CRDT 抽象 — State 四盒之一的分布式状态层。
//!
//! 提供基础 CRDT 类型与统一 trait，支持：
//! - `GCounter`：单调递增计数器
//! - `ORSet<T>`：添加优先集合
//! - `LWWRegister<T>`：最后写入胜出寄存器
//!
//! 所有类型均实现 `Crdt` trait，支持本地 `apply` 与跨节点 `merge`。

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::common::NodeId;
use crate::runtime::state::HlcTimestamp;

/// CRDT 统一 trait。
///
/// `Op` 为单次本地更新操作；`merge` 用于合并来自其他节点的完整状态。
pub trait Crdt: Clone + Send + Sync + 'static {
    /// 本地更新操作类型。
    type Op: Send + Sync + Clone;

    /// 应用一次本地操作。
    fn apply(&mut self, op: &Self::Op);

    /// 合并另一个节点的完整状态。
    fn merge(&mut self, other: &Self);
}

// ============================================================================
// GCounter
// ============================================================================

/// 单调递增计数器。
///
/// 每个节点维护自己视角下其他节点的计数，合并时取最大值。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GCounter {
    counts: HashMap<NodeId, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// 返回所有节点计数之和。
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// 递增指定节点的计数。
    pub fn increment(&mut self, node: NodeId) {
        let entry = self.counts.entry(node).or_insert(0);
        *entry += 1;
    }

    /// 直接设置某节点的最小计数值（来自本地操作或反熵）。
    pub fn set(&mut self, node: NodeId, count: u64) {
        let entry = self.counts.entry(node).or_insert(0);
        *entry = (*entry).max(count);
    }
}

impl Crdt for GCounter {
    type Op = (NodeId, u64);

    fn apply(&mut self, op: &Self::Op) {
        self.set(op.0.clone(), op.1);
    }

    fn merge(&mut self, other: &Self) {
        for (node, count) in &other.counts {
            self.set(node.clone(), *count);
        }
    }
}

// ============================================================================
// ORSet
// ============================================================================

/// 添加优先集合（Observed-Removed Set）。
///
/// 每个元素关联一组已观察到它的节点 ID。合并时取节点 ID 集合的并集。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ORSet<T: Hash + Eq + Clone + Send + Sync + 'static> {
    entries: HashMap<T, HashSet<NodeId>>,
    _marker: PhantomData<T>,
}

impl<T: Hash + Eq + Clone + Send + Sync + 'static> Default for ORSet<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

impl<T: Hash + Eq + Clone + Send + Sync + 'static> ORSet<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// 添加元素，由指定节点观察。
    pub fn add(&mut self, node: NodeId, value: T) {
        self.entries.entry(value).or_default().insert(node);
    }

    /// 返回集合中所有元素。
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.entries.keys()
    }

    /// 返回元素数量。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<T: Hash + Eq + Clone + Send + Sync + 'static> Crdt for ORSet<T> {
    type Op = (NodeId, T);

    fn apply(&mut self, op: &Self::Op) {
        self.add(op.0.clone(), op.1.clone());
    }

    fn merge(&mut self, other: &Self) {
        for (value, nodes) in &other.entries {
            let entry = self.entries.entry(value.clone()).or_default();
            entry.extend(nodes.iter().cloned());
        }
    }
}

// ============================================================================
// LWWRegister
// ============================================================================

/// 最后写入胜出寄存器。
///
/// 使用 HLC 时间戳解决冲突；时间戳相同时按节点 ID 字典序决胜。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LWWRegister<T: Clone + Send + Sync + 'static> {
    value: Option<T>,
    timestamp: HlcTimestamp,
    node: Option<NodeId>,
}

impl<T: Clone + Send + Sync + 'static> Default for LWWRegister<T> {
    fn default() -> Self {
        Self {
            value: None,
            timestamp: HlcTimestamp::zero(),
            node: None,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> LWWRegister<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// 写入值，由指定节点与时间戳标记。
    pub fn set(&mut self, node: NodeId, timestamp: HlcTimestamp, value: T) {
        if self.should_replace(&timestamp, &node) {
            self.value = Some(value);
            self.timestamp = timestamp;
            self.node = Some(node);
        }
    }

    pub fn get(&self) -> Option<&T> {
        self.value.as_ref()
    }

    fn should_replace(&self, timestamp: &HlcTimestamp, node: &NodeId) -> bool {
        if self.value.is_none() {
            return true;
        }
        match timestamp.cmp(&self.timestamp) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => {
                // 时间戳相同则按节点 ID 字典序决胜，保证确定性。
                self.node.as_ref().map(|n| n.as_str()) < Some(node.as_str())
            }
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Crdt for LWWRegister<T> {
    type Op = (NodeId, HlcTimestamp, T);

    fn apply(&mut self, op: &Self::Op) {
        self.set(op.0.clone(), op.1, op.2.clone());
    }

    fn merge(&mut self, other: &Self) {
        if let (Some(value), Some(node)) = (&other.value, &other.node) {
            self.set(node.clone(), other.timestamp, value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
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
}
