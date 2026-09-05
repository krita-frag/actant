//! Property-based tests for HybridLogicalClock monotonicity.
//!
//! 验证核心不变量：无论多节点以何种乱序、多旧的时间戳互相 merge，
//! 任一节点输出的 HLC 时间戳序列必须全序严格递增（Kulkarni 标准
//! HLC 算法的单调性保证）。随机生成 merge 序列覆盖物理时钟主导、
//! 本地历史主导、远端主导、并列主导与 drift cap 全部分支。

use actant::runtime::state::{HlcTimestamp, HybridLogicalClock};
use proptest::prelude::*;

/// 参与互 merge 的节点数。
const NODES: usize = 3;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// 随机多节点乱序 merge 序列：任一节点的 HLC 输出必须全序严格递增。
    ///
    /// - `seed_offsets`：初始"时钟偏差"（相对本机物理时钟的纳秒偏移），
    ///   部分超过 drift 上限以覆盖 cap 分支；
    /// - `ops`：每个操作为 (目标节点, 来源节点)，把来源节点当前状态
    ///   merge 进目标节点，模拟 gossip 常态下的乱序时间戳交换。
    #[test]
    fn multi_node_merge_sequence_is_totally_ordered(
        seed_offsets in prop::collection::vec(0u64..20_000_000, 1..8),
        seed_logicals in prop::collection::vec(0u32..1000, 1..8),
        ops in prop::collection::vec((0usize..NODES, 0usize..NODES), 1..60),
    ) {
        let drift_ms = 5u64;
        let base = actant_now() + 1_000_000; // 1ms 余量，避免真实时钟前进干扰种子
        let clocks: Vec<HybridLogicalClock> = (0..NODES)
            .map(|_| HybridLogicalClock::with_max_drift_ms(drift_ms))
            .collect();
        let mut last: Vec<Option<HlcTimestamp>> = vec![None; NODES];

        // 各节点吸收随机偏差的初始时间戳（含超出 drift 的未来时间戳 → cap）。
        let seed_count = seed_offsets.len().min(seed_logicals.len());
        for i in 0..seed_count {
            let node = i % NODES;
            let ts = HlcTimestamp::from_parts(base + seed_offsets[i], seed_logicals[i]);
            let out = clocks[node].merge(&ts);
            assert_advances(last[node].as_ref(), &out);
            last[node] = Some(out);
        }

        // 未被种子覆盖的节点先归位，保证后续 ops 的来源节点总有状态可取。
        for node in 0..NODES {
            if last[node].is_none() {
                let out = clocks[node].merge(&HlcTimestamp::from_parts(base, 0));
                assert_advances(last[node].as_ref(), &out);
                last[node] = Some(out);
            }
        }

        // 乱序 merge：远端时间戳可能比目标节点历史旧得多（gossip 常态）。
        for (target, source) in ops {
            if target == source {
                // 自环操作视为本地 tick。
                let out = clocks[target].tick();
                assert_advances(last[target].as_ref(), &out);
                last[target] = Some(out);
                continue;
            }
            let remote = last[source].expect("source initialized above");
            let out = clocks[target].merge(&remote);
            assert_advances(last[target].as_ref(), &out);
            last[target] = Some(out);
        }
    }
}

/// 当前物理时钟（纳秒），与 `HybridLogicalClock::physical_now` 同源，
/// 用于在真实时钟附近构造测试时间戳。
fn actant_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// 断言 `next` 严格大于该节点此前输出的最大时间戳 `prev`。
///
/// 使用普通 `assert!`（panic）；proptest 会将 panic 记为该用例失败。
fn assert_advances(prev: Option<&HlcTimestamp>, next: &HlcTimestamp) {
    if let Some(prev) = prev {
        assert!(
            next > prev,
            "HLC regressed: ({}, {}) <= ({}, {})",
            next.wall_time(),
            next.logical(),
            prev.wall_time(),
            prev.logical()
        );
    }
}
