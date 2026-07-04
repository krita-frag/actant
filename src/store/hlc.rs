use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(bytecheck())]
pub struct HlcTimestamp {
    wall_time: u64,
    logical: u32,
}

impl HlcTimestamp {
    pub fn zero() -> Self {
        Self {
            wall_time: 0,
            logical: 0,
        }
    }

    pub fn wall_time(&self) -> u64 {
        self.wall_time
    }

    pub fn logical(&self) -> u32 {
        self.logical
    }

    /// 从原始 wall_time（纳秒）和逻辑计数器构造时间戳。用于从序列化数据重建时间戳。
    pub fn from_parts(wall_time: u64, logical: u32) -> Self {
        Self { wall_time, logical }
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.wall_time().cmp(&other.wall_time()) {
            Ordering::Equal => self.logical().cmp(&other.logical()),
            other => other,
        }
    }
}

pub struct HybridLogicalClock {
    inner: Mutex<Inner>,
    max_drift_nanos: u64,
}

struct Inner {
    last_time: u64,
    logical: u32,
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        Self::with_max_drift_ms(500)
    }

    /// 创建具有可配置最大漂移阈值（毫秒）的 HLC。
    pub fn with_max_drift_ms(max_drift_ms: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                last_time: 0,
                logical: 0,
            }),
            max_drift_nanos: max_drift_ms.saturating_mul(1_000_000),
        }
    }

    fn physical_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    pub fn tick(&self) -> HlcTimestamp {
        let mut inner = self.inner.lock();
        let physical = Self::physical_now();

        if physical > inner.last_time {
            inner.last_time = physical;
            inner.logical = 0;
        } else {
            inner.logical += 1;
        }

        HlcTimestamp {
            wall_time: inner.last_time,
            logical: inner.logical,
        }
    }

    pub fn merge(&self, remote: &HlcTimestamp) -> HlcTimestamp {
        let mut inner = self.inner.lock();
        let physical = Self::physical_now();
        let max_drift = self.max_drift_nanos;

        // 限制远端 wall_time，防止过大时钟漂移传播。
        // 若远端时间戳远在未来，使用 local + max_drift 作为上界，而非盲目接受。
        let capped_wall_time = if remote.wall_time() > physical.saturating_add(max_drift) {
            tracing::warn!(
                "HLC drift detected: remote wall_time {}ns exceeds local physical {}ns by >{}ms, capping",
                remote.wall_time(),
                physical,
                max_drift / 1_000_000,
            );
            physical.saturating_add(max_drift)
        } else {
            remote.wall_time()
        };

        inner.last_time = physical.max(inner.last_time).max(capped_wall_time);
        if inner.last_time == capped_wall_time && inner.last_time == physical {
            inner.logical = inner.logical.max(remote.logical()) + 1;
        } else if inner.last_time == capped_wall_time {
            inner.logical = remote.logical() + 1;
        } else if inner.last_time == physical {
            inner.logical += 1;
        } else {
            inner.logical = 0;
        }

        HlcTimestamp {
            wall_time: inner.last_time,
            logical: inner.logical,
        }
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_monotonic() {
        let hlc = HybridLogicalClock::new();
        let t1 = hlc.tick();
        let t2 = hlc.tick();
        assert!(t2 > t1);
    }

    #[test]
    fn merge_is_monotonic() {
        let hlc = HybridLogicalClock::new();
        let t1 = hlc.tick();
        let remote = HlcTimestamp::from_parts(t1.wall_time() + 1000, 5);
        let t2 = hlc.merge(&remote);
        assert!(t2.wall_time() >= remote.wall_time());
        let t3 = hlc.tick();
        assert!(t3 > t2);
    }

    #[test]
    fn physical_now_nanos_precision() {
        let t1 = HybridLogicalClock::physical_now();
        let t2 = HybridLogicalClock::physical_now();
        assert!(t2 >= t1);
        assert!(t1 > 0);
    }

    #[test]
    fn merge_caps_excessive_drift() {
        let hlc = HybridLogicalClock::new();
        let max_drift_nanos = 500_000_000;
        let t1 = hlc.tick();
        let far_future =
            HlcTimestamp::from_parts(t1.wall_time() + max_drift_nanos + 1_000_000_000, 0);
        let t2 = hlc.merge(&far_future);
        // 结果应被限制在 local + max_drift_nanos 以内，
        // 而非 far_future 的值
        assert!(
            t2.wall_time() < far_future.wall_time(),
            "drifted timestamp should be capped, got {} >= {}",
            t2.wall_time(),
            far_future.wall_time()
        );
        assert!(
            t2.wall_time() >= t1.wall_time(),
            "capped timestamp should still advance past local time"
        );
        let t3 = hlc.tick();
        assert!(t3 > t2);
    }

    // -----------------------------------------------------------------------
    // 排序语义
    // -----------------------------------------------------------------------

    #[test]
    fn zero_is_minimum() {
        let zero = HlcTimestamp::zero();
        assert_eq!(zero.wall_time(), 0);
        assert_eq!(zero.logical(), 0);
        // zero <= 任意正时间戳
        let positive = HlcTimestamp::from_parts(1, 0);
        assert!(zero < positive);
        assert!(zero <= positive);
    }

    #[test]
    fn ordering_wall_time_dominates() {
        // wall_time 不同时，logical 不影响顺序
        let a = HlcTimestamp::from_parts(100, 99);
        let b = HlcTimestamp::from_parts(200, 0);
        assert!(
            a < b,
            "lower wall_time must be smaller regardless of logical"
        );
        assert!(b > a);
    }

    #[test]
    fn ordering_logical_ties_wall_time() {
        // wall_time 相同时，logical 决定顺序
        let a = HlcTimestamp::from_parts(100, 1);
        let b = HlcTimestamp::from_parts(100, 2);
        assert!(a < b);
        assert_eq!(a.cmp(&a.clone()), Ordering::Equal);
    }

    #[test]
    fn from_parts_round_trips() {
        let ts = HlcTimestamp::from_parts(1_700_000_000_000, 42);
        assert_eq!(ts.wall_time(), 1_700_000_000_000);
        assert_eq!(ts.logical(), 42);
        // 通过 from_parts 重建应相等
        let rebuilt = HlcTimestamp::from_parts(ts.wall_time(), ts.logical());
        assert_eq!(ts, rebuilt);
    }

    // -----------------------------------------------------------------------
    // merge 分支覆盖
    // -----------------------------------------------------------------------

    #[test]
    fn merge_older_remote_uses_local_physical() {
        // 远端时间戳较旧：本地 physical 主导，logical 重置
        let hlc = HybridLogicalClock::new();
        let local = hlc.tick();
        let older = HlcTimestamp::from_parts(local.wall_time().saturating_sub(1_000_000), 10);
        let merged = hlc.merge(&older);
        // merged.wall_time 至少为本地 physical（>= local.wall_time）
        assert!(
            merged.wall_time() >= local.wall_time(),
            "merge with older remote should not regress below local wall_time"
        );
    }

    #[test]
    fn merge_equal_wall_time_advances_logical() {
        // 远端 wall_time 等于本地物理时间：logical 取 max + 1
        let hlc = HybridLogicalClock::new();
        let local = hlc.tick();
        // 构造一个 wall_time == local.wall_time 的远端时间戳
        let remote_same_wall = HlcTimestamp::from_parts(local.wall_time(), local.logical() + 5);
        let merged = hlc.merge(&remote_same_wall);
        // merged 应严格大于 local 和 remote_same_wall
        assert!(merged > local, "merge result must exceed local tick");
        assert!(
            merged > remote_same_wall,
            "merge result must exceed remote when wall_time ties"
        );
    }

    #[test]
    fn merge_preserves_monotonicity_across_many_ticks() {
        let hlc = HybridLogicalClock::new();
        let mut prev = hlc.tick();
        for _ in 0..50 {
            let remote = HlcTimestamp::from_parts(prev.wall_time() + 1_000, prev.logical());
            let merged = hlc.merge(&remote);
            assert!(merged > prev, "merge must be strictly monotonic");
            let ticked = hlc.tick();
            assert!(
                ticked > merged,
                "tick after merge must be strictly monotonic"
            );
            prev = ticked;
        }
    }

    // -----------------------------------------------------------------------
    // 默认值与配置
    // -----------------------------------------------------------------------

    #[test]
    fn default_matches_new() {
        // Default 和 new() 都使用 500ms 漂移阈值；行为应一致
        let a = HybridLogicalClock::default();
        let b = HybridLogicalClock::new();
        let ta = a.tick();
        let tb = b.tick();
        // 两个独立时钟的首次 tick 应在合理范围内（同一物理时间附近）
        // 主要验证不 panic 且产生有效时间戳
        assert!(ta.wall_time() > 0);
        assert!(tb.wall_time() > 0);
    }

    #[test]
    fn with_max_drift_ms_zero_caps_at_physical() {
        // max_drift = 0：远端漂移立即被限制到本地 physical
        let hlc = HybridLogicalClock::with_max_drift_ms(0);
        let local = hlc.tick();
        let far_future = HlcTimestamp::from_parts(local.wall_time() + 10_000_000_000, 0);
        let merged = hlc.merge(&far_future);
        assert!(
            merged.wall_time() <= local.wall_time() + 1_000_000,
            "with zero max_drift, merged wall_time should stay near local physical (got {}, local {})",
            merged.wall_time(),
            local.wall_time()
        );
    }

    // -----------------------------------------------------------------------
    // 序列化往返
    // -----------------------------------------------------------------------

    #[test]
    fn serde_round_trip() {
        // 使用 postcard（serde 格式，已是依赖）验证 serde derive 正确性
        let ts = HlcTimestamp::from_parts(1_700_000_000_123_456, 7);
        let bytes = postcard::to_allocvec(&ts).expect("serialize");
        let decoded: HlcTimestamp = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(ts, decoded);
    }

    #[test]
    fn rkyv_round_trip() {
        use crate::common::serialization::{deserialize_rkyv_value, serialize_rkyv};
        let ts = HlcTimestamp::from_parts(1_700_000_000_654_321, 99);
        let bytes = serialize_rkyv(&ts).expect("rkyv serialize");
        let decoded: HlcTimestamp =
            deserialize_rkyv_value::<HlcTimestamp>(&bytes).expect("rkyv deserialize");
        assert_eq!(ts, decoded);
    }
}
