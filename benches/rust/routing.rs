//! 路由决策基准测试。
//!
//! 测量内置策略在两个规模敏感维度上的开销：
//! - 候选数量（10 / 100 / 500 / 1000 peers）
//! - 本地是否满足诉求（决定策略是否必须扫描远端候选）
//!
//! 候选是心跳视图的快照，由调用方每任务构造一次；`routing` 组只测
//! 「选择目标」这一段（过滤 + 比较/排序），`candidate_build` 组测其前置的
//! 快照构造开销，两者相加才是路由的每任务成本。
//!
//! 运行：`cargo bench --bench routing`

use std::collections::BTreeMap;

use actant_core::common::{epoch_millis, NodeId, NodeRole};
use actant_core::runtime::workflow::{
    DefaultRoutePolicy, LabelAffinityRoutePolicy, PeerInfo, RouteCandidate, RouteContext,
    RoutePolicy,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn context(local_slots: u32, required_tags: &[&str]) -> RouteContext {
    RouteContext {
        local_role: NodeRole::Both,
        local_draining: false,
        local_available_slots: local_slots,
        // TTL 取最大值：基准运行时长远超真实心跳周期，避免候选在测量中途过期。
        heartbeat_ttl_ms: u64::MAX,
        local_labels: BTreeMap::new(),
        required_tags: required_tags.iter().map(|tag| (*tag).to_string()).collect(),
        required_quota: None,
        local_available_quotas: BTreeMap::new(),
    }
}

fn candidates(count: usize) -> Vec<RouteCandidate> {
    let now = epoch_millis();
    (0..count)
        .map(|index| {
            let mut labels = BTreeMap::new();
            labels.insert("zone".to_string(), format!("az-{}", index % 4));
            let mut available_quotas = BTreeMap::new();
            available_quotas.insert("gpu".to_string(), (index % 4) as u32);
            RouteCandidate {
                node_id: NodeId::new(format!("peer-{index:04}")),
                endpoint_addr: Some(format!("addr-{index}")),
                available_slots: (index % 8) as u32 + 1,
                max_slots: 8,
                role: NodeRole::Both,
                draining: false,
                last_heartbeat_ms: now,
                labels,
                available_quotas,
            }
        })
        .collect()
}

/// 模拟 `FailoverManager::get_peer_infos()` 返回的 peer 快照。
fn peer_infos(count: usize) -> Vec<PeerInfo> {
    let now = epoch_millis();
    (0..count)
        .map(|index| {
            let mut labels = BTreeMap::new();
            labels.insert("zone".to_string(), format!("az-{}", index % 4));
            let mut available_quotas = BTreeMap::new();
            available_quotas.insert("gpu".to_string(), (index % 4) as u32);
            PeerInfo {
                node_id: NodeId::new(format!("peer-{index:04}")),
                last_heartbeat_ms: now,
                active_workflows: std::collections::HashSet::new(),
                available_slots: (index % 8) as u32 + 1,
                max_slots: 8,
                endpoint_addr: Some(format!("addr-{index}")),
                labels,
                platform: None,
                available_quotas,
                role: NodeRole::Both,
                draining: false,
            }
        })
        .collect()
}

fn bench_route_policies(c: &mut Criterion) {
    let mut group = c.benchmark_group("routing");
    group.sample_size(50);

    // local_fits：本地满足诉求，DefaultRoutePolicy 仍需比较远端最优节点；
    // remote_only：本地不满足，两个策略都必须扫描候选。
    let contexts = [
        ("local_fits", context(8, &[])),
        ("remote_only", context(0, &["zone=az-1"])),
    ];

    for peers in [10usize, 100, 500, 1000] {
        let cands = candidates(peers);
        for (ctx_name, ctx) in &contexts {
            group.bench_with_input(
                BenchmarkId::new(format!("default/{ctx_name}"), peers),
                &cands,
                |b, cands| {
                    b.iter(|| {
                        black_box(
                            DefaultRoutePolicy.select_target(black_box(ctx), black_box(cands)),
                        )
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("label_affinity/{ctx_name}"), peers),
                &cands,
                |b, cands| {
                    b.iter(|| {
                        black_box(
                            LabelAffinityRoutePolicy
                                .select_target(black_box(ctx), black_box(cands)),
                        )
                    });
                },
            );
        }
    }

    group.finish();
}

/// 每任务构造候选快照的开销（`route_task` 在调用策略前的固定成本）。
///
/// `clone_peer_infos` 是 `get_peer_infos()` 的深拷贝部分；`to_route_candidates`
/// 模拟其后的字段搬运。两者都不含 `HashMap` 构建，属于该路径的下界。
fn bench_candidate_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("candidate_build");
    group.sample_size(50);

    for peers in [10usize, 100, 500] {
        let infos = peer_infos(peers);
        group.bench_with_input(
            BenchmarkId::new("clone_peer_infos", peers),
            &infos,
            |b, infos| {
                b.iter(|| black_box(infos.clone()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("to_route_candidates", peers),
            &infos,
            |b, infos| {
                b.iter(|| {
                    let candidates: Vec<RouteCandidate> = infos
                        .iter()
                        .map(|info| RouteCandidate {
                            node_id: info.node_id.clone(),
                            endpoint_addr: info.endpoint_addr.clone(),
                            available_slots: info.available_slots,
                            max_slots: info.max_slots,
                            role: info.role,
                            draining: info.draining,
                            last_heartbeat_ms: info.last_heartbeat_ms,
                            labels: info.labels.clone(),
                            available_quotas: info.available_quotas.clone(),
                        })
                        .collect();
                    black_box(candidates)
                });
            },
        );
    }

    group.finish();
}

/// 缓存命中路径：版本比较 + `RwLock` 读 + `Arc::clone`。
///
/// 与 `FailoverManager::route_candidates` 的命中分支同形；命中后每任务
/// 不再构造候选，O(N) 成本只在成员/容量变更后发生。
fn bench_snapshot_hit(c: &mut Criterion) {
    use std::sync::Arc;

    let version = 7u64;
    let cache = parking_lot::RwLock::new(Some((version, Arc::new(candidates(100)))));

    let mut group = c.benchmark_group("snapshot");
    group.bench_function("hit_100", |b| {
        b.iter(|| {
            let cached = cache.read();
            let (cached_version, snapshot) = cached.as_ref().expect("snapshot cached");
            assert_eq!(*cached_version, version);
            black_box(Arc::clone(snapshot))
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_route_policies,
    bench_candidate_build,
    bench_snapshot_hit
);
criterion_main!(benches);
