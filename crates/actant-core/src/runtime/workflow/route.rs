//! 远端任务路由策略（二次开发条件）。
//!
//! 任务本地无法执行（槽位不足）时，Worker 需要选择一个远端节点转发。
//! 选择逻辑经 [`RoutePolicy`] trait 暴露为扩展缝：默认实现
//! [`DefaultRoutePolicy`] 复刻内置行为（心跳新鲜度过滤 + 槽位比较）并叠加
//! 标签适配；[`LabelAffinityRoutePolicy`] 直接按标签亲和占位。
//! 框架用户可实现自己的策略经 `Worker::with_route_policy` 注入。
//!
//! ## 三态决策
//!
//! [`RouteDecision`] 区分「本地执行」「转发远端」「延后重试」：
//! `Defer` 表达「本节点不适合且暂无可转发目标」，由 Worker 走退避重入队，
//! 而不是像旧版 `None` 那样静默本地执行。

use std::collections::BTreeMap;

use crate::common::{NodeId, NodeRole};

/// 路由候选：单个 peer 心跳视图的快照。
#[derive(Debug, Clone)]
pub struct RouteCandidate {
    pub node_id: NodeId,
    /// 对端 iroh endpoint 地址（公钥字符串）。
    pub endpoint_addr: Option<String>,
    pub available_slots: u32,
    pub max_slots: u32,
    /// 对端拓扑角色。Orchestrator 不执行任务，不进入候选。
    pub role: NodeRole,
    /// 对端是否正在 draining。draining 节点不接收新任务。
    pub draining: bool,
    /// 对端最近一次心跳的接收时刻（UNIX 毫秒）；0 表示从未收到。
    pub last_heartbeat_ms: u64,
    /// 对端随心跳广播的标签（用于放置诉求匹配）。
    pub labels: BTreeMap<String, String>,
    /// 对端各命名配额的剩余容量（心跳快照）。
    pub available_quotas: BTreeMap<String, u32>,
}

/// 路由决策上下文：策略实现所需的调用方状态。
#[derive(Debug, Clone)]
pub struct RouteContext {
    /// 本地拓扑角色。Orchestrator 不允许本地执行任务。
    pub local_role: NodeRole,
    /// 本地是否正在 draining。draining 时本地不接收新任务。
    pub local_draining: bool,
    /// 本地剩余并发槽位，供"远端是否真的更空"的比较
    /// （内置策略据此避免把任务转发到比本地更拥挤的节点）。
    pub local_available_slots: u32,
    /// 心跳 TTL（毫秒）：距上次心跳超过该时长的 peer 视为失联。
    /// 与 failover 的 `failure_timeout_ms` 同源，由 Worker 传入。
    pub heartbeat_ttl_ms: u64,
    /// 本地节点标签，用于判断本地是否满足任务放置诉求。
    pub local_labels: BTreeMap<String, String>,
    /// 任务放置诉求（见 [`TaskDefinition::tags`](crate::common::TaskDefinition)）。
    pub required_tags: Vec<String>,
    /// 任务所需的命名配额（`None` 表示不占用配额）。
    pub required_quota: Option<String>,
    /// 本节点各命名配额的剩余容量。
    pub local_available_quotas: BTreeMap<String, u32>,
}

/// 路由决策三态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// 本地执行。
    Local,
    /// 转发到指定 peer。
    Remote {
        node_id: NodeId,
        endpoint_addr: String,
    },
    /// 本轮无合适节点，延后重试（由 Worker 走退避重入队）。
    Defer {
        /// 延后原因（用于日志/指标/最终失败信息）。
        reason: String,
    },
}

/// 判断节点标签是否满足任务诉求。
///
/// 每项标签支持 `key`（键存在即可）与 `key=value`（键值精确匹配）两种形式。
/// 空诉求恒满足。
pub fn labels_match(labels: &BTreeMap<String, String>, tags: &[String]) -> bool {
    tags.iter().all(|tag| match tag.split_once('=') {
        Some((key, value)) => labels.get(key).is_some_and(|v| v == value),
        None => labels.contains_key(tag),
    })
}

/// 构造"无节点满足标签诉求"的统一延后原因。
pub fn unschedulable_reason(tags: &[String]) -> String {
    format!("no node satisfies required labels: {}", tags.join(","))
}

/// 节点是否具备任务所需的命名配额（剩余容量 > 0）。
///
/// `required` 为 `None` 时恒满足。缺失键视为容量 0。
pub fn quota_available(available: &BTreeMap<String, u32>, required: Option<&str>) -> bool {
    match required {
        None => true,
        Some(key) => available.get(key).copied().unwrap_or(0) > 0,
    }
}

/// 构造"无节点满足配额诉求"的延后原因。
pub fn unschedulable_quota_reason(key: &str) -> String {
    format!("no node has available quota '{key}'")
}

/// 远端路由策略扩展缝。
///
/// `candidates` 是 failover 心跳视图的**全量** peer 快照——新鲜度过滤
/// 也属于策略语义，由实现自行决定是否应用。
pub trait RoutePolicy: Send + Sync {
    /// 从候选中选择一个转发目标，或决定本地执行 / 延后重试。
    fn select_target(&self, ctx: &RouteContext, candidates: &[RouteCandidate]) -> RouteDecision;
}

/// 过滤出「心跳新鲜、有可用槽位、标签满足诉求」的候选，并按剩余槽位降序排列。
fn eligible_peers<'a>(
    ctx: &RouteContext,
    candidates: &'a [RouteCandidate],
) -> Vec<&'a RouteCandidate> {
    let now_ms = crate::common::epoch_millis();
    let mut peers: Vec<&RouteCandidate> = candidates
        .iter()
        .filter(|c| {
            c.role.can_execute_locally()
                && !c.draining
                && c.available_slots > 0
                && c.max_slots > 0
                && c.last_heartbeat_ms > 0
                && now_ms.saturating_sub(c.last_heartbeat_ms) <= ctx.heartbeat_ttl_ms
                && labels_match(&c.labels, &ctx.required_tags)
                && quota_available(&c.available_quotas, ctx.required_quota.as_deref())
        })
        .collect();
    peers.sort_by(|a, b| {
        role_rank(a.role)
            .cmp(&role_rank(b.role))
            .then_with(|| b.available_slots.cmp(&a.available_slots))
            .then_with(|| b.max_slots.cmp(&a.max_slots))
            .then_with(|| a.node_id.as_str().cmp(b.node_id.as_str()))
    });
    peers
}

/// 候选排序中的角色优先级：专用 worker 先于 both 节点。
///
/// orchestrator 已在过滤阶段排除，不会进入排序。
fn role_rank(role: NodeRole) -> u8 {
    match role {
        NodeRole::Worker => 0,
        NodeRole::Both => 1,
        NodeRole::Orchestrator => 2,
    }
}

fn remote(candidate: &RouteCandidate) -> RouteDecision {
    RouteDecision::Remote {
        node_id: candidate.node_id.clone(),
        endpoint_addr: candidate
            .endpoint_addr
            .clone()
            .unwrap_or_else(|| candidate.node_id.as_str().to_string()),
    }
}

/// 内置默认路由策略：槽位比较 + 标签适配。
///
/// 依次应用：标签适配 + 槽位非零 + 心跳 TTL 过滤 → 按可用槽位降序
/// （并列取槽位多者、再取节点名稳定排序）。本地满足标签诉求且不比最优
/// peer 更拥挤时本地执行（避免无收益转发）；本地不满足诉求时转发最优
/// 匹配 peer；两者都不满足则 [`RouteDecision::Defer`]。
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRoutePolicy;

impl RoutePolicy for DefaultRoutePolicy {
    fn select_target(&self, ctx: &RouteContext, candidates: &[RouteCandidate]) -> RouteDecision {
        let local_fits = ctx.local_role.can_execute_locally()
            && !ctx.local_draining
            && labels_match(&ctx.local_labels, &ctx.required_tags)
            && quota_available(&ctx.local_available_quotas, ctx.required_quota.as_deref());
        let peers = eligible_peers(ctx, candidates);
        if peers.is_empty() {
            return if local_fits {
                RouteDecision::Local
            } else {
                RouteDecision::Defer {
                    reason: defer_reason(ctx),
                }
            };
        }
        let best = peers[0];
        if local_fits && best.available_slots <= ctx.local_available_slots {
            return RouteDecision::Local;
        }
        remote(best)
    }
}

/// 标签亲和路由策略：优先本地适配，其次按剩余槽位最大的匹配 peer 转发。
///
/// 与 [`DefaultRoutePolicy`] 的差异：本地满足诉求时直接本地执行，不做
/// "远端是否更空"的比较——适合"标签是硬约束、性能次要"的部署。
#[derive(Debug, Clone, Copy, Default)]
pub struct LabelAffinityRoutePolicy;

impl RoutePolicy for LabelAffinityRoutePolicy {
    fn select_target(&self, ctx: &RouteContext, candidates: &[RouteCandidate]) -> RouteDecision {
        if ctx.local_role.can_execute_locally()
            && !ctx.local_draining
            && labels_match(&ctx.local_labels, &ctx.required_tags)
            && quota_available(&ctx.local_available_quotas, ctx.required_quota.as_deref())
        {
            return RouteDecision::Local;
        }
        match eligible_peers(ctx, candidates).first() {
            Some(best) => remote(best),
            None => RouteDecision::Defer {
                reason: defer_reason(ctx),
            },
        }
    }
}

/// 依据标签/配额诉求构造延后原因（配额优先，便于诊断）。
fn defer_reason(ctx: &RouteContext) -> String {
    if !ctx.local_role.can_execute_locally() {
        return "local node role does not execute tasks".to_string();
    }
    if ctx.local_draining {
        return "local node is draining".to_string();
    }
    match ctx.required_quota.as_deref() {
        Some(key) if !quota_available(&ctx.local_available_quotas, Some(key)) => {
            unschedulable_quota_reason(key)
        }
        _ => unschedulable_reason(&ctx.required_tags),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RouteContext {
        RouteContext {
            local_role: NodeRole::Both,
            local_draining: false,
            local_available_slots: 0,
            heartbeat_ttl_ms: 15_000,
            local_labels: BTreeMap::new(),
            required_tags: Vec::new(),
            required_quota: None,
            local_available_quotas: BTreeMap::new(),
        }
    }

    fn cand(node: &str, available: u32, max: u32, hb_age_ms: u64) -> RouteCandidate {
        RouteCandidate {
            node_id: NodeId::from(node.to_string()),
            endpoint_addr: Some(format!("ep-{node}")),
            available_slots: available,
            max_slots: max,
            role: NodeRole::Both,
            draining: false,
            last_heartbeat_ms: crate::common::epoch_millis().saturating_sub(hb_age_ms),
            labels: BTreeMap::new(),
            available_quotas: BTreeMap::new(),
        }
    }

    fn quotas(pairs: &[(&str, u32)]) -> BTreeMap<String, u32> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn quota_filter_skips_full_and_missing_peers() {
        let mut local = ctx();
        local.required_quota = Some("gpu".into());
        local.local_available_quotas = quotas(&[("gpu", 0)]);

        let mut full = cand("full", 5, 8, 100);
        full.available_quotas = quotas(&[("gpu", 0)]);
        let mut missing = cand("missing", 6, 8, 100);
        missing.available_quotas = BTreeMap::new();
        let mut free = cand("free", 2, 8, 100);
        free.available_quotas = quotas(&[("gpu", 1)]);

        assert_eq!(
            DefaultRoutePolicy.select_target(&local, &[full, missing, free.clone()]),
            RouteDecision::Remote {
                node_id: free.node_id.clone(),
                endpoint_addr: "ep-free".to_string(),
            }
        );
        // 本地配额耗尽且无可用 peer → Defer，原因是配额而非标签。
        let mut no_free = free;
        no_free.available_quotas = quotas(&[("gpu", 0)]);
        match DefaultRoutePolicy.select_target(&local, &[no_free]) {
            RouteDecision::Defer { reason } => assert!(reason.contains("quota"), "{reason}"),
            other => panic!("expected Defer, got {other:?}"),
        }
    }

    #[test]
    fn local_quota_available_keeps_task_local() {
        let mut local = ctx();
        local.required_quota = Some("gpu".into());
        local.local_available_quotas = quotas(&[("gpu", 1)]);
        assert_eq!(
            DefaultRoutePolicy.select_target(&local, &[]),
            RouteDecision::Local
        );
        assert_eq!(
            LabelAffinityRoutePolicy.select_target(&local, &[]),
            RouteDecision::Local
        );
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn selects_most_available_peer() {
        let candidates = vec![cand("a", 1, 8, 100), cand("b", 5, 8, 100)];
        let picked = DefaultRoutePolicy.select_target(&ctx(), &candidates);
        assert_eq!(
            picked,
            RouteDecision::Remote {
                node_id: NodeId::from("b".to_string()),
                endpoint_addr: "ep-b".to_string(),
            }
        );
    }

    #[test]
    fn skips_stale_and_full_peers() {
        let candidates = vec![cand("stale", 5, 8, 15_001), cand("full", 0, 8, 100)];
        assert_eq!(
            DefaultRoutePolicy.select_target(&ctx(), &candidates),
            RouteDecision::Local
        );
    }

    #[test]
    fn skips_never_heartbeat_peers() {
        let mut c = cand("never", 5, 8, 0);
        c.last_heartbeat_ms = 0;
        assert_eq!(
            DefaultRoutePolicy.select_target(&ctx(), &[c]),
            RouteDecision::Local
        );
    }

    #[test]
    fn no_forward_when_local_at_least_as_free() {
        let candidates = vec![cand("b", 3, 8, 100)];
        // 本地 3 槽空闲 ≥ peer 3：转发无收益，本地执行。
        let local = RouteContext {
            local_available_slots: 3,
            ..ctx()
        };
        assert_eq!(
            DefaultRoutePolicy.select_target(&local, &candidates),
            RouteDecision::Local
        );
        // 本地 2 < peer 3：转发。
        let local = RouteContext {
            local_available_slots: 2,
            ..ctx()
        };
        assert!(matches!(
            DefaultRoutePolicy.select_target(&local, &candidates),
            RouteDecision::Remote { .. }
        ));
    }

    #[test]
    fn falls_back_to_node_id_when_no_endpoint() {
        let mut c = cand("b", 5, 8, 100);
        c.endpoint_addr = None;
        assert_eq!(
            DefaultRoutePolicy.select_target(&ctx(), &[c]),
            RouteDecision::Remote {
                node_id: NodeId::from("b".to_string()),
                endpoint_addr: "b".to_string(),
            }
        );
    }

    #[test]
    fn default_policy_routes_by_required_labels() {
        let mut a = cand("a", 5, 8, 100);
        a.labels = labels(&[("engine", "a")]);
        let mut b = cand("b", 2, 8, 100);
        b.labels = labels(&[("engine", "b")]);

        // 本地不具备诉求 → 只能转发到匹配节点。
        let mut local = ctx();
        local.required_tags = vec!["engine=a".to_string()];
        assert_eq!(
            DefaultRoutePolicy.select_target(&local, &[a, b]),
            RouteDecision::Remote {
                node_id: NodeId::from("a".to_string()),
                endpoint_addr: "ep-a".to_string(),
            }
        );
    }

    #[test]
    fn default_policy_defers_when_no_node_matches() {
        let mut a = cand("a", 5, 8, 100);
        a.labels = labels(&[("engine", "b")]);
        let mut local = ctx();
        local.required_tags = vec!["engine=a".to_string()];
        assert!(matches!(
            DefaultRoutePolicy.select_target(&local, &[a]),
            RouteDecision::Defer { .. }
        ));
    }

    #[test]
    fn label_affinity_prefers_local_when_fit() {
        let mut local = ctx();
        local.local_labels = labels(&[("role", "test")]);
        local.required_tags = vec!["role=test".to_string()];
        let mut peer = cand("a", 5, 8, 100);
        peer.labels = labels(&[("role", "test")]);
        assert_eq!(
            LabelAffinityRoutePolicy.select_target(&local, &[peer]),
            RouteDecision::Local
        );
    }

    #[test]
    fn label_affinity_orders_matching_peers_by_slots() {
        let mut local = ctx();
        local.required_tags = vec!["role=test".to_string()];
        let mut a = cand("a", 2, 8, 100);
        a.labels = labels(&[("role", "test")]);
        let mut b = cand("b", 6, 8, 100);
        b.labels = labels(&[("role", "test")]);
        assert_eq!(
            LabelAffinityRoutePolicy.select_target(&local, &[a, b]),
            RouteDecision::Remote {
                node_id: NodeId::from("b".to_string()),
                endpoint_addr: "ep-b".to_string(),
            }
        );
    }

    #[test]
    fn labels_match_supports_key_and_key_value() {
        let l = labels(&[("gpu", "a100"), ("ssd", "")]);
        assert!(labels_match(&l, &[]));
        assert!(labels_match(
            &l,
            &["gpu=a100".to_string(), "ssd".to_string()]
        ));
        assert!(!labels_match(&l, &["gpu=h100".to_string()]));
        assert!(!labels_match(&l, &["nvme".to_string()]));
    }

    #[test]
    fn orchestrator_never_executes_locally() {
        let mut local = ctx();
        local.local_role = NodeRole::Orchestrator;
        local.local_available_slots = 8;
        assert!(matches!(
            DefaultRoutePolicy.select_target(&local, &[]),
            RouteDecision::Defer { .. }
        ));
    }

    #[test]
    fn orchestrator_candidates_are_not_routed_to() {
        let mut local = ctx();
        local.local_role = NodeRole::Orchestrator;
        let mut orchestrator = cand("o", 8, 8, 100);
        orchestrator.role = NodeRole::Orchestrator;
        assert!(matches!(
            DefaultRoutePolicy.select_target(&local, &[orchestrator]),
            RouteDecision::Defer { .. }
        ));
    }

    #[test]
    fn draining_candidates_are_not_routed_to() {
        // 本地缺少诉求配额 → 只能转发；唯一 peer 正在 draining，应延后而非转发。
        let mut local = ctx();
        local.required_quota = Some("gpu".into());
        let mut peer = cand("draining", 8, 8, 100);
        peer.available_quotas = quotas(&[("gpu", 1)]);
        peer.draining = true;
        assert!(matches!(
            DefaultRoutePolicy.select_target(&local, &[peer]),
            RouteDecision::Defer { .. }
        ));
    }

    #[test]
    fn draining_local_never_selected() {
        let mut local = ctx();
        local.local_draining = true;
        local.local_available_slots = 8;
        assert!(matches!(
            DefaultRoutePolicy.select_target(&local, &[]),
            RouteDecision::Defer { .. }
        ));
    }

    #[test]
    fn dedicated_worker_preferred_over_both_with_equal_slots() {
        let local = ctx();
        let mut both = cand("both", 4, 8, 100);
        both.role = NodeRole::Both;
        let mut worker = cand("worker", 4, 8, 100);
        worker.role = NodeRole::Worker;
        assert_eq!(
            DefaultRoutePolicy.select_target(&local, &[both, worker]),
            RouteDecision::Remote {
                node_id: NodeId::from("worker".to_string()),
                endpoint_addr: "ep-worker".to_string(),
            }
        );
    }
}
