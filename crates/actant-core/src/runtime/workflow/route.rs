//! 远端任务路由策略（二次开发条件）。
//!
//! 任务本地无法执行（槽位不足）时，Worker 需要选择一个远端节点转发。
//! 选择逻辑经 [`RoutePolicy`] trait 暴露为扩展缝：默认实现
//! [`DefaultRoutePolicy`] 复刻内置行为（心跳新鲜度过滤 + 槽位比较），
//! 框架用户可实现自己的策略（如就近路由、按标签亲和）经
//! `Worker::with_route_policy` 注入。

use crate::common::NodeId;

/// 路由候选：单个 peer 心跳视图的快照。
#[derive(Debug, Clone)]
pub struct RouteCandidate {
    pub node_id: NodeId,
    /// 对端 iroh endpoint 地址（公钥字符串）。
    pub endpoint_addr: Option<String>,
    pub available_slots: u32,
    pub max_slots: u32,
    /// 对端最近一次心跳的接收时刻（UNIX 毫秒）；0 表示从未收到。
    pub last_heartbeat_ms: u64,
}

/// 路由决策上下文：策略实现所需的调用方状态。
#[derive(Debug, Clone, Copy)]
pub struct RouteContext {
    /// 本地剩余并发槽位，供"远端是否真的更空"的比较
    /// （内置策略据此避免把任务转发到比本地更拥挤的节点）。
    pub local_available_slots: u32,
    /// 心跳 TTL（毫秒）：距上次心跳超过该时长的 peer 视为失联。
    /// 与 failover 的 `failure_timeout_ms` 同源，由 Worker 传入。
    pub heartbeat_ttl_ms: u64,
}

/// 远端路由策略扩展缝。
///
/// `candidates` 是 failover 心跳视图的**全量** peer 快照——新鲜度过滤
/// 也属于策略语义，由实现自行决定是否应用。返回 `None` 表示本轮不转发。
pub trait RoutePolicy: Send + Sync {
    /// 从候选中选择一个转发目标，返回 `(节点, 地址)`。
    fn select_target(
        &self,
        ctx: &RouteContext,
        candidates: &[RouteCandidate],
    ) -> Option<(NodeId, String)>;
}

/// 内置默认路由策略：复刻原 `Worker::select_remote_target` 硬编码逻辑。
///
/// 依次应用：槽位非零过滤 → 心跳 TTL 过滤（超过 `heartbeat_ttl_ms` 视为
/// 失联）→ 按可用槽位降序（并列取槽位多者、再取节点名稳定排序）→
/// 本地槽位比较（最优 peer 的可用槽位不多于本地时不转发）。
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRoutePolicy;

impl RoutePolicy for DefaultRoutePolicy {
    fn select_target(
        &self,
        ctx: &RouteContext,
        candidates: &[RouteCandidate],
    ) -> Option<(NodeId, String)> {
        let now_ms = crate::common::epoch_millis();
        // 心跳 TTL 过滤：排除心跳超时的 peer，避免将任务路由到已失联的节点。
        // last_heartbeat_ms == 0 表示从未收到心跳，跳过。
        let mut peers: Vec<&RouteCandidate> = candidates
            .iter()
            .filter(|c| {
                c.available_slots > 0
                    && c.max_slots > 0
                    && c.last_heartbeat_ms > 0
                    && now_ms.saturating_sub(c.last_heartbeat_ms) <= ctx.heartbeat_ttl_ms
            })
            .collect();
        if peers.is_empty() {
            return None;
        }
        peers.sort_by(|a, b| {
            b.available_slots
                .cmp(&a.available_slots)
                .then_with(|| b.max_slots.cmp(&a.max_slots))
                .then_with(|| a.node_id.as_str().cmp(b.node_id.as_str()))
        });
        let best = peers[0];
        if best.available_slots <= ctx.local_available_slots {
            return None;
        }
        Some((
            best.node_id.clone(),
            best.endpoint_addr
                .clone()
                .unwrap_or_else(|| best.node_id.as_str().to_string()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RouteContext {
        RouteContext {
            local_available_slots: 0,
            heartbeat_ttl_ms: 15_000,
        }
    }

    fn cand(node: &str, available: u32, max: u32, hb_age_ms: u64) -> RouteCandidate {
        RouteCandidate {
            node_id: NodeId::from(node.to_string()),
            endpoint_addr: Some(format!("ep-{node}")),
            available_slots: available,
            max_slots: max,
            last_heartbeat_ms: crate::common::epoch_millis().saturating_sub(hb_age_ms),
        }
    }

    #[test]
    fn selects_most_available_peer() {
        let candidates = vec![cand("a", 1, 8, 100), cand("b", 5, 8, 100)];
        let picked = DefaultRoutePolicy
            .select_target(&ctx(), &candidates)
            .unwrap();
        assert_eq!(picked.0.as_str(), "b");
        assert_eq!(picked.1, "ep-b");
    }

    #[test]
    fn skips_stale_and_full_peers() {
        let candidates = vec![cand("stale", 5, 8, 15_001), cand("full", 0, 8, 100)];
        assert!(DefaultRoutePolicy
            .select_target(&ctx(), &candidates)
            .is_none());
    }

    #[test]
    fn skips_never_heartbeat_peers() {
        let mut c = cand("never", 5, 8, 0);
        c.last_heartbeat_ms = 0;
        assert!(DefaultRoutePolicy.select_target(&ctx(), &[c]).is_none());
    }

    #[test]
    fn no_forward_when_local_at_least_as_free() {
        let candidates = vec![cand("b", 3, 8, 100)];
        // 本地 3 槽空闲 ≥ peer 3：转发无收益，不转发。
        let local = RouteContext {
            local_available_slots: 3,
            heartbeat_ttl_ms: 15_000,
        };
        assert!(DefaultRoutePolicy
            .select_target(&local, &candidates)
            .is_none());
        // 本地 2 < peer 3：转发。
        let local = RouteContext {
            local_available_slots: 2,
            heartbeat_ttl_ms: 15_000,
        };
        assert!(DefaultRoutePolicy
            .select_target(&local, &candidates)
            .is_some());
    }

    #[test]
    fn falls_back_to_node_id_when_no_endpoint() {
        let mut c = cand("b", 5, 8, 100);
        c.endpoint_addr = None;
        let picked = DefaultRoutePolicy.select_target(&ctx(), &[c]).unwrap();
        assert_eq!(picked.1, "b");
    }
}
