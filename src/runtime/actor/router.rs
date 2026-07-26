//! 跨节点 Actor 路由：Actor 注册表 + 路由策略 + Gossip 同步（A2）。
//!
//! 三部分协同工作：
//! - [`ActorRegistry`]：维护 `NodeId → actor_types` 映射，是路由器的唯一数据源。
//! - [`ActorRouter`] trait + 三个内置策略：`Random` / `RoundRobin` / `LeastLoaded`。
//! - [`ActorRegistryGossipActor`]：周期性广播本地 actor 注册表，接收远端更新。
//!
//! 注册表更新路径：
//! 1. 本地：[`ActorSystem::spawn`] 调用 [`ActorRegistry::register_local_type`]，
//!    递增 `local_sequence`，下次 gossip 广播时同步给对端。
//! 2. 远端：[`ActorRegistryGossipActor::handle_gossip`] 解码广播消息，调用
//!    [`ActorRegistry::update_peer`]，按 sequence 去重并接受更新。
//!
//! 路由调用路径：
//! - 调用方通过 [`ActorSystem::call_by_type`] 触发，ActorSystem 持有的
//!   `router` 选择目标 NodeId，构造 [`DirectRequest::ActorCallByType`] 发送。
//! - 接收方 [`NetworkEventRouter::handle_direct_request`] 分发到本地 ActorSystem，
//!   按 actor_type 查找本地 actor 实例并调用（这部分已有路由逻辑复用）。

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::common::{ActantError, NodeId, Result};
use crate::runtime::network::Transport;

// ─── Actor Registry ──────────────────────────────────────────────────────

/// 远端节点在注册表中的快照条目。
///
/// 用于观测/调试：通过 [`ActorRegistry::snapshot_peers`] 获取当前所有远端节点的
/// actor 类型注册情况。不应基于此快照做路由决策（注册表是动态的）。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerActorRegistryEntry {
    /// 该节点上已注册的 actor 类型集合。
    pub actor_types: BTreeSet<String>,
    /// 来自源节点的单调递增序号，用于去重。新消息 sequence 必须严格大于已存值。
    pub sequence: u64,
}

/// 跨节点 Actor 注册表：维护 `NodeId → actor_types` 映射。
///
/// 设计：本地与远端注册表分离。
/// - 本地：由 `register_local_type` / `unregister_local_type` 维护，
///   每次变更递增 `local_sequence`，供 [`ActorRegistryGossipActor`] 广播。
/// - 远端：由 `update_peer` 接收 gossip 消息更新，按 sequence 去重。
///
/// 线程安全：所有可变状态使用 `DashMap` 或 `parking_lot::RwLock`，
/// 不持有 GIL，可被 Rust 后台任务直接调用。
pub struct ActorRegistry {
    /// 远端节点的 actor 注册表。键为 NodeId。
    peers: DashMap<NodeId, PeerActorRegistryEntry>,
    /// 本地节点已注册的 actor 类型集合。
    local: parking_lot::RwLock<BTreeSet<String>>,
    /// 本地节点 ID（用于在 `known_nodes` 中排除自身，避免不必要回环）。
    local_node_id: parking_lot::RwLock<Option<NodeId>>,
    /// 本地注册的 sequence：每次本地 actor 类型集合变更时递增。
    local_sequence: AtomicU64,
}

impl ActorRegistry {
    pub fn new() -> Self {
        Self {
            peers: DashMap::new(),
            local: parking_lot::RwLock::new(BTreeSet::new()),
            local_node_id: parking_lot::RwLock::new(None),
            local_sequence: AtomicU64::new(0),
        }
    }

    pub fn with_local_node_id(self, node_id: NodeId) -> Self {
        *self.local_node_id.write() = Some(node_id);
        self
    }

    pub fn set_local_node_id(&self, node_id: NodeId) {
        *self.local_node_id.write() = Some(node_id);
    }

    /// 注册本地 actor 类型。返回 true 表示集合发生变化（即新增类型）。
    pub fn register_local_type(&self, actor_type: &str) -> bool {
        let mut local = self.local.write();
        let inserted = local.insert(actor_type.to_string());
        if inserted {
            self.local_sequence.fetch_add(1, Ordering::SeqCst);
        }
        inserted
    }

    /// 注销本地 actor 类型。返回 true 表示集合发生变化（即移除了类型）。
    pub fn unregister_local_type(&self, actor_type: &str) -> bool {
        let mut local = self.local.write();
        let removed = local.remove(actor_type);
        if removed {
            self.local_sequence.fetch_add(1, Ordering::SeqCst);
        }
        removed
    }

    /// 返回本地 actor 类型集合的快照。
    pub fn local_types(&self) -> BTreeSet<String> {
        self.local.read().clone()
    }

    /// 返回本地注册的当前 sequence（每次本地集合变更时递增）。
    pub fn local_sequence(&self) -> u64 {
        self.local_sequence.load(Ordering::SeqCst)
    }

    /// 返回所有远端节点注册表的快照（A2，观测用）。
    ///
    /// 仅用于观测/调试：调用方不应依赖此快照做路由决策（注册表是动态的，
    /// 且此处读到的是某一时刻的一致视图，调用返回时可能已过期）。
    pub fn snapshot_peers(&self) -> Vec<(NodeId, PeerActorRegistryEntry)> {
        self.peers
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// 更新远端节点的 actor 注册表。
    ///
    /// `sequence` 必须严格大于已存储值才会接受更新，避免乱序消息导致回退。
    /// 返回 true 表示接受更新（新节点或更高 sequence）。
    ///
    /// 自身节点的更新应通过 `register_local_type` 走本地路径，不应来自 gossip。
    /// 收到自身节点的 gossip 广播时返回 false（防回环）。
    pub fn update_peer(
        &self,
        node_id: NodeId,
        actor_types: BTreeSet<String>,
        sequence: u64,
    ) -> bool {
        if self.local_node_id.read().as_ref() == Some(&node_id) {
            return false;
        }
        match self.peers.get_mut(&node_id) {
            Some(mut entry) => {
                if sequence <= entry.sequence {
                    return false;
                }
                entry.actor_types = actor_types;
                entry.sequence = sequence;
                true
            }
            None => {
                self.peers.insert(
                    node_id,
                    PeerActorRegistryEntry {
                        actor_types,
                        sequence,
                    },
                );
                true
            }
        }
    }

    /// 移除远端节点（节点下线时调用）。返回 true 表示存在该节点条目。
    pub fn remove_peer(&self, node_id: &NodeId) -> bool {
        self.peers.remove(node_id).is_some()
    }

    /// 返回已知承载指定 actor 类型的节点列表。
    ///
    /// 优先返回远端节点（避免不必要回环）；本地节点排在最后，
    /// 调用方可通过 `exclude` 参数排除自身。
    pub fn known_nodes(&self, actor_type: &str) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self
            .peers
            .iter()
            .filter(|entry| entry.actor_types.contains(actor_type))
            .map(|entry| entry.key().clone())
            .collect();
        // 本地节点排在最后，使路由器优先选择远端节点。
        // 若本地有此类型且未排除，路由器仍可选择本地（避免无远端可用时调用失败）。
        if let Some(local_id) = self.local_node_id.read().as_ref() {
            if self.local.read().contains(actor_type) {
                nodes.push(local_id.clone());
            }
        }
        nodes
    }

    /// 返回所有已知远端节点（不含本地）。用于调试与监控。
    pub fn peer_nodes(&self) -> Vec<NodeId> {
        self.peers.iter().map(|entry| entry.key().clone()).collect()
    }
}

impl Default for ActorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Router ─────────────────────────────────────────────────────────────

/// Actor 路由策略配置。
///
/// 序列化为小写字符串以匹配 Python 层配置约定（参见 `SchedulerKind`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RouterStrategy {
    /// 随机选择已知节点。
    Random,
    /// 轮询选择已知节点（默认）。
    #[default]
    RoundRobin,
    /// 选择当前 in-flight 调用最少的节点。
    LeastLoaded,
}

impl RouterStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::RoundRobin => "round-robin",
            Self::LeastLoaded => "least-loaded",
        }
    }

    /// 校验策略名称字符串。未知名称返回错误。
    ///
    /// 镜像 [`crate::common::SchedulerKind::parse`] 模式：未知策略在启动时
    /// 返回错误而非静默回退默认值。
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "random" => Ok(Self::Random),
            "round-robin" | "roundrobin" => Ok(Self::RoundRobin),
            "least-loaded" | "leastloaded" => Ok(Self::LeastLoaded),
            other => Err(ActantError::Config(format!(
                "unknown actor router strategy '{}': expected one of: random, round-robin, least-loaded",
                other
            ))),
        }
    }
}

/// Actor 路由器抽象。
///
/// 输入 actor 类型字符串，输出目标 NodeId。
/// 实现基于 [`ActorRegistry`] 中维护的远端节点 actor 注册表选择目标节点。
///
/// 策略实现无需持久化状态：所有共享状态（注册表、计数器）由 [`ActorRegistry`]
/// 持有，路由器仅持有只读引用 + 自身的轮询/随机计数器。
pub trait ActorRouter: Send + Sync + 'static {
    /// 选择承载指定 actor 类型的目标节点。
    ///
    /// `exclude` 用于排除特定节点（如本次调用失败的节点，重试时排除）。
    /// 返回 `None` 表示无可用节点。
    fn select_node(&self, actor_type: &str, exclude: Option<&NodeId>) -> Option<NodeId>;

    /// 路由策略名称（用于日志与监控）。
    fn strategy_name(&self) -> &'static str;

    /// 标记一次调用开始（用于 LeastLoaded 策略跟踪 in-flight 计数）。
    /// 默认空实现；只有 LeastLoaded 覆写。
    fn on_call_start(&self, _node_id: &NodeId) {}

    /// 标记一次调用结束（与 on_call_start 配对）。
    fn on_call_end(&self, _node_id: &NodeId) {}
}

/// 随机选择路由器。
///
/// 使用 UUID v4 + 本地计数器作为伪随机源：避免引入 `rand` crate 依赖，
/// UUID v4 内部已使用 CSPRNG，对其取模得到的选择分布足以满足路由需求。
pub struct RandomRouter {
    registry: Arc<ActorRegistry>,
    counter: AtomicU64,
}

impl RandomRouter {
    pub fn new(registry: Arc<ActorRegistry>) -> Self {
        Self {
            registry,
            counter: AtomicU64::new(0),
        }
    }
}

impl ActorRouter for RandomRouter {
    fn select_node(&self, actor_type: &str, exclude: Option<&NodeId>) -> Option<NodeId> {
        let mut nodes = self.registry.known_nodes(actor_type);
        if let Some(ex) = exclude {
            nodes.retain(|n| n != ex);
        }
        if nodes.is_empty() {
            return None;
        }
        // UUID v4 已是 CSPRNG 随机；取首 8 字节 + counter 异或作为种子。
        // counter 保证并发调用不产生相同选择。
        let uuid_bytes = *uuid::Uuid::new_v4().as_bytes();
        let mut seed_bytes = [0u8; 8];
        seed_bytes.copy_from_slice(&uuid_bytes[..8]);
        let seed = self
            .counter
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(u64::from_le_bytes(seed_bytes));
        let idx = (seed as usize) % nodes.len();
        Some(nodes.remove(idx))
    }

    fn strategy_name(&self) -> &'static str {
        "random"
    }
}

/// 轮询路由器（默认策略）。
pub struct RoundRobinRouter {
    registry: Arc<ActorRegistry>,
    counter: AtomicU64,
}

impl RoundRobinRouter {
    pub fn new(registry: Arc<ActorRegistry>) -> Self {
        Self {
            registry,
            counter: AtomicU64::new(0),
        }
    }
}

impl ActorRouter for RoundRobinRouter {
    fn select_node(&self, actor_type: &str, exclude: Option<&NodeId>) -> Option<NodeId> {
        let mut nodes = self.registry.known_nodes(actor_type);
        if let Some(ex) = exclude {
            nodes.retain(|n| n != ex);
        }
        if nodes.is_empty() {
            return None;
        }
        // 节点列表顺序由 `known_nodes` 决定（远端节点按 DashMap 迭代序），
        // 在并发注册/注销场景下可能短暂不一致；但 fetch_add 保证单次调用的
        // 单调递增，长期来看分布均匀。
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) as usize % nodes.len();
        Some(nodes.remove(idx))
    }

    fn strategy_name(&self) -> &'static str {
        "round-robin"
    }
}

/// 最少 in-flight 调用路由器。
///
/// 跟踪每节点本地 in-flight 调用计数（调用方须配合 `on_call_start` / `on_call_end`）。
/// 选择计数最少的节点；并列时退化为轮询。
///
/// in-flight 计数仅在本节点视角下统计：当本节点发起 `call_by_type` 时计数 +1，
/// 收到响应或失败时 -1。其他节点的 in-flight 不计入（P2P 架构无中心化计数器）。
/// 在多节点同时调用同一 actor_type 的场景下，本策略退化为近似公平分配。
pub struct LeastLoadedRouter {
    registry: Arc<ActorRegistry>,
    in_flight: DashMap<NodeId, AtomicU64>,
    counter: AtomicU64,
}

impl LeastLoadedRouter {
    pub fn new(registry: Arc<ActorRegistry>) -> Self {
        Self {
            registry,
            in_flight: DashMap::new(),
            counter: AtomicU64::new(0),
        }
    }
}

impl ActorRouter for LeastLoadedRouter {
    fn select_node(&self, actor_type: &str, exclude: Option<&NodeId>) -> Option<NodeId> {
        let mut nodes = self.registry.known_nodes(actor_type);
        if let Some(ex) = exclude {
            nodes.retain(|n| n != ex);
        }
        if nodes.is_empty() {
            return None;
        }
        if nodes.len() == 1 {
            return Some(nodes.remove(0));
        }
        // 选择 in-flight 计数最少的节点。并列时按 round-robin 选择，
        // 避免同一节点总被选中。
        let tie_break = self.counter.fetch_add(1, Ordering::Relaxed) as usize;
        let mut best_idx = 0usize;
        let mut best_count = u64::MAX;
        for (i, node) in nodes.iter().enumerate() {
            let count = self
                .in_flight
                .entry(node.clone())
                .or_insert_with(|| AtomicU64::new(0))
                .load(Ordering::Relaxed);
            if count < best_count {
                best_count = count;
                best_idx = i;
            } else if count == best_count {
                // 并列时按 tie_break 决定：保证多节点并发场景下分布均匀。
                if (tie_break + i) % nodes.len() == 0 {
                    best_idx = i;
                }
            }
        }
        Some(nodes.remove(best_idx))
    }

    fn strategy_name(&self) -> &'static str {
        "least-loaded"
    }

    fn on_call_start(&self, node_id: &NodeId) {
        self.in_flight
            .entry(node_id.clone())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    fn on_call_end(&self, node_id: &NodeId) {
        if let Some(counter) = self.in_flight.get(node_id) {
            let prev = counter.fetch_sub(1, Ordering::Relaxed);
            // underflow 防护：调用方保证 on_call_start/end 配对使用，
            // 但若因 panic 或路径遗漏导致未配对，回滚到 0 而非下溢。
            if prev == 0 {
                tracing::warn!(
                    node = %node_id,
                    "LeastLoadedRouter::on_call_end called without matching on_call_start"
                );
                counter.store(0, Ordering::Relaxed);
            }
        }
    }
}

/// 根据 [`RouterStrategy`] 创建路由器实例。
pub fn make_router(strategy: RouterStrategy, registry: Arc<ActorRegistry>) -> Arc<dyn ActorRouter> {
    match strategy {
        RouterStrategy::Random => Arc::new(RandomRouter::new(registry)),
        RouterStrategy::RoundRobin => Arc::new(RoundRobinRouter::new(registry)),
        RouterStrategy::LeastLoaded => Arc::new(LeastLoadedRouter::new(registry)),
    }
}

// ─── Gossip ──────────────────────────────────────────────────────────────

/// Actor 注册表 Gossip 广播消息。
///
/// 由 [`ActorRegistryGossipActor`] 周期性广播，接收方通过
/// [`ActorRegistry::update_peer`] 更新本地注册表。
///
/// `actor_types` 使用 `Vec<String>` 而非 `BTreeSet<String>`：postcard 序列化
/// Vec 更紧凑（无需 BTreeSet 的有序保证），接收方再构造 BTreeSet 用于查找。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorRegistryGossipMsg {
    pub node_id: NodeId,
    pub actor_types: Vec<String>,
    pub sequence: u64,
}

impl ActorRegistryGossipMsg {
    pub fn new(node_id: NodeId, actor_types: BTreeSet<String>, sequence: u64) -> Self {
        Self {
            node_id,
            actor_types: actor_types.into_iter().collect(),
            sequence,
        }
    }

    pub fn topic() -> &'static str {
        crate::common::wire::constants::TOPIC_ACTOR_REGISTRY
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self).map_err(|e| ActantError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        crate::common::decode_postcard(bytes)
    }
}

/// Actor 注册表 Gossip 子系统：周期性广播本地 actor 注册表，处理远端广播。
///
/// 不实现 `Actor` trait：单一后台循环 + 网络被动接收，
/// 与 [`crate::runtime::capability::gossip::CapabilityGossipActor`] 保持一致的设计。
/// 由 [`crate::runtime::builder::RuntimeBuilder`] 直接 spawn 后台任务。
///
/// 接收路径：本 actor 的 `handle_gossip` 由 [`NetworkEventRouter`] 在收到
/// `TOPIC_ACTOR_REGISTRY` gossip 消息时调用（区别于 `CapabilityGossipActor`，
/// 后者的接收路径尚未在 NetworkEventRouter 中接通）。
pub struct ActorRegistryGossipActor {
    node_id: NodeId,
    registry: Arc<ActorRegistry>,
    network: Arc<dyn Transport>,
    seen: DashMap<NodeId, u64>,
    broadcast_interval: Duration,
}

impl ActorRegistryGossipActor {
    pub fn new(node_id: NodeId, registry: Arc<ActorRegistry>, network: Arc<dyn Transport>) -> Self {
        Self {
            node_id,
            registry,
            network,
            seen: DashMap::new(),
            broadcast_interval: Duration::from_secs(30),
        }
    }

    pub fn with_broadcast_interval(mut self, interval: Duration) -> Self {
        self.broadcast_interval = interval;
        self
    }

    pub async fn broadcast_registry(&self) -> Result<()> {
        let actor_types = self.registry.local_types();
        let sequence = self.registry.local_sequence();
        // 即使 actor_types 为空也广播：让对端知道本节点不再承载任何 actor
        // （节点上所有 actor 已停止的场景）。空 Vec 序列化仅占少量字节。
        let gossip = ActorRegistryGossipMsg::new(self.node_id.clone(), actor_types, sequence);
        let bytes = gossip.to_bytes()?;
        self.network
            .broadcast(ActorRegistryGossipMsg::topic(), bytes)
            .await
            .map_err(|e| ActantError::Network(e.to_string()))?;
        Ok(())
    }

    pub fn handle_gossip(&self, payload: &[u8]) {
        match ActorRegistryGossipMsg::from_bytes(payload) {
            Ok(gossip) => {
                if gossip.node_id == self.node_id {
                    return;
                }
                if let Some(prev) = self.seen.get(&gossip.node_id) {
                    if *prev >= gossip.sequence {
                        return;
                    }
                }
                self.seen.insert(gossip.node_id.clone(), gossip.sequence);
                let actor_types: BTreeSet<String> = gossip.actor_types.iter().cloned().collect();
                if self
                    .registry
                    .update_peer(gossip.node_id.clone(), actor_types, gossip.sequence)
                {
                    tracing::debug!(
                        from = %gossip.node_id,
                        actor_types = gossip.actor_types.len(),
                        sequence = gossip.sequence,
                        "received actor registry gossip"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to decode actor registry gossip");
            }
        }
    }

    /// 启动周期性广播的后台循环。返回 cancel 句柄，发送 `true` 即可停止。
    pub fn start_background_loop(self: Arc<Self>) -> watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.broadcast_interval);
            // 第一次 tick 立即触发，让 actor 注册信息尽快扩散。
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = ticker.tick() => {
                        if let Err(e) = self.broadcast_registry().await {
                            tracing::warn!(error = %e, "actor registry gossip broadcast failed");
                        }
                    }
                }
            }
            tracing::debug!("actor registry gossip background loop stopped");
        });
        cancel_tx
    }
}

#[cfg(test)]
#[path = "../../../tests/rust/unit/runtime/actor_router.rs"]
mod tests;
