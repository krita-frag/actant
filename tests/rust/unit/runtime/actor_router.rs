//! Unit tests for `src/runtime/actor_router.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::NodeId;
use crate::runtime::network::{
    DirectRequest, DirectResponse, DirectResponseChannel, ListenAddresses, NetworkEvent, PeerId,
};
use std::collections::BTreeSet;
use std::sync::Mutex;

// ─── ActorRegistry ───────────────────────────────────────────────────────

#[test]
fn registry_register_local_type_increments_sequence() {
    let reg = ActorRegistry::new();
    assert_eq!(reg.local_sequence(), 0);
    assert!(reg.register_local_type("Foo"));
    assert_eq!(reg.local_sequence(), 1);
    // 重复注册不递增
    assert!(!reg.register_local_type("Foo"));
    assert_eq!(reg.local_sequence(), 1);
    // 新类型递增
    assert!(reg.register_local_type("Bar"));
    assert_eq!(reg.local_sequence(), 2);
}

#[test]
fn registry_unregister_local_type_increments_sequence() {
    let reg = ActorRegistry::new();
    reg.register_local_type("Foo");
    reg.register_local_type("Bar");
    let before = reg.local_sequence();
    assert!(reg.unregister_local_type("Foo"));
    assert_eq!(reg.local_sequence(), before + 1);
    // 重复注销不递增
    assert!(!reg.unregister_local_type("Foo"));
    assert_eq!(reg.local_sequence(), before + 1);
}

#[test]
fn registry_update_peer_accepts_higher_sequence() {
    let reg = ActorRegistry::new();
    let peer = NodeId::new("peer-1".into());
    let mut types = BTreeSet::new();
    types.insert("Foo".to_string());
    assert!(reg.update_peer(peer.clone(), types.clone(), 1));
    // 同 sequence 被拒绝
    assert!(!reg.update_peer(peer.clone(), types.clone(), 1));
    // 低 sequence 被拒绝
    assert!(!reg.update_peer(peer.clone(), types, 0));
    // 高 sequence 接受
    let mut types2 = BTreeSet::new();
    types2.insert("Foo".to_string());
    types2.insert("Bar".to_string());
    assert!(reg.update_peer(peer.clone(), types2, 5));
    // 验证已知节点反映更新后的类型集合
    let known = reg.known_nodes("Bar");
    assert_eq!(known.len(), 1);
    assert_eq!(known[0], peer);
}

#[test]
fn registry_update_peer_rejects_self_node() {
    // 自身节点的更新不应来自 gossip（防回环）
    let reg = ActorRegistry::new().with_local_node_id(NodeId::new("self".into()));
    let mut types = BTreeSet::new();
    types.insert("Foo".to_string());
    assert!(!reg.update_peer(NodeId::new("self".into()), types, 1));
}

#[test]
fn registry_known_nodes_includes_local_when_local_has_type() {
    let reg = ActorRegistry::new().with_local_node_id(NodeId::new("local".into()));
    reg.register_local_type("Foo");
    // 仅本地有此类型：known_nodes 应包含本地（排在最后）
    let known = reg.known_nodes("Foo");
    assert_eq!(known.len(), 1);
    assert_eq!(known[0], NodeId::new("local".into()));
}

#[test]
fn registry_known_nodes_prefers_remote_over_local() {
    let reg = ActorRegistry::new().with_local_node_id(NodeId::new("local".into()));
    reg.register_local_type("Foo");
    // 远端节点也有此类型
    let mut peer_types = BTreeSet::new();
    peer_types.insert("Foo".to_string());
    reg.update_peer(NodeId::new("peer-1".into()), peer_types, 1);
    let known = reg.known_nodes("Foo");
    assert_eq!(known.len(), 2);
    // 本地排在最后，远端优先
    assert_eq!(known[0], NodeId::new("peer-1".into()));
    assert_eq!(known[1], NodeId::new("local".into()));
}

#[test]
fn registry_remove_peer_returns_true_when_existed() {
    let reg = ActorRegistry::new();
    let peer = NodeId::new("peer-1".into());
    let types = BTreeSet::new();
    reg.update_peer(peer.clone(), types, 1);
    assert!(reg.remove_peer(&peer));
    assert!(!reg.remove_peer(&peer));
}

// ─── RouterStrategy ──────────────────────────────────────────────────────

#[test]
fn router_strategy_parse_accepts_canonical_names() {
    assert_eq!(
        RouterStrategy::parse("random").unwrap(),
        RouterStrategy::Random
    );
    assert_eq!(
        RouterStrategy::parse("round-robin").unwrap(),
        RouterStrategy::RoundRobin
    );
    // 接受连字符省略形式
    assert_eq!(
        RouterStrategy::parse("roundrobin").unwrap(),
        RouterStrategy::RoundRobin
    );
    assert_eq!(
        RouterStrategy::parse("least-loaded").unwrap(),
        RouterStrategy::LeastLoaded
    );
    assert_eq!(
        RouterStrategy::parse("leastloaded").unwrap(),
        RouterStrategy::LeastLoaded
    );
}

#[test]
fn router_strategy_parse_case_insensitive() {
    assert_eq!(
        RouterStrategy::parse("RANDOM").unwrap(),
        RouterStrategy::Random
    );
    assert_eq!(
        RouterStrategy::parse("RoundRobin").unwrap(),
        RouterStrategy::RoundRobin
    );
}

#[test]
fn router_strategy_parse_rejects_unknown() {
    assert!(RouterStrategy::parse("invalid").is_err());
    assert!(RouterStrategy::parse("").is_err());
}

#[test]
fn router_strategy_default_is_round_robin() {
    assert_eq!(RouterStrategy::default(), RouterStrategy::RoundRobin);
}

#[test]
fn router_strategy_as_str_is_stable() {
    assert_eq!(RouterStrategy::Random.as_str(), "random");
    assert_eq!(RouterStrategy::RoundRobin.as_str(), "round-robin");
    assert_eq!(RouterStrategy::LeastLoaded.as_str(), "least-loaded");
}

#[test]
fn make_router_creates_correct_strategy() {
    let reg = Arc::new(ActorRegistry::new());
    let r1 = make_router(RouterStrategy::Random, reg.clone());
    assert_eq!(r1.strategy_name(), "random");
    let r2 = make_router(RouterStrategy::RoundRobin, reg.clone());
    assert_eq!(r2.strategy_name(), "round-robin");
    let r3 = make_router(RouterStrategy::LeastLoaded, reg);
    assert_eq!(r3.strategy_name(), "least-loaded");
}

// ─── Router Strategies ──────────────────────────────────────────────────

fn registry_with_peers(
    self_id: &str,
    peers: &[(&str, &[&str])],
    local_types: &[&str],
) -> Arc<ActorRegistry> {
    let reg = Arc::new(ActorRegistry::new().with_local_node_id(NodeId::new(self_id.into())));
    for t in local_types {
        reg.register_local_type(t);
    }
    for (peer_id, types) in peers {
        let mut bt = BTreeSet::new();
        for t in *types {
            bt.insert(t.to_string());
        }
        reg.update_peer(NodeId::new(peer_id.to_string()), bt, 1);
    }
    reg
}

#[test]
fn router_returns_none_when_no_node_has_type() {
    let reg = registry_with_peers("self", &[], &[]);
    let router = RoundRobinRouter::new(reg);
    assert!(router.select_node("Foo", None).is_none());
}

#[test]
fn round_robin_rotates_through_nodes() {
    let reg = registry_with_peers(
        "self",
        &[("p1", &["Foo"]), ("p2", &["Foo"]), ("p3", &["Foo"])],
        &[],
    );
    let router = RoundRobinRouter::new(reg);
    // 三次连续选择应得到三个不同节点（顺序可能因 DashMap 迭代序而不同，
    // 但每个节点应被选到一次）。
    let mut selected = std::collections::HashSet::new();
    for _ in 0..3 {
        let n = router.select_node("Foo", None).expect("应有节点");
        selected.insert(n.as_str().to_string());
    }
    assert_eq!(selected.len(), 3, "round-robin 应轮询所有节点");
}

#[test]
fn round_robin_excludes_specified_node() {
    let reg = registry_with_peers("self", &[("p1", &["Foo"]), ("p2", &["Foo"])], &[]);
    let router = RoundRobinRouter::new(reg);
    let excluded = NodeId::new("p1".into());
    let n = router
        .select_node("Foo", Some(&excluded))
        .expect("应有节点");
    assert_ne!(n, excluded, "被排除的节点不应被选中");
}

#[test]
fn random_router_returns_valid_node() {
    let reg = registry_with_peers(
        "self",
        &[("p1", &["Foo"]), ("p2", &["Foo"]), ("p3", &["Foo"])],
        &[],
    );
    let router = RandomRouter::new(reg);
    for _ in 0..10 {
        let n = router.select_node("Foo", None).expect("应有节点");
        let s = n.as_str();
        assert!(s == "p1" || s == "p2" || s == "p3", "应返回承载 Foo 的节点");
    }
}

#[test]
fn least_loaded_router_picks_least_busy() {
    let reg = registry_with_peers("self", &[("p1", &["Foo"]), ("p2", &["Foo"])], &[]);
    let router = Arc::new(LeastLoadedRouter::new(reg));
    // 模拟 p1 上有 2 个 in-flight 调用
    router.on_call_start(&NodeId::new("p1".into()));
    router.on_call_start(&NodeId::new("p1".into()));
    // p2 上无调用，应被选中
    let n = router.select_node("Foo", None).expect("应有节点");
    assert_eq!(n, NodeId::new("p2".into()), "应选择 in-flight 最少的节点");
}

#[test]
fn least_loaded_router_balances_under_load() {
    let reg = registry_with_peers("self", &[("p1", &["Foo"]), ("p2", &["Foo"])], &[]);
    let router = Arc::new(LeastLoadedRouter::new(reg));
    // 两个节点 in-flight 相同：应通过 tie-break 轮询，不会只选一个
    let mut seen_p1 = false;
    let mut seen_p2 = false;
    for _ in 0..20 {
        let n = router.select_node("Foo", None).expect("应有节点");
        match n.as_str() {
            "p1" => seen_p1 = true,
            "p2" => seen_p2 = true,
            _ => panic!("unexpected node"),
        }
    }
    assert!(seen_p1 && seen_p2, "并列时应轮询两个节点");
}

#[test]
fn least_loaded_router_underflow_protection_does_not_panic() {
    // on_call_end 在未配对 on_call_start 时不应 panic（防 underflow）
    let reg = registry_with_peers("self", &[], &[]);
    let router = Arc::new(LeastLoadedRouter::new(reg));
    router.on_call_end(&NodeId::new("p1".into()));
    // 应能继续选择（不会因 underflow 阻塞后续调用）
    assert!(router.select_node("Foo", None).is_none());
}

// ─── Gossip Message ──────────────────────────────────────────────────────

#[test]
fn gossip_msg_roundtrip() {
    let mut types = BTreeSet::new();
    types.insert("Foo".to_string());
    types.insert("Bar".to_string());
    let msg = ActorRegistryGossipMsg::new(NodeId::new("n1".into()), types, 42);
    let bytes = msg.to_bytes().unwrap();
    let decoded = ActorRegistryGossipMsg::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.node_id, msg.node_id);
    assert_eq!(decoded.sequence, msg.sequence);
    assert_eq!(decoded.actor_types.len(), 2);
}

#[test]
fn gossip_msg_topic_is_stable() {
    // topic 用于订阅匹配，必须是稳定常量
    assert_eq!(
        ActorRegistryGossipMsg::topic(),
        crate::common::wire::constants::TOPIC_ACTOR_REGISTRY
    );
}

#[test]
fn gossip_msg_from_bytes_rejects_garbage() {
    assert!(ActorRegistryGossipMsg::from_bytes(b"not postcard").is_err());
}

#[test]
fn gossip_msg_handles_empty_types() {
    // 节点上无 actor 时也应能广播（让对端知道本节点已清空）
    let msg = ActorRegistryGossipMsg::new(NodeId::new("n1".into()), BTreeSet::new(), 0);
    let bytes = msg.to_bytes().unwrap();
    let decoded = ActorRegistryGossipMsg::from_bytes(&bytes).unwrap();
    assert!(decoded.actor_types.is_empty());
}

// ─── ActorRegistryGossipActor ────────────────────────────────────────────

/// 记录所有 broadcast 调用的 Transport 桩，用于验证广播行为。
struct RecordingTransport {
    node_id: NodeId,
    broadcasts: Mutex<Vec<(String, Vec<u8>)>>,
}

impl RecordingTransport {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            broadcasts: Mutex::new(Vec::new()),
        }
    }

    fn broadcasts(&self) -> Vec<(String, Vec<u8>)> {
        self.broadcasts.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Transport for RecordingTransport {
    fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    fn local_peer_id(&self) -> &str {
        "recording-peer"
    }
    async fn broadcast(&self, topic: &str, data: Vec<u8>) -> crate::common::Result<()> {
        self.broadcasts
            .lock()
            .unwrap()
            .push((topic.to_string(), data));
        Ok(())
    }
    async fn subscribe(&self, _topic: &str) -> crate::common::Result<()> {
        Ok(())
    }
    async fn recv_event(&self) -> Option<NetworkEvent> {
        None
    }
    async fn dial(&self, _addr: &str) -> crate::common::Result<()> {
        Ok(())
    }
    async fn add_gossip_peer(&self, _peer_id: &str) -> crate::common::Result<()> {
        Ok(())
    }
    fn listen_addresses(&self) -> crate::common::Result<ListenAddresses> {
        Ok(ListenAddresses {
            endpoint_id: String::new(),
            relay_url: None,
            direct_addrs: Vec::new(),
            endpoint_addr: String::new(),
        })
    }
    async fn send_direct_request(
        &self,
        _peer_id_str: &str,
        _request: DirectRequest,
    ) -> crate::common::Result<DirectResponse> {
        Ok(DirectResponse::Error {
            message: "not implemented in stub".into(),
        })
    }
    async fn send_direct_response(
        &self,
        _channel: DirectResponseChannel,
        _response: DirectResponse,
    ) -> crate::common::Result<()> {
        Ok(())
    }
    async fn discover_peers(&self) -> crate::common::Result<Vec<PeerId>> {
        Ok(Vec::new())
    }
    async fn shutdown(&self) -> crate::common::Result<()> {
        Ok(())
    }
}

fn make_gossip_actor(
    node_id: &str,
) -> (
    ActorRegistryGossipActor,
    Arc<RecordingTransport>,
    Arc<ActorRegistry>,
) {
    let reg = Arc::new(ActorRegistry::new().with_local_node_id(NodeId::new(node_id.into())));
    let rec = Arc::new(RecordingTransport::new(NodeId::new(node_id.into())));
    let transport: Arc<dyn Transport> = rec.clone();
    let actor = ActorRegistryGossipActor::new(NodeId::new(node_id.into()), reg.clone(), transport);
    (actor, rec, reg)
}

#[tokio::test]
async fn broadcast_registry_emits_on_correct_topic() {
    let (actor, rec, reg) = make_gossip_actor("self-node");
    reg.register_local_type("Foo");
    reg.register_local_type("Bar");
    actor.broadcast_registry().await.unwrap();
    let broadcasts = rec.broadcasts();
    assert_eq!(broadcasts.len(), 1);
    let (topic, bytes) = &broadcasts[0];
    assert_eq!(topic, ActorRegistryGossipMsg::topic());
    let msg = ActorRegistryGossipMsg::from_bytes(bytes).unwrap();
    assert_eq!(msg.node_id, NodeId::new("self-node".into()));
    assert_eq!(msg.actor_types.len(), 2);
    assert_eq!(msg.sequence, 2, "sequence 应等于本地变更次数");
}

#[tokio::test]
async fn broadcast_registry_works_with_no_local_types() {
    // 空注册表也应能广播：让对端知道本节点无任何 actor 类型
    let (actor, rec, _reg) = make_gossip_actor("self-node");
    actor.broadcast_registry().await.unwrap();
    let broadcasts = rec.broadcasts();
    assert_eq!(broadcasts.len(), 1);
    let (_, bytes) = &broadcasts[0];
    let msg = ActorRegistryGossipMsg::from_bytes(bytes).unwrap();
    assert!(msg.actor_types.is_empty());
}

#[test]
fn handle_gossip_ignores_own_messages() {
    let (actor, _rec, _reg) = make_gossip_actor("self-node");
    let own_msg = ActorRegistryGossipMsg::new(
        NodeId::new("self-node".into()),
        BTreeSet::from(["Foo".to_string()]),
        5,
    );
    actor.handle_gossip(&own_msg.to_bytes().unwrap());
    // 自身消息不应记录到 seen
    assert!(actor.seen.is_empty());
}

#[test]
fn handle_gossip_dedups_by_sequence() {
    let (actor, _rec, reg) = make_gossip_actor("self-node");
    let peer = NodeId::new("peer-1".into());
    let types = BTreeSet::from(["Foo".to_string()]);
    // 第一次广播 seq=1
    let msg1 = ActorRegistryGossipMsg::new(peer.clone(), types.clone(), 1);
    actor.handle_gossip(&msg1.to_bytes().unwrap());
    assert_eq!(reg.known_nodes("Foo").len(), 1);
    // 同 sequence 重复：应被忽略
    actor.handle_gossip(&msg1.to_bytes().unwrap());
    // 低 sequence：应被忽略
    let msg0 = ActorRegistryGossipMsg::new(peer.clone(), types.clone(), 0);
    actor.handle_gossip(&msg0.to_bytes().unwrap());
    assert_eq!(
        reg.known_nodes("Foo").len(),
        1,
        "重复或低 seq 不应导致重新注册"
    );
    // 高 sequence：应接受更新
    let msg2 = ActorRegistryGossipMsg::new(
        peer.clone(),
        BTreeSet::from(["Foo".to_string(), "Bar".to_string()]),
        2,
    );
    actor.handle_gossip(&msg2.to_bytes().unwrap());
    assert_eq!(reg.known_nodes("Bar").len(), 1, "高 seq 应被接受");
}

#[test]
fn handle_gossip_ignores_garbage_payload() {
    let (actor, _rec, _reg) = make_gossip_actor("self-node");
    // 不应 panic
    actor.handle_gossip(b"not a valid postcard payload");
}

#[tokio::test]
async fn end_to_end_gossip_sync() {
    // 模拟两个节点的 gossip 交互：
    // 节点 A 有 Foo actor，节点 B 通过 gossip 收到 A 的注册表，
    // B 的路由器应能选中 A。
    let (actor_a, rec_a, _reg_a) = make_gossip_actor("node-a");
    {
        let _reg_a = _reg_a.clone();
        _reg_a.register_local_type("Foo");
    }
    actor_a.broadcast_registry().await.unwrap();
    let (_, bytes) = &rec_a.broadcasts()[0];

    let (actor_b, _rec_b, reg_b) = make_gossip_actor("node-b");
    actor_b.handle_gossip(bytes);

    // B 应能在 known_nodes 中找到 A
    let known = reg_b.known_nodes("Foo");
    assert_eq!(known.len(), 1);
    assert_eq!(known[0], NodeId::new("node-a".into()));
}

#[tokio::test]
async fn end_to_end_router_selects_peer_after_gossip() {
    // 完整流程：A 注册 Foo → 广播 → B 接收 → B 的路由器选中 A
    let (actor_a, rec_a, reg_a) = make_gossip_actor("node-a");
    reg_a.register_local_type("Foo");
    actor_a.broadcast_registry().await.unwrap();
    let (_, bytes) = &rec_a.broadcasts()[0];

    let (actor_b, _rec_b, reg_b) = make_gossip_actor("node-b");
    actor_b.handle_gossip(bytes);

    let router = RoundRobinRouter::new(reg_b);
    let selected = router.select_node("Foo", None).expect("应选中 node-a");
    assert_eq!(selected, NodeId::new("node-a".into()));
}
