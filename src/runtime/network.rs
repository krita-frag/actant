//! 网络传输抽象与 iroh 实现。
//!
//! 作为 Runtime 的 Network 盒。
//!
//! 公开内容：
//! - `Discovery` trait 与内置发现策略（local / mdns / none / 预留 dns / relay）。
//! - `Transport` trait：网络传输抽象，便于测试与替换实现。
//! - `NetworkManager`：基于 iroh 的 `Transport` 实现，提供 gossip、直连请求-响应、
//!   peer 发现等能力。
//! - `DirectRequest` / `DirectResponse`：直连请求-响应协议消息。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use iroh::endpoint::{presets::Preset, Builder, Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId, KeyParsingError, TransportAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::{Gossip, TopicId};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::common::discovery_mode;
use crate::common::model::{BlobHash, NodeId, TaskId, WorkflowId};
use crate::common::wire::WireTaskOutcome;
use crate::common::{ActantError, NetworkConfig};
use crate::runtime::blobs::{BlobFetch, BlobStore};

/// `Transport::listen_addresses()` 的结构化结果。
#[derive(Debug, Clone)]
pub struct ListenAddresses {
    pub endpoint_id: String,
    pub relay_url: Option<String>,
    pub direct_addrs: Vec<String>,
    pub endpoint_addr: String,
}

#[derive(Debug, Clone)]
pub struct PeerId(pub String);

/// Gossip 广播消息。
#[derive(Debug, Clone)]
pub struct NetworkMessage {
    pub topic: String,
    pub data: Vec<u8>,
}

/// 直连请求-响应通道的不透明句柄。
#[derive(Debug)]
pub struct DirectResponseChannel(Option<iroh::endpoint::SendStream>);

impl DirectResponseChannel {
    pub fn new(send_stream: iroh::endpoint::SendStream) -> Self {
        Self(Some(send_stream))
    }

    pub fn take(mut self) -> Option<iroh::endpoint::SendStream> {
        self.0.take()
    }

    #[cfg(test)]
    pub fn test_stub() -> Self {
        Self(None)
    }

    /// 将响应写入底层 SendStream 并关闭流。
    ///
    /// 提取自 `NetworkManager::send_direct_response`，使 `DirectResponseChannel`
    /// 可独立于 `NetworkManager` 发送响应——例如 EventBus 在丢弃 DirectRequest
    /// 时通过此方法回送 `DirectResponse::Error`，避免调用方永久阻塞。
    pub async fn send_response(mut self, response: DirectResponse) -> crate::common::Result<()> {
        let Some(mut send) = self.0.take() else {
            return Err(ActantError::Network(
                "direct response channel already consumed".into(),
            ));
        };
        let response_bytes = postcard::to_allocvec(&response)
            .map_err(|e| ActantError::Serialization(e.to_string()))?;
        write_length_prefixed(&mut send, &response_bytes).await?;
        send.finish()
            .map_err(|e| ActantError::Network(format!("finish response stream: {e}")))?;
        Ok(())
    }

    /// 便捷方法：发送一个 `DirectResponse::Error` 响应。
    ///
    /// 当无法处理或投递 DirectRequest 时调用，确保对端能收到明确错误而非
    /// 永久阻塞。错误发送失败时仅记录日志——此时对端只能依赖自身超时。
    pub async fn send_error(self, message: impl Into<String>) {
        let msg = message.into();
        if let Err(e) = self
            .send_response(DirectResponse::Error {
                message: msg.clone(),
            })
            .await
        {
            tracing::warn!(
                error = %e,
                message = %msg,
                "failed to send DirectResponse::Error; caller must rely on its own timeout",
            );
        }
    }
}

/// 网络传输的抽象。
///
/// Runtime 其他部分只依赖此 trait，不直接依赖 iroh。实现需要同时支持 gossip
/// 主题广播、peer 发现事件和直连 request/response。
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    /// 返回本 Actant 节点 ID。
    fn node_id(&self) -> &NodeId;
    /// 返回底层传输 peer ID（iroh endpoint id 字符串）。
    fn local_peer_id(&self) -> &str;
    /// 向已订阅的 gossip topic 广播字节消息。
    ///
    /// # Errors
    ///
    /// 如果 topic 未订阅或底层 gossip 发送失败，返回错误。
    async fn broadcast(&self, topic: &str, data: Vec<u8>) -> crate::common::Result<()>;
    /// 订阅 gossip topic 并把后续消息转发为 [`NetworkEvent`]。
    ///
    /// # Errors
    ///
    /// 如果底层传输无法订阅 topic，返回错误。
    async fn subscribe(&self, topic: &str) -> crate::common::Result<()>;
    /// 接收下一条网络事件。
    async fn recv_event(&self) -> Option<NetworkEvent>;
    /// 通过 endpoint address 建立直连，并把 peer 加入已有 gossip topic。
    ///
    /// # Errors
    ///
    /// 如果地址解析或连接失败，返回错误。
    async fn dial(&self, addr: &str) -> crate::common::Result<()>;
    /// 将已知 peer id 加入所有已订阅 gossip topic。
    ///
    /// # Errors
    ///
    /// 如果 peer id 格式无效，返回错误。
    async fn add_gossip_peer(&self, peer_id: &str) -> crate::common::Result<()>;
    /// 返回当前节点可被其他节点连接的地址信息。
    ///
    /// # Errors
    ///
    /// 如果 endpoint address 无法序列化，返回错误。
    fn listen_addresses(&self) -> crate::common::Result<ListenAddresses>;
    /// 发送一次直连 request 并等待 response。
    ///
    /// # Errors
    ///
    /// 如果连接、写入、读取、反序列化或超时失败，返回错误。
    async fn send_direct_request(
        &self,
        peer_id_str: &str,
        request: DirectRequest,
    ) -> crate::common::Result<DirectResponse>;
    /// 通过收到的 response channel 回复直连请求。
    ///
    /// # Errors
    ///
    /// 如果底层 channel 写入失败，返回错误。
    async fn send_direct_response(
        &self,
        channel: DirectResponseChannel,
        response: DirectResponse,
    ) -> crate::common::Result<()>;
    /// 返回当前 gossip 视图中仍连接的 peer。
    async fn discover_peers(&self) -> crate::common::Result<Vec<PeerId>>;
    /// 关闭传输并释放底层 endpoint。
    async fn shutdown(&self) -> crate::common::Result<()>;
}

#[derive(Debug)]
#[non_exhaustive]
pub enum NetworkEvent {
    Message(NetworkMessage),
    PeerConnected {
        peer_id: String,
    },
    PeerDisconnected {
        peer_id: String,
    },
    DirectRequest {
        peer_id: String,
        request: Box<DirectRequest>,
        channel: DirectResponseChannel,
    },
}

/// 在 iroh endpoint 上配置 peer 发现的 trait。
///
/// 实现接收一个最小配置的 iroh `Builder`（已设置 crypto provider），
/// 返回应用了发现机制的 builder。
pub trait Discovery: std::fmt::Debug + Send + Sync + 'static {
    /// 将此发现机制应用到 iroh endpoint builder。
    fn apply(&self, builder: Builder) -> Builder;

    /// 此发现策略的可读名称。
    fn name(&self) -> &'static str;
}

/// 无自动发现。
#[derive(Debug, Clone, Copy, Default)]
pub struct NoDiscovery;

impl Discovery for NoDiscovery {
    #[tracing::instrument(name = "discovery.none", level = "debug", skip_all)]
    fn apply(&self, builder: Builder) -> Builder {
        iroh::endpoint::presets::Minimal.apply(builder)
    }

    fn name(&self) -> &'static str {
        discovery_mode::NONE
    }
}

/// 使用 iroh n0 preset 的本地发现。
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalDiscovery;

impl Discovery for LocalDiscovery {
    #[tracing::instrument(name = "discovery.local", level = "debug", skip_all)]
    fn apply(&self, builder: Builder) -> Builder {
        iroh::endpoint::presets::N0.apply(builder)
    }

    fn name(&self) -> &'static str {
        discovery_mode::LOCAL
    }
}

/// 基于 mDNS 的本地网络发现。
#[derive(Debug, Clone, Copy, Default)]
pub struct MdnsDiscovery;

impl Discovery for MdnsDiscovery {
    #[tracing::instrument(name = "discovery.mdns", level = "debug", skip_all)]
    fn apply(&self, builder: Builder) -> Builder {
        iroh::endpoint::presets::N0DisableRelay.apply(builder)
    }

    fn name(&self) -> &'static str {
        discovery_mode::MDNS
    }
}

/// DNS endpoint 发现策略。
///
/// 启用 iroh 的 `DnsAddressLookup` + `PkarrPublisher`，禁用 relay。
/// 适合 K8s Headless Service 或自建 DNS 场景：节点通过 DNS TXT 记录
/// `_iroh.<z32-endpoint-id>.<origin_domain>` 互相发现。
///
/// 若 `dns_origin_domain` 为空，使用 n0 默认域 `iroh.link`。
#[derive(Debug, Clone, Default)]
pub struct DnsDiscovery {
    /// 自定义 DNS 起源域。空表示使用 n0 默认域。
    pub origin_domain: String,
}

impl Discovery for DnsDiscovery {
    #[tracing::instrument(name = "discovery.dns", level = "debug", skip_all, fields(origin = %self.origin_domain))]
    fn apply(&self, builder: Builder) -> Builder {
        let mut builder = iroh::endpoint::presets::Minimal.apply(builder);
        let lookup = if self.origin_domain.is_empty() {
            iroh::address_lookup::DnsAddressLookup::n0_dns()
        } else {
            iroh::address_lookup::DnsAddressLookup::builder(self.origin_domain.clone())
        };
        builder = builder
            .address_lookup(iroh::address_lookup::PkarrPublisher::n0_dns())
            .address_lookup(lookup)
            .relay_mode(iroh::RelayMode::Disabled);
        builder
    }

    fn name(&self) -> &'static str {
        discovery_mode::DNS
    }
}

/// 强制启用 iroh relay 中继的发现策略。
///
/// 等价于 n0 预设但显式启用 `RelayMode::Default`，确保 NAT 穿透场景下
/// 节点可通过 n0 公共 relay 中继。若需使用自定义 relay 集群，请扩展
/// `NetworkConfig` 增加自定义 relay map（暂未实现，0.4 计划）。
#[derive(Debug, Clone, Copy, Default)]
pub struct RelayDiscovery;

impl Discovery for RelayDiscovery {
    #[tracing::instrument(name = "discovery.relay", level = "debug", skip_all)]
    fn apply(&self, builder: Builder) -> Builder {
        iroh::endpoint::presets::N0.apply(builder)
    }

    fn name(&self) -> &'static str {
        discovery_mode::RELAY
    }
}

/// 装箱的类型擦除发现策略。
#[derive(Debug, Clone)]
pub struct BoxedDiscovery(Arc<dyn Discovery>);

impl BoxedDiscovery {
    pub fn new<D: Discovery>(discovery: D) -> Self {
        Self(Arc::new(discovery))
    }
}

impl Discovery for BoxedDiscovery {
    fn apply(&self, builder: Builder) -> Builder {
        self.0.apply(builder)
    }

    fn name(&self) -> &'static str {
        self.0.name()
    }
}

/// 若 `name` 是内置发现策略则返回 `true`。
pub fn is_registered(name: &str) -> bool {
    matches!(
        name,
        discovery_mode::NONE
            | discovery_mode::LOCAL
            | discovery_mode::MDNS
            | discovery_mode::DNS
            | discovery_mode::RELAY
    )
}

/// 返回内置发现策略名称的排序列表。
pub fn registered_names() -> Vec<String> {
    vec![
        discovery_mode::NONE.to_string(),
        discovery_mode::LOCAL.to_string(),
        discovery_mode::MDNS.to_string(),
        discovery_mode::DNS.to_string(),
        discovery_mode::RELAY.to_string(),
    ]
}

/// 从字符串名创建发现策略。
///
/// `dns` 模式下 `config.dns_origin_domain` 非空时使用自定义 DNS 起源域，
/// 否则回退到 n0 默认 `iroh.link`。
pub fn discovery_from_name(
    name: &str,
    config: &NetworkConfig,
) -> Result<BoxedDiscovery, ActantError> {
    let _span = tracing::debug_span!("discovery.resolve", name = name).entered();
    match name {
        discovery_mode::NONE => Ok(BoxedDiscovery::new(NoDiscovery)),
        discovery_mode::LOCAL => Ok(BoxedDiscovery::new(LocalDiscovery)),
        discovery_mode::MDNS => Ok(BoxedDiscovery::new(MdnsDiscovery)),
        discovery_mode::DNS => Ok(BoxedDiscovery::new(DnsDiscovery {
            origin_domain: config.dns_origin_domain.clone(),
        })),
        discovery_mode::RELAY => Ok(BoxedDiscovery::new(RelayDiscovery)),
        other => Err(ActantError::Config(format!(
            "unknown discovery mode '{}': expected one of {}",
            other,
            registered_names().join(", ")
        ))),
    }
}

/// 直接请求-响应协议类型，用于点对点通信。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DirectRequest {
    /// 直接将任务完成结果交付给协调节点。
    TaskResult {
        workflow_id: WorkflowId,
        task_id: TaskId,
        task_name: String,
        outcome: WireTaskOutcome,
        worker_node: NodeId,
    },
    /// 直接将任务分发给特定工作节点。
    DispatchTask {
        task: crate::common::model::TaskDefinition,
    },
    /// 从协调节点查询工作流状态。
    QueryWorkflowState {
        workflow_id: WorkflowId,
        requesting_node: NodeId,
    },
}

/// 直接响应-请求协议类型，用于点对点通信。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DirectResponse {
    /// 任务结果交付的确认。
    TaskResultAck { accepted: bool },
    /// 任务分发的确认。
    DispatchAck { accepted: bool },
    /// 工作流状态查询的响应。
    WorkflowState {
        dag: Option<Vec<u8>>,
        execution: Option<Vec<u8>>,
        pending: Option<Vec<u8>>,
    },
    /// 服务端无法投递或处理请求时返回的错误响应。
    ///
    /// 例如：EventBus 上无订阅者接收 DirectRequest 时，会通过此变体
    /// 主动通知调用方，避免其永久阻塞在 `await` 上等待响应。
    Error {
        /// 人类可读的错误描述。
        message: String,
    },
}

/// Actant 直连请求-响应协议的 ALPN。
const ACTANT_DIRECT_ALPN: &[u8] = b"actant/direct/1";

/// 帧长度前缀：4 字节（u32 大端序）。
const LEN_PREFIX_SIZE: usize = 4;

#[derive(Clone)]
/// 基于 iroh 的 [`Transport`] 实现。
///
/// `NetworkManager` 同时维护 gossip 订阅和直连协议。gossip 用于 topic 广播
/// （任务分发、心跳、DAG 状态、取消），直连协议用于需要确认的请求响应
/// （远端任务结果投递、Actor call 等）。
pub struct NetworkManager {
    endpoint: Endpoint,
    gossip: Gossip,
    router: Router,
    event_tx: mpsc::Sender<NetworkEvent>,
    event_rx: Arc<Mutex<mpsc::Receiver<NetworkEvent>>>,
    node_id: NodeId,
    endpoint_id_str: String,
    topic_subscriptions: Arc<RwLock<HashMap<String, (TopicId, GossipSender)>>>,
    peer_neighbor_count: Arc<RwLock<HashMap<String, usize>>>,
    gossip_bootstrap_peers: Arc<Vec<EndpointId>>,
    max_message_size: usize,
    allowed_peer_ids: Arc<HashSet<String>>,
    direct_request_timeout: Duration,
    /// blob 原语 facade；`None` = 未启用（`blob_store` 返回明确错误）。
    blobs: Option<Arc<BlobStore>>,
}

impl NetworkManager {
    /// 创建 iroh endpoint、gossip router 和 direct request 处理循环。
    ///
    /// 不启用 blob 原语；生产装配走 [`Self::with_blob_store`]（RuntimeBuilder）。
    ///
    /// # Errors
    ///
    /// 如果配置校验失败、发现策略未知、endpoint bind 失败、router 启动失败，
    /// 或 bootstrap peer 解析失败，返回错误。
    pub async fn new(node_id: NodeId, config: NetworkConfig) -> crate::common::Result<Self> {
        Self::build(node_id, config, None).await
    }

    /// 创建启用了 blob 原语的 [`NetworkManager`]。
    ///
    /// blob 存储由调用方打开（生产装配为 `data_dir/blobs` 下的 FsStore），
    /// 并与 gossip / 直连协议在同一个 Router 上 accept `iroh_blobs::ALPN`。
    ///
    /// # Errors
    ///
    /// 同 [`Self::new`]。
    pub async fn with_blob_store(
        node_id: NodeId,
        config: NetworkConfig,
        blobs: Arc<BlobStore>,
    ) -> crate::common::Result<Self> {
        Self::build(node_id, config, Some(blobs)).await
    }

    async fn build(
        node_id: NodeId,
        config: NetworkConfig,
        blobs: Option<Arc<BlobStore>>,
    ) -> crate::common::Result<Self> {
        let _span = tracing::info_span!("network.new", node = %node_id).entered();
        tracing::info!(
            discovery_mode = %config.discovery_mode.as_str(),
            listen_port = config.listen_port,
            "network.new: enter"
        );
        let discovery = discovery_from_name(config.discovery_mode.as_str(), &config)?;
        tracing::info!("network.new: discovery resolved");

        let builder = Endpoint::builder(iroh::endpoint::presets::Minimal);
        let builder = discovery.apply(builder);
        tracing::info!("network.new: discovery applied");

        let builder = if config.listen_port != 0 || !config.listen_ip.is_empty() {
            let ip: std::net::Ipv4Addr = if config.listen_ip.is_empty() {
                std::net::Ipv4Addr::UNSPECIFIED
            } else {
                config
                    .listen_ip
                    .parse()
                    .map_err(|e: std::net::AddrParseError| {
                        ActantError::Config(format!(
                            "invalid listen_ip '{}': {}",
                            config.listen_ip, e
                        ))
                    })?
            };
            let port = if config.listen_port == 0 {
                0
            } else {
                config.listen_port
            };
            let addr = std::net::SocketAddrV4::new(ip, port);
            builder
                .bind_addr(addr)
                .map_err(|e| ActantError::Config(format!("invalid bind address {}: {}", addr, e)))?
        } else {
            builder
        };
        tracing::info!("network.new: calling endpoint.bind()");
        let endpoint = builder
            .bind()
            .await
            .map_err(|e| ActantError::Network(format!("failed to bind endpoint: {e}")))?;
        tracing::info!("network.new: endpoint.bind() returned");

        let endpoint_id = endpoint.id();
        let endpoint_id_str = endpoint_id.to_string();

        tracing::info!("iroh endpoint created: endpoint_id={}", endpoint_id_str);

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let allowed_peer_ids = Arc::new(build_allowed_peer_ids(&config.allowed_peer_ids));
        let max_message_size = config.max_message_size;
        let event_channel_capacity = config.event_channel_capacity;

        let (direct_event_tx, direct_event_rx) = mpsc::channel(event_channel_capacity);
        let direct_handler = Arc::new(DirectProtocolHandler {
            event_tx: direct_event_tx,
            allowed_peer_ids: allowed_peer_ids.clone(),
            semaphore: Arc::new(tokio::sync::Semaphore::new(
                config.max_pending_direct_requests,
            )),
            max_message_size,
        });

        // blob 原语（0.3.2 R1）：存储由调用方随 data_dir 打开；未传入的
        // 装配路径（直连 `new`，如纯嵌入/测试）不启用，blob_store 返回明确错误。
        tracing::info!("network.new: spawning router");
        let mut router_builder = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(ACTANT_DIRECT_ALPN, direct_handler.clone());
        router_builder = match &blobs {
            Some(store) => router_builder.accept(iroh_blobs::ALPN, store.protocol_handler()),
            None => router_builder,
        };
        let router = router_builder.spawn();
        tracing::info!("network.new: router spawned");

        if !config.bootstrap_nodes.is_empty()
            && config.discovery_mode.as_str() == discovery_mode::LOCAL
        {
            tracing::info!("network.new: calling endpoint.online() (LOCAL + bootstrap)");
            endpoint.online().await;
            tracing::info!("iroh endpoint online: endpoint_id={}", endpoint_id_str);
        }

        for node_addr in &config.bootstrap_nodes {
            match parse_endpoint_addr(node_addr) {
                Ok(addr) => {
                    tracing::info!("connecting to bootstrap node: {:?}", addr);
                    if let Err(e) = endpoint.connect(addr, ACTANT_DIRECT_ALPN).await {
                        tracing::warn!("failed to connect to bootstrap node: {}", e);
                    }
                }
                Err(e) => {
                    tracing::warn!("invalid bootstrap node address '{}': {}", node_addr, e);
                }
            }
        }

        let gossip_bootstrap_peers: Vec<EndpointId> = config
            .gossip_bootstrap_peers
            .iter()
            .filter_map(|s| match s.parse::<EndpointId>() {
                Ok(id) => Some(id),
                Err(e) => {
                    tracing::warn!("invalid gossip_bootstrap_peer '{}': {}", s, e);
                    None
                }
            })
            .collect();

        let (event_tx, event_rx) = mpsc::channel(config.event_channel_capacity);

        let max_message_size = config.max_message_size;
        let direct_request_timeout = Duration::from_millis(config.direct_request_timeout_ms);

        let manager = Self {
            endpoint,
            gossip,
            router,
            event_tx,
            event_rx: Arc::new(Mutex::new(event_rx)),
            node_id,
            endpoint_id_str,
            topic_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            peer_neighbor_count: Arc::new(RwLock::new(HashMap::new())),
            gossip_bootstrap_peers: Arc::new(gossip_bootstrap_peers),
            max_message_size,
            allowed_peer_ids,
            direct_request_timeout,
            blobs,
        };

        manager.spawn_direct_event_loop(direct_event_rx);

        Ok(manager)
    }

    /// 向已订阅 topic 广播 gossip 消息。
    ///
    /// # Errors
    ///
    /// 如果 topic 未订阅或底层 gossip 广播失败，返回错误。
    pub async fn broadcast(&self, topic: &str, data: Vec<u8>) -> crate::common::Result<()> {
        let subs = self.topic_subscriptions.read().await;
        if let Some((_topic_id, sender)) = subs.get(topic) {
            sender
                .broadcast(Bytes::from(data))
                .await
                .map_err(|e| ActantError::Network(format!("gossip broadcast failed: {e}")))?;
            Ok(())
        } else {
            Err(ActantError::Network(format!(
                "not subscribed to topic '{topic}'"
            )))
        }
    }

    /// 订阅 gossip topic 并启动对应的接收任务。
    ///
    /// 重复订阅同一 topic 是 no-op。检查与插入在同一把写锁内完成，
    /// 并发订阅同一 topic 时严格只执行一次 gossip.subscribe，不会产生
    /// 被覆盖插入后泄漏的 sender/receiver。
    ///
    /// # Errors
    ///
    /// 如果 iroh gossip 订阅失败，返回错误。
    pub async fn subscribe(&self, topic: &str) -> crate::common::Result<()> {
        // 写锁覆盖整个 check-then-insert：iroh gossip.subscribe 内部不回调
        // topic_subscriptions，持锁跨该 await 不会构成环，仅串行化并发订阅。
        let mut subs = self.topic_subscriptions.write().await;
        if subs.contains_key(topic) {
            return Ok(());
        }

        let topic_id = topic_id_from_str(topic);

        let bootstrap: Vec<EndpointId> = self.gossip_bootstrap_peers.to_vec();
        let (sender, mut receiver) = self
            .gossip
            .subscribe(topic_id, bootstrap)
            .await
            .map_err(|e| ActantError::Network(format!("gossip subscribe failed: {e}")))?
            .split();

        subs.insert(topic.to_string(), (topic_id, sender));
        drop(subs);

        let event_tx = self.event_tx.clone();
        let topic_name = topic.to_string();
        let peer_counts = self.peer_neighbor_count.clone();
        let max_gossip_msg_size = self.max_message_size;
        let allowed_peer_ids = self.allowed_peer_ids.clone();
        tokio::spawn(async move {
            while let Some(result) = receiver.next().await {
                match handle_gossip_event(
                    result,
                    &topic_name,
                    &allowed_peer_ids,
                    max_gossip_msg_size,
                ) {
                    GossipEventOutcome::Message(event) => {
                        if let Err(mpsc::error::TrySendError::Full(_)) = event_tx.try_send(event) {
                            tracing::warn!(
                                "network event channel full, dropping gossip message on '{}'",
                                topic_name
                            );
                            crate::metrics::inc_gossip_updates_dropped();
                        }
                    }
                    GossipEventOutcome::PeerConnected(peer_str) => {
                        let mut counts = peer_counts.write().await;
                        let count = counts.entry(peer_str.clone()).or_insert(0);
                        *count += 1;
                        if *count == 1 {
                            drop(counts);
                            if let Err(mpsc::error::TrySendError::Full(_)) =
                                event_tx.try_send(NetworkEvent::PeerConnected { peer_id: peer_str })
                            {
                                tracing::warn!(
                                    "network event channel full, dropping peer connected event"
                                );
                            }
                        }
                    }
                    GossipEventOutcome::PeerDisconnected(peer_str) => {
                        let mut counts = peer_counts.write().await;
                        if let Some(count) = counts.get_mut(&peer_str) {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                counts.remove(&peer_str);
                                drop(counts);
                                if let Err(mpsc::error::TrySendError::Full(_)) = event_tx
                                    .try_send(NetworkEvent::PeerDisconnected { peer_id: peer_str })
                                {
                                    tracing::warn!(
                                        "network event channel full, dropping peer disconnected event"
                                    );
                                }
                            }
                        }
                    }
                    GossipEventOutcome::None => {}
                }
            }
        });

        Ok(())
    }

    /// 返回当前已发现的 peer。
    pub async fn discover_peers(&self) -> crate::common::Result<Vec<PeerId>> {
        let counts = self.peer_neighbor_count.read().await;
        let peers = counts.keys().map(|id| PeerId(id.clone())).collect();
        Ok(peers)
    }

    /// 接收下一条网络事件。
    ///
    /// 实现持 `tokio::Mutex` 跨 `recv().await`，串行化事件消费。
    /// 当前为单消费者模型（Worker 仅启动一个 `start_network_event_loop`），
    /// 故不会成为瓶颈。若未来改为多 worker 共享 `event_rx`，需重构为
    /// `broadcast`/`mpsc` 分发，否则多消费者会在此互斥锁上排队。
    pub async fn recv_event(&self) -> Option<NetworkEvent> {
        let mut guard = self.event_rx.lock().await;
        guard.recv().await
    }

    /// 连接 endpoint address，并把该 peer 加入所有已订阅 gossip topic。
    ///
    /// # Errors
    ///
    /// 如果 address 解码或 iroh 连接失败，返回错误。
    pub async fn dial(&self, addr: &str) -> crate::common::Result<()> {
        let endpoint_addr = parse_endpoint_addr(addr)
            .map_err(|e| ActantError::Network(format!("invalid endpoint address: {e}")))?;
        let conn = self
            .endpoint
            .connect(endpoint_addr.clone(), ACTANT_DIRECT_ALPN)
            .await
            .map_err(|e| ActantError::Network(format!("dial failed: {e}")))?;
        let remote_id = conn.remote_id();
        tracing::info!(
            "connected to {:?} (endpoint_id={})",
            endpoint_addr,
            remote_id
        );

        self.join_gossip_peer(remote_id).await;

        Ok(())
    }

    /// 手动把 peer id 加入所有已订阅 gossip topic。
    ///
    /// # Errors
    ///
    /// 如果 peer id 不是合法 endpoint id，返回错误。
    pub async fn add_gossip_peer(&self, peer_id_str: &str) -> crate::common::Result<()> {
        let peer_id: EndpointId = peer_id_str.parse().map_err(|e| {
            ActantError::Network(format!("invalid endpoint id '{peer_id_str}': {e}"))
        })?;
        self.join_gossip_peer(peer_id).await;
        Ok(())
    }

    async fn join_gossip_peer(&self, peer_id: EndpointId) {
        let subs = self.topic_subscriptions.read().await;
        if subs.is_empty() {
            return;
        }
        let peer_str = peer_id.to_string();
        for (topic_name, (_topic_id, sender)) in subs.iter() {
            match sender.join_peers(vec![peer_id]).await {
                Ok(()) => {
                    tracing::debug!(
                        "joined peer {} into gossip topic '{}'",
                        peer_str,
                        topic_name
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to join peer {} into gossip topic '{}': {}",
                        peer_str,
                        topic_name,
                        e
                    );
                }
            }
        }
    }

    /// 返回 endpoint id、relay URL、direct addresses 和 hex 编码 endpoint address。
    ///
    /// # Errors
    ///
    /// 如果 endpoint address 不能序列化，返回错误。
    pub fn listen_addresses(&self) -> crate::common::Result<ListenAddresses> {
        let addr = self.endpoint.addr();
        let endpoint_id = addr.id.to_string();
        let relay_url = addr.addrs.iter().find_map(|a| match a {
            TransportAddr::Relay(url) => Some(url.to_string()),
            _ => None,
        });
        let direct_addrs: Vec<String> = addr
            .addrs
            .iter()
            .filter_map(|a| match a {
                TransportAddr::Ip(sock) => Some(sock.to_string()),
                _ => None,
            })
            .collect();
        let endpoint_addr = data_encoding::HEXLOWER.encode(
            &postcard::to_allocvec(&addr).map_err(|e| ActantError::Serialization(e.to_string()))?,
        );
        Ok(ListenAddresses {
            endpoint_id,
            relay_url,
            direct_addrs,
            endpoint_addr,
        })
    }

    /// 返回本 Actant 节点 ID。
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// blob 存储 facade 句柄；未启用时返回 `None`。
    pub fn blobs(&self) -> Option<&Arc<BlobStore>> {
        self.blobs.as_ref()
    }

    /// 将数据写入本地 blob 存储，返回内容寻址 hash（blake3 32 字节）。
    ///
    /// # Errors
    ///
    /// 节点未启用 blob 存储（直连 [`Self::new`] 构造）时返回
    /// [`ActantError::Config`]；存储写入失败时返回 [`ActantError::Storage`]。
    pub async fn blob_store(&self, data: Vec<u8>) -> crate::common::Result<BlobHash> {
        let Some(blobs) = self.blobs.as_ref() else {
            return Err(ActantError::Config(
                "blob store not enabled on this node: construct NetworkManager with \
                 with_blob_store (RuntimeBuilder does this when data_dir is set)"
                    .into(),
            ));
        };
        blobs.store(data).await
    }

    /// 从指定节点流式拉取 blob，返回逐块（≤16KiB leaf，已校验）读取句柄。
    ///
    /// 取消语义：drop 句柄或显式 [`BlobFetch::close`] 都会立即关闭底层连接。
    ///
    /// # Errors
    ///
    /// 节点地址无效或不可达（[`ActantError::Network`]）、hash 不存在
    /// （[`ActantError::NotFound`]）时返回错误。
    pub async fn blob_fetch(
        &self,
        node: &NodeId,
        hash: BlobHash,
    ) -> crate::common::Result<BlobFetch> {
        let addr = parse_endpoint_addr(node.as_str()).map_err(|e| {
            ActantError::Network(format!("invalid blob source node '{}': {e}", node.as_str()))
        })?;
        BlobFetch::start(&self.endpoint, addr, hash).await
    }

    /// 发送一次直连请求并等待响应。
    ///
    /// # Errors
    ///
    /// 如果 peer address 无效、连接失败、请求或响应序列化失败、读取超过大小上限、
    /// 或超过 `direct_request_timeout`，返回错误。
    #[tracing::instrument(level = "debug", skip(self, request), fields(peer = %peer_id_str))]
    pub async fn send_direct_request(
        &self,
        peer_id_str: &str,
        request: DirectRequest,
    ) -> crate::common::Result<DirectResponse> {
        let t0 = std::time::Instant::now();
        let endpoint_addr = parse_endpoint_addr(peer_id_str).map_err(|e| {
            ActantError::Network(format!("invalid peer address '{peer_id_str}': {e}"))
        })?;

        let timeout = self.direct_request_timeout;
        let peer_for_timeout = peer_id_str.to_string();
        let max_message_size = self.max_message_size;
        let endpoint = self.endpoint.clone();

        let result = tokio::time::timeout(timeout, async {
            let conn = endpoint
                .connect(endpoint_addr, ACTANT_DIRECT_ALPN)
                .await
                .map_err(|e| {
                    ActantError::Network(format!("connect to {peer_id_str} failed: {e}"))
                })?;

            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| ActantError::Network(format!("open_bi failed: {e}")))?;

            let request_bytes = postcard::to_allocvec(&request)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            write_length_prefixed(&mut send, &request_bytes).await?;

            send.finish()
                .map_err(|e| ActantError::Network(format!("finish send stream: {e}")))?;

            let response_bytes = read_length_prefixed(&mut recv, max_message_size).await?;
            let response: DirectResponse = crate::common::decode_postcard(&response_bytes)?;

            Ok::<_, ActantError>(response)
        })
        .await;

        match result {
            Ok(inner_result) => {
                let response = inner_result?;
                let total = t0.elapsed();
                crate::metrics::observe_direct_request_ms(total.as_millis() as u64);
                Ok(response)
            }
            Err(_) => {
                tracing::warn!(
                    peer = %peer_for_timeout,
                    timeout = ?timeout,
                    "direct request timed out"
                );
                Err(ActantError::Timeout(format!(
                    "direct request to {peer_for_timeout} timed out after {:?}",
                    timeout
                )))
            }
        }
    }

    /// 回复一次直连请求。
    ///
    /// # Errors
    ///
    /// 如果 response channel 写入失败，返回错误。
    pub async fn send_direct_response(
        &self,
        channel: DirectResponseChannel,
        response: DirectResponse,
    ) -> crate::common::Result<()> {
        channel.send_response(response).await
    }

    /// 关闭 router 和 iroh endpoint。
    ///
    /// # Errors
    ///
    /// 如果 router shutdown 失败，返回错误。
    pub async fn shutdown(&self) -> crate::common::Result<()> {
        self.router
            .shutdown()
            .await
            .map_err(|e| ActantError::Network(format!("failed to shutdown network router: {e}")))?;
        self.endpoint.close().await;
        Ok(())
    }

    fn spawn_direct_event_loop(&self, mut rx: mpsc::Receiver<DirectEvent>) {
        let event_tx = self.event_tx.clone();
        let max_message_size = self.max_message_size;
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                route_direct_request(&event_tx, max_message_size, event).await;
            }
        });
    }
}

#[async_trait::async_trait]
impl Transport for NetworkManager {
    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn local_peer_id(&self) -> &str {
        &self.endpoint_id_str
    }

    async fn broadcast(&self, topic: &str, data: Vec<u8>) -> crate::common::Result<()> {
        self.broadcast(topic, data).await
    }

    async fn subscribe(&self, topic: &str) -> crate::common::Result<()> {
        self.subscribe(topic).await
    }

    async fn recv_event(&self) -> Option<NetworkEvent> {
        self.recv_event().await
    }

    async fn dial(&self, addr: &str) -> crate::common::Result<()> {
        self.dial(addr).await
    }

    async fn add_gossip_peer(&self, peer_id: &str) -> crate::common::Result<()> {
        self.add_gossip_peer(peer_id).await
    }

    fn listen_addresses(&self) -> crate::common::Result<ListenAddresses> {
        self.listen_addresses()
    }

    async fn send_direct_request(
        &self,
        peer_id_str: &str,
        request: DirectRequest,
    ) -> crate::common::Result<DirectResponse> {
        self.send_direct_request(peer_id_str, request).await
    }

    async fn send_direct_response(
        &self,
        channel: DirectResponseChannel,
        response: DirectResponse,
    ) -> crate::common::Result<()> {
        self.send_direct_response(channel, response).await
    }

    async fn discover_peers(&self) -> crate::common::Result<Vec<PeerId>> {
        self.discover_peers().await
    }

    async fn shutdown(&self) -> crate::common::Result<()> {
        self.shutdown().await
    }
}

#[derive(Debug)]
enum DirectEvent {
    Request {
        peer_id: String,
        request: DirectRequest,
        channel: DirectResponseChannel,
    },
}

/// [`DirectEvent`] 路由结果，供单元测试断言丢弃分支（不依赖真实 iroh 流）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectRouteOutcome {
    /// 请求已转发到 network event channel。
    Forwarded,
    /// 请求超尺寸被拒绝。
    RejectedOversize,
    /// network event channel 已满，请求被丢弃。
    ChannelFull,
}

/// 将单个直连请求路由到 network event channel。
///
/// 两条丢弃路径（超尺寸、channel 满）都会在 [`DirectResponseChannel`] 仍可写时
/// 回送 `DirectResponse::Error`，让对端**快速失败**而非阻塞等待自身
/// `direct_request_timeout`（默认 30s）。回送失败仅记录日志——此时对端只能
/// 依赖自身超时，与 [`DirectResponseChannel::send_error`] 的契约一致。
async fn route_direct_request(
    event_tx: &mpsc::Sender<NetworkEvent>,
    max_message_size: usize,
    event: DirectEvent,
) -> DirectRouteOutcome {
    let DirectEvent::Request {
        peer_id,
        request,
        channel,
    } = event;
    let request_size = postcard::to_allocvec(&request)
        .map(|v| v.len())
        .unwrap_or(0);
    if request_size > max_message_size {
        tracing::warn!(
            "direct request from {} exceeds size limit: {} > {}, rejecting with error response",
            peer_id,
            request_size,
            max_message_size
        );
        channel
            .send_error(format!(
                "direct request rejected: size {request_size} exceeds limit {max_message_size}"
            ))
            .await;
        return DirectRouteOutcome::RejectedOversize;
    }
    let event = NetworkEvent::DirectRequest {
        peer_id,
        request: Box::new(request),
        channel,
    };
    if let Err(mpsc::error::TrySendError::Full(event)) = event_tx.try_send(event) {
        tracing::warn!("network event channel full, rejecting direct request with error response");
        // try_send 满时把事件（连同 channel 所有权）原样返回，借此收回 channel 回错。
        if let NetworkEvent::DirectRequest { channel, .. } = event {
            channel
                .send_error("node busy: network event channel full")
                .await;
        }
        return DirectRouteOutcome::ChannelFull;
    }
    DirectRouteOutcome::Forwarded
}

/// Gossip 事件处理结果，将 ``subscribe`` 中的后台循环逻辑抽出为可单元测试的纯函数。
#[derive(Debug)]
enum GossipEventOutcome {
    /// 应转发为 ``NetworkEvent::Message`` 的消息事件。
    Message(NetworkEvent),
    /// 有 peer 加入该 topic，需更新邻居计数并可能触发 ``PeerConnected``。
    PeerConnected(String),
    /// 有 peer 离开该 topic，需更新邻居计数并可能触发 ``PeerDisconnected``。
    PeerDisconnected(String),
    /// 无需进一步处理的事件（已被丢弃、Lagged、错误等）。
    None,
}

/// 将单个 gossip 接收结果转换为可处理的事件 outcome。
///
/// 负责 peer allowlist、消息大小检查、邻居计数语义，便于在测试中直接驱动
/// 而不必构造真实 iroh gossip receiver。
fn handle_gossip_event<E: std::fmt::Display>(
    result: Result<Event, E>,
    topic_name: &str,
    allowed_peer_ids: &HashSet<String>,
    max_gossip_msg_size: usize,
) -> GossipEventOutcome {
    match result {
        Ok(Event::Received(msg)) => {
            if !peer_allowed(allowed_peer_ids, &msg.delivered_from.to_string()) {
                tracing::warn!(
                    "gossip message on '{}' from {} rejected: peer not in allowed_peer_ids",
                    topic_name,
                    msg.delivered_from,
                );
                crate::metrics::inc_gossip_updates_dropped();
                return GossipEventOutcome::None;
            }
            let content_len = msg.content.len();
            if content_len > max_gossip_msg_size {
                tracing::warn!(
                    "gossip message on '{}' from {} exceeds size limit: {} > {}, dropping",
                    topic_name,
                    msg.delivered_from,
                    content_len,
                    max_gossip_msg_size,
                );
                crate::metrics::inc_gossip_updates_dropped();
                return GossipEventOutcome::None;
            }
            GossipEventOutcome::Message(NetworkEvent::Message(NetworkMessage {
                topic: topic_name.to_string(),
                data: msg.content.to_vec(),
            }))
        }
        Ok(Event::NeighborUp(peer_id)) => {
            tracing::debug!("gossip neighbor up on '{}': {}", topic_name, peer_id);
            GossipEventOutcome::PeerConnected(peer_id.to_string())
        }
        Ok(Event::NeighborDown(peer_id)) => {
            tracing::debug!("gossip neighbor down on '{}': {}", topic_name, peer_id);
            GossipEventOutcome::PeerDisconnected(peer_id.to_string())
        }
        Ok(Event::Lagged) => {
            tracing::warn!("gossip lagged on topic '{}'", topic_name);
            GossipEventOutcome::None
        }
        Err(e) => {
            tracing::warn!("gossip error on '{}': {}", topic_name, e);
            GossipEventOutcome::None
        }
    }
}

#[derive(Debug, Clone)]
struct DirectProtocolHandler {
    event_tx: mpsc::Sender<DirectEvent>,
    allowed_peer_ids: Arc<HashSet<String>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    max_message_size: usize,
}

impl ProtocolHandler for DirectProtocolHandler {
    fn accept(
        &self,
        conn: Connection,
    ) -> impl std::future::Future<Output = Result<(), AcceptError>> + Send {
        let this = Arc::new(self.clone());
        async move {
            this.handle_connection(conn)
                .await
                .map_err(AcceptError::from_err)
        }
    }

    async fn shutdown(&self) {}
}

impl DirectProtocolHandler {
    async fn handle_connection(self: Arc<Self>, conn: Connection) -> crate::common::Result<()> {
        let remote_id = conn.remote_id().to_string();

        if !peer_allowed(&self.allowed_peer_ids, &remote_id) {
            return Err(ActantError::Network(format!(
                "peer {} not in allowed_peer_ids",
                remote_id
            )));
        }

        loop {
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| ActantError::Internal(format!("semaphore closed: {e}")))?;

            let (send, mut recv) = conn
                .accept_bi()
                .await
                .map_err(|e| ActantError::Network(format!("accept_bi failed: {e}")))?;

            let request_bytes = match read_length_prefixed(&mut recv, self.max_message_size).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!("failed to read direct request from {}: {}", remote_id, e);
                    // 连接尚可写时回送错误，让对端快速失败；发送失败仅记录日志
                    //（连接本身已损坏时对端只能依赖自身超时）。
                    DirectResponseChannel::new(send)
                        .send_error(format!("failed to read direct request: {e}"))
                        .await;
                    continue;
                }
            };

            let request: DirectRequest = match crate::common::decode_postcard(&request_bytes) {
                Ok(req) => req,
                Err(e) => {
                    tracing::warn!(
                        "failed to deserialize direct request from {}: {}",
                        remote_id,
                        e
                    );
                    DirectResponseChannel::new(send)
                        .send_error(format!("failed to decode direct request: {e}"))
                        .await;
                    continue;
                }
            };

            let channel = DirectResponseChannel::new(send);
            if let Err(mpsc::error::TrySendError::Full(event)) =
                self.event_tx.try_send(DirectEvent::Request {
                    peer_id: remote_id.clone(),
                    request,
                    channel,
                })
            {
                tracing::warn!(
                    "direct event channel full, rejecting request from {} with error response",
                    remote_id
                );
                // try_send 满时原样返回事件，收回 channel 回错，避免对端等超时。
                // DirectEvent 当前仅 Request 一个变体，直接解构。
                let DirectEvent::Request { channel, .. } = event;
                channel
                    .send_error("node busy: direct event channel full")
                    .await;
            }

            drop(permit);
            drop(recv);
        }
    }
}

fn build_allowed_peer_ids(peers: &[String]) -> HashSet<String> {
    peers
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn peer_allowed(allowed: &HashSet<String>, peer_id: &str) -> bool {
    allowed.is_empty() || allowed.contains(peer_id.trim())
}

fn topic_id_from_str(topic: &str) -> TopicId {
    let hash = blake3::hash(topic.as_bytes());
    TopicId::from(*hash.as_bytes())
}

fn parse_endpoint_addr(s: &str) -> crate::common::Result<EndpointAddr> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(ActantError::Config("empty endpoint address".into()));
    }

    // Hex-encoded postcard, produced by endpoint_addr_str().
    if let Ok(bytes) = data_encoding::HEXLOWER.decode(trimmed.as_bytes()) {
        // 配置输入：用 decode_postcard 校验大小，避免巨大 hex 字符串触发 OOM。
        if let Ok(addr) = crate::common::decode_postcard(&bytes) {
            return Ok(addr);
        }
    }

    // node_id@ip:port shorthand used in tests and bootstrap configs.
    if let Some((node_part, addr_part)) = trimmed.split_once('@') {
        let node_id: EndpointId = node_part.parse().map_err(|e: KeyParsingError| {
            ActantError::Config(format!("invalid endpoint id '{node_part}': {e}"))
        })?;
        let socket_addr: std::net::SocketAddr = addr_part.parse().map_err(|e| {
            ActantError::Config(format!("invalid socket address '{addr_part}': {e}"))
        })?;
        return Ok(EndpointAddr::new(node_id).with_ip_addr(socket_addr));
    }

    Err(ActantError::Config(format!(
        "invalid endpoint address '{}': expected hex-encoded postcard or node_id@ip:port",
        s
    )))
}

async fn write_length_prefixed(send: &mut SendStream, data: &[u8]) -> crate::common::Result<()> {
    // 长度前缀用 u32 BE 编码。data.len() > u32::MAX 时显式失败，
    // 否则 `as u32` 会静默截断，使对端按截断后的长度读取导致协议失步。
    let len = u32::try_from(data.len()).map_err(|_| {
        ActantError::Network(format!(
            "write_length_prefixed: data len {} exceeds u32::MAX",
            data.len()
        ))
    })?;
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| ActantError::Network(format!("write length prefix: {e}")))?;
    send.write_all(data)
        .await
        .map_err(|e| ActantError::Network(format!("write payload: {e}")))?;
    Ok(())
}

async fn read_length_prefixed(
    recv: &mut RecvStream,
    max_size: usize,
) -> crate::common::Result<Vec<u8>> {
    let mut len_buf = [0u8; LEN_PREFIX_SIZE];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| ActantError::Network(format!("read length prefix: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > max_size {
        return Err(ActantError::Network(format!(
            "message frame too large: {} > {}",
            len, max_size
        )));
    }

    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| ActantError::Network(format!("read payload: {e}")))?;
    Ok(buf)
}

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/network.rs"]
mod tests;
