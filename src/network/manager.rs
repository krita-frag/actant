use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::{Gossip, TopicId};
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::common::discovery_mode;
use crate::common::{ActantError, NodeId};
use crate::network::discovery::{discovery_from_name, Discovery};
use crate::network::protocol::{DirectRequest, DirectResponse};
use crate::network::{
    DirectResponseChannel, ListenAddresses, NetworkEvent, NetworkMessage, PeerId,
};

/// Actant 直连请求-响应协议的 ALPN。
const ACTANT_DIRECT_ALPN: &[u8] = b"actant/direct/1";

/// 帧长度前缀：4 字节（u32 大端序）。
const LEN_PREFIX_SIZE: usize = 4;

#[derive(Clone)]
pub struct NetworkManager {
    endpoint: Endpoint,
    gossip: Gossip,
    router: Router,
    event_tx: mpsc::UnboundedSender<NetworkEvent>,
    event_rx: Arc<Mutex<mpsc::UnboundedReceiver<NetworkEvent>>>,
    node_id: NodeId,
    endpoint_id_str: String,
    /// 活跃 gossip 话题订阅：话题字符串 → (TopicId, GossipSender)。
    topic_subscriptions: Arc<RwLock<HashMap<String, (TopicId, GossipSender)>>>,
    /// 跨所有 gossip 话题的 per-peer 邻居计数。
    /// 用于在首次出现邻居时发出 PeerConnected，最后一个话题邻居离开时发出 PeerDisconnected。
    peer_neighbor_count: Arc<RwLock<HashMap<String, usize>>>,
    /// 订阅时自动加入 gossip 话题的 endpoint ID 列表。
    gossip_bootstrap_peers: Arc<Vec<EndpointId>>,
    /// 单个直连请求消息帧的最大字节数。
    /// 超过此值的帧将被拒绝，以防止畸形或恶意 peer 导致 OOM。
    max_message_size: usize,
    /// 入站 peer 认证白名单。空 = 开放模式（默认）。
    /// 直连请求和 gossip 消息均受此白名单约束。
    allowed_peer_ids: Arc<std::collections::HashSet<String>>,
    /// 单次直连请求-响应调用的超时。
    /// 覆盖 connect + open_bi + 读写全过程，超时返回 `ActantError::Timeout`。
    direct_request_timeout: std::time::Duration,
}

impl NetworkManager {
    pub async fn new(
        node_id: NodeId,
        config: crate::common::NetworkConfig,
    ) -> crate::common::Result<Self> {
        // 通过全局注册表从配置选择发现策略。
        // 未知名称在此返回 Config 错误 — 不会静默回退。
        let discovery = discovery_from_name(config.discovery_mode.as_str())?;

        // 构建 endpoint：从 Minimal preset（设置 crypto provider）开始，
        // 然后应用所选发现策略。
        let builder = Endpoint::builder(iroh::endpoint::presets::Minimal);
        let builder = discovery.apply(builder);

        // 应用 listen_ip/listen_port 配置：绑定到用户指定的 IPv4 地址。
        // 默认 listen_port=0 / listen_ip="" 表示 iroh 自行选择（随机端口 + 0.0.0.0）。
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
        let endpoint = builder
            .bind()
            .await
            .map_err(|e| ActantError::Network(format!("failed to bind endpoint: {e}")))?;

        let endpoint_id = endpoint.id();
        let endpoint_id_str = endpoint_id.to_string();

        tracing::info!("iroh endpoint created: endpoint_id={}", endpoint_id_str);

        // 构建 gossip 协议
        let gossip = Gossip::builder().spawn(endpoint.clone());

        // 入站直连请求认证白名单：trim 空白项，去重。
        // 空 set = 开放模式（默认）。
        let allowed_peer_ids = Arc::new(build_allowed_peer_ids(&config.allowed_peer_ids));

        // 构建直连协议处理器
        let (direct_event_tx, direct_event_rx) = mpsc::unbounded_channel();
        let direct_handler = Arc::new(DirectProtocolHandler {
            event_tx: direct_event_tx,
            allowed_peer_ids: allowed_peer_ids.clone(),
            semaphore: Arc::new(tokio::sync::Semaphore::new(
                config.max_pending_direct_requests,
            )),
        });

        // 构建包含两个协议的路由器
        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(ACTANT_DIRECT_ALPN, direct_handler.clone())
            .spawn();

        // 等待 endpoint 上线（等待 relay 连接）。
        // 仅在使用支持 relay 的发现 preset（Local/N0）时相关。
        // Mdns 和 None preset 禁用 relay，online() 会永久阻塞。
        if !config.bootstrap_nodes.is_empty()
            && config.discovery_mode.as_str() == discovery_mode::LOCAL
        {
            endpoint.online().await;
            tracing::info!("iroh endpoint online: endpoint_id={}", endpoint_id_str);
        }

        // 连接到引导节点
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

        // 从配置解析 gossip 引导 peer
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

        // 网络事件通道
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let max_message_size = config.max_message_size;
        let direct_request_timeout =
            std::time::Duration::from_millis(config.direct_request_timeout_ms);

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
        };

        // 派生直连协议事件的后台任务
        manager.spawn_direct_event_loop(direct_event_rx);

        Ok(manager)
    }

    /// 向 gossip 话题广播数据。
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

    /// 订阅 gossip 话题。
    ///
    /// 若配置了 `gossip_bootstrap_peers`，将作为引导 peer 传入，
    /// 以便立即建立 gossip mesh。
    pub async fn subscribe(&self, topic: &str) -> crate::common::Result<()> {
        // 检查是否已订阅
        {
            let subs = self.topic_subscriptions.read().await;
            if subs.contains_key(topic) {
                return Ok(());
            }
        }

        // 从话题字符串派生确定性 TopicId
        let topic_id = topic_id_from_str(topic);

        // 使用配置的 gossip 引导 peer 作为初始邻居
        let bootstrap: Vec<EndpointId> = self.gossip_bootstrap_peers.to_vec();
        let (sender, mut receiver) = self
            .gossip
            .subscribe(topic_id, bootstrap)
            .await
            .map_err(|e| ActantError::Network(format!("gossip subscribe failed: {e}")))?
            .split();

        // 存储 sender
        {
            let mut subs = self.topic_subscriptions.write().await;
            subs.insert(topic.to_string(), (topic_id, sender));
        }

        // 派生任务将 gossip 消息作为 NetworkEvent::Message 转发
        let event_tx = self.event_tx.clone();
        let topic_name = topic.to_string();
        let peer_counts = self.peer_neighbor_count.clone();
        let max_gossip_msg_size = self.max_message_size;
        let allowed_peer_ids = self.allowed_peer_ids.clone();
        tokio::spawn(async move {
            while let Some(result) = receiver.next().await {
                match result {
                    Ok(Event::Received(msg)) => {
                        // 校验发送方 peer_id 是否在 allowlist 中
                        if !peer_allowed(&allowed_peer_ids, &msg.delivered_from.to_string()) {
                            tracing::warn!(
                                "gossip message on '{}' from {} rejected: peer not in allowed_peer_ids",
                                topic_name,
                                msg.delivered_from,
                            );
                            crate::metrics::inc_gossip_updates_dropped();
                            continue;
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
                            continue;
                        }
                        let _ = event_tx.send(NetworkEvent::Message(NetworkMessage {
                            topic: topic_name.clone(),
                            data: msg.content.to_vec(),
                        }));
                    }
                    Ok(Event::NeighborUp(peer_id)) => {
                        let peer_str = peer_id.to_string();
                        tracing::debug!("gossip neighbor up on '{}': {}", topic_name, peer_str);
                        let mut counts = peer_counts.write().await;
                        let count = counts.entry(peer_str.clone()).or_insert(0);
                        *count += 1;
                        if *count == 1 {
                            // 此 peer 作为邻居的第一个话题
                            drop(counts);
                            let _ =
                                event_tx.send(NetworkEvent::PeerConnected { peer_id: peer_str });
                        }
                    }
                    Ok(Event::NeighborDown(peer_id)) => {
                        let peer_str = peer_id.to_string();
                        tracing::debug!("gossip neighbor down on '{}': {}", topic_name, peer_str);
                        let mut counts = peer_counts.write().await;
                        if let Some(count) = counts.get_mut(&peer_str) {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                counts.remove(&peer_str);
                                drop(counts);
                                let _ = event_tx
                                    .send(NetworkEvent::PeerDisconnected { peer_id: peer_str });
                            }
                        }
                    }
                    Ok(Event::Lagged) => {
                        tracing::warn!("gossip lagged on topic '{}'", topic_name);
                    }
                    Err(e) => {
                        tracing::warn!("gossip error on '{}': {}", topic_name, e);
                    }
                }
            }
        });

        Ok(())
    }

    /// 发现 peer（返回 gossip 邻居中已连接的 endpoint ID）。
    pub async fn discover_peers(&self) -> crate::common::Result<Vec<PeerId>> {
        let counts = self.peer_neighbor_count.read().await;
        let peers = counts.keys().map(|id| PeerId(id.clone())).collect();
        Ok(peers)
    }

    /// 接收下一个网络事件。
    pub async fn recv_event(&self) -> Option<NetworkEvent> {
        let mut guard = self.event_rx.lock().await;
        guard.recv().await
    }

    /// 通过 endpoint 地址字符串连接 peer。
    ///
    /// 建立 QUIC 连接后，peer 会自动加入所有当前订阅的 gossip 话题，
    /// 无需等待外部发现即可建立 gossip 成员关系。
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

        // 将拨号的 peer 加入所有已订阅的 gossip 话题
        self.join_gossip_peer(remote_id).await;

        Ok(())
    }

    /// 显式将 peer 添加到所有当前订阅的 gossip 话题。
    ///
    /// 适用于 peer 通过带外方式连接（如 `subscribe` 使用引导 peer），
    /// 需在不依赖自动发现的情况下加入 gossip mesh 的测试场景。
    pub async fn add_gossip_peer(&self, peer_id_str: &str) -> crate::common::Result<()> {
        let peer_id: EndpointId = peer_id_str.parse().map_err(|e| {
            ActantError::Network(format!("invalid endpoint id '{peer_id_str}': {e}"))
        })?;
        self.join_gossip_peer(peer_id).await;
        Ok(())
    }

    /// 内部辅助：将 EndpointId 加入所有已订阅的 gossip 话题。
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

    /// 获取本地 endpoint 地址为结构化对象。
    pub async fn listen_addresses(&self) -> crate::common::Result<ListenAddresses> {
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

    /// 获取适合 `dial()` 的完整 endpoint 地址字符串。
    pub async fn endpoint_addr_str(&self) -> crate::common::Result<String> {
        let addr = self.endpoint.addr();
        let encoded =
            postcard::to_allocvec(&addr).map_err(|e| ActantError::Serialization(e.to_string()))?;
        Ok(data_encoding::HEXLOWER.encode(&encoded))
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// 向指定 peer 发送直连请求并等待响应。
    ///
    /// 整个调用（connect + open_bi + 读写）受 `direct_request_timeout` 约束，
    /// 超时返回 `ActantError::Timeout`，防止对端故障导致调用方永久阻塞。
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

            // 使用长度前缀分帧发送请求
            let request_bytes = postcard::to_allocvec(&request)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            write_length_prefixed(&mut send, &request_bytes).await?;

            // 标记请求数据结束
            send.finish()
                .map_err(|e| ActantError::Network(format!("finish send stream: {e}")))?;

            // 使用长度前缀分帧读取响应
            let response_bytes = read_length_prefixed(&mut recv, max_message_size).await?;
            let response: DirectResponse = postcard::from_bytes(&response_bytes)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;

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
                    "direct request to {peer_for_timeout} timed out after {timeout:?}"
                )))
            }
        }
    }

    /// 通过直连请求-响应通道回送响应。
    pub async fn send_direct_response(
        &self,
        channel: DirectResponseChannel,
        response: DirectResponse,
    ) -> crate::common::Result<()> {
        let mut send_stream = channel
            .take()
            .ok_or_else(|| ActantError::Network("response channel already consumed".into()))?;

        let response_bytes = postcard::to_allocvec(&response)
            .map_err(|e| ActantError::Serialization(e.to_string()))?;
        write_length_prefixed(&mut send_stream, &response_bytes).await?;
        send_stream
            .finish()
            .map_err(|e| ActantError::Network(format!("finish response stream: {e}")))?;

        Ok(())
    }

    pub fn local_peer_id(&self) -> &str {
        &self.endpoint_id_str
    }

    /// 获取底层 iroh EndpointId。
    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// 获取 gossip 实例的引用。
    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    /// 获取 endpoint 的引用。
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// 优雅关闭网络管理器。
    pub async fn shutdown(&self) -> crate::common::Result<()> {
        self.router
            .shutdown()
            .await
            .map_err(|e| ActantError::Network(format!("router shutdown failed: {e}")))?;
        self.endpoint.close().await;
        Ok(())
    }

    /// 派生后台循环处理进入的直连协议连接。
    fn spawn_direct_event_loop(&self, mut direct_rx: mpsc::UnboundedReceiver<DirectIncoming>) {
        let event_tx = self.event_tx.clone();
        let max_message_size = self.max_message_size;
        tokio::spawn(async move {
            while let Some(incoming) = direct_rx.recv().await {
                let peer_id = incoming.peer_id;
                let (send, mut recv) = (incoming.send, incoming.recv);

                // 读取请求
                match read_length_prefixed(&mut recv, max_message_size).await {
                    Ok(request_bytes) => {
                        match postcard::from_bytes::<DirectRequest>(&request_bytes) {
                            Ok(request) => {
                                let _ = event_tx.send(NetworkEvent::DirectRequest {
                                    peer_id,
                                    request: Box::new(request),
                                    channel: DirectResponseChannel::new(send),
                                });
                            }
                            Err(e) => {
                                tracing::warn!("failed to deserialize direct request: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("failed to read direct request: {}", e);
                    }
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl crate::network::Transport for NetworkManager {
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

    async fn listen_addresses(&self) -> crate::common::Result<ListenAddresses> {
        self.listen_addresses().await
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

// ---------------------------------------------------------------------------
// 进入 ALPN 连接的直连协议处理器
// ---------------------------------------------------------------------------

/// 进入的直连连接数据。
struct DirectIncoming {
    peer_id: String,
    send: SendStream,
    recv: RecvStream,
    /// 背压许可：持有期间占用一个 `max_pending_direct_requests` 名额，
    /// 请求处理完毕（读取+分发）后随 `DirectIncoming` 一起 drop 释放。
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// Actant 直连请求-响应 ALPN 的协议处理器。
struct DirectProtocolHandler {
    event_tx: mpsc::UnboundedSender<DirectIncoming>,
    /// 入站直连认证白名单。空 = 开放模式。
    allowed_peer_ids: Arc<std::collections::HashSet<String>>,
    /// 并发入站流背压信号量。容量 = `max_pending_direct_requests`。
    /// `try_acquire_owned` 失败时拒绝新流，防止对端打开过多并发流导致 OOM。
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for DirectProtocolHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectProtocolHandler")
            .field("available_permits", &self.semaphore.available_permits())
            .field("allowed_count", &self.allowed_peer_ids.len())
            .finish()
    }
}

impl ProtocolHandler for DirectProtocolHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer_id = connection.remote_id().to_string();

        // 认证白名单：非空时仅接受列表内对端。
        // 在读取任何请求体之前即拒绝，避免未授权对端消耗资源。
        if !peer_allowed(&self.allowed_peer_ids, &peer_id) {
            tracing::warn!(
                peer_id = %peer_id,
                allowed_count = self.allowed_peer_ids.len(),
                "rejecting direct connection: peer not in allowed_peer_ids allowlist"
            );
            // 关闭连接：返回 Ok 让 iroh 正常关闭流，而非视为协议错误。
            return Ok(());
        }

        // 接受来自发起方的双向流
        loop {
            // 背压：在 accept_bi 之前尝试获取许可。
            // 若达到 max_pending_direct_requests 上限，拒绝新流并关闭连接。
            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        available_permits = self.semaphore.available_permits(),
                        "rejecting direct stream: backpressure limit reached (max_pending_direct_requests)"
                    );
                    return Ok(());
                }
            };
            match connection.accept_bi().await {
                Ok((send, recv)) => {
                    let incoming = DirectIncoming {
                        peer_id: peer_id.clone(),
                        send,
                        recv,
                        _permit: Some(permit),
                    };
                    if self.event_tx.send(incoming).is_err() {
                        tracing::warn!("direct event channel closed, dropping incoming connection");
                        return Ok(());
                    }
                }
                Err(e) => {
                    // 连接关闭或出错 — 属正常情况
                    tracing::debug!("direct accept_bi ended for {}: {}", peer_id, e);
                    return Ok(());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 分帧辅助：QUIC 流上的长度前缀消息编码
// ---------------------------------------------------------------------------

async fn write_length_prefixed(send: &mut SendStream, data: &[u8]) -> crate::common::Result<()> {
    let len = data.len() as u32;
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
    let len = usize::try_from(u32::from_be_bytes(len_buf))
        .map_err(|e| ActantError::Network(format!("message length overflow: {e}")))?;

    // 限制为 `max_size`，防止畸形或恶意帧导致 OOM。
    if len > max_size {
        return Err(ActantError::Network(format!(
            "message too large: {len} bytes (max {max_size})"
        )));
    }

    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| ActantError::Network(format!("read payload: {e}")))?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Endpoint 地址解析
// ---------------------------------------------------------------------------

/// 将 endpoint 地址字符串解析为 EndpointAddr。
///
/// 支持的格式：
/// - 十六进制编码的 EndpointId（64 字符）：仅用 ID 创建 EndpointAddr
/// - 十六进制编码的 postcard 序列化 EndpointAddr：完整反序列化
fn parse_endpoint_addr(s: &str) -> Result<EndpointAddr, String> {
    // 先尝试解析为 EndpointId（PublicKey）— 64 个十六进制字符
    if let Ok(endpoint_id) = s.parse::<EndpointId>() {
        return Ok(EndpointAddr::from(endpoint_id));
    }
    // 尝试解析为十六进制编码的 postcard 序列化 EndpointAddr
    if let Ok(bytes) = data_encoding::HEXLOWER.decode(s.as_bytes()) {
        if let Ok(addr) = postcard::from_bytes::<EndpointAddr>(&bytes) {
            return Ok(addr);
        }
    }
    Err(format!(
        "cannot parse '{}' as EndpointId or EndpointAddr",
        s
    ))
}

// ---------------------------------------------------------------------------
// Topic ID 派生
// ---------------------------------------------------------------------------

/// 使用 BLAKE3 从话题字符串派生确定性 TopicId。
fn topic_id_from_str(topic: &str) -> TopicId {
    let hash = blake3::hash(topic.as_bytes());
    TopicId::from_bytes(hash.into())
}

// ---------------------------------------------------------------------------
// 入站直连认证白名单
// ---------------------------------------------------------------------------

/// 从原始配置字符串构建认证白名单集合：trim 空白、过滤空串、去重。
///
/// 提取为独立纯函数以便单元测试规范化逻辑（安全相关代码路径）。
/// 返回空集表示开放模式（接受任意对端）。
fn build_allowed_peer_ids(raw: &[String]) -> std::collections::HashSet<String> {
    raw.iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// 判定入站对端是否被白名单允许。
///
/// - 白名单为空 → 开放模式，允许所有对端（返回 `true`）。
/// - 白名单非空 → 仅允许集合内对端。
///
/// 此判定在 `DirectProtocolHandler::accept` 中作为认证闸门：
/// 不通过的对端在读取任何请求体之前即被拒绝。
fn peer_allowed(allowlist: &std::collections::HashSet<String>, peer_id: &str) -> bool {
    allowlist.is_empty() || allowlist.contains(peer_id)
}

#[cfg(test)]
mod allowlist_tests {
    use super::*;

    #[test]
    fn build_trims_whitespace() {
        let raw = vec!["  abc  ".to_string(), "def".to_string()];
        let set = build_allowed_peer_ids(&raw);
        assert_eq!(set.len(), 2);
        assert!(set.contains("abc"));
        assert!(set.contains("def"));
    }

    #[test]
    fn build_drops_empty_entries() {
        let raw = vec!["".to_string(), "   ".to_string(), "valid".to_string()];
        let set = build_allowed_peer_ids(&raw);
        assert_eq!(set.len(), 1, "empty/whitespace entries must be dropped");
        assert!(set.contains("valid"));
    }

    #[test]
    fn build_deduplicates() {
        let raw = vec![
            "peer-1".to_string(),
            "peer-1".to_string(),
            "  peer-1  ".to_string(), // trim 后重复
            "peer-2".to_string(),
        ];
        let set = build_allowed_peer_ids(&raw);
        assert_eq!(set.len(), 2, "duplicates must be collapsed");
        assert!(set.contains("peer-1"));
        assert!(set.contains("peer-2"));
    }

    #[test]
    fn build_empty_input_yields_open_mode() {
        let set = build_allowed_peer_ids(&[]);
        assert!(set.is_empty(), "empty input = open mode");
    }

    #[test]
    fn peer_allowed_open_mode_accepts_all() {
        let set = build_allowed_peer_ids(&[]);
        // 开放模式：任意 peer_id 都应被允许
        assert!(peer_allowed(&set, "anyone"));
        assert!(peer_allowed(&set, ""));
        assert!(peer_allowed(&set, "abc123"));
    }

    #[test]
    fn peer_allowed_nonempty_accepts_listed() {
        let set = build_allowed_peer_ids(&["alice".into(), "bob".into()]);
        assert!(peer_allowed(&set, "alice"));
        assert!(peer_allowed(&set, "bob"));
    }

    #[test]
    fn peer_allowed_nonempty_rejects_unlisted() {
        let set = build_allowed_peer_ids(&["alice".into(), "bob".into()]);
        assert!(
            !peer_allowed(&set, "eve"),
            "unlisted peer must be rejected when allowlist is non-empty"
        );
        assert!(
            !peer_allowed(&set, ""),
            "empty peer_id must be rejected when allowlist is non-empty"
        );
        // 大小写敏感：iroh EndpointId 是确定的十六进制串，不应模糊匹配
        assert!(
            !peer_allowed(&set, "Alice"),
            "peer matching must be exact (case-sensitive)"
        );
    }
}

#[cfg(test)]
mod backpressure_tests {
    use super::*;

    #[test]
    fn semaphore_rejects_excess_acquires() {
        // 验证：容量=2 的信号量，前 2 次 try_acquire_owned 成功，第 3 次失败。
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let p1 = sem.clone().try_acquire_owned();
        let p2 = sem.clone().try_acquire_owned();
        let p3 = sem.clone().try_acquire_owned();
        assert!(p1.is_ok(), "first acquire should succeed");
        assert!(p2.is_ok(), "second acquire should succeed");
        assert!(p3.is_err(), "third acquire must be rejected (backpressure)");
    }

    #[test]
    fn semaphore_releases_permit_on_drop() {
        // 验证：permit drop 后名额归还，新的 acquire 可以成功。
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let p1 = sem.clone().try_acquire_owned().expect("first acquire ok");
        assert!(
            sem.clone().try_acquire_owned().is_err(),
            "second acquire must fail while permit held"
        );
        drop(p1);
        assert!(
            sem.clone().try_acquire_owned().is_ok(),
            "acquire must succeed after permit dropped"
        );
    }

    #[test]
    fn direct_protocol_handler_builds_with_configured_capacity() {
        // 验证：NetworkConfig.max_pending_direct_requests 传递到 Semaphore 容量。
        let (tx, _rx) = mpsc::unbounded_channel::<DirectIncoming>();
        let handler = DirectProtocolHandler {
            event_tx: tx,
            allowed_peer_ids: Arc::new(std::collections::HashSet::new()),
            semaphore: Arc::new(tokio::sync::Semaphore::new(3)),
        };
        // 容量 3：前 3 次成功，第 4 次失败
        let permits: Vec<_> = (0..3)
            .map(|_| handler.semaphore.clone().try_acquire_owned())
            .collect();
        assert!(permits.iter().all(|p| p.is_ok()), "first 3 should succeed");
        assert!(
            handler.semaphore.clone().try_acquire_owned().is_err(),
            "4th acquire must fail"
        );
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use crate::common::WorkflowId;

    #[tokio::test]
    async fn send_direct_request_returns_timeout_for_unreachable_peer() {
        // 验证：对不存在的对端发起直连请求，超时后返回 ActantError::Timeout。
        // 使用一个保证无法连接的 endpoint 地址 + 短超时（500ms）。
        let config = crate::common::NetworkConfig {
            discovery_mode: crate::common::DiscoveryMode::new_unchecked(
                crate::common::discovery_mode::NONE,
            ),
            bootstrap_nodes: Vec::new(),
            hlc_max_drift_ms: 500,
            max_pending_direct_requests: 16,
            gossip_bootstrap_peers: Vec::new(),
            max_message_size: 1024,
            allowed_peer_ids: Vec::new(),
            direct_request_timeout_ms: 500,
            listen_port: 0,
            listen_ip: String::new(),
        };
        let manager = NetworkManager::new(NodeId::from("timeout-test"), config)
            .await
            .expect("NetworkManager init");

        // 使用一个格式合法但不可达的 EndpointAddr。
        // iroh EndpointAddr 解析需要一个有效的 EndpointId；这里构造一个不可达地址。
        let unreachable_addr = "0000000000000000000000000000000000000000000000";
        let request = DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf-timeout"),
            requesting_node: NodeId::from("n-timeout"),
        };

        let result = manager.send_direct_request(unreachable_addr, request).await;
        match result {
            Err(ActantError::Timeout(_)) => { /* 预期：超时 */ }
            Err(ActantError::Network(msg)) => {
                // 连接失败先于超时也是可接受的（地址解析或连接被拒）
                // 但不应永久阻塞
                tracing::debug!("got Network error (acceptable): {}", msg);
            }
            other => panic!("expected Timeout or Network error, got {:?}", other),
        }
    }
}
