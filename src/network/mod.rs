pub mod discovery;
pub(crate) mod manager;
pub(crate) mod protocol;

pub use discovery::{
    discovery_from_name, BoxedDiscovery, Discovery, DnsDiscovery, DnsRecordType, LocalDiscovery,
    MdnsDiscovery, NoDiscovery, RelayDiscovery,
};
pub use manager::NetworkManager;
pub use protocol::{DirectRequest, DirectResponse};

use crate::common::NodeId;

/// `Transport::listen_addresses()` 的结构化结果。
///
/// 包含 endpoint 标识符、可选 relay URL、直连 IP 地址，以及适合 `dial()` 的完整 endpoint 地址字符串。
#[derive(Debug, Clone)]
pub struct ListenAddresses {
    /// endpoint 的公钥标识符（iroh EndpointId，或内存传输的合成 id）。
    pub endpoint_id: String,
    /// 若配置了 relay 服务器，则为其 URL。
    pub relay_url: Option<String>,
    /// endpoint 监听的直连 IP 地址。
    pub direct_addrs: Vec<String>,
    /// 适合 `dial()` 的完整 endpoint 地址字符串。iroh 为十六进制编码的 postcard `EndpointAddr`；
    /// 内存传输为 peer id 字符串本身，由 `dial()` 通过共享 mesh 解析。
    pub endpoint_addr: String,
}

#[derive(Debug, Clone)]
pub struct PeerId(pub String);

#[derive(Debug, Clone)]
pub struct NetworkMessage {
    pub topic: String,
    pub data: Vec<u8>,
}

/// 直连请求-响应通道的不透明句柄。
///
/// 包装 iroh QUIC 双向流的发送半部。`NetworkEvent::DirectRequest` 的生产者构造此句柄，
/// 消费者通过 [`DirectResponseChannel::take`] 在发送响应时取出。
#[derive(Debug)]
pub struct DirectResponseChannel(Option<iroh::endpoint::SendStream>);

impl DirectResponseChannel {
    /// 构造包装 iroh QUIC 发送流的通道。
    pub fn new(send_stream: iroh::endpoint::SendStream) -> Self {
        Self(Some(send_stream))
    }

    /// 取出内部 iroh 发送流。若已被消费则返回 `None`。
    pub fn take(mut self) -> Option<iroh::endpoint::SendStream> {
        self.0.take()
    }

    /// 测试专用桩构造器：创建一个无底层 QUIC 流的通道。
    ///
    /// 仅在 `#[cfg(test)]` 下可见，用于在不依赖真实 iroh 连接的前提下测试
    /// 事件总线的独占投递路径（`DirectRequest` 分发）。生产代码不会调用。
    #[cfg(test)]
    pub fn test_stub() -> Self {
        Self(None)
    }
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
    /// 通过 ALPN 协议来自对端的直连请求。
    DirectRequest {
        peer_id: String,
        request: Box<DirectRequest>,
        channel: DirectResponseChannel,
    },
}

/// 网络传输的抽象。
///
/// 此 trait 将 crate 其余部分（orchestrator、worker、actor 系统、failover、gossip）
/// 与任何具体网络实现解耦。唯一生产级实现是 [`NetworkManager`]，封装 iroh P2P 网络（QUIC + gossip）。
///
/// # 公共扩展点
///
/// 此 trait 是 Rust 核心的公共扩展点。外部 Rust 用户可实现此 trait 以替换网络层
/// （例如使用 libp2p、原始 QUIC 或内存传输用于测试）。实现只需满足 `Send + Sync + 'static`。
///
/// # 0.1.0 限制
///
/// Python 层**无法注入**自定义 `Transport` 实现。Python 用户仅通过 `NetworkConfig.preset`
/// 选择内置发现策略（`local`/`mdns`/`none`），底层始终使用 iroh `NetworkManager`。
/// 自定义 `Transport` 实现目前仅适用于纯 Rust 嵌入场景（Python 绑定暂未暴露注入入口）；
/// 0.2 计划通过 PyO3 暴露注入入口。
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    /// 本地节点 id。
    fn node_id(&self) -> &NodeId;

    /// 此 endpoint 的简短可读标识符（用于日志和 `ListenAddresses::endpoint_id`）。
    /// iroh 为 `EndpointId`；内存传输为合成的 per-mesh 唯一 id。
    fn local_peer_id(&self) -> &str;

    /// 将 `data` 广播到 `topic` 的所有订阅者。
    async fn broadcast(&self, topic: &str, data: Vec<u8>) -> crate::common::Result<()>;

    /// 订阅 gossip 话题。mesh 中任何 peer 后续在此话题上的广播
    /// 都会通过 [`Transport::recv_event`] 投递。
    async fn subscribe(&self, topic: &str) -> crate::common::Result<()>;

    /// 接收下一个网络事件（gossip 消息、peer 上下线或直连请求）。
    /// 传输关闭后返回 `None`。
    async fn recv_event(&self) -> Option<NetworkEvent>;

    /// 通过 endpoint 地址字符串（`listen_addresses().endpoint_addr` 返回值）连接 peer。
    /// 返回后 peer 已加入所有当前订阅话题的 gossip mesh。
    async fn dial(&self, addr: &str) -> crate::common::Result<()>;

    /// 显式将 peer（按 `local_peer_id()` 字符串）添加到所有当前订阅的 gossip 话题。
    /// 适用于 peer 已通过带外方式连接的场景。
    async fn add_gossip_peer(&self, peer_id: &str) -> crate::common::Result<()>;

    /// 返回此 endpoint 监听的地址，适合发布给其他节点。
    async fn listen_addresses(&self) -> crate::common::Result<ListenAddresses>;

    /// 向 `peer_id_str` 发送直连（点对点）请求并等待响应。
    /// `peer_id_str` 与远端 peer 的 `local_peer_id()` 返回值一致。
    async fn send_direct_request(
        &self,
        peer_id_str: &str,
        request: DirectRequest,
    ) -> crate::common::Result<DirectResponse>;

    /// 通过 `NetworkEvent::DirectRequest` 中收到的直连请求-响应通道回送响应。
    async fn send_direct_response(
        &self,
        channel: DirectResponseChannel,
        response: DirectResponse,
    ) -> crate::common::Result<()>;

    /// 发现当前连接到此 endpoint 的 peer。由 worker 运行时用于枚举分发目标。
    async fn discover_peers(&self) -> crate::common::Result<Vec<PeerId>>;

    /// 优雅关闭传输，释放所有 socket 和后台任务。返回后 `recv_event` 返回 `None`。
    async fn shutdown(&self) -> crate::common::Result<()>;
}
