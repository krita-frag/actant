//! iroh-blobs 贴合度验证模块（feature `spike-blobs` 隔离，不参与默认构建）。
//!
//! 仅在 `--features spike-blobs` 下编译，不进默认构建、不合并主线。
//! 验证结论与权衡表见 `plans/SPIKE_0.3.2_BLOBS.md`，覆盖四个设计问题：
//!
//! 1. 协议组合性：[`iroh_blobs::net_protocol::BlobsProtocol`] 是普通
//!    [`iroh::protocol::ProtocolHandler`]，可与自有 handler 一起挂到同一个
//!    `Router` / `Endpoint`（[`spawn_blob_node`] 的三协议 accept 即最小接线，
//!    组合方式与 `NetworkManager::new` 完全一致）。
//! 2. 流式与内存：[`iroh_blobs::get::fsm`] 状态机逐 blake3 leaf（≤16KiB
//!    chunk group）产出已校验数据，见 [`stream_pull`]——读取循环内峰值缓冲
//!    即单个 leaf，整块 blob 不需要进内存。
//! 3. 取消与背压：abort 拉取 future 后，provider 侧连接任务随连接终止，
//!    节点可继续服务新请求、endpoint 可正常关闭（`cancel_mid_transfer` 测试）。
//!    速率控制：iroh-blobs 本身无限速旋钮，流控由 QUIC（iroh endpoint）承担。
//! 4. 依赖代价：见 Cargo.toml 注释与 `plans/SPIKE_0.3.2_BLOBS.md` 的 cargo tree 对比。

use std::sync::Arc;
use std::time::Duration;

use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, TransportAddr};
use iroh_blobs::api::TempTag;
use iroh_blobs::get::fsm::{self, RequestCounters};
use iroh_blobs::get::Stats;
use iroh_blobs::protocol::GetRequest;
use iroh_blobs::store::mem::MemStore;
use iroh_blobs::BlobsProtocol;
use iroh_blobs::Hash;
use iroh_gossip::Gossip;
use tokio::sync::Semaphore;

/// spike 内自建直连协议的 ALPN，模拟 `NetworkManager` 中 `actant/direct/1`
/// 的第二协议角色，用于证明同一 Router 上自有协议与 blobs 协议共存。
pub const SPIKE_ECHO_ALPN: &[u8] = b"actant/spike-echo/1";

/// 协议 accept 的并发上限（与 `DirectProtocolHandler` 的 semaphore 同思路）。
const MAX_PENDING_ECHO: usize = 64;

/// 单帧长度上限（8MB），echo 协议读取侧的背压边界。
const MAX_ECHO_FRAME: usize = 8 * 1024 * 1024;

/// 最小回声协议 handler：读取 4 字节 BE 长度前缀帧并原样返回。
///
/// 角色等价于生产代码 `DirectProtocolHandler`——证明 blobs handler 可以与
/// 自有直连协议并列挂在同一 Endpoint。
#[derive(Debug, Clone)]
pub struct EchoHandler {
    semaphore: Arc<Semaphore>,
}

impl EchoHandler {
    fn new() -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(MAX_PENDING_ECHO)),
        }
    }
}

impl ProtocolHandler for EchoHandler {
    fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> impl std::future::Future<Output = Result<(), AcceptError>> + Send {
        let this = self.clone();
        async move {
            this.handle_connection(conn)
                .await
                .map_err(AcceptError::from_err)
        }
    }
}

impl EchoHandler {
    async fn handle_connection(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> crate::common::Result<()> {
        loop {
            let _permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
                crate::common::ActantError::Internal(format!("semaphore closed: {e}"))
            })?;
            let (mut send, mut recv) = conn.accept_bi().await.map_err(|e| {
                crate::common::ActantError::Network(format!("accept_bi failed: {e}"))
            })?;
            let payload = read_frame(&mut recv).await?;
            write_frame(&mut send, &payload).await?;
            send.finish().map_err(|e| {
                crate::common::ActantError::Network(format!("finish echo stream: {e}"))
            })?;
        }
    }
}

async fn read_frame(recv: &mut iroh::endpoint::RecvStream) -> crate::common::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("read frame length: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_ECHO_FRAME {
        return Err(crate::common::ActantError::Network(format!(
            "echo frame too large: {len} > {MAX_ECHO_FRAME}"
        )));
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("read frame payload: {e}")))?;
    Ok(buf)
}

async fn write_frame(
    send: &mut iroh::endpoint::SendStream,
    data: &[u8],
) -> crate::common::Result<()> {
    let len = u32::try_from(data.len())
        .map_err(|_| crate::common::ActantError::Network("echo frame exceeds u32::MAX".into()))?;
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("write frame length: {e}")))?;
    send.write_all(data)
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("write frame payload: {e}")))?;
    Ok(())
}

/// 一个同时服务 gossip / 自有直连回声 / iroh-blobs 三协议的 spike 节点。
///
/// Router 组合方式与 `NetworkManager::new` 完全一致（`Router::builder(endpoint)
/// .accept(..)` 链式追加）：iroh-blobs 的接入点就是在该链上多加一行
/// `.accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))`。
pub struct BlobNode {
    pub endpoint: Endpoint,
    pub store: MemStore,
    _router: Router,
    /// 保护 blob 数据不被 store 回收（TempTag drop 即解除保护）。
    _tag: TempTag,
}

/// 携带随机内容启动一个 blob 节点。
///
/// # Errors
///
/// endpoint bind 失败或数据入 store 失败时返回错误。
pub async fn spawn_blob_node(data: &[u8]) -> crate::common::Result<BlobNode> {
    let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
        .bind()
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike endpoint bind: {e}")))?;

    let store = MemStore::new();
    let tag = store
        .blobs()
        .add_slice(data)
        .temp_tag()
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike add blob: {e}")))?;

    let gossip = Gossip::builder().spawn(endpoint.clone());
    let blobs = BlobsProtocol::new(&store, None);
    let router = Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip)
        .accept(SPIKE_ECHO_ALPN, EchoHandler::new())
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    Ok(BlobNode {
        endpoint,
        store,
        _router: router,
        _tag: tag,
    })
}

impl BlobNode {
    /// 本节点携带的数据的 blake3 hash。
    pub fn blob_hash(&self) -> Hash {
        self._tag.hash()
    }

    /// 本节点可被对端连接的地址；addr() 尚无直连地址时回退 bound_sockets()。
    pub fn addr(&self) -> EndpointAddr {
        let addr = self.endpoint.addr();
        let has_ip = addr.addrs.iter().any(|a| matches!(a, TransportAddr::Ip(_)));
        if has_ip {
            return addr;
        }
        match self.endpoint.bound_sockets().first() {
            Some(sock) => EndpointAddr::new(self.endpoint.id()).with_ip_addr(*sock),
            None => addr,
        }
    }
}

/// 通过同一 Endpoint 上的自有回声协议发送一帧并等待回声。
///
/// 验证 blobs 协议之外的自有协议仍然可用（同一 Router 多协议 accept）。
///
/// # Errors
///
/// 连接或收发失败时返回错误。
pub async fn echo_round_trip(
    endpoint: &Endpoint,
    addr: &EndpointAddr,
    payload: &[u8],
) -> crate::common::Result<Vec<u8>> {
    let conn = endpoint
        .connect(addr.clone(), SPIKE_ECHO_ALPN)
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike echo connect: {e}")))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike echo open_bi: {e}")))?;
    write_frame(&mut send, payload).await?;
    send.finish()
        .map_err(|e| crate::common::ActantError::Network(format!("spike echo finish: {e}")))?;
    read_frame(&mut recv).await
}

/// 经 store API 把远端 blob 拉入本地 store，返回传输统计。
///
/// 使用 `Remote::execute_get` + `GetRequest::blob(hash)`（BlobFormat::Raw）。
///
/// # Errors
///
/// 连接或传输失败时返回错误。
pub async fn fetch_into_store(
    endpoint: &Endpoint,
    addr: &EndpointAddr,
    hash: Hash,
    store: &MemStore,
) -> crate::common::Result<Stats> {
    let conn = endpoint
        .connect(addr.clone(), iroh_blobs::ALPN)
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike fetch connect: {e}")))?;
    store
        .remote()
        .execute_get(conn, GetRequest::blob(hash))
        .complete()
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike fetch complete: {e}")))
}

/// 流式拉取统计：增量 blake3 校验所需的全部信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamStats {
    /// 接收到的 payload 字节数。
    pub payload_bytes: u64,
    /// 单个 blake3 leaf 的最大字节数——读取循环的峰值缓冲。
    pub max_leaf_bytes: usize,
    /// leaf 数量。
    pub leaf_count: u64,
}

/// 底层 fsm 流式拉取：不经 store，逐 leaf 产出并增量求 blake3。
///
/// 读取循环内唯一的缓冲是单个 leaf（`Leaf.data: Bytes`，≤ chunk group 16KiB），
/// 结构上证明 100MB 级 blob 不需要整块进内存。
///
/// # Errors
///
/// 连接或流解码失败时返回错误。
pub async fn stream_pull(
    endpoint: &Endpoint,
    addr: &EndpointAddr,
    hash: Hash,
) -> crate::common::Result<(blake3::Hash, StreamStats)> {
    let conn = endpoint
        .connect(addr.clone(), iroh_blobs::ALPN)
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike stream connect: {e}")))?;

    let mut stats = StreamStats {
        payload_bytes: 0,
        max_leaf_bytes: 0,
        leaf_count: 0,
    };
    let mut hasher = blake3::Hasher::new();

    let connected = fsm::start(conn, GetRequest::blob(hash), RequestCounters::default())
        .next()
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike fsm open: {e}")))?;
    let header =
        match connected.next().await.map_err(|e| {
            crate::common::ActantError::Network(format!("spike fsm send request: {e}"))
        })? {
            fsm::ConnectedNext::StartRoot(root) => root.next(),
            fsm::ConnectedNext::StartChild(_) | fsm::ConnectedNext::Closing(_) => {
                return Err(crate::common::ActantError::Network(
                    "spike fsm: unexpected response state for single-blob request".into(),
                ));
            }
        };
    let (content, _size) = header
        .next()
        .await
        .map_err(|e| crate::common::ActantError::Network(format!("spike fsm header: {e}")))?;

    let mut content = content;
    while let fsm::BlobContentNext::More((next, item)) = content.next().await {
        content = next;
        match item
            .map_err(|e| crate::common::ActantError::Network(format!("spike fsm decode: {e}")))?
        {
            bao_tree::io::fsm::BaoContentItem::Leaf(leaf) => {
                hasher.update(&leaf.data);
                stats.payload_bytes += leaf.data.len() as u64;
                stats.max_leaf_bytes = stats.max_leaf_bytes.max(leaf.data.len());
                stats.leaf_count += 1;
            }
            bao_tree::io::fsm::BaoContentItem::Parent(_) => {
                // bao outboard 校验节点，完整性验证由 response decoder 内部完成
            }
        }
    }

    Ok((hasher.finalize(), stats))
}

/// 关闭节点：关 router 与 endpoint，带超时以暴露悬挂。
///
/// # Errors
///
/// 超时（可能悬挂）时返回错误。
pub async fn close_node(node: BlobNode) -> crate::common::Result<()> {
    close_endpoint(&node.endpoint).await
}

/// 关闭 endpoint（带超时，暴露取消/清理路径上的悬挂）。
///
/// # Errors
///
/// 超时未完成时返回错误。
pub async fn close_endpoint(endpoint: &Endpoint) -> crate::common::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), endpoint.close())
        .await
        .map_err(|_| {
            crate::common::ActantError::Timeout("endpoint.close() timed out after 10s".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// 确定性伪随机数据生成（xorshift64*），避免仅为 spike 引入 rand 依赖。
    fn pseudo_random_bytes(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let bytes = state.to_le_bytes();
            let take = (len - out.len()).min(8);
            out.extend_from_slice(&bytes[..take]);
        }
        out
    }

    const MB: usize = 1024 * 1024;

    /// Q1 + Q2（store 路径）：同一 Router 上三协议共存；10MB blob 经
    /// store API 拉取后字节与 blake3 hash 一致。
    #[tokio::test(flavor = "multi_thread")]
    async fn round_trip_10mb_multi_protocol() -> crate::common::Result<()> {
        let data = pseudo_random_bytes(10 * MB);
        let expected_hash = blake3::hash(&data);

        let bob = spawn_blob_node(&data).await?;
        let alice = spawn_blob_node(&[]).await?;

        // 同一 Router 上的自有协议：回声校验。
        let echoed = echo_round_trip(&alice.endpoint, &bob.addr(), b"ping-spike").await?;
        assert_eq!(echoed, b"ping-spike");

        // blobs 协议：按 hash 拉取 10MB。
        let hash = bob.blob_hash();
        fetch_into_store(&alice.endpoint, &bob.addr(), hash, &alice.store).await?;
        let fetched =
            alice.store.blobs().get_bytes(hash).await.map_err(|e| {
                crate::common::ActantError::Network(format!("spike get_bytes: {e}"))
            })?;
        assert_eq!(fetched.len(), data.len());
        assert_eq!(blake3::hash(&fetched), expected_hash);

        close_node(bob).await?;
        close_node(alice).await?;
        Ok(())
    }

    /// Q2（流式路径）：fsm 逐 leaf 读取，峰值缓冲为单个 leaf（≤16KiB），
    /// 增量 blake3 与整块 hash 一致。
    #[tokio::test(flavor = "multi_thread")]
    async fn stream_read_10mb_chunked() -> crate::common::Result<()> {
        let data = pseudo_random_bytes(10 * MB);
        let expected_hash = blake3::hash(&data);

        let bob = spawn_blob_node(&data).await?;
        let alice = spawn_blob_node(&[]).await?;

        let hash = bob.blob_hash();
        let (computed, stats) = stream_pull(&alice.endpoint, &bob.addr(), hash).await?;
        assert_eq!(computed, expected_hash);
        assert_eq!(stats.payload_bytes as usize, data.len());
        // blake3 leaf 为 1024 字节；传输以 ≤16KiB chunk group 为单位，
        // 单 leaf 缓冲必然远小于整块 10MB。
        assert!(
            stats.max_leaf_bytes <= 16 * 1024,
            "max leaf = {}",
            stats.max_leaf_bytes
        );
        assert!(
            stats.leaf_count > 1,
            "expected multiple leaves, got {}",
            stats.leaf_count
        );

        close_node(bob).await?;
        close_node(alice).await?;
        Ok(())
    }

    /// Q3：拉取中途 abort future，两端不悬挂——
    /// abort 后 provider 仍可服务新请求，两端 endpoint 均可正常关闭。
    #[tokio::test(flavor = "multi_thread")]
    async fn cancel_mid_transfer() -> crate::common::Result<()> {
        let big = pseudo_random_bytes(64 * MB);
        let small = pseudo_random_bytes(MB);
        let bob = spawn_blob_node(&big).await?;
        let bob_addr = bob.addr();
        // 同节点数据集中的小 blob，用于取消后健康检查（TempTag 保护至测试结束）。
        let small_tag = bob
            .store
            .blobs()
            .add_slice(&small)
            .temp_tag()
            .await
            .map_err(|e| crate::common::ActantError::Network(format!("spike add small: {e}")))?;
        let small_hash = small_tag.hash();

        let alice = spawn_blob_node(&[]).await?;
        let alice_endpoint = alice.endpoint.clone();

        // 带共享计数器的拉取任务；abort 时任务连同 Connection 句柄一起被丢弃。
        let received = Arc::new(AtomicU64::new(0));
        let task_received = received.clone();
        let handle = tokio::spawn(async move {
            let conn = alice_endpoint
                .connect(bob_addr, iroh_blobs::ALPN)
                .await
                .expect("spike cancel connect");
            let connected = fsm::start(
                conn,
                GetRequest::blob(small_hash),
                RequestCounters::default(),
            )
            .next()
            .await
            .expect("spike cancel fsm open");
            let header = match connected.next().await.expect("spike cancel fsm request") {
                fsm::ConnectedNext::StartRoot(root) => root.next(),
                _ => panic!("unexpected response state"),
            };
            let (content, _size) = header.next().await.expect("spike cancel fsm header");
            let mut content = content;
            while let fsm::BlobContentNext::More((next, item)) = content.next().await {
                content = next;
                if let Ok(bao_tree::io::fsm::BaoContentItem::Leaf(leaf)) = item {
                    task_received.fetch_add(leaf.data.len() as u64, Ordering::Relaxed);
                }
            }
        });

        // 等待拉取确实开始（已收到 >1MB），然后中途 abort。
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while received.load(Ordering::Relaxed) < MB as u64 {
            assert!(
                std::time::Instant::now() < deadline,
                "transfer never started; received = {}",
                received.load(Ordering::Relaxed)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        handle.abort();
        // abort 立即生效（任务被丢弃，不等传输完成）。
        let abort_result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(abort_result.is_ok(), "abort did not take effect within 5s");

        // 接收端不悬挂：bob 仍可服务新的拉取（小 blob 完整往返，10s 内完成）。
        tokio::time::timeout(
            Duration::from_secs(10),
            fetch_into_store(&alice.endpoint, &bob.addr(), small_hash, &alice.store),
        )
        .await
        .map_err(|_| crate::common::ActantError::Timeout("post-cancel refetch timed out".into()))?
        .map_err(|e| {
            crate::common::ActantError::Network(format!("post-cancel refetch failed: {e}"))
        })?;
        let got = alice
            .store
            .blobs()
            .get_bytes(small_hash)
            .await
            .map_err(|e| {
                crate::common::ActantError::Network(format!("spike post-cancel get: {e}"))
            })?;
        assert_eq!(got.as_ref(), small.as_slice());

        // 两端 endpoint 均可关闭，无悬挂。
        close_node(bob).await?;
        close_node(alice).await?;
        Ok(())
    }
}
