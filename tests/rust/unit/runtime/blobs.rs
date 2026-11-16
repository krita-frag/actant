//! Unit tests for `src/runtime/blobs.rs`（三协议共存往返、10MB 流式读取、中途取消不悬挂）。
//! Compiled via `#[path]` attribute — retains `super::` access to private items.
//!
//! 覆盖三协议共存往返、10MB 流式读取（峰值 ≤16KiB）、中途取消不悬挂，经生产装配路径
//! `NetworkManager::with_blob_store`（gossip + 直连 + blobs 同一 Router）验证。

use std::time::{Duration, Instant};

use blake3::Hasher;
use tempfile::tempdir;

use super::*;
use crate::common::{NetworkConfig, NodeId};
use crate::runtime::network::{DirectRequest, DirectResponse, NetworkManager};

const MB: usize = 1024 * 1024;

/// 确定性伪随机数据（xorshift64*），不引入 rand 依赖。
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

/// 启动一个生产形态节点（gossip + 直连 + blobs 同 Router）。
///
/// `with_blobs = false` 时走 [`NetworkManager::new`]（不注册 blobs 协议），
/// 用于验证未启用 blob 存储的节点拒绝 blob 拉取。
async fn spawn_node_opt(name: &str, with_blobs: bool) -> (NetworkManager, tempfile::TempDir) {
    let dir = tempdir().expect("temp dir");
    let config = NetworkConfig {
        discovery_mode: crate::common::DiscoveryMode::parse(crate::common::discovery_mode::NONE)
            .unwrap(),
        listen_ip: "127.0.0.1".into(),
        listen_port: 0,
        direct_request_timeout_ms: 5_000,
        ..NetworkConfig::default()
    };
    let manager = if with_blobs {
        let store = BlobStore::open(&dir.path().join("blobs"))
            .await
            .expect("open blob store");
        NetworkManager::with_blob_store(NodeId::new(name.to_string()), config, store.into())
            .await
            .expect("spawn network manager")
    } else {
        NetworkManager::new(NodeId::new(name.to_string()), config)
            .await
            .expect("spawn network manager")
    };
    (manager, dir)
}

async fn spawn_node(name: &str) -> (NetworkManager, tempfile::TempDir) {
    spawn_node_opt(name, true).await
}

/// 对端可寻址 NodeId：listen_addresses 的 hex endpoint addr 编码。
fn addr_node_id(manager: &NetworkManager) -> NodeId {
    let addresses = manager.listen_addresses().expect("listen addresses");
    NodeId::new(addresses.endpoint_addr)
}

/// 消费整条 BlobFetch 流，返回（增量 blake3、总字节数、单块最大字节数）。
async fn drain(fetch: &mut BlobFetch) -> (blake3::Hash, usize, usize) {
    let mut hasher = Hasher::new();
    let mut total = 0usize;
    let mut max_chunk = 0usize;
    while let Some(item) = fetch.next_chunk().await {
        // 取消/中断路径流会以 Err 收尾：停止消费即可。
        let Ok(chunk) = item else { break };
        hasher.update(&chunk);
        total += chunk.len();
        max_chunk = max_chunk.max(chunk.len());
    }
    (hasher.finalize(), total, max_chunk)
}

/// 同一 Router 上直连协议与
/// blobs 协议共存——先走直连请求-响应（bob 侧经 recv_event 消费并回错，
/// 模拟无业务订阅者时的快速失败路径），再经 blob_fetch 拉取并校验 hash。
#[tokio::test(flavor = "multi_thread")]
async fn blob_roundtrip_with_direct_protocol_coexistence() {
    let data = pseudo_random_bytes(256 * 1024);
    let (bob, _bob_dir) = spawn_node("bob").await;
    let bob = std::sync::Arc::new(bob);

    // bob 侧直连请求消费者：无业务订阅者时按契约回 Error 响应（快速失败），
    // 证明 accept 链未被 blobs 接入破坏。
    let responder_bob = bob.clone();
    tokio::spawn(async move {
        while let Some(event) = responder_bob.recv_event().await {
            if let crate::runtime::network::NetworkEvent::DirectRequest { channel, .. } = event {
                channel.send_error("no subscriber in test").await;
            }
        }
    });
    let bob_addr = addr_node_id(&bob);

    let hash = bob.blob_store(data.clone()).await.expect("store blob");

    let (alice, _alice_dir) = spawn_node("alice").await;

    // 直连协议：请求-响应完整往返，返回约定的 Error 变体。
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        alice.send_direct_request(bob_addr.as_str(), DirectRequest::Ping),
    )
    .await
    .expect("direct request must not hang");
    match response.expect("direct request ok") {
        DirectResponse::Error { .. } => {}
        other => panic!("expected Error variant (no subscriber), got {other:?}"),
    }

    // blobs 协议：流式拉取，字节与 hash 一致。
    let mut fetch =
        tokio::time::timeout(Duration::from_secs(10), alice.blob_fetch(&bob_addr, hash))
            .await
            .expect("blob_fetch must not hang")
            .expect("fetch start");
    assert_eq!(fetch.hash(), &hash);
    let (computed, total, _) = drain(&mut fetch).await;
    assert_eq!(total, data.len());
    assert_eq!(computed, blake3::hash(&data));
}

/// 10MB 流式拉取，单块 ≤16KiB
/// （blake3 chunk group），增量 hash 与整块一致——数据从未整块进内存。
#[tokio::test(flavor = "multi_thread")]
async fn blob_fetch_10mb_streams_in_chunks() {
    let data = pseudo_random_bytes(10 * MB);
    let expected_hash = blake3::hash(&data);
    let (bob, _bob_dir) = spawn_node("bob").await;
    let (alice, _alice_dir) = spawn_node("alice").await;

    let hash = bob.blob_store(data.clone()).await.expect("store blob");
    let mut fetch = alice
        .blob_fetch(&addr_node_id(&bob), hash)
        .await
        .expect("fetch start");
    let (computed, total, max_chunk) = drain(&mut fetch).await;

    assert_eq!(total, data.len());
    assert_eq!(computed, expected_hash);
    assert!(
        max_chunk <= 16 * 1024,
        "peak chunk = {max_chunk} bytes, expected ≤ 16KiB leaf"
    );
}

/// 拉取中途显式取消，取消即时生效
/// （不再产出数据），provider 随后仍可服务新请求，两端不悬挂。
#[tokio::test(flavor = "multi_thread")]
async fn blob_fetch_cancel_mid_transfer_does_not_hang() {
    let big = pseudo_random_bytes(64 * MB);
    let small = pseudo_random_bytes(MB);
    let (bob, _bob_dir) = spawn_node("bob").await;
    let big_len = big.len();
    let big_hash = bob.blob_store(big).await.expect("store big");
    let small_hash = bob.blob_store(small.clone()).await.expect("store small");
    let (alice, _alice_dir) = spawn_node("alice").await;
    let bob_addr = addr_node_id(&bob);

    let mut fetch = alice
        .blob_fetch(&bob_addr, big_hash)
        .await
        .expect("fetch start");
    // 等待传输确实开始（已收到 >1MB）。
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut received = 0usize;
    while received < MB {
        assert!(Instant::now() < deadline, "transfer never started");
        match tokio::time::timeout(Duration::from_secs(10), fetch.next_chunk()).await {
            Ok(Some(Ok(chunk))) => received += chunk.len(),
            Ok(Some(Err(e))) => panic!("chunk error before cancel: {e}"),
            Ok(None) => panic!("stream ended before {received} bytes"),
            Err(_) => panic!("timed out waiting for data"),
        }
    }

    // 显式取消：连接立即关闭，后续读取迅速结束（None），不悬挂。
    fetch.close();
    let ended = tokio::time::timeout(Duration::from_secs(5), drain(&mut fetch)).await;
    assert!(ended.is_ok(), "cancel did not take effect within 5s");
    let (_, total_after_cancel, _) = ended.unwrap();
    // 取消后流终止；已缓冲块可有少量残余，但整块 64MB 不可能传完。
    assert!(
        total_after_cancel < big_len,
        "stream should terminate on cancel, got {total_after_cancel}"
    );

    // provider 不悬挂：取消后仍可完整拉取另一 blob（10s 内）。
    let mut refetch = tokio::time::timeout(
        Duration::from_secs(10),
        alice.blob_fetch(&bob_addr, small_hash),
    )
    .await
    .expect("post-cancel refetch must not hang")
    .expect("refetch start");
    let (computed, total, _) = drain(&mut refetch).await;
    assert_eq!(total, small.len());
    assert_eq!(computed, blake3::hash(&small));
}

/// 句柄 drop（隐式取消）同样立即关闭连接：中断后 provider 仍可服务新请求。
#[tokio::test(flavor = "multi_thread")]
async fn blob_fetch_drop_closes_connection() {
    let big = pseudo_random_bytes(64 * MB);
    let small = pseudo_random_bytes(MB);
    let (bob, _bob_dir) = spawn_node("bob").await;
    let big_hash = bob.blob_store(big).await.expect("store big");
    let small_hash = bob.blob_store(small.clone()).await.expect("store small");
    let (alice, _alice_dir) = spawn_node("alice").await;
    let bob_addr = addr_node_id(&bob);

    let mut fetch = alice
        .blob_fetch(&bob_addr, big_hash)
        .await
        .expect("fetch start");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut received = 0usize;
    while received < MB {
        assert!(Instant::now() < deadline, "transfer never started");
        if let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_secs(10), fetch.next_chunk()).await
        {
            received += chunk.len();
        }
    }
    // drop 触发 BlobFetch::Drop → conn.close，清理即时而非 idle timeout。
    drop(fetch);

    let mut refetch = tokio::time::timeout(
        Duration::from_secs(10),
        alice.blob_fetch(&bob_addr, small_hash),
    )
    .await
    .expect("post-drop refetch must not hang")
    .expect("refetch start");
    let (computed, total, _) = drain(&mut refetch).await;
    assert_eq!(total, small.len());
    assert_eq!(computed, blake3::hash(&small));
}

/// 失败路径：provider 上不存在该 hash 时返回语义化 NotFound，不吞。
#[tokio::test(flavor = "multi_thread")]
async fn blob_fetch_missing_hash_returns_not_found() {
    let (bob, _bob_dir) = spawn_node("bob").await;
    let (alice, _alice_dir) = spawn_node("alice").await;

    let bogus = BlobHash::from_bytes(blake3::hash(b"nonexistent").into());
    let err = alice
        .blob_fetch(&addr_node_id(&bob), bogus)
        .await
        .expect_err("missing hash must fail");
    assert!(
        matches!(err, crate::common::ActantError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}

/// 失败路径：对端未注册 blobs 协议（未启用 blob 存储）时，iroh 拒绝连接，
/// blob_fetch 以 Network 错误快速失败，不悬挂。
#[tokio::test(flavor = "multi_thread")]
async fn blob_fetch_without_blob_protocol_fails_fast() {
    let (plain_bob, _bob_dir) = spawn_node_opt("plain-bob", false).await;
    let (alice, _alice_dir) = spawn_node("alice").await;

    let bogus = BlobHash::from_bytes([0u8; 32]);
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        alice.blob_fetch(&addr_node_id(&plain_bob), bogus),
    )
    .await
    .expect("fetch must not hang");
    match result {
        Err(crate::common::ActantError::Network(_)) => {}
        other => panic!("expected Network error, got {other:?}"),
    }
}

/// 挂起 blob provider：回应 8 字节 size 头后停滞，不发任何 bao 数据——
/// 真实构造「size 头之后传输中途停滞」的 provider。
///
/// 流必须被显式持有（挂入长驻任务）：accept future 结束或流 drop 会 reset
/// 客户端 fsm（表现为连接层错误而非内容停滞），与被测语义不符。
#[derive(Debug)]
struct StalledBlobProvider;

// iroh 的 ProtocolHandler / Connection / AcceptError 经 use super::* 不可达
// （blobs.rs 未转引），显式引入；`Result` 在本模块被 crate::common::Result
// 遮蔽，accept 签名用 std 全限定。
use iroh::protocol::{AcceptError, ProtocolHandler, Router};

impl ProtocolHandler for StalledBlobProvider {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> std::result::Result<(), AcceptError> {
        let (send, recv) = conn.accept_bi().await.map_err(AcceptError::from_err)?;
        tokio::spawn(async move {
            let mut send = send;
            // bao 响应头 = u64 LE blob size；此后故意停滞（不 finish，
            // 客户端停在内容阶段等 bao 数据，QUIC 流保持打开）。
            let _ = send.write_all(&1024u64.to_le_bytes()).await;
            let held = (send, recv);
            tokio::time::sleep(Duration::from_secs(60)).await;
            drop(held);
        });
        Ok(())
    }

    async fn shutdown(&self) {}
}

/// 内容阶段的停滞防护：provider 在 size 头之后停滞时，`next_chunk` 按
/// `idle_timeout` 产出 `Err(Timeout)`，调用方不再依赖 QUIC idle timeout
/// 兜底（不可配置且远大于请求超时）。
///
/// 判别力：移除 `next_chunk` 的超时包裹后，`rx.recv()` 永久 pending
/// （挂起 provider 不发任何数据），外层 guard 必然触发，本用例红。
#[tokio::test(flavor = "multi_thread")]
async fn blob_content_idle_timeout_aborts_stalled_provider() {
    let data = pseudo_random_bytes(1024);
    let hash = BlobHash::from_bytes(*blake3::hash(&data).as_bytes());

    let (alice, _alice_dir) = spawn_node("alice").await;

    // 独立的挂起 provider 节点：裸 endpoint（显式监听 loopback，与
    // NetworkManager 的默认装配路径一致）+ 仅挂 blobs ALPN 的 Router。
    let bind_addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 0);
    let stalled_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .bind_addr(bind_addr)
        .expect("bind_addr")
        .bind()
        .await
        .expect("bind stalled provider endpoint");
    let mut stalled_addr = stalled_endpoint.addr();
    // bind 返回时 direct addr 列表可能尚未填充（异步发现），先等非空再
    // 连接——否则 connect 拿不到可达地址，表现为握手阶段悬挂。
    for _ in 0..100 {
        if !stalled_addr.addrs.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        stalled_addr = stalled_endpoint.addr();
    }
    assert!(
        !stalled_addr.addrs.is_empty(),
        "stalled provider must have a direct address after bind"
    );
    let stalled_router = Router::builder(stalled_endpoint.clone())
        .accept(iroh_blobs::ALPN, StalledBlobProvider)
        .spawn();

    // 直接驱动 BlobFetch：idle_timeout 取短值（300ms）便于测试，
    // 生产装配传 direct_request_timeout。
    let mut fetch = tokio::time::timeout(
        Duration::from_secs(10),
        BlobFetch::start(
            alice.endpoint(),
            stalled_addr,
            hash,
            Duration::from_millis(300),
        ),
    )
    .await
    .expect("start must not hang: provider answers the size header")
    .expect("handshake succeeds (size header arrives)");

    let first = tokio::time::timeout(Duration::from_secs(10), fetch.next_chunk())
        .await
        .expect("must not hang: idle timeout bounds the wait")
        .expect("stream yields a verdict");
    let err = first.expect_err("stalled provider must surface Timeout, not data");
    assert!(
        matches!(err, crate::common::ActantError::Timeout(_)),
        "expected Timeout, got {err:?}"
    );
    fetch.close();
    let _ = stalled_router.shutdown().await;
}

/// 正常配置下 roundtrip 不受消费侧空闲超时影响：合法传输的 leaf 间隔
/// 远小于超时值，数据完整交付。
#[tokio::test(flavor = "multi_thread")]
async fn blob_fetch_with_idle_timeout_roundtrip_unaffected() {
    let data = pseudo_random_bytes(256 * 1024);
    let (bob, _bob_dir) = spawn_node("bob").await;
    let (alice, _alice_dir) = spawn_node("alice").await;

    let hash = bob.blob_store(data.clone()).await.expect("store blob");
    let mut fetch = alice
        .blob_fetch(&addr_node_id(&bob), hash)
        .await
        .expect("fetch start");
    let (computed, total, _) = drain(&mut fetch).await;
    assert_eq!(total, data.len());
    assert_eq!(computed, blake3::hash(&data));
}
