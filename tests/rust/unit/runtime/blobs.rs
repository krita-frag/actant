//! Unit tests for `src/runtime/blobs.rs`（0.3.2 R1，spike 用例改写为正式单测）。
//! Compiled via `#[path]` attribute — retains `super::` access to private items.
//!
//! 原 spike 验证（plans/archive/SPIKE_0.3.2_BLOBS.md）：三协议共存往返、10MB 流式
//! 读取（峰值 ≤16KiB）、中途取消不悬挂。此处以生产装配路径
//! `NetworkManager::with_blob_store`（gossip + 直连 + blobs 同一 Router）重写。

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

/// spike `round_trip_10mb_multi_protocol` 改写：同一 Router 上直连协议与
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
        alice.send_direct_request(
            bob_addr.as_str(),
            DirectRequest::QueryWorkflowState {
                workflow_id: crate::common::WorkflowId::from("wf-coexist"),
                requesting_node: NodeId::from("alice"),
            },
        ),
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

/// spike `stream_read_10mb_chunked` 改写：10MB 流式拉取，单块 ≤16KiB
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

/// spike `cancel_mid_transfer` 改写：拉取中途显式取消，取消即时生效
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
