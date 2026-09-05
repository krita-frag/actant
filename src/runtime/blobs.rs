//! 内容寻址 blob 原语（0.3.2 R1）：store / fetch / hash 三个能力的薄封装。
//!
//! 底座为 iroh-blobs（bao/blake3 逐块校验流式传输），本模块对其类型完全
//! 封装——公共 API 只暴露 [`BlobHash`]（common 层 newtype）、[`BlobStore`]
//! 与 [`BlobFetch`]，替换底层实现不波及调用方。接入点：[`crate::runtime::
//! network::NetworkManager`] 在与 gossip / 直连协议同一个 `Router` 上
//! `.accept(iroh_blobs::ALPN, ...)`（spike Q1 已验证多协议共存）。
//!
//! blob 传输走独立 ALPN 连接，不占用 `DirectRequest` 帧通道，因此不受
//! `max_message_size` 帧上限约束——这是 0.3.2 "100MB 值仅一次序列化" 的
//! 传输前提。
//!
//! ## 取消语义（spike Q3 结论）
//!
//! 仅 drop 连接句柄时 QUIC 连接会存活到 idle timeout；[`BlobFetch`] 在
//! `Drop` 与显式 [`close`](BlobFetch::close) 中都直接调用 `Connection::close`
//! （同步），取消清理从 30s 级缩短到即时。
//!
//! ## 流式与内存（spike Q2 结论）
//!
//! 拉取经 `iroh_blobs::get::fsm` 状态机逐 blake3 leaf（≤16KiB chunk group）
//! 产出已校验数据，峰值缓冲为通道缓冲容量 × 单 leaf，结构上不整块缓冲；
//! 读取侧拉式背压（消费慢 → fsm 停读 → QUIC 流控限住 provider）。

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use iroh::endpoint::VarInt;
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::get::fsm::{self, RequestCounters};
use iroh_blobs::protocol::GetRequest;
use tokio::sync::mpsc;

use crate::common::model::BlobHash;
use crate::common::{ActantError, Result};

/// 流式拉取的数据块通道容量（chunk 数）。消费慢时发送端 await 背压，
/// 峰值缓冲上界 = 此容量 × 单 leaf（≤16KiB）。
const FETCH_CHANNEL_CAPACITY: usize = 64;

/// 本地 blob 存储 facade：内容寻址存取 + iroh Router 接入。
///
/// 存储后端选型（spike 五、R1 设计建议）：**FsStore 落盘**（`data_dir/blobs/`）
/// 而非内存 store——Ref 是可能跨重启消费的持久值引用，与 orchestrator/
/// actor 存储随 `data_dir` 持久化的既有语义一致。FsStore 默认 `gc: None`
/// （blob 永不回收），写入侧建立持久 tag 双重保护；后续需要 GC 策略时在
/// 此处加配置，不影响调用方。
#[derive(Debug, Clone)]
pub struct BlobStore {
    store: iroh_blobs::api::Store,
}

impl BlobStore {
    /// 打开（或初始化）落盘 blob 存储。
    ///
    /// # Errors
    ///
    /// 目录无法创建或存储初始化失败时返回 [`ActantError::Storage`]。
    pub async fn open(dir: &Path) -> Result<Self> {
        let store = iroh_blobs::store::fs::FsStore::load(dir)
            .await
            .map_err(|e| {
                ActantError::Storage(format!(
                    "failed to open blob store at {}: {e}",
                    dir.display()
                ))
            })?;
        tracing::debug!(dir = %dir.display(), "blob store opened");
        Ok(Self {
            store: store.into(),
        })
    }

    /// 将数据写入本地 blob 存储，返回内容寻址 hash（blake3 32 字节）。
    ///
    /// # Errors
    ///
    /// 存储写入失败时返回 [`ActantError::Storage`]。
    pub async fn store(&self, data: Vec<u8>) -> Result<BlobHash> {
        // await 产出 TagInfo：持久 tag 保护内容（叠加 FsStore 默认不回收）。
        let tag = self
            .store
            .blobs()
            .add_slice(data)
            .await
            .map_err(|e| ActantError::Storage(format!("failed to store blob: {e}")))?;
        Ok(BlobHash::from_bytes(tag.hash.into()))
    }

    /// 读取本地已有的 blob（小数据便捷路径；大数据应直接消费文件/流）。
    ///
    /// # Errors
    ///
    /// 本地不存在该 hash 或读取失败时返回错误（不吞）。
    pub async fn get_bytes(&self, hash: &BlobHash) -> Result<Vec<u8>> {
        let hash = iroh_blobs::Hash::from(*hash.as_bytes());
        self.store
            .blobs()
            .get_bytes(hash)
            .await
            .map(|b| b.to_vec())
            .map_err(|e| ActantError::Storage(format!("failed to read blob {hash}: {e}")))
    }

    /// iroh Router accept 所需的协议 handler（仅仓内装配，不进公共 API）。
    pub(crate) fn protocol_handler(&self) -> iroh_blobs::BlobsProtocol {
        iroh_blobs::BlobsProtocol::new(&self.store, None)
    }
}

/// 流式拉取句柄：逐 leaf 产出已通过 bao/blake3 校验的数据块。
///
/// 同时实现 [`Stream`]；[`Drop`] 与 [`close`](Self::close) 均显式关闭底层
/// QUIC 连接（取消语义见模块文档）。传输中途数据损坏表现为 `Err` 块
/// （hash mismatch），不会静默产出未校验数据。
#[derive(Debug)]
pub struct BlobFetch {
    rx: mpsc::Receiver<Result<Bytes>>,
    conn: Option<iroh::endpoint::Connection>,
    hash: BlobHash,
}

impl BlobFetch {
    /// 连接 provider 并发送请求；读到响应 size 头（拉取已就绪）后返回句柄。
    ///
    /// hash 在 provider 上不存在时在**本调用**返回
    /// [`ActantError::NotFound`]；节点不可达返回 [`ActantError::Network`]。
    ///
    /// # Errors
    ///
    /// 连接失败、协议握手失败或 hash 不存在时返回错误。
    pub async fn start(endpoint: &Endpoint, addr: EndpointAddr, hash: BlobHash) -> Result<Self> {
        let conn = endpoint
            .connect(addr, iroh_blobs::ALPN)
            .await
            .map_err(|e| ActantError::Network(format!("blob fetch connect failed: {e}")))?;

        // 驱动 fsm 至内容态：NotFound（provider 无此 hash）在此显式暴露。
        let connected = fsm::start(
            conn.clone(),
            GetRequest::blob(iroh_blobs::Hash::from(*hash.as_bytes())),
            RequestCounters::default(),
        )
        .next()
        .await
        .map_err(|e| ActantError::Network(format!("blob fetch handshake failed: {e}")))?;
        let header = match connected
            .next()
            .await
            .map_err(|e| ActantError::Network(format!("blob fetch request failed: {e}")))?
        {
            fsm::ConnectedNext::StartRoot(root) => root.next(),
            fsm::ConnectedNext::StartChild(_) | fsm::ConnectedNext::Closing(_) => {
                return Err(ActantError::Network(
                    "unexpected blob response state for single-blob request".into(),
                ));
            }
        };
        let (content, _size) = header.next().await.map_err(|e| match e {
            // 干净 EOF：iroh 语义上的 not found。
            fsm::AtBlobHeaderNextError::NotFound { .. } => not_found_error(&hash),
            // FsStore 上缺失 blob 的实际表现：provider 在发出 size 头之前以
            // ERR_INTERNAL reset 掉发送流（iroh-blobs provider 对导出失败的
            // 统一处理），客户端只看到连接重置。header 阶段（尚未收到任何
            // 数据）的重置即视为"该 provider 无法提供此 blob"。
            fsm::AtBlobHeaderNextError::Read { source, .. }
                if source.to_string().contains("stream reset") =>
            {
                not_found_error(&hash)
            }
            other => ActantError::Network(format!("blob fetch header failed: {other}")),
        })?;

        let (tx, rx) = mpsc::channel(FETCH_CHANNEL_CAPACITY);
        // 连接克隆留在句柄中供 Drop/close 显式关闭；驱动任务仅持有 fsm 状态。
        tokio::spawn(drive_content(content, tx));
        Ok(Self {
            rx,
            conn: Some(conn),
            hash,
        })
    }

    /// 拉取目标的内容寻址 hash。
    pub fn hash(&self) -> &BlobHash {
        &self.hash
    }

    /// 接收下一个已校验数据块；流结束（传输完成或已出错）返回 `None`。
    pub async fn next_chunk(&mut self) -> Option<Result<Bytes>> {
        self.rx.recv().await
    }

    /// 显式取消拉取并立即关闭底层连接。
    pub fn close(&mut self) {
        if let Some(conn) = self.conn.take() {
            conn.close(VarInt::from_u32(0), b"actant blob fetch cancelled");
        }
    }
}

impl Drop for BlobFetch {
    fn drop(&mut self) {
        // 取消语义：句柄 drop 时显式关闭连接，不依赖 QUIC idle timeout。
        if let Some(conn) = self.conn.take() {
            conn.close(VarInt::from_u32(0), b"actant blob fetch dropped");
        }
    }
}

impl Stream for BlobFetch {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_recv(cx)
    }
}

/// header 阶段判定"provider 无此 blob"的语义化错误。
fn not_found_error(hash: &BlobHash) -> ActantError {
    ActantError::NotFound(format!("blob {hash} not found on provider"))
}

/// fsm 内容循环：逐 leaf 转发到通道，读取侧拉式背压由有界通道承担。
///
/// 消费方取消（通道关闭）或解码失败时任务退出；连接由 [`BlobFetch`] 的
/// Drop/close 显式关闭。
async fn drive_content(mut content: fsm::AtBlobContent, tx: mpsc::Sender<Result<Bytes>>) {
    while let fsm::BlobContentNext::More((next, item)) = content.next().await {
        content = next;
        match item {
            Ok(bao_tree::io::fsm::BaoContentItem::Leaf(leaf)) => {
                if tx.send(Ok(leaf.data)).await.is_err() {
                    return;
                }
            }
            // bao outboard 校验节点：完整性由 response decoder 内部完成。
            Ok(bao_tree::io::fsm::BaoContentItem::Parent(_)) => {}
            Err(e) => {
                let _ = tx
                    .send(Err(ActantError::Network(format!(
                        "blob fetch decode failed: {e}"
                    ))))
                    .await;
                return;
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/blobs.rs"]
mod tests;
