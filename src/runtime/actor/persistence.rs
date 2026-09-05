//! Actor 状态持久化：检查点 + WAL。
//!
//! `ActorPersistence` 同时管理 CheckpointManager（最近状态快照）与
//! WalWriter（增量事件流）。`persist` 先写检查点再追加 WAL，保证崩溃
//! 后可从 (最新 checkpoint, wal_offset) 恢复；`start_compaction` 周期
//! 压缩 WAL 旧段，并通过 `Topic::WalCompacted` 公告。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::sync::watch;

use crate::common::{ActorId, NodeId};
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::state::{
    ActorSnapshot, CheckpointManager, HybridLogicalClock, LmdbStore, WalEvent, WalReader, WalWriter,
};

#[derive(Clone)]
pub struct ActorPersistence {
    wal_writer: Arc<RwLock<Option<WalWriter>>>,
    pub(crate) checkpoint: Arc<Mutex<Option<CheckpointManager>>>,
    sequence: Arc<AtomicU64>,
    hlc: Arc<HybridLogicalClock>,
}

impl ActorPersistence {
    pub fn new() -> Self {
        Self {
            wal_writer: Arc::new(RwLock::new(None)),
            checkpoint: Arc::new(Mutex::new(None)),
            sequence: Arc::new(AtomicU64::new(0)),
            hlc: Arc::new(HybridLogicalClock::new()),
        }
    }

    pub(crate) fn with_wal(&self, wal_writer: WalWriter, store: LmdbStore) -> Self {
        let checkpoint = CheckpointManager::new(store);
        Self {
            wal_writer: Arc::new(RwLock::new(Some(wal_writer))),
            checkpoint: Arc::new(Mutex::new(Some(checkpoint))),
            sequence: Arc::new(AtomicU64::new(0)),
            hlc: Arc::new(HybridLogicalClock::new()),
        }
    }

    pub fn with_checkpoint(&self, checkpoint: CheckpointManager) -> Self {
        let cloned = self.clone();
        *cloned.checkpoint.lock() = Some(checkpoint);
        cloned
    }

    /// 加载最新检查点状态（若存在）。返回 `(state_bytes, wal_offset)`。
    pub async fn load_latest(&self, actor_id: ActorId) -> Option<(Vec<u8>, u64)> {
        let persistence = self.clone();
        // spawn_blocking 要求 'static，actor_id 必须移入闭包；保留一份克隆
        // 供外层 JoinError 路径的日志使用，避免 borrow of moved value。
        let actor_id_for_log = actor_id.clone();
        let result = tokio::task::spawn_blocking(move || {
            let checkpoint = persistence.checkpoint.lock();
            let checkpoint = checkpoint.as_ref()?;
            // checkpoint.load_latest 失败（如 LMDB 损坏）需记录 warning，
            // 而非静默返回 None——后者会让 Actor 以 "无状态" 启动，
            // 丢失已持久化的进度。
            match checkpoint.load_latest(&actor_id) {
                Ok(Some(s)) => Some((s.state, s.wal_offset)),
                Ok(None) => None,
                Err(e) => {
                    tracing::warn!(
                        actor_id = %actor_id.0,
                        "checkpoint load_latest failed: {}",
                        e
                    );
                    None
                }
            }
        })
        .await;
        match result {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(
                    actor_id = %actor_id_for_log.0,
                    "load_latest spawn_blocking panicked: {}",
                    e
                );
                None
            }
        }
    }

    /// 重放检查点之后的全部 WAL 事件，返回该 Actor 的最终状态字节。
    ///
    /// 当前 WAL 事件负载为完整状态快照，因此只需按顺序遍历并取最后一个
    /// 匹配事件即可得到最新状态；若未来改为增量事件，可在此依次应用。
    pub async fn replay_after(&self, actor_id: ActorId, wal_offset: u64) -> Option<Vec<u8>> {
        let persistence = self.clone();
        // 同 load_latest：保留克隆供外层日志使用。
        let actor_id_for_log = actor_id.clone();
        let result = tokio::task::spawn_blocking(move || {
            let wal_guard = persistence.wal_writer.read();
            let wal = wal_guard.as_ref()?;
            // WalReader::open / recover_events 失败（如文件损坏）需记录 warning，
            // 而非静默返回 None——后者会让 Actor 丢弃 WAL 中尚未 checkpoint 的进度。
            let reader = match WalReader::open(wal.path().to_path_buf()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        actor_id = %actor_id.0,
                        "WalReader::open failed: {}",
                        e
                    );
                    return None;
                }
            };
            let events = match reader.recover_events(wal_offset) {
                Ok(ev) => ev,
                Err(e) => {
                    tracing::warn!(
                        actor_id = %actor_id.0,
                        "WAL recover_events from offset {} failed: {}",
                        wal_offset,
                        e
                    );
                    return None;
                }
            };
            let mut latest: Option<Vec<u8>> = None;
            for (_, event) in events {
                if event.actor_id == actor_id {
                    latest = Some(event.payload);
                }
            }
            latest
        })
        .await;
        match result {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(
                    actor_id = %actor_id_for_log.0,
                    "replay_after spawn_blocking panicked: {}",
                    e
                );
                None
            }
        }
    }

    /// 持久化 Actor 状态：先写检查点，再追加 WAL 事件。
    pub async fn persist(&self, actor_id: ActorId, actor_type: String, state: Vec<u8>) {
        if state.is_empty() {
            return;
        }

        let persistence = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let seq = persistence.sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let hlc_ts = persistence.hlc.tick();

            if let Some(ref checkpoint) = *persistence.checkpoint.lock() {
                let wal_offset = {
                    let wal_guard = persistence.wal_writer.read();
                    wal_guard.as_ref().map(|w| w.current_offset()).unwrap_or(0)
                };

                let snapshot = ActorSnapshot {
                    actor_id: actor_id.clone(),
                    actor_type: actor_type.clone(),
                    state: state.clone(),
                    timestamp: hlc_ts,
                    sequence: seq,
                    wal_offset,
                };

                if let Err(e) = checkpoint.save(&snapshot) {
                    tracing::error!("checkpoint save failed for {}: {}", actor_id.0, e);
                }
            }

            let mut wal_guard = persistence.wal_writer.write();
            if let Some(ref mut wal) = *wal_guard {
                let event = WalEvent {
                    actor_id: actor_id.clone(),
                    sequence: seq,
                    payload: state,
                };
                if let Err(e) = wal.append_event(hlc_ts, &event) {
                    tracing::warn!("WAL append failed for {}: {}", actor_id.0, e);
                }
            }
        })
        .await;

        if let Err(e) = result {
            tracing::error!("actor persistence task panicked: {}", e);
        }
    }

    pub fn start_compaction(
        self: Arc<Self>,
        interval_secs: u64,
        checkpoint_retention_count: usize,
        event_bus: Option<EventBus>,
        node_id: Option<NodeId>,
    ) -> watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let persistence = self.clone();
                        let event_bus = event_bus.clone();
                        let node_id = node_id.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            let checkpoint_opt = persistence.checkpoint.lock().as_ref().cloned();
                            let mut wal_guard = persistence.wal_writer.write();
                            if let (Some(ref mut writer), Some(checkpoint)) = (&mut *wal_guard, checkpoint_opt) {
                                let wal_path = writer.path().to_path_buf();
                                let current_offset = writer.current_offset();
                                let compactor = crate::runtime::state::WalCompactor::new(
                                    wal_path, current_offset, checkpoint, checkpoint_retention_count,
                                );
                                if let Err(e) = compactor.compact() {
                                    tracing::warn!("WAL compaction failed: {}", e);
                                    return;
                                }
                                if let Err(e) = writer.sync() {
                                    tracing::warn!("WAL sync after compaction failed: {}", e);
                                    return;
                                }
                                let path = writer.path().to_path_buf();
                                if let Ok(new_writer) = WalWriter::open(&path) {
                                    *writer = new_writer;
                                }
                                // 压缩成功后公告 WalCompacted——retained_events
                                // 取压缩后的 WAL 起始 offset（旧事件已被清理）。
                                if let (Some(bus), Some(node)) = (event_bus, node_id) {
                                    bus.publish(BusEvent::WalCompacted {
                                        node_id: node,
                                        retained_events: writer.current_offset(),
                                    });
                                }
                            }
                        }).await;
                        if let Err(e) = result {
                            tracing::error!("WAL compaction task panicked: {}", e);
                        }
                    }
                    Ok(()) = cancel_rx.changed() => break,
                    else => break,
                }
            }
        });

        cancel_tx
    }
}

impl Default for ActorPersistence {
    fn default() -> Self {
        Self::new()
    }
}
