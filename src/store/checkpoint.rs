use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::common::wire::constants::store_keys::CHECKPOINT;
use crate::common::{
    serialization::{deserialize_rkyv_value, serialize_rkyv},
    ActorId, Result,
};
use crate::store::engine::Store;
use crate::store::hlc::HlcTimestamp;

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub(crate) struct ActorSnapshot {
    pub(crate) actor_id: ActorId,
    pub(crate) actor_type: String,
    pub(crate) state: Vec<u8>,
    pub(crate) timestamp: HlcTimestamp,
    pub(crate) sequence: u64,
    pub(crate) wal_offset: u64,
}

#[derive(Clone)]
pub struct CheckpointManager {
    store: Store,
}

impl CheckpointManager {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub(crate) fn save(&self, snapshot: &ActorSnapshot) -> Result<()> {
        let key = checkpoint_key(&snapshot.actor_id, snapshot.sequence);
        let data = serialize_rkyv(snapshot)?;
        self.store.put(&key, &data)
    }

    #[cfg(test)]
    pub(crate) fn load(&self, actor_id: &ActorId, sequence: u64) -> Result<Option<ActorSnapshot>> {
        let key = checkpoint_key(actor_id, sequence);
        match self.store.get(&key)? {
            Some(data) => Ok(Some(deserialize_rkyv_value(&data)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn load_latest(&self, actor_id: &ActorId) -> Result<Option<ActorSnapshot>> {
        let prefix = format!("{}{}:", CHECKPOINT, actor_id.0);
        match self.store.scan_prefix_last(&prefix)? {
            Some((_, data)) => Ok(Some(deserialize_rkyv_value(&data)?)),
            None => Ok(None),
        }
    }

    pub fn delete_old(&self, actor_id: &ActorId, keep_latest: usize) -> Result<()> {
        let prefix = format!("{}{}:", CHECKPOINT, actor_id.0);
        let entries = self.store.scan_prefix(&prefix)?;
        if entries.len() <= keep_latest {
            return Ok(());
        }
        let to_delete = entries.len() - keep_latest;
        for (key, _) in entries.iter().take(to_delete) {
            self.store.delete(key)?;
        }
        Ok(())
    }
}

fn checkpoint_key(actor_id: &ActorId, sequence: u64) -> String {
    format!("{}{}:{:020}", CHECKPOINT, actor_id.0, sequence)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn save_and_load() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);

        let snapshot = ActorSnapshot {
            actor_id: ActorId("actor-1".into()),
            actor_type: "test".into(),
            state: b"state-data".to_vec(),
            timestamp: HlcTimestamp::from_parts(1000, 0),
            sequence: 1,
            wal_offset: 0,
        };

        cm.save(&snapshot).unwrap();
        let loaded = cm.load(&snapshot.actor_id, 1).unwrap().unwrap();
        assert_eq!(loaded.state, b"state-data");
    }

    #[test]
    fn load_latest_returns_most_recent() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        let actor_id = ActorId("actor-1".into());

        for seq in 1..=3 {
            cm.save(&ActorSnapshot {
                actor_id: actor_id.clone(),
                actor_type: "test".into(),
                state: format!("state-{}", seq).into_bytes(),
                timestamp: HlcTimestamp::from_parts(1000 + seq, 0),
                sequence: seq,
                wal_offset: 0,
            })
            .unwrap();
        }

        let latest = cm.load_latest(&actor_id).unwrap().unwrap();
        assert_eq!(latest.sequence, 3);
        assert_eq!(latest.state, b"state-3");
    }
}
