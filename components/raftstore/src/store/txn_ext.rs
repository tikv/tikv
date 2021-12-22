// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use collections::HashMap;
use parking_lot::RwLock;
use txn_types::{Key, PessimisticLock};

/// Transaction extensions related to a peer.
#[derive(Default)]
pub struct TxnExt {
    /// The max timestamp recorded in the concurrency manager is only updated at leader.
    /// So if a peer becomes leader from a follower, the max timestamp can be outdated.
    /// We need to update the max timestamp with a latest timestamp from PD before this
    /// peer can work.
    /// From the least significant to the most, 1 bit marks whether the timestamp is
    /// updated, 31 bits for the current epoch version, 32 bits for the current term.
    /// The version and term are stored to prevent stale UpdateMaxTimestamp task from
    /// marking the lowest bit.
    pub max_ts_sync_status: AtomicU64,

    /// The in-memory pessimistic lock table of the peer.
    pub pessimistic_locks: RwLock<PeerPessimisticLocks>,
}

impl TxnExt {
    pub fn is_max_ts_synced(&self) -> bool {
        self.max_ts_sync_status.load(Ordering::SeqCst) & 1 == 1
    }
}

impl fmt::Debug for TxnExt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("TxnExt");
        debug_struct.field("max_ts_sync_status", &self.max_ts_sync_status);
        if let Some(pessimistic_locks) = self.pessimistic_locks.try_read() {
            debug_struct.field("pessimistic_locks", &pessimistic_locks);
        } else {
            debug_struct.field("pessimistic_locks", &"(Locked)");
        }
        debug_struct.finish()
    }
}

/// Pessimistic locks of a region peer.
pub struct PeerPessimisticLocks {
    pub map: HashMap<Key, PessimisticLock>,
    /// Whether the pessimistic lock map is valid to read or write. If it is invalid,
    /// the in-memory pessimistic lock feature cannot be used at the moment.
    pub is_valid: bool,
    /// Refers to the Raft term in which the pessimistic lock table is valid.
    pub term: u64,
    /// Refers to the region version in which the pessimistic lock table is valid.
    pub version: u64,
}

impl fmt::Debug for PeerPessimisticLocks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerPessimisticLocks")
            .field("pessimistic_locks_size", &self.map.len())
            .field("is_valid", &self.is_valid)
            .field("term", &self.term)
            .field("version", &self.version)
            .finish()
    }
}

impl Default for PeerPessimisticLocks {
    fn default() -> Self {
        PeerPessimisticLocks {
            map: HashMap::default(),
            is_valid: true,
            term: 0,
            version: 0,
        }
    }
}
