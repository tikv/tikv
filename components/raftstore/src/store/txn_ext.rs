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
    /// The table that stores pessimistic locks.
    ///
    /// The bool marks an ongoing write request (which has been sent to the raftstore while not
    /// applied yet) will delete this lock. The lock will be really deleted after applying the
    /// write request. The flag will decide whether this lock should be migrated to other peers
    /// on leader or region changes:
    ///
    /// - Transfer leader
    ///   The lock with the deleted mark SHOULD NOT be proposed before transferring leader.
    ///   Considering the following cases with different orders:
    ///   1. Propose write -> propose locks -> apply write -> apply locks -> transfer leader
    ///      Because the locks marking deleted will not be proposed. The lock will be deleted when
    ///      applying the write while not showing up again after applying the locks.
    ///   2. Propose locks -> propose write -> transfer leader
    ///      No lock will be lost in normal cases because the write request has been sent to the
    ///      raftstore, it is likely to be proposed successfully, while the leader will need at
    ///      least another round to receive the transfer leader message from the transferree.
    ///  
    /// - Split region
    ///   The lock with the deleted mark SHOULD be moved to new regions on region split.
    ///   Considering the following cases with different orders:
    ///   1. Propose write -> propose split -> apply write -> execute split
    ///      The write will be applied earlier than split. So, the lock will be deleted earlier
    ///      than moving locks to new regions.
    ///   2. Propose split -> propose write -> ready split -> apply write
    ///      The write will be skipped because its version is lower than the new region. So, no
    ///      lock should be deleted in this case.
    ///   3. Propose split -> ready split -> propose write
    ///      The write proposal will be rejected because of version mismatch.
    ///
    /// - Merge region
    ///   The lock with the deleted mark SHOULD be included in the catch up logs on region merge.
    ///   Considering the following cases with different orders:
    ///   1. Propose write -> propose prepare merge -> apply write -> execute merge
    ///      The locks marked deleted will be deleted when applying the write request. So, the
    ///      deleted locks will not be included again in the commit merge request.
    ///   2. Propose prepare merge -> propose write -> execute merge -> apply write
    ///      Applying the write will be skipped because of version mismatch. So, no lock should
    ///      be deleted. It's correct that we include the locks that are marked deleted in the
    ///      commit merge request.
    pub map: HashMap<Key, (PessimisticLock, bool)>,
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

impl PeerPessimisticLocks {
    pub fn insert(&mut self, key: Key, lock: PessimisticLock) {
        self.map.insert(key, (lock, false));
    }
}
