// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, Bound},
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use kvproto::metapb;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use prometheus::{register_int_gauge, IntGauge};
use txn_types::{Key, Lock, PessimisticLock};

/// Transaction extensions related to a peer.
#[derive(Default)]
pub struct TxnExt {
    /// The max timestamp recorded in the concurrency manager is only updated at
    /// leader. So if a peer becomes leader from a follower, the max timestamp
    /// can be outdated. We need to update the max timestamp with a latest
    /// timestamp from PD before this peer can work. From the least significant
    /// to the most, 1 bit marks whether the timestamp is updated, 31 bits for
    /// the current epoch version, 32 bits for the current term. The version
    /// and term are stored to prevent stale UpdateMaxTimestamp task from
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

lazy_static! {
    pub static ref GLOBAL_MEM_SIZE: IntGauge = register_int_gauge!(
        "tikv_pessimistic_lock_memory_size",
        "Total memory size of pessimistic locks in bytes."
    )
    .unwrap();
}

const GLOBAL_MEM_SIZE_LIMIT: usize = 100 << 20; // 100 MiB

// 512 KiB, so pessimistic locks in one region can be proposed in a single
// command.
const PEER_MEM_SIZE_LIMIT: usize = 512 << 10;

/// Pessimistic locks of a region peer.
#[derive(PartialEq)]
pub struct PeerPessimisticLocks {
    /// The table that stores pessimistic locks.
    ///
    /// The bool marks an ongoing write request (which has been sent to the
    /// raftstore while not applied yet) will delete this lock. The lock will be
    /// really deleted after applying the write request. The flag will decide
    /// whether this lock should be migrated to other peers on leader or region
    /// changes:
    ///
    /// - Transfer leader The lock with the deleted mark SHOULD NOT be proposed
    ///   before transferring leader. Considering the following cases with
    ///   different orders: 1. Propose write -> propose locks -> apply write ->
    ///   apply locks -> transfer leader Because the locks marking deleted will
    ///   not be proposed. The lock will be deleted when applying the write
    ///   while not showing up again after applying the locks. 2. Propose locks
    ///   -> propose write -> transfer leader No lock will be lost in normal
    ///   cases because the write request has been sent to the raftstore, it is
    ///   likely to be proposed successfully, while the leader will need at
    ///   least another round to receive the transfer leader message from the
    ///   transferee.
    ///
    /// - Split region The lock with the deleted mark SHOULD be moved to new
    ///   regions on region split. Considering the following cases with
    ///   different orders: 1. Propose write -> propose split -> apply write ->
    ///   execute split The write will be applied earlier than split. So, the
    ///   lock will be deleted earlier than moving locks to new regions. 2.
    ///   Propose split -> propose write -> ready split -> apply write The write
    ///   will be skipped because its version is lower than the new region. So,
    ///   no lock should be deleted in this case. 3. Propose split -> ready
    ///   split -> propose write The write proposal will be rejected because of
    ///   version mismatch.
    ///
    /// - Merge region The lock with the deleted mark SHOULD be included in the
    ///   catch up logs on region merge. Considering the following cases with
    ///   different orders: 1. Propose write -> propose prepare merge -> apply
    ///   write -> execute merge The locks marked deleted will be deleted when
    ///   applying the write request. So, the deleted locks will not be included
    ///   again in the commit merge request. 2. Propose prepare merge -> propose
    ///   write -> execute merge -> apply write Applying the write will be
    ///   skipped because of version mismatch. So, no lock should be deleted.
    ///   It's correct that we include the locks that are marked deleted in the
    ///   commit merge request.
    map: BTreeMap<Key, (PessimisticLock, bool)>,
    /// Status of the pessimistic lock map.
    /// The map is writable only in the Normal state.
    pub status: LocksStatus,
    /// Refers to the Raft term in which the pessimistic lock table is valid.
    pub term: u64,
    /// Refers to the region version in which the pessimistic lock table is
    /// valid.
    pub version: u64,
    /// Estimated memory used by the pessimistic locks.
    pub memory_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LocksStatus {
    Normal,
    TransferringLeader,
    MergingRegion,
    NotLeader,
    IsInFlashback,
}

impl fmt::Debug for PeerPessimisticLocks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerPessimisticLocks")
            .field("count", &self.map.len())
            .field("memory_size", &self.memory_size)
            .field("status", &self.status)
            .field("term", &self.term)
            .field("version", &self.version)
            .finish()
    }
}

impl Default for PeerPessimisticLocks {
    fn default() -> Self {
        PeerPessimisticLocks {
            map: BTreeMap::default(),
            status: LocksStatus::Normal,
            term: 0,
            version: 0,
            memory_size: 0,
        }
    }
}

impl PeerPessimisticLocks {
    /// Inserts pessimistic locks into the map.
    ///
    /// Returns whether the operation succeeds.
    pub fn insert<P: PessimisticLockPair>(&mut self, pairs: Vec<P>) -> Result<(), Vec<P>> {
        let mut incr = 0;
        // Pre-check the memory limit of pessimistic locks.
        for pair in &pairs {
            let (key, lock) = pair.as_pair();
            // If the key already exists in the map, it's an overwrite.
            // The primary lock does not change during an overwrite, so we don't need to
            // update the memory size.
            if !self.map.contains_key(key) {
                incr += key.len() + lock.memory_size();
            }
        }
        if self.memory_size + incr > PEER_MEM_SIZE_LIMIT
            || GLOBAL_MEM_SIZE.get() as usize + incr > GLOBAL_MEM_SIZE_LIMIT
        {
            return Err(pairs);
        }
        // Insert after check has passed.
        for pair in pairs {
            let (key, lock) = pair.into_pair();
            self.map.insert(key, (lock, false));
        }
        self.memory_size += incr;
        GLOBAL_MEM_SIZE.add(incr as i64);
        Ok(())
    }

    pub fn remove(&mut self, key: &Key) {
        if let Some((lock, _)) = self.map.remove(key) {
            let desc = key.len() + lock.memory_size();
            self.memory_size -= desc;
            GLOBAL_MEM_SIZE.sub(desc as i64);
        }
    }

    pub fn clear(&mut self) {
        self.map = BTreeMap::default();
        GLOBAL_MEM_SIZE.sub(self.memory_size as i64);
        self.memory_size = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_writable(&self) -> bool {
        self.status == LocksStatus::Normal
    }

    pub fn get(&self, key: &Key) -> Option<&(PessimisticLock, bool)> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &Key) -> Option<&mut (PessimisticLock, bool)> {
        self.map.get_mut(key)
    }

    /// Group pessimistic locks in the original region to the split regions.
    ///
    /// The given regions MUST be sorted by key in the ascending order. The
    /// returned `HashMap`s are in the same order of the given regions.
    ///
    /// The locks belonging to the derived region will be kept in the given
    /// `locks` map, and the corresponding position in the returned `Vec`
    /// will be an empty map.
    pub fn group_by_regions(
        &mut self,
        regions: &[metapb::Region],
        derived: &metapb::Region,
    ) -> Vec<PeerPessimisticLocks> {
        // Assert regions are sorted by key in ascending order.
        if cfg!(debug_assertions) {
            for (r1, r2) in regions.iter().zip(regions.iter().skip(1)) {
                assert!(r1.get_start_key() < r2.get_start_key());
            }
        }

        let mut res: Vec<PeerPessimisticLocks> = regions
            .iter()
            .map(|_| PeerPessimisticLocks::default())
            .collect();
        // Locks that are marked deleted still need to be moved to the new regions,
        // and the deleted mark should also be cleared.
        // Refer to the comment in `PeerPessimisticLocks` for details.
        // There is no drain_filter for BtreeMap, so extra clone are needed.
        let mut removed_locks = Vec::new();
        self.map.retain(|key, value| {
            let key_ref = key.as_encoded().as_slice();
            let (start_key, end_key) = (derived.get_start_key(), derived.get_end_key());
            if key_ref < start_key || (!end_key.is_empty() && key_ref >= end_key) {
                removed_locks.push((key.clone(), value.clone()));
                false
            } else {
                true
            }
        });

        for (key, (lock, _)) in removed_locks.into_iter() {
            let idx = regions
                .binary_search_by_key(&&**key.as_encoded(), |region| region.get_start_key())
                .unwrap_or_else(|idx| idx - 1);
            let size = key.len() + lock.memory_size();
            self.memory_size -= size;
            res[idx].map.insert(key, (lock, false));
            res[idx].memory_size += size;
        }
        res
    }

    /// Scan and return locks in the current pessimistic lock map, the map
    /// should be locked first before calling this method.
    pub fn scan_locks<F>(
        &self,
        start: Option<&Key>,
        end: Option<&Key>,
        filter: F,
        limit: usize,
    ) -> (Vec<(Key, Lock)>, bool)
    where
        F: Fn(&Key, &PessimisticLock) -> bool,
    {
        if let (Some(start_key), Some(end_key)) = (start, end) {
            assert!(end_key >= start_key);
        }
        let mut locks = Vec::new();
        let mut iter = self.map.range((
            start.map_or(Bound::Unbounded, |k| Bound::Included(k)),
            end.map_or(Bound::Unbounded, |k| Bound::Excluded(k)),
        ));
        while let Some((key, (lock, _))) = iter.next() {
            if filter(key, lock) {
                locks.push((key.clone(), lock.clone().into_lock()));
            }
            if limit > 0 && locks.len() >= limit {
                return (locks, iter.next().is_some());
            }
        }
        (locks, false)
    }

    #[cfg(test)]
    fn from_locks(locks: impl IntoIterator<Item = (Key, (PessimisticLock, bool))>) -> Self {
        let mut res = PeerPessimisticLocks::default();
        for (key, (locks, is_deleted)) in locks {
            res.memory_size += key.len() + locks.memory_size();
            res.map.insert(key, (locks, is_deleted));
        }
        res
    }
}

impl<'a> IntoIterator for &'a PeerPessimisticLocks {
    type Item = (&'a Key, &'a (PessimisticLock, bool));
    type IntoIter = std::collections::btree_map::Iter<'a, Key, (PessimisticLock, bool)>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

impl Drop for PeerPessimisticLocks {
    fn drop(&mut self) {
        GLOBAL_MEM_SIZE.sub(self.memory_size as i64);
    }
}

pub trait PessimisticLockPair {
    fn as_pair(&self) -> (&Key, &PessimisticLock);

    fn into_pair(self) -> (Key, PessimisticLock);
}

impl PessimisticLockPair for (Key, PessimisticLock) {
    fn as_pair(&self) -> (&Key, &PessimisticLock) {
        (&self.0, &self.1)
    }

    fn into_pair(self) -> (Key, PessimisticLock) {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tikv_util::defer;
    use txn_types::LastChange;

    use super::*;

    lazy_static! {
        static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
    }

    fn lock(primary: &[u8]) -> PessimisticLock {
        PessimisticLock {
            primary: primary.to_vec().into_boxed_slice(),
            start_ts: 100.into(),
            ttl: 3000,
            for_update_ts: 110.into(),
            min_commit_ts: 110.into(),
            last_change: LastChange::make_exist(105.into(), 2),
            is_locked_with_conflict: false,
        }
    }

    fn lock_with_key(key: &[u8], deleted: bool) -> (Key, (PessimisticLock, bool)) {
        (
            Key::from_raw(key),
            (
                PessimisticLock {
                    primary: key.to_vec().into_boxed_slice(),
                    start_ts: 10.into(),
                    ttl: 1000,
                    for_update_ts: 10.into(),
                    min_commit_ts: 20.into(),
                    last_change: LastChange::make_exist(5.into(), 2),
                    is_locked_with_conflict: false,
                },
                deleted,
            ),
        )
    }

    #[test]
    fn test_memory_size() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let mut locks1 = PeerPessimisticLocks::default();
        let mut locks2 = PeerPessimisticLocks::default();
        let k1 = Key::from_raw(b"k1");
        let k2 = Key::from_raw(b"k22");
        let k3 = Key::from_raw(b"k333");

        // Test the memory size of peer pessimistic locks after inserting.
        locks1.insert(vec![(k1.clone(), lock(b"k1"))]).unwrap();
        assert_eq!(locks1.get(&k1), Some(&(lock(b"k1"), false)));
        assert_eq!(locks1.memory_size, k1.len() + lock(b"k1").memory_size());
        locks1.insert(vec![(k2.clone(), lock(b"k1"))]).unwrap();
        assert_eq!(locks1.get(&k2), Some(&(lock(b"k1"), false)));
        assert_eq!(
            locks1.memory_size,
            k1.len() + k2.len() + 2 * lock(b"k1").memory_size()
        );

        // Test the global memory size after inserting.
        locks2.insert(vec![(k3.clone(), lock(b"k1"))]).unwrap();
        assert_eq!(locks2.get(&k3), Some(&(lock(b"k1"), false)));
        assert_eq!(
            GLOBAL_MEM_SIZE.get() as usize,
            locks1.memory_size + locks2.memory_size
        );

        // Test the memory size after replacing, it should not change.
        locks1.insert(vec![(k2.clone(), lock(b"k2"))]).unwrap();
        assert_eq!(locks1.get(&k2), Some(&(lock(b"k2"), false)));
        assert_eq!(
            locks1.memory_size,
            k1.len() + k2.len() + 2 * lock(b"k1").memory_size()
        );
        assert_eq!(
            GLOBAL_MEM_SIZE.get() as usize,
            locks1.memory_size + locks2.memory_size
        );

        // Test the memory size after removing.
        locks1.remove(&k1);
        assert!(locks1.get(&k1).is_none());
        assert_eq!(locks1.memory_size, k2.len() + lock(b"k2").memory_size());
        assert_eq!(
            GLOBAL_MEM_SIZE.get() as usize,
            locks1.memory_size + locks2.memory_size
        );

        // Test the memory size after clearing.
        locks2.clear();
        assert!(locks2.is_empty());
        assert_eq!(locks2.memory_size, 0);
        assert_eq!(GLOBAL_MEM_SIZE.get() as usize, locks1.memory_size);

        // Test the global memory size after dropping.
        drop(locks1);
        drop(locks2);
        assert_eq!(GLOBAL_MEM_SIZE.get(), 0);
    }

    #[test]
    fn test_insert_checking_memory_limit() {
        let _guard = TEST_MUTEX.lock().unwrap();
        defer!(GLOBAL_MEM_SIZE.set(0));

        let mut locks = PeerPessimisticLocks::default();
        locks
            .insert(vec![(Key::from_raw(b"k1"), lock(&[0; 512000]))])
            .unwrap();

        // Exceeding the region limit
        locks
            .insert(vec![(Key::from_raw(b"k2"), lock(&[0; 32000]))])
            .unwrap_err();
        assert!(locks.get(&Key::from_raw(b"k2")).is_none());

        // Not exceeding the region limit, but exceeding the global limit
        GLOBAL_MEM_SIZE.set(101 << 20);
        let res = locks.insert(vec![(Key::from_raw(b"k2"), lock(b"abc"))]);
        res.unwrap_err();
        assert!(locks.get(&Key::from_raw(b"k2")).is_none());
    }

    #[test]
    fn test_group_locks_by_regions() {
        fn region(start_key: &[u8], end_key: &[u8]) -> metapb::Region {
            let mut region = metapb::Region::default();
            region.set_start_key(start_key.to_vec());
            region.set_end_key(end_key.to_vec());
            region
        }
        let _guard = TEST_MUTEX.lock().unwrap();
        defer!(GLOBAL_MEM_SIZE.set(0));

        let mut original = PeerPessimisticLocks::from_locks(vec![
            lock_with_key(b"a", true),
            lock_with_key(b"c", false),
            lock_with_key(b"e", true),
            lock_with_key(b"g", false),
            lock_with_key(b"i", false),
        ]);
        let regions = vec![
            region(b"", b"b"),  // test leftmost region
            region(b"b", b"c"), // no lock inside
            region(b"c", b"d"), // test key equals to start_key
            region(b"d", b"h"), // test multiple locks inside
            region(b"h", b""),  // test rightmost region
        ];
        let output = original.group_by_regions(&regions, &regions[4]);
        let expected: Vec<_> = vec![
            vec![lock_with_key(b"a", false)],
            vec![],
            vec![lock_with_key(b"c", false)],
            vec![lock_with_key(b"e", false), lock_with_key(b"g", false)],
            vec![], // the position of the derived region is empty
        ]
        .into_iter()
        .map(PeerPessimisticLocks::from_locks)
        .collect();
        assert_eq!(output, expected);
        // The lock that belongs to the derived region is kept in the original map.
        assert_eq!(
            original,
            PeerPessimisticLocks::from_locks(vec![lock_with_key(b"i", false)])
        );
    }

    #[test]
    fn test_scan_memory_lock() {
        // Create a sample PeerPessimisticLocks instance with some locks.
        let peer_locks = PeerPessimisticLocks::from_locks(vec![
            lock_with_key(b"key1", false),
            lock_with_key(b"key2", false),
            lock_with_key(b"key3", false),
        ]);

        fn txn_lock(key: &[u8], deleted: bool) -> Lock {
            let (_, (pessimistic_lock, _)) = lock_with_key(key, deleted);
            pessimistic_lock.into_lock()
        }

        type LockFilter = fn(&Key, &PessimisticLock) -> bool;

        fn filter_pass_all(_: &Key, _: &PessimisticLock) -> bool {
            true
        }

        fn filter_pass_key2(key: &Key, _: &PessimisticLock) -> bool {
            key.as_encoded().starts_with(b"key2")
        }

        // Case parameter: start_key, end_key, filter, limit, expected results, expected
        // has more.
        let cases: [(
            Option<Key>,
            Option<Key>,
            LockFilter,
            usize,
            Vec<(Key, Lock)>,
            bool,
        ); 12] = [
            (
                None,
                None,
                filter_pass_all,
                1,
                vec![(Key::from_raw(b"key1"), txn_lock(b"key1", false))],
                true,
            ),
            (
                None,
                None,
                filter_pass_all,
                10,
                vec![
                    (Key::from_raw(b"key1"), txn_lock(b"key1", false)),
                    (Key::from_raw(b"key2"), txn_lock(b"key2", false)),
                    (Key::from_raw(b"key3"), txn_lock(b"key3", false)),
                ],
                false,
            ),
            (
                Some(Key::from_raw(b"key0")),
                Some(Key::from_raw(b"key1")),
                filter_pass_all,
                10,
                vec![],
                false,
            ),
            (
                Some(Key::from_raw(b"key0")),
                Some(Key::from_raw(b"key2")),
                filter_pass_all,
                10,
                vec![(Key::from_raw(b"key1"), txn_lock(b"key1", false))],
                false,
            ),
            (
                Some(Key::from_raw(b"key1")),
                Some(Key::from_raw(b"key3")),
                filter_pass_all,
                10,
                vec![
                    (Key::from_raw(b"key1"), txn_lock(b"key1", false)),
                    (Key::from_raw(b"key2"), txn_lock(b"key2", false)),
                ],
                false,
            ),
            (
                Some(Key::from_raw(b"key1")),
                Some(Key::from_raw(b"key4")),
                filter_pass_all,
                2,
                vec![
                    (Key::from_raw(b"key1"), txn_lock(b"key1", false)),
                    (Key::from_raw(b"key2"), txn_lock(b"key2", false)),
                ],
                true,
            ),
            (
                Some(Key::from_raw(b"key1")),
                Some(Key::from_raw(b"key4")),
                filter_pass_all,
                10,
                vec![
                    (Key::from_raw(b"key1"), txn_lock(b"key1", false)),
                    (Key::from_raw(b"key2"), txn_lock(b"key2", false)),
                    (Key::from_raw(b"key3"), txn_lock(b"key3", false)),
                ],
                false,
            ),
            (
                Some(Key::from_raw(b"key2")),
                Some(Key::from_raw(b"key4")),
                filter_pass_all,
                10,
                vec![
                    (Key::from_raw(b"key2"), txn_lock(b"key2", false)),
                    (Key::from_raw(b"key3"), txn_lock(b"key3", false)),
                ],
                false,
            ),
            (
                Some(Key::from_raw(b"key4")),
                Some(Key::from_raw(b"key4")),
                filter_pass_all,
                10,
                vec![],
                false,
            ),
            (
                None,
                None,
                filter_pass_key2,
                10,
                vec![(Key::from_raw(b"key2"), txn_lock(b"key2", false))],
                false,
            ),
            (
                Some(Key::from_raw(b"key2")),
                None,
                filter_pass_key2,
                1,
                vec![(Key::from_raw(b"key2"), txn_lock(b"key2", false))],
                true,
            ),
            (
                None,
                Some(Key::from_raw(b"key2")),
                filter_pass_key2,
                1,
                vec![],
                false,
            ),
        ];

        for (start_key, end_key, filter, limit, expected_locks, expected_has_more) in cases {
            let (locks, has_more) =
                peer_locks.scan_locks(start_key.as_ref(), end_key.as_ref(), filter, limit);
            assert_eq!(locks, expected_locks);
            assert_eq!(has_more, expected_has_more);
        }
    }
}
