// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    collections::{
        hash_map::{DefaultHasher, RandomState},
        BinaryHeap, VecDeque,
    },
    hash::{Hash, Hasher},
    num::NonZeroU64,
    sync::atomic::Ordering,
    usize,
};

use collections::HashMap;
use crossbeam::utils::CachePadded;
use parking_lot::{Mutex, MutexGuard};
use txn_types::Key;

use crate::storage::txn::commands::{ReleasedLocks, WriteResultLockInfo};

const WAITING_LIST_SHRINK_SIZE: usize = 8;
const WAITING_LIST_MAX_CAPACITY: usize = 16;

pub struct LockWaitInfoComparableWrapper(pub Box<WriteResultLockInfo>);

impl From<WriteResultLockInfo> for LockWaitInfoComparableWrapper {
    fn from(x: WriteResultLockInfo) -> Self {
        LockWaitInfoComparableWrapper(Box::new(x))
    }
}

impl LockWaitInfoComparableWrapper {
    pub fn unwrap(self) -> WriteResultLockInfo {
        *self.0
    }
}

impl Eq for LockWaitInfoComparableWrapper {}

impl PartialEq<Self> for LockWaitInfoComparableWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.parameters.start_ts == other.0.parameters.start_ts
    }
}

impl PartialOrd<Self> for LockWaitInfoComparableWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Reverse it since the std BinaryHeap is max heap and we want to pop the
        // minimal.
        other
            .0
            .parameters
            .start_ts
            .partial_cmp(&self.0.parameters.start_ts)
    }
}

impl Ord for LockWaitInfoComparableWrapper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse it since the std BinaryHeap is max heap and we want to pop the
        // minimal.
        other.0.parameters.start_ts.cmp(&self.0.parameters.start_ts)
    }
}

pub(super) type LockWaitQueueMap =
    std::collections::HashMap<Key, BinaryHeap<LockWaitInfoComparableWrapper>, RandomState>;

/// Latch which is used to serialize accesses to resources hashed to the same
/// slot.
///
/// Latches are indexed by slot IDs. The keys of a command are hashed into
/// unsigned numbers, then the command is added to the waiting queues of the
/// latches.
///
/// If command A is ahead of command B in one latch, it must be ahead of command
/// B in all the overlapping latches. This is an invariant ensured by the
/// `gen_lock`, `acquire` and `release`.
struct Latch {
    // store hash value of the key and command ID which requires this key.
    pub waiting: VecDeque<Option<(u64, u64)>>,
    // TODO: Filter by the known hash to make it perhaps faster.
    pub lock_waiting: HashMap<u64, LockWaitQueueMap>,
}

impl Latch {
    /// Creates a latch with an empty waiting queue.
    pub fn new() -> Latch {
        Latch {
            waiting: VecDeque::new(),
            lock_waiting: HashMap::default(),
        }
    }

    /// Find the first command ID in the queue whose hash value is equal to
    /// hash.
    pub fn get_first_req_by_hash(&self, hash: u64) -> Option<u64> {
        for (h, cid) in self.waiting.iter().flatten() {
            if *h == hash {
                return Some(*cid);
            }
        }
        None
    }

    /// Remove the first command ID in the queue whose hash value is equal to
    /// hash_key. If the element which would be removed does not appear at the
    /// front of the queue, it will leave a hole in the queue. So we must remove
    /// consecutive hole when remove the head of the queue to make the queue not
    /// too long.
    pub fn pop_front(&mut self, key_hash: u64, ignore_cid: Option<u64>) -> Option<(u64, u64)> {
        if let Some(item) = self.waiting.pop_front() {
            if let Some((k, cid)) = item.as_ref() {
                if *k == key_hash && Some(*cid) != ignore_cid {
                    self.maybe_shrink();
                    return item;
                }
                self.waiting.push_front(item);
            }
            // FIXME: remove this clippy attribute once https://github.com/rust-lang/rust-clippy/issues/6784 is fixed.
            #[allow(clippy::manual_flatten)]
            for it in self.waiting.iter_mut() {
                if let Some((v, cid)) = it {
                    if *v == key_hash && Some(*cid) != ignore_cid {
                        return it.take();
                    }
                }
            }
        }
        None
    }

    pub fn wait_for_wake(&mut self, key_hash: u64, cid: u64) {
        self.waiting.push_back(Some((key_hash, cid)));
    }

    pub fn push_preemptive(&mut self, key_hash: u64, cid: u64) {
        self.waiting.push_front(Some((key_hash, cid)));
    }

    /// For some hot keys, the waiting list maybe very long, so we should shrink
    /// the waiting VecDeque after pop.
    fn maybe_shrink(&mut self) {
        // Pop item which is none to make queue not too long.
        while let Some(item) = self.waiting.front() {
            if item.is_some() {
                break;
            }
            self.waiting.pop_front().unwrap();
        }
        if self.waiting.capacity() > WAITING_LIST_MAX_CAPACITY
            && self.waiting.len() < WAITING_LIST_SHRINK_SIZE
        {
            self.waiting.shrink_to_fit();
        }
    }
}

/// Lock required for a command. It also represents the logical ownership of the
/// latches it has acquired, carrying the necessary information that moves along
/// with the ownership.
pub struct Lock {
    /// The hash value of the keys that a command must acquire before being able
    /// to be processed.
    pub required_hashes: Vec<u64>,

    ///  The lock wait queues of the latch slots that's acquired.
    pub lock_wait_queues: HashMap<u64, LockWaitQueueMap>,

    /// The number of latches that the command has acquired.
    pub owned_count: usize,
}

impl Lock {
    /// Creates a lock specifing all the required latches for a command.
    pub fn new<'a, K, I>(keys: I) -> Lock
    where
        K: Hash + 'a,
        I: IntoIterator<Item = &'a K>,
    {
        // prevent from deadlock, so we sort and deduplicate the index
        let mut required_hashes: Vec<u64> = keys
            .into_iter()
            .map(|key| {
                let mut s = DefaultHasher::new();
                key.hash(&mut s);
                s.finish()
            })
            .collect();
        required_hashes.sort_unstable();
        required_hashes.dedup();
        Lock {
            required_hashes,
            lock_wait_queues: HashMap::default(),
            owned_count: 0,
        }
    }

    pub fn new_already_acquired(
        required_hashes: Vec<u64>,
        lock_wait_queues: HashMap<u64, LockWaitQueueMap>,
    ) -> Self {
        let owned_count = required_hashes.len();
        Self {
            required_hashes,
            lock_wait_queues,
            owned_count,
        }
    }

    /// Returns true if all the required latches have be acquired, false
    /// otherwise.
    pub fn acquired(&self) -> bool {
        self.required_hashes.len() == self.owned_count
    }

    pub fn is_write_lock(&self) -> bool {
        !self.required_hashes.is_empty()
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        assert!(self.lock_wait_queues.is_empty());
    }
}

/// Latches which are used for concurrency control in the scheduler.
///
/// Each latch is indexed by a slot ID, hence the term latch and slot are used
/// interchangeably, but conceptually a latch is a queue, and a slot is an index
/// to the queue.
pub struct Latches {
    slots: Vec<CachePadded<Mutex<Latch>>>,
    size: usize,
}

impl Latches {
    /// Creates latches.
    ///
    /// The size will be rounded up to the power of 2.
    pub fn new(size: usize) -> Latches {
        let size = usize::next_power_of_two(size);
        let mut slots = Vec::with_capacity(size);
        (0..size).for_each(|_| slots.push(Mutex::new(Latch::new()).into()));
        Latches { slots, size }
    }

    /// Tries to acquire the latches specified by the `lock` for command with ID
    /// `who`.
    ///
    /// This method will enqueue the command ID into the waiting queues of the
    /// latches. A latch is considered acquired if the command ID is the first
    /// one of elements in the queue which have the same hash value. Returns
    /// true if all the Latches are acquired, false otherwise.
    pub fn acquire(&self, lock: &mut Lock, who: u64) -> bool {
        let mut acquired_count: usize = 0;
        for &key_hash in &lock.required_hashes[lock.owned_count..] {
            let mut latch = self.lock_latch(key_hash);
            match latch.get_first_req_by_hash(key_hash) {
                Some(cid) => {
                    if cid == who {
                        if let Some(q) = latch.lock_waiting.remove(&key_hash) {
                            let replaced = lock.lock_wait_queues.insert(key_hash, q);
                            assert!(replaced.is_none());
                        }
                        acquired_count += 1;
                    } else {
                        latch.wait_for_wake(key_hash, who);
                        break;
                    }
                }
                None => {
                    latch.wait_for_wake(key_hash, who);
                    if let Some(q) = latch.lock_waiting.remove(&key_hash) {
                        let replaced = lock.lock_wait_queues.insert(key_hash, q);
                        assert!(replaced.is_none());
                    }
                    acquired_count += 1;
                }
            }
        }
        lock.owned_count += acquired_count;
        lock.acquired()
    }

    /// Releases all latches owned by the `lock` of command with ID `who`,
    /// returns:
    /// * The cids to wake up
    /// * The latch hashes to be inherited by the next awakened pessimistic lock
    ///   command, if any
    /// * The lock info of the next awakened pessimistic lock command, if any
    /// * The `LockWaitQueueMap`s inherited along with the latch hashes.
    ///
    /// Preconditions: the caller must ensure the command is at the front of the
    /// latches.
    pub fn release(
        &self,
        mut lock: Lock,
        who: u64,
        wait_for_locks: Option<Vec<WriteResultLockInfo>>,
        released_locks: Option<ReleasedLocks>,
        new_cid_for_holding_latches: Option<u64>,
    ) -> (
        Vec<u64>,
        Vec<u64>,
        Vec<WriteResultLockInfo>,
        HashMap<u64, LockWaitQueueMap>,
    ) {
        // It's not allowed that a command releases locks and acquire locks at the same
        // time.
        assert!(!(wait_for_locks.is_some() && released_locks.is_some()));

        let mut wait_for_locks = if let Some(mut v) = wait_for_locks {
            v.sort_unstable_by_key(|x| x.hash_for_latch);
            v.into_iter().peekable()
        } else {
            vec![].into_iter().peekable()
        };
        let mut released_locks = if let Some(ReleasedLocks(mut v)) = released_locks {
            v.sort_unstable_by_key(|x| x.hash_for_latch);
            v.into_iter().peekable()
        } else {
            vec![].into_iter().peekable()
        };

        let mut cmd_wakeup_list: Vec<u64> = vec![];
        let mut latch_keep_list: Vec<u64> = vec![];
        let mut txn_lock_wakeup_list = vec![];
        for &key_hash in &lock.required_hashes[..lock.owned_count] {
            let mut queues = lock.lock_wait_queues.remove(&key_hash);

            while let Some(next_lock) = wait_for_locks.next_if(|v| v.hash_for_latch <= key_hash) {
                assert_eq!(next_lock.hash_for_latch, key_hash);
                Self::push_lock_waiting(&mut queues, next_lock);
            }

            let mut has_awakened_lock_needs_derive_latch = false;
            while let Some(next_lock) = released_locks.next_if(|v| v.hash_for_latch <= key_hash) {
                assert_eq!(next_lock.hash_for_latch, key_hash);
                // TODO: Pass term here
                if let Some(lock_wait) = Self::pop_lock_waiting(&mut queues, &next_lock.key, None) {
                    if lock_wait.allow_lock_with_conflict {
                        has_awakened_lock_needs_derive_latch = true;
                    }
                    txn_lock_wakeup_list.push(lock_wait);
                }
            }

            let mut latch = self.lock_latch(key_hash);
            let (v, front) = latch
                .pop_front(key_hash, new_cid_for_holding_latches)
                .unwrap();
            assert_eq!(front, who);
            assert_eq!(v, key_hash);

            // If we are waking up some blocked pessimistic lock requests, do not wake up
            // next queueing command.
            if !has_awakened_lock_needs_derive_latch {
                // Put back the queue to latch
                if let Some(queues) = queues {
                    if !queues.is_empty() {
                        assert!(latch.lock_waiting.insert(key_hash, queues).is_none());
                    }
                }
                if let Some(wakeup) = latch.get_first_req_by_hash(key_hash) {
                    cmd_wakeup_list.push(wakeup);
                }
            } else {
                // Return the queue.
                if let Some(queues) = queues {
                    if !queues.is_empty() {
                        assert!(lock.lock_wait_queues.insert(key_hash, queues).is_none());
                    }
                }
                latch_keep_list.push(key_hash);
                latch.push_preemptive(key_hash, new_cid_for_holding_latches.unwrap());
            }
        }
        assert!(wait_for_locks.peek().is_none());
        assert!(latch_keep_list.len() >= lock.lock_wait_queues.len());
        (
            cmd_wakeup_list,
            latch_keep_list,
            txn_lock_wakeup_list,
            std::mem::take(&mut lock.lock_wait_queues),
        )
    }

    #[inline]
    fn push_lock_waiting(queues: &mut Option<LockWaitQueueMap>, lock_info: WriteResultLockInfo) {
        let key = lock_info.key.clone();
        queues
            .get_or_insert_with(LockWaitQueueMap::new)
            .entry(key)
            .or_insert_with(BinaryHeap::new)
            .push(lock_info.into());
    }

    #[inline]
    fn pop_lock_waiting(
        queues: &mut Option<LockWaitQueueMap>,
        key: &Key,
        _term: Option<NonZeroU64>,
    ) -> Option<WriteResultLockInfo> {
        let queue = queues.as_mut()?.get_mut(key)?;
        let mut result = None;
        while let Some(lock_info) = queue.pop() {
            // TODO: Early cancel entries with mismatching term.
            if lock_info
                .0
                .req_states
                .as_ref()
                .unwrap()
                .finished
                .load(Ordering::Acquire)
            {
                info!("expired lock wait entry dropped";
                    "start_ts" => lock_info.0.parameters.start_ts, "for_update_ts" => lock_info.0.parameters.for_update_ts, "key" => %lock_info.0.key,
                    "is_new_mode" => lock_info.0.allow_lock_with_conflict);
                // Drop already-finished entry, which might have been canceled by error.
                continue;
            }
            result = Some(lock_info);
            break;
        }

        if queue.is_empty() {
            let queue_map = queues.as_mut().unwrap();
            queue_map.remove(key);
            if queue_map.is_empty() {
                *queues = None;
            }
        }

        result.map(LockWaitInfoComparableWrapper::unwrap)
    }

    #[inline]
    fn lock_latch(&self, hash: u64) -> MutexGuard<'_, Latch> {
        self.slots[(hash as usize) & (self.size - 1)].lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wakeup() {
        let latches = Latches::new(256);

        let keys_a = vec!["k1", "k3", "k5"];
        let mut lock_a = Lock::new(keys_a.iter());
        let keys_b = vec!["k4", "k5", "k6"];
        let mut lock_b = Lock::new(keys_b.iter());
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;

        // a acquire lock success
        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // b acquire lock failed
        let mut acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(lock_a, cid_a, None, None, None).0;
        assert_eq!(wakeup[0], cid_b);

        // b acquire lock success
        acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);
    }

    #[test]
    fn test_wakeup_by_multi_cmds() {
        let latches = Latches::new(256);

        let keys_a = vec!["k1", "k2", "k3"];
        let keys_b = vec!["k4", "k5", "k6"];
        let keys_c = vec!["k3", "k4"];
        let mut lock_a = Lock::new(keys_a.iter());
        let mut lock_b = Lock::new(keys_b.iter());
        let mut lock_c = Lock::new(keys_c.iter());
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;
        let cid_c: u64 = 3;

        // a acquire lock success
        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // b acquire lock success
        let acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);

        // c acquire lock failed, cause a occupied slot 3
        let mut acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(lock_a, cid_a, None, None, None).0;
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock failed again, cause b occupied slot 4
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // b release lock, and get wakeup list
        let wakeup = latches.release(lock_b, cid_b, None, None, None).0;
        assert_eq!(wakeup[0], cid_c);

        // finally c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);
    }

    #[test]
    fn test_wakeup_by_small_latch_slot() {
        let latches = Latches::new(5);

        let keys_a = vec!["k1", "k2", "k3"];
        let keys_b = vec!["k6", "k7", "k8"];
        let keys_c = vec!["k3", "k4"];
        let keys_d = vec!["k7", "k10"];
        let mut lock_a = Lock::new(keys_a.iter());
        let mut lock_b = Lock::new(keys_b.iter());
        let mut lock_c = Lock::new(keys_c.iter());
        let mut lock_d = Lock::new(keys_d.iter());
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;
        let cid_c: u64 = 3;
        let cid_d: u64 = 4;

        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // c acquire lock failed, cause a occupied slot 3
        let mut acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // b acquire lock success
        let acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);

        // d acquire lock failed, cause a occupied slot 7
        let mut acquired_d = latches.acquire(&mut lock_d, cid_d);
        assert_eq!(acquired_d, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(lock_a, cid_a, None, None, None).0;
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);

        // b release lock, and get wakeup list
        let wakeup = latches.release(lock_b, cid_b, None, None, None).0;
        assert_eq!(wakeup[0], cid_d);

        // finally d acquire lock success
        acquired_d = latches.acquire(&mut lock_d, cid_d);
        assert_eq!(acquired_d, true);
    }
}
