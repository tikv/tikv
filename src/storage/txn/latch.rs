// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    collections::{hash_map::DefaultHasher, VecDeque},
    hash::{Hash, Hasher},
    usize,
};

use crossbeam::utils::CachePadded;
use parking_lot::{Mutex, MutexGuard};

const WAITING_LIST_SHRINK_SIZE: usize = 8;
const WAITING_LIST_MAX_CAPACITY: usize = 16;

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
#[derive(Clone)]
struct Latch {
    // store hash value of the key and command ID which requires this key.
    pub waiting: VecDeque<Option<(u64, u64)>>,
}

impl Latch {
    /// Creates a latch with an empty waiting queue.
    pub fn new() -> Latch {
        Latch {
            waiting: VecDeque::new(),
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
    pub fn pop_front(&mut self, key_hash: u64) -> Option<(u64, u64)> {
        if let Some(item) = self.waiting.pop_front() {
            if let Some((k, _)) = item.as_ref() {
                if *k == key_hash {
                    self.maybe_shrink();
                    return item;
                }
                self.waiting.push_front(item);
            }
            for it in self.waiting.iter_mut() {
                if let Some((v, _)) = it {
                    if *v == key_hash {
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

    /// Pushes the cid to the front of the queue. Be careful when using it.
    fn push_preemptive(&mut self, key_hash: u64, cid: u64) {
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

/// Lock required for a command.
#[derive(Clone)]
pub struct Lock {
    /// The hash value of the keys that a command must acquire before being able
    /// to be processed.
    pub required_hashes: Vec<u64>,

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
        let mut required_hashes: Vec<u64> = keys.into_iter().map(|key| Self::hash(key)).collect();
        required_hashes.sort_unstable();
        required_hashes.dedup();
        Lock {
            required_hashes,
            owned_count: 0,
        }
    }

    pub fn hash<K: Hash>(key: &K) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    /// Returns true if all the required latches have be acquired, false
    /// otherwise.
    pub fn acquired(&self) -> bool {
        self.required_hashes.len() == self.owned_count
    }

    /// Force set the state of the `Lock` to be already-acquired. Be careful
    /// when using it.
    pub fn force_assume_acquired(&mut self) {
        self.owned_count = self.required_hashes.len();
    }

    pub fn is_write_lock(&self) -> bool {
        !self.required_hashes.is_empty()
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
                        acquired_count += 1;
                    } else {
                        latch.wait_for_wake(key_hash, who);
                        break;
                    }
                }
                None => {
                    latch.wait_for_wake(key_hash, who);
                    acquired_count += 1;
                }
            }
        }
        lock.owned_count += acquired_count;
        lock.acquired()
    }

    /// Releases all latches owned by the `lock` of command with ID `who`,
    /// returns the wakeup list.
    ///
    /// Optionally, this function can release partial of the given `Lock` while
    /// leaving the renaming unlocked, so that some of the latches can be
    /// used in another command. This can be done by passing the cid of the
    /// command who will use the kept latch slots later, and the `Lock` that
    /// need to be kept via the parameter `keep_latches_for_next_cmd`. Note
    /// that the lock in it is assumed to be a subset of the parameter
    /// `lock` which is going to be released.
    ///
    /// Preconditions: the caller must ensure the command is at the front of the
    /// latches.
    pub fn release(
        &self,
        lock: &Lock,
        who: u64,
        keep_latches_for_next_cmd: Option<(u64, &Lock)>,
    ) -> Vec<u64> {
        // Used to
        let dummy_vec = [];
        let (keep_latches_for_cid, mut keep_latches_it) = match keep_latches_for_next_cmd {
            Some((cid, lock)) => (Some(cid), lock.required_hashes.iter().peekable()),
            None => (None, dummy_vec.iter().peekable()),
        };

        // `keep_latches_it` must be sorted and deduped since it's retrieved from a
        // `Lock` object.

        let mut wakeup_list: Vec<u64> = vec![];
        for &key_hash in &lock.required_hashes[..lock.owned_count] {
            let mut latch = self.lock_latch(key_hash);
            let (v, front) = latch.pop_front(key_hash).unwrap();
            assert_eq!(front, who);
            assert_eq!(v, key_hash);

            let keep_for_next_cmd = if let Some(&&next_keep_hash) = keep_latches_it.peek() {
                assert!(next_keep_hash >= key_hash);
                if next_keep_hash == key_hash {
                    keep_latches_it.next();
                    true
                } else {
                    false
                }
            } else {
                false
            };

            if !keep_for_next_cmd {
                if let Some(wakeup) = latch.get_first_req_by_hash(key_hash) {
                    wakeup_list.push(wakeup);
                }
            } else {
                latch.push_preemptive(key_hash, keep_latches_for_cid.unwrap());
            }
        }

        assert!(keep_latches_it.next().is_none());

        wakeup_list
    }

    #[inline]
    fn lock_latch(&self, hash: u64) -> MutexGuard<'_, Latch> {
        self.slots[(hash as usize) & (self.size - 1)].lock()
    }
}

#[cfg(test)]
mod tests {
    use std::iter::once;

    use super::*;

    #[test]
    fn test_wakeup() {
        let latches = Latches::new(256);

        let keys_a = ["k1", "k3", "k5"];
        let mut lock_a = Lock::new(keys_a.iter());
        let keys_b = ["k4", "k5", "k6"];
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
        let wakeup = latches.release(&lock_a, cid_a, None);
        assert_eq!(wakeup[0], cid_b);

        // b acquire lock success
        acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);
    }

    #[test]
    fn test_wakeup_by_multi_cmds() {
        let latches = Latches::new(256);

        let keys_a = ["k1", "k2", "k3"];
        let keys_b = ["k4", "k5", "k6"];
        let keys_c = ["k3", "k4"];
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
        let wakeup = latches.release(&lock_a, cid_a, None);
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock failed again, cause b occupied slot 4
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // b release lock, and get wakeup list
        let wakeup = latches.release(&lock_b, cid_b, None);
        assert_eq!(wakeup[0], cid_c);

        // finally c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);
    }

    #[test]
    fn test_wakeup_by_small_latch_slot() {
        let latches = Latches::new(5);

        let keys_a = ["k1", "k2", "k3"];
        let keys_b = ["k6", "k7", "k8"];
        let keys_c = ["k3", "k4"];
        let keys_d = ["k7", "k10"];
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
        let wakeup = latches.release(&lock_a, cid_a, None);
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);

        // b release lock, and get wakeup list
        let wakeup = latches.release(&lock_b, cid_b, None);
        assert_eq!(wakeup[0], cid_d);

        // finally d acquire lock success
        acquired_d = latches.acquire(&mut lock_d, cid_d);
        assert_eq!(acquired_d, true);
    }

    fn check_latch_holder(latches: &Latches, key: &[u8], expected_holder_cid: Option<u64>) {
        let hash = Lock::hash(&key);
        let actual_holder = latches.lock_latch(hash).get_first_req_by_hash(hash);
        assert_eq!(actual_holder, expected_holder_cid);
    }

    fn is_latches_empty(latches: &Latches) -> bool {
        for i in 0..(latches.size as u64) {
            if !latches.lock_latch(i).waiting.iter().all(|x| x.is_none()) {
                return false;
            }
        }
        true
    }

    fn test_partially_releasing_impl(size: usize) {
        let latches = Latches::new(size);

        // Single key.
        let key = b"k1";
        let mut lock = Lock::new(once(key));
        assert!(latches.acquire(&mut lock, 1));
        assert!(!is_latches_empty(&latches));
        let mut lock2 = Lock::new(once(key));
        let wakeup = latches.release(&lock, 1, Some((2, &lock2)));
        assert!(wakeup.is_empty());
        check_latch_holder(&latches, key, Some(2));
        lock2.force_assume_acquired();
        let wakeup = latches.release(&lock2, 2, None);
        assert!(wakeup.is_empty());
        assert!(is_latches_empty(&latches));

        // Single key with queueing commands.
        let mut lock = Lock::new(once(key));
        let mut queueing_lock = Lock::new(once(key));
        assert!(latches.acquire(&mut lock, 3));
        assert!(!latches.acquire(&mut queueing_lock, 4));
        let mut lock2 = Lock::new(once(key));
        let wakeup = latches.release(&lock, 3, Some((5, &lock2)));
        assert!(wakeup.is_empty());
        check_latch_holder(&latches, key, Some(5));
        lock2.force_assume_acquired();
        let wakeup = latches.release(&lock2, 5, None);
        assert_eq!(wakeup, vec![4u64]);
        assert!(latches.acquire(&mut queueing_lock, 4));
        let wakeup = latches.release(&queueing_lock, 4, None);
        assert!(wakeup.is_empty());
        assert!(is_latches_empty(&latches));

        // Multi keys, keep all.
        let keys = vec![b"k1", b"k2", b"k3", b"k4"];
        let mut lock = Lock::new(keys.iter());
        assert!(latches.acquire(&mut lock, 11));
        let mut lock2 = Lock::new(keys.iter());
        let wakeup = latches.release(&lock, 11, Some((12, &lock2)));
        assert!(wakeup.is_empty());
        for &key in &keys {
            check_latch_holder(&latches, key, Some(12));
        }
        assert!(!is_latches_empty(&latches));
        lock2.force_assume_acquired();
        let wakeup = latches.release(&lock2, 12, None);
        assert!(wakeup.is_empty());
        assert!(is_latches_empty(&latches));

        // Multi keys, keep all, with queueing command.
        let mut lock = Lock::new(keys.iter());
        assert!(latches.acquire(&mut lock, 11));
        let mut queueing_locks: Vec<_> = keys.iter().map(|k| Lock::new(once(k))).collect();
        for (cid, lock) in (12..16).zip(queueing_locks.iter_mut()) {
            assert!(!latches.acquire(lock, cid));
        }
        let mut lock2 = Lock::new(keys.iter());
        let wakeup = latches.release(&lock, 11, Some((17, &lock2)));
        assert!(wakeup.is_empty());
        for &key in &keys {
            check_latch_holder(&latches, key, Some(17));
        }
        assert!(!is_latches_empty(&latches));
        lock2.force_assume_acquired();
        let mut wakeup = latches.release(&lock2, 17, None);
        wakeup.sort_unstable();
        // Wake up queueing commands.
        assert_eq!(wakeup, vec![12u64, 13, 14, 15]);
        for (cid, mut lock) in (12..16).zip(queueing_locks) {
            assert!(latches.acquire(&mut lock, cid));
            let wakeup = latches.release(&lock, cid, None);
            assert!(wakeup.is_empty());
        }
        assert!(is_latches_empty(&latches));

        // 4 keys, keep 2 of them.
        for (i1, &k1) in keys[0..3].iter().enumerate() {
            for &k2 in keys[i1 + 1..4].iter() {
                let mut lock = Lock::new(keys.iter());
                assert!(latches.acquire(&mut lock, 21));
                let mut lock2 = Lock::new(vec![k1, k2]);
                let wakeup = latches.release(&lock, 21, Some((22, &lock2)));
                assert!(wakeup.is_empty());
                check_latch_holder(&latches, k1, Some(22));
                check_latch_holder(&latches, k2, Some(22));
                lock2.force_assume_acquired();
                let wakeup = latches.release(&lock2, 22, None);
                assert!(wakeup.is_empty());
                assert!(is_latches_empty(&latches));
            }
        }

        // 4 keys keep 2 of them, with queueing commands.
        for (i1, &k1) in keys[0..3].iter().enumerate() {
            for (i2, &k2) in keys[i1 + 1..4].iter().enumerate() {
                let mut lock = Lock::new(keys.iter());
                assert!(latches.acquire(&mut lock, 21));

                let mut queueing_locks: Vec<_> = keys.iter().map(|k| Lock::new(once(k))).collect();
                for (cid, lock) in (22..26).zip(queueing_locks.iter_mut()) {
                    assert!(!latches.acquire(lock, cid));
                }

                let mut lock2 = Lock::new(vec![k1, k2]);
                let mut wakeup = latches.release(&lock, 21, Some((27, &lock2)));
                assert_eq!(wakeup.len(), 2);

                // The latch of k1 and k2 is preempted, and queueing locks on the other two keys
                // will be woken up.
                let preempted_cids = vec![(i1 + 22) as u64, (i1 + 1 + i2 + 22) as u64];
                let expected_wakeup_cids: Vec<_> = (22..26u64)
                    .filter(|x| !preempted_cids.contains(x))
                    .collect();
                wakeup.sort_unstable();
                assert_eq!(wakeup, expected_wakeup_cids);

                check_latch_holder(&latches, k1, Some(27));
                check_latch_holder(&latches, k2, Some(27));

                lock2.force_assume_acquired();
                let mut wakeup = latches.release(&lock2, 27, None);
                wakeup.sort_unstable();
                assert_eq!(wakeup, preempted_cids);

                for (cid, mut lock) in (22..26).zip(queueing_locks) {
                    assert!(latches.acquire(&mut lock, cid));
                    let wakeup = latches.release(&lock, cid, None);
                    assert!(wakeup.is_empty());
                }

                assert!(is_latches_empty(&latches));
            }
        }
    }

    #[test]
    fn test_partially_releasing() {
        test_partially_releasing_impl(256);
        test_partially_releasing_impl(4);
        test_partially_releasing_impl(2);
    }
}
