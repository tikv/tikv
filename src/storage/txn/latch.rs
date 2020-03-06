// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::DefaultHasher;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::usize;

use parking_lot::{Mutex, MutexGuard};

const WAITING_LIST_SHRINK_SIZE: usize = 8;
const WAITING_LIST_MAX_CAPACITY: usize = 16;

/// Latch which is used to serialize accesses to resources hashed to the same slot.
///
/// Latches are indexed by slot IDs. The keys of a command are hashed into unsigned numbers,
/// then the command is added to the waiting queues of the latches.
///
/// If command A is ahead of command B in one latch, it must be ahead of command B in all the
/// overlapping latches. This is an invariant ensured by the `gen_lock`, `acquire` and `release`.
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

    /// Find the first command ID in the queue whose hash value is equal to hash.
    pub fn get_first_req_by_hash(&self, hash: u64) -> Option<u64> {
        for item in self.waiting.iter() {
            if let Some((h, cid)) = item {
                if *h == hash {
                    return Some(*cid);
                }
            }
        }
        None
    }

    /// Remove the first command ID in the queue whose hash value is equal to hash_key.
    /// If the element which would be removed does not appear at the front of the queue, it will leave
    /// a hole in the queue. So we must remove consecutive hole when remove the head of the
    /// queue to make the queue not too long.
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

    /// For some hot keys, the waiting list maybe very long, so we should shrink the waiting
    /// VecDeque after pop.
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
    /// The hash value of the keys that a command must acquire before being able to be processed.
    pub required_hashes: Vec<u64>,

    /// The number of latches that the command has acquired.
    pub owned_count: usize,
}

impl Lock {
    /// Creates a lock.
    pub fn new(required_hashes: Vec<u64>) -> Lock {
        Lock {
            required_hashes,
            owned_count: 0,
        }
    }

    /// Returns true if all the required latches have be acquired, false otherwise.
    pub fn acquired(&self) -> bool {
        self.required_hashes.len() == self.owned_count
    }

    pub fn is_write_lock(&self) -> bool {
        !self.required_hashes.is_empty()
    }
}

/// Latches which are used for concurrency control in the scheduler.
///
/// Each latch is indexed by a slot ID, hence the term latch and slot are used interchangeably, but
/// conceptually a latch is a queue, and a slot is an index to the queue.
pub struct Latches {
    slots: Vec<Mutex<Latch>>,
    size: usize,
}

impl Latches {
    /// Creates latches.
    ///
    /// The size will be rounded up to the power of 2.
    pub fn new(size: usize) -> Latches {
        let size = usize::next_power_of_two(size);
        let mut slots = Vec::with_capacity(size);
        (0..size).for_each(|_| slots.push(Mutex::new(Latch::new())));
        Latches { slots, size }
    }

    /// Creates a lock which specifies all the required latches for a command.
    pub fn gen_lock<H>(&self, keys: &[H]) -> Lock
    where
        H: Hash,
    {
        // prevent from deadlock, so we sort and deduplicate the index
        let mut hashes: Vec<u64> = keys.iter().map(|x| self.calc_slot(x)).collect();
        hashes.sort();
        hashes.dedup();
        Lock::new(hashes)
    }

    /// Tries to acquire the latches specified by the `lock` for command with ID `who`.
    ///
    /// This method will enqueue the command ID into the waiting queues of the latches. A latch is
    /// considered acquired if the command ID is the first one of elements in the queue which have
    /// the same hash value. Returns true if all the Latches are acquired, false otherwise.
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

    /// Releases all latches owned by the `lock` of command with ID `who`, returns the wakeup list.
    ///
    /// Preconditions: the caller must ensure the command is at the front of the latches.
    pub fn release(&self, lock: &Lock, who: u64) -> Vec<u64> {
        let mut wakeup_list: Vec<u64> = vec![];
        for &key_hash in &lock.required_hashes[..lock.owned_count] {
            let mut latch = self.lock_latch(key_hash);
            let (v, front) = latch.pop_front(key_hash).unwrap();
            assert_eq!(front, who);
            assert_eq!(v, key_hash);
            if let Some(wakeup) = latch.get_first_req_by_hash(key_hash) {
                wakeup_list.push(wakeup);
            }
        }
        wakeup_list
    }

    /// Calculates the hash value of the `key`.
    fn calc_slot<H>(&self, key: &H) -> u64
    where
        H: Hash,
    {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);

        s.finish()
    }

    #[inline]
    fn lock_latch(&self, hash: u64) -> MutexGuard<Latch> {
        self.slots[(hash as usize) & (self.size - 1)].lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wakeup() {
        let latches = Latches::new(256);

        let slots_a: Vec<u64> = vec![1, 3, 5];
        let mut lock_a = Lock::new(slots_a);
        let slots_b: Vec<u64> = vec![4, 5, 6];
        let mut lock_b = Lock::new(slots_b);
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;

        // a acquire lock success
        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // b acquire lock failed
        let mut acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(&lock_a, cid_a);
        assert_eq!(wakeup[0], cid_b);

        // b acquire lock success
        acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);
    }

    #[test]
    fn test_wakeup_by_multi_cmds() {
        let latches = Latches::new(256);

        let slots_a: Vec<u64> = vec![1, 2, 3];
        let slots_b: Vec<u64> = vec![4, 5, 6];
        let slots_c: Vec<u64> = vec![3, 4];
        let mut lock_a = Lock::new(slots_a);
        let mut lock_b = Lock::new(slots_b);
        let mut lock_c = Lock::new(slots_c);
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
        let wakeup = latches.release(&lock_a, cid_a);
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock failed again, cause b occupied slot 4
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // b release lock, and get wakeup list
        let wakeup = latches.release(&lock_b, cid_b);
        assert_eq!(wakeup[0], cid_c);

        // finally c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);
    }

    #[test]
    fn test_wakeup_by_small_latch_slot() {
        let latches = Latches::new(5);

        let slots_a: Vec<u64> = vec![1, 2, 3];
        let slots_b: Vec<u64> = vec![6, 7, 8];
        let slots_c: Vec<u64> = vec![3, 4];
        let slots_d: Vec<u64> = vec![7, 10];
        let mut lock_a = Lock::new(slots_a);
        let mut lock_b = Lock::new(slots_b);
        let mut lock_c = Lock::new(slots_c);
        let mut lock_d = Lock::new(slots_d);
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
        let wakeup = latches.release(&lock_a, cid_a);
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);

        // b release lock, and get wakeup list
        let wakeup = latches.release(&lock_b, cid_b);
        assert_eq!(wakeup[0], cid_d);

        // finally d acquire lock success
        acquired_d = latches.acquire(&mut lock_d, cid_d);
        assert_eq!(acquired_d, true);
    }
}
