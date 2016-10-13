// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


#![allow(deprecated)]

use std::collections::VecDeque;
use std::hash::{Hash, SipHasher as DefaultHasher, Hasher};
use std::usize;

/// Latch which is used to serialize accesses to resources hashed to the same slot.
///
/// Latches are indexed by slot IDs. The keys of a command are hashed to slot IDs, then the command
/// is added to the waiting queues of the latches.
///
/// If command A is ahead of command B in one latch, it must be ahead of command B in all the
/// overlapping latches. This is an invariant ensured by the `gen_lock`, `acquire` and `release`.
#[derive(Clone)]
struct Latch {
    // store waiting commands
    pub waiting: VecDeque<u64>,
}

impl Latch {
    /// Creates a latch with an empty waiting queue.
    pub fn new() -> Latch {
        Latch { waiting: VecDeque::new() }
    }
}

/// Lock required for a command.
#[derive(Clone)]
pub struct Lock {
    /// The slot IDs of the latches that a command must acquire before being able to be processed.
    pub required_slots: Vec<usize>,

    /// The number of latches that the command has acquired.
    pub owned_count: usize,
}

impl Lock {
    /// Creates a lock.
    pub fn new(required_slots: Vec<usize>) -> Lock {
        Lock {
            required_slots: required_slots,
            owned_count: 0,
        }
    }

    /// Returns true if all the required latches have be acquired, false otherwise.
    pub fn acquired(&self) -> bool {
        self.required_slots.len() == self.owned_count
    }
}

/// Latches which are used for concurrency control in the scheduler.
///
/// Each latch is indexed by a slot ID, hence the term latch and slot are used interchangably, but
/// conceptually a latch is a queue, and a slot is an index to the queue.
pub struct Latches {
    slots: Vec<Latch>,
    size: usize,
}

impl Latches {
    /// Creates latches.
    ///
    /// The size will be rounded up to the power of 2.
    pub fn new(size: usize) -> Latches {
        let power_of_two_size = usize::next_power_of_two(size);
        Latches {
            slots: vec![Latch::new(); power_of_two_size],
            size: power_of_two_size,
        }
    }

    /// Creates a lock which specifies all the required latches for a command.
    pub fn gen_lock<H>(&self, keys: &[H]) -> Lock
        where H: Hash
    {
        // prevent from deadlock, so we sort and deduplicate the index
        let mut slots: Vec<usize> = keys.iter().map(|x| self.calc_slot(x)).collect();
        slots.sort();
        slots.dedup();
        Lock::new(slots)
    }

    /// Tries to acquire the latches specified by the `lock` for command with ID `who`.
    ///
    /// This method will enqueue the command ID into the waiting queues of the latches. A latch is
    /// considered acquired if the command ID is at the front of the queue. Returns true if all the
    /// Latches are acquired, false otherwise.
    pub fn acquire(&mut self, lock: &mut Lock, who: u64) -> bool {
        let mut acquired_count: usize = 0;
        for i in &lock.required_slots[lock.owned_count..] {
            let latch = &mut self.slots[*i];

            let front = latch.waiting.front().cloned();
            match front {
                Some(cid) => {
                    if cid == who {
                        acquired_count += 1;
                    } else {
                        latch.waiting.push_back(who);
                        break;
                    }
                }
                None => {
                    latch.waiting.push_back(who);
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
    pub fn release(&mut self, lock: &Lock, who: u64) -> Vec<u64> {
        let mut wakeup_list: Vec<u64> = vec![];
        for i in &lock.required_slots[..lock.owned_count] {
            let latch = &mut self.slots[*i];
            let front = latch.waiting.pop_front().unwrap();
            assert_eq!(front, who);

            if let Some(wakeup) = latch.waiting.front() {
                wakeup_list.push(*wakeup);
            }
        }
        wakeup_list
    }

    /// Calculates the slot ID by hashing the `key`.
    fn calc_slot<H>(&self, key: &H) -> usize
        where H: Hash
    {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) & (self.size - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::{Latches, Lock};

    #[test]
    fn test_wakeup() {
        let mut latches = Latches::new(256);

        let slots_a: Vec<usize> = vec![1, 3, 5];
        let mut lock_a = Lock::new(slots_a);
        let slots_b: Vec<usize> = vec![4, 5, 6];
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
        let mut latches = Latches::new(256);

        let slots_a: Vec<usize> = vec![1, 2, 3];
        let slots_b: Vec<usize> = vec![4, 5, 6];
        let slots_c: Vec<usize> = vec![3, 4];
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
}
