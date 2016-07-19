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


use std::collections::{VecDeque, BTreeSet};
use std::hash::{Hash, SipHasher, Hasher};

// simulate lock of one row
pub struct RowLock {
    // store waiting commands
    pub waiting: VecDeque<u64>,

    // use to check existed
    pub set: BTreeSet<u64>,
}

impl RowLock {
    pub fn new() -> RowLock {
        RowLock {
            waiting: VecDeque::new(),
            set: BTreeSet::new(),
        }
    }
}

pub struct MemRowLocks {
    locks: Vec<RowLock>,
    size: usize,
}

impl MemRowLocks {
    pub fn new(size: usize) -> MemRowLocks {
        MemRowLocks {
            locks: (0..size).map(|_|  RowLock::new()).collect(),
            size: size,
        }
    }

    pub fn calc_lock_indexs<H>(&self, keys: &[H]) -> Vec<usize>
        where H: Hash
    {
        let mut indices: Vec<usize> = keys.iter().map(|x| self.calc_index(x)).collect();

        // prevent from deadlock, so we sort and deduplicate the index
        indices.sort();
        indices.dedup();
        indices
    }

    pub fn acquire_by_indexs(&mut self, indexs: &[usize], who: u64) -> usize {

        let mut acquired_count: usize = 0;
        for i in indexs {
            let rowlock = &mut self.locks.get_mut(*i).unwrap();

            let mut front: Option<u64> = None;
            if let Some(cid) = rowlock.waiting.front() {
                front = Some(*cid);
            }

            match front {
                Some(cid) => {
                    if cid == who {
                        acquired_count += 1;
                    } else {
                        if !rowlock.set.contains(&who) {
                            rowlock.waiting.push_back(who);
                            rowlock.set.insert(who);
                        }
                        return acquired_count;
                    }
                }
                None => {
                    rowlock.waiting.push_back(who);
                    rowlock.set.insert(who);
                    acquired_count += 1;
                }
            }
        }

        acquired_count
    }

    // release all locks owned, and return wakeup list
    pub fn release_by_indexs(&mut self, indexs: &[usize], who: u64) -> Vec<u64> {
        let mut wakeup_list: Vec<u64> = vec![];

        for i in indexs {
            let rowlock = &mut self.locks.get_mut(*i).unwrap();

            match rowlock.waiting.pop_front() {
                Some(head) => {
                    if head != who {
                        panic!("release lock not owned");
                    }
                }
                None => {
                    panic!("should not happen");
                }
            }
            rowlock.set.remove(&who);

            if let Some(wakeup) = rowlock.waiting.front() {
                wakeup_list.push(*wakeup);
            }
        }

        wakeup_list
    }

    fn calc_index<H>(&self, key: &H) -> usize
        where H: Hash
    {
        let mut s = SipHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) % self.size
    }
}

#[cfg(test)]
mod tests {
    use super::MemRowLocks;

    #[test]
    fn test_wakeup_cmd() {
        let mut mem_rowlocks = MemRowLocks::new(256);

        let indexs_a: Vec<usize> = vec![1, 3, 5];
        let indexs_b: Vec<usize> = vec![4, 5, 6];
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;

        let acquired_a: usize = mem_rowlocks.acquire_by_indexs(&indexs_a, cid_a);
        assert_eq!(acquired_a, 3);

        let acquired_b: usize = mem_rowlocks.acquire_by_indexs(&indexs_b, cid_b);
        assert_eq!(acquired_b, 1);

        let wakeup = mem_rowlocks.release_by_indexs(&indexs_a, cid_a);
        assert_eq!(wakeup[0], cid_b);
    }
}
