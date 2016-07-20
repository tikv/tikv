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

struct Latch {
    // store waiting commands
    pub waiting: VecDeque<u64>,

    // use to check existed
    pub set: BTreeSet<u64>,
}

impl Latch {
    pub fn new() -> Latch {
        Latch {
            waiting: VecDeque::new(),
            set: BTreeSet::new(),
        }
    }
}

pub struct Latches {
    slots: Vec<Latch>,
    size: usize,
}

impl Latches {
    pub fn new(size: usize) -> Latches {
        Latches {
            slots: (0..size).map(|_|  Latch::new()).collect(),
            size: size,
        }
    }

    pub fn calc_slots<H>(&self, keys: &[H]) -> Vec<usize>
        where H: Hash {
        // prevent from deadlock, so we sort and deduplicate the index
        let mut slots: Vec<usize> = keys.iter().map(|x| self.calc_slot(x)).collect();
        slots.sort();
        slots.dedup();
        slots
    }

    pub fn acquire(&mut self, slots: &[usize], who: u64) -> usize {
        let mut acquired_count: usize = 0;
        for i in slots {
            let latch = &mut self.slots.get_mut(*i).unwrap();

            let mut front: Option<u64> = None;
            if let Some(cid) = latch.waiting.front() {
                front = Some(*cid);
            }

            match front {
                Some(cid) => {
                    if cid == who {
                        acquired_count += 1;
                    } else {
                        if !latch.set.contains(&who) {
                            latch.waiting.push_back(who);
                            latch.set.insert(who);
                        }
                        return acquired_count;
                    }
                }
                None => {
                    latch.waiting.push_back(who);
                    latch.set.insert(who);
                    acquired_count += 1;
                }
            }
        }

        acquired_count
    }

    // release all latches owned, and return wakeup list
    pub fn release(&mut self, slots: &[usize], who: u64) -> Vec<u64> {
        let mut wakeup_list: Vec<u64> = vec![];
        for i in slots {
            let latch = &mut self.slots.get_mut(*i).unwrap();
            let front = latch.waiting.pop_front().unwrap();
            assert_eq!(front, who);
            latch.set.remove(&who);

            if let Some(wakeup) = latch.waiting.front() {
                wakeup_list.push(*wakeup);
            }
        }
        wakeup_list
    }

    fn calc_slot<H>(&self, key: &H) -> usize
        where H: Hash
    {
        let mut s = SipHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) % self.size
    }
}

#[cfg(test)]
mod tests {
    use super::Latches;

    #[test]
    fn test_wakeup_cmd() {
        let mut latches = Latches::new(256);

        let slots_a: Vec<usize> = vec![1, 3, 5];
        let slots_b: Vec<usize> = vec![4, 5, 6];
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;

        let acquired_a: usize = latches.acquire(&slots_a, cid_a);
        assert_eq!(acquired_a, 3);

        let acquired_b: usize = latches.acquire(&slots_b, cid_b);
        assert_eq!(acquired_b, 1);

        let wakeup = latches.release(&slots_a, cid_a);
        assert_eq!(wakeup[0], cid_b);
    }
}
