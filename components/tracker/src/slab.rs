// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{array, cell::Cell, fmt};

use lazy_static::lazy_static;
use parking_lot::Mutex;
use slab::Slab;

use crate::{metrics::*, Tracker};

const SLAB_SHARD_BITS: u32 = 6;
const SLAB_SHARD_COUNT: usize = 1 << SLAB_SHARD_BITS; // 64
const SLAB_SHARD_INIT_CAPACITY: usize = 256;
const SLAB_SHARD_MAX_CAPACITY: usize = 4096;

lazy_static! {
    pub static ref GLOBAL_TRACKERS: ShardedSlab = ShardedSlab::new(SLAB_SHARD_INIT_CAPACITY);
}

fn next_shard_id() -> usize {
    thread_local! {
        static CURRENT_SHARD_ID: Cell<usize> = Cell::new(0);
    }
    CURRENT_SHARD_ID.with(|c| {
        let shard_id = c.get();
        c.set((shard_id + 1) % SLAB_SHARD_COUNT);
        shard_id
    })
}

pub struct ShardedSlab {
    shards: [Mutex<TrackerSlab>; SLAB_SHARD_COUNT],
}

impl ShardedSlab {
    pub fn new(capacity_per_shard: usize) -> ShardedSlab {
        let shards = array::from_fn(|shard_id| {
            Mutex::new(TrackerSlab::with_capacity(
                shard_id as u32,
                capacity_per_shard,
            ))
        });
        ShardedSlab { shards }
    }

    pub fn insert(&self, tracker: Tracker) -> TrackerToken {
        let shard_id = next_shard_id();
        self.shards[shard_id].lock().insert(tracker)
    }

    pub fn remove(&self, token: TrackerToken) -> Option<Tracker> {
        if token != INVALID_TRACKER_TOKEN {
            let shard_id = token.shard_id();
            self.shards[shard_id as usize].lock().remove(token)
        } else {
            None
        }
    }

    pub fn with_tracker<F, T>(&self, token: TrackerToken, f: F) -> Option<T>
    where
        F: FnOnce(&mut Tracker) -> T,
    {
        if token != INVALID_TRACKER_TOKEN {
            let shard_id = token.shard_id();
            self.shards[shard_id as usize].lock().get_mut(token).map(f)
        } else {
            None
        }
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&mut Tracker),
    {
        for shard in &self.shards {
            for (_, tracker) in shard.lock().slab.iter_mut() {
                f(&mut tracker.tracker)
            }
        }
    }
}

const SLAB_KEY_BITS: u32 = 32;
const SHARD_ID_BITS_SHIFT: u32 = 64 - SLAB_SHARD_BITS;
const SEQ_BITS_MASK: u32 = (1 << (SHARD_ID_BITS_SHIFT - SLAB_KEY_BITS)) - 1;

struct TrackerSlab {
    slab: Slab<SlabEntry>,
    shard_id: u32,
    seq: u32,
}

impl TrackerSlab {
    fn with_capacity(shard_id: u32, capacity: usize) -> Self {
        assert!(capacity < SLAB_SHARD_MAX_CAPACITY);
        TrackerSlab {
            slab: Slab::with_capacity(capacity),
            shard_id,
            seq: 0,
        }
    }

    // Returns the seq and key of the inserted tracker.
    // If the slab reaches the max capacity, the tracker will be dropped silently
    // and INVALID_TRACKER_TOKEN will be returned.
    fn insert(&mut self, tracker: Tracker) -> TrackerToken {
        if self.slab.len() < SLAB_SHARD_MAX_CAPACITY {
            self.seq = (self.seq + 1) & SEQ_BITS_MASK;
            let key = self.slab.insert(SlabEntry {
                tracker,
                seq: self.seq,
            });
            TrackerToken::new(self.shard_id, self.seq, key)
        } else {
            SLAB_FULL_COUNTER.inc();
            INVALID_TRACKER_TOKEN
        }
    }

    pub fn get_mut(&mut self, token: TrackerToken) -> Option<&mut Tracker> {
        if let Some(entry) = self.slab.get_mut(token.key()) {
            if entry.seq == token.seq() {
                return Some(&mut entry.tracker);
            }
        }
        None
    }

    pub fn remove(&mut self, token: TrackerToken) -> Option<Tracker> {
        if self.get_mut(token).is_some() {
            Some(self.slab.remove(token.key()).tracker)
        } else {
            None
        }
    }
}

struct SlabEntry {
    tracker: Tracker,
    seq: u32,
}

pub const INVALID_TRACKER_TOKEN: TrackerToken = TrackerToken(u64::MAX);

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct TrackerToken(u64);

impl TrackerToken {
    fn new(shard_id: u32, seq: u32, key: usize) -> TrackerToken {
        debug_assert!(shard_id < SLAB_SHARD_COUNT as u32);
        debug_assert!(seq <= SEQ_BITS_MASK);
        debug_assert!(key < (1 << SLAB_KEY_BITS));
        TrackerToken(
            ((shard_id as u64) << SHARD_ID_BITS_SHIFT)
                | ((seq as u64) << SLAB_KEY_BITS)
                | (key as u64),
        )
    }

    fn shard_id(&self) -> u32 {
        (self.0 >> SHARD_ID_BITS_SHIFT) as u32
    }

    fn seq(&self) -> u32 {
        (self.0 >> SLAB_KEY_BITS) as u32 & SEQ_BITS_MASK
    }

    fn key(&self) -> usize {
        (self.0 & ((1 << SLAB_KEY_BITS) - 1)) as usize
    }
}

impl fmt::Debug for TrackerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackerToken")
            .field("shard_id", &self.shard_id())
            .field("seq", &self.seq())
            .field("key", &self.key())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;
    use crate::RequestInfo;

    #[test]
    fn test_tracker_token() {
        let shard_id = 47;
        let seq = SEQ_BITS_MASK - 3;
        let key = 65535;
        let token = TrackerToken::new(shard_id, seq, key);
        assert_eq!(token.shard_id(), shard_id);
        assert_eq!(token.seq(), seq);
        assert_eq!(token.key(), key);
    }

    #[test]
    fn test_basic() {
        let slab = ShardedSlab::new(2);
        // Insert 192 trackers
        let tokens: Vec<TrackerToken> = (0..192)
            .map(|i| {
                let tracker = Tracker::new(RequestInfo {
                    task_id: i,
                    ..Default::default()
                });
                slab.insert(tracker)
            })
            .collect();
        // Get the tracker with the token and check the content
        for (i, token) in tokens.iter().enumerate() {
            slab.with_tracker(*token, |tracker| {
                assert_eq!(i as u64, tracker.req_info.task_id);
            });
        }
        // Remove 0 ~ 128 trackers
        for (i, token) in tokens[..128].iter().enumerate() {
            let tracker = slab.remove(*token).unwrap();
            assert_eq!(i as u64, tracker.req_info.task_id);
        }
        // Insert another 192 trackers
        for i in 192..384 {
            let tracker = Tracker::new(RequestInfo {
                task_id: i,
                ..Default::default()
            });
            slab.insert(tracker);
        }
        // Iterate over all trackers in the slab
        let mut tracker_ids = Vec::new();
        slab.for_each(|tracker| tracker_ids.push(tracker.req_info.task_id));
        tracker_ids.sort_unstable();
        assert_eq!(tracker_ids, (128..384).collect::<Vec<_>>());
    }

    #[test]
    fn test_shard() {
        let slab = Arc::new(ShardedSlab::new(4));
        let threads = [1, 2].map(|i| {
            let slab = slab.clone();
            thread::spawn(move || {
                for _ in 0..SLAB_SHARD_COUNT {
                    slab.insert(Tracker::new(RequestInfo {
                        task_id: i,
                        ..Default::default()
                    }));
                }
            })
        });
        for th in threads {
            th.join().unwrap();
        }
        for shard in &slab.shards {
            let mut v: Vec<_> = shard
                .lock()
                .slab
                .iter()
                .map(|(_, entry)| entry.tracker.req_info.task_id)
                .collect();
            v.sort_unstable();
            assert_eq!(v, [1, 2]);
        }
    }
}
