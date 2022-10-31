// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::UnsafeCell,
    fmt, mem,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        OnceLock,
    },
};

use crossbeam_utils::CachePadded;

use crate::{metrics::*, Tracker};

const DEFAULT_SLAB_CAPACITY: usize = 65536;

const SLAB_SHARD_BITS: u32 = 6;
const SLAB_SHARD_COUNT: usize = 1 << SLAB_SHARD_BITS; // 64
const SLAB_SHARD_INIT_CAPACITY: usize = 256;
const SLAB_SHARD_MAX_CAPACITY: usize = 4096;

pub static GLOBAL_TRACKERS: GlobalTrackers = GlobalTrackers;
static GLOBAL_TRACKER_SLAB: OnceLock<TrackerSlab> = OnceLock::new();

pub struct GlobalTrackers;

impl GlobalTrackers {
    pub fn init(&self, capacity_pow_of_two: u32) {
        let _ = GLOBAL_TRACKER_SLAB.set(TrackerSlab::new(capacity_pow_of_two));
    }

    #[inline(always)]
    pub fn allocate(&self) -> TrackerToken {
        GLOBAL_TRACKER_SLAB
            .get()
            .map(|slab| slab.allocate())
            .unwrap_or(INVALID_TRACKER_TOKEN)
    }

    #[inline(always)]
    pub fn remove(&self, token: TrackerToken) -> Option<Tracker> {
        GLOBAL_TRACKER_SLAB
            .get()
            .and_then(|slab| slab.remove(token))
    }

    #[inline(always)]
    pub fn with_tracker<F, T>(&self, token: TrackerToken, f: F) -> Option<T>
    where
        F: FnOnce(&Tracker) -> T,
    {
        GLOBAL_TRACKER_SLAB.get().and_then(|slab| {
            if token != INVALID_TRACKER_TOKEN {
                let entry = &slab.entries[token.key()];
                if entry.seq.load(Ordering::Acquire) == token.seq() {
                    return Some(f(unsafe { &*entry.tracker.get() }));
                }
            }
            None
        })
    }
}

pub struct TrackerSlab {
    entries: Vec<CachePadded<SlabEntry>>,
    free_list: Vec<AtomicU32>,
    cursors: AtomicU64,
    mask: u64,
}

impl TrackerSlab {
    pub fn new(capacity_pow_of_two: u32) -> Self {
        let capacity = 1 << capacity_pow_of_two;
        let entries = (0..capacity)
            .map(|i| {
                CachePadded::new(SlabEntry {
                    tracker: UnsafeCell::new(Tracker::default()),
                    seq: AtomicU32::new(0),
                })
            })
            .collect();
        let free_list = (0..capacity).map(|i| AtomicU32::new(i as u32)).collect();
        let mask = 1u64.wrapping_shl(capacity_pow_of_two).wrapping_sub(1);
        TrackerSlab {
            entries,
            free_list,
            cursors: AtomicU64::new(1),
            mask,
        }
    }

    pub fn allocate(&self) -> TrackerToken {
        let mut cursors = self.cursors.load(Ordering::Acquire);
        loop {
            let (head, tail) = (cursors & ((1 << 32) - 1), cursors >> 32);
            if head & self.mask == tail & self.mask {
                return INVALID_TRACKER_TOKEN;
            }
            let new_head = head + 1;
            let new_cursors = (new_head & ((1 << 32) - 1)) | (tail << 32);
            match self.cursors.compare_exchange_weak(
                cursors,
                new_cursors,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let index = self.free_list[(head & self.mask) as usize]
                        .swap(u32::MAX, Ordering::AcqRel) as usize;
                    let seq = self.entries[index].seq.load(Ordering::Acquire);
                    return TrackerToken::new(index, seq);
                }
                Err(new_cursors) => cursors = new_cursors,
            }
        }
    }

    pub fn remove(&self, token: TrackerToken) -> Option<Tracker> {
        let key = token.key();
        let seq = token.seq();
        if token != INVALID_TRACKER_TOKEN
            && self.entries[key]
                .seq
                .compare_exchange(
                    seq,
                    seq.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            let tracker = unsafe { mem::take(&mut *self.entries[key].tracker.get()) };
            let mut cursors = self.cursors.load(Ordering::Acquire);
            loop {
                let (head, tail) = (cursors & ((1 << 32) - 1), cursors >> 32);
                let new_tail = tail + 1;
                let new_cursors = (head & ((1 << 32) - 1)) | (new_tail << 32);
                match self.free_list[(new_tail & self.mask) as usize].compare_exchange_weak(
                    u32::MAX,
                    key as u32,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.cursors.fetch_add(1 << 32, Ordering::Release);
                        return Some(tracker);
                    }
                    Err(_) => cursors = self.cursors.load(Ordering::Acquire),
                }
            }
        } else {
            None
        }
    }
}

unsafe impl Sync for TrackerSlab {}

struct SlabEntry {
    tracker: UnsafeCell<Tracker>,
    seq: AtomicU32,
}

pub const INVALID_TRACKER_TOKEN: TrackerToken = TrackerToken(u64::MAX);

#[derive(Clone, Copy, PartialEq)]
pub struct TrackerToken(u64);

impl TrackerToken {
    fn new(key: usize, seq: u32) -> TrackerToken {
        TrackerToken(((seq as u64) << 32) | (key as u64))
    }

    fn seq(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    fn key(&self) -> usize {
        (self.0 & ((1u64 << 32) - 1)) as usize
    }
}

impl fmt::Debug for TrackerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackerToken")
            .field("seq", &self.seq())
            .field("key", &self.key())
            .finish()
    }
}

impl Default for TrackerToken {
    fn default() -> Self {
        INVALID_TRACKER_TOKEN
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;
    use crate::RequestInfo;

    // #[test]
    // fn test_tracker_token() {
    //     let shard_id = 47;
    //     let seq = SEQ_BITS_MASK - 3;
    //     let key = 65535;
    //     let token = TrackerToken::new(shard_id, seq, key);
    //     assert_eq!(token.shard_id(), shard_id);
    //     assert_eq!(token.seq(), seq);
    //     assert_eq!(token.key(), key);
    // }

    // #[test]
    // fn test_basic() {
    //     let slab = ShardedSlab::new(2);
    //     // Insert 192 trackers
    //     let tokens: Vec<TrackerToken> = (0..192)
    //         .map(|i| {
    //             let tracker = Tracker::new(RequestInfo {
    //                 task_id: i,
    //                 ..Default::default()
    //             });
    //             slab.insert(tracker)
    //         })
    //         .collect();
    //     // Get the tracker with the token and check the content
    //     for (i, token) in tokens.iter().enumerate() {
    //         slab.with_tracker(*token, |tracker| {
    //             assert_eq!(i as u64, tracker.req_info.task_id);
    //         });
    //     }
    //     // Remove 0 ~ 128 trackers
    //     for (i, token) in tokens[..128].iter().enumerate() {
    //         let tracker = slab.remove(*token).unwrap();
    //         assert_eq!(i as u64, tracker.req_info.task_id);
    //     }
    //     // Insert another 192 trackers
    //     for i in 192..384 {
    //         let tracker = Tracker::new(RequestInfo {
    //             task_id: i,
    //             ..Default::default()
    //         });
    //         slab.insert(tracker);
    //     }
    //     // Iterate over all trackers in the slab
    //     let mut tracker_ids = Vec::new();
    //     slab.for_each(|tracker| tracker_ids.push(tracker.req_info.task_id));
    //     tracker_ids.sort_unstable();
    //     assert_eq!(tracker_ids, (128..384).collect::<Vec<_>>());
    // }

    // #[test]
    // fn test_shard() {
    //     let slab = Arc::new(ShardedSlab::new(4));
    //     let threads = [1, 2].map(|i| {
    //         let slab = slab.clone();
    //         thread::spawn(move || {
    //             for _ in 0..SLAB_SHARD_COUNT {
    //                 slab.insert(Tracker::new(RequestInfo {
    //                     task_id: i,
    //                     ..Default::default()
    //                 }));
    //             }
    //         })
    //     });
    //     for th in threads {
    //         th.join().unwrap();
    //     }
    //     for shard in &slab.shards {
    //         let mut v: Vec<_> = shard
    //             .lock()
    //             .slab
    //             .iter()
    //             .map(|(_, entry)| entry.tracker.req_info.task_id)
    //             .collect();
    //         v.sort_unstable();
    //         assert_eq!(v, [1, 2]);
    //     }
    // }
}
