// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam::utils::CachePadded;
use parking_lot::RwLock;
use tikv_util::{
    lru,
    lru::{GetTailKV, LruCache},
};
use txn_types::TimeStamp;

use crate::storage::metrics::SCHED_TXN_STATUS_CACHE_SIZE;

const TXN_STATUS_CACHE_SLOTS: usize = 128;

pub const REQ_MAX_FLYING_TIME_MILLIS: u64 = 30000;

struct CacheEntry {
    commit_ts: TimeStamp,
    insert_time: u64,
}

struct TxnStatusCacheEvictPolicy {
    limit_millis: u64,
}

impl lru::EvictPolicy<TimeStamp, CacheEntry> for TxnStatusCacheEvictPolicy {
    fn should_evict(
        &self,
        current_size: usize,
        capacity: usize,
        get_tail_kv: &impl GetTailKV<TimeStamp, CacheEntry>,
    ) -> bool {
        if current_size < capacity {
            return false;
        }

        if let Some((_, v)) = get_tail_kv.get_tail_kv() {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                - v.insert_time
                > self.limit_millis
        } else {
            true
        }
    }
}

struct TxnStatusCacheSlot {
    cache: LruCache<TimeStamp, CacheEntry, lru::CountTracker, TxnStatusCacheEvictPolicy>,
    last_check_size: usize,
    last_check_capacity: usize,
}

pub struct TxnStatusCache {
    slots: Vec<CachePadded<RwLock<TxnStatusCacheSlot>>>,
}

unsafe impl Sync for TxnStatusCache {}

impl TxnStatusCache {
    pub fn new() -> Self {
        Self::with_slots_and_time_limit(TXN_STATUS_CACHE_SLOTS, REQ_MAX_FLYING_TIME_MILLIS)
    }

    #[cfg(test)]
    pub fn new_for_test() -> Self {
        Self::with_slots_and_time_limit(16, REQ_MAX_FLYING_TIME_MILLIS)
    }

    pub fn with_slots_and_time_limit(slots: usize, limit_millis: u64) -> Self {
        let mut initial_capacity = 0;
        let res = Self {
            slots: (0..slots)
                .map(|_| {
                    let cache = LruCache::new(
                        16,
                        0,
                        lru::CountTracker::default(),
                        TxnStatusCacheEvictPolicy { limit_millis },
                    );
                    let capacity = cache.internal_mem_capacity();
                    initial_capacity += capacity;
                    RwLock::new(TxnStatusCacheSlot {
                        cache,
                        last_check_size: 0,
                        last_check_capacity: capacity,
                    })
                    .into()
                })
                .collect(),
        };
        SCHED_TXN_STATUS_CACHE_SIZE
            .allocated
            .set(initial_capacity as i64);
        res
    }

    fn slot_index(start_ts: TimeStamp) -> usize {
        fxhash::hash(&start_ts) % TXN_STATUS_CACHE_SLOTS
    }

    pub fn insert(&self, start_ts: TimeStamp, commit_ts: TimeStamp, now: SystemTime) {
        let mut slot = self.slots[Self::slot_index(start_ts)].write();
        let insert_time = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        slot.cache.insert_if_not_exist(
            start_ts,
            CacheEntry {
                commit_ts,
                insert_time,
            },
        );
        let size = slot.cache.size();
        let capacity = slot.cache.internal_mem_capacity();
        // Update statistics.
        // CAUTION: Assuming that only one TxnStatusCache instance is in a TiKV process.
        SCHED_TXN_STATUS_CACHE_SIZE
            .used
            .add(size as i64 - slot.last_check_size as i64);
        SCHED_TXN_STATUS_CACHE_SIZE
            .allocated
            .add(capacity as i64 - slot.last_check_capacity as i64);
        slot.last_check_size = size;
        slot.last_check_capacity = capacity;
    }

    pub fn get(&self, start_ts: TimeStamp) -> Option<TimeStamp> {
        let slot = self.slots[Self::slot_index(start_ts)].read();
        slot.cache
            .get_no_promote(&start_ts)
            .map(|entry| entry.commit_ts)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use rand::Rng;

    use super::*;

    fn bench_insert_impl(b: &mut test::Bencher, init_size: usize) {
        let c = TxnStatusCache::with_slots_and_time_limit(TXN_STATUS_CACHE_SLOTS, init_size as u64);
        let now = SystemTime::now();
        for i in 1..=init_size {
            c.insert(
                (i as u64).into(),
                (i as u64 + 1).into(),
                now + Duration::from_millis(i as u64),
            );
        }
        let mut current_time_shift = (init_size + 1) as u64;
        b.iter(|| {
            c.insert(
                current_time_shift.into(),
                (current_time_shift + 1).into(),
                now + Duration::from_millis(current_time_shift),
            );
            current_time_shift += 1;
        });
        test::black_box(&c);
    }

    fn bench_get_impl(b: &mut test::Bencher, init_size: usize) {
        let c = TxnStatusCache::with_slots_and_time_limit(
            TXN_STATUS_CACHE_SLOTS,
            REQ_MAX_FLYING_TIME_MILLIS,
        );
        let now = SystemTime::now();
        for i in 1..=init_size {
            c.insert(
                (i as u64).into(),
                (i as u64 + 1).into(),
                now + Duration::from_millis(i as u64),
            );
        }
        let rand_range = if init_size == 0 { 10000 } else { init_size } as u64;
        b.iter(|| {
            let ts = rand::thread_rng().gen_range(0u64, rand_range);
            let res = c.get(ts.into());
            test::black_box(&res);
        })
    }

    #[bench]
    fn bench_insert_empty(b: &mut test::Bencher) {
        // 120ns/iter
        bench_insert_impl(b, 0);
    }

    #[bench]
    fn bench_insert_100000(b: &mut test::Bencher) {
        // 132ns/iter
        bench_insert_impl(b, 100000);
    }

    #[bench]
    fn bench_get_empty(b: &mut test::Bencher) {
        // 25ns/iter
        bench_get_impl(b, 0);
    }

    #[bench]
    fn bench_get_100000(b: &mut test::Bencher) {
        // 35ns/iter
        bench_get_impl(b, 100000);
    }
}
