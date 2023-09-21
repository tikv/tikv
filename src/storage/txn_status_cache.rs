// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::UNIX_EPOCH;

use crossbeam::utils::CachePadded;
use parking_lot::RwLock;
use tikv_util::{
    lru,
    lru::{CountTracker, GetTailKV, LruCache},
};
use txn_types::TimeStamp;

use crate::storage::{lock_manager::WaitTimeout::Default, ProcessResult::TxnStatus};

const TXN_STATUS_CACHE_BUCKETS: usize = 128;

const REQ_MAX_FLYING_TIME_MILLIS: u64 = 30000;

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
            std::time::SystemTime::now()
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

pub(crate) struct TxnStatusCache {
    slots: [CachePadded<
        RwLock<LruCache<TimeStamp, CacheEntry, lru::CountTracker, TxnStatusCacheEvictPolicy>>,
    >; TXN_STATUS_CACHE_BUCKETS],
}

impl TxnStatusCache {
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|| {
                RwLock::new(LruCache::new(
                    64,
                    0,
                    CountTracker,
                    TxnStatusCacheEvictPolicy {
                        limit_millis: REQ_MAX_FLYING_TIME_MILLIS,
                    },
                ))
                .into()
            }),
        }
    }

    fn slot_index(start_ts: TimeStamp) -> usize {
        fxhash::hash(&start_ts) % TXN_STATUS_CACHE_BUCKETS
    }

    pub fn insert(&self, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let mut slot = self.slots[Self::slot_index(start_ts)].write();
        let insert_time = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        slot.insert_if_not_exist(
            start_ts,
            CacheEntry {
                commit_ts,
                insert_time,
            },
        );
    }

    pub fn get(&self, start_ts: TimeStamp) -> Option<TimeStamp> {
        let slot = self.slots[Self::slot_index(start_ts)].read();
        slot.get_no_promote(&start_ts).map(|entry| entry.commit_ts)
    }
}
