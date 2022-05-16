// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_histogram, Histogram};

use crate::store::QueryStats;

lazy_static! {
    pub static ref APPLY_PROPOSAL: Histogram = register_histogram!(
        "tikv_raftstore_apply_proposal",
        "The count of proposals sent by a region at once",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
}

#[derive(Default)]
pub struct StoreStat {
    pub lock_cf_bytes_written: AtomicU64,
    pub engine_total_bytes_written: AtomicU64,
    pub engine_total_keys_written: AtomicU64,
    pub engine_total_query_put: AtomicU64,
    pub engine_total_query_delete: AtomicU64,
    pub engine_total_query_delete_range: AtomicU64,
    pub is_busy: AtomicBool,
}

#[derive(Clone, Default)]
pub struct GlobalStoreStat {
    pub stat: Arc<StoreStat>,
}

impl GlobalStoreStat {
    #[inline]
    pub fn local(&self) -> LocalStoreStat {
        LocalStoreStat {
            lock_cf_bytes_written: 0,
            engine_total_bytes_written: 0,
            engine_total_keys_written: 0,
            engine_total_query_stats: QueryStats::default(),
            is_busy: false,

            global: self.clone(),
        }
    }
}

pub struct LocalStoreStat {
    pub lock_cf_bytes_written: u64,
    pub engine_total_bytes_written: u64,
    pub engine_total_keys_written: u64,
    pub engine_total_query_stats: QueryStats,
    pub is_busy: bool,

    global: GlobalStoreStat,
}

impl Clone for LocalStoreStat {
    #[inline]
    fn clone(&self) -> LocalStoreStat {
        self.global.local()
    }
}

impl LocalStoreStat {
    pub fn flush(&mut self) {
        if self.lock_cf_bytes_written != 0 {
            self.global
                .stat
                .lock_cf_bytes_written
                .fetch_add(self.lock_cf_bytes_written, Ordering::Relaxed);
            self.lock_cf_bytes_written = 0;
        }
        if self.engine_total_bytes_written != 0 {
            self.global
                .stat
                .engine_total_bytes_written
                .fetch_add(self.engine_total_bytes_written, Ordering::Relaxed);
            self.engine_total_bytes_written = 0;
        }
        if self.engine_total_keys_written != 0 {
            self.global
                .stat
                .engine_total_keys_written
                .fetch_add(self.engine_total_keys_written, Ordering::Relaxed);
            self.engine_total_keys_written = 0;
        }
        let put_query_num = self.engine_total_query_stats.0.get_put();
        if put_query_num != 0 {
            self.global
                .stat
                .engine_total_query_put
                .fetch_add(put_query_num, Ordering::Relaxed);
            self.engine_total_query_stats.0.set_put(0);
        }
        let delete_query_num = self.engine_total_query_stats.0.get_delete();
        if delete_query_num != 0 {
            self.global
                .stat
                .engine_total_query_delete
                .fetch_add(delete_query_num, Ordering::Relaxed);
            self.engine_total_query_stats.0.set_delete(0);
        }
        let delete_range_query_num = self.engine_total_query_stats.0.get_delete_range();
        if delete_range_query_num != 0 {
            self.global
                .stat
                .engine_total_query_delete_range
                .fetch_add(delete_range_query_num, Ordering::Relaxed);
            self.engine_total_query_stats.0.set_delete_range(0);
        }
        if self.is_busy {
            self.global.stat.is_busy.store(true, Ordering::Relaxed);
            self.is_busy = false;
        }
    }
}
