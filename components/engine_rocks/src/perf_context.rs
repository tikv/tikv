// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::raw::PerfContext as RawPerfContext;
use crate::raw_util;
use engine_traits::{PerfContext, PerfContextExt, PerfLevel, PerfContextKind};
use crate::perf_context_impl::PerfContextStatistics;

impl PerfContextExt for RocksEngine {
    type PerfContext = RocksPerfContext;

    fn get_perf_context(&self, level: PerfLevel, kind: PerfContextKind) -> Option<Self::PerfContext> {
        Some(RocksPerfContext {
            raw: RawPerfContext::get(),
            stats: PerfContextStatistics::new(level, kind),
            engine: self.clone(),
        })
    }

    fn get_perf_level(&self) -> PerfLevel {
        raw_util::from_raw_perf_level(rocksdb::get_perf_level())
    }

    fn set_perf_level(&self, level: PerfLevel) {
        rocksdb::set_perf_level(raw_util::to_raw_perf_level(level))
    }
}

pub struct RocksPerfContext {
    raw: RawPerfContext,
    stats: PerfContextStatistics,
    engine: RocksEngine,
}

impl PerfContext for RocksPerfContext {
    fn start_observe(&mut self) {
        self.stats.start(&self.engine)
    }

    fn report_metrics(&mut self) {
        self.stats.report(&self.engine)
    }

    fn reset(&mut self) {
        self.raw.reset()
    }

    fn write_wal_time(&self) -> u64 {
        self.raw.write_wal_time()
    }

    fn write_memtable_time(&self) -> u64 {
        self.raw.write_memtable_time()
    }

    fn write_delay_time(&self) -> u64 {
        self.raw.write_delay_time()
    }

    fn write_pre_and_post_process_time(&self) -> u64 {
        self.raw.write_pre_and_post_process_time()
    }

    fn db_mutex_lock_nanos(&self) -> u64 {
        self.raw.db_mutex_lock_nanos()
    }

    fn write_thread_wait_nanos(&self) -> u64 {
        self.raw.write_thread_wait_nanos()
    }

    fn write_scheduling_flushes_compactions_time(&self) -> u64 {
        self.raw.write_scheduling_flushes_compactions_time()
    }

    fn db_condition_wait_nanos(&self) -> u64 {
        self.raw.db_condition_wait_nanos()
    }
}
