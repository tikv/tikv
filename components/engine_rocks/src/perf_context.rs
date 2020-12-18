// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::raw::PerfContext as RawPerfContext;
use crate::raw_util;
use engine_traits::{PerfContext, PerfContextExt, PerfLevel};

impl PerfContextExt for RocksEngine {
    type PerfContext = RocksPerfContext;

    fn get_perf_context(&self) -> Option<Self::PerfContext> {
        Some(RocksPerfContext(RawPerfContext::get()))
    }

    fn get_perf_level(&self) -> PerfLevel {
        raw_util::from_raw_perf_level(rocksdb::get_perf_level())
    }

    fn set_perf_level(&self, level: PerfLevel) {
        rocksdb::set_perf_level(raw_util::to_raw_perf_level(level))
    }
}

pub struct RocksPerfContext(RawPerfContext);

impl PerfContext for RocksPerfContext {
    fn start_observe(&self) {
        panic!()
    }

    fn report_metrics(&self) {
        panic!()
    }

    fn reset(&mut self) {
        self.0.reset()
    }

    fn write_wal_time(&self) -> u64 {
        self.0.write_wal_time()
    }

    fn write_memtable_time(&self) -> u64 {
        self.0.write_memtable_time()
    }

    fn write_delay_time(&self) -> u64 {
        self.0.write_delay_time()
    }

    fn write_pre_and_post_process_time(&self) -> u64 {
        self.0.write_pre_and_post_process_time()
    }

    fn db_mutex_lock_nanos(&self) -> u64 {
        self.0.db_mutex_lock_nanos()
    }

    fn write_thread_wait_nanos(&self) -> u64 {
        self.0.write_thread_wait_nanos()
    }

    fn write_scheduling_flushes_compactions_time(&self) -> u64 {
        self.0.write_scheduling_flushes_compactions_time()
    }

    fn db_condition_wait_nanos(&self) -> u64 {
        self.0.db_condition_wait_nanos()
    }
}
