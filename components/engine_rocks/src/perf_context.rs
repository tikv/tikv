// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine_traits::{PerfContextExt, PerfContext};
use crate::raw::PerfContext as RawPerfContext;

impl PerfContextExt for RocksEngine {
    type PerfContext = RocksPerfContext;

    fn get_perf_context() -> Option<Self::PerfContext> {
        Some(RocksPerfContext(RawPerfContext::get()))
    }
}

pub struct RocksPerfContext(RawPerfContext);

impl PerfContext for RocksPerfContext {
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
