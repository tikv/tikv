// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub trait PerfContextExt {
    type PerfContext: PerfContext;

    fn get_perf_context() -> Option<Self::PerfContext>;
}

pub trait PerfContext {
    fn write_wal_time(&self) -> u64;
    fn write_memtable_time(&self) -> u64;
    fn write_delay_time(&self) -> u64;
    fn write_pre_and_post_process_time(&self) -> u64;
    fn db_mutex_lock_nanos(&self) -> u64;
    fn write_thread_wait_nanos(&self) -> u64;
    fn write_scheduling_flushes_compactions_time(&self) -> u64;
    fn db_condition_wait_nanos(&self) -> u64;
}
