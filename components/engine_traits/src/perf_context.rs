// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PerfLevel {
    Uninitialized,
    Disable,
    EnableCount,
    EnableTimeExceptForMutex,
    EnableTimeAndCPUTimeExceptForMutex,
    EnableTime,
    OutOfBounds,
}

/// Extensions for measuring engine performance.
///
/// This is very rocks-specific and is optional for other engines.
/// Eventually it will need to be rethought to make sense for other engines.
///
/// In rocks, `PerfContext` uses global state, and does not require
/// access through an engine. Thus perf data is not per-engine.
/// This doesn't seem like a reasonable assumption for engines generally,
/// so this abstraction follows the existing pattern in this crate and
/// requires `PerfContext` to be accessed through the engine.
pub trait PerfContextExt {
    type PerfContext: PerfContext;

    fn get_perf_context(&self) -> Option<Self::PerfContext>;
    fn get_perf_level(&self) -> PerfLevel;
    fn set_perf_level(&self, level: PerfLevel);
}

pub trait PerfContext {
    fn reset(&mut self);
    fn write_wal_time(&self) -> u64;
    fn write_memtable_time(&self) -> u64;
    fn write_delay_time(&self) -> u64;
    fn write_pre_and_post_process_time(&self) -> u64;
    fn db_mutex_lock_nanos(&self) -> u64;
    fn write_thread_wait_nanos(&self) -> u64;
    fn write_scheduling_flushes_compactions_time(&self) -> u64;
    fn db_condition_wait_nanos(&self) -> u64;
}
