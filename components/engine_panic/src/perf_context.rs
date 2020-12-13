// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{PerfContextExt, PerfContext};

impl PerfContextExt for PanicEngine {
    type PerfContext = PanicPerfContext;

    fn get_perf_context() -> Option<Self::PerfContext> {
        panic!()
    }
}

pub struct PanicPerfContext;

impl PerfContext for PanicPerfContext {
    fn write_wal_time(&self) -> u64 { panic!() }
    fn write_memtable_time(&self) -> u64 { panic!() }
    fn write_delay_time(&self) -> u64 { panic!() }
    fn write_pre_and_post_process_time(&self) -> u64 { panic!() }
    fn db_mutex_lock_nanos(&self) -> u64 { panic!() }
    fn write_thread_wait_nanos(&self) -> u64 { panic!() }
    fn write_scheduling_flushes_compactions_time(&self) -> u64 { panic!() }
    fn db_condition_wait_nanos(&self) -> u64 { panic!() }
}
