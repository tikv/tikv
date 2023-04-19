// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use raft_engine::get_perf_context;
use tracker::{TrackerToken, GLOBAL_TRACKERS};

#[derive(Debug)]
pub struct RaftEnginePerfContext;

impl engine_traits::PerfContext for RaftEnginePerfContext {
    fn start_observe(&mut self) {
        raft_engine::set_perf_context(Default::default());
    }

    fn report_metrics(&mut self, trackers: &[TrackerToken]) {
        let perf_context = get_perf_context();
        for token in trackers {
            GLOBAL_TRACKERS.with_tracker(*token, |t| {
                t.metrics.store_thread_wait_nanos =
                    perf_context.write_wait_duration.as_nanos() as u64;
                t.metrics.store_write_wal_nanos = (perf_context.log_write_duration
                    + perf_context.log_sync_duration
                    + perf_context.log_rotate_duration)
                    .as_nanos() as u64;
                t.metrics.store_write_memtable_nanos =
                    perf_context.apply_duration.as_nanos() as u64;
            });
        }
    }
}
