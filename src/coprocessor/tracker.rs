// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::kvrpcpb;

use crate::storage::engine::{PerfStatisticsDelta, PerfStatisticsInstant};
use crate::util::time::{self, Duration, Instant};

use crate::coprocessor::dag::executor::ExecutorMetrics;
use crate::coprocessor::read_pool_impl::*;
use crate::coprocessor::*;

// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

#[derive(Debug, Clone, Copy, PartialEq)]
enum TrackerState {
    /// The tracker is just created and not initialized. Initialize means `ctxd` is attached.
    NotInitialized,

    /// The tracker is initialized with a `ctxd` instance.
    Initialized,

    /// The tracker is notified that all items just began.
    AllItemsBegan,

    /// The tracker is notified that a single item just began.
    ItemBegan,

    /// The tracker is notified that a single item just finished.
    ItemFinished,

    /// The tracker is notified that all items just finished.
    AllItemFinished,

    /// The tracker has finished all tracking and there will be no future operations.
    Tracked,
}

/// Track coprocessor requests to update statistics and provide slow logs.
#[derive(Debug)]
pub struct Tracker {
    request_begin_at: Instant,
    item_begin_at: Instant,
    perf_statistics_start: Option<PerfStatisticsInstant>, // The perf statistics when handle begins

    // Intermediate results
    current_stage: TrackerState,
    wait_time: Duration,
    req_time: Duration,
    item_process_time: Duration,
    total_process_time: Duration,
    total_exec_metrics: ExecutorMetrics,
    total_perf_statistics: PerfStatisticsDelta, // Accumulated perf statistics

    // Request info, used to print slow log.
    pub req_ctx: ReqContext,
}

impl Tracker {
    /// Initialize the tracker. Normally it is called outside future pool's factory context,
    /// because the future pool might be full and we need to wait it. This kind of wait time
    /// has to be recorded.
    pub fn new(req_ctx: ReqContext) -> Tracker {
        Tracker {
            request_begin_at: Instant::now_coarse(),
            item_begin_at: Instant::now_coarse(),
            perf_statistics_start: None,

            current_stage: TrackerState::NotInitialized,
            wait_time: Duration::default(),
            req_time: Duration::default(),
            item_process_time: Duration::default(),
            total_process_time: Duration::default(),
            total_exec_metrics: ExecutorMetrics::default(),
            total_perf_statistics: PerfStatisticsDelta::default(),

            req_ctx,
        }
    }

    /// Init current stage
    pub fn init_current_stage(&mut self) {
        assert_eq!(self.current_stage, TrackerState::NotInitialized);
        self.current_stage = TrackerState::Initialized;
    }

    pub fn on_begin_all_items(&mut self) {
        assert_eq!(self.current_stage, TrackerState::Initialized);
        self.wait_time = Instant::now_coarse() - self.request_begin_at;
        self.current_stage = TrackerState::AllItemsBegan;
    }

    pub fn on_begin_item(&mut self) {
        assert!(
            self.current_stage == TrackerState::AllItemsBegan
                || self.current_stage == TrackerState::ItemFinished
        );
        self.item_begin_at = Instant::now_coarse();
        self.perf_statistics_start = Some(PerfStatisticsInstant::new());
        self.current_stage = TrackerState::ItemBegan;
    }

    pub fn on_finish_item(&mut self, some_exec_metrics: Option<ExecutorMetrics>) {
        assert_eq!(self.current_stage, TrackerState::ItemBegan);
        self.item_process_time = Instant::now_coarse() - self.item_begin_at;
        self.total_process_time += self.item_process_time;
        if let Some(mut exec_metrics) = some_exec_metrics {
            self.total_exec_metrics.merge(&mut exec_metrics);
        }
        // Record delta perf statistics
        if let Some(perf_stats) = self.perf_statistics_start.take() {
            // TODO: We should never failed to `take()`?
            self.total_perf_statistics += perf_stats.delta();
        }
        self.current_stage = TrackerState::ItemFinished;
    }

    /// Get current item's ExecDetail according to previous collected metrics.
    /// TiDB asks for ExecDetail to be printed in its log.
    pub fn get_item_exec_details(&self) -> kvrpcpb::ExecDetails {
        assert_eq!(self.current_stage, TrackerState::ItemFinished);
        let is_slow_query = time::duration_to_sec(self.item_process_time) > SLOW_QUERY_LOWER_BOUND;
        let mut exec_details = kvrpcpb::ExecDetails::new();
        if self.req_ctx.context.get_handle_time() || is_slow_query {
            let mut handle = kvrpcpb::HandleTime::new();
            handle.set_process_ms((time::duration_to_sec(self.item_process_time) * 1000.0) as i64);
            handle.set_wait_ms((time::duration_to_sec(self.wait_time) * 1000.0) as i64);
            exec_details.set_handle_time(handle);
        }
        if self.req_ctx.context.get_scan_detail() || is_slow_query {
            let detail = self.total_exec_metrics.cf_stats.scan_detail();
            exec_details.set_scan_detail(detail);
        }
        exec_details
    }

    pub fn on_finish_all_items(&mut self) {
        assert!(
            self.current_stage == TrackerState::AllItemsBegan
                || self.current_stage == TrackerState::ItemFinished
        );
        self.req_time = Instant::now_coarse() - self.request_begin_at;
        self.current_stage = TrackerState::AllItemFinished;
        self.track();
    }

    fn track(&mut self) {
        if self.current_stage != TrackerState::AllItemFinished {
            return;
        }

        // Print slow log if *process* time is long.
        if time::duration_to_sec(self.total_process_time) > SLOW_QUERY_LOWER_BOUND {
            let some_table_id = self.req_ctx.first_range.as_ref().map(|range| {
                super::codec::table::decode_table_id(range.get_start()).unwrap_or_default()
            });

            info!("slow-query";
                "region_id" => self.req_ctx.context.get_region_id(),
                "peer_id" => &self.req_ctx.peer,
                "total_process_time" => ?self.total_process_time,
                "wait_time" => ?self.wait_time,
                "txn_start_ts" => self.req_ctx.txn_start_ts,
                "table_id" => some_table_id,
                "tag" => self.req_ctx.tag,
                "scan_is_desc" => self.req_ctx.is_desc_scan,
                "scan_iter_ops" => self.total_exec_metrics.cf_stats.total_op_count(),
                "scan_iter_processed" => self.total_exec_metrics.cf_stats.total_processed(),
                "scan_ranges" => self.req_ctx.ranges_len,
                "scan_first_range" => self.req_ctx.first_range.as_ref().map(::log_wrappers::kvproto::coprocessor::KeyRange),
                self.total_perf_statistics,
            );
        }

        let total_exec_metrics =
            std::mem::replace(&mut self.total_exec_metrics, ExecutorMetrics::default());

        // req time
        LOCAL_COPR_REQ_HISTOGRAM_VEC.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag])
                .observe(time::duration_to_sec(self.req_time))
        });
        // wait time
        LOCAL_COPR_REQ_WAIT_TIME.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag])
                .observe(time::duration_to_sec(self.wait_time))
        });
        // handle time
        LOCAL_COPR_REQ_HANDLE_TIME.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag])
                .observe(time::duration_to_sec(self.total_process_time))
        });
        // scan keys
        LOCAL_COPR_SCAN_KEYS.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag])
                .observe(total_exec_metrics.cf_stats.total_op_count() as f64)
        });
        // rocksdb perf stats
        LOCAL_COPR_ROCKSDB_PERF_COUNTER.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag, "internal_key_skipped_count"])
                .inc_by(self.total_perf_statistics.internal_key_skipped_count as i64);
        });
        LOCAL_COPR_ROCKSDB_PERF_COUNTER.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag, "internal_delete_skipped_count"])
                .inc_by(self.total_perf_statistics.internal_delete_skipped_count as i64);
        });
        LOCAL_COPR_ROCKSDB_PERF_COUNTER.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag, "block_cache_hit_count"])
                .inc_by(self.total_perf_statistics.block_cache_hit_count as i64);
        });
        LOCAL_COPR_ROCKSDB_PERF_COUNTER.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag, "block_read_count"])
                .inc_by(self.total_perf_statistics.block_read_count as i64);
        });
        LOCAL_COPR_ROCKSDB_PERF_COUNTER.with(|m| {
            m.borrow_mut()
                .with_label_values(&[self.req_ctx.tag, "block_read_byte"])
                .inc_by(self.total_perf_statistics.block_read_byte as i64);
        });

        ReadPoolImpl::collect(
            self.req_ctx.context.get_region_id(),
            self.req_ctx.tag,
            total_exec_metrics,
        );

        self.current_stage = TrackerState::Tracked;
    }
}

impl Drop for Tracker {
    /// `Tracker` may be dropped without even calling `on_begin_all_items`. For example, if
    /// get snapshot failed. So we fast-forward if some steps are missing.
    fn drop(&mut self) {
        // If `on_begin_all_items` is not called after `new` and attached a ctxd, call it.
        if self.current_stage == TrackerState::Initialized {
            self.on_begin_all_items();
        }
        // If `on_finish_item` is not called after `on_begin_item`, call it.
        if self.current_stage == TrackerState::ItemBegan {
            // TODO: We should never meet this scenario?
            self.on_finish_item(None);
        }
        // If `on_finish_all_items` is not called after `on_begin_all_items` or after `on_finish_item`,
        // call it.
        if self.current_stage == TrackerState::AllItemsBegan
            || self.current_stage == TrackerState::ItemFinished
        {
            self.on_finish_all_items();
        }
    }
}
