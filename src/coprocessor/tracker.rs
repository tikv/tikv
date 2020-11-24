// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb;
use kvproto::kvrpcpb::ScanDetailV2;

use crate::storage::kv::PerfStatisticsDelta;

use engine_rocks::set_perf_level;
use tikv_util::time::{self, Duration, Instant};

use super::metrics::*;
use crate::coprocessor::*;
use crate::storage::Statistics;

#[derive(Debug, Clone, Copy, PartialEq)]
enum TrackerState {
    /// The tracker is initialized.
    Initialized,

    /// The tracker is notified that the task is scheduled on a thread pool and start running.
    Scheduled(Instant),

    /// The tracker is notified that the snapshot needed by the task is ready.
    SnapshotRetrieved(Instant),

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

    // Intermediate results
    current_stage: TrackerState,
    wait_time: Duration,          // Total wait time
    schedule_wait_time: Duration, // Wait time spent on waiting for scheduling
    snapshot_wait_time: Duration, // Wait time spent on waiting for a snapshot
    handler_build_time: Duration, // Time spent on building the handler (not included in total wait time)
    req_lifetime: Duration,
    item_process_time: Duration,
    total_process_time: Duration,
    total_storage_stats: Statistics,
    total_perf_stats: PerfStatisticsDelta, // Accumulated perf statistics
    slow_log_threshold: Duration,

    // Request info, used to print slow log.
    pub req_ctx: ReqContext,
}

impl Tracker {
    /// Initialize the tracker. Normally it is called outside future pool's factory context,
    /// because the future pool might be full and we need to wait it. This kind of wait time
    /// has to be recorded.
    pub fn new(req_ctx: ReqContext, slow_log_threshold: Duration) -> Tracker {
        let now = Instant::now_coarse();
        Tracker {
            request_begin_at: now,
            item_begin_at: now,
            current_stage: TrackerState::Initialized,
            wait_time: Duration::default(),
            schedule_wait_time: Duration::default(),
            snapshot_wait_time: Duration::default(),
            handler_build_time: Duration::default(),
            req_lifetime: Duration::default(),
            item_process_time: Duration::default(),
            total_process_time: Duration::default(),
            total_storage_stats: Statistics::default(),
            total_perf_stats: PerfStatisticsDelta::default(),
            slow_log_threshold,
            req_ctx,
        }
    }

    pub fn on_scheduled(&mut self) {
        assert_eq!(self.current_stage, TrackerState::Initialized);
        let now = Instant::now_coarse();
        self.schedule_wait_time = now - self.request_begin_at;
        self.current_stage = TrackerState::Scheduled(now);
    }

    pub fn on_snapshot_finished(&mut self) {
        if let TrackerState::Scheduled(at) = self.current_stage {
            let now = Instant::now_coarse();
            self.snapshot_wait_time = now - at;
            self.wait_time = now - self.request_begin_at;
            self.current_stage = TrackerState::SnapshotRetrieved(now);
        } else {
            unreachable!()
        }
    }

    pub fn on_begin_all_items(&mut self) {
        if let TrackerState::SnapshotRetrieved(at) = self.current_stage {
            let now = Instant::now_coarse();
            self.handler_build_time = now - at;
            self.current_stage = TrackerState::AllItemsBegan;
        } else {
            unreachable!()
        }
    }

    pub fn on_begin_item(&mut self) {
        assert!(
            self.current_stage == TrackerState::AllItemsBegan
                || self.current_stage == TrackerState::ItemFinished
        );
        set_perf_level(self.req_ctx.perf_level);
        self.item_begin_at = Instant::now_coarse();
        self.current_stage = TrackerState::ItemBegan;
    }

    pub fn on_finish_item(
        &mut self,
        some_storage_stats: Option<Statistics>,
        perf_statistics: PerfStatisticsDelta,
    ) {
        assert_eq!(self.current_stage, TrackerState::ItemBegan);
        self.item_process_time = Instant::now_coarse() - self.item_begin_at;
        self.total_process_time += self.item_process_time;
        if let Some(storage_stats) = some_storage_stats {
            self.total_storage_stats.add(&storage_stats);
        }
        // Record delta perf statistics
        self.total_perf_stats += perf_statistics;
        self.current_stage = TrackerState::ItemFinished;
        // TODO: Need to record time between Finish -> Begin Next?
    }

    pub fn collect_storage_statistics(&mut self, storage_stats: Statistics) {
        self.total_storage_stats.add(&storage_stats);
    }

    /// Get current item's ExecDetail according to previous collected metrics.
    /// TiDB asks for ExecDetail to be printed in its log.
    /// WARN: TRY BEST NOT TO USE THIS FUNCTION.
    pub fn get_item_exec_details(&self) -> kvrpcpb::ExecDetails {
        assert_eq!(self.current_stage, TrackerState::ItemFinished);
        self.exec_details(self.item_process_time)
    }

    /// Get ExecDetail according to previous collected metrics.
    /// TiDB asks for ExecDetail to be printed in its log.
    pub fn get_exec_details(&self) -> kvrpcpb::ExecDetails {
        assert_eq!(self.current_stage, TrackerState::ItemFinished);
        self.exec_details(self.total_process_time)
    }

    fn exec_details(&self, measure: Duration) -> kvrpcpb::ExecDetails {
        let mut exec_details = kvrpcpb::ExecDetails::default();

        let mut handle = kvrpcpb::HandleTime::default();
        handle.set_process_ms((time::duration_to_sec(measure) * 1000.0) as i64);
        handle.set_wait_ms((time::duration_to_sec(self.wait_time) * 1000.0) as i64);
        exec_details.set_handle_time(handle);

        let detail = self.total_storage_stats.scan_detail();

        let mut detail_v2 = ScanDetailV2::default();
        detail_v2.set_processed_versions(self.total_storage_stats.write.processed_keys as u64);
        detail_v2.set_total_versions(self.total_storage_stats.write.total_op_count() as u64);
        detail_v2.set_rocksdb_delete_skipped_count(
            self.total_perf_stats.0.internal_delete_skipped_count as u64,
        );
        detail_v2.set_rocksdb_key_skipped_count(
            self.total_perf_stats.0.internal_key_skipped_count as u64,
        );
        detail_v2.set_rocksdb_block_cache_hit_count(
            self.total_perf_stats.0.block_cache_hit_count as u64,
        );
        detail_v2.set_rocksdb_block_read_count(self.total_perf_stats.0.block_read_count as u64);
        detail_v2.set_rocksdb_block_read_byte(self.total_perf_stats.0.block_read_byte as u64);

        exec_details.set_use_scan_detail_v2(true);
        exec_details.set_scan_detail(detail);
        exec_details.set_scan_detail_v2(detail_v2);
        exec_details
    }

    pub fn on_finish_all_items(&mut self) {
        assert!(
            self.current_stage == TrackerState::AllItemsBegan
                || self.current_stage == TrackerState::ItemFinished
        );
        self.req_lifetime = Instant::now_coarse() - self.request_begin_at;
        self.current_stage = TrackerState::AllItemFinished;
        self.track();
    }

    fn track(&mut self) {
        if self.current_stage != TrackerState::AllItemFinished {
            return;
        }

        let total_storage_stats = std::mem::take(&mut self.total_storage_stats);

        if self.req_lifetime > self.slow_log_threshold {
            let first_range = self.req_ctx.ranges.first();
            let some_table_id = first_range.as_ref().map(|range| {
                tidb_query_datatype::codec::table::decode_table_id(range.get_start())
                    .unwrap_or_default()
            });

            info!(#"slow_log", "slow-query";
                "region_id" => &self.req_ctx.context.get_region_id(),
                "remote_host" => &self.req_ctx.peer,
                "total_lifetime" => ?self.req_lifetime,
                "wait_time" => ?self.wait_time,
                "wait_time.schedule" => ?self.schedule_wait_time,
                "wait_time.snapshot" => ?self.snapshot_wait_time,
                "handler_build_time" => ?self.handler_build_time,
                "total_process_time" => ?self.total_process_time,
                "txn_start_ts" => self.req_ctx.txn_start_ts,
                "table_id" => some_table_id,
                "tag" => self.req_ctx.tag.get_str(),
                "scan.is_desc" => self.req_ctx.is_desc_scan,
                "scan.processed" => total_storage_stats.write.processed_keys,
                "scan.total" => total_storage_stats.write.total_op_count(),
                "scan.ranges" => self.req_ctx.ranges.len(),
                "scan.range.first" => ?first_range,
                "perf_stats.block_cache_hit_count" => self.total_perf_stats.0.block_cache_hit_count,
                "perf_stats.block_read_count" => self.total_perf_stats.0.block_read_count,
                "perf_stats.block_read_byte" => self.total_perf_stats.0.block_read_byte,
                "perf_stats.internal_key_skipped_count"
                    => self.total_perf_stats.0.internal_key_skipped_count,
                "perf_stats.internal_delete_skipped_count"
                    => self.total_perf_stats.0.internal_delete_skipped_count,
            );
        }

        // req time
        COPR_REQ_HISTOGRAM_STATIC
            .get(self.req_ctx.tag)
            .observe(time::duration_to_sec(self.req_lifetime));

        // wait time
        COPR_REQ_WAIT_TIME_STATIC
            .get(self.req_ctx.tag)
            .all
            .observe(time::duration_to_sec(self.wait_time));

        // schedule wait time
        COPR_REQ_WAIT_TIME_STATIC
            .get(self.req_ctx.tag)
            .schedule
            .observe(time::duration_to_sec(self.schedule_wait_time));

        // snapshot wait time
        COPR_REQ_WAIT_TIME_STATIC
            .get(self.req_ctx.tag)
            .snapshot
            .observe(time::duration_to_sec(self.snapshot_wait_time));

        // handler build time
        COPR_REQ_HANDLER_BUILD_TIME_STATIC
            .get(self.req_ctx.tag)
            .observe(time::duration_to_sec(self.handler_build_time));

        // handle time
        COPR_REQ_HANDLE_TIME_STATIC
            .get(self.req_ctx.tag)
            .observe(time::duration_to_sec(self.total_process_time));

        // scan keys
        COPR_SCAN_KEYS_STATIC
            .get(self.req_ctx.tag)
            .total
            .observe(total_storage_stats.write.total_op_count() as f64);
        COPR_SCAN_KEYS_STATIC
            .get(self.req_ctx.tag)
            .processed_keys
            .observe(total_storage_stats.write.processed_keys as f64);

        tls_collect_scan_details(self.req_ctx.tag, &total_storage_stats);
        tls_collect_read_flow(self.req_ctx.context.get_region_id(), &total_storage_stats);
        tls_collect_perf_stats(self.req_ctx.tag, &self.total_perf_stats);

        let peer = self.req_ctx.context.get_peer();
        let region_id = self.req_ctx.context.get_region_id();
        let start_key = &self.req_ctx.lower_bound;
        let end_key = &self.req_ctx.upper_bound;
        let reverse_scan = if let Some(reverse_scan) = self.req_ctx.is_desc_scan {
            reverse_scan
        } else {
            false
        };

        tls_collect_qps(region_id, peer, start_key, end_key, reverse_scan);
        self.current_stage = TrackerState::Tracked;
    }
}

impl Drop for Tracker {
    /// `Tracker` may be dropped without even calling `on_begin_all_items`. For example, if
    /// get snapshot failed. So we fast-forward if some steps are missing.
    fn drop(&mut self) {
        if self.current_stage == TrackerState::Initialized {
            self.on_scheduled();
        }
        if let TrackerState::Scheduled(_) = self.current_stage {
            self.on_snapshot_finished();
        }
        if let TrackerState::SnapshotRetrieved(_) = self.current_stage {
            self.on_begin_all_items();
        }
        if self.current_stage == TrackerState::ItemBegan {
            self.on_finish_item(None, PerfStatisticsDelta::default());
        }
        if self.current_stage == TrackerState::AllItemsBegan
            || self.current_stage == TrackerState::ItemFinished
        {
            self.on_finish_all_items();
        }
    }
}
