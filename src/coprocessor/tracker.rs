// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, marker::PhantomData};

use ::tracker::{get_tls_tracker_token, with_tls_tracker};
use engine_traits::{PerfContext, PerfContextExt, PerfContextKind};
use kvproto::{kvrpcpb, kvrpcpb::ScanDetailV2};
use pd_client::BucketMeta;
use tikv_kv::{with_tls_engine, Engine};
use tikv_util::time::{self, Duration, Instant};
use txn_types::Key;

use super::metrics::*;
use crate::{coprocessor::*, storage::Statistics};

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
    ItemBegan(Instant),

    /// The tracker is notified that a single item just finished.
    ItemFinished(Instant),

    /// The tracker is notified that all items just finished.
    AllItemFinished,

    /// The tracker has finished all tracking and there will be no future operations.
    Tracked,
}

/// Track coprocessor requests to update statistics and provide slow logs.
#[derive(Debug)]
pub struct Tracker<E: Engine> {
    request_begin_at: Instant,

    // Intermediate results
    current_stage: TrackerState,
    wait_time: Duration,          // Total wait time
    schedule_wait_time: Duration, // Wait time spent on waiting for scheduling
    snapshot_wait_time: Duration, // Wait time spent on waiting for a snapshot
    handler_build_time: Duration, // Time spent on building the handler (not included in total wait time)
    req_lifetime: Duration,

    // Suspend time between processing two items
    //
    // In a cooperative environment, a copr task may suspend itself at finishing an item,
    // and be resumed by the runtime later. That will raise a considerable suspend time.
    item_suspend_time: Duration,
    total_suspend_time: Duration,

    item_process_time: Duration,
    total_process_time: Duration,
    total_storage_stats: Statistics,
    slow_log_threshold: Duration,
    scan_process_time_ms: u64,

    pub buckets: Option<Arc<BucketMeta>>,

    // Request info, used to print slow log.
    pub req_ctx: ReqContext,

    _phantom: PhantomData<fn() -> E>,
}

impl<E: Engine> Tracker<E> {
    /// Initialize the tracker. Normally it is called outside future pool's factory context,
    /// because the future pool might be full and we need to wait it. This kind of wait time
    /// has to be recorded.
    pub fn new(req_ctx: ReqContext, slow_log_threshold: Duration) -> Self {
        let now = Instant::now_coarse();
        Tracker {
            request_begin_at: now,
            current_stage: TrackerState::Initialized,
            wait_time: Duration::default(),
            schedule_wait_time: Duration::default(),
            snapshot_wait_time: Duration::default(),
            handler_build_time: Duration::default(),
            req_lifetime: Duration::default(),
            item_process_time: Duration::default(),
            item_suspend_time: Duration::default(),
            total_suspend_time: Duration::default(),
            total_process_time: Duration::default(),
            total_storage_stats: Statistics::default(),
            scan_process_time_ms: 0,
            slow_log_threshold,
            req_ctx,
            buckets: None,
            _phantom: PhantomData,
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
        let now = Instant::now_coarse();
        match self.current_stage {
            TrackerState::AllItemsBegan => {}
            TrackerState::ItemFinished(at) => {
                self.item_suspend_time = now - at;
                self.total_suspend_time += self.item_suspend_time;
            }
            _ => unreachable!(),
        }

        self.with_perf_context(|perf_context| perf_context.start_observe());
        self.current_stage = TrackerState::ItemBegan(now);
    }

    pub fn on_finish_item(&mut self, some_storage_stats: Option<Statistics>) {
        if let TrackerState::ItemBegan(at) = self.current_stage {
            let now = Instant::now_coarse();
            self.item_process_time = now - at;
            self.total_process_time += self.item_process_time;
            if let Some(storage_stats) = some_storage_stats {
                self.total_storage_stats.add(&storage_stats);
            }
            self.with_perf_context(|perf_context| {
                perf_context.report_metrics(&[get_tls_tracker_token()])
            });
            self.current_stage = TrackerState::ItemFinished(now);
        } else {
            unreachable!()
        }
    }

    pub fn collect_storage_statistics(&mut self, storage_stats: Statistics) {
        self.total_storage_stats.add(&storage_stats);
    }

    pub fn collect_scan_process_time(&mut self, exec_summary: ExecSummary) {
        self.scan_process_time_ms = (exec_summary.time_processed_ns / 1000000) as u64;
    }

    /// Get current item's ExecDetail according to previous collected metrics.
    /// TiDB asks for ExecDetail to be printed in its log.
    /// WARN: TRY BEST NOT TO USE THIS FUNCTION.
    pub fn get_item_exec_details(&self) -> (kvrpcpb::ExecDetails, kvrpcpb::ExecDetailsV2) {
        if let TrackerState::ItemFinished(_) = self.current_stage {
            self.exec_details(self.item_process_time)
        } else {
            unreachable!()
        }
    }

    /// Get ExecDetail according to previous collected metrics.
    /// TiDB asks for ExecDetail to be printed in its log.
    pub fn get_exec_details(&self) -> (kvrpcpb::ExecDetails, kvrpcpb::ExecDetailsV2) {
        if let TrackerState::ItemFinished(_) = self.current_stage {
            // TODO: Separate process time and suspend time
            self.exec_details(self.total_process_time + self.total_suspend_time)
        } else {
            unreachable!()
        }
    }

    fn exec_details(&self, measure: Duration) -> (kvrpcpb::ExecDetails, kvrpcpb::ExecDetailsV2) {
        // For compatibility, ExecDetails field is still filled.
        let mut exec_details = kvrpcpb::ExecDetails::default();

        let mut td = kvrpcpb::TimeDetail::default();
        td.set_process_wall_time_ms(time::duration_to_ms(measure) as i64);
        td.set_wait_wall_time_ms(time::duration_to_ms(self.wait_time) as i64);
        td.set_kv_read_wall_time_ms(self.scan_process_time_ms as i64);
        exec_details.set_time_detail(td.clone());

        let detail = self.total_storage_stats.scan_detail();
        exec_details.set_scan_detail(detail);

        let mut exec_details_v2 = kvrpcpb::ExecDetailsV2::default();
        exec_details_v2.set_time_detail(td);

        let mut detail_v2 = ScanDetailV2::default();
        detail_v2.set_processed_versions(self.total_storage_stats.write.processed_keys as u64);
        detail_v2.set_processed_versions_size(self.total_storage_stats.processed_size as u64);
        detail_v2.set_total_versions(self.total_storage_stats.write.total_op_count() as u64);
        with_tls_tracker(|tracker| tracker.write_scan_detail(&mut detail_v2));
        exec_details_v2.set_scan_detail_v2(detail_v2);

        (exec_details, exec_details_v2)
    }

    pub fn on_finish_all_items(&mut self) {
        match self.current_stage {
            TrackerState::AllItemsBegan => {}
            TrackerState::ItemFinished(_) => {}
            _ => unreachable!(),
        }

        self.req_lifetime = Instant::now_coarse() - self.request_begin_at;
        self.current_stage = TrackerState::AllItemFinished;
        self.track();
    }

    fn track(&mut self) {
        if self.current_stage != TrackerState::AllItemFinished {
            return;
        }

        let total_storage_stats = std::mem::take(&mut self.total_storage_stats);

        if self.total_process_time > self.slow_log_threshold {
            let first_range = self.req_ctx.ranges.first();
            let some_table_id = first_range.as_ref().map(|range| {
                tidb_query_datatype::codec::table::decode_table_id(range.get_start())
                    .unwrap_or_default()
            });

            with_tls_tracker(|tracker| {
                info!(#"slow_log", "slow-query";
                    "region_id" => &self.req_ctx.context.get_region_id(),
                    "remote_host" => &self.req_ctx.peer,
                    "total_lifetime" => ?self.req_lifetime,
                    "wait_time" => ?self.wait_time,
                    "wait_time.schedule" => ?self.schedule_wait_time,
                    "wait_time.snapshot" => ?self.snapshot_wait_time,
                    "handler_build_time" => ?self.handler_build_time,
                    "total_process_time" => ?self.total_process_time,
                    "total_suspend_time" => ?self.total_suspend_time,
                    "txn_start_ts" => self.req_ctx.txn_start_ts,
                    "table_id" => some_table_id,
                    "tag" => self.req_ctx.tag.get_str(),
                    "scan.is_desc" => self.req_ctx.is_desc_scan,
                    "scan.processed" => total_storage_stats.write.processed_keys,
                    "scan.processed_size" => total_storage_stats.processed_size,
                    "scan.total" => total_storage_stats.write.total_op_count(),
                    "scan.ranges" => self.req_ctx.ranges.len(),
                    "scan.range.first" => ?first_range,
                    "perf_stats.block_cache_hit_count" => tracker.metrics.block_cache_hit_count,
                    "perf_stats.block_read_count" => tracker.metrics.block_read_count,
                    "perf_stats.block_read_byte" => tracker.metrics.block_read_byte,
                    "perf_stats.internal_key_skipped_count"
                        => tracker.metrics.internal_key_skipped_count,
                    "perf_stats.internal_delete_skipped_count"
                        => tracker.metrics.deleted_key_skipped_count,
                )
            });
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

        let peer = self.req_ctx.context.get_peer();
        let region_id = self.req_ctx.context.get_region_id();
        let start_key = &self.req_ctx.lower_bound;
        let end_key = &self.req_ctx.upper_bound;
        let reverse_scan = if let Some(reverse_scan) = self.req_ctx.is_desc_scan {
            reverse_scan
        } else {
            false
        };

        tls_collect_query(
            region_id,
            peer,
            Key::from_raw(start_key).as_encoded(),
            Key::from_raw(end_key).as_encoded(),
            reverse_scan,
        );
        tls_collect_read_flow(
            self.req_ctx.context.get_region_id(),
            Some(start_key),
            Some(end_key),
            &total_storage_stats,
            self.buckets.as_ref(),
        );
        self.current_stage = TrackerState::Tracked;
    }

    fn with_perf_context<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut Box<dyn PerfContext>) -> T,
    {
        thread_local! {
            static SELECT: RefCell<Option<Box<dyn PerfContext>>> = RefCell::new(None);
            static INDEX: RefCell<Option<Box<dyn PerfContext>>> = RefCell::new(None);
            static ANALYZE_TABLE: RefCell<Option<Box<dyn PerfContext>>> = RefCell::new(None);
            static ANALYZE_INDEX: RefCell<Option<Box<dyn PerfContext>>> = RefCell::new(None);
            static ANALYZE_FULL_SAMPLING: RefCell<Option<Box<dyn PerfContext>>> = RefCell::new(None);
            static CHECKSUM_TABLE: RefCell<Option<Box<dyn PerfContext>>> = RefCell::new(None);
            static CHECKSUM_INDEX: RefCell<Option<Box<dyn PerfContext>>> = RefCell::new(None);
            static TEST: RefCell<Option<Box<dyn PerfContext>>> = RefCell::new(None);
        }
        let tls_cell = match self.req_ctx.tag {
            ReqTag::select => &SELECT,
            ReqTag::index => &INDEX,
            ReqTag::analyze_table => &ANALYZE_TABLE,
            ReqTag::analyze_index => &ANALYZE_INDEX,
            ReqTag::analyze_full_sampling => &ANALYZE_FULL_SAMPLING,
            ReqTag::checksum_table => &CHECKSUM_TABLE,
            ReqTag::checksum_index => &CHECKSUM_INDEX,
            ReqTag::test => &TEST,
        };
        tls_cell.with(|c| {
            let mut c = c.borrow_mut();
            let perf_context = c.get_or_insert_with(|| unsafe {
                with_tls_engine::<E, _, _>(|engine| {
                    Box::new(engine.kv_engine().get_perf_context(
                        PerfLevel::Uninitialized,
                        PerfContextKind::Coprocessor(self.req_ctx.tag.get_str()),
                    ))
                })
            });
            f(perf_context)
        })
    }
}

impl<E: Engine> Drop for Tracker<E> {
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
        if let TrackerState::ItemBegan(_) = self.current_stage {
            self.on_finish_item(None);
        }
        if self.current_stage == TrackerState::AllItemsBegan {
            self.on_finish_all_items();
        }
        if let TrackerState::ItemFinished(_) = self.current_stage {
            self.on_finish_all_items();
        }
    }
}
