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

use std::cell::RefMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use kvproto::coprocessor as coppb;
use kvproto::kvrpcpb::{CommandPri, HandleTime};

use storage::engine::{PerfStatisticsDelta, PerfStatisticsInstant};
use util::futurepool;
use util::time::{duration_to_sec, Instant};

use super::codec::table;
use super::dag::executor::ExecutorMetrics;
use super::local_metrics::BasicLocalMetrics;
use super::metrics::*;
use super::*;

// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

#[derive(Debug)]
pub struct Tracker {
    running_task_count: Option<Arc<AtomicUsize>>,
    ctx_pool: Option<futurepool::ContextDelegators<ReadPoolContext>>,

    exec_metrics: ExecutorMetrics,
    perf_statistics_start: Option<PerfStatisticsInstant>, // The perf statistics when handle begins
    start: Instant,                                       // The request start time.
    total_handle_time: f64,
    total_perf_statistics: PerfStatisticsDelta, // Accumulated perf statistics

    // These 4 fields are for ExecDetails.
    wait_start: Option<Instant>,
    handle_start: Option<Instant>,
    wait_time: Option<f64>,
    handle_time: Option<f64>,

    pub req_ctx: ReqContext,
    pub pri_str: &'static str, // TODO: Remove this metrics.
}

impl Tracker {
    pub fn new(req_ctx: ReqContext) -> Tracker {
        let start_time = Instant::now_coarse();

        Tracker {
            running_task_count: None,
            ctx_pool: None,

            start: start_time,
            total_handle_time: 0f64,
            wait_start: Some(start_time),
            handle_start: None,
            wait_time: None,
            handle_time: None,
            exec_metrics: ExecutorMetrics::default(),
            perf_statistics_start: None,
            total_perf_statistics: PerfStatisticsDelta::default(),

            pri_str: get_req_pri_str(req_ctx.context.get_priority()),
            req_ctx,
        }
    }

    pub fn task_count(&mut self, running_task_count: Arc<AtomicUsize>) {
        running_task_count.fetch_add(1, Ordering::Release);
        self.running_task_count = Some(running_task_count);
    }

    pub fn ctx_pool(&mut self, ctx_pool: futurepool::ContextDelegators<ReadPoolContext>) {
        self.ctx_pool = Some(ctx_pool);
    }

    // This function will be only called in thread pool.
    pub fn get_basic_metrics(&self) -> RefMut<BasicLocalMetrics> {
        let ctx_pool = self.ctx_pool.as_ref().unwrap();
        let ctx = ctx_pool.current_thread_context_mut();
        RefMut::map(ctx, |c| &mut c.basic_local_metrics)
    }

    /// This function will be only called in thread pool.
    /// In this function, we record the wait time, which is the time elapsed until this call.
    /// We also record the initial perf_statistics.
    pub fn on_handle_start(&mut self) {
        let stop_first_wait = self.wait_time.is_none();
        let wait_start = self.wait_start.take().unwrap();
        let now = Instant::now_coarse();
        self.wait_time = Some(duration_to_sec(now - wait_start));
        self.handle_start = Some(now);

        if stop_first_wait {
            COPR_PENDING_REQS
                .with_label_values(&[self.req_ctx.tag, self.pri_str])
                .dec();

            let ctx_pool = self.ctx_pool.as_ref().unwrap();
            let mut cop_ctx = ctx_pool.current_thread_context_mut();
            cop_ctx
                .basic_local_metrics
                .wait_time
                .with_label_values(&[self.req_ctx.tag])
                .observe(self.wait_time.unwrap());
        }

        self.perf_statistics_start = Some(PerfStatisticsInstant::new());
    }

    /// Must pair with `on_handle_start` previously.
    #[cfg_attr(feature = "cargo-clippy", allow(useless_let_if_seq))]
    pub fn on_handle_finish(
        &mut self,
        resp: Option<&mut coppb::Response>,
        mut exec_metrics: ExecutorMetrics,
    ) {
        // Record delta perf statistics
        if let Some(perf_stats) = self.perf_statistics_start.take() {
            self.total_perf_statistics += perf_stats.delta();
        }

        let handle_start = self.handle_start.take().unwrap();
        let now = Instant::now_coarse();
        self.handle_time = Some(duration_to_sec(now - handle_start));
        self.wait_start = Some(now);
        self.total_handle_time += self.handle_time.unwrap();

        self.exec_metrics.merge(&mut exec_metrics);

        let mut record_handle_time = self.req_ctx.context.get_handle_time();
        let mut record_scan_detail = self.req_ctx.context.get_scan_detail();
        if self.handle_time.unwrap() > SLOW_QUERY_LOWER_BOUND {
            record_handle_time = true;
            record_scan_detail = true;
        }
        if let Some(resp) = resp {
            if record_handle_time {
                let mut handle = HandleTime::new();
                handle.set_process_ms((self.handle_time.unwrap() * 1000f64) as i64);
                handle.set_wait_ms((self.wait_time.unwrap() * 1000f64) as i64);
                resp.mut_exec_details().set_handle_time(handle);
            }
            if record_scan_detail {
                let detail = self.exec_metrics.cf_stats.scan_detail();
                resp.mut_exec_details().set_scan_detail(detail);
            }
        }
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        if let Some(task_count) = self.running_task_count.take() {
            task_count.fetch_sub(1, Ordering::Release);
        }

        if self.total_handle_time > SLOW_QUERY_LOWER_BOUND {
            let table_id = if let Some(ref range) = self.req_ctx.first_range {
                table::decode_table_id(range.get_start()).unwrap_or_default()
            } else {
                0
            };

            info!(
                "[region {}] [slow-query] execute takes {:?}, wait takes {:?}, \
                 peer: {:?}, start_ts: {:?}, table_id: {:?}, \
                 scan_type: {} (desc: {:?}) \
                 [keys: {}, hit: {}, ranges: {} ({:?}), perf: {:?}]",
                self.req_ctx.context.get_region_id(),
                self.total_handle_time,
                self.wait_time,
                self.req_ctx.peer,
                self.req_ctx.txn_start_ts,
                table_id,
                self.req_ctx.tag,
                self.req_ctx.is_desc_scan,
                self.exec_metrics.cf_stats.total_op_count(),
                self.exec_metrics.cf_stats.total_processed(),
                self.req_ctx.ranges_len,
                self.req_ctx.first_range,
                self.total_perf_statistics,
            );
        }

        // `wait_time` is none means the request has not entered thread pool.
        if self.wait_time.is_none() {
            COPR_PENDING_REQS
                .with_label_values(&[self.req_ctx.tag, self.pri_str])
                .dec();

            // For the request is failed before enter into thread pool.
            let wait_start = self.wait_start.take().unwrap();
            let now = Instant::now_coarse();
            let wait_time = duration_to_sec(now - wait_start);
            BasicLocalMetrics::default()
                .wait_time
                .with_label_values(&[self.req_ctx.tag])
                .observe(wait_time);
            return;
        }

        let ctx_pool = self.ctx_pool.take().unwrap();
        let mut cop_ctx = ctx_pool.current_thread_context_mut();

        cop_ctx
            .basic_local_metrics
            .req_time
            .with_label_values(&[self.req_ctx.tag])
            .observe(duration_to_sec(self.start.elapsed()));
        cop_ctx
            .basic_local_metrics
            .handle_time
            .with_label_values(&[self.req_ctx.tag])
            .observe(self.total_handle_time);
        cop_ctx
            .basic_local_metrics
            .scan_keys
            .with_label_values(&[self.req_ctx.tag])
            .observe(self.exec_metrics.cf_stats.total_op_count() as f64);

        cop_ctx
            .basic_local_metrics
            .rocksdb_perf_stats
            .with_label_values(&[self.req_ctx.tag, "internal_key_skipped_count"])
            .inc_by(self.total_perf_statistics.internal_key_skipped_count as i64);
        cop_ctx
            .basic_local_metrics
            .rocksdb_perf_stats
            .with_label_values(&[self.req_ctx.tag, "internal_delete_skipped_count"])
            .inc_by(self.total_perf_statistics.internal_delete_skipped_count as i64);
        cop_ctx
            .basic_local_metrics
            .rocksdb_perf_stats
            .with_label_values(&[self.req_ctx.tag, "block_cache_hit_count"])
            .inc_by(self.total_perf_statistics.block_cache_hit_count as i64);
        cop_ctx
            .basic_local_metrics
            .rocksdb_perf_stats
            .with_label_values(&[self.req_ctx.tag, "block_read_count"])
            .inc_by(self.total_perf_statistics.block_read_count as i64);
        cop_ctx
            .basic_local_metrics
            .rocksdb_perf_stats
            .with_label_values(&[self.req_ctx.tag, "block_read_byte"])
            .inc_by(self.total_perf_statistics.block_read_byte as i64);

        let exec_metrics = ::std::mem::replace(&mut self.exec_metrics, ExecutorMetrics::default());
        cop_ctx.collect(
            self.req_ctx.context.get_region_id(),
            self.req_ctx.tag,
            exec_metrics,
        );
    }
}

const STR_REQ_PRI_LOW: &str = "low";
const STR_REQ_PRI_NORMAL: &str = "normal";
const STR_REQ_PRI_HIGH: &str = "high";

#[inline]
fn get_req_pri_str(pri: CommandPri) -> &'static str {
    match pri {
        CommandPri::Low => STR_REQ_PRI_LOW,
        CommandPri::Normal => STR_REQ_PRI_NORMAL,
        CommandPri::High => STR_REQ_PRI_HIGH,
    }
}
