// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::pd::PdTask;
use crate::server::readpool::{self, Builder, ReadPool};
use tikv_util::worker::FutureScheduler;

use super::metrics::*;
use prometheus::local::*;

use crate::coprocessor::dag::executor::ExecutorMetrics;

pub struct CopLocalMetrics {
    pub local_copr_req_histogram_vec: LocalHistogramVec,
    pub local_outdated_req_wait_time: LocalHistogramVec,
    pub local_copr_req_handle_time: LocalHistogramVec,
    pub local_copr_req_wait_time: LocalHistogramVec,
    pub local_copr_req_error: LocalIntCounterVec,
    pub local_copr_scan_keys: LocalHistogramVec,
    pub local_copr_scan_details: LocalIntCounterVec,
    pub local_copr_rocksdb_perf_counter: LocalIntCounterVec,
    local_copr_executor_count: LocalIntCounterVec,
    local_copr_get_or_scan_count: LocalIntCounterVec,
    local_cop_flow_stats: HashMap<u64, crate::storage::FlowStatistics>,
}

thread_local! {
    pub static TLS_COP_METRICS: RefCell<CopLocalMetrics> = RefCell::new(
        CopLocalMetrics {
            local_copr_req_histogram_vec:
                COPR_REQ_HISTOGRAM_VEC.local(),
            local_outdated_req_wait_time:
                OUTDATED_REQ_WAIT_TIME.local(),
            local_copr_req_handle_time:
                COPR_REQ_HANDLE_TIME.local(),
            local_copr_req_wait_time:
                COPR_REQ_WAIT_TIME.local(),
            local_copr_req_error:
                COPR_REQ_ERROR.local(),
            local_copr_scan_keys:
                COPR_SCAN_KEYS.local(),
            local_copr_scan_details:
                COPR_SCAN_DETAILS.local(),
            local_copr_rocksdb_perf_counter:
                COPR_ROCKSDB_PERF_COUNTER.local(),
            local_copr_executor_count:
                COPR_EXECUTOR_COUNT.local(),
            local_copr_get_or_scan_count:
                COPR_GET_OR_SCAN_COUNT.local(),
            local_cop_flow_stats:
                HashMap::default(),
        }
    );
}

pub fn build_read_pool(
    config: &readpool::Config,
    pd_sender: FutureScheduler<PdTask>,
    name_prefix: &str,
) -> ReadPool {
    let pd_sender2 = pd_sender.clone();

    Builder::from_config(config)
        .name_prefix(name_prefix)
        .on_tick(move || tls_flush(&pd_sender))
        .before_stop(move || tls_flush(&pd_sender2))
        .build()
}

#[inline]
fn tls_flush(pd_sender: &FutureScheduler<PdTask>) {
    TLS_COP_METRICS.with(|m| {
        // Flush Prometheus metrics
        let mut cop_metrics = m.borrow_mut();
        cop_metrics.local_copr_req_histogram_vec.flush();
        cop_metrics.local_copr_req_handle_time.flush();
        cop_metrics.local_copr_req_wait_time.flush();
        cop_metrics.local_copr_scan_keys.flush();
        cop_metrics.local_copr_rocksdb_perf_counter.flush();
        cop_metrics.local_copr_scan_details.flush();
        cop_metrics.local_copr_get_or_scan_count.flush();
        cop_metrics.local_copr_executor_count.flush();

        // Report PD metrics
        if cop_metrics.local_cop_flow_stats.is_empty() {
            // Stats to report to PD is empty, ignore.
            return;
        }

        let read_stats = cop_metrics.local_cop_flow_stats.clone();
        cop_metrics.local_cop_flow_stats = HashMap::default();

        let result = pd_sender.schedule(PdTask::ReadStats { read_stats });
        if let Err(e) = result {
            error!("Failed to send cop pool read flow statistics"; "err" => ?e);
        }
    });
}

pub fn tls_collect_executor_metrics(region_id: u64, type_str: &str, metrics: ExecutorMetrics) {
    let stats = &metrics.cf_stats;
    // cf statistics group by type
    for (cf, details) in stats.details() {
        for (tag, count) in details {
            TLS_COP_METRICS.with(|m| {
                m.borrow_mut()
                    .local_copr_scan_details
                    .with_label_values(&[type_str, cf, tag])
                    .inc_by(count as i64);
            });
        }
    }
    // flow statistics group by region
    tls_collect_read_flow(region_id, stats);

    // scan count
    let scan_counter = metrics.scan_counter;
    // exec count
    let executor_count = metrics.executor_count;
    TLS_COP_METRICS.with(|m| {
        scan_counter.consume(&mut m.borrow_mut().local_copr_get_or_scan_count);
        executor_count.consume(&mut m.borrow_mut().local_copr_executor_count);
    });
}

#[inline]
pub fn tls_collect_read_flow(region_id: u64, statistics: &crate::storage::Statistics) {
    TLS_COP_METRICS.with(|m| {
        let map = &mut m.borrow_mut().local_cop_flow_stats;
        let flow_stats = map
            .entry(region_id)
            .or_insert_with(crate::storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    });
}
