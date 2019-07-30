// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use crate::coprocessor::metrics::*;
use crate::pd::PdTask;
use crate::storage::kv::{FlowStatistics, Statistics};
use prometheus::local::{LocalHistogramVec, LocalIntCounterVec};
use tikv_util::collections::HashMap;
use tikv_util::worker::FutureScheduler;

/// `CopFlowStatistics` is for flow statistics, it would be reported to PD by flush.
pub struct CopFlowStatistics {
    data: HashMap<u64, FlowStatistics>,
    sender: FutureScheduler<PdTask>,
}

impl CopFlowStatistics {
    pub fn new(sender: FutureScheduler<PdTask>) -> CopFlowStatistics {
        CopFlowStatistics {
            sender,
            data: Default::default(),
        }
    }

    pub fn add(&mut self, region_id: u64, stats: &Statistics) {
        let flow_stats = self.data.entry(region_id).or_default();
        flow_stats.add(&stats.write.flow_stats);
        flow_stats.add(&stats.data.flow_stats);
    }

    pub fn flush(&mut self) {
        if self.data.is_empty() {
            return;
        }
        let mut to_send_stats = HashMap::default();
        mem::swap(&mut to_send_stats, &mut self.data);
        if let Err(e) = self.sender.schedule(PdTask::ReadStats {
            read_stats: to_send_stats,
        }) {
            error!(
                "send coprocessor statistics failed";
                "err" => %e
            );
        };
    }
}

/// `BasicLocalMetrics` is for the basic metrics for coprocessor requests.
pub struct BasicLocalMetrics {
    pub req_time: LocalHistogramVec,
    pub handle_time: LocalHistogramVec,
    pub wait_time: LocalHistogramVec,
    pub error_cnt: LocalIntCounterVec,
    pub scan_keys: LocalHistogramVec,
    pub rocksdb_perf_stats: LocalIntCounterVec,
}

impl Default for BasicLocalMetrics {
    fn default() -> BasicLocalMetrics {
        BasicLocalMetrics {
            req_time: COPR_REQ_HISTOGRAM_VEC.local(),
            handle_time: COPR_REQ_HANDLE_TIME.local(),
            wait_time: COPR_REQ_WAIT_TIME.local(),
            error_cnt: COPR_REQ_ERROR.local(),
            scan_keys: COPR_SCAN_KEYS.local(),
            rocksdb_perf_stats: COPR_ROCKSDB_PERF_COUNTER.local(),
        }
    }
}

impl BasicLocalMetrics {
    pub fn flush(&mut self) {
        self.req_time.flush();
        self.handle_time.flush();
        self.wait_time.flush();
        self.scan_keys.flush();
        self.error_cnt.flush();
        self.rocksdb_perf_stats.flush();
    }
}

impl Drop for BasicLocalMetrics {
    fn drop(&mut self) {
        self.flush();
    }
}
