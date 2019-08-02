// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use crate::coprocessor::metrics::*;
use prometheus::local::{LocalHistogramVec, LocalIntCounterVec};

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
