// Copyright 2017 PingCAP, Inc.
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

use coprocessor::metrics::*;
use storage::engine::Statistics;
use prometheus::local::LocalHistogramVec;
use util::collections::HashMap;
#[derive(Default, Clone)]
pub struct CopMetrics {
    pub cf_stats: Statistics,
    pub scan_counter: ScanCounter,
    //TODO:
    pub executor_count: HashMap<&'static str, i64>,
}

impl CopMetrics {
    /// Merge records from `other` into `self`, and clear `other`.
    #[inline]
    pub fn merge(&mut self, other: &mut CopMetrics) {
        self.cf_stats.add(&other.cf_stats);
        self.cf_stats = Default::default();
        self.scan_counter.merge(&mut other.scan_counter);
    }
}

/// `ScanCounter` is for recording range query and point query.
#[derive(Default, Clone)]
pub struct ScanCounter {
    range: usize,
    point: usize,
}

impl ScanCounter {
    #[inline]
    pub fn inc_range(&mut self) {
        self.range += 1;
    }

    #[inline]
    pub fn inc_point(&mut self) {
        self.point += 1;
    }

    /// Merge records from `other` into `self`, and clear `other`.
    #[inline]
    pub fn merge(&mut self, other: &mut ScanCounter) {
        self.range += other.range;
        self.point += other.point;
        other.range = 0;
        other.point = 0;
    }

    #[inline]
    pub fn flush(&mut self) {
        if self.range > 0 {
            let range_counter = COPR_GET_OR_SCAN_COUNT.with_label_values(&["range"]);
            range_counter.inc_by(self.range as f64).unwrap();
            self.range = 0;
        }
        if self.point > 0 {
            let point_counter = COPR_GET_OR_SCAN_COUNT.with_label_values(&["point"]);
            point_counter.inc_by(self.point as f64).unwrap();
            self.point = 0;
        }
    }
}

#[derive(Default, Clone)]
pub struct ExecCounter {
    data: HashMap<&'static str, i64>,
}

impl ExecCounter {
    pub fn merge(&mut self, data: &mut HashMap<&'static str, i64>) {
        for (k, v) in data.drain() {
            let mut va = self.data.entry(k).or_insert(0);
            *va += v;
        }
    }

    pub fn flush(&mut self) {
        for (k, v) in self.data.drain() {
            COPR_EXECUTOR_COUNT
                .with_label_values(&[k])
                .inc_by(v as f64)
                .unwrap();
        }
    }
}

pub struct LocalMetrics {
    pub req_time: LocalHistogramVec,
    pub outdate_time: LocalHistogramVec,
    pub handle_time: LocalHistogramVec,
    pub wait_time: LocalHistogramVec,
    pub error_cnt: HashMap<&'static str, i64>,
    pub pending_cnt: HashMap<&'static str, HashMap<&'static str, i64>>,
    pub scan_keys: LocalHistogramVec,
}

impl Default for LocalMetrics {
    fn default() -> LocalMetrics {
        LocalMetrics {
            req_time: COPR_REQ_HISTOGRAM_VEC.local(),
            outdate_time: OUTDATED_REQ_WAIT_TIME.local(),
            handle_time: COPR_REQ_HANDLE_TIME.local(),
            wait_time: COPR_REQ_WAIT_TIME.local(),
            error_cnt: HashMap::default(),
            pending_cnt: HashMap::default(),
            scan_keys: COPR_SCAN_KEYS.local(),
        }
    }
}

impl LocalMetrics {
    pub fn flush(&mut self) {
        self.req_time.flush();
        self.outdate_time.flush();
        self.handle_time.flush();
        self.wait_time.flush();
        self.scan_keys.flush();
        // error cnt
        for (k, v) in self.error_cnt.drain() {
            COPR_REQ_ERROR
                .with_label_values(&[k])
                .inc_by(v as f64)
                .unwrap();
        }
        // pending cnt
        for (req, v) in self.pending_cnt.drain() {
            for (pri, count) in v {
                if count != 0 {
                    COPR_PENDING_REQS
                        .with_label_values(&[req, pri])
                        .add(count as f64);
                }
            }
        }
    }

    pub fn add_pending_reqs(&mut self, type_str: &'static str, pri: &'static str, count: i64) {
        let mut v = self.pending_cnt
            .entry(type_str)
            .or_insert_with(Default::default);
        let mut data = v.entry(pri).or_insert(0);
        *data += count
    }
}

impl Drop for LocalMetrics {
    fn drop(&mut self) {
        self.flush();
    }
}
