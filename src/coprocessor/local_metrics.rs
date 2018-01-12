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

use std::mem;

use coprocessor::metrics::*;
use storage::engine::{FlowStatistics, Statistics, StatisticsSummary};
use prometheus::local::LocalHistogramVec;
use util::collections::HashMap;
use util::worker::FutureScheduler;
use pd::PdTask;

/// `ExecutorMetrics` is metrics collected from executors group by request.
#[derive(Default)]
pub struct ExecutorMetrics {
    pub cf_stats: Statistics,
    pub scan_counter: ScanCounter,
    pub executor_count: ExecCounter,
}

impl ExecutorMetrics {
    /// Merge records from `other` into `self`, and clear `other`.
    #[inline]
    pub fn merge(&mut self, other: &mut ExecutorMetrics) {
        self.cf_stats.add(&other.cf_stats);
        other.cf_stats = Default::default();
        self.scan_counter.merge(&mut other.scan_counter);
        self.executor_count.merge(&mut other.executor_count);
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

/// `ExecCounter` is for recording number of each executor.
#[derive(Default)]
pub struct ExecCounter {
    data: HashMap<&'static str, i64>,
}

impl ExecCounter {
    pub fn increase(&mut self, tag: &'static str) {
        let count = self.data.entry(tag).or_insert(0);
        *count += 1;
    }

    pub fn merge(&mut self, other: &mut ExecCounter) {
        for (k, v) in other.data.drain() {
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

/// `ScanDetails` is for scan details for each cf.
#[derive(Default)]
pub struct ScanDetails {
    data: HashMap<&'static str, StatisticsSummary>,
}

impl ScanDetails {
    pub fn add(&mut self, type_str: &'static str, other: &Statistics) {
        let mut statics = self.data.entry(type_str).or_insert_with(Default::default);
        statics.add_statistics(other);
    }

    pub fn flush(&mut self) {
        for (type_str, v) in self.data.drain() {
            for (cf, details) in v.stat.details() {
                for (tag, count) in details {
                    COPR_SCAN_DETAILS
                        .with_label_values(&[type_str, cf, tag])
                        .inc_by(count as f64)
                        .unwrap();
                }
            }
        }
    }
}

/// `CopFlowStatistics` is for flow statistics, it would been reported to Pd by flush.
pub struct CopFlowStatistics {
    data: HashMap<u64, FlowStatistics>,
    sender: FutureScheduler<PdTask>,
}

impl CopFlowStatistics {
    pub fn new(sender: FutureScheduler<PdTask>) -> CopFlowStatistics {
        CopFlowStatistics {
            sender: sender,
            data: Default::default(),
        }
    }

    pub fn add(&mut self, region_id: u64, stats: &Statistics) {
        let flow_stats = self.data
            .entry(region_id)
            .or_insert_with(FlowStatistics::default);
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
            error!("send coprocessor statistics: {:?}", e);
        };
    }
}

/// `ExecLocalMetrics` collects metrics from `ExectorMetrics`
pub struct ExecLocalMetrics {
    flow_stats: CopFlowStatistics,
    scan_details: ScanDetails,
    scan_counter: ScanCounter,
    exec_counter: ExecCounter,
}

impl ExecLocalMetrics {
    pub fn new(sender: FutureScheduler<PdTask>) -> ExecLocalMetrics {
        ExecLocalMetrics {
            flow_stats: CopFlowStatistics::new(sender),
            scan_details: Default::default(),
            scan_counter: Default::default(),
            exec_counter: Default::default(),
        }
    }

    pub fn finish_task(
        &mut self,
        type_str: &'static str,
        region_id: u64,
        mut metrics: ExecutorMetrics,
    ) {
        let stats = &metrics.cf_stats;
        // cf statistics group by type
        self.scan_details.add(type_str, stats);
        // flow statistics group by region
        self.flow_stats.add(region_id, stats);
        // scan count
        self.scan_counter.merge(&mut metrics.scan_counter);
        // executor count
        self.exec_counter.merge(&mut metrics.executor_count);
    }

    pub fn flush(&mut self) {
        self.flow_stats.flush();
        self.scan_details.flush();
        self.scan_counter.flush();
        self.exec_counter.flush();
    }
}

/// `BasicLocalMetrics` is for the basic metrics for coprocessor requests.
pub struct BasicLocalMetrics {
    pub req_time: LocalHistogramVec,
    pub outdate_time: LocalHistogramVec,
    pub handle_time: LocalHistogramVec,
    pub wait_time: LocalHistogramVec,
    pub error_cnt: HashMap<&'static str, i64>,
    pub pending_cnt: HashMap<&'static str, HashMap<&'static str, i64>>,
    pub scan_keys: LocalHistogramVec,
}

impl Default for BasicLocalMetrics {
    fn default() -> BasicLocalMetrics {
        BasicLocalMetrics {
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

impl BasicLocalMetrics {
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

impl Drop for BasicLocalMetrics {
    fn drop(&mut self) {
        self.flush();
    }
}
