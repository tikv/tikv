// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::Statistics;
use prometheus::local::LocalIntCounterVec;

/// `ExecutorMetrics` is metrics collected from executors group by request.
#[derive(Default, Clone, Debug)]
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
#[derive(Default, Clone, Debug)]
pub struct ScanCounter {
    pub range: usize,
    pub point: usize,
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

    pub fn consume(self, metrics: &mut LocalIntCounterVec) {
        if self.point > 0 {
            metrics
                .with_label_values(&["point"])
                .inc_by(self.point as i64);
        }
        if self.range > 0 {
            metrics
                .with_label_values(&["range"])
                .inc_by(self.range as i64);
        }
    }
}

/// `ExecCounter` is for recording number of each executor.
#[derive(Default, Clone, Debug)]
pub struct ExecCounter {
    pub aggregation: i64,
    pub index_scan: i64,
    pub limit: i64,
    pub selection: i64,
    pub table_scan: i64,
    pub topn: i64,
}

impl ExecCounter {
    pub fn merge(&mut self, other: &mut ExecCounter) {
        self.aggregation += other.aggregation;
        self.index_scan += other.index_scan;
        self.limit += other.limit;
        self.selection += other.selection;
        self.table_scan += other.table_scan;
        self.topn += other.topn;
        *other = ExecCounter::default();
    }

    pub fn consume(self, metrics: &mut LocalIntCounterVec) {
        metrics
            .with_label_values(&["tblscan"])
            .inc_by(self.table_scan);
        metrics
            .with_label_values(&["idxscan"])
            .inc_by(self.index_scan);
        metrics
            .with_label_values(&["selection"])
            .inc_by(self.selection);
        metrics.with_label_values(&["topn"]).inc_by(self.topn);
        metrics.with_label_values(&["limit"]).inc_by(self.limit);
        metrics
            .with_label_values(&["aggregation"])
            .inc_by(self.aggregation);
    }
}
