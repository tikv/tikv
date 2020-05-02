// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::server::metrics::{GcKeysCF, GcKeysDetail};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::{ScanDetail, ScanInfo};
pub use raftstore::store::{FlowStatistics, FlowStatsReporter};

const STAT_TOTAL: &str = "total";
const STAT_PROCESSED: &str = "processed";
const STAT_GET: &str = "get";
const STAT_NEXT: &str = "next";
const STAT_PREV: &str = "prev";
const STAT_SEEK: &str = "seek";
const STAT_SEEK_FOR_PREV: &str = "seek_for_prev";
const STAT_OVER_SEEK_BOUND: &str = "over_seek_bound";

/// Statistics collects the ops taken when fetching data.
#[derive(Default, Clone, Debug)]
pub struct CfStatistics {
    // How many keys that's effective to user. This counter should be increased
    // by the caller.
    pub processed: usize,
    pub get: usize,
    pub next: usize,
    pub prev: usize,
    pub seek: usize,
    pub seek_for_prev: usize,
    pub over_seek_bound: usize,
    pub flow_stats: FlowStatistics,
}

impl CfStatistics {
    #[inline]
    pub fn total_op_count(&self) -> usize {
        self.get + self.next + self.prev + self.seek + self.seek_for_prev
    }

    pub fn details(&self) -> [(&'static str, usize); 8] {
        [
            (STAT_TOTAL, self.total_op_count()),
            (STAT_PROCESSED, self.processed),
            (STAT_GET, self.get),
            (STAT_NEXT, self.next),
            (STAT_PREV, self.prev),
            (STAT_SEEK, self.seek),
            (STAT_SEEK_FOR_PREV, self.seek_for_prev),
            (STAT_OVER_SEEK_BOUND, self.over_seek_bound),
        ]
    }

    pub fn details_enum(&self) -> [(GcKeysDetail, usize); 8] {
        [
            (GcKeysDetail::total, self.total_op_count()),
            (GcKeysDetail::processed, self.processed),
            (GcKeysDetail::get, self.get),
            (GcKeysDetail::next, self.next),
            (GcKeysDetail::prev, self.prev),
            (GcKeysDetail::seek, self.seek),
            (GcKeysDetail::seek_for_prev, self.seek_for_prev),
            (GcKeysDetail::over_seek_bound, self.over_seek_bound),
        ]
    }

    pub fn add(&mut self, other: &Self) {
        self.processed = self.processed.saturating_add(other.processed);
        self.get = self.get.saturating_add(other.get);
        self.next = self.next.saturating_add(other.next);
        self.prev = self.prev.saturating_add(other.prev);
        self.seek = self.seek.saturating_add(other.seek);
        self.seek_for_prev = self.seek_for_prev.saturating_add(other.seek_for_prev);
        self.over_seek_bound = self.over_seek_bound.saturating_add(other.over_seek_bound);
        self.flow_stats.add(&other.flow_stats);
    }

    pub fn scan_info(&self) -> ScanInfo {
        let mut info = ScanInfo::default();
        info.set_processed(self.processed as i64);
        info.set_total(self.total_op_count() as i64);
        info
    }
}

#[derive(Default, Clone, Debug)]
pub struct Statistics {
    pub lock: CfStatistics,
    pub write: CfStatistics,
    pub data: CfStatistics,
}

impl Statistics {
    pub fn total_op_count(&self) -> usize {
        self.lock.total_op_count() + self.write.total_op_count() + self.data.total_op_count()
    }

    pub fn total_processed(&self) -> usize {
        self.lock.processed + self.write.processed + self.data.processed
    }

    pub fn details(&self) -> [(&'static str, [(&'static str, usize); 8]); 3] {
        [
            (CF_DEFAULT, self.data.details()),
            (CF_LOCK, self.lock.details()),
            (CF_WRITE, self.write.details()),
        ]
    }

    pub fn details_enum(&self) -> [(GcKeysCF, [(GcKeysDetail, usize); 8]); 3] {
        [
            (GcKeysCF::default, self.data.details_enum()),
            (GcKeysCF::lock, self.lock.details_enum()),
            (GcKeysCF::write, self.write.details_enum()),
        ]
    }

    pub fn add(&mut self, other: &Self) {
        self.lock.add(&other.lock);
        self.write.add(&other.write);
        self.data.add(&other.data);
    }

    pub fn scan_detail(&self) -> ScanDetail {
        let mut detail = ScanDetail::default();
        detail.set_data(self.data.scan_info());
        detail.set_lock(self.lock.scan_info());
        detail.set_write(self.write.scan_info());
        detail
    }

    pub fn mut_cf_statistics(&mut self, cf: &str) -> &mut CfStatistics {
        if cf.is_empty() {
            return &mut self.data;
        }
        match cf {
            CF_DEFAULT => &mut self.data,
            CF_LOCK => &mut self.lock,
            CF_WRITE => &mut self.write,
            _ => unreachable!(),
        }
    }
}

#[derive(Default, Debug)]
pub struct StatisticsSummary {
    pub stat: Statistics,
    pub count: u64,
}

impl StatisticsSummary {
    pub fn add_statistics(&mut self, v: &Statistics) {
        self.stat.add(v);
        self.count += 1;
    }
}
