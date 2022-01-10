// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::IGNORED_DATA_COUNTER;
use crate::{Collector, RawRecords, Task};

use std::sync::Arc;

use tikv_util::warn;
use tikv_util::worker::Scheduler;

/// A [Collector] implementation for scheduling [RawRecords].
///
/// See [Collector] for more relevant designs.
///
/// [RawRecords]: crate::model::RawRecords
pub struct CollectorImpl {
    scheduler: Scheduler<Task>,
}

impl Collector for CollectorImpl {
    fn collect(&self, records: Arc<RawRecords>) {
        let record_cnt = records.records.len();
        if let Err(err) = self.scheduler.schedule(Task::Records(records)) {
            IGNORED_DATA_COUNTER
                .with_label_values(&["collect"])
                .inc_by(record_cnt as _);
            warn!("failed to collect records"; "error" => ?err);
        }
    }
}

impl CollectorImpl {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}
