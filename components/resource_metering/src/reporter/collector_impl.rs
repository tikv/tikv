// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tikv_util::{warn, worker::Scheduler};

use crate::{metrics::IGNORED_DATA_COUNTER, Collector, RawRecords, Task};

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
