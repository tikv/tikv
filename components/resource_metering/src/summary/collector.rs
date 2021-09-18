// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::Collector;
use crate::reporter::Task;
use crate::summary::SummaryRecord;
use collections::HashMap;
use std::sync::Arc;
use tikv_util::worker::Scheduler;

/// A [Collector] implementation for scheduling summary records.
///
/// See [Collector] for more relevant designs.
///
/// [Collector]: crate::collector::Collector
pub struct SummaryCollector {
    scheduler: Scheduler<Task>,
}

impl Collector<Arc<HashMap<Vec<u8>, SummaryRecord>>> for SummaryCollector {
    fn collect(&self, records: Arc<HashMap<Vec<u8>, SummaryRecord>>) {
        self.scheduler.schedule(Task::SummaryRecords(records)).ok();
    }
}

impl SummaryCollector {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporter::Task;
    use tikv_util::worker::{LazyWorker, Runnable};

    struct MockRunner;

    impl Runnable for MockRunner {
        type Task = Task;

        fn run(&mut self, task: Self::Task) {
            assert!(matches!(task, Task::SummaryRecords(_)));
        }
    }

    #[test]
    fn test_collect() {
        let mut worker = LazyWorker::new("test-worker");
        worker.start(MockRunner);
        let collector = SummaryCollector::new(worker.scheduler());
        for _ in 0..3 {
            collector.collect(Arc::new(HashMap::default()));
        }
        worker.stop();
    }
}
