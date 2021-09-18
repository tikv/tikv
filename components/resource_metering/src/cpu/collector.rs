// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::Collector;
use crate::cpu::RawCpuRecords;
use crate::reporter::Task;
use std::sync::Arc;
use tikv_util::worker::Scheduler;

/// A [Collector] implementation for scheduling cpu records.
///
/// See [Collector] for more relevant designs.
///
/// [Collector]: crate::collector::Collector
pub struct CpuCollector {
    scheduler: Scheduler<Task>,
}

impl Collector<Arc<RawCpuRecords>> for CpuCollector {
    fn collect(&self, records: Arc<RawCpuRecords>) {
        self.scheduler.schedule(Task::CpuRecords(records)).ok();
    }
}

impl CpuCollector {
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
            assert!(matches!(task, Task::CpuRecords(_)));
        }
    }

    #[test]
    fn test_collect() {
        let mut worker = LazyWorker::new("test-worker");
        worker.start(MockRunner);
        let collector = CpuCollector::new(worker.scheduler());
        for _ in 0..3 {
            collector.collect(Arc::new(RawCpuRecords::default()));
        }
        worker.stop();
    }
}
