// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::Collector;
use crate::cpu::RawCpuRecords;
use crate::reporter::Task;
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
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

/// In the cpu module, we specially provide a method of dynamically
/// registering/unloading the collectors.
pub fn register_cpu_dyn_collector(
    collector: Box<dyn Collector<Arc<RawCpuRecords>>>,
) -> DynCpuCollectorHandle {
    static NEXT_COLLECTOR_ID: AtomicU64 = AtomicU64::new(1);
    let id = DynCpuCollectorId(NEXT_COLLECTOR_ID.fetch_add(1, Relaxed));
    COLLECTOR_REGISTRATION_CHANNEL
        .0
        .send(DynCpuCollectorReg::Register { collector, id })
        .ok();
    DynCpuCollectorHandle { id }
}

lazy_static! {
    pub(crate) static ref COLLECTOR_REGISTRATION_CHANNEL: (Sender<DynCpuCollectorReg>, Receiver<DynCpuCollectorReg>) =
        unbounded();
}

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DynCpuCollectorId(pub(crate) u64);

#[allow(dead_code)] // Avoid warning, already used in cpu::recorder::linux
pub(crate) enum DynCpuCollectorReg {
    Register {
        collector: Box<dyn Collector<Arc<RawCpuRecords>>>,
        id: DynCpuCollectorId,
    },
    Unregister {
        id: DynCpuCollectorId,
    },
}

pub struct DynCpuCollectorHandle {
    id: DynCpuCollectorId,
}

impl Drop for DynCpuCollectorHandle {
    fn drop(&mut self) {
        COLLECTOR_REGISTRATION_CHANNEL
            .0
            .send(DynCpuCollectorReg::Unregister { id: self.id })
            .ok();
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
