// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{RawRecords, Task};

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use tikv_util::worker::Scheduler;

/// `Collector` is used to connect [Recorder] and [Reporter].
///
/// The `Recorder` is mainly responsible for collecting data, and it is
/// only responsible for passing the collected data to the `Collector`.
/// The `Recorder` does not know anything about "scheduling" or "uploading".
///
/// Typically, constructing a `Collector` instance requires passing in a
/// [Scheduler], The `Collector` will send the data passed by the recorder
/// to the `Scheduler` for processing.
///
/// `Reporter` implements [Runnable] and [RunnableWithTimer], aggregates the
/// data sent by the `Collector` internally, and reports it regularly through RPC.
///
/// [Recorder]: crate::recorder::Recorder
/// [Reporter]: crate::reporter::Reporter
/// [Scheduler]: tikv_util::worker::Scheduler
/// [Runnable]: tikv_util::worker::Runnable
/// [RunnableWithTimer]: tikv_util::worker::RunnableWithTimer
pub trait Collector<T>: Send {
    fn collect(&self, records: T);
}

/// A [Collector] implementation for scheduling [RawRecords].
///
/// See [Collector] for more relevant designs.
///
/// [RawRecords]: crate::model::RawRecords
pub struct RawRecordsCollector {
    scheduler: Scheduler<Task>,
}

impl Collector<Arc<RawRecords>> for RawRecordsCollector {
    fn collect(&self, records: Arc<RawRecords>) {
        self.scheduler.schedule(Task::Records(records)).ok();
    }
}

impl RawRecordsCollector {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

lazy_static! {
    pub static ref COLLECTOR_REG_CHAN: (Sender<DynCollectorReg>, Receiver<DynCollectorReg>) =
        unbounded();
}

/// We specially provide a method of dynamically registering/unloading the collectors.
pub fn register_dyn_collector(
    collector: Box<dyn Collector<Arc<RawRecords>>>,
) -> DynCollectorHandle {
    static NEXT_COLLECTOR_ID: AtomicU64 = AtomicU64::new(1);
    let id = DynCollectorId(NEXT_COLLECTOR_ID.fetch_add(1, Relaxed));
    COLLECTOR_REG_CHAN
        .0
        .send(DynCollectorReg::Register { collector, id })
        .ok();
    DynCollectorHandle { id }
}

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Hash)]
pub struct DynCollectorId(pub u64);

pub enum DynCollectorReg {
    Register {
        id: DynCollectorId,
        collector: Box<dyn Collector<Arc<RawRecords>>>,
    },
    Unregister {
        id: DynCollectorId,
    },
}

pub struct DynCollectorHandle {
    id: DynCollectorId,
}

impl Drop for DynCollectorHandle {
    fn drop(&mut self) {
        COLLECTOR_REG_CHAN
            .0
            .send(DynCollectorReg::Unregister { id: self.id })
            .ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporter::Task;
    use std::sync::atomic::AtomicU32;
    use tikv_util::worker::{LazyWorker, Runnable};

    static OP_COUNT: AtomicU32 = AtomicU32::new(0);

    struct MockRunner;

    impl Runnable for MockRunner {
        type Task = Task;

        fn run(&mut self, task: Self::Task) {
            assert!(matches!(task, Task::Records(_)));
            OP_COUNT.fetch_add(1, Relaxed);
        }
    }

    #[test]
    fn test_collect() {
        let mut worker = LazyWorker::new("test-worker");
        worker.start(MockRunner);
        let collector = RawRecordsCollector::new(worker.scheduler());
        let n = 3;
        for _ in 0..n {
            collector.collect(Arc::new(RawRecords::default()));
        }
        worker.stop();
        assert_eq!(OP_COUNT.load(Relaxed), n);
    }
}
