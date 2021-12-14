// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::IGNORED_DATA_COUNTER;
use crate::{RawRecords, Task};

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam::channel::{bounded, Sender};
use tikv_util::warn;
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
pub trait Collector: Send {
    fn collect(&self, records: Arc<RawRecords>);
}

/// `CollectorRegHandle` accepts registrations of [Collector].
///
/// [Collector]: crate::collector::Collector
#[derive(Clone)]
pub struct CollectorRegHandle {
    tx: Sender<CollectorReg>,
}

impl CollectorRegHandle {
    pub(crate) fn new(tx: Sender<CollectorReg>) -> Self {
        Self { tx }
    }

    pub fn new_for_test() -> Self {
        let (tx, _) = bounded(1024);
        Self { tx }
    }

    pub fn register(&self, collector: Box<dyn Collector>) -> CollectorHandle {
        static NEXT_COLLECTOR_ID: AtomicU64 = AtomicU64::new(1);
        let id = CollectorId(NEXT_COLLECTOR_ID.fetch_add(1, Ordering::SeqCst));

        let reg_msg = CollectorReg::Register { collector, id };
        match self.tx.send(reg_msg) {
            Ok(_) => CollectorHandle {
                id,
                tx: Some(self.tx.clone()),
            },
            Err(err) => {
                warn!("failed to register collector"; "err" => ?err);
                CollectorHandle { id, tx: None }
            }
        }
    }
}

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

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Hash)]
pub struct CollectorId(pub u64);

pub enum CollectorReg {
    Register {
        id: CollectorId,
        collector: Box<dyn Collector>,
    },
    Unregister {
        id: CollectorId,
    },
}

pub struct CollectorHandle {
    id: CollectorId,
    tx: Option<Sender<CollectorReg>>,
}

impl Drop for CollectorHandle {
    fn drop(&mut self) {
        if self.tx.is_none() {
            return;
        }

        if let Err(err) = self
            .tx
            .as_ref()
            .unwrap()
            .send(CollectorReg::Unregister { id: self.id })
        {
            warn!("failed to unregister collector"; "err" => ?err);
        }
    }
}
