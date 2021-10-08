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
pub trait Collector: Send {
    fn collect(&self, records: Arc<RawRecords>);
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
        self.scheduler.schedule(Task::Records(records)).ok();
    }
}

impl CollectorImpl {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

lazy_static! {
    pub static ref COLLECTOR_REG_CHAN: (Sender<CollectorReg>, Receiver<CollectorReg>) = unbounded();
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
}

impl Drop for CollectorHandle {
    fn drop(&mut self) {
        COLLECTOR_REG_CHAN
            .0
            .send(CollectorReg::Unregister { id: self.id })
            .ok();
    }
}

/// Dynamically registering a collector.
pub fn register_collector(collector: Box<dyn Collector>) -> CollectorHandle {
    static NEXT_COLLECTOR_ID: AtomicU64 = AtomicU64::new(1);
    let id = CollectorId(NEXT_COLLECTOR_ID.fetch_add(1, Relaxed));
    COLLECTOR_REG_CHAN
        .0
        .send(CollectorReg::Register { collector, id })
        .ok();
    CollectorHandle { id }
}
