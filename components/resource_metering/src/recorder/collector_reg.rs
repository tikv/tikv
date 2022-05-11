// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use tikv_util::{
    warn,
    worker::{Scheduler, Worker},
};

use crate::{recorder::Task, Collector};

/// `CollectorRegHandle` accepts registrations of [Collector].
///
/// [Collector]: crate::collector::Collector
#[derive(Clone)]
pub struct CollectorRegHandle {
    scheduler: Scheduler<Task>,
}

impl CollectorRegHandle {
    pub(crate) fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }

    pub fn new_for_test() -> Self {
        Self {
            scheduler: Worker::new("mock-collector-reg-handle")
                .lazy_build("mock-collector-reg-handle")
                .scheduler(),
        }
    }

    /// Register a collector to the recorder. Dropping the returned [CollectorGuard] will
    /// preform deregistering.
    ///
    /// The second argument `as_observer` indicates that whether the given `collector` will
    /// control the enabled state of the recorder:
    /// - When `as_observer` is false, the recorder will respect it and begin to profile if it's
    ///   off before. In other words, if there is at least one non-observed collector, the recorder
    ///   will keep running.
    /// - When `as_observer` is true, whether the recorder to be on or off won't depend on if
    ///   the collector exists.
    pub fn register(&self, collector: Box<dyn Collector>, as_observer: bool) -> CollectorGuard {
        static NEXT_COLLECTOR_ID: AtomicU64 = AtomicU64::new(1);
        let id = CollectorId(NEXT_COLLECTOR_ID.fetch_add(1, Ordering::SeqCst));

        let reg_msg = Task::CollectorReg(CollectorReg::Register {
            collector,
            as_observer,
            id,
        });
        match self.scheduler.schedule(reg_msg) {
            Ok(_) => CollectorGuard {
                id,
                tx: Some(self.scheduler.clone()),
            },
            Err(err) => {
                warn!("failed to register collector"; "err" => ?err);
                CollectorGuard { id, tx: None }
            }
        }
    }
}

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Hash)]
pub struct CollectorId(pub u64);

pub enum CollectorReg {
    Register {
        id: CollectorId,
        as_observer: bool,
        collector: Box<dyn Collector>,
    },
    Deregister {
        id: CollectorId,
    },
}

pub struct CollectorGuard {
    id: CollectorId,
    tx: Option<Scheduler<Task>>,
}

impl Drop for CollectorGuard {
    fn drop(&mut self) {
        if self.tx.is_none() {
            return;
        }

        if let Err(err) = self
            .tx
            .as_ref()
            .unwrap()
            .schedule(Task::CollectorReg(CollectorReg::Deregister { id: self.id }))
        {
            warn!("failed to unregister collector"; "err" => ?err);
        }
    }
}
