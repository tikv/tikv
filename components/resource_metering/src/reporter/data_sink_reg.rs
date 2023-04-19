// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use tikv_util::{warn, worker::Scheduler};

use super::Task;
use crate::DataSink;

/// `DataSinkRegHandle` accepts registrations of [DataSink].
///
/// [DataSink]: crate::DataSink
#[derive(Clone)]
pub struct DataSinkRegHandle {
    reporter_scheduler: Scheduler<Task>,
}

impl DataSinkRegHandle {
    pub(crate) fn new(reporter_scheduler: Scheduler<Task>) -> Self {
        Self { reporter_scheduler }
    }

    pub fn register(&self, data_sink: Box<dyn DataSink>) -> DataSinkGuard {
        static NEXT_DATA_SINK_ID: AtomicU64 = AtomicU64::new(1);
        let id = DataSinkId(NEXT_DATA_SINK_ID.fetch_add(1, Ordering::SeqCst));

        let reg_msg = Task::DataSinkReg(DataSinkReg::Register { data_sink, id });
        match self.reporter_scheduler.schedule(reg_msg) {
            Ok(_) => DataSinkGuard {
                id,
                reporter_scheduler: Some(self.reporter_scheduler.clone()),
            },
            Err(err) => {
                warn!("failed to register datasink"; "err" => ?err);
                DataSinkGuard {
                    id,
                    reporter_scheduler: None,
                }
            }
        }
    }
}

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Hash)]
pub struct DataSinkId(pub u64);

pub enum DataSinkReg {
    Register {
        id: DataSinkId,
        data_sink: Box<dyn DataSink>,
    },
    Deregister {
        id: DataSinkId,
    },
}

pub struct DataSinkGuard {
    id: DataSinkId,
    reporter_scheduler: Option<Scheduler<Task>>,
}

impl Drop for DataSinkGuard {
    fn drop(&mut self) {
        if self.reporter_scheduler.is_none() {
            return;
        }

        if let Err(err) = self
            .reporter_scheduler
            .as_ref()
            .unwrap()
            .schedule(Task::DataSinkReg(DataSinkReg::Deregister { id: self.id }))
        {
            warn!("failed to unregister datasink"; "err" => ?err);
        }
    }
}
