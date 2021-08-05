// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::collector::{Collector, CollectorId};

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;

lazy_static! {
    pub(crate) static ref COLLECTOR_REGISTRATION_CHANNEL: (
        Sender<CollectorRegistrationMsg>,
        Receiver<CollectorRegistrationMsg>
    ) = unbounded();
}
pub(crate) enum CollectorRegistrationMsg {
    Register {
        collector: Box<dyn Collector>,
        id: CollectorId,
    },
    Unregister {
        id: CollectorId,
    },
}

pub fn register_collector(collector: Box<dyn Collector>) -> CollectorHandle {
    static NEXT_COLLECTOR_ID: AtomicU64 = AtomicU64::new(1);
    let id = CollectorId(NEXT_COLLECTOR_ID.fetch_add(1, Relaxed));
    COLLECTOR_REGISTRATION_CHANNEL
        .0
        .send(CollectorRegistrationMsg::Register { collector, id })
        .ok();
    CollectorHandle { id }
}

pub struct CollectorHandle {
    id: CollectorId,
}
impl Drop for CollectorHandle {
    fn drop(&mut self) {
        COLLECTOR_REGISTRATION_CHANNEL
            .0
            .send(CollectorRegistrationMsg::Unregister { id: self.id })
            .ok();
    }
}
