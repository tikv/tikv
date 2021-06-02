// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::recorder::CpuRecords;

use std::sync::Arc;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{register_collector, CollectorHandle};
#[cfg(target_os = "linux")]
pub(crate) use linux::{CollectorRegistrationMsg, COLLECTOR_REGISTRATION_CHANNEL};

#[cfg(not(target_os = "linux"))]
mod dummy;
#[cfg(not(target_os = "linux"))]
pub use dummy::{register_collector, CollectorHandle};

pub trait Collector: Send {
    fn collect(&self, records: Arc<CpuRecords>);
}

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Hash)]
pub struct CollectorId(pub(crate) u64);
