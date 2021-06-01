// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod collector;
pub use collector::Collector;

mod future_ext;
pub use future_ext::FutureExt;

use crate::ResourceMeteringTag;

use std::time::{SystemTime, UNIX_EPOCH};

use collections::HashMap;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{init_recorder, register_collector, CollectorHandle, Guard};

#[cfg(not(target_os = "linux"))]
mod dummy;
#[cfg(not(target_os = "linux"))]
pub use dummy::{init_recorder, register_collector, CollectorHandle, Guard};

#[derive(Debug, Clone)]
pub struct CpuRecords {
    begin_unix_time_ms: u64,
    duration_ms: u64,
    records: HashMap<ResourceMeteringTag, u64>,
}

impl Default for CpuRecords {
    fn default() -> Self {
        let now_unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Clock may have gone backwards");
        Self {
            begin_unix_time_ms: now_unix_time.as_millis() as _,
            duration_ms: 0,
            records: HashMap::default(),
        }
    }
}

pub struct CpuRecorderConfig {
    record_interval_ms: f64,
    collect_interval_ms: u64,
    gc_interval_ms: u64,
}

impl Default for CpuRecorderConfig {
    fn default() -> Self {
        Self {
            record_interval_ms: 10.1,
            collect_interval_ms: 1_000,
            gc_interval_ms: 10 * 60 * 1_000,
        }
    }
}
