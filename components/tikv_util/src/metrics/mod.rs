// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;

#[cfg(target_os = "linux")]
mod threads_linux;
#[cfg(target_os = "linux")]
pub use self::threads_linux::{cpu_total, get_thread_ids, monitor_threads, ThreadInfoStatistics};

#[cfg(target_os = "linux")]
mod process_linux;
#[cfg(target_os = "linux")]
pub use self::process_linux::monitor_process;

#[cfg(not(target_os = "linux"))]
mod threads_dummy;
#[cfg(not(target_os = "linux"))]
pub use self::threads_dummy::{monitor_threads, ThreadInfoStatistics};

#[cfg(not(target_os = "linux"))]
mod process_dummy;
#[cfg(not(target_os = "linux"))]
pub use self::process_dummy::monitor_process;

pub use self::allocator_metrics::monitor_allocator_stats;

pub mod allocator_metrics;

pub use self::metrics_reader::HistogramReader;

mod metrics_reader;

pub fn dump() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    for mf in metric_families {
        if let Err(e) = encoder.encode(&[mf], &mut buffer) {
            warn!("prometheus encoding error"; "err" => ?e);
        }
    }
    String::from_utf8(buffer).unwrap()
}

lazy_static! {
    pub static ref CRITICAL_ERROR: IntCounterVec = register_int_counter_vec!(
        "tikv_critical_error_total",
        "Counter of critical error.",
        &["type"]
    )
    .unwrap();
}

#[cfg(not(target_os = "linux"))]
pub use self::threads_dummy::dump_thread_stats;
#[cfg(target_os = "linux")]
pub use self::threads_linux::dump_thread_stats;
