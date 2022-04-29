// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

#[cfg(target_os = "linux")]
mod threads_linux;
#[cfg(target_os = "linux")]
pub use self::threads_linux::{monitor_threads, ThreadInfoStatistics};

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

use kvproto::pdpb;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
pub type RecordPairVec = Vec<pdpb::RecordPair>;


lazy_static! {
    // the registry for high priority metrics.
    pub static ref HIGH_PRIORITY_REGISTRY: Registry = Registry::new();
    
    static ref METRICS_DUMP_COUNTER: AtomicU64 = Default::default();
}

// the factor in simplify mode to return full metrics.
const METRICS_DUMP_FACTOR: u64 = 3;


pub fn dump(should_simplify: bool) -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();

    dump_metrics(
        HIGH_PRIORITY_REGISTRY.gather(),
        &encoder,
        &mut buffer,
        should_simplify,
    );
    if !should_simplify || METRICS_DUMP_COUNTER.fetch_add(1, Ordering::Relaxed) % METRICS_DUMP_FACTOR == 0 {
        dump_metrics(prometheus::gather(), &encoder, &mut buffer, should_simplify);
    }

    String::from_utf8(buffer).unwrap()
}

fn dump_metrics<'a, E: Encoder, W: std::io::Write>(
    metric_families: Vec<proto::MetricFamily>,
    encoder: &E,
    writer: &mut W,
    should_simplify: bool,
) {
    use prometheus::proto::MetricType;
    if !should_simplify {
        if let Err(e) = encoder.encode(&*metric_families, writer) {
            warn!("prometheus encoding error"; "err" => ?e);
        }
        return;
    }

    for mut mf in metric_families {
        let mut metrics = mf.take_metric().into_vec();
        match mf.get_field_type() {
            // filter out counters with value 0.
            MetricType::COUNTER => {
                metrics.retain(|m| m.get_counter().get_value() > 0.0);
            }
            // filter all histogram that the total accumulated value is 0.
            MetricType::HISTOGRAM => {
                metrics.retain(|m| {
                    let buckets = m.get_histogram().get_bucket();
                    !buckets.is_empty() && buckets[buckets.len() - 1].get_cumulative_count() > 0
                });
            }
            _ => {}
        }

        if !metrics.is_empty() {
            mf.set_metric(metrics.into());
            if let Err(e) = encoder.encode(&[mf], writer) {
                warn!("prometheus encoding error"; "err" => ?e);
            }
        }
    }
}

make_auto_flush_static_metric! {
    // Some non-txn related types are placed here.
    // ref `TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC`.
    pub label_enum ThrottleType {
        dag,
        analyze_full_sampling,
    }

    pub struct NonTxnCommandThrottleTimeCounterVec: LocalIntCounter {
        "type" => ThrottleType,
    }
}

lazy_static! {
    pub static ref CRITICAL_ERROR: IntCounterVec = register_int_counter_vec_with_registry!(
        "tikv_critical_error_total",
        "Counter of critical error.",
        &["type"],
        HIGH_PRIORITY_REGISTRY
    )
    .unwrap();
    pub static ref NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_non_txn_command_throttle_time_total",
            "Total throttle time (microsecond) of non txn processing.",
            &["type"]
        )
        .unwrap();
    pub static ref NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC: NonTxnCommandThrottleTimeCounterVec = auto_flush_from!(
        NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC,
        NonTxnCommandThrottleTimeCounterVec
    );
}

pub fn convert_record_pairs(m: HashMap<String, u64>) -> RecordPairVec {
    m.into_iter()
        .map(|(k, v)| {
            let mut pair = pdpb::RecordPair::default();
            pair.set_key(k);
            pair.set_value(v);
            pair
        })
        .collect()
}
