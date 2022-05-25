// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, io::Write};

use kvproto::pdpb;
use lazy_static::lazy_static;
use prometheus::{proto::MetricType, *};
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
pub use self::allocator_metrics::monitor_allocator_stats;
#[cfg(not(target_os = "linux"))]
pub use self::process_dummy::monitor_process;

pub mod allocator_metrics;

pub use self::metrics_reader::HistogramReader;

mod metrics_reader;

pub type RecordPairVec = Vec<pdpb::RecordPair>;

pub fn dump(should_simplify: bool) -> String {
    let mut buffer = vec![];
    dump_to(&mut buffer, should_simplify);
    String::from_utf8(buffer).unwrap()
}

pub fn dump_to(w: &mut impl Write, should_simplify: bool) {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    if !should_simplify {
        if let Err(e) = encoder.encode(&*metric_families, w) {
            warn!("prometheus encoding error"; "err" => ?e);
        }
    }

    // filter out mertics that has no sample values
    for mut mf in metric_families {
        let mut metrics = mf.take_metric().into_vec();
        match mf.get_field_type() {
            MetricType::COUNTER => {
                metrics.retain(|m| m.get_counter().get_value() > 0.0);
            }
            MetricType::HISTOGRAM => metrics.retain(|m| m.get_histogram().get_sample_count() > 0),
            _ => {}
        }
        if !metrics.is_empty() {
            mf.set_metric(metrics.into());
            if let Err(e) = encoder.encode(&[mf], w) {
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
    pub static ref CRITICAL_ERROR: IntCounterVec = register_int_counter_vec!(
        "tikv_critical_error_total",
        "Counter of critical error.",
        &["type"]
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
