// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
};

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

lazy_static! {
    // the registry for high priority metrics.
    pub static ref HIGH_PRIORITY_REGISTRY: Registry = Registry::new();
    // the registry for histograms that should always not be compacted.
    pub static ref FULL_HISTOGRAM_REGISTRY: Registry = Registry::new();
    // the registry for metrics that are no appeared in current grafana.
    pub static ref UNUSED_METRICS_REGISTRY: Registry = Registry::new();

    static ref METRICS_DUMP_COUNTER: AtomicU64 = Default::default();
}

// the factor in simplify mode to return full metrics.
const METRICS_DUMP_FACTOR: u64 = 2;

pub fn dump(should_simplify: bool) -> String {
    let mut buffer = vec![];
    dump_to(&mut buffer, should_simplify);
    String::from_utf8(buffer).unwrap()
}

pub fn dump_to(w: &mut impl Write, should_simplify: bool) {
    let encoder = TextEncoder::new();

    dump_metrics(
        HIGH_PRIORITY_REGISTRY.gather(),
        &encoder,
        w,
        should_simplify,
        true,
    );
    dump_metrics(
        FULL_HISTOGRAM_REGISTRY.gather(),
        &encoder,
        w,
        should_simplify,
        false,
    );
    if !should_simplify
        || METRICS_DUMP_COUNTER.fetch_add(1, Ordering::Relaxed) % METRICS_DUMP_FACTOR == 0
    {
        dump_metrics(prometheus::gather(), &encoder, w, should_simplify, true);
    }
    if !should_simplify {
        dump_metrics(UNUSED_METRICS_REGISTRY.gather(), &encoder, w, false, false);
    }
}

fn dump_metrics<E: Encoder, W: std::io::Write>(
    metric_families: Vec<proto::MetricFamily>,
    encoder: &E,
    writer: &mut W,
    should_simplify: bool,
    compact_histogram: bool,
) {
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
            // filter or compact histogram samples based on following rule:
            // 1. if the total cumulative count is 0(means no sample data at all), just remove this metric.
            // 2. if `compact_histogram` is true, then remove the middle in 3 successive sample bucket with the same cumulative count, or 2 if it is the last one.
            // example:
            // a histogram bucket sample can be represented as (bucket_threshold, accumulate_count) tuple for simplicity.
            // so give a full histogram buckets samples as:
            // (0.001, 10), (0.002, 10), (0.004, 10), (0.008, 15), (0.016, 15), (0.032, 16), (0.064, 16), (0.128, 16)
            // it can be campacted to:
            // (0.001, 10), (0.004, 10), (0.008, 15), (0.016, 15), (0.032, 16)
            // NOTE: in theory, this is not information loss after this compact, but due to the implementation by prometheus, exprs that depend on
            // adjacent bucket like `delta` can't be calculated correctly.
            MetricType::HISTOGRAM => {
                metrics.retain_mut(|m| {
                    let mut need_rewrite = false;
                    {
                        let buckets = m.get_histogram().get_bucket();
                        if buckets.is_empty()
                            || buckets[buckets.len() - 1].get_cumulative_count() == 0
                        {
                            return false;
                        }
                        if compact_histogram && buckets.len() > 1 {
                            let mut last_count = 0;
                            let mut same_count = 0;
                            for (i, b) in buckets.iter().enumerate() {
                                if b.get_cumulative_count() == last_count {
                                    same_count += 1;
                                    // we can remove the middle in 3 successive sample with the same cumulative count, or 2 if it is the last one.
                                    if same_count >= 3 || i == buckets.len() - 1 {
                                        need_rewrite = true;
                                        break;
                                    }
                                } else {
                                    last_count = b.get_cumulative_count();
                                    same_count = 1;
                                }
                            }
                        }
                    }
                    if !need_rewrite {
                        return true;
                    }
                    let mut buckets = m.mut_histogram().take_bucket().into_vec();
                    let mut last_count = buckets[0].get_cumulative_count();
                    // always keep the first one
                    let mut same_count = 1;
                    let mut i = 1;
                    let mut j = 1;
                    let len = buckets.len();
                    while i < len {
                        if buckets[i].get_cumulative_count() == last_count {
                            same_count += 1;
                        } else {
                            last_count = buckets[i].get_cumulative_count();
                            same_count = 1;
                        }
                        // skip the sample if it's cumulative count equals to its privious and next
                        if same_count < 2
                            || (i < len - 1
                                && buckets[i].get_cumulative_count()
                                    != buckets[i + 1].get_cumulative_count())
                        {
                            if i != j {
                                buckets.swap(i, j);
                            }
                            j += 1;
                        }
                        i += 1;
                    }

                    buckets.truncate(j);
                    m.mut_histogram().set_bucket(buckets.into());
                    true
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
