// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    io::Write,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use kvproto::pdpb;
use lazy_static::lazy_static;
use online_config::ConfigValue;
use prometheus::{
    proto::{MetricFamily, MetricType},
    register_int_counter_vec, register_int_counter_vec_with_registry, Encoder, IntCounterVec,
    Registry, TextEncoder,
};
use prometheus_static_metric::*;
use serde_repr::*;

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
}

static METRICS_COMPACT_LEVZEL: AtomicU32 = AtomicU32::new(MetricsCompactLevel::NoCompact as u32);
static METRICS_LEVEL: AtomicU32 = AtomicU32::new(MetricsLevel::All as u32);

// reduce the return frequency of normal metrics factor
const NORMAL_METRICS_REDUCE_FACTOR: u64 = 2;
static METRICS_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn set_metrics_compact_level(level: MetricsCompactLevel) {
    METRICS_COMPACT_LEVZEL.store(level as u32, Ordering::Relaxed);
}

pub fn get_metrics_compact_level() -> MetricsCompactLevel {
    METRICS_COMPACT_LEVZEL.load(Ordering::Relaxed).into()
}

pub fn set_metrics_level(level: MetricsLevel) {
    METRICS_LEVEL.store(level as u32, Ordering::Relaxed);
}

pub fn get_metrics_level() -> MetricsLevel {
    METRICS_LEVEL.load(Ordering::Relaxed).into()
}

pub fn should_return_normal_metrics() -> bool {
    let metrics_level = get_metrics_level();
    metrics_level == MetricsLevel::All
        || (metrics_level == MetricsLevel::ReduceFrequency
            && (METRICS_REQUEST_COUNTER.load(Ordering::Relaxed) % NORMAL_METRICS_REDUCE_FACTOR
                == 0))
}

pub fn dump() -> String {
    let mut buffer = vec![];

    dump_to(&mut buffer);
    String::from_utf8(buffer).unwrap()
}

pub fn dump_to(w: &mut impl Write) {
    METRICS_REQUEST_COUNTER.fetch_add(1, Ordering::Release);

    dump_metrics_to(w, HIGH_PRIORITY_REGISTRY.gather());
    if should_return_normal_metrics() {
        dump_metrics_to(w, prometheus::gather());
    }
}

pub fn dump_metrics_to(w: &mut impl Write, metric_families: Vec<MetricFamily>) {
    let simplify_level = get_metrics_compact_level();

    let encoder = TextEncoder::new();
    if simplify_level == MetricsCompactLevel::NoCompact {
        if let Err(e) = encoder.encode(&*metric_families, w) {
            warn!("prometheus encoding error"; "err" => ?e);
        }
        return;
    }

    // filter out mertics that has no sample values
    for mut mf in metric_families {
        let mut metrics = mf.take_metric().into_vec();
        match mf.get_field_type() {
            MetricType::COUNTER => {
                metrics.retain(|m| m.get_counter().get_value() > 0.0);
            }
            MetricType::HISTOGRAM => {
                let threshold = if simplify_level == MetricsCompactLevel::LoseLess {
                    0
                } else {
                    // only retain histogram that the sample count > 0.01 * max_sample_count
                    metrics
                        .iter()
                        .map(|m| m.get_histogram().get_sample_count())
                        .max()
                        .unwrap_or(0)
                        / 100
                };
                metrics.retain(|m| m.get_histogram().get_sample_count() > threshold);
            }
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

/// MetricsCompactLevel defines the level of compact metrics sample data, a higher level of
/// means smaller data size and more information loss.
#[derive(Serialize_repr, Deserialize_repr, Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u32)]
pub enum MetricsCompactLevel {
    // return full original data.
    NoCompact = 0,
    // this level try to compact sample without infromation loss.
    // currently only filter counter with 0 value and histogram with 0 samples.
    LoseLess = 1,
    // this level try to reduce the data size as much as possible.
    // this level also compact histogram vector type by remove histograms which sample count is
    // smaller than 1% of the max sample count.
    Aggressive = 2,
}

impl From<u32> for MetricsCompactLevel {
    fn from(v: u32) -> Self {
        match v {
            0 => Self::NoCompact,
            1 => Self::LoseLess,
            2 => Self::Aggressive,
            _ => unreachable!(),
        }
    }
}

impl From<MetricsCompactLevel> for ConfigValue {
    fn from(l: MetricsCompactLevel) -> ConfigValue {
        ConfigValue::U32(l as u32)
    }
}

impl From<ConfigValue> for MetricsCompactLevel {
    fn from(c: ConfigValue) -> MetricsCompactLevel {
        if let ConfigValue::U32(v) = c {
            v.into()
        } else {
            panic!("expect: ConfigValue::U32, got: {:?}", c);
        }
    }
}

impl From<&ConfigValue> for MetricsCompactLevel {
    fn from(c: &ConfigValue) -> Self {
        c.clone().into()
    }
}

// MetricsLevel controls how much metrics can be returned. A higher level means less data.
#[derive(Serialize_repr, Deserialize_repr, Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u32)]
pub enum MetricsLevel {
    // return all metrics, the lowest level.
    All = 0,
    // always return high priority metrics, returns low level metrics with a low freq (1/2 for now).
    ReduceFrequency = 1,
    // only return high priority metrics
    OnlyHighPriority = 2,
}

impl From<u32> for MetricsLevel {
    fn from(v: u32) -> Self {
        match v {
            0 => Self::All,
            1 => Self::ReduceFrequency,
            2 => Self::OnlyHighPriority,
            _ => unreachable!(),
        }
    }
}

impl From<MetricsLevel> for ConfigValue {
    fn from(l: MetricsLevel) -> ConfigValue {
        ConfigValue::U32(l as u32)
    }
}

impl From<ConfigValue> for MetricsLevel {
    fn from(c: ConfigValue) -> MetricsLevel {
        if let ConfigValue::U32(v) = c {
            v.into()
        } else {
            panic!("expect: ConfigValue::U32, got: {:?}", c);
        }
    }
}

impl From<&ConfigValue> for MetricsLevel {
    fn from(c: &ConfigValue) -> Self {
        c.clone().into()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use prometheus::*;

    use super::*;

    #[test]
    fn test_dump_metrics() {
        // register some metrics
        let _counter = register_int_counter!("test_counter", "this is a counter for test").unwrap();
        let counter_vec =
            register_int_counter_vec!("test_counter_vec", "test counter vec", &["label"]).unwrap();
        let histogram = register_histogram_vec!(
            "test_histogram",
            "test histogram",
            &["type"],
            exponential_buckets(0.01, 2.0, 20).unwrap()
        )
        .unwrap();
        let gauge = register_gauge!("test_gauge", "test gauge").unwrap();

        fn check_duplicate(s: &str) {
            let mut lines = HashSet::new();
            for l in s.lines() {
                assert!(lines.insert(l));
            }
        }

        // test all data is 0.
        set_metrics_compact_level(MetricsCompactLevel::NoCompact);
        let full_metrics = dump();
        assert!(!full_metrics.is_empty());
        check_duplicate(&full_metrics);

        set_metrics_compact_level(MetricsCompactLevel::LoseLess);
        let filtered_metrics = dump();
        check_duplicate(&full_metrics);
        assert!(full_metrics.len() > filtered_metrics.len());

        counter_vec.with_label_values(&["test"]).inc();
        histogram.with_label_values(&["test"]).observe(1.0);
        gauge.inc();

        set_metrics_compact_level(MetricsCompactLevel::NoCompact);
        let new_full_metrics = dump();
        assert!(!new_full_metrics.is_empty());
        check_duplicate(&new_full_metrics);
        assert!(new_full_metrics.len() > full_metrics.len());

        set_metrics_compact_level(MetricsCompactLevel::LoseLess);
        let new_filtered_metrics = dump();
        check_duplicate(&new_filtered_metrics);
        assert!(new_filtered_metrics.len() > filtered_metrics.len());
        assert!(new_full_metrics.len() > new_filtered_metrics.len());
    }
}
