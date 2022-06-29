// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    convert::TryFrom,
    io::Write,
    sync::atomic::{AtomicU8, Ordering},
};

use kvproto::pdpb;
use lazy_static::lazy_static;
use online_config::ConfigValue;
use prometheus::{proto::MetricType, *};
use prometheus_static_metric::*;
use serde::{Deserialize, Serialize};

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

static METRICS_COMPACT_POLICY: AtomicU8 = AtomicU8::new(MetricsCompactPolicy::No as u8);

pub fn set_metrics_compact_policy(p: MetricsCompactPolicy) {
    METRICS_COMPACT_POLICY.store(p as u8, Ordering::Release);
}

pub fn get_metrics_compact_policy() -> MetricsCompactPolicy {
    METRICS_COMPACT_POLICY.load(Ordering::Acquire).into()
}

pub fn dump() -> String {
    let mut buffer = vec![];
    dump_to(&mut buffer);
    String::from_utf8(buffer).unwrap()
}

pub fn dump_to(w: &mut impl Write) {
    let compact_policy = get_metrics_compact_policy();
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    if compact_policy == MetricsCompactPolicy::No {
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
                let threshold = if compact_policy == MetricsCompactPolicy::Lossless {
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
        quota_limiter_auto_tuned,
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
    pub static ref INSTANCE_BACKEND_CPU_QUOTA: IntGauge =
        register_int_gauge!("tikv_backend_cpu_quota", "cpu quota for backend request").unwrap();
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

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
/// MetricsCompactPolicy defines the level of compact metrics sample data, a higher level of
/// means smaller data size and more information loss.
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum MetricsCompactPolicy {
    // return full original data, this is the default policy.
    No = 0,
    // this level try to compact sample without infromation loss.
    // currently only filter counter with 0 value and histogram with 0 samples.
    Lossless = 1,
    // this level try to reduce the data size as much as possible.
    // this level also compact histogram vector type by remove histograms which sample count is
    // smaller than 1% of the max sample count.
    Lossy = 2,
}

impl From<u8> for MetricsCompactPolicy {
    fn from(val: u8) -> Self {
        match val {
            0 => Self::No,
            1 => Self::Lossless,
            2 => Self::Lossy,
            // only used in internal logic, so this breanch is unreachable.
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Display for MetricsCompactPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_value = match *self {
            MetricsCompactPolicy::No => "no",
            MetricsCompactPolicy::Lossless => "lossless",
            MetricsCompactPolicy::Lossy => "lossy",
        };
        f.write_str(str_value)
    }
}

impl From<MetricsCompactPolicy> for ConfigValue {
    fn from(v: MetricsCompactPolicy) -> Self {
        Self::String(format!("{v}"))
    }
}

impl TryFrom<ConfigValue> for MetricsCompactPolicy {
    type Error = String;
    fn try_from(value: ConfigValue) -> std::result::Result<Self, Self::Error> {
        if let ConfigValue::String(s) = value {
            match s.as_str() {
                "no" => Ok(Self::No),
                "lossless" => Ok(Self::Lossless),
                "lossy" => Ok(Self::Lossy),
                v => Err(format!(
                    "unknown value '{v}', expected one of ['no', 'lossless', 'lossy']"
                )),
            }
        } else {
            Err(format!("expected ConfigValue::String, got '{:?}'", value))
        }
    }
}

impl TryFrom<&ConfigValue> for MetricsCompactPolicy {
    type Error = String;
    fn try_from(c: &ConfigValue) -> std::result::Result<Self, Self::Error> {
        Self::try_from(c.clone())
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
        set_metrics_compact_policy(MetricsCompactPolicy::No);
        let full_metrics = dump();
        assert!(!full_metrics.is_empty());
        check_duplicate(&full_metrics);

        set_metrics_compact_policy(MetricsCompactPolicy::Lossless);
        let filtered_metrics = dump();
        check_duplicate(&full_metrics);
        assert!(full_metrics.len() > filtered_metrics.len());

        counter_vec.with_label_values(&["test"]).inc();
        histogram.with_label_values(&["test"]).observe(1.0);
        gauge.inc();

        set_metrics_compact_policy(MetricsCompactPolicy::No);
        let new_full_metrics = dump();
        assert!(!new_full_metrics.is_empty());
        check_duplicate(&new_full_metrics);
        assert!(new_full_metrics.len() > full_metrics.len());

        set_metrics_compact_policy(MetricsCompactPolicy::Lossless);
        let new_filtered_metrics = dump();
        check_duplicate(&new_filtered_metrics);
        assert!(new_filtered_metrics.len() > filtered_metrics.len());
        assert!(new_full_metrics.len() > new_filtered_metrics.len());
    }
}
