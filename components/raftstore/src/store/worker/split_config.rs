// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use lazy_static::lazy_static;
use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tikv_util::{config::VersionTrack, info};

const DEFAULT_DETECT_TIMES: u64 = 10;
const DEFAULT_SAMPLE_THRESHOLD: u64 = 100;
pub(crate) const DEFAULT_SAMPLE_NUM: usize = 20;
const DEFAULT_QPS_THRESHOLD: usize = 3000;
const DEFAULT_BYTE_THRESHOLD: usize = 30 * 1024 * 1024;

// We get balance score by
// abs(sample.left-sample.right)/(sample.right+sample.left). It will be used to
// measure left and right balance
const DEFAULT_SPLIT_BALANCE_SCORE: f64 = 0.25;
// We get contained score by
// sample.contained/(sample.right+sample.left+sample.contained). It will be used
// to avoid to split regions requested by range.
const DEFAULT_SPLIT_CONTAINED_SCORE: f64 = 0.5;

// If the `split_balance_score` and `split_contained_score` above could not be
// satisfied, we will try to split the region according to its CPU load,
// then these parameters below will start to work.
// When the gRPC poll thread CPU usage (over the past `detect_times` seconds by
// default) is higher than gRPC poll thread count *
// `DEFAULT_GRPC_THREAD_CPU_OVERLOAD_THRESHOLD_RATIO`, the CPU-based split won't
// be triggered no matter if the
// `DEFAULT_UNIFIED_READ_POOL_THREAD_CPU_OVERLOAD_THRESHOLD_RATIO` and
// `REGION_CPU_OVERLOAD_THRESHOLD_RATIO` are exceeded to prevent from increasing
// the gRPC poll CPU usage.
const DEFAULT_GRPC_THREAD_CPU_OVERLOAD_THRESHOLD_RATIO: f64 = 0.5;
// When the Unified Read Poll thread CPU usage is higher than Unified Read Poll
// thread count *
// `DEFAULT_UNIFIED_READ_POOL_THREAD_CPU_OVERLOAD_THRESHOLD_RATIO`,
// the CPU-based split will try to check and record the top hot CPU region.
const DEFAULT_UNIFIED_READ_POOL_THREAD_CPU_OVERLOAD_THRESHOLD_RATIO: f64 = 0.8;
// When the Unified Read Poll is hot and the region's CPU usage reaches
// `REGION_CPU_OVERLOAD_THRESHOLD_RATIO` as a percentage of the Unified Read
// Poll, it will be added into the hot region list and may be split later as the
// top hot CPU region.
pub(crate) const REGION_CPU_OVERLOAD_THRESHOLD_RATIO: f64 = 0.25;

lazy_static! {
    static ref SPLIT_CONFIG: Mutex<Option<Arc<VersionTrack<SplitConfig>>>> = Mutex::new(None);
}

pub fn get_sample_num() -> usize {
    let sample_num = if let Some(ref config) = *SPLIT_CONFIG.lock() {
        config.value().sample_num
    } else {
        DEFAULT_SAMPLE_NUM
    };
    sample_num
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct SplitConfig {
    pub qps_threshold: usize,
    pub split_balance_score: f64,
    pub split_contained_score: f64,
    pub detect_times: u64,
    pub sample_num: usize,
    pub sample_threshold: u64,
    pub byte_threshold: usize,
    #[doc(hidden)]
    pub grpc_thread_cpu_overload_threshold_ratio: f64,
    #[doc(hidden)]
    pub unified_read_pool_thread_cpu_overload_threshold_ratio: f64,
    pub region_cpu_overload_threshold_ratio: f64,
    // deprecated.
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub size_threshold: Option<usize>,
    // deprecated.
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub key_threshold: Option<usize>,
}

impl Default for SplitConfig {
    fn default() -> SplitConfig {
        SplitConfig {
            qps_threshold: DEFAULT_QPS_THRESHOLD,
            split_balance_score: DEFAULT_SPLIT_BALANCE_SCORE,
            split_contained_score: DEFAULT_SPLIT_CONTAINED_SCORE,
            detect_times: DEFAULT_DETECT_TIMES,
            sample_num: DEFAULT_SAMPLE_NUM,
            sample_threshold: DEFAULT_SAMPLE_THRESHOLD,
            byte_threshold: DEFAULT_BYTE_THRESHOLD,
            grpc_thread_cpu_overload_threshold_ratio:
                DEFAULT_GRPC_THREAD_CPU_OVERLOAD_THRESHOLD_RATIO,
            unified_read_pool_thread_cpu_overload_threshold_ratio:
                DEFAULT_UNIFIED_READ_POOL_THREAD_CPU_OVERLOAD_THRESHOLD_RATIO,
            region_cpu_overload_threshold_ratio: REGION_CPU_OVERLOAD_THRESHOLD_RATIO,
            size_threshold: None, // deprecated.
            key_threshold: None,  // deprecated.
        }
    }
}

impl SplitConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.split_balance_score > 1.0
            || self.split_balance_score < 0.0
            || self.split_contained_score > 1.0
            || self.split_contained_score < 0.0
        {
            return Err(
                ("split_balance_score or split_contained_score should be between 0 and 1.").into(),
            );
        }
        if self.sample_num >= self.qps_threshold {
            return Err(
                ("sample_num should be less than qps_threshold for load-base-split.").into(),
            );
        }
        if self.grpc_thread_cpu_overload_threshold_ratio > 1.0
            || self.grpc_thread_cpu_overload_threshold_ratio < 0.0
            || self.unified_read_pool_thread_cpu_overload_threshold_ratio > 1.0
            || self.unified_read_pool_thread_cpu_overload_threshold_ratio < 0.0
            || self.region_cpu_overload_threshold_ratio > 1.0
            || self.region_cpu_overload_threshold_ratio < 0.0
        {
            return Err(("threshold ratio should be between 0 and 1.").into());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SplitConfigManager(pub Arc<VersionTrack<SplitConfig>>);

impl SplitConfigManager {
    pub fn new(split_config: Arc<VersionTrack<SplitConfig>>) -> Self {
        *SPLIT_CONFIG.lock() = Some(split_config.clone());
        Self(split_config)
    }
}

impl Default for SplitConfigManager {
    fn default() -> Self {
        let split_config = Arc::new(VersionTrack::new(SplitConfig::default()));
        *SPLIT_CONFIG.lock() = Some(split_config.clone());
        Self(split_config)
    }
}

impl ConfigManager for SplitConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.0
                .update(move |cfg: &mut SplitConfig| cfg.update(change))?;
        }
        info!(
            "load base split config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

impl std::ops::Deref for SplitConfigManager {
    type Target = Arc<VersionTrack<SplitConfig>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use tikv_util::config::VersionTrack;

    use crate::store::{
        worker::split_config::{get_sample_num, DEFAULT_SAMPLE_NUM},
        SplitConfig, SplitConfigManager,
    };

    #[test]
    fn test_static_var_after_config_change() {
        assert_eq!(get_sample_num(), DEFAULT_SAMPLE_NUM);

        let mut split_config = SplitConfig::default();
        split_config.sample_num = 30;
        let mut cfg_manager = SplitConfigManager::new(Arc::new(VersionTrack::new(split_config)));

        assert_eq!(get_sample_num(), 30);

        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("sample_num"), ConfigValue::Usize(50));
        cfg_manager.dispatch(config_change).unwrap();

        assert_eq!(get_sample_num(), 50);
    }
}
