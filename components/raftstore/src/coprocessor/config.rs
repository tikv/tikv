// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use config_info::ConfigInfo;
use engine_traits::{perf_level_serde, PerfLevel};
use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::{box_err, config::ReadableSize, worker::Scheduler};

use super::Result;
use crate::store::SplitCheckTask;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig, ConfigInfo)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// When it is true, it will try to split a region with table prefix if
    /// that region crosses tables.
    pub split_region_on_table: bool,

    /// For once split check, there are several split_key produced for batch.
    /// batch_split_limit limits the number of produced split-key for one batch.
    #[config_info(min = 1)]
    pub batch_split_limit: u64,

    /// The maximum size of a Region. When the value is exceeded, the Region splits into many.
    ///
    /// When region [a,e) size meets region_max_size, it will be split into
    /// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
    /// [b,c), [c,d) will be region_split_size (maybe a little larger).
    #[config_info(min_desc = "value of `region-split-size`")]
    pub region_max_size: ReadableSize,
    /// The size of the newly split Region. This value is an estimate.
    pub region_split_size: ReadableSize,

    /// The maximum allowable number of keys in a Region. When this value is exceeded,
    /// the Region splits into many.
    ///
    /// When the number of keys in region [a,e) meets the region_max_keys,
    /// it will be split into two several regions [a,b), [b,c), [c,d), [d,e).
    /// And the number of keys in [a,b), [b,c), [c,d) will be region_split_keys.
    #[config_info(min_desc = "value of `region-split-keys`")]
    pub region_max_keys: u64,
    /// The number of keys in the newly split Region. This value is an estimate.
    pub region_split_keys: u64,

    /// ConsistencyCheckMethod can not be chanaged dynamically.
    #[config_info(skip)]
    #[online_config(skip)]
    pub consistency_check_method: ConsistencyCheckMethod,

    // Deprecated. Perf level is not applicable to the raftstore coprocessor.
    // It was mistakenly used to refer to the perf level of the TiKV coprocessor
    // and should be replaced with `server.end-point-perf-level`.
    #[config_info(skip)]
    #[serde(with = "perf_level_serde", skip_serializing)]
    #[online_config(skip)]
    pub perf_level: PerfLevel,

    // enable subsplit ranges (aka bucket) within the region
    #[config_info(skip)]
    pub enable_region_bucket: bool,
    #[config_info(skip)]
    pub region_bucket_size: ReadableSize,
    // region size threshold for using approximate size instead of scan
    #[config_info(skip)]
    pub region_size_threshold_for_approximate: ReadableSize,
    // ratio of region_bucket_size. (0, 0.5)
    // The region_bucket_merge_size_ratio * region_bucket_size is threshold to merge with its left neighbor bucket
    #[config_info(skip)]
    pub region_bucket_merge_size_ratio: f64,
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConsistencyCheckMethod {
    /// Does consistency check for regions based on raw data. Only used when
    /// raw APIs are enabled and MVCC-GC is disabled.
    Raw = 0,

    /// Does consistency check for regions based on MVCC.
    Mvcc = 1,
}

/// Default region split size.
pub const SPLIT_SIZE_MB: u64 = 96;
/// Default region split keys.
pub const SPLIT_KEYS: u64 = 960000;
/// Default batch split limit.
pub const BATCH_SPLIT_LIMIT: u64 = 10;

pub const DEFAULT_BUCKET_SIZE: ReadableSize = ReadableSize::mb(128);

pub const DEFAULT_REGION_BUCKET_MERGE_SIZE_RATIO: f64 = 0.33;

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(SPLIT_SIZE_MB);
        Config {
            split_region_on_table: false,
            batch_split_limit: BATCH_SPLIT_LIMIT,
            region_split_size: split_size,
            region_max_size: split_size / 2 * 3,
            region_split_keys: SPLIT_KEYS,
            region_max_keys: SPLIT_KEYS / 2 * 3,
            consistency_check_method: ConsistencyCheckMethod::Mvcc,
            perf_level: PerfLevel::Uninitialized,
            enable_region_bucket: false,
            region_bucket_size: DEFAULT_BUCKET_SIZE,
            region_size_threshold_for_approximate: DEFAULT_BUCKET_SIZE * 4,
            region_bucket_merge_size_ratio: DEFAULT_REGION_BUCKET_MERGE_SIZE_RATIO,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.region_max_size.0 < self.region_split_size.0 {
            return Err(box_err!(
                "region max size {} must >= split size {}",
                self.region_max_size.0,
                self.region_split_size.0
            ));
        }
        if self.region_max_keys < self.region_split_keys {
            return Err(box_err!(
                "region max keys {} must >= split keys {}",
                self.region_max_keys,
                self.region_split_keys
            ));
        }
        if self.enable_region_bucket {
            if self.region_split_size.0 < self.region_bucket_size.0 {
                return Err(box_err!(
                    "region split size {} must >= region bucket size {}",
                    self.region_split_size.0,
                    self.region_bucket_size.0
                ));
            }
            if self.region_size_threshold_for_approximate.0 < self.region_bucket_size.0 {
                return Err(box_err!(
                    "large region threshold size {} must >= region bucket size {}",
                    self.region_size_threshold_for_approximate.0,
                    self.region_bucket_size.0
                ));
            }
            if self.region_bucket_size.0 == 0 {
                return Err(box_err!("region_bucket size cannot be 0."));
            }
            if self.region_bucket_merge_size_ratio <= 0.0
                || self.region_bucket_merge_size_ratio >= 0.5
            {
                return Err(box_err!(
                    "region-bucket-merge-size-ratio should be 0 to 0.5 (not include both ends)."
                ));
            }
        }
        Ok(())
    }
}

pub struct SplitCheckConfigManager(pub Scheduler<SplitCheckTask>);

impl ConfigManager for SplitCheckConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.0.schedule(SplitCheckTask::ChangeConfig(change))?;
        Ok(())
    }
}

impl std::ops::Deref for SplitCheckConfigManager {
    type Target = Scheduler<SplitCheckTask>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::default();
        cfg.validate().unwrap();

        cfg = Config::default();
        cfg.region_max_size = ReadableSize(10);
        cfg.region_split_size = ReadableSize(20);
        assert!(cfg.validate().is_err());

        cfg = Config::default();
        cfg.region_max_keys = 10;
        cfg.region_split_keys = 20;
        assert!(cfg.validate().is_err());

        cfg = Config::default();
        cfg.enable_region_bucket = false;
        cfg.region_split_size = ReadableSize(20);
        cfg.region_bucket_size = ReadableSize(30);
        assert!(cfg.validate().is_ok());
    }
}
