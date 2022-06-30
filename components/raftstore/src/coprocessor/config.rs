// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{perf_level_serde, PerfLevel};
use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::{box_err, config::ReadableSize, worker::Scheduler};

use super::Result;
use crate::store::SplitCheckTask;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// When it is true, it will try to split a region with table prefix if
    /// that region crosses tables.
    pub split_region_on_table: bool,

    /// For once split check, there are several split_key produced for batch.
    /// batch_split_limit limits the number of produced split-key for one batch.
    pub batch_split_limit: u64,

    /// When region [a,e) size meets region_max_size, it will be split into
    /// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
    /// [b,c), [c,d) will be region_split_size (maybe a little larger).
    /// by default, region_max_size = region_split_size * 2 / 3.
    pub region_max_size: Option<ReadableSize>,
    pub region_split_size: ReadableSize,

    /// When the number of keys in region [a,e) meets the region_max_keys,
    /// it will be split into two several regions [a,b), [b,c), [c,d), [d,e).
    /// And the number of keys in [a,b), [b,c), [c,d) will be region_split_keys.
    /// by default, region_max_keys = region_split_keys * 2 / 3.
    pub region_max_keys: Option<u64>,
    pub region_split_keys: Option<u64>,

    /// ConsistencyCheckMethod can not be chanaged dynamically.
    #[online_config(skip)]
    pub consistency_check_method: ConsistencyCheckMethod,

    // Deprecated. Perf level is not applicable to the raftstore coprocessor.
    // It was mistakenly used to refer to the perf level of the TiKV coprocessor
    // and should be replaced with `server.end-point-perf-level`.
    #[serde(with = "perf_level_serde", skip_serializing)]
    #[online_config(skip)]
    pub perf_level: PerfLevel,

    // enable subsplit ranges (aka bucket) within the region
    pub enable_region_bucket: bool,
    pub region_bucket_size: ReadableSize,
    // region size threshold for using approximate size instead of scan
    pub region_size_threshold_for_approximate: ReadableSize,
    #[online_config(skip)]
    pub prefer_approximate_bucket: bool,
    // ratio of region_bucket_size. (0, 0.5)
    // The region_bucket_merge_size_ratio * region_bucket_size is threshold to merge with its left neighbor bucket
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
/// Default batch split limit.
pub const BATCH_SPLIT_LIMIT: u64 = 10;

pub const DEFAULT_BUCKET_SIZE: ReadableSize = ReadableSize::mb(96);

pub const DEFAULT_REGION_BUCKET_MERGE_SIZE_RATIO: f64 = 0.33;

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(SPLIT_SIZE_MB);
        Config {
            split_region_on_table: false,
            batch_split_limit: BATCH_SPLIT_LIMIT,
            region_split_size: split_size,
            region_max_size: None,
            region_split_keys: None,
            region_max_keys: None,
            consistency_check_method: ConsistencyCheckMethod::Mvcc,
            perf_level: PerfLevel::Uninitialized,
            enable_region_bucket: false,
            region_bucket_size: DEFAULT_BUCKET_SIZE,
            region_size_threshold_for_approximate: DEFAULT_BUCKET_SIZE * BATCH_SPLIT_LIMIT / 2 * 3,
            region_bucket_merge_size_ratio: DEFAULT_REGION_BUCKET_MERGE_SIZE_RATIO,
            prefer_approximate_bucket: true,
        }
    }
}

impl Config {
    pub fn region_max_keys(&self) -> u64 {
        let default_split_keys = self.region_split_size.as_mb_f64() * 10000.0;
        self.region_max_keys
            .unwrap_or(default_split_keys as u64 / 2 * 3)
    }

    pub fn region_max_size(&self) -> ReadableSize {
        self.region_max_size
            .unwrap_or(self.region_split_size / 2 * 3)
    }

    pub fn region_split_keys(&self) -> u64 {
        // Assume the average size of KVs is 100B.
        self.region_split_keys
            .unwrap_or((self.region_split_size.as_mb_f64() * 10000.0) as u64)
    }

    pub fn validate(&mut self) -> Result<()> {
        if self.region_split_keys.is_none() {
            self.region_split_keys = Some((self.region_split_size.as_mb_f64() * 10000.0) as u64);
        }

        match self.region_max_size {
            Some(region_max_size) => {
                if region_max_size.0 < self.region_split_size.0 {
                    return Err(box_err!(
                        "region max size {} must >= split size {}",
                        region_max_size.0,
                        self.region_split_size.0
                    ));
                }
            }
            None => self.region_max_size = Some(self.region_split_size / 2 * 3),
        }

        match self.region_max_keys {
            Some(region_max_keys) => {
                if region_max_keys < self.region_split_keys() {
                    return Err(box_err!(
                        "region max keys {} must >= split keys {}",
                        region_max_keys,
                        self.region_split_keys()
                    ));
                }
            }
            None => self.region_max_keys = Some(self.region_split_keys() / 2 * 3),
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
        cfg.region_max_size = Some(ReadableSize(10));
        cfg.region_split_size = ReadableSize(20);
        assert!(cfg.validate().is_err());

        cfg = Config::default();
        cfg.region_max_size = None;
        cfg.region_split_size = ReadableSize(20);
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.region_max_size, Some(ReadableSize(30)));

        cfg = Config::default();
        cfg.region_max_keys = Some(10);
        cfg.region_split_keys = Some(20);
        assert!(cfg.validate().is_err());

        cfg = Config::default();
        cfg.region_max_keys = None;
        cfg.region_split_keys = Some(20);
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.region_max_keys, Some(30));

        cfg = Config::default();
        cfg.enable_region_bucket = false;
        cfg.region_split_size = ReadableSize(20);
        cfg.region_bucket_size = ReadableSize(30);
        assert!(cfg.validate().is_ok());

        cfg = Config::default();
        cfg.region_split_size = ReadableSize::mb(20);
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.region_split_keys, Some(200000));
    }
}
