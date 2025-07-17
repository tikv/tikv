// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use tikv_util::{
    config::{ReadableSize, VersionTrack},
    yatp_pool::FuturePool,
};

const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
pub const DEFAULT_GC_BATCH_KEYS: usize = 512;
// No limit
const DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC: u64 = 0;

// Auto compaction defaults - matching raftstore defaults
const DEFAULT_AUTO_COMPACTION_CHECK_INTERVAL_SECS: u64 = 300; // 5 minutes, same as raftstore

// Compaction threshold defaults - matching raftstore defaults
const DEFAULT_TOMBSTONES_NUM_THRESHOLD: u64 = 10000; // same as region_compact_min_tombstones
const DEFAULT_TOMBSTONES_PERCENT_THRESHOLD: u64 = 30; // same as region_compact_tombstones_percent
const DEFAULT_REDUNDANT_ROWS_THRESHOLD: u64 = 50000; // same as region_compact_min_redundant_rows
const DEFAULT_REDUNDANT_ROWS_PERCENT_THRESHOLD: u64 = 20; // same as region_compact_redundant_rows_percent

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct GcConfig {
    pub ratio_threshold: f64,
    pub batch_keys: usize,
    pub max_write_bytes_per_sec: ReadableSize,
    pub enable_compaction_filter: bool,
    /// By default compaction_filter can only works if `cluster_version` is
    /// greater than 5.0.0. Change `compaction_filter_skip_version_check`
    /// can enable it by force.
    pub compaction_filter_skip_version_check: bool,
    /// gc threads count
    pub num_threads: usize,
    
    // Auto compaction settings
    /// How often to check for new compaction candidates (in seconds)
    pub auto_compaction_check_interval_secs: u64,
    
    // Compaction threshold settings
    /// Minimum number of tombstones to trigger compaction
    pub compaction_tombstones_num_threshold: u64,
    /// Minimum percentage of tombstones to trigger compaction
    pub compaction_tombstones_percent_threshold: u64,
    /// Minimum number of redundant rows to trigger compaction
    pub compaction_redundant_rows_threshold: u64,
    /// Minimum percentage of redundant rows to trigger compaction
    pub compaction_redundant_rows_percent_threshold: u64,
    /// Force compaction of bottommost level
    pub compaction_bottommost_level_force: bool,
}

impl Default for GcConfig {
    fn default() -> GcConfig {
        GcConfig {
            ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            batch_keys: DEFAULT_GC_BATCH_KEYS,
            max_write_bytes_per_sec: ReadableSize(DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC),
            enable_compaction_filter: true,
            compaction_filter_skip_version_check: false,
            num_threads: 1,
            auto_compaction_check_interval_secs: DEFAULT_AUTO_COMPACTION_CHECK_INTERVAL_SECS,
            compaction_tombstones_num_threshold: DEFAULT_TOMBSTONES_NUM_THRESHOLD,
            compaction_tombstones_percent_threshold: DEFAULT_TOMBSTONES_PERCENT_THRESHOLD,
            compaction_redundant_rows_threshold: DEFAULT_REDUNDANT_ROWS_THRESHOLD,
            compaction_redundant_rows_percent_threshold: DEFAULT_REDUNDANT_ROWS_PERCENT_THRESHOLD,
            compaction_bottommost_level_force: false,
        }
    }
}

impl GcConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.batch_keys == 0 {
            return Err("gc.batch_keys should not be 0".into());
        }
        if self.num_threads == 0 {
            return Err("gc.thread_count should not be 0".into());
        }
        if self.auto_compaction_check_interval_secs == 0 {
            return Err("gc.auto_compaction_check_interval_secs should not be 0".into());
        }
        if self.compaction_tombstones_percent_threshold > 100 {
            return Err("gc.compaction_tombstones_percent_threshold should not exceed 100".into());
        }
        if self.compaction_redundant_rows_percent_threshold > 100 {
            return Err("gc.compaction_redundant_rows_percent_threshold should not exceed 100".into());
        }
        Ok(())
    }

    /// Get auto compaction check interval as Duration
    pub fn auto_compaction_check_interval(&self) -> Duration {
        Duration::from_secs(self.auto_compaction_check_interval_secs)
    }
}

#[derive(Clone, Default)]
pub struct GcWorkerConfigManager(pub Arc<VersionTrack<GcConfig>>, pub Option<FuturePool>);

impl ConfigManager for GcWorkerConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            if let Some(pool) = self.1.as_ref() {
                if let Some(v) = change.get("num_threads") {
                    let pool_size: usize = v.into();
                    pool.scale_pool_size(pool_size);
                    info!(
                        "GC worker thread count is changed";
                        "new_thread_count" => pool_size,
                    );
                }
            }
            self.0
                .update(move |cfg: &mut GcConfig| cfg.update(change))?;
        }
        info!(
            "GC worker config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

impl std::ops::Deref for GcWorkerConfigManager {
    type Target = Arc<VersionTrack<GcConfig>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
