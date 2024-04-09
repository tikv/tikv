// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use tikv_util::{
    config::{ReadableSize, VersionTrack},
    yatp_pool::FuturePool,
};

const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
pub const DEFAULT_GC_BATCH_KEYS: usize = 512;
// No limit
const DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC: u64 = 0;

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
        Ok(())
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
