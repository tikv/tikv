// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use configuration::{ConfigChange, ConfigManager, Configuration};
use tikv_util::config::{ReadableSize, VersionTrack};

const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
pub const DEFAULT_GC_BATCH_KEYS: usize = 512;
// No limit
const DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC: u64 = 0;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct GcConfig {
    pub ratio_threshold: f64,
    pub batch_keys: usize,
    pub max_write_bytes_per_sec: ReadableSize,
    pub enable_compaction_filter: bool,
    /// By default compaction_filter can only works if `cluster_version` is greater than 5.0.0.
    /// Change `compaction_filter_skip_version_check` can enable it by force.
    pub compaction_filter_skip_version_check: bool,
}

impl Default for GcConfig {
    fn default() -> GcConfig {
        GcConfig {
            ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            batch_keys: DEFAULT_GC_BATCH_KEYS,
            max_write_bytes_per_sec: ReadableSize(DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC),
            enable_compaction_filter: true,
            compaction_filter_skip_version_check: false,
        }
    }
}

impl GcConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.batch_keys == 0 {
            return Err("gc.batch_keys should not be 0".into());
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct GcWorkerConfigManager(pub Arc<VersionTrack<GcConfig>>);

impl ConfigManager for GcWorkerConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.0.update(move |cfg: &mut GcConfig| cfg.update(change));
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
