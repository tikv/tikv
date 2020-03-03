// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;
use std::sync::Arc;

use configuration::{rollback_or, ConfigChange, ConfigManager, Configuration, RollbackCollector};
use cron::Schedule;
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
    /// cron expression for scheduling traditional GC when compaction filter is enabled.
    pub traditional_gc_cron: String,
}

impl Default for GcConfig {
    fn default() -> GcConfig {
        GcConfig {
            ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            batch_keys: DEFAULT_GC_BATCH_KEYS,
            max_write_bytes_per_sec: ReadableSize(DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC),
            enable_compaction_filter: false,
            // If compaction filter is enabled, traditional GC will be started on 3 AM every day.
            traditional_gc_cron: "0 0 3 * * * *".to_owned(),
        }
    }
}

impl GcConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.validate_or_rollback(None)
    }

    pub fn validate_or_rollback(
        &self,
        mut rb_collector: Option<RollbackCollector<GcConfig>>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.batch_keys == 0 {
            rollback_or!(rb_collector, batch_keys, {
                Err(("gc.batch_keys should not be 0.").into())
            })
        }
        if Schedule::from_str(&self.traditional_gc_cron).is_err() {
            rollback_or!(rb_collector, batch_keys, {
                Err(("gc.traditional_gc_cron parse fail.").into())
            })
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
