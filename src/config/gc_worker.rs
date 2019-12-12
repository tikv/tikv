// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::config::ReadableSize;

const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
pub const DEFAULT_GC_BATCH_KEYS: usize = 512;
// No limit
const DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC: u64 = 0;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct GcConfig {
    pub ratio_threshold: f64,
    pub batch_keys: usize,
    pub max_write_bytes_per_sec: ReadableSize,
}

impl Default for GcConfig {
    fn default() -> GcConfig {
        GcConfig {
            ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            batch_keys: DEFAULT_GC_BATCH_KEYS,
            max_write_bytes_per_sec: ReadableSize(DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC),
        }
    }
}

impl GcConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.batch_keys == 0 {
            return Err(("gc.batch_keys should not be 0.").into());
        }
        Ok(())
    }
}
