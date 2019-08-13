// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage configuration.

use std::error::Error;

use sys_info;

use tikv_util::config::{self, ReadableSize, KB};

use engine::rocks::{Cache, LRUCacheOptions};

use libc::c_int;

pub const DEFAULT_DATA_DIR: &str = "./";
pub const DEFAULT_ROCKSDB_SUB_DIR: &str = "db";
const DEFAULT_MAX_KEY_SIZE: usize = 4 * 1024;
const DEFAULT_SCHED_CAPACITY: usize = 10240;
const DEFAULT_SCHED_CONCURRENCY: usize = 2048000;

// According to "Little's law", assuming you can write 100MB per
// second, and it takes about 100ms to process the write requests
// on average, in that situation the writing bytes estimated 10MB,
// here we use 100MB as default value for tolerate 1s latency.
const DEFAULT_SCHED_PENDING_WRITE_MB: u64 = 100;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub data_dir: String,
    // Replaced by `GCConfig.ratio_threshold`. Keep it for backward compatibility.
    pub gc_ratio_threshold: f64,
    pub max_key_size: usize,
    pub scheduler_notify_capacity: usize,
    pub scheduler_concurrency: usize,
    pub scheduler_worker_pool_size: usize,
    pub scheduler_pending_write_threshold: ReadableSize,
    pub block_cache: BlockCacheConfig,
    pub gc: GCConfig,
}

impl Default for Config {
    fn default() -> Config {
        let total_cpu = sys_info::cpu_num().unwrap();
        Config {
            data_dir: DEFAULT_DATA_DIR.to_owned(),
            gc_ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            scheduler_notify_capacity: DEFAULT_SCHED_CAPACITY,
            scheduler_concurrency: DEFAULT_SCHED_CONCURRENCY,
            scheduler_worker_pool_size: if total_cpu >= 16 { 8 } else { 4 },
            scheduler_pending_write_threshold: ReadableSize::mb(DEFAULT_SCHED_PENDING_WRITE_MB),
            block_cache: BlockCacheConfig::default(),
            gc: GCConfig::default(),
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.data_dir != DEFAULT_DATA_DIR {
            self.data_dir = config::canonicalize_path(&self.data_dir)?
        }
        self.gc.validate()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct BlockCacheConfig {
    pub shared: bool,
    pub capacity: Option<ReadableSize>,
    pub num_shard_bits: i32,
    pub strict_capacity_limit: bool,
    pub high_pri_pool_ratio: f64,
}

impl Default for BlockCacheConfig {
    fn default() -> BlockCacheConfig {
        BlockCacheConfig {
            shared: true,
            capacity: None,
            num_shard_bits: 6,
            strict_capacity_limit: false,
            high_pri_pool_ratio: 0.0,
        }
    }
}

impl BlockCacheConfig {
    pub fn build_shared_cache(&self) -> Option<Cache> {
        if !self.shared {
            return None;
        }
        let capacity = match self.capacity {
            None => {
                let total_mem = sys_info::mem_info().unwrap().total * KB;
                ((total_mem as f64) * 0.45) as usize
            }
            Some(c) => c.0 as usize,
        };
        let mut cache_opts = LRUCacheOptions::new();
        cache_opts.set_capacity(capacity);
        cache_opts.set_num_shard_bits(self.num_shard_bits as c_int);
        cache_opts.set_strict_capacity_limit(self.strict_capacity_limit);
        cache_opts.set_high_pri_pool_ratio(self.high_pri_pool_ratio);
        Some(Cache::new_lru_cache(cache_opts))
    }
}

pub const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
pub const DEFAULT_GC_BATCH_KEYS: usize = 512;
// No limit
const DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC: u64 = 0;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct GCConfig {
    pub ratio_threshold: f64,
    pub batch_keys: usize,
    pub max_write_bytes_per_sec: ReadableSize,
}

impl Default for GCConfig {
    fn default() -> GCConfig {
        GCConfig {
            ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            batch_keys: DEFAULT_GC_BATCH_KEYS,
            max_write_bytes_per_sec: ReadableSize(DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC),
        }
    }
}

impl GCConfig {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.batch_keys == 0 {
            return Err(("storage.gc.batch_keys should not be 0.").into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_config_validate() {
        let cfg = GCConfig::default();
        cfg.validate().unwrap();

        let mut invalid_cfg = GCConfig::default();
        invalid_cfg.batch_keys = 0;
        assert!(invalid_cfg.validate().is_err());
    }
}
