// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.
use std::error::Error;

use sys_info;

use crate::util::config::{self, ReadableSize};

pub const DEFAULT_DATA_DIR: &str = "./";
pub const DEFAULT_ROCKSDB_SUB_DIR: &str = "db";
const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
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
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub data_dir: String,
    pub gc_ratio_threshold: f64,
    pub max_key_size: usize,
    pub scheduler_notify_capacity: usize,
    pub scheduler_concurrency: usize,
    pub scheduler_worker_pool_size: usize,
    pub scheduler_pending_write_threshold: ReadableSize,
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
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.data_dir != DEFAULT_DATA_DIR {
            self.data_dir = config::canonicalize_path(&self.data_dir)?
        }
        Ok(())
    }
}
