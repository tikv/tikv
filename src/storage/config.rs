// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage configuration.

use std::error::Error;

use sys_info;

use tikv_util::config::{self, ReadableSize, KB};

use engine::rocks::{Cache, LRUCacheOptions, MemoryAllocator};

use libc::c_int;

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
    pub block_cache: BlockCacheConfig,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BlockCacheConfig {
    pub shared: bool,
    pub capacity: Option<ReadableSize>,
    pub num_shard_bits: i32,
    pub strict_capacity_limit: bool,
    pub high_pri_pool_ratio: f64,
    pub memory_allocator: Option<String>,
}

impl Default for BlockCacheConfig {
    fn default() -> BlockCacheConfig {
        BlockCacheConfig {
            shared: true,
            capacity: None,
            num_shard_bits: 6,
            strict_capacity_limit: false,
            high_pri_pool_ratio: 0.8,
            memory_allocator: Some(String::from("nodump")),
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
        if let Some(allocator) = self.new_memory_allocator() {
            cache_opts.set_memory_allocator(allocator);
        }
        Some(Cache::new_lru_cache(cache_opts))
    }

    fn new_memory_allocator(&self) -> Option<MemoryAllocator> {
        if let Some(ref alloc) = self.memory_allocator {
            match alloc.as_str() {
                #[cfg(feature = "jemalloc")]
                "nodump" => match MemoryAllocator::new_jemalloc_memory_allocator() {
                    Ok(allocator) => {
                        return Some(allocator);
                    }
                    Err(e) => {
                        warn!("Create jemalloc nodump allocator for block cache failed: {}, continue with default allocator", e);
                    }
                },
                "" => {}
                other => {
                    warn!(
                        "Memory allocator {} is not supported, continue with default allocator",
                        other
                    );
                }
            }
        };
        None
    }
}
