// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage configuration.

use crate::server::CONFIG_ROCKSDB_GAUGE;
use configuration::{ConfigChange, ConfigManager, ConfigValue, Configuration, Result as CfgResult};
use engine_rocks::raw::{Cache, LRUCacheOptions, MemoryAllocator};
use engine_rocks::RocksEngine;
use engine_traits::{CFOptionsExt, ColumnFamilyOptions, CF_DEFAULT};
use file_system::{get_io_rate_limiter, IOPriority, IORateLimiter, IOType};
use libc::c_int;
use std::error::Error;
use tikv_util::config::{self, OptionReadableSize, ReadableSize};
use tikv_util::sys::sys_quota::SysQuota;

pub const DEFAULT_DATA_DIR: &str = "./";
const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
const DEFAULT_MAX_KEY_SIZE: usize = 4 * 1024;
const DEFAULT_SCHED_CONCURRENCY: usize = 1024 * 512;
const MAX_SCHED_CONCURRENCY: usize = 2 * 1024 * 1024;

// According to "Little's law", assuming you can write 100MB per
// second, and it takes about 100ms to process the write requests
// on average, in that situation the writing bytes estimated 10MB,
// here we use 100MB as default value for tolerate 1s latency.
const DEFAULT_SCHED_PENDING_WRITE_MB: u64 = 100;

const DEFAULT_RESERVED_SPACE_GB: u64 = 5;
// 20GB for reserved space is enough because size of one compaction is limited,
// generally less than 2GB.
pub const MAX_RESERVED_SPACE_GB: u64 = 20;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[config(skip)]
    pub data_dir: String,
    // Replaced by `GcConfig.ratio_threshold`. Keep it for backward compatibility.
    #[config(skip)]
    pub gc_ratio_threshold: f64,
    #[config(skip)]
    pub max_key_size: usize,
    #[config(skip)]
    pub scheduler_concurrency: usize,
    #[config(skip)]
    pub scheduler_worker_pool_size: usize,
    #[config(skip)]
    pub scheduler_pending_write_threshold: ReadableSize,
    #[config(skip)]
    // Reserve disk space to make tikv would have enough space to compact when disk is full.
    pub reserve_space: ReadableSize,
    #[config(skip)]
    pub enable_async_apply_prewrite: bool,
    #[config(submodule)]
    pub block_cache: BlockCacheConfig,
    #[config(submodule)]
    pub io_rate_limit: IORateLimitConfig,
}

impl Default for Config {
    fn default() -> Config {
        let cpu_num = SysQuota::new().cpu_cores_quota();
        Config {
            data_dir: DEFAULT_DATA_DIR.to_owned(),
            gc_ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            scheduler_concurrency: DEFAULT_SCHED_CONCURRENCY,
            scheduler_worker_pool_size: if cpu_num >= 16.0 { 8 } else { 4 },
            scheduler_pending_write_threshold: ReadableSize::mb(DEFAULT_SCHED_PENDING_WRITE_MB),
            reserve_space: ReadableSize::gb(DEFAULT_RESERVED_SPACE_GB),
            enable_async_apply_prewrite: false,
            block_cache: BlockCacheConfig::default(),
            io_rate_limit: IORateLimitConfig::default(),
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.data_dir != DEFAULT_DATA_DIR {
            self.data_dir = config::canonicalize_path(&self.data_dir)?
        }
        if self.scheduler_concurrency > MAX_SCHED_CONCURRENCY {
            warn!("TiKV has optimized latch since v4.0, so it is not necessary to set large schedule \
                concurrency. To save memory, change it from {:?} to {:?}",
                  self.scheduler_concurrency, MAX_SCHED_CONCURRENCY);
            self.scheduler_concurrency = MAX_SCHED_CONCURRENCY;
        }
        if self.reserve_space.0 > ReadableSize::gb(MAX_RESERVED_SPACE_GB).0 {
            self.reserve_space = ReadableSize::gb(MAX_RESERVED_SPACE_GB);
            warn!(
                "reserve-space is too large, sanitized to {:?}",
                self.reserve_space
            );
        }
        Ok(())
    }
}

pub struct StorageConfigManger {
    kvdb: RocksEngine,
    shared_block_cache: bool,
}

impl StorageConfigManger {
    pub fn new(kvdb: RocksEngine, shared_block_cache: bool) -> StorageConfigManger {
        StorageConfigManger {
            kvdb,
            shared_block_cache,
        }
    }
}

impl ConfigManager for StorageConfigManger {
    fn dispatch(&mut self, mut change: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Module(mut block_cache)) = change.remove("block_cache") {
            if !self.shared_block_cache {
                return Err("shared block cache is disabled".into());
            }
            if let Some(size) = block_cache.remove("capacity") {
                let s: OptionReadableSize = size.into();
                if let Some(size) = s.0 {
                    // Hack: since all CFs in both kvdb and raftdb share a block cache, we can change
                    // the size through any of them. Here we change it through default CF in kvdb.
                    // A better way to do it is to hold the cache reference somewhere, and use it to
                    // change cache size.
                    let opt = self.kvdb.get_options_cf(CF_DEFAULT).unwrap(); // FIXME unwrap
                    opt.set_block_cache_capacity(size.0)?;
                    // Write config to metric
                    CONFIG_ROCKSDB_GAUGE
                        .with_label_values(&[CF_DEFAULT, "block_cache_size"])
                        .set(size.0 as f64);
                }
            }
        }
        if let Some(ConfigValue::Module(mut io_rate_limit)) = change.remove("io_rate_limit") {
            let limiter = get_io_rate_limiter();
            if limiter.is_none() {
                return Err("IO rate limiter is not present".into());
            }
            let limiter = limiter.unwrap();
            if let Some(limit) = io_rate_limit.remove("total") {
                if let OptionReadableSize(Some(limit)) = limit.into() {
                    limiter.set_io_rate_limit(limit.as_b() as usize);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BlockCacheConfig {
    #[config(skip)]
    pub shared: bool,
    pub capacity: OptionReadableSize,
    #[config(skip)]
    pub num_shard_bits: i32,
    #[config(skip)]
    pub strict_capacity_limit: bool,
    #[config(skip)]
    pub high_pri_pool_ratio: f64,
    #[config(skip)]
    pub memory_allocator: Option<String>,
}

impl Default for BlockCacheConfig {
    fn default() -> BlockCacheConfig {
        BlockCacheConfig {
            shared: true,
            capacity: OptionReadableSize(None),
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
        let capacity = match self.capacity.0 {
            None => {
                let total_mem = SysQuota::new().memory_limit_in_bytes();
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct IORateLimitConfig {
    pub total: OptionReadableSize,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub foreground_read_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub foreground_write_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub flush_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub compaction_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub replication_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub load_balance_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub gc_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub import_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub export_priority: IOPriority,
    #[serde(with = "file_system::io_priority_serde")]
    #[config(skip)]
    pub other_priority: IOPriority,
}

impl Default for IORateLimitConfig {
    fn default() -> IORateLimitConfig {
        IORateLimitConfig {
            total: OptionReadableSize(Some(ReadableSize::mb(2000))),
            foreground_read_priority: IOPriority::High,
            foreground_write_priority: IOPriority::High,
            flush_priority: IOPriority::High,
            compaction_priority: IOPriority::High,
            replication_priority: IOPriority::High,
            load_balance_priority: IOPriority::High,
            gc_priority: IOPriority::High,
            import_priority: IOPriority::High,
            export_priority: IOPriority::High,
            other_priority: IOPriority::High,
        }
    }
}

impl IORateLimitConfig {
    pub fn build(&self) -> IORateLimiter {
        let mut limiter = IORateLimiter::new();
        if let Some(limit) = self.total.0 {
            limiter.set_io_rate_limit(limit.as_b() as usize);
        }
        limiter.set_io_priority(IOType::ForegroundRead, self.foreground_read_priority);
        limiter.set_io_priority(IOType::ForegroundWrite, self.foreground_write_priority);
        limiter.set_io_priority(IOType::Flush, self.flush_priority);
        limiter.set_io_priority(IOType::Compaction, self.compaction_priority);
        limiter.set_io_priority(IOType::Replication, self.replication_priority);
        limiter.set_io_priority(IOType::LoadBalance, self.load_balance_priority);
        limiter.set_io_priority(IOType::Gc, self.gc_priority);
        limiter.set_io_priority(IOType::Import, self.import_priority);
        limiter.set_io_priority(IOType::Export, self.export_priority);
        limiter.set_io_priority(IOType::Other, self.other_priority);
        limiter
    }
}
