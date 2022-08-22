// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage configuration.

use std::{cmp::max, error::Error};

use engine_rocks::raw::{Cache, LRUCacheOptions, MemoryAllocator};
use file_system::{IOPriority, IORateLimitMode, IORateLimiter, IOType};
use kvproto::kvrpcpb::ApiVersion;
use libc::c_int;
use online_config::OnlineConfig;
use tikv_util::{
    config::{self, ReadableDuration, ReadableSize},
    sys::SysQuota,
};

use crate::config::{BLOCK_CACHE_RATE, MIN_BLOCK_CACHE_SHARD_SIZE};

pub const DEFAULT_DATA_DIR: &str = "./";
const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
const DEFAULT_MAX_KEY_SIZE: usize = 8 * 1024;
const DEFAULT_SCHED_CONCURRENCY: usize = 1024 * 512;
const MAX_SCHED_CONCURRENCY: usize = 2 * 1024 * 1024;

// According to "Little's law", assuming you can write 100MB per
// second, and it takes about 100ms to process the write requests
// on average, in that situation the writing bytes estimated 10MB,
// here we use 100MB as default value for tolerate 1s latency.
const DEFAULT_SCHED_PENDING_WRITE_MB: u64 = 100;

const DEFAULT_RESERVED_SPACE_GB: u64 = 5;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[online_config(skip)]
    pub data_dir: String,
    // Replaced by `GcConfig.ratio_threshold`. Keep it for backward compatibility.
    #[online_config(skip)]
    pub gc_ratio_threshold: f64,
    #[online_config(skip)]
    pub max_key_size: usize,
    #[online_config(skip)]
    pub scheduler_concurrency: usize,
    pub scheduler_worker_pool_size: usize,
    #[online_config(skip)]
    pub scheduler_pending_write_threshold: ReadableSize,
    #[online_config(skip)]
    // Reserve disk space to make tikv would have enough space to compact when disk is full.
    pub reserve_space: ReadableSize,
    #[online_config(skip)]
    pub enable_async_apply_prewrite: bool,
    #[online_config(skip)]
    pub api_version: u8,
    #[online_config(skip)]
    pub enable_ttl: bool,
    #[online_config(skip)]
    pub background_error_recovery_window: ReadableDuration,
    /// Interval to check TTL for all SSTs,
    pub ttl_check_poll_interval: ReadableDuration,
    #[online_config(submodule)]
    pub flow_control: FlowControlConfig,
    #[online_config(submodule)]
    pub block_cache: BlockCacheConfig,
    #[online_config(submodule)]
    pub io_rate_limit: IORateLimitConfig,
}

impl Default for Config {
    fn default() -> Config {
        let cpu_num = SysQuota::cpu_cores_quota();
        Config {
            data_dir: DEFAULT_DATA_DIR.to_owned(),
            gc_ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            scheduler_concurrency: DEFAULT_SCHED_CONCURRENCY,
            scheduler_worker_pool_size: if cpu_num >= 16.0 {
                8
            } else {
                std::cmp::max(1, std::cmp::min(4, cpu_num as usize))
            },
            scheduler_pending_write_threshold: ReadableSize::mb(DEFAULT_SCHED_PENDING_WRITE_MB),
            reserve_space: ReadableSize::gb(DEFAULT_RESERVED_SPACE_GB),
            enable_async_apply_prewrite: false,
            api_version: 1,
            enable_ttl: false,
            ttl_check_poll_interval: ReadableDuration::hours(12),
            flow_control: FlowControlConfig::default(),
            block_cache: BlockCacheConfig::default(),
            io_rate_limit: IORateLimitConfig::default(),
            background_error_recovery_window: ReadableDuration::hours(1),
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.data_dir != DEFAULT_DATA_DIR {
            self.data_dir = config::canonicalize_path(&self.data_dir)?
        }
        if self.scheduler_concurrency > MAX_SCHED_CONCURRENCY {
            warn!(
                "TiKV has optimized latch since v4.0, so it is not necessary to set large schedule \
                concurrency. To save memory, change it from {:?} to {:?}",
                self.scheduler_concurrency, MAX_SCHED_CONCURRENCY
            );
            self.scheduler_concurrency = MAX_SCHED_CONCURRENCY;
        }
        if !matches!(self.api_version, 1 | 2) {
            return Err("storage.api_version can only be set to 1 or 2.".into());
        }
        if self.api_version == 2 && !self.enable_ttl {
            return Err(
                "storage.enable_ttl must be true in API V2 because API V2 forces to enable TTL."
                    .into(),
            );
        };
        // max worker pool size should be at least 4.
        let max_pool_size = std::cmp::max(4, SysQuota::cpu_cores_quota() as usize);
        if self.scheduler_worker_pool_size == 0 || self.scheduler_worker_pool_size > max_pool_size {
            return Err(
                format!(
                    "storage.scheduler_worker_pool_size should be greater than 0 and less than or equal to {}",
                    max_pool_size
                ).into()
            );
        }
        self.io_rate_limit.validate()?;

        Ok(())
    }

    pub fn api_version(&self) -> ApiVersion {
        match self.api_version {
            1 if self.enable_ttl => ApiVersion::V1ttl,
            1 => ApiVersion::V1,
            2 => ApiVersion::V2,
            _ => unreachable!(),
        }
    }

    pub fn set_api_version(&mut self, api_version: ApiVersion) {
        match api_version {
            ApiVersion::V1 => {
                self.api_version = 1;
                self.enable_ttl = false;
            }
            ApiVersion::V1ttl => {
                self.api_version = 1;
                self.enable_ttl = true;
            }
            ApiVersion::V2 => {
                self.api_version = 2;
                self.enable_ttl = true;
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FlowControlConfig {
    pub enable: bool,
    #[online_config(skip)]
    pub soft_pending_compaction_bytes_limit: ReadableSize,
    #[online_config(skip)]
    pub hard_pending_compaction_bytes_limit: ReadableSize,
    #[online_config(skip)]
    pub memtables_threshold: u64,
    #[online_config(skip)]
    pub l0_files_threshold: u64,
}

impl Default for FlowControlConfig {
    fn default() -> FlowControlConfig {
        FlowControlConfig {
            enable: true,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(1024),
            memtables_threshold: 5,
            l0_files_threshold: 20,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BlockCacheConfig {
    #[online_config(skip)]
    pub shared: bool,
    pub capacity: Option<ReadableSize>,
    #[online_config(skip)]
    pub num_shard_bits: i32,
    #[online_config(skip)]
    pub strict_capacity_limit: bool,
    #[online_config(skip)]
    pub high_pri_pool_ratio: f64,
    #[online_config(skip)]
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
    fn adjust_shard_bits(&self, capacity: usize) -> i32 {
        let max_shard_count = capacity / MIN_BLOCK_CACHE_SHARD_SIZE;
        if max_shard_count < (2 << self.num_shard_bits) {
            max((max_shard_count as f64).log2() as i32, 1)
        } else {
            self.num_shard_bits
        }
    }

    pub fn build_shared_cache(&self) -> Option<Cache> {
        if !self.shared {
            return None;
        }
        let capacity = match self.capacity {
            None => {
                let total_mem = SysQuota::memory_limit_in_bytes();
                ((total_mem as f64) * BLOCK_CACHE_RATE) as usize
            }
            Some(c) => c.0 as usize,
        };
        let mut cache_opts = LRUCacheOptions::new();
        cache_opts.set_capacity(capacity);
        cache_opts.set_num_shard_bits(self.adjust_shard_bits(capacity) as c_int);
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
                        warn!(
                            "Create jemalloc nodump allocator for block cache failed: {}, continue with default allocator",
                            e
                        );
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct IORateLimitConfig {
    pub max_bytes_per_sec: ReadableSize,
    #[online_config(skip)]
    pub mode: IORateLimitMode,
    /// When this flag is off, high-priority IOs are counted but not limited. Default
    /// set to false because the optimal throughput target provided by user might not be
    /// the maximum available bandwidth. For multi-tenancy use case, this flag should be
    /// turned on.
    #[online_config(skip)]
    pub strict: bool,
    pub foreground_read_priority: IOPriority,
    pub foreground_write_priority: IOPriority,
    pub flush_priority: IOPriority,
    pub level_zero_compaction_priority: IOPriority,
    pub compaction_priority: IOPriority,
    pub replication_priority: IOPriority,
    pub load_balance_priority: IOPriority,
    pub gc_priority: IOPriority,
    pub import_priority: IOPriority,
    pub export_priority: IOPriority,
    pub other_priority: IOPriority,
}

impl Default for IORateLimitConfig {
    fn default() -> IORateLimitConfig {
        IORateLimitConfig {
            max_bytes_per_sec: ReadableSize::mb(0),
            mode: IORateLimitMode::WriteOnly,
            strict: false,
            foreground_read_priority: IOPriority::High,
            foreground_write_priority: IOPriority::High,
            flush_priority: IOPriority::High,
            level_zero_compaction_priority: IOPriority::Medium,
            compaction_priority: IOPriority::Low,
            replication_priority: IOPriority::High,
            load_balance_priority: IOPriority::High,
            gc_priority: IOPriority::High,
            import_priority: IOPriority::Medium,
            export_priority: IOPriority::Medium,
            other_priority: IOPriority::High,
        }
    }
}

impl IORateLimitConfig {
    pub fn build(&self, enable_statistics: bool) -> IORateLimiter {
        let limiter = IORateLimiter::new(self.mode, self.strict, enable_statistics);
        limiter.set_io_rate_limit(self.max_bytes_per_sec.0 as usize);
        limiter.set_io_priority(IOType::ForegroundRead, self.foreground_read_priority);
        limiter.set_io_priority(IOType::ForegroundWrite, self.foreground_write_priority);
        limiter.set_io_priority(IOType::Flush, self.flush_priority);
        limiter.set_io_priority(
            IOType::LevelZeroCompaction,
            self.level_zero_compaction_priority,
        );
        limiter.set_io_priority(IOType::Compaction, self.compaction_priority);
        limiter.set_io_priority(IOType::Replication, self.replication_priority);
        limiter.set_io_priority(IOType::LoadBalance, self.load_balance_priority);
        limiter.set_io_priority(IOType::Gc, self.gc_priority);
        limiter.set_io_priority(IOType::Import, self.import_priority);
        limiter.set_io_priority(IOType::Export, self.export_priority);
        limiter.set_io_priority(IOType::Other, self.other_priority);
        limiter
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.other_priority != IOPriority::High {
            warn!(
                "Occasionally some critical IO operations are tagged as IOType::Other, \
                  e.g. IOs are fired from unmanaged threads, thread-local type storage exceeds \
                  capacity. To be on the safe side, change priority for IOType::Other from \
                  {:?} to {:?}",
                self.other_priority,
                IOPriority::High
            );
            self.other_priority = IOPriority::High;
        }
        if self.gc_priority != self.foreground_write_priority {
            warn!(
                "GC writes are merged with foreground writes. To avoid priority inversion, change \
                  priority for IOType::Gc from {:?} to {:?}",
                self.gc_priority, self.foreground_write_priority,
            );
            self.gc_priority = self.foreground_write_priority;
        }
        if self.mode != IORateLimitMode::WriteOnly {
            return Err(
                "storage.io-rate-limit.mode other than write-only is not supported.".into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_storage_config() {
        let mut cfg = Config::default();
        assert!(cfg.validate().is_ok());

        let max_pool_size = std::cmp::max(4, SysQuota::cpu_cores_quota() as usize);
        cfg.scheduler_worker_pool_size = max_pool_size;
        assert!(cfg.validate().is_ok());

        cfg.scheduler_worker_pool_size = 0;
        assert!(cfg.validate().is_err());

        cfg.scheduler_worker_pool_size = max_pool_size + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_adjust_shard_bits() {
        let config = BlockCacheConfig::default();
        let shard_bits = config.adjust_shard_bits(ReadableSize::gb(1).0 as usize);
        assert_eq!(shard_bits, 3);

        let shard_bits = config.adjust_shard_bits(ReadableSize::gb(4).0 as usize);
        assert_eq!(shard_bits, 5);

        let shard_bits = config.adjust_shard_bits(ReadableSize::gb(8).0 as usize);
        assert_eq!(shard_bits, 6);

        let shard_bits = config.adjust_shard_bits(ReadableSize::mb(1).0 as usize);
        assert_eq!(shard_bits, 1);
    }
}
