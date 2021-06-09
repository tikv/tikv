// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage configuration.

use crate::config::BLOCK_CACHE_RATE;
use crate::server::ttl::TTLCheckerTask;
use crate::server::CONFIG_ROCKSDB_GAUGE;
use engine_rocks::raw::{Cache, LRUCacheOptions, MemoryAllocator};
use engine_traits::{ColumnFamilyOptions, CF_DEFAULT, KvEngine};
use file_system::{get_io_rate_limiter, IOPriority, IORateLimitMode, IORateLimiter, IOType};
use libc::c_int;
use online_config::{ConfigChange, ConfigManager, ConfigValue, OnlineConfig, Result as CfgResult};
use std::error::Error;
use strum::IntoEnumIterator;
use tikv_util::config::{self, OptionReadableSize, ReadableDuration, ReadableSize};
use tikv_util::sys::SysQuota;
use tikv_util::worker::Scheduler;

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
    #[online_config(skip)]
    pub scheduler_worker_pool_size: usize,
    #[online_config(skip)]
    pub scheduler_pending_write_threshold: ReadableSize,
    #[online_config(skip)]
    // Reserve disk space to make tikv would have enough space to compact when disk is full.
    pub reserve_space: ReadableSize,
    #[online_config(skip)]
    pub enable_async_apply_prewrite: bool,
    #[online_config(skip)]
    pub enable_ttl: bool,
    /// Interval to check TTL for all SSTs,
    pub ttl_check_poll_interval: ReadableDuration,
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
            scheduler_worker_pool_size: if cpu_num >= 16.0 { 8 } else { 4 },
            scheduler_pending_write_threshold: ReadableSize::mb(DEFAULT_SCHED_PENDING_WRITE_MB),
            reserve_space: ReadableSize::gb(DEFAULT_RESERVED_SPACE_GB),
            enable_async_apply_prewrite: false,
            enable_ttl: false,
            ttl_check_poll_interval: ReadableDuration::hours(12),
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
            warn!(
                "TiKV has optimized latch since v4.0, so it is not necessary to set large schedule \
                concurrency. To save memory, change it from {:?} to {:?}",
                self.scheduler_concurrency, MAX_SCHED_CONCURRENCY
            );
            self.scheduler_concurrency = MAX_SCHED_CONCURRENCY;
        }
        self.io_rate_limit.validate()
    }
}

pub struct StorageConfigManger<EK: KvEngine> {
    kvdb: EK,
    shared_block_cache: bool,
    ttl_checker_scheduler: Scheduler<TTLCheckerTask>,
}

impl<EK: KvEngine> StorageConfigManger<EK> {
    pub fn new(
        kvdb: EK,
        shared_block_cache: bool,
        ttl_checker_scheduler: Scheduler<TTLCheckerTask>,
    ) -> StorageConfigManger<EK> {
        StorageConfigManger {
            kvdb,
            shared_block_cache,
            ttl_checker_scheduler,
        }
    }
}

impl<EK: KvEngine> ConfigManager for StorageConfigManger<EK> {
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
        } else if let Some(v) = change.remove("ttl_check_poll_interval") {
            let interval: ReadableDuration = v.into();
            self.ttl_checker_scheduler
                .schedule(TTLCheckerTask::UpdatePollInterval(interval.into()))
                .unwrap();
        }
        if let Some(ConfigValue::Module(mut io_rate_limit)) = change.remove("io_rate_limit") {
            let limiter = match get_io_rate_limiter() {
                None => return Err("IO rate limiter is not present".into()),
                Some(limiter) => limiter,
            };
            if let Some(limit) = io_rate_limit.remove("max_bytes_per_sec") {
                let limit: ReadableSize = limit.into();
                limiter.set_io_rate_limit(limit.0 as usize);
            }

            for t in IOType::iter() {
                if let Some(priority) = io_rate_limit.remove(&(t.as_str().to_owned() + "_priority"))
                {
                    let priority: IOPriority = priority.into();
                    limiter.set_io_priority(t, priority);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BlockCacheConfig {
    #[online_config(skip)]
    pub shared: bool,
    pub capacity: OptionReadableSize,
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
                let total_mem = SysQuota::memory_limit_in_bytes();
                ((total_mem as f64) * BLOCK_CACHE_RATE) as usize
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
