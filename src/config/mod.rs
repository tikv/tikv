// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

//! Configuration for the entire server.
//!
//! TiKV is configured through the `TikvConfig` type, which is in turn
//! made up of many other configuration types.

mod configurable;

use std::{
    cmp,
    collections::{HashMap, HashSet},
    convert::TryFrom,
    error::Error,
    fs,
    io::{Error as IoError, Write},
    path::Path,
    str,
    sync::{Arc, RwLock},
};

use api_version::ApiV1Ttl;
use causal_ts::Config as CausalTsConfig;
pub use configurable::{ConfigRes, ConfigurableDb, loop_registry};
use encryption_export::DataKeyManager;
use engine_rocks::{
    DEFAULT_PROP_KEYS_INDEX_DISTANCE, DEFAULT_PROP_SIZE_INDEX_DISTANCE, RaftDbLogger,
    RangePropertiesCollectorFactory, RawMvccPropertiesCollectorFactory, RocksCfOptions,
    RocksDbOptions, RocksEngine, RocksEventListener, RocksStatistics, RocksTitanDbOptions,
    RocksdbLogger, TtlPropertiesCollectorFactory,
    config::{self as rocks_config, BlobRunMode, CompressionType, LogLevel as RocksLogLevel},
    get_env,
    properties::MvccPropertiesCollectorFactory,
    raw::{
        BlockBasedOptions, Cache, ChecksumType, CompactionPriority, ConcurrentTaskLimiter,
        DBCompactionStyle, DBCompressionType, DBRateLimiterMode, DBRecoveryMode, Env,
        LRUCacheOptions, PrepopulateBlockCache, RateLimiter, WriteBufferManager,
    },
    util::{
        FixedPrefixSliceTransform, FixedSuffixSliceTransform, NoopSliceTransform,
        RangeCompactionFilterFactory, StackingCompactionFilterFactory,
    },
};
use engine_traits::{
    CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, CfOptions as _, DbOptions as _, MiscExt,
    TitanCfOptions as _,
};
use file_system::IoRateLimiter;
use in_memory_engine::InMemoryEngineConfig;
use keys::region_raft_prefix_len;
use kvproto::kvrpcpb::ApiVersion;
use lazy_static::lazy_static;
use online_config::{ConfigChange, ConfigManager, ConfigValue, OnlineConfig, Result as CfgResult};
use pd_client::Config as PdConfig;
use raft_log_engine::{
    RaftEngineConfig as RawRaftEngineConfig, ReadableSize as RaftEngineReadableSize,
};
use raftstore::{
    coprocessor::{Config as CopConfig, RegionInfoAccessor},
    store::{
        CompactionGuardGeneratorFactory, Config as RaftstoreConfig, ForcePartitionRangeManager,
        SplitConfig,
    },
};
use resource_control::config::Config as ResourceControlConfig;
use resource_metering::Config as ResourceMeteringConfig;
use security::SecurityConfig;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DError, Unexpected},
};
use serde_json::{Map, Value, to_value};
use tikv_util::{
    config::{
        self, LogFormat, MIB, RaftDataStateMachine, ReadableDuration, ReadableSchedule,
        ReadableSize, TomlWriter,
    },
    logger::{get_level_by_string, get_string_by_level, set_log_level},
    sys::SysQuota,
    time::duration_to_sec,
    yatp_pool,
};

use crate::{
    coprocessor_v2::Config as CoprocessorV2Config,
    import::Config as ImportConfig,
    server::{
        CONFIG_ROCKSDB_CF_GAUGE, CONFIG_ROCKSDB_DB_GAUGE, Config as ServerConfig,
        gc_worker::{GcConfig, RawCompactionFilterFactory, WriteCompactionFilterFactory},
        lock_manager::Config as PessimisticTxnConfig,
        ttl::TtlCompactionFilterFactory,
    },
    storage::config::{Config as StorageConfig, DEFAULT_DATA_DIR, EngineType},
};

pub const DEFAULT_ROCKSDB_SUB_DIR: &str = "db";
pub const DEFAULT_TABLET_SUB_DIR: &str = "tablets";

/// By default, block cache size will be set to 45% of system memory.
pub const BLOCK_CACHE_RATE: f64 = 0.45;
/// Because multi-rocksdb has 25% memory table quota, we have to reduce block
/// cache a bit
pub const RAFTSTORE_V2_BLOCK_CACHE_RATE: f64 = 0.30;
/// By default, TiKV will try to limit memory usage to 75% of system memory.
pub const MEMORY_USAGE_LIMIT_RATE: f64 = 0.75;

/// Min block cache shard's size. If a shard is too small, the index/filter data
/// may not fit one shard
pub const MIN_BLOCK_CACHE_SHARD_SIZE: usize = 128 * MIB as usize;

/// Maximum of 15% of system memory can be used by Raft Engine. Normally its
/// memory usage is much smaller than that.
const RAFT_ENGINE_MEMORY_LIMIT_RATE: f64 = 0.15;

const WRITE_BUFFER_MEMORY_LIMIT_RATE: f64 = 0.2;
// Too large will increase Raft Engine memory usage.
const WRITE_BUFFER_MEMORY_LIMIT_MAX: u64 = ReadableSize::gb(8).0;
const DEFAULT_LOCK_BUFFER_MEMORY_LIMIT: ReadableSize = ReadableSize::mb(128);

/// Configs that actually took effect in the last run
pub const LAST_CONFIG_FILE: &str = "last_tikv.toml";
const TMP_CONFIG_FILE: &str = "tmp_tikv.toml";
const MAX_BLOCK_SIZE: usize = 32 * MIB as usize;

fn bloom_filter_ratio(et: EngineType) -> f64 {
    match et {
        EngineType::RaftKv => 0.1,
        // In v2, every peer has its own tablet. The data scale is about tens of
        // GiBs. We only need a small portion for those key.
        // TODO: disable it for now until find out the proper ratio
        EngineType::RaftKv2 => 0.0,
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TitanCfConfig {
    pub min_blob_size: Option<ReadableSize>,
    pub blob_file_compression: CompressionType,
    #[online_config(skip)]
    pub zstd_dict_size: ReadableSize,
    #[online_config(skip)]
    pub shared_blob_cache: bool,
    #[online_config(skip)]
    pub blob_cache_size: ReadableSize,
    #[online_config(skip)]
    pub min_gc_batch_size: ReadableSize,
    #[online_config(skip)]
    pub max_gc_batch_size: ReadableSize,
    pub discardable_ratio: f64,
    #[online_config(skip)]
    pub merge_small_file_threshold: ReadableSize,
    pub blob_run_mode: BlobRunMode,
    #[online_config(skip)]
    pub level_merge: bool,
    #[online_config(skip)]
    pub range_merge: bool,
    #[online_config(skip)]
    pub max_sorted_runs: i32,

    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "The feature is removed"]
    pub gc_merge_rewrite: bool,
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "Titan doesn't need to sample anymore"]
    pub sample_ratio: Option<f64>,
}
const DEFAULT_MIN_BLOB_SIZE: ReadableSize = ReadableSize::kb(32);

impl Default for TitanCfConfig {
    #[allow(deprecated)]
    fn default() -> Self {
        Self {
            min_blob_size: None, /* 32KB for newly created instances, and keep config value from
                                  * old installation.
                                  * The logic is in `optional_default_cfg_adjust_with` */
            blob_file_compression: CompressionType::Zstd,
            zstd_dict_size: ReadableSize::kb(0),
            shared_blob_cache: true,
            blob_cache_size: ReadableSize::mb(0),
            min_gc_batch_size: ReadableSize::mb(16),
            max_gc_batch_size: ReadableSize::mb(64),
            discardable_ratio: 0.5,
            sample_ratio: None,
            merge_small_file_threshold: ReadableSize::mb(8),
            blob_run_mode: BlobRunMode::Normal,
            level_merge: false,
            range_merge: true,
            max_sorted_runs: 20,
            gc_merge_rewrite: false,
        }
    }
}

impl TitanCfConfig {
    fn default_for_disabled() -> Self {
        Self {
            blob_run_mode: BlobRunMode::ReadOnly,
            ..Default::default()
        }
    }

    fn build_opts(&self, cache: &Cache) -> RocksTitanDbOptions {
        let mut opts = RocksTitanDbOptions::new();
        opts.set_min_blob_size(self.min_blob_size.unwrap_or(DEFAULT_MIN_BLOB_SIZE).0);
        opts.set_blob_file_compression(self.blob_file_compression.into());
        // To try zstd dict compression, set dict size to 4k, sample size to 100X dict
        // size
        opts.set_compression_options(
            -14,                                // window_bits
            32767,                              // level
            0,                                  // strategy
            self.zstd_dict_size.0 as i32,       // zstd dict size
            self.zstd_dict_size.0 as i32 * 100, // zstd sample size
        );
        if self.shared_blob_cache {
            opts.set_blob_cache(cache);
        } else {
            let mut cache_opts = LRUCacheOptions::new();
            cache_opts.set_capacity(self.blob_cache_size.0 as usize);
            let cache = Cache::new_lru_cache(cache_opts);
            opts.set_blob_cache(&cache);
        }
        opts.set_min_gc_batch_size(self.min_gc_batch_size.0);
        opts.set_max_gc_batch_size(self.max_gc_batch_size.0);
        opts.set_discardable_ratio(self.discardable_ratio);
        opts.set_merge_small_file_threshold(self.merge_small_file_threshold.0);
        opts.set_blob_run_mode(self.blob_run_mode.into());
        opts.set_level_merge(self.level_merge);
        opts.set_range_merge(self.range_merge);
        opts.set_max_sorted_runs(self.max_sorted_runs);
        opts
    }

    #[allow(deprecated)]
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.gc_merge_rewrite {
            return Err(
                "gc-merge-rewrite is deprecated. The data produced when this \
                option is enabled cannot be read by this version. Therefore, if \
                this option has been applied to an existing node, you must downgrade \
                it to the previous version and fully clean up the old data. See more \
                details of how to do that in the documentation for the blob-run-mode \
                confuguration."
                    .into(),
            );
        }
        if self.sample_ratio.is_some() {
            warn!("sample-ratio is deprecated. Ignoring the value.");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BackgroundJobLimits {
    max_background_jobs: u32,
    max_background_flushes: u32,
    max_sub_compactions: u32,
    max_titan_background_gc: u32,
}

const KVDB_DEFAULT_BACKGROUND_JOB_LIMITS: BackgroundJobLimits = BackgroundJobLimits {
    max_background_jobs: 9,
    max_background_flushes: 3,
    max_sub_compactions: 3,
    max_titan_background_gc: 1,
};

const RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS: BackgroundJobLimits = BackgroundJobLimits {
    max_background_jobs: 4,
    max_background_flushes: 1,
    max_sub_compactions: 2,
    max_titan_background_gc: 1,
};

// `defaults` serves as an upper bound for returning limits.
fn get_background_job_limits_impl(
    engine_type: EngineType,
    cpu_num: u32,
    defaults: &BackgroundJobLimits,
) -> BackgroundJobLimits {
    // At the minimum, we should have two background jobs: one for flush and one for
    // compaction. Otherwise, the number of background jobs should not exceed
    // cpu_num - 1.
    let mut max_background_jobs = cmp::max(2, cmp::min(defaults.max_background_jobs, cpu_num - 1));
    // Scale flush threads proportionally to cpu cores. Also make sure the number of
    // flush threads doesn't exceed total jobs.
    let max_background_flushes = cmp::min(
        max_background_jobs.div_ceil(4),
        defaults.max_background_flushes,
    );

    // set the default compaction threads differently for v1 and v2:
    // v1: cap max_sub_compactions to allow at least two compactions.
    // v2: decrease the compaction threads to make the qps more stable.
    let max_compactions = match engine_type {
        EngineType::RaftKv => max_background_jobs - max_background_flushes,
        EngineType::RaftKv2 => max_background_jobs.div_ceil(4),
    };
    let max_sub_compactions: u32 = (max_compactions - 1).clamp(1, defaults.max_sub_compactions);
    max_background_jobs = max_background_flushes + max_compactions;
    // Maximum background GC threads for Titan
    let max_titan_background_gc = cmp::min(defaults.max_titan_background_gc, cpu_num);

    BackgroundJobLimits {
        max_background_jobs,
        max_background_flushes,
        max_sub_compactions,
        max_titan_background_gc,
    }
}

fn get_background_job_limits(
    engine_type: EngineType,
    defaults: &BackgroundJobLimits,
) -> BackgroundJobLimits {
    let cpu_num = cmp::max(SysQuota::cpu_cores_quota() as u32, 1);
    get_background_job_limits_impl(engine_type, cpu_num, defaults)
}

macro_rules! cf_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            #[online_config(skip)]
            pub block_size: ReadableSize,
            // FIXME: deprecate it and update all tests related to it.
            pub block_cache_size: Option<ReadableSize>,
            #[online_config(skip)]
            pub disable_block_cache: bool,
            #[online_config(skip)]
            pub cache_index_and_filter_blocks: bool,
            #[online_config(skip)]
            pub pin_l0_filter_and_index_blocks: bool,
            #[online_config(skip)]
            pub use_bloom_filter: bool,
            #[online_config(skip)]
            pub optimize_filters_for_hits: bool,
            #[online_config(skip)]
            pub optimize_filters_for_memory: bool,
            #[online_config(skip)]
            pub whole_key_filtering: bool,
            #[online_config(skip)]
            pub bloom_filter_bits_per_key: i32,
            #[online_config(skip)]
            pub block_based_bloom_filter: bool,
            #[online_config(skip)]
            pub ribbon_filter_above_level: Option<u32>,
            #[online_config(skip)]
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "rocks_config::compression_type_level_serde")]
            #[online_config(skip)]
            pub compression_per_level: [DBCompressionType; 7],
            pub write_buffer_size: Option<ReadableSize>,
            pub write_buffer_limit: Option<ReadableSize>,
            pub max_write_buffer_number: i32,
            #[online_config(skip)]
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: Option<ReadableSize>,
            pub level0_file_num_compaction_trigger: i32,
            // See `fill_cf_opts` for detailed comments on the following two fields.
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: Option<i32>,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "rocks_config::compaction_pri_serde")]
            #[online_config(skip)]
            pub compaction_pri: CompactionPriority,
            #[online_config(skip)]
            pub dynamic_level_bytes: bool,
            #[online_config(skip)]
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
            #[serde(with = "rocks_config::compaction_style_serde")]
            #[online_config(skip)]
            pub compaction_style: DBCompactionStyle,
            pub disable_auto_compactions: bool,
            pub disable_write_stall: bool,
            pub soft_pending_compaction_bytes_limit: ReadableSize,
            pub hard_pending_compaction_bytes_limit: Option<ReadableSize>,
            #[online_config(skip)]
            pub force_consistency_checks: bool,
            #[online_config(skip)]
            pub prop_size_index_distance: u64,
            #[online_config(skip)]
            pub prop_keys_index_distance: u64,
            #[online_config(skip)]
            pub enable_doubly_skiplist: bool,
            #[online_config(skip)]
            pub enable_compaction_guard: Option<bool>,
            #[online_config(skip)]
            pub compaction_guard_min_output_file_size: ReadableSize,
            #[online_config(skip)]
            pub compaction_guard_max_output_file_size: ReadableSize,
            #[serde(with = "rocks_config::compression_type_serde")]
            #[online_config(skip)]
            pub bottommost_level_compression: DBCompressionType,
            #[online_config(skip)]
            pub bottommost_zstd_compression_dict_size: i32,
            #[online_config(skip)]
            pub bottommost_zstd_compression_sample_size: i32,
            #[serde(with = "rocks_config::prepopulate_block_cache_serde")]
            #[online_config(skip)]
            pub prepopulate_block_cache: PrepopulateBlockCache,
            #[online_config(skip)]
            pub format_version: Option<u32>,
            #[serde(with = "rocks_config::checksum_serde")]
            #[online_config(skip)]
            pub checksum: ChecksumType,
            #[online_config(skip)]
            pub max_compactions: Option<u32>,
            // `ttl == None` means disable this feature in Rocksdb.
            // `ttl` in Rocksdb is 30 days as default.
            #[online_config(skip)]
            pub ttl: Option<ReadableDuration>,
            // `periodic_compaction_seconds == None` means disabled this feature in Rocksdb.
            // `periodic_compaction_seconds` in Rocksdb is 30 days as default.
            #[online_config(skip)]
            pub periodic_compaction_seconds: Option<ReadableDuration>,
            #[online_config(submodule)]
            pub titan: TitanCfConfig,
        }

        impl $name {
            #[inline]
            fn target_file_size_base(&self) -> u64 {
                self.target_file_size_base.unwrap_or(ReadableSize::mb(8)).0
            }

            fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.block_size.0 as usize > MAX_BLOCK_SIZE {
                    return Err(format!(
                        "invalid block-size {} for {}, exceed max size {}",
                        self.block_size.0,
                        stringify!($name),
                        MAX_BLOCK_SIZE
                    )
                    .into());
                }
                if self.format_version.is_some_and(|v| v > 5) {
                    return Err("format-version larger than 5 is unsupported".into());
                }
                if self.ribbon_filter_above_level.is_some()
                    && self.format_version.is_none_or(|v| v < 5)
                {
                    return Err(
                        "ribbon-filter-above-level is only supported when format-version >= 5"
                            .into(),
                    );
                }
                self.titan.validate()?;
                Ok(())
            }
        }
    };
}

macro_rules! write_into_cf_metrics {
    ($cf:expr, $tag:expr, $metrics:expr) => {{
        $metrics
            .with_label_values(&[$tag, "block_size"])
            .set($cf.block_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "block_cache_size"])
            .set($cf.block_cache_size.map(|s| s.0).unwrap_or_default() as f64);
        $metrics
            .with_label_values(&[$tag, "disable_block_cache"])
            .set(($cf.disable_block_cache as i32).into());

        $metrics
            .with_label_values(&[$tag, "cache_index_and_filter_blocks"])
            .set(($cf.cache_index_and_filter_blocks as i32).into());
        $metrics
            .with_label_values(&[$tag, "pin_l0_filter_and_index_blocks"])
            .set(($cf.pin_l0_filter_and_index_blocks as i32).into());
        $metrics
            .with_label_values(&[$tag, "use_bloom_filter"])
            .set(($cf.use_bloom_filter as i32).into());
        $metrics
            .with_label_values(&[$tag, "optimize_filters_for_hits"])
            .set(($cf.optimize_filters_for_hits as i32).into());
        $metrics
            .with_label_values(&[$tag, "optimize_filters_for_memory"])
            .set(($cf.optimize_filters_for_memory as i32).into());
        $metrics
            .with_label_values(&[$tag, "whole_key_filtering"])
            .set(($cf.whole_key_filtering as i32).into());
        $metrics
            .with_label_values(&[$tag, "bloom_filter_bits_per_key"])
            .set($cf.bloom_filter_bits_per_key.into());
        $metrics
            .with_label_values(&[$tag, "block_based_bloom_filter"])
            .set(($cf.block_based_bloom_filter as i32).into());
        if let Some(level) = $cf.ribbon_filter_above_level {
            $metrics
                .with_label_values(&[$tag, "ribbon_filter_above_level"])
                .set((level as i32).into());
        }
        $metrics
            .with_label_values(&[$tag, "read_amp_bytes_per_bit"])
            .set($cf.read_amp_bytes_per_bit.into());
        for (i, level_compression) in $cf.compression_per_level.iter().enumerate() {
            $metrics
                .with_label_values(&[$tag, &format!("compression_level_{}", i)])
                .set(*level_compression as u64 as f64);
        }
        $metrics
            .with_label_values(&[$tag, "write_buffer_size"])
            .set($cf.write_buffer_size.unwrap_or_default().0 as f64);
        $metrics
            .with_label_values(&[$tag, "write_buffer_limit"])
            .set($cf.write_buffer_limit.unwrap_or_default().0 as f64);
        $metrics
            .with_label_values(&[$tag, "max_write_buffer_number"])
            .set($cf.max_write_buffer_number.into());
        $metrics
            .with_label_values(&[$tag, "min_write_buffer_number_to_merge"])
            .set($cf.min_write_buffer_number_to_merge.into());
        $metrics
            .with_label_values(&[$tag, "max_bytes_for_level_base"])
            .set($cf.max_bytes_for_level_base.0 as f64);
        $metrics
            .with_label_values(&[$tag, "target_file_size_base"])
            .set($cf.target_file_size_base() as f64);
        $metrics
            .with_label_values(&[$tag, "level0_file_num_compaction_trigger"])
            .set($cf.level0_file_num_compaction_trigger.into());
        $metrics
            .with_label_values(&[$tag, "level0_slowdown_writes_trigger"])
            .set($cf.level0_slowdown_writes_trigger.into());
        $metrics
            .with_label_values(&[$tag, "level0_stop_writes_trigger"])
            .set($cf.level0_stop_writes_trigger.unwrap_or_default().into());
        $metrics
            .with_label_values(&[$tag, "max_compaction_bytes"])
            .set($cf.max_compaction_bytes.0 as f64);
        $metrics
            .with_label_values(&[$tag, "compaction_pri"])
            .set($cf.compaction_pri as u64 as f64);
        $metrics
            .with_label_values(&[$tag, "dynamic_level_bytes"])
            .set(($cf.dynamic_level_bytes as i32).into());
        $metrics
            .with_label_values(&[$tag, "num_levels"])
            .set($cf.num_levels.into());
        $metrics
            .with_label_values(&[$tag, "max_bytes_for_level_multiplier"])
            .set($cf.max_bytes_for_level_multiplier.into());
        $metrics
            .with_label_values(&[$tag, "compaction_style"])
            .set($cf.compaction_style as u64 as f64);
        $metrics
            .with_label_values(&[$tag, "disable_auto_compactions"])
            .set(($cf.disable_auto_compactions as i32).into());
        $metrics
            .with_label_values(&[$tag, "disable_write_stall"])
            .set(($cf.disable_write_stall as i32).into());
        $metrics
            .with_label_values(&[$tag, "soft_pending_compaction_bytes_limit"])
            .set($cf.soft_pending_compaction_bytes_limit.0 as f64);
        $metrics
            .with_label_values(&[$tag, "hard_pending_compaction_bytes_limit"])
            .set(
                $cf.hard_pending_compaction_bytes_limit
                    .unwrap_or_default()
                    .0 as f64,
            );
        $metrics
            .with_label_values(&[$tag, "force_consistency_checks"])
            .set(($cf.force_consistency_checks as i32).into());
        $metrics
            .with_label_values(&[$tag, "prop_size_index_distance"])
            .set($cf.prop_size_index_distance as f64);
        $metrics
            .with_label_values(&[$tag, "prop_keys_index_distance"])
            .set($cf.prop_keys_index_distance as f64);
        $metrics
            .with_label_values(&[$tag, "enable_doubly_skiplist"])
            .set(($cf.enable_doubly_skiplist as i32).into());
        $metrics
            .with_label_values(&[$tag, "enable_compaction_guard"])
            .set($cf.enable_compaction_guard.unwrap_or_default().into());
        $metrics
            .with_label_values(&[$tag, "compaction_guard_min_output_file_size"])
            .set($cf.compaction_guard_min_output_file_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "compaction_guard_max_output_file_size"])
            .set($cf.compaction_guard_max_output_file_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "bottommost_level_compression"])
            .set($cf.bottommost_level_compression as u64 as f64);
        $metrics
            .with_label_values(&[$tag, "bottommost_zstd_compression_dict_size"])
            .set($cf.bottommost_zstd_compression_dict_size.into());
        $metrics
            .with_label_values(&[$tag, "bottommost_zstd_compression_sample_size"])
            .set($cf.bottommost_zstd_compression_sample_size.into());
        $metrics
            .with_label_values(&[$tag, "prepopulate_block_cache"])
            .set($cf.prepopulate_block_cache as u64 as f64);
        $metrics
            .with_label_values(&[$tag, "format_version"])
            .set($cf.format_version.unwrap_or(2) as f64);
        $metrics
            .with_label_values(&[$tag, "checksum"])
            .set($cf.checksum as u64 as f64);
        $metrics
            .with_label_values(&[$tag, "max_compactions"])
            .set($cf.max_compactions.unwrap_or_default().into());
        $metrics
            .with_label_values(&[$tag, "ttl"])
            .set($cf.ttl.unwrap_or_default().as_secs_f64());
        $metrics
            .with_label_values(&[$tag, "periodic_compaction_seconds"])
            .set(
                $cf.periodic_compaction_seconds
                    .unwrap_or_default()
                    .as_secs_f64(),
            );

        // Titan specific metrics.
        $metrics
            .with_label_values(&[$tag, "titan_min_blob_size"])
            .set($cf.titan.min_blob_size.unwrap_or_default().0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_blob_file_compression"])
            .set($cf.titan.blob_file_compression as u64 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_zstd_dict_size"])
            .set($cf.titan.zstd_dict_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_shared_blob_cache"])
            .set($cf.titan.shared_blob_cache.into());
        $metrics
            .with_label_values(&[$tag, "titan_blob_cache_size"])
            .set($cf.titan.blob_cache_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_min_gc_batch_size"])
            .set($cf.titan.min_gc_batch_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_max_gc_batch_size"])
            .set($cf.titan.max_gc_batch_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_discardable_ratio"])
            .set($cf.titan.discardable_ratio);
        $metrics
            .with_label_values(&[$tag, "titan_merge_small_file_threshold"])
            .set($cf.titan.merge_small_file_threshold.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_blob_run_mode"])
            .set($cf.titan.blob_run_mode as u64 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_level_merge"])
            .set($cf.titan.level_merge.into());
        $metrics
            .with_label_values(&[$tag, "titan_range_merge"])
            .set($cf.titan.range_merge.into());
        $metrics
            .with_label_values(&[$tag, "titan_max_sorted_runs"])
            .set($cf.titan.max_sorted_runs.into());
    }};
}

macro_rules! build_cf_opt {
    (
        $opt:ident,
        $cf_name:ident,
        $cache:expr,
        $compaction_limiter:expr,
        $region_info_provider:ident,
        $force_partition_range_mgr:expr,
    ) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        block_base_opts.set_block_cache($cache);
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts
            .set_pin_l0_filter_and_index_blocks_in_cache($opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            if let Some(level) = $opt.ribbon_filter_above_level {
                block_base_opts.set_ribbon_filter(
                    $opt.bloom_filter_bits_per_key as f64,
                    level as i32 - 1, // bloom_before_level
                );
            } else {
                block_base_opts.set_bloom_filter(
                    $opt.bloom_filter_bits_per_key as f64,
                    $opt.block_based_bloom_filter,
                );
            }
            block_base_opts.set_whole_key_filtering($opt.whole_key_filtering);
        }
        block_base_opts.set_read_amp_bytes_per_bit($opt.read_amp_bytes_per_bit);
        block_base_opts.set_prepopulate_block_cache($opt.prepopulate_block_cache);
        block_base_opts.set_format_version($opt.format_version.unwrap_or(2));
        block_base_opts.set_checksum($opt.checksum);
        block_base_opts.set_optimize_filters_for_memory($opt.optimize_filters_for_memory);
        let mut cf_opts = RocksCfOptions::default();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.set_num_levels($opt.num_levels);
        assert!($opt.compression_per_level.len() >= $opt.num_levels as usize);
        let compression_per_level = $opt.compression_per_level[..$opt.num_levels as usize].to_vec();
        cf_opts.compression_per_level(compression_per_level.as_slice());
        cf_opts.bottommost_compression($opt.bottommost_level_compression);
        // To set for bottommost level sst compression. The first 3 parameters refer to
        // the default value in `CompressionOptions` in
        // `rocksdb/include/rocksdb/advanced_options.h`.
        cf_opts.set_bottommost_level_compression_options(
            -14,   // window_bits
            32767, // level
            0,     // strategy
            $opt.bottommost_zstd_compression_dict_size,
            $opt.bottommost_zstd_compression_sample_size,
            1, // parallel_threads
        );
        cf_opts.set_write_buffer_size($opt.write_buffer_size.unwrap_or(ReadableSize::mb(32)).0);
        cf_opts.set_max_write_buffer_number($opt.max_write_buffer_number);
        cf_opts.set_min_write_buffer_number_to_merge($opt.min_write_buffer_number_to_merge);
        cf_opts.set_max_bytes_for_level_base($opt.max_bytes_for_level_base.0);
        cf_opts.set_target_file_size_base($opt.target_file_size_base());
        cf_opts.set_level_zero_file_num_compaction_trigger($opt.level0_file_num_compaction_trigger);
        cf_opts.set_level_zero_slowdown_writes_trigger($opt.level0_slowdown_writes_trigger);
        cf_opts.set_level_zero_stop_writes_trigger(
            $opt.level0_stop_writes_trigger.unwrap_or_default(),
        );
        cf_opts.set_max_compaction_bytes($opt.max_compaction_bytes.0);
        cf_opts.compaction_priority($opt.compaction_pri);
        cf_opts.set_level_compaction_dynamic_level_bytes($opt.dynamic_level_bytes);
        cf_opts.set_max_bytes_for_level_multiplier($opt.max_bytes_for_level_multiplier);
        cf_opts.set_compaction_style($opt.compaction_style);
        cf_opts.set_disable_auto_compactions($opt.disable_auto_compactions);
        cf_opts.set_disable_write_stall($opt.disable_write_stall);
        cf_opts.set_soft_pending_compaction_bytes_limit($opt.soft_pending_compaction_bytes_limit.0);
        cf_opts.set_hard_pending_compaction_bytes_limit(
            $opt.hard_pending_compaction_bytes_limit
                .unwrap_or_default()
                .0,
        );
        cf_opts.set_optimize_filters_for_hits($opt.optimize_filters_for_hits);
        cf_opts.set_force_consistency_checks($opt.force_consistency_checks);
        if $opt.enable_doubly_skiplist {
            cf_opts.set_doubly_skiplist();
        }
        if $opt.enable_compaction_guard.unwrap_or(false) {
            if let Some(provider) = $region_info_provider {
                let factory = CompactionGuardGeneratorFactory::new(
                    $cf_name,
                    provider.clone(),
                    $opt.compaction_guard_min_output_file_size.0,
                    $opt.max_compaction_bytes.0,
                    $force_partition_range_mgr,
                )
                .unwrap();
                cf_opts.set_sst_partitioner_factory(factory);
                cf_opts.set_target_file_size_base($opt.compaction_guard_max_output_file_size.0);
            } else {
                warn!("compaction guard is disabled due to region info provider not available")
            }
        }
        if let Some(r) = $compaction_limiter {
            cf_opts.set_compaction_thread_limiter(r);
        }
        cf_opts.set_ttl($opt.ttl.unwrap_or(ReadableDuration::secs(0)).0.as_secs());
        cf_opts.set_periodic_compaction_seconds(
            $opt.periodic_compaction_seconds
                .unwrap_or(ReadableDuration::secs(0))
                .0
                .as_secs(),
        );
        cf_opts
    }};
}

pub struct CfResources {
    pub cache: Cache,
    pub compaction_thread_limiters: HashMap<&'static str, ConcurrentTaskLimiter>,
    pub write_buffer_managers: HashMap<&'static str, Arc<WriteBufferManager>>,
    pub force_partition_range_mgr: ForcePartitionRangeManager,
}

cf_config!(DefaultCfConfig);

impl Default for DefaultCfConfig {
    fn default() -> DefaultCfConfig {
        DefaultCfConfig {
            block_size: ReadableSize::kb(32),
            block_cache_size: None,
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            optimize_filters_for_memory: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            // Ribbon filter causes memory surge during compaction,
            // so disable it by default.
            ribbon_filter_above_level: None,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: Some(ReadableSize::mb(128)),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: None,
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: None,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: None,
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: None,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            bottommost_level_compression: DBCompressionType::Zstd,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
            prepopulate_block_cache: PrepopulateBlockCache::Disabled,
            format_version: None,
            checksum: ChecksumType::CRC32c,
            max_compactions: None,
            ttl: None,
            periodic_compaction_seconds: None,
            titan: TitanCfConfig::default(),
            write_buffer_limit: None,
        }
    }
}

impl DefaultCfConfig {
    pub fn build_opt(
        &self,
        shared: &CfResources,
        region_info_accessor: Option<&RegionInfoAccessor>,
        api_version: ApiVersion,
        filter_factory: Option<&RangeCompactionFilterFactory>,
        for_engine: EngineType,
    ) -> RocksCfOptions {
        let mut cf_opts = build_cf_opt!(
            self,
            CF_DEFAULT,
            &shared.cache,
            shared.compaction_thread_limiters.get(CF_DEFAULT),
            region_info_accessor,
            shared.force_partition_range_mgr.clone(),
        );
        cf_opts.set_memtable_prefix_bloom_size_ratio(bloom_filter_ratio(for_engine));
        let f = RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        };
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        if let Some(factory) = filter_factory {
            match api_version {
                ApiVersion::V1 => {
                    cf_opts
                        .set_compaction_filter_factory("range_filter_factory", factory.clone())
                        .unwrap();
                }
                ApiVersion::V1ttl => {
                    cf_opts.add_table_properties_collector_factory(
                        "tikv.ttl-properties-collector",
                        TtlPropertiesCollectorFactory::<ApiV1Ttl>::default(),
                    );
                    let factory = StackingCompactionFilterFactory::new(
                        factory.clone(),
                        TtlCompactionFilterFactory::<ApiV1Ttl>::default(),
                    );
                    cf_opts
                        .set_compaction_filter_factory(
                            "range_filter_factory.ttl_compaction_filter_factory",
                            factory,
                        )
                        .unwrap();
                }
                ApiVersion::V2 => {
                    cf_opts.add_table_properties_collector_factory(
                        "tikv.rawkv-mvcc-properties-collector",
                        RawMvccPropertiesCollectorFactory::default(),
                    );
                    let factory = StackingCompactionFilterFactory::new(
                        factory.clone(),
                        RawCompactionFilterFactory,
                    );
                    cf_opts
                        .set_compaction_filter_factory(
                            "range_filter_factory.apiv2_gc_compaction_filter_factory",
                            factory,
                        )
                        .unwrap();
                }
            }
        } else {
            match api_version {
                ApiVersion::V1 => {
                    // nothing to do
                }
                ApiVersion::V1ttl => {
                    cf_opts.add_table_properties_collector_factory(
                        "tikv.ttl-properties-collector",
                        TtlPropertiesCollectorFactory::<ApiV1Ttl>::default(),
                    );
                    cf_opts
                        .set_compaction_filter_factory(
                            "ttl_compaction_filter_factory",
                            TtlCompactionFilterFactory::<ApiV1Ttl>::default(),
                        )
                        .unwrap();
                }
                ApiVersion::V2 => {
                    cf_opts.add_table_properties_collector_factory(
                        "tikv.rawkv-mvcc-properties-collector",
                        RawMvccPropertiesCollectorFactory::default(),
                    );
                    cf_opts
                        .set_compaction_filter_factory(
                            "apiv2_gc_compaction_filter_factory",
                            RawCompactionFilterFactory,
                        )
                        .unwrap();
                }
            }
        }
        cf_opts.set_titan_cf_options(&self.titan.build_opts(&shared.cache));
        if let Some(write_buffer_manager) = shared.write_buffer_managers.get(CF_DEFAULT) {
            cf_opts.set_write_buffer_manager(write_buffer_manager);
        }
        cf_opts
    }
}

cf_config!(WriteCfConfig);

impl Default for WriteCfConfig {
    fn default() -> WriteCfConfig {
        WriteCfConfig {
            block_size: ReadableSize::kb(32),
            block_cache_size: None,
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            optimize_filters_for_memory: false,
            whole_key_filtering: false,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            // Ribbon filter causes memory surge during compaction,
            // so disable it by default.
            ribbon_filter_above_level: None,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: Some(ReadableSize::mb(128)),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: None,
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: None,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: None,
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: None,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            bottommost_level_compression: DBCompressionType::Zstd,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
            prepopulate_block_cache: PrepopulateBlockCache::Disabled,
            format_version: None,
            checksum: ChecksumType::CRC32c,
            max_compactions: None,
            ttl: None,
            periodic_compaction_seconds: None,
            titan: TitanCfConfig::default_for_disabled(),
            write_buffer_limit: None,
        }
    }
}

impl WriteCfConfig {
    pub fn build_opt(
        &self,
        shared: &CfResources,
        region_info_accessor: Option<&RegionInfoAccessor>,
        filter_factory: Option<&RangeCompactionFilterFactory>,
        for_engine: EngineType,
    ) -> RocksCfOptions {
        let mut cf_opts = build_cf_opt!(
            self,
            CF_WRITE,
            &shared.cache,
            shared.compaction_thread_limiters.get(CF_WRITE),
            region_info_accessor,
            shared.force_partition_range_mgr.clone(),
        );
        // Prefix extractor(trim the timestamp at tail) for write cf.
        cf_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                FixedSuffixSliceTransform::new(8),
            )
            .unwrap();
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(bloom_filter_ratio(for_engine));
        // Collects user defined properties.
        cf_opts.add_table_properties_collector_factory(
            "tikv.mvcc-properties-collector",
            MvccPropertiesCollectorFactory::default(),
        );
        let f = RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        };
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        if let Some(factory) = filter_factory {
            let factory =
                StackingCompactionFilterFactory::new(factory.clone(), WriteCompactionFilterFactory);
            cf_opts
                .set_compaction_filter_factory(
                    "range_filter_factory.write_compaction_filter_factory",
                    factory,
                )
                .unwrap();
        } else {
            cf_opts
                .set_compaction_filter_factory(
                    "write_compaction_filter_factory",
                    WriteCompactionFilterFactory,
                )
                .unwrap();
        }
        cf_opts.set_titan_cf_options(&self.titan.build_opts(&shared.cache));
        if let Some(write_buffer_manager) = shared.write_buffer_managers.get(CF_WRITE) {
            cf_opts.set_write_buffer_manager(write_buffer_manager);
        }
        cf_opts
    }
}

cf_config!(LockCfConfig);

impl Default for LockCfConfig {
    fn default() -> LockCfConfig {
        LockCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: None,
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            optimize_filters_for_memory: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            // Ribbon filter causes memory surge during compaction,
            // so disable it by default.
            ribbon_filter_above_level: None,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: None,
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: None,
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: None,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: None,
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: None,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
            prepopulate_block_cache: PrepopulateBlockCache::Disabled,
            format_version: None,
            checksum: ChecksumType::CRC32c,
            max_compactions: None,
            ttl: None,
            periodic_compaction_seconds: None,
            titan: TitanCfConfig::default_for_disabled(),
            write_buffer_limit: None,
        }
    }
}

impl LockCfConfig {
    pub fn build_opt(
        &self,
        shared: &CfResources,
        filter_factory: Option<&RangeCompactionFilterFactory>,
        for_engine: EngineType,
    ) -> RocksCfOptions {
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut cf_opts = build_cf_opt!(
            self,
            CF_LOCK,
            &shared.cache,
            shared.compaction_thread_limiters.get(CF_LOCK),
            no_region_info_accessor,
            Default::default(),
        );
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", NoopSliceTransform)
            .unwrap();
        let f = RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        };
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        cf_opts.set_memtable_prefix_bloom_size_ratio(bloom_filter_ratio(for_engine));
        if let Some(factory) = filter_factory {
            cf_opts
                .set_compaction_filter_factory("range_filter_factory", factory.clone())
                .unwrap();
        }
        cf_opts.set_titan_cf_options(&self.titan.build_opts(&shared.cache));
        if let Some(write_buffer_manager) = shared.write_buffer_managers.get(CF_LOCK) {
            cf_opts.set_write_buffer_manager(write_buffer_manager);
        }
        cf_opts
    }
}

cf_config!(RaftCfConfig);

impl Default for RaftCfConfig {
    fn default() -> RaftCfConfig {
        RaftCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: None,
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            optimize_filters_for_memory: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            // Ribbon filter causes memory surge during compaction,
            // so disable it by default.
            ribbon_filter_above_level: None,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: Some(ReadableSize::mb(128)),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: None,
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: None,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: None,
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: None,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
            prepopulate_block_cache: PrepopulateBlockCache::Disabled,
            format_version: None,
            checksum: ChecksumType::CRC32c,
            max_compactions: None,
            ttl: None,
            periodic_compaction_seconds: None,
            titan: TitanCfConfig::default_for_disabled(),
            write_buffer_limit: None,
        }
    }
}

impl RaftCfConfig {
    pub fn build_opt(&self, shared: &CfResources) -> RocksCfOptions {
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut cf_opts = build_cf_opt!(
            self,
            CF_RAFT,
            &shared.cache,
            shared.compaction_thread_limiters.get(CF_RAFT),
            no_region_info_accessor,
            Default::default(),
        );
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", NoopSliceTransform)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts.set_titan_cf_options(&self.titan.build_opts(&shared.cache));
        cf_opts
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TitanDbConfig {
    pub enabled: Option<bool>,
    pub dirname: String,
    pub disable_gc: bool,
    pub max_background_gc: i32,
    // The value of this field will be truncated to seconds.
    pub purge_obsolete_files_period: ReadableDuration,
}

impl Default for TitanDbConfig {
    fn default() -> Self {
        Self {
            enabled: None, // Enabled only for newly created cluster
            dirname: "".to_owned(),
            disable_gc: false,
            max_background_gc: 1,
            purge_obsolete_files_period: ReadableDuration::secs(10),
        }
    }
}

impl TitanDbConfig {
    fn build_opts(&self) -> RocksTitanDbOptions {
        let mut opts = RocksTitanDbOptions::new();
        opts.set_dirname(&self.dirname);
        opts.set_disable_background_gc(self.disable_gc);
        opts.set_max_background_gc(self.max_background_gc);
        opts.set_purge_obsolete_files_period(self.purge_obsolete_files_period.as_secs() as usize);
        opts
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn write_into_metrics(&self) {
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["titan", "enabled"])
            .set((self.enabled.unwrap_or_default() as i32).into());

        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["titan", "disable_gc"])
            .set((self.disable_gc as i32).into());

        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["titan", "max_background_gc"])
            .set(self.max_background_gc.into());

        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["titan", "purge_obsolete_files_period_secs"])
            .set(self.purge_obsolete_files_period.0.as_secs_f64());
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    #[deprecated = "Rocksdb log is replaced by tikv log. Please use `log.file` to config log."]
    pub info_log_level: RocksLogLevel,
    #[serde(with = "rocks_config::recovery_mode_serde")]
    #[online_config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[online_config(skip)]
    pub wal_dir: String,
    #[online_config(skip)]
    pub wal_ttl_seconds: u64,
    #[online_config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: Option<ReadableSize>,
    pub max_background_jobs: i32,
    pub max_background_flushes: i32,
    #[online_config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[online_config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub enable_statistics: bool,
    #[online_config(skip)]
    pub stats_dump_period: Option<ReadableDuration>,
    pub compaction_readahead_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    #[deprecated = "Rocksdb log is replaced by tikv log. Please use `log.file` to config log."]
    pub info_log_max_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    #[deprecated = "Rocksdb log is replaced with tikv log. Please use `log.file` to config log."]
    pub info_log_roll_time: ReadableDuration,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    #[deprecated = "Rocksdb log is replaced with tikv log. Please use `log.file` to config log."]
    pub info_log_keep_log_file_num: u64,
    #[online_config(skip)]
    pub info_log_dir: String,
    pub rate_bytes_per_sec: ReadableSize,
    #[online_config(skip)]
    pub rate_limiter_refill_period: ReadableDuration,
    #[serde(with = "rocks_config::rate_limiter_mode_serde")]
    #[online_config(skip)]
    pub rate_limiter_mode: DBRateLimiterMode,
    #[online_config(hidden)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "The configuration has been removed. Use `rate_limiter_auto_tuned` instead"]
    pub auto_tuned: Option<bool>,
    pub rate_limiter_auto_tuned: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[online_config(skip)]
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    #[online_config(skip)]
    pub use_direct_io_for_flush_and_compaction: bool,
    #[online_config(skip)]
    pub enable_pipelined_write: bool,
    #[online_config(skip)]
    pub enable_multi_batch_write: Option<bool>,
    #[online_config(skip)]
    pub enable_unordered_write: bool,
    #[online_config(skip)]
    pub allow_concurrent_memtable_write: Option<bool>,
    pub write_buffer_limit: Option<ReadableSize>,
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub write_buffer_stall_ratio: f32,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub write_buffer_flush_oldest_first: bool,
    #[online_config(skip)]
    pub track_and_verify_wals_in_manifest: bool,
    // Dangerous option only for programming use.
    #[online_config(skip)]
    #[serde(skip)]
    pub paranoid_checks: Option<bool>,
    #[online_config(submodule)]
    pub defaultcf: DefaultCfConfig,
    #[online_config(submodule)]
    pub writecf: WriteCfConfig,
    #[online_config(submodule)]
    pub lockcf: LockCfConfig,
    #[online_config(submodule)]
    pub raftcf: RaftCfConfig,
    #[online_config(skip)]
    pub titan: TitanDbConfig,
}

#[derive(Clone)]
pub struct DbResources {
    // DB Options.
    pub env: Arc<Env>,
    pub statistics: Arc<RocksStatistics>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub write_buffer_manager: Option<Arc<WriteBufferManager>>,
}

impl Default for DbConfig {
    #[allow(deprecated)]
    fn default() -> DbConfig {
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: None,
            max_background_jobs: 0,
            max_background_flushes: 0,
            max_manifest_file_size: ReadableSize::mb(256),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: None,
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            info_log_level: RocksLogLevel::Info,
            rate_bytes_per_sec: ReadableSize::gb(10),
            rate_limiter_refill_period: ReadableDuration::millis(100),
            rate_limiter_mode: DBRateLimiterMode::WriteOnly,
            auto_tuned: None, // deprecated
            rate_limiter_auto_tuned: true,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_compactions: 0,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: false,
            enable_multi_batch_write: None, // deprecated
            enable_unordered_write: false,
            allow_concurrent_memtable_write: None,
            write_buffer_limit: None,
            write_buffer_stall_ratio: 0.0,
            write_buffer_flush_oldest_first: true,
            track_and_verify_wals_in_manifest: true,
            paranoid_checks: None,
            defaultcf: DefaultCfConfig::default(),
            writecf: WriteCfConfig::default(),
            lockcf: LockCfConfig::default(),
            raftcf: RaftCfConfig::default(),
            titan: TitanDbConfig::default(),
        }
    }
}

impl DbConfig {
    pub fn optimize_for(&mut self, storage_config: &StorageConfig) {
        match storage_config.engine {
            EngineType::RaftKv => {
                self.allow_concurrent_memtable_write.get_or_insert(true);
                self.max_total_wal_size.get_or_insert(ReadableSize::gb(4));
                self.stats_dump_period
                    .get_or_insert(ReadableDuration::minutes(10));
                self.defaultcf.enable_compaction_guard.get_or_insert(true);
                self.writecf.enable_compaction_guard.get_or_insert(true);
                if self.lockcf.write_buffer_size.is_none() {
                    self.lockcf.write_buffer_size = Some(ReadableSize::mb(32));
                }
            }
            EngineType::RaftKv2 => {
                self.enable_multi_batch_write.get_or_insert(false);
                self.allow_concurrent_memtable_write.get_or_insert(false);
                let total_mem = SysQuota::memory_limit_in_bytes() as f64;
                // purge-threshold is set to twice the limit. Too large limit will cause trouble
                // to raft log replay.
                self.write_buffer_limit.get_or_insert(ReadableSize(cmp::min(
                    (total_mem * WRITE_BUFFER_MEMORY_LIMIT_RATE) as u64,
                    WRITE_BUFFER_MEMORY_LIMIT_MAX,
                )));
                self.max_total_wal_size.get_or_insert(ReadableSize(1));
                self.stats_dump_period
                    .get_or_insert(ReadableDuration::minutes(0));
                // In RaftKv2, every region uses its own rocksdb instance, it's actually the
                // even stricter compaction guard, so use the same output file size base.
                self.writecf
                    .target_file_size_base
                    .get_or_insert(self.writecf.compaction_guard_max_output_file_size);
                self.defaultcf
                    .target_file_size_base
                    .get_or_insert(self.defaultcf.compaction_guard_max_output_file_size);
                self.defaultcf.disable_write_stall = true;
                self.writecf.disable_write_stall = true;
                self.lockcf.disable_write_stall = true;
                self.raftcf.disable_write_stall = true;
                // Initially only allow one compaction. Pace up when pending bytes is high. This
                // strategy is consistent with single RocksDB.
                self.defaultcf.max_compactions.get_or_insert(1);
                self.writecf.max_compactions.get_or_insert(1);
                self.lockcf
                    .write_buffer_size
                    .get_or_insert(ReadableSize::mb(32));
                self.lockcf
                    .write_buffer_limit
                    .get_or_insert(DEFAULT_LOCK_BUFFER_MEMORY_LIMIT);
            }
        }
        let bg_job_limits =
            get_background_job_limits(storage_config.engine, &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS);
        if self.max_background_jobs == 0 {
            self.max_background_jobs = bg_job_limits.max_background_jobs as i32;
        }
        if self.max_background_flushes == 0 {
            self.max_background_flushes = bg_job_limits.max_background_flushes as i32;
        }
        if self.max_sub_compactions == 0 {
            self.max_sub_compactions = bg_job_limits.max_sub_compactions;
        }
        if self.titan.max_background_gc == 0 {
            self.titan.max_background_gc = bg_job_limits.max_titan_background_gc as i32;
        }
    }

    pub fn build_resources(&self, env: Arc<Env>, engine: EngineType) -> DbResources {
        let rate_limiter = if self.rate_bytes_per_sec.0 > 0 {
            // for raft-v2, we use a longer window to make the compaction io smoother
            let (tune_per_secs, window_size, recent_size) = match engine {
                // 1s tune duration, long term window is 5m, short term window is 30s.
                // this is the default settings.
                EngineType::RaftKv => (1, 300, 30),
                // 5s tune duration, long term window is 1h, short term window is 5m
                EngineType::RaftKv2 => (5, 720, 60),
            };
            Some(Arc::new(RateLimiter::new_writeampbased_with_auto_tuned(
                self.rate_bytes_per_sec.0 as i64,
                (self.rate_limiter_refill_period.as_millis() * 1000) as i64,
                10, // fairness
                self.rate_limiter_mode,
                self.rate_limiter_auto_tuned,
                tune_per_secs,
                window_size,
                recent_size,
            )))
        } else {
            None
        };
        DbResources {
            env,
            statistics: Arc::new(RocksStatistics::new_titan()),
            rate_limiter,
            write_buffer_manager: self.write_buffer_limit.map(|limit| {
                Arc::new(WriteBufferManager::new(
                    limit.0 as usize,
                    self.write_buffer_stall_ratio,
                    self.write_buffer_flush_oldest_first,
                ))
            }),
        }
    }

    pub fn build_opt(&self, shared: &DbResources, for_engine: EngineType) -> RocksDbOptions {
        let mut opts = RocksDbOptions::default();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_total_wal_size(self.max_total_wal_size.unwrap_or(ReadableSize(0)).0);
        opts.set_max_background_jobs(self.max_background_jobs);
        // RocksDB will cap flush and compaction threads to at least one
        opts.set_max_background_flushes(self.max_background_flushes);
        opts.set_max_background_compactions(self.max_background_jobs - self.max_background_flushes);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.set_stats_dump_period_sec(
            self.stats_dump_period.unwrap_or_default().as_secs() as usize
        );
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_bytes_per_sync(self.bytes_per_sync.0);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0);
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        let mut enable_multi_batch_write =
            !self.enable_pipelined_write && !self.enable_unordered_write;
        if self.allow_concurrent_memtable_write == Some(false)
            && self.enable_multi_batch_write == Some(false)
        {
            enable_multi_batch_write = false
        }
        opts.enable_multi_batch_write(enable_multi_batch_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write.unwrap_or(true));
        if let Some(b) = self.paranoid_checks {
            opts.set_paranoid_checks(b);
        }
        if for_engine == EngineType::RaftKv {
            opts.set_info_log(RocksdbLogger);
        }
        if let Some(true) = self.titan.enabled {
            opts.set_titandb_options(&self.titan.build_opts());
        }
        opts.set_env(shared.env.clone());
        opts.set_statistics(&shared.statistics);
        if let Some(r) = &shared.rate_limiter {
            opts.set_rate_limiter(r);
        }
        if let Some(r) = &shared.write_buffer_manager {
            opts.set_write_buffer_manager(r);
        }
        if for_engine == EngineType::RaftKv2 {
            // Historical stats are not used.
            opts.set_stats_persist_period_sec(0);
        }
        opts.set_track_and_verify_wals_in_manifest(self.track_and_verify_wals_in_manifest);
        opts
    }

    pub fn build_cf_resources(
        &self,
        cache: Cache,
        force_partition_range_mgr: ForcePartitionRangeManager,
    ) -> CfResources {
        let mut compaction_thread_limiters = HashMap::new();
        if let Some(n) = self.defaultcf.max_compactions {
            if n > 0 {
                compaction_thread_limiters
                    .insert(CF_DEFAULT, ConcurrentTaskLimiter::new(CF_DEFAULT, n));
            }
        }
        if let Some(n) = self.writecf.max_compactions {
            if n > 0 {
                compaction_thread_limiters
                    .insert(CF_WRITE, ConcurrentTaskLimiter::new(CF_WRITE, n));
            }
        }
        if let Some(n) = self.lockcf.max_compactions {
            if n > 0 {
                compaction_thread_limiters.insert(CF_LOCK, ConcurrentTaskLimiter::new(CF_LOCK, n));
            }
        }
        if let Some(n) = self.raftcf.max_compactions {
            if n > 0 {
                compaction_thread_limiters.insert(CF_RAFT, ConcurrentTaskLimiter::new(CF_RAFT, n));
            }
        }
        let mut write_buffer_managers = HashMap::default();
        self.lockcf.write_buffer_limit.map(|limit| {
            write_buffer_managers.insert(
                CF_LOCK,
                Arc::new(WriteBufferManager::new(limit.0 as usize, 0f32, true)),
            )
        });
        self.defaultcf.write_buffer_limit.map(|limit| {
            write_buffer_managers.insert(
                CF_DEFAULT,
                Arc::new(WriteBufferManager::new(limit.0 as usize, 0f32, true)),
            )
        });
        self.writecf.write_buffer_limit.map(|limit| {
            write_buffer_managers.insert(
                CF_WRITE,
                Arc::new(WriteBufferManager::new(limit.0 as usize, 0f32, true)),
            )
        });
        CfResources {
            cache,
            compaction_thread_limiters,
            write_buffer_managers,
            force_partition_range_mgr,
        }
    }

    pub fn build_cf_opts(
        &self,
        shared: &CfResources,
        region_info_accessor: Option<&RegionInfoAccessor>,
        api_version: ApiVersion,
        filter_factory: Option<&RangeCompactionFilterFactory>,
        for_engine: EngineType,
    ) -> Vec<(&'static str, RocksCfOptions)> {
        let mut cf_opts = Vec::with_capacity(4);
        cf_opts.push((
            CF_DEFAULT,
            self.defaultcf.build_opt(
                shared,
                region_info_accessor,
                api_version,
                filter_factory,
                for_engine,
            ),
        ));
        cf_opts.push((
            CF_LOCK,
            self.lockcf.build_opt(shared, filter_factory, for_engine),
        ));
        cf_opts.push((
            CF_WRITE,
            self.writecf
                .build_opt(shared, region_info_accessor, filter_factory, for_engine),
        ));
        if for_engine == EngineType::RaftKv {
            cf_opts.push((CF_RAFT, self.raftcf.build_opt(shared)));
        }
        cf_opts
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcf.validate()?;
        self.lockcf.validate()?;
        self.writecf.validate()?;
        self.raftcf.validate()?;
        self.titan.validate()?;
        if self.raftcf.write_buffer_limit.is_some() {
            return Err("raftcf does not support cf based write buffer manager".into());
        }
        if self.writecf.titan.blob_run_mode != BlobRunMode::ReadOnly {
            return Err(
                "writecf does not support enabling Titan due to compaction filter incompatibility"
                    .into(),
            );
        }
        if self.enable_unordered_write {
            if let Some(true) = self.titan.enabled {
                return Err("RocksDB.unordered_write does not support Titan".into());
            }
            self.enable_pipelined_write = false;
        }

        // Since the following configuration supports online update, in order to
        // prevent mistakenly inputting too large values, the max limit is made
        // according to the cpu quota * 10. Notice 10 is only an estimate, not an
        // empirical value.
        let limit = (SysQuota::cpu_cores_quota() * 10.0) as i32;
        if self.max_background_jobs <= 0 || self.max_background_jobs > limit {
            return Err(format!(
                "max_background_jobs should be greater than 0 and less than or equal to {:?}",
                limit,
            )
            .into());
        }
        if self.max_background_flushes <= 0 || self.max_background_flushes > limit {
            return Err(format!(
                "max_background_flushes should be greater than 0 and less than or equal to {:?}",
                limit,
            )
            .into());
        }
        if !self.enable_statistics {
            warn!("kvdb: ignoring `enable_statistics`, statistics is always on.")
        }
        Ok(())
    }

    fn write_into_db_metrics(&self) {
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "wal_recovery_mode"])
            .set(self.wal_recovery_mode as u64 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "wal_ttl_seconds"])
            .set(self.wal_ttl_seconds as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "wal_size_limit"])
            .set(self.wal_size_limit.0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "max_total_wal_size"])
            .set(self.max_total_wal_size.unwrap_or_default().0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "max_background_jobs"])
            .set(self.max_background_jobs.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "max_background_flushes"])
            .set(self.max_background_flushes.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "max_manifest_file_size"])
            .set(self.max_manifest_file_size.0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "create_if_missing"])
            .set(self.create_if_missing.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "max_open_files"])
            .set(self.max_open_files.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "stats_dump_period"])
            .set(self.stats_dump_period.unwrap_or_default().as_secs_f64());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "compaction_readahead_size"])
            .set(self.compaction_readahead_size.0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "rate_bytes_per_sec"])
            .set(self.rate_bytes_per_sec.0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "rate_limiter_refill_period"])
            .set(self.rate_limiter_refill_period.as_secs_f64());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "rate_limiter_mode"])
            .set(self.rate_limiter_mode as u64 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "rate_limiter_auto_tuned"])
            .set(self.rate_limiter_auto_tuned.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "bytes_per_sync"])
            .set(self.bytes_per_sync.0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "wal_bytes_per_sync"])
            .set(self.wal_bytes_per_sync.0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "max_sub_compactions"])
            .set(self.max_sub_compactions.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "writable_file_max_buffer_size"])
            .set(self.writable_file_max_buffer_size.0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "use_direct_io_for_flush_and_compaction"])
            .set(self.use_direct_io_for_flush_and_compaction.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "enable_pipelined_write"])
            .set(self.enable_pipelined_write.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "enable_multi_batch_write"])
            .set(self.enable_multi_batch_write.unwrap_or_default().into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "enable_unordered_write"])
            .set(self.enable_unordered_write.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "allow_concurrent_memtable_write"])
            .set(
                self.allow_concurrent_memtable_write
                    .unwrap_or_default()
                    .into(),
            );
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "write_buffer_limit"])
            .set(self.write_buffer_limit.unwrap_or_default().0 as f64);
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "track_and_verify_wals_in_manifest"])
            .set(self.track_and_verify_wals_in_manifest.into());
        CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "paranoid_checks"])
            .set(self.paranoid_checks.unwrap_or_default().into());
    }
    fn write_into_metrics(&self) {
        self.write_into_db_metrics();
        write_into_cf_metrics!(self.defaultcf, CF_DEFAULT, CONFIG_ROCKSDB_CF_GAUGE);
        write_into_cf_metrics!(self.lockcf, CF_LOCK, CONFIG_ROCKSDB_CF_GAUGE);
        write_into_cf_metrics!(self.writecf, CF_WRITE, CONFIG_ROCKSDB_CF_GAUGE);
        write_into_cf_metrics!(self.raftcf, CF_RAFT, CONFIG_ROCKSDB_CF_GAUGE);
        self.titan.write_into_metrics();
    }
}

cf_config!(RaftDefaultCfConfig);

impl Default for RaftDefaultCfConfig {
    fn default() -> RaftDefaultCfConfig {
        RaftDefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: None,
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            optimize_filters_for_memory: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            // Ribbon filter causes memory surge during compaction,
            // so disable it by default.
            ribbon_filter_above_level: None,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: Some(ReadableSize::mb(128)),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: None,
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: None,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            disable_write_stall: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(192),
            hard_pending_compaction_bytes_limit: None,
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_compaction_guard: None,
            compaction_guard_min_output_file_size: ReadableSize::mb(8),
            compaction_guard_max_output_file_size: ReadableSize::mb(128),
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
            prepopulate_block_cache: PrepopulateBlockCache::Disabled,
            format_version: Some(2),
            checksum: ChecksumType::CRC32c,
            max_compactions: None,
            ttl: None,
            periodic_compaction_seconds: None,
            titan: TitanCfConfig::default(),
            write_buffer_limit: None,
        }
    }
}

impl RaftDefaultCfConfig {
    pub fn build_opt(&self, cache: &Cache) -> RocksCfOptions {
        let limiter = if let Some(n) = self.max_compactions {
            if n > 0 {
                Some(ConcurrentTaskLimiter::new(CF_DEFAULT, n))
            } else {
                None
            }
        } else {
            None
        };
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut cf_opts = build_cf_opt!(
            self,
            CF_DEFAULT,
            cache,
            limiter.as_ref(),
            no_region_info_accessor,
            Default::default(),
        );
        let f = FixedPrefixSliceTransform::new(region_raft_prefix_len());
        cf_opts
            .set_memtable_insert_hint_prefix_extractor("RaftPrefixSliceTransform", f)
            .unwrap();
        cf_opts.set_titan_cf_options(&self.titan.build_opts(cache));
        cf_opts
    }
}

// RocksDB Env associate thread pools of multiple instances from the same
// process. When construct Options, options.env is set to same singleton
// Env::Default() object. So total max_background_jobs =
// max(rocksdb.max_background_jobs, raftdb.max_background_jobs)
// But each instance will limit their background jobs according to their own
// max_background_jobs
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftDbConfig {
    #[serde(with = "rocks_config::recovery_mode_serde")]
    #[online_config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[online_config(skip)]
    pub wal_dir: String,
    #[online_config(skip)]
    pub wal_ttl_seconds: u64,
    #[online_config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_background_flushes: i32,
    #[online_config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[online_config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub enable_statistics: bool,
    #[online_config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    #[deprecated = "Rocksdb log is replaced by tikv log. Please use `log.file` to config log."]
    pub info_log_max_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    #[deprecated = "Rocksdb log is replaced by tikv log. Please use `log.file` to config log."]
    pub info_log_roll_time: ReadableDuration,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    #[deprecated = "Rocksdb log is replaced by tikv log. Please use `log.file` to config log."]
    pub info_log_keep_log_file_num: u64,
    #[online_config(skip)]
    pub info_log_dir: String,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    #[deprecated = "Rocksdb log is replaced by tikv log. Please use `log.file` to config log."]
    pub info_log_level: RocksLogLevel,
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    #[online_config(skip)]
    pub use_direct_io_for_flush_and_compaction: bool,
    #[online_config(skip)]
    pub enable_pipelined_write: bool,
    #[online_config(skip)]
    pub enable_unordered_write: bool,
    #[online_config(skip)]
    pub allow_concurrent_memtable_write: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[online_config(submodule)]
    pub defaultcf: RaftDefaultCfConfig,
    #[online_config(skip)]
    pub titan: TitanDbConfig,
}

impl Default for RaftDbConfig {
    fn default() -> RaftDbConfig {
        // raftdb should only be used for raftkv
        let bg_job_limits =
            get_background_job_limits(EngineType::RaftKv, &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS);
        let titan_config = TitanDbConfig {
            enabled: Some(false),
            max_background_gc: bg_job_limits.max_titan_background_gc as i32,
            ..Default::default()
        };
        #[allow(deprecated)]
        RaftDbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: bg_job_limits.max_background_jobs as i32,
            max_background_flushes: bg_job_limits.max_background_flushes as i32,
            max_manifest_file_size: ReadableSize::mb(20),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            info_log_level: RocksLogLevel::Info,
            max_sub_compactions: bg_job_limits.max_sub_compactions,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            enable_unordered_write: false,
            allow_concurrent_memtable_write: true,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            defaultcf: RaftDefaultCfConfig::default(),
            titan: titan_config,
        }
    }
}

impl RaftDbConfig {
    pub fn build_opt(&self, env: Arc<Env>, statistics: Option<&RocksStatistics>) -> RocksDbOptions {
        let mut opts = RocksDbOptions::default();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_background_flushes(self.max_background_flushes);
        opts.set_max_background_compactions(self.max_background_jobs - self.max_background_flushes);
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        match statistics {
            Some(s) => opts.set_statistics(s),
            None => opts.set_statistics(&RocksStatistics::new_titan()),
        }
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_info_log(RaftDbLogger);
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write);
        opts.add_event_listener(RocksEventListener::new("raft", None));
        opts.set_bytes_per_sync(self.bytes_per_sync.0);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0);
        // TODO maybe create a new env for raft engine
        if let Some(true) = self.titan.enabled {
            opts.set_titandb_options(&self.titan.build_opts());
        }
        opts.set_env(env);
        opts
    }

    pub fn build_cf_opts(&self, cache: &Cache) -> Vec<(&'static str, RocksCfOptions)> {
        vec![(CF_DEFAULT, self.defaultcf.build_opt(cache))]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcf.validate()?;
        if self.enable_unordered_write {
            if let Some(true) = self.titan.enabled {
                return Err("raftdb: unordered_write is not compatible with Titan".into());
            }
            if self.enable_pipelined_write {
                return Err(
                    "raftdb: pipelined_write is not compatible with unordered_write".into(),
                );
            }
        }
        if !self.enable_statistics {
            warn!("raftdb: ignoring `enable_statistics`, statistics is always on.")
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(default, rename_all = "kebab-case")]
pub struct RaftEngineConfig {
    pub enable: bool,
    #[serde(flatten)]
    config: RawRaftEngineConfig,
}

impl Default for RaftEngineConfig {
    fn default() -> Self {
        Self {
            enable: true,
            config: RawRaftEngineConfig::default(),
        }
    }
}

impl RaftEngineConfig {
    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.config.sanitize().map_err(Box::new)?;
        if self.config.memory_limit.is_none() {
            let total_mem = SysQuota::memory_limit_in_bytes() as f64;
            let memory_limit = total_mem * RAFT_ENGINE_MEMORY_LIMIT_RATE;
            self.config.memory_limit = Some(RaftEngineReadableSize(memory_limit as u64));
        }
        Ok(())
    }

    fn optimize_for(&mut self, raft_store: &RaftstoreConfig, raft_kv_v2: bool) {
        if raft_kv_v2 {
            return;
        }
        let default_config = RawRaftEngineConfig::default();
        let cur_batch_compression_thd = self.config().batch_compression_threshold;
        // Currently, it only takes whether the configuration
        // batch-compression-threshold of RaftEngine are set manually
        // into consideration to determine whether the RaftEngine is customized.
        let customized = cur_batch_compression_thd != default_config.batch_compression_threshold;
        // As the async-io is enabled by default (raftstore.store_io_pool_size == 1),
        // testing records shows that using 4kb as the default value can achieve
        // better performance and reduce the IO overhead.
        // Meanwhile, the batch_compression_threshold cannot be modified dynamically if
        // the threads count of async-io are changed manually.
        if !customized && raft_store.store_io_pool_size > 0 {
            let adaptive_batch_comp_thd = RaftEngineReadableSize(std::cmp::max(
                cur_batch_compression_thd.0 / (raft_store.store_io_pool_size + 1) as u64,
                RaftEngineReadableSize::kb(4).0,
            ));
            self.mut_config().batch_compression_threshold = adaptive_batch_comp_thd;
            warn!(
                "raft-engine.batch-compression-threshold {} should be adpative to the size of async-io. Set it to {} instead.",
                cur_batch_compression_thd, adaptive_batch_comp_thd,
            );
        }
    }

    pub fn config(&self) -> RawRaftEngineConfig {
        self.config.clone()
    }

    pub fn mut_config(&mut self) -> &mut RawRaftEngineConfig {
        &mut self.config
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DbType {
    Kv,
    Raft,
}

pub struct DbConfigManger<D> {
    cfg: DbConfig,
    db: D,
    db_type: DbType,
}

impl<D> DbConfigManger<D> {
    pub fn new(cfg: DbConfig, db: D, db_type: DbType) -> Self {
        DbConfigManger { cfg, db, db_type }
    }
}

impl<D: ConfigurableDb> DbConfigManger<D> {
    fn set_cf_config(&self, cf: &str, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.validate_cf(cf)?;
        self.db.set_cf_config(cf, opts)?;

        // Write config to metric
        for (cfg_name, cfg_value) in opts {
            let cfg_value = match cfg_value {
                v if *v == "true" => Ok(1f64),
                v if *v == "false" => Ok(0f64),
                v => v.parse::<f64>(),
            };
            if let Ok(v) = cfg_value {
                CONFIG_ROCKSDB_CF_GAUGE
                    .with_label_values(&[cf, cfg_name])
                    .set(v);
            }
        }
        Ok(())
    }

    fn validate_cf(&self, cf: &str) -> Result<(), Box<dyn Error>> {
        match (self.db_type, cf) {
            (DbType::Kv, CF_DEFAULT)
            | (DbType::Kv, CF_WRITE)
            | (DbType::Kv, CF_LOCK)
            | (DbType::Kv, CF_RAFT)
            | (DbType::Raft, CF_DEFAULT) => Ok(()),
            _ => Err(format!("invalid cf {:?} for db {:?}", cf, self.db_type).into()),
        }
    }

    fn update_background_cfg(
        &self,
        max_background_jobs: i32,
        max_background_flushes: i32,
    ) -> Result<(), Box<dyn Error>> {
        assert!(max_background_jobs > 0 && max_background_flushes > 0);
        let max_background_compacts =
            std::cmp::max(max_background_jobs - max_background_flushes, 1);
        self.db
            .set_db_config(&[("max_background_jobs", &max_background_jobs.to_string())])?;
        self.db.set_db_config(&[(
            "max_background_flushes",
            &max_background_flushes.to_string(),
        )])?;
        self.db.set_db_config(&[(
            "max_background_compactions",
            &max_background_compacts.to_string(),
        )])
    }
}

impl<T: ConfigurableDb + Send + Sync> ConfigManager for DbConfigManger<T> {
    fn dispatch(&mut self, change: ConfigChange) -> Result<(), Box<dyn Error>> {
        self.cfg.update(change.clone())?;
        let change_str = format!("{:?}", change);
        let mut change: Vec<(String, ConfigValue)> = change.into_iter().collect();
        let cf_config = change.extract_if(.., |(name, _)| name.ends_with("cf"));
        for (cf_name, cf_change) in cf_config {
            if let ConfigValue::Module(mut cf_change) = cf_change {
                // defaultcf -> default
                let cf_name = &cf_name[..(cf_name.len() - 2)];
                if cf_change.remove("block_cache_size").is_some() {
                    // currently we can't modify block_cache_size via set_options_cf
                    return Err("shared block cache is enabled, change cache size through \
                block-cache.capacity in storage module instead"
                        .into());
                }
                if let Some(ConfigValue::Module(titan_change)) = cf_change.remove("titan") {
                    for (name, value) in titan_change {
                        cf_change.insert(name, value);
                    }
                }
                if let Some(f) = cf_change.remove("write_buffer_limit") {
                    if cf_name != CF_LOCK {
                        return Err(
                            "cf write buffer manager is only supportted for lock cf now".into()
                        );
                    }
                    let size: ReadableSize = f.into();
                    self.db.set_cf_flush_size(cf_name, size.0 as usize)?;
                }
                if !cf_change.is_empty() {
                    let cf_change = config_value_to_string(cf_change.into_iter().collect());
                    let cf_change_slice = config_to_slice(&cf_change);
                    self.set_cf_config(cf_name, &cf_change_slice)?;
                }
            }
        }

        if let Some(rate_bytes_config) = change
            .extract_if(.., |(name, _)| name == "rate_bytes_per_sec")
            .next()
        {
            let rate_bytes_per_sec: ReadableSize = rate_bytes_config.1.into();
            self.db
                .set_rate_bytes_per_sec(rate_bytes_per_sec.0 as i64)?;
        }

        if let Some(rate_bytes_config) = change
            .extract_if(.., |(name, _)| name == "rate_limiter_auto_tuned")
            .next()
        {
            let rate_limiter_auto_tuned: bool = rate_bytes_config.1.into();
            self.db
                .set_rate_limiter_auto_tuned(rate_limiter_auto_tuned)?;
        }

        if let Some(size) = change
            .extract_if(.., |(name, _)| name == "write_buffer_limit")
            .next()
        {
            let size: ReadableSize = size.1.into();
            self.db.set_flush_size(size.0 as usize)?;
        }

        if let Some(f) = change
            .extract_if(.., |(name, _)| name == "write_buffer_flush_oldest_first")
            .next()
        {
            self.db.set_flush_oldest_first(f.1.into())?;
        }

        if let Some(background_jobs_config) = change
            .extract_if(.., |(name, _)| name == "max_background_jobs")
            .next()
        {
            let max_background_jobs: i32 = background_jobs_config.1.into();
            self.update_background_cfg(max_background_jobs, self.cfg.max_background_flushes)?;
        }

        if let Some(background_subcompactions_config) = change
            .extract_if(.., |(name, _)| name == "max_sub_compactions")
            .next()
        {
            let max_subcompactions: u32 = background_subcompactions_config.1.into();
            self.db
                .set_db_config(&[("max_subcompactions", &max_subcompactions.to_string())])?;
        }

        if let Some(background_flushes_config) = change
            .extract_if(.., |(name, _)| name == "max_background_flushes")
            .next()
        {
            let max_background_flushes: i32 = background_flushes_config.1.into();
            self.update_background_cfg(self.cfg.max_background_jobs, max_background_flushes)?;
        }

        if !change.is_empty() {
            let change = config_value_to_string(change);
            let change_slice = config_to_slice(&change);
            self.db.set_db_config(&change_slice)?;
        }
        info!(
            "rocksdb config changed";
            "db" => ?self.db_type,
            "change" => change_str
        );
        Ok(())
    }
}

fn config_to_slice(config_change: &[(String, String)]) -> Vec<(&str, &str)> {
    config_change
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect()
}

// Convert `ConfigValue` to formatted String that can pass to
// `DB::set_db_options`
fn config_value_to_string(config_change: Vec<(String, ConfigValue)>) -> Vec<(String, String)> {
    config_change
        .into_iter()
        .filter_map(|(name, value)| {
            let v = match value {
                d @ ConfigValue::Duration(_) => {
                    let d: ReadableDuration = d.into();
                    Some(d.as_secs().to_string())
                }
                s @ ConfigValue::Size(_) => {
                    let s: ReadableSize = s.into();
                    Some(s.0.to_string())
                }
                ConfigValue::Module(_) => unreachable!(),
                v => Some(format!("{}", v)),
            };
            v.map(|v| (name, v))
        })
        .collect()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MetricConfig {
    pub job: String,

    // Push is deprecated.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub interval: ReadableDuration,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub address: String,
}

impl Default for MetricConfig {
    fn default() -> MetricConfig {
        MetricConfig {
            interval: ReadableDuration::secs(15),
            address: "".to_owned(),
            job: "tikv".to_owned(),
        }
    }
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct UnifiedReadPoolConfig {
    #[online_config(skip)]
    pub min_thread_count: usize,
    pub max_thread_count: usize,
    #[online_config(skip)]
    pub stack_size: ReadableSize,
    pub max_tasks_per_worker: usize,
    pub auto_adjust_pool_size: bool,
    /// Maximum CPU usage percentage for the unified read pool (0.0-1.0).
    /// When set to 0, uses the busy thread scaling algorithm only.
    /// When > 0, CPU threshold constraints ON TOP OF the busy thread scaling
    /// algorithm:
    /// - Forces scale down when CPU exceeds threshold + 10% leeway
    /// - Prevents scale up when it would exceed threshold - 10% leeway Example:
    ///   0.8 means read pool should not use more than 80% of available CPU
    ///   cores.
    pub cpu_threshold: f64,
    // FIXME: Add more configs when they are effective in yatp
}

impl UnifiedReadPoolConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.min_thread_count == 0 {
            return Err("readpool.unified.min-thread-count should be > 0".into());
        }
        if self.max_thread_count < self.min_thread_count {
            return Err(
                "readpool.unified.max-thread-count should be >= readpool.unified.min-thread-count"
                    .into(),
            );
        }
        let limit = cmp::max(
            UNIFIED_READPOOL_MIN_CONCURRENCY,
            SysQuota::cpu_cores_quota() as usize * 10, // at most 10 threads per core
        );
        if self.max_thread_count > limit {
            return Err(format!(
                "readpool.unified.max-thread-count should be smaller than {}",
                limit
            )
            .into());
        }
        if self.stack_size.0 < ReadableSize::mb(2).0 {
            return Err("readpool.unified.stack-size should be >= 2mb".into());
        }
        if self.max_tasks_per_worker <= 1 {
            return Err("readpool.unified.max-tasks-per-worker should be > 1".into());
        }
        if self.cpu_threshold < 0.0 || self.cpu_threshold > 1.0 {
            return Err("readpool.unified.cpu-threshold should be between 0.0 and 1.0".into());
        }
        Ok(())
    }
}

pub const UNIFIED_READPOOL_MIN_CONCURRENCY: usize = 4;

// FIXME: Use macros to generate it if yatp is used elsewhere besides readpool.
impl Default for UnifiedReadPoolConfig {
    fn default() -> UnifiedReadPoolConfig {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.8) as usize;
        concurrency = cmp::max(UNIFIED_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            min_thread_count: 1,
            max_thread_count: concurrency,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
            max_tasks_per_worker: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            auto_adjust_pool_size: false,
            cpu_threshold: 0.0, // 0 means no threshold (disabled)
        }
    }
}

#[cfg(test)]
mod unified_read_pool_tests {
    use super::*;

    #[test]
    fn test_validate() {
        let cfg = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            stack_size: ReadableSize::mb(2),
            max_tasks_per_worker: 2000,
            auto_adjust_pool_size: false,
            cpu_threshold: 0.0,
        };
        cfg.validate().unwrap();
        let cfg = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: cmp::max(
                UNIFIED_READPOOL_MIN_CONCURRENCY,
                SysQuota::cpu_cores_quota() as usize,
            ),
            ..cfg
        };
        cfg.validate().unwrap();

        let invalid_cfg = UnifiedReadPoolConfig {
            min_thread_count: 0,
            ..cfg
        };
        invalid_cfg.validate().unwrap_err();

        let invalid_cfg = UnifiedReadPoolConfig {
            min_thread_count: 2,
            max_thread_count: 1,
            ..cfg
        };
        invalid_cfg.validate().unwrap_err();

        let invalid_cfg = UnifiedReadPoolConfig {
            stack_size: ReadableSize::mb(1),
            ..cfg
        };
        invalid_cfg.validate().unwrap_err();

        let invalid_cfg = UnifiedReadPoolConfig {
            max_tasks_per_worker: 1,
            ..cfg
        };
        invalid_cfg.validate().unwrap_err();
        let invalid_cfg = UnifiedReadPoolConfig {
            max_thread_count: SysQuota::cpu_cores_quota() as usize * 10 + 1,
            ..cfg
        };
        invalid_cfg.validate().unwrap_err();
    }
}

macro_rules! readpool_config {
    ($struct_name:ident, $test_mod_name:ident, $display_name:expr) => {
        #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $struct_name {
            pub use_unified_pool: Option<bool>,
            pub high_concurrency: usize,
            pub normal_concurrency: usize,
            pub low_concurrency: usize,
            pub max_tasks_per_worker_high: usize,
            pub max_tasks_per_worker_normal: usize,
            pub max_tasks_per_worker_low: usize,
            pub stack_size: ReadableSize,
        }

        impl $struct_name {
            /// Builds configurations for low, normal and high priority pools.
            pub fn to_yatp_pool_configs(self) -> Vec<yatp_pool::Config> {
                vec![
                    yatp_pool::Config {
                        workers: self.low_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_low,
                        stack_size: self.stack_size.0 as usize,
                    },
                    yatp_pool::Config {
                        workers: self.normal_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_normal,
                        stack_size: self.stack_size.0 as usize,
                    },
                    yatp_pool::Config {
                        workers: self.high_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_high,
                        stack_size: self.stack_size.0 as usize,
                    },
                ]
            }

            pub fn default_for_test() -> Self {
                Self {
                    use_unified_pool: None,
                    high_concurrency: 2,
                    normal_concurrency: 2,
                    low_concurrency: 2,
                    max_tasks_per_worker_high: 2000,
                    max_tasks_per_worker_normal: 2000,
                    max_tasks_per_worker_low: 2000,
                    stack_size: ReadableSize::mb(1),
                }
            }

            pub fn use_unified_pool(&self) -> bool {
                // The unified pool is used by default unless the corresponding module has
                // customized configurations.
                self.use_unified_pool
                    .unwrap_or_else(|| *self == Default::default())
            }

            pub fn adjust_use_unified_pool(&mut self) {
                if self.use_unified_pool.is_none() {
                    // The unified pool is used by default unless the corresponding module has customized configurations.
                    if *self == Default::default() {
                        info!("readpool.{}.use-unified-pool is not set, set to true by default", $display_name);
                        self.use_unified_pool = Some(true);
                    } else {
                        info!("readpool.{}.use-unified-pool is not set, set to false because there are other customized configurations", $display_name);
                        self.use_unified_pool = Some(false);
                    }
                }
            }

            pub fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.use_unified_pool() {
                    return Ok(());
                }
                if self.high_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.high-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.normal_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.normal-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.low_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.low-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.stack_size.0 < ReadableSize::mb(MIN_READPOOL_STACK_SIZE_MB).0 {
                    return Err(format!(
                        "readpool.{}.stack-size should be >= {}mb",
                        $display_name, MIN_READPOOL_STACK_SIZE_MB
                    )
                    .into());
                }
                if self.max_tasks_per_worker_high <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-high should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_normal <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-normal should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_low <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-low should be > 1",
                        $display_name
                    )
                    .into());
                }

                Ok(())
            }
        }

        #[cfg(test)]
        mod $test_mod_name {
            use super::*;

            #[allow(unused_assignments)]
            #[test]
            fn test_validate() {
                let cfg = $struct_name::default();
                cfg.validate().unwrap();

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.high_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.normal_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.low_concurrency = 0;
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.stack_size = ReadableSize::mb(1);
                assert!(invalid_cfg.validate().is_err());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_high = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_high = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_high = 100;
                cfg.validate().unwrap();

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_normal = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_normal = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_normal = 100;
                cfg.validate().unwrap();

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_low = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_low = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_low = 100;
                cfg.validate().unwrap();

                let mut invalid_but_unified = cfg.clone();
                invalid_but_unified.use_unified_pool = Some(true);
                invalid_but_unified.low_concurrency = 0;
                invalid_but_unified.validate().unwrap();
            }
        }
    };
}

const DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY: usize = 4;
const DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY: usize = 8;

// Assume a request can be finished in 1ms, a request at position x will wait
// about 0.001 * x secs to be actual started. A server-is-busy error will
// trigger 2 seconds backoff. So when it needs to wait for more than 2 seconds,
// return error won't causse larger latency.
const DEFAULT_READPOOL_MAX_TASKS_PER_WORKER: usize = 2 * 1000;

const MIN_READPOOL_STACK_SIZE_MB: u64 = 2;
const DEFAULT_READPOOL_STACK_SIZE_MB: u64 = 10;

readpool_config!(StorageReadPoolConfig, storage_read_pool_test, "storage");

impl Default for StorageReadPoolConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.5) as usize;
        concurrency = cmp::max(DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY, concurrency);
        concurrency = cmp::min(DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY, concurrency);
        Self {
            use_unified_pool: None,
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

const DEFAULT_COPROCESSOR_READPOOL_MIN_CONCURRENCY: usize = 2;

readpool_config!(
    CoprReadPoolConfig,
    coprocessor_read_pool_test,
    "coprocessor"
);

impl Default for CoprReadPoolConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.8) as usize;
        concurrency = cmp::max(DEFAULT_COPROCESSOR_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            use_unified_pool: None,
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadPoolConfig {
    #[online_config(submodule)]
    pub unified: UnifiedReadPoolConfig,
    #[online_config(skip)]
    pub storage: StorageReadPoolConfig,
    #[online_config(skip)]
    pub coprocessor: CoprReadPoolConfig,
}

impl ReadPoolConfig {
    pub fn is_unified_pool_enabled(&self) -> bool {
        self.storage.use_unified_pool() || self.coprocessor.use_unified_pool()
    }

    pub fn adjust_use_unified_pool(&mut self) {
        self.storage.adjust_use_unified_pool();
        self.coprocessor.adjust_use_unified_pool();
    }

    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.is_unified_pool_enabled() {
            self.unified.validate()?;
        }
        self.storage.validate()?;
        self.coprocessor.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod readpool_tests {
    use super::*;

    #[test]
    fn test_unified_disabled() {
        // Allow invalid yatp config when yatp is not used.
        let unified = UnifiedReadPoolConfig {
            min_thread_count: 0,
            max_thread_count: 0,
            stack_size: ReadableSize::mb(0),
            max_tasks_per_worker: 0,
            auto_adjust_pool_size: false,
            cpu_threshold: 0.0,
        };
        unified.validate().unwrap_err();
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        storage.validate().unwrap();
        let coprocessor = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        coprocessor.validate().unwrap();
        let cfg = ReadPoolConfig {
            unified,
            storage,
            coprocessor,
        };
        assert!(!cfg.is_unified_pool_enabled());
        cfg.validate().unwrap();

        // Storage and coprocessor config must be valid when yatp is not used.
        let unified = UnifiedReadPoolConfig::default();
        unified.validate().unwrap();
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            high_concurrency: 0,
            ..Default::default()
        };
        storage.validate().unwrap_err();
        let coprocessor = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        let invalid_cfg = ReadPoolConfig {
            unified,
            storage,
            coprocessor,
        };
        assert!(!invalid_cfg.is_unified_pool_enabled());
        invalid_cfg.validate().unwrap_err();
    }

    #[test]
    fn test_unified_enabled() {
        // Yatp config must be valid when yatp is used.
        let unified = UnifiedReadPoolConfig {
            min_thread_count: 0,
            max_thread_count: 0,
            ..Default::default()
        };
        unified.validate().unwrap_err();
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        storage.validate().unwrap();
        let coprocessor = CoprReadPoolConfig::default();
        coprocessor.validate().unwrap();
        let mut cfg = ReadPoolConfig {
            unified,
            storage,
            coprocessor,
        };
        cfg.adjust_use_unified_pool();
        assert!(cfg.is_unified_pool_enabled());
        cfg.validate().unwrap_err();
    }

    #[test]
    fn test_is_unified() {
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(!storage.use_unified_pool());
        let coprocessor = CoprReadPoolConfig::default();
        assert!(coprocessor.use_unified_pool());

        let mut cfg = ReadPoolConfig {
            storage,
            coprocessor,
            ..Default::default()
        };
        assert!(cfg.is_unified_pool_enabled());

        cfg.storage.use_unified_pool = Some(false);
        cfg.coprocessor.use_unified_pool = Some(false);
        assert!(!cfg.is_unified_pool_enabled());
    }

    #[test]
    fn test_partially_unified() {
        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            low_concurrency: 0,
            ..Default::default()
        };
        assert!(!storage.use_unified_pool());
        let coprocessor = CoprReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(coprocessor.use_unified_pool());
        let mut cfg = ReadPoolConfig {
            storage,
            coprocessor,
            ..Default::default()
        };
        assert!(cfg.is_unified_pool_enabled());
        cfg.validate().unwrap_err();
        cfg.storage.low_concurrency = 1;
        cfg.validate().unwrap();

        let storage = StorageReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(storage.use_unified_pool());
        let coprocessor = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            low_concurrency: 0,
            ..Default::default()
        };
        assert!(!coprocessor.use_unified_pool());
        let mut cfg = ReadPoolConfig {
            storage,
            coprocessor,
            ..Default::default()
        };
        assert!(cfg.is_unified_pool_enabled());
        cfg.validate().unwrap_err();
        cfg.coprocessor.low_concurrency = 1;
        cfg.validate().unwrap();
    }
}

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct HadoopConfig {
    pub home: String,
    pub linux_user: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BackupConfig {
    pub num_threads: usize,
    pub batch_size: usize,
    pub sst_max_size: ReadableSize,
    pub enable_auto_tune: bool,
    pub auto_tune_remain_threads: usize,
    pub auto_tune_refresh_interval: ReadableDuration,
    pub io_thread_size: usize,
    // Do not expose this config to user.
    // It used to debug s3 503 error.
    pub s3_multi_part_size: ReadableSize,
    #[online_config(submodule)]
    pub hadoop: HadoopConfig,
}

impl BackupConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        let limit = SysQuota::cpu_cores_quota() as usize;
        let default_cfg = BackupConfig::default();
        if self.num_threads == 0 || self.num_threads > limit {
            warn!(
                "backup.num_threads cannot be 0 or larger than {}, change it to {}",
                limit, default_cfg.num_threads
            );
            self.num_threads = default_cfg.num_threads;
        }
        if self.batch_size == 0 {
            warn!(
                "backup.batch_size cannot be 0, change it to {}",
                default_cfg.batch_size
            );
            self.batch_size = default_cfg.batch_size;
        }
        if self.s3_multi_part_size.0 > ReadableSize::gb(5).0 {
            warn!(
                "backup.s3_multi_part_size cannot larger than 5GB, change it to {:?}",
                default_cfg.s3_multi_part_size
            );
            self.s3_multi_part_size = default_cfg.s3_multi_part_size;
        }

        Ok(())
    }
}

impl Default for BackupConfig {
    fn default() -> Self {
        let default_coprocessor = CopConfig::default();
        let cpu_num = SysQuota::cpu_cores_quota();
        Self {
            // use at most 50% of vCPU by default
            num_threads: (cpu_num * 0.5).clamp(1.0, 8.0) as usize,
            batch_size: 8,
            sst_max_size: default_coprocessor.region_max_size(),
            enable_auto_tune: true,
            auto_tune_remain_threads: (cpu_num * 0.2).round() as usize,
            auto_tune_refresh_interval: ReadableDuration::secs(60),
            io_thread_size: 2,
            // 5MB is the minimum part size that S3 allowed.
            s3_multi_part_size: ReadableSize::mb(5),
            hadoop: Default::default(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BackupStreamConfig {
    #[online_config(skip)]
    pub min_ts_interval: ReadableDuration,

    pub max_flush_interval: ReadableDuration,
    #[online_config(skip)]
    pub num_threads: usize,
    #[online_config(skip)]
    pub enable: bool,
    #[online_config(skip)]
    pub temp_path: String,

    pub file_size_limit: ReadableSize,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    // Let's hide this config for now.
    pub temp_file_memory_quota: ReadableSize,

    #[online_config(skip)]
    pub initial_scan_pending_memory_quota: ReadableSize,
    #[online_config(skip)]
    pub initial_scan_rate_limit: ReadableSize,
    pub initial_scan_concurrency: usize,
    pub s3_multi_part_size: ReadableSize,
}

impl BackupStreamConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        let limit = SysQuota::cpu_cores_quota() as usize;
        let default_cfg = BackupStreamConfig::default();
        if self.num_threads == 0 || self.num_threads > limit {
            warn!(
                "log_backup.num_threads cannot be 0 or larger than {}, change it to {}",
                limit, default_cfg.num_threads
            );
            self.num_threads = default_cfg.num_threads;
        }
        if self.max_flush_interval < ReadableDuration::secs(10) {
            return Err(format!(
                "the max_flush_interval is too small, it is {}, and should be greater than 10s.",
                self.max_flush_interval
            )
            .into());
        }
        if self.min_ts_interval < ReadableDuration::secs(1) {
            return Err(format!(
                "the min_ts_interval is too small, it is {}, and should be greater than 1s.",
                self.min_ts_interval
            )
            .into());
        }
        if self.initial_scan_concurrency == 0 {
            return Err("the `initial_scan_concurrency` shouldn't be zero".into());
        }
        if self.initial_scan_rate_limit.0 < 1024 {
            return Err("the `initial_scan_rate_limit` should be at least 1024 bytes".into());
        }
        if self.s3_multi_part_size.0 > ReadableSize::gb(5).0 {
            warn!(
                "backup.s3_multi_part_size cannot larger than 5GB, change it to {:?}",
                default_cfg.s3_multi_part_size
            );
            self.s3_multi_part_size = default_cfg.s3_multi_part_size;
        }

        Ok(())
    }
}

impl Default for BackupStreamConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();
        let total_mem = SysQuota::memory_limit_in_bytes();
        let file_size_limit = ReadableSize::mb(256);
        // Don't use too many memory.
        let temp_file_quota = total_mem / 16;
        let quota_size = (total_mem as f64 * 0.1).min(ReadableSize::mb(512).0 as _);
        // 2x of the max pending bytes. The extra buffer make us easier to keep all
        // files in memory.
        let preferred_cache_size = file_size_limit.0 * 2;
        let cache_size = ReadableSize(temp_file_quota.min(preferred_cache_size));
        Self {
            min_ts_interval: ReadableDuration::secs(10),
            max_flush_interval: ReadableDuration::minutes(3),
            // use at most 50% of vCPU by default
            num_threads: (cpu_num * 0.5).clamp(2.0, 12.0) as usize,
            enable: true,
            // TODO: may be use raft store directory
            temp_path: String::new(),
            file_size_limit,
            initial_scan_pending_memory_quota: ReadableSize(quota_size as _),
            initial_scan_rate_limit: ReadableSize::mb(60),
            initial_scan_concurrency: 6,
            temp_file_memory_quota: cache_size,
            s3_multi_part_size: ReadableSize::mb(5),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CdcConfig {
    pub min_ts_interval: ReadableDuration,
    pub hibernate_regions_compatible: bool,
    // TODO(hi-rustin): Consider resizing the thread pool based on `incremental_scan_threads`.
    #[online_config(skip)]
    pub incremental_scan_threads: usize,
    // The number of scan tasks that is allowed to run concurrently.
    pub incremental_scan_concurrency: usize,
    // The number of scan tasks that is allowed to be created. In other words,
    // there will be at most `incremental_scan_concurrency_limit - incremental_scan_concurrency`
    // number of scan tasks that is waitting to run.
    pub incremental_scan_concurrency_limit: usize,
    /// Limit scan speed based on disk I/O traffic.
    pub incremental_scan_speed_limit: ReadableSize,
    /// Limit scan speed based on memory accesing traffic.
    #[doc(hidden)]
    pub incremental_fetch_speed_limit: ReadableSize,
    /// `TsFilter` can increase speed and decrease resource usage when
    /// incremental content is much less than total content. However in
    /// other cases, `TsFilter` can make performance worse because it needs
    /// to re-fetch old row values if they are required.
    ///
    /// `TsFilter` will be enabled if `incremental/total <=
    /// incremental_scan_ts_filter_ratio`.
    /// Set `incremental_scan_ts_filter_ratio` to 0 will disable it.
    pub incremental_scan_ts_filter_ratio: f64,

    /// Count of threads to confirm Region leadership in TiKV instances, 1 by
    /// default. Please consider to increase it if count of regions on one
    /// TiKV instance is greater than 20k.
    #[online_config(skip)]
    pub tso_worker_threads: usize,

    pub sink_memory_quota: ReadableSize,
    pub old_value_cache_memory_quota: ReadableSize,

    // Deprecated! preserved for compatibility check.
    #[online_config(hidden)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "The configuration has been removed."]
    pub old_value_cache_size: usize,
}

impl Default for CdcConfig {
    #[allow(deprecated)]
    fn default() -> Self {
        Self {
            min_ts_interval: ReadableDuration::secs(1),
            hibernate_regions_compatible: true,
            // 4 threads for incremental scan.
            incremental_scan_threads: 4,
            // At most 6 concurrent running tasks.
            incremental_scan_concurrency: 6,
            // At most 10000 tasks can exist simultaneously.
            incremental_scan_concurrency_limit: 10000,
            // TiCDC requires a SSD, the typical write speed of SSD
            // is more than 500MB/s, so 128MB/s is enough.
            incremental_scan_speed_limit: ReadableSize::mb(128),
            incremental_fetch_speed_limit: ReadableSize::mb(512),
            incremental_scan_ts_filter_ratio: 0.2,
            tso_worker_threads: 1,
            // 512MB memory for CDC sink.
            sink_memory_quota: ReadableSize::mb(512),
            // 512MB memory for old value cache.
            old_value_cache_memory_quota: ReadableSize::mb(512),
            // Deprecated! preserved for compatibility check.
            old_value_cache_size: 0,
        }
    }
}

impl CdcConfig {
    pub fn validate(&mut self, raftstore_v2: bool) -> Result<(), Box<dyn Error>> {
        let default_cfg = CdcConfig::default();
        if self.min_ts_interval.is_zero() {
            warn!(
                "cdc.min-ts-interval can't be 0, change it to {}",
                default_cfg.min_ts_interval
            );
            self.min_ts_interval = default_cfg.min_ts_interval;
        }
        if self.incremental_scan_threads == 0 {
            warn!(
                "cdc.incremental-scan-threads can't be 0, change it to {}",
                default_cfg.incremental_scan_threads
            );
            self.incremental_scan_threads = default_cfg.incremental_scan_threads;
        }
        if self.incremental_scan_concurrency < self.incremental_scan_threads {
            warn!(
                "cdc.incremental-scan-concurrency must be larger than cdc.incremental-scan-threads,
                change it to {}",
                self.incremental_scan_threads
            );
            self.incremental_scan_concurrency = self.incremental_scan_threads
        }
        if self.incremental_scan_concurrency_limit < self.incremental_scan_concurrency {
            warn!(
                "cdc.incremental-scan-concurrency-limit must be larger than cdc.incremental-scan-concurrency,
                change it to {}",
                self.incremental_scan_concurrency
            );
            self.incremental_scan_concurrency_limit = self.incremental_scan_concurrency
        }
        if self.incremental_scan_ts_filter_ratio < 0.0
            || self.incremental_scan_ts_filter_ratio > 1.0
        {
            warn!(
                "cdc.incremental-scan-ts-filter-ratio should be larger than 0 and less than 1,
                change it to {}",
                default_cfg.incremental_scan_ts_filter_ratio
            );
            self.incremental_scan_ts_filter_ratio = default_cfg.incremental_scan_ts_filter_ratio;
        }
        if raftstore_v2 && self.hibernate_regions_compatible {
            warn!(
                "cdc.hibernate_regions_compatible is overwritten to false for partitioned-raft-kv"
            );
            self.hibernate_regions_compatible = false;
        }

        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ResolvedTsConfig {
    #[online_config(skip)]
    pub enable: bool,
    pub advance_ts_interval: ReadableDuration,
    #[online_config(skip)]
    pub scan_lock_pool_size: usize,
    pub memory_quota: ReadableSize,
    pub incremental_scan_concurrency: usize,
    pub memory_quota_active_check_interval: ReadableDuration,
    // Re-register regions backoff duration when memory quota is exceeded.
    // The actual backoff duration will be in the range
    // [configured_duration, 2 * configured_duration)
    pub memory_quota_exceeded_backoff_duration: ReadableDuration,
}

impl ResolvedTsConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.advance_ts_interval.is_zero() {
            return Err("resolved-ts.advance-ts-interval can't be zero".into());
        }
        if self.scan_lock_pool_size == 0 {
            return Err("resolved-ts.scan-lock-pool-size can't be zero".into());
        }
        Ok(())
    }
}

impl Default for ResolvedTsConfig {
    fn default() -> Self {
        Self {
            enable: true,
            advance_ts_interval: ReadableDuration::secs(20),
            scan_lock_pool_size: 2,
            memory_quota: ReadableSize::mb(256),
            incremental_scan_concurrency: 6,
            memory_quota_active_check_interval: ReadableDuration::secs(10),
            memory_quota_exceeded_backoff_duration: ReadableDuration::secs(30),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct File {
    pub filename: String,
    // The unit is MB
    pub max_size: u64,
    // The unit is Day
    pub max_days: u64,
    pub max_backups: usize,
}

impl Default for File {
    fn default() -> Self {
        Self {
            filename: "".to_owned(),
            max_size: 300,
            max_days: 0,
            max_backups: 0,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct LogConfig {
    pub level: LogLevel,
    #[online_config(skip)]
    pub format: LogFormat,
    #[online_config(skip)]
    pub enable_timestamp: bool,
    #[online_config(skip)]
    pub file: File,
}

/// LogLevel is a wrapper type of `slog::Level`
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct LogLevel(slog::Level);

impl From<LogLevel> for slog::Level {
    fn from(l: LogLevel) -> Self {
        l.0
    }
}

impl From<slog::Level> for LogLevel {
    fn from(l: slog::Level) -> Self {
        Self(l)
    }
}

impl Serialize for LogLevel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        get_string_by_level(self.0).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for LogLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        get_level_by_string(&string)
            .map(LogLevel)
            .ok_or_else(|| D::Error::invalid_value(Unexpected::Str(&string), &"a valid log level"))
    }
}

impl From<LogLevel> for ConfigValue {
    fn from(l: LogLevel) -> Self {
        Self::String(get_string_by_level(l.0).into())
    }
}

impl TryFrom<ConfigValue> for LogLevel {
    type Error = String;
    fn try_from(value: ConfigValue) -> Result<Self, Self::Error> {
        if let ConfigValue::String(s) = value {
            get_level_by_string(&s)
                .map(LogLevel)
                .ok_or_else(|| format!("invalid log level: '{}'", s))
        } else {
            panic!("expect ConfigValue::String, found: {:?}", value)
        }
    }
}

impl TryFrom<&ConfigValue> for LogLevel {
    type Error = String;
    fn try_from(value: &ConfigValue) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel(slog::Level::Info),
            format: LogFormat::Text,
            enable_timestamp: true,
            file: File::default(),
        }
    }
}

impl LogConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.file.max_size > 4096 {
            return Err("Max log file size upper limit to 4096MB".into());
        }
        Ok(())
    }
}

pub struct LogConfigManager;

impl ConfigManager for LogConfigManager {
    fn dispatch(&mut self, changes: ConfigChange) -> CfgResult<()> {
        if let Some(v) = changes.get("level") {
            let log_level = LogLevel::try_from(v)?;
            set_log_level(log_level.0);
        }
        info!("update log config"; "config" => ?changes);
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MemoryConfig {
    // Whether enables the heap profiling which may have a bit performance overhead about 2% for
    // the default sample rate.
    pub enable_heap_profiling: bool,

    // Average interval between allocation samples, as measured in bytes of allocation activity.
    // Increasing the sampling interval decreases profile fidelity, but also decreases the
    // computational overhead.
    // The default sample interval is 512 KB. It only accepts power of two, otherwise it will be
    // rounded up to the next power of two.
    pub profiling_sample_per_bytes: ReadableSize,

    // Whether allocates the exclusive arena for threads.
    // When disabled, the metric of memory usage for each thread would be unavailable.
    #[online_config(skip)]
    pub enable_thread_exclusive_arena: bool,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enable_heap_profiling: true,
            profiling_sample_per_bytes: ReadableSize::kb(512),
            enable_thread_exclusive_arena: true,
        }
    }
}

impl MemoryConfig {
    pub fn init(&self) {
        if self.enable_heap_profiling {
            if let Err(e) = tikv_alloc::activate_prof() {
                error!("failed to enable heap profiling"; "err" => ?e);
                return;
            }
            tikv_alloc::set_prof_sample(self.profiling_sample_per_bytes.0).unwrap();
        }
        tikv_alloc::set_thread_exclusive_arena(self.enable_thread_exclusive_arena);
    }
}

pub struct MemoryConfigManager;

impl ConfigManager for MemoryConfigManager {
    fn dispatch(&mut self, changes: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Bool(enable)) = changes.get("enable_heap_profiling") {
            if *enable {
                tikv_alloc::activate_prof()?;
            } else {
                tikv_alloc::deactivate_prof()?;
            }
        }

        if let Some(ConfigValue::Size(sample_rate)) = changes.get("profiling_sample_per_bytes") {
            tikv_alloc::set_prof_sample(*sample_rate).unwrap();
        }
        info!("update memory config"; "config" => ?changes);
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct QuotaConfig {
    pub foreground_cpu_time: usize,
    pub foreground_write_bandwidth: ReadableSize,
    pub foreground_read_bandwidth: ReadableSize,
    pub max_delay_duration: ReadableDuration,
    pub background_cpu_time: usize,
    pub background_write_bandwidth: ReadableSize,
    pub background_read_bandwidth: ReadableSize,
    pub enable_auto_tune: bool,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            foreground_cpu_time: 0,
            foreground_write_bandwidth: ReadableSize(0),
            foreground_read_bandwidth: ReadableSize(0),
            max_delay_duration: ReadableDuration::millis(500),
            background_cpu_time: 0,
            background_write_bandwidth: ReadableSize(0),
            background_read_bandwidth: ReadableSize(0),
            enable_auto_tune: false,
        }
    }
}

impl QuotaConfig {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        const MAX_DELAY_DURATION: ReadableDuration = ReadableDuration::micros(u64::MAX / 1000);

        if self.max_delay_duration > MAX_DELAY_DURATION {
            return Err(format!("quota.max-delay-duration must <= {}", MAX_DELAY_DURATION).into());
        }

        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TikvConfig {
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    pub cfg_path: String,

    #[doc(hidden)]
    #[online_config(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "The configuration has been moved to log.level."]
    pub log_level: LogLevel,
    #[doc(hidden)]
    #[online_config(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "The configuration has been moved to log.file.filename."]
    pub log_file: String,
    #[doc(hidden)]
    #[online_config(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "The configuration has been moved to log.format."]
    pub log_format: LogFormat,
    #[online_config(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "The configuration has been moved to log.file.max_days."]
    pub log_rotation_timespan: ReadableDuration,
    #[doc(hidden)]
    #[online_config(hidden)]
    #[serde(skip_serializing)]
    #[deprecated = "The configuration has been moved to log.file.max_size."]
    pub log_rotation_size: ReadableSize,

    #[online_config(skip)]
    pub slow_log_file: String,

    #[online_config(skip)]
    pub slow_log_threshold: ReadableDuration,

    #[online_config(hidden)]
    pub panic_when_unexpected_key_or_data: bool,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub enable_io_snoop: bool,

    #[online_config(skip)]
    pub abort_on_panic: bool,

    #[doc(hidden)]
    #[online_config(skip)]
    pub memory_usage_limit: Option<ReadableSize>,

    #[doc(hidden)]
    #[online_config(skip)]
    pub memory_usage_high_water: f64,

    #[online_config(submodule)]
    pub log: LogConfig,

    #[online_config(submodule)]
    pub memory: MemoryConfig,

    #[online_config(submodule)]
    pub quota: QuotaConfig,

    #[online_config(submodule)]
    pub readpool: ReadPoolConfig,

    #[online_config(submodule)]
    pub server: ServerConfig,

    #[online_config(submodule)]
    pub storage: StorageConfig,

    #[online_config(skip)]
    pub pd: PdConfig,

    #[online_config(hidden)]
    pub metric: MetricConfig,

    #[online_config(submodule)]
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,

    #[online_config(submodule)]
    pub coprocessor: CopConfig,

    #[online_config(skip)]
    pub coprocessor_v2: CoprocessorV2Config,

    #[online_config(submodule)]
    pub rocksdb: DbConfig,

    #[online_config(submodule)]
    pub raftdb: RaftDbConfig,

    #[online_config(skip)]
    pub raft_engine: RaftEngineConfig,

    #[online_config(submodule)]
    pub security: SecurityConfig,

    #[online_config(submodule)]
    pub import: ImportConfig,

    #[online_config(submodule)]
    pub backup: BackupConfig,

    #[online_config(submodule)]
    // The term "log backup" and "backup stream" are identity.
    // The "log backup" should be the only product name exposed to the user.
    pub log_backup: BackupStreamConfig,

    #[online_config(submodule)]
    pub pessimistic_txn: PessimisticTxnConfig,

    #[online_config(submodule)]
    pub gc: GcConfig,

    #[online_config(submodule)]
    pub split: SplitConfig,

    #[online_config(submodule)]
    pub cdc: CdcConfig,

    #[online_config(submodule)]
    pub resolved_ts: ResolvedTsConfig,

    #[online_config(submodule)]
    pub resource_metering: ResourceMeteringConfig,

    #[online_config(skip)]
    pub causal_ts: CausalTsConfig,

    #[online_config(submodule)]
    pub resource_control: ResourceControlConfig,

    #[online_config(submodule)]
    pub in_memory_engine: InMemoryEngineConfig,
}

impl Default for TikvConfig {
    #[allow(deprecated)]
    fn default() -> TikvConfig {
        TikvConfig {
            cfg_path: "".to_owned(),
            log_level: slog::Level::Info.into(),
            log_file: "".to_owned(),
            log_format: LogFormat::Text,
            log_rotation_timespan: ReadableDuration::hours(0),
            log_rotation_size: ReadableSize::mb(300),
            slow_log_file: "".to_owned(),
            slow_log_threshold: ReadableDuration::secs(1),
            panic_when_unexpected_key_or_data: false,
            enable_io_snoop: true,
            abort_on_panic: false,
            memory_usage_limit: None,
            memory_usage_high_water: 0.9,
            log: LogConfig::default(),
            memory: MemoryConfig::default(),
            quota: QuotaConfig::default(),
            readpool: ReadPoolConfig::default(),
            server: ServerConfig::default(),
            metric: MetricConfig::default(),
            raft_store: RaftstoreConfig::default(),
            coprocessor: CopConfig::default(),
            coprocessor_v2: CoprocessorV2Config::default(),
            pd: PdConfig::default(),
            rocksdb: DbConfig::default(),
            raftdb: RaftDbConfig::default(),
            raft_engine: RaftEngineConfig::default(),
            storage: StorageConfig::default(),
            security: SecurityConfig::default(),
            import: ImportConfig::default(),
            backup: BackupConfig::default(),
            pessimistic_txn: PessimisticTxnConfig::default(),
            gc: GcConfig::default(),
            split: SplitConfig::default(),
            cdc: CdcConfig::default(),
            resolved_ts: ResolvedTsConfig::default(),
            resource_metering: ResourceMeteringConfig::default(),
            log_backup: BackupStreamConfig::default(),
            causal_ts: CausalTsConfig::default(),
            resource_control: ResourceControlConfig::default(),
            in_memory_engine: InMemoryEngineConfig::default(),
        }
    }
}

impl TikvConfig {
    pub fn infer_raft_db_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        if self.raft_store.raftdb_path.is_empty() {
            let data_dir = data_dir.unwrap_or(&self.storage.data_dir);
            config::canonicalize_sub_path(data_dir, "raft")
        } else {
            config::canonicalize_path(&self.raft_store.raftdb_path)
        }
    }

    pub fn infer_raft_engine_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        if self.raft_engine.config.dir.is_empty() {
            let data_dir = data_dir.unwrap_or(&self.storage.data_dir);
            config::canonicalize_sub_path(data_dir, "raft-engine")
        } else {
            config::canonicalize_path(&self.raft_engine.config.dir)
        }
    }

    // FIXME: consider engine_type.
    pub fn infer_kv_engine_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        let data_dir = data_dir.unwrap_or(&self.storage.data_dir);
        config::canonicalize_sub_path(data_dir, DEFAULT_ROCKSDB_SUB_DIR)
    }

    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        // Setting up data paths.
        if self.cfg_path.is_empty() {
            self.cfg_path = Path::new(&self.storage.data_dir)
                .join(LAST_CONFIG_FILE)
                .to_str()
                .unwrap()
                .to_owned();
        }

        match (
            self.raft_store.raftdb_path.is_empty(),
            self.raft_engine.config.dir.is_empty(),
        ) {
            (false, true) => {
                // If raftdb_path is specified, raft_engine_path will inherit it, this will be
                // useful when updating from older version.
                self.raft_engine.config.dir =
                    self.infer_raft_engine_path(Some(self.raft_store.raftdb_path.as_str()))?;
                self.raft_store.raftdb_path = self.infer_raft_db_path(None)?;
            }
            _ => {
                self.raft_store.raftdb_path = self.infer_raft_db_path(None)?;
                self.raft_engine.config.dir = self.infer_raft_engine_path(None)?;
            }
        }

        if self.log_backup.temp_path.is_empty() {
            self.log_backup.temp_path =
                config::canonicalize_sub_path(&self.storage.data_dir, "log-backup-temp")?;
        }

        // Validating data paths.
        if self.raft_engine.config.dir == self.raft_store.raftdb_path {
            return Err("raft_engine.config.dir can't be same as raft_store.raftdb_path".into());
        }
        // Newly created dbs will be optimized with certain options. e.g. Titan.
        let kv_data_exists = match self.storage.engine {
            EngineType::RaftKv => {
                let kv_db_path = self.infer_kv_engine_path(None)?;
                let kv_db_wal_path = if self.rocksdb.wal_dir.is_empty() {
                    config::canonicalize_path(&kv_db_path)?
                } else {
                    config::canonicalize_path(&self.rocksdb.wal_dir)?
                };
                if self.raft_engine.enable {
                    if kv_db_path == self.raft_engine.config.dir {
                        return Err("raft-engine.dir can't be same as storage.data_dir/db".into());
                    }
                } else {
                    if kv_db_path == self.raft_store.raftdb_path {
                        return Err(
                            "raft_store.raftdb_path can't be same as storage.data_dir/db".into(),
                        );
                    }
                    let raft_db_wal_path = if self.raftdb.wal_dir.is_empty() {
                        config::canonicalize_path(&self.raft_store.raftdb_path)?
                    } else {
                        config::canonicalize_path(&self.raftdb.wal_dir)?
                    };
                    if kv_db_wal_path == raft_db_wal_path {
                        return Err("raftdb.wal_dir can't be same as rocksdb.wal_dir".into());
                    }
                }
                // Check blob file dir is empty when titan is disabled
                let titandb_path = if self.rocksdb.titan.dirname.is_empty() {
                    Path::new(&kv_db_path).join("titandb")
                } else {
                    Path::new(&self.rocksdb.titan.dirname).to_path_buf()
                };
                if let Err(e) =
                    tikv_util::config::check_data_dir_empty(titandb_path.to_str().unwrap(), "blob")
                {
                    if let Some(false) = self.rocksdb.titan.enabled {
                        // If Titan is disabled explicitly but Titan's data directory is not empty,
                        // return an error.
                        return Err(format!(
                            "check: titandb-data-dir-empty; err: \"{}\"; \
                            hint: You have disabled titan when its data directory is not empty. \
                            To properly shutdown titan, please enter fallback blob-run-mode and \
                            wait till titandb files are all safely ingested.",
                            e
                        )
                        .into());
                    }
                }
                RocksEngine::exists(&kv_db_path)
            }
            EngineType::RaftKv2 => {
                if !self.rocksdb.wal_dir.is_empty() {
                    return Err(
                        "partitioned-raft-kv doesn't support configuring rocksdb.wal-dir".into(),
                    );
                }
                Path::new(&self.storage.data_dir)
                    .join(DEFAULT_TABLET_SUB_DIR)
                    .exists()
            }
        };
        RaftDataStateMachine::new(
            &self.storage.data_dir,
            &self.raft_store.raftdb_path,
            &self.raft_engine.config.dir,
        )
        .validate(kv_data_exists)?;

        // Optimize.
        self.rocksdb.optimize_for(&self.storage);
        self.coprocessor
            .optimize_for(self.storage.engine == EngineType::RaftKv2);
        self.split
            .optimize_for(self.coprocessor.region_split_size());
        self.raft_store
            .optimize_for(self.storage.engine == EngineType::RaftKv2);
        self.server
            .optimize_for(self.coprocessor.region_split_size());
        if self.storage.engine == EngineType::RaftKv2 {
            self.raft_store.store_io_pool_size = cmp::max(self.raft_store.store_io_pool_size, 1);
        }
        self.raft_engine
            .optimize_for(&self.raft_store, self.storage.engine == EngineType::RaftKv2);
        if self.storage.block_cache.capacity.is_none() {
            let total_mem = SysQuota::memory_limit_in_bytes();
            let capacity = if self.storage.engine == EngineType::RaftKv2 {
                (total_mem as f64) * RAFTSTORE_V2_BLOCK_CACHE_RATE
            } else {
                (total_mem as f64) * BLOCK_CACHE_RATE
            };
            self.storage.block_cache.capacity = Some(ReadableSize(capacity as u64));
        }

        // Validate for v2.
        if self.storage.engine == EngineType::RaftKv2 {
            if !self.raft_engine.enable {
                return Err("partitioned-raft-kv only supports raft log engine.".into());
            }
            let recovery_threads = cmp::min((SysQuota::cpu_cores_quota() * 1.5) as usize, 16);
            if self.raft_engine.config.recovery_threads < recovery_threads {
                info!(
                    "raft-engine.recovery-threads is too small. Set it to {} instead.",
                    recovery_threads,
                );
                self.raft_engine.config.recovery_threads = recovery_threads;
            }
            // Filled in DbOptions::optimize_for.
            let write_buffer_limit = self.rocksdb.write_buffer_limit.unwrap();
            if self.raft_engine.config.purge_threshold.0 < write_buffer_limit.0 * 2 {
                self.raft_engine.config.purge_threshold.0 = write_buffer_limit.0 * 2;
                info!(
                    "raft-engine.purge-threshold is too small. Set it to {} instead.",
                    self.raft_engine.config.purge_threshold,
                );
            }
            if let Some(true) = self.rocksdb.titan.enabled {
                return Err("partitioned-raft-kv doesn't support titan.".into());
            }
            if self.raft_store.enable_v2_compatible_learner {
                self.raft_store.enable_v2_compatible_learner = false;
                warn!(
                    "raftstore.enable-partitioned-raft-kv-compatible-learner was true but \
                    storage.engine was partitioned-raft-kv, no need to enable \
                    enable-partitioned-raft-kv-compatible-learner, overwrite to false"
                );
            }
            if self.raft_store.use_delete_range {
                return Err(
                    "partitioned-raft-kv doesn't support raftstore.use-delete-range=true.".into(),
                );
            }
        }

        // Validate raftstore with other components.
        let expect_keepalive = self.raft_store.raft_heartbeat_interval() * 2;
        if expect_keepalive > self.server.grpc_keepalive_time.0 {
            return Err(format!(
                "grpc_keepalive_time is too small, it should not less than the double of \
                 raft tick interval (>= {})",
                duration_to_sec(expect_keepalive)
            )
            .into());
        }
        if self.raft_store.hibernate_regions && !self.cdc.hibernate_regions_compatible {
            warn!(
                "raftstore.hibernate-regions was enabled but cdc.hibernate-regions-compatible \
                was disabled, hibernate regions may be broken up if you want to deploy a cdc cluster"
            );
        }

        // Validate flow control and rocksdb write stall.
        if self.storage.flow_control.enable {
            self.rocksdb.defaultcf.disable_write_stall = true;
            self.rocksdb.writecf.disable_write_stall = true;
            self.rocksdb.lockcf.disable_write_stall = true;
            self.rocksdb.raftcf.disable_write_stall = true;
        }
        // Fill in values for unspecified write stall configurations.

        macro_rules! fill_cf_opts {
            ($cf_opts:expr, $cfg:expr) => {
                // Automatically override `level0_slowdown_writes_trigger` if it is set too high,
                // to ensure that RocksDB's compaction speed-up mechanism can help avoid triggering flow control,
                // since RocksDB starts speed up compaction when the number of L0 files reaches this threshold.
                //
                // For the same reason, when `l0-file-threshold` is set to a higher value, we do **not** override it,
                // because we still want RocksDB's compaction speed-up mechanism to take effect—
                // allowing compaction to proceed more quickly and smoothly, leading to more stable performance
                // and avoiding issues like https://github.com/tikv/tikv/issues/18708.
                if $cfg.enable && $cf_opts.level0_slowdown_writes_trigger > $cfg.l0_files_threshold as i32 {
                    warn!(
                        "{}.level0-slowdown-writes-trigger is too large. Setting it to \
                        storage.flow-control.l0-files-threshold ({})",
                        stringify!($cf_opts), $cfg.l0_files_threshold
                    );
                    $cf_opts.level0_slowdown_writes_trigger = $cfg.l0_files_threshold as i32;
                }
                // If unset, `level0_stop_writes_trigger` defaults to `l0_files_threshold`
                // (unlike `level0_slowdown_writes_trigger`, which defaults to 20)  to ensure ingest_maybe_stall
                // integrates properly with flow control for correctness on SST ingestion
                if let Some(v) = &mut $cf_opts.level0_stop_writes_trigger {
                    if $cfg.enable && *v > $cfg.l0_files_threshold as i32 {
                        warn!(
                            "{}.level0-stop-writes-trigger is too large. Setting it to \
                            storage.flow-control.l0-files-threshold ({})",
                            stringify!($cf_opts), $cfg.l0_files_threshold
                        );
                        *v = $cfg.l0_files_threshold as i32;
                    }
                } else {
                    $cf_opts.level0_stop_writes_trigger =
                        Some($cfg.l0_files_threshold as i32);
                }
                // The reason for automatically overriding it is the same as for `level0_slowdown_writes_trigger`.
                if $cfg.enable && $cf_opts.soft_pending_compaction_bytes_limit.0 > $cfg.soft_pending_compaction_bytes_limit.0 {
                    warn!(
                        "{}.soft-pending-compaction-bytes-limit is too large. Setting it to \
                        storage.flow-control.soft-pending-compaction-bytes-limit ({})",
                        stringify!($cf_opts), $cfg.soft_pending_compaction_bytes_limit.0
                    );
                    $cf_opts.soft_pending_compaction_bytes_limit = $cfg.soft_pending_compaction_bytes_limit;
                }
                if let Some(v) = &mut $cf_opts.hard_pending_compaction_bytes_limit {
                    if $cfg.enable && v.0 > $cfg.hard_pending_compaction_bytes_limit.0 {
                        warn!(
                            "{}.hard-pending-compaction-bytes-limit is too large. Setting it to \
                            storage.flow-control.hard-pending-compaction-bytes-limit ({})",
                            stringify!($cf_opts), $cfg.hard_pending_compaction_bytes_limit.0
                        );
                        *v = $cfg.hard_pending_compaction_bytes_limit;
                    }
                } else {
                    $cf_opts.hard_pending_compaction_bytes_limit =
                        Some($cfg.hard_pending_compaction_bytes_limit);
                }
            };
        }
        let flow_control_cfg = if self.storage.flow_control.enable {
            self.storage.flow_control.clone()
        } else {
            crate::storage::config::FlowControlConfig {
                enable: false,
                ..Default::default()
            }
        };
        fill_cf_opts!(self.raftdb.defaultcf, flow_control_cfg);
        fill_cf_opts!(self.rocksdb.defaultcf, flow_control_cfg);
        fill_cf_opts!(self.rocksdb.writecf, flow_control_cfg);
        fill_cf_opts!(self.rocksdb.lockcf, flow_control_cfg);
        fill_cf_opts!(self.rocksdb.raftcf, flow_control_cfg);

        // Validate memory usage limit.
        let block_cache_cap = self.storage.block_cache.capacity.unwrap();
        let write_buffer_cap = self.rocksdb.write_buffer_limit.unwrap_or(ReadableSize(0));
        if let Some(memory_usage_limit) = self.memory_usage_limit {
            let total = SysQuota::memory_limit_in_bytes();
            if memory_usage_limit.0 > total {
                // Explicitly exceeds system memory capacity is not allowed.
                return Err(format!(
                    "memory_usage_limit is greater than system memory capacity ({})",
                    total
                )
                .into());
            }
        } else {
            let mut limit =
                (block_cache_cap.0 as f64 / BLOCK_CACHE_RATE * MEMORY_USAGE_LIMIT_RATE) as u64;
            if self.storage.engine == EngineType::RaftKv2 {
                limit = cmp::max(
                    limit,
                    (write_buffer_cap.0 as f64 / WRITE_BUFFER_MEMORY_LIMIT_RATE
                        * MEMORY_USAGE_LIMIT_RATE) as u64,
                );
            }
            let limit = ReadableSize(cmp::min(limit, SysQuota::memory_limit_in_bytes()));
            let default = Self::suggested_memory_usage_limit();
            if limit > default {
                warn!(
                    "memory_usage_limit ({}) > recommanded ({}), maybe page cache isn't enough",
                    limit, default,
                );
            }
            self.memory_usage_limit = Some(limit);
        }
        if block_cache_cap.0 + write_buffer_cap.0 > self.memory_usage_limit.unwrap().0 {
            return Err(format!(
                "The sum of `storage.block-cache.capacity` and `rocksdb.write-buffer-limit` \
                is greater than memory-usage-limit: {} + {} > {}",
                block_cache_cap,
                write_buffer_cap,
                self.memory_usage_limit.unwrap(),
            )
            .into());
        }

        // Validate sub-components.
        self.log.validate()?;
        self.readpool.validate()?;
        self.storage.validate()?;
        self.rocksdb.validate()?;
        self.raftdb.validate()?;
        self.raft_engine.validate()?;
        self.server.validate()?;
        self.pd.validate()?;
        self.coprocessor
            .validate(self.storage.engine == EngineType::RaftKv2)?;
        self.raft_store.validate(
            self.coprocessor.region_split_size(),
            self.coprocessor.enable_region_bucket(),
            self.coprocessor.region_bucket_size,
            self.storage.engine == EngineType::RaftKv2,
        )?;
        self.security.validate()?;
        self.import.validate()?;
        self.backup.validate()?;
        self.log_backup.validate()?;
        self.cdc
            .validate(self.storage.engine == EngineType::RaftKv2)?;
        self.pessimistic_txn.validate()?;
        self.gc.validate()?;
        self.resolved_ts.validate()?;
        self.resource_metering.validate()?;
        self.quota.validate()?;
        self.causal_ts.validate()?;

        // Disable in memory engine if api version is V1ttl or V2.
        if (self.storage.api_version() == ApiVersion::V2 || self.storage.enable_ttl)
            && self.in_memory_engine.enable
        {
            return Err("in-memory-engine is unavailable for feature TTL or API v2".into());
        }
        self.in_memory_engine.expected_region_size = self.coprocessor.region_split_size();
        self.in_memory_engine.validate(
            &mut self.storage.block_cache.capacity.as_mut().unwrap().0,
            self.coprocessor.region_split_size(),
        )?;

        // Now, only support cross check in in-memory engine when compaction filter is
        // enabled.
        if self.in_memory_engine.enable
            && !self.in_memory_engine.cross_check_interval.is_zero()
            && !self.gc.enable_compaction_filter
        {
            return Err("Compaction-filter should be enabled when cross-check is turned on in in-memory engine".to_string().into());
        }

        // Validate feature TTL with Titan configuration.
        if matches!(self.rocksdb.titan.enabled, Some(true)) && self.storage.enable_ttl {
            return Err("Titan is unavailable for feature TTL".into());
        }

        Ok(())
    }

    fn titan_data_exists(&self) -> Result<bool, Box<dyn Error>> {
        let exist = match self.storage.engine {
            EngineType::RaftKv => {
                // Check blob file dir is empty when titan is disabled
                let titandb_path = if self.rocksdb.titan.dirname.is_empty() {
                    let kv_db_path = self.infer_kv_engine_path(None)?;
                    Path::new(&kv_db_path).join("titandb")
                } else {
                    Path::new(&self.rocksdb.titan.dirname).to_path_buf()
                };
                tikv_util::config::check_data_dir_empty(titandb_path.to_str().unwrap(), "blob")
                    .is_err()
            }
            EngineType::RaftKv2 => false,
        };
        Ok(exist)
    }

    fn kv_data_exists(&self) -> Result<bool, Box<dyn Error>> {
        let kv_data_exists = match self.storage.engine {
            EngineType::RaftKv => {
                let kv_db_path = self.infer_kv_engine_path(None)?;
                RocksEngine::exists(&kv_db_path)
            }
            EngineType::RaftKv2 => Path::new(&self.storage.data_dir)
                .join(DEFAULT_TABLET_SUB_DIR)
                .exists(),
        };
        Ok(kv_data_exists)
    }

    // As the init of `logger` is very early, this adjust needs to be separated and
    // called immediately after parsing the command line.
    #[allow(deprecated)]
    pub fn logger_compatible_adjust(&mut self) {
        let default_tikv_cfg = TikvConfig::default();
        let default_log_cfg = LogConfig::default();
        if self.log_level != default_tikv_cfg.log_level {
            eprintln!("deprecated configuration, log-level has been moved to log.level");
            if self.log.level == default_log_cfg.level {
                eprintln!("override log.level with log-level, {:?}", self.log_level);
                self.log.level = self.log_level;
            }
            self.log_level = default_tikv_cfg.log_level;
        }
        if self.log_file != default_tikv_cfg.log_file {
            eprintln!("deprecated configuration, log-file has been moved to log.file.filename");
            if self.log.file.filename == default_log_cfg.file.filename {
                eprintln!(
                    "override log.file.filename with log-file, {:?}",
                    self.log_file
                );
                self.log.file.filename = self.log_file.clone();
            }
            self.log_file = default_tikv_cfg.log_file;
        }
        if self.log_format != default_tikv_cfg.log_format {
            eprintln!("deprecated configuration, log-format has been moved to log.format");
            if self.log.format == default_log_cfg.format {
                eprintln!("override log.format with log-format, {:?}", self.log_format);
                self.log.format = self.log_format;
            }
            self.log_format = default_tikv_cfg.log_format;
        }
        if self.log_rotation_timespan.as_secs() > 0 {
            eprintln!(
                "deprecated configuration, log-rotation-timespan is no longer used and ignored."
            );
        }
        if self.log_rotation_size != default_tikv_cfg.log_rotation_size {
            eprintln!(
                "deprecated configuration, \
                 log-ratation-size has been moved to log.file.max-size"
            );
            if self.log.file.max_size == default_log_cfg.file.max_size {
                eprintln!(
                    "override log.file.max_size with log-rotation-size, {:?}",
                    self.log_rotation_size
                );
                self.log.file.max_size = self.log_rotation_size.as_mb();
            }
            self.log_rotation_size = default_tikv_cfg.log_rotation_size;
        }
    }

    pub fn optional_default_cfg_adjust_with(
        &mut self,
        last_cfg: &Option<Self>,
    ) -> Result<(), Box<dyn Error>> {
        let kv_data_exists = self.kv_data_exists()?;
        let titan_data_exists = self.titan_data_exists()?;

        match self.storage.engine {
            EngineType::RaftKv => {
                if self.rocksdb.titan.enabled.is_none() {
                    // Override titan.enabled with last_cfg if it exists.
                    self.rocksdb.titan.enabled =
                        last_cfg.as_ref().and_then(|cfg| cfg.rocksdb.titan.enabled);
                }
                if self.rocksdb.titan.enabled.is_none() {
                    // If the user doesn't specify titan.enabled, we enable it by default for newly
                    // created clusters.
                    if (kv_data_exists && !titan_data_exists) || self.storage.enable_ttl {
                        self.rocksdb.titan.enabled = Some(false);
                    } else {
                        self.rocksdb.titan.enabled = Some(true);
                    }
                }
                if self.rocksdb.defaultcf.titan.min_blob_size.is_none() {
                    // get blob size from last config
                    self.rocksdb.defaultcf.titan.min_blob_size =
                        Some(if let Some(last_cfg) = &last_cfg {
                            // If previous config has titan enabled, we use the previous
                            // min-blob-size.
                            if last_cfg.rocksdb.titan.enabled.unwrap_or(false) {
                                last_cfg
                                    .rocksdb
                                    .defaultcf
                                    .titan
                                    .min_blob_size
                                    .unwrap_or(DEFAULT_MIN_BLOB_SIZE)
                            } else {
                                // If previous config has titan disabled, we use the current default
                                // value
                                DEFAULT_MIN_BLOB_SIZE
                            }
                        } else {
                            DEFAULT_MIN_BLOB_SIZE
                        });
                }
            }
            EngineType::RaftKv2 => {
                self.rocksdb.titan.enabled = Some(false);
                self.rocksdb.defaultcf.titan.min_blob_size = Some(DEFAULT_MIN_BLOB_SIZE);
            }
        }
        Ok(())
    }

    /// Adjusts the current configuration to maintain compatibility with the
    /// last persisted configuration. This function performs two main tasks:
    ///
    /// 1. Checks for configuration inconsistencies between the current and last
    ///    configuration, particularly for region size settings that have been
    ///    moved from raftstore to coprocessor.
    ///
    /// 2. Inherits critical configuration values from the last configuration
    ///    when appropriate, specifically:
    ///    - For region size settings (max_size, split_size, max_keys,
    ///      split_keys), inheritance only occurs if the current value is not
    ///      manually specified
    ///
    /// # Parameters
    ///
    /// * `last_config` - An optional reference to the last persisted
    ///   configuration. If `None`, default values will be used instead of
    ///   inheritance.
    ///
    /// # Behavior
    ///
    /// * Region Size Settings:
    ///   - If `raftstore.region-max-size` differs from the last config, it's
    ///     considered deprecated and will be moved to
    ///     `coprocessor.region-max-size` if not already set
    ///   - If `raftstore.region-split-size` differs from the last config, it's
    ///     considered deprecated and will be moved to
    ///     `coprocessor.region-split-size` if not already set
    /// # Examples
    ///
    /// ```rust
    /// // Start with default configuration
    /// let mut cfg = TikvConfig::default();
    /// cfg.compatible_adjust(None); // Uses default values
    ///
    /// // Adjust with last configuration
    /// let last_cfg = get_last_config();
    /// cfg.compatible_adjust(Some(&last_cfg)); // Inherits values from last_cfg if needed
    /// ```
    #[allow(deprecated)]
    pub fn compatible_adjust(&mut self, last_config: Option<&TikvConfig>) {
        let (default_raft_store, default_coprocessor) =
            last_config.map_or((RaftstoreConfig::default(), CopConfig::default()), |cfg| {
                (
                    RaftstoreConfig {
                        region_max_size: cfg.raft_store.region_max_size,
                        region_split_size: cfg.raft_store.region_split_size,
                        ..cfg.raft_store.clone()
                    },
                    cfg.coprocessor.clone(),
                )
            });
        if self.raft_store.region_max_size != default_raft_store.region_max_size {
            warn!(
                "deprecated configuration, \
                 raftstore.region-max-size has been moved to coprocessor"
            );
            if self.coprocessor.region_max_size == default_coprocessor.region_max_size {
                warn!(
                    "override coprocessor.region-max-size with raftstore.region-max-size, {:?}",
                    self.raft_store.region_max_size
                );
                self.coprocessor.region_max_size = Some(self.raft_store.region_max_size);
            }
            self.raft_store.region_max_size = default_raft_store.region_max_size;
        }
        if self.raft_store.region_split_size != default_raft_store.region_split_size {
            warn!(
                "deprecated configuration, \
                 raftstore.region-split-size has been moved to coprocessor",
            );
            if self.coprocessor.region_split_size == default_coprocessor.region_split_size {
                warn!(
                    "override coprocessor.region-split-size with raftstore.region-split-size, {:?}",
                    self.raft_store.region_split_size
                );
                self.coprocessor.region_split_size = Some(self.raft_store.region_split_size);
            }
            self.raft_store.region_split_size = default_raft_store.region_split_size;
        }
        // If any one of the configurations, corresponsive for the size of region, not
        // manully changed or set, TiKV should inherit the relative settings
        // from the previous config file.
        if self.coprocessor.region_max_size.is_none() {
            self.coprocessor.region_max_size = default_coprocessor.region_max_size;
        }
        if self.coprocessor.region_max_keys.is_none() {
            self.coprocessor.region_max_keys = default_coprocessor.region_max_keys;
        }
        if self.coprocessor.region_split_size.is_none() {
            self.coprocessor.region_split_size = default_coprocessor.region_split_size;
        }
        if self.coprocessor.region_split_keys.is_none() {
            self.coprocessor.region_split_keys = default_coprocessor.region_split_keys;
        }
        if self.server.end_point_concurrency.is_some() {
            warn!(
                "deprecated configuration, {} has been moved to {}",
                "server.end-point-concurrency", "readpool.coprocessor.xxx-concurrency",
            );
            warn!(
                "override {} with {}, {:?}",
                "readpool.coprocessor.xxx-concurrency",
                "server.end-point-concurrency",
                self.server.end_point_concurrency
            );
            let concurrency = self.server.end_point_concurrency.take().unwrap();
            self.readpool.coprocessor.high_concurrency = concurrency;
            self.readpool.coprocessor.normal_concurrency = concurrency;
            self.readpool.coprocessor.low_concurrency = concurrency;
        }
        if self.server.end_point_stack_size.is_some() {
            warn!(
                "deprecated configuration, {} has been moved to {}",
                "server.end-point-stack-size", "readpool.coprocessor.stack-size",
            );
            warn!(
                "override {} with {}, {:?}",
                "readpool.coprocessor.stack-size",
                "server.end-point-stack-size",
                self.server.end_point_stack_size
            );
            self.readpool.coprocessor.stack_size = self.server.end_point_stack_size.take().unwrap();
        }
        if self.server.end_point_max_tasks.is_some() {
            warn!(
                "deprecated configuration, {} is no longer used and ignored, please use {}.",
                "server.end-point-max-tasks", "readpool.coprocessor.max-tasks-per-worker-xxx",
            );
            // Note:
            // Our `end_point_max_tasks` is mostly mistakenly configured, so we don't
            // override new configuration using old values.
            self.server.end_point_max_tasks = None;
        }
        if self.raft_store.clean_stale_peer_delay.as_secs() > 0 {
            warn!(
                "deprecated configuration, {} is no longer used and ignored.",
                "raft_store.clean_stale_peer_delay",
            );
        }
        if self.rocksdb.auto_tuned.is_some() {
            warn!(
                "deprecated configuration, {} is no longer used and ignored, please use {}.",
                "rocksdb.auto_tuned", "rocksdb.rate_limiter_auto_tuned",
            );
            self.rocksdb.auto_tuned = None;
        }
        // When shared block cache is enabled, if its capacity is set, it overrides
        // individual block cache sizes. Otherwise use the sum of block cache
        // size of all column families as the shared cache size.
        if let Some(a) = self.rocksdb.defaultcf.block_cache_size {
            if let Some(b) = self.rocksdb.writecf.block_cache_size {
                if let Some(c) = self.rocksdb.lockcf.block_cache_size {
                    let d = self
                        .raftdb
                        .defaultcf
                        .block_cache_size
                        .map(|s| s.0)
                        .unwrap_or_default();
                    let sum = a.0 + b.0 + c.0 + d;
                    self.storage.block_cache.capacity = Some(ReadableSize(sum));
                }
            }
        }
        if self.backup.sst_max_size.0 < default_coprocessor.region_max_size().0 / 10 {
            warn!(
                "override backup.sst-max-size with min sst-max-size, {:?}",
                default_coprocessor.region_max_size() / 10
            );
            self.backup.sst_max_size = default_coprocessor.region_max_size() / 10;
        } else if self.backup.sst_max_size.0 > default_coprocessor.region_max_size().0 * 2 {
            warn!(
                "override backup.sst-max-size with max sst-max-size, {:?}",
                default_coprocessor.region_max_size() * 2
            );
            self.backup.sst_max_size = default_coprocessor.region_max_size() * 2;
        }

        self.readpool.adjust_use_unified_pool();
    }

    pub fn check_critical_cfg_with(&self, last_cfg: &Self) -> Result<(), String> {
        if last_cfg.storage.data_dir != self.storage.data_dir {
            // In tikv 3.0 the default value of storage.data-dir changed
            // from "" to "./"
            let using_default_after_upgrade =
                last_cfg.storage.data_dir.is_empty() && self.storage.data_dir == DEFAULT_DATA_DIR;

            if !using_default_after_upgrade {
                return Err(format!(
                    "storage data dir have been changed, former data dir is {}, \
                     current data dir is {}, please check if it is expected.",
                    last_cfg.storage.data_dir, self.storage.data_dir
                ));
            }
        }
        if last_cfg.rocksdb.wal_dir != self.rocksdb.wal_dir {
            return Err(format!(
                "db wal dir have been changed, former is '{}', \
                 current db wal_dir is '{}', please guarantee all data wal logs \
                 have been moved to destination directory.",
                last_cfg.rocksdb.wal_dir, self.rocksdb.wal_dir
            ));
        }

        // It's possible that `last_cfg` is not fully validated.
        let last_raftdb_dir = last_cfg
            .infer_raft_db_path(None)
            .map_err(|e| e.to_string())?;
        let last_raft_engine_dir = last_cfg
            .infer_raft_engine_path(None)
            .map_err(|e| e.to_string())?;

        // FIXME: We cannot reliably determine the actual value of
        // `last_cfg.raft_engine.enable`, because some old versions don't have
        // this field (so it is automatically interpreted as the current
        // default value). To be safe, we will check both engines regardless
        // of whether raft engine is enabled.
        if last_raftdb_dir != self.raft_store.raftdb_path {
            return Err(format!(
                "raft db dir have been changed, former is '{}', \
                current is '{}', please check if it is expected.",
                last_raftdb_dir, self.raft_store.raftdb_path
            ));
        }
        if last_cfg.raftdb.wal_dir != self.raftdb.wal_dir {
            return Err(format!(
                "raft db wal dir have been changed, former is '{}', \
                current is '{}', please check if it is expected.",
                last_cfg.raftdb.wal_dir, self.raftdb.wal_dir
            ));
        }

        if RaftDataStateMachine::raftengine_exists(Path::new(&last_raft_engine_dir))
            && last_raft_engine_dir != self.raft_engine.config.dir
        {
            return Err(format!(
                "raft engine dir have been changed, former is '{}', \
                 current is '{}', please check if it is expected.",
                last_raft_engine_dir, self.raft_engine.config.dir
            ));
        }

        // Check validation of api version change between API V1 and V1ttl only.
        // Validation between V1/V1ttl and V2 is checked in `Node::check_api_version`.
        if last_cfg.storage.api_version == 1 && self.storage.api_version == 1 {
            if last_cfg.storage.enable_ttl && !self.storage.enable_ttl {
                return Err("can't disable ttl on a ttl instance".to_owned());
            } else if !last_cfg.storage.enable_ttl && self.storage.enable_ttl {
                return Err("can't enable ttl on a non-ttl instance".to_owned());
            }
        }

        if last_cfg
            .raftdb
            .defaultcf
            .format_version
            .is_some_and(|v| v > 5)
            || last_cfg
                .rocksdb
                .defaultcf
                .format_version
                .is_some_and(|v| v > 5)
            || last_cfg
                .rocksdb
                .writecf
                .format_version
                .is_some_and(|v| v > 5)
            || last_cfg
                .rocksdb
                .lockcf
                .format_version
                .is_some_and(|v| v > 5)
            || last_cfg
                .rocksdb
                .raftcf
                .format_version
                .is_some_and(|v| v > 5)
        {
            return Err("format_version larger than 5 is unsupported".into());
        }

        Ok(())
    }

    pub fn from_file(
        path: &Path,
        unrecognized_keys: Option<&mut Vec<String>>,
    ) -> Result<Self, Box<dyn Error>> {
        let s = fs::read_to_string(path)?;
        let mut deserializer = toml::Deserializer::new(&s);
        let mut cfg = if let Some(keys) = unrecognized_keys {
            serde_ignored::deserialize(&mut deserializer, |key| keys.push(key.to_string()))
        } else {
            <TikvConfig as serde::Deserialize>::deserialize(&mut deserializer)
        }?;
        deserializer.end()?;
        cfg.cfg_path = path.display().to_string();
        Ok(cfg)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), IoError> {
        let content = ::toml::to_string(&self).unwrap();
        let mut f = fs::File::create(&path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;

        Ok(())
    }

    pub fn write_into_metrics(&self) {
        self.raft_store.write_into_metrics();
        self.rocksdb.write_into_metrics();
        self.storage.flow_control.write_into_metrics();
    }

    pub fn with_tmp() -> Result<(TikvConfig, tempfile::TempDir), IoError> {
        let tmp = tempfile::tempdir()?;
        let mut cfg = TikvConfig::default();
        cfg.storage.data_dir = tmp.path().display().to_string();
        cfg.cfg_path = tmp.path().join("config.toml").display().to_string();

        // create tmp config file
        let mut f = fs::File::create(&cfg.cfg_path)?;
        // write storage data dir to tmp config file
        let content = format!(
            r#"
            [storage]
            data-dir = "{}"
            "#,
            cfg.storage.data_dir,
        );
        f.write_all(content.as_bytes())?;
        f.sync_all()?;

        Ok((cfg, tmp))
    }

    fn suggested_memory_usage_limit() -> ReadableSize {
        let total = SysQuota::memory_limit_in_bytes();
        // Reserve some space for page cache. The
        ReadableSize((total as f64 * MEMORY_USAGE_LIMIT_RATE) as u64)
    }

    pub fn build_shared_rocks_env(
        &self,
        key_manager: Option<Arc<DataKeyManager>>,
        limiter: Option<Arc<IoRateLimiter>>,
    ) -> Result<Arc<Env>, String> {
        let env = get_env(key_manager, limiter)?;
        if !self.raft_engine.enable {
            // RocksDB makes sure there are at least `max_background_flushes`
            // high-priority workers in env. That is not enough when multiple
            // RocksDB instances share the same env. We manually configure the
            // worker count in this case.
            env.set_high_priority_background_threads(
                self.raftdb.max_background_flushes + self.rocksdb.max_background_flushes,
            );
        }
        Ok(env)
    }
}

pub fn validate_and_persist_config(config: &mut TikvConfig, persist: bool) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    let mut last_cfg = get_last_config(&config.storage.data_dir);
    if let Some(last_cfg) = &mut last_cfg {
        // Validate and normalize the last persisted configuration to ensure it's in a
        // consistent state before using it as a reference for inheritance. This helps
        // prevent propagating invalid or deprecated settings to the current
        // configuration.
        last_cfg.compatible_adjust(None);
        if let Err(e) = last_cfg.validate() {
            warn!("last_tikv.toml is invalid but ignored: {:?}", e);
        }
        // Inherit critical configuration values from the validated last configuration
        // if they are not explicitly set in the current configuration. This ensures
        // smooth upgrades while preserving user-specified settings.
        config.compatible_adjust(Some(last_cfg));
    } else {
        // For newly deployed nodes or when no previous configuration exists,
        // initialize with default values. No inheritance is needed in this case.
        config.compatible_adjust(None);
    }

    if let Err(e) = config.validate() {
        return Err(format!("invalid configuration: {}", e));
    }
    if let Err(e) = config.optional_default_cfg_adjust_with(&last_cfg) {
        return Err(format!(
            "failed to adjust optional default configuration: {}",
            e
        ));
    }

    if let Some(ref last_cfg) = last_cfg {
        if let Err(e) = config.check_critical_cfg_with(last_cfg) {
            return Err(format!("critical config check failed: {}", e));
        }
    }

    if persist {
        if let Err(e) = persist_config(config) {
            return Err(format!("persist critical config failed: {}", e));
        }
    }
    Ok(())
}

pub fn get_last_config(data_dir: &str) -> Option<TikvConfig> {
    let store_path = Path::new(data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    if last_cfg_path.exists() {
        return Some(
            TikvConfig::from_file(&last_cfg_path, None).unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {}, err {}",
                    last_cfg_path.display(),
                    e
                );
            }),
        );
    }
    None
}

/// Persists config to `last_tikv.toml`
pub fn persist_config(config: &TikvConfig) -> Result<(), String> {
    let store_path = Path::new(&config.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    let tmp_cfg_path = store_path.join(TMP_CONFIG_FILE);

    let same_as_last_cfg = fs::read_to_string(&last_cfg_path)
        .is_ok_and(|last_cfg| toml::to_string(&config).unwrap() == last_cfg);
    if same_as_last_cfg {
        return Ok(());
    }

    // Create parent directory if missing.
    if let Err(e) = fs::create_dir_all(store_path) {
        return Err(format!(
            "create parent directory '{}' failed: {}",
            store_path.to_str().unwrap(),
            e
        ));
    }

    // Persist current configurations to temporary file.
    if let Err(e) = config.write_to_file(&tmp_cfg_path) {
        return Err(format!(
            "persist config to '{}' failed: {}",
            tmp_cfg_path.to_str().unwrap(),
            e
        ));
    }

    // Rename temporary file to last config file.
    if let Err(e) = fs::rename(&tmp_cfg_path, &last_cfg_path) {
        return Err(format!(
            "rename config file from '{}' to '{}' failed: {}",
            tmp_cfg_path.to_str().unwrap(),
            last_cfg_path.to_str().unwrap(),
            e
        ));
    }

    Ok(())
}

pub fn write_config<P: AsRef<Path>>(path: P, content: &[u8]) -> CfgResult<()> {
    let tmp_cfg_path = match path.as_ref().parent() {
        Some(p) => p.join(TMP_CONFIG_FILE),
        None => {
            return Err(Box::new(IoError::other(format!(
                "failed to get parent path of config file: {}",
                path.as_ref().display()
            ))));
        }
    };
    {
        let mut f = fs::File::create(&tmp_cfg_path)?;
        f.write_all(content)?;
        f.sync_all()?;
    }
    fs::rename(&tmp_cfg_path, &path)?;
    Ok(())
}

// convert tikv config to a flatten array.
pub fn to_flatten_config_info(cfg: &TikvConfig) -> Vec<Value> {
    fn to_cfg_value(default_value: &Value, cfg_value: Option<&Value>, key: &str) -> Value {
        let mut res = Map::with_capacity(2);
        res.insert("Name".into(), Value::String(key.into()));
        res.insert("DefaultValue".into(), default_value.clone());
        if let Some(cfg_val) = cfg_value {
            if default_value != cfg_val {
                res.insert("ValueInFile".into(), cfg_val.clone());
            }
        }

        Value::Object(res)
    }

    // configs that should not be flatten because the config type is HashMap instead
    // of submodule.
    lazy_static! {
        static ref NO_FLATTEN_CFGS: HashSet<&'static str> = {
            let mut set = HashSet::new();
            set.insert("server.labels");
            set
        };
    }

    fn flatten_value(
        default_obj: &Map<String, Value>,
        value_obj: &Map<String, Value>,
        key_buf: &mut String,
        res: &mut Vec<Value>,
    ) {
        for (k, v) in default_obj.iter() {
            let cfg_val = value_obj.get(k);
            let prev_len = key_buf.len();
            if !key_buf.is_empty() {
                key_buf.push('.');
            }
            key_buf.push_str(k);
            if v.is_object() && !NO_FLATTEN_CFGS.contains(key_buf.as_str()) {
                flatten_value(
                    v.as_object().unwrap(),
                    cfg_val.unwrap().as_object().unwrap(),
                    key_buf,
                    res,
                );
            } else {
                res.push(to_cfg_value(v, cfg_val, key_buf));
            }
            key_buf.truncate(prev_len);
        }
    }

    let cfg_value = to_value(cfg).unwrap();
    let default_value = to_value(TikvConfig::default()).unwrap();

    let mut key_buf = String::new();
    let mut res = Vec::new();
    flatten_value(
        default_value.as_object().unwrap(),
        cfg_value.as_object().unwrap(),
        &mut key_buf,
        &mut res,
    );
    res
}

lazy_static! {
    pub static ref TIKVCONFIG_TYPED: ConfigChange = TikvConfig::default().typed();
}

fn serde_to_online_config(name: String) -> String {
    let res = name.replace("raftstore", "raft_store").replace('-', "_");
    match res.as_ref() {
        "raft_store.store_pool_size" | "raft_store.store_max_batch_size" => {
            res.replace("store_", "store_batch_system.")
        }
        "raft_store.apply_pool_size" | "raft_store.apply_max_batch_size" => {
            res.replace("apply_", "apply_batch_system.")
        }
        _ => res,
    }
}

fn to_config_change(change: HashMap<String, String>) -> CfgResult<ConfigChange> {
    fn helper(
        mut fields: Vec<String>,
        dst: &mut ConfigChange,
        typed: &ConfigChange,
        value: String,
    ) -> CfgResult<()> {
        if let Some(field) = fields.pop() {
            return match typed.get(&field) {
                None => Err(format!("unexpect fields: {}", field).into()),
                Some(ConfigValue::Skip) => {
                    Err(format!("config {} can not be changed", field).into())
                }
                Some(ConfigValue::Module(m)) => {
                    if let ConfigValue::Module(n_dst) = dst
                        .entry(field)
                        .or_insert_with(|| ConfigValue::Module(HashMap::new()))
                    {
                        return helper(fields, n_dst, m, value);
                    }
                    panic!("unexpect config value");
                }
                Some(v) => {
                    if fields.is_empty() {
                        return match to_change_value(&value, v) {
                            Err(_) => Err(format!("failed to parse: {}", value).into()),
                            Ok(v) => {
                                dst.insert(field, v);
                                Ok(())
                            }
                        };
                    }
                    let c: Vec<_> = fields.into_iter().rev().collect();
                    Err(format!("unexpect fields: {}", c[..].join(".")).into())
                }
            };
        }
        Ok(())
    }
    let mut res = HashMap::new();
    for (mut name, value) in change {
        name = serde_to_online_config(name);
        let fields: Vec<_> = name
            .as_str()
            .split('.')
            .map(|s| s.to_owned())
            .rev()
            .collect();
        helper(fields, &mut res, &TIKVCONFIG_TYPED, value)?;
    }
    Ok(res)
}

fn to_change_value(v: &str, typed: &ConfigValue) -> CfgResult<ConfigValue> {
    let v = v.trim_matches('\"');
    let res = match typed {
        ConfigValue::Duration(_) => ConfigValue::from(v.parse::<ReadableDuration>()?),
        ConfigValue::Size(_) => ConfigValue::from(v.parse::<ReadableSize>()?),
        ConfigValue::U64(_) => ConfigValue::from(v.parse::<u64>()?),
        ConfigValue::F64(_) => ConfigValue::from(v.parse::<f64>()?),
        ConfigValue::U32(_) => ConfigValue::from(v.parse::<u32>()?),
        ConfigValue::I32(_) => ConfigValue::from(v.parse::<i32>()?),
        ConfigValue::Usize(_) => ConfigValue::from(v.parse::<usize>()?),
        ConfigValue::Bool(_) => ConfigValue::from(v.parse::<bool>()?),
        ConfigValue::String(_) => ConfigValue::String(v.to_owned()),
        ConfigValue::Schedule(_) => {
            let schedule = v.parse::<ReadableSchedule>()?;
            ConfigValue::from(schedule)
        }
        ConfigValue::Skip | ConfigValue::None | ConfigValue::Module(_) => unreachable!(),
    };
    Ok(res)
}

fn to_toml_encode(change: HashMap<String, String>) -> CfgResult<HashMap<String, String>> {
    fn helper(mut fields: Vec<String>, typed: &ConfigChange) -> CfgResult<bool> {
        if let Some(field) = fields.pop() {
            match typed.get(&field) {
                None | Some(ConfigValue::Skip) => Err(Box::new(IoError::other(format!(
                    "failed to get field: {}",
                    field
                )))),
                Some(ConfigValue::Module(m)) => helper(fields, m),
                Some(c) => {
                    if !fields.is_empty() {
                        return Err(Box::new(IoError::other(format!(
                            "unexpect fields: {:?}",
                            fields
                        ))));
                    }
                    match c {
                        ConfigValue::Duration(_)
                        | ConfigValue::Size(_)
                        | ConfigValue::String(_) => Ok(true),
                        ConfigValue::None => Err(Box::new(IoError::other(format!(
                            "unexpect none field: {:?}",
                            c
                        )))),
                        _ => Ok(false),
                    }
                }
            }
        } else {
            Err(Box::new(IoError::other("failed to get field")))
        }
    }
    let mut dst = HashMap::new();
    for (name, value) in change {
        let online_config_name = serde_to_online_config(name.clone());
        let fields: Vec<_> = online_config_name
            .as_str()
            .split('.')
            .map(|s| s.to_owned())
            .rev()
            .collect();
        if helper(fields, &TIKVCONFIG_TYPED)? {
            dst.insert(name.replace('_', "-"), format!("\"{}\"", value));
        } else {
            dst.insert(name.replace('_', "-"), value);
        }
    }
    Ok(dst)
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Module {
    Readpool,
    Server,
    Metric,
    Raftstore,
    Coprocessor,
    Pd,
    Rocksdb,
    Raftdb,
    RaftEngine,
    InMemoryEngine,
    Storage,
    Security,
    Encryption,
    Import,
    Backup,
    PessimisticTxn,
    Gc,
    Split,
    Cdc,
    ResolvedTs,
    ResourceMetering,
    ResourceControl,
    BackupStream,
    Quota,
    Log,
    Memory,
    Unknown(String),
}

impl From<&str> for Module {
    fn from(m: &str) -> Module {
        match m {
            "readpool" => Module::Readpool,
            "server" => Module::Server,
            "metric" => Module::Metric,
            "raft_store" => Module::Raftstore,
            "coprocessor" => Module::Coprocessor,
            "pd" => Module::Pd,
            "split" => Module::Split,
            "rocksdb" => Module::Rocksdb,
            "raftdb" => Module::Raftdb,
            "raft_engine" => Module::RaftEngine,
            "in_memory_engine" => Module::InMemoryEngine,
            "storage" => Module::Storage,
            "security" => Module::Security,
            "import" => Module::Import,
            "backup" => Module::Backup,
            "log_backup" => Module::BackupStream,
            "pessimistic_txn" => Module::PessimisticTxn,
            "gc" => Module::Gc,
            "cdc" => Module::Cdc,
            "resolved_ts" => Module::ResolvedTs,
            "resource_metering" => Module::ResourceMetering,
            "resource_control" => Module::ResourceControl,
            "quota" => Module::Quota,
            "log" => Module::Log,
            "memory" => Module::Memory,
            n => Module::Unknown(n.to_owned()),
        }
    }
}

/// ConfigController use to register each module's config manager,
/// and dispatch the change of config to corresponding managers or
/// return the change if the incoming change is invalid.
#[derive(Default, Clone)]
pub struct ConfigController {
    inner: Arc<RwLock<ConfigInner>>,
}

#[derive(Default)]
struct ConfigInner {
    current: TikvConfig,
    config_mgrs: HashMap<Module, Box<dyn ConfigManager>>,
}

impl ConfigController {
    pub fn new(current: TikvConfig) -> Self {
        ConfigController {
            inner: Arc::new(RwLock::new(ConfigInner {
                current,
                config_mgrs: HashMap::new(),
            })),
        }
    }

    pub fn update(&self, change: HashMap<String, String>) -> CfgResult<()> {
        let diff = to_config_change(change.clone())?;
        self.update_impl(diff, Some(change), true)
    }

    pub fn update_without_persist(&self, change: HashMap<String, String>) -> CfgResult<()> {
        let diff = to_config_change(change.clone())?;
        self.update_impl(diff, Some(change), false)
    }

    pub fn update_from_toml_file(&self) -> CfgResult<()> {
        let current = self.get_current();
        match TikvConfig::from_file(Path::new(&current.cfg_path), None) {
            Ok(incoming) => {
                let diff = current.diff(&incoming);
                self.update_impl(diff, None, true)
            }
            Err(e) => Err(e),
        }
    }

    fn update_impl(
        &self,
        mut diff: HashMap<String, ConfigValue>,
        change: Option<HashMap<String, String>>,
        persist: bool,
    ) -> CfgResult<()> {
        diff = {
            let incoming = self.get_current();
            let mut updated = incoming.clone();
            updated.update(diff)?;
            // Config might be adjusted in `validate`.
            updated.validate()?;
            incoming.diff(&updated)
        };
        let mut inner = self.inner.write().unwrap();
        let mut to_update = HashMap::with_capacity(diff.len());
        for (name, change) in diff.into_iter() {
            match change {
                ConfigValue::Module(change) => {
                    // update a submodule's config only if changes had been successfully
                    // dispatched to corresponding config manager, to avoid dispatch change twice
                    if let Some(mgr) = inner.config_mgrs.get_mut(&Module::from(name.as_str())) {
                        if let Err(e) = mgr.dispatch(change.clone()) {
                            // we already verified the correctness at the beginning of this
                            // function.
                            inner.current.update(to_update).unwrap();
                            return Err(e);
                        }
                    }
                    to_update.insert(name, ConfigValue::Module(change));
                }
                _ => {
                    let _ = to_update.insert(name, change);
                }
            }
        }
        debug!("all config change had been dispatched"; "change" => ?to_update);
        // we already verified the correctness at the beginning of this function.
        inner.current.update(to_update).unwrap();

        if !persist {
            return Ok(());
        }

        // Write change to the config file
        if let Some(change) = change {
            let content = {
                let change = to_toml_encode(change)?;
                let src = if Path::new(&inner.current.cfg_path).exists() {
                    fs::read_to_string(&inner.current.cfg_path)?
                } else {
                    String::new()
                };
                let mut t = TomlWriter::new();
                t.write_change(src, change);
                t.finish()
            };
            write_config(&inner.current.cfg_path, &content)?;
        }
        Ok(())
    }

    pub fn update_config(&self, name: &str, value: &str) -> CfgResult<()> {
        let mut m = HashMap::new();
        m.insert(name.to_owned(), value.to_owned());
        self.update(m)
    }

    pub fn register(&self, module: Module, cfg_mgr: Box<dyn ConfigManager>) {
        let mut inner = self.inner.write().unwrap();
        if inner.config_mgrs.insert(module.clone(), cfg_mgr).is_some() {
            warn!("config manager for module {:?} already registered", module)
        }
    }

    pub fn get_current(&self) -> TikvConfig {
        self.inner.read().unwrap().current.clone()
    }

    pub fn get_engine_type(&self) -> &'static str {
        if self.get_current().storage.engine == EngineType::RaftKv2 {
            return "partitioned-raft-kv";
        }
        "raft-kv"
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, mpsc::channel},
        time::Duration,
    };

    use api_version::{ApiV1, KvFormat};
    use engine_rocks::raw::LRUCacheOptions;
    use engine_traits::{CfOptions as _, CfOptionsExt, DbOptions as _, DbOptionsExt};
    use futures::executor::block_on;
    use grpcio::ResourceQuota;
    use in_memory_engine::config::InMemoryEngineConfigManager;
    use itertools::Itertools;
    use kvproto::kvrpcpb::CommandPri;
    use raft_log_engine::RaftLogEngine;
    use raftstore::{
        coprocessor::{
            config::{RAFTSTORE_V2_SPLIT_SIZE, SPLIT_SIZE},
            region_info_accessor::MockRegionInfoProvider,
        },
        store::{
            BIG_REGION_CPU_OVERLOAD_THRESHOLD_RATIO, DEFAULT_BIG_REGION_BYTE_THRESHOLD,
            DEFAULT_BIG_REGION_QPS_THRESHOLD, DEFAULT_BYTE_THRESHOLD, DEFAULT_QPS_THRESHOLD,
            REGION_CPU_OVERLOAD_THRESHOLD_RATIO,
        },
    };
    use slog::Level;
    use tempfile::Builder;
    use test_util::assert_eq_debug;
    use tikv_kv::RocksEngine as RocksDBEngine;
    use tikv_util::{
        config::{GIB, VersionTrack},
        logger::get_log_level,
        quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
        sys::SysQuota,
        worker::{ReceiverWrapper, dummy_scheduler},
    };

    use super::*;
    use crate::{
        server::{config::ServerConfigManager, ttl::TtlCheckerTask},
        storage::{
            Storage, TestStorageBuilder,
            config_manager::StorageConfigManger,
            lock_manager::MockLockManager,
            txn::flow_controller::{EngineFlowController, FlowController},
        },
    };

    fn create_mock_raftdb(path: &Path) {
        fs::create_dir_all(path).unwrap();
        fs::File::create(path.join("CURRENT")).unwrap();
    }

    fn create_mock_raftengine(path: &Path) {
        fs::create_dir_all(path).unwrap();
        fs::File::create(path.join("0000000000000001.raftlog")).unwrap();
    }

    fn create_mock_kv_data(path: &Path) {
        fs::create_dir_all(path.join("db")).unwrap();
        fs::File::create(path.join("db").join("CURRENT")).unwrap();
    }

    #[test]
    fn test_check_critical_cfg_with() {
        let mut tikv_cfg = TikvConfig::default();
        let last_cfg = TikvConfig::default();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        let mut tikv_cfg = TikvConfig::default();
        let mut last_cfg = TikvConfig::default();
        tikv_cfg.rocksdb.wal_dir = "/data/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap_err();

        last_cfg.rocksdb.wal_dir = "/data/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        let mut tikv_cfg = TikvConfig::default();
        let mut last_cfg = TikvConfig::default();
        tikv_cfg.storage.data_dir = "/data1".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap_err();

        last_cfg.storage.data_dir = "/data1".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        // Enable Raft Engine.
        let mut tikv_cfg = TikvConfig::default();
        let mut last_cfg = TikvConfig::default();
        tikv_cfg.raft_engine.enable = true;
        last_cfg.raft_engine.enable = true;

        tikv_cfg.raft_engine.mut_config().dir = "/raft/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        // no actual raft engine data
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        last_cfg.raft_engine.mut_config().dir = "/raft/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        // Disable Raft Engine and uses RocksDB.
        let mut tikv_cfg = TikvConfig::default();
        let mut last_cfg = TikvConfig::default();
        tikv_cfg.raft_engine.enable = false;
        last_cfg.raft_engine.enable = false;

        tikv_cfg.raftdb.wal_dir = "/raft/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap_err();

        last_cfg.raftdb.wal_dir = "/raft/wal_dir".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        tikv_cfg.raft_store.raftdb_path = "/raft_path".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap_err();

        last_cfg.raft_store.raftdb_path = "/raft_path".to_owned();
        tikv_cfg.validate().unwrap();
        tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();

        // Check api version.
        {
            tikv_cfg.rocksdb.titan.enabled = Some(false);
            let cases = [
                (ApiVersion::V1, ApiVersion::V1, true),
                (ApiVersion::V1, ApiVersion::V1ttl, false),
                (ApiVersion::V1, ApiVersion::V2, true),
                (ApiVersion::V1ttl, ApiVersion::V1, false),
                (ApiVersion::V1ttl, ApiVersion::V1ttl, true),
                (ApiVersion::V1ttl, ApiVersion::V2, true),
                (ApiVersion::V2, ApiVersion::V1, true),
                (ApiVersion::V2, ApiVersion::V1ttl, true),
                (ApiVersion::V2, ApiVersion::V2, true),
            ];
            for (from_api, to_api, expected) in cases {
                last_cfg.storage.set_api_version(from_api);
                tikv_cfg.storage.set_api_version(to_api);
                tikv_cfg.validate().unwrap();
                assert_eq!(
                    tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok(),
                    expected
                );
            }
        }

        let test_dir = tempfile::Builder::new()
            .tempdir()
            .unwrap()
            .into_path()
            .join("unittest_raft_engine_dir");
        let data_dir = test_dir.join("data");

        // simulate tikv restart
        // enable raft engine: true
        // need dump data from raftdb: false
        // custom raft dir: true
        {
            let raft_dir = test_dir.join("raft");
            tikv_cfg = TikvConfig::default();
            last_cfg = TikvConfig::default();

            last_cfg.raft_engine.mut_config().dir = raft_dir.to_str().unwrap().to_owned();
            last_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            last_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();
            tikv_cfg.raft_engine.mut_config().dir = raft_dir.to_str().unwrap().to_owned();
            tikv_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            tikv_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();

            create_mock_raftengine(&raft_dir);
            create_mock_kv_data(&data_dir);

            tikv_cfg.validate().unwrap();
            tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();
            fs::remove_dir_all(&test_dir).unwrap();
        }

        // simulate tikv restart
        // enable raft engine: true
        // need dump data from raftdb: false
        // custom raft dir: false
        {
            tikv_cfg = TikvConfig::default();
            last_cfg = TikvConfig::default();

            last_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            last_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            last_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();
            tikv_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            tikv_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            tikv_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();

            create_mock_kv_data(&data_dir);
            create_mock_raftengine(&data_dir.join("raft-engine"));

            tikv_cfg.validate().unwrap();
            tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();
            fs::remove_dir_all(&test_dir).unwrap();
        }

        // simulate tikv update
        // enable raft engine: true
        // need dump data from raftdb: true
        // custom raft dir: false
        {
            tikv_cfg = TikvConfig::default();
            last_cfg = TikvConfig::default();

            last_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            last_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            last_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();
            tikv_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            tikv_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            tikv_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();

            create_mock_kv_data(&data_dir);
            create_mock_raftdb(&data_dir.join("raft"));

            tikv_cfg.validate().unwrap();
            tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();
            fs::remove_dir_all(&test_dir).unwrap();
        }

        // multi raft engine dir
        {
            tikv_cfg = TikvConfig::default();
            last_cfg = TikvConfig::default();

            last_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            last_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            last_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();
            tikv_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            tikv_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            tikv_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();

            create_mock_kv_data(&data_dir);
            create_mock_raftdb(&data_dir.join("raft"));
            create_mock_raftengine(&data_dir.join("raft-engine"));

            tikv_cfg.validate().unwrap_err();
            tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();
            fs::remove_dir_all(&test_dir).unwrap();
        }

        // simulate tikv update with custom raft dir
        // enable raft engine: true
        // need dump data from raftdb: true
        // custom raft dir: false
        {
            tikv_cfg = TikvConfig::default();
            last_cfg = TikvConfig::default();

            last_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            last_cfg.raft_store.raftdb_path = test_dir.join("raft").to_str().unwrap().to_owned();
            last_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();
            tikv_cfg.raft_engine.mut_config().dir =
                test_dir.join("raft-engine").to_str().unwrap().to_owned();
            tikv_cfg.raft_store.raftdb_path = test_dir.join("raft").to_str().unwrap().to_owned();
            tikv_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();

            create_mock_kv_data(&data_dir);
            create_mock_raftdb(&test_dir.join("raft"));

            tikv_cfg.validate().unwrap();
            tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();
            fs::remove_dir_all(&test_dir).unwrap();
        }

        // simulate tikv update with custom raft dir
        // enable raft engine: true
        // need dump data from raftdb: true
        // custom raft dir: false
        {
            tikv_cfg = TikvConfig::default();
            last_cfg = TikvConfig::default();

            last_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            last_cfg.raft_store.raftdb_path = test_dir.join("raft").to_str().unwrap().to_owned();
            last_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();
            tikv_cfg.raft_engine.mut_config().dir = "".to_owned();
            tikv_cfg.raft_store.raftdb_path = test_dir.join("raft").to_str().unwrap().to_owned();
            tikv_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();

            create_mock_kv_data(&data_dir);
            create_mock_raftdb(&test_dir.join("raft"));

            tikv_cfg.validate().unwrap();
            assert_eq!(
                tikv_cfg.raft_engine.config.dir,
                test_dir.join("raft").join("raft-engine").to_str().unwrap()
            );
            tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();
            fs::remove_dir_all(&test_dir).unwrap();
        }

        // simulate tikv downgrade to raftdb
        // need dump data from raft-engine
        // custom raft dir: false
        {
            tikv_cfg = TikvConfig::default();
            last_cfg = TikvConfig::default();

            last_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            last_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            last_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();
            last_cfg.raft_engine.enable = true;

            tikv_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            tikv_cfg.raft_engine.enable = false;
            tikv_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            tikv_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();

            create_mock_kv_data(&data_dir);
            create_mock_raftengine(&data_dir.join("raft-engine"));

            tikv_cfg.validate().unwrap();
            tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap();
            fs::remove_dir_all(&test_dir).unwrap();
        }

        {
            tikv_cfg = TikvConfig::default();
            last_cfg = TikvConfig::default();

            last_cfg.raft_engine.mut_config().dir =
                data_dir.join("raft-engine").to_str().unwrap().to_owned();
            last_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            last_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();
            last_cfg.raft_engine.enable = true;

            tikv_cfg.raft_engine.mut_config().dir = "".to_owned();
            tikv_cfg.raft_engine.enable = false;
            tikv_cfg.raft_store.raftdb_path = data_dir.join("raft").to_str().unwrap().to_owned();
            tikv_cfg.storage.data_dir = data_dir.to_str().unwrap().to_owned();

            create_mock_kv_data(&data_dir);
            create_mock_raftengine(&data_dir.join("raft-engine"));

            tikv_cfg.validate().unwrap_err();
            tikv_cfg.check_critical_cfg_with(&last_cfg).unwrap_err();
            fs::remove_dir_all(&test_dir).unwrap();
        }
    }

    #[test]
    fn test_last_cfg_modified() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        let store_path = Path::new(&cfg.storage.data_dir);
        let last_cfg_path = store_path.join(LAST_CONFIG_FILE);

        cfg.write_to_file(&last_cfg_path).unwrap();

        let mut last_cfg_metadata = last_cfg_path.metadata().unwrap();
        let first_modified = last_cfg_metadata.modified().unwrap();

        // not write to file when config is the equivalent of last one.
        persist_config(&cfg).unwrap();
        last_cfg_metadata = last_cfg_path.metadata().unwrap();
        assert_eq!(last_cfg_metadata.modified().unwrap(), first_modified);

        // write to file when config is the inequivalent of last one.
        cfg.log.level = slog::Level::Warning.into();
        persist_config(&cfg).unwrap();
        last_cfg_metadata = last_cfg_path.metadata().unwrap();
        assert_ne!(last_cfg_metadata.modified().unwrap(), first_modified);
    }

    #[test]
    fn test_persist_cfg() {
        let dir = Builder::new().prefix("test_persist_cfg").tempdir().unwrap();
        let path_buf = dir.path().join(LAST_CONFIG_FILE);
        let file = path_buf.as_path();
        let (s1, s2) = ("/xxx/wal_dir".to_owned(), "/yyy/wal_dir".to_owned());

        let mut tikv_cfg = TikvConfig::default();

        tikv_cfg.rocksdb.wal_dir = s1.clone();
        tikv_cfg.raftdb.wal_dir = s2.clone();
        tikv_cfg.coprocessor.region_split_size = Some(ReadableSize::mb(16));
        tikv_cfg.write_to_file(file).unwrap();
        let cfg_from_file = TikvConfig::from_file(file, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                file.display(),
                e
            );
        });
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s1);
        assert_eq!(cfg_from_file.raftdb.wal_dir, s2);

        // write critical config when exist.
        tikv_cfg.rocksdb.wal_dir = s2.clone();
        tikv_cfg.raftdb.wal_dir = s1.clone();
        tikv_cfg.write_to_file(file).unwrap();
        let cfg_from_file = TikvConfig::from_file(file, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                file.display(),
                e
            );
        });
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s2);
        assert_eq!(cfg_from_file.raftdb.wal_dir, s1);
        assert_eq!(
            cfg_from_file.coprocessor.region_split_size.unwrap(),
            tikv_cfg.coprocessor.region_split_size.unwrap()
        );
    }

    #[test]
    fn test_flatten_cfg() {
        let mut cfg = TikvConfig::default();
        cfg.server.labels.insert("zone".into(), "test".into());
        cfg.raft_store.raft_log_gc_count_limit = Some(123);

        let flattened = to_flatten_config_info(&cfg);

        let mut expected = HashMap::new();
        let mut labels = Map::new();
        labels.insert("zone".into(), Value::String("test".into()));
        expected.insert("server.labels", Value::Object(labels));
        expected.insert(
            "raftstore.raft-log-gc-count-limit",
            Value::Number(123.into()),
        );

        for v in &flattened {
            let obj = v.as_object().unwrap();
            if let Some(v) = expected.get(&obj["Name"].as_str().unwrap()) {
                assert_eq!(v, &obj["ValueInFile"]);
            } else {
                assert!(!obj.contains_key("ValueInFile"));
            }
        }
    }

    #[test]
    fn test_create_parent_dir_if_missing() {
        let root_path = Builder::new()
            .prefix("test_create_parent_dir_if_missing")
            .tempdir()
            .unwrap();
        let path = root_path.path().join("not_exist_dir");

        let mut tikv_cfg = TikvConfig::default();
        tikv_cfg.storage.data_dir = path.as_path().to_str().unwrap().to_owned();
        persist_config(&tikv_cfg).unwrap();
    }

    #[test]
    fn test_keepalive_check() {
        let mut tikv_cfg = TikvConfig::default();
        tikv_cfg.pd.endpoints = vec!["".to_owned()];
        let dur = tikv_cfg.raft_store.raft_heartbeat_interval();
        tikv_cfg.server.grpc_keepalive_time = ReadableDuration(dur);
        tikv_cfg.validate().unwrap_err();
        tikv_cfg.server.grpc_keepalive_time = ReadableDuration(dur * 2);
        tikv_cfg.validate().unwrap();
    }

    #[test]
    fn test_illegal_backupstream_config_parm() {
        let mut backup_stream_cfg = BackupStreamConfig::default();
        backup_stream_cfg.initial_scan_rate_limit.0 = 0;
        backup_stream_cfg.validate().unwrap_err();
        backup_stream_cfg.initial_scan_rate_limit.0 = 1000;
        backup_stream_cfg.validate().unwrap_err();
        backup_stream_cfg.initial_scan_rate_limit.0 = 1024;
        backup_stream_cfg.validate().unwrap();
        backup_stream_cfg.initial_scan_rate_limit.0 = 2048;
        backup_stream_cfg.validate().unwrap();
    }

    #[test]
    fn test_block_size() {
        let mut tikv_cfg = TikvConfig::default();
        tikv_cfg.pd.endpoints = vec!["".to_owned()];
        tikv_cfg.rocksdb.defaultcf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.lockcf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.writecf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.raftcf.block_size = ReadableSize::gb(10);
        tikv_cfg.raftdb.defaultcf.block_size = ReadableSize::gb(10);
        tikv_cfg.validate().unwrap_err();
        tikv_cfg.rocksdb.defaultcf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.lockcf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.writecf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.raftcf.block_size = ReadableSize::kb(10);
        tikv_cfg.raftdb.defaultcf.block_size = ReadableSize::kb(10);
        tikv_cfg.validate().unwrap();
    }

    #[test]
    fn test_rocks_rate_limit_zero() {
        let mut tikv_cfg = TikvConfig::default();
        tikv_cfg.rocksdb.rate_bytes_per_sec = ReadableSize(0);
        let resource = tikv_cfg
            .rocksdb
            .build_resources(Arc::new(Env::default()), tikv_cfg.storage.engine);
        tikv_cfg
            .rocksdb
            .build_opt(&resource, tikv_cfg.storage.engine);
    }

    #[test]
    fn test_parse_log_level() {
        #[derive(Serialize, Deserialize, Debug)]
        struct LevelHolder {
            v: LogLevel,
        }

        let legal_cases = vec![
            ("fatal", Level::Critical),
            ("error", Level::Error),
            ("warn", Level::Warning),
            ("debug", Level::Debug),
            ("trace", Level::Trace),
            ("info", Level::Info),
        ];
        for (serialized, deserialized) in legal_cases {
            let holder = LevelHolder {
                v: deserialized.into(),
            };
            let res_string = toml::to_string(&holder).unwrap();
            let exp_string = format!("v = \"{}\"\n", serialized);
            assert_eq!(res_string, exp_string);
            let res_value: LevelHolder = toml::from_str(&exp_string).unwrap();
            assert_eq!(res_value.v, deserialized.into());
        }

        let compatibility_cases = vec![("warning", Level::Warning), ("critical", Level::Critical)];
        for (serialized, deserialized) in compatibility_cases {
            let variant_string = format!("v = \"{}\"\n", serialized);
            let res_value: LevelHolder = toml::from_str(&variant_string).unwrap();
            assert_eq!(res_value.v, deserialized.into());
        }

        let illegal_cases = vec!["foobar", ""];
        for case in illegal_cases {
            let string = format!("v = \"{}\"\n", case);
            toml::from_str::<LevelHolder>(&string).unwrap_err();
        }
    }

    #[test]
    fn test_write_metrics() {
        use raftstore::store::config::CONFIG_RAFTSTORE_GAUGE;
        let mut cfg = TikvConfig::default();
        cfg.rocksdb.max_background_jobs = 5;
        cfg.rocksdb.wal_recovery_mode = DBRecoveryMode::SkipAnyCorruptedRecords;
        cfg.rocksdb.defaultcf.block_size = ReadableSize::kb(64);
        cfg.raft_store.inspect_interval = ReadableDuration::secs(15);
        cfg.raft_store.reactive_memory_lock_tick_interval = ReadableDuration::secs(3);
        cfg.raft_store.reactive_memory_lock_timeout_tick = 10;
        cfg.raft_store.report_region_buckets_tick_interval = ReadableDuration::secs(20);
        cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(60);
        cfg.raft_store.unreachable_backoff = ReadableDuration::secs(5);
        cfg.validate().unwrap();

        CONFIG_ROCKSDB_DB_GAUGE.reset();
        CONFIG_ROCKSDB_CF_GAUGE.reset();
        CONFIG_RAFTSTORE_GAUGE.reset();

        cfg.write_into_metrics();

        let wal_mode_value = CONFIG_ROCKSDB_DB_GAUGE
            .with_label_values(&["rocksdb", "wal_recovery_mode"])
            .get();
        assert_eq!(
            wal_mode_value as u64,
            DBRecoveryMode::SkipAnyCorruptedRecords as u64
        );

        let block_size_value = CONFIG_ROCKSDB_CF_GAUGE
            .with_label_values(&["default", "block_size"])
            .get();
        assert_eq!(block_size_value as u64, ReadableSize::kb(64).0);

        let max_bytes_for_level_base = CONFIG_ROCKSDB_CF_GAUGE
            .with_label_values(&["default", "max_bytes_for_level_base"])
            .get();
        assert_eq!(
            max_bytes_for_level_base as u64,
            cfg.rocksdb.defaultcf.max_bytes_for_level_base.0
        );

        let compression_level_0 = CONFIG_ROCKSDB_CF_GAUGE
            .with_label_values(&["default", "compression_level_0"])
            .get();
        assert_eq!(
            compression_level_0 as u64,
            cfg.rocksdb.defaultcf.compression_per_level[0] as u64
        );
        let disable_auto_compactions = CONFIG_ROCKSDB_CF_GAUGE
            .with_label_values(&["default", "disable_auto_compactions"])
            .get();
        assert_eq!(
            disable_auto_compactions as u64,
            cfg.rocksdb.defaultcf.disable_auto_compactions as u64
        );

        let inspect_interval = CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["inspect_interval"])
            .get();
        assert_eq!(inspect_interval, 15.0);

        let reactive_tick = CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["reactive_memory_lock_tick_interval"])
            .get();
        assert_eq!(reactive_tick, 3.0);

        let reactive_timeout = CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["reactive_memory_lock_timeout_tick"])
            .get();
        assert_eq!(reactive_timeout, 10.0);

        let region_buckets = CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["report_region_buckets_tick_interval"])
            .get();
        assert_eq!(region_buckets, 20.0);

        let warmup_duration = CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["max_entry_cache_warmup_duration"])
            .get();
        assert_eq!(warmup_duration, 60.0);

        let unreachable_backoff = CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["unreachable_backoff"])
            .get();
        assert_eq!(unreachable_backoff, 5.0);
    }

    #[test]
    fn test_to_config_change() {
        assert_eq!(
            to_change_value("10h", &ConfigValue::Duration(0)).unwrap(),
            ConfigValue::from(ReadableDuration::hours(10))
        );
        assert_eq!(
            to_change_value("100MB", &ConfigValue::Size(0)).unwrap(),
            ConfigValue::from(ReadableSize::mb(100))
        );
        assert_eq!(
            to_change_value("10000", &ConfigValue::U64(0)).unwrap(),
            ConfigValue::from(10000u64)
        );

        let old = TikvConfig::default();
        let mut incoming = TikvConfig::default();
        incoming.coprocessor.region_split_keys = Some(10000);
        incoming.gc.max_write_bytes_per_sec = ReadableSize::mb(100);
        incoming.rocksdb.defaultcf.block_cache_size = Some(ReadableSize::mb(500));
        incoming.storage.io_rate_limit.import_priority = file_system::IoPriority::High;
        incoming.security.redact_info_log = log_wrappers::RedactOption::Marker;
        let diff = old.diff(&incoming);
        let mut change = HashMap::new();
        change.insert(
            "coprocessor.region-split-keys".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "rocksdb.defaultcf.block-cache-size".to_owned(),
            "500MB".to_owned(),
        );
        change.insert(
            "storage.io-rate-limit.import-priority".to_owned(),
            "high".to_owned(),
        );
        change.insert("security.redact-info-log".to_owned(), "marker".to_owned());
        let res = to_config_change(change).unwrap();
        assert_eq!(diff, res);

        // illegal cases
        let cases = vec![
            // wrong value type
            ("gc.max-write-bytes-per-sec".to_owned(), "10s".to_owned()),
            (
                "pessimistic-txn.wait-for-lock-timeout".to_owned(),
                "1MB".to_owned(),
            ),
            // missing or unknown config fields
            ("xxx.yyy".to_owned(), "12".to_owned()),
            (
                "rocksdb.defaultcf.block-cache-size.xxx".to_owned(),
                "50MB".to_owned(),
            ),
            ("rocksdb.xxx.block-cache-size".to_owned(), "50MB".to_owned()),
            ("rocksdb.block-cache-size".to_owned(), "50MB".to_owned()),
            // not support change config
            (
                "raftstore.raft-heartbeat-ticks".to_owned(),
                "100".to_owned(),
            ),
            ("raftstore.prevote".to_owned(), "false".to_owned()),
        ];
        for (name, value) in cases {
            let mut change = HashMap::new();
            change.insert(name, value);
            to_config_change(change).unwrap_err();
        }
    }

    #[test]
    fn test_to_toml_encode() {
        let mut change = HashMap::new();
        change.insert(
            "raftstore.pd-heartbeat-tick-interval".to_owned(),
            "1h".to_owned(),
        );
        change.insert(
            "coprocessor.region-split-keys".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "rocksdb.defaultcf.titan.blob-run-mode".to_owned(),
            "read-only".to_owned(),
        );
        change.insert("raftstore.apply_pool_size".to_owned(), "7".to_owned());
        change.insert("raftstore.store-pool-size".to_owned(), "17".to_owned());
        let res = to_toml_encode(change).unwrap();
        assert_eq!(
            res.get("raftstore.pd-heartbeat-tick-interval"),
            Some(&"\"1h\"".to_owned())
        );
        assert_eq!(
            res.get("coprocessor.region-split-keys"),
            Some(&"10000".to_owned())
        );
        assert_eq!(
            res.get("gc.max-write-bytes-per-sec"),
            Some(&"\"100MB\"".to_owned())
        );
        assert_eq!(
            res.get("rocksdb.defaultcf.titan.blob-run-mode"),
            Some(&"\"read-only\"".to_owned())
        );
        assert_eq!(res.get("raftstore.apply-pool-size"), Some(&"7".to_owned()));
        assert_eq!(res.get("raftstore.store-pool-size"), Some(&"17".to_owned()));
    }

    #[allow(clippy::type_complexity)]
    fn new_engines<F: KvFormat>(
        cfg: TikvConfig,
    ) -> (
        Storage<RocksDBEngine, MockLockManager, F>,
        ConfigController,
        ReceiverWrapper<TtlCheckerTask>,
        Arc<FlowController>,
    ) {
        assert_eq!(F::TAG, cfg.storage.api_version());
        let resource = cfg
            .rocksdb
            .build_resources(Arc::default(), cfg.storage.engine);
        let engine = RocksDBEngine::new(
            &cfg.infer_kv_engine_path(None).unwrap(),
            Some(cfg.rocksdb.build_opt(&resource, cfg.storage.engine)),
            cfg.rocksdb.build_cf_opts(
                &cfg.rocksdb.build_cf_resources(
                    cfg.storage.block_cache.build_shared_cache(),
                    Default::default(),
                ),
                None,
                cfg.storage.api_version(),
                None,
                cfg.storage.engine,
            ),
            None,
        )
        .unwrap();
        let mut raft_cfg = cfg.raft_engine.config.clone();
        raft_cfg.dir = cfg.infer_raft_engine_path(None).unwrap();
        let _raft_db = RaftLogEngine::new(
            raft_cfg, None, None, // io_rate_limiter
        )
        .unwrap();
        let storage =
            TestStorageBuilder::<_, _, F>::from_engine_and_lock_mgr(engine, MockLockManager::new())
                .config(cfg.storage.clone())
                .build()
                .unwrap();
        let engine = storage.get_engine().get_rocksdb();
        let (_tx, rx) = std::sync::mpsc::channel();
        let flow_controller = Arc::new(FlowController::Singleton(EngineFlowController::new(
            &cfg.storage.flow_control,
            engine.clone(),
            rx,
        )));

        let cfg_controller = ConfigController::new(cfg);
        cfg_controller.register(
            Module::Rocksdb,
            Box::new(DbConfigManger::new(
                cfg_controller.get_current().rocksdb,
                engine.clone(),
                DbType::Kv,
            )),
        );
        let (scheduler, receiver) = dummy_scheduler();
        cfg_controller.register(
            Module::Storage,
            Box::new(StorageConfigManger::new(
                engine,
                scheduler,
                flow_controller.clone(),
                storage.get_scheduler(),
                storage.get_concurrency_manager(),
            )),
        );
        (storage, cfg_controller, receiver, flow_controller)
    }

    struct MockCfgManager(Box<dyn Fn(ConfigChange) + Send + Sync>);

    impl ConfigManager for MockCfgManager {
        fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
            (self.0)(change);
            Ok(())
        }
    }

    #[test]
    fn test_change_resolved_ts_config() {
        let (cfg, _dir) = TikvConfig::with_tmp().unwrap();
        let cfg_controller = ConfigController::new(cfg);
        let (tx, rx) = channel();
        cfg_controller.register(
            Module::ResolvedTs,
            Box::new(MockCfgManager(Box::new(move |c| {
                tx.send(c).unwrap();
            }))),
        );

        // Return error if try to update not support config or unknow config
        cfg_controller
            .update_config("resolved-ts.enable", "false")
            .unwrap_err();
        cfg_controller
            .update_config("resolved-ts.scan-lock-pool-size", "10")
            .unwrap_err();
        cfg_controller
            .update_config("resolved-ts.xxx", "false")
            .unwrap_err();

        let mut resolved_ts_cfg = cfg_controller.get_current().resolved_ts;
        // Default value
        assert_eq!(
            resolved_ts_cfg.advance_ts_interval,
            ReadableDuration::secs(20)
        );

        // Update `advance-ts-interval` to 100ms
        cfg_controller
            .update_config("resolved-ts.advance-ts-interval", "100ms")
            .unwrap();
        resolved_ts_cfg.update(rx.recv().unwrap()).unwrap();
        assert_eq!(
            resolved_ts_cfg.advance_ts_interval,
            ReadableDuration::millis(100)
        );

        // Return error if try to update `advance-ts-interval` to an invalid value
        cfg_controller
            .update_config("resolved-ts.advance-ts-interval", "0m")
            .unwrap_err();
        assert_eq!(
            resolved_ts_cfg.advance_ts_interval,
            ReadableDuration::millis(100)
        );

        // Update `advance-ts-interval` to 3s
        cfg_controller
            .update_config("resolved-ts.advance-ts-interval", "3s")
            .unwrap();
        resolved_ts_cfg.update(rx.recv().unwrap()).unwrap();
        assert_eq!(
            resolved_ts_cfg.advance_ts_interval,
            ReadableDuration::secs(3)
        );
    }

    #[test]
    fn test_change_rocksdb_config() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.rocksdb.max_background_jobs = 4;
        cfg.rocksdb.max_background_flushes = 2;
        cfg.rocksdb.defaultcf.disable_auto_compactions = false;
        cfg.rocksdb.defaultcf.target_file_size_base = Some(ReadableSize::mb(64));
        cfg.rocksdb.defaultcf.block_cache_size = Some(ReadableSize::mb(8));
        cfg.rocksdb.rate_bytes_per_sec = ReadableSize::mb(64);
        cfg.rocksdb.rate_limiter_auto_tuned = false;
        cfg.rocksdb.lockcf.write_buffer_limit = Some(ReadableSize::mb(1));
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        let db = storage.get_engine().get_rocksdb();

        // update max_background_jobs
        assert_eq!(db.get_db_options().get_max_background_jobs(), 4);

        cfg_controller
            .update_config("rocksdb.max-background-jobs", "8")
            .unwrap();
        assert_eq!(db.get_db_options().get_max_background_jobs(), 8);
        assert_eq!(db.get_db_options().get_max_background_compactions(), 6);

        // update max_background_flushes, set to a bigger value
        assert_eq!(db.get_db_options().get_max_background_flushes(), 2);

        cfg_controller
            .update_config("rocksdb.max-background-flushes", "5")
            .unwrap();
        assert_eq!(db.get_db_options().get_max_background_flushes(), 5);
        assert_eq!(db.get_db_options().get_max_background_compactions(), 3);

        // update rate_bytes_per_sec
        assert_eq!(
            db.get_db_options().get_rate_bytes_per_sec().unwrap(),
            ReadableSize::mb(64).0 as i64
        );

        cfg_controller
            .update_config("rocksdb.rate-bytes-per-sec", "128MB")
            .unwrap();
        assert_eq!(
            db.get_db_options().get_rate_bytes_per_sec().unwrap(),
            ReadableSize::mb(128).0 as i64
        );

        cfg_controller
            .update_config("rocksdb.write-buffer-limit", "10MB")
            .unwrap();
        let flush_size = db.get_db_options().get_flush_size().unwrap();
        assert_eq!(flush_size, ReadableSize::mb(10).0);

        cfg_controller
            .update_config("rocksdb.lockcf.write-buffer-limit", "22MB")
            .unwrap();
        let cf_opt = db.get_options_cf("lock").unwrap();
        let flush_size = cf_opt.get_flush_size().unwrap();
        assert_eq!(flush_size, ReadableSize::mb(22).0);

        cfg_controller
            .update_config("rocksdb.lockcf.write-buffer-size", "102MB")
            .unwrap();
        let cf_opt = db.get_options_cf("lock").unwrap();
        let bsize = cf_opt.get_write_buffer_size();
        assert_eq!(bsize, ReadableSize::mb(102).0);

        cfg_controller
            .update_config("rocksdb.writecf.write-buffer-size", "102MB")
            .unwrap();
        let cf_opt = db.get_options_cf("write").unwrap();
        let bsize = cf_opt.get_write_buffer_size();
        assert_eq!(bsize, ReadableSize::mb(102).0);

        cfg_controller
            .update_config("rocksdb.defaultcf.write-buffer-size", "102MB")
            .unwrap();
        let cf_opt = db.get_options_cf("default").unwrap();
        let bsize = cf_opt.get_write_buffer_size();
        assert_eq!(bsize, ReadableSize::mb(102).0);

        // update some configs on default cf
        let cf_opts = db.get_options_cf(CF_DEFAULT).unwrap();
        assert!(!cf_opts.get_disable_auto_compactions());
        assert_eq!(cf_opts.get_target_file_size_base(), ReadableSize::mb(64).0);

        let mut change = HashMap::new();
        change.insert(
            "rocksdb.defaultcf.disable-auto-compactions".to_owned(),
            "true".to_owned(),
        );
        change.insert(
            "rocksdb.defaultcf.target-file-size-base".to_owned(),
            "32MB".to_owned(),
        );
        cfg_controller.update(change).unwrap();

        let cf_opts = db.get_options_cf(CF_DEFAULT).unwrap();
        assert!(cf_opts.get_disable_auto_compactions());
        assert_eq!(cf_opts.get_target_file_size_base(), ReadableSize::mb(32).0);
    }

    #[test]
    fn test_change_rate_limiter_auto_tuned() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        // vanilla limiter does not support dynamically changing auto-tuned mode.
        cfg.rocksdb.rate_limiter_auto_tuned = true;
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        let db = storage.get_engine().get_rocksdb();

        // update rate_limiter_auto_tuned
        assert!(db.get_db_options().get_rate_limiter_auto_tuned().unwrap());

        cfg_controller
            .update_config("rocksdb.rate_limiter_auto_tuned", "false")
            .unwrap();
        assert!(!db.get_db_options().get_rate_limiter_auto_tuned().unwrap());
    }

    #[test]
    fn test_change_shared_block_cache() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        let db = storage.get_engine().get_rocksdb();

        // Can not update shared block cache through rocksdb module
        cfg_controller
            .update_config("rocksdb.defaultcf.block-cache-size", "256MB")
            .unwrap_err();

        cfg_controller
            .update_config("storage.block-cache.capacity", "256MB")
            .unwrap();

        let defaultcf_opts = db.get_options_cf(CF_DEFAULT).unwrap();
        assert_eq!(
            defaultcf_opts.get_block_cache_capacity(),
            ReadableSize::mb(256).0
        );
    }

    #[test]
    fn test_change_log_config() {
        let (cfg, _dir) = TikvConfig::with_tmp().unwrap();
        let cfg_controller = ConfigController::new(cfg);

        cfg_controller.register(Module::Log, Box::new(LogConfigManager));

        cfg_controller.update_config("log.level", "warn").unwrap();
        assert_eq!(get_log_level().unwrap(), Level::Warning);
        assert_eq!(
            cfg_controller.get_current().log.level,
            LogLevel(Level::Warning)
        );

        cfg_controller
            .update_config("log.level", "invalid")
            .unwrap_err();
        assert_eq!(
            cfg_controller.get_current().log.level,
            LogLevel(Level::Warning)
        );
    }

    #[test]
    #[ignore = "#ifdef MALLOC_CONF"]
    #[cfg(feature = "mem-profiling")]
    fn test_change_memory_config_ifdef_malloc_conf() {
        let (cfg, _dir) = TikvConfig::with_tmp().unwrap();
        let cfg_controller = ConfigController::new(cfg);

        cfg_controller.register(Module::Memory, Box::new(MemoryConfigManager));
        cfg_controller
            .update_config("memory.enable_heap_profiling", "false")
            .unwrap();
        assert_eq!(tikv_alloc::is_profiling_active(), false);
        cfg_controller
            .update_config("memory.enable_heap_profiling", "true")
            .unwrap();
        assert_eq!(tikv_alloc::is_profiling_active(), true);

        cfg_controller
            .update_config("memory.profiling_sample_per_bytes", "1MB")
            .unwrap();
        assert_eq!(
            cfg_controller
                .get_current()
                .memory
                .profiling_sample_per_bytes,
            ReadableSize::mb(1),
        );
        cfg_controller
            .update_config("memory.profiling_sample_per_bytes", "invalid")
            .unwrap_err();
    }

    #[test]
    fn test_dispatch_titan_blob_run_mode_config() {
        let mut cfg = TikvConfig::default();
        let mut incoming = cfg.clone();
        cfg.rocksdb.defaultcf.titan.blob_run_mode = BlobRunMode::Normal;
        incoming.rocksdb.defaultcf.titan.blob_run_mode = BlobRunMode::Fallback;

        let diff = cfg
            .rocksdb
            .defaultcf
            .titan
            .diff(&incoming.rocksdb.defaultcf.titan);
        assert_eq!(diff.len(), 1);

        let diff = config_value_to_string(diff.into_iter().collect());
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0.as_str(), "blob_run_mode");
        assert_eq!(diff[0].1.as_str(), "kFallback");
    }

    #[test]
    fn test_update_titan_blob_run_mode_config() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.rocksdb.titan.enabled = Some(true);
        let (_, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        for run_mode in [
            "kFallback",
            "kNormal",
            "kReadOnly",
            "fallback",
            "normal",
            "read-only",
        ] {
            let change = HashMap::from([(
                "rocksdb.defaultcf.titan.blob-run-mode".to_string(),
                run_mode.to_string(),
            )]);
            cfg_controller.update_without_persist(change).unwrap();
        }
    }

    #[test]
    fn test_change_ttl_check_poll_interval() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.validate().unwrap();
        let (_, cfg_controller, mut rx, _) = new_engines::<ApiV1>(cfg);

        // Can not update shared block cache through rocksdb module
        cfg_controller
            .update_config("storage.ttl_check_poll_interval", "10s")
            .unwrap();
        match rx.recv() {
            None => unreachable!(),
            Some(TtlCheckerTask::UpdatePollInterval(d)) => assert_eq!(d, Duration::from_secs(10)),
        }
    }

    #[test]
    fn test_titan_config_compatible_upgrade() {
        // Case 1: Upgrade from existing instance with titan disabled
        {
            // Mock a existing instance with titan disabled
            let (mut cfg, dir) = TikvConfig::with_tmp().unwrap();
            cfg.rocksdb.titan.enabled = Some(false);
            cfg.rocksdb.defaultcf.titan.min_blob_size = Some(ReadableSize::kb(1));
            persist_config(&cfg).unwrap();

            let (storage, ..) = new_engines::<ApiV1>(cfg);
            drop(storage);

            let mut cfg = TikvConfig::from_file(&dir.path().join("config.toml"), None).unwrap();
            assert_eq!(cfg.rocksdb.titan.enabled, None);
            validate_and_persist_config(&mut cfg, true).unwrap();
            // Titan is kept disabled
            assert_eq!(cfg.rocksdb.titan.enabled, Some(false));
            assert_eq!(
                cfg.rocksdb.defaultcf.titan.min_blob_size,
                Some(ReadableSize::kb(32)),
            );
            let (_storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
            assert_eq!(
                cfg_controller.get_current().rocksdb.titan.enabled,
                Some(false)
            );
            assert_eq!(
                cfg_controller
                    .get_current()
                    .rocksdb
                    .defaultcf
                    .titan
                    .min_blob_size,
                Some(ReadableSize::kb(32)),
            );
            drop(dir);
        }

        // Case 2: Upgrade from existing instance with titan enabled
        {
            // Mock a existing instance with titan enabled
            let (mut cfg, dir) = TikvConfig::with_tmp().unwrap();
            cfg.rocksdb.titan.enabled = Some(true);
            cfg.rocksdb.defaultcf.titan.min_blob_size = Some(ReadableSize::kb(1));
            persist_config(&cfg).unwrap();
            let (storage, ..) = new_engines::<ApiV1>(cfg.clone());
            drop(storage);

            let mut cfg = TikvConfig::from_file(&dir.path().join("config.toml"), None).unwrap();
            cfg.rocksdb.titan.enabled = Some(true);
            validate_and_persist_config(&mut cfg, true).unwrap();
            assert_eq!(cfg.rocksdb.titan.enabled, Some(true));
            // The min blob size is kept
            assert_eq!(
                cfg.rocksdb.defaultcf.titan.min_blob_size,
                Some(ReadableSize::kb(1)),
            );
            let (_storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
            assert_eq!(
                cfg_controller.get_current().rocksdb.titan.enabled,
                Some(true)
            );
            assert_eq!(
                cfg_controller
                    .get_current()
                    .rocksdb
                    .defaultcf
                    .titan
                    .min_blob_size,
                Some(ReadableSize::kb(1)),
            );
        }

        // Case 3: Upgrade from existing instance with titan disabled and enable it now
        {
            // Mock a existing instance with titan disabled
            let (mut cfg, dir) = TikvConfig::with_tmp().unwrap();
            cfg.rocksdb.titan.enabled = Some(false);
            cfg.rocksdb.defaultcf.titan.min_blob_size = Some(ReadableSize::kb(1));
            persist_config(&cfg).unwrap();
            let (storage, ..) = new_engines::<ApiV1>(cfg);
            drop(storage);

            let mut cfg = TikvConfig::from_file(&dir.path().join("config.toml"), None).unwrap();
            cfg.rocksdb.titan.enabled = Some(true);
            validate_and_persist_config(&mut cfg, true).unwrap();
            assert_eq!(cfg.rocksdb.titan.enabled, Some(true));
            // The min blob size is current default value
            assert_eq!(
                cfg.rocksdb.defaultcf.titan.min_blob_size,
                Some(ReadableSize::kb(32)),
            );
            let (_storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
            assert_eq!(
                cfg_controller.get_current().rocksdb.titan.enabled,
                Some(true)
            );
            assert_eq!(
                cfg_controller
                    .get_current()
                    .rocksdb
                    .defaultcf
                    .titan
                    .min_blob_size,
                Some(ReadableSize::kb(32)),
            );
        }

        // Case 4: Create a new instance
        {
            let (mut cfg, dir) = TikvConfig::with_tmp().unwrap();
            assert_eq!(cfg.rocksdb.titan.enabled, None);
            validate_and_persist_config(&mut cfg, true).unwrap();
            assert_eq!(cfg.rocksdb.titan.enabled, Some(true));
            assert_eq!(
                cfg.rocksdb.defaultcf.titan.min_blob_size,
                Some(ReadableSize::kb(32)),
            );
            let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
            assert_eq!(
                cfg_controller.get_current().rocksdb.titan.enabled,
                Some(true)
            );
            assert_eq!(
                cfg_controller
                    .get_current()
                    .rocksdb
                    .defaultcf
                    .titan
                    .min_blob_size,
                Some(ReadableSize::kb(32)),
            );
            drop(storage);
            drop(cfg_controller);

            // Restart the instance
            let mut cfg = TikvConfig::from_file(&dir.path().join("config.toml"), None).unwrap();
            assert_eq!(cfg.rocksdb.titan.enabled, None);
            validate_and_persist_config(&mut cfg, true).unwrap();
            assert_eq!(cfg.rocksdb.titan.enabled, Some(true));
            let (_storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
            assert_eq!(
                cfg_controller.get_current().rocksdb.titan.enabled,
                Some(true)
            );
            assert_eq!(
                cfg_controller
                    .get_current()
                    .rocksdb
                    .defaultcf
                    .titan
                    .min_blob_size,
                Some(ReadableSize::kb(32)),
            );
        }
    }

    #[test]
    fn test_change_store_scheduler_worker_pool_size() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.storage.scheduler_worker_pool_size = 4;
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg);
        let scheduler = storage.get_scheduler();

        let max_pool_size = std::cmp::max(4, SysQuota::cpu_cores_quota() as usize);

        let check_scale_pool_size = |size: usize, ok: bool| {
            let origin_pool_size = scheduler.get_sched_pool().get_pool_size(CommandPri::Normal);
            let origin_pool_size_high = scheduler.get_sched_pool().get_pool_size(CommandPri::High);
            let res = cfg_controller
                .update_config("storage.scheduler-worker-pool-size", &format!("{}", size));
            let (expected_size, expected_size_high) = if ok {
                res.unwrap();
                (size, std::cmp::max(size / 2, 1))
            } else {
                res.unwrap_err();
                (origin_pool_size, origin_pool_size_high)
            };
            assert_eq!(
                scheduler.get_sched_pool().get_pool_size(CommandPri::Normal),
                expected_size
            );
            assert_eq!(
                scheduler.get_sched_pool().get_pool_size(CommandPri::High),
                expected_size_high
            );
        };

        check_scale_pool_size(0, false);
        check_scale_pool_size(max_pool_size + 1, false);
        check_scale_pool_size(1, true);
        check_scale_pool_size(max_pool_size, true);
    }

    #[test]
    fn test_change_store_scheduler_memory_quota() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.storage.memory_quota = ReadableSize::mb(100);
        cfg.storage.scheduler_pending_write_threshold = ReadableSize::mb(10);
        cfg.validate().unwrap();
        let (storage, cfg_controller, ..) = new_engines::<ApiV1>(cfg.clone());
        let scheduler = storage.get_scheduler();

        let check_scheduler_memory_quota = |size: &str, expected: Option<usize>| {
            let res = cfg_controller.update_config("storage.memory-quota", size);
            let Some(expected_size) = expected else {
                res.unwrap_err();
                return;
            };
            assert_eq!(scheduler.memory_quota_capacity(), expected_size);
        };

        check_scheduler_memory_quota("11h", None);
        check_scheduler_memory_quota(
            "0B",
            Some(cfg.storage.scheduler_pending_write_threshold.0 as usize),
        );
        check_scheduler_memory_quota("11MB", Some(ReadableSize::mb(11).0 as usize));
        check_scheduler_memory_quota("111MB", Some(ReadableSize::mb(111).0 as usize));
    }

    #[test]
    fn test_change_quota_config() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.quota.foreground_cpu_time = 1000;
        cfg.quota.foreground_write_bandwidth = ReadableSize::mb(128);
        cfg.quota.foreground_read_bandwidth = ReadableSize::mb(256);
        cfg.quota.background_cpu_time = 1000;
        cfg.quota.background_write_bandwidth = ReadableSize::mb(128);
        cfg.quota.background_read_bandwidth = ReadableSize::mb(256);
        cfg.quota.max_delay_duration = ReadableDuration::secs(1);
        cfg.validate().unwrap();

        let quota_limiter = Arc::new(QuotaLimiter::new(
            cfg.quota.foreground_cpu_time,
            cfg.quota.foreground_write_bandwidth,
            cfg.quota.foreground_read_bandwidth,
            cfg.quota.background_cpu_time,
            cfg.quota.background_write_bandwidth,
            cfg.quota.background_read_bandwidth,
            cfg.quota.max_delay_duration,
            false,
        ));

        let cfg_controller = ConfigController::new(cfg.clone());
        cfg_controller.register(
            Module::Quota,
            Box::new(QuotaLimitConfigManager::new(Arc::clone(&quota_limiter))),
        );
        assert_eq_debug(&cfg_controller.get_current(), &cfg);

        // u64::MAX ns casts to 213503d.
        cfg_controller
            .update_config("quota.max-delay-duration", "213504d")
            .unwrap_err();
        assert_eq_debug(&cfg_controller.get_current(), &cfg);

        cfg_controller
            .update_config("quota.foreground-cpu-time", "2000")
            .unwrap();
        cfg.quota.foreground_cpu_time = 2000;
        assert_eq_debug(&cfg_controller.get_current(), &cfg);

        cfg_controller
            .update_config("quota.foreground-write-bandwidth", "256MB")
            .unwrap();
        cfg.quota.foreground_write_bandwidth = ReadableSize::mb(256);
        assert_eq_debug(&cfg_controller.get_current(), &cfg);

        let mut sample = quota_limiter.new_sample(true);
        sample.add_read_bytes(ReadableSize::mb(32).0 as usize);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        assert_eq!(should_delay, Duration::from_millis(125));

        cfg_controller
            .update_config("quota.foreground-read-bandwidth", "512MB")
            .unwrap();
        cfg.quota.foreground_read_bandwidth = ReadableSize::mb(512);
        assert_eq!(cfg_controller.get_current(), cfg);
        let mut sample = quota_limiter.new_sample(true);
        sample.add_write_bytes(ReadableSize::mb(128).0 as usize);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        assert_eq!(should_delay, Duration::from_millis(500));

        cfg_controller
            .update_config("quota.background-cpu-time", "2000")
            .unwrap();
        cfg.quota.background_cpu_time = 2000;
        assert_eq_debug(&cfg_controller.get_current(), &cfg);

        cfg_controller
            .update_config("quota.background-write-bandwidth", "256MB")
            .unwrap();
        cfg.quota.background_write_bandwidth = ReadableSize::mb(256);
        assert_eq_debug(&cfg_controller.get_current(), &cfg);

        let mut sample = quota_limiter.new_sample(false);
        sample.add_read_bytes(ReadableSize::mb(32).0 as usize);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        assert_eq!(should_delay, Duration::from_millis(125));

        cfg_controller
            .update_config("quota.background-read-bandwidth", "512MB")
            .unwrap();
        cfg.quota.background_read_bandwidth = ReadableSize::mb(512);
        assert_eq_debug(&cfg_controller.get_current(), &cfg);
        let mut sample = quota_limiter.new_sample(false);
        sample.add_write_bytes(ReadableSize::mb(128).0 as usize);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        assert_eq!(should_delay, Duration::from_millis(500));

        cfg_controller
            .update_config("quota.max-delay-duration", "50ms")
            .unwrap();
        cfg.quota.max_delay_duration = ReadableDuration::millis(50);
        assert_eq_debug(&cfg_controller.get_current(), &cfg);
        let mut sample = quota_limiter.new_sample(true);
        sample.add_write_bytes(ReadableSize::mb(128).0 as usize);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        assert_eq!(should_delay, Duration::from_millis(50));

        let mut sample = quota_limiter.new_sample(false);
        sample.add_write_bytes(ReadableSize::mb(128).0 as usize);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        assert_eq!(should_delay, Duration::from_millis(50));

        assert!(!cfg.quota.enable_auto_tune);
        cfg_controller
            .update_config("quota.enable-auto-tune", "true")
            .unwrap();
        cfg.quota.enable_auto_tune = true;
        assert_eq_debug(&cfg_controller.get_current(), &cfg);
    }

    #[test]
    fn test_change_server_config() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.validate().unwrap();
        let cfg_controller = ConfigController::new(cfg.clone());
        let (scheduler, _receiver) = dummy_scheduler();
        let version_tracker = Arc::new(VersionTrack::new(cfg.server.clone()));
        let cop_manager = MockCfgManager(Box::new(|_| {}));
        cfg_controller.register(
            Module::Server,
            Box::new(ServerConfigManager::new(
                scheduler,
                version_tracker.clone(),
                ResourceQuota::new(None),
                Box::new(cop_manager),
            )),
        );

        let check_cfg = |cfg: &TikvConfig| {
            assert_eq_debug(&cfg_controller.get_current(), cfg);
            assert_eq!(&*version_tracker.value(), &cfg.server);
        };

        cfg_controller
            .update_config("server.max-grpc-send-msg-len", "10000")
            .unwrap();
        cfg.server.max_grpc_send_msg_len = 10000;
        check_cfg(&cfg);

        cfg_controller
            .update_config("server.raft-msg-max-batch-size", "32")
            .unwrap();
        cfg.server.raft_msg_max_batch_size = 32;
        assert_eq_debug(&cfg_controller.get_current(), &cfg);
        check_cfg(&cfg);
    }

    #[test]
    fn test_endpoint_config() {
        let mut default_cfg = TikvConfig::default();
        default_cfg.storage.engine = EngineType::RaftKv;
        default_cfg.validate().unwrap();
        assert_eq!(
            default_cfg.server.end_point_request_max_handle_duration(),
            ReadableDuration::secs(60)
        );

        let mut default_cfg = TikvConfig::default();
        default_cfg.storage.engine = EngineType::RaftKv2;
        default_cfg.validate().unwrap();
        assert_eq!(
            default_cfg.server.end_point_request_max_handle_duration(),
            ReadableDuration::secs(1800)
        );

        let mut default_cfg = TikvConfig::default();
        default_cfg.storage.engine = EngineType::RaftKv2;
        default_cfg.server.end_point_request_max_handle_duration =
            Some(ReadableDuration::secs(900));
        default_cfg.validate().unwrap();
        assert_eq!(
            default_cfg.server.end_point_request_max_handle_duration(),
            ReadableDuration::secs(900)
        );
    }

    #[test]
    fn test_change_coprocessor_endpoint_config() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.validate().unwrap();
        let cfg_controller = ConfigController::new(cfg.clone());
        let (scheduler, _receiver) = dummy_scheduler();
        let version_tracker = Arc::new(VersionTrack::new(cfg.server.clone()));

        let (cop_tx, cop_rx) = channel();
        let cop_manager = MockCfgManager(Box::new(move |c| {
            cop_tx.send(c).unwrap();
        }));
        cfg_controller.register(
            Module::Server,
            Box::new(ServerConfigManager::new(
                scheduler,
                version_tracker.clone(),
                ResourceQuota::new(None),
                Box::new(cop_manager),
            )),
        );

        cfg_controller
            .update_config("server.end-point-memory-quota", "32MB")
            .unwrap();
        let mut change = cop_rx.try_recv().unwrap();
        let quota = change.remove("end_point_memory_quota").unwrap();
        let cap: ReadableSize = quota.into();
        assert_eq!(cap, ReadableSize::mb(32));
        cfg.server.end_point_memory_quota = ReadableSize::mb(32);
        assert_eq_debug(&cfg_controller.get_current(), &cfg);
    }

    #[test]
    fn test_compatible_adjust_validate_equal() {
        // After calling many time of `compatible_adjust` and `validate` should has
        // the same effect as calling `compatible_adjust` and `validate` one time
        let mut c = TikvConfig::default();
        let mut cfg = c.clone();
        c.compatible_adjust(None);
        c.validate().unwrap();

        for _ in 0..10 {
            cfg.compatible_adjust(None);
            cfg.validate().unwrap();
            assert_eq_debug(&c, &cfg);
        }
    }

    #[test]
    fn test_readpool_compatible_adjust_config() {
        let content = r#"
        [readpool.storage]
        [readpool.coprocessor]
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.compatible_adjust(None);
        assert_eq!(cfg.readpool.storage.use_unified_pool, Some(true));
        assert_eq!(cfg.readpool.coprocessor.use_unified_pool, Some(true));

        let content = r#"
        [readpool.storage]
        stack-size = "1MB"
        [readpool.coprocessor]
        normal-concurrency = 1
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.compatible_adjust(None);
        assert_eq!(cfg.readpool.storage.use_unified_pool, Some(false));
        assert_eq!(cfg.readpool.coprocessor.use_unified_pool, Some(false));
    }

    #[test]
    fn test_unrecognized_config_keys() {
        let mut temp_config_file = tempfile::NamedTempFile::new().unwrap();
        let temp_config_writer = temp_config_file.as_file_mut();
        temp_config_writer
            .write_all(
                br#"
                    log-level = "debug"
                    log-fmt = "json"
                    [readpool.unified]
                    min-threads-count = 5
                    stack-size = "20MB"
                    [import]
                    num_threads = 4
                    [gcc]
                    batch-keys = 1024
                    [[security.encryption.master-keys]]
                    type = "file"
                "#,
            )
            .unwrap();
        temp_config_writer.sync_data().unwrap();

        let mut unrecognized_keys = Vec::new();
        let _ = TikvConfig::from_file(temp_config_file.path(), Some(&mut unrecognized_keys));

        assert_eq!(
            unrecognized_keys,
            vec![
                "log-fmt".to_owned(),
                "readpool.unified.min-threads-count".to_owned(),
                "import.num_threads".to_owned(),
                "gcc".to_owned(),
                "security.encryption.master-keys".to_owned(),
            ],
        );
    }

    #[test]
    fn test_raft_engine_dir() {
        let content = r#"
            [raft-engine]
            enable = true
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.raft_engine.config.dir,
            config::canonicalize_sub_path(&cfg.storage.data_dir, "raft-engine").unwrap()
        );
    }

    #[test]
    fn test_compaction_guard() {
        let cache = Cache::new_lru_cache(LRUCacheOptions::new());
        let no_limiter: Option<ConcurrentTaskLimiter> = None;
        // Test comopaction guard disabled.
        let config = DefaultCfConfig {
            target_file_size_base: Some(ReadableSize::mb(16)),
            enable_compaction_guard: Some(false),
            ..Default::default()
        };
        let provider = Some(MockRegionInfoProvider::new(vec![]));
        let cf_opts = build_cf_opt!(
            config,
            CF_DEFAULT,
            &cache,
            no_limiter.as_ref(),
            provider,
            Default::default(),
        );
        assert_eq!(
            config.target_file_size_base(),
            cf_opts.get_target_file_size_base()
        );

        // Test compaction guard enabled but region info provider is missing.
        let config = DefaultCfConfig {
            target_file_size_base: Some(ReadableSize::mb(16)),
            enable_compaction_guard: Some(true),
            ..Default::default()
        };
        let provider: Option<MockRegionInfoProvider> = None;
        let cf_opts = build_cf_opt!(
            config,
            CF_DEFAULT,
            &cache,
            no_limiter.as_ref(),
            provider,
            Default::default(),
        );
        assert_eq!(
            config.target_file_size_base(),
            cf_opts.get_target_file_size_base()
        );

        // Test compaction guard enabled.
        let config = DefaultCfConfig {
            target_file_size_base: Some(ReadableSize::mb(16)),
            enable_compaction_guard: Some(true),
            compaction_guard_min_output_file_size: ReadableSize::mb(4),
            compaction_guard_max_output_file_size: ReadableSize::mb(64),
            ..Default::default()
        };
        let provider = Some(MockRegionInfoProvider::new(vec![]));
        let cf_opts = build_cf_opt!(
            config,
            CF_DEFAULT,
            &cache,
            no_limiter.as_ref(),
            provider,
            Default::default(),
        );
        assert_eq!(
            config.compaction_guard_max_output_file_size.0,
            cf_opts.get_target_file_size_base()
        );
    }

    #[test]
    fn test_validate_tikv_config() {
        let mut cfg = TikvConfig::default();
        cfg.validate().unwrap();
        let default_region_split_check_diff = cfg.raft_store.region_split_check_diff().0;
        cfg.raft_store.region_split_check_diff =
            Some(ReadableSize(cfg.raft_store.region_split_check_diff().0 + 1));
        cfg.validate().unwrap();
        assert_eq!(
            cfg.raft_store.region_split_check_diff().0,
            default_region_split_check_diff + 1
        );

        // Test validating memory_usage_limit when it's greater than max.
        cfg.memory_usage_limit = Some(ReadableSize(SysQuota::memory_limit_in_bytes() * 2));
        cfg.validate().unwrap_err();

        // Test memory_usage_limit is based on block cache size if it's not configured.
        cfg.memory_usage_limit = None;
        cfg.storage.block_cache.capacity = Some(ReadableSize(3 * GIB));
        cfg.validate().unwrap();
        assert_eq!(cfg.memory_usage_limit.unwrap(), ReadableSize(5 * GIB));

        // Test memory_usage_limit will fallback to system memory capacity with huge
        // block cache.
        cfg.memory_usage_limit = None;
        let system = SysQuota::memory_limit_in_bytes();
        cfg.storage.block_cache.capacity = Some(ReadableSize(system * 3 / 4));
        cfg.validate().unwrap();
        assert_eq!(cfg.memory_usage_limit.unwrap(), ReadableSize(system));

        // Test raftstore.enable-partitioned-raft-kv-compatible-learner.
        let mut cfg = TikvConfig::default();
        cfg.raft_store.enable_v2_compatible_learner = true;
        cfg.storage.engine = EngineType::RaftKv2;
        cfg.validate().unwrap();
        assert!(!cfg.raft_store.enable_v2_compatible_learner);

        // Ribbon filter and format version.
        let mut cfg = TikvConfig::default();
        cfg.rocksdb.writecf.ribbon_filter_above_level = Some(6);
        cfg.rocksdb.writecf.format_version = None;
        cfg.validate().unwrap_err();
        cfg.rocksdb.writecf.format_version = Some(3);
        cfg.validate().unwrap_err();
        cfg.rocksdb.writecf.format_version = Some(5);
        cfg.validate().unwrap();

        let mut valid_cfg = TikvConfig::default();
        valid_cfg.storage.api_version = 2;
        valid_cfg.storage.enable_ttl = true;
        valid_cfg.rocksdb.titan.enabled = None;
        valid_cfg.validate().unwrap();

        let mut invalid_cfg = TikvConfig::default();
        invalid_cfg.storage.api_version = 2;
        invalid_cfg.storage.enable_ttl = true;
        invalid_cfg.rocksdb.titan.enabled = Some(true);
        assert_eq!(
            invalid_cfg.validate().unwrap_err().to_string(),
            "Titan is unavailable for feature TTL"
        );
    }

    #[test]
    fn test_validate_tikv_wal_config() {
        let tmp_path = tempfile::Builder::new().tempdir().unwrap().into_path();
        macro_rules! tmp_path_string_generate {
            ($base:expr, $($sub:expr),+) => {{
                let mut path: ::std::path::PathBuf = $base.clone();
                $(
                    path.push($sub);
                )*
                String::from(path.to_str().unwrap())
            }}
        }

        {
            let mut cfg = TikvConfig::default();
            cfg.validate().unwrap();
        }

        // raft path == kv path
        {
            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = false;
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data");
            cfg.raft_store.raftdb_path = tmp_path_string_generate!(tmp_path, "data", "db");
            cfg.validate().unwrap_err();

            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = true;
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data");
            cfg.raft_engine.config.dir = tmp_path_string_generate!(tmp_path, "data", "db");
            cfg.validate().unwrap_err();

            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = true;
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data");
            cfg.raft_store.raftdb_path = tmp_path_string_generate!(tmp_path, "data", "db");
            cfg.validate().unwrap();
        }

        // raft path == kv wal path
        {
            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = false;
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            cfg.raft_store.raftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.rocksdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.validate().unwrap_err();

            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = true;
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            cfg.raft_store.raftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.rocksdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.validate().unwrap();
        }

        // raft wal path == kv path
        {
            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = false;
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            cfg.raft_store.raftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.raftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb", "db");
            cfg.validate().unwrap_err();

            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = true;
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            cfg.raft_store.raftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.raftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb", "db");
            cfg.validate().unwrap();
        }

        // raft wal path == kv wal path
        {
            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = false;
            cfg.rocksdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "wal");
            cfg.raftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "wal");
            cfg.validate().unwrap_err();

            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = true;
            cfg.rocksdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "wal");
            cfg.raftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "wal");
            cfg.validate().unwrap();
        }

        {
            let mut cfg = TikvConfig::default();
            cfg.storage.engine = EngineType::RaftKv;
            cfg.raft_engine.enable = false;
            cfg.storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            cfg.raft_store.raftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.rocksdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb", "db");
            cfg.raftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "raftdb", "db");
            cfg.validate().unwrap();
        }
    }

    #[test]
    fn test_background_job_limits() {
        for engine in [EngineType::RaftKv, EngineType::RaftKv2] {
            // cpu num = 1
            assert_eq!(
                get_background_job_limits_impl(
                    engine,
                    1, // cpu_num
                    &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
                ),
                BackgroundJobLimits {
                    max_background_jobs: 2,
                    max_background_flushes: 1,
                    max_sub_compactions: 1,
                    max_titan_background_gc: 1,
                }
            );
            assert_eq!(
                get_background_job_limits_impl(
                    engine,
                    1, // cpu_num
                    &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
                ),
                BackgroundJobLimits {
                    max_background_jobs: 2,
                    max_background_flushes: 1,
                    max_sub_compactions: 1,
                    max_titan_background_gc: 1,
                }
            );
            // cpu num = 2
            assert_eq!(
                get_background_job_limits_impl(
                    EngineType::RaftKv,
                    2, // cpu_num
                    &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
                ),
                BackgroundJobLimits {
                    max_background_jobs: 2,
                    max_background_flushes: 1,
                    max_sub_compactions: 1,
                    max_titan_background_gc: 1,
                }
            );
            assert_eq!(
                get_background_job_limits_impl(
                    EngineType::RaftKv,
                    2, // cpu_num
                    &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
                ),
                BackgroundJobLimits {
                    max_background_jobs: 2,
                    max_background_flushes: 1,
                    max_sub_compactions: 1,
                    max_titan_background_gc: 1,
                }
            );
        }

        // cpu num = 4
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv,
                4, // cpu_num
                &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 3,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 1,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv2,
                4, // cpu_num
                &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 1,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv,
                4, // cpu_num
                &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 3,
                max_background_flushes: 1,
                max_sub_compactions: 1,
                max_titan_background_gc: 1,
            }
        );
        // cpu num = 8
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv,
                8, // cpu_num
                &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 7,
                max_background_flushes: 2,
                max_sub_compactions: 3,
                max_titan_background_gc: 1,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv2,
                8, // cpu_num
                &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 4,
                max_background_flushes: 2,
                max_sub_compactions: 1,
                max_titan_background_gc: 1,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv,
                8, // cpu_num
                &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
        // cpu num = 16
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv,
                16, // cpu_num
                &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            KVDB_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv2,
                16, // cpu_num
                &KVDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 6,
                max_background_flushes: 3,
                max_sub_compactions: 2,
                max_titan_background_gc: 1,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                EngineType::RaftKv,
                16, // cpu_num
                &RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            RAFTDB_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
    }

    static CONFIG_TEMPLATE: &str = include_str!("../../etc/config-template.toml");

    #[test]
    fn test_config_template_is_valid() {
        let template_config = CONFIG_TEMPLATE
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        let mut cfg: TikvConfig = toml::from_str(&template_config).unwrap();
        cfg.validate().unwrap();
    }

    fn must_no_unknown_key(content: &str) {
        let mut deserializer = toml::Deserializer::new(content);
        let mut unrecognized_keys = Vec::new();
        let _: TikvConfig = serde_ignored::deserialize(&mut deserializer, |key| {
            unrecognized_keys.push(key.to_string())
        })
        .unwrap();

        // Don't use `is_empty()` so we see which keys are superfluous on failure.
        assert_eq!(unrecognized_keys, Vec::<String>::new());
    }

    #[test]
    fn test_config_template_no_superfluous_keys() {
        let template_config = CONFIG_TEMPLATE
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        must_no_unknown_key(&template_config);
    }

    #[test]
    fn test_config_template_matches_default() {
        let template_config = CONFIG_TEMPLATE
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        let mut cfg: TikvConfig = toml::from_str(&template_config).unwrap();
        let mut default_cfg = TikvConfig::default();

        // Some default values are computed based on the environment.
        // Because we can't set config values for these in `config-template.toml`, we
        // will handle them manually.
        cfg.readpool.unified.max_thread_count = default_cfg.readpool.unified.max_thread_count;
        cfg.readpool.storage.high_concurrency = default_cfg.readpool.storage.high_concurrency;
        cfg.readpool.storage.normal_concurrency = default_cfg.readpool.storage.normal_concurrency;
        cfg.readpool.storage.low_concurrency = default_cfg.readpool.storage.low_concurrency;
        cfg.readpool.coprocessor.high_concurrency =
            default_cfg.readpool.coprocessor.high_concurrency;
        cfg.readpool.coprocessor.normal_concurrency =
            default_cfg.readpool.coprocessor.normal_concurrency;
        cfg.readpool.coprocessor.low_concurrency = default_cfg.readpool.coprocessor.low_concurrency;
        cfg.server.grpc_memory_pool_quota = default_cfg.server.grpc_memory_pool_quota;
        cfg.server.grpc_concurrency = default_cfg.server.grpc_concurrency;
        cfg.server.grpc_raft_conn_num = default_cfg.server.grpc_raft_conn_num;
        cfg.server.background_thread_count = default_cfg.server.background_thread_count;
        cfg.server.end_point_max_concurrency = default_cfg.server.end_point_max_concurrency;
        cfg.server.end_point_memory_quota = default_cfg.server.end_point_memory_quota;
        cfg.storage.scheduler_worker_pool_size = default_cfg.storage.scheduler_worker_pool_size;
        cfg.rocksdb.max_background_jobs = default_cfg.rocksdb.max_background_jobs;
        cfg.rocksdb.max_background_flushes = default_cfg.rocksdb.max_background_flushes;
        cfg.rocksdb.max_sub_compactions = default_cfg.rocksdb.max_sub_compactions;
        cfg.rocksdb.titan.max_background_gc = default_cfg.rocksdb.titan.max_background_gc;
        cfg.raftdb.max_background_jobs = default_cfg.raftdb.max_background_jobs;
        cfg.raftdb.max_background_flushes = default_cfg.raftdb.max_background_flushes;
        cfg.raftdb.max_sub_compactions = default_cfg.raftdb.max_sub_compactions;
        cfg.raftdb.titan.max_background_gc = default_cfg.raftdb.titan.max_background_gc;
        cfg.backup.num_threads = default_cfg.backup.num_threads;
        cfg.log_backup.num_threads = default_cfg.log_backup.num_threads;

        // There is another set of config values that we can't directly compare:
        // When the default values are `None`, but are then resolved to `Some(_)` later
        // on.
        default_cfg.readpool.storage.adjust_use_unified_pool();
        default_cfg.readpool.coprocessor.adjust_use_unified_pool();
        default_cfg
            .coprocessor
            .optimize_for(default_cfg.storage.engine == EngineType::RaftKv2);
        default_cfg
            .server
            .optimize_for(default_cfg.coprocessor.region_split_size());
        default_cfg
            .raft_store
            .optimize_for(default_cfg.storage.engine == EngineType::RaftKv2);
        default_cfg.raft_engine.optimize_for(
            &default_cfg.raft_store,
            default_cfg.storage.engine == EngineType::RaftKv2,
        );
        assert_eq!(
            default_cfg.raft_engine.config().batch_compression_threshold,
            RaftEngineReadableSize::kb(4)
        );
        default_cfg.security.redact_info_log = log_wrappers::RedactOption::default();
        default_cfg.coprocessor.region_max_size = Some(default_cfg.coprocessor.region_max_size());
        default_cfg.coprocessor.region_max_keys = Some(default_cfg.coprocessor.region_max_keys());
        default_cfg.coprocessor.region_split_keys =
            Some(default_cfg.coprocessor.region_split_keys());
        default_cfg.raft_store.raft_log_gc_size_limit =
            Some(default_cfg.coprocessor.region_split_size() * 3 / 4);
        default_cfg.raft_store.raft_log_gc_count_limit =
            Some(default_cfg.coprocessor.region_split_size() * 3 / 4 / ReadableSize::kb(1));
        default_cfg.raft_store.region_split_check_diff =
            Some(default_cfg.coprocessor.region_split_size() / 16);
        default_cfg.rocksdb.writecf.target_file_size_base = Some(ReadableSize::mb(8));
        default_cfg.rocksdb.defaultcf.target_file_size_base = Some(ReadableSize::mb(8));
        default_cfg.rocksdb.lockcf.target_file_size_base = Some(ReadableSize::mb(8));
        default_cfg.rocksdb.lockcf.write_buffer_size = Some(ReadableSize::mb(32));
        default_cfg.raftdb.defaultcf.target_file_size_base = Some(ReadableSize::mb(8));
        default_cfg.rocksdb.titan.enabled = Some(true);

        // Other special cases.
        cfg.pd.retry_max_count = default_cfg.pd.retry_max_count; // Both -1 and isize::MAX are the same.
        cfg.storage.block_cache.capacity = None; // Either `None` and a value is computed or `Some(_)` fixed value.
        cfg.memory_usage_limit = None;
        cfg.raft_engine.mut_config().memory_limit = None;
        cfg.coprocessor_v2.coprocessor_plugin_directory = None; // Default is `None`, which is represented by not setting the key.
        cfg.rocksdb.write_buffer_limit = None;
        cfg.rocksdb.max_total_wal_size = None;
        cfg.rocksdb.stats_dump_period = None;
        //
        cfg.rocksdb.defaultcf.enable_compaction_guard = None;
        cfg.rocksdb.writecf.enable_compaction_guard = None;
        cfg.rocksdb.lockcf.enable_compaction_guard = None;
        cfg.rocksdb.raftcf.enable_compaction_guard = None;
        cfg.raftdb.defaultcf.enable_compaction_guard = None;
        //
        cfg.rocksdb.defaultcf.level0_slowdown_writes_trigger = 20;
        cfg.rocksdb.writecf.level0_slowdown_writes_trigger = 20;
        cfg.rocksdb.lockcf.level0_slowdown_writes_trigger = 20;
        cfg.rocksdb.raftcf.level0_slowdown_writes_trigger = 20;
        cfg.raftdb.defaultcf.level0_slowdown_writes_trigger = 20;
        //
        cfg.rocksdb.defaultcf.level0_stop_writes_trigger = None;
        cfg.rocksdb.writecf.level0_stop_writes_trigger = None;
        cfg.rocksdb.lockcf.level0_stop_writes_trigger = None;
        cfg.rocksdb.raftcf.level0_stop_writes_trigger = None;
        cfg.raftdb.defaultcf.level0_stop_writes_trigger = None;
        //
        cfg.rocksdb.defaultcf.soft_pending_compaction_bytes_limit = ReadableSize::gb(192);
        cfg.rocksdb.writecf.soft_pending_compaction_bytes_limit = ReadableSize::gb(192);
        cfg.rocksdb.lockcf.soft_pending_compaction_bytes_limit = ReadableSize::gb(192);
        cfg.rocksdb.raftcf.soft_pending_compaction_bytes_limit = ReadableSize::gb(192);
        cfg.raftdb.defaultcf.soft_pending_compaction_bytes_limit = ReadableSize::gb(192);
        //
        cfg.rocksdb.defaultcf.hard_pending_compaction_bytes_limit = None;
        cfg.rocksdb.writecf.hard_pending_compaction_bytes_limit = None;
        cfg.rocksdb.lockcf.hard_pending_compaction_bytes_limit = None;
        cfg.rocksdb.raftcf.hard_pending_compaction_bytes_limit = None;
        cfg.raftdb.defaultcf.hard_pending_compaction_bytes_limit = None;
        //
        cfg.rocksdb.defaultcf.ribbon_filter_above_level = None;
        cfg.rocksdb.writecf.ribbon_filter_above_level = None;
        cfg.rocksdb.lockcf.ribbon_filter_above_level = None;
        cfg.rocksdb.raftcf.ribbon_filter_above_level = None;
        cfg.raftdb.defaultcf.ribbon_filter_above_level = None;
        // ColumnFamily::ttl
        cfg.rocksdb.defaultcf.ttl = None;
        cfg.rocksdb.writecf.ttl = None;
        cfg.rocksdb.lockcf.ttl = None;
        cfg.rocksdb.raftcf.ttl = None;
        cfg.raftdb.defaultcf.ttl = None;
        // ColumnFamily::periodic_compaction_seconds
        cfg.rocksdb.defaultcf.periodic_compaction_seconds = None;
        cfg.rocksdb.writecf.periodic_compaction_seconds = None;
        cfg.rocksdb.lockcf.periodic_compaction_seconds = None;
        cfg.rocksdb.raftcf.periodic_compaction_seconds = None;
        cfg.raftdb.defaultcf.periodic_compaction_seconds = None;
        //
        cfg.rocksdb.defaultcf.format_version = None;
        cfg.rocksdb.writecf.format_version = None;
        cfg.rocksdb.lockcf.format_version = None;
        cfg.rocksdb.raftcf.format_version = None;
        //
        cfg.rocksdb.defaultcf.max_compactions = None;
        cfg.rocksdb.writecf.max_compactions = None;
        cfg.rocksdb.lockcf.max_compactions = None;
        cfg.rocksdb.raftcf.max_compactions = None;
        cfg.raftdb.defaultcf.max_compactions = None;

        cfg.rocksdb.defaultcf.titan.min_blob_size = None;

        cfg.coprocessor
            .optimize_for(default_cfg.storage.engine == EngineType::RaftKv2);
        cfg.raft_engine.optimize_for(
            &cfg.raft_store,
            default_cfg.storage.engine == EngineType::RaftKv2,
        );

        assert_eq_debug(&cfg, &default_cfg);
    }

    #[test]
    fn test_region_size_config() {
        let mut default_cfg = TikvConfig::default();
        default_cfg.storage.engine = EngineType::RaftKv;
        default_cfg.validate().unwrap();
        assert_eq!(default_cfg.coprocessor.region_split_size(), SPLIT_SIZE);
        assert!(!default_cfg.coprocessor.enable_region_bucket());

        assert_eq!(default_cfg.split.qps_threshold(), DEFAULT_QPS_THRESHOLD);
        assert_eq!(
            default_cfg.split.region_cpu_overload_threshold_ratio(),
            REGION_CPU_OVERLOAD_THRESHOLD_RATIO
        );
        assert_eq!(default_cfg.split.byte_threshold(), DEFAULT_BYTE_THRESHOLD);

        let mut default_cfg = TikvConfig::default();
        default_cfg.storage.engine = EngineType::RaftKv2;
        default_cfg.validate().unwrap();
        assert_eq!(
            default_cfg.coprocessor.region_split_size(),
            RAFTSTORE_V2_SPLIT_SIZE
        );
        assert_eq!(
            default_cfg.split.qps_threshold(),
            DEFAULT_BIG_REGION_QPS_THRESHOLD
        );
        assert_eq!(
            default_cfg.split.region_cpu_overload_threshold_ratio(),
            BIG_REGION_CPU_OVERLOAD_THRESHOLD_RATIO
        );
        assert_eq!(
            default_cfg.split.byte_threshold(),
            DEFAULT_BIG_REGION_BYTE_THRESHOLD
        );
        assert!(default_cfg.coprocessor.enable_region_bucket());

        let mut default_cfg = TikvConfig::default();
        default_cfg.coprocessor.region_split_size = Some(ReadableSize::mb(500));
        default_cfg.coprocessor.optimize_for(false);
        default_cfg.coprocessor.validate(false).unwrap();
        assert_eq!(
            default_cfg.coprocessor.region_split_size(),
            ReadableSize::mb(500)
        );
        assert!(!default_cfg.coprocessor.enable_region_bucket());
        default_cfg.coprocessor.validate(true).unwrap();
        assert!(default_cfg.coprocessor.enable_region_bucket());

        let mut default_cfg = TikvConfig::default();
        default_cfg.coprocessor.region_split_size = Some(ReadableSize::mb(500));
        default_cfg.coprocessor.optimize_for(true);
        default_cfg.coprocessor.validate(false).unwrap();
        assert_eq!(
            default_cfg.coprocessor.region_split_size(),
            ReadableSize::mb(500)
        );
        assert!(!default_cfg.coprocessor.enable_region_bucket());
        default_cfg.coprocessor.validate(true).unwrap();
        assert!(default_cfg.coprocessor.enable_region_bucket());
    }

    #[test]
    fn test_inherit_region_size_config() {
        let default_cfg = TikvConfig::default();

        // Case 1: start with empty settings
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.compatible_adjust(None);
        assert_eq!(
            default_cfg.coprocessor.region_max_size,
            cfg.coprocessor.region_max_size
        );
        assert_eq!(
            default_cfg.coprocessor.region_split_size,
            cfg.coprocessor.region_split_size
        );

        // Case 2: persist and load the last tikv configurations, then make the current
        // config compatible to it. And it manually set `raft_entry_max_size` in
        // Raftstore, but the new config does not set it.
        cfg.coprocessor.region_split_size = Some(ReadableSize::mb(16));
        cfg.raft_store.raft_entry_max_size = ReadableSize::kb(16);
        validate_and_persist_config(&mut cfg, true).unwrap();
        let cfg_from_file = TikvConfig::from_file(
            &Path::new(&cfg.storage.data_dir).join(LAST_CONFIG_FILE),
            None,
        )
        .unwrap();
        assert!(
            cfg_from_file.coprocessor.region_max_size.is_some()
                && cfg_from_file.coprocessor.region_max_keys.is_some()
        );
        assert_eq!(
            cfg_from_file.coprocessor.region_split_size,
            cfg.coprocessor.region_split_size
        );
        let mut case2_cfg = TikvConfig::default();
        case2_cfg.compatible_adjust(Some(&cfg_from_file));
        assert_eq!(
            cfg_from_file.coprocessor.region_max_size,
            case2_cfg.coprocessor.region_max_size
        );
        assert_eq!(
            cfg_from_file.coprocessor.region_split_size,
            case2_cfg.coprocessor.region_split_size
        );
        assert_eq!(
            cfg_from_file.coprocessor.region_max_keys,
            case2_cfg.coprocessor.region_max_keys
        );
        assert_eq!(
            cfg_from_file.coprocessor.region_split_keys,
            case2_cfg.coprocessor.region_split_keys
        );
        // Other configs in RaftstoreConfig should not
        // inherit the last config.
        assert_eq!(
            default_cfg.raft_store.raft_entry_max_size,
            case2_cfg.raft_store.raft_entry_max_size
        );
        assert_ne!(
            cfg_from_file.raft_store.raft_entry_max_size,
            case2_cfg.raft_store.raft_entry_max_size
        );

        // Case 3: manually specify region-split-size, then make it compatible to the
        // last config. The current configuration should inherit the remained configs.
        let content = r#"
        [coprocessor]
        region-split-size = "32MiB"
        "#;
        let mut case3_cfg: TikvConfig = toml::from_str(content).unwrap();
        case3_cfg.compatible_adjust(None);
        assert_eq!(
            case3_cfg.coprocessor.region_split_size,
            Some(ReadableSize::mb(32))
        );
        case3_cfg.compatible_adjust(Some(&cfg_from_file));
        assert_eq!(
            case3_cfg.coprocessor.region_split_size,
            Some(ReadableSize::mb(32))
        );
        assert_eq!(
            cfg_from_file.coprocessor.region_max_size,
            case3_cfg.coprocessor.region_max_size
        );
        assert_eq!(
            cfg_from_file.coprocessor.region_max_keys,
            case3_cfg.coprocessor.region_max_keys
        );
        assert_eq!(
            cfg_from_file.coprocessor.region_split_keys,
            case3_cfg.coprocessor.region_split_keys
        );
        // Invalid configuration as `regions-split-size` > inherited.`region-max-size`
        case3_cfg.coprocessor.validate(false).unwrap_err();

        // Case 4: all settings are manually changed in the current config file,
        // then make it compatible to the last config. The current
        // configuration should not be changed.
        let content = r#"
        [coprocessor]
        region-split-size = "24MiB"
        region-max-size = "32MiB"
        region-split-keys = 24000
        region-max-keys = 32000
        "#;
        let mut case4_cfg: TikvConfig = toml::from_str(content).unwrap();
        case4_cfg.compatible_adjust(Some(&cfg_from_file));
        assert_eq!(
            case4_cfg.coprocessor.region_split_size,
            Some(ReadableSize::mb(24))
        );
        assert_eq!(
            case4_cfg.coprocessor.region_max_size,
            Some(ReadableSize::mb(32))
        );
        assert_eq!(case4_cfg.coprocessor.region_split_keys, Some(24000));
        assert_eq!(case4_cfg.coprocessor.region_max_keys, Some(32000));
    }

    #[test]
    fn test_compatibility_with_old_config_template() {
        let mut buf = Vec::new();
        let resp = reqwest::blocking::get(
            "https://raw.githubusercontent.com/tikv/tikv/master/etc/config-template.toml",
        );
        match resp {
            Ok(mut resp) => {
                std::io::copy(&mut resp, &mut buf).expect("failed to copy content");
                let template_config = std::str::from_utf8(&buf)
                    .unwrap()
                    .lines()
                    .map(|l| l.strip_prefix('#').unwrap_or(l))
                    .join("\n");
                let _: TikvConfig = toml::from_str(&template_config).unwrap();
            }
            Err(e) => {
                if e.is_timeout() {
                    println!("warn: fail to download latest config template due to timeout");
                } else {
                    panic!("fail to download latest config template");
                }
            }
        }
    }

    #[test]
    fn test_cdc() {
        let content = r#"
            [cdc]
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        // old-value-cache-size is deprecated, 0 must not report error.
        let content = r#"
            [cdc]
            old-value-cache-size = 0
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        let content = r#"
            [cdc]
            min-ts-interval = "0s"
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        let content = r#"
            [cdc]
            incremental-scan-threads = 0
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        let content = r#"
            [cdc]
            incremental-scan-concurrency = 0
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        let content = r#"
            [cdc]
            incremental-scan-concurrency = 1
            incremental-scan-threads = 2
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        let content = r#"
            [cdc]
            incremental-scan-concurrency = 6
            incremental-scan-concurrency-limit = 0
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();
        assert!(cfg.cdc.incremental_scan_concurrency_limit >= cfg.cdc.incremental_scan_concurrency);

        let content = r#"
            [storage]
            engine = "partitioned-raft-kv"
            [cdc]
            hibernate-regions-compatible = true
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();
        assert!(!cfg.cdc.hibernate_regions_compatible);
    }

    #[test]
    fn test_module_from_str() {
        let cases = vec![
            ("readpool", Module::Readpool),
            ("server", Module::Server),
            ("metric", Module::Metric),
            ("raft_store", Module::Raftstore),
            ("coprocessor", Module::Coprocessor),
            ("pd", Module::Pd),
            ("split", Module::Split),
            ("rocksdb", Module::Rocksdb),
            ("raft_engine", Module::RaftEngine),
            ("storage", Module::Storage),
            ("security", Module::Security),
            ("import", Module::Import),
            ("backup", Module::Backup),
            ("log_backup", Module::BackupStream),
            ("pessimistic_txn", Module::PessimisticTxn),
            ("gc", Module::Gc),
            ("cdc", Module::Cdc),
            ("resolved_ts", Module::ResolvedTs),
            ("resource_metering", Module::ResourceMetering),
            ("unknown", Module::Unknown("unknown".to_string())),
        ];
        for (name, module) in cases {
            assert_eq!(Module::from(name), module);
        }
    }

    #[test]
    fn test_numeric_enum_serializing() {
        let normal_string_config = r#"
            compaction-style = 1
        "#;
        let config: DefaultCfConfig = toml::from_str(normal_string_config).unwrap();
        assert_eq!(config.compaction_style, DBCompactionStyle::Universal);

        // Test if we support string value
        let normal_string_config = r#"
            compaction-style = "universal"
        "#;
        let config: DefaultCfConfig = toml::from_str(normal_string_config).unwrap();
        assert_eq!(config.compaction_style, DBCompactionStyle::Universal);
        assert!(
            toml::to_string(&config)
                .unwrap()
                .contains("compaction-style = 1")
        );

        let bad_string_config = r#"
            compaction-style = "level1"
        "#;
        let r = panic_hook::recover_safe(|| {
            let _: DefaultCfConfig = toml::from_str(bad_string_config).unwrap();
        });
        r.unwrap_err();

        let bad_string_config = r#"
            compaction-style = 4
        "#;
        let r = panic_hook::recover_safe(|| {
            let _: DefaultCfConfig = toml::from_str(bad_string_config).unwrap();
        });
        r.unwrap_err();

        // rate-limiter-mode default values is 2
        let config_str = r#"
            rate-limiter-mode = 1
        "#;

        let config: DbConfig = toml::from_str(config_str).unwrap();
        assert_eq!(config.rate_limiter_mode, DBRateLimiterMode::ReadOnly);

        assert!(
            toml::to_string(&config)
                .unwrap()
                .contains("rate-limiter-mode = 1")
        );
    }

    #[test]
    fn test_serde_to_online_config() {
        let cases = vec![
            (
                "raftstore.store_pool_size",
                "raft_store.store_batch_system.pool_size",
            ),
            (
                "raftstore.store-pool-size",
                "raft_store.store_batch_system.pool_size",
            ),
            (
                "raftstore.store_max_batch_size",
                "raft_store.store_batch_system.max_batch_size",
            ),
            (
                "raftstore.store-max-batch-size",
                "raft_store.store_batch_system.max_batch_size",
            ),
            (
                "raftstore.apply_pool_size",
                "raft_store.apply_batch_system.pool_size",
            ),
            (
                "raftstore.apply-pool-size",
                "raft_store.apply_batch_system.pool_size",
            ),
            (
                "raftstore.apply_max_batch_size",
                "raft_store.apply_batch_system.max_batch_size",
            ),
            (
                "raftstore.apply-max-batch-size",
                "raft_store.apply_batch_system.max_batch_size",
            ),
            (
                "raftstore.store_io_pool_size",
                "raft_store.store_io_pool_size",
            ),
            (
                "raftstore.store-io-pool-size",
                "raft_store.store_io_pool_size",
            ),
            (
                "raftstore.apply_yield_duration",
                "raft_store.apply_yield_duration",
            ),
            (
                "raftstore.apply-yield-duration",
                "raft_store.apply_yield_duration",
            ),
            (
                "raftstore.raft_store_max_leader_lease",
                "raft_store.raft_store_max_leader_lease",
            ),
        ];

        for (name, res) in cases {
            assert_eq!(serde_to_online_config(name.into()).as_str(), res);
        }
    }

    #[test]
    fn test_flow_control() {
        let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
        cfg.storage.flow_control.l0_files_threshold = 50;
        cfg.validate().unwrap();
        let (storage, cfg_controller, _, flow_controller) = new_engines::<ApiV1>(cfg);
        let db = storage.get_engine().get_rocksdb();
        // level0_slowdown_writes_trigger should preserve its default value (20) when
        // it's smaller than l0_files_threshold (50), only override if it's too
        // large
        assert_eq!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_level_zero_slowdown_writes_trigger(),
            20 // Default value preserved since 20 < 50 (l0_files_threshold)
        );
        // level0_stop_writes_trigger is overridden for flow control correctness
        assert_eq!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_level_zero_stop_writes_trigger(),
            50
        );

        assert!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_disable_write_stall()
        );
        assert!(flow_controller.enabled());
        cfg_controller
            .update_config("storage.flow-control.enable", "false")
            .unwrap();
        assert!(
            !db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_disable_write_stall()
        );
        assert!(!flow_controller.enabled());
        cfg_controller
            .update_config("storage.flow-control.enable", "true")
            .unwrap();
        assert!(
            db.get_options_cf(CF_DEFAULT)
                .unwrap()
                .get_disable_write_stall()
        );
        assert!(flow_controller.enabled());
    }

    #[test]
    fn test_flow_control_override_with_default_values() {
        // When flow control thresholds are higher than defaults, preserve default
        // values
        let content = r#"
            [storage.flow-control]
            enable = true
            l0-files-threshold = 50
            soft-pending-compaction-bytes-limit = "300GB"
            hard-pending-compaction-bytes-limit = "500GB"
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        // level0_slowdown_writes_trigger default (20) < l0_files_threshold (50), should
        // be preserved
        assert_eq!(cfg.rocksdb.defaultcf.level0_slowdown_writes_trigger, 20);
        // level0_stop_writes_trigger should always be set to flow control threshold
        assert_eq!(cfg.rocksdb.defaultcf.level0_stop_writes_trigger, Some(50));
        // soft_pending_compaction_bytes_limit default (192GB) < flow control limit
        // (300GB), should be preserved
        assert_eq!(
            cfg.rocksdb.defaultcf.soft_pending_compaction_bytes_limit,
            ReadableSize::gb(192)
        );
        // hard_pending_compaction_bytes_limit should always be set to flow control
        // threshold
        assert_eq!(
            cfg.rocksdb.defaultcf.hard_pending_compaction_bytes_limit,
            Some(ReadableSize::gb(500))
        );
    }

    #[test]
    fn test_flow_control_override_disabled() {
        // When flow control is disabled, preserve all user configurations
        let content = r#"
            [storage.flow-control]
            enable = false
            l0-files-threshold = 10
            soft-pending-compaction-bytes-limit = "100GB"
            hard-pending-compaction-bytes-limit = "200GB"
            [rocksdb.defaultcf]
            level0-slowdown-writes-trigger = 888
            level0-stop-writes-trigger = 999
            soft-pending-compaction-bytes-limit = "888GB"
            hard-pending-compaction-bytes-limit = "999GB"
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        // All user configurations should be preserved when flow control is disabled
        assert_eq!(cfg.rocksdb.defaultcf.level0_slowdown_writes_trigger, 888);
        assert_eq!(cfg.rocksdb.defaultcf.level0_stop_writes_trigger, Some(999));
        assert_eq!(
            cfg.rocksdb.defaultcf.soft_pending_compaction_bytes_limit,
            ReadableSize::gb(888)
        );
        assert_eq!(
            cfg.rocksdb.defaultcf.hard_pending_compaction_bytes_limit,
            Some(ReadableSize::gb(999))
        );
    }

    #[test]
    fn test_flow_control_override_user_config_preserved() {
        // When user config is smaller than flow control threshold, preserve user values
        let content = r#"
            [storage.flow-control]
            enable = true
            l0-files-threshold = 77
            soft-pending-compaction-bytes-limit = "777GB"
            hard-pending-compaction-bytes-limit = "888GB"
            [rocksdb.defaultcf]
            level0-slowdown-writes-trigger = 66
            level0-stop-writes-trigger = 70
            soft-pending-compaction-bytes-limit = "666GB"
            hard-pending-compaction-bytes-limit = "800GB"
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        // User configs smaller than flow control thresholds should be preserved
        assert_eq!(cfg.rocksdb.defaultcf.level0_slowdown_writes_trigger, 66);
        assert_eq!(cfg.rocksdb.defaultcf.level0_stop_writes_trigger, Some(70));
        assert_eq!(
            cfg.rocksdb.defaultcf.soft_pending_compaction_bytes_limit,
            ReadableSize::gb(666)
        );
        assert_eq!(
            cfg.rocksdb.defaultcf.hard_pending_compaction_bytes_limit,
            Some(ReadableSize::gb(800))
        );
    }

    #[test]
    fn test_flow_control_override_user_config_too_large() {
        // When user config is larger than flow control threshold, override with
        // threshold
        let content = r#"
            [storage.flow-control]
            enable = true
            l0-files-threshold = 10
            soft-pending-compaction-bytes-limit = "50GB"
            hard-pending-compaction-bytes-limit = "100GB"
            [rocksdb.defaultcf]
            level0-slowdown-writes-trigger = 88
            level0-stop-writes-trigger = 99
            soft-pending-compaction-bytes-limit = "888GB"
            hard-pending-compaction-bytes-limit = "999GB"
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();

        // User configs larger than flow control thresholds should be overridden
        assert_eq!(cfg.rocksdb.defaultcf.level0_slowdown_writes_trigger, 10);
        assert_eq!(cfg.rocksdb.defaultcf.level0_stop_writes_trigger, Some(10));
        assert_eq!(
            cfg.rocksdb.defaultcf.soft_pending_compaction_bytes_limit,
            ReadableSize::gb(50)
        );
        assert_eq!(
            cfg.rocksdb.defaultcf.hard_pending_compaction_bytes_limit,
            Some(ReadableSize::gb(100))
        );
    }

    #[test]
    fn test_in_memory_engine_and_api_version() {
        let tests = [
            (
                true,
                vec![
                    r#"
                        [in-memory-engine]
                    "#,
                    r#"
                        [in-memory-engine]
                        enable = true
                        evict-threshold = "1GB"
                        capacity = "2GB"
                    "#,
                    r#"
                        [in-memory-engine]
                        enable = false
                    "#,
                    // Ok if in-memory engine is off.
                    r#"
                        [in-memory-engine]
                        enable = false
                        [storage]
                        api-version = 1
                        enable-ttl = true
                    "#,
                    r#"
                        [in-memory-engine]
                        enable = false
                        [storage]
                        api-version = 2
                        enable-ttl = true
                    "#,
                ],
            ),
            (
                false,
                vec![
                    // Error for incompatiable API version.
                    r#"
                        [in-memory-engine]
                        enable = true
                        evict-threshold = "1GB"
                        capacity = "2GB"
                        [storage]
                        api-version = 1
                        enable-ttl = true
                    "#,
                    r#"
                        [in-memory-engine]
                        enable = true
                        evict-threshold = "1GB"
                        capacity = "2GB"
                        [storage]
                        api-version = 2
                        enable-ttl = true
                    "#,
                ],
            ),
        ];

        for t in tests {
            for content in t.1 {
                let mut cfg: TikvConfig = toml::from_str(content).unwrap();
                if t.0 {
                    cfg.validate().unwrap();
                } else {
                    cfg.validate().unwrap_err();
                }
                must_no_unknown_key(content);
            }
        }
    }

    #[test]
    fn test_in_memory_engine_change_config() {
        let content = r#"
            [in-memory-engine]
            enable = true
            evict-threshold = "1GB"
            capacity = "2GB"
        "#;
        let mut cfg: TikvConfig = toml::from_str(content).unwrap();
        cfg.validate().unwrap();
        let cfg_controller = ConfigController::new(cfg.clone());
        let version_tracker = Arc::new(VersionTrack::new(cfg.in_memory_engine.clone()));
        cfg_controller.register(
            Module::InMemoryEngine,
            Box::new(InMemoryEngineConfigManager::new(version_tracker.clone())),
        );

        let check_cfg = |cfg: &TikvConfig| {
            assert_eq_debug(&cfg_controller.get_current(), cfg);
            assert_eq!(&*version_tracker.value(), &cfg.in_memory_engine);
        };

        cfg_controller
            .update_config("in-memory-engine.capacity", "3GB")
            .unwrap();
        cfg.in_memory_engine.capacity = Some(ReadableSize::gb(3));
        check_cfg(&cfg);

        cfg_controller
            .update_config("in-memory-engine.evict-threshold", "2GB")
            .unwrap();
        cfg.in_memory_engine.evict_threshold = Some(ReadableSize::gb(2));
        check_cfg(&cfg);

        cfg_controller
            .update_config("in-memory-engine.stop-load-threshold", "1GB")
            .unwrap();
        cfg.in_memory_engine.stop_load_threshold = Some(ReadableSize::gb(1));
        check_cfg(&cfg);

        cfg_controller
            .update_config("in-memory-engine.mvcc-amplification-threshold", "777")
            .unwrap();
        cfg.in_memory_engine.mvcc_amplification_threshold = 777;
        check_cfg(&cfg);

        cfg_controller
            .update_config("in-memory-engine.gc-run-interval", "7m")
            .unwrap();
        cfg.in_memory_engine.gc_run_interval = ReadableDuration::minutes(7);
        check_cfg(&cfg);

        cfg_controller
            .update_config("in-memory-engine.enable", "false")
            .unwrap();
        cfg.in_memory_engine.enable = false;
        check_cfg(&cfg);

        // Test snake case.
        cfg_controller
            .update_config("in_memory_engine.enable", "true")
            .unwrap();
        cfg.in_memory_engine.enable = true;
        check_cfg(&cfg);
    }
}
