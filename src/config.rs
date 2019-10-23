// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

//! Configuration for the entire server.
//!
//! TiKV is configured through the `TiKvConfig` type, which is in turn
//! made up of many other configuration types.

use std::cmp;
use std::error::Error;
use std::fs;
use std::i32;
use std::io::Error as IoError;
use std::io::Write;
use std::path::Path;
use std::usize;

use engine::rocks::{
    BlockBasedOptions, Cache, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle,
    DBCompressionType, DBOptions, DBRateLimiterMode, DBRecoveryMode, LRUCacheOptions,
    TitanDBOptions,
};
use slog;
use sys_info;

use crate::import::Config as ImportConfig;
use crate::raftstore::coprocessor::properties::{
    MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory,
};
use crate::raftstore::coprocessor::properties::{
    DEFAULT_PROP_KEYS_INDEX_DISTANCE, DEFAULT_PROP_SIZE_INDEX_DISTANCE,
};
use crate::raftstore::coprocessor::Config as CopConfig;
use crate::raftstore::store::keys::region_raft_prefix_len;
use crate::raftstore::store::Config as RaftstoreConfig;
use crate::server::lock_manager::Config as PessimisticTxnConfig;
use crate::server::Config as ServerConfig;
use crate::server::CONFIG_ROCKSDB_GAUGE;
use crate::storage::config::DEFAULT_DATA_DIR;
use crate::storage::{Config as StorageConfig, DEFAULT_ROCKSDB_SUB_DIR};
use engine::rocks::util::config::{self as rocks_config, BlobRunMode, CompressionType};
use engine::rocks::util::{
    db_exist, CFOptions, EventListener, FixedPrefixSliceTransform, FixedSuffixSliceTransform,
    NoopSliceTransform,
};
use engine::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use pd_client::Config as PdConfig;
use tikv_util::config::{self, ReadableDuration, ReadableSize, GB, KB, MB};
use tikv_util::future_pool;
use tikv_util::security::SecurityConfig;
use tikv_util::time::duration_to_sec;

const LOCKCF_MIN_MEM: usize = 256 * MB as usize;
const LOCKCF_MAX_MEM: usize = GB as usize;
const RAFT_MIN_MEM: usize = 256 * MB as usize;
const RAFT_MAX_MEM: usize = 2 * GB as usize;
const LAST_CONFIG_FILE: &str = "last_tikv.toml";
const MAX_BLOCK_SIZE: usize = 32 * MB as usize;

fn memory_mb_for_cf(is_raft_db: bool, cf: &str) -> usize {
    let total_mem = sys_info::mem_info().unwrap().total * KB;
    let (ratio, min, max) = match (is_raft_db, cf) {
        (true, CF_DEFAULT) => (0.02, RAFT_MIN_MEM, RAFT_MAX_MEM),
        (false, CF_DEFAULT) => (0.25, 0, usize::MAX),
        (false, CF_LOCK) => (0.02, LOCKCF_MIN_MEM, LOCKCF_MAX_MEM),
        (false, CF_WRITE) => (0.15, 0, usize::MAX),
        _ => unreachable!(),
    };
    let mut size = (total_mem as f64 * ratio) as usize;
    if size < min {
        size = min;
    } else if size > max {
        size = max;
    }
    size / MB as usize
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TitanCfConfig {
    pub min_blob_size: ReadableSize,
    pub blob_file_compression: CompressionType,
    pub blob_cache_size: ReadableSize,
    pub min_gc_batch_size: ReadableSize,
    pub max_gc_batch_size: ReadableSize,
    pub discardable_ratio: f64,
    pub sample_ratio: f64,
    pub merge_small_file_threshold: ReadableSize,
    pub blob_run_mode: BlobRunMode,
}

impl Default for TitanCfConfig {
    fn default() -> Self {
        Self {
            min_blob_size: ReadableSize::kb(1), // disable titan default
            blob_file_compression: CompressionType::Lz4,
            blob_cache_size: ReadableSize::mb(0),
            min_gc_batch_size: ReadableSize::mb(16),
            max_gc_batch_size: ReadableSize::mb(64),
            discardable_ratio: 0.5,
            sample_ratio: 0.1,
            merge_small_file_threshold: ReadableSize::mb(8),
            blob_run_mode: BlobRunMode::Normal,
        }
    }
}

impl TitanCfConfig {
    fn build_opts(&self) -> TitanDBOptions {
        let mut opts = TitanDBOptions::new();
        opts.set_min_blob_size(self.min_blob_size.0 as u64);
        opts.set_blob_file_compression(self.blob_file_compression.into());
        opts.set_blob_cache(self.blob_cache_size.0 as usize, -1, false, 0.0);
        opts.set_min_gc_batch_size(self.min_gc_batch_size.0 as u64);
        opts.set_max_gc_batch_size(self.max_gc_batch_size.0 as u64);
        opts.set_discardable_ratio(self.discardable_ratio);
        opts.set_sample_ratio(self.sample_ratio);
        opts.set_merge_small_file_threshold(self.merge_small_file_threshold.0 as u64);
        opts.set_blob_run_mode(self.blob_run_mode.into());
        opts
    }
}

macro_rules! cf_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            pub disable_block_cache: bool,
            pub cache_index_and_filter_blocks: bool,
            pub pin_l0_filter_and_index_blocks: bool,
            pub use_bloom_filter: bool,
            pub optimize_filters_for_hits: bool,
            pub whole_key_filtering: bool,
            pub bloom_filter_bits_per_key: i32,
            pub block_based_bloom_filter: bool,
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "rocks_config::compression_type_level_serde")]
            pub compression_per_level: [DBCompressionType; 7],
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub level0_file_num_compaction_trigger: i32,
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: i32,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "rocks_config::compaction_pri_serde")]
            pub compaction_pri: CompactionPriority,
            pub dynamic_level_bytes: bool,
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
            #[serde(with = "rocks_config::compaction_style_serde")]
            pub compaction_style: DBCompactionStyle,
            pub disable_auto_compactions: bool,
            pub soft_pending_compaction_bytes_limit: ReadableSize,
            pub hard_pending_compaction_bytes_limit: ReadableSize,
            pub force_consistency_checks: bool,
            pub prop_size_index_distance: u64,
            pub prop_keys_index_distance: u64,
            pub enable_doubly_skiplist: bool,
            pub titan: TitanCfConfig,
        }

        impl $name {
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
                Ok(())
            }
        }
    };
}

macro_rules! write_into_metrics {
    ($cf:expr, $tag:expr, $metrics:expr) => {{
        $metrics
            .with_label_values(&[$tag, "block_size"])
            .set($cf.block_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "block_cache_size"])
            .set($cf.block_cache_size.0 as f64);
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
            .with_label_values(&[$tag, "whole_key_filtering"])
            .set(($cf.whole_key_filtering as i32).into());
        $metrics
            .with_label_values(&[$tag, "bloom_filter_bits_per_key"])
            .set($cf.bloom_filter_bits_per_key.into());
        $metrics
            .with_label_values(&[$tag, "block_based_bloom_filter"])
            .set(($cf.block_based_bloom_filter as i32).into());

        $metrics
            .with_label_values(&[$tag, "read_amp_bytes_per_bit"])
            .set($cf.read_amp_bytes_per_bit.into());
        $metrics
            .with_label_values(&[$tag, "write_buffer_size"])
            .set($cf.write_buffer_size.0 as f64);
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
            .set($cf.target_file_size_base.0 as f64);
        $metrics
            .with_label_values(&[$tag, "level0_file_num_compaction_trigger"])
            .set($cf.level0_file_num_compaction_trigger.into());
        $metrics
            .with_label_values(&[$tag, "level0_slowdown_writes_trigger"])
            .set($cf.level0_slowdown_writes_trigger.into());
        $metrics
            .with_label_values(&[$tag, "level0_stop_writes_trigger"])
            .set($cf.level0_stop_writes_trigger.into());
        $metrics
            .with_label_values(&[$tag, "max_compaction_bytes"])
            .set($cf.max_compaction_bytes.0 as f64);
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
            .with_label_values(&[$tag, "disable_auto_compactions"])
            .set(($cf.disable_auto_compactions as i32).into());
        $metrics
            .with_label_values(&[$tag, "soft_pending_compaction_bytes_limit"])
            .set($cf.soft_pending_compaction_bytes_limit.0 as f64);
        $metrics
            .with_label_values(&[$tag, "hard_pending_compaction_bytes_limit"])
            .set($cf.hard_pending_compaction_bytes_limit.0 as f64);
        $metrics
            .with_label_values(&[$tag, "force_consistency_checks"])
            .set(($cf.force_consistency_checks as i32).into());
        $metrics
            .with_label_values(&[$tag, "enable_doubly_skiplist"])
            .set(($cf.enable_doubly_skiplist as i32).into());
        $metrics
            .with_label_values(&[$tag, "titan_min_blob_size"])
            .set($cf.titan.min_blob_size.0 as f64);
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
            .with_label_values(&[$tag, "titan_sample_ratio"])
            .set($cf.titan.sample_ratio);
        $metrics
            .with_label_values(&[$tag, "titan_merge_small_file_threshold"])
            .set($cf.titan.merge_small_file_threshold.0 as f64);
    }};
}

macro_rules! build_cf_opt {
    ($opt:ident, $cache:ident) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        if let Some(cache) = $cache {
            block_base_opts.set_block_cache(cache);
        } else {
            let mut cache_opts = LRUCacheOptions::new();
            cache_opts.set_capacity($opt.block_cache_size.0 as usize);
            block_base_opts.set_block_cache(&Cache::new_lru_cache(cache_opts));
        }
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts
            .set_pin_l0_filter_and_index_blocks_in_cache($opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            block_base_opts.set_bloom_filter(
                $opt.bloom_filter_bits_per_key,
                $opt.block_based_bloom_filter,
            );
            block_base_opts.set_whole_key_filtering($opt.whole_key_filtering);
        }
        block_base_opts.set_read_amp_bytes_per_bit($opt.read_amp_bytes_per_bit);
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.set_num_levels($opt.num_levels);
        assert!($opt.compression_per_level.len() >= $opt.num_levels as usize);
        let compression_per_level = $opt.compression_per_level[..$opt.num_levels as usize].to_vec();
        cf_opts.compression_per_level(compression_per_level.as_slice());
        cf_opts.set_write_buffer_size($opt.write_buffer_size.0);
        cf_opts.set_max_write_buffer_number($opt.max_write_buffer_number);
        cf_opts.set_min_write_buffer_number_to_merge($opt.min_write_buffer_number_to_merge);
        cf_opts.set_max_bytes_for_level_base($opt.max_bytes_for_level_base.0);
        cf_opts.set_target_file_size_base($opt.target_file_size_base.0);
        cf_opts.set_level_zero_file_num_compaction_trigger($opt.level0_file_num_compaction_trigger);
        cf_opts.set_level_zero_slowdown_writes_trigger($opt.level0_slowdown_writes_trigger);
        cf_opts.set_level_zero_stop_writes_trigger($opt.level0_stop_writes_trigger);
        cf_opts.set_max_compaction_bytes($opt.max_compaction_bytes.0);
        cf_opts.compaction_priority($opt.compaction_pri);
        cf_opts.set_level_compaction_dynamic_level_bytes($opt.dynamic_level_bytes);
        cf_opts.set_max_bytes_for_level_multiplier($opt.max_bytes_for_level_multiplier);
        cf_opts.set_compaction_style($opt.compaction_style);
        cf_opts.set_disable_auto_compactions($opt.disable_auto_compactions);
        cf_opts.set_soft_pending_compaction_bytes_limit($opt.soft_pending_compaction_bytes_limit.0);
        cf_opts.set_hard_pending_compaction_bytes_limit($opt.hard_pending_compaction_bytes_limit.0);
        cf_opts.set_optimize_filters_for_hits($opt.optimize_filters_for_hits);
        cf_opts.set_force_consistency_checks($opt.force_consistency_checks);
        if $opt.enable_doubly_skiplist {
            cf_opts.set_doubly_skiplist();
        }
        cf_opts
    }};
}

cf_config!(DefaultCfConfig);

impl Default for DefaultCfConfig {
    fn default() -> DefaultCfConfig {
        DefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_DEFAULT) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
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
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan: TitanCfConfig::default(),
        }
    }
}

impl DefaultCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        });
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(WriteCfConfig);

impl Default for WriteCfConfig {
    fn default() -> WriteCfConfig {
        // Setting blob_run_mode=read_only effectively disable Titan.
        let mut titan = TitanCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
        WriteCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_WRITE) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_key_filtering: false,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
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
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl WriteCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        // Prefix extractor(trim the timestamp at tail) for write cf.
        let e = Box::new(FixedSuffixSliceTransform::new(8));
        cf_opts
            .set_prefix_extractor("FixedSuffixSliceTransform", e)
            .unwrap();
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        // Collects user defined properties.
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
        let f = Box::new(RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        });
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(LockCfConfig);

impl Default for LockCfConfig {
    fn default() -> LockCfConfig {
        // Setting blob_run_mode=read_only effectively disable Titan.
        let mut titan = TitanCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
        LockCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_LOCK) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl LockCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(NoopSliceTransform);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

cf_config!(RaftCfConfig);

impl Default for RaftCfConfig {
    fn default() -> RaftCfConfig {
        // Setting blob_run_mode=read_only effectively disable Titan.
        let mut titan = TitanCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
        RaftCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(128),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl RaftCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(NoopSliceTransform);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
// Note that Titan is still an experimental feature. Once enabled, it can't fall back.
// Forced fallback may result in data loss.
pub struct TitanDBConfig {
    pub enabled: bool,
    pub dirname: String,
    pub disable_gc: bool,
    pub max_background_gc: i32,
    pub purge_obsolete_files_period: ReadableDuration,
}

impl Default for TitanDBConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dirname: "".to_owned(),
            disable_gc: false,
            max_background_gc: 1,
            purge_obsolete_files_period: ReadableDuration::secs(10),
        }
    }
}

impl TitanDBConfig {
    fn build_opts(&self) -> TitanDBOptions {
        let mut opts = TitanDBOptions::new();
        opts.set_dirname(&self.dirname);
        opts.set_disable_background_gc(self.disable_gc);
        opts.set_max_background_gc(self.max_background_gc);
        opts.set_purge_obsolete_files_period(self.purge_obsolete_files_period.as_secs() as usize);
        opts
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[serde(with = "rocks_config::recovery_mode_serde")]
    pub wal_recovery_mode: DBRecoveryMode,
    pub wal_dir: String,
    pub wal_ttl_seconds: u64,
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_manifest_file_size: ReadableSize,
    pub create_if_missing: bool,
    pub max_open_files: i32,
    pub enable_statistics: bool,
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    pub info_log_max_size: ReadableSize,
    pub info_log_roll_time: ReadableDuration,
    pub info_log_keep_log_file_num: u64,
    pub info_log_dir: String,
    pub rate_bytes_per_sec: ReadableSize,
    #[serde(with = "rocks_config::rate_limiter_mode_serde")]
    pub rate_limiter_mode: DBRateLimiterMode,
    pub auto_tuned: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub enable_pipelined_write: bool,
    pub defaultcf: DefaultCfConfig,
    pub writecf: WriteCfConfig,
    pub lockcf: LockCfConfig,
    pub raftcf: RaftCfConfig,
    pub titan: TitanDBConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: 6,
            max_manifest_file_size: ReadableSize::mb(128),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            rate_bytes_per_sec: ReadableSize::kb(0),
            rate_limiter_mode: DBRateLimiterMode::WriteOnly,
            auto_tuned: false,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_compactions: 1,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            defaultcf: DefaultCfConfig::default(),
            writecf: WriteCfConfig::default(),
            lockcf: LockCfConfig::default(),
            raftcf: RaftCfConfig::default(),
            titan: TitanDBConfig::default(),
        }
    }
}

impl DbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        if !self.info_log_dir.is_empty() {
            opts.create_info_log(&self.info_log_dir)
                .unwrap_or_else(|e| {
                    panic!(
                        "create RocksDB info log {} error: {:?}",
                        self.info_log_dir, e
                    );
                })
        }

        if self.rate_bytes_per_sec.0 > 0 {
            opts.set_ratelimiter_with_auto_tuned(
                self.rate_bytes_per_sec.0 as i64,
                self.rate_limiter_mode,
                self.auto_tuned,
            );
        }

        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.add_event_listener(EventListener::new("kv"));

        if self.titan.enabled {
            opts.set_titandb_options(&self.titan.build_opts());
        }
        opts
    }

    pub fn build_cf_opts(&self, cache: &Option<Cache>) -> Vec<CFOptions<'_>> {
        vec![
            CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt(cache)),
            CFOptions::new(CF_LOCK, self.lockcf.build_opt(cache)),
            CFOptions::new(CF_WRITE, self.writecf.build_opt(cache)),
            // TODO: rmeove CF_RAFT.
            CFOptions::new(CF_RAFT, self.raftcf.build_opt(cache)),
        ]
    }

    pub fn build_cf_opts_v2(&self, cache: &Option<Cache>) -> Vec<CFOptions<'_>> {
        vec![
            CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt(cache)),
            CFOptions::new(CF_LOCK, self.lockcf.build_opt(cache)),
            CFOptions::new(CF_WRITE, self.writecf.build_opt(cache)),
            CFOptions::new(CF_RAFT, self.raftcf.build_opt(cache)),
        ]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcf.validate()?;
        self.lockcf.validate()?;
        self.writecf.validate()?;
        self.raftcf.validate()?;
        self.titan.validate()?;
        Ok(())
    }

    fn write_into_metrics(&self) {
        write_into_metrics!(self.defaultcf, CF_DEFAULT, CONFIG_ROCKSDB_GAUGE);
        write_into_metrics!(self.lockcf, CF_LOCK, CONFIG_ROCKSDB_GAUGE);
        write_into_metrics!(self.writecf, CF_WRITE, CONFIG_ROCKSDB_GAUGE);
        write_into_metrics!(self.raftcf, CF_RAFT, CONFIG_ROCKSDB_GAUGE);
    }
}

cf_config!(RaftDefaultCfConfig);

impl Default for RaftDefaultCfConfig {
    fn default() -> RaftDefaultCfConfig {
        RaftDefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(true, CF_DEFAULT) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
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
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan: TitanCfConfig::default(),
        }
    }
}

impl RaftDefaultCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(FixedPrefixSliceTransform::new(region_raft_prefix_len()));
        cf_opts
            .set_memtable_insert_hint_prefix_extractor("RaftPrefixSliceTransform", f)
            .unwrap();
        cf_opts
    }
}

// RocksDB Env associate thread pools of multiple instances from the same process.
// When construct Options, options.env is set to same singleton Env::Default() object.
// So total max_background_jobs = max(rocksdb.max_background_jobs, raftdb.max_background_jobs)
// But each instance will limit their background jobs according to their own max_background_jobs
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftDbConfig {
    #[serde(with = "rocks_config::recovery_mode_serde")]
    pub wal_recovery_mode: DBRecoveryMode,
    pub wal_dir: String,
    pub wal_ttl_seconds: u64,
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_manifest_file_size: ReadableSize,
    pub create_if_missing: bool,
    pub max_open_files: i32,
    pub enable_statistics: bool,
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    pub info_log_max_size: ReadableSize,
    pub info_log_roll_time: ReadableDuration,
    pub info_log_keep_log_file_num: u64,
    pub info_log_dir: String,
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub enable_pipelined_write: bool,
    pub allow_concurrent_memtable_write: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    pub defaultcf: RaftDefaultCfConfig,
}

impl Default for RaftDbConfig {
    fn default() -> RaftDbConfig {
        RaftDbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: 4,
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
            max_sub_compactions: 2,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            allow_concurrent_memtable_write: false,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            defaultcf: RaftDefaultCfConfig::default(),
        }
    }
}

impl RaftDbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        if !self.info_log_dir.is_empty() {
            opts.create_info_log(&self.info_log_dir)
                .unwrap_or_else(|e| {
                    panic!(
                        "create RocksDB info log {} error: {:?}",
                        self.info_log_dir, e
                    );
                })
        }
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write);
        opts.add_event_listener(EventListener::new("raft"));
        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        // TODO maybe create a new env for raft engine

        opts
    }

    pub fn build_cf_opts(&self, cache: &Option<Cache>) -> Vec<CFOptions<'_>> {
        vec![CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt(cache))]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcf.validate()?;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MetricConfig {
    pub interval: ReadableDuration,
    pub address: String,
    pub job: String,
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

pub mod log_level_serde {
    use serde::{
        de::{Error, Unexpected},
        Deserialize, Deserializer, Serialize, Serializer,
    };
    use slog::Level;
    use tikv_util::logger::{get_level_by_string, get_string_by_level};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        get_level_by_string(&string)
            .ok_or_else(|| D::Error::invalid_value(Unexpected::Str(&string), &"a valid log level"))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(value: &Level, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        get_string_by_level(*value).serialize(serializer)
    }
}

macro_rules! readpool_config {
    ($struct_name:ident, $test_mod_name:ident, $display_name:expr) => {
        #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $struct_name {
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
            pub fn to_future_pool_configs(&self) -> Vec<future_pool::Config> {
                vec![
                    future_pool::Config {
                        workers: self.low_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_low,
                        stack_size: self.stack_size.0 as usize,
                    },
                    future_pool::Config {
                        workers: self.normal_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_normal,
                        stack_size: self.stack_size.0 as usize,
                    },
                    future_pool::Config {
                        workers: self.high_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_high,
                        stack_size: self.stack_size.0 as usize,
                    },
                ]
            }

            pub fn default_for_test() -> Self {
                Self {
                    high_concurrency: 2,
                    normal_concurrency: 2,
                    low_concurrency: 2,
                    max_tasks_per_worker_high: 2000,
                    max_tasks_per_worker_normal: 2000,
                    max_tasks_per_worker_low: 2000,
                    stack_size: ReadableSize::mb(1),
                }
            }

            pub fn validate(&self) -> Result<(), Box<dyn Error>> {
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
                if self.stack_size.0 < ReadableSize::mb(2).0 {
                    return Err(
                        format!("readpool.{}.stack-size should be >= 2mb", $display_name).into(),
                    );
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

            #[test]
            fn test_validate() {
                let cfg = $struct_name::default();
                assert!(cfg.validate().is_ok());

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
                assert!(cfg.validate().is_ok());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_normal = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_normal = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_normal = 100;
                assert!(cfg.validate().is_ok());

                let mut invalid_cfg = cfg.clone();
                invalid_cfg.max_tasks_per_worker_low = 0;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_low = 1;
                assert!(invalid_cfg.validate().is_err());
                invalid_cfg.max_tasks_per_worker_low = 100;
                assert!(cfg.validate().is_ok());
            }
        }
    };
}

const DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY: usize = 4;
const DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY: usize = 8;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
const DEFAULT_READPOOL_MAX_TASKS_PER_WORKER: usize = 2 as usize * 1000;

const DEFAULT_READPOOL_STACK_SIZE_MB: u64 = 10;

readpool_config!(StorageReadPoolConfig, storage_read_pool_test, "storage");

impl Default for StorageReadPoolConfig {
    fn default() -> Self {
        let cpu_num = sys_info::cpu_num().unwrap();
        let mut concurrency = (f64::from(cpu_num) * 0.5) as usize;
        concurrency = cmp::max(DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY, concurrency);
        concurrency = cmp::min(DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY, concurrency);
        Self {
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
        let cpu_num = sys_info::cpu_num().unwrap();
        let mut concurrency = (f64::from(cpu_num) * 0.8) as usize;
        concurrency = cmp::max(DEFAULT_COPROCESSOR_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadPoolConfig {
    pub storage: StorageReadPoolConfig,
    pub coprocessor: CoprReadPoolConfig,
}

impl ReadPoolConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.storage.validate()?;
        self.coprocessor.validate()?;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiKvConfig {
    #[serde(with = "log_level_serde")]
    pub log_level: slog::Level,
    pub log_file: String,
    pub log_rotation_timespan: ReadableDuration,
    pub panic_when_unexpected_key_or_data: bool,
    pub readpool: ReadPoolConfig,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub pd: PdConfig,
    pub metric: MetricConfig,
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,
    pub coprocessor: CopConfig,
    pub rocksdb: DbConfig,
    pub raftdb: RaftDbConfig,
    pub security: SecurityConfig,
    pub import: ImportConfig,
    pub pessimistic_txn: PessimisticTxnConfig,
}

impl Default for TiKvConfig {
    fn default() -> TiKvConfig {
        TiKvConfig {
            log_level: slog::Level::Info,
            log_file: "".to_owned(),
            log_rotation_timespan: ReadableDuration::hours(24),
            panic_when_unexpected_key_or_data: false,
            readpool: ReadPoolConfig::default(),
            server: ServerConfig::default(),
            metric: MetricConfig::default(),
            raft_store: RaftstoreConfig::default(),
            coprocessor: CopConfig::default(),
            pd: PdConfig::default(),
            rocksdb: DbConfig::default(),
            raftdb: RaftDbConfig::default(),
            storage: StorageConfig::default(),
            security: SecurityConfig::default(),
            import: ImportConfig::default(),
            pessimistic_txn: PessimisticTxnConfig::default(),
        }
    }
}

impl TiKvConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.readpool.validate()?;
        self.storage.validate()?;

        self.raft_store.region_split_check_diff = self.coprocessor.region_split_size / 16;
        self.raft_store.raftdb_path = if self.raft_store.raftdb_path.is_empty() {
            config::canonicalize_sub_path(&self.storage.data_dir, "raft")?
        } else {
            config::canonicalize_path(&self.raft_store.raftdb_path)?
        };

        let kv_db_path =
            config::canonicalize_sub_path(&self.storage.data_dir, DEFAULT_ROCKSDB_SUB_DIR)?;

        if kv_db_path == self.raft_store.raftdb_path {
            return Err("raft_store.raftdb_path can not same with storage.data_dir/db".into());
        }
        if db_exist(&kv_db_path) && !db_exist(&self.raft_store.raftdb_path) {
            return Err("default rocksdb exist, buf raftdb not exist".into());
        }
        if !db_exist(&kv_db_path) && db_exist(&self.raft_store.raftdb_path) {
            return Err("default rocksdb not exist, buf raftdb exist".into());
        }

        let expect_keepalive = self.raft_store.raft_heartbeat_interval() * 2;
        if expect_keepalive > self.server.grpc_keepalive_time.0 {
            return Err(format!(
                "grpc_keepalive_time is too small, it should not less than the double of \
                 raft tick interval (>= {})",
                duration_to_sec(expect_keepalive)
            )
            .into());
        }

        self.rocksdb.validate()?;
        self.raftdb.validate()?;
        self.server.validate()?;
        self.raft_store.validate()?;
        self.pd.validate()?;
        self.coprocessor.validate()?;
        self.security.validate()?;
        self.import.validate()?;
        self.pessimistic_txn.validate()?;
        Ok(())
    }

    pub fn compatible_adjust(&mut self) {
        let default_raft_store = RaftstoreConfig::default();
        let default_coprocessor = CopConfig::default();
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
                self.coprocessor.region_max_size = self.raft_store.region_max_size;
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
                self.coprocessor.region_split_size = self.raft_store.region_split_size;
            }
            self.raft_store.region_split_size = default_raft_store.region_split_size;
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
            let concurrency = self.server.end_point_concurrency.unwrap();
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
            self.readpool.coprocessor.stack_size = self.server.end_point_stack_size.unwrap();
        }
        if self.server.end_point_max_tasks.is_some() {
            warn!(
                "deprecated configuration, {} is no longer used and ignored, please use {}.",
                "server.end-point-max-tasks", "readpool.coprocessor.max-tasks-per-worker-xxx",
            );
            // Note:
            // Our `end_point_max_tasks` is mostly mistakenly configured, so we don't override
            // new configuration using old values.
            self.server.end_point_max_tasks = None;
        }
        if self.raft_store.clean_stale_peer_delay.as_secs() > 0 {
            let delay_secs = self.raft_store.clean_stale_peer_delay.as_secs()
                + self.server.end_point_request_max_handle_duration.as_secs();
            self.raft_store.clean_stale_peer_delay = ReadableDuration::secs(delay_secs);
        }
        // When shared block cache is enabled, if its capacity is set, it overrides individual
        // block cache sizes. Otherwise use the sum of block cache size of all column families
        // as the shared cache size.
        let cache_cfg = &mut self.storage.block_cache;
        if cache_cfg.shared && cache_cfg.capacity.is_none() {
            cache_cfg.capacity = Some(ReadableSize {
                0: self.rocksdb.defaultcf.block_cache_size.0
                    + self.rocksdb.writecf.block_cache_size.0
                    + self.rocksdb.lockcf.block_cache_size.0
                    + self.raftdb.defaultcf.block_cache_size.0,
            });
        }
    }

    pub fn check_critical_cfg_with(&self, last_cfg: &Self) -> Result<(), String> {
        if last_cfg.rocksdb.wal_dir != self.rocksdb.wal_dir {
            return Err(format!(
                "db wal_dir have been changed, former db wal_dir is '{}', \
                 current db wal_dir is '{}', please guarantee all data wal logs \
                 have been moved to destination directory.",
                last_cfg.rocksdb.wal_dir, self.rocksdb.wal_dir
            ));
        }

        if last_cfg.raftdb.wal_dir != self.raftdb.wal_dir {
            return Err(format!(
                "raftdb wal_dir have been changed, former raftdb wal_dir is '{}', \
                 current raftdb wal_dir is '{}', please guarantee all raft wal logs \
                 have been moved to destination directory.",
                last_cfg.raftdb.wal_dir, self.rocksdb.wal_dir
            ));
        }

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

        if last_cfg.raft_store.raftdb_path != self.raft_store.raftdb_path {
            return Err(format!(
                "raft dir have been changed, former raft dir is '{}', \
                 current raft dir is '{}', please check if it is expected.",
                last_cfg.raft_store.raftdb_path, self.raft_store.raftdb_path
            ));
        }

        Ok(())
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        (|| -> Result<Self, Box<dyn Error>> {
            let s = fs::read_to_string(&path)?;
            Ok(::toml::from_str(&s)?)
        })()
        .unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                path.as_ref().display(),
                e
            );
        })
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
    }
}

/// Prevents launching with an incompatible configuration
///
/// Loads the previously-loaded configuration from `last_tikv.toml`,
/// compares key configuration items and fails if they are not
/// identical.
pub fn check_critical_config(config: &TiKvConfig) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    let store_path = Path::new(&config.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);

    if last_cfg_path.exists() {
        let last_cfg = TiKvConfig::from_file(&last_cfg_path);
        config.check_critical_cfg_with(&last_cfg)?;
    }

    Ok(())
}

/// Persists critical config to `last_tikv.toml`
pub fn persist_critical_config(config: &TiKvConfig) -> Result<(), String> {
    let store_path = Path::new(&config.storage.data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);

    // Create parent directory if missing.
    if let Err(e) = fs::create_dir_all(&store_path) {
        return Err(format!(
            "create parent directory '{}' failed: {}",
            store_path.to_str().unwrap(),
            e
        ));
    }

    // Persist current critical configurations to file.
    if let Err(e) = config.write_to_file(&last_cfg_path) {
        return Err(format!(
            "persist critical config to '{}' failed: {}",
            last_cfg_path.to_str().unwrap(),
            e
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use slog::Level;
    use toml;

    #[test]
    fn test_check_critical_cfg_with() {
        let mut tikv_cfg = TiKvConfig::default();
        let mut last_cfg = TiKvConfig::default();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());

        tikv_cfg.rocksdb.wal_dir = "/data/wal_dir".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.rocksdb.wal_dir = "/data/wal_dir".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());

        tikv_cfg.raftdb.wal_dir = "/raft/wal_dir".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.raftdb.wal_dir = "/raft/wal_dir".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());

        tikv_cfg.storage.data_dir = "/data1".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.storage.data_dir = "/data1".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());

        tikv_cfg.raft_store.raftdb_path = "/raft_path".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_err());

        last_cfg.raft_store.raftdb_path = "/raft_path".to_owned();
        assert!(tikv_cfg.check_critical_cfg_with(&last_cfg).is_ok());
    }

    #[test]
    fn test_persist_cfg() {
        let dir = Builder::new().prefix("test_persist_cfg").tempdir().unwrap();
        let path_buf = dir.path().join(LAST_CONFIG_FILE);
        let file = path_buf.as_path().to_str().unwrap();
        let (s1, s2) = ("/xxx/wal_dir".to_owned(), "/yyy/wal_dir".to_owned());

        let mut tikv_cfg = TiKvConfig::default();

        tikv_cfg.rocksdb.wal_dir = s1.clone();
        tikv_cfg.raftdb.wal_dir = s2.clone();
        tikv_cfg.write_to_file(file).unwrap();
        let cfg_from_file = TiKvConfig::from_file(file);
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s1.clone());
        assert_eq!(cfg_from_file.raftdb.wal_dir, s2.clone());

        // write critical config when exist.
        tikv_cfg.rocksdb.wal_dir = s2.clone();
        tikv_cfg.raftdb.wal_dir = s1.clone();
        tikv_cfg.write_to_file(file).unwrap();
        let cfg_from_file = TiKvConfig::from_file(file);
        assert_eq!(cfg_from_file.rocksdb.wal_dir, s2.clone());
        assert_eq!(cfg_from_file.raftdb.wal_dir, s1.clone());
    }

    #[test]
    fn test_create_parent_dir_if_missing() {
        let root_path = Builder::new()
            .prefix("test_create_parent_dir_if_missing")
            .tempdir()
            .unwrap();
        let path = root_path.path().join("not_exist_dir");

        let mut tikv_cfg = TiKvConfig::default();
        tikv_cfg.storage.data_dir = path.as_path().to_str().unwrap().to_owned();
        assert!(persist_critical_config(&tikv_cfg).is_ok());
    }

    #[test]
    fn test_keepalive_check() {
        let mut tikv_cfg = TiKvConfig::default();
        tikv_cfg.pd.endpoints = vec!["".to_owned()];
        let dur = tikv_cfg.raft_store.raft_heartbeat_interval();
        tikv_cfg.server.grpc_keepalive_time = ReadableDuration(dur);
        assert!(tikv_cfg.validate().is_err());
        tikv_cfg.server.grpc_keepalive_time = ReadableDuration(dur * 2);
        tikv_cfg.validate().unwrap();
    }

    #[test]
    fn test_block_size() {
        let mut tikv_cfg = TiKvConfig::default();
        tikv_cfg.pd.endpoints = vec!["".to_owned()];
        tikv_cfg.rocksdb.defaultcf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.lockcf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.writecf.block_size = ReadableSize::gb(10);
        tikv_cfg.rocksdb.raftcf.block_size = ReadableSize::gb(10);
        tikv_cfg.raftdb.defaultcf.block_size = ReadableSize::gb(10);
        assert!(tikv_cfg.validate().is_err());
        tikv_cfg.rocksdb.defaultcf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.lockcf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.writecf.block_size = ReadableSize::kb(10);
        tikv_cfg.rocksdb.raftcf.block_size = ReadableSize::kb(10);
        tikv_cfg.raftdb.defaultcf.block_size = ReadableSize::kb(10);
        tikv_cfg.validate().unwrap();
    }

    #[test]
    fn test_parse_log_level() {
        #[derive(Serialize, Deserialize, Debug)]
        struct LevelHolder {
            #[serde(with = "log_level_serde")]
            v: Level,
        }

        let legal_cases = vec![
            ("critical", Level::Critical),
            ("error", Level::Error),
            ("warning", Level::Warning),
            ("debug", Level::Debug),
            ("trace", Level::Trace),
            ("info", Level::Info),
        ];
        for (serialized, deserialized) in legal_cases {
            let holder = LevelHolder { v: deserialized };
            let res_string = toml::to_string(&holder).unwrap();
            let exp_string = format!("v = \"{}\"\n", serialized);
            assert_eq!(res_string, exp_string);
            let res_value: LevelHolder = toml::from_str(&exp_string).unwrap();
            assert_eq!(res_value.v, deserialized);
        }

        let compatibility_cases = vec![("warn", Level::Warning)];
        for (serialized, deserialized) in compatibility_cases {
            let variant_string = format!("v = \"{}\"\n", serialized);
            let res_value: LevelHolder = toml::from_str(&variant_string).unwrap();
            assert_eq!(res_value.v, deserialized);
        }

        let illegal_cases = vec!["foobar", ""];
        for case in illegal_cases {
            let string = format!("v = \"{}\"\n", case);
            toml::from_str::<LevelHolder>(&string).unwrap_err();
        }
    }
}
