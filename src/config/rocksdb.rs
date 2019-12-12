// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::error::Error;
use std::usize;

use sys_info;

use crate::raftstore::coprocessor::properties::{
    MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory,
};
use crate::raftstore::coprocessor::properties::{
    DEFAULT_PROP_KEYS_INDEX_DISTANCE, DEFAULT_PROP_SIZE_INDEX_DISTANCE,
};
use crate::server::CONFIG_ROCKSDB_GAUGE;
use engine::rocks::util::config::{self as rocks_config, BlobRunMode, CompressionType};
use engine::rocks::util::{
    CFOptions, EventListener, FixedPrefixSliceTransform, FixedSuffixSliceTransform,
    NoopSliceTransform,
};
use engine::rocks::{
    BlockBasedOptions, Cache, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle,
    DBCompressionType, DBOptions, DBRateLimiterMode, DBRecoveryMode, LRUCacheOptions,
    TitanDBOptions,
};
use engine::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use keys::region_raft_prefix_len;
use tikv_util::config::{ReadableDuration, ReadableSize, GB, KB, MB};

const LOCKCF_MIN_MEM: usize = 256 * MB as usize;
const LOCKCF_MAX_MEM: usize = GB as usize;
const RAFT_MIN_MEM: usize = 256 * MB as usize;
const RAFT_MAX_MEM: usize = 2 * GB as usize;
const MAX_BLOCK_SIZE: usize = 32 * MB as usize;
pub const LAST_CONFIG_FILE: &str = "last_tikv.toml";

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

fn get_background_job_limit(
    default_background_jobs: i32,
    default_sub_compactions: u32,
) -> (i32, u32) {
    let cpu_num = sys_info::cpu_num().unwrap();
    // At the minimum, we should have two background jobs: one for flush and one for compaction.
    // Otherwise, the number of background jobs should not exceed cpu_num - 1.
    // By default, rocksdb assign (max_background_jobs / 4) threads dedicated for flush, and
    // the rest shared by flush and compaction.
    let max_background_jobs: i32 =
        cmp::max(2, cmp::min(default_background_jobs, (cpu_num - 1) as i32));
    // Cap max_sub_compactions to allow at least two compactions.
    let max_compactions = max_background_jobs - max_background_jobs / 4;
    let max_sub_compactions: u32 = cmp::max(
        1,
        cmp::min(default_sub_compactions, (max_compactions - 1) as u32),
    );
    (max_background_jobs, max_sub_compactions)
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
            force_consistency_checks: false,
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
            force_consistency_checks: false,
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
            force_consistency_checks: false,
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
            force_consistency_checks: false,
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
    // The value of this field will be truncated to seconds.
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
    pub enable_unordered_write: bool,
    pub defaultcf: DefaultCfConfig,
    pub writecf: WriteCfConfig,
    pub lockcf: LockCfConfig,
    pub raftcf: RaftCfConfig,
    pub titan: TitanDBConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        let (max_background_jobs, max_sub_compactions) = get_background_job_limit(8, 3);
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs,
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
            max_sub_compactions,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            enable_unordered_write: false,
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
        opts.enable_unordered_write(self.enable_unordered_write);
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
            // TODO: remove CF_RAFT.
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

    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcf.validate()?;
        self.lockcf.validate()?;
        self.writecf.validate()?;
        self.raftcf.validate()?;
        self.titan.validate()?;
        if self.enable_unordered_write {
            if self.titan.enabled {
                return Err("RocksDB.unordered_write does not support Titan".into());
            }
            if self.enable_pipelined_write {
                return Err("pipelined_write is not compatible with unordered_write".into());
            }
        }
        Ok(())
    }

    pub fn write_into_metrics(&self) {
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
            force_consistency_checks: false,
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
        cf_opts.set_titandb_options(&self.titan.build_opts());
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
    pub enable_unordered_write: bool,
    pub allow_concurrent_memtable_write: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    pub defaultcf: RaftDefaultCfConfig,
    pub titan: TitanDBConfig,
}

impl Default for RaftDbConfig {
    fn default() -> RaftDbConfig {
        let (max_background_jobs, max_sub_compactions) = get_background_job_limit(4, 2);
        RaftDbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs,
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
            max_sub_compactions,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            enable_unordered_write: false,
            allow_concurrent_memtable_write: false,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            defaultcf: RaftDefaultCfConfig::default(),
            titan: TitanDBConfig::default(),
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
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write);
        opts.add_event_listener(EventListener::new("raft"));
        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        // TODO maybe create a new env for raft engine
        if self.titan.enabled {
            opts.set_titandb_options(&self.titan.build_opts());
        }

        opts
    }

    pub fn build_cf_opts(&self, cache: &Option<Cache>) -> Vec<CFOptions<'_>> {
        vec![CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt(cache))]
    }

    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcf.validate()?;
        if self.enable_unordered_write {
            if self.titan.enabled {
                return Err("raftdb: unordered_write is not compatible with Titan".into());
            }
            if self.enable_pipelined_write {
                return Err(
                    "raftdb: pipelined_write is not compatible with unordered_write".into(),
                );
            }
        }
        Ok(())
    }
}
