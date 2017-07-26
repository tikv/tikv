// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use rocksdb::{DBCompressionType, DBRecoveryMode};
use system_info;
use tikv::storage::{CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT};
use tikv::util::config::{ReadableDuration, ReadableSize, KB, MB, GB};

const LOCKCF_MIN_MEM: usize = 256 * MB as usize;
const LOCKCF_MAX_MEM: usize = GB as usize;
const RAFTCF_MIN_MEM: usize = 256 * MB as usize;
const RAFTCF_MAX_MEM: usize = 2 * GB as usize;

fn memory_for_cf(cf: &str) -> usize {
    let total_mem_in_kb = system_info::mem_info().unwrap().total * KB;
    let (radio, min, max) = match cf {
        CF_DEFAULT => (0.25, 0, usize::MAX),
        CF_LOCK => (0.02, LOCKCF_MIN_MEM, LOCKCF_MAX_MEM),
        CF_WRITE => (0.15, 0, usize::MAX),
        CF_RAFT => (0.02, RAFTCF_MIN_MEM, RAFTCF_MAX_MEM),
    };
    let mut size = (total_mem_in_kb as f64 * radio) as usize;
    if size < min {
        size = min;
    } else if size > max {
        size = max;
    }
    size / MB
}

pub struct DbConfig {
    wal_recovery_mode: DBRecoveryMode,
    wal_dir: String,
    wal_ttl_seconds: usize,
    wal_size_limit: ReadableSize,
    max_total_wal_size: ReadableSize,
    max_background_jobs: i32,
    max_manifest_file_size: ReadableSize,
    create_if_missing: bool,
    max_open_files: usize,
    enable_statistics: bool,
    stats_dump_period: ReadableDuration,
    compaction_readahead_size: ReadableSize,
    info_log_max_size: ReadableSize,
    info_log_roll_time: ReadableDuration,
    info_log_dir: String,
    rate_bytes_per_sec: i64,
    max_sub_compactions: u32,
    writable_file_max_buffer_size: ReadableSize,
    use_direct_io_for_flush_and_compaction: bool,
    enable_pipelined_write: bool,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize(0),
            max_total_wal_size: ReadableSize(4 * 1024 * 1024 * 1024),
            max_background_jobs: 6,
            max_manifest_file_size: ReadableSize(20 * 1024 * 1024),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration(Duration::new(600, 0)),
            compaction_readahead_size: ReadableSize(0),
            info_log_max_size: ReadableSize(0),
            info_log_roll_time: ReadableDuration(Duration::new(0, 0)),
            info_log_dir: "".to_owned(),
            rate_bytes_per_sec: 0,
            max_sub_compactions: 1,
            writable_file_max_buffer_size: ReadableSize(1024 * 1024),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
        }
    }
}

impl DbConfig {
    fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(true);
        opts.set_max_open_files(self.max_open_files);
        if self.enable_statistics {
            opts.enable_statistics();
            opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs());
        }
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        if !self.info_log_dir.is_empty() {
            opts.create_info_log(&self.info_log_dir).unwrap_or_else(|e| {
                panic!("create RocksDB info log {} error: {:?}", self.info_log_dir, e);
            })
        }
        if self.rate_bytes_per_sec > 0 {
            opts.set_ratelimiter(self.rate_bytes_per_sec);
        }
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size);
        opts.set_use_direct_io_for_flush_and_compaction(self.use_direct_io_for_flush_and_compaction);
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts
    }
}

struct CfConfig {
    cf: &'static str,
    block_size: ReadableSize,
    block_cache_size: ReadableSize,
    cache_index_and_filter_blocks: bool,
    use_bloom_filter: bool,
    whole_key_filtering: bool,
    bloom_bits_per_key: usize,
    block_based_filter: bool,
    compression_per_level: Vec<CompressionType>,
    write_buffer_size: ReadableSize,
    max_write_buffer_number: usize,
    min_write_buffer_number_to_merge: usize,
    max_bytes_for_level_base: ReadableSize,
    target_file_size_base: ReadableSize,
    level_zero_file_num_compaction_trigger: usize,
    level_zero_slowdown_writes_trigger: usize,
    level_zero_stop_writes_trigger: usize,
    max_compaction_bytes: ReadableSize,
    compaction_pri: CompactionPriority,
}

impl Default for CfConfig {
    fn default() -> CfConfig {
        CfConfig {
            cf: CF_DEFAULT,
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(256),
            cache_index_and_filter_blocks: true,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_bits_per_key: 10,
            block_based_filter: false,
            compression_per_level: vec![
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
            target_file_size_base: ReadableSize::mb(32),
            level_zero_file_num_compaction_trigger: 4,
            level_zero_slowdown_writes_trigger: 20,
            level_zero_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
        }
    }
}

impl CfConfig {
    fn default_config() -> CfConfig {
        let mut cfg = CfConfig::default();
        cfg.cf = CF_DEFAULT;
        cfg.block_cache_size = ReadableSize::mb(memory_for_cf(CF_DEFAULT) as u64);
        cfg.use_bloom_filter = true;
        cfg.whole_key_filtering = true;
        cfg
    }

    fn write_config() -> CfConfig {
        let mut cfg = CfConfig::default();
        cfg.cf = CF_WRITE;
        cfg.block_cache_size = ReadableSize::mb(memory_for_cf(CF_WRITE) as u64);
        cfg.use_bloom_filter = true;
        cfg.whole_key_filtering = false;
        cfg
    }

    fn raft_config() -> CfConfig {
        let mut cfg = CfConfig::default();
        cfg.cf = CF_RAFT;
        cfg.block_cache_size = ReadableSize::mb(memory_for_cf(CF_RAFT) as u64);
        cfg
    }

    fn lock_config() -> CfConfig {
        let mut cfg = CfConfig::default();
        cfg.cf = CF_LOCK;
        cfg.block_cache_size = ReadableSize::mb(memory_for_cf(CF_LOCK) as u64);
        cfg.block_size = ReadableSize::kb(16);
        cfg.use_bloom_filter = true;
        cfg.whole_key_filtering = true;
        cfg.compression_per_level = vec![CompressionType::No; 7];
        cfg.level_zero_file_num_compaction_trigger = 1;
        cfg.max_bytes_for_level_base = ReadableSize::mb(128);
        cfg
    }

    pub fn build_opt(&self) -> CfOption {
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size(self.block_size);
        block_base_opts.set_lru_cache(self.block_cache_size);
        block_base_opts.set_cache_index_and_filter_blocks(self.cache_index_and_filter_blocks);
        if self.use_bloom_filter {
            block_base_opts.set_bloom_filter(self.bloom_bits_per_key, self.block_based_filter);
            block_base_opts.set_whole_key_filtering(self.whole_key_filtering);
        }
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.compression_per_level(&self.compression_per_level);
        cf_opts.set_write_buffer_size(self.write_buffer_size);
        cf_opts.set_max_write_buffer_number(self.max_write_buffer_number);
        cf_opts.set_min_write_buffer_number_to_merge(self.min_write_buffer_number_to_merge);
        cf_opts.set_max_bytes_for_level_base(self.max_bytes_for_level_base);
        cf_opts.set_target_file_size_base(self.target_file_size_base);
        cf_opts.set_level_zero_file_num_compaction_trigger(self.level_zero_file_num_compaction_trigger);
        cf_opts.set_level_zero_slowdown_writes_trigger(self.level_zero_slowdown_writes_trigger);
        cf_opts.set_level_zero_stop_writes_trigger(self.level_zero_stop_writes_trigger);
        cf_opts.set_max_compaction_bytes(self.max_compaction_bytes);
        cf_opts.compaction_priority(self.compaction_pri);

        match self.cf {
            CF_DEFAULT => {},
            CF_WRITE => {
                cf_opts.set_prefix_extractor("FixedSuffixSliceTransform", Box::new(rocksdb_util::FixedSuffixSliceTransform::new(8))).unwrap();
                cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
                let f = Box::new(UserPropertiesCollectorFactory::default());
                cf_opts.add_table_properties_collector_factory("tikv.user-properties-collector", f);
            }
            CF_RAFT => {
                cf_opts.set_memtable_insert_hint_prefix_extractor("RaftPrefixSliceTransform",
            Box::new(rocksdb_util::FixedPrefixSliceTransform::new(region_raft_prefix_len())));
            }
            CF_LOCK => {
                cf_opts.set_prefix_extractor("NoopSliceTransform",
                              Box::new(rocksdb_util::NoopSliceTransform));
                cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
            }
        }

        cf_opts
    }
}
