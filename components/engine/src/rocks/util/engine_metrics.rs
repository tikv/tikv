// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: Need delete this defs after move the event_listener into engine_rocks.
use prometheus::{exponential_buckets, GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec};
use std::i64;

use crate::rocks;
use engine_traits::CF_DEFAULT;
use rocksdb::{DBStatisticsHistogramType as HistType, DBStatisticsTickerType as TickerType, DB};

pub const ROCKSDB_TOTAL_SST_FILES_SIZE: &str = "rocksdb.total-sst-files-size";
pub const ROCKSDB_TABLE_READERS_MEM: &str = "rocksdb.estimate-table-readers-mem";
pub const ROCKSDB_CUR_SIZE_ALL_MEM_TABLES: &str = "rocksdb.cur-size-all-mem-tables";
pub const ROCKSDB_ESTIMATE_NUM_KEYS: &str = "rocksdb.estimate-num-keys";
pub const ROCKSDB_PENDING_COMPACTION_BYTES: &str = "rocksdb.\
                                                    estimate-pending-compaction-bytes";
pub const ROCKSDB_COMPRESSION_RATIO_AT_LEVEL: &str = "rocksdb.compression-ratio-at-level";
pub const ROCKSDB_NUM_SNAPSHOTS: &str = "rocksdb.num-snapshots";
pub const ROCKSDB_OLDEST_SNAPSHOT_TIME: &str = "rocksdb.oldest-snapshot-time";
pub const ROCKSDB_NUM_FILES_AT_LEVEL: &str = "rocksdb.num-files-at-level";
pub const ROCKSDB_NUM_IMMUTABLE_MEM_TABLE: &str = "rocksdb.num-immutable-mem-table";

pub const ROCKSDB_TITANDB_LIVE_BLOB_SIZE: &str = "rocksdb.titandb.live-blob-size";
pub const ROCKSDB_TITANDB_NUM_LIVE_BLOB_FILE: &str = "rocksdb.titandb.num-live-blob-file";
pub const ROCKSDB_TITANDB_NUM_OBSOLETE_BLOB_FILE: &str = "rocksdb.titandb.\
                                                          num-obsolete-blob-file";
pub const ROCKSDB_TITANDB_LIVE_BLOB_FILE_SIZE: &str = "rocksdb.titandb.\
                                                       live-blob-file-size";
pub const ROCKSDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE: &str = "rocksdb.titandb.\
                                                           obsolete-blob-file-size";
pub const ROCKSDB_CFSTATS: &str = "rocksdb.cfstats";
pub const ROCKSDB_IOSTALL_KEY: &[&str] = &[
    "io_stalls.level0_slowdown",
    "io_stalls.level0_numfiles",
    "io_stalls.slowdown_for_pending_compaction_bytes",
    "io_stalls.stop_for_pending_compaction_bytes",
    "io_stalls.memtable_slowdown",
    "io_stalls.memtable_compaction",
];

pub const ROCKSDB_IOSTALL_TYPE: &[&str] = &[
    "level0_file_limit_slowdown",
    "level0_file_limit_stop",
    "pending_compaction_bytes_slowdown",
    "pending_compaction_bytes_stop",
    "memtable_count_limit_slowdown",
    "memtable_count_limit_stop",
];

pub const ENGINE_TICKER_TYPES: &[TickerType] = &[
    TickerType::BlockCacheMiss,
    TickerType::BlockCacheHit,
    TickerType::BlockCacheAdd,
    TickerType::BlockCacheAddFailures,
    TickerType::BlockCacheIndexMiss,
    TickerType::BlockCacheIndexHit,
    TickerType::BlockCacheIndexAdd,
    TickerType::BlockCacheIndexBytesInsert,
    TickerType::BlockCacheIndexBytesEvict,
    TickerType::BlockCacheFilterMiss,
    TickerType::BlockCacheFilterHit,
    TickerType::BlockCacheFilterAdd,
    TickerType::BlockCacheFilterBytesInsert,
    TickerType::BlockCacheFilterBytesEvict,
    TickerType::BlockCacheDataMiss,
    TickerType::BlockCacheDataHit,
    TickerType::BlockCacheDataAdd,
    TickerType::BlockCacheDataBytesInsert,
    TickerType::BlockCacheBytesRead,
    TickerType::BlockCacheBytesWrite,
    TickerType::BloomFilterUseful,
    TickerType::MemtableHit,
    TickerType::MemtableMiss,
    TickerType::GetHitL0,
    TickerType::GetHitL1,
    TickerType::GetHitL2AndUp,
    TickerType::CompactionKeyDropNewerEntry,
    TickerType::CompactionKeyDropObsolete,
    TickerType::CompactionKeyDropRangeDel,
    TickerType::CompactionRangeDelDropObsolete,
    TickerType::NumberKeysWritten,
    TickerType::NumberKeysRead,
    TickerType::BytesWritten,
    TickerType::BytesRead,
    TickerType::NumberDbSeek,
    TickerType::NumberDbNext,
    TickerType::NumberDbPrev,
    TickerType::NumberDbSeekFound,
    TickerType::NumberDbNextFound,
    TickerType::NumberDbPrevFound,
    TickerType::IterBytesRead,
    TickerType::NoFileCloses,
    TickerType::NoFileOpens,
    TickerType::NoFileErrors,
    TickerType::StallMicros,
    TickerType::BloomFilterPrefixChecked,
    TickerType::BloomFilterPrefixUseful,
    TickerType::WalFileSynced,
    TickerType::WalFileBytes,
    TickerType::WriteDoneBySelf,
    TickerType::WriteDoneByOther,
    TickerType::WriteTimedout,
    TickerType::WriteWithWal,
    TickerType::CompactReadBytes,
    TickerType::CompactWriteBytes,
    TickerType::FlushWriteBytes,
    TickerType::ReadAmpEstimateUsefulBytes,
    TickerType::ReadAmpTotalReadBytes,
    TickerType::BlobDbNumSeek,
    TickerType::BlobDbNumNext,
    TickerType::BlobDbNumPrev,
    TickerType::BlobDbNumKeysWritten,
    TickerType::BlobDbNumKeysRead,
    TickerType::BlobDbBytesWritten,
    TickerType::BlobDbBytesRead,
    TickerType::BlobDbBlobFileBytesWritten,
    TickerType::BlobDbBlobFileBytesRead,
    TickerType::BlobDbBlobFileSynced,
    TickerType::BlobDbGcNumFiles,
    TickerType::BlobDbGcNumNewFiles,
    TickerType::BlobDbGcNumKeysOverwritten,
    TickerType::BlobDbGcNumKeysRelocated,
    TickerType::BlobDbGcBytesOverwritten,
    TickerType::BlobDbGcBytesRelocated,
];

pub const ENGINE_HIST_TYPES: &[HistType] = &[
    HistType::DbGet,
    HistType::DbWrite,
    HistType::CompactionTime,
    HistType::TableSyncMicros,
    HistType::CompactionOutfileSyncMicros,
    HistType::WalFileSyncMicros,
    HistType::ManifestFileSyncMicros,
    HistType::StallL0SlowdownCount,
    HistType::StallMemtableCompactionCount,
    HistType::StallL0NumFilesCount,
    HistType::HardRateLimitDelayCount,
    HistType::SoftRateLimitDelayCount,
    HistType::NumFilesInSingleCompaction,
    HistType::DbSeek,
    HistType::WriteStall,
    HistType::SstReadMicros,
    HistType::NumSubcompactionsScheduled,
    HistType::BytesPerRead,
    HistType::BytesPerWrite,
    HistType::BytesCompressed,
    HistType::BytesDecompressed,
    HistType::CompressionTimesNanos,
    HistType::DecompressionTimesNanos,
    HistType::BlobDbKeySize,
    HistType::BlobDbValueSize,
    HistType::BlobDbSeekMicros,
    HistType::BlobDbNextMicros,
    HistType::BlobDbPrevMicros,
    HistType::BlobDbBlobFileWriteMicros,
    HistType::BlobDbBlobFileReadMicros,
    HistType::BlobDbBlobFileSyncMicros,
    HistType::BlobDbGcMicros,
    HistType::DbWriteWalTime,
];

pub fn flush_engine_iostall_properties(engine: &DB, name: &str) {
    let stall_num = ROCKSDB_IOSTALL_KEY.len();
    let mut counter = vec![0; stall_num];
    for cf in engine.cf_names() {
        let handle = rocks::util::get_cf_handle(engine, cf).unwrap();
        if let Some(info) = engine.get_map_property_cf(handle, ROCKSDB_CFSTATS) {
            for i in 0..stall_num {
                let value = info.get_property_int_value(ROCKSDB_IOSTALL_KEY[i]);
                counter[i] += value as i64;
            }
        } else {
            return;
        }
    }
    for i in 0..stall_num {
        STORE_ENGINE_WRITE_STALL_REASON_GAUSE_VEC
            .with_label_values(&[name, ROCKSDB_IOSTALL_TYPE[i]])
            .set(counter[i]);
    }
}

pub fn flush_engine_properties(engine: &DB, name: &str, shared_block_cache: bool) {
    for cf in engine.cf_names() {
        let handle = rocks::util::get_cf_handle(engine, cf).unwrap();
        // It is important to monitor each cf's size, especially the "raft" and "lock" column
        // families.
        let cf_used_size = rocks::util::get_engine_cf_used_size(engine, handle);
        STORE_ENGINE_SIZE_GAUGE_VEC
            .with_label_values(&[name, cf])
            .set(cf_used_size as i64);

        if !shared_block_cache {
            let block_cache_usage = engine.get_block_cache_usage_cf(handle);
            STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC
                .with_label_values(&[name, cf])
                .set(block_cache_usage as i64);
        }

        // TODO: find a better place to record these metrics.
        // Refer: https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
        // For index and filter blocks memory
        if let Some(readers_mem) = engine.get_property_int_cf(handle, ROCKSDB_TABLE_READERS_MEM) {
            STORE_ENGINE_MEMORY_GAUGE_VEC
                .with_label_values(&[name, cf, "readers-mem"])
                .set(readers_mem as i64);
        }

        // For memtable
        if let Some(mem_table) = engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES)
        {
            STORE_ENGINE_MEMORY_GAUGE_VEC
                .with_label_values(&[name, cf, "mem-tables"])
                .set(mem_table as i64);
        }

        // TODO: add cache usage and pinned usage.

        if let Some(num_keys) = engine.get_property_int_cf(handle, ROCKSDB_ESTIMATE_NUM_KEYS) {
            STORE_ENGINE_ESTIMATE_NUM_KEYS_VEC
                .with_label_values(&[name, cf])
                .set(num_keys as i64);
        }

        // Pending compaction bytes
        if let Some(pending_compaction_bytes) =
            engine.get_property_int_cf(handle, ROCKSDB_PENDING_COMPACTION_BYTES)
        {
            STORE_ENGINE_PENDING_COMPACTION_BYTES_VEC
                .with_label_values(&[name, cf])
                .set(pending_compaction_bytes as i64);
        }

        let opts = engine.get_options_cf(handle);
        for level in 0..opts.get_num_levels() {
            // Compression ratio at levels
            if let Some(v) =
                rocks::util::get_engine_compression_ratio_at_level(engine, handle, level)
            {
                STORE_ENGINE_COMPRESSION_RATIO_VEC
                    .with_label_values(&[name, cf, &level.to_string()])
                    .set(v);
            }

            // Num files at levels
            if let Some(v) = rocks::util::get_cf_num_files_at_level(engine, handle, level) {
                STORE_ENGINE_NUM_FILES_AT_LEVEL_VEC
                    .with_label_values(&[name, cf, &level.to_string()])
                    .set(v as i64);
            }
        }

        // Num immutable mem-table
        if let Some(v) = rocks::util::get_num_immutable_mem_table(engine, handle) {
            STORE_ENGINE_NUM_IMMUTABLE_MEM_TABLE_VEC
                .with_label_values(&[name, cf])
                .set(v as i64);
        }

        // Titan live blob size
        if let Some(v) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_LIVE_BLOB_SIZE) {
            STORE_ENGINE_TITANDB_LIVE_BLOB_SIZE_VEC
                .with_label_values(&[name, cf])
                .set(v as i64);
        }

        // Titan num live blob file
        if let Some(v) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_NUM_LIVE_BLOB_FILE) {
            STORE_ENGINE_TITANDB_NUM_LIVE_BLOB_FILE_VEC
                .with_label_values(&[name, cf])
                .set(v as i64);
        }

        // Titan num obsolete blob file
        if let Some(v) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_NUM_OBSOLETE_BLOB_FILE)
        {
            STORE_ENGINE_TITANDB_NUM_OBSOLETE_BLOB_FILE_VEC
                .with_label_values(&[name, cf])
                .set(v as i64);
        }

        // Titan live blob file size
        if let Some(v) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_LIVE_BLOB_FILE_SIZE) {
            STORE_ENGINE_TITANDB_LIVE_BLOB_FILE_SIZE_VEC
                .with_label_values(&[name, cf])
                .set(v as i64);
        }

        // Titan obsolete blob file size
        if let Some(v) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
        {
            STORE_ENGINE_TITANDB_OBSOLETE_BLOB_FILE_SIZE_VEC
                .with_label_values(&[name, cf])
                .set(v as i64);
        }
    }

    // For snapshot
    if let Some(n) = engine.get_property_int(ROCKSDB_NUM_SNAPSHOTS) {
        STORE_ENGINE_NUM_SNAPSHOTS_GAUGE_VEC
            .with_label_values(&[name])
            .set(n as i64);
    }
    if let Some(t) = engine.get_property_int(ROCKSDB_OLDEST_SNAPSHOT_TIME) {
        // RocksDB returns 0 if no snapshots.
        let now = time::get_time().sec as u64;
        let d = if t > 0 && now > t { now - t } else { 0 };
        STORE_ENGINE_OLDEST_SNAPSHOT_DURATION_GAUGE_VEC
            .with_label_values(&[name])
            .set(d as i64);
    }

    if shared_block_cache {
        // Since block cache is shared, getting cache size from any CF is fine. Here we get from
        // default CF.
        let handle = rocks::util::get_cf_handle(engine, CF_DEFAULT).unwrap();
        let block_cache_usage = engine.get_block_cache_usage_cf(handle);
        STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC
            .with_label_values(&[name, "all"])
            .set(block_cache_usage as i64);
    }
}

// For property metrics
#[rustfmt::skip]
lazy_static! {
    pub static ref STORE_ENGINE_SIZE_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_size_bytes",
        "Sizes of each column families",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_block_cache_size_bytes",
        "Usage of each column families' block cache",
        &["db", "cf"]
    ).unwrap();
    pub static ref STORE_ENGINE_MEMORY_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_memory_bytes",
        "Sizes of each column families",
        &["db", "cf", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_ESTIMATE_NUM_KEYS_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_estimate_num_keys",
        "Estimate num keys of each column families",
        &["db", "cf"]
    ).unwrap();
    pub static ref STORE_ENGINE_PENDING_COMPACTION_BYTES_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_pending_compaction_bytes",
        "Pending compaction bytes",
        &["db", "cf"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPRESSION_RATIO_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_compression_ratio",
        "Compression ratio at different levels",
        &["db", "cf", "level"]
    ).unwrap();
    pub static ref STORE_ENGINE_NUM_FILES_AT_LEVEL_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_num_files_at_level",
        "Number of files at each level",
        &["db", "cf", "level"]
    ).unwrap();
    pub static ref STORE_ENGINE_NUM_SNAPSHOTS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_num_snapshots",
        "Number of unreleased snapshots",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_OLDEST_SNAPSHOT_DURATION_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_oldest_snapshot_duration",
        "Oldest unreleased snapshot duration in seconds",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_WRITE_STALL_REASON_GAUSE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_write_stall_reason",
        "QPS of each reason which cause tikv write stall",
        &["db", "type"]
    ).unwrap();
}

// For ticker type
#[rustfmt::skip]
lazy_static! {
    pub static ref STORE_ENGINE_CACHE_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_cache_efficiency",
        "Efficiency of rocksdb's block cache",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_memtable_efficiency",
        "Hit and miss of memtable",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_GET_SERVED_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_get_served",
        "Get queries served by engine",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_WRITE_SERVED_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_write_served",
        "Write queries served by engine",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOOM_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_bloom_efficiency",
        "Efficiency of rocksdb's bloom filter",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_flow_bytes",
        "Bytes and keys of read/written",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_STALL_MICROS: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_stall_micro_seconds",
        "Stall micros",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_compaction_flow_bytes",
        "Bytes of read/written during compaction",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_DROP_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_compaction_key_drop",
        "Count the reasons for key drop during compaction",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_DURATIONS_VEC: HistogramVec = register_histogram_vec!(
        "tikv_engine_compaction_duration_seconds",
        "Histogram of compaction duration seconds",
        &["db", "cf"],
        exponential_buckets(0.005, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_NUM_CORRUPT_KEYS_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_compaction_num_corrupt_keys",
        "Number of corrupt keys during compaction",
        &["db", "cf"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_REASON_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_compaction_reason",
        "Number of compaction reason",
        &["db", "cf", "reason"]
    ).unwrap();
    pub static ref STORE_ENGINE_LOCATE_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_locate",
        "Number of calls to seek/next/prev",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_FILE_STATUS_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_file_status",
        "Number of different status of files",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_READ_AMP_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_read_amp_flow_bytes",
        "Bytes of read amplification",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_NO_ITERATORS: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_no_iterator",
        "Number of iterators currently open",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_WAL_FILE_SYNCED: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_wal_file_synced",
        "Number of times WAL sync is done",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_EVENT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_event_total",
        "Number of engine events",
        &["db", "cf", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_NUM_IMMUTABLE_MEM_TABLE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_num_immutable_mem_table",
        "Number of immutable mem-table",
        &["db", "cf"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_LOCATE_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_locate",
        "Number of calls to blob seek/next/prev",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_flow_bytes",
        "Bytes and keys of blob read/written",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_GC_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_gc_flow_bytes",
        "Bytes and keys of blob gc read/written",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_GC_FILE_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_gc_file_count",
        "Number of blob file involved in gc",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FILE_SYNCED: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_file_synced",
        "Number of times blob file sync is done",
        &["db"]
    ).unwrap();
}

// For histogram type
#[rustfmt::skip]
lazy_static! {
    pub static ref STORE_ENGINE_GET_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_get_micro_seconds",
        "Histogram of get micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_WRITE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_write_micro_seconds",
        "Histogram of write micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_TIME_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_compaction_time",
        "Histogram of compaction time",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_TABLE_SYNC_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_table_sync_micro_seconds",
        "Histogram of table sync micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_OUTFILE_SYNC_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_compaction_outfile_sync_micro_seconds",
        "Histogram of compaction outfile sync micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_MANIFEST_FILE_SYNC_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_manifest_file_sync_micro_seconds",
        "Histogram of manifest file sync micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_wal_file_sync_micro_seconds",
        "Histogram of WAL file sync micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_stall_l0_slowdown_count",
        "Histogram of stall l0 slowdown count",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_stall_memtable_compaction_count",
        "Histogram of stall memtable compaction count",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_STALL_L0_NUM_FILES_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_stall_l0_num_files_count",
        "Histogram of stall l0 num files count",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_HARD_RATE_LIMIT_DELAY_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_hard_rate_limit_delay_count",
        "Histogram of hard rate limit delay count",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_soft_rate_limit_delay_count",
        "Histogram of soft rate limit delay count",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_num_files_in_single_compaction",
        "Histogram of number of files in single compaction",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_SEEK_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_seek_micro_seconds",
        "Histogram of seek micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_WRITE_STALL_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_write_stall",
        "Histogram of write stall",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_SST_READ_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_sst_read_micros",
        "Histogram of SST read micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_num_subcompaction_scheduled",
        "Histogram of number of subcompaction scheduled",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BYTES_PER_READ_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_bytes_per_read",
        "Histogram of bytes per read",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BYTES_PER_WRITE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_bytes_per_write",
        "Histogram of bytes per write",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BYTES_COMPRESSED_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_bytes_compressed",
        "Histogram of bytes compressed",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BYTES_DECOMPRESSED_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_bytes_decompressed",
        "Histogram of bytes decompressed",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_compression_time_nanos",
        "Histogram of compression time nanos",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_decompression_time_nanos",
        "Histogram of decompression time nanos",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_KEY_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_key_size",
        "Histogram of blob key size",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_VALUE_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_value_size",
        "Histogram of blob value size",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_SEEK_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_seek_micros_seconds",
        "Histogram of blob read micros for calling seek",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_NEXT_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_next_micros_seconds",
        "Histogram of blob read micros for calling next",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_PREV_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_prev_micros_seconds",
        "Histogram of blob read micros for calling prev",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FILE_WRITE_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_file_write_micros_seconds",
        "Histogram of blob file write micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FILE_READ_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_file_read_micros_seconds",
        "Histogram of blob file read micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FILE_SYNC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_file_sync_micros_seconds",
        "Histogram of blob file sync micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_GC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_gc_micros_seconds",
        "Histogram of blob gc micros",
        &["db", "type"]
    ).unwrap();

    pub static ref STORE_ENGINE_TITANDB_LIVE_BLOB_SIZE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_titandb_live_blob_size",
        "Total blob value size referenced by LSM tree",
        &["db", "cf"]
    ).unwrap();

    pub static ref STORE_ENGINE_TITANDB_NUM_LIVE_BLOB_FILE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_titandb_num_live_blob_file",
        "Number of live blob file",
        &["db", "cf"]
    ).unwrap();

    pub static ref STORE_ENGINE_TITANDB_NUM_OBSOLETE_BLOB_FILE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_titandb_num_obsolete_blob_file",
        "Number of obsolete blob file",
        &["db", "cf"]
    ).unwrap();

    pub static ref STORE_ENGINE_TITANDB_LIVE_BLOB_FILE_SIZE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_titandb_live_blob_file_size",
        "Size of live blob file",
        &["db", "cf"]
    ).unwrap();

    pub static ref STORE_ENGINE_TITANDB_OBSOLETE_BLOB_FILE_SIZE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_titandb_obsolete_blob_file_size",
        "Size of obsolete blob file",
        &["db", "cf"]
    ).unwrap();
    pub static ref STORE_ENGINE_WRITE_WAL_TIME_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_write_wal_time_micro_seconds",
        "Histogram of write wal micros",
        &["db", "type"]
    ).unwrap();
}
