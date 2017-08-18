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

use prometheus::{exponential_buckets, CounterVec, GaugeVec, HistogramVec};
use rocksdb::{DBStatisticsHistogramType as HistType, DBStatisticsTickerType as TickerType,
              HistogramData, DB};
use storage::CfName;
use util::rocksdb;

pub const ROCKSDB_TOTAL_SST_FILES_SIZE: &'static str = "rocksdb.total-sst-files-size";
pub const ROCKSDB_TABLE_READERS_MEM: &'static str = "rocksdb.estimate-table-readers-mem";
pub const ROCKSDB_CUR_SIZE_ALL_MEM_TABLES: &'static str = "rocksdb.cur-size-all-mem-tables";
pub const ROCKSDB_ESTIMATE_NUM_KEYS: &'static str = "rocksdb.estimate-num-keys";
pub const ROCKSDB_PENDING_COMPACTION_BYTES: &'static str = "rocksdb.\
                                                            estimate-pending-compaction-bytes";
pub const ENGINE_TICKER_TYPES: &'static [TickerType] = &[
    TickerType::BlockCacheMiss,
    TickerType::BlockCacheHit,
    TickerType::BlockCacheIndexMiss,
    TickerType::BlockCacheIndexHit,
    TickerType::BlockCacheFilterMiss,
    TickerType::BlockCacheFilterHit,
    TickerType::BlockCacheDataMiss,
    TickerType::BlockCacheDataHit,
    TickerType::BlockCacheByteRead,
    TickerType::BlockCacheByteWrite,
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
    TickerType::NoIterators,
    TickerType::BloomFilterPrefixChecked,
    TickerType::BloomFilterPrefixUseful,
    TickerType::WalFileSynced,
    TickerType::WalFileBytes,
    TickerType::CompactReadBytes,
    TickerType::CompactWriteBytes,
    TickerType::FlushWriteBytes,
    TickerType::ReadAmpEstimateUsefulBytes,
    TickerType::ReadAmpTotalReadBytes,
];
pub const ENGINE_HIST_TYPES: &'static [HistType] = &[
    HistType::GetMicros,
    HistType::WriteMicros,
    HistType::SeekMicros,
];

pub fn flush_engine_ticker_metrics(t: TickerType, value: u64, name: &str) {
    match t {
        TickerType::BlockCacheMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_miss"])
                .set(value as f64);
        }
        TickerType::BlockCacheHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_hit"])
                .set(value as f64);
        }
        TickerType::BlockCacheIndexMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_index_miss"])
                .set(value as f64);
        }
        TickerType::BlockCacheIndexHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_index_hit"])
                .set(value as f64);
        }
        TickerType::BlockCacheFilterMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_filter_miss"])
                .set(value as f64);
        }
        TickerType::BlockCacheFilterHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_filter_hit"])
                .set(value as f64);
        }
        TickerType::BlockCacheDataMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_data_miss"])
                .set(value as f64);
        }
        TickerType::BlockCacheDataHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_data_hit"])
                .set(value as f64);
        }
        TickerType::BlockCacheByteRead => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "block_cache_byte_read"])
                .set(value as f64);
        }
        TickerType::BlockCacheByteWrite => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "block_cache_byte_write"])
                .set(value as f64);
        }
        TickerType::BloomFilterUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                .with_label_values(&[name, "bloom_useful"])
                .set(value as f64);
        }
        TickerType::MemtableHit => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC
                .with_label_values(&[name, "memtable_hit"])
                .set(value as f64);
        }
        TickerType::MemtableMiss => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC
                .with_label_values(&[name, "memtable_miss"])
                .set(value as f64);
        }
        TickerType::GetHitL0 => {
            STORE_ENGINE_READ_SURVED_VEC
                .with_label_values(&[name, "get_hit_l0"])
                .set(value as f64);
        }
        TickerType::GetHitL1 => {
            STORE_ENGINE_READ_SURVED_VEC
                .with_label_values(&[name, "get_hit_l1"])
                .set(value as f64);
        }
        TickerType::GetHitL2AndUp => {
            STORE_ENGINE_READ_SURVED_VEC
                .with_label_values(&[name, "get_hit_l2_and_up"])
                .set(value as f64);
        }
        TickerType::CompactionKeyDropNewerEntry => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "compaction_key_drop_newer_entry"])
                .set(value as f64);
        }
        TickerType::CompactionKeyDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "compaction_key_drop_obsolete"])
                .set(value as f64);
        }
        TickerType::CompactionKeyDropRangeDel => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "compaction_key_drop_range_del"])
                .set(value as f64);
        }
        TickerType::CompactionRangeDelDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "range_del_drop_obsolete"])
                .set(value as f64);
        }
        TickerType::NumberKeysWritten => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "keys_written"])
                .set(value as f64);
        }
        TickerType::NumberKeysRead => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "keys_read"])
                .set(value as f64);
        }
        TickerType::BytesWritten => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "bytes_written"])
                .set(value as f64);
        }
        TickerType::BytesRead => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "bytes_read"])
                .set(value as f64);
        }
        TickerType::NumberDbSeek => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_seek"])
                .set(value as f64);
        }
        TickerType::NumberDbNext => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_next"])
                .set(value as f64);
        }
        TickerType::NumberDbPrev => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_prev"])
                .set(value as f64);
        }
        TickerType::NumberDbSeekFound => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_seek_found"])
                .set(value as f64);
        }
        TickerType::NumberDbNextFound => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_next_found"])
                .set(value as f64);
        }
        TickerType::NumberDbPrevFound => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_prev_found"])
                .set(value as f64);
        }
        TickerType::IterBytesRead => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "iter_bytes_read"])
                .set(value as f64);
        }
        TickerType::NoFileCloses => {
            STORE_ENGINE_FILE_STATUS_VEC
                .with_label_values(&[name, "no_file_closes"])
                .set(value as f64);
        }
        TickerType::NoFileOpens => {
            STORE_ENGINE_FILE_STATUS_VEC
                .with_label_values(&[name, "no_file_opens"])
                .set(value as f64);
        }
        TickerType::NoFileErrors => {
            STORE_ENGINE_FILE_STATUS_VEC
                .with_label_values(&[name, "no_file_errors"])
                .set(value as f64);
        }
        TickerType::StallMicros => {
            STORE_ENGINE_STALL_MICROS
                .with_label_values(&[name])
                .set(value as f64);
        }
        TickerType::NoIterators => {
            STORE_ENGINE_NO_ITERATORS
                .with_label_values(&[name])
                .set(value as f64);
        }
        TickerType::BloomFilterPrefixChecked => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                .with_label_values(&[name, "bloom_prefix_checked"])
                .set(value as f64);
        }
        TickerType::BloomFilterPrefixUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                .with_label_values(&[name, "bloom_prefix_useful"])
                .set(value as f64);
        }
        TickerType::WalFileSynced => {
            STORE_ENGINE_WAL_FILE_SYNCED
                .with_label_values(&[name])
                .set(value as f64);
        }
        TickerType::WalFileBytes => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "wal_file_bytes"])
                .set(value as f64);
        }
        TickerType::CompactReadBytes => {
            STORE_ENGINE_COMPACTION_FLOW_VEC
                .with_label_values(&[name, "bytes_read"])
                .set(value as f64);
        }
        TickerType::CompactWriteBytes => {
            STORE_ENGINE_COMPACTION_FLOW_VEC
                .with_label_values(&[name, "bytes_written"])
                .set(value as f64);
        }
        TickerType::FlushWriteBytes => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "flush_write_bytes"])
                .set(value as f64);
        }
        TickerType::ReadAmpEstimateUsefulBytes => {
            STORE_ENGINE_READ_AMP_FLOW_VEC
                .with_label_values(&[name, "read_amp_estimate_useful_bytes"])
                .set(value as f64);
        }
        TickerType::ReadAmpTotalReadBytes => {
            STORE_ENGINE_READ_AMP_FLOW_VEC
                .with_label_values(&[name, "read_amp_total_read_bytes"])
                .set(value as f64);
        }
    }
}

pub fn flush_engine_histogram_metrics(t: HistType, value: HistogramData, name: &str) {
    match t {
        HistType::GetMicros => {
            STORE_ENGINE_GET_MICROS_VEC
                .with_label_values(&[name, "get_median"])
                .set(value.median);
            STORE_ENGINE_GET_MICROS_VEC
                .with_label_values(&[name, "get_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_GET_MICROS_VEC
                .with_label_values(&[name, "get_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_GET_MICROS_VEC
                .with_label_values(&[name, "get_average"])
                .set(value.average);
            STORE_ENGINE_GET_MICROS_VEC
                .with_label_values(&[name, "get_standard_deviation"])
                .set(value.standard_deviation);
        }
        HistType::WriteMicros => {
            STORE_ENGINE_WRITE_MICROS_VEC
                .with_label_values(&[name, "write_median"])
                .set(value.median);
            STORE_ENGINE_WRITE_MICROS_VEC
                .with_label_values(&[name, "write_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_WRITE_MICROS_VEC
                .with_label_values(&[name, "write_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_WRITE_MICROS_VEC
                .with_label_values(&[name, "write_average"])
                .set(value.average);
            STORE_ENGINE_WRITE_MICROS_VEC
                .with_label_values(&[name, "write_standard_deviation"])
                .set(value.standard_deviation);
        }
        HistType::SeekMicros => {
            STORE_ENGINE_SEEK_MICROS_VEC
                .with_label_values(&[name, "seek_median"])
                .set(value.median);
            STORE_ENGINE_SEEK_MICROS_VEC
                .with_label_values(&[name, "seek_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_SEEK_MICROS_VEC
                .with_label_values(&[name, "seek_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_SEEK_MICROS_VEC
                .with_label_values(&[name, "seek_average"])
                .set(value.average);
            STORE_ENGINE_SEEK_MICROS_VEC
                .with_label_values(&[name, "seek_standard_deviation"])
                .set(value.standard_deviation);
        }
    }
}

pub fn flush_engine_properties(engine: &DB, cfs: &[CfName], name: &str) {
    for cf in cfs {
        let handle = rocksdb::get_cf_handle(engine, cf).unwrap();
        // It is important to monitor each cf's size, especially the "raft" and "lock" column
        // families.
        let cf_used_size = engine
            .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
            .expect("rocksdb is too old, missing total-sst-files-size property");
        STORE_ENGINE_SIZE_GAUGE_VEC
            .with_label_values(&[name, cf])
            .set(cf_used_size as f64);

        // For block cache usage
        let block_cache_usage = engine.get_block_cache_usage_cf(handle);
        STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC
            .with_label_values(&[name, cf])
            .set(block_cache_usage as f64);

        // TODO: find a better place to record these metrics.
        // Refer: https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
        // For index and filter blocks memory
        if let Some(readers_mem) = engine.get_property_int_cf(handle, ROCKSDB_TABLE_READERS_MEM) {
            STORE_ENGINE_MEMORY_GAUGE_VEC
                .with_label_values(&[name, cf, "readers-mem"])
                .set(readers_mem as f64);
        }

        // For memtable
        if let Some(mem_table) =
            engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES)
        {
            STORE_ENGINE_MEMORY_GAUGE_VEC
                .with_label_values(&[name, cf, "mem-tables"])
                .set(mem_table as f64);
        }

        // TODO: add cache usage and pinned usage.

        if let Some(num_keys) = engine.get_property_int_cf(handle, ROCKSDB_ESTIMATE_NUM_KEYS) {
            STORE_ENGINE_ESTIMATE_NUM_KEYS_VEC
                .with_label_values(&[name, cf])
                .set(num_keys as f64);
        }

        // Pending compaction bytes
        if let Some(pending_compaction_bytes) =
            engine.get_property_int_cf(handle, ROCKSDB_PENDING_COMPACTION_BYTES)
        {
            STORE_ENGINE_PENDING_COMACTION_BYTES_VEC
                .with_label_values(&[name, cf])
                .set(pending_compaction_bytes as f64);
        }
    }
}

lazy_static!{
    pub static ref STORE_ENGINE_SIZE_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_size_bytes",
            "Sizes of each column families.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_block_cache_size_bytes",
            "Usage of each column families' block cache.",
            &["db", "cf"]
        ).unwrap();

    pub static ref STORE_ENGINE_MEMORY_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_memory_bytes",
            "Sizes of each column families.",
            &["db", "cf", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_ESTIMATE_NUM_KEYS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_estimate_num_keys",
            "Estimate num keys of each column families.",
            &["db", "cf"]
        ).unwrap();

    pub static ref STORE_ENGINE_CACHE_EFFICIENCY_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_cache_efficiency",
            "Efficiency of rocksdb's block cache.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_memtable_efficiency",
            "Hit and miss of memtable.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_READ_SURVED_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_get_served",
            "Get queries served by.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_BLOOM_EFFICIENCY_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_bloom_efficiency",
            "Efficiency of rocksdb's bloom filter.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_FLOW_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_flow_bytes",
            "Bytes and keys of read/written.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_STALL_MICROS: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_stall_micro_seconds",
            "Stall micros.",
            &["db"]
        ).unwrap();

    pub static ref STORE_ENGINE_GET_MICROS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_get_micro_seconds",
            "Get micros histogram.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_WRITE_MICROS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_write_micro_seconds",
            "Write micros histogram.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_SEEK_MICROS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_seek_micro_seconds",
            "Seek micros histogram.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_PENDING_COMACTION_BYTES_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_pending_compaction_bytes",
            "Pending compaction bytes.",
            &["db", "cf"]
        ).unwrap();

    pub static ref STORE_ENGINE_COMPACTION_FLOW_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_compaction_flow_bytes",
            "Bytes of read/written during compaction.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_COMPACTION_DROP_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_compaction_key_drop",
            "Count the reasons for key drop during compaction",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_COMPACTION_DURATIONS_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_engine_compaction_duration_seconds",
            "Histogram of compaction duration seconds",
            &["db", "cf"],
            exponential_buckets(0.005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_ENGINE_COMPACTION_NUM_CORRUPT_KEYS_VEC: CounterVec =
        register_counter_vec!(
            "tikv_engine_compaction_num_corrupt_keys",
            "Number of corrupt keys during compaction",
            &["db", "cf"]
        ).unwrap();

    pub static ref STORE_ENGINE_LOCATE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_locate",
            "Number of calls to seek/next/prev",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_FILE_STATUS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_file_status",
            "Number of different status of files.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_READ_AMP_FLOW_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_read_amp_flow_bytes",
            "Bytes of read amplification.",
            &["db", "type"]
        ).unwrap();

    pub static ref STORE_ENGINE_NO_ITERATORS: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_no_iterator",
            "Number of iterators currently open.",
            &["db"]
        ).unwrap();

    pub static ref STORE_ENGINE_WAL_FILE_SYNCED: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_wal_file_synced",
            "Number of times WAL sync is done.",
            &["db"]
        ).unwrap();

    pub static ref STORE_ENGINE_EVENT_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_engine_event_total",
            "Number of engine events",
            &["db", "cf", "type"]
        ).unwrap();
}
