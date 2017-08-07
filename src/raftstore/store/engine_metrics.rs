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

use std::sync::Arc;
use prometheus::GaugeVec;
use rocksdb::{DB, DBStatisticsTickerType as TickerType, DBStatisticsHistogramType as HistType,
              HistogramData};
use storage::CfName;
use util::rocksdb;

pub const ROCKSDB_TOTAL_SST_FILES_SIZE: &'static str = "rocksdb.total-sst-files-size";
pub const ROCKSDB_TABLE_READERS_MEM: &'static str = "rocksdb.estimate-table-readers-mem";
pub const ROCKSDB_CUR_SIZE_ALL_MEM_TABLES: &'static str = "rocksdb.cur-size-all-mem-tables";
pub const ROCKSDB_ESTIMATE_NUM_KEYS: &'static str = "rocksdb.estimate-num-keys";
pub const ROCKSDB_ESTIMATE_PENDING_COMPACTION_BYTES: &'static str = "rocksdb.\
                                                         estimate-pending-compaction-bytes";
pub const ENGINE_TICKER_TYPES: &'static [TickerType] = &[TickerType::BlockCacheMiss,
                                                         TickerType::BlockCacheHit,
                                                         TickerType::MemtableHit,
                                                         TickerType::MemtableMiss,
                                                         TickerType::GetHitL0,
                                                         TickerType::GetHitL1,
                                                         TickerType::GetHitL2AndUp,
                                                         TickerType::BloomFilterUseful,
                                                         TickerType::BloomFilterPrefixChecked,
                                                         TickerType::BloomFilterPrefixUseful,
                                                         TickerType::NumberKeysWritten,
                                                         TickerType::NumberKeysRead,
                                                         TickerType::BytesWritten,
                                                         TickerType::BytesRead,
                                                         TickerType::IterBytesRead,
                                                         TickerType::StallMicros,
                                                         TickerType::CompactReadBytes,
                                                         TickerType::CompactWriteBytes];
pub const ENGINE_HIST_TYPES: &'static [HistType] =
    &[HistType::GetMicros, HistType::WriteMicros, HistType::SeekMicros];

pub fn flush_engine_ticker_metrics(t: TickerType, value: u64, name: &str) {
    match t {
        TickerType::BlockCacheMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC.with_label_values(&[name, "block_cache_miss"])
                .set(value as f64);
        }
        TickerType::BlockCacheHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC.with_label_values(&[name, "block_cache_hit"])
                .set(value as f64);
        }
        TickerType::MemtableHit => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC.with_label_values(&[name, "memtable_hit"])
                .set(value as f64);
        }
        TickerType::MemtableMiss => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC.with_label_values(&[name, "memtable_miss"])
                .set(value as f64);
        }
        TickerType::GetHitL0 => {
            STORE_ENGINE_READ_SURVED_VEC.with_label_values(&[name, "get_hit_l0"]).set(value as f64);
        }
        TickerType::GetHitL1 => {
            STORE_ENGINE_READ_SURVED_VEC.with_label_values(&[name, "get_hit_l1"]).set(value as f64);
        }
        TickerType::GetHitL2AndUp => {
            STORE_ENGINE_READ_SURVED_VEC.with_label_values(&[name, "get_hit_l2_and_up"])
                .set(value as f64);
        }
        TickerType::BloomFilterUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC.with_label_values(&[name, "bloom_useful"])
                .set(value as f64);
        }
        TickerType::BloomFilterPrefixChecked => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC.with_label_values(&[name, "bloom_prefix_checked"])
                .set(value as f64);
        }
        TickerType::BloomFilterPrefixUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC.with_label_values(&[name, "bloom_prefix_useful"])
                .set(value as f64);
        }
        TickerType::NumberKeysWritten => {
            STORE_ENGINE_FLOW_VEC.with_label_values(&[name, "keys_written"]).set(value as f64);
        }
        TickerType::NumberKeysRead => {
            STORE_ENGINE_FLOW_VEC.with_label_values(&[name, "keys_read"]).set(value as f64);
        }
        TickerType::BytesWritten => {
            STORE_ENGINE_FLOW_VEC.with_label_values(&[name, "bytes_written"]).set(value as f64);
        }
        TickerType::BytesRead => {
            STORE_ENGINE_FLOW_VEC.with_label_values(&[name, "bytes_read"]).set(value as f64);
        }
        TickerType::IterBytesRead => {
            STORE_ENGINE_FLOW_VEC.with_label_values(&[name, "iter_bytes_read"]).set(value as f64);
        }
        TickerType::StallMicros => {
            STORE_ENGINE_STALL_MICROS.with_label_values(&[name]).set(value as f64);
        }
        TickerType::CompactReadBytes => {
            STORE_ENGINE_COMPACTION_FLOW_VEC.with_label_values(&[name, "bytes_read"])
                .set(value as f64);
        }
        TickerType::CompactWriteBytes => {
            STORE_ENGINE_COMPACTION_FLOW_VEC.with_label_values(&[name, "bytes_written"])
                .set(value as f64);
        }
        _ => {}
    }
}

pub fn flush_engine_histogram_metrics(t: HistType, value: HistogramData, name: &str) {
    match t {
        HistType::GetMicros => {
            STORE_ENGINE_GET_MICROS_VEC.with_label_values(&[name, "get_median"]).set(value.median);
            STORE_ENGINE_GET_MICROS_VEC.with_label_values(&[name, "get_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_GET_MICROS_VEC.with_label_values(&[name, "get_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_GET_MICROS_VEC.with_label_values(&[name, "get_average"])
                .set(value.average);
            STORE_ENGINE_GET_MICROS_VEC.with_label_values(&[name, "get_standard_deviation"])
                .set(value.standard_deviation);
        }
        HistType::WriteMicros => {
            STORE_ENGINE_WRITE_MICROS_VEC.with_label_values(&[name, "write_median"])
                .set(value.median);
            STORE_ENGINE_WRITE_MICROS_VEC.with_label_values(&[name, "write_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_WRITE_MICROS_VEC.with_label_values(&[name, "write_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_WRITE_MICROS_VEC.with_label_values(&[name, "write_average"])
                .set(value.average);
            STORE_ENGINE_WRITE_MICROS_VEC.with_label_values(&[name, "write_standard_deviation"])
                .set(value.standard_deviation);
        }
        HistType::SeekMicros => {
            STORE_ENGINE_SEEK_MICROS_VEC.with_label_values(&[name, "seek_median"])
                .set(value.median);
            STORE_ENGINE_SEEK_MICROS_VEC.with_label_values(&[name, "seek_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_SEEK_MICROS_VEC.with_label_values(&[name, "seek_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_SEEK_MICROS_VEC.with_label_values(&[name, "seek_average"])
                .set(value.average);
            STORE_ENGINE_SEEK_MICROS_VEC.with_label_values(&[name, "seek_standard_deviation"])
                .set(value.standard_deviation);
        }
    }
}

fn get_cf_size(engine: &Arc<DB>, name: &str, cf: &str, used_size: &mut u64) {
    let handle = rocksdb::get_cf_handle(engine, cf).unwrap();
    let cf_used_size = engine.get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
        .expect("rocksdb is too old, missing total-sst-files-size property");

    // For block cache usage
    let block_cache_usage = engine.get_block_cache_usage_cf(handle);
    STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC.with_label_values(&[name, cf])
        .set(block_cache_usage as f64);

    // It is important to monitor each cf's size, especially the "raft" and "lock" column
    // families.
    STORE_ENGINE_SIZE_GAUGE_VEC.with_label_values(&[name, cf]).set(cf_used_size as f64);

    *used_size += cf_used_size;

    // TODO: find a better place to record these metrics.
    // Refer: https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
    // For index and filter blocks memory
    if let Some(readers_mem) = engine.get_property_int_cf(handle, ROCKSDB_TABLE_READERS_MEM) {
        STORE_ENGINE_MEMORY_GAUGE_VEC.with_label_values(&[name, cf, "readers-mem"])
            .set(readers_mem as f64);
    }

    // For memtable
    if let Some(mem_table) = engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES) {
        STORE_ENGINE_MEMORY_GAUGE_VEC.with_label_values(&[name, cf, "mem-tables"])
            .set(mem_table as f64);
        *used_size += mem_table;
    }

    // TODO: add cache usage and pinned usage.

    if let Some(num_keys) = engine.get_property_int_cf(handle, ROCKSDB_ESTIMATE_NUM_KEYS) {
        STORE_ENGINE_ESTIMATE_NUM_KEYS_VEC.with_label_values(&[name, cf])
            .set(num_keys as f64);
    }

    // Pending compaction bytes
    if let Some(pending_compaction_bytes) =
           engine.get_property_int_cf(handle, ROCKSDB_ESTIMATE_PENDING_COMPACTION_BYTES) {
        STORE_ENGINE_PENDING_COMACTION_BYTES_VEC.with_label_values(&[name, cf])
            .set(pending_compaction_bytes as f64);
    }
}

pub fn flush_engine_properties_and_get_used_size(engine: Arc<DB>,
                                                 name: &str,
                                                 cfs: &[CfName])
                                                 -> u64 {
    let mut used_size: u64 = 0;
    for cf in cfs {
        get_cf_size(&engine, name, cf, &mut used_size);
    }
    used_size
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
}
