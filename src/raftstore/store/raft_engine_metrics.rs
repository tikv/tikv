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
use prometheus::{Gauge, GaugeVec};
use rocksdb::{DB, DBStatisticsTickerType as TickerType, DBStatisticsHistogramType as HistType,
              HistogramData};
use storage::{CF_DEFAULT, CF_RAFT};
use util::rocksdb;
use super::engine_metrics::*;

pub fn flush_raft_engine_ticker_metrics(t: TickerType, value: u64) {
    match t {
        TickerType::BlockCacheMiss => {
            RAFT_ENGINE_CACHE_EFFICIENCY_VEC.with_label_values(&["block_cache_miss"])
                .set(value as f64);
        }
        TickerType::BlockCacheHit => {
            RAFT_ENGINE_CACHE_EFFICIENCY_VEC.with_label_values(&["block_cache_hit"])
                .set(value as f64);
        }
        TickerType::MemtableHit => {
            RAFT_ENGINE_MEMTABLE_EFFICIENCY_VEC.with_label_values(&["memtable_hit"])
                .set(value as f64);
        }
        TickerType::MemtableMiss => {
            RAFT_ENGINE_MEMTABLE_EFFICIENCY_VEC.with_label_values(&["memtable_miss"])
                .set(value as f64);
        }
        TickerType::GetHitL0 => {
            RAFT_ENGINE_READ_SURVED_VEC.with_label_values(&["get_hit_l0"]).set(value as f64);
        }
        TickerType::GetHitL1 => {
            RAFT_ENGINE_READ_SURVED_VEC.with_label_values(&["get_hit_l1"]).set(value as f64);
        }
        TickerType::GetHitL2AndUp => {
            RAFT_ENGINE_READ_SURVED_VEC.with_label_values(&["get_hit_l2_and_up"])
                .set(value as f64);
        }
        TickerType::BloomFilterUseful => {
            RAFT_ENGINE_BLOOM_EFFICIENCY_VEC.with_label_values(&["bloom_useful"])
                .set(value as f64);
        }
        TickerType::BloomFilterPrefixChecked => {
            RAFT_ENGINE_BLOOM_EFFICIENCY_VEC.with_label_values(&["bloom_prefix_checked"])
                .set(value as f64);
        }
        TickerType::BloomFilterPrefixUseful => {
            RAFT_ENGINE_BLOOM_EFFICIENCY_VEC.with_label_values(&["bloom_prefix_useful"])
                .set(value as f64);
        }
        TickerType::NumberKeysWritten => {
            RAFT_ENGINE_FLOW_VEC.with_label_values(&["keys_written"]).set(value as f64);
        }
        TickerType::NumberKeysRead => {
            RAFT_ENGINE_FLOW_VEC.with_label_values(&["keys_read"]).set(value as f64);
        }
        TickerType::BytesWritten => {
            RAFT_ENGINE_FLOW_VEC.with_label_values(&["bytes_written"]).set(value as f64);
        }
        TickerType::BytesRead => {
            RAFT_ENGINE_FLOW_VEC.with_label_values(&["bytes_read"]).set(value as f64);
        }
        TickerType::IterBytesRead => {
            RAFT_ENGINE_FLOW_VEC.with_label_values(&["iter_bytes_read"]).set(value as f64);
        }
        TickerType::StallMicros => {
            RAFT_ENGINE_STALL_MICROS.set(value as f64);
        }
        TickerType::CompactReadBytes => {
            RAFT_ENGINE_COMPACTION_FLOW_VEC.with_label_values(&["bytes_read"]).set(value as f64);
        }
        TickerType::CompactWriteBytes => {
            RAFT_ENGINE_COMPACTION_FLOW_VEC.with_label_values(&["bytes_written"])
                .set(value as f64);
        }
        _ => {}
    }
}

pub fn flush_raft_engine_histogram_metrics(t: HistType, value: HistogramData) {
    match t {
        HistType::DbGetMicros => {
            RAFT_ENGINE_GET_MICROS_VEC.with_label_values(&["get_median"]).set(value.median);
            RAFT_ENGINE_GET_MICROS_VEC.with_label_values(&["get_percentile95"])
                .set(value.percentile95);
            RAFT_ENGINE_GET_MICROS_VEC.with_label_values(&["get_percentile99"])
                .set(value.percentile99);
            RAFT_ENGINE_GET_MICROS_VEC.with_label_values(&["get_average"]).set(value.average);
            RAFT_ENGINE_GET_MICROS_VEC.with_label_values(&["get_standard_deviation"])
                .set(value.standard_deviation);
        }
        HistType::DbWriteMicros => {
            RAFT_ENGINE_WRITE_MICROS_VEC.with_label_values(&["write_median"]).set(value.median);
            RAFT_ENGINE_WRITE_MICROS_VEC.with_label_values(&["write_percentile95"])
                .set(value.percentile95);
            RAFT_ENGINE_WRITE_MICROS_VEC.with_label_values(&["write_percentile99"])
                .set(value.percentile99);
            RAFT_ENGINE_WRITE_MICROS_VEC.with_label_values(&["write_average"]).set(value.average);
            RAFT_ENGINE_WRITE_MICROS_VEC.with_label_values(&["write_standard_deviation"])
                .set(value.standard_deviation);
        }
        HistType::DbSeekMicros => {
            RAFT_ENGINE_SEEK_MICROS_VEC.with_label_values(&["seek_median"]).set(value.median);
            RAFT_ENGINE_SEEK_MICROS_VEC.with_label_values(&["seek_percentile95"])
                .set(value.percentile95);
            RAFT_ENGINE_SEEK_MICROS_VEC.with_label_values(&["seek_percentile99"])
                .set(value.percentile99);
            RAFT_ENGINE_SEEK_MICROS_VEC.with_label_values(&["seek_average"]).set(value.average);
            RAFT_ENGINE_SEEK_MICROS_VEC.with_label_values(&["seek_standard_deviation"])
                .set(value.standard_deviation);
        }
    }
}

fn get_cf_size(engine: &Arc<DB>, cf: &str, used_size: &mut u64) {
    let handle = rocksdb::get_cf_handle(engine, cf).unwrap();
    let cf_used_size = engine.get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
        .expect("rocksdb is too old, missing total-sst-files-size property");

    // For block cache usage
    let block_cache_usage = engine.get_block_cache_usage_cf(handle);
    RAFT_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC.with_label_values(&[cf])
        .set(block_cache_usage as f64);

    // It is important to monitor each cf's size, especially the "raft" and "lock" column
    // families.
    RAFT_ENGINE_SIZE_GAUGE_VEC.with_label_values(&[cf]).set(cf_used_size as f64);

    *used_size += cf_used_size;

    // TODO: find a better place to record these metrics.
    // Refer: https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
    // For index and filter blocks memory
    if let Some(readers_mem) = engine.get_property_int_cf(handle, ROCKSDB_TABLE_READERS_MEM) {
        RAFT_ENGINE_MEMORY_GAUGE_VEC.with_label_values(&[cf, "readers-mem"])
            .set(readers_mem as f64);
    }

    // For memtable
    if let Some(mem_table) = engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES) {
        RAFT_ENGINE_MEMORY_GAUGE_VEC.with_label_values(&[cf, "mem-tables"])
            .set(mem_table as f64);
        *used_size += mem_table;
    }

    // TODO: add cache usage and pinned usage.

    if let Some(num_keys) = engine.get_property_int_cf(handle, ROCKSDB_ESTIMATE_NUM_KEYS) {
        RAFT_ENGINE_ESTIMATE_NUM_KEYS_VEC.with_label_values(&[cf])
            .set(num_keys as f64);
    }

    // Pending compaction bytes
    if let Some(pending_compaction_bytes) =
           engine.get_property_int_cf(handle, ROCKSDB_ESTIMATE_PENDING_COMPACTION_BYTES) {
        RAFT_ENGINE_PENDING_COMACTION_BYTES_VEC.with_label_values(&[cf])
            .set(pending_compaction_bytes as f64);
    }
}

pub fn flush_raft_engine_properties_and_get_used_size(raft_engine: Arc<DB>) -> u64 {
    let mut used_size: u64 = 0;
    for cf in &[CF_DEFAULT, CF_RAFT] {
        get_cf_size(&raft_engine, cf, &mut used_size);
    }
    used_size
}

lazy_static!{
    pub static ref RAFT_ENGINE_SIZE_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_size_bytes",
            "Sizes of each column families.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_block_cache_size_bytes",
            "Usage of each column families' block cache.",
            &["cf"]
        ).unwrap();

    pub static ref RAFT_ENGINE_MEMORY_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_memory_bytes",
            "Sizes of each column families.",
            &["cf", "type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_ESTIMATE_NUM_KEYS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_estimate_num_keys",
            "Estimate num keys of each column families.",
            &["cf"]
        ).unwrap();

    pub static ref RAFT_ENGINE_CACHE_EFFICIENCY_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_cache_efficiency",
            "Efficiency of rocksdb's block cache.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_MEMTABLE_EFFICIENCY_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_memtable_efficiency",
            "Hit and miss of memtable.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_READ_SURVED_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_get_served",
            "Get queries served by.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_BLOOM_EFFICIENCY_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_bloom_efficiency",
            "Efficiency of rocksdb's bloom filter.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_FLOW_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_flow_bytes",
            "Bytes and keys of read/written.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_STALL_MICROS: Gauge =
        register_gauge!(
            "tikv_raft_engine_stall_micro_seconds",
            "Stall micros."
        ).unwrap();

    pub static ref RAFT_ENGINE_GET_MICROS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_get_micro_seconds",
            "Get micros histogram.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_WRITE_MICROS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_write_micro_seconds",
            "Write micros histogram.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_SEEK_MICROS_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_seek_micro_seconds",
            "Seek micros histogram.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_ENGINE_PENDING_COMACTION_BYTES_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_pending_compaction_bytes",
            "Pending compaction bytes.",
            &["cf"]
        ).unwrap();

    pub static ref RAFT_ENGINE_COMPACTION_FLOW_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raft_engine_compaction_flow_bytes",
            "Bytes of read/written during compaction.",
            &["type"]
        ).unwrap();
}
