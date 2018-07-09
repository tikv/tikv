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

use std::i64;

use prometheus::{exponential_buckets, GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec};
use rocksdb::{
    DBStatisticsHistogramType as HistType, DBStatisticsTickerType as TickerType, HistogramData, DB,
};
use time;
use util::rocksdb;

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
    TickerType::BloomFilterPrefixChecked,
    TickerType::BloomFilterPrefixUseful,
    TickerType::WalFileSynced,
    TickerType::WalFileBytes,
    TickerType::WriteDoneBySelf,
    TickerType::WriteDoneByOther,
    TickerType::WriteTimeout,
    TickerType::WriteWithWAL,
    TickerType::CompactReadBytes,
    TickerType::CompactWriteBytes,
    TickerType::FlushWriteBytes,
    TickerType::ReadAmpEstimateUsefulBytes,
    TickerType::ReadAmpTotalReadBytes,
];
pub const ENGINE_HIST_TYPES: &[HistType] = &[
    HistType::GetMicros,
    HistType::WriteMicros,
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
    HistType::SeekMicros,
    HistType::WriteStall,
    HistType::SSTReadMicros,
    HistType::NumSubcompactionsScheduled,
    HistType::BytesPerRead,
    HistType::BytesPerWrite,
    HistType::BytesCompressed,
    HistType::BytesDecompressed,
    HistType::CompressionTimesNanos,
    HistType::DecompressionTimesNanos,
];

pub fn flush_engine_ticker_metrics(t: TickerType, value: u64, name: &str) {
    let v = value as i64;
    if v < 0 {
        warn!("engine ticker {:?} is overflow {}", t, value);
        return;
    }

    match t {
        TickerType::BlockCacheMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_miss"])
                .inc_by(v);
        }
        TickerType::BlockCacheHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_hit"])
                .inc_by(v);
        }
        TickerType::BlockCacheAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_add"])
                .inc_by(v);
        }
        TickerType::BlockCacheAddFailures => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_add_failures"])
                .inc_by(v);
        }
        TickerType::BlockCacheIndexMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_index_miss"])
                .inc_by(v);
        }
        TickerType::BlockCacheIndexHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_index_hit"])
                .inc_by(v);
        }
        TickerType::BlockCacheIndexAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_index_add"])
                .inc_by(v);
        }
        TickerType::BlockCacheIndexBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_index_bytes_insert"])
                .inc_by(v);
        }
        TickerType::BlockCacheIndexBytesEvict => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_index_bytes_evict"])
                .inc_by(v);
        }
        TickerType::BlockCacheFilterMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_filter_miss"])
                .inc_by(v);
        }
        TickerType::BlockCacheFilterHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_filter_hit"])
                .inc_by(v);
        }
        TickerType::BlockCacheFilterAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_filter_add"])
                .inc_by(v);
        }
        TickerType::BlockCacheFilterBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_filter_bytes_insert"])
                .inc_by(v);
        }
        TickerType::BlockCacheFilterBytesEvict => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_filter_bytes_evict"])
                .inc_by(v);
        }
        TickerType::BlockCacheDataMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_data_miss"])
                .inc_by(v);
        }
        TickerType::BlockCacheDataHit => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_data_hit"])
                .inc_by(v);
        }
        TickerType::BlockCacheDataAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_data_add"])
                .inc_by(v);
        }
        TickerType::BlockCacheDataBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY_VEC
                .with_label_values(&[name, "block_cache_data_bytes_insert"])
                .inc_by(v);
        }
        TickerType::BlockCacheByteRead => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "block_cache_byte_read"])
                .inc_by(v);
        }
        TickerType::BlockCacheByteWrite => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "block_cache_byte_write"])
                .inc_by(v);
        }
        TickerType::BloomFilterUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                .with_label_values(&[name, "bloom_useful"])
                .inc_by(v);
        }
        TickerType::MemtableHit => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC
                .with_label_values(&[name, "memtable_hit"])
                .inc_by(v);
        }
        TickerType::MemtableMiss => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC
                .with_label_values(&[name, "memtable_miss"])
                .inc_by(v);
        }
        TickerType::GetHitL0 => {
            STORE_ENGINE_GET_SERVED_VEC
                .with_label_values(&[name, "get_hit_l0"])
                .inc_by(v);
        }
        TickerType::GetHitL1 => {
            STORE_ENGINE_GET_SERVED_VEC
                .with_label_values(&[name, "get_hit_l1"])
                .inc_by(v);
        }
        TickerType::GetHitL2AndUp => {
            STORE_ENGINE_GET_SERVED_VEC
                .with_label_values(&[name, "get_hit_l2_and_up"])
                .inc_by(v);
        }
        TickerType::CompactionKeyDropNewerEntry => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "compaction_key_drop_newer_entry"])
                .inc_by(v);
        }
        TickerType::CompactionKeyDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "compaction_key_drop_obsolete"])
                .inc_by(v);
        }
        TickerType::CompactionKeyDropRangeDel => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "compaction_key_drop_range_del"])
                .inc_by(v);
        }
        TickerType::CompactionRangeDelDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "range_del_drop_obsolete"])
                .inc_by(v);
        }
        TickerType::CompactionOptimizedDelDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP_VEC
                .with_label_values(&[name, "optimized_del_drop_obsolete"])
                .inc_by(v);
        }
        TickerType::NumberKeysWritten => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "keys_written"])
                .inc_by(v);
        }
        TickerType::NumberKeysRead => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "keys_read"])
                .inc_by(v);
        }
        TickerType::NumberKeysUpdated => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "keys_updated"])
                .inc_by(v);
        }
        TickerType::BytesWritten => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "bytes_written"])
                .inc_by(v);
        }
        TickerType::BytesRead => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "bytes_read"])
                .inc_by(v);
        }
        TickerType::NumberDbSeek => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_seek"])
                .inc_by(v);
        }
        TickerType::NumberDbNext => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_next"])
                .inc_by(v);
        }
        TickerType::NumberDbPrev => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_prev"])
                .inc_by(v);
        }
        TickerType::NumberDbSeekFound => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_seek_found"])
                .inc_by(v);
        }
        TickerType::NumberDbNextFound => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_next_found"])
                .inc_by(v);
        }
        TickerType::NumberDbPrevFound => {
            STORE_ENGINE_LOCATE_VEC
                .with_label_values(&[name, "number_db_prev_found"])
                .inc_by(v);
        }
        TickerType::IterBytesRead => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "iter_bytes_read"])
                .inc_by(v);
        }
        TickerType::NoFileCloses => {
            STORE_ENGINE_FILE_STATUS_VEC
                .with_label_values(&[name, "no_file_closes"])
                .inc_by(v);
        }
        TickerType::NoFileOpens => {
            STORE_ENGINE_FILE_STATUS_VEC
                .with_label_values(&[name, "no_file_opens"])
                .inc_by(v);
        }
        TickerType::NoFileErrors => {
            STORE_ENGINE_FILE_STATUS_VEC
                .with_label_values(&[name, "no_file_errors"])
                .inc_by(v);
        }
        TickerType::StallMicros => {
            STORE_ENGINE_STALL_MICROS
                .with_label_values(&[name])
                .inc_by(v);
        }
        TickerType::BloomFilterPrefixChecked => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                .with_label_values(&[name, "bloom_prefix_checked"])
                .inc_by(v);
        }
        TickerType::BloomFilterPrefixUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY_VEC
                .with_label_values(&[name, "bloom_prefix_useful"])
                .inc_by(v);
        }
        TickerType::WalFileSynced => {
            STORE_ENGINE_WAL_FILE_SYNCED
                .with_label_values(&[name])
                .inc_by(v);
        }
        TickerType::WalFileBytes => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "wal_file_bytes"])
                .inc_by(v);
        }
        TickerType::WriteDoneBySelf => {
            STORE_ENGINE_WRITE_SERVED_VEC
                .with_label_values(&[name, "write_done_by_self"])
                .inc_by(v);
        }
        TickerType::WriteDoneByOther => {
            STORE_ENGINE_WRITE_SERVED_VEC
                .with_label_values(&[name, "write_done_by_other"])
                .inc_by(v);
        }
        TickerType::WriteTimeout => {
            STORE_ENGINE_WRITE_SERVED_VEC
                .with_label_values(&[name, "write_timeout"])
                .inc_by(v);
        }
        TickerType::WriteWithWAL => {
            STORE_ENGINE_WRITE_SERVED_VEC
                .with_label_values(&[name, "write_with_wal"])
                .inc_by(v);
        }
        TickerType::CompactReadBytes => {
            STORE_ENGINE_COMPACTION_FLOW_VEC
                .with_label_values(&[name, "bytes_read"])
                .inc_by(v);
        }
        TickerType::CompactWriteBytes => {
            STORE_ENGINE_COMPACTION_FLOW_VEC
                .with_label_values(&[name, "bytes_written"])
                .inc_by(v);
        }
        TickerType::FlushWriteBytes => {
            STORE_ENGINE_FLOW_VEC
                .with_label_values(&[name, "flush_write_bytes"])
                .inc_by(v);
        }
        TickerType::ReadAmpEstimateUsefulBytes => {
            STORE_ENGINE_READ_AMP_FLOW_VEC
                .with_label_values(&[name, "read_amp_estimate_useful_bytes"])
                .inc_by(v);
        }
        TickerType::ReadAmpTotalReadBytes => {
            STORE_ENGINE_READ_AMP_FLOW_VEC
                .with_label_values(&[name, "read_amp_total_read_bytes"])
                .inc_by(v);
        }
        _ => {}
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
            STORE_ENGINE_GET_MICROS_VEC
                .with_label_values(&[name, "get_max"])
                .set(value.max);
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
            STORE_ENGINE_WRITE_MICROS_VEC
                .with_label_values(&[name, "write_max"])
                .set(value.max);
        }
        HistType::CompactionTime => {
            STORE_ENGINE_COMPACTION_TIME_VEC
                .with_label_values(&[name, "compaction_time_median"])
                .set(value.median);
            STORE_ENGINE_COMPACTION_TIME_VEC
                .with_label_values(&[name, "compaction_time_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_COMPACTION_TIME_VEC
                .with_label_values(&[name, "compaction_time_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_COMPACTION_TIME_VEC
                .with_label_values(&[name, "compaction_time_average"])
                .set(value.average);
            STORE_ENGINE_COMPACTION_TIME_VEC
                .with_label_values(&[name, "compaction_time_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_COMPACTION_TIME_VEC
                .with_label_values(&[name, "compaction_time_max"])
                .set(value.max);
        }
        HistType::TableSyncMicros => {
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                .with_label_values(&[name, "table_sync_median"])
                .set(value.median);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                .with_label_values(&[name, "table_sync_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                .with_label_values(&[name, "table_sync_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                .with_label_values(&[name, "table_sync_average"])
                .set(value.average);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                .with_label_values(&[name, "table_sync_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_TABLE_SYNC_MICROS_VEC
                .with_label_values(&[name, "table_sync_max"])
                .set(value.max);
        }
        HistType::CompactionOutfileSyncMicros => {
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "compaction_outfile_sync_median"])
                .set(value.median);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "compaction_outfile_sync_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "compaction_outfile_sync_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "compaction_outfile_sync_average"])
                .set(value.average);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "compaction_outfile_sync_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "compaction_outfile_sync_max"])
                .set(value.max);
        }
        HistType::WalFileSyncMicros => {
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "wal_file_sync_median"])
                .set(value.median);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "wal_file_sync_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "wal_file_sync_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "wal_file_sync_average"])
                .set(value.average);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "wal_file_sync_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "wal_file_sync_max"])
                .set(value.standard_deviation);
        }
        HistType::ManifestFileSyncMicros => {
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "manifest_file_sync_median"])
                .set(value.median);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "manifest_file_sync_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "manifest_file_sync_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "manifest_file_sync_average"])
                .set(value.average);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "manifest_file_sync_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC
                .with_label_values(&[name, "manifest_file_sync_max"])
                .set(value.max);
        }
        HistType::StallL0SlowdownCount => {
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                .with_label_values(&[name, "stall_l0_slowdown_count_median"])
                .set(value.median);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                .with_label_values(&[name, "stall_l0_slowdown_count_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                .with_label_values(&[name, "stall_l0_slowdown_count_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                .with_label_values(&[name, "stall_l0_slowdown_count_average"])
                .set(value.average);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                .with_label_values(&[name, "stall_l0_slowdown_count_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC
                .with_label_values(&[name, "stall_l0_slowdown_count_max"])
                .set(value.max);
        }
        HistType::StallMemtableCompactionCount => {
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                .with_label_values(&[name, "stall_memtable_compaction_count_median"])
                .set(value.median);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                .with_label_values(&[name, "stall_memtable_compaction_count_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                .with_label_values(&[name, "stall_memtable_compaction_count_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                .with_label_values(&[name, "stall_memtable_compaction_count_average"])
                .set(value.average);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                .with_label_values(&[name, "stall_memtable_compaction_count_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC
                .with_label_values(&[name, "stall_memtable_compaction_count_max"])
                .set(value.max);
        }
        HistType::StallL0NumFilesCount => {
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                .with_label_values(&[name, "stall_l0_num_files_count_median"])
                .set(value.median);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                .with_label_values(&[name, "stall_l0_num_files_count_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                .with_label_values(&[name, "stall_l0_num_files_count_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                .with_label_values(&[name, "stall_l0_num_files_count_average"])
                .set(value.average);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                .with_label_values(&[name, "stall_l0_num_files_count_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC
                .with_label_values(&[name, "stall_l0_num_files_count_max"])
                .set(value.max);
        }
        HistType::HardRateLimitDelayCount => {
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "hard_rate_limit_delay_median"])
                .set(value.median);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "hard_rate_limit_delay_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "hard_rate_limit_delay_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "hard_rate_limit_delay_average"])
                .set(value.average);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "hard_rate_limit_delay_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "hard_rate_limit_delay_max"])
                .set(value.max);
        }
        HistType::SoftRateLimitDelayCount => {
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "soft_rate_limit_delay_median"])
                .set(value.median);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "soft_rate_limit_delay_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "soft_rate_limit_delay_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "soft_rate_limit_delay_average"])
                .set(value.average);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "soft_rate_limit_delay_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC
                .with_label_values(&[name, "soft_rate_limit_delay_max"])
                .set(value.max);
        }
        HistType::NumFilesInSingleCompaction => {
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                .with_label_values(&[name, "num_files_in_single_compaction_median"])
                .set(value.median);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                .with_label_values(&[name, "num_files_in_single_compaction_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                .with_label_values(&[name, "num_files_in_single_compaction_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                .with_label_values(&[name, "num_files_in_single_compaction_average"])
                .set(value.average);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                .with_label_values(&[name, "num_files_in_single_compaction_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC
                .with_label_values(&[name, "num_files_in_single_compaction_max"])
                .set(value.max);
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
            STORE_ENGINE_SEEK_MICROS_VEC
                .with_label_values(&[name, "seek_max"])
                .set(value.max);
        }
        HistType::WriteStall => {
            STORE_ENGINE_WRITE_STALL_VEC
                .with_label_values(&[name, "write_stall_median"])
                .set(value.median);
            STORE_ENGINE_WRITE_STALL_VEC
                .with_label_values(&[name, "write_stall_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_WRITE_STALL_VEC
                .with_label_values(&[name, "write_stall_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_WRITE_STALL_VEC
                .with_label_values(&[name, "write_stall_average"])
                .set(value.average);
            STORE_ENGINE_WRITE_STALL_VEC
                .with_label_values(&[name, "write_stall_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_WRITE_STALL_VEC
                .with_label_values(&[name, "write_stall_max"])
                .set(value.max);
        }
        HistType::SSTReadMicros => {
            STORE_ENGINE_SST_READ_MICROS_VEC
                .with_label_values(&[name, "sst_read_micros_median"])
                .set(value.median);
            STORE_ENGINE_SST_READ_MICROS_VEC
                .with_label_values(&[name, "sst_read_micros_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_SST_READ_MICROS_VEC
                .with_label_values(&[name, "sst_read_micros_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_SST_READ_MICROS_VEC
                .with_label_values(&[name, "sst_read_micros_average"])
                .set(value.average);
            STORE_ENGINE_SST_READ_MICROS_VEC
                .with_label_values(&[name, "sst_read_micros_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_SST_READ_MICROS_VEC
                .with_label_values(&[name, "sst_read_micros_max"])
                .set(value.max);
        }
        HistType::NumSubcompactionsScheduled => {
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                .with_label_values(&[name, "num_subcompaction_scheduled_median"])
                .set(value.median);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                .with_label_values(&[name, "num_subcompaction_scheduled_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                .with_label_values(&[name, "num_subcompaction_scheduled_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                .with_label_values(&[name, "num_subcompaction_scheduled_average"])
                .set(value.average);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                .with_label_values(&[name, "num_subcompaction_scheduled_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC
                .with_label_values(&[name, "num_subcompaction_scheduled_max"])
                .set(value.max);
        }
        HistType::BytesPerRead => {
            STORE_ENGINE_BYTES_PER_READ_VEC
                .with_label_values(&[name, "bytes_per_read_median"])
                .set(value.median);
            STORE_ENGINE_BYTES_PER_READ_VEC
                .with_label_values(&[name, "bytes_per_read_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_BYTES_PER_READ_VEC
                .with_label_values(&[name, "bytes_per_read_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_BYTES_PER_READ_VEC
                .with_label_values(&[name, "bytes_per_read_average"])
                .set(value.average);
            STORE_ENGINE_BYTES_PER_READ_VEC
                .with_label_values(&[name, "bytes_per_read_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_BYTES_PER_READ_VEC
                .with_label_values(&[name, "bytes_per_read_max"])
                .set(value.max);
        }
        HistType::BytesPerWrite => {
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                .with_label_values(&[name, "bytes_per_write_median"])
                .set(value.median);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                .with_label_values(&[name, "bytes_per_write_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                .with_label_values(&[name, "bytes_per_write_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                .with_label_values(&[name, "bytes_per_write_average"])
                .set(value.average);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                .with_label_values(&[name, "bytes_per_write_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_BYTES_PER_WRITE_VEC
                .with_label_values(&[name, "bytes_per_write_max"])
                .set(value.max);
        }
        HistType::BytesCompressed => {
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                .with_label_values(&[name, "bytes_compressed_median"])
                .set(value.median);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                .with_label_values(&[name, "bytes_compressed_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                .with_label_values(&[name, "bytes_compressed_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                .with_label_values(&[name, "bytes_compressed_average"])
                .set(value.average);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                .with_label_values(&[name, "bytes_compressed_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_BYTES_COMPRESSED_VEC
                .with_label_values(&[name, "bytes_compressed_max"])
                .set(value.max);
        }
        HistType::BytesDecompressed => {
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                .with_label_values(&[name, "bytes_decompressed_median"])
                .set(value.median);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                .with_label_values(&[name, "bytes_decompressed_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                .with_label_values(&[name, "bytes_decompressed_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                .with_label_values(&[name, "bytes_decompressed_average"])
                .set(value.average);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                .with_label_values(&[name, "bytes_decompressed_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_BYTES_DECOMPRESSED_VEC
                .with_label_values(&[name, "bytes_decompressed_max"])
                .set(value.max);
        }
        HistType::CompressionTimesNanos => {
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "compression_time_nanos_median"])
                .set(value.median);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "compression_time_nanos_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "compression_time_nanos_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "compression_time_nanos_average"])
                .set(value.average);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "compression_time_nanos_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "compression_time_nanos_max"])
                .set(value.max);
        }
        HistType::DecompressionTimesNanos => {
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "decompression_time_nanos_median"])
                .set(value.median);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "decompression_time_nanos_percentile95"])
                .set(value.percentile95);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "decompression_time_nanos_percentile99"])
                .set(value.percentile99);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "decompression_time_nanos_average"])
                .set(value.average);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "decompression_time_nanos_standard_deviation"])
                .set(value.standard_deviation);
            STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC
                .with_label_values(&[name, "decompression_time_nanos_max"])
                .set(value.max);
        }
        _ => {}
    }
}

pub fn flush_engine_properties(engine: &DB, name: &str) {
    for cf in engine.cf_names() {
        let handle = rocksdb::get_cf_handle(engine, cf).unwrap();
        // It is important to monitor each cf's size, especially the "raft" and "lock" column
        // families.
        let cf_used_size = engine
            .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
            .expect("rocksdb is too old, missing total-sst-files-size property");
        STORE_ENGINE_SIZE_GAUGE_VEC
            .with_label_values(&[name, cf])
            .set(cf_used_size as i64);

        // For block cache usage
        let block_cache_usage = engine.get_block_cache_usage_cf(handle);
        STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC
            .with_label_values(&[name, cf])
            .set(block_cache_usage as i64);

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
            STORE_ENGINE_PENDING_COMACTION_BYTES_VEC
                .with_label_values(&[name, cf])
                .set(pending_compaction_bytes as i64);
        }

        // Compression ratio at levels
        let opts = engine.get_options_cf(handle);
        for level in 0..opts.get_num_levels() {
            if let Some(v) = rocksdb::get_engine_compression_ratio_at_level(engine, handle, level) {
                let level_str = level.to_string();
                STORE_ENGINE_COMPRESSION_RATIO_VEC
                    .with_label_values(&[name, cf, &level_str])
                    .set(v);
            }
        }

        // Num files at levels
        let opts = engine.get_options_cf(handle);
        for level in 0..opts.get_num_levels() {
            let prop = format!("{}{}", ROCKSDB_NUM_FILES_AT_LEVEL, level);
            let level_str = level.to_string();
            if let Some(v) = engine.get_property_int_cf(handle, &prop) {
                STORE_ENGINE_NUM_FILES_AT_LEVEL_VEC
                    .with_label_values(&[name, cf, &level_str])
                    .set(v as i64);
            }
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
}

// Skip with rustfmt since several names are too long.
#[cfg_attr(rustfmt, rustfmt_skip)]
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
    pub static ref STORE_ENGINE_STALL_MICROS: IntCounterVec =
        register_int_counter_vec!("tikv_engine_stall_micro_seconds", "Stall micros", &["db"])
            .unwrap();
    pub static ref STORE_ENGINE_GET_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_get_micro_seconds",
        "Histogram of get micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_WRITE_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_write_micro_seconds",
        "Histogram of write micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_TIME_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_compaction_time",
        "Histogram of compaction time",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_TABLE_SYNC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_table_sync_micro_seconds",
        "Histogram of table sync micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_OUTFILE_SYNC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_compaction_outfile_sync_micro_seconds",
        "Histogram of compaction outfile sync micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_MANIFEST_FILE_SYNC_MICROS_VEC: GaugeVec = register_gauge_vec!(
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
    pub static ref STORE_ENGINE_STALL_LO_NUM_FILES_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_stall_l0_num_files_count",
        "Histogram of stall l0 num files count",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_HARD_RATE_LIMIT_DELAY_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_hard_rate_limit_delay_count",
        "Histogram of hard rate limit delay count",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_COUNT_VEC: GaugeVec = register_gauge_vec!(
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
    pub static ref STORE_ENGINE_PENDING_COMACTION_BYTES_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_pending_compaction_bytes",
        "Pending compaction bytes",
        &["db", "cf"]
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
    pub static ref STORE_ENGINE_COMPRESSION_RATIO_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_compression_ratio",
        "Compression ratio at different levels",
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

    pub static ref STORE_ENGINE_NUM_FILES_AT_LEVEL_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_num_files_at_level",
        "Number of files at each level",
        &["db", "cf", "level"]
    ).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempdir::TempDir;

    use storage::ALL_CFS;
    use util::rocksdb;

    #[test]
    fn test_flush() {
        let dir = TempDir::new("test-flush").unwrap();
        let db = rocksdb::new_engine(dir.path().to_str().unwrap(), ALL_CFS, None).unwrap();
        for tp in ENGINE_TICKER_TYPES {
            flush_engine_ticker_metrics(*tp, 2, "test-name");
        }

        for tp in ENGINE_HIST_TYPES {
            flush_engine_histogram_metrics(*tp, HistogramData::default(), "test-name");
        }

        flush_engine_properties(&db, "test-name");
    }
}
