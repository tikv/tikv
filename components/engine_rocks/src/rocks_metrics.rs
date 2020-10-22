// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::rocks_metrics_defs::*;
use engine_traits::engine_histogram_metrics;
use engine_traits::metrics::*;
use engine_traits::CF_DEFAULT;
use lazy_static::lazy_static;
use prometheus::*;

use rocksdb::{
    DBStatisticsHistogramType as HistType, DBStatisticsTickerType as TickerType, HistogramData, DB,
};
use std::i64;

pub fn flush_engine_ticker_metrics(t: TickerType, value: u64, name: &str) {
    let v = value as i64;
    if v < 0 {
        warn!("engine ticker is overflow";
            "ticker" => ?t, "value" => value
        );
        return;
    }

    let name_enum = match name {
        "kv" => TickerName::kv,
        "raft" => TickerName::raft,
        unexpected => panic!(format!("unexpected name {}", unexpected)),
    };

    match t {
        TickerType::BlockCacheMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_miss
                .inc_by(v);
        }
        TickerType::BlockCacheHit => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_hit
                .inc_by(v);
        }
        TickerType::BlockCacheAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_add
                .inc_by(v);
        }
        TickerType::BlockCacheAddFailures => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_add_failures
                .inc_by(v);
        }
        TickerType::BlockCacheIndexMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_miss
                .inc_by(v);
        }
        TickerType::BlockCacheIndexHit => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_hit
                .inc_by(v);
        }
        TickerType::BlockCacheIndexAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_add
                .inc_by(v);
        }
        TickerType::BlockCacheIndexBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_bytes_insert
                .inc_by(v);
        }
        TickerType::BlockCacheIndexBytesEvict => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_bytes_evict
                .inc_by(v);
        }
        TickerType::BlockCacheFilterMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_miss
                .inc_by(v);
        }
        TickerType::BlockCacheFilterHit => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_hit
                .inc_by(v);
        }
        TickerType::BlockCacheFilterAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_add
                .inc_by(v);
        }
        TickerType::BlockCacheFilterBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_bytes_insert
                .inc_by(v);
        }
        TickerType::BlockCacheFilterBytesEvict => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_bytes_evict
                .inc_by(v);
        }
        TickerType::BlockCacheDataMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_miss
                .inc_by(v);
        }
        TickerType::BlockCacheDataHit => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_hit
                .inc_by(v);
        }
        TickerType::BlockCacheDataAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_add
                .inc_by(v);
        }
        TickerType::BlockCacheDataBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_bytes_insert
                .inc_by(v);
        }
        TickerType::BlockCacheBytesRead => {
            STORE_ENGINE_FLOW
                .get(name_enum)
                .block_cache_byte_read
                .inc_by(v);
        }
        TickerType::BlockCacheBytesWrite => {
            STORE_ENGINE_FLOW
                .get(name_enum)
                .block_cache_byte_write
                .inc_by(v);
        }
        TickerType::BloomFilterUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_useful
                .inc_by(v);
        }
        TickerType::MemtableHit => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY
                .get(name_enum)
                .memtable_hit
                .inc_by(v);
        }
        TickerType::MemtableMiss => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY
                .get(name_enum)
                .memtable_miss
                .inc_by(v);
        }
        TickerType::GetHitL0 => {
            STORE_ENGINE_GET_SERVED.get(name_enum).get_hit_l0.inc_by(v);
        }
        TickerType::GetHitL1 => {
            STORE_ENGINE_GET_SERVED.get(name_enum).get_hit_l1.inc_by(v);
        }
        TickerType::GetHitL2AndUp => {
            STORE_ENGINE_GET_SERVED
                .get(name_enum)
                .get_hit_l2_and_up
                .inc_by(v);
        }
        TickerType::CompactionKeyDropNewerEntry => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .compaction_key_drop_newer_entry
                .inc_by(v);
        }
        TickerType::CompactionKeyDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .compaction_key_drop_obsolete
                .inc_by(v);
        }
        TickerType::CompactionKeyDropRangeDel => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .compaction_key_drop_range_del
                .inc_by(v);
        }
        TickerType::CompactionRangeDelDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .range_del_drop_obsolete
                .inc_by(v);
        }
        TickerType::CompactionOptimizedDelDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .optimized_del_drop_obsolete
                .inc_by(v);
        }
        TickerType::NumberKeysWritten => {
            STORE_ENGINE_FLOW.get(name_enum).keys_written.inc_by(v);
        }
        TickerType::NumberKeysRead => {
            STORE_ENGINE_FLOW.get(name_enum).keys_read.inc_by(v);
        }
        TickerType::NumberKeysUpdated => {
            STORE_ENGINE_FLOW.get(name_enum).keys_updated.inc_by(v);
        }
        TickerType::BytesWritten => {
            STORE_ENGINE_FLOW.get(name_enum).bytes_written.inc_by(v);
        }
        TickerType::BytesRead => {
            STORE_ENGINE_FLOW.get(name_enum).bytes_read.inc_by(v);
        }
        TickerType::NumberDbSeek => {
            STORE_ENGINE_LOCATE.get(name_enum).number_db_seek.inc_by(v);
        }
        TickerType::NumberDbNext => {
            STORE_ENGINE_LOCATE.get(name_enum).number_db_next.inc_by(v);
        }
        TickerType::NumberDbPrev => {
            STORE_ENGINE_LOCATE.get(name_enum).number_db_prev.inc_by(v);
        }
        TickerType::NumberDbSeekFound => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_seek_found
                .inc_by(v);
        }
        TickerType::NumberDbNextFound => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_next_found
                .inc_by(v);
        }
        TickerType::NumberDbPrevFound => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_prev_found
                .inc_by(v);
        }
        TickerType::IterBytesRead => {
            STORE_ENGINE_FLOW.get(name_enum).iter_bytes_read.inc_by(v);
        }
        TickerType::NoFileCloses => {
            STORE_ENGINE_FILE_STATUS
                .get(name_enum)
                .no_file_closes
                .inc_by(v);
        }
        TickerType::NoFileOpens => {
            STORE_ENGINE_FILE_STATUS
                .get(name_enum)
                .no_file_opens
                .inc_by(v);
        }
        TickerType::NoFileErrors => {
            STORE_ENGINE_FILE_STATUS
                .get(name_enum)
                .no_file_errors
                .inc_by(v);
        }
        TickerType::StallMicros => {
            STORE_ENGINE_STALL_MICROS.get(name_enum).inc_by(v);
        }
        TickerType::BloomFilterPrefixChecked => {
            STORE_ENGINE_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_prefix_checked
                .inc_by(v);
        }
        TickerType::BloomFilterPrefixUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_prefix_useful
                .inc_by(v);
        }
        TickerType::WalFileSynced => {
            STORE_ENGINE_WAL_FILE_SYNCED.get(name_enum).inc_by(v);
        }
        TickerType::WalFileBytes => {
            STORE_ENGINE_FLOW.get(name_enum).wal_file_bytes.inc_by(v);
        }
        TickerType::WriteDoneBySelf => {
            STORE_ENGINE_WRITE_SERVED
                .get(name_enum)
                .write_done_by_self
                .inc_by(v);
        }
        TickerType::WriteDoneByOther => {
            STORE_ENGINE_WRITE_SERVED
                .get(name_enum)
                .write_done_by_other
                .inc_by(v);
        }
        TickerType::WriteTimedout => {
            STORE_ENGINE_WRITE_SERVED
                .get(name_enum)
                .write_timeout
                .inc_by(v);
        }
        TickerType::WriteWithWal => {
            STORE_ENGINE_WRITE_SERVED
                .get(name_enum)
                .write_with_wal
                .inc_by(v);
        }
        TickerType::CompactReadBytes => {
            STORE_ENGINE_COMPACTION_FLOW
                .get(name_enum)
                .bytes_read
                .inc_by(v);
        }
        TickerType::CompactWriteBytes => {
            STORE_ENGINE_COMPACTION_FLOW
                .get(name_enum)
                .bytes_written
                .inc_by(v);
        }
        TickerType::FlushWriteBytes => {
            STORE_ENGINE_FLOW.get(name_enum).flush_write_bytes.inc_by(v);
        }
        TickerType::ReadAmpEstimateUsefulBytes => {
            STORE_ENGINE_READ_AMP_FLOW
                .get(name_enum)
                .read_amp_estimate_useful_bytes
                .inc_by(v);
        }
        TickerType::ReadAmpTotalReadBytes => {
            STORE_ENGINE_READ_AMP_FLOW
                .get(name_enum)
                .read_amp_total_read_bytes
                .inc_by(v);
        }
        TickerType::TitanNumGet => {
            STORE_ENGINE_BLOB_LOCATE
                .get(name_enum)
                .number_blob_get
                .inc_by(v);
        }
        TickerType::TitanNumSeek => {
            STORE_ENGINE_BLOB_LOCATE
                .get(name_enum)
                .number_blob_seek
                .inc_by(v);
        }
        TickerType::TitanNumNext => {
            STORE_ENGINE_BLOB_LOCATE
                .get(name_enum)
                .number_blob_next
                .inc_by(v);
        }
        TickerType::TitanNumPrev => {
            STORE_ENGINE_BLOB_LOCATE
                .get(name_enum)
                .number_blob_prev
                .inc_by(v);
        }
        TickerType::TitanBlobFileNumKeysWritten => {
            STORE_ENGINE_BLOB_FLOW.get(name_enum).keys_written.inc_by(v);
        }
        TickerType::TitanBlobFileNumKeysRead => {
            STORE_ENGINE_BLOB_FLOW.get(name_enum).keys_read.inc_by(v);
        }
        TickerType::TitanBlobFileBytesWritten => {
            STORE_ENGINE_BLOB_FLOW
                .get(name_enum)
                .bytes_written
                .inc_by(v);
        }
        TickerType::TitanBlobFileBytesRead => {
            STORE_ENGINE_BLOB_FLOW.get(name_enum).bytes_read.inc_by(v);
        }
        TickerType::TitanBlobFileSynced => STORE_ENGINE_BLOB_FILE_SYNCED.get(name_enum).inc_by(v),
        TickerType::TitanGcNumFiles => {
            STORE_ENGINE_BLOB_GC_FILE
                .get(name_enum)
                .gc_input_files_count
                .inc_by(v);
        }
        TickerType::TitanGcNumNewFiles => {
            STORE_ENGINE_BLOB_GC_FILE
                .get(name_enum)
                .gc_output_files_count
                .inc_by(v);
        }
        TickerType::TitanGcNumKeysOverwritten => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .keys_overwritten
                .inc_by(v);
        }
        TickerType::TitanGcNumKeysRelocated => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .keys_relocated
                .inc_by(v);
        }
        TickerType::TitanGcBytesOverwritten => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .bytes_overwritten
                .inc_by(v);
        }
        TickerType::TitanGcBytesRelocated => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .bytes_relocated
                .inc_by(v);
        }
        TickerType::TitanGcBytesWritten => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .bytes_written
                .inc_by(v);
        }
        TickerType::TitanGcBytesRead => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .bytes_read
                .inc_by(v);
        }
        TickerType::TitanBlobCacheHit => {
            STORE_ENGINE_BLOB_CACHE_EFFICIENCY
                .get(name_enum)
                .blob_cache_hit
                .inc_by(v);
        }
        TickerType::TitanBlobCacheMiss => {
            STORE_ENGINE_BLOB_CACHE_EFFICIENCY
                .get(name_enum)
                .blob_cache_miss
                .inc_by(v);
        }
        TickerType::TitanGcNoNeed => {
            STORE_ENGINE_BLOB_GC_ACTION.get(name_enum).no_need.inc_by(v);
        }
        TickerType::TitanGcRemain => {
            STORE_ENGINE_BLOB_GC_ACTION.get(name_enum).remain.inc_by(v);
        }
        TickerType::TitanGcDiscardable => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .discardable
                .inc_by(v);
        }
        TickerType::TitanGcSample => {
            STORE_ENGINE_BLOB_GC_ACTION.get(name_enum).sample.inc_by(v);
        }
        TickerType::TitanGcSmallFile => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .small_file
                .inc_by(v);
        }
        TickerType::TitanGcFailure => {
            STORE_ENGINE_BLOB_GC_ACTION.get(name_enum).failure.inc_by(v);
        }
        TickerType::TitanGcSuccess => {
            STORE_ENGINE_BLOB_GC_ACTION.get(name_enum).success.inc_by(v);
        }
        TickerType::TitanGcTriggerNext => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .trigger_next
                .inc_by(v);
        }
        _ => {}
    }
}

pub fn flush_engine_histogram_metrics(t: HistType, value: HistogramData, name: &str) {
    match t {
        HistType::DbGet => {
            engine_histogram_metrics!(STORE_ENGINE_GET_VEC, "get", name, value);
        }
        HistType::DbWrite => {
            engine_histogram_metrics!(STORE_ENGINE_WRITE_VEC, "write", name, value);
        }
        HistType::CompactionTime => {
            engine_histogram_metrics!(
                STORE_ENGINE_COMPACTION_TIME_VEC,
                "compaction_time",
                name,
                value
            );
        }
        HistType::TableSyncMicros => {
            engine_histogram_metrics!(STORE_ENGINE_TABLE_SYNC_VEC, "table_sync", name, value);
        }
        HistType::CompactionOutfileSyncMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_COMPACTION_OUTFILE_SYNC_VEC,
                "compaction_outfile_sync",
                name,
                value
            );
        }
        HistType::WalFileSyncMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC,
                "wal_file_sync",
                name,
                value
            );
        }
        HistType::ManifestFileSyncMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_MANIFEST_FILE_SYNC_VEC,
                "manifest_file_sync",
                name,
                value
            );
        }
        HistType::StallL0SlowdownCount => {
            engine_histogram_metrics!(
                STORE_ENGINE_STALL_L0_SLOWDOWN_COUNT_VEC,
                "stall_l0_slowdown_count",
                name,
                value
            );
        }
        HistType::StallMemtableCompactionCount => {
            engine_histogram_metrics!(
                STORE_ENGINE_STALL_MEMTABLE_COMPACTION_COUNT_VEC,
                "stall_memtable_compaction_count",
                name,
                value
            );
        }
        HistType::StallL0NumFilesCount => {
            engine_histogram_metrics!(
                STORE_ENGINE_STALL_L0_NUM_FILES_COUNT_VEC,
                "stall_l0_num_files_count",
                name,
                value
            );
        }
        HistType::HardRateLimitDelayCount => {
            engine_histogram_metrics!(
                STORE_ENGINE_HARD_RATE_LIMIT_DELAY_VEC,
                "hard_rate_limit_delay",
                name,
                value
            );
        }
        HistType::SoftRateLimitDelayCount => {
            engine_histogram_metrics!(
                STORE_ENGINE_SOFT_RATE_LIMIT_DELAY_VEC,
                "soft_rate_limit_delay",
                name,
                value
            );
        }
        HistType::NumFilesInSingleCompaction => {
            engine_histogram_metrics!(
                STORE_ENGINE_NUM_FILES_IN_SINGLE_COMPACTION_VEC,
                "num_files_in_single_compaction",
                name,
                value
            );
        }
        HistType::DbSeek => {
            engine_histogram_metrics!(STORE_ENGINE_SEEK_MICROS_VEC, "seek", name, value);
        }
        HistType::WriteStall => {
            engine_histogram_metrics!(STORE_ENGINE_WRITE_STALL_VEC, "write_stall", name, value);
        }
        HistType::SstReadMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_SST_READ_MICROS_VEC,
                "sst_read_micros",
                name,
                value
            );
        }
        HistType::NumSubcompactionsScheduled => {
            engine_histogram_metrics!(
                STORE_ENGINE_NUM_SUBCOMPACTION_SCHEDULED_VEC,
                "num_subcompaction_scheduled",
                name,
                value
            );
        }
        HistType::BytesPerRead => {
            engine_histogram_metrics!(
                STORE_ENGINE_BYTES_PER_READ_VEC,
                "bytes_per_read",
                name,
                value
            );
        }
        HistType::BytesPerWrite => {
            engine_histogram_metrics!(
                STORE_ENGINE_BYTES_PER_WRITE_VEC,
                "bytes_per_write",
                name,
                value
            );
        }
        HistType::BytesCompressed => {
            engine_histogram_metrics!(
                STORE_ENGINE_BYTES_COMPRESSED_VEC,
                "bytes_compressed",
                name,
                value
            );
        }
        HistType::BytesDecompressed => {
            engine_histogram_metrics!(
                STORE_ENGINE_BYTES_DECOMPRESSED_VEC,
                "bytes_decompressed",
                name,
                value
            );
        }
        HistType::CompressionTimesNanos => {
            engine_histogram_metrics!(
                STORE_ENGINE_COMPRESSION_TIMES_NANOS_VEC,
                "compression_time_nanos",
                name,
                value
            );
        }
        HistType::DecompressionTimesNanos => {
            engine_histogram_metrics!(
                STORE_ENGINE_DECOMPRESSION_TIMES_NANOS_VEC,
                "decompression_time_nanos",
                name,
                value
            );
        }
        HistType::DbWriteWalTime => {
            engine_histogram_metrics!(
                STORE_ENGINE_WRITE_WAL_TIME_VEC,
                "write_wal_micros",
                name,
                value
            );
        }
        HistType::TitanKeySize => {
            engine_histogram_metrics!(STORE_ENGINE_BLOB_KEY_SIZE_VEC, "blob_key_size", name, value);
        }
        HistType::TitanValueSize => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_VALUE_SIZE_VEC,
                "blob_value_size",
                name,
                value
            );
        }
        HistType::TitanGetMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_GET_MICROS_VEC,
                "blob_get_micros",
                name,
                value
            );
        }
        HistType::TitanSeekMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_SEEK_MICROS_VEC,
                "blob_seek_micros",
                name,
                value
            );
        }
        HistType::TitanNextMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_NEXT_MICROS_VEC,
                "blob_next_micros",
                name,
                value
            );
        }
        HistType::TitanPrevMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_PREV_MICROS_VEC,
                "blob_prev_micros",
                name,
                value
            );
        }
        HistType::TitanBlobFileWriteMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_FILE_WRITE_MICROS_VEC,
                "blob_file_write_micros",
                name,
                value
            );
        }
        HistType::TitanBlobFileReadMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_FILE_READ_MICROS_VEC,
                "blob_file_read_micros",
                name,
                value
            );
        }
        HistType::TitanBlobFileSyncMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_FILE_SYNC_MICROS_VEC,
                "blob_file_sync_micros",
                name,
                value
            );
        }
        HistType::TitanGcMicros => {
            engine_histogram_metrics!(
                STORE_ENGINE_BLOB_GC_MICROS_VEC,
                "blob_gc_micros",
                name,
                value
            );
        }
        HistType::TitanGcInputFileSize => {
            engine_histogram_metrics!(
                STORE_ENGINE_GC_INPUT_BLOB_FILE_SIZE_VEC,
                "blob_gc_input_file",
                name,
                value
            );
        }
        HistType::TitanGcOutputFileSize => {
            engine_histogram_metrics!(
                STORE_ENGINE_GC_OUTPUT_BLOB_FILE_SIZE_VEC,
                "blob_gc_output_file",
                name,
                value
            );
        }
        HistType::TitanIterTouchBlobFileCount => {
            engine_histogram_metrics!(
                STORE_ENGINE_ITER_TOUCH_BLOB_FILE_COUNT_VEC,
                "blob_iter_touch_blob_file_count",
                name,
                value
            );
        }
        _ => {}
    }
}

pub fn flush_engine_iostall_properties(engine: &DB, name: &str) {
    let stall_num = ROCKSDB_IOSTALL_KEY.len();
    let mut counter = vec![0; stall_num];
    for cf in engine.cf_names() {
        let handle = crate::util::get_cf_handle(engine, cf).unwrap();
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
        STORE_ENGINE_WRITE_STALL_REASON_GAUGE_VEC
            .with_label_values(&[name, ROCKSDB_IOSTALL_TYPE[i]])
            .set(counter[i]);
    }
}

pub fn flush_engine_properties(engine: &DB, name: &str, shared_block_cache: bool) {
    for cf in engine.cf_names() {
        let handle = crate::util::get_cf_handle(engine, cf).unwrap();
        // It is important to monitor each cf's size, especially the "raft" and "lock" column
        // families.
        let cf_used_size = crate::util::get_engine_cf_used_size(engine, handle);
        STORE_ENGINE_SIZE_GAUGE_VEC
            .with_label_values(&[name, cf])
            .set(cf_used_size as i64);

        if !shared_block_cache {
            let block_cache_usage = engine.get_block_cache_usage_cf(handle);
            STORE_ENGINE_BLOCK_CACHE_USAGE_GAUGE_VEC
                .with_label_values(&[name, cf])
                .set(block_cache_usage as i64);
        }

        let blob_cache_usage = engine.get_blob_cache_usage_cf(handle);
        STORE_ENGINE_BLOB_CACHE_USAGE_GAUGE_VEC
            .with_label_values(&[name, cf])
            .set(blob_cache_usage as i64);

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
                crate::util::get_engine_compression_ratio_at_level(engine, handle, level)
            {
                STORE_ENGINE_COMPRESSION_RATIO_VEC
                    .with_label_values(&[name, cf, &level.to_string()])
                    .set(v);
            }

            // Num files at levels
            if let Some(v) = crate::util::get_cf_num_files_at_level(engine, handle, level) {
                STORE_ENGINE_NUM_FILES_AT_LEVEL_VEC
                    .with_label_values(&[name, cf, &level.to_string()])
                    .set(v as i64);
            }

            // Titan Num blob files at levels
            if let Some(v) = crate::util::get_cf_num_blob_files_at_level(engine, handle, level) {
                STORE_ENGINE_TITANDB_NUM_BLOB_FILES_AT_LEVEL_VEC
                    .with_label_values(&[name, cf, &level.to_string()])
                    .set(v as i64);
            }
        }

        // Num immutable mem-table
        if let Some(v) = crate::util::get_num_immutable_mem_table(engine, handle) {
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

        // Titan blob file discardable ratio
        if let Some(v) =
            engine.get_property_int_cf(handle, ROCKSDB_TITANDB_DISCARDABLE_RATIO_LE0_FILE)
        {
            STORE_ENGINE_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, cf, "le0"])
                .set(v as i64);
        }
        if let Some(v) =
            engine.get_property_int_cf(handle, ROCKSDB_TITANDB_DISCARDABLE_RATIO_LE20_FILE)
        {
            STORE_ENGINE_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, cf, "le20"])
                .set(v as i64);
        }
        if let Some(v) =
            engine.get_property_int_cf(handle, ROCKSDB_TITANDB_DISCARDABLE_RATIO_LE50_FILE)
        {
            STORE_ENGINE_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, cf, "le50"])
                .set(v as i64);
        }
        if let Some(v) =
            engine.get_property_int_cf(handle, ROCKSDB_TITANDB_DISCARDABLE_RATIO_LE80_FILE)
        {
            STORE_ENGINE_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, cf, "le80"])
                .set(v as i64);
        }
        if let Some(v) =
            engine.get_property_int_cf(handle, ROCKSDB_TITANDB_DISCARDABLE_RATIO_LE100_FILE)
        {
            STORE_ENGINE_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, cf, "le100"])
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
        let handle = crate::util::get_cf_handle(engine, CF_DEFAULT).unwrap();
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
    pub static ref STORE_ENGINE_BLOB_CACHE_USAGE_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_blob_cache_size_bytes",
        "Usage of each column families' blob cache",
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
    pub static ref STORE_ENGINE_WRITE_STALL_REASON_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_write_stall_reason",
        "QPS of each reason which cause tikv write stall",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_TITANDB_NUM_BLOB_FILES_AT_LEVEL_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_titandb_num_blob_files_at_level",
        "Number of blob files at each level",
        &["db", "cf", "level"]
    ).unwrap();
    pub static ref STORE_ENGINE_TITANDB_LIVE_BLOB_SIZE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_titandb_live_blob_size",
        "Total titan blob value size referenced by LSM tree",
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
    pub static ref STORE_ENGINE_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_engine_titandb_blob_file_discardable_ratio",
        "Size of obsolete blob file",
        &["db", "cf", "ratio"]
    ).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::Builder;

    use engine_traits::ALL_CFS;
    use rocksdb::HistogramData;

    #[test]
    fn test_flush() {
        let dir = Builder::new().prefix("test-flush").tempdir().unwrap();
        let engine =
            crate::util::new_engine(dir.path().to_str().unwrap(), None, ALL_CFS, None).unwrap();
        for tp in ENGINE_TICKER_TYPES {
            flush_engine_ticker_metrics(*tp, 2, "kv");
        }

        for tp in ENGINE_HIST_TYPES {
            flush_engine_histogram_metrics(*tp, HistogramData::default(), "kv");
        }

        let shared_block_cache = false;
        flush_engine_properties(&engine.as_inner(), "kv", shared_block_cache);
        let handle = engine.as_inner().cf_handle("default").unwrap();
        let info = engine
            .as_inner()
            .get_map_property_cf(handle, ROCKSDB_CFSTATS);
        assert!(info.is_some());
    }
}
