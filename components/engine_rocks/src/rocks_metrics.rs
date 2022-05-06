// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CF_DEFAULT;
use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;
use rocksdb::{
    DBStatisticsHistogramType as HistType, DBStatisticsTickerType as TickerType, HistogramData, DB,
};

use crate::rocks_metrics_defs::*;

make_auto_flush_static_metric! {
    pub label_enum TickerName {
        kv,
        raft,
    }

    pub label_enum TickerEnum {
        block_cache_add,
        block_cache_add_failures,
        block_cache_byte_read,
        block_cache_byte_write,
        block_cache_data_add,
        block_cache_data_bytes_insert,
        block_cache_data_hit,
        block_cache_data_miss,
        block_cache_filter_add,
        block_cache_filter_bytes_evict,
        block_cache_filter_bytes_insert,
        block_cache_filter_hit,
        block_cache_filter_miss,
        block_cache_hit,
        block_cache_index_add,
        block_cache_index_bytes_evict,
        block_cache_index_bytes_insert,
        block_cache_index_hit,
        block_cache_index_miss,
        block_cache_miss,
        bloom_prefix_checked,
        bloom_prefix_useful,
        bloom_useful,
        bytes_overwritten,
        bytes_read,
        bytes_relocated,
        bytes_written,
        compaction_key_drop_newer_entry,
        compaction_key_drop_obsolete,
        compaction_key_drop_range_del,
        flush_write_bytes,
        gc_input_files_count,
        gc_output_files_count,
        get_hit_l0,
        get_hit_l1,
        get_hit_l2_and_up,
        iter_bytes_read,
        keys_overwritten,
        keys_read,
        keys_relocated,
        keys_updated,
        keys_written,
        memtable_hit,
        memtable_miss,
        no_file_closes,
        no_file_errors,
        no_file_opens,
        number_blob_get,
        number_blob_next,
        number_blob_prev,
        number_blob_seek,
        number_db_next,
        number_db_next_found,
        number_db_prev,
        number_db_prev_found,
        number_db_seek,
        number_db_seek_found,
        optimized_del_drop_obsolete,
        range_del_drop_obsolete,
        read_amp_estimate_useful_bytes,
        read_amp_total_read_bytes,
        wal_file_bytes,
        write_done_by_other,
        write_done_by_self,
        write_timeout,
        write_with_wal,
        blob_cache_hit,
        blob_cache_miss,
        no_need,
        remain,
        discardable,
        sample,
        small_file,
        failure,
        success,
        trigger_next,
    }

    pub struct EngineTickerMetrics : LocalIntCounter {
        "db" => TickerName,
        "type" => TickerEnum,
    }

    pub struct SimpleEngineTickerMetrics : LocalIntCounter {
        "db" => TickerName,
    }
}

pub fn flush_engine_ticker_metrics(t: TickerType, value: u64, name: &str) {
    let name_enum = match name {
        "kv" => TickerName::kv,
        "raft" => TickerName::raft,
        unexpected => panic!("unexpected name {}", unexpected),
    };

    match t {
        TickerType::BlockCacheMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_miss
                .inc_by(value);
        }
        TickerType::BlockCacheHit => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_hit
                .inc_by(value);
        }
        TickerType::BlockCacheAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_add
                .inc_by(value);
        }
        TickerType::BlockCacheAddFailures => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_add_failures
                .inc_by(value);
        }
        TickerType::BlockCacheIndexMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_miss
                .inc_by(value);
        }
        TickerType::BlockCacheIndexHit => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_hit
                .inc_by(value);
        }
        TickerType::BlockCacheIndexAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_add
                .inc_by(value);
        }
        TickerType::BlockCacheIndexBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_bytes_insert
                .inc_by(value);
        }
        TickerType::BlockCacheIndexBytesEvict => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_bytes_evict
                .inc_by(value);
        }
        TickerType::BlockCacheFilterMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_miss
                .inc_by(value);
        }
        TickerType::BlockCacheFilterHit => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_hit
                .inc_by(value);
        }
        TickerType::BlockCacheFilterAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_add
                .inc_by(value);
        }
        TickerType::BlockCacheFilterBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_bytes_insert
                .inc_by(value);
        }
        TickerType::BlockCacheFilterBytesEvict => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_bytes_evict
                .inc_by(value);
        }
        TickerType::BlockCacheDataMiss => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_miss
                .inc_by(value);
        }
        TickerType::BlockCacheDataHit => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_hit
                .inc_by(value);
        }
        TickerType::BlockCacheDataAdd => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_add
                .inc_by(value);
        }
        TickerType::BlockCacheDataBytesInsert => {
            STORE_ENGINE_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_bytes_insert
                .inc_by(value);
        }
        TickerType::BlockCacheBytesRead => {
            STORE_ENGINE_FLOW
                .get(name_enum)
                .block_cache_byte_read
                .inc_by(value);
        }
        TickerType::BlockCacheBytesWrite => {
            STORE_ENGINE_FLOW
                .get(name_enum)
                .block_cache_byte_write
                .inc_by(value);
        }
        TickerType::BloomFilterUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_useful
                .inc_by(value);
        }
        TickerType::MemtableHit => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY
                .get(name_enum)
                .memtable_hit
                .inc_by(value);
        }
        TickerType::MemtableMiss => {
            STORE_ENGINE_MEMTABLE_EFFICIENCY
                .get(name_enum)
                .memtable_miss
                .inc_by(value);
        }
        TickerType::GetHitL0 => {
            STORE_ENGINE_GET_SERVED
                .get(name_enum)
                .get_hit_l0
                .inc_by(value);
        }
        TickerType::GetHitL1 => {
            STORE_ENGINE_GET_SERVED
                .get(name_enum)
                .get_hit_l1
                .inc_by(value);
        }
        TickerType::GetHitL2AndUp => {
            STORE_ENGINE_GET_SERVED
                .get(name_enum)
                .get_hit_l2_and_up
                .inc_by(value);
        }
        TickerType::CompactionKeyDropNewerEntry => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .compaction_key_drop_newer_entry
                .inc_by(value);
        }
        TickerType::CompactionKeyDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .compaction_key_drop_obsolete
                .inc_by(value);
        }
        TickerType::CompactionKeyDropRangeDel => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .compaction_key_drop_range_del
                .inc_by(value);
        }
        TickerType::CompactionRangeDelDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .range_del_drop_obsolete
                .inc_by(value);
        }
        TickerType::CompactionOptimizedDelDropObsolete => {
            STORE_ENGINE_COMPACTION_DROP
                .get(name_enum)
                .optimized_del_drop_obsolete
                .inc_by(value);
        }
        TickerType::NumberKeysWritten => {
            STORE_ENGINE_FLOW.get(name_enum).keys_written.inc_by(value);
        }
        TickerType::NumberKeysRead => {
            STORE_ENGINE_FLOW.get(name_enum).keys_read.inc_by(value);
        }
        TickerType::NumberKeysUpdated => {
            STORE_ENGINE_FLOW.get(name_enum).keys_updated.inc_by(value);
        }
        TickerType::BytesWritten => {
            STORE_ENGINE_FLOW.get(name_enum).bytes_written.inc_by(value);
        }
        TickerType::BytesRead => {
            STORE_ENGINE_FLOW.get(name_enum).bytes_read.inc_by(value);
        }
        TickerType::NumberDbSeek => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_seek
                .inc_by(value);
        }
        TickerType::NumberDbNext => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_next
                .inc_by(value);
        }
        TickerType::NumberDbPrev => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_prev
                .inc_by(value);
        }
        TickerType::NumberDbSeekFound => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_seek_found
                .inc_by(value);
        }
        TickerType::NumberDbNextFound => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_next_found
                .inc_by(value);
        }
        TickerType::NumberDbPrevFound => {
            STORE_ENGINE_LOCATE
                .get(name_enum)
                .number_db_prev_found
                .inc_by(value);
        }
        TickerType::IterBytesRead => {
            STORE_ENGINE_FLOW
                .get(name_enum)
                .iter_bytes_read
                .inc_by(value);
        }
        TickerType::NoFileCloses => {
            STORE_ENGINE_FILE_STATUS
                .get(name_enum)
                .no_file_closes
                .inc_by(value);
        }
        TickerType::NoFileOpens => {
            STORE_ENGINE_FILE_STATUS
                .get(name_enum)
                .no_file_opens
                .inc_by(value);
        }
        TickerType::NoFileErrors => {
            STORE_ENGINE_FILE_STATUS
                .get(name_enum)
                .no_file_errors
                .inc_by(value);
        }
        TickerType::StallMicros => {
            STORE_ENGINE_STALL_MICROS.get(name_enum).inc_by(value);
        }
        TickerType::BloomFilterPrefixChecked => {
            STORE_ENGINE_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_prefix_checked
                .inc_by(value);
        }
        TickerType::BloomFilterPrefixUseful => {
            STORE_ENGINE_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_prefix_useful
                .inc_by(value);
        }
        TickerType::WalFileSynced => {
            STORE_ENGINE_WAL_FILE_SYNCED.get(name_enum).inc_by(value);
        }
        TickerType::WalFileBytes => {
            STORE_ENGINE_FLOW
                .get(name_enum)
                .wal_file_bytes
                .inc_by(value);
        }
        TickerType::WriteDoneBySelf => {
            STORE_ENGINE_WRITE_SERVED
                .get(name_enum)
                .write_done_by_self
                .inc_by(value);
        }
        TickerType::WriteDoneByOther => {
            STORE_ENGINE_WRITE_SERVED
                .get(name_enum)
                .write_done_by_other
                .inc_by(value);
        }
        TickerType::WriteTimedout => {
            STORE_ENGINE_WRITE_SERVED
                .get(name_enum)
                .write_timeout
                .inc_by(value);
        }
        TickerType::WriteWithWal => {
            STORE_ENGINE_WRITE_SERVED
                .get(name_enum)
                .write_with_wal
                .inc_by(value);
        }
        TickerType::CompactReadBytes => {
            STORE_ENGINE_COMPACTION_FLOW
                .get(name_enum)
                .bytes_read
                .inc_by(value);
        }
        TickerType::CompactWriteBytes => {
            STORE_ENGINE_COMPACTION_FLOW
                .get(name_enum)
                .bytes_written
                .inc_by(value);
        }
        TickerType::FlushWriteBytes => {
            STORE_ENGINE_FLOW
                .get(name_enum)
                .flush_write_bytes
                .inc_by(value);
        }
        TickerType::ReadAmpEstimateUsefulBytes => {
            STORE_ENGINE_READ_AMP_FLOW
                .get(name_enum)
                .read_amp_estimate_useful_bytes
                .inc_by(value);
        }
        TickerType::ReadAmpTotalReadBytes => {
            STORE_ENGINE_READ_AMP_FLOW
                .get(name_enum)
                .read_amp_total_read_bytes
                .inc_by(value);
        }
        TickerType::TitanNumGet => {
            STORE_ENGINE_BLOB_LOCATE
                .get(name_enum)
                .number_blob_get
                .inc_by(value);
        }
        TickerType::TitanNumSeek => {
            STORE_ENGINE_BLOB_LOCATE
                .get(name_enum)
                .number_blob_seek
                .inc_by(value);
        }
        TickerType::TitanNumNext => {
            STORE_ENGINE_BLOB_LOCATE
                .get(name_enum)
                .number_blob_next
                .inc_by(value);
        }
        TickerType::TitanNumPrev => {
            STORE_ENGINE_BLOB_LOCATE
                .get(name_enum)
                .number_blob_prev
                .inc_by(value);
        }
        TickerType::TitanBlobFileNumKeysWritten => {
            STORE_ENGINE_BLOB_FLOW
                .get(name_enum)
                .keys_written
                .inc_by(value);
        }
        TickerType::TitanBlobFileNumKeysRead => {
            STORE_ENGINE_BLOB_FLOW
                .get(name_enum)
                .keys_read
                .inc_by(value);
        }
        TickerType::TitanBlobFileBytesWritten => {
            STORE_ENGINE_BLOB_FLOW
                .get(name_enum)
                .bytes_written
                .inc_by(value);
        }
        TickerType::TitanBlobFileBytesRead => {
            STORE_ENGINE_BLOB_FLOW
                .get(name_enum)
                .bytes_read
                .inc_by(value);
        }
        TickerType::TitanBlobFileSynced => {
            STORE_ENGINE_BLOB_FILE_SYNCED.get(name_enum).inc_by(value)
        }
        TickerType::TitanGcNumFiles => {
            STORE_ENGINE_BLOB_GC_FILE
                .get(name_enum)
                .gc_input_files_count
                .inc_by(value);
        }
        TickerType::TitanGcNumNewFiles => {
            STORE_ENGINE_BLOB_GC_FILE
                .get(name_enum)
                .gc_output_files_count
                .inc_by(value);
        }
        TickerType::TitanGcNumKeysOverwritten => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .keys_overwritten
                .inc_by(value);
        }
        TickerType::TitanGcNumKeysRelocated => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .keys_relocated
                .inc_by(value);
        }
        TickerType::TitanGcBytesOverwritten => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .bytes_overwritten
                .inc_by(value);
        }
        TickerType::TitanGcBytesRelocated => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .bytes_relocated
                .inc_by(value);
        }
        TickerType::TitanGcBytesWritten => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .bytes_written
                .inc_by(value);
        }
        TickerType::TitanGcBytesRead => {
            STORE_ENGINE_BLOB_GC_FLOW
                .get(name_enum)
                .bytes_read
                .inc_by(value);
        }
        TickerType::TitanBlobCacheHit => {
            STORE_ENGINE_BLOB_CACHE_EFFICIENCY
                .get(name_enum)
                .blob_cache_hit
                .inc_by(value);
        }
        TickerType::TitanBlobCacheMiss => {
            STORE_ENGINE_BLOB_CACHE_EFFICIENCY
                .get(name_enum)
                .blob_cache_miss
                .inc_by(value);
        }
        TickerType::TitanGcNoNeed => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .no_need
                .inc_by(value);
        }
        TickerType::TitanGcRemain => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .remain
                .inc_by(value);
        }
        TickerType::TitanGcDiscardable => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .discardable
                .inc_by(value);
        }
        TickerType::TitanGcSample => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .sample
                .inc_by(value);
        }
        TickerType::TitanGcSmallFile => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .small_file
                .inc_by(value);
        }
        TickerType::TitanGcFailure => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .failure
                .inc_by(value);
        }
        TickerType::TitanGcSuccess => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .success
                .inc_by(value);
        }
        TickerType::TitanGcTriggerNext => {
            STORE_ENGINE_BLOB_GC_ACTION
                .get(name_enum)
                .trigger_next
                .inc_by(value);
        }
        _ => {}
    }
}

macro_rules! engine_histogram_metrics {
    ($metric:ident, $prefix:expr, $db:expr, $value:expr) => {
        $metric
            .with_label_values(&[$db, concat!($prefix, "_median")])
            .set($value.median);
        $metric
            .with_label_values(&[$db, concat!($prefix, "_percentile95")])
            .set($value.percentile95);
        $metric
            .with_label_values(&[$db, concat!($prefix, "_percentile99")])
            .set($value.percentile99);
        $metric
            .with_label_values(&[$db, concat!($prefix, "_average")])
            .set($value.average);
        $metric
            .with_label_values(&[$db, concat!($prefix, "_standard_deviation")])
            .set($value.standard_deviation);
        $metric
            .with_label_values(&[$db, concat!($prefix, "_max")])
            .set($value.max);
    };
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
            crate::util::get_cf_pending_compaction_bytes(engine, handle)
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
        if let Some(v) = crate::util::get_cf_num_immutable_mem_table(engine, handle) {
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

// For ticker type
#[rustfmt::skip]
lazy_static! {
    pub static ref STORE_ENGINE_CACHE_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_cache_efficiency",
        "Efficiency of rocksdb's block cache",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_CACHE_EFFICIENCY: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_CACHE_EFFICIENCY_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_memtable_efficiency",
        "Hit and miss of memtable",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_MEMTABLE_EFFICIENCY: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_MEMTABLE_EFFICIENCY_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_GET_SERVED_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_get_served",
        "Get queries served by engine",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_GET_SERVED: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_GET_SERVED_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_WRITE_SERVED_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_write_served",
        "Write queries served by engine",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_WRITE_SERVED: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_WRITE_SERVED_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_BLOOM_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_bloom_efficiency",
        "Efficiency of rocksdb's bloom filter",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOOM_EFFICIENCY: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_BLOOM_EFFICIENCY_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_flow_bytes",
        "Bytes and keys of read/written",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_FLOW: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_FLOW_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_STALL_MICROS_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_stall_micro_seconds",
        "Stall micros",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_STALL_MICROS: SimpleEngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_STALL_MICROS_VEC, SimpleEngineTickerMetrics);

    pub static ref STORE_ENGINE_COMPACTION_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_compaction_flow_bytes",
        "Bytes of read/written during compaction",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_FLOW: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_COMPACTION_FLOW_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_COMPACTION_DROP_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_compaction_key_drop",
        "Count the reasons for key drop during compaction",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_COMPACTION_DROP: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_COMPACTION_DROP_VEC, EngineTickerMetrics);

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
    pub static ref STORE_ENGINE_INGESTION_PICKED_LEVEL_VEC: HistogramVec = register_histogram_vec!(
        "tikv_engine_ingestion_picked_level",
        "Histogram of ingestion picked level",
        &["db", "cf"],
        linear_buckets(0.0, 1.0, 7).unwrap()
    ).unwrap();
    pub static ref STORE_ENGINE_LOCATE_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_locate",
        "Number of calls to seek/next/prev",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_LOCATE: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_LOCATE_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_FILE_STATUS_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_file_status",
        "Number of different status of files",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_FILE_STATUS: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_FILE_STATUS_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_READ_AMP_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_read_amp_flow_bytes",
        "Bytes of read amplification",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_READ_AMP_FLOW: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_READ_AMP_FLOW_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_NO_ITERATORS: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_no_iterator",
        "Number of iterators currently open",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_WAL_FILE_SYNCED_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_wal_file_synced",
        "Number of times WAL sync is done",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_WAL_FILE_SYNCED: SimpleEngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_WAL_FILE_SYNCED_VEC, SimpleEngineTickerMetrics);

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
        "Number of calls to titan blob seek/next/prev",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_LOCATE: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_BLOB_LOCATE_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_BLOB_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_flow_bytes",
        "Bytes and keys of titan blob read/written",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FLOW: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_BLOB_FLOW_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_BLOB_GC_FLOW_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_gc_flow_bytes",
        "Bytes and keys of titan blob gc read/written",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_GC_FLOW: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_BLOB_GC_FLOW_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_BLOB_GC_FILE_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_gc_file_count",
        "Number of blob file involved in titan blob gc",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_GC_FILE: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_BLOB_GC_FILE_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_BLOB_GC_ACTION_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_gc_action_count",
        "Number of actions of titan gc",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_GC_ACTION: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_BLOB_GC_ACTION_VEC, EngineTickerMetrics);

    pub static ref STORE_ENGINE_BLOB_FILE_SYNCED_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_file_synced",
        "Number of times titan blob file sync is done",
        &["db"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FILE_SYNCED: SimpleEngineTickerMetrics = 
        auto_flush_from!(STORE_ENGINE_BLOB_FILE_SYNCED_VEC, SimpleEngineTickerMetrics); 
    
    pub static ref STORE_ENGINE_BLOB_CACHE_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_engine_blob_cache_efficiency",
        "Efficiency of titan's blob cache",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_CACHE_EFFICIENCY: EngineTickerMetrics =
        auto_flush_from!(STORE_ENGINE_BLOB_CACHE_EFFICIENCY_VEC, EngineTickerMetrics);
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
    pub static ref STORE_ENGINE_WRITE_WAL_TIME_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_write_wal_time_micro_seconds",
        "Histogram of write wal micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_KEY_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_key_size",
        "Histogram of titan blob key size",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_VALUE_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_value_size",
        "Histogram of titan blob value size",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_GET_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_get_micros_seconds",
        "Histogram of titan blob read micros for calling get",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_SEEK_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_seek_micros_seconds",
        "Histogram of titan blob read micros for calling seek",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_NEXT_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_next_micros_seconds",
        "Histogram of titan blob read micros for calling next",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_PREV_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_prev_micros_seconds",
        "Histogram of titan blob read micros for calling prev",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FILE_WRITE_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_file_write_micros_seconds",
        "Histogram of titan blob file write micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FILE_READ_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_file_read_micros_seconds",
        "Histogram of titan blob file read micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_FILE_SYNC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_file_sync_micros_seconds",
        "Histogram of titan blob file sync micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_BLOB_GC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_gc_micros_seconds",
        "Histogram of titan blob gc micros",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_GC_INPUT_BLOB_FILE_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_gc_input_file",
        "Histogram of titan blob gc input file size",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_GC_OUTPUT_BLOB_FILE_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_gc_output_file",
        "Histogram of titan blob gc output file size",
        &["db", "type"]
    ).unwrap();
    pub static ref STORE_ENGINE_ITER_TOUCH_BLOB_FILE_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "tikv_engine_blob_iter_touch_blob_file_count",
        "Histogram of titan iter touched blob file count",
        &["db", "type"]
    ).unwrap();
}

#[cfg(test)]
mod tests {
    use engine_traits::ALL_CFS;
    use rocksdb::HistogramData;
    use tempfile::Builder;

    use super::*;

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
        flush_engine_properties(engine.as_inner(), "kv", shared_block_cache);
        let handle = engine.as_inner().cf_handle("default").unwrap();
        let info = engine
            .as_inner()
            .get_map_property_cf(handle, ROCKSDB_CFSTATS);
        assert!(info.is_some());
    }
}
