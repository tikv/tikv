// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum PerfContextType {
        write_wal_time,
        write_delay_time,
        write_scheduling_flushes_compactions_time,
        db_condition_wait_nanos,
        write_memtable_time,
        pre_and_post_process,
        write_thread_wait,
        db_mutex_lock_nanos,
    }

    pub struct PerfContextTimeDuration : LocalHistogram {
        "type" => PerfContextType
    }
}

make_static_metric! {
    pub label_enum CFName {
        default,
        lock,
        write,
        raft
    }

    pub label_enum IngestType {
        block,
        non_block,
    }

    pub struct IngestExternalFileTimeDuration : Histogram {
        "cf" => CFName,
        "type" => IngestType
    }
}

impl From<&str> for CFName {
    fn from(s: &str) -> Self {
        match s {
            "DEFAULT" | "default" => CFName::default,
            "WRITE" | "write" => CFName::write,
            "LOCK" | "lock" => CFName::lock,
            "RAFT" | "raft" => CFName::raft,
            _ => CFName::default,
        }
    }
}

lazy_static! {
    pub static ref APPLY_PERF_CONTEXT_TIME_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_raftstore_apply_perf_context_time_duration_secs",
        "Bucketed histogram of request wait time duration.",
        &["type"],
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref STORE_PERF_CONTEXT_TIME_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_raftstore_store_perf_context_time_duration_secs",
        "Bucketed histogram of request wait time duration.",
        &["type"],
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref STORAGE_ROCKSDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_rocksdb_perf",
        "Total number of RocksDB internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();
    pub static ref COPR_ROCKSDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_rocksdb_perf",
        "Total number of RocksDB internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();
    pub static ref APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC: PerfContextTimeDuration =
        auto_flush_from!(APPLY_PERF_CONTEXT_TIME_HISTOGRAM, PerfContextTimeDuration);
    pub static ref STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC: PerfContextTimeDuration =
        auto_flush_from!(STORE_PERF_CONTEXT_TIME_HISTOGRAM, PerfContextTimeDuration);
    pub static ref INGEST_EXTERNAL_FILE_TIME_HISTOGRAM: IngestExternalFileTimeDuration =
        register_static_histogram_vec!(
            IngestExternalFileTimeDuration,
            "tikv_storage_ingest_external_file_duration_secs",
            "Bucketed histogram of ingest external file duration.",
            &["cf", "type"],
            exponential_buckets(0.005, 2.0, 20).unwrap()
        )
        .unwrap();
}
