// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;

lazy_static! {
    pub static ref IMPORT_RPC_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_import_rpc_duration",
        "Bucketed histogram of import rpc duration",
        &["request", "result"],
        // Start from 10ms.
        exponential_buckets(0.01, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORT_RPC_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "tikv_import_rpc_count",
        "Total number of import rpc",
        &["type"],
    )
    .unwrap();
    pub static ref IMPORT_UPLOAD_CHUNK_BYTES: Histogram = register_histogram!(
        "tikv_import_upload_chunk_bytes",
        "Bucketed histogram of import upload chunk bytes",
        exponential_buckets(1024.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORT_UPLOAD_CHUNK_DURATION: Histogram = register_histogram!(
        "tikv_import_upload_chunk_duration",
        "Bucketed histogram of import upload chunk duration",
        // Start from 10ms.
        exponential_buckets(0.01, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORT_LOCAL_WRITE_CHUNK_DURATION_VEC: HistogramVec = register_histogram_vec!(
        "tikv_import_local_write_chunk_duration",
        "Bucketed histogram of local backend write chunk duration",
        &["type"],
        // Start from 10ms.
        exponential_buckets(0.01, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORT_LOCAL_WRITE_BYTES_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_import_local_write_bytes",
        "Number of bytes written from local backend",
        &["type"]
    )
    .unwrap();
    pub static ref IMPORT_LOCAL_WRITE_KEYS_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_import_local_write_keys",
        "Number of keys written from local backend",
        &["type"]
    )
    .unwrap();
    pub static ref IMPORTER_DOWNLOAD_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_import_download_duration",
        "Bucketed histogram of importer download duration",
        &["type"],
        // Start from 10ms.
        exponential_buckets(0.01, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORTER_DOWNLOAD_BYTES: Histogram = register_histogram!(
        "tikv_import_download_bytes",
        "Bucketed histogram of importer download bytes",
        exponential_buckets(16.0, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORTER_APPLY_BYTES: Histogram = register_histogram!(
        "tikv_import_apply_bytes",
        "Bucketed histogram of importer apply bytes",
        exponential_buckets(16.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORTER_INGEST_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_import_ingest_duration",
        "Bucketed histogram of importer ingest duration",
        &["type"],
        // Start from 10ms.
        exponential_buckets(0.01, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IMPORTER_INGEST_BYTES: Histogram = register_histogram!(
        "tikv_import_ingest_bytes",
        "Bucketed histogram of importer ingest bytes",
        exponential_buckets(1024.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref INPORTER_INGEST_COUNT: Histogram = register_histogram!(
        "tikv_import_ingest_count",
        "Bucketed histogram of importer ingest count",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORTER_ERROR_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_import_error_counter",
        "Total number of importer errors",
        &["type", "error"]
    )
    .unwrap();
    pub static ref IMPORTER_APPLY_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_import_apply_duration",
        "Bucketed histogram of importer apply duration",
        &["type"],
        // Start from 10ms.
        exponential_buckets(0.01, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref INPORTER_APPLY_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_import_apply_count",
        "Bucketed histogram of importer apply count",
        &["type"]
    ).unwrap();
    pub static ref EXT_STORAGE_CACHE_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_import_storage_cache",
        "The operations over storage cache",
        &["operation"]
    ).unwrap();

    pub static ref CACHED_FILE_IN_MEM: IntGauge = register_int_gauge!(
        "tikv_import_apply_cached_bytes",
        "The files cached by the apply requests of importer."
    ).unwrap();
    pub static ref CACHE_EVENT: IntCounterVec = register_int_counter_vec!(
        "tikv_import_apply_cache_event",
        "The events of caching. event = {add, remove, out-of-quota, hit}",
        &["type"]
    ).unwrap();
    pub static ref APPLIER_EVENT: IntCounterVec = register_int_counter_vec!(
        "tikv_import_applier_event",
        "The events of applier event.",
        &["type"]
    ).unwrap();
    pub static ref APPLIER_ENGINE_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_import_engine_request",
        "The request lifetime track of requesting the RaftKv.",
        &["type"],
        exponential_buckets(0.01, 4.0, 8).unwrap()
    ).unwrap();
}
