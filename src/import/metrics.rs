// Copyright 2018 PingCAP, Inc.
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

use prometheus::*;

lazy_static! {
    pub static ref IMPORT_RPC_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_import_rpc_duration",
        "Bucketed histogram of import rpc duration",
        &["request", "result"],
        exponential_buckets(0.001, 2.0, 30).unwrap()
    ).unwrap();
    pub static ref IMPORT_WRITE_CHUNK_BYTES: Histogram = register_histogram!(
        "tikv_import_write_chunk_bytes",
        "Bucketed histogram of import write chunk bytes",
        exponential_buckets(1024.0, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_WRITE_CHUNK_DURATION: Histogram = register_histogram!(
        "tikv_import_write_chunk_duration",
        "Bucketed histogram of import write chunk duration",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_UPLOAD_CHUNK_BYTES: Histogram = register_histogram!(
        "tikv_import_upload_chunk_bytes",
        "Bucketed histogram of import upload chunk bytes",
        exponential_buckets(1024.0, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_UPLOAD_CHUNK_DURATION: Histogram = register_histogram!(
        "tikv_import_upload_chunk_duration",
        "Bucketed histogram of import upload chunk duration",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_RANGE_DELIVERY_DURATION: Histogram = register_histogram!(
        "tikv_import_range_delivery_duration",
        "Bucketed histogram of import range delivery duration",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_SPLIT_SST_DURATION: Histogram = register_histogram!(
        "tikv_import_split_sst_duration",
        "Bucketed histogram of import split sst duration",
        exponential_buckets(0.1, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_SST_DELIVERY_DURATION: Histogram = register_histogram!(
        "tikv_import_sst_delivery_duration",
        "Bucketed histogram of import sst delivery duration",
        exponential_buckets(0.1, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_SST_RECV_DURATION: Histogram = register_histogram!(
        "tikv_import_sst_recv_duration",
        "Bucketed histogram of import sst recv duration",
        exponential_buckets(0.1, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_SST_UPLOAD_DURATION: Histogram = register_histogram!(
        "tikv_import_sst_upload_duration",
        "Bucketed histogram of import sst upload duration",
        exponential_buckets(0.1, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_SST_INGEST_DURATION: Histogram = register_histogram!(
        "tikv_import_sst_ingest_duration",
        "Bucketed histogram of import sst ingest duration",
        exponential_buckets(0.1, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_SST_CHUNK_BYTES: Histogram = register_histogram!(
        "tikv_import_sst_chunk_bytes",
        "Bucketed histogram of sst chunk bytes",
        exponential_buckets(1024.0, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref IMPORT_EACH_PHASE: GaugeVec = register_gauge_vec!(
        "tikv_import_each_phase",
        "Import each phase duration of importer",
        &["phase"]
    ).unwrap();
    pub static ref IMPORT_STORE_SAPCE_NOT_ENOUGH_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_import_wait_store_available_count",
        "Counter of wait store available",
        &["store_id"]
    ).unwrap();
}
