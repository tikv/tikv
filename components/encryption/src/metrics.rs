// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;

lazy_static::lazy_static! {
    pub static ref ENCRYPTION_DATA_KEY_GAUGE: IntGauge = register_int_gauge!(
        "tikv_encryption_data_key_storage_total",
        "Total number of ecryption data keys in use"
    ).unwrap();
    pub static ref ENCRYPTION_FILE_NUM_GAUGE: IntGauge = register_int_gauge!(
        "tikv_encryption_file_num",
        "Number of files being encrypted"
    ).unwrap();
    pub static ref ENCRYPTION_INITIALIZED_GAUGE: IntGauge = register_int_gauge!(
        "tikv_encryption_is_initialized",
        "Flag to indicate if KeyDictionary encryption is initialized"
    ).unwrap();
    pub static ref ENCRYPT_DECRPTION_FILE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_encryption_write_read_file_duration_seconds",
        "Histogram of writing or reading file duration",
        &["type", "operation"],
        exponential_buckets(0.001, 2.0, 16).unwrap() // Up to 65.5 seconds
    ).unwrap();
    pub static ref ENCRYPTION_FILE_SIZE_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_encryption_meta_file_size_bytes",
        "Total size of ecryption meta files",
        &["name"]
    ).unwrap();
}
