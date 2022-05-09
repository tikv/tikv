// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref TIKV_ROCKSDB_DAMAGED_FILES: IntGauge = register_int_gauge!(
        "tikv_rocksdb_damaged_files",
        "Number of rocksdb damaged sst files waiting for recovery"
    )
    .unwrap();
    pub static ref TIKV_ROCKSDB_DAMAGED_FILES_DELETED: IntCounter = register_int_counter!(
        "tikv_rocksdb_damaged_files_deleted",
        "Number of rocksdb damaged sst files deleted by sst recovery"
    )
    .unwrap();
}
