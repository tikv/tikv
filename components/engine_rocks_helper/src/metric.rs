// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref TIKV_ROCKSDB_DAMAGED_FILES: IntGauge = register_int_gauge!(
        "tikv_rocksdb_damaged_files",
        "Number of rocksdb damaged sst files waiting for recovery"
    )
    .unwrap();
}
