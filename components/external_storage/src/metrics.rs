// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref EXT_STORAGE_CREATE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_external_storage_create_seconds",
        "Bucketed histogram of creating external storage duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();

    pub static ref LOCAL_STORAGE_NUM_GAUGE: IntCounterVec = register_int_counter_vec!(
        "tikv_localstorage_cache_num",
        "Number of local storage being cached",
        &["type"]
    )
    .unwrap();
}
