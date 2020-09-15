// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref SKIPLIST_ACTION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_skiplist_action_duration_seconds",
        "Bucketed histogram of skiplist actions",
        &["db", "type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SKIPLIST_WRITE_SIZE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_skiplist_write_size",
        "Bucketed histogram of skiplist write batch size",
        &["db"],
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SKIPLIST_WRITE_KEYS_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_skiplist_write_keys",
        "Bucketed histogram of skiplist write batch keys",
        &["db"],
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
}
