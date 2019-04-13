// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::{exponential_buckets, Histogram, IntGaugeVec};

lazy_static! {
    pub static ref REGION_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_region_size",
        "Bucketed histogram of approximate region size.",
        exponential_buckets(1024.0 * 1024.0, 2.0, 20).unwrap() // max bucket would be 512GB
    ).unwrap();

    pub static ref REGION_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_region_keys",
        "Bucketed histogram of approximate region keys.",
        exponential_buckets(1.0, 2.0, 30).unwrap()
    ).unwrap();

    pub static ref REGION_COUNT_GAUGE_VEC: IntGaugeVec =
    register_int_gauge_vec!(
        "tikv_raftstore_region_count",
        "Number of regions collected in region_collector",
        &["type"]
    ).unwrap();
}
