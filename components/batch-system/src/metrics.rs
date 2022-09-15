// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref CHANNEL_FULL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_channel_full_total",
        "Total number of channel full errors.",
        &["type"]
    )
    .unwrap();

    pub static ref ROUTER_CACHE_MISS: IntCounter = register_int_counter!(
        "tikv_router_cache_miss_total",
        "Total number of channel full errors.",
    )
    .unwrap();

    pub static ref ROUTER_MAILBOX_DURATION_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_router_mailbox_duration_secs",
            "Bucketed histogram of handle store write msg duration.",
            exponential_buckets(0.00001, 2.0, 26).unwrap()
        ).unwrap();
}
