// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use tikv_util::metrics::HIGH_PRIORITY_REGISTRY;

lazy_static! {
    pub static ref CHANNEL_FULL_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec_with_registry!(
            "tikv_channel_full_total",
            "Total number of channel full errors.",
            &["type"],
            HIGH_PRIORITY_REGISTRY
        )
        .unwrap();
}
