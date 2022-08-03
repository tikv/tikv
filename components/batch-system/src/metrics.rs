// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum BatchActionType {
        full,
        not_full,
    }

    pub struct BatchAction : LocalIntCounter {
        "type" => BatchActionType
    }
}

lazy_static! {
    pub static ref CHANNEL_FULL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_channel_full_total",
        "Total number of channel full errors.",
        &["type"]
    )
    .unwrap();
    pub static ref BATCH_ACTION_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_batch_action_total",
        "Total number of channel full errors.",
        &["type"]
    )
    .unwrap();
    pub static ref BATCH_ACTION_COUNTER: BatchAction =
        auto_flush_from!(BATCH_ACTION_COUNTER_VEC, BatchAction);
}
