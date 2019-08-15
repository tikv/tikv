// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;

lazy_static! {
    pub static ref COPR_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_executor_count",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();
}
