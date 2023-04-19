// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref SLAB_FULL_COUNTER: IntCounter = register_int_counter!(
        "tikv_tracker_slab_full_counter",
        "Number of tracker slab insert failures because of fullness"
    )
    .unwrap();
}
