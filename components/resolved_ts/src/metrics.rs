// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref RESOLVED_TS_ADVANCE_METHOD: IntGauge = register_int_gauge!(
        "tikv_resolved_ts_advance_method",
        "Resolved Ts advance method, 0 = advanced through raft command, 1 = advanced through store RPC"
    )
    .unwrap();
}
