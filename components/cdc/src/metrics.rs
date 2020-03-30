// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref CDC_RESOLVED_TS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_cdc_resolved_ts",
        "Bucketed histogram of cdc resolved ts"
    )
    .unwrap();
    pub static ref CDC_SCAN_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_cdc_scan_duration_seconds",
        "Bucketed histogram of cdc async scan duration"
    )
    .unwrap();
    pub static ref CDC_MIN_TS_REGION: IntGauge = register_int_gauge!(
        "tikv_cdc_min_ts_region",
        "The region which has minimal resolved ts"
    )
    .unwrap();
    pub static ref CDC_PENDING_SCAN_BYTES: IntCounter = register_int_counter!(
        "tikv_cdc_pending_scan_bytes",
        "Bytes of pending scan result"
    )
    .unwrap();
    pub static ref CDC_PENDING_CMD_BYTES: IntCounter =
        register_int_counter!("tikv_cdc_pending_cmd_bytes", "Bytes of pending cmds").unwrap();
}
