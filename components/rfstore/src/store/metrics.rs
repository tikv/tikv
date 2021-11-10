// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum RejectReason {
        store_id_mismatch,
        peer_id_mismatch,
        term_mismatch,
        lease_expire,
        no_region,
        no_lease,
        epoch,
        appiled_term,
        channel_full,
        safe_ts,
    }

    pub struct ReadRejectCounter : IntCounter {
       "reason" => RejectReason
    }
}

lazy_static! {
    pub static ref LOCAL_READ_REJECT_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_local_read_reject_total",
        "Total number of rejections from the local reader.",
        &["reason"]
    )
    .unwrap();
    pub static ref LOCAL_READ_REJECT: ReadRejectCounter =
        ReadRejectCounter::from(&LOCAL_READ_REJECT_VEC);
    pub static ref LOCAL_READ_EXECUTED_REQUESTS: IntCounter = register_int_counter!(
        "tikv_raftstore_local_read_executed_requests",
        "Total number of requests directly executed by local reader."
    )
    .unwrap();
    pub static ref LOCAL_READ_EXECUTED_CACHE_REQUESTS: IntCounter = register_int_counter!(
        "tikv_raftstore_local_read_cache_requests",
        "Total number of requests directly executed by local reader."
    )
    .unwrap();

    pub static ref RAFT_READ_INDEX_PENDING_DURATION: Histogram =
    register_histogram!(
        "tikv_raftstore_read_index_pending_duration",
        "Duration of pending read index.",
        exponential_buckets(0.001, 2.0, 20).unwrap() // max 1000s
    ).unwrap();

    pub static ref RAFT_READ_INDEX_PENDING_COUNT: IntGauge =
    register_int_gauge!(
        "tikv_raftstore_read_index_pending",
        "Pending read index count."
    ).unwrap();
}
