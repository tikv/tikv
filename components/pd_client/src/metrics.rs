// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum PDRequestEventType {
        get_region,
        get_region_by_id,
        get_region_leader_by_id,
        scatter_region,
        get_store,
        get_store_async,
        put_store,
        get_all_stores,
        get_store_and_stats,
        store_global_config,
        load_global_config,
        watch_global_config,
        bootstrap_cluster,
        is_cluster_bootstrapped,
        get_cluster_config,
        ask_split,
        ask_batch_split,
        report_batch_split,
        get_gc_safe_point,
        update_service_safe_point,
        min_resolved_ts,
        get_operator,
        alloc_id,
        is_recovering_marked,
        store_heartbeat,
        tso,
        scan_regions,
        get_members,

        meta_storage_put,
        meta_storage_get,
        meta_storage_watch,
    }

    pub label_enum PDReconnectEventKind {
        success,
        failure,
        no_need,
        cancel,
        try_connect,
    }

    pub struct PDRequestEventHistogramVec: Histogram {
        "type" => PDRequestEventType,
    }
    pub struct PDReconnectEventCounterVec: IntCounter {
        "type" => PDReconnectEventKind,
    }
}

lazy_static! {
    pub static ref PD_REQUEST_HISTOGRAM_VEC: PDRequestEventHistogramVec =
        register_static_histogram_vec!(
            PDRequestEventHistogramVec,
            "tikv_pd_request_duration_seconds",
            "Bucketed histogram of PD requests duration",
            &["type"]
        )
        .unwrap();
    pub static ref PD_HEARTBEAT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_pd_heartbeat_message_total",
        "Total number of PD heartbeat messages.",
        &["type"]
    )
    .unwrap();
    pub static ref PD_BUCKETS_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_pd_buckets_message_total",
        "Total number of PD buckets messages.",
        &["type"]
    )
    .unwrap();
    pub static ref PD_RECONNECT_COUNTER_VEC: PDReconnectEventCounterVec =
        register_static_int_counter_vec!(
            PDReconnectEventCounterVec,
            "tikv_pd_reconnect_total",
            "Total number of PD reconnections.",
            &["type"]
        )
        .unwrap();
    pub static ref PD_PENDING_HEARTBEAT_GAUGE: IntGauge = register_int_gauge!(
        "tikv_pd_pending_heartbeat_total",
        "Total number of pending region heartbeat"
    )
    .unwrap();
    pub static ref PD_PENDING_BUCKETS_GAUGE: IntGauge = register_int_gauge!(
        "tikv_pd_pending_buckets_total",
        "Total number of pending region buckets"
    )
    .unwrap();
    pub static ref PD_VALIDATE_PEER_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_pd_validate_peer_total",
        "Total number of pd worker validate peer task.",
        &["type"]
    )
    .unwrap();
    pub static ref STORE_SIZE_GAUGE_VEC: IntGaugeVec =
        register_int_gauge_vec!("tikv_store_size_bytes", "Size of storage.", &["type"]).unwrap();
    pub static ref REGION_READ_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_region_read_keys",
        "Histogram of keys written for regions",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_READ_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "tikv_region_read_bytes",
        "Histogram of bytes written for regions",
        exponential_buckets(256.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_WRITTEN_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "tikv_region_written_bytes",
        "Histogram of bytes written for regions",
        exponential_buckets(256.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_WRITTEN_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_region_written_keys",
        "Histogram of keys written for regions",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REQUEST_FORWARDED_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_pd_request_forwarded",
        "The status to indicate if the request is forwarded",
        &["host"]
    )
    .unwrap();
    pub static ref PD_PENDING_TSO_REQUEST_GAUGE: IntGauge = register_int_gauge!(
        "tikv_pd_pending_tso_request_total",
        "Total number of pending tso requests"
    )
    .unwrap();
}
