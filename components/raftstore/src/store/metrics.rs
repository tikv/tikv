// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum RaftEventDurationType {
        compact_check,
        pd_store_heartbeat,
        snap_gc,
        compact_lock_cf,
        consistency_check,
        cleanup_import_sst,
        raft_engine_purge,
        peer_msg,
        destroy_peer,
        split_region,
        peer_gc_snap,
        merge_result,
        raft_message,
        raft_command,
        tick,
        apply_res,
        significant_msg,
        start,
        noop,
        casual_message,
        heartbeat_pd,
    }

    pub struct RaftEventDurationVec: Histogram {
        "type" => RaftEventDurationType,
    }
}

lazy_static! {
    pub static ref PEER_PROPOSAL_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_proposal_total",
            "Total number of proposal made.",
            &["type"]
        ).unwrap();

    pub static ref PEER_ADMIN_CMD_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_admin_cmd_total",
            "Total number of admin cmd processed.",
            &["type", "status"]
        ).unwrap();

    pub static ref PEER_APPEND_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_append_log_duration_seconds",
            "Bucketed histogram of peer appending log duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_COMMIT_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_commit_log_duration_seconds",
            "Bucketed histogram of peer commits logs duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_APPLY_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_log_duration_seconds",
            "Bucketed histogram of peer applying log duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref APPLY_TASK_WAIT_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_wait_time_duration_secs",
            "Bucketed histogram of apply task wait time duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_RAFT_READY_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_ready_handled_total",
            "Total number of raft ready handled.",
            &["type"]
        ).unwrap();

    pub static ref STORE_RAFT_SENT_MESSAGE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_sent_message_total",
            "Total number of raft ready sent messages.",
            &["type"]
        ).unwrap();

    pub static ref STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_dropped_message_total",
            "Total number of raft dropped messages.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC: IntGaugeVec =
        register_int_gauge_vec!(
            "tikv_raftstore_snapshot_traffic_total",
            "Total number of raftstore snapshot traffic.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_snapshot_validation_failure_total",
            "Total number of raftstore snapshot validation failure.",
            &["type"]
        ).unwrap();

    pub static ref PEER_RAFT_PROCESS_DURATION: HistogramVec =
        register_histogram_vec!(
            "tikv_raftstore_raft_process_duration_secs",
            "Bucketed histogram of peer processing raft duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_PROPOSE_LOG_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_propose_log_size",
            "Bucketed histogram of peer proposing log size",
            vec![256.0, 512.0, 1024.0, 4096.0, 65536.0, 262144.0, 524288.0, 1048576.0,
                    2097152.0, 4194304.0, 8388608.0, 16777216.0]
        ).unwrap();

    pub static ref REGION_HASH_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_hash_total",
            "Total number of hash has been computed.",
            &["type", "result"]
        ).unwrap();

    pub static ref REGION_MAX_LOG_LAG: Histogram =
        register_histogram!(
            "tikv_raftstore_log_lag",
            "Bucketed histogram of log lag in a region",
            vec![2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0,
                    512.0, 1024.0, 5120.0, 10240.0]
        ).unwrap();

    pub static ref REQUEST_WAIT_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_request_wait_time_duration_secs",
            "Bucketed histogram of request wait time duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_GC_RAFT_LOG_COUNTER: IntCounter =
        register_int_counter!(
            "tikv_raftstore_gc_raft_log_total",
            "Total number of GC raft log."
        ).unwrap();

    pub static ref UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER: IntCounter =
        register_int_counter!(
            "update_region_size_count_by_compaction",
            "Total number of update region size caused by compaction."
        ).unwrap();

    pub static ref COMPACTION_RELATED_REGION_COUNT: HistogramVec =
        register_histogram_vec!(
            "compaction_related_region_count",
            "Associated number of regions for each compaction job",
            &["output_level"],
            exponential_buckets(1.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref COMPACTION_DECLINED_BYTES: HistogramVec =
        register_histogram_vec!(
            "compaction_declined_bytes",
            "total bytes declined for each compaction job",
            &["output_level"],
            exponential_buckets(1024.0, 2.0, 30).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_CF_KV_COUNT: HistogramVec =
        register_histogram_vec!(
            "tikv_snapshot_cf_kv_count",
            "Total number of kv in each cf file of snapshot",
            &["type"],
            exponential_buckets(100.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_CF_SIZE: HistogramVec =
        register_histogram_vec!(
            "tikv_snapshot_cf_size",
            "Total size of each cf file of snapshot",
            &["type"],
            exponential_buckets(1024.0, 2.0, 31).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_BUILD_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_snapshot_build_time_duration_secs",
            "Bucketed histogram of snapshot build time duration.",
            exponential_buckets(0.05, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_KV_COUNT_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_snapshot_kv_count",
            "Total number of kv in snapshot",
            exponential_buckets(100.0, 2.0, 20).unwrap() //100,100*2^1,...100M
        ).unwrap();

    pub static ref SNAPSHOT_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_snapshot_size",
            "Size of snapshot",
            exponential_buckets(1024.0, 2.0, 22).unwrap() // 1024,1024*2^1,..,4G
        ).unwrap();

    pub static ref RAFT_ENTRY_FETCHES: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_entry_fetches",
            "Total number of raft entry fetches",
            &["type"]
        ).unwrap();

    pub static ref LEADER_MISSING: IntGauge =
        register_int_gauge!(
            "tikv_raftstore_leader_missing",
            "Total number of leader missed region"
        ).unwrap();

    pub static ref INGEST_SST_DURATION_SECONDS: Histogram =
        register_histogram!(
            "tikv_snapshot_ingest_sst_duration_seconds",
            "Bucketed histogram of rocksdb ingestion durations",
            exponential_buckets(0.005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref RAFT_INVALID_PROPOSAL_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_invalid_proposal_total",
            "Total number of raft invalid proposal.",
            &["type"]
        ).unwrap();

    pub static ref RAFT_EVENT_DURATION_VEC: HistogramVec = register_histogram_vec!(
            "tikv_raftstore_event_duration",
            "Duration of raft store events.",
            &["type"],
            exponential_buckets(0.001, 1.59, 20).unwrap() // max 10s
        ).unwrap();
    pub static ref RAFT_EVENT_DURATION_VEC_STATIC: RaftEventDurationVec =
        RaftEventDurationVec::from(&RAFT_EVENT_DURATION_VEC);

    pub static ref RAFT_READ_INDEX_PENDING_DURATION: Histogram =
        register_histogram!(
            "tikv_raftstore_read_index_pending_duration",
            "Duration of pending read index",
            exponential_buckets(0.001, 2.0, 20).unwrap() // max 1000s
        ).unwrap();

    pub static ref RAFT_READ_INDEX_PENDING_COUNT: IntGauge =
        register_int_gauge!(
            "tikv_raftstore_read_index_pending",
            "pending read index count"
        ).unwrap();

    pub static ref APPLY_PERF_CONTEXT_TIME_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "tikv_raftstore_apply_perf_context_time_duration_secs",
            "Bucketed histogram of request wait time duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_PERF_CONTEXT_TIME_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "tikv_raftstore_store_perf_context_time_duration_secs",
            "Bucketed histogram of request wait time duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref READ_QPS_TOPN: GaugeVec =
        register_gauge_vec!(
            "tikv_read_qps_topn",
            "collect topN of read qps",
        &["order"]
        ).unwrap();

    pub static ref PEER_MSG_LEN: Histogram =
        register_histogram!(
            "tikv_raftstore_peer_msg_len",
            "Length of peer msg.",
            exponential_buckets(1.0, 2.0, 20).unwrap() // max 1000s
        ).unwrap();
}
