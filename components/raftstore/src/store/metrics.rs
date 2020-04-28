// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum PerfContextType {
        write_wal_time,
        write_memtable_time,
        pre_and_post_process,
        write_thread_wait,
        db_mutex_lock_nanos,
        report_time,
    }
    pub label_enum ProposalType {
        all,
        local_read,
        read_index,
        unsafe_read_index,
        normal,
        transfer_leader,
        conf_change,
    }

    pub label_enum AdminCmdType {
        conf_change,
        add_peer,
        remove_peer,
        add_learner,
        batch_split : "batch-split",
        prepare_merge,
        commit_merge,
        rollback_merge,
        compact,
    }

    pub label_enum AdminCmdStatus {
        reject_unsafe,
        all,
        success,
    }

    pub label_enum RaftReadyType {
        message,
        commit,
        append,
        snapshot,
        pending_region,
        has_ready_region,
    }

    pub label_enum MessageCounterType {
        append,
        append_resp,
        prevote,
        prevote_resp,
        vote,
        vote_resp,
        snapshot,
        request_snapshot,
        heartbeat,
        heartbeat_resp,
        transfer_leader,
        timeout_now,
        read_index,
        read_index_resp,
    }

    pub label_enum RaftDroppedMessage {
        mismatch_store_id,
        mismatch_region_epoch,
        stale_msg,
        region_overlap,
        region_no_peer,
        region_tombstone_peer,
        region_nonexistent,
        applying_snap,
    }

    pub label_enum SnapValidationType {
        stale,
        decode,
        epoch,
    }

    pub label_enum RegionHashType {
        verify,
        compute,
    }

    pub label_enum RegionHashResult {
        miss,
        matched,
        all,
        failed,
    }

    pub label_enum CfNames {
        default,
        lock,
        write,
        raft,
    }

    pub label_enum RaftEntryType {
        hit,
        miss
    }

    pub label_enum RaftInvalidProposal {
        mismatch_store_id,
        region_not_found,
        not_leader,
        mismatch_peer_id,
        stale_command,
        epoch_not_match,
        read_index_no_leader,
        region_not_initialized,
        is_applying_snapshot,
    }
    pub label_enum RaftEventDurationType {
        compact_check,
        pd_store_heartbeat,
        snap_gc,
        compact_lock_cf,
        consistency_check,
        cleanup_import_sst,
    }

    pub struct RaftEventDuration : LocalHistogram {
        "type" => RaftEventDurationType
    }
    pub struct RaftInvalidProposalCount : LocalIntCounter {
        "type" => RaftInvalidProposal
    }
    pub struct RaftEntryFetches : LocalIntCounter {
        "type" => RaftEntryType
    }
    pub struct SnapCf : LocalHistogram {
        "type" => CfNames,
    }
    pub struct SnapCfSize : LocalHistogram {
        "type" => CfNames,
    }
    pub struct RegionHashCounter: LocalIntCounter {
        "type" => RegionHashType,
        "result" => RegionHashResult,
    }
    pub struct ProposalVec: LocalIntCounter {
        "type" => ProposalType,
    }

    pub struct AdminCmdVec : LocalIntCounter {
        "type" => AdminCmdType,
        "status" => AdminCmdStatus,
    }

    pub struct RaftReadyVec : LocalIntCounter {
        "type" => RaftReadyType,
    }

    pub struct MessageCounterVec : LocalIntCounter {
        "type" => MessageCounterType,
    }

    pub struct RaftDropedVec : LocalIntCounter {
        "type" => RaftDroppedMessage,
    }

    pub struct SnapValidVec : LocalIntCounter {
        "type" => SnapValidationType
    }
    pub struct PerfContextTimeDuration : LocalHistogram {
        "type" => PerfContextType
    }
}

lazy_static! {
    pub static ref PEER_PROPOSAL_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_proposal_total",
            "Total number of proposal made.",
            &["type"]
        ).unwrap();
    pub static ref PEER_PROPOSAL_COUNTER: ProposalVec =
        auto_flush_from!(PEER_PROPOSAL_COUNTER_VEC, ProposalVec);

    pub static ref PEER_ADMIN_CMD_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_admin_cmd_total",
            "Total number of admin cmd processed.",
            &["type", "status"]
        ).unwrap();
    pub static ref PEER_ADMIN_CMD_COUNTER: AdminCmdVec =
        auto_flush_from!(PEER_ADMIN_CMD_COUNTER_VEC, AdminCmdVec);

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
    pub static ref STORE_RAFT_READY_COUNTER: RaftReadyVec =
        auto_flush_from!(STORE_RAFT_READY_COUNTER_VEC, RaftReadyVec);

    pub static ref STORE_RAFT_SENT_MESSAGE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_sent_message_total",
            "Total number of raft ready sent messages.",
            &["type"]
        ).unwrap();
    pub static ref STORE_RAFT_SENT_MESSAGE_COUNTER: MessageCounterVec =
        auto_flush_from!(STORE_RAFT_SENT_MESSAGE_COUNTER_VEC, MessageCounterVec);

    pub static ref STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_dropped_message_total",
            "Total number of raft dropped messages.",
            &["type"]
        ).unwrap();
    pub static ref STORE_RAFT_DROPPED_MESSAGE_COUNTER: RaftDropedVec =
        auto_flush_from!(STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC, RaftDropedVec);

    pub static ref STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC: IntGaugeVec =
        register_int_gauge_vec!(
            "tikv_raftstore_snapshot_traffic_total",
            "Total number of raftstore snapshot traffic.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_snapshot_validation_failure_total",
            "Total number of raftstore snapshot validation failure.",
            &["type"]
        ).unwrap();
    pub static ref STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER: SnapValidVec =
        auto_flush_from!(STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER_VEC, SnapValidVec);

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
    pub static ref REGION_HASH_COUNTER: RegionHashCounter =
        auto_flush_from!(REGION_HASH_COUNTER_VEC, RegionHashCounter);

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

    pub static ref SNAPSHOT_CF_KV_COUNT_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_snapshot_cf_kv_count",
            "Total number of kv in each cf file of snapshot",
            &["type"],
            exponential_buckets(100.0, 2.0, 20).unwrap()
        ).unwrap();
    pub static ref SNAPSHOT_CF_KV_COUNT: SnapCf =
        auto_flush_from!(SNAPSHOT_CF_KV_COUNT_VEC, SnapCf);

    pub static ref SNAPSHOT_CF_SIZE_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_snapshot_cf_size",
            "Total size of each cf file of snapshot",
            &["type"],
            exponential_buckets(1024.0, 2.0, 31).unwrap()
        ).unwrap();
    pub static ref SNAPSHOT_CF_SIZE: SnapCfSize =
        auto_flush_from!(SNAPSHOT_CF_SIZE_VEC, SnapCfSize);
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

    pub static ref RAFT_ENTRY_FETCHES_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_entry_fetches",
            "Total number of raft entry fetches",
            &["type"]
        ).unwrap();
    pub static ref RAFT_ENTRY_FETCHES: RaftEntryFetches =
        auto_flush_from!(RAFT_ENTRY_FETCHES_VEC, RaftEntryFetches);

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
    pub static ref RAFT_INVALID_PROPOSAL_COUNTER: RaftInvalidProposalCount =
        auto_flush_from!(RAFT_INVALID_PROPOSAL_COUNTER_VEC, RaftInvalidProposalCount);

    pub static ref RAFT_EVENT_DURATION_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_raftstore_event_duration",
            "Duration of raft store events.",
            &["type"],
            exponential_buckets(0.001, 1.59, 20).unwrap() // max 10s
        ).unwrap();
    pub static ref RAFT_EVENT_DURATION: RaftEventDuration =
        auto_flush_from!(RAFT_EVENT_DURATION_VEC, RaftEventDuration);

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

    pub static ref APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC: PerfContextTimeDuration=
        auto_flush_from!(APPLY_PERF_CONTEXT_TIME_HISTOGRAM, PerfContextTimeDuration);

    pub static ref STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC: PerfContextTimeDuration=
        auto_flush_from!(STORE_PERF_CONTEXT_TIME_HISTOGRAM, PerfContextTimeDuration);

    pub static ref READ_QPS_TOPN: GaugeVec =
        register_gauge_vec!(
            "tikv_read_qps_topn",
            "collect topN of read qps",
        &["order"]
        ).unwrap();
}
