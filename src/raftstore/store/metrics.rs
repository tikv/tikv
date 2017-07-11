// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::{Counter, CounterVec, GaugeVec, Histogram, HistogramVec, exponential_buckets};

lazy_static! {
    pub static ref PEER_PROPOSAL_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_proposal_total",
            "Total number of proposal made.",
            &["type"]
        ).unwrap();

    pub static ref PEER_ADMIN_CMD_COUNTER_VEC: CounterVec =
        register_counter_vec!(
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

    pub static ref STORE_APPLY_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_log_duration_seconds",
            "Bucketed histogram of peer applying log duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_RAFT_READY_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_raft_ready_handled_total",
            "Total number of raft ready handled.",
            &["type"]
        ).unwrap();

    pub static ref STORE_RAFT_SENT_MESSAGE_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_raft_sent_message_total",
            "Total number of raft ready sent messages.",
            &["type"]
        ).unwrap();

    pub static ref STORE_PD_HEARTBEAT_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_pd_heartbeat_tick_total",
            "Total number of pd heartbeat ticks.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SIZE_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raftstore_store_size_bytes",
            "Size of raftstore storage.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raftstore_snapshot_traffic_total",
            "Total number of raftstore snapshot traffic.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_snapshot_validation_failure_total",
            "Total number of raftstore snapshot validation failure.",
            &["type"]
        ).unwrap();

    pub static ref STORE_COMPACT_RANGE_TASKS_BATCH_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_compact_range_tasks_batch_size",
            "Bucketed histogram of compact range tasks batch size",
            exponential_buckets(1.0, 2.0, 10).unwrap()
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

    pub static ref REGION_HASH_COUNTER_VEC: CounterVec =
        register_counter_vec!(
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

    pub static ref REGION_WRITTEN_BYTES_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_region_written_bytes",
            "Histogram of bytes written for regions",
             exponential_buckets(256.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref REGION_WRITTEN_KEYS_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_region_written_keys",
            "Histogram of keys written for regions",
             exponential_buckets(1.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref REQUEST_WAIT_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_request_wait_time_duration_secs",
            "Bucketed histogram of request wait time duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_GC_RAFT_LOG_COUNTER: Counter =
        register_counter!(
            "tikv_raftstore_gc_raft_log_total",
            "Total number of GC raft log."
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
            exponential_buckets(1024.0, 2.0, 22).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_BUILD_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_snapshot_build_time_duration_secs",
            "Bucketed histogram of snapshot build time duration.",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
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

    pub static ref RAFT_ENTRY_FETCHES: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_entry_fetches",
            "Total number of raft entry fetches",
            &["type"]
        ).unwrap();
}
