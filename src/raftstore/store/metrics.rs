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

use prometheus::{CounterVec, Counter, GaugeVec, Histogram};

lazy_static! {
    pub static ref PEER_RAFT_READY_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_raft_ready_handled_total",
            "Total number of raft ready handled.",
            &["type"]
        ).unwrap();

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

    pub static ref PEER_ENTRY_CONF_CHANGE_COUNTER: Counter =
        register_counter!(
            "tikv_raftstore_handle_raft_entry_conf_change_total",
            "Total number of raft entry conf change handled."
        ).unwrap();

    pub static ref PEER_APPLY_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_log_duration_seconds",
            "Bucketed histogram of peer applying log duration"
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
}
