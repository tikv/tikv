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

use prometheus::{CounterVec, Counter};

lazy_static! {
    pub static ref RAFTSTORE_RAFT_MESSAGE_COUNTER: Counter =
        register_counter!(
            "tikv_raftstore_raft_message_sent_total",
            "Total number of raft messages sent."
        ).unwrap();

    pub static ref RAFTSTORE_SNAPSHOT_COUNTER: Counter =
        register_counter!(
            "tikv_raftstore_apply_snapshot_total",
            "Total number of snapshots applied."
        ).unwrap();

    pub static ref RAFTSTORE_ENTRY_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_entry_processed_total",
            "Total number of raft enties be processed.",
            &["action"]
        ).unwrap();
}
