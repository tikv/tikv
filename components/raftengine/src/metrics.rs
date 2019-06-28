// Copyright 2017 PingCAP, Inc.
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

use prometheus::{exponential_buckets, Counter, Gauge, Histogram};

lazy_static! {
    pub static ref RAFTENGINE_MEMORY_USAGE_GAUGE: Gauge = register_gauge!(
        "tikv_raftengine_memory_usage_bytes",
        "Total bytes of all memtables."
    )
    .unwrap();
    pub static ref REWRITE_ENTRIES_COUNT_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftengine_rewrite_entries_count",
        "Bucketed histogram of rewrite entries count.",
        exponential_buckets(1.0, 2.0, 8).unwrap()
    )
    .unwrap();
    pub static ref REWRITE_COUNTER: Counter = register_counter!(
        "tikv_raftengine_rewrite_counter",
        "Total number of rewriting happens"
    )
    .unwrap();
    pub static ref NEED_COMPACT_REGIONS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftengine_need_compact_regions_count",
        "Bucketed histogram of regions count need compact.",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref EXPIRED_FILES_PURGED_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftengine_expired_files_purged_count",
        "Bucketed histogram of expired files purged count.",
        exponential_buckets(1.0, 2.0, 8).unwrap()
    )
    .unwrap();
    pub static ref PIPE_FILES_COUNT_GAUGE: Gauge = register_gauge!(
        "tikv_raftengine_total_pipe_files_count",
        "Total number of current pipe log files."
    )
    .unwrap();
    pub static ref READ_ENTRY_FROM_PIPE_FILE: Counter = register_counter!(
        "tikv_raftengine_fread_counter",
        "Total number of read from file happens"
    )
    .unwrap();
    pub static ref RAFT_ENGINE_WRITE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftengine_write_duration_seconds",
        "Bucketed histogram of raft engine write duration",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref APPEND_LOG_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftengine_append_log_size",
        "Bucketed histogram of raft engine append log size",
        vec![
            128.0, 256.0, 512.0, 1024.0, 4096.0, 65536.0, 262144.0, 524288.0, 1048576.0, 2097152.0,
            4194304.0, 8388608.0, 16777216.0
        ]
    )
    .unwrap();
}
