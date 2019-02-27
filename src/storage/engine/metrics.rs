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

use prometheus::exponential_buckets;
use prometheus_static_metric::*;

use storage::ErrorHeaderKind;

make_static_metric! {
    pub label_enum RequestStatusKind {
        all,
        success,
        err_rocksdb,
        err_timeout,
        err_empty_request,
        err_other,
        err_io,
        err_server,
        err_invalid_resp,
        err_invalid_req,
        err_not_leader,
        err_region_not_found,
        err_key_not_in_region,
        err_epoch_not_match,
        err_server_is_busy,
        err_stale_command,
        err_store_not_match,
        err_raft_entry_too_large,
    }

    pub label_enum RequestTypeKind {
        write,
        snapshot,
    }

    pub struct AsyncRequestsCounterVec: IntCounter {
        "type" => RequestTypeKind,
        "status" => RequestStatusKind,
    }

    pub struct AsyncRequestsDurationVec: Histogram {
        "type" => RequestTypeKind,
    }
}

impl From<ErrorHeaderKind> for RequestStatusKind {
    fn from(kind: ErrorHeaderKind) -> Self {
        match kind {
            ErrorHeaderKind::NotLeader => RequestStatusKind::err_not_leader,
            ErrorHeaderKind::RegionNotFound => RequestStatusKind::err_region_not_found,
            ErrorHeaderKind::KeyNotInRegion => RequestStatusKind::err_key_not_in_region,
            ErrorHeaderKind::EpochNotMatch => RequestStatusKind::err_epoch_not_match,
            ErrorHeaderKind::ServerIsBusy => RequestStatusKind::err_server_is_busy,
            ErrorHeaderKind::StaleCommand => RequestStatusKind::err_stale_command,
            ErrorHeaderKind::StoreNotMatch => RequestStatusKind::err_store_not_match,
            ErrorHeaderKind::RaftEntryTooLarge => RequestStatusKind::err_raft_entry_too_large,
            ErrorHeaderKind::Other => RequestStatusKind::err_other,
        }
    }
}

lazy_static! {
    pub static ref ASYNC_REQUESTS_COUNTER_VEC: AsyncRequestsCounterVec = {
        register_static_int_counter_vec!(
            AsyncRequestsCounterVec,
            "tikv_storage_engine_async_request_total",
            "Total number of engine asynchronous requests",
            &["type", "status"]
        ).unwrap()
    };
    pub static ref ASYNC_REQUESTS_DURATIONS_VEC: AsyncRequestsDurationVec = {
        register_static_histogram_vec!(
            AsyncRequestsDurationVec,
            "tikv_storage_engine_async_request_duration_seconds",
            "Bucketed histogram of processing successful asynchronous requests.",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap()
    };
}
