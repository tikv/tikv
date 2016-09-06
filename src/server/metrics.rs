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

use prometheus::Histogram;

lazy_static! {
    pub static ref SEND_SNAP_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_server_send_snapshot_duration_seconds",
            "Bucketed histogram of server send snapshots duration"
        ).unwrap();
}
