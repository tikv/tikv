// Copyright 2019 PingCAP, Inc.
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

use prometheus::*;

lazy_static! {
    pub static ref FUTUREPOOL_RUNNING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_futurepool_pending_task_total",
        "Current future_pool pending + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref FUTUREPOOL_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_futurepool_handled_task_total",
        "Total number of future_pool handled tasks.",
        &["name"]
    )
    .unwrap();
}
