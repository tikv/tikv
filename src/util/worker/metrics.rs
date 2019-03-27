// Copyright 2016 TiKV Project Authors.
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
    pub static ref WORKER_PENDING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_worker_pending_task_total",
        "Current worker pending + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref WORKER_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_worker_handled_task_total",
        "Total number of worker handled tasks.",
        &["name"]
    )
    .unwrap();
}
