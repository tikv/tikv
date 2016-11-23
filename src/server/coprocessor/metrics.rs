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

use prometheus::HistogramVec;

lazy_static! {
    pub static ref COPR_REQ_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_request_duration_seconds",
            "Bucketed histogram of coprocessor handle request duration",
            &["type", "req"]
        ).unwrap();

    pub static ref OUTDATED_REQ_WAIT_TIME: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_outdated_request_wait_seconds",
            "Bucketed histogram of outdated coprocessor request wait duration",
            &["type", "req"]
        ).unwrap();
}
