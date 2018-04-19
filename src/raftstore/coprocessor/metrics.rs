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

use prometheus::{exponential_buckets, Histogram};

lazy_static! {
    pub static ref REGION_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_region_size",
        "Bucketed histogram of approximate region size.",
        exponential_buckets(4096.0, 2.0, 20).unwrap()
    ).unwrap();
}
