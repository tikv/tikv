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

use prometheus::{exponential_buckets, Histogram, IntGaugeVec};

lazy_static! {
    pub static ref REGION_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_region_size",
        "Bucketed histogram of approximate region size.",
        exponential_buckets(1024.0 * 1024.0, 2.0, 20).unwrap() // max bucket would be 512GB
    ).unwrap();

    pub static ref REGION_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_region_keys",
        "Bucketed histogram of approximate region keys.",
        exponential_buckets(1.0, 2.0, 30).unwrap()
    ).unwrap();

    pub static ref REGION_COUNT_GAUGE_VEC: IntGaugeVec =
    register_int_gauge_vec!(
        "tikv_raftstore_region_count",
        "Number of regions collected in region_collector",
        &["type"]
    ).unwrap();
}
