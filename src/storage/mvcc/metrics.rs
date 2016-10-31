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

use prometheus::{Histogram, exponential_buckets};

lazy_static! {
    pub static ref MVCC_VERSIONS_HISTOGRAM: Histogram =
        register_histogram!(
            histogram_opts!{
                "tikv_storage_mvcc_versions",
                "Histogram of versions for each key",
                [ exponential_buckets(1.0, 2.0, 10).unwrap() ]
            }
        ).unwrap();

    pub static ref GC_DELETE_VERSIONS_HISTOGRAM: Histogram =
        register_histogram!(
            histogram_opts!{
                "tikv_storage_mvcc_gc_delete_versions",
                "Histogram of versions deleted by gc for each key",
                [ exponential_buckets(1.0, 2.0, 10).unwrap() ]
            }
        ).unwrap();
}
