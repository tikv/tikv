// Copyright 2018 PingCAP, Inc.
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

use rocksdb::PerfContext;

macro_rules! define_perf_statistics {
    ($($field: ident),+ $(,)*) => {
        /// Store statistics we need. Data comes from RocksDB's `PerfContext`.
        /// This statistics store instant values.
        #[derive(Debug, Clone, Copy)]
        pub struct PerfStatisticsInstant {
            $(pub $field: usize,)+
        }

        impl PerfStatisticsInstant {
            /// Create an instance which stores instant statistics values, retrieved at creation.
            pub fn new() -> Self {
                let perf_context = PerfContext::get();
                Self {
                    $($field: perf_context.$field() as usize,)+
                }
            }

            /// Calculates delta values.
            pub fn delta_between(&self, other: &Self) -> PerfStatisticsDelta {
                PerfStatisticsDelta {
                    $($field: other.$field - self.$field,)+
                }
            }

            /// Calculate delta values until now.
            pub fn delta(&self) -> PerfStatisticsDelta {
                let now = Self::new();
                self.delta_between(&now)
            }
        }

        /// Store statistics we need. Data comes from RocksDB's `PerfContext`.
        /// This this statistics store delta values between two instant statistics.
        #[derive(Default, Debug, Clone, Copy)]
        pub struct PerfStatisticsDelta {
            $(pub $field: usize,)+
        }
    }
}

define_perf_statistics!{
    internal_key_skipped_count,
    internal_delete_skipped_count,
    block_cache_hit_count,
    block_read_count,
    block_read_byte,
}
