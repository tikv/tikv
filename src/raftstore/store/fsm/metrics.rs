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

use prometheus::{exponential_buckets, Histogram};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

lazy_static! {
    pub static ref APPLY_PROPOSAL: Histogram = register_histogram!(
        "tikv_raftstore_apply_proposal",
        "The count of proposals sent by a region at once",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
}

#[derive(Default)]
pub struct StoreStat {
    pub lock_cf_bytes_written: AtomicU64,
    pub engine_total_bytes_written: AtomicU64,
    pub engine_total_keys_written: AtomicU64,
    pub is_busy: AtomicBool,
}

#[derive(Clone, Default)]
pub struct GlobalStoreStat {
    pub stat: Arc<StoreStat>,
}

impl GlobalStoreStat {
    #[inline]
    pub fn local(&self) -> LocalStoreStat {
        LocalStoreStat {
            lock_cf_bytes_written: 0,
            engine_total_bytes_written: 0,
            engine_total_keys_written: 0,
            is_busy: false,

            global: self.clone(),
        }
    }
}

pub struct LocalStoreStat {
    pub lock_cf_bytes_written: u64,
    pub engine_total_bytes_written: u64,
    pub engine_total_keys_written: u64,
    pub is_busy: bool,

    global: GlobalStoreStat,
}

impl Clone for LocalStoreStat {
    #[inline]
    fn clone(&self) -> LocalStoreStat {
        self.global.local()
    }
}

impl LocalStoreStat {
    pub fn flush(&mut self) {
        if self.lock_cf_bytes_written != 0 {
            self.global
                .stat
                .lock_cf_bytes_written
                .fetch_add(self.lock_cf_bytes_written, Ordering::SeqCst);
            self.lock_cf_bytes_written = 0;
        }
        if self.engine_total_bytes_written != 0 {
            self.global
                .stat
                .engine_total_bytes_written
                .fetch_add(self.engine_total_bytes_written, Ordering::SeqCst);
            self.engine_total_bytes_written = 0;
        }
        if self.engine_total_keys_written != 0 {
            self.global
                .stat
                .engine_total_keys_written
                .fetch_add(self.engine_total_keys_written, Ordering::SeqCst);
            self.engine_total_keys_written = 0;
        }
        if self.is_busy {
            self.global.stat.is_busy.store(true, Ordering::SeqCst);
            self.is_busy = false;
        }
    }
}
