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

use std::u64;
use std::fmt;
use std::sync::Arc;

use rocksdb::DB;

use util::escape;
use util::worker::{Runnable, Scheduler};
use raftstore::store::util;
use raftstore::store::worker::ApplyTask;
use storage::{ALL_CFS, CF_WRITE};
use storage::types::Key;
use util::rocksdb::get_cf_handle;

pub enum Task {
    UnsafeCleanupRange {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Task::UnsafeCleanupRange {
                ref region_id,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "UnsafeCleanupRange task region [{}] start_key [{:?}], end_key [{:?}]",
                region_id,
                escape(start_key),
                escape(end_key)
            ),
        }
    }
}

pub struct Runner {
    engine: Arc<DB>,
    use_delete_range: bool,
    apply_ch: Scheduler<ApplyTask>,
}

impl Runner {
    pub fn new(engine: Arc<DB>, use_delete_range: bool, apply_ch: Scheduler<ApplyTask>) -> Runner {
        Runner {
            engine: engine,
            use_delete_range: use_delete_range,
            apply_ch: apply_ch,
        }
    }

    fn unsafe_cleanup_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8]) {
        let handle = get_cf_handle(&self.engine, cf).unwrap();

        // Use delete_files_in_range to drop as many sst files as possible, this
        // is a way to reclaim disk space quickly after drop a table/index.
        self.engine
            .delete_files_in_range_cf(handle, start_key, end_key, /* include_end */ false)
            .unwrap_or_else(|e| {
                panic!(
                    "failed to delete files in range [{}, {}): {:?}",
                    escape(start_key),
                    escape(end_key),
                    e
                )
            });

        // Delete all remaining keys.
        util::delete_all_in_range_cf(&self.engine, cf, start_key, end_key, self.use_delete_range)
            .unwrap_or_else(|e| {
                panic!(
                    "failed to delete all in range [{}, {}), cf: {}, err: {:?}",
                    escape(start_key),
                    escape(end_key),
                    cf,
                    e
                );
            });
    }

    fn unsafe_cleanup_range(&self, start_key: &[u8], end_key: &[u8]) {
        for cf in ALL_CFS {
            if *cf == CF_WRITE {
                let mut key = Key::from_encoded(start_key.to_vec());
                key.append_ts(u64::MAX);
                self.unsafe_cleanup_range_cf(cf, key.encoded(), end_key);
            } else {
                self.unsafe_cleanup_range_cf(cf, start_key, end_key);
            }
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::UnsafeCleanupRange {
                region_id,
                start_key,
                end_key,
            } => {
                self.unsafe_cleanup_range(&start_key, &end_key);
                self.apply_ch
                    .schedule(ApplyTask::resume(region_id))
                    .unwrap_or_else(|e| {
                        panic!(
                            "schedule resume task to apply worker failed, error: {:?}",
                            e
                        )
                    });
            }
        }
    }
}
