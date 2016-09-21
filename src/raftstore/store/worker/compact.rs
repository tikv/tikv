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

use raftstore::store::{PeerStorage, keys};
use raftstore::store::engine::Iterable;
use util::worker::Runnable;
use util::rocksdb;
use storage::CF_RAFT;

use rocksdb::{DB, WriteBatch, Writable};
use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::error;

/// Compact task.
pub struct Task {
    engine: Arc<DB>,
    region_id: u64,
    compact_idx: u64,
}

impl Task {
    pub fn new(ps: &PeerStorage, compact_idx: u64) -> Task {
        Task {
            engine: ps.get_engine().clone(),
            region_id: ps.get_region_id(),
            compact_idx: compact_idx,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "Compact Task [region: {}, to: {}]",
               self.region_id,
               self.compact_idx)
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("compact failed {:?}", err)
        }
    }
}

pub struct Runner;

impl Runner {
    /// Do the compact job and return the count of log compacted.
    fn compact(&mut self, task: Task) -> Result<u64, Error> {
        let start_key = keys::raft_log_key(task.region_id, 0);
        let mut first_idx = task.compact_idx;
        if let Some((k, _)) = box_try!(task.engine.seek_cf(CF_RAFT, &start_key)) {
            first_idx = box_try!(keys::raft_log_index(&k));
        }
        if first_idx >= task.compact_idx {
            info!("[region {}] no need to compact", task.region_id);
            return Ok(0);
        }
        let wb = WriteBatch::new();
        let handle = box_try!(rocksdb::get_cf_handle(&task.engine, CF_RAFT));
        for idx in first_idx..task.compact_idx {
            let key = keys::raft_log_key(task.region_id, idx);
            box_try!(wb.delete_cf(handle, &key));
        }
        // It's not safe to disable WAL here. We may lost data after crashed for unknown reason.
        box_try!(task.engine.write(wb));
        Ok(task.compact_idx - first_idx)
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!("[region {}] execute compacting log to {}",
               task.region_id,
               task.compact_idx);
        let region_id = task.region_id;
        match self.compact(task) {
            Err(e) => error!("[region {}] failed to compact: {:?}", region_id, e),
            Ok(n) => info!("[region {}] compact {} log entries", region_id, n),
        }
    }
}
