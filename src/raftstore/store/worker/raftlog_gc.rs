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

use raftstore::store::keys;
use raftstore::store::engine::Iterable;
use util::worker::Runnable;
use util::rocksdb;
use storage::CF_RAFT;

use rocksdb::{DB, WriteBatch, Writable};
use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::error;

pub struct Task {
    pub engine: Arc<DB>,
    pub region_id: u64,
    pub start_idx: u64,
    pub end_idx: u64,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "GC Raft Log Task [region: {}, from: {}, to: {}]",
               self.region_id,
               self.start_idx,
               self.end_idx)
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("raftlog gc failed {:?}", err)
        }
    }
}

pub struct Runner;

impl Runner {
    /// Do the gc job and return the count of log collected.
    fn gc_raft_log(&mut self,
                   engine: Arc<DB>,
                   region_id: u64,
                   start_idx: u64,
                   end_idx: u64)
                   -> Result<u64, Error> {
        let mut first_idx = start_idx;
        if first_idx == 0 {
            let start_key = keys::raft_log_key(region_id, 0);
            first_idx = end_idx;
            if let Some((k, _)) = box_try!(engine.seek_cf(CF_RAFT, &start_key)) {
                first_idx = box_try!(keys::raft_log_index(&k));
            }
        }
        if first_idx >= end_idx {
            info!("[region {}] no need to gc", region_id);
            return Ok(0);
        }
        let wb = WriteBatch::new();
        let handle = box_try!(rocksdb::get_cf_handle(&engine, CF_RAFT));
        for idx in first_idx..end_idx {
            let key = keys::raft_log_key(region_id, idx);
            box_try!(wb.delete_cf(handle, &key));
        }
        // It's not safe to disable WAL here. We may lost data after crashed for unknown reason.
        engine.write(wb).unwrap();
        Ok(end_idx - first_idx)
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!("[region {}] execute gc log to {}",
               task.region_id,
               task.end_idx);
        match self.gc_raft_log(task.engine, task.region_id, task.start_idx, task.end_idx) {
            Err(e) => error!("[region {}] failed to gc: {:?}", task.region_id, e),
            Ok(n) => info!("[region {}] collected {} log entries", task.region_id, n),
        }
    }
}
