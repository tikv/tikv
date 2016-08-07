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

use raftstore::store::{PeerStorage, keys, delete_raft_log};
use raftstore::store::engine::Iterable;
use util::worker::Runnable;

use rocksdb::DB;
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
        if let Some((k, _)) = box_try!(task.engine.seek(&start_key)) {
            first_idx = box_try!(keys::raft_log_index(&k));
        }
        if first_idx >= task.compact_idx {
            info!("no need to compact");
            return Ok(0);
        }

        box_try!(delete_raft_log(task.engine.as_ref(),
                                 task.region_id,
                                 first_idx,
                                 task.compact_idx));
        Ok(task.compact_idx - first_idx)
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!("executing task {}", task);
        let region_id = task.region_id;
        match self.compact(task) {
            Err(e) => error!("failed to compact: {:?}", e),
            Ok(n) => {
                info!("{} log entries have been compacted for region {}",
                      n,
                      region_id)
            }
        }
    }
}
