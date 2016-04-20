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

use rocksdb::{DB, WriteBatch, Writable};
use std::sync::Arc;
use std::fmt::{self, Formatter, Display};

use util::worker::Runnable;

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

pub struct Runner;

impl Runner {}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!("executing task {}", task);
        let start_key = keys::raft_log_key(task.region_id, 0);
        let end_key = keys::raft_log_key(task.region_id, task.compact_idx);
        let w = WriteBatch::new();
        let mut cnt = 0;

        let res = task.engine.scan(&start_key,
                                   &end_key,
                                   &mut |key, _| {
                                       cnt += 1;
                                       try!(w.delete(key));
                                       Ok(true)
                                   });
        if let Err(e) = res {
            error!("failed to scan to compact index: {:?}", e);
            return;
        }
        if let Err(e) = task.engine.write(w) {
            error!("failed to compact: {:?}", e);
        }
        info!("{} log entries have been compacted for region {}",
              cnt,
              task.region_id);
    }
}
