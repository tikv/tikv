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

use raftstore::store::{PeerStorage, keys, SendCh, Msg};
use raftstore::store::engine::Iterable;

use rocksdb::DB;
use std::sync::Arc;
use std::fmt::{self, Formatter, Display};

use util::worker::Runnable;

/// Split checking task.
pub struct Task {
    region_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    engine: Arc<DB>,
}

impl Task {
    pub fn new(ps: &PeerStorage) -> Task {
        Task {
            region_id: ps.get_region_id(),
            start_key: keys::enc_start_key(&ps.region),
            end_key: keys::enc_end_key(&ps.region),
            engine: ps.get_engine().clone(),
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Split Check Task for {}", self.region_id)
    }
}

pub struct Runner {
    ch: SendCh,
    region_max_size: u64,
    split_size: u64,
}

impl Runner {
    pub fn new(ch: SendCh, region_max_size: u64, split_size: u64) -> Runner {
        Runner {
            ch: ch,
            region_max_size: region_max_size,
            split_size: split_size,
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!("executing task {:?} {:?}", task.start_key, task.end_key);
        let mut size = 0;
        let mut split_key = vec![];
        let res = task.engine.scan(&task.start_key,
                                   &task.end_key,
                                   &mut |k, v| {
                                       size += k.len() as u64;
                                       size += v.len() as u64;
                                       if split_key.is_empty() && size > self.split_size {
                                           split_key = k.to_vec();
                                       }
                                       Ok(size < self.region_max_size)
                                   });
        if let Err(e) = res {
            error!("failed to scan split key of region {}: {:?}",
                   task.region_id,
                   e);
            return;
        }
        if size < self.region_max_size {
            debug!("no need to send for {} < {}", size, self.region_max_size);
            return;
        }
        let res = self.ch.send(new_split_check_result(task.region_id, split_key));
        if let Err(e) = res {
            warn!("failed to send check result of {}: {}", task.region_id, e);
        }
    }
}

fn new_split_check_result(region_id: u64, split_key: Vec<u8>) -> Msg {
    Msg::SplitCheckResult {
        region_id: region_id,
        split_key: split_key,
    }
}
