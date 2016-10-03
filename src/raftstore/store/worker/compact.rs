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
use super::metrics::COMPACT_RANGE_FOR_CF;

pub enum Task {
    CompactRangeCF {
        cf_name: String,
        start_key: Option<Vec<u8>>, // None means smallest key
        end_key: Option<Vec<u8>>, // None means largest key
    },
    CompactRaftLog {
        engine: Arc<DB>,
        region_id: u64,
        compact_idx: u64,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::CompactRaftLog { region_id, compact_idx, .. } => {
                write!(f,
                       "Compact Raft Log Task [region: {}, to: {}]",
                       region_id,
                       compact_idx)
            }
            Task::CompactRangeCF { ref cf_name, ref start_key, ref end_key } => {
                write!(f,
                       "Compact for CF[{}], range[{:?}, {:?}]",
                       cf_name,
                       start_key,
                       end_key)
            }
        }
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

pub struct Runner {
    engine: Arc<DB>,
}

impl Runner {
    pub fn new(engine: Arc<DB>) -> Runner {
        Runner { engine: engine }
    }

    /// Do the compact job and return the count of log compacted.
    fn compact_raft_log(&mut self,
                        engine: Arc<DB>,
                        region_id: u64,
                        compact_idx: u64)
                        -> Result<u64, Error> {
        let start_key = keys::raft_log_key(region_id, 0);
        let mut first_idx = compact_idx;
        if let Some((k, _)) = box_try!(engine.seek_cf(CF_RAFT, &start_key)) {
            first_idx = box_try!(keys::raft_log_index(&k));
        }
        if first_idx >= compact_idx {
            info!("[region {}] no need to compact", region_id);
            return Ok(0);
        }
        let wb = WriteBatch::new();
        let handle = box_try!(rocksdb::get_cf_handle(&engine, CF_RAFT));
        for idx in first_idx..compact_idx {
            let key = keys::raft_log_key(region_id, idx);
            box_try!(wb.delete_cf(handle, &key));
        }
        // It's not safe to disable WAL here. We may lost data after crashed for unknown reason.
        box_try!(engine.write(wb));
        Ok(compact_idx - first_idx)
    }

    fn compact_range_for_cf(&mut self,
                            cf_name: String,
                            start_key: Option<Vec<u8>>,
                            end_key: Option<Vec<u8>>)
                            -> Result<(), Error> {
        let cf_handle = box_try!(rocksdb::get_cf_handle(&self.engine, &cf_name));

        let compact_range_timer = COMPACT_RANGE_FOR_CF.with_label_values(&[&cf_name])
            .start_timer();
        self.engine.compact_range_cf(cf_handle,
                                     start_key.as_ref().map(Vec::as_slice),
                                     end_key.as_ref().map(Vec::as_slice));

        compact_range_timer.observe_duration();

        Ok(())
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::CompactRaftLog { engine, region_id, compact_idx } => {
                debug!("[region {}] execute compacting log to {}",
                       region_id,
                       compact_idx);
                match self.compact_raft_log(engine, region_id, compact_idx) {
                    Err(e) => error!("[region {}] failed to compact: {:?}", region_id, e),
                    Ok(n) => info!("[region {}] compact {} log entries", region_id, n),
                }
            }
            Task::CompactRangeCF { cf_name, start_key, end_key } => {
                debug!("execute compact range for cf {}", cf_name.clone());
                if let Err(e) = self.compact_range_for_cf(cf_name, start_key, end_key) {
                    error!("execute compact lock cf failed, err {}", e);
                }
            }
        }
    }
}
