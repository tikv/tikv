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

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};

use rocksdb::DB;

use kvproto::metapb::RegionEpoch;
use raftstore::store::{PeerStorage, keys, Msg};
use raftstore::store::engine::Iterable;
use util::escape;
use util::transport::SendCh;
use util::worker::Runnable;

use super::metrics::*;

pub enum Task {
    SplitCheck {
        region_id: u64,
        epoch: RegionEpoch,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        engine: Arc<DB>,
    },
    MergeCheck {
        region_id: u64,
        epoch: RegionEpoch,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        engine: Arc<DB>,
    },
}

pub fn new_split_check_task(ps: &PeerStorage) -> Task {
    Task::SplitCheck {
        region_id: ps.get_region_id(),
        epoch: ps.get_region().get_region_epoch().clone(),
        start_key: keys::enc_start_key(&ps.region),
        end_key: keys::enc_end_key(&ps.region),
        engine: ps.get_engine().clone(),
    }
}

pub fn new_merge_check_task(ps: &PeerStorage) -> Task {
    Task::MergeCheck {
        region_id: ps.get_region_id(),
        epoch: ps.get_region().get_region_epoch().clone(),
        start_key: keys::enc_start_key(&ps.region),
        end_key: keys::enc_end_key(&ps.region),
        engine: ps.get_engine().clone(),
    }
}


impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::SplitCheck { ref region_id, ref start_key, ref end_key, .. } => {
                write!(f,
                       "Split Check Task for region {}, start_key {}, end_key {}",
                       region_id,
                       escape(start_key),
                       escape(end_key))
            }
            Task::MergeCheck { ref region_id, ref start_key, ref end_key, .. } => {
                write!(f,
                       "Merge Check Task for region {}, start_key {}, end_key {}",
                       region_id,
                       escape(start_key),
                       escape(end_key))
            }
        }
    }
}

pub struct Runner {
    ch: SendCh<Msg>,
    region_max_size: u64,
    split_size: u64,
    merge_size: u64,
}

impl Runner {
    pub fn new(ch: SendCh<Msg>, region_max_size: u64, split_size: u64, merge_size: u64) -> Runner {
        Runner {
            ch: ch,
            region_max_size: region_max_size,
            split_size: split_size,
            merge_size: merge_size,
        }
    }
}

impl Runner {
    fn handle_check_split(&self,
                          region_id: u64,
                          epoch: RegionEpoch,
                          start_key: Vec<u8>,
                          end_key: Vec<u8>,
                          engine: Arc<DB>) {
        REGION_CHECK_COUNTER_VEC.with_label_values(&["split", "all"]).inc();

        let mut size = 0;
        let mut split_key = vec![];
        let histogram = REGION_CHECK_HISTOGRAM.with_label_values(&["split"]);
        let timer = histogram.start_timer();

        let res = engine.scan(&start_key,
                              &end_key,
                              false,
                              &mut |k, v| {
            size += k.len() as u64;
            size += v.len() as u64;
            if split_key.is_empty() && size > self.split_size {
                split_key = k.to_vec();
            }
            Ok(size < self.region_max_size)
        });
        if let Err(e) = res {
            error!("failed to scan split key of region {}: {:?}", region_id, e);
            return;
        }

        timer.observe_duration();

        if size < self.region_max_size {
            debug!("[region {}] no need to send for {} < {}",
                   region_id,
                   size,
                   self.region_max_size);

            REGION_CHECK_COUNTER_VEC.with_label_values(&["split", "ignore"]).inc();
            return;
        }
        let res = self.ch.try_send(new_split_check_result(region_id, epoch, split_key));
        if let Err(e) = res {
            warn!("[region {}] failed to send check result, err {:?}",
                  region_id,
                  e);
        }

        REGION_CHECK_COUNTER_VEC.with_label_values(&["split", "success"]).inc();
    }

    fn handle_check_merge(&self,
                          region_id: u64,
                          epoch: RegionEpoch,
                          start_key: Vec<u8>,
                          end_key: Vec<u8>,
                          engine: Arc<DB>) {
        REGION_CHECK_COUNTER_VEC.with_label_values(&["merge", "all"]).inc();

        let mut size = 0;
        let histogram = REGION_CHECK_HISTOGRAM.with_label_values(&["merge"]);
        let timer = histogram.start_timer();

        // Scan the engine and get the totol size of the region.
        let _ = engine.scan(&start_key,
                            &end_key,
                            false,
                            &mut |k, v| {
                                size += k.len() as u64;
                                size += v.len() as u64;
                                Ok(size >= self.merge_size)
                            });

        timer.observe_duration();

        if size >= self.merge_size {
            debug!("[region {}] no need to ask PD to merge, since size {} >= merge_size {}",
                   region_id,
                   size,
                   self.merge_size);
            REGION_CHECK_COUNTER_VEC.with_label_values(&["merge", "ignore"]).inc();
            return;
        }

        let res = self.ch.try_send(new_merge_check_result(region_id, epoch));
        if let Err(e) = res {
            warn!("[region {}] failed to send merge check result, err {:?}",
                  region_id,
                  e)
        }
        REGION_CHECK_COUNTER_VEC.with_label_values(&["merge", "success"]).inc()
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!("executing task {}", task);
        match task {
            Task::SplitCheck { region_id, epoch, start_key, end_key, engine } => {
                self.handle_check_split(region_id, epoch, start_key, end_key, engine)
            }
            Task::MergeCheck { region_id, epoch, start_key, end_key, engine } => {
                self.handle_check_merge(region_id, epoch, start_key, end_key, engine)
            }
        }
    }
}

fn new_split_check_result(region_id: u64, epoch: RegionEpoch, split_key: Vec<u8>) -> Msg {
    Msg::SplitCheckResult {
        region_id: region_id,
        epoch: epoch,
        split_key: split_key,
    }
}

fn new_merge_check_result(region_id: u64, epoch: RegionEpoch) -> Msg {
    Msg::MergeCheckResult {
        region_id: region_id,
        epoch: epoch,
    }
}
