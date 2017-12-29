// Copyright 2017 PingCAP, Inc.
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

use std::collections::{BTreeSet, VecDeque};
use std::error;
use std::sync::mpsc::Sender;

use rocksdb::{CFHandle, Range, DB};
use std::sync::Arc;
use std::fmt::{self, Display, Formatter};

use util::rocksdb;
use util::worker::Runnable;
use util::properties::MvccProperties;
use storage::CF_WRITE;

type Key = Vec<u8>;

const MIN_NUM_DEL: u64 = 10000;

pub struct Task {
    pub ranges: BTreeSet<Key>,
}

#[derive(Default, Debug)]
pub struct TaskRes {
    pub ranges_need_compact: VecDeque<(Key, Key)>,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "ranges count {}", self.ranges.len())
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("check ranges need reclaim failed, err: {:?}", err)
        }
    }
}

pub struct Runner {
    engine: Arc<DB>,
    notifier: Sender<TaskRes>,
}

impl Runner {
    pub fn new(engine: Arc<DB>, notifier: Sender<TaskRes>) -> Runner {
        Runner {
            engine: engine,
            notifier: notifier,
        }
    }

    pub fn gather_range_entries_and_puts(
        &self,
        cf: &CFHandle,
        start: &[u8],
        end: &[u8],
    ) -> Option<(u64, u64)> {
        let range = Range::new(start, end);
        let collection = match self.engine.get_properties_of_tables_in_range(cf, &[range]) {
            Ok(v) => v,
            Err(_) => return None,
        };

        if collection.is_empty() {
            return None;
        }

        // Aggregate MVCC properties and total number entries.
        let mut props = MvccProperties::new();
        let mut num_entries = 0;
        for (_, v) in &*collection {
            let mvcc = match MvccProperties::decode(v.user_collected_properties()) {
                Ok(v) => v,
                Err(_) => return None,
            };
            num_entries += v.num_entries();
            props.add(&mvcc);
        }
        Some((num_entries, props.num_versions))
    }

    fn need_compact(num_dels: u64, num_versions: u64) -> bool {
        num_dels >= MIN_NUM_DEL && num_dels >= num_versions / 2
    }

    pub fn check_ranges_need_reclaim(&self, ranges: BTreeSet<Key>) -> Result<TaskRes, Error> {
        let cf = box_try!(rocksdb::get_cf_handle(&self.engine, CF_WRITE));
        let mut task_res = TaskRes::default();
        let mut last_start = vec![];
        let mut need_compact_start = None;
        for key in &ranges {
            if last_start.is_empty() {
                last_start = key.clone();
                continue;
            }
            if let Some((num_entries, num_versions)) =
                self.gather_range_entries_and_puts(cf, &last_start, key)
            {
                if num_entries > num_versions {
                    let estimate_num_del = num_entries - num_versions;
                    if Self::need_compact(estimate_num_del, num_versions) {
                        if need_compact_start.is_none() {
                            need_compact_start = Some(last_start.clone());
                        }
                        last_start = key.clone();
                        continue;
                    }
                }
            }
            // collect ranges need manual compaction
            if need_compact_start.is_some() {
                task_res
                    .ranges_need_compact
                    .push_back((need_compact_start.unwrap().to_vec(), last_start));
                need_compact_start = None;
            }
            last_start = key.clone();
        }

        if need_compact_start.is_some() {
            task_res
                .ranges_need_compact
                .push_back((need_compact_start.unwrap().to_vec(), last_start.to_vec()));
        }
        info!("{:?}", task_res);
        Ok(task_res)
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match self.check_ranges_need_reclaim(task.ranges) {
            Ok(task_res) => self.notifier.send(task_res).unwrap(),
            Err(e) => warn!("check ranges need reclaim failed, err: {:?}", e),
        }
    }
}
