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

use raftstore::store::engine::Iterable;
use raftstore::store::keys;
use util::worker::Runnable;

use rocksdb::{Writable, WriteBatch, DB};
use std::error;
use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub struct Task {
    pub raft_engine: Arc<DB>,
    pub region_id: u64,
    pub start_idx: u64,
    pub end_idx: u64,
}

pub struct TaskRes {
    pub collected: u64,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "GC Raft Log Task [region: {}, from: {}, to: {}]",
            self.region_id, self.start_idx, self.end_idx
        )
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

pub struct Runner {
    ch: Option<Sender<TaskRes>>,
}

impl Runner {
    pub fn new(ch: Option<Sender<TaskRes>>) -> Runner {
        Runner { ch }
    }

    /// Do the gc job and return the count of log collected.
    fn gc_raft_log(
        &mut self,
        raft_engine: Arc<DB>,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) -> Result<u64, Error> {
        let mut first_idx = start_idx;
        if first_idx == 0 {
            let start_key = keys::raft_log_key(region_id, 0);
            first_idx = end_idx;
            if let Some((k, _)) = box_try!(raft_engine.seek(&start_key)) {
                first_idx = box_try!(keys::raft_log_index(&k));
            }
        }
        if first_idx >= end_idx {
            info!("[region {}] no need to gc", region_id);
            return Ok(0);
        }
        let raft_wb = WriteBatch::new();
        for idx in first_idx..end_idx {
            let key = keys::raft_log_key(region_id, idx);
            box_try!(raft_wb.delete(&key));
        }
        // TODO: disable WAL here.
        raft_engine.write(raft_wb).unwrap();
        Ok(end_idx - first_idx)
    }

    fn report_collected(&self, collected: u64) {
        if self.ch.is_none() {
            return;
        }
        self.ch
            .as_ref()
            .unwrap()
            .send(TaskRes { collected })
            .unwrap();
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!(
            "[region {}] execute gc log to {}",
            task.region_id, task.end_idx
        );
        match self.gc_raft_log(
            task.raft_engine,
            task.region_id,
            task.start_idx,
            task.end_idx,
        ) {
            Err(e) => {
                error!("[region {}] failed to gc: {:?}", task.region_id, e);
                self.report_collected(0);
            }
            Ok(n) => {
                debug!("[region {}] collected {} log entries", task.region_id, n);
                self.report_collected(n);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;
    use storage::CF_DEFAULT;
    use tempdir::TempDir;
    use util::rocksdb::new_engine;

    #[test]
    fn test_gc_raft_log() {
        let path = TempDir::new("gc-raft-log-test").unwrap();
        let raft_db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT], None).unwrap();
        let raft_db = Arc::new(raft_db);

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::new(Some(tx));

        // generate raft logs
        let region_id = 1;
        let raft_wb = WriteBatch::new();
        for i in 0..100 {
            let k = keys::raft_log_key(region_id, i);
            raft_wb.put(&k, b"entry").unwrap();
        }
        raft_db.write(raft_wb).unwrap();

        let tbls = vec![
            (
                Task {
                    raft_engine: Arc::clone(&raft_db),
                    region_id,
                    start_idx: 0,
                    end_idx: 10,
                },
                10,
                (0, 10),
                (10, 100),
            ),
            (
                Task {
                    raft_engine: Arc::clone(&raft_db),
                    region_id,
                    start_idx: 0,
                    end_idx: 50,
                },
                40,
                (0, 50),
                (50, 100),
            ),
            (
                Task {
                    raft_engine: Arc::clone(&raft_db),
                    region_id,
                    start_idx: 50,
                    end_idx: 50,
                },
                0,
                (0, 50),
                (50, 100),
            ),
            (
                Task {
                    raft_engine: Arc::clone(&raft_db),
                    region_id,
                    start_idx: 50,
                    end_idx: 60,
                },
                10,
                (0, 60),
                (60, 100),
            ),
        ];

        for (task, expected_collectd, not_exist_range, exist_range) in tbls {
            runner.run(task);
            let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(res.collected, expected_collectd);
            raft_log_must_not_exist(&raft_db, 1, not_exist_range.0, not_exist_range.1);
            raft_log_must_exist(&raft_db, 1, exist_range.0, exist_range.1);
        }
    }

    fn raft_log_must_not_exist(raft_engine: &DB, region_id: u64, start_idx: u64, end_idx: u64) {
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(raft_engine.get(&k).unwrap().is_none());
        }
    }

    fn raft_log_must_exist(raft_engine: &DB, region_id: u64, start_idx: u64, end_idx: u64) {
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(raft_engine.get(&k).unwrap().is_some());
        }
    }
}
