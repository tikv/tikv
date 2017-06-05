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
use std::sync::mpsc::Sender;
use std::ops::{Deref, DerefMut};

pub struct Task {
    pub engine: Arc<DB>,
    pub region_id: u64,
    pub start_idx: u64,
    pub end_idx: u64,
}

pub struct Tasks(Vec<Task>);

impl Deref for Tasks {
    type Target = Vec<Task>;

    fn deref(&self) -> &Vec<Task> {
        &self.0
    }
}

impl DerefMut for Tasks {
    fn deref_mut(&mut self) -> &mut Vec<Task> {
        &mut self.0
    }
}

pub struct TaskRes {
    pub collected: u64,
}

impl Display for Tasks {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Count of GC Raft Log Tasks: {}", self.len())
    }
}

impl Tasks {
    pub fn from_vec(tasks: Vec<Task>) -> Tasks {
        Tasks(tasks)
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
        Runner { ch: ch }
    }

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
        // TODO: disable WAL here.
        engine.write(wb).unwrap();
        Ok(end_idx - first_idx)
    }

    fn report_collected(&self, collected: u64) {
        if self.ch.is_none() {
            return;
        }
        self.ch.as_ref().unwrap().send(TaskRes { collected: collected }).unwrap();
    }
}

impl Runnable<Tasks> for Runner {
    fn run(&mut self, mut tasks: Tasks) {
        for task in tasks.drain(..) {
            debug!("[region {}] execute gc log to {}",
                   task.region_id,
                   task.end_idx);
            match self.gc_raft_log(task.engine, task.region_id, task.start_idx, task.end_idx) {
                Err(e) => {
                    error!("[region {}] failed to gc: {:?}", task.region_id, e);
                    self.report_collected(0);
                }
                Ok(n) => {
                    info!("[region {}] collected {} log entries", task.region_id, n);
                    self.report_collected(n);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc;
    use std::time::Duration;
    use util::rocksdb::new_engine;
    use tempdir::TempDir;
    use storage::{CF_DEFAULT, CF_RAFT};
    use super::*;

    #[test]
    fn test_gc_raft_log() {
        let path = TempDir::new("gc-raft-log-test").unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT, CF_RAFT]).unwrap();
        let db = Arc::new(db);

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::new(Some(tx));

        // generate raft logs
        let raft_handle = rocksdb::get_cf_handle(&db, CF_RAFT).unwrap();
        let region_id = 1;
        let wb = WriteBatch::new();
        for i in 0..100 {
            let k = keys::raft_log_key(region_id, i);
            wb.put_cf(raft_handle, &k, b"entry").unwrap();
        }
        db.write(wb).unwrap();

        let tbls = vec![(task_to_tasks(Task {
                            engine: db.clone(),
                            region_id: region_id,
                            start_idx: 0,
                            end_idx: 10,
                        }),
                         10,
                         (0, 10),
                         (10, 100)),
                        (task_to_tasks(Task {
                            engine: db.clone(),
                            region_id: region_id,
                            start_idx: 0,
                            end_idx: 50,
                        }),
                         40,
                         (0, 50),
                         (50, 100)),
                        (task_to_tasks(Task {
                            engine: db.clone(),
                            region_id: region_id,
                            start_idx: 50,
                            end_idx: 50,
                        }),
                         0,
                         (0, 50),
                         (50, 100)),
                        (task_to_tasks(Task {
                            engine: db.clone(),
                            region_id: region_id,
                            start_idx: 50,
                            end_idx: 60,
                        }),
                         10,
                         (0, 60),
                         (60, 100))];

        for (task, expected_collectd, not_exist_range, exist_range) in tbls {
            runner.run(task);
            let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(res.collected, expected_collectd);
            raft_log_must_not_exist(&db, 1, not_exist_range.0, not_exist_range.1);
            raft_log_must_exist(&db, 1, exist_range.0, exist_range.1);
        }
    }

    fn raft_log_must_not_exist(engine: &DB, region_id: u64, start_idx: u64, end_idx: u64) {
        let raft_handle = rocksdb::get_cf_handle(engine, CF_RAFT).unwrap();
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(engine.get_cf(raft_handle, &k).unwrap().is_none());
        }
    }

    fn raft_log_must_exist(engine: &DB, region_id: u64, start_idx: u64, end_idx: u64) {
        let raft_handle = rocksdb::get_cf_handle(engine, CF_RAFT).unwrap();
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(engine.get_cf(raft_handle, &k).unwrap().is_some());
        }
    }

    fn task_to_tasks(task: Task) -> Tasks {
        Tasks::from_vec(vec![task])
    }
}
