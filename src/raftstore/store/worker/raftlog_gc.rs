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

pub struct Task {
    pub engine: Arc<DB>,
    pub region_id: u64,
    pub start_idx: u64,
    pub end_idx: u64,
}

pub struct TaskRes {
    pub collected: u64,
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
        // It's not safe to disable WAL here.
        engine.write(wb).unwrap();
        Ok(end_idx - first_idx)
    }

    fn finish_task(&mut self, collected: u64) {
        if self.ch.is_none() {
            return;
        }
        self.ch.as_mut().unwrap().send(TaskRes { collected: collected }).unwrap();
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        debug!("[region {}] execute gc log to {}",
               task.region_id,
               task.end_idx);
        match self.gc_raft_log(task.engine, task.region_id, task.start_idx, task.end_idx) {
            Err(e) => {
                error!("[region {}] failed to gc: {:?}", task.region_id, e);
                self.finish_task(0);
            }
            Ok(n) => {
                info!("[region {}] collected {} log entries", task.region_id, n);
                self.finish_task(n);
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

        // gc 0..10
        runner.run(Task {
            engine: db.clone(),
            region_id: region_id,
            start_idx: 0,
            end_idx: 10,
        });
        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(res.collected, 10);

        // gc 10..50
        runner.run(Task {
            engine: db.clone(),
            region_id: region_id,
            start_idx: 0,
            end_idx: 50,
        });
        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(res.collected, 40);

        // gc nothing
        runner.run(Task {
            engine: db.clone(),
            region_id: region_id,
            start_idx: 50,
            end_idx: 50,
        });
        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(res.collected, 0);

        // 50..60
        runner.run(Task {
            engine: db.clone(),
            region_id: region_id,
            start_idx: 50,
            end_idx: 60,
        });
        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(res.collected, 10);
    }
}
