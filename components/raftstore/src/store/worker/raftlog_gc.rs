// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::Sender;
use std::sync::Arc;

use engine_traits::MAX_DELETE_BATCH_SIZE;
use engine::Iterable;
use engine::DB;
use engine_rocks::Compat;
use engine_traits::{Mutable, WriteBatch, WriteBatchExt};
use tikv_util::worker::Runnable;

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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
        Other(err: Box<dyn error::Error + Sync + Send>) {
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

    /// Does the GC job and returns the count of logs collected.
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
            info!("no need to gc"; "region_id" => region_id);
            return Ok(0);
        }
        let raft_wb = raft_engine.c().write_batch();
        for idx in first_idx..end_idx {
            let key = keys::raft_log_key(region_id, idx);
            box_try!(raft_wb.delete(&key));
            if raft_wb.data_size() >= MAX_DELETE_BATCH_SIZE {
                // Avoid large write batch to reduce latency.
                raft_engine.c().write(&raft_wb).unwrap();
                raft_wb.clear();
            }
        }
        // TODO: disable WAL here.
        if !raft_wb.is_empty() {
            raft_engine.c().write(&raft_wb).unwrap();
        }
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
            "execute gc log";
            "region_id" => task.region_id,
            "end_index" => task.end_idx,
        );
        match self.gc_raft_log(
            task.raft_engine,
            task.region_id,
            task.start_idx,
            task.end_idx,
        ) {
            Err(e) => {
                error!("failed to gc"; "region_id" => task.region_id, "err" => %e);
                self.report_collected(0);
            }
            Ok(n) => {
                debug!("collected log entries"; "region_id" => task.region_id, "entry_count" => n);
                self.report_collected(n);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::rocks::util::new_engine;
    use engine_traits::CF_DEFAULT;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::Builder;

    #[test]
    fn test_gc_raft_log() {
        let path = Builder::new().prefix("gc-raft-log-test").tempdir().unwrap();
        let raft_db = new_engine(path.path().to_str().unwrap(), None, &[CF_DEFAULT], None).unwrap();
        let raft_db = Arc::new(raft_db);

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::new(Some(tx));

        // generate raft logs
        let region_id = 1;
        let raft_wb = raft_db.c().write_batch();
        for i in 0..100 {
            let k = keys::raft_log_key(region_id, i);
            raft_wb.put(&k, b"entry").unwrap();
        }
        raft_db.c().write(&raft_wb).unwrap();

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
