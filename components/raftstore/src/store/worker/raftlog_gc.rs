// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::Sender;

use engine_traits::{Engines, KvEngine};
use raft_engine::RaftEngine;
use tikv_util::time::Duration;
use tikv_util::timer::Timer;
use tikv_util::worker::{Runnable, RunnableWithTimer};

const MAX_GC_REGION_BATCH: usize = 128;
const COMPACT_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub struct Task {
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
            display("raftlog gc failed {:?}", err)
        }
    }
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    ch: Option<Sender<TaskRes>>,
    tasks: Vec<Task>,
    engines: Engines<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(ch: Option<Sender<TaskRes>>, engines: Engines<EK, ER>) -> Runner<EK, ER> {
        Runner {
            ch,
            engines,
            tasks: vec![],
        }
    }

    /// Does the GC job and returns the count of logs collected.
    fn gc_raft_log(
        &mut self,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) -> Result<usize, Error> {
        let deleted = box_try!(self.engines.raft.gc(region_id, start_idx, end_idx));
        Ok(deleted)
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

    fn flush(&mut self) {
        // Sync wal of kv_db to make sure the data before apply_index has been persisted to disk.
        self.engines.kv.sync().unwrap_or_else(|e| {
            panic!("failed to sync kv_engine in raft_log_gc: {:?}", e);
        });
        let tasks = std::mem::replace(&mut self.tasks, vec![]);
        for t in tasks {
            debug!(
                "execute gc log";
                "region_id" => t.region_id,
                "end_index" => t.end_idx,
            );
            match self.gc_raft_log(t.region_id, t.start_idx, t.end_idx) {
                Err(e) => {
                    error!("failed to gc"; "region_id" => t.region_id, "err" => %e);
                    self.report_collected(0 as u64);
                }
                Ok(n) => {
                    debug!("collected log entries"; "region_id" => t.region_id, "entry_count" => n);
                    self.report_collected(n as u64);
                }
            }
        }
    }

    pub fn new_timer(&self) -> Timer<()> {
        let mut timer = Timer::new(1);
        timer.add_task(COMPACT_LOG_INTERVAL, ());
        timer
    }
}

impl<EK: KvEngine, ER: RaftEngine> Runnable<Task> for Runner<EK, ER> {
    fn run(&mut self, task: Task) {
        self.tasks.push(task);
        if self.tasks.len() < MAX_GC_REGION_BATCH {
            return;
        }
        self.flush();
    }

    fn shutdown(&mut self) {
        self.flush();
    }
}

impl<EK, ER> RunnableWithTimer<Task, ()> for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        self.flush();
        timer.add_task(COMPACT_LOG_INTERVAL, ());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::util::new_engine;
    use engine_traits::{Engines, KvEngine, Mutable, WriteBatchExt, ALL_CFS, CF_DEFAULT};
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::Builder;

    #[test]
    fn test_gc_raft_log() {
        let dir = Builder::new().prefix("gc-raft-log-test").tempdir().unwrap();
        let path_raft = dir.path().join("raft");
        let path_kv = dir.path().join("kv");
        let raft_db = new_engine(path_kv.to_str().unwrap(), None, &[CF_DEFAULT], None).unwrap();
        let kv_db = new_engine(path_raft.to_str().unwrap(), None, ALL_CFS, None).unwrap();
        let engines = Engines::new(kv_db, raft_db.clone(), false);

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::new(Some(tx), engines);

        // generate raft logs
        let region_id = 1;
        let mut raft_wb = raft_db.write_batch();
        for i in 0..100 {
            let k = keys::raft_log_key(region_id, i);
            raft_wb.put(&k, b"entry").unwrap();
        }
        raft_db.write(&raft_wb).unwrap();

        let tbls = vec![
            (
                Task {
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
            runner.flush();
            let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(res.collected, expected_collectd);
            raft_log_must_not_exist(&raft_db, 1, not_exist_range.0, not_exist_range.1);
            raft_log_must_exist(&raft_db, 1, exist_range.0, exist_range.1);
        }
    }

    fn raft_log_must_not_exist(
        raft_engine: &impl KvEngine,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) {
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(raft_engine.get_value(&k).unwrap().is_none());
        }
    }

    fn raft_log_must_exist(
        raft_engine: &impl KvEngine,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) {
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(raft_engine.get_value(&k).unwrap().is_some());
        }
    }
}
