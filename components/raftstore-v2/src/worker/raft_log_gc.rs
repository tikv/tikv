// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
    sync::mpsc::Sender,
    time::Duration,
};

use engine_traits::{RaftEngine, RaftLogGcTask};
use file_system::{IoType, WithIoType};
use raftstore::store::worker::metrics::*;
use slog::{error, Logger};
use thiserror::Error;
use tikv_util::{
    box_try,
    worker::{Runnable, RunnableWithTimer},
};

const MAX_GC_REGION_BATCH: usize = 512;

pub struct Task {
    region_id: u64,
    start_idx: u64,
    end_idx: u64,
}

impl Task {
    pub fn gc(region_id: u64, start: u64, end: u64) -> Self {
        Task {
            region_id,
            start_idx: start,
            end_idx: end,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GC Raft Logs [region: {}, from: {}, to: {}]",
            self.region_id, self.start_idx, self.end_idx,
        )
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub struct Runner<ER: RaftEngine> {
    raft_engine: ER,
    tasks: Vec<Task>,
    gc_entries: Option<Sender<usize>>,
    logger: Logger,
}

impl<ER: RaftEngine> Runner<ER> {
    pub fn new(raft_engine: ER, logger: Logger) -> Self {
        Self {
            raft_engine,
            tasks: vec![],
            gc_entries: None,
            logger,
        }
    }

    fn flush(&mut self) {
        let tasks = self
            .tasks
            .drain(..)
            .map(|task| RaftLogGcTask {
                raft_group_id: task.region_id,
                from: task.start_idx,
                to: task.end_idx,
            })
            .collect();
        match self.gc_raft_log(tasks) {
            Ok(n) => self.report_collected(n),
            Err(e) => error!(self.logger, "failed to gc raft log"; "err" => ?e),
        }
    }

    /// Does the GC job and returns the count of logs collected.
    fn gc_raft_log(&mut self, regions: Vec<RaftLogGcTask>) -> Result<usize, Error> {
        fail::fail_point!("worker_gc_raft_log", |s| {
            Ok(s.and_then(|s| s.parse().ok()).unwrap_or(0))
        });
        let deleted = box_try!(self.raft_engine.batch_gc(regions));
        fail::fail_point!("worker_gc_raft_log_finished", |_| { Ok(deleted) });
        Ok(deleted)
    }

    fn report_collected(&self, collected: usize) {
        if let Some(ref ch) = self.gc_entries {
            ch.send(collected).unwrap();
        }
    }
}

impl<ER> Runnable for Runner<ER>
where
    ER: RaftEngine,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        let _io_type_guard = WithIoType::new(IoType::ForegroundWrite);
        self.tasks.push(task);
        if self.tasks.len() > MAX_GC_REGION_BATCH {
            self.flush();
        }
    }
}

impl<ER> RunnableWithTimer for Runner<ER>
where
    ER: RaftEngine,
{
    fn on_timeout(&mut self) {
        self.flush();
    }

    fn get_interval(&self) -> Duration {
        Duration::from_secs(2)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use engine_traits::{RaftEngine, RaftLogBatch, ALL_CFS};
    use raft::eraftpb::Entry;
    use slog::o;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_gc_raft_log() {
        let dir = Builder::new().prefix("gc-raft-log-test").tempdir().unwrap();
        let path_raft = dir.path().join("raft");
        let path_kv = dir.path().join("kv");
        let raft_db = engine_test::raft::new_engine(path_kv.to_str().unwrap(), None).unwrap();

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner {
            raft_engine: raft_db.clone(),
            tasks: vec![],
            gc_entries: Some(tx),
            logger: slog_global::borrow_global().new(o!()),
        };

        // generate raft logs
        let region_id = 1;
        let mut raft_wb = raft_db.log_batch(0);
        for i in 0..100 {
            let mut e = Entry::new();
            e.set_index(i);
            raft_wb.append(region_id, vec![e]).unwrap();
        }
        raft_db.consume(&mut raft_wb, false /* sync */).unwrap();

        let tbls = vec![
            (Task::gc(region_id, 0, 10), 10, (0, 10), (10, 100)),
            (Task::gc(region_id, 0, 50), 40, (0, 50), (50, 100)),
            (Task::gc(region_id, 50, 50), 0, (0, 50), (50, 100)),
            (Task::gc(region_id, 50, 60), 10, (0, 60), (60, 100)),
        ];

        for (task, expected_collectd, not_exist_range, exist_range) in tbls {
            runner.run(task);
            runner.flush();
            let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(res, expected_collectd);
            raft_log_must_not_exist(&raft_db, 1, not_exist_range.0, not_exist_range.1);
            raft_log_must_exist(&raft_db, 1, exist_range.0, exist_range.1);
        }
    }

    fn raft_log_must_not_exist(
        raft_engine: &impl RaftEngine,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) {
        for i in start_idx..end_idx {
            assert!(raft_engine.get_entry(region_id, i).unwrap().is_none());
        }
    }

    fn raft_log_must_exist(
        raft_engine: &impl RaftEngine,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) {
        for i in start_idx..end_idx {
            assert!(raft_engine.get_entry(region_id, i).unwrap().is_some());
        }
    }
}
