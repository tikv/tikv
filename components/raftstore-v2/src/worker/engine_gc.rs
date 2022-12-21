// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
    sync::mpsc::Sender,
    time::Duration,
};

use engine_traits::{RaftEngine, RaftLogGcTask};
use file_system::{IoType, WithIoType};
use slog::{error, Logger};
use thiserror::Error;
use tikv_util::worker::{Runnable, RunnableWithTimer};

const MAX_GC_REGION_BATCH: usize = 512;

pub enum Task {
    RaftLogGc {
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    },
}

impl Task {
    pub fn raft_log_gc(region_id: u64, start_idx: u64, end_idx: u64) -> Self {
        Task::RaftLogGc {
            region_id,
            start_idx,
            end_idx,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::RaftLogGc {
                region_id,
                start_idx,
                end_idx,
            } => write!(
                f,
                "RaftLogGc [region: {}, from: {}, to: {}]",
                region_id, start_idx, end_idx,
            ),
        }
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub struct Runner<ER: RaftEngine> {
    raft_engine: ER,
    raft_log_gc_tasks: Vec<RaftLogGcTask>,
    gc_entries: Option<Sender<usize>>,
    logger: Logger,
}

impl<ER: RaftEngine> Runner<ER> {
    pub fn new(raft_engine: ER, logger: Logger) -> Self {
        Self {
            raft_engine,
            raft_log_gc_tasks: vec![],
            gc_entries: None,
            logger,
        }
    }

    fn flush(&mut self) {
        let tasks = std::mem::take(&mut self.raft_log_gc_tasks);
        match self.raft_engine.batch_gc(tasks) {
            Ok(deleted) => self.report_collected(deleted),
            Err(e) => error!(self.logger, "failed to gc raft log"; "err" => ?e),
        }
        // TODO: clean up raft states.
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
        match task {
            Task::RaftLogGc {
                region_id,
                start_idx,
                end_idx,
            } => {
                self.raft_log_gc_tasks.push(RaftLogGcTask {
                    raft_group_id: region_id,
                    from: start_idx,
                    to: end_idx,
                });
                if self.raft_log_gc_tasks.len() > MAX_GC_REGION_BATCH {
                    self.flush();
                }
            }
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

    use engine_traits::{RaftEngine, RaftLogBatch};
    use raft::eraftpb::Entry;
    use slog::o;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_gc_raft_log() {
        let dir = Builder::new().prefix("gc-raft-log-test").tempdir().unwrap();
        let path_raft = dir.path().join("raft");
        let raft_db = engine_test::raft::new_engine(path_raft.to_str().unwrap(), None).unwrap();

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner {
            raft_engine: raft_db.clone(),
            raft_log_gc_tasks: vec![],
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
            (Task::raft_log_gc(region_id, 0, 10), 10, (0, 10), (10, 100)),
            (Task::raft_log_gc(region_id, 0, 50), 40, (0, 50), (50, 100)),
            (Task::raft_log_gc(region_id, 50, 50), 0, (0, 50), (50, 100)),
            (Task::raft_log_gc(region_id, 50, 60), 10, (0, 60), (60, 100)),
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
