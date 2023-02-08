// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

use engine_traits::{Engines, KvEngine, RaftEngine};
use file_system::{IoType, WithIoType};
use thiserror::Error;
use tikv_util::{
    box_try, debug, error,
    time::{Duration, Instant},
    warn,
    worker::{Runnable, RunnableWithTimer},
};

use crate::store::worker::metrics::*;

const MAX_GC_REGION_BATCH: usize = 512;
const MAX_REGION_NORMAL_GC_LOG_NUMBER: u64 = 10240;

pub struct Task {
    region_id: u64,
    start_idx: u64,
    end_idx: u64,
    flush: bool,
    cb: Option<Box<dyn FnOnce() + Send>>,
}

impl Task {
    pub fn gc(region_id: u64, start: u64, end: u64) -> Self {
        Task {
            region_id,
            start_idx: start,
            end_idx: end,
            flush: false,
            cb: None,
        }
    }

    pub fn flush(mut self) -> Self {
        self.flush = true;
        self
    }

    pub fn when_done(mut self, callback: impl FnOnce() + Send + 'static) -> Self {
        self.cb = Some(Box::new(callback));
        self
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GC Raft Logs [region: {}, from: {}, to: {}, has_cb: {}]",
            self.region_id,
            self.start_idx,
            self.end_idx,
            self.cb.is_some()
        )
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("raftlog gc failed {0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    tasks: Vec<Task>,
    engines: Engines<EK, ER>,
    compact_sync_interval: Duration,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(engines: Engines<EK, ER>, compact_log_interval: Duration) -> Runner<EK, ER> {
        Runner {
            engines,
            tasks: vec![],
            compact_sync_interval: compact_log_interval,
        }
    }

    fn raft_log_gc(&mut self, mut batch: ER::LogBatch) -> Result<(), Error> {
        fail::fail_point!("worker_gc_raft_log", |_| Ok(()));
        box_try!(self.engines.raft.consume(&mut batch, false));
        fail::fail_point!("worker_gc_raft_log_finished");
        Ok(())
    }

    fn flush(&mut self) {
        if self.tasks.is_empty() {
            return;
        }
        fail::fail_point!("worker_gc_raft_log_flush");
        // Sync wal of kv_db to make sure the data before apply_index has been persisted
        // to disk.
        let start = Instant::now();
        self.engines.kv.sync().unwrap_or_else(|e| {
            panic!("failed to sync kv_engine in raft_log_gc: {:?}", e);
        });
        RAFT_LOG_GC_KV_SYNC_DURATION_HISTOGRAM.observe(start.saturating_elapsed_secs());

        let tasks = std::mem::take(&mut self.tasks);
        let mut cbs = Vec::new();
        let mut batch = self.engines.raft.log_batch(tasks.len());
        let start = Instant::now();
        for t in tasks {
            debug!("gc raft log"; "region_id" => t.region_id, "start_index" => t.start_idx, "end_index" => t.end_idx);
            if let Some(cb) = t.cb {
                cbs.push(cb);
            }
            if t.start_idx == t.end_idx {
                // It's only for flush.
                continue;
            }
            if t.start_idx == 0 {
                RAFT_LOG_GC_SEEK_OPERATIONS.inc();
            } else if t.end_idx > t.start_idx + MAX_REGION_NORMAL_GC_LOG_NUMBER {
                warn!(
                    "gc raft log with a large range";
                    "region_id" => t.region_id,
                    "start_index" => t.start_idx,
                    "end_index" => t.end_idx,
                );
            }
            if let Err(e) = self
                .engines
                .raft
                .gc(t.region_id, t.start_idx, t.end_idx, &mut batch)
            {
                error!("failed to gc"; "err" => %e);
                RAFT_LOG_GC_FAILED.inc();
            }
        }
        if let Err(e) = self.raft_log_gc(batch) {
            error!("failed to write gc task"; "err" => %e);
            RAFT_LOG_GC_FAILED.inc();
        }
        RAFT_LOG_GC_WRITE_DURATION_HISTOGRAM.observe(start.saturating_elapsed_secs());
        for cb in cbs {
            cb();
        }
    }
}

impl<EK, ER> Runnable for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        let _io_type_guard = WithIoType::new(IoType::ForegroundWrite);
        let flush_now = task.flush;
        self.tasks.push(task);
        // TODO: maybe they should also be batched even `flush_now` is true.
        if flush_now || self.tasks.len() > MAX_GC_REGION_BATCH {
            self.flush();
        }
    }

    fn shutdown(&mut self) {
        self.flush();
    }
}

impl<EK, ER> RunnableWithTimer for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn on_timeout(&mut self) {
        self.flush();
    }

    fn get_interval(&self) -> Duration {
        self.compact_sync_interval
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use engine_traits::{RaftEngine, RaftLogBatch, ALL_CFS};
    use raft::eraftpb::Entry;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_gc_raft_log() {
        let dir = Builder::new().prefix("gc-raft-log-test").tempdir().unwrap();
        let path_raft = dir.path().join("raft");
        let path_kv = dir.path().join("kv");
        let raft_db = engine_test::raft::new_engine(path_kv.to_str().unwrap(), None).unwrap();
        let kv_db = engine_test::kv::new_engine(path_raft.to_str().unwrap(), ALL_CFS).unwrap();
        let engines = Engines::new(kv_db, raft_db.clone());

        let mut runner = Runner {
            engines,
            tasks: vec![],
            compact_sync_interval: Duration::from_secs(5),
        };

        // generate raft logs
        let region_id = 1;
        let mut raft_wb = raft_db.log_batch(0);
        for i in 0..100 {
            let mut e = Entry::new();
            e.set_index(i);
            raft_wb.append(region_id, None, vec![e]).unwrap();
        }
        raft_db.consume(&mut raft_wb, false /* sync */).unwrap();

        let tbls = vec![
            (Task::gc(region_id, 0, 10), (0, 10), (10, 100)),
            (Task::gc(region_id, 0, 50), (0, 50), (50, 100)),
            (Task::gc(region_id, 50, 50), (0, 50), (50, 100)),
            (Task::gc(region_id, 50, 60), (0, 60), (60, 100)),
        ];

        for (task, not_exist_range, exist_range) in tbls {
            runner.run(task);
            runner.flush();
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
