// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::Sender;

use thiserror::Error;

use engine_traits::{Engines, KvEngine, RaftEngine, RaftLogGCTask};
use file_system::{IOType, WithIOType};
use tikv_util::time::{Duration, Instant};
use tikv_util::worker::{Runnable, RunnableWithTimer};
use tikv_util::{box_try, debug, error, warn};

use crate::store::worker::metrics::*;

const MAX_GC_REGION_BATCH: usize = 512;
const MAX_REGION_NORMAL_GC_LOG_NUBER: u64 = 10240;

pub enum Task {
    Gc {
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    },
}

impl Task {
    pub fn gc(region_id: u64, start: u64, end: u64) -> Self {
        Task::Gc {
            region_id,
            start_idx: start,
            end_idx: end,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Gc {
                region_id,
                start_idx,
                end_idx,
            } => write!(
                f,
                "GC Raft Logs [region: {}, from: {}, to: {}]",
                region_id, start_idx, end_idx
            ),
        }
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
    gc_entries: Option<Sender<usize>>,
    compact_sync_interval: Duration,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(engines: Engines<EK, ER>, compact_log_interval: Duration) -> Runner<EK, ER> {
        Runner {
            engines,
            tasks: vec![],
            gc_entries: None,
            compact_sync_interval: compact_log_interval,
        }
    }

    /// Does the GC job and returns the count of logs collected.
    fn gc_raft_log(&mut self, regions: Vec<RaftLogGCTask>) -> Result<usize, Error> {
        let deleted = box_try!(self.engines.raft.batch_gc(regions));
        Ok(deleted)
    }

    fn report_collected(&self, collected: usize) {
        if let Some(ref ch) = self.gc_entries {
            ch.send(collected).unwrap();
        }
    }

    fn flush(&mut self) {
        if self.tasks.is_empty() {
            return;
        }
        // Sync wal of kv_db to make sure the data before apply_index has been persisted to disk.
        let start = Instant::now();
        self.engines.kv.sync().unwrap_or_else(|e| {
            panic!("failed to sync kv_engine in raft_log_gc: {:?}", e);
        });
        RAFT_LOG_GC_KV_SYNC_DURATION_HISTOGRAM.observe(start.saturating_elapsed_secs());
        let tasks = std::mem::take(&mut self.tasks);
        let mut groups = Vec::with_capacity(tasks.len());
        for t in tasks {
            match t {
                Task::Gc {
                    region_id,
                    start_idx,
                    end_idx,
                } => {
                    debug!("gc raft log"; "region_id" => region_id, "end_index" => end_idx);
                    if start_idx == 0 {
                        RAFT_LOG_GC_SEEK_OPERATIONS.inc();
                    } else if end_idx > start_idx + MAX_REGION_NORMAL_GC_LOG_NUBER {
                        warn!("gc raft log with a large range"; "region_id" => region_id,
                            "start_index" => start_idx,
                            "end_index" => end_idx);
                    }
                    groups.push(RaftLogGCTask {
                        raft_group_id: region_id,
                        from: start_idx,
                        to: end_idx,
                    });
                }
            }
        }
        let start = Instant::now();
        match self.gc_raft_log(groups) {
            Err(e) => {
                error!("failed to gc"; "err" => %e);
                self.report_collected(0);
                RAFT_LOG_GC_FAILED.inc();
            }
            Ok(n) => {
                debug!("gc log entries";  "entry_count" => n);
                self.report_collected(n);
                RAFT_LOG_GC_DELETED_KEYS_HISTOGRAM.observe(n as f64);
            }
        }
        RAFT_LOG_GC_WRITE_DURATION_HISTOGRAM.observe(start.saturating_elapsed_secs());
    }
}

impl<EK, ER> Runnable for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        let _io_type_guard = WithIOType::new(IOType::ForegroundWrite);
        self.tasks.push(task);
        if self.tasks.len() > MAX_GC_REGION_BATCH {
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
    use super::*;
    use engine_traits::{KvEngine, Mutable, WriteBatch, WriteBatchExt, ALL_CFS, CF_DEFAULT};
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::Builder;

    #[test]
    fn test_gc_raft_log() {
        let dir = Builder::new().prefix("gc-raft-log-test").tempdir().unwrap();
        let path_raft = dir.path().join("raft");
        let path_kv = dir.path().join("kv");
        let raft_db =
            engine_test::raft::new_engine(path_kv.to_str().unwrap(), None, CF_DEFAULT, None)
                .unwrap();
        let kv_db =
            engine_test::kv::new_engine(path_raft.to_str().unwrap(), None, ALL_CFS, None).unwrap();
        let engines = Engines::new(kv_db, raft_db.clone());

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner {
            gc_entries: Some(tx),
            engines,
            tasks: vec![],
            compact_sync_interval: Duration::from_secs(5),
        };

        // generate raft logs
        let region_id = 1;
        let mut raft_wb = raft_db.write_batch();
        for i in 0..100 {
            let k = keys::raft_log_key(region_id, i);
            raft_wb.put(&k, b"entry").unwrap();
        }
        raft_wb.write().unwrap();

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
