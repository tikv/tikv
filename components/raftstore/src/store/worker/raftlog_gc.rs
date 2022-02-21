// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::Sender;

use thiserror::Error;

use engine_traits::{Engines, KvEngine, RaftEngine, RaftLogGCTask};
use file_system::{IOType, WithIOType};
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer};
use tikv_util::{box_try, debug, error, warn};

use crate::store::{CasualMessage, CasualRouter};

const MAX_GC_REGION_BATCH: usize = 512;
const MAX_REGION_NORMAL_GC_LOG_NUMBER: u64 = 10240;

pub struct TaskGcItem {
    region_id: u64,
    start_idx: u64,
    end_idx: u64,
    flush: bool,
    cb: Option<Box<dyn FnOnce() + Send>>,
}

pub enum Task {
    Gc(TaskGcItem),
    Purge,
}

impl Task {
    pub fn gc(region_id: u64, start: u64, end: u64) -> Self {
        Task::Gc(TaskGcItem {
            region_id,
            start_idx: start,
            end_idx: end,
            flush: false,
            cb: None,
        })
    }

    pub fn flush(mut self) -> Self {
        if let Task::Gc(t) = &mut self {
            t.flush = true;
        }
        self
    }

    pub fn when_done(mut self, callback: impl FnOnce() + Send + 'static) -> Self {
        if let Task::Gc(t) = &mut self {
            t.cb = Some(Box::new(callback));
        }
        self
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Gc(t) => write!(
                f,
                "GC Raft Logs [region: {}, from: {}, to: {}, has_cb: {}]",
                t.region_id,
                t.start_idx,
                t.end_idx,
                t.cb.is_some()
            ),
            Task::Purge => write!(f, "Purge Expired Files",),
        }
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("raftlog gc failed {0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub struct Runner<EK: KvEngine, ER: RaftEngine, R: CasualRouter<EK>> {
    ch: R,
    tasks: Vec<Task>,
    engines: Engines<EK, ER>,
    gc_entries: Option<Sender<usize>>,
    compact_sync_interval: Duration,
}

impl<EK: KvEngine, ER: RaftEngine, R: CasualRouter<EK>> Runner<EK, ER, R> {
    pub fn new(
        ch: R,
        engines: Engines<EK, ER>,
        compact_log_interval: Duration,
    ) -> Runner<EK, ER, R> {
        Runner {
            ch,
            engines,
            tasks: vec![],
            gc_entries: None,
            compact_sync_interval: compact_log_interval,
        }
    }

    /// Does the GC job and returns the count of logs collected.
    fn gc_raft_log(&mut self, regions: Vec<RaftLogGCTask>) -> Result<usize, Error> {
        fail::fail_point!("worker_gc_raft_log", |s| {
            Ok(s.and_then(|s| s.parse().ok()).unwrap_or(0))
        });
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
        if self.tasks.is_empty() {
            return;
        }
        self.engines.kv.sync().unwrap_or_else(|e| {
            panic!("failed to sync kv_engine in raft_log_gc: {:?}", e);
        });
        let tasks = std::mem::take(&mut self.tasks);
        let mut groups = Vec::with_capacity(tasks.len());
        let mut cbs = Vec::new();
        let mut need_purge = false;
        for t in tasks {
            match t {
                Task::Gc(t) => {
                    debug!("gc raft log"; "region_id" => t.region_id, "start_index" => t.start_idx, "end_index" => t.end_idx);
                    if let Some(cb) = t.cb {
                        cbs.push(cb);
                    }
                    if t.start_idx == t.end_idx {
                        // It's only for flush.
                        continue;
                    }
                    if t.start_idx != 0 && t.end_idx > t.start_idx + MAX_REGION_NORMAL_GC_LOG_NUMBER
                    {
                        warn!(
                            "gc raft log with a large range";
                            "region_id" => t.region_id,
                            "start_index" => t.start_idx,
                            "end_index" => t.end_idx);
                    }
                    groups.push(RaftLogGCTask {
                        raft_group_id: t.region_id,
                        from: t.start_idx,
                        to: t.end_idx,
                    });
                }
                Task::Purge => {
                    need_purge = true;
                }
            }
        }
        match self.gc_raft_log(groups) {
            Err(e) => {
                error!("failed to gc"; "err" => %e);
                self.report_collected(0);
            }
            Ok(n) => {
                debug!("gc log entries";  "entry_count" => n);
                self.report_collected(n);
            }
        }
        for cb in cbs {
            cb()
        }
        if !need_purge {
            return;
        }
        let regions = match self.engines.raft.purge_expired_files() {
            Ok(regions) => regions,
            Err(e) => {
                warn!("purge expired files"; "err" => %e);
                return;
            }
        };
        for region_id in regions {
            let _ = self.ch.send(region_id, CasualMessage::ForceCompactRaftLogs);
        }
    }
}

impl<EK, ER, R> Runnable for Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        let _io_type_guard = WithIOType::new(IOType::ForegroundWrite);
        let flush_now = match &task {
            Task::Gc(t) => t.flush,
            _ => false,
        };
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

impl<EK, ER, R> RunnableWithTimer for Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK>,
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
        let (r, _) = mpsc::sync_channel(1);
        let mut runner = Runner {
            gc_entries: Some(tx),
            engines,
            ch: r,
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
