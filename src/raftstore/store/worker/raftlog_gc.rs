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

use crate::raftengine::{RaftEngine, Result as RaftEngineResult};
use crate::raftstore::store::fsm::RaftRouter;
use crate::raftstore::store::msg::PeerMsg;
use crate::util::worker::Runnable;

use std::error;
use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub enum Task {
    RegionTask(RegionTask),
    EngineTask(EngineTask),
}

pub struct RegionTask {
    pub raft_engine: Arc<RaftEngine>,
    pub region_id: u64,
    pub compact_to: u64,
}

#[derive(Debug)]
pub struct EngineTask {
    pub raft_engine: Arc<RaftEngine>,
}

pub struct TaskRes {
    pub collected: u64,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::RegionTask(ref task) => write!(
                f,
                "GC Raft Log Task [region: {}, compact to: {}]",
                task.region_id, task.compact_to
            ),
            Task::EngineTask(ref task) => write!(
                f,
                "Gc raft engine expired files Task, engine info {:?}",
                task
            ),
        }
    }
}

impl Task {
    pub fn region_task(raft_engine: Arc<RaftEngine>, region_id: u64, compact_to: u64) -> Self {
        Task::RegionTask(RegionTask {
            raft_engine,
            region_id,
            compact_to,
        })
    }

    pub fn engine_task(raft_engine: Arc<RaftEngine>) -> Self {
        Task::EngineTask(EngineTask { raft_engine })
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
    router: Option<RaftRouter>,
}

impl Runner {
    pub fn new(ch: Option<Sender<TaskRes>>, router: Option<RaftRouter>) -> Runner {
        Runner { ch, router }
    }

    /// Does the GC job and returns the count of logs collected.
    fn gc_raft_log(
        &mut self,
        raft_engine: Arc<RaftEngine>,
        region_id: u64,
        compact_to: u64,
    ) -> Result<u64, Error> {
        let collected = raft_engine.compact_to(region_id, compact_to);
        self.report_collected(collected);
        Ok(collected)
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

    fn gc_expired_files(&mut self, raft_engine: Arc<RaftEngine>) -> RaftEngineResult<()> {
        // Rewrite inactive regions' entries to new file, so the old
        // files can be dropped ASAP.
        if raft_engine.rewrite_inactive() {
            raft_engine.sync_data()?;
        }

        // Collect regions that need force compact.
        let mut regions = raft_engine.regions_need_force_compact();
        if self.router.is_some() {
            for region_id in regions.drain() {
                if let Err(e) = self
                    .router
                    .as_ref()
                    .unwrap()
                    .send(region_id, PeerMsg::ForceCompactRaftLog)
                {
                    error!("Send force compact raft log failed, error {:?}", e);
                }
            }
        }

        // Evict old entries from cache, keep cache below limited size.
        raft_engine.evict_old_from_cache();

        // Purge old files.
        raft_engine.purge_expired_files()
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::RegionTask(task) => {
                let n = self.gc_raft_log(task.raft_engine, task.region_id, task.compact_to);
                debug!("[region {}] collected {:?} log entries", task.region_id, n);
            }
            Task::EngineTask(task) => {
                if let Err(e) = self.gc_expired_files(task.raft_engine) {
                    error!("GC expired files for raft engine error {:?}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raftengine::{Config as RaftEngineCfg, LogBatch, RaftEngine};
    use raft::eraftpb::Entry;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempdir::TempDir;

    #[test]
    fn test_gc_raft_log() {
        let path = TempDir::new("gc-raft-log-test").unwrap();
        let mut raft_cfg = RaftEngineCfg::new();
        raft_cfg.dir = String::from(path.path().to_str().unwrap());
        let raft_engine = Arc::new(RaftEngine::new(raft_cfg));

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::new(Some(tx), None);

        // generate raft logs
        let region_id = 1;
        let mut entries = vec![];
        for i in 0..100 {
            let mut ent = Entry::new();
            ent.set_index(i);
            entries.push(ent);
        }
        let raft_wb = LogBatch::new();
        raft_wb.add_entries(region_id, entries);
        raft_engine.write(raft_wb, true).unwrap();

        let tbls = vec![
            (
                Task::region_task(Arc::clone(&raft_engine), region_id, 10),
                10,
                (0, 10),
                (10, 100),
            ),
            (
                Task::region_task(Arc::clone(&raft_engine), region_id, 50),
                40,
                (0, 50),
                (50, 100),
            ),
            (
                Task::region_task(Arc::clone(&raft_engine), region_id, 50),
                0,
                (0, 50),
                (50, 100),
            ),
            (
                Task::region_task(Arc::clone(&raft_engine), region_id, 60),
                10,
                (0, 60),
                (60, 100),
            ),
        ];

        for (task, expected_collectd, not_exist_range, exist_range) in tbls {
            runner.run(task);
            let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(res.collected, expected_collectd);
            raft_log_must_not_exist(&raft_engine, 1, not_exist_range.0, not_exist_range.1);
            raft_log_must_exist(&raft_engine, 1, exist_range.0, exist_range.1);
        }
    }

    fn raft_log_must_not_exist(
        raft_engine: &RaftEngine,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) {
        for i in start_idx..end_idx {
            assert!(raft_engine.get_entry(region_id, i).unwrap().is_none());
        }
    }

    fn raft_log_must_exist(raft_engine: &RaftEngine, region_id: u64, start_idx: u64, end_idx: u64) {
        for i in start_idx..end_idx {
            assert!(raft_engine.get_entry(region_id, i).unwrap().is_some());
        }
    }
}
