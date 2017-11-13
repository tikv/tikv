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

use util::worker::Runnable;
use util::collections::HashSet;
use raftengine::{RaftEngine, Result as RaftEngineResult};

use std::sync::Arc;
use std::fmt::{self, Display, Formatter};
use std::error;
use std::sync::mpsc::Sender;


pub enum Task {
    RegionTask(RegionTask),
    EngineTask(EngineTask),
}

impl Task {
    pub fn region_task(raft_engine: Arc<RaftEngine>, region_id: u64, compact_to: u64) -> Task {
        Task::RegionTask(RegionTask {
            raft_engine: raft_engine,
            region_id: region_id,
            compact_to: compact_to,
        })
    }

    pub fn engine_task(raft_engine: Arc<RaftEngine>) -> Task {
        Task::EngineTask(EngineTask {
            raft_engine: raft_engine,
        })
    }
}

pub struct TaskRes {
    pub regions_need_compact: HashSet<u64>,
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

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::RegionTask(ref task) => write!(
                f,
                "GC Raft Log Task [region: {}, compact to: {}]",
                task.region_id,
                task.compact_to
            ),
            Task::EngineTask(ref task) => write!(
                f,
                "GC raft engine expired files Task, engine info {:?}",
                task
            ),
        }
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
    fn gc_raft_log(&mut self, raft_engine: Arc<RaftEngine>, region_id: u64, idx: u64) -> u64 {
        raft_engine.compact_to(region_id, idx)
    }

    fn gc_expired_files(&mut self, raft_engine: Arc<RaftEngine>) -> RaftEngineResult<()> {
        if raft_engine.rewrite_inactive() {
            raft_engine.sync_data()?;
        }

        // Collect regions which have entries exist for a long time.
        if self.ch.is_some() {
            let regions = raft_engine.regions_need_compact();
            self.ch
                .as_ref()
                .unwrap()
                .send(TaskRes {
                    regions_need_compact: regions,
                })
                .unwrap();
        }

        raft_engine.purge_expired_files()
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::RegionTask(task) => {
                let n = self.gc_raft_log(task.raft_engine, task.region_id, task.compact_to);
                debug!("[region {}] collected {} log entries", task.region_id, n);
            }
            Task::EngineTask(task) => if let Err(e) = self.gc_expired_files(task.raft_engine) {
                error!("GC expired files for raft engine error : {:?}", e);
            },
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use kvproto::eraftpb::Entry;

    use raftengine::{Config as RaftEngineCfg, LogBatch, RaftEngine};
    use super::*;

    #[test]
    fn test_gc_raft_log() {
        let path = TempDir::new("gc-raft-log-test").unwrap();
        let mut cfg = RaftEngineCfg::new();
        cfg.dir = path.path().to_str().unwrap().to_string();
        let raft_engine = Arc::new(RaftEngine::new(cfg));

        let mut runner = Runner::new(None);

        // generate raft logs
        let region_id = 1;
        let mut ents = vec![];
        for i in 0..100 {
            let mut e = Entry::new();
            e.set_index(i);
            ents.push(e);
        }
        let mut log_batch = LogBatch::default();
        log_batch.add_entries(region_id, ents);
        raft_engine.write(log_batch, false).unwrap();

        let tbls = vec![
            (
                Task::region_task(raft_engine.clone(), region_id, 10),
                (0, 10),
                (10, 100),
            ),
            (
                Task::region_task(raft_engine.clone(), region_id, 50),
                (0, 50),
                (50, 100),
            ),
            (
                Task::region_task(raft_engine.clone(), region_id, 50),
                (0, 50),
                (50, 100),
            ),
            (
                Task::region_task(raft_engine.clone(), region_id, 60),
                (0, 60),
                (60, 100),
            ),
        ];

        for (task, not_exist_range, exist_range) in tbls {
            runner.run(task);
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
        for idx in start_idx..end_idx {
            assert!(raft_engine.get_entry(region_id, idx).unwrap().is_none());
        }
    }

    fn raft_log_must_exist(raft_engine: &RaftEngine, region_id: u64, start_idx: u64, end_idx: u64) {
        let mut vec = vec![];
        raft_engine
            .fetch_entries_to(region_id, start_idx, end_idx, None, &mut vec)
            .unwrap();
        assert_eq!(vec.len(), (end_idx - start_idx) as usize);

    }
}
