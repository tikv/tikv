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


use std::fmt::{self, Formatter, Display};
use std::error;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use rocksdb::DB;

use util::worker::Runnable;
use util::HandyRwLock;
use raftstore::store::{self, SnapState, SnapManager};
use raftstore::store::engine::Snapshot;

/// Snapshot generating task.
pub struct Task {
    region_id: u64,
    state: Arc<RwLock<SnapState>>,
}

impl Task {
    pub fn new(region_id: u64, state: Arc<RwLock<SnapState>>) -> Task {
        Task {
            region_id: region_id,
            state: state,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot Task for {}", self.region_id)
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub struct Runner {
    db: Arc<DB>,
    mgr: SnapManager,
}

impl Runner {
    pub fn new(db: Arc<DB>, mgr: SnapManager) -> Runner {
        Runner { db: db, mgr: mgr }
    }

    fn generate_snap(&self, task: &Task) -> Result<(), Error> {
        // do we need to check leader here?
        let raw_snap = Snapshot::new(self.db.clone());

        if *task.state.rl() != SnapState::Generating {
            // task has been canceled.
            return Ok(());
        }

        let res = store::do_snapshot(self.mgr.clone(), &raw_snap, task.region_id);
        let mut state = task.state.wl();
        if *state != SnapState::Generating {
            // task has been canceled.
            return Ok(());
        }
        match res {
            Ok(snap) => {
                *state = SnapState::Snap(snap);
                Ok(())
            }
            Err(e) => {
                *state = SnapState::Failed;
                Err(Error::Other(box e))
            }
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        metric_incr!("raftstore.generate_snap");
        let ts = Instant::now();
        if let Err(e) = self.generate_snap(&task) {
            error!("failed to generate snap: {:?}!!!", e);
            return;
        }
        metric_incr!("raftstore.generate_snap.success");
        metric_time!("raftstore.generate_snap.cost", ts.elapsed());
    }
}
