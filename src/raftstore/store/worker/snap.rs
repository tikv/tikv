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

use raftstore::store::{self, RaftStorage, SnapState, SnapManager, SnapKey};

use std::fmt::{self, Formatter, Display};
use std::error;
use std::time::Instant;

use util::worker::Runnable;
use util::HandyRwLock;

/// Snapshot generating task.
pub struct Task {
    storage: RaftStorage,
}

impl Task {
    pub fn new(storage: RaftStorage) -> Task {
        Task { storage: storage }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot Task for {}", self.storage.rl().get_region_id())
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
    mgr: SnapManager,
}

impl Runner {
    pub fn new(mgr: SnapManager) -> Runner {
        Runner { mgr: mgr }
    }

    fn generate_snap(&self, task: &Task) -> Result<(), Error> {
        // do we need to check leader here?
        let db = task.storage.rl().get_engine();
        let raw_snap;
        let ranges;
        let key;

        {
            let storage = task.storage.rl();
            raw_snap = db.snapshot();
            ranges = storage.region_key_ranges();
            let applied_idx = box_try!(storage.load_applied_index(&raw_snap));
            let term = box_try!(storage.term(applied_idx));
            key = SnapKey::new(storage.get_region_id(), term, applied_idx);
        }

        self.mgr.wl().register(key.clone(), true);
        match store::do_snapshot(self.mgr.clone(), &raw_snap, key.clone(), ranges) {
            Ok(snap) => task.storage.wl().snap_state = SnapState::Snap(snap),
            Err(e) => {
                self.mgr.wl().deregister(&key, true);
                return Err(Error::Other(box e));
            }
        }
        Ok(())
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        metric_incr!("raftstore.generate_snap");
        let ts = Instant::now();
        if let Err(e) = self.generate_snap(&task) {
            error!("failed to generate snap: {:?}!!!", e);
            task.storage.wl().snap_state = SnapState::Failed;
            return;
        }
        metric_incr!("raftstore.generate_snap.success");
        metric_time!("raftstore.generate_snap.cost", ts.elapsed());
    }
}
