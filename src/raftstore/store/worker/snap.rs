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

use raftstore::store::{self, Range, SnapState, SnapFile};
use raftstore::store::engine::Snapshot;

use std::fmt::{self, Formatter, Display};
use std::{error, result};
use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use util::worker::Runnable;

/// Snapshot generating task.
pub struct Task {
    snap: Snapshot,
    state: Arc<SnapState>,
    ranges: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Task {
    pub fn new(snap: Snapshot, state: Arc<SnapState>, ranges: Vec<Range>) -> Task {
        Task {
            snap: snap,
            state: state,
            ranges: ranges,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot Task for {}", self.state.key)
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

pub struct Runner {
    base_dir: String,
}

impl Runner {
    pub fn new(base_dir: &str) -> Runner {
        Runner { base_dir: base_dir.to_owned() }
    }

    fn generate_snap(&self, task: &Task) -> Result<()> {
        if task.state.failed.load(Ordering::SeqCst) {
            // task has been canceled.
            return Ok(());
        }
        let key = task.state.key.clone();
        let mut snap_file = box_try!(SnapFile::new(self.base_dir.clone(), true, &key));
        // maybe check if cancel during scan.
        let snap = box_try!(store::do_snapshot(&mut snap_file, &task.snap, &key, &task.ranges));
        if task.state.failed.load(Ordering::SeqCst) {
            // task has been canceled.
            snap_file.delete();
            return Ok(());
        }
        *task.state.snap.lock().unwrap() = Some(snap);
        Ok(())
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        metric_incr!("raftstore.generate_snap");
        let ts = Instant::now();
        if let Err(e) = self.generate_snap(&task) {
            error!("failed to generate snap: {:?}!!!", e);
            task.state.failed.store(true, Ordering::SeqCst);
            return;
        }
        metric_incr!("raftstore.generate_snap.success");
        metric_time!("raftstore.generate_snap.cost", ts.elapsed());
    }
}
