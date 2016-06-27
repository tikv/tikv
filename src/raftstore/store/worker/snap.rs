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
use std::fs::File;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use rocksdb::{DB, Writable, WriteBatch};

use util::worker::Runnable;
use util::codec::bytes::CompactBytesDecoder;
use util::HandyRwLock;
use raftstore::store::{self, SnapState, SnapManager, SnapKey, SnapEntry};
use raftstore::store::engine::Snapshot;

const BATCH_SIZE: usize = 1024 * 1024 * 10; // 10m

/// Snapshot related task.
pub enum Task {
    Gen {
        region_id: u64,
        state: Arc<RwLock<SnapState>>,
    },
    Apply {
        snap_key: SnapKey,
        state: Arc<RwLock<SnapState>>,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snap gen for {}", region_id),
            Task::Apply { ref snap_key, .. } => write!(f, "Snap apply for {}", snap_key),
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

    fn generate_snap(&self, region_id: u64, state: Arc<RwLock<SnapState>>) -> Result<(), Error> {
        // do we need to check leader here?
        let raw_snap = Snapshot::new(self.db.clone());

        if *state.rl() != SnapState::Generating {
            // task has been canceled.
            return Ok(());
        }

        let res = store::do_snapshot(self.mgr.clone(), &raw_snap, region_id);
        let mut state = state.wl();
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

    fn handle_gen(&self, region_id: u64, state: Arc<RwLock<SnapState>>) {
        metric_incr!("raftstore.generate_snap");
        let ts = Instant::now();
        if let Err(e) = self.generate_snap(region_id, state) {
            error!("failed to generate snap: {:?}!!!", e);
            return;
        }
        metric_incr!("raftstore.generate_snap.success");
        metric_time!("raftstore.generate_snap.cost", ts.elapsed());
    }

    fn apply_snap(&self, snap_key: SnapKey) -> Result<(), Error> {
        info!("begin apply snap data for {}", snap_key);
        let snap_file = box_try!(self.mgr.rl().get_snap_file(&snap_key, false));
        self.mgr.wl().register(snap_key.clone(), SnapEntry::Applying);
        defer!({
            self.mgr.wl().deregister(&snap_key, &SnapEntry::Applying);
            snap_file.delete();
        });
        if !snap_file.exists() {
            return Err(box_err!("missing snap file {}", snap_file.path().display()));
        }
        box_try!(snap_file.validate());
        let mut reader = box_try!(File::open(snap_file.path()));

        let timer = Instant::now();
        // Write the snapshot into the region.
        let mut wb = WriteBatch::new();
        let mut batch_size = 0;
        loop {
            // TODO: avoid too many allocation
            let key = box_try!(reader.decode_compact_bytes());
            if key.is_empty() {
                box_try!(self.db.write(wb));
                break;
            }
            batch_size += key.len();
            let value = box_try!(reader.decode_compact_bytes());
            batch_size += value.len();
            box_try!(wb.put(&key, &value));
            if batch_size > BATCH_SIZE {
                box_try!(self.db.write(wb));
                wb = WriteBatch::new();
                batch_size = 0;
            }
        }
        info!("apply new data takes {:?}", timer.elapsed());
        Ok(())
    }

    fn handle_apply(&self, snap_key: SnapKey, state: Arc<RwLock<SnapState>>) {
        metric_incr!("raftstore.apply_snap");
        let ts = Instant::now();
        assert_eq!(*state.rl(), SnapState::Applying);
        if let Err(e) = self.apply_snap(snap_key) {
            // TODO: gracefully remove region instead.
            panic!("failed to apply snap: {:?}!!!", e);
        }
        let mut snap_state = state.wl();
        assert_eq!(*snap_state, SnapState::Applying);
        // It's safe to just overwrite here. Any other pending snapshot is kept in
        // ready and will never be applied before following is executed.
        *snap_state = SnapState::Relax;
        metric_incr!("raftstore.apply_snap.success");
        metric_time!("raftstore.apply_snap.cost", ts.elapsed());
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Gen { region_id, state } => self.handle_gen(region_id, state),
            Task::Apply { snap_key, state } => self.handle_apply(snap_key, state),
        }
    }
}
