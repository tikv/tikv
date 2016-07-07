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
use std::sync::Arc;
use std::time::Instant;

use rocksdb::{DB, Writable, WriteBatch};
use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState, PeerState};

use util::worker::Runnable;
use util::codec::bytes::CompactBytesDecoder;
use util::{escape, HandyRwLock};
use raftstore;
use raftstore::store::engine::Mutable;
use raftstore::store::{self, SnapManager, SnapKey, SnapEntry, SendCh, Msg, keys, Peekable};
use raftstore::store::engine::Snapshot;

const BATCH_SIZE: usize = 1024 * 1024 * 10; // 10m

/// Snapshot related task.
pub enum Task {
    Gen {
        region_id: u64,
    },
    Apply {
        region_id: u64,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snap gen for {}", region_id),
            Task::Apply { region_id, .. } => write!(f, "Snap apply for {}", region_id),
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

pub trait MsgSender {
    fn send(&self, msg: Msg) -> raftstore::Result<()>;
}

impl MsgSender for SendCh {
    fn send(&self, msg: Msg) -> raftstore::Result<()> {
        SendCh::send(self, msg)
    }
}

// TODO: seperate snap generate and apply to different thread.
pub struct Runner<T: MsgSender> {
    db: Arc<DB>,
    ch: T,
    mgr: SnapManager,
}

impl<T: MsgSender> Runner<T> {
    pub fn new(db: Arc<DB>, ch: T, mgr: SnapManager) -> Runner<T> {
        Runner {
            db: db,
            ch: ch,
            mgr: mgr,
        }
    }

    fn generate_snap(&self, region_id: u64) -> Result<(), Error> {
        // do we need to check leader here?
        let raw_snap = Snapshot::new(self.db.clone());

        let snap = box_try!(store::do_snapshot(self.mgr.clone(), &raw_snap, region_id));
        let msg = Msg::SnapGenRes {
            region_id: region_id,
            snap: Some(snap),
        };
        if let Err(e) = self.ch.send(msg) {
            error!("failed to notify snap result of {}: {:?}", region_id, e);
        }
        Ok(())
    }

    fn handle_gen(&self, region_id: u64) {
        metric_incr!("raftstore.generate_snap");
        let ts = Instant::now();
        if let Err(e) = self.generate_snap(region_id) {
            if let Err(e) = self.ch.send(Msg::SnapGenRes {
                region_id: region_id,
                snap: None,
            }) {
                panic!("failed to notify snap result of {}: {:?}", region_id, e);
            }
            error!("failed to generate snap: {:?}!!!", e);
            return;
        }
        metric_incr!("raftstore.generate_snap.success");
        metric_time!("raftstore.generate_snap.cost", ts.elapsed());
    }

    fn apply_snap(&self, region_id: u64) -> Result<(), Error> {
        info!("begin apply snap data for {}", region_id);
        let state_key = keys::apply_state_key(region_id);
        let apply_state: RaftApplyState = match box_try!(self.db.get_msg(&state_key)) {
            Some(state) => state,
            None => return Err(box_err!("failed to get raftstate from {}", escape(&state_key))),
        };
        let term = apply_state.get_truncated_state().get_term();
        let idx = apply_state.get_truncated_state().get_index();
        let snap_key = SnapKey::new(region_id, term, idx);
        let snap_file = box_try!(self.mgr.rl().get_snap_file(&snap_key, false));
        self.mgr.wl().register(snap_key.clone(), SnapEntry::Applying);
        defer!({
            self.mgr.wl().deregister(&snap_key, &SnapEntry::Applying);
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
        let state_key = keys::region_state_key(region_id);
        let mut region_state: RegionLocalState = match box_try!(self.db.get_msg(&state_key)) {
            Some(state) => state,
            None => return Err(box_err!("failed to get region_state from {}", escape(&state_key))),
        };
        region_state.set_state(PeerState::Normal);
        box_try!(self.db.put_msg(&state_key, &region_state));
        snap_file.delete();
        info!("apply new data takes {:?}", timer.elapsed());
        Ok(())
    }

    fn handle_apply(&self, region_id: u64) {
        metric_incr!("raftstore.apply_snap");
        let ts = Instant::now();
        let mut is_success = true;
        if let Err(e) = self.apply_snap(region_id) {
            is_success = false;
            error!("failed to apply snap: {:?}!!!", e);
        }
        let msg = Msg::SnapApplyRes {
            region_id: region_id,
            is_success: is_success,
        };
        if let Err(e) = self.ch.send(msg) {
            panic!("failed to notify snap apply result of {}: {:?}",
                   region_id,
                   e);
        }
        metric_incr!("raftstore.apply_snap.success");
        metric_time!("raftstore.apply_snap.cost", ts.elapsed());
    }
}

impl<T: MsgSender> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Gen { region_id } => self.handle_gen(region_id),
            Task::Apply { region_id } => self.handle_apply(region_id),
        }
    }
}
