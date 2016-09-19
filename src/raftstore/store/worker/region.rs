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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use std::str;

use rocksdb::{DB, Writable, WriteBatch};
use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState, PeerState};

use util::worker::Runnable;
use util::codec::bytes::CompactBytesDecoder;
use util::{escape, HandyRwLock, rocksdb};
use util::transport::SendCh;
use raftstore;
use raftstore::store::engine::{self, Mutable, Snapshot, Iterable};
use raftstore::store::{self, SnapManager, SnapKey, SnapEntry, Msg, keys, Peekable};
use storage::CF_RAFT;

use super::metrics::*;

const BATCH_SIZE: usize = 1024 * 1024 * 10; // 10m

/// region related task.
pub enum Task {
    Gen { region_id: u64 },
    Apply {
        region_id: u64,
        abort: Arc<AtomicBool>,
    },
    /// Destroy data between [start_key, end_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl Task {
    pub fn destroy(region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Task {
        Task::Destroy {
            region_id: region_id,
            start_key: start_key,
            end_key: end_key,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snap gen for {}", region_id),
            Task::Apply { region_id, .. } => write!(f, "Snap apply for {}", region_id),
            Task::Destroy { region_id, ref start_key, ref end_key } => {
                write!(f,
                       "Destroy {} [{}, {})",
                       region_id,
                       escape(&start_key),
                       escape(&end_key))
            }
        }
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Abort {
            description("abort")
            display("abort")
        }
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

impl MsgSender for SendCh<Msg> {
    fn send(&self, msg: Msg) -> raftstore::Result<()> {
        SendCh::send(self, msg).map_err(|e| box_err!("{:?}", e))
    }
}

#[inline]
fn check_abort(abort: &AtomicBool) -> Result<(), Error> {
    if abort.load(Ordering::Relaxed) {
        return Err(Error::Abort);
    }
    Ok(())
}

fn delete_all_in_range(db: &DB,
                       start_key: &[u8],
                       end_key: &[u8],
                       abort: &AtomicBool)
                       -> Result<(), Error> {
    for cf in db.cf_names() {
        try!(check_abort(&abort));
        let handle = box_try!(rocksdb::get_cf_handle(db, cf));
        box_try!(db.delete_file_in_range_cf(*handle, start_key, end_key));

        let mut it = box_try!(db.new_iterator_cf(cf, Some(end_key)));

        let mut wb = WriteBatch::new();
        try!(check_abort(&abort));
        it.seek(start_key.into());
        while it.valid() {
            {
                let key = it.key();
                if key >= end_key {
                    break;
                }

                box_try!(wb.delete_cf(*handle, key));
                if wb.count() == BATCH_SIZE {
                    // Can't use write_without_wal here.
                    // Otherwise it may cause dirty data when applying snapshot.
                    box_try!(db.write(wb));
                    wb = WriteBatch::new();
                }
            };
            try!(check_abort(&abort));
            if !it.next() {
                break;
            }
        }

        if wb.count() > 0 {
            box_try!(db.write(wb));
        }
    }

    Ok(())
}

// TODO: use threadpool to do task concurrently
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
        SNAP_COUNTER_VEC.with_label_values(&["generate", "all"]).inc();
        let gen_histogram = SNAP_HISTOGRAM.with_label_values(&["generate"]);
        let timer = gen_histogram.start_timer();

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

        SNAP_COUNTER_VEC.with_label_values(&["generate", "success"]).inc();
        timer.observe_duration();
    }

    fn apply_snap(&self, region_id: u64, abort: Arc<AtomicBool>) -> Result<(), Error> {
        info!("[region {}] begin apply snap data", region_id);
        try!(check_abort(&abort));
        let region_key = keys::region_state_key(region_id);
        let mut region_state: RegionLocalState = match box_try!(self.db.get_msg(&region_key)) {
            Some(state) => state,
            None => return Err(box_err!("failed to get region_state from {}", escape(&region_key))),
        };

        // clear up origin data.
        let start_key = keys::enc_start_key(region_state.get_region());
        let end_key = keys::enc_end_key(region_state.get_region());
        box_try!(delete_all_in_range(self.db.as_ref(), &start_key, &end_key, &abort));

        let state_key = keys::apply_state_key(region_id);
        let apply_state: RaftApplyState = match box_try!(self.db.get_msg_cf(CF_RAFT, &state_key)) {
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
        try!(check_abort(&abort));
        box_try!(snap_file.validate());
        let mut reader = box_try!(File::open(snap_file.path()));

        let timer = Instant::now();
        // Write the snapshot into the region.
        loop {
            // TODO: avoid too many allocation
            try!(check_abort(&abort));
            let cf = box_try!(reader.decode_compact_bytes());
            if cf.is_empty() {
                break;
            }
            let handle = box_try!(rocksdb::get_cf_handle(&self.db,
                                                         unsafe { str::from_utf8_unchecked(&cf) }));
            let mut wb = WriteBatch::new();
            let mut batch_size = 0;
            loop {
                try!(check_abort(&abort));
                let key = box_try!(reader.decode_compact_bytes());
                if key.is_empty() {
                    box_try!(self.db.write(wb));
                    break;
                }
                batch_size += key.len();
                let value = box_try!(reader.decode_compact_bytes());
                batch_size += value.len();
                box_try!(wb.put_cf(*handle, &key, &value));
                if batch_size > BATCH_SIZE {
                    box_try!(self.db.write(wb));
                    wb = WriteBatch::new();
                    batch_size = 0;
                }
            }
        }

        region_state.set_state(PeerState::Normal);
        box_try!(self.db.put_msg(&region_key, &region_state));
        snap_file.delete();
        info!("[region {}] apply new data takes {:?}",
              region_id,
              timer.elapsed());
        Ok(())
    }

    fn handle_apply(&self, region_id: u64, abort: Arc<AtomicBool>) {
        SNAP_COUNTER_VEC.with_label_values(&["apply", "all"]).inc();
        let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        let timer = apply_histogram.start_timer();

        let (is_success, is_aborted) = match self.apply_snap(region_id, abort) {
            Ok(()) => (true, false),
            Err(Error::Abort) => {
                warn!("applying snapshot for region {} is aborted.", region_id);
                (false, true)
            }
            Err(e) => {
                error!("failed to apply snap: {:?}!!!", e);
                (false, false)
            }
        };
        let msg = Msg::SnapApplyRes {
            region_id: region_id,
            is_success: is_success,
            is_aborted: is_aborted,
        };
        if let Err(e) = self.ch.send(msg) {
            panic!("failed to notify snap apply result of {}: {:?}",
                   region_id,
                   e);
        }

        if is_aborted {
            SNAP_COUNTER_VEC.with_label_values(&["apply", "abort"]).inc();
        } else if is_success {
            SNAP_COUNTER_VEC.with_label_values(&["apply", "success"]).inc();
        } else {
            SNAP_COUNTER_VEC.with_label_values(&["apply", "fail"]).inc();
        }
        timer.observe_duration();
    }

    fn handle_destroy(&mut self, region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) {
        info!("[region {}] deleting data in [{}, {})",
              region_id,
              escape(&start_key),
              escape(&end_key));
        if let Err(e) = engine::delete_all_in_range(&self.db, &start_key, &end_key) {
            error!("failed to delete data in [{}, {}): {:?}",
                   escape(&start_key),
                   escape(&end_key),
                   e);
        }
    }
}

impl<T: MsgSender> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Gen { region_id } => self.handle_gen(region_id),
            Task::Apply { region_id, abort } => self.handle_apply(region_id, abort),
            Task::Destroy { region_id, start_key, end_key } => {
                self.handle_destroy(region_id, start_key, end_key)
            }
        }
    }
}
