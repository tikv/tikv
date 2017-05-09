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

use std::sync::{self, Arc};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::RefCell;
use std::error;
use std::time::Instant;

use rocksdb::{DB, WriteBatch, Writable};
use protobuf::Message;

use kvproto::metapb::{self, Region};
use kvproto::eraftpb::{Entry, Snapshot, ConfState, HardState};
use kvproto::raft_serverpb::{RaftSnapshotData, RaftLocalState, RegionLocalState, RaftApplyState,
                             PeerState};
use util::worker::Scheduler;
use util::rocksdb;
use raft::{self, Storage, RaftState, StorageError, Error as RaftError, Ready};
use raftstore::{Result, Error};
use super::worker::RegionTask;
use super::keys::{self, enc_start_key, enc_end_key};
use super::engine::{Snapshot as DbSnapshot, Peekable, Iterable, Mutable};
use super::peer::ReadyContext;
use super::metrics::*;
use super::{SnapshotStatistics, SnapKey, SnapEntry, SnapManager};
use storage::CF_RAFT;

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;

pub const JOB_STATUS_PENDING: usize = 0;
pub const JOB_STATUS_RUNNING: usize = 1;
pub const JOB_STATUS_CANCELLING: usize = 2;
pub const JOB_STATUS_CANCELLED: usize = 3;
pub const JOB_STATUS_FINISHED: usize = 4;
pub const JOB_STATUS_FAILED: usize = 5;


#[derive(Debug)]
pub enum SnapState {
    Relax,
    Generating(Receiver<Snapshot>),
    Applying(Arc<AtomicUsize>),
    ApplyAborted,
}

impl PartialEq for SnapState {
    fn eq(&self, other: &SnapState) -> bool {
        match (self, other) {
            (&SnapState::Relax, &SnapState::Relax) |
            (&SnapState::ApplyAborted, &SnapState::ApplyAborted) |
            (&SnapState::Generating(_), &SnapState::Generating(_)) => true,
            (&SnapState::Applying(ref b1), &SnapState::Applying(ref b2)) => {
                b1.load(Ordering::Relaxed) == b2.load(Ordering::Relaxed)
            }
            _ => false,
        }
    }
}

// Discard all log entries prior to compact_index. We must guarantee
// that the compact_index is not greater than applied index.
pub fn compact_raft_log(tag: &str,
                        state: &mut RaftApplyState,
                        compact_index: u64,
                        compact_term: u64)
                        -> Result<()> {
    debug!("{} compact log entries to prior to {}", tag, compact_index);

    if compact_index <= state.get_truncated_state().get_index() {
        return Err(box_err!("try to truncate compacted entries"));
    } else if compact_index > state.get_applied_index() {
        return Err(box_err!("compact index {} > applied index {}",
                            compact_index,
                            state.get_applied_index()));
    }

    // we don't actually delete the logs now, we add an async task to do it.

    state.mut_truncated_state().set_index(compact_index);
    state.mut_truncated_state().set_term(compact_term);

    Ok(())
}

#[inline]
pub fn first_index(state: &RaftApplyState) -> u64 {
    state.get_truncated_state().get_index() + 1
}

pub struct PeerStorage {
    pub engine: Arc<DB>,

    pub region: metapb::Region,
    pub raft_state: RaftLocalState,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub last_term: u64,

    snap_state: RefCell<SnapState>,
    region_sched: Scheduler<RegionTask>,
    snap_tried_cnt: RefCell<usize>,

    pub tag: String,
}

fn storage_error<E>(error: E) -> raft::Error
    where E: Into<Box<error::Error + Send + Sync>>
{
    raft::Error::Store(StorageError::Other(error.into()))
}

impl From<Error> for RaftError {
    fn from(err: Error) -> RaftError {
        storage_error(err)
    }
}

impl<T> From<sync::PoisonError<T>> for RaftError {
    fn from(_: sync::PoisonError<T>) -> RaftError {
        storage_error("lock failed")
    }
}

pub struct ApplySnapResult {
    // prev_region is the region before snapshot applied.
    pub prev_region: metapb::Region,
    pub region: metapb::Region,
}

pub struct InvokeContext {
    pub region_id: u64,
    pub raft_state: RaftLocalState,
    pub apply_state: RaftApplyState,
    last_term: u64,
    pub snap_region: Option<Region>,
}

impl InvokeContext {
    pub fn new(store: &PeerStorage) -> InvokeContext {
        InvokeContext {
            region_id: store.get_region_id(),
            raft_state: store.raft_state.clone(),
            apply_state: store.apply_state.clone(),
            last_term: store.last_term,
            snap_region: None,
        }
    }

    #[inline]
    pub fn has_snapshot(&self) -> bool {
        self.snap_region.is_some()
    }

    #[inline]
    pub fn save_raft_to(&self, db: &DB, wb: &mut WriteBatch) -> Result<()> {
        let handle = try!(rocksdb::get_cf_handle(db, CF_RAFT));
        try!(wb.put_msg_cf(handle,
                           &keys::raft_state_key(self.region_id),
                           &self.raft_state));
        Ok(())
    }

    #[inline]
    pub fn save_apply_to(&self, db: &DB, wb: &mut WriteBatch) -> Result<()> {
        let handle = try!(rocksdb::get_cf_handle(db, CF_RAFT));
        try!(wb.put_msg_cf(handle,
                           &keys::apply_state_key(self.region_id),
                           &self.apply_state));
        Ok(())
    }
}

fn init_raft_state(engine: &DB, region: &Region) -> Result<RaftLocalState> {
    Ok(match try!(engine.get_msg_cf(CF_RAFT, &keys::raft_state_key(region.get_id()))) {
        Some(s) => s,
        None => {
            let mut raft_state = RaftLocalState::new();
            if !region.get_peers().is_empty() {
                raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
            }
            raft_state
        }
    })
}

fn init_apply_state(engine: &DB, region: &Region) -> Result<RaftApplyState> {
    Ok(match try!(engine.get_msg_cf(CF_RAFT, &keys::apply_state_key(region.get_id()))) {
        Some(s) => s,
        None => {
            let mut apply_state = RaftApplyState::new();
            if !region.get_peers().is_empty() {
                apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
                let state = apply_state.mut_truncated_state();
                state.set_index(RAFT_INIT_LOG_INDEX);
                state.set_term(RAFT_INIT_LOG_TERM);
            }
            apply_state
        }
    })
}

fn init_last_term(engine: &DB,
                  region: &Region,
                  raft_state: &RaftLocalState,
                  apply_state: &RaftApplyState)
                  -> Result<u64> {
    let last_idx = raft_state.get_last_index();
    if last_idx == 0 {
        return Ok(0);
    } else if last_idx == RAFT_INIT_LOG_INDEX {
        return Ok(RAFT_INIT_LOG_TERM);
    } else if last_idx == apply_state.get_truncated_state().get_index() {
        return Ok(apply_state.get_truncated_state().get_term());
    } else {
        assert!(last_idx > RAFT_INIT_LOG_INDEX);
    }
    let last_log_key = keys::raft_log_key(region.get_id(), last_idx);
    Ok(match try!(engine.get_msg_cf::<Entry>(CF_RAFT, &last_log_key)) {
        None => {
            return Err(box_err!("[region {}] entry at {} doesn't exist, may lose data.",
                                region.get_id(),
                                last_idx))
        }
        Some(e) => e.get_term(),
    })
}

impl PeerStorage {
    pub fn new(engine: Arc<DB>,
               region: &metapb::Region,
               region_sched: Scheduler<RegionTask>,
               tag: String)
               -> Result<PeerStorage> {
        debug!("creating storage on {} for {:?}", engine.path(), region);
        let raft_state = try!(init_raft_state(&engine, region));
        let apply_state = try!(init_apply_state(&engine, region));
        let last_term = try!(init_last_term(&engine, region, &raft_state, &apply_state));

        Ok(PeerStorage {
            engine: engine,
            region: region.clone(),
            raft_state: raft_state,
            apply_state: apply_state,
            snap_state: RefCell::new(SnapState::Relax),
            region_sched: region_sched,
            snap_tried_cnt: RefCell::new(0),
            tag: tag,
            applied_index_term: RAFT_INIT_LOG_TERM,
            last_term: last_term,
        })
    }

    pub fn is_initialized(&self) -> bool {
        !self.region.get_peers().is_empty()
    }

    pub fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.raft_state.get_hard_state().clone();
        let mut conf_state = ConfState::new();
        if hard_state == HardState::new() {
            assert!(!self.is_initialized(),
                    "peer for region {:?} is initialized but local state {:?} has empty hard \
                     state",
                    self.region,
                    self.raft_state);

            return Ok(RaftState {
                hard_state: hard_state,
                conf_state: conf_state,
            });
        }

        for p in self.region.get_peers() {
            conf_state.mut_nodes().push(p.get_id());
        }

        Ok(RaftState {
            hard_state: hard_state,
            conf_state: conf_state,
        })
    }

    fn check_range(&self, low: u64, high: u64) -> raft::Result<()> {
        if low > high {
            return Err(storage_error(format!("low: {} is greater that high: {}", low, high)));
        } else if low <= self.truncated_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        } else if high > self.last_index() + 1 {
            return Err(storage_error(format!("entries' high {} is out of bound lastindex {}",
                                             high,
                                             self.last_index())));
        }
        Ok(())
    }

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        try!(self.check_range(low, high));
        let mut ents = Vec::with_capacity((high - low) as usize);
        if low == high {
            return Ok(ents);
        }
        let mut total_size: u64 = 0;
        let mut next_index = low;
        let mut exceeded_max_size = false;

        let start_key = keys::raft_log_key(self.get_region_id(), low);

        if low + 1 == high {
            // If election happens in inactive regions, they will just try
            // to fetch one empty log.
            let handle = self.engine.cf_handle(CF_RAFT).unwrap();
            match box_try!(self.engine.get_cf(handle, &start_key)) {
                None => return Err(RaftError::Store(StorageError::Unavailable)),
                Some(v) => {
                    let mut entry = Entry::new();
                    box_try!(entry.merge_from_bytes(&v));
                    assert_eq!(entry.get_index(), low);
                    return Ok(vec![entry]);
                }
            }
        }

        let end_key = keys::raft_log_key(self.get_region_id(), high);

        try!(self.engine.scan_cf(CF_RAFT,
                                 &start_key,
                                 &end_key,
                                 true, // fill_cache
                                 &mut |_, value| {
            let mut entry = Entry::new();
            try!(entry.merge_from_bytes(value));

            // May meet gap or has been compacted.
            if entry.get_index() != next_index {
                return Ok(false);
            }

            next_index += 1;

            total_size += value.len() as u64;
            exceeded_max_size = total_size > max_size;

            if !exceeded_max_size || ents.is_empty() {
                ents.push(entry);
            }

            Ok(!exceeded_max_size)
        }));

        // If we get the correct number of entries the total size exceeds max_size, returns.
        if ents.len() == (high - low) as usize || exceeded_max_size {
            return Ok(ents);
        }

        // Here means we don't fetch enough entries.
        Err(RaftError::Store(StorageError::Unavailable))
    }

    pub fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }
        try!(self.check_range(idx, idx + 1));
        if self.truncated_term() == self.last_term || idx == self.last_index() {
            return Ok(self.last_term);
        }
        let key = keys::raft_log_key(self.get_region_id(), idx);
        match try!(self.engine.get_msg_cf::<Entry>(CF_RAFT, &key)) {
            Some(entry) => Ok(entry.get_term()),
            None => Err(RaftError::Store(StorageError::Unavailable)),
        }
    }

    #[inline]
    pub fn first_index(&self) -> u64 {
        first_index(&self.apply_state)
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.raft_state.get_last_index()
    }

    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.apply_state.get_applied_index()
    }

    #[inline]
    pub fn committed_index(&self) -> u64 {
        self.raft_state.get_hard_state().get_commit()
    }

    #[inline]
    pub fn truncated_index(&self) -> u64 {
        self.apply_state.get_truncated_state().get_index()
    }

    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.apply_state.get_truncated_state().get_term()
    }

    pub fn get_region(&self) -> &metapb::Region {
        &self.region
    }

    pub fn raw_snapshot(&self) -> DbSnapshot {
        DbSnapshot::new(self.engine.clone())
    }

    fn validate_snap(&self, snap: &Snapshot) -> bool {
        let idx = snap.get_metadata().get_index();
        if idx < self.truncated_index() {
            // stale snapshot, should generate again.
            info!("{} snapshot {} < {} is stale, generate again.",
                  self.tag,
                  idx,
                  self.truncated_index());
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.with_label_values(&["stale"]).inc();
            return false;
        }

        let mut snap_data = RaftSnapshotData::new();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            error!("{} decode snapshot fail, it may be corrupted: {:?}",
                   self.tag,
                   e);
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.with_label_values(&["decode"]).inc();
            return false;
        }
        let snap_epoch = snap_data.get_region().get_region_epoch();
        let latest_epoch = self.get_region().get_region_epoch();
        if snap_epoch.get_conf_ver() < latest_epoch.get_conf_ver() {
            info!("{} snapshot epoch {:?} < {:?}, generate again.",
                  self.tag,
                  snap_epoch,
                  latest_epoch);
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.with_label_values(&["epoch"]).inc();
            return false;
        }

        true
    }

    pub fn snapshot(&self) -> raft::Result<Snapshot> {
        let mut snap_state = self.snap_state.borrow_mut();
        let mut tried_cnt = self.snap_tried_cnt.borrow_mut();

        let (mut tried, mut snap) = (false, None);
        if let SnapState::Generating(ref recv) = *snap_state {
            tried = true;
            match recv.try_recv() {
                Err(TryRecvError::Disconnected) => {}
                Err(TryRecvError::Empty) => {
                    return Err(raft::Error::Store(
                        raft::StorageError::SnapshotTemporarilyUnavailable));
                }
                Ok(s) => snap = Some(s),
            }
        }

        if tried {
            *snap_state = SnapState::Relax;
            match snap {
                Some(s) => {
                    *tried_cnt = 0;
                    if self.validate_snap(&s) {
                        return Ok(s);
                    }
                }
                None => {
                    warn!("{} snapshot generating failed at {} try time",
                          self.tag,
                          *tried_cnt);
                }
            }
        }

        if SnapState::Relax != *snap_state {
            panic!("{} unexpected state: {:?}", self.tag, *snap_state);
        }

        if *tried_cnt >= MAX_SNAP_TRY_CNT {
            let cnt = *tried_cnt;
            *tried_cnt = 0;
            return Err(raft::Error::Store(box_err!("failed to get snapshot after {} times", cnt)));
        }

        info!("{} requesting snapshot...", self.tag);
        *tried_cnt += 1;
        let (tx, rx) = mpsc::sync_channel(1);
        *snap_state = SnapState::Generating(rx);

        let task = RegionTask::Gen {
            region_id: self.get_region_id(),
            notifier: tx,
        };
        if let Err(e) = self.region_sched.schedule(task) {
            error!("{} failed to schedule task snap generation: {:?}",
                   self.tag,
                   e);
            // update the status next time the function is called, also backoff for retry.
        }
        Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable))
    }

    // Append the given entries to the raft log using previous last index or self.last_index.
    // Return the new last index for later update. After we commit in engine, we can set last_index
    // to the return one.
    pub fn append(&self,
                  ctx: &mut InvokeContext,
                  entries: &[Entry],
                  wb: &mut WriteBatch)
                  -> Result<u64> {
        debug!("{} append {} entries", self.tag, entries.len());
        let prev_last_index = ctx.raft_state.get_last_index();
        if entries.is_empty() {
            return Ok(prev_last_index);
        }

        let (last_index, last_term) = {
            let e = entries.last().unwrap();
            (e.get_index(), e.get_term())
        };

        let handle = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        for entry in entries {
            try!(wb.put_msg_cf(handle,
                               &keys::raft_log_key(self.get_region_id(), entry.get_index()),
                               entry));
        }

        // Delete any previously appended log entries which never committed.
        for i in (last_index + 1)..(prev_last_index + 1) {
            try!(wb.delete_cf(handle, &keys::raft_log_key(self.get_region_id(), i)));
        }

        ctx.raft_state.set_last_index(last_index);
        ctx.last_term = last_term;

        Ok(last_index)
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot(&self,
                          ctx: &mut InvokeContext,
                          snap: &Snapshot,
                          wb: &mut WriteBatch)
                          -> Result<()> {
        info!("{} begin to apply snapshot", self.tag);

        let mut snap_data = RaftSnapshotData::new();
        try!(snap_data.merge_from_bytes(snap.get_data()));

        let region_id = self.get_region_id();

        let region = snap_data.take_region();
        if region.get_id() != region_id {
            return Err(box_err!("mismatch region id {} != {}", region_id, region.get_id()));
        }

        if self.is_initialized() {
            // we can only delete the old data when the peer is initialized.
            try!(self.clear_meta(wb));
        }

        try!(write_peer_state(wb, &region, PeerState::Applying));

        let last_index = snap.get_metadata().get_index();

        ctx.raft_state.set_last_index(last_index);
        ctx.last_term = snap.get_metadata().get_term();
        ctx.apply_state.set_applied_index(last_index);

        // The snapshot only contains log which index > applied index, so
        // here the truncate state's (index, term) is in snapshot metadata.
        ctx.apply_state.mut_truncated_state().set_index(last_index);
        ctx.apply_state.mut_truncated_state().set_term(snap.get_metadata().get_term());

        info!("{} apply snapshot for region {:?} with state {:?} ok",
              self.tag,
              region,
              ctx.apply_state);

        ctx.snap_region = Some(region);
        Ok(())
    }

    /// Delete all meta belong to the region. Results are stored in `wb`.
    pub fn clear_meta(&self, wb: &WriteBatch) -> Result<()> {
        let t = Instant::now();
        let mut meta_count = 0;
        let mut raft_count = 0;
        let region_id = self.get_region_id();
        let (meta_start, meta_end) = (keys::region_meta_prefix(region_id),
                                      keys::region_meta_prefix(region_id + 1));
        try!(self.engine.scan(&meta_start,
                              &meta_end,
                              false,
                              &mut |key, _| {
                                  try!(wb.delete(key));
                                  meta_count += 1;
                                  Ok(true)
                              }));

        let handle = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        let (raft_start, raft_end) = (keys::region_raft_prefix(region_id),
                                      keys::region_raft_prefix(region_id + 1));
        try!(self.engine.scan_cf(CF_RAFT,
                                 &raft_start,
                                 &raft_end,
                                 false,
                                 &mut |key, _| {
                                     try!(wb.delete_cf(handle, key));
                                     raft_count += 1;
                                     Ok(true)
                                 }));
        info!("{} clear peer {} meta keys and {} raft keys, takes {:?}",
              self.tag,
              meta_count,
              raft_count,
              t.elapsed());
        Ok(())
    }

    /// Delete all data belong to the region.
    /// If return Err, data may get partial deleted.
    pub fn clear_data(&self) -> Result<()> {
        let (start_key, end_key) = (enc_start_key(self.get_region()),
                                    enc_end_key(self.get_region()));
        let region_id = self.get_region_id();
        box_try!(self.region_sched.schedule(RegionTask::destroy(region_id, start_key, end_key)));
        Ok(())
    }

    /// Delete all data that is not covered by `new_region`.
    fn clear_extra_data(&self, new_region: &metapb::Region) -> Result<()> {
        let (old_start_key, old_end_key) = (enc_start_key(self.get_region()),
                                            enc_end_key(self.get_region()));
        let (new_start_key, new_end_key) = (enc_start_key(new_region), enc_end_key(new_region));
        let region_id = new_region.get_id();
        if old_start_key < new_start_key {
            box_try!(self.region_sched
                .schedule(RegionTask::destroy(region_id, old_start_key, new_start_key)));
        }
        if new_end_key < old_end_key {
            box_try!(self.region_sched
                .schedule(RegionTask::destroy(region_id, new_end_key, old_end_key)));
        }
        Ok(())
    }

    pub fn get_engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    /// Check whether the storage has finished applying snapshot.
    #[inline]
    pub fn is_applying_snapshot(&self) -> bool {
        match *self.snap_state.borrow() {
            SnapState::Applying(_) => true,
            _ => false,
        }
    }

    /// Check if the storage is applying a snapshot.
    #[inline]
    pub fn check_applying_snap(&mut self) -> bool {
        let new_state = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                let s = status.load(Ordering::Relaxed);
                if s == JOB_STATUS_FINISHED {
                    SnapState::Relax
                } else if s == JOB_STATUS_CANCELLED {
                    SnapState::ApplyAborted
                } else if s == JOB_STATUS_FAILED {
                    // TODO: cleanup region and treat it as tombstone.
                    panic!("{} applying snapshot failed", self.tag);
                } else {
                    return true;
                }
            }
            _ => return false,
        };
        *self.snap_state.borrow_mut() = new_state;
        false
    }

    #[inline]
    pub fn is_canceling_snap(&self) -> bool {
        match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                status.load(Ordering::Relaxed) == JOB_STATUS_CANCELLING
            }
            _ => false,
        }
    }

    /// Cancel applying snapshot, return true if the job can be considered not be run again.
    pub fn cancel_applying_snap(&mut self) -> bool {
        let is_cancelled = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                if status.compare_and_swap(JOB_STATUS_PENDING,
                                           JOB_STATUS_CANCELLING,
                                           Ordering::SeqCst) ==
                   JOB_STATUS_PENDING {
                    true
                } else if status.compare_and_swap(JOB_STATUS_RUNNING,
                                                  JOB_STATUS_CANCELLING,
                                                  Ordering::SeqCst) ==
                          JOB_STATUS_RUNNING {
                    return false;
                } else {
                    false
                }
            }
            _ => return false,
        };
        if is_cancelled {
            *self.snap_state.borrow_mut() = SnapState::ApplyAborted;
            return true;
        }
        // now status can only be JOB_STATUS_CANCELLING, JOB_STATUS_CANCELLED,
        // JOB_STATUS_FAILED and JOB_STATUS_FINISHED.
        !self.check_applying_snap()
    }

    #[inline]
    pub fn set_snap_state(&mut self, state: SnapState) {
        *self.snap_state.borrow_mut() = state
    }

    #[inline]
    pub fn is_snap_state(&self, state: SnapState) -> bool {
        *self.snap_state.borrow() == state
    }

    pub fn get_region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn schedule_applying_snapshot(&mut self) {
        let status = Arc::new(AtomicUsize::new(JOB_STATUS_PENDING));
        self.set_snap_state(SnapState::Applying(status.clone()));
        let task = RegionTask::Apply {
            region_id: self.get_region_id(),
            status: status,
        };
        // TODO: gracefully remove region instead.
        self.region_sched.schedule(task).expect("snap apply job should not fail");
    }

    /// Save memory states to disk.
    ///
    /// This function only write data to `ready_ctx`'s `WriteBatch`. It's caller's duty to write
    /// it explictly to disk. If it's flushed to disk successfully, `post_ready` should be called
    /// to update the memory states properly.
    // Using `&Ready` here to make sure `Ready` struct is not modified in this function. This is
    // a requirement to advance the ready object properly later.
    pub fn handle_raft_ready<T>(&mut self,
                                ready_ctx: &mut ReadyContext<T>,
                                ready: &Ready)
                                -> Result<InvokeContext> {
        let mut ctx = InvokeContext::new(self);
        if !raft::is_empty_snap(&ready.snapshot) {
            try!(self.apply_snapshot(&mut ctx, &ready.snapshot, &mut ready_ctx.wb));
        }

        if !ready.entries.is_empty() {
            try!(self.append(&mut ctx, &ready.entries, &mut ready_ctx.wb));
        }

        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if ctx.raft_state.get_last_index() > 0 {
            if let Some(ref hs) = ready.hs {
                ctx.raft_state.set_hard_state(hs.clone());
            }
        }

        if ctx.raft_state != self.raft_state {
            try!(ctx.save_raft_to(&self.engine, &mut ready_ctx.wb));
        }

        if ctx.apply_state != self.apply_state {
            try!(ctx.save_apply_to(&self.engine, &mut ready_ctx.wb));
        }

        Ok(ctx)
    }

    /// Update the memory state after ready changes are flushed to disk successfully.
    pub fn post_ready(&mut self, ctx: InvokeContext) -> Option<ApplySnapResult> {
        self.raft_state = ctx.raft_state;
        self.apply_state = ctx.apply_state;
        self.last_term = ctx.last_term;
        // If we apply snapshot ok, we should update some infos like applied index too.
        let snap_region = match ctx.snap_region {
            Some(r) => r,
            None => return None,
        };
        // cleanup data before scheduling apply task
        if self.is_initialized() {
            if let Err(e) = self.clear_extra_data(&self.region) {
                // No need panic here, when applying snapshot, the deletion will be tried
                // again. But if the region range changes, like [a, c) -> [a, b) and [b, c),
                // [b, c) will be kept in rocksdb until a covered snapshot is applied or
                // store is restarted.
                error!("{} cleanup data fail, may leave some dirty data: {:?}",
                       self.tag,
                       e);
            }
        }

        self.schedule_applying_snapshot();
        let prev_region = self.region.clone();
        self.region = snap_region;

        Some(ApplySnapResult {
            prev_region: prev_region,
            region: self.region.clone(),
        })
    }
}

pub fn do_snapshot(mgr: SnapManager, snap: &DbSnapshot, region_id: u64) -> raft::Result<Snapshot> {
    debug!("[region {}] begin to generate a snapshot", region_id);

    let apply_state: RaftApplyState =
        match try!(snap.get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))) {
            None => return Err(box_err!("could not load raft state of region {}", region_id)),
            Some(state) => state,
        };

    let idx = apply_state.get_applied_index();
    let term = if idx == apply_state.get_truncated_state().get_index() {
        apply_state.get_truncated_state().get_term()
    } else {
        match try!(snap.get_msg_cf::<Entry>(CF_RAFT, &keys::raft_log_key(region_id, idx))) {
            None => return Err(box_err!("entry {} of {} not found.", idx, region_id)),
            Some(entry) => entry.get_term(),
        }
    };

    let key = SnapKey::new(region_id, term, idx);

    mgr.register(key.clone(), SnapEntry::Generating);
    defer!(mgr.deregister(&key, &SnapEntry::Generating));

    let state: RegionLocalState = try!(snap.get_msg(&keys::region_state_key(key.region_id))
        .and_then(|res| {
            match res {
                None => Err(box_err!("could not find region info")),
                Some(state) => Ok(state),
            }
        }));

    if state.get_state() != PeerState::Normal {
        return Err(box_err!("snap job for {} seems stale, skip.", region_id));
    }

    let mut snapshot = Snapshot::new();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let mut conf_state = ConfState::new();
    for p in state.get_region().get_peers() {
        conf_state.mut_nodes().push(p.get_id());
    }

    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut s = try!(mgr.get_snapshot_for_building(&key, snap));
    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::new();
    snap_data.set_region(state.get_region().clone());
    let mut stat = SnapshotStatistics::new();
    try!(s.build(snap, state.get_region(), &mut snap_data, &mut stat));
    let mut v = vec![];
    box_try!(snap_data.write_to_vec(&mut v));
    snapshot.set_data(v);

    SNAPSHOT_KV_COUNT_HISTOGRAM.observe(stat.kv_count as f64);
    SNAPSHOT_SIZE_HISTOGRAM.observe(stat.size as f64);

    Ok(snapshot)
}

// When we bootstrap the region or handling split new region, we must
// call this to initialize region local state first.
pub fn write_initial_state<T: Mutable>(engine: &DB, w: &T, region_id: u64) -> Result<()> {
    let mut raft_state = RaftLocalState::new();
    raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);

    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state.mut_truncated_state().set_index(RAFT_INIT_LOG_INDEX);
    apply_state.mut_truncated_state().set_term(RAFT_INIT_LOG_TERM);

    let raft_cf = try!(rocksdb::get_cf_handle(engine, CF_RAFT));
    try!(w.put_msg_cf(raft_cf, &keys::raft_state_key(region_id), &raft_state));
    try!(w.put_msg_cf(raft_cf, &keys::apply_state_key(region_id), &apply_state));

    Ok(())
}

pub fn write_peer_state<T: Mutable>(w: &T,
                                    region: &metapb::Region,
                                    state: PeerState)
                                    -> Result<()> {
    let region_id = region.get_id();
    let mut region_state = RegionLocalState::new();
    region_state.set_state(state);
    region_state.set_region(region.clone());
    try!(w.put_msg(&keys::region_state_key(region_id), &region_state));
    Ok(())
}

impl Storage for PeerStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.initial_state()
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        self.entries(low, high, max_size)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index())
    }

    fn snapshot(&self) -> raft::Result<Snapshot> {
        self.snapshot()
    }
}

#[cfg(test)]
mod test {
    use std::sync::*;
    use std::sync::atomic::*;
    use std::sync::mpsc::*;
    use std::cell::RefCell;
    use std::time::Duration;
    use kvproto::eraftpb::{Entry, ConfState};
    use kvproto::raft_serverpb::RaftSnapshotData;
    use raft::{StorageError, Error as RaftError};
    use tempdir::*;
    use protobuf;
    use raftstore::store::{bootstrap, SnapKey, copy_snapshot};
    use raftstore::store::worker::RegionRunner;
    use raftstore::store::worker::RegionTask;
    use util::worker::{Worker, Scheduler};
    use util::rocksdb::new_engine;
    use storage::ALL_CFS;
    use kvproto::eraftpb::HardState;
    use rocksdb::WriteBatch;

    use super::*;

    fn new_storage(sched: Scheduler<RegionTask>, path: &TempDir) -> PeerStorage {
        let db = new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
        let db = Arc::new(db);
        bootstrap::bootstrap_store(&db, 1, 1).expect("");
        let region = bootstrap::prepare_bootstrap(&db, 1, 1, 1).expect("");
        PeerStorage::new(db, &region, sched, "".to_owned()).unwrap()
    }

    fn new_storage_from_ents(sched: Scheduler<RegionTask>,
                             path: &TempDir,
                             ents: &[Entry])
                             -> PeerStorage {
        let mut store = new_storage(sched, path);
        let mut wb = WriteBatch::new();
        let mut ctx = InvokeContext::new(&store);
        store.append(&mut ctx, &ents[1..], &mut wb).expect("");
        ctx.apply_state.mut_truncated_state().set_index(ents[0].get_index());
        ctx.apply_state.mut_truncated_state().set_term(ents[0].get_term());
        ctx.apply_state.set_applied_index(ents.last().unwrap().get_index());
        ctx.save_apply_to(&store.engine, &mut wb).unwrap();
        store.engine.write(wb).expect("");
        store.raft_state = ctx.raft_state;
        store.apply_state = ctx.apply_state;
        store
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(term);
        e
    }

    fn size_of<T: protobuf::Message>(m: &T) -> u32 {
        m.compute_size()
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];

        let mut tests = vec![(2, Err(RaftError::Store(StorageError::Compacted))),
                             (3, Ok(3)),
                             (4, Ok(4)),
                             (5, Ok(5))];
        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let worker = Worker::new("snap_manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let t = store.term(idx);
            if wterm != t {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];
        let max_u64 = u64::max_value();
        let mut tests =
            vec![(2, 6, max_u64, Err(RaftError::Store(StorageError::Compacted))),
                 (3, 4, max_u64, Err(RaftError::Store(StorageError::Compacted))),
                 (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
                 (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                 (4, 7, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)])),
                 // even if maxsize is zero, the first entry should be returned
                 (4, 7, 0, Ok(vec![new_entry(4, 4)])),
                 // limit to 2
                 (4,
                  7,
                  (size_of(&ents[1]) + size_of(&ents[2])) as u64,
                  Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                 (4,
                  7,
                  (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2) as u64,
                  Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                 (4,
                  7,
                  (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1) as u64,
                  Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                 // all
                 (4,
                  7,
                  (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])) as u64,
                  Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]))];

        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let worker = Worker::new("snap_manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let e = store.entries(lo, hi, maxsize);
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }
    }

    // last_index and first_index are not mutated by PeerStorage on its own,
    // so we don't test them here.

    #[test]
    fn test_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![(2, Err(RaftError::Store(StorageError::Compacted))),
                             (3, Err(RaftError::Store(StorageError::Compacted))),
                             (4, Ok(())),
                             (5, Ok(()))];
        for (i, (idx, werr)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let worker = Worker::new("snap_manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let mut ctx = InvokeContext::new(&store);
            let res = store.term(idx)
                .map_err(From::from)
                .and_then(|term| compact_raft_log(&store.tag, &mut ctx.apply_state, idx, term));
            // TODO check exact error type after refactoring error.
            if res.is_err() ^ werr.is_err() {
                panic!("#{}: want {:?}, got {:?}", i, werr, res);
            }
            if res.is_ok() {
                let mut wb = WriteBatch::new();
                ctx.save_apply_to(&store.engine, &mut wb).unwrap();
                store.engine.write(wb).expect("");
            }
        }
    }

    #[test]
    fn test_storage_create_snapshot_with_plain_format() {
        test_storage_create_snapshot(false);
    }

    #[test]
    fn test_storage_create_snapshot_with_sst_format() {
        test_storage_create_snapshot(true);
    }

    fn test_storage_create_snapshot(use_sst_file_snapshot: bool) {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);

        let td = TempDir::new("tikv-store-test").unwrap();
        let snap_dir = TempDir::new("snap_dir").unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap(),
                                   None,
                                   use_sst_file_snapshot);
        let mut worker = Worker::new("snap_manager");
        let sched = worker.scheduler();
        let mut s = new_storage_from_ents(sched, &td, &ents);
        let runner = RegionRunner::new(s.engine.clone(), mgr, 0);
        worker.start(runner).unwrap();
        let snap = s.snapshot();
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        let snap = match *s.snap_state.borrow() {
            SnapState::Generating(ref rx) => rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(snap.get_metadata().get_index(), 5);
        assert_eq!(snap.get_metadata().get_term(), 5);
        assert!(!snap.get_data().is_empty());

        let mut data = RaftSnapshotData::new();
        protobuf::Message::merge_from_bytes(&mut data, snap.get_data()).expect("");
        assert_eq!(data.get_region().get_id(), 1);
        assert_eq!(data.get_region().get_peers().len(), 1);

        let (tx, rx) = channel();
        s.set_snap_state(SnapState::Generating(rx));
        // Empty channel should cause snapshot call to wait.
        assert_eq!(s.snapshot().unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        tx.send(snap.clone()).unwrap();
        assert_eq!(s.snapshot(), Ok(snap.clone()));
        assert_eq!(*s.snap_tried_cnt.borrow(), 0);

        let mut ctx = InvokeContext::new(&s);
        let mut wb = WriteBatch::new();
        s.append(&mut ctx, &[new_entry(6, 5), new_entry(7, 5)], &mut wb).unwrap();
        let mut hs = HardState::new();
        hs.set_commit(7);
        hs.set_term(5);
        ctx.raft_state.set_hard_state(hs);
        ctx.raft_state.set_last_index(7);
        ctx.apply_state.set_applied_index(7);
        ctx.save_apply_to(&s.engine, &mut wb).unwrap();
        ctx.save_raft_to(&s.engine, &mut wb).unwrap();
        s.engine.write(wb).unwrap();
        s.apply_state = ctx.apply_state;
        s.raft_state = ctx.raft_state;
        ctx = InvokeContext::new(&s);
        let term = s.term(7).unwrap();
        compact_raft_log(&s.tag, &mut ctx.apply_state, 7, term).unwrap();
        wb = WriteBatch::new();
        ctx.save_apply_to(&s.engine, &mut wb).unwrap();
        s.engine.write(wb).unwrap();
        s.apply_state = ctx.apply_state;
        let (tx, rx) = channel();
        tx.send(snap.clone()).unwrap();
        s.set_snap_state(SnapState::Generating(rx));
        *s.snap_tried_cnt.borrow_mut() = 1;
        // stale snapshot should be abandoned.
        assert_eq!(s.snapshot().unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        match *s.snap_state.borrow() {
            SnapState::Generating(ref rx) => {
                rx.recv_timeout(Duration::from_secs(3)).unwrap();
                worker.stop().unwrap().join().unwrap();
                match rx.try_recv() {
                    Err(TryRecvError::Disconnected) => {}
                    res => panic!("unexpected result: {:?}", res),
                }
            }
            ref s => panic!("unexpected state {:?}", s),
        }
        // Disconnected channel should trigger another try.
        assert_eq!(s.snapshot().unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 2);

        for cnt in 2..super::MAX_SNAP_TRY_CNT {
            // Scheduled job failed should trigger .
            assert_eq!(s.snapshot().unwrap_err(), unavailable);
            assert_eq!(*s.snap_tried_cnt.borrow(), cnt + 1);
        }

        // When retry too many times, it should report a different error.
        match s.snapshot() {
            Err(RaftError::Store(StorageError::Other(_))) => {}
            res => panic!("unexpected res: {:?}", res),
        }
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests =
            vec![(vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                  vec![new_entry(4, 4), new_entry(5, 5)]),
                 (vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                  vec![new_entry(4, 6), new_entry(5, 6)]),
                 (vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
                  vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)]),
                 // truncate incoming entries, truncate the existing entries and append
                 (vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)], vec![new_entry(4, 5)]),
                 // truncate the existing entries and append
                 (vec![new_entry(4, 5)], vec![new_entry(4, 5)]),
                 // direct append
                 (vec![new_entry(6, 5)], vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)])];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let worker = Worker::new("snap_manager");
            let sched = worker.scheduler();
            let mut store = new_storage_from_ents(sched, &td, &ents);
            let mut ctx = InvokeContext::new(&store);
            let mut wb = WriteBatch::new();
            store.append(&mut ctx, &entries, &mut wb).unwrap();
            ctx.save_raft_to(&store.engine, &mut wb).unwrap();
            store.engine.write(wb).expect("");
            store.raft_state = ctx.raft_state;
            let li = store.last_index();
            let actual_entries = store.entries(4, li + 1, u64::max_value()).expect("");
            if actual_entries != wentries {
                panic!("#{}: want {:?}, got {:?}", i, wentries, actual_entries);
            }
        }
    }

    #[test]
    fn test_storage_apply_snapshot_with_old_format() {
        test_storage_apply_snapshot(false);
    }

    #[test]
    fn test_storage_apply_snapshot_with_sst_file_format() {
        test_storage_apply_snapshot(true);
    }

    fn test_storage_apply_snapshot(use_sst_file_snapshot: bool) {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);

        let td1 = TempDir::new("tikv-store-test").unwrap();
        let snap_dir = TempDir::new("snap").unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap(),
                                   None,
                                   use_sst_file_snapshot);
        let mut worker = Worker::new("snap_manager");
        let sched = worker.scheduler();
        let s1 = new_storage_from_ents(sched.clone(), &td1, &ents);
        let runner = RegionRunner::new(s1.engine.clone(), mgr.clone(), 0);
        worker.start(runner).unwrap();
        assert!(s1.snapshot().is_err());
        let snap1 = match *s1.snap_state.borrow() {
            SnapState::Generating(ref rx) => rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(s1.truncated_index(), 3);
        assert_eq!(s1.truncated_term(), 3);

        let key = SnapKey::from_snap(&snap1).unwrap();
        let from = mgr.get_snapshot_for_sending(&key).unwrap();
        let to = mgr.get_snapshot_for_receiving(&key, b"").unwrap();
        copy_snapshot(from, to).unwrap();

        let td2 = TempDir::new("tikv-store-test").unwrap();
        let s2 = new_storage(sched, &td2);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        let mut ctx = InvokeContext::new(&s2);
        assert_ne!(ctx.last_term, snap1.get_metadata().get_term());
        let mut wb = WriteBatch::new();
        s2.apply_snapshot(&mut ctx, &snap1, &mut wb).unwrap();
        assert_eq!(ctx.last_term, snap1.get_metadata().get_term());
        assert_eq!(ctx.apply_state.get_applied_index(), 6);
        assert_eq!(ctx.raft_state.get_last_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_term(), 6);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
    }

    #[test]
    fn test_canceling_snapshot() {
        let td = TempDir::new("tikv-store-test").unwrap();
        let worker = Worker::new("snap_manager");
        let sched = worker.scheduler();
        let mut s = new_storage(sched, &td);

        // PENDING can be canceled directly.
        s.snap_state =
            RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING))));
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        // RUNNING can't be canceled directly.
        s.snap_state =
            RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING))));
        assert!(!s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(),
                   SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_CANCELLING))));
        // CANCEL can't be canceled again.
        assert!(!s.cancel_applying_snap());

        s.snap_state =
            RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_CANCELLED))));
        // canceled snapshot can be cancel directly.
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        s.snap_state =
            RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_FINISHED))));
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);

        s.snap_state =
            RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_FAILED))));
        let res = recover_safe!(|| s.cancel_applying_snap());
        assert!(res.is_err());
    }

    #[test]
    fn test_try_finish_snapshot() {
        let td = TempDir::new("tikv-store-test").unwrap();
        let worker = Worker::new("snap_manager");
        let sched = worker.scheduler();
        let mut s = new_storage(sched, &td);

        // PENDING can be finished.
        let mut snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING)));
        s.snap_state = RefCell::new(snap_state);
        assert!(s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(),
                   SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING))));

        // RUNNING can't be finished.
        snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)));
        s.snap_state = RefCell::new(snap_state);
        assert!(s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(),
                   SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING))));

        snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_CANCELLED)));
        s.snap_state = RefCell::new(snap_state);
        assert!(!s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);
        // ApplyAborted is not applying snapshot.
        assert!(!s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        s.snap_state =
            RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_FINISHED))));
        assert!(!s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);
        // Relax is not applying snapshot.
        assert!(!s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);

        s.snap_state =
            RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_FAILED))));
        let res = recover_safe!(|| s.check_applying_snap());
        assert!(res.is_err());
    }
}
