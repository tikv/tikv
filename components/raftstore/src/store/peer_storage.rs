// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, error, u64};

use engine::rocks::DB;
use engine::Engines;
use engine::{Iterable, Mutable, Peekable};
use engine_rocks::{RocksSnapshot, RocksWriteBatch};
use engine_traits::CF_RAFT;
use engine_traits::{KvEngine, Mutable as MutableTrait, Peekable as PeekableTrait};
use keys::{self, enc_end_key, enc_start_key};
use kvproto::metapb::{self, Region};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftApplyState, RaftLocalState, RaftSnapshotData, RegionLocalState,
};
use protobuf::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{self, Error as RaftError, RaftState, Ready, Storage, StorageError};

use crate::store::fsm::GenSnapTask;
use crate::store::util::conf_state_from_region;
use crate::store::ProposalContext;
use crate::{Error, Result};
use into_other::into_other;
use tikv_util::worker::Scheduler;

use super::metrics::*;
use super::worker::RegionTask;
use super::{SnapEntry, SnapKey, SnapManager, SnapshotStatistics};

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;
const RAFT_LOG_MULTI_GET_CNT: u64 = 8;

/// The initial region epoch version.
pub const INIT_EPOCH_VER: u64 = 1;
/// The initial region epoch conf_version.
pub const INIT_EPOCH_CONF_VER: u64 = 1;

// One extra slot for VecDeque internal usage.
const MAX_CACHE_CAPACITY: usize = 1024 - 1;
const SHRINK_CACHE_CAPACITY: usize = 64;

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
            (&SnapState::Relax, &SnapState::Relax)
            | (&SnapState::ApplyAborted, &SnapState::ApplyAborted)
            | (&SnapState::Generating(_), &SnapState::Generating(_)) => true,
            (&SnapState::Applying(ref b1), &SnapState::Applying(ref b2)) => {
                b1.load(Ordering::Relaxed) == b2.load(Ordering::Relaxed)
            }
            _ => false,
        }
    }
}

#[inline]
pub fn first_index(state: &RaftApplyState) -> u64 {
    state.get_truncated_state().get_index() + 1
}

#[inline]
pub fn last_index(state: &RaftLocalState) -> u64 {
    state.get_last_index()
}

#[derive(Default)]
struct EntryCache {
    cache: VecDeque<Entry>,
}

impl EntryCache {
    fn first_index(&self) -> Option<u64> {
        self.cache.front().map(|e| e.get_index())
    }

    fn fetch_entries_to(
        &self,
        begin: u64,
        end: u64,
        mut fetched_size: u64,
        max_size: u64,
        ents: &mut Vec<Entry>,
    ) {
        if begin >= end {
            return;
        }
        assert!(!self.cache.is_empty());
        let cache_low = self.cache.front().unwrap().get_index();
        let start_idx = begin.checked_sub(cache_low).unwrap() as usize;
        let limit_idx = end.checked_sub(cache_low).unwrap() as usize;

        let mut end_idx = start_idx;
        self.cache
            .iter()
            .skip(start_idx)
            .take_while(|e| {
                let cur_idx = end_idx as u64 + cache_low;
                assert_eq!(e.get_index(), cur_idx);
                let m = u64::from(e.compute_size());
                fetched_size += m;
                if fetched_size == m {
                    end_idx += 1;
                    fetched_size <= max_size && end_idx < limit_idx
                } else if fetched_size <= max_size {
                    end_idx += 1;
                    end_idx < limit_idx
                } else {
                    false
                }
            })
            .count();
        // Cache either is empty or contains latest log. Hence we don't need to fetch log
        // from rocksdb anymore.
        assert!(end_idx == limit_idx || fetched_size > max_size);
        let (first, second) = tikv_util::slices_in_range(&self.cache, start_idx, end_idx);
        ents.extend_from_slice(first);
        ents.extend_from_slice(second);
    }

    fn append(&mut self, tag: &str, entries: &[Entry]) {
        if entries.is_empty() {
            return;
        }
        if let Some(cache_last_index) = self.cache.back().map(|e| e.get_index()) {
            let first_index = entries[0].get_index();
            if cache_last_index >= first_index {
                if self.cache.front().unwrap().get_index() >= first_index {
                    self.cache.clear();
                } else {
                    let left = self.cache.len() - (cache_last_index - first_index + 1) as usize;
                    self.cache.truncate(left);
                }
                if self.cache.len() + entries.len() < SHRINK_CACHE_CAPACITY
                    && self.cache.capacity() > SHRINK_CACHE_CAPACITY
                {
                    self.cache.shrink_to_fit();
                }
            } else if cache_last_index + 1 < first_index {
                panic!(
                    "{} unexpected hole: {} < {}",
                    tag, cache_last_index, first_index
                );
            }
        }
        let mut start_idx = 0;
        if let Some(len) = (self.cache.len() + entries.len()).checked_sub(MAX_CACHE_CAPACITY) {
            if len < self.cache.len() {
                self.cache.drain(..len);
            } else {
                start_idx = len - self.cache.len();
                self.cache.clear();
            }
        }
        for e in &entries[start_idx..] {
            self.cache.push_back(e.to_owned());
        }
    }

    pub fn compact_to(&mut self, idx: u64) {
        let cache_first_idx = self.first_index().unwrap_or(u64::MAX);
        if cache_first_idx > idx {
            return;
        }
        let cache_last_idx = self.cache.back().unwrap().get_index();
        // Use `cache_last_idx + 1` to make sure cache can be cleared completely
        // if necessary.
        self.cache
            .drain(..(cmp::min(cache_last_idx + 1, idx) - cache_first_idx) as usize);
        if self.cache.len() < SHRINK_CACHE_CAPACITY && self.cache.capacity() > SHRINK_CACHE_CAPACITY
        {
            // So the peer storage doesn't have much writes since the proposal of compaction,
            // we can consider this peer is going to be inactive.
            self.cache.shrink_to_fit();
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

#[derive(Default)]
pub struct CacheQueryStats {
    pub hit: Cell<u64>,
    pub miss: Cell<u64>,
}

impl CacheQueryStats {
    pub fn flush(&mut self) {
        if self.hit.get() > 0 {
            RAFT_ENTRY_FETCHES
                .with_label_values(&["hit"])
                .inc_by(self.hit.replace(0) as i64);
        }
        if self.miss.get() > 0 {
            RAFT_ENTRY_FETCHES
                .with_label_values(&["miss"])
                .inc_by(self.miss.replace(0) as i64);
        }
    }
}

pub trait HandleRaftReadyContext {
    fn kv_wb(&self) -> &RocksWriteBatch;
    fn kv_wb_mut(&mut self) -> &mut RocksWriteBatch;
    fn raft_wb(&self) -> &RocksWriteBatch;
    fn raft_wb_mut(&mut self) -> &mut RocksWriteBatch;
    fn sync_log(&self) -> bool;
    fn set_sync_log(&mut self, sync: bool);
}

fn storage_error<E>(error: E) -> raft::Error
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    raft::Error::Store(StorageError::Other(error.into()))
}

impl From<Error> for RaftError {
    fn from(err: Error) -> RaftError {
        storage_error(err)
    }
}

pub struct ApplySnapResult {
    // prev_region is the region before snapshot applied.
    pub prev_region: metapb::Region,
    pub region: metapb::Region,
}

/// Returned by `PeerStorage::handle_raft_ready`, used for recording changed status of
/// `RaftLocalState` and `RaftApplyState`.
pub struct InvokeContext {
    pub region_id: u64,
    /// Changed RaftLocalState is stored into `raft_state`.
    pub raft_state: RaftLocalState,
    /// Changed RaftApplyState is stored into `apply_state`.
    pub apply_state: RaftApplyState,
    last_term: u64,
    /// The old region is stored here if there is a snapshot.
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
    pub fn save_raft_state_to(&self, raft_wb: &mut RocksWriteBatch) -> Result<()> {
        raft_wb.put_msg(&keys::raft_state_key(self.region_id), &self.raft_state)?;
        Ok(())
    }

    #[inline]
    pub fn save_snapshot_raft_state_to(
        &self,
        snapshot_index: u64,
        kv_wb: &mut RocksWriteBatch,
    ) -> Result<()> {
        let mut snapshot_raft_state = self.raft_state.clone();
        snapshot_raft_state
            .mut_hard_state()
            .set_commit(snapshot_index);
        snapshot_raft_state.set_last_index(snapshot_index);

        kv_wb.put_msg_cf(
            CF_RAFT,
            &keys::snapshot_raft_state_key(self.region_id),
            &snapshot_raft_state,
        )?;
        Ok(())
    }

    #[inline]
    pub fn save_apply_state_to(&self, kv_wb: &mut RocksWriteBatch) -> Result<()> {
        kv_wb.put_msg_cf(
            CF_RAFT,
            &keys::apply_state_key(self.region_id),
            &self.apply_state,
        )?;
        Ok(())
    }
}

pub fn recover_from_applying_state(
    engines: &Engines,
    raft_wb: &RocksWriteBatch,
    region_id: u64,
) -> Result<()> {
    let snapshot_raft_state_key = keys::snapshot_raft_state_key(region_id);
    let snapshot_raft_state: RaftLocalState =
        match box_try!(engines.kv.get_msg_cf(CF_RAFT, &snapshot_raft_state_key)) {
            Some(state) => state,
            None => {
                return Err(box_err!(
                    "[region {}] failed to get raftstate from kv engine, \
                     when recover from applying state",
                    region_id
                ));
            }
        };

    let raft_state_key = keys::raft_state_key(region_id);
    let raft_state: RaftLocalState = match box_try!(engines.raft.get_msg(&raft_state_key)) {
        Some(state) => state,
        None => RaftLocalState::default(),
    };

    // if we recv append log when applying snapshot, last_index in raft_local_state will
    // larger than snapshot_index. since raft_local_state is written to raft engine, and
    // raft write_batch is written after kv write_batch, raft_local_state may wrong if
    // restart happen between the two write. so we copy raft_local_state to kv engine
    // (snapshot_raft_state), and set snapshot_raft_state.last_index = snapshot_index.
    // after restart, we need check last_index.
    if last_index(&snapshot_raft_state) > last_index(&raft_state) {
        raft_wb.put_msg(&raft_state_key, &snapshot_raft_state)?;
    }
    Ok(())
}

pub fn init_raft_state(engines: &Engines, region: &Region) -> Result<RaftLocalState> {
    let state_key = keys::raft_state_key(region.get_id());
    Ok(match engines.raft.get_msg(&state_key)? {
        Some(s) => s,
        None => {
            let mut raft_state = RaftLocalState::default();
            if !region.get_peers().is_empty() {
                // new split region
                raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
                raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
                raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);
                engines.raft.put_msg(&state_key, &raft_state)?;
            }
            raft_state
        }
    })
}

pub fn init_apply_state(engines: &Engines, region: &Region) -> Result<RaftApplyState> {
    Ok(
        match engines
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(region.get_id()))?
        {
            Some(s) => s,
            None => {
                let mut apply_state = RaftApplyState::default();
                if !region.get_peers().is_empty() {
                    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
                    let state = apply_state.mut_truncated_state();
                    state.set_index(RAFT_INIT_LOG_INDEX);
                    state.set_term(RAFT_INIT_LOG_TERM);
                }
                apply_state
            }
        },
    )
}

fn init_last_term(
    engines: &Engines,
    region: &Region,
    raft_state: &RaftLocalState,
    apply_state: &RaftApplyState,
) -> Result<u64> {
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
    let entry = engines.raft.get_msg::<Entry>(&last_log_key)?;
    match entry {
        None => Err(box_err!(
            "[region {}] entry at {} doesn't exist, may lose data.",
            region.get_id(),
            last_idx
        )),
        Some(e) => Ok(e.get_term()),
    }
}

pub struct PeerStorage {
    pub engines: Engines,

    peer_id: u64,
    region: metapb::Region,
    raft_state: RaftLocalState,
    apply_state: RaftApplyState,
    applied_index_term: u64,
    last_term: u64,

    snap_state: RefCell<SnapState>,
    gen_snap_task: RefCell<Option<GenSnapTask>>,
    region_sched: Scheduler<RegionTask>,
    snap_tried_cnt: RefCell<usize>,

    cache: EntryCache,
    stats: CacheQueryStats,

    pub tag: String,
}

impl Storage for PeerStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        self.entries(low, high, max_size.into().unwrap_or(u64::MAX))
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

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        self.snapshot(request_index)
    }
}

impl PeerStorage {
    pub fn new(
        engines: Engines,
        region: &metapb::Region,
        region_sched: Scheduler<RegionTask>,
        peer_id: u64,
        tag: String,
    ) -> Result<PeerStorage> {
        debug!(
            "creating storage on specified path";
            "region_id" => region.get_id(),
            "peer_id" => peer_id,
            "path" => ?engines.kv.path(),
        );
        let raft_state = init_raft_state(&engines, region)?;
        let apply_state = init_apply_state(&engines, region)?;
        if raft_state.get_last_index() < apply_state.get_applied_index() {
            panic!(
                "{} unexpected raft log index: last_index {} < applied_index {}",
                tag,
                raft_state.get_last_index(),
                apply_state.get_applied_index()
            );
        }
        let last_term = init_last_term(&engines, region, &raft_state, &apply_state)?;

        Ok(PeerStorage {
            engines,
            peer_id,
            region: region.clone(),
            raft_state,
            apply_state,
            snap_state: RefCell::new(SnapState::Relax),
            gen_snap_task: RefCell::new(None),
            region_sched,
            snap_tried_cnt: RefCell::new(0),
            tag,
            applied_index_term: RAFT_INIT_LOG_TERM,
            last_term,
            cache: EntryCache::default(),
            stats: CacheQueryStats::default(),
        })
    }

    pub fn is_initialized(&self) -> bool {
        !self.region().get_peers().is_empty()
    }

    pub fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.raft_state.get_hard_state().clone();
        if hard_state == HardState::default() {
            assert!(
                !self.is_initialized(),
                "peer for region {:?} is initialized but local state {:?} has empty hard \
                 state",
                self.region,
                self.raft_state
            );

            return Ok(RaftState::new(hard_state, ConfState::default()));
        }
        Ok(RaftState::new(
            hard_state,
            conf_state_from_region(self.region()),
        ))
    }

    fn check_range(&self, low: u64, high: u64) -> raft::Result<()> {
        if low > high {
            return Err(storage_error(format!(
                "low: {} is greater that high: {}",
                low, high
            )));
        } else if low <= self.truncated_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        } else if high > self.last_index() + 1 {
            return Err(storage_error(format!(
                "entries' high {} is out of bound lastindex {}",
                high,
                self.last_index()
            )));
        }
        Ok(())
    }

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        self.check_range(low, high)?;
        let mut ents = Vec::with_capacity((high - low) as usize);
        if low == high {
            return Ok(ents);
        }
        let cache_low = self.cache.first_index().unwrap_or(u64::MAX);
        let region_id = self.get_region_id();
        if high <= cache_low {
            // not overlap
            self.stats.miss.update(|m| m + 1);
            fetch_entries_to(
                &self.engines.raft,
                region_id,
                low,
                high,
                max_size,
                &mut ents,
            )?;
            return Ok(ents);
        }
        let mut fetched_size = 0;
        let begin_idx = if low < cache_low {
            self.stats.miss.update(|m| m + 1);
            fetched_size = fetch_entries_to(
                &self.engines.raft,
                region_id,
                low,
                cache_low,
                max_size,
                &mut ents,
            )?;
            if fetched_size > max_size {
                // max_size exceed.
                return Ok(ents);
            }
            cache_low
        } else {
            low
        };

        self.stats.hit.update(|h| h + 1);
        self.cache
            .fetch_entries_to(begin_idx, high, fetched_size, max_size, &mut ents);
        Ok(ents)
    }

    pub fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }
        self.check_range(idx, idx + 1)?;
        if self.truncated_term() == self.last_term || idx == self.last_index() {
            return Ok(self.last_term);
        }
        let entries = self.entries(idx, idx + 1, raft::NO_LIMIT)?;
        Ok(entries[0].get_term())
    }

    #[inline]
    pub fn first_index(&self) -> u64 {
        first_index(&self.apply_state)
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        last_index(&self.raft_state)
    }

    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.apply_state.get_applied_index()
    }

    #[inline]
    pub fn set_applied_state(&mut self, apply_state: RaftApplyState) {
        self.apply_state = apply_state;
    }

    #[inline]
    pub fn set_applied_term(&mut self, applied_index_term: u64) {
        self.applied_index_term = applied_index_term;
    }

    #[inline]
    pub fn apply_state(&self) -> &RaftApplyState {
        &self.apply_state
    }

    #[inline]
    pub fn applied_index_term(&self) -> u64 {
        self.applied_index_term
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

    pub fn region(&self) -> &metapb::Region {
        &self.region
    }

    pub fn set_region(&mut self, region: metapb::Region) {
        self.region = region;
    }

    pub fn raw_snapshot(&self) -> RocksSnapshot {
        RocksSnapshot::new(Arc::clone(&self.engines.kv))
    }

    fn validate_snap(&self, snap: &Snapshot, request_index: u64) -> bool {
        let idx = snap.get_metadata().get_index();
        if idx < self.truncated_index() || idx < request_index {
            // stale snapshot, should generate again.
            info!(
                "snapshot is stale, generate again";
                "region_id" => self.region.get_id(),
                "peer_id" => self.peer_id,
                "snap_index" => idx,
                "truncated_index" => self.truncated_index(),
                "request_index" => request_index,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER
                .with_label_values(&["stale"])
                .inc();
            return false;
        }

        let mut snap_data = RaftSnapshotData::default();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            error!(
                "failed to decode snapshot, it may be corrupted";
                "region_id" => self.region.get_id(),
                "peer_id" => self.peer_id,
                "err" => ?e,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER
                .with_label_values(&["decode"])
                .inc();
            return false;
        }
        let snap_epoch = snap_data.get_region().get_region_epoch();
        let latest_epoch = self.region().get_region_epoch();
        if snap_epoch.get_conf_ver() < latest_epoch.get_conf_ver() {
            info!(
                "snapshot epoch is stale";
                "region_id" => self.region.get_id(),
                "peer_id" => self.peer_id,
                "snap_epoch" => ?snap_epoch,
                "latest_epoch" => ?latest_epoch,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER
                .with_label_values(&["epoch"])
                .inc();
            return false;
        }

        true
    }

    /// Gets a snapshot. Returns `SnapshotTemporarilyUnavailable` if there is no unavailable
    /// snapshot.
    pub fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        let mut snap_state = self.snap_state.borrow_mut();
        let mut tried_cnt = self.snap_tried_cnt.borrow_mut();

        let (mut tried, mut snap) = (false, None);
        if let SnapState::Generating(ref recv) = *snap_state {
            tried = true;
            match recv.try_recv() {
                Err(TryRecvError::Disconnected) => {}
                Err(TryRecvError::Empty) => {
                    return Err(raft::Error::Store(
                        raft::StorageError::SnapshotTemporarilyUnavailable,
                    ));
                }
                Ok(s) => snap = Some(s),
            }
        }

        if tried {
            *snap_state = SnapState::Relax;
            match snap {
                Some(s) => {
                    *tried_cnt = 0;
                    if self.validate_snap(&s, request_index) {
                        return Ok(s);
                    }
                }
                None => {
                    warn!(
                        "failed to try generating snapshot";
                        "region_id" => self.region.get_id(),
                        "peer_id" => self.peer_id,
                        "times" => *tried_cnt,
                    );
                }
            }
        }

        if SnapState::Relax != *snap_state {
            panic!("{} unexpected state: {:?}", self.tag, *snap_state);
        }

        if *tried_cnt >= MAX_SNAP_TRY_CNT {
            let cnt = *tried_cnt;
            *tried_cnt = 0;
            return Err(raft::Error::Store(box_err!(
                "failed to get snapshot after {} times",
                cnt
            )));
        }

        info!(
            "requesting snapshot";
            "region_id" => self.region.get_id(),
            "peer_id" => self.peer_id,
            "request_index" => request_index,
        );
        *tried_cnt += 1;
        let (tx, rx) = mpsc::sync_channel(1);
        *snap_state = SnapState::Generating(rx);

        let task = GenSnapTask::new(self.region.get_id(), self.committed_index(), tx);
        let mut gen_snap_task = self.gen_snap_task.borrow_mut();
        assert!(gen_snap_task.is_none());
        *gen_snap_task = Some(task);
        Err(raft::Error::Store(
            raft::StorageError::SnapshotTemporarilyUnavailable,
        ))
    }

    pub fn take_gen_snap_task(&mut self) -> Option<GenSnapTask> {
        self.gen_snap_task.get_mut().take()
    }

    // Append the given entries to the raft log using previous last index or self.last_index.
    // Return the new last index for later update. After we commit in engine, we can set last_index
    // to the return one.
    pub fn append<H: HandleRaftReadyContext>(
        &mut self,
        invoke_ctx: &mut InvokeContext,
        entries: &[Entry],
        ready_ctx: &mut H,
    ) -> Result<u64> {
        debug!(
            "append entries";
            "region_id" => self.region.get_id(),
            "peer_id" => self.peer_id,
            "count" => entries.len(),
        );
        let prev_last_index = invoke_ctx.raft_state.get_last_index();
        if entries.is_empty() {
            return Ok(prev_last_index);
        }

        let (last_index, last_term) = {
            let e = entries.last().unwrap();
            (e.get_index(), e.get_term())
        };

        for entry in entries {
            if !ready_ctx.sync_log() {
                ready_ctx.set_sync_log(get_sync_log_from_entry(entry));
            }
            ready_ctx.raft_wb_mut().put_msg(
                &keys::raft_log_key(self.get_region_id(), entry.get_index()),
                entry,
            )?;
        }

        // Delete any previously appended log entries which never committed.
        for i in (last_index + 1)..=prev_last_index {
            // TODO: Wrap it as an engine::Error.
            box_try!(ready_ctx
                .raft_wb_mut()
                .delete(&keys::raft_log_key(self.get_region_id(), i)));
        }

        invoke_ctx.raft_state.set_last_index(last_index);
        invoke_ctx.last_term = last_term;

        // TODO: if the writebatch is failed to commit, the cache will be wrong.
        self.cache.append(&self.tag, entries);
        Ok(last_index)
    }

    pub fn compact_to(&mut self, idx: u64) {
        self.cache.compact_to(idx);
    }

    #[inline]
    pub fn is_cache_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn maybe_gc_cache(&mut self, replicated_idx: u64, apply_idx: u64) {
        if replicated_idx == apply_idx {
            // The region is inactive, clear the cache immediately.
            self.cache.compact_to(apply_idx + 1);
        } else {
            let cache_first_idx = match self.cache.first_index() {
                None => return,
                Some(idx) => idx,
            };
            if cache_first_idx > replicated_idx + 1 {
                // Catching up log requires accessing fs already, let's optimize for
                // the common case.
                // Maybe gc to second least replicated_idx is better.
                self.cache.compact_to(apply_idx + 1);
            }
        }
    }

    #[inline]
    pub fn flush_cache_metrics(&mut self) {
        self.stats.flush();
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot(
        &mut self,
        ctx: &mut InvokeContext,
        snap: &Snapshot,
        kv_wb: &RocksWriteBatch,
        raft_wb: &RocksWriteBatch,
    ) -> Result<()> {
        info!(
            "begin to apply snapshot";
            "region_id" => self.region.get_id(),
            "peer_id" => self.peer_id,
        );

        let mut snap_data = RaftSnapshotData::default();
        snap_data.merge_from_bytes(snap.get_data())?;

        let region_id = self.get_region_id();

        let region = snap_data.take_region();
        if region.get_id() != region_id {
            return Err(box_err!(
                "mismatch region id {} != {}",
                region_id,
                region.get_id()
            ));
        }

        if self.is_initialized() {
            // we can only delete the old data when the peer is initialized.
            self.clear_meta(kv_wb, raft_wb)?;
        }

        write_peer_state(kv_wb, &region, PeerState::Applying, None)?;

        let last_index = snap.get_metadata().get_index();

        ctx.raft_state.set_last_index(last_index);
        ctx.last_term = snap.get_metadata().get_term();
        ctx.apply_state.set_applied_index(last_index);

        // The snapshot only contains log which index > applied index, so
        // here the truncate state's (index, term) is in snapshot metadata.
        ctx.apply_state.mut_truncated_state().set_index(last_index);
        ctx.apply_state
            .mut_truncated_state()
            .set_term(snap.get_metadata().get_term());

        info!(
            "apply snapshot with state ok";
            "region_id" => self.region.get_id(),
            "peer_id" => self.peer_id,
            "region" => ?region,
            "state" => ?ctx.apply_state,
        );

        fail_point!("before_apply_snap_update_region", |_| { Ok(()) });

        ctx.snap_region = Some(region);
        Ok(())
    }

    /// Delete all meta belong to the region. Results are stored in `wb`.
    pub fn clear_meta(&mut self, kv_wb: &RocksWriteBatch, raft_wb: &RocksWriteBatch) -> Result<()> {
        let region_id = self.get_region_id();
        clear_meta(&self.engines, kv_wb, raft_wb, region_id, &self.raft_state)?;
        self.cache = EntryCache::default();
        Ok(())
    }

    /// Delete all data belong to the region.
    /// If return Err, data may get partial deleted.
    pub fn clear_data(&self) -> Result<()> {
        let (start_key, end_key) = (enc_start_key(self.region()), enc_end_key(self.region()));
        let region_id = self.get_region_id();
        box_try!(self
            .region_sched
            .schedule(RegionTask::destroy(region_id, start_key, end_key)));
        Ok(())
    }

    /// Delete all data that is not covered by `new_region`.
    fn clear_extra_data(&self, new_region: &metapb::Region) -> Result<()> {
        let (old_start_key, old_end_key) =
            (enc_start_key(self.region()), enc_end_key(self.region()));
        let (new_start_key, new_end_key) = (enc_start_key(new_region), enc_end_key(new_region));
        let region_id = new_region.get_id();
        if old_start_key < new_start_key {
            box_try!(self.region_sched.schedule(RegionTask::destroy(
                region_id,
                old_start_key,
                new_start_key
            )));
        }
        if new_end_key < old_end_key {
            box_try!(self.region_sched.schedule(RegionTask::destroy(
                region_id,
                new_end_key,
                old_end_key
            )));
        }
        Ok(())
    }

    pub fn get_raft_engine(&self) -> Arc<DB> {
        Arc::clone(&self.engines.raft)
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
                    panic!("{} applying snapshot failed", self.tag,);
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
                if status.compare_and_swap(
                    JOB_STATUS_PENDING,
                    JOB_STATUS_CANCELLING,
                    Ordering::SeqCst,
                ) == JOB_STATUS_PENDING
                {
                    true
                } else if status.compare_and_swap(
                    JOB_STATUS_RUNNING,
                    JOB_STATUS_CANCELLING,
                    Ordering::SeqCst,
                ) == JOB_STATUS_RUNNING
                {
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
        self.region().get_id()
    }

    pub fn schedule_applying_snapshot(&mut self) {
        let status = Arc::new(AtomicUsize::new(JOB_STATUS_PENDING));
        self.set_snap_state(SnapState::Applying(Arc::clone(&status)));
        let task = RegionTask::Apply {
            region_id: self.get_region_id(),
            status,
        };
        // TODO: gracefully remove region instead.
        if let Err(e) = self.region_sched.schedule(task) {
            info!(
                "failed to to schedule apply job, are we shutting down?";
                "region_id" => self.region.get_id(),
                "peer_id" => self.peer_id,
                "err" => ?e,
            );
        }
    }

    /// Save memory states to disk.
    ///
    /// This function only write data to `ready_ctx`'s `WriteBatch`. It's caller's duty to write
    /// it explicitly to disk. If it's flushed to disk successfully, `post_ready` should be called
    /// to update the memory states properly.
    // Using `&Ready` here to make sure `Ready` struct is not modified in this function. This is
    // a requirement to advance the ready object properly later.
    pub fn handle_raft_ready<H: HandleRaftReadyContext>(
        &mut self,
        ready_ctx: &mut H,
        ready: &Ready,
    ) -> Result<InvokeContext> {
        let mut ctx = InvokeContext::new(self);
        let snapshot_index = if raft::is_empty_snap(ready.snapshot()) {
            0
        } else {
            fail_point!("raft_before_apply_snap");
            self.apply_snapshot(
                &mut ctx,
                ready.snapshot(),
                &ready_ctx.kv_wb(),
                &ready_ctx.raft_wb(),
            )?;
            fail_point!("raft_after_apply_snap");

            last_index(&ctx.raft_state)
        };

        if ready.must_sync() {
            ready_ctx.set_sync_log(true);
        }

        if !ready.entries().is_empty() {
            self.append(&mut ctx, ready.entries(), ready_ctx)?;
        }

        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if ctx.raft_state.get_last_index() > 0 {
            if let Some(hs) = ready.hs() {
                ctx.raft_state.set_hard_state(hs.clone());
            }
        }

        // Save raft state if it has changed or peer has applied a snapshot.
        if ctx.raft_state != self.raft_state || snapshot_index != 0 {
            ctx.save_raft_state_to(ready_ctx.raft_wb_mut())?;
            if snapshot_index > 0 {
                // in case of restart happen when we just write region state to Applying,
                // but not write raft_local_state to raft rocksdb in time.
                // we write raft state to default rocksdb, with last index set to snap index,
                // in case of recv raft log after snapshot.
                ctx.save_snapshot_raft_state_to(snapshot_index, &mut ready_ctx.kv_wb_mut())?;
            }
        }

        // only when apply snapshot
        if snapshot_index != 0 {
            ctx.save_apply_state_to(&mut ready_ctx.kv_wb_mut())?;
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
            if let Err(e) = self.clear_extra_data(self.region()) {
                // No need panic here, when applying snapshot, the deletion will be tried
                // again. But if the region range changes, like [a, c) -> [a, b) and [b, c),
                // [b, c) will be kept in rocksdb until a covered snapshot is applied or
                // store is restarted.
                error!(
                    "failed to cleanup data, may leave some dirty data";
                    "region_id" => self.region.get_id(),
                    "peer_id" => self.peer_id,
                    "err" => ?e,
                );
            }
        }

        self.schedule_applying_snapshot();
        let prev_region = self.region().clone();
        self.set_region(snap_region);

        Some(ApplySnapResult {
            prev_region,
            region: self.region().clone(),
        })
    }
}

fn get_sync_log_from_entry(entry: &Entry) -> bool {
    if entry.get_sync_log() {
        return true;
    }

    let ctx = entry.get_context();
    if !ctx.is_empty() {
        let ctx = ProposalContext::from_bytes(ctx);
        if ctx.contains(ProposalContext::SYNC_LOG) {
            return true;
        }
    }

    false
}

pub fn fetch_entries_to(
    engine: &DB,
    region_id: u64,
    low: u64,
    high: u64,
    max_size: u64,
    buf: &mut Vec<Entry>,
) -> raft::Result<u64> {
    let mut total_size: u64 = 0;
    let mut next_index = low;
    let mut exceeded_max_size = false;
    if high - low <= RAFT_LOG_MULTI_GET_CNT {
        // If election happens in inactive regions, they will just try
        // to fetch one empty log.
        for i in low..high {
            let key = keys::raft_log_key(region_id, i);
            match engine.get(&key) {
                Ok(None) => return Err(RaftError::Store(StorageError::Unavailable)),
                Ok(Some(v)) => {
                    let mut entry = Entry::default();
                    entry.merge_from_bytes(&v)?;
                    assert_eq!(entry.get_index(), i);
                    total_size += v.len() as u64;
                    if buf.is_empty() || total_size <= max_size {
                        buf.push(entry);
                    }
                    if total_size > max_size {
                        break;
                    }
                }
                Err(e) => return Err(storage_error(e)),
            }
        }
        return Ok(total_size);
    }

    let start_key = keys::raft_log_key(region_id, low);
    let end_key = keys::raft_log_key(region_id, high);
    engine.scan(
        &start_key,
        &end_key,
        true, // fill_cache
        |_, value| {
            let mut entry = Entry::default();
            entry.merge_from_bytes(value)?;

            // May meet gap or has been compacted.
            if entry.get_index() != next_index {
                return Ok(false);
            }
            next_index += 1;

            total_size += value.len() as u64;
            exceeded_max_size = total_size > max_size;
            if !exceeded_max_size || buf.is_empty() {
                buf.push(entry);
            }
            Ok(!exceeded_max_size)
        },
    )?;

    // If we get the correct number of entries, returns,
    // or the total size almost exceeds max_size, returns.
    if buf.len() == (high - low) as usize || exceeded_max_size {
        return Ok(total_size);
    }

    // Here means we don't fetch enough entries.
    Err(RaftError::Store(StorageError::Unavailable))
}

/// Delete all meta belong to the region. Results are stored in `wb`.
pub fn clear_meta(
    engines: &Engines,
    kv_wb: &RocksWriteBatch,
    raft_wb: &RocksWriteBatch,
    region_id: u64,
    raft_state: &RaftLocalState,
) -> Result<()> {
    let t = Instant::now();
    box_try!(kv_wb.delete_cf(CF_RAFT, &keys::region_state_key(region_id)));
    box_try!(kv_wb.delete_cf(CF_RAFT, &keys::apply_state_key(region_id)));

    let last_index = last_index(raft_state);
    let mut first_index = last_index + 1;
    let begin_log_key = keys::raft_log_key(region_id, 0);
    let end_log_key = keys::raft_log_key(region_id, first_index);
    engines
        .raft
        .scan(&begin_log_key, &end_log_key, false, |key, _| {
            first_index = keys::raft_log_index(key).unwrap();
            Ok(false)
        })?;
    for id in first_index..=last_index {
        box_try!(raft_wb.delete(&keys::raft_log_key(region_id, id)));
    }
    box_try!(raft_wb.delete(&keys::raft_state_key(region_id)));

    info!(
        "finish clear peer meta";
        "region_id" => region_id,
        "meta_key" => 1,
        "apply_key" => 1,
        "raft_key" => 1,
        "raft_logs" => last_index + 1 - first_index,
        "takes" => ?t.elapsed(),
    );
    Ok(())
}

pub fn do_snapshot<E>(
    mgr: SnapManager,
    raft_snap: E::Snapshot,
    kv_snap: E::Snapshot,
    region_id: u64,
) -> raft::Result<Snapshot>
where
    E: KvEngine,
{
    debug!(
        "begin to generate a snapshot";
        "region_id" => region_id,
    );

    let msg = kv_snap
        .get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))
        .map_err(into_other::<_, raft::Error>)?;
    let apply_state: RaftApplyState = match msg {
        None => {
            return Err(storage_error(format!(
                "could not load raft state of region {}",
                region_id
            )));
        }
        Some(state) => state,
    };

    let idx = apply_state.get_applied_index();
    let term = if idx == apply_state.get_truncated_state().get_index() {
        apply_state.get_truncated_state().get_term()
    } else {
        let msg = raft_snap
            .get_msg::<Entry>(&keys::raft_log_key(region_id, idx))
            .map_err(into_other::<_, raft::Error>)?;
        match msg {
            None => {
                return Err(storage_error(format!(
                    "entry {} of {} not found.",
                    idx, region_id
                )));
            }
            Some(entry) => entry.get_term(),
        }
    };
    // Release raft engine snapshot to avoid too many open files.
    drop(raft_snap);

    let key = SnapKey::new(region_id, term, idx);

    mgr.register(key.clone(), SnapEntry::Generating);
    defer!(mgr.deregister(&key, &SnapEntry::Generating));

    let state: RegionLocalState = kv_snap
        .get_msg_cf(CF_RAFT, &keys::region_state_key(key.region_id))
        .and_then(|res| match res {
            None => Err(box_err!("region {} could not find region info", region_id)),
            Some(state) => Ok(state),
        })
        .map_err(into_other::<_, raft::Error>)?;

    if state.get_state() != PeerState::Normal {
        return Err(storage_error(format!(
            "snap job for {} seems stale, skip.",
            region_id
        )));
    }

    let mut snapshot = Snapshot::default();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let conf_state = conf_state_from_region(state.get_region());
    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut s = mgr.get_snapshot_for_building::<E>(&key)?;
    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::default();
    snap_data.set_region(state.get_region().clone());
    let mut stat = SnapshotStatistics::new();
    s.build(
        &kv_snap,
        state.get_region(),
        &mut snap_data,
        &mut stat,
        Box::new(mgr.clone()),
    )?;
    let v = snap_data.write_to_bytes()?;
    snapshot.set_data(v);

    SNAPSHOT_KV_COUNT_HISTOGRAM.observe(stat.kv_count as f64);
    SNAPSHOT_SIZE_HISTOGRAM.observe(stat.size as f64);

    Ok(snapshot)
}

// When we bootstrap the region we must call this to initialize region local state first.
pub fn write_initial_raft_state<T: MutableTrait>(raft_wb: &T, region_id: u64) -> Result<()> {
    let mut raft_state = RaftLocalState::default();
    raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);

    raft_wb.put_msg(&keys::raft_state_key(region_id), &raft_state)?;
    Ok(())
}

// When we bootstrap the region or handling split new region, we must
// call this to initialize region apply state first.
pub fn write_initial_apply_state<T: MutableTrait>(kv_wb: &T, region_id: u64) -> Result<()> {
    let mut apply_state = RaftApplyState::default();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_term(RAFT_INIT_LOG_TERM);

    kv_wb.put_msg_cf(CF_RAFT, &keys::apply_state_key(region_id), &apply_state)?;
    Ok(())
}

pub fn write_peer_state<T: MutableTrait>(
    kv_wb: &T,
    region: &metapb::Region,
    state: PeerState,
    merge_state: Option<MergeState>,
) -> Result<()> {
    let region_id = region.get_id();
    let mut region_state = RegionLocalState::default();
    region_state.set_state(state);
    region_state.set_region(region.clone());
    if let Some(state) = merge_state {
        region_state.set_merge_state(state);
    }

    debug!(
        "writing merge state";
        "region_id" => region_id,
        "state" => ?region_state,
    );
    kv_wb.put_msg_cf(CF_RAFT, &keys::region_state_key(region_id), &region_state)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::coprocessor::CoprocessorHost;
    use crate::store::fsm::apply::compact_raft_log;
    use crate::store::worker::RegionRunner;
    use crate::store::worker::RegionTask;
    use crate::store::{bootstrap_store, initial_region, prepare_bootstrap_cluster};
    use engine::rocks::util::new_engine;
    use engine::Engines;
    use engine_rocks::{Compat, RocksWriteBatch};
    use engine_traits::{ALL_CFS, CF_DEFAULT};
    use engine_traits::{WriteBatchExt};
    use kvproto::raft_serverpb::RaftSnapshotData;
    use raft::eraftpb::HardState;
    use raft::eraftpb::{ConfState, Entry};
    use raft::{Error as RaftError, StorageError};
    use std::cell::RefCell;
    use std::path::Path;
    use std::sync::atomic::*;
    use std::sync::mpsc::*;
    use std::sync::*;
    use std::time::Duration;
    use tempfile::{Builder, TempDir};
    use tikv_util::worker::{Scheduler, Worker};

    use super::*;

    fn new_storage(sched: Scheduler<RegionTask>, path: &TempDir) -> PeerStorage {
        let kv_db =
            Arc::new(new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap());
        let raft_path = path.path().join(Path::new("raft"));
        let raft_db =
            Arc::new(new_engine(raft_path.to_str().unwrap(), None, &[CF_DEFAULT], None).unwrap());
        let shared_block_cache = false;
        let engines = Engines::new(kv_db, raft_db, shared_block_cache);
        bootstrap_store(&engines, 1, 1).unwrap();

        let region = initial_region(1, 1, 1);
        prepare_bootstrap_cluster(&engines, &region).unwrap();
        PeerStorage::new(engines, &region, sched, 0, "".to_owned()).unwrap()
    }

    struct ReadyContext {
        kv_wb: RocksWriteBatch,
        raft_wb: RocksWriteBatch,
        sync_log: bool,
    }

    impl ReadyContext {
        fn new(s: &PeerStorage) -> ReadyContext {
            ReadyContext {
                kv_wb: s.engines.kv.c().write_batch(),
                raft_wb: s.engines.raft.c().write_batch(),
                sync_log: false,
            }
        }
    }

    impl HandleRaftReadyContext for ReadyContext {
        fn kv_wb(&self) -> &RocksWriteBatch {
            &self.kv_wb
        }
        fn kv_wb_mut(&mut self) -> &mut RocksWriteBatch {
            &mut self.kv_wb
        }
        fn raft_wb(&self) -> &RocksWriteBatch {
            &self.raft_wb
        }
        fn raft_wb_mut(&mut self) -> &mut RocksWriteBatch {
            &mut self.raft_wb
        }
        fn sync_log(&self) -> bool {
            self.sync_log
        }
        fn set_sync_log(&mut self, sync: bool) {
            self.sync_log = sync;
        }
    }

    fn new_storage_from_ents(
        sched: Scheduler<RegionTask>,
        path: &TempDir,
        ents: &[Entry],
    ) -> PeerStorage {
        let mut store = new_storage(sched, path);
        let mut kv_wb = store.engines.kv.c().write_batch();
        let mut ctx = InvokeContext::new(&store);
        let mut ready_ctx = ReadyContext::new(&store);
        store.append(&mut ctx, &ents[1..], &mut ready_ctx).unwrap();
        ctx.apply_state
            .mut_truncated_state()
            .set_index(ents[0].get_index());
        ctx.apply_state
            .mut_truncated_state()
            .set_term(ents[0].get_term());
        ctx.apply_state
            .set_applied_index(ents.last().unwrap().get_index());
        ctx.save_apply_state_to(&mut kv_wb).unwrap();
        store.engines.raft.c().write(&ready_ctx.raft_wb).unwrap();
        store.engines.kv.c().write(&kv_wb).unwrap();
        store.raft_state = ctx.raft_state;
        store.apply_state = ctx.apply_state;
        store
    }

    fn append_ents(store: &mut PeerStorage, ents: &[Entry]) {
        let mut ctx = InvokeContext::new(store);
        let mut ready_ctx = ReadyContext::new(store);
        store.append(&mut ctx, ents, &mut ready_ctx).unwrap();
        ctx.save_raft_state_to(&mut ready_ctx.raft_wb).unwrap();
        store.engines.raft.c().write(&ready_ctx.raft_wb).unwrap();
        store.raft_state = ctx.raft_state;
    }

    fn validate_cache(store: &PeerStorage, exp_ents: &[Entry]) {
        assert_eq!(store.cache.cache, exp_ents);
        for e in exp_ents {
            let key = keys::raft_log_key(store.get_region_id(), e.get_index());
            let bytes = store.engines.raft.get(&key).unwrap().unwrap();
            let mut entry = Entry::default();
            entry.merge_from_bytes(&bytes).unwrap();
            assert_eq!(entry, *e);
        }
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
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

        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
        ];
        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
            let worker = Worker::new("snap-manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let t = store.term(idx);
            if wterm != t {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    fn get_meta_key_count(store: &PeerStorage) -> usize {
        let region_id = store.get_region_id();
        let mut count = 0;
        let (meta_start, meta_end) = (
            keys::region_meta_prefix(region_id),
            keys::region_meta_prefix(region_id + 1),
        );
        store
            .engines
            .kv
            .scan_cf(CF_RAFT, &meta_start, &meta_end, false, |_, _| {
                count += 1;
                Ok(true)
            })
            .unwrap();

        let (raft_start, raft_end) = (
            keys::region_raft_prefix(region_id),
            keys::region_raft_prefix(region_id + 1),
        );
        store
            .engines
            .kv
            .scan_cf(CF_RAFT, &raft_start, &raft_end, false, |_, _| {
                count += 1;
                Ok(true)
            })
            .unwrap();

        store
            .engines
            .raft
            .scan(&raft_start, &raft_end, false, |_, _| {
                count += 1;
                Ok(true)
            })
            .unwrap();

        count
    }

    #[test]
    fn test_storage_clear_meta() {
        let td = Builder::new().prefix("tikv-store").tempdir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.scheduler();
        let mut store = new_storage_from_ents(sched, &td, &[new_entry(3, 3), new_entry(4, 4)]);
        append_ents(&mut store, &[new_entry(5, 5), new_entry(6, 6)]);

        assert_eq!(6, get_meta_key_count(&store));

        let kv_wb = store.engines.kv.c().write_batch();
        let raft_wb = store.engines.raft.c().write_batch();
        store.clear_meta(&kv_wb, &raft_wb).unwrap();
        store.engines.kv.c().write(&kv_wb).unwrap();
        store.engines.raft.c().write(&raft_wb).unwrap();

        assert_eq!(0, get_meta_key_count(&store));
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (
                2,
                6,
                max_u64,
                Err(RaftError::Store(StorageError::Compacted)),
            ),
            (
                3,
                4,
                max_u64,
                Err(RaftError::Store(StorageError::Compacted)),
            ),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (
                4,
                7,
                max_u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            // all
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];

        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
            let worker = Worker::new("snap-manager");
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
        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Err(RaftError::Store(StorageError::Compacted))),
            (4, Ok(())),
            (5, Ok(())),
        ];
        for (i, (idx, werr)) in tests.drain(..).enumerate() {
            let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
            let worker = Worker::new("snap-manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let mut ctx = InvokeContext::new(&store);
            let res = store
                .term(idx)
                .map_err(From::from)
                .and_then(|term| compact_raft_log(&store.tag, &mut ctx.apply_state, idx, term));
            // TODO check exact error type after refactoring error.
            if res.is_err() ^ werr.is_err() {
                panic!("#{}: want {:?}, got {:?}", i, werr, res);
            }
            if res.is_ok() {
                let mut kv_wb = store.engines.kv.c().write_batch();
                ctx.save_apply_state_to(&mut kv_wb).unwrap();
                store.engines.kv.c().write(&kv_wb).unwrap();
            }
        }
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::default();
        cs.set_nodes(vec![1, 2, 3]);

        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap(), None);
        let mut worker = Worker::new("region-worker");
        let sched = worker.scheduler();
        let mut s = new_storage_from_ents(sched.clone(), &td, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let runner = RegionRunner::new(
            s.engines.clone(),
            mgr,
            0,
            true,
            Duration::from_secs(0),
            CoprocessorHost::default(),
            router,
        );
        worker.start(runner).unwrap();
        let snap = s.snapshot(0);
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        gen_task
            .generate_and_schedule_snapshot(&s.engines, &sched)
            .unwrap();
        let snap = match *s.snap_state.borrow() {
            SnapState::Generating(ref rx) => rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(snap.get_metadata().get_index(), 5);
        assert_eq!(snap.get_metadata().get_term(), 5);
        assert!(!snap.get_data().is_empty());

        let mut data = RaftSnapshotData::default();
        protobuf::Message::merge_from_bytes(&mut data, snap.get_data()).unwrap();
        assert_eq!(data.get_region().get_id(), 1);
        assert_eq!(data.get_region().get_peers().len(), 1);

        let (tx, rx) = channel();
        s.set_snap_state(SnapState::Generating(rx));
        // Empty channel should cause snapshot call to wait.
        assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        tx.send(snap.clone()).unwrap();
        assert_eq!(s.snapshot(0), Ok(snap.clone()));
        assert_eq!(*s.snap_tried_cnt.borrow(), 0);

        let (tx, rx) = channel();
        tx.send(snap.clone()).unwrap();
        s.set_snap_state(SnapState::Generating(rx));
        // stale snapshot should be abandoned, snapshot index < request index.
        assert_eq!(
            s.snapshot(snap.get_metadata().get_index() + 1).unwrap_err(),
            unavailable
        );
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);
        // Drop the task.
        let _ = s.gen_snap_task.borrow_mut().take().unwrap();

        let mut ctx = InvokeContext::new(&s);
        let mut kv_wb = s.engines.kv.c().write_batch();
        let mut ready_ctx = ReadyContext::new(&s);
        s.append(
            &mut ctx,
            &[new_entry(6, 5), new_entry(7, 5)],
            &mut ready_ctx,
        )
        .unwrap();
        let mut hs = HardState::default();
        hs.set_commit(7);
        hs.set_term(5);
        ctx.raft_state.set_hard_state(hs);
        ctx.raft_state.set_last_index(7);
        ctx.apply_state.set_applied_index(7);
        ctx.save_raft_state_to(&mut ready_ctx.raft_wb).unwrap();
        ctx.save_apply_state_to(&mut kv_wb).unwrap();
        s.engines.kv.c().write(&kv_wb).unwrap();
        s.engines.raft.c().write(&ready_ctx.raft_wb).unwrap();
        s.apply_state = ctx.apply_state;
        s.raft_state = ctx.raft_state;
        ctx = InvokeContext::new(&s);
        let term = s.term(7).unwrap();
        compact_raft_log(&s.tag, &mut ctx.apply_state, 7, term).unwrap();
        kv_wb = s.engines.kv.c().write_batch();
        ctx.save_apply_state_to(&mut kv_wb).unwrap();
        s.engines.kv.c().write(&kv_wb).unwrap();
        s.apply_state = ctx.apply_state;

        let (tx, rx) = channel();
        tx.send(snap).unwrap();
        s.set_snap_state(SnapState::Generating(rx));
        *s.snap_tried_cnt.borrow_mut() = 1;
        // stale snapshot should be abandoned, snapshot index < truncated index.
        assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        gen_task
            .generate_and_schedule_snapshot(&s.engines, &sched)
            .unwrap();
        match *s.snap_state.borrow() {
            SnapState::Generating(ref rx) => {
                rx.recv_timeout(Duration::from_secs(3)).unwrap();
                worker.stop().unwrap().join().unwrap();
                match rx.recv_timeout(Duration::from_secs(3)) {
                    Err(RecvTimeoutError::Disconnected) => {}
                    res => panic!("unexpected result: {:?}", res),
                }
            }
            ref s => panic!("unexpected state {:?}", s),
        }
        // Disconnected channel should trigger another try.
        assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        gen_task
            .generate_and_schedule_snapshot(&s.engines, &sched)
            .unwrap_err();
        assert_eq!(*s.snap_tried_cnt.borrow(), 2);

        for cnt in 2..super::MAX_SNAP_TRY_CNT {
            // Scheduled job failed should trigger .
            assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
            let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
            gen_task
                .generate_and_schedule_snapshot(&s.engines, &sched)
                .unwrap_err();
            assert_eq!(*s.snap_tried_cnt.borrow(), cnt + 1);
        }

        // When retry too many times, it should report a different error.
        match s.snapshot(0) {
            Err(RaftError::Store(StorageError::Other(_))) => {}
            res => panic!("unexpected res: {:?}", res),
        }
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                vec![new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                vec![new_entry(4, 6), new_entry(5, 6)],
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            ),
            // truncate incoming entries, truncate the existing entries and append
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                vec![new_entry(4, 5)],
            ),
            // truncate the existing entries and append
            (vec![new_entry(4, 5)], vec![new_entry(4, 5)]),
            // direct append
            (
                vec![new_entry(6, 5)],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
            let worker = Worker::new("snap-manager");
            let sched = worker.scheduler();
            let mut store = new_storage_from_ents(sched, &td, &ents);
            append_ents(&mut store, &entries);
            let li = store.last_index();
            let actual_entries = store.entries(4, li + 1, u64::max_value()).unwrap();
            if actual_entries != wentries {
                panic!("#{}: want {:?}, got {:?}", i, wentries, actual_entries);
            }
        }
    }

    #[test]
    fn test_storage_cache_fetch() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.scheduler();
        let mut store = new_storage_from_ents(sched, &td, &ents);
        store.cache.cache.clear();
        // empty cache should fetch data from rocksdb directly.
        let mut res = store.entries(4, 6, u64::max_value()).unwrap();
        assert_eq!(*res, ents[1..]);

        let entries = vec![new_entry(6, 5), new_entry(7, 5)];
        append_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // direct cache access
        res = store.entries(6, 8, u64::max_value()).unwrap();
        assert_eq!(res, entries);

        // size limit should be supported correctly.
        res = store.entries(4, 8, 0).unwrap();
        assert_eq!(res, vec![new_entry(4, 4)]);
        let mut size = ents[1..].iter().map(|e| u64::from(e.compute_size())).sum();
        res = store.entries(4, 8, size).unwrap();
        let mut exp_res = ents[1..].to_vec();
        assert_eq!(res, exp_res);
        for e in &entries {
            size += u64::from(e.compute_size());
            exp_res.push(e.clone());
            res = store.entries(4, 8, size).unwrap();
            assert_eq!(res, exp_res);
        }

        // range limit should be supported correctly.
        for low in 4..9 {
            for high in low..9 {
                let res = store.entries(low, high, u64::max_value()).unwrap();
                assert_eq!(*res, exp_res[low as usize - 4..high as usize - 4]);
            }
        }
    }

    #[test]
    fn test_storage_cache_update() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.scheduler();
        let mut store = new_storage_from_ents(sched, &td, &ents);
        store.cache.cache.clear();

        // initial cache
        let mut entries = vec![new_entry(6, 5), new_entry(7, 5)];
        append_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // rewrite
        entries = vec![new_entry(6, 6), new_entry(7, 6)];
        append_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // rewrite old entry
        entries = vec![new_entry(5, 6), new_entry(6, 6)];
        append_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // partial rewrite
        entries = vec![new_entry(6, 7), new_entry(7, 7)];
        append_ents(&mut store, &entries);
        let mut exp_res = vec![new_entry(5, 6), new_entry(6, 7), new_entry(7, 7)];
        validate_cache(&store, &exp_res);

        // direct append
        entries = vec![new_entry(8, 7), new_entry(9, 7)];
        append_ents(&mut store, &entries);
        exp_res.extend_from_slice(&entries);
        validate_cache(&store, &exp_res);

        // rewrite middle
        entries = vec![new_entry(7, 8)];
        append_ents(&mut store, &entries);
        exp_res.truncate(2);
        exp_res.push(new_entry(7, 8));
        validate_cache(&store, &exp_res);

        let cap = MAX_CACHE_CAPACITY as u64;

        // result overflow
        entries = (3..=cap).map(|i| new_entry(i + 5, 8)).collect();
        append_ents(&mut store, &entries);
        exp_res.remove(0);
        exp_res.extend_from_slice(&entries);
        validate_cache(&store, &exp_res);

        // input overflow
        entries = (0..=cap).map(|i| new_entry(i + cap + 6, 8)).collect();
        append_ents(&mut store, &entries);
        exp_res = entries[entries.len() - cap as usize..].to_vec();
        validate_cache(&store, &exp_res);

        // compact
        store.compact_to(cap + 10);
        exp_res = (cap + 10..cap * 2 + 7).map(|i| new_entry(i, 8)).collect();
        validate_cache(&store, &exp_res);

        // compact shrink
        assert!(store.cache.cache.capacity() >= cap as usize);
        store.compact_to(cap * 2);
        exp_res = (cap * 2..cap * 2 + 7).map(|i| new_entry(i, 8)).collect();
        validate_cache(&store, &exp_res);
        assert!(store.cache.cache.capacity() < cap as usize);

        // append shrink
        entries = (0..=cap).map(|i| new_entry(i, 8)).collect();
        append_ents(&mut store, &entries);
        assert!(store.cache.cache.capacity() >= cap as usize);
        append_ents(&mut store, &[new_entry(6, 8)]);
        exp_res = (1..7).map(|i| new_entry(i, 8)).collect();
        validate_cache(&store, &exp_res);
        assert!(store.cache.cache.capacity() < cap as usize);

        // compact all
        store.compact_to(cap + 2);
        validate_cache(&store, &[]);
        // invalid compaction should be ignored.
        store.compact_to(cap);
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let mut cs = ConfState::default();
        cs.set_nodes(vec![1, 2, 3]);

        let td1 = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let snap_dir = Builder::new().prefix("snap").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap(), None);
        let mut worker = Worker::new("snap-manager");
        let sched = worker.scheduler();
        let s1 = new_storage_from_ents(sched.clone(), &td1, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let runner = RegionRunner::new(
            s1.engines.clone(),
            mgr,
            0,
            true,
            Duration::from_secs(0),
            CoprocessorHost::default(),
            router,
        );
        worker.start(runner).unwrap();
        assert!(s1.snapshot(0).is_err());
        let gen_task = s1.gen_snap_task.borrow_mut().take().unwrap();
        gen_task
            .generate_and_schedule_snapshot(&s1.engines, &sched)
            .unwrap();
        let snap1 = match *s1.snap_state.borrow() {
            SnapState::Generating(ref rx) => rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(s1.truncated_index(), 3);
        assert_eq!(s1.truncated_term(), 3);
        worker.stop().unwrap().join().unwrap();

        let td2 = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let mut s2 = new_storage(sched.clone(), &td2);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        let mut ctx = InvokeContext::new(&s2);
        assert_ne!(ctx.last_term, snap1.get_metadata().get_term());
        let kv_wb = s2.engines.kv.c().write_batch();
        let raft_wb = s2.engines.raft.c().write_batch();
        s2.apply_snapshot(&mut ctx, &snap1, &kv_wb, &raft_wb)
            .unwrap();
        assert_eq!(ctx.last_term, snap1.get_metadata().get_term());
        assert_eq!(ctx.apply_state.get_applied_index(), 6);
        assert_eq!(ctx.raft_state.get_last_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_term(), 6);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        validate_cache(&s2, &[]);

        let td3 = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let ents = &[new_entry(3, 3), new_entry(4, 3)];
        let mut s3 = new_storage_from_ents(sched, &td3, ents);
        validate_cache(&s3, &ents[1..]);
        let mut ctx = InvokeContext::new(&s3);
        assert_ne!(ctx.last_term, snap1.get_metadata().get_term());
        let kv_wb = s3.engines.kv.c().write_batch();
        let raft_wb = s3.engines.raft.c().write_batch();
        s3.apply_snapshot(&mut ctx, &snap1, &kv_wb, &raft_wb)
            .unwrap();
        assert_eq!(ctx.last_term, snap1.get_metadata().get_term());
        assert_eq!(ctx.apply_state.get_applied_index(), 6);
        assert_eq!(ctx.raft_state.get_last_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_index(), 6);
        assert_eq!(ctx.apply_state.get_truncated_state().get_term(), 6);
        validate_cache(&s3, &[]);
    }

    #[test]
    fn test_canceling_snapshot() {
        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.scheduler();
        let mut s = new_storage(sched, &td);

        // PENDING can be canceled directly.
        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_PENDING,
        ))));
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        // RUNNING can't be canceled directly.
        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_RUNNING,
        ))));
        assert!(!s.cancel_applying_snap());
        assert_eq!(
            *s.snap_state.borrow(),
            SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_CANCELLING)))
        );
        // CANCEL can't be canceled again.
        assert!(!s.cancel_applying_snap());

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_CANCELLED,
        ))));
        // canceled snapshot can be cancel directly.
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FINISHED,
        ))));
        assert!(s.cancel_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FAILED,
        ))));
        let res = panic_hook::recover_safe(|| s.cancel_applying_snap());
        assert!(res.is_err());
    }

    #[test]
    fn test_try_finish_snapshot() {
        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let worker = Worker::new("snap-manager");
        let sched = worker.scheduler();
        let mut s = new_storage(sched, &td);

        // PENDING can be finished.
        let mut snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING)));
        s.snap_state = RefCell::new(snap_state);
        assert!(s.check_applying_snap());
        assert_eq!(
            *s.snap_state.borrow(),
            SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING)))
        );

        // RUNNING can't be finished.
        snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)));
        s.snap_state = RefCell::new(snap_state);
        assert!(s.check_applying_snap());
        assert_eq!(
            *s.snap_state.borrow(),
            SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)))
        );

        snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_CANCELLED)));
        s.snap_state = RefCell::new(snap_state);
        assert!(!s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);
        // ApplyAborted is not applying snapshot.
        assert!(!s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FINISHED,
        ))));
        assert!(!s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);
        // Relax is not applying snapshot.
        assert!(!s.check_applying_snap());
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FAILED,
        ))));
        let res = panic_hook::recover_safe(|| s.check_applying_snap());
        assert!(res.is_err());
    }

    #[test]
    fn test_sync_log() {
        let mut tbl = vec![];

        // Do not sync empty entrise.
        tbl.push((Entry::default(), false));

        // Sync if sync_log is set.
        let mut e = Entry::default();
        e.set_sync_log(true);
        tbl.push((e, true));

        // Sync if context is marked sync.
        let context = ProposalContext::SYNC_LOG.to_vec();
        let mut e = Entry::default();
        e.set_context(context);
        tbl.push((e.clone(), true));

        // Sync if sync_log is set and context is marked sync_log.
        e.set_sync_log(true);
        tbl.push((e, true));

        for (e, sync) in tbl {
            assert_eq!(get_sync_log_from_entry(&e), sync, "{:?}", e);
        }
    }
}
