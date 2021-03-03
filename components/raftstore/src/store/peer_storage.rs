// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, error, u64};

use engine_traits::CF_RAFT;
use engine_traits::{Engines, KvEngine, Mutable, Peekable};
use keys::{self, enc_end_key, enc_start_key};
use kvproto::metapb::{self, Region};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftApplyState, RaftLocalState, RaftSnapshotData, RegionLocalState,
};
use protobuf::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{self, Error as RaftError, RaftState, Ready, Storage, StorageError};

use crate::store::fsm::GenSnapTask;
use crate::store::util;
use crate::store::ProposalContext;
use crate::{Error, Result};
use engine_traits::{RaftEngine, RaftLogBatch};
use into_other::into_other;
use tikv_util::time::{duration_to_sec, Instant as UtilInstant};
use tikv_util::worker::Scheduler;

use super::local_metrics::StoreIOLockMetrics;
use super::metrics::*;
use super::worker::RegionTask;
use super::{SnapEntry, SnapKey, SnapManager, SnapshotStatistics};

use crate::store::fsm::async_io::AsyncWriter;
use std::sync::atomic::AtomicU64;

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;

/// The initial region epoch version.
pub const INIT_EPOCH_VER: u64 = 1;
/// The initial region epoch conf_version.
pub const INIT_EPOCH_CONF_VER: u64 = 1;

// One extra slot for VecDeque internal usage.
const SHRINK_CACHE_CAPACITY: usize = 64;

pub const JOB_STATUS_PENDING: usize = 0;
pub const JOB_STATUS_RUNNING: usize = 1;
pub const JOB_STATUS_CANCELLING: usize = 2;
pub const JOB_STATUS_CANCELLED: usize = 3;
pub const JOB_STATUS_FINISHED: usize = 4;
pub const JOB_STATUS_FAILED: usize = 5;

/// Possible status returned by `check_applying_snap`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckApplyingSnapStatus {
    /// A snapshot is just applied.
    Success,
    /// A snapshot is being applied.
    Applying,
    /// No snapshot is being applied at all or the snapshot is cancelled
    Idle,
}

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

pub const ENTRY_MEM_SIZE: usize = std::mem::size_of::<Entry>();

struct EntryCache {
    cache: VecDeque<Entry>,
    hit: Cell<i64>,
    miss: Cell<i64>,
    mem_size_change: i64,
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
                    self.update_mem_size_change_before_clear();
                    self.cache.clear();
                } else {
                    let left = self.cache.len() - (cache_last_index - first_index + 1) as usize;
                    self.mem_size_change -= self
                        .cache
                        .iter()
                        .skip(left)
                        .map(|e| (e.data.capacity() + e.context.capacity()) as i64)
                        .sum::<i64>();
                    self.cache.truncate(left);
                }
                if self.cache.len() + entries.len() < SHRINK_CACHE_CAPACITY
                    && self.cache.capacity() > SHRINK_CACHE_CAPACITY
                {
                    let old_capacity = self.cache.capacity();
                    self.cache.shrink_to_fit();
                    self.mem_size_change += self.get_cache_vec_mem_size_change(
                        self.cache.capacity() as i64,
                        old_capacity as i64,
                    )
                }
            } else if cache_last_index + 1 < first_index {
                panic!(
                    "{} unexpected hole: {} < {}",
                    tag, cache_last_index, first_index
                );
            }
        }

        let old_capacity = self.cache.capacity();
        let mut entries_mem_size = 0;
        for e in entries {
            self.cache.push_back(e.to_owned());
            entries_mem_size += (e.data.capacity() + e.context.capacity()) as i64;
        }
        self.mem_size_change += self
            .get_cache_vec_mem_size_change(self.cache.capacity() as i64, old_capacity as i64)
            + entries_mem_size;
    }

    pub fn term(&self, idx: u64) -> u64 {
        let cache_low = self.cache.front().unwrap().get_index();
        let start_idx = idx.checked_sub(cache_low).unwrap() as usize;
        self.cache[start_idx].get_term()
    }

    pub fn compact_to(&mut self, idx: u64) {
        let cache_first_idx = self.first_index().unwrap_or(u64::MAX);
        if cache_first_idx > idx {
            return;
        }
        let mut drained_cache_entries_size = 0;
        let cache_last_idx = self.cache.back().unwrap().get_index();
        // Use `cache_last_idx + 1` to make sure cache can be cleared completely
        // if necessary.
        self.cache
            .drain(..(cmp::min(cache_last_idx + 1, idx) - cache_first_idx) as usize)
            .for_each(|e| {
                drained_cache_entries_size += (e.data.capacity() + e.context.capacity()) as i64
            });
        self.mem_size_change -= drained_cache_entries_size;
        if self.cache.len() < SHRINK_CACHE_CAPACITY && self.cache.capacity() > SHRINK_CACHE_CAPACITY
        {
            let old_capacity = self.cache.capacity();
            // So the peer storage doesn't have much writes since the proposal of compaction,
            // we can consider this peer is going to be inactive.
            self.cache.shrink_to_fit();
            self.mem_size_change += self
                .get_cache_vec_mem_size_change(self.cache.capacity() as i64, old_capacity as i64)
        }
    }

    fn update_mem_size_change_before_clear(&mut self) {
        self.mem_size_change -= self
            .cache
            .iter()
            .map(|e| (e.data.capacity() + e.context.capacity()) as i64)
            .sum::<i64>();
    }

    fn get_cache_vec_mem_size_change(&self, new_capacity: i64, old_capacity: i64) -> i64 {
        ENTRY_MEM_SIZE as i64 * (new_capacity - old_capacity)
    }

    fn get_total_mem_size(&self) -> i64 {
        let data_size: usize = self
            .cache
            .iter()
            .map(|e| e.data.capacity() + e.context.capacity())
            .sum();
        (ENTRY_MEM_SIZE * self.cache.capacity() + data_size) as i64
    }

    fn flush_mem_size_change(&mut self) {
        RAFT_ENTRIES_CACHES_GAUGE.add(self.mem_size_change);
        self.mem_size_change = 0;
    }

    fn flush_stats(&self) {
        let hit = self.hit.replace(0);
        RAFT_ENTRY_FETCHES.hit.inc_by(hit);
        let miss = self.miss.replace(0);
        RAFT_ENTRY_FETCHES.miss.inc_by(miss);
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for EntryCache {
    fn default() -> Self {
        let cache = VecDeque::default();
        let size = ENTRY_MEM_SIZE * cache.capacity();
        let mut entry_cache = EntryCache {
            cache,
            hit: Cell::new(0),
            miss: Cell::new(0),
            mem_size_change: size as i64,
        };
        entry_cache.flush_mem_size_change();
        entry_cache
    }
}

impl Drop for EntryCache {
    fn drop(&mut self) {
        self.flush_mem_size_change();
        RAFT_ENTRIES_CACHES_GAUGE.sub(self.get_total_mem_size());
        self.flush_stats();
    }
}

pub trait HandleRaftReadyContext<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn async_writer(&mut self, id: usize) -> (&AsyncWriter<EK, ER>, &mut StoreIOLockMetrics);
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
    pub destroyed_regions: Vec<metapb::Region>,
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
    /// If the ready has new entries.
    pub has_new_entries: bool,
    /// The old region is stored here if there is a snapshot.
    pub snap_region: Option<Region>,
    /// The regions whose range are overlapped with this region
    pub destroyed_regions: Vec<metapb::Region>,
}

impl InvokeContext {
    pub fn new<EK: KvEngine, ER: RaftEngine>(store: &PeerStorage<EK, ER>) -> InvokeContext {
        InvokeContext {
            region_id: store.get_region_id(),
            raft_state: store.raft_state.clone(),
            apply_state: store.apply_state.clone(),
            last_term: store.last_term,
            has_new_entries: false,
            snap_region: None,
            destroyed_regions: vec![],
        }
    }

    #[inline]
    pub fn has_snapshot(&self) -> bool {
        self.snap_region.is_some()
    }

    #[inline]
    pub fn save_raft_state_to<W: RaftLogBatch>(&self, raft_wb: &mut W) -> Result<()> {
        raft_wb.put_raft_state(self.region_id, &self.raft_state)?;
        Ok(())
    }

    #[inline]
    pub fn save_snapshot_raft_state_to(
        &self,
        snapshot_index: u64,
        kv_wb: &mut impl Mutable,
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
    pub fn save_apply_state_to(&self, kv_wb: &mut impl Mutable) -> Result<()> {
        kv_wb.put_msg_cf(
            CF_RAFT,
            &keys::apply_state_key(self.region_id),
            &self.apply_state,
        )?;
        Ok(())
    }
}

pub fn recover_from_applying_state<EK: KvEngine, ER: RaftEngine>(
    engines: &Engines<EK, ER>,
    raft_wb: &mut ER::LogBatch,
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

    let raft_state = box_try!(engines.raft.get_raft_state(region_id)).unwrap_or_default();

    // if we recv append log when applying snapshot, last_index in raft_local_state will
    // larger than snapshot_index. since raft_local_state is written to raft engine, and
    // raft write_batch is written after kv write_batch, raft_local_state may wrong if
    // restart happen between the two write. so we copy raft_local_state to kv engine
    // (snapshot_raft_state), and set snapshot_raft_state.last_index = snapshot_index.
    // after restart, we need check last_index.
    if last_index(&snapshot_raft_state) > last_index(&raft_state) {
        raft_wb.put_raft_state(region_id, &snapshot_raft_state)?;
    }
    Ok(())
}

fn init_applied_index_term<EK: KvEngine, ER: RaftEngine>(
    engines: &Engines<EK, ER>,
    region: &Region,
    apply_state: &RaftApplyState,
) -> Result<u64> {
    if apply_state.applied_index == RAFT_INIT_LOG_INDEX {
        return Ok(RAFT_INIT_LOG_TERM);
    }
    let truncated_state = apply_state.get_truncated_state();
    if apply_state.applied_index == truncated_state.get_index() {
        return Ok(truncated_state.get_term());
    }

    match engines
        .raft
        .get_entry(region.get_id(), apply_state.applied_index)?
    {
        Some(e) => Ok(e.term),
        None => Err(box_err!(
            "[region {}] entry at apply index {} doesn't exist, may lose data.",
            region.get_id(),
            apply_state.applied_index
        )),
    }
}

fn init_raft_state<EK: KvEngine, ER: RaftEngine>(
    engines: &Engines<EK, ER>,
    region: &Region,
) -> Result<RaftLocalState> {
    if let Some(state) = engines.raft.get_raft_state(region.get_id())? {
        return Ok(state);
    }

    let mut raft_state = RaftLocalState::default();
    if util::is_region_initialized(region) {
        // new split region
        raft_state.last_index = RAFT_INIT_LOG_INDEX;
        raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
        raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);
        engines.raft.put_raft_state(region.get_id(), &raft_state)?;
    }
    Ok(raft_state)
}

fn init_apply_state<EK: KvEngine, ER: RaftEngine>(
    engines: &Engines<EK, ER>,
    region: &Region,
) -> Result<RaftApplyState> {
    Ok(
        match engines
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(region.get_id()))?
        {
            Some(s) => s,
            None => {
                let mut apply_state = RaftApplyState::default();
                if util::is_region_initialized(region) {
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

fn init_last_term<EK: KvEngine, ER: RaftEngine>(
    engines: &Engines<EK, ER>,
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
    let entry = engines.raft.get_entry(region.get_id(), last_idx)?;
    match entry {
        None => Err(box_err!(
            "[region {}] entry at {} doesn't exist, may lose data.",
            region.get_id(),
            last_idx
        )),
        Some(e) => Ok(e.get_term()),
    }
}

fn validate_states<EK: KvEngine, ER: RaftEngine>(
    region_id: u64,
    engines: &Engines<EK, ER>,
    raft_state: &mut RaftLocalState,
    apply_state: &RaftApplyState,
) -> Result<()> {
    let last_index = raft_state.get_last_index();
    let mut commit_index = raft_state.get_hard_state().get_commit();
    let recorded_commit_index = apply_state.get_commit_index();
    let state_str = || -> String {
        format!(
            "region {}, raft state {:?}, apply state {:?}",
            region_id, raft_state, apply_state
        )
    };
    // The commit index of raft state may be less than the recorded commit index.
    // If so, forward the commit index.
    if commit_index < recorded_commit_index {
        let entry = engines.raft.get_entry(region_id, recorded_commit_index)?;
        if entry.map_or(true, |e| e.get_term() != apply_state.get_commit_term()) {
            return Err(box_err!(
                "log at recorded commit index [{}] {} doesn't exist, may lose data, {}",
                apply_state.get_commit_term(),
                recorded_commit_index,
                state_str()
            ));
        }
        info!("updating commit index"; "region_id" => region_id, "old" => commit_index, "new" => recorded_commit_index);
        commit_index = recorded_commit_index;
    }
    // Invariant: applied index <= max(commit index, recorded commit index)
    if apply_state.get_applied_index() > commit_index {
        return Err(box_err!(
            "applied index > max(commit index, recorded commit index), {}",
            state_str()
        ));
    }
    // Invariant: max(commit index, recorded commit index) <= last index
    if commit_index > last_index {
        return Err(box_err!(
            "max(commit index, recorded commit index) > last index, {}",
            state_str()
        ));
    }
    // Since the entries must be persisted before applying, the term of raft state should also
    // be persisted. So it should be greater than the commit term of apply state.
    if raft_state.get_hard_state().get_term() < apply_state.get_commit_term() {
        return Err(box_err!(
            "term of raft state < commit term of apply state, {}",
            state_str()
        ));
    }

    raft_state.mut_hard_state().set_commit(commit_index);

    Ok(())
}

pub struct PeerStorage<EK, ER>
where
    EK: KvEngine,
{
    pub engines: Engines<EK, ER>,

    peer_id: u64,
    region: metapb::Region,
    raft_state: RaftLocalState,
    apply_state: RaftApplyState,
    applied_index_term: u64,
    last_term: u64,

    snap_state: RefCell<SnapState>,
    gen_snap_task: RefCell<Option<GenSnapTask>>,
    region_sched: Scheduler<RegionTask<EK::Snapshot>>,
    snap_tried_cnt: RefCell<usize>,

    // Entry cache if `ER doesn't have an internal entry cache.
    cache: Option<EntryCache>,

    pub tag: String,
}

impl<EK, ER> Storage for PeerStorage<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
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

impl<EK, ER> PeerStorage<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        engines: Engines<EK, ER>,
        region: &metapb::Region,
        region_sched: Scheduler<RegionTask<EK::Snapshot>>,
        peer_id: u64,
        tag: String,
    ) -> Result<PeerStorage<EK, ER>> {
        debug!(
            "creating storage on specified path";
            "region_id" => region.get_id(),
            "peer_id" => peer_id,
            "path" => ?engines.kv.path(),
        );
        let mut raft_state = init_raft_state(&engines, region)?;
        let apply_state = init_apply_state(&engines, region)?;
        if let Err(e) = validate_states(region.get_id(), &engines, &mut raft_state, &apply_state) {
            return Err(box_err!("{} validate state fail: {:?}", tag, e));
        }
        let last_term = init_last_term(&engines, region, &raft_state, &apply_state)?;
        let applied_index_term = init_applied_index_term(&engines, region, &apply_state)?;

        /*let cache = if engines.raft.has_builtin_entry_cache() {
            None
        } else {
            Some(EntryCache::default())
        };*/

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
            applied_index_term,
            last_term,
            cache: Some(EntryCache::default()),
        })
    }

    pub fn is_initialized(&self) -> bool {
        util::is_region_initialized(self.region())
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
            util::conf_state_from_region(self.region()),
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
        let region_id = self.get_region_id();
        if let Some(ref cache) = self.cache {
            let cache_low = cache.first_index().unwrap_or(u64::MAX);
            if high <= cache_low {
                cache.miss.update(|m| m + 1);
                self.engines.raft.fetch_entries_to(
                    region_id,
                    low,
                    high,
                    Some(max_size as usize),
                    &mut ents,
                )?;
                return Ok(ents);
            }
            let begin_idx = if low < cache_low {
                cache.miss.update(|m| m + 1);
                let fetched_count = self.engines.raft.fetch_entries_to(
                    region_id,
                    low,
                    cache_low,
                    Some(max_size as usize),
                    &mut ents,
                )?;
                if fetched_count < (cache_low - low) as usize {
                    // Less entries are fetched than expected.
                    return Ok(ents);
                }
                cache_low
            } else {
                low
            };
            cache.hit.update(|h| h + 1);
            let fetched_size = ents.iter().fold(0, |acc, e| acc + e.compute_size());
            cache.fetch_entries_to(begin_idx, high, fetched_size as u64, max_size, &mut ents);
        } else {
            self.engines.raft.fetch_entries_to(
                region_id,
                low,
                high,
                Some(max_size as usize),
                &mut ents,
            )?;
        }
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
        let cache_low = self
            .cache
            .as_ref()
            .unwrap()
            .first_index()
            .unwrap_or(u64::MAX);
        if idx >= cache_low {
            Ok(self.cache.as_ref().unwrap().term(idx))
        } else {
            let mut entries = vec![];
            self.engines.raft.fetch_entries_to(
                self.get_region_id(),
                idx,
                idx + 1,
                None,
                &mut entries,
            )?;
            Ok(entries[0].get_term())
        }
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
    pub fn last_term(&self) -> u64 {
        self.last_term
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
    pub fn commit_index(&self) -> u64 {
        self.raft_state.get_hard_state().get_commit()
    }

    #[inline]
    pub fn set_commit_index(&mut self, commit: u64) {
        assert!(commit >= self.commit_index());
        self.raft_state.mut_hard_state().set_commit(commit);
    }

    #[inline]
    pub fn hard_state(&self) -> &HardState {
        self.raft_state.get_hard_state()
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

    pub fn raw_snapshot(&self) -> EK::Snapshot {
        self.engines.kv.snapshot()
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
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.stale.inc();
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
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.decode.inc();
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
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.epoch.inc();
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

        let task = GenSnapTask::new(self.region.get_id(), tx);
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
    // WARNING: If this function returns error, the caller must panic otherwise the entry cache may
    // be wrong and break correctness.
    pub fn append(
        &mut self,
        invoke_ctx: &mut InvokeContext,
        entries: Vec<Entry>,
        raft_wb: &mut ER::LogBatch,
    ) -> Result<u64> {
        let region_id = self.get_region_id();
        debug!(
            "append entries";
            "region_id" => region_id,
            "peer_id" => self.peer_id,
            "count" => entries.len(),
        );
        let prev_last_index = invoke_ctx.raft_state.get_last_index();
        if entries.is_empty() {
            return Ok(prev_last_index);
        }

        invoke_ctx.has_new_entries = true;

        let (last_index, last_term) = {
            let e = entries.last().unwrap();
            (e.get_index(), e.get_term())
        };

        // WARNING: This code is correct based on the assumption that
        // if this function returns error, the TiKV will panic soon,
        // otherwise, the entry cache may be wrong and break correctness.
        if let Some(ref mut cache) = self.cache {
            cache.append(&self.tag, &entries);
        }

        raft_wb.append(region_id, entries)?;
        // Delete any previously appended log entries which never committed.
        // TODO: Wrap it as an engine::Error.
        raft_wb.cut_logs(region_id, last_index + 1, prev_last_index);

        invoke_ctx.raft_state.set_last_index(last_index);
        invoke_ctx.last_term = last_term;

        Ok(last_index)
    }

    pub fn compact_to(&mut self, idx: u64) {
        if let Some(ref mut cache) = self.cache {
            cache.compact_to(idx);
        } else {
            let rid = self.get_region_id();
            self.engines.raft.gc_entry_cache(rid, idx);
        }
    }

    #[inline]
    pub fn is_cache_empty(&self) -> bool {
        self.cache.as_ref().map_or(true, |c| c.is_empty())
    }

    pub fn maybe_gc_cache(&mut self, replicated_idx: u64, apply_idx: u64) {
        /*if self.engines.raft.has_builtin_entry_cache() {
            let rid = self.get_region_id();
            self.engines.raft.gc_entry_cache(rid, apply_idx + 1);
            return;
        }*/

        let cache = self.cache.as_mut().unwrap();
        if replicated_idx == apply_idx {
            // The region is inactive, clear the cache immediately.
            cache.compact_to(apply_idx + 1);
            return;
        }
        let cache_first_idx = match cache.first_index() {
            None => return,
            Some(idx) => idx,
        };
        if cache_first_idx > replicated_idx + 1 {
            // Catching up log requires accessing fs already, let's optimize for
            // the common case.
            // Maybe gc to second least replicated_idx is better.
            cache.compact_to(apply_idx + 1);
        }
    }

    #[inline]
    pub fn flush_cache_metrics(&mut self) {
        if let Some(ref mut cache) = self.cache {
            cache.flush_mem_size_change();
            cache.flush_stats();
            return;
        }
        if let Some(stats) = self.engines.raft.flush_stats() {
            RAFT_ENTRIES_CACHES_GAUGE.set(stats.cache_size as i64);
            RAFT_ENTRY_FETCHES.hit.inc_by(stats.hit as i64);
            RAFT_ENTRY_FETCHES.miss.inc_by(stats.miss as i64);
        }
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot(
        &mut self,
        ctx: &mut InvokeContext,
        snap: &Snapshot,
        kv_wb: &mut EK::WriteBatch,
        raft_wb: &mut ER::LogBatch,
        destroy_regions: &[metapb::Region],
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
        // Write its source peers' `RegionLocalState` together with itself for atomicity
        for r in destroy_regions {
            write_peer_state(kv_wb, r, PeerState::Tombstone, None)?;
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

        ctx.snap_region = Some(region);
        Ok(())
    }

    /// Delete all meta belong to the region. Results are stored in `wb`.
    pub fn clear_meta(
        &mut self,
        kv_wb: &mut EK::WriteBatch,
        raft_wb: &mut ER::LogBatch,
    ) -> Result<()> {
        let region_id = self.get_region_id();
        clear_meta(
            &self.engines,
            kv_wb,
            raft_wb,
            region_id,
            self.raft_state.get_last_index(),
        )?;
        /*if !self.engines.raft.has_builtin_entry_cache() {
            self.cache = Some(EntryCache::default());
        }*/
        self.cache = Some(EntryCache::default());
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
    fn clear_extra_data(
        &self,
        old_region: &metapb::Region,
        new_region: &metapb::Region,
    ) -> Result<()> {
        let (old_start_key, old_end_key) = (enc_start_key(old_region), enc_end_key(old_region));
        let (new_start_key, new_end_key) = (enc_start_key(new_region), enc_end_key(new_region));
        if old_start_key < new_start_key {
            box_try!(self.region_sched.schedule(RegionTask::destroy(
                old_region.get_id(),
                old_start_key,
                new_start_key
            )));
        }
        if new_end_key < old_end_key {
            box_try!(self.region_sched.schedule(RegionTask::destroy(
                old_region.get_id(),
                new_end_key,
                old_end_key
            )));
        }
        Ok(())
    }

    /// Delete all extra split data from the `start_key` to `end_key`.
    pub fn clear_extra_split_data(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<()> {
        box_try!(self.region_sched.schedule(RegionTask::destroy(
            self.get_region_id(),
            start_key,
            end_key
        )));
        Ok(())
    }

    pub fn get_raft_engine(&self) -> ER {
        self.engines.raft.clone()
    }

    /// Check whether the storage has finished applying snapshot.
    #[inline]
    pub fn is_applying_snapshot(&self) -> bool {
        matches!(*self.snap_state.borrow(), SnapState::Applying(_))
    }

    #[inline]
    pub fn is_generating_snapshot(&self) -> bool {
        fail_point!("is_generating_snapshot", |_| { true });
        matches!(*self.snap_state.borrow(), SnapState::Generating(_))
    }

    /// Check if the storage is applying a snapshot.
    #[inline]
    pub fn check_applying_snap(&mut self) -> CheckApplyingSnapStatus {
        let mut res = CheckApplyingSnapStatus::Idle;
        let new_state = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                let s = status.load(Ordering::Relaxed);
                if s == JOB_STATUS_FINISHED {
                    res = CheckApplyingSnapStatus::Success;
                    SnapState::Relax
                } else if s == JOB_STATUS_CANCELLED {
                    SnapState::ApplyAborted
                } else if s == JOB_STATUS_FAILED {
                    // TODO: cleanup region and treat it as tombstone.
                    panic!("{} applying snapshot failed", self.tag,);
                } else {
                    return CheckApplyingSnapStatus::Applying;
                }
            }
            _ => return res,
        };
        *self.snap_state.borrow_mut() = new_state;
        res
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
        self.check_applying_snap() != CheckApplyingSnapStatus::Applying
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

        // Don't schedule the snapshot to region worker.
        fail_point!("skip_schedule_applying_snapshot", |_| {});

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
    /// WARNING: If this function returns error, the caller must panic(details in `append` function).
    pub fn handle_raft_ready<H: HandleRaftReadyContext<EK, ER>>(
        &mut self,
        ready_ctx: &mut H,
        ready: &mut Ready,
        destroy_regions: Vec<metapb::Region>,
        region_notifier: Arc<AtomicU64>,
        async_writer_id: usize,
        proposal_times: Vec<Instant>,
    ) -> Result<InvokeContext> {
        let region_id = self.get_region_id();
        let mut ctx = InvokeContext::new(self);
        let mut snapshot_index = 0;

        let (async_writer, io_lock_metrics) = ready_ctx.async_writer(async_writer_id);
        let wait_lock = UtilInstant::now();
        let mut locked_writer = async_writer.0.lock().unwrap();
        let hold_lock = UtilInstant::now();

        io_lock_metrics
            .wait_lock_sec
            .observe(duration_to_sec(wait_lock.elapsed()) as f64);

        let current = locked_writer.prepare_current_for_write();

        if !ready.snapshot().is_empty() {
            fail_point!("raft_before_apply_snap");
            self.apply_snapshot(
                &mut ctx,
                ready.snapshot(),
                &mut current.kv_wb,
                &mut current.raft_wb,
                &destroy_regions,
            )?;
            fail_point!("raft_after_apply_snap");
            ctx.destroyed_regions = destroy_regions;
            snapshot_index = last_index(&ctx.raft_state);
        };

        if !ready.entries().is_empty() {
            self.append(&mut ctx, ready.take_entries(), &mut current.raft_wb)?;
        }
        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if ctx.raft_state.get_last_index() > 0 {
            if let Some(hs) = ready.hs() {
                ctx.raft_state.set_hard_state(hs.clone());
            }
        }

        // Save raft state if it has changed or there is a snapshot.
        if ctx.raft_state != self.raft_state || snapshot_index > 0 {
            ctx.save_raft_state_to(&mut current.raft_wb)?;
        }

        // only when apply snapshot
        if snapshot_index > 0 {
            // in case of restart happen when we just write region state to Applying,
            // but not write raft_local_state to raft rocksdb in time.
            // we write raft state to default rocksdb, with last index set to snap index,
            // in case of recv raft log after snapshot.
            ctx.save_snapshot_raft_state_to(snapshot_index, &mut current.kv_wb)?;
            ctx.save_apply_state_to(&mut current.kv_wb)?;
        }

        if ready.must_sync() {
            current.update_ready(region_id, ready.number(), region_notifier);
        }

        for ts in &proposal_times {
            STORE_TO_WRITE_QUEUE_DURATION_HISTOGRAM.observe(duration_to_sec(ts.elapsed()));
            current.proposal_times.push(*ts);
        }

        if locked_writer.should_notify() {
            async_writer.1.notify_one();
        }
        io_lock_metrics
            .hold_lock_sec
            .observe(duration_to_sec(hold_lock.elapsed()) as f64);

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
            if let Err(e) = self.clear_extra_data(self.region(), &snap_region) {
                // No need panic here, when applying snapshot, the deletion will be tried
                // again. But if the region range changes, like [a, c) -> [a, b) and [b, c),
                // [b, c) will be kept in rocksdb until a covered snapshot is applied or
                // store is restarted.
                error!(?e;
                    "failed to cleanup data, may leave some dirty data";
                    "region_id" => self.get_region_id(),
                    "peer_id" => self.peer_id,
                );
            }
        }

        // Note that the correctness depends on the fact that these source regions MUST NOT
        // serve read request otherwise a corrupt data may be returned.
        // For now, it is ensured by
        // 1. After `PrepareMerge` log is committed, the source region leader's lease will be
        //    suspected immediately which makes the local reader not serve read request.
        // 2. No read request can be responsed in peer fsm during merging.
        // These conditions are used to prevent reading **stale** data in the past.
        // At present, they are also used to prevent reading **corrupt** data.
        for r in &ctx.destroyed_regions {
            if let Err(e) = self.clear_extra_data(r, &snap_region) {
                error!(?e;
                    "failed to cleanup data, may leave some dirty data";
                    "region_id" => r.get_id(),
                );
            }
        }

        let prev_region = self.region().clone();
        self.set_region(snap_region);

        Some(ApplySnapResult {
            prev_region,
            region: self.region().clone(),
            destroyed_regions: ctx.destroyed_regions,
        })
    }
}

#[allow(dead_code)]
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

/// Delete all meta belong to the region. Results are stored in `wb`.
pub fn clear_meta<EK, ER>(
    engines: &Engines<EK, ER>,
    kv_wb: &mut EK::WriteBatch,
    raft_wb: &mut ER::LogBatch,
    region_id: u64,
    last_index: u64,
) -> Result<()>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    let t = Instant::now();
    box_try!(kv_wb.delete_cf(CF_RAFT, &keys::region_state_key(region_id)));
    box_try!(kv_wb.delete_cf(CF_RAFT, &keys::apply_state_key(region_id)));
    box_try!(engines.raft.clean(region_id, last_index, raft_wb));

    info!(
        "finish clear peer meta";
        "region_id" => region_id,
        "meta_key" => 1,
        "apply_key" => 1,
        "raft_key" => 1,
        "takes" => ?t.elapsed(),
    );
    Ok(())
}

pub fn do_snapshot<E>(
    mgr: SnapManager,
    engine: &E,
    kv_snap: E::Snapshot,
    region_id: u64,
    last_applied_index_term: u64,
    last_applied_state: RaftApplyState,
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
    assert_eq!(apply_state, last_applied_state);

    let key = SnapKey::new(
        region_id,
        last_applied_index_term,
        apply_state.get_applied_index(),
    );

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

    let conf_state = util::conf_state_from_region(state.get_region());
    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut s = mgr.get_snapshot_for_building(&key)?;
    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::default();
    snap_data.set_region(state.get_region().clone());
    let mut stat = SnapshotStatistics::new();
    s.build(
        engine,
        &kv_snap,
        state.get_region(),
        &mut snap_data,
        &mut stat,
    )?;
    let v = snap_data.write_to_bytes()?;
    snapshot.set_data(v);

    SNAPSHOT_KV_COUNT_HISTOGRAM.observe(stat.kv_count as f64);
    SNAPSHOT_SIZE_HISTOGRAM.observe(stat.size as f64);

    Ok(snapshot)
}

// When we bootstrap the region we must call this to initialize region local state first.
pub fn write_initial_raft_state<W: RaftLogBatch>(raft_wb: &mut W, region_id: u64) -> Result<()> {
    let mut raft_state = RaftLocalState::default();
    raft_state.last_index = RAFT_INIT_LOG_INDEX;
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);
    raft_wb.put_raft_state(region_id, &raft_state)?;
    Ok(())
}

// When we bootstrap the region or handling split new region, we must
// call this to initialize region apply state first.
pub fn write_initial_apply_state<T: Mutable>(kv_wb: &mut T, region_id: u64) -> Result<()> {
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

pub fn write_peer_state<T: Mutable>(
    kv_wb: &mut T,
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
    use engine_test::kv::{KvTestEngine, KvTestSnapshot, KvTestWriteBatch};
    use engine_test::raft::{RaftTestEngine, RaftTestWriteBatch};
    use engine_traits::Engines;
    use engine_traits::{Iterable, SyncMutable, WriteBatchExt};
    use engine_traits::{ALL_CFS, CF_DEFAULT};
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
    use tikv_util::worker::{LazyWorker, Scheduler, Worker};

    use super::*;

    fn new_storage(
        sched: Scheduler<RegionTask<KvTestSnapshot>>,
        path: &TempDir,
    ) -> PeerStorage<KvTestEngine, RaftTestEngine> {
        let kv_db = engine_test::kv::new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None)
            .unwrap();
        let raft_path = path.path().join(Path::new("raft"));
        let raft_db =
            engine_test::raft::new_engine(raft_path.to_str().unwrap(), None, CF_DEFAULT, None)
                .unwrap();
        let engines = Engines::new(kv_db, raft_db);
        bootstrap_store(&engines, 1, 1).unwrap();

        let region = initial_region(1, 1, 1);
        prepare_bootstrap_cluster(&engines, &region).unwrap();
        PeerStorage::new(engines, &region, sched, 0, "".to_owned()).unwrap()
    }

    struct ReadyContext {
        kv_wb: KvTestWriteBatch,
        raft_wb: RaftTestWriteBatch,
        sync_log: bool,
    }

    impl ReadyContext {
        fn new(s: &PeerStorage<KvTestEngine, RaftTestEngine>) -> ReadyContext {
            ReadyContext {
                kv_wb: s.engines.kv.write_batch(),
                raft_wb: s.engines.raft.write_batch(),
                sync_log: false,
            }
        }
    }

    impl HandleRaftReadyContext<KvTestWriteBatch, RaftTestWriteBatch> for ReadyContext {
        //fn wb_mut(&mut self) -> (&mut KvTestWriteBatch, &mut RaftTestWriteBatch) {
        //    (&mut self.kv_wb, &mut self.raft_wb)
        //}
        fn kv_wb_mut(&mut self) -> &mut KvTestWriteBatch {
            &mut self.kv_wb
        }
        fn raft_wb_pool(&mut self) -> &mut RaftTestWriteBatch {
            &mut self.raft_wbs
        }
        fn sync_log(&self) -> bool {
            self.sync_log
        }
        fn set_sync_log(&mut self, sync: bool) {
            self.sync_log = sync;
        }
    }

    fn new_storage_from_ents(
        sched: Scheduler<RegionTask<KvTestSnapshot>>,
        path: &TempDir,
        ents: &[Entry],
    ) -> PeerStorage<KvTestEngine, RaftTestEngine> {
        let mut store = new_storage(sched, path);
        let mut kv_wb = store.engines.kv.write_batch();
        let mut ctx = InvokeContext::new(&store);
        let mut ready_ctx = ReadyContext::new(&store);
        store
            .append(&mut ctx, ents[1..].to_vec(), &mut ready_ctx)
            .unwrap();
        ctx.apply_state
            .mut_truncated_state()
            .set_index(ents[0].get_index());
        ctx.apply_state
            .mut_truncated_state()
            .set_term(ents[0].get_term());
        ctx.apply_state
            .set_applied_index(ents.last().unwrap().get_index());
        ctx.save_apply_state_to(&mut kv_wb).unwrap();
        store.engines.raft.write(&ready_ctx.raft_wb).unwrap();
        store.engines.kv.write(&kv_wb).unwrap();
        store.raft_state = ctx.raft_state;
        store.apply_state = ctx.apply_state;
        store
    }

    fn append_ents(store: &mut PeerStorage<KvTestEngine, RaftTestEngine>, ents: &[Entry]) {
        let mut ctx = InvokeContext::new(store);
        let mut ready_ctx = ReadyContext::new(store);
        store
            .append(&mut ctx, ents.to_vec(), &mut ready_ctx)
            .unwrap();
        ctx.save_raft_state_to(&mut ready_ctx.raft_wb).unwrap();
        store.engines.raft.write(&ready_ctx.raft_wb).unwrap();
        store.raft_state = ctx.raft_state;
    }

    fn validate_cache(store: &PeerStorage<KvTestEngine, RaftTestEngine>, exp_ents: &[Entry]) {
        assert_eq!(store.cache.as_ref().unwrap().cache, exp_ents);
        for e in exp_ents {
            let key = keys::raft_log_key(store.get_region_id(), e.get_index());
            let bytes = store.engines.raft.get_value(&key).unwrap().unwrap();
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
            let worker = Worker::new("snap-manager").lazy_build("snap-manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let t = store.term(idx);
            if wterm != t {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    fn get_meta_key_count(store: &PeerStorage<KvTestEngine, RaftTestEngine>) -> usize {
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
        let worker = Worker::new("snap-manager").lazy_build("snap-manager");
        let sched = worker.scheduler();
        let mut store = new_storage_from_ents(sched, &td, &[new_entry(3, 3), new_entry(4, 4)]);
        append_ents(&mut store, &[new_entry(5, 5), new_entry(6, 6)]);

        assert_eq!(6, get_meta_key_count(&store));

        let mut kv_wb = store.engines.kv.write_batch();
        let mut raft_wb = store.engines.raft.write_batch();
        store.clear_meta(&mut kv_wb, &mut raft_wb).unwrap();
        store.engines.kv.write(&kv_wb).unwrap();
        store.engines.raft.write(&raft_wb).unwrap();

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
            let worker = Worker::new("snap-manager").lazy_build("snap-manager");
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
            let worker = Worker::new("snap-manager").lazy_build("snap-manager");
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
                let mut kv_wb = store.engines.kv.write_batch();
                ctx.save_apply_state_to(&mut kv_wb).unwrap();
                store.engines.kv.write(&kv_wb).unwrap();
            }
        }
    }

    fn generate_and_schedule_snapshot(
        gen_task: GenSnapTask,
        engines: &Engines<KvTestEngine, RaftTestEngine>,
        sched: &Scheduler<RegionTask<KvTestSnapshot>>,
    ) -> Result<()> {
        let apply_state: RaftApplyState = engines
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(gen_task.region_id))
            .unwrap()
            .unwrap();
        let idx = apply_state.get_applied_index();
        let entry = engines
            .raft
            .get_msg::<Entry>(&keys::raft_log_key(gen_task.region_id, idx))
            .unwrap()
            .unwrap();
        gen_task.generate_and_schedule_snapshot::<KvTestEngine>(
            engines.kv.clone().snapshot(),
            entry.get_term(),
            apply_state,
            sched,
        )
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::default();
        cs.set_voters(vec![1, 2, 3]);

        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let mut worker = Worker::new("region-worker").lazy_build("la");
        let sched = worker.scheduler();
        let mut s = new_storage_from_ents(sched.clone(), &td, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let runner = RegionRunner::new(
            s.engines.kv.clone(),
            mgr,
            0,
            true,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
        );
        worker.start_with_timer(runner);
        let snap = s.snapshot(0);
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap();
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
        let mut kv_wb = s.engines.kv.write_batch();
        let mut ready_ctx = ReadyContext::new(&s);
        s.append(
            &mut ctx,
            [new_entry(6, 5), new_entry(7, 5)].to_vec(),
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
        s.engines.kv.write(&kv_wb).unwrap();
        s.engines.raft.write(&ready_ctx.raft_wb).unwrap();
        s.apply_state = ctx.apply_state;
        s.raft_state = ctx.raft_state;
        ctx = InvokeContext::new(&s);
        let term = s.term(7).unwrap();
        compact_raft_log(&s.tag, &mut ctx.apply_state, 7, term).unwrap();
        kv_wb = s.engines.kv.write_batch();
        ctx.save_apply_state_to(&mut kv_wb).unwrap();
        s.engines.kv.write(&kv_wb).unwrap();
        s.apply_state = ctx.apply_state;

        let (tx, rx) = channel();
        tx.send(snap).unwrap();
        s.set_snap_state(SnapState::Generating(rx));
        *s.snap_tried_cnt.borrow_mut() = 1;
        // stale snapshot should be abandoned, snapshot index < truncated index.
        assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap();
        match *s.snap_state.borrow() {
            SnapState::Generating(ref rx) => {
                rx.recv_timeout(Duration::from_secs(3)).unwrap();
                worker.stop();
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
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap_err();
        assert_eq!(*s.snap_tried_cnt.borrow(), 2);

        for cnt in 2..super::MAX_SNAP_TRY_CNT {
            // Scheduled job failed should trigger .
            assert_eq!(s.snapshot(0).unwrap_err(), unavailable);
            let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
            generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap_err();
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
            let worker = LazyWorker::new("snap-manager");
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
        let worker = LazyWorker::new("snap-manager");
        let sched = worker.scheduler();
        let mut store = new_storage_from_ents(sched, &td, &ents);
        store.cache.as_mut().unwrap().cache.clear();
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
        let worker = LazyWorker::new("snap-manager");
        let sched = worker.scheduler();
        let mut store = new_storage_from_ents(sched, &td, &ents);
        store.cache.as_mut().unwrap().cache.clear();

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

        let MAX_CACHE_CAPACITY: usize = 1024 - 1;
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
        assert!(store.cache.as_ref().unwrap().cache.capacity() >= cap as usize);
        store.compact_to(cap * 2);
        exp_res = (cap * 2..cap * 2 + 7).map(|i| new_entry(i, 8)).collect();
        validate_cache(&store, &exp_res);
        assert!(store.cache.as_ref().unwrap().cache.capacity() < cap as usize);

        // append shrink
        entries = (0..=cap).map(|i| new_entry(i, 8)).collect();
        append_ents(&mut store, &entries);
        assert!(store.cache.as_ref().unwrap().cache.capacity() >= cap as usize);
        append_ents(&mut store, &[new_entry(6, 8)]);
        exp_res = (1..7).map(|i| new_entry(i, 8)).collect();
        validate_cache(&store, &exp_res);
        assert!(store.cache.as_ref().unwrap().cache.capacity() < cap as usize);

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
        cs.set_voters(vec![1, 2, 3]);

        let td1 = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let snap_dir = Builder::new().prefix("snap").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let mut worker = LazyWorker::new("snap-manager");
        let sched = worker.scheduler();
        let s1 = new_storage_from_ents(sched.clone(), &td1, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let runner = RegionRunner::new(
            s1.engines.kv.clone(),
            mgr,
            0,
            true,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
        );
        worker.start(runner);
        assert!(s1.snapshot(0).is_err());
        let gen_task = s1.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s1.engines, &sched).unwrap();

        let snap1 = match *s1.snap_state.borrow() {
            SnapState::Generating(ref rx) => rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(s1.truncated_index(), 3);
        assert_eq!(s1.truncated_term(), 3);
        worker.stop();

        let td2 = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let mut s2 = new_storage(sched.clone(), &td2);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        let mut ctx = InvokeContext::new(&s2);
        assert_ne!(ctx.last_term, snap1.get_metadata().get_term());
        let mut kv_wb = s2.engines.kv.write_batch();
        let mut raft_wb = s2.engines.raft.write_batch();
        s2.apply_snapshot(&mut ctx, &snap1, &mut kv_wb, &mut raft_wb, &[])
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
        let mut kv_wb = s3.engines.kv.write_batch();
        let mut raft_wb = s3.engines.raft.write_batch();
        s3.apply_snapshot(&mut ctx, &snap1, &mut kv_wb, &mut raft_wb, &[])
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
        let worker = LazyWorker::new("snap-manager");
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
        let worker = LazyWorker::new("snap-manager");
        let sched = worker.scheduler();
        let mut s = new_storage(sched, &td);

        // PENDING can be finished.
        let mut snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING)));
        s.snap_state = RefCell::new(snap_state);
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Applying);
        assert_eq!(
            *s.snap_state.borrow(),
            SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_PENDING)))
        );

        // RUNNING can't be finished.
        snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)));
        s.snap_state = RefCell::new(snap_state);
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Applying);
        assert_eq!(
            *s.snap_state.borrow(),
            SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_RUNNING)))
        );

        snap_state = SnapState::Applying(Arc::new(AtomicUsize::new(JOB_STATUS_CANCELLED)));
        s.snap_state = RefCell::new(snap_state);
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Idle);
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);
        // ApplyAborted is not applying snapshot.
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Idle);
        assert_eq!(*s.snap_state.borrow(), SnapState::ApplyAborted);

        s.snap_state = RefCell::new(SnapState::Applying(Arc::new(AtomicUsize::new(
            JOB_STATUS_FINISHED,
        ))));
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Success);
        assert_eq!(*s.snap_state.borrow(), SnapState::Relax);
        // Relax is not applying snapshot.
        assert_eq!(s.check_applying_snap(), CheckApplyingSnapStatus::Idle);
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

    #[test]
    fn test_validate_states() {
        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let worker = LazyWorker::new("snap-manager");
        let sched = worker.scheduler();
        let kv_db =
            engine_test::kv::new_engine(td.path().to_str().unwrap(), None, ALL_CFS, None).unwrap();
        let raft_path = td.path().join(Path::new("raft"));
        let raft_db =
            engine_test::raft::new_engine(raft_path.to_str().unwrap(), None, CF_DEFAULT, None)
                .unwrap();
        let engines = Engines::new(kv_db, raft_db);
        bootstrap_store(&engines, 1, 1).unwrap();

        let region = initial_region(1, 1, 1);
        prepare_bootstrap_cluster(&engines, &region).unwrap();
        let build_storage = || -> Result<PeerStorage<KvTestEngine, RaftTestEngine>> {
            PeerStorage::new(engines.clone(), &region, sched.clone(), 0, "".to_owned())
        };
        let mut s = build_storage().unwrap();
        let mut raft_state = RaftLocalState::default();
        raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
        raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
        raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *raft_state.get_hard_state());

        // last_index < commit_index is invalid.
        let raft_state_key = keys::raft_state_key(1);
        raft_state.set_last_index(11);
        let log_key = keys::raft_log_key(1, 11);
        engines
            .raft
            .put_msg(&log_key, &new_entry(11, RAFT_INIT_LOG_TERM))
            .unwrap();
        raft_state.mut_hard_state().set_commit(12);
        engines.raft.put_msg(&raft_state_key, &raft_state).unwrap();
        assert!(build_storage().is_err());

        let log_key = keys::raft_log_key(1, 20);
        engines
            .raft
            .put_msg(&log_key, &new_entry(20, RAFT_INIT_LOG_TERM))
            .unwrap();
        raft_state.set_last_index(20);
        engines.raft.put_msg(&raft_state_key, &raft_state).unwrap();
        s = build_storage().unwrap();
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *raft_state.get_hard_state());

        // Missing last log is invalid.
        engines.raft.delete(&log_key).unwrap();
        assert!(build_storage().is_err());
        engines
            .raft
            .put_msg(&log_key, &new_entry(20, RAFT_INIT_LOG_TERM))
            .unwrap();

        // applied_index > commit_index is invalid.
        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(13);
        apply_state.mut_truncated_state().set_index(13);
        apply_state
            .mut_truncated_state()
            .set_term(RAFT_INIT_LOG_TERM);
        let apply_state_key = keys::apply_state_key(1);
        engines
            .kv
            .put_msg_cf(CF_RAFT, &apply_state_key, &apply_state)
            .unwrap();
        assert!(build_storage().is_err());

        // It should not recover if corresponding log doesn't exist.
        apply_state.set_commit_index(14);
        apply_state.set_commit_term(RAFT_INIT_LOG_TERM);
        engines
            .kv
            .put_msg_cf(CF_RAFT, &apply_state_key, &apply_state)
            .unwrap();
        assert!(build_storage().is_err());

        let log_key = keys::raft_log_key(1, 14);
        engines
            .raft
            .put_msg(&log_key, &new_entry(14, RAFT_INIT_LOG_TERM))
            .unwrap();
        raft_state.mut_hard_state().set_commit(14);
        s = build_storage().unwrap();
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *raft_state.get_hard_state());

        // log term miss match is invalid.
        engines
            .raft
            .put_msg(&log_key, &new_entry(14, RAFT_INIT_LOG_TERM - 1))
            .unwrap();
        assert!(build_storage().is_err());

        // hard state term miss match is invalid.
        engines
            .raft
            .put_msg(&log_key, &new_entry(14, RAFT_INIT_LOG_TERM))
            .unwrap();
        raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM - 1);
        engines.raft.put_msg(&raft_state_key, &raft_state).unwrap();
        assert!(build_storage().is_err());

        // last index < recorded_commit_index is invalid.
        raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
        raft_state.set_last_index(13);
        let log_key = keys::raft_log_key(1, 13);
        engines
            .raft
            .put_msg(&log_key, &new_entry(13, RAFT_INIT_LOG_TERM))
            .unwrap();
        engines.raft.put_msg(&raft_state_key, &raft_state).unwrap();
        assert!(build_storage().is_err());
    }
}
