// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::{Cell, RefCell},
    cmp,
    collections::VecDeque,
    error, mem,
    ops::Range,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, TryRecvError},
        Arc, Mutex,
    },
    u64,
};

use collections::HashMap;
use engine_traits::{
    Engines, KvEngine, Mutable, Peekable, RaftEngine, RaftLogBatch, CF_RAFT, RAFT_LOG_MULTI_GET_CNT,
};
use fail::fail_point;
use into_other::into_other;
use keys::{self, enc_end_key, enc_start_key};
use kvproto::{
    metapb::{self, Region},
    raft_serverpb::{
        MergeState, PeerState, RaftApplyState, RaftLocalState, RaftSnapshotData, RegionLocalState,
    },
};
use protobuf::Message;
use raft::{
    self,
    eraftpb::{self, ConfState, Entry, HardState, Snapshot},
    util::limit_size,
    Error as RaftError, GetEntriesContext, RaftState, Ready, Storage, StorageError,
};
use tikv_alloc::trace::TraceEvent;
use tikv_util::{
    box_err, box_try, debug, defer, error, info, time::Instant, warn, worker::Scheduler,
};

use super::{metrics::*, worker::RegionTask, SnapEntry, SnapKey, SnapManager, SnapshotStatistics};
use crate::{
    bytes_capacity,
    store::{
        async_io::write::WriteTask, fsm::GenSnapTask, memory::*, peer::PersistSnapshotResult, util,
        worker::RaftlogFetchTask,
    },
    Error, Result,
};

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;
const MAX_ASYNC_FETCH_TRY_CNT: usize = 3;

pub const MAX_INIT_ENTRY_COUNT: usize = 1024;

/// The initial region epoch version.
pub const INIT_EPOCH_VER: u64 = 1;
/// The initial region epoch conf_version.
pub const INIT_EPOCH_CONF_VER: u64 = 1;

const SHRINK_CACHE_CAPACITY: usize = 64;

pub const JOB_STATUS_PENDING: usize = 0;
pub const JOB_STATUS_RUNNING: usize = 1;
pub const JOB_STATUS_CANCELLING: usize = 2;
pub const JOB_STATUS_CANCELLED: usize = 3;
pub const JOB_STATUS_FINISHED: usize = 4;
pub const JOB_STATUS_FAILED: usize = 5;

const ENTRY_MEM_SIZE: usize = mem::size_of::<Entry>();

/// Possible status returned by `check_applying_snap`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckApplyingSnapStatus {
    /// A snapshot is just applied.
    Success,
    /// A snapshot is being applied.
    Applying,
    /// No snapshot is being applied at all or the snapshot is canceled
    Idle,
}

#[derive(Debug)]
pub enum SnapState {
    Relax,
    Generating {
        canceled: Arc<AtomicBool>,
        index: Arc<AtomicU64>,
        receiver: Receiver<Snapshot>,
    },
    Applying(Arc<AtomicUsize>),
    ApplyAborted,
}

impl PartialEq for SnapState {
    fn eq(&self, other: &SnapState) -> bool {
        match (self, other) {
            (&SnapState::Relax, &SnapState::Relax)
            | (&SnapState::ApplyAborted, &SnapState::ApplyAborted)
            | (&SnapState::Generating { .. }, &SnapState::Generating { .. }) => true,
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

struct EntryCache {
    // The last index of persisted entry.
    // It should be equal to `RaftLog::persisted`.
    persisted: u64,
    cache: VecDeque<Entry>,
    trace: VecDeque<CachedEntries>,
    hit: Cell<u64>,
    miss: Cell<u64>,
    #[cfg(test)]
    size_change_cb: Option<Box<dyn Fn(i64) + Send + 'static>>,
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
        if !entries.is_empty() {
            let mut mem_size_change = 0;
            let old_capacity = self.cache.capacity();
            mem_size_change += self.append_impl(tag, entries);
            let new_capacity = self.cache.capacity();
            mem_size_change += Self::get_cache_vec_mem_size_change(new_capacity, old_capacity);
            mem_size_change += self.shrink_if_necessary();
            self.flush_mem_size_change(mem_size_change);
        }
    }

    fn append_impl(&mut self, tag: &str, entries: &[Entry]) -> i64 {
        let mut mem_size_change = 0;

        if let Some(cache_last_index) = self.cache.back().map(|e| e.get_index()) {
            let first_index = entries[0].get_index();
            if cache_last_index >= first_index {
                let cache_len = self.cache.len();
                let truncate_to = cache_len
                    .checked_sub((cache_last_index - first_index + 1) as usize)
                    .unwrap_or_default();
                let trunc_to_idx = self.cache[truncate_to].index;
                for e in self.cache.drain(truncate_to..) {
                    mem_size_change -=
                        (bytes_capacity(&e.data) + bytes_capacity(&e.context)) as i64;
                }
                if let Some(cached) = self.trace.back() {
                    // Only committed entries can be traced, and only uncommitted entries
                    // can be truncated. So there won't be any overlaps.
                    let cached_last = cached.range.end - 1;
                    assert!(cached_last < trunc_to_idx);
                }
            } else if cache_last_index + 1 < first_index {
                panic!(
                    "{} unexpected hole: {} < {}",
                    tag, cache_last_index, first_index
                );
            }
        }

        for e in entries {
            self.cache.push_back(e.to_owned());
            mem_size_change += (bytes_capacity(&e.data) + bytes_capacity(&e.context)) as i64;
        }
        // In the past, the entry cache will be truncated if its size exceeds a certain number.
        // However, after introducing async write io, the entry must stay in cache if it's not
        // persisted to raft db because the raft-rs may need to read entries.(e.g. leader sends
        // MsgAppend to followers)

        mem_size_change
    }

    pub fn entry(&self, idx: u64) -> Option<&Entry> {
        let cache_low = self.cache.front()?.get_index();
        if idx >= cache_low {
            Some(&self.cache[(idx - cache_low) as usize])
        } else {
            None
        }
    }

    /// Compact all entries whose indexes are less than `idx`.
    pub fn compact_to(&mut self, mut idx: u64) -> u64 {
        if idx > self.persisted + 1 {
            // Only the persisted entries can be compacted
            idx = self.persisted + 1;
        }

        let mut mem_size_change = 0;

        // Clean cached entries which have been already sent to apply threads. For example,
        // if entries [1, 10), [10, 20), [20, 30) are sent to apply threads and `compact_to(15)`
        // is called, only [20, 30) will still be kept in cache.
        let old_trace_cap = self.trace.capacity();
        while let Some(cached_entries) = self.trace.pop_front() {
            if cached_entries.range.start >= idx {
                self.trace.push_front(cached_entries);
                let trace_len = self.trace.len();
                let trace_cap = self.trace.capacity();
                if trace_len < SHRINK_CACHE_CAPACITY && trace_cap > SHRINK_CACHE_CAPACITY {
                    self.trace.shrink_to(SHRINK_CACHE_CAPACITY);
                }
                break;
            }
            let (_, dangle_size) = cached_entries.take_entries();
            mem_size_change -= dangle_size as i64;
            idx = cmp::max(cached_entries.range.end, idx);
        }
        let new_trace_cap = self.trace.capacity();
        mem_size_change += Self::get_trace_vec_mem_size_change(new_trace_cap, old_trace_cap);

        let cache_first_idx = self.first_index().unwrap_or(u64::MAX);
        if cache_first_idx >= idx {
            self.flush_mem_size_change(mem_size_change);
            assert!(mem_size_change <= 0);
            return -mem_size_change as u64;
        }

        let cache_last_idx = self.cache.back().unwrap().get_index();
        // Use `cache_last_idx + 1` to make sure cache can be cleared completely if necessary.
        let compact_to = (cmp::min(cache_last_idx + 1, idx) - cache_first_idx) as usize;
        for e in self.cache.drain(..compact_to) {
            mem_size_change -= (bytes_capacity(&e.data) + bytes_capacity(&e.context)) as i64
        }

        mem_size_change += self.shrink_if_necessary();
        self.flush_mem_size_change(mem_size_change);
        assert!(mem_size_change <= 0);
        -mem_size_change as u64
    }

    fn get_total_mem_size(&self) -> i64 {
        let data_size: i64 = self
            .cache
            .iter()
            .map(|e| (bytes_capacity(&e.data) + bytes_capacity(&e.context)) as i64)
            .sum();
        let cache_vec_size = Self::get_cache_vec_mem_size_change(self.cache.capacity(), 0);
        let trace_vec_size = Self::get_trace_vec_mem_size_change(self.trace.capacity(), 0);
        data_size + cache_vec_size + trace_vec_size
    }

    fn get_cache_vec_mem_size_change(new_capacity: usize, old_capacity: usize) -> i64 {
        ENTRY_MEM_SIZE as i64 * (new_capacity as i64 - old_capacity as i64)
    }

    fn get_trace_vec_mem_size_change(new_capacity: usize, old_capacity: usize) -> i64 {
        mem::size_of::<CachedEntries>() as i64 * (new_capacity as i64 - old_capacity as i64)
    }

    fn flush_mem_size_change(&self, mem_size_change: i64) {
        #[cfg(test)]
        if let Some(size_change_cb) = self.size_change_cb.as_ref() {
            size_change_cb(mem_size_change);
        }
        let event = if mem_size_change > 0 {
            TraceEvent::Add(mem_size_change as usize)
        } else {
            TraceEvent::Sub(-mem_size_change as usize)
        };
        MEMTRACE_ENTRY_CACHE.trace(event);
        RAFT_ENTRIES_CACHES_GAUGE.add(mem_size_change);
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

    fn trace_cached_entries(&mut self, entries: CachedEntries) {
        let dangle_size = {
            let mut guard = entries.entries.lock().unwrap();

            let last_idx = guard.0.last().map(|e| e.index).unwrap();
            let cache_front = match self.cache.front().map(|e| e.index) {
                Some(i) => i,
                None => u64::MAX,
            };

            let dangle_range = if last_idx < cache_front {
                // All entries are not in entry cache.
                0..guard.0.len()
            } else if let Ok(i) = guard.0.binary_search_by(|e| e.index.cmp(&cache_front)) {
                // Some entries are in entry cache.
                0..i
            } else {
                // All entries are in entry cache.
                0..0
            };

            let mut size = 0;
            for e in &guard.0[dangle_range] {
                size += bytes_capacity(&e.data) + bytes_capacity(&e.context);
            }
            guard.1 = size;
            size
        };

        let old_capacity = self.trace.capacity();
        self.trace.push_back(entries);
        let new_capacity = self.trace.capacity();
        let diff = Self::get_trace_vec_mem_size_change(new_capacity, old_capacity);

        self.flush_mem_size_change(diff + dangle_size as i64);
    }

    fn shrink_if_necessary(&mut self) -> i64 {
        if self.cache.len() < SHRINK_CACHE_CAPACITY && self.cache.capacity() > SHRINK_CACHE_CAPACITY
        {
            let old_capacity = self.cache.capacity();
            self.cache.shrink_to_fit();
            let new_capacity = self.cache.capacity();
            return Self::get_cache_vec_mem_size_change(new_capacity, old_capacity);
        }
        0
    }

    fn update_persisted(&mut self, persisted: u64) {
        self.persisted = persisted;
    }
}

impl Default for EntryCache {
    fn default() -> Self {
        let entry_cache = EntryCache {
            persisted: 0,
            cache: Default::default(),
            trace: Default::default(),
            hit: Cell::new(0),
            miss: Cell::new(0),
            #[cfg(test)]
            size_change_cb: None,
        };
        entry_cache.flush_mem_size_change(entry_cache.get_total_mem_size());
        entry_cache
    }
}

impl Drop for EntryCache {
    fn drop(&mut self) {
        let mem_size_change = self.get_total_mem_size();
        self.flush_mem_size_change(-mem_size_change);
        self.flush_stats();
    }
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

#[derive(PartialEq, Debug)]
pub enum HandleReadyResult {
    SendIOTask,
    Snapshot {
        msgs: Vec<eraftpb::Message>,
        snap_region: metapb::Region,
        /// The regions whose range are overlapped with this region
        destroy_regions: Vec<Region>,
        /// The first index before applying the snapshot.
        last_first_index: u64,
    },
    NoIOTask,
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
    region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    snap_tried_cnt: RefCell<usize>,

    cache: EntryCache,

    raftlog_fetch_scheduler: Scheduler<RaftlogFetchTask>,
    raftlog_fetch_stats: AsyncFetchStats,
    async_fetch_results: RefCell<HashMap<u64, RaftlogFetchState>>,

    pub tag: String,
}

#[derive(Debug, PartialEq)]
pub enum RaftlogFetchState {
    Fetching,
    Fetched(Box<RaftlogFetchResult>),
}

#[derive(Debug, PartialEq)]
pub struct RaftlogFetchResult {
    pub ents: raft::Result<Vec<Entry>>,
    // because entries may be empty, so store the original low index that the task issued
    pub low: u64,
    // the original max size that the task issued
    pub max_size: u64,
    // if the ents hit max_size
    pub hit_size_limit: bool,
    // the times that async fetch have already tried
    pub tried_cnt: usize,
    // the term when the task issued
    pub term: u64,
}

#[derive(Default)]
struct AsyncFetchStats {
    async_fetch: Cell<u64>,
    sync_fetch: Cell<u64>,
    fallback_fetch: Cell<u64>,
    fetch_invalid: Cell<u64>,
    fetch_unused: Cell<u64>,
}

impl AsyncFetchStats {
    fn flush_stats(&mut self) {
        RAFT_ENTRY_FETCHES
            .async_fetch
            .inc_by(self.async_fetch.replace(0));
        RAFT_ENTRY_FETCHES
            .sync_fetch
            .inc_by(self.sync_fetch.replace(0));
        RAFT_ENTRY_FETCHES
            .fallback_fetch
            .inc_by(self.fallback_fetch.replace(0));
        RAFT_ENTRY_FETCHES
            .fetch_invalid
            .inc_by(self.fetch_invalid.replace(0));
        RAFT_ENTRY_FETCHES
            .fetch_unused
            .inc_by(self.fetch_unused.replace(0));
    }
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
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let max_size = max_size.into();
        self.entries(low, high, max_size.unwrap_or(u64::MAX), context)
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

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        self.snapshot(request_index, to)
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
        region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
        raftlog_fetch_scheduler: Scheduler<RaftlogFetchTask>,
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

        Ok(PeerStorage {
            engines,
            peer_id,
            region: region.clone(),
            raft_state,
            apply_state,
            snap_state: RefCell::new(SnapState::Relax),
            gen_snap_task: RefCell::new(None),
            region_scheduler,
            raftlog_fetch_scheduler,
            snap_tried_cnt: RefCell::new(0),
            tag,
            applied_index_term,
            last_term,
            cache: EntryCache::default(),
            async_fetch_results: RefCell::new(HashMap::default()),
            raftlog_fetch_stats: AsyncFetchStats::default(),
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

    pub fn clean_async_fetch_res(&mut self, low: u64) {
        self.async_fetch_results.borrow_mut().remove(&low);
    }

    // Update the async fetch result.
    // None indicates cleanning the fetched result.
    pub fn update_async_fetch_res(&mut self, low: u64, res: Option<Box<RaftlogFetchResult>>) {
        // If it's in fetching, don't clean the async fetch result.
        if self.async_fetch_results.borrow().get(&low) == Some(&RaftlogFetchState::Fetching)
            && res.is_none()
        {
            return;
        }

        match res {
            Some(res) => {
                if let Some(RaftlogFetchState::Fetched(prev)) = self
                    .async_fetch_results
                    .borrow_mut()
                    .insert(low, RaftlogFetchState::Fetched(res))
                {
                    info!(
                        "unconsumed async fetch res";
                        "region_id" => self.region.get_id(),
                        "peer_id" => self.peer_id,
                        "res" => ?prev,
                        "low" => low,
                    );
                }
            }
            None => {
                let prev = self.async_fetch_results.borrow_mut().remove(&low);
                if prev.is_some() {
                    self.raftlog_fetch_stats.fetch_unused.update(|m| m + 1);
                }
            }
        }
    }

    fn async_fetch(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: u64,
        context: GetEntriesContext,
        buf: &mut Vec<Entry>,
    ) -> raft::Result<usize> {
        if let Some(RaftlogFetchState::Fetching) = self.async_fetch_results.borrow().get(&low) {
            // already an async fetch in flight
            return Err(raft::Error::Store(
                raft::StorageError::LogTemporarilyUnavailable,
            ));
        }

        let tried_cnt = if let Some(RaftlogFetchState::Fetched(res)) =
            self.async_fetch_results.borrow_mut().remove(&low)
        {
            assert_eq!(res.low, low);
            let mut ents = res.ents?;
            let first = ents.first().map(|e| e.index).unwrap();
            assert_eq!(first, res.low);
            let last = ents.last().map(|e| e.index).unwrap();

            if last + 1 >= high {
                // async fetch res covers [low, high)
                ents.truncate((high - first) as usize);
                assert_eq!(ents.last().map(|e| e.index).unwrap(), high - 1);
                if max_size < res.max_size {
                    limit_size(&mut ents, Some(max_size));
                }
                let count = ents.len();
                buf.append(&mut ents);
                fail_point!("on_async_fetch_return");
                return Ok(count);
            } else if res.hit_size_limit && max_size <= res.max_size {
                // async fetch res doesn't cover [low, high) due to hit size limit
                if max_size < res.max_size {
                    limit_size(&mut ents, Some(max_size));
                };
                let count = ents.len();
                buf.append(&mut ents);
                return Ok(count);
            } else if last + RAFT_LOG_MULTI_GET_CNT > high - 1
                && res.tried_cnt + 1 == MAX_ASYNC_FETCH_TRY_CNT
            {
                let mut fetched_size = ents.iter().fold(0, |acc, e| acc + e.compute_size() as u64);
                if max_size <= fetched_size {
                    limit_size(&mut ents, Some(max_size));
                    let count = ents.len();
                    buf.append(&mut ents);
                    return Ok(count);
                }

                // the count of left entries isn't too large, fetch the remaining entries synchronously one by one
                for idx in last + 1..high {
                    let ent = self.engines.raft.get_entry(region_id, idx)?;
                    match ent {
                        None => {
                            return Err(raft::Error::Store(raft::StorageError::Unavailable));
                        }
                        Some(ent) => {
                            let size = ent.compute_size() as u64;
                            if fetched_size + size > max_size {
                                break;
                            } else {
                                fetched_size += size;
                                ents.push(ent);
                            }
                        }
                    }
                }
                let count = ents.len();
                buf.append(&mut ents);
                return Ok(count);
            }
            info!(
                "async fetch invalid";
                "region_id" => self.region.get_id(),
                "peer_id" => self.peer_id,
                "first" => first,
                "last" => last,
                "low" => low,
                "high" => high,
                "max_size" => max_size,
                "res_max_size" => res.max_size,
            );
            // low index or max size is changed, the result is not fit for the current range, so refetch again.
            self.raftlog_fetch_stats.fetch_invalid.update(|m| m + 1);
            res.tried_cnt + 1
        } else {
            1
        };

        // the first/second try: get [low, high) asynchronously
        // the third try:
        //  - if term and low are matched: use result of [low, persisted) and get [persisted, high) synchronously
        //  - else: get [low, high) synchronously
        if tried_cnt >= MAX_ASYNC_FETCH_TRY_CNT {
            // even the larger range is invalid again, fallback to fetch in sync way
            self.raftlog_fetch_stats.fallback_fetch.update(|m| m + 1);
            let count = self.engines.raft.fetch_entries_to(
                region_id,
                low,
                high,
                Some(max_size as usize),
                buf,
            )?;
            return Ok(count);
        }

        self.raftlog_fetch_stats.async_fetch.update(|m| m + 1);
        self.async_fetch_results
            .borrow_mut()
            .insert(low, RaftlogFetchState::Fetching);
        self.raftlog_fetch_scheduler
            .schedule(RaftlogFetchTask::PeerStorage {
                region_id,
                context,
                low,
                high,
                max_size: (max_size as usize),
                tried_cnt,
                term: self.hard_state().get_term(),
            })
            .unwrap();
        Err(raft::Error::Store(
            raft::StorageError::LogTemporarilyUnavailable,
        ))
    }

    pub fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: u64,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.check_range(low, high)?;
        let mut ents =
            Vec::with_capacity(std::cmp::min((high - low) as usize, MAX_INIT_ENTRY_COUNT));
        if low == high {
            return Ok(ents);
        }
        let region_id = self.get_region_id();
        let cache_low = self.cache.first_index().unwrap_or(u64::MAX);
        if high <= cache_low {
            self.cache.miss.update(|m| m + 1);
            return if context.can_async() {
                self.async_fetch(region_id, low, high, max_size, context, &mut ents)?;
                Ok(ents)
            } else {
                self.raftlog_fetch_stats.sync_fetch.update(|m| m + 1);
                self.engines.raft.fetch_entries_to(
                    region_id,
                    low,
                    high,
                    Some(max_size as usize),
                    &mut ents,
                )?;
                Ok(ents)
            };
        }
        let begin_idx = if low < cache_low {
            self.cache.miss.update(|m| m + 1);
            let fetched_count = if context.can_async() {
                self.async_fetch(region_id, low, cache_low, max_size, context, &mut ents)?
            } else {
                self.raftlog_fetch_stats.sync_fetch.update(|m| m + 1);
                self.engines.raft.fetch_entries_to(
                    region_id,
                    low,
                    cache_low,
                    Some(max_size as usize),
                    &mut ents,
                )?
            };
            if fetched_count < (cache_low - low) as usize {
                // Less entries are fetched than expected.
                return Ok(ents);
            }
            cache_low
        } else {
            low
        };
        self.cache.hit.update(|h| h + 1);
        let fetched_size = ents.iter().fold(0, |acc, e| acc + e.compute_size());
        self.cache
            .fetch_entries_to(begin_idx, high, fetched_size as u64, max_size, &mut ents);
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
        if let Some(e) = self.cache.entry(idx) {
            Ok(e.get_term())
        } else {
            Ok(self
                .engines
                .raft
                .get_entry(self.get_region_id(), idx)
                .unwrap()
                .unwrap()
                .get_term())
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

    #[inline]
    pub fn region(&self) -> &metapb::Region {
        &self.region
    }

    #[inline]
    pub fn set_region(&mut self, region: metapb::Region) {
        self.region = region;
    }

    #[inline]
    pub fn raw_snapshot(&self) -> EK::Snapshot {
        self.engines.kv.snapshot()
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
            &keys::snapshot_raft_state_key(self.region.get_id()),
            &snapshot_raft_state,
        )?;
        Ok(())
    }

    #[inline]
    pub fn save_apply_state_to(&self, kv_wb: &mut impl Mutable) -> Result<()> {
        kv_wb.put_msg_cf(
            CF_RAFT,
            &keys::apply_state_key(self.region.get_id()),
            &self.apply_state,
        )?;
        Ok(())
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
    pub fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        let mut snap_state = self.snap_state.borrow_mut();
        let mut tried_cnt = self.snap_tried_cnt.borrow_mut();

        let (mut tried, mut last_canceled, mut snap) = (false, false, None);
        if let SnapState::Generating {
            ref canceled,
            ref receiver,
            ..
        } = *snap_state
        {
            tried = true;
            last_canceled = canceled.load(Ordering::SeqCst);
            match receiver.try_recv() {
                Err(TryRecvError::Empty) => {
                    let e = raft::StorageError::SnapshotTemporarilyUnavailable;
                    return Err(raft::Error::Store(e));
                }
                Ok(s) if !last_canceled => snap = Some(s),
                Err(TryRecvError::Disconnected) | Ok(_) => {}
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
                        "request_peer" => to,
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
            "request_peer" => to,
        );

        if !tried || !last_canceled {
            *tried_cnt += 1;
        }

        let (sender, receiver) = mpsc::sync_channel(1);
        let canceled = Arc::new(AtomicBool::new(false));
        let index = Arc::new(AtomicU64::new(0));
        *snap_state = SnapState::Generating {
            canceled: canceled.clone(),
            index: index.clone(),
            receiver,
        };
        let mut to_store_id = 0;
        if let Some(peer) = self.region().get_peers().iter().find(|p| p.id == to) {
            to_store_id = peer.store_id;
        }
        let task = GenSnapTask::new(self.region.get_id(), index, canceled, sender, to_store_id);

        let mut gen_snap_task = self.gen_snap_task.borrow_mut();
        assert!(gen_snap_task.is_none());
        *gen_snap_task = Some(task);
        Err(raft::Error::Store(
            raft::StorageError::SnapshotTemporarilyUnavailable,
        ))
    }

    pub fn has_gen_snap_task(&self) -> bool {
        self.gen_snap_task.borrow().is_some()
    }

    pub fn mut_gen_snap_task(&mut self) -> &mut Option<GenSnapTask> {
        self.gen_snap_task.get_mut()
    }

    pub fn take_gen_snap_task(&mut self) -> Option<GenSnapTask> {
        self.gen_snap_task.get_mut().take()
    }

    // Append the given entries to the raft log using previous last index or self.last_index.
    pub fn append(&mut self, entries: Vec<Entry>, task: &mut WriteTask<EK, ER>) {
        if entries.is_empty() {
            return;
        }
        let region_id = self.get_region_id();
        debug!(
            "append entries";
            "region_id" => region_id,
            "peer_id" => self.peer_id,
            "count" => entries.len(),
        );
        let prev_last_index = self.raft_state.get_last_index();

        let (last_index, last_term) = {
            let e = entries.last().unwrap();
            (e.get_index(), e.get_term())
        };

        self.cache.append(&self.tag, &entries);

        task.entries = entries;
        // Delete any previously appended log entries which never committed.
        task.cut_logs = Some((last_index + 1, prev_last_index + 1));

        self.raft_state.set_last_index(last_index);
        self.last_term = last_term;
    }

    pub fn compact_to(&mut self, idx: u64) {
        self.compact_cache_to(idx);

        self.cancel_generating_snap(Some(idx));
    }

    pub fn compact_cache_to(&mut self, idx: u64) {
        self.cache.compact_to(idx);
        let rid = self.get_region_id();
        if self.engines.raft.has_builtin_entry_cache() {
            self.engines.raft.gc_entry_cache(rid, idx);
        }
    }

    #[inline]
    pub fn is_cache_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn maybe_gc_cache(&mut self, replicated_idx: u64, apply_idx: u64) {
        if self.engines.raft.has_builtin_entry_cache() {
            let rid = self.get_region_id();
            self.engines.raft.gc_entry_cache(rid, apply_idx + 1);
        }
        if replicated_idx == apply_idx {
            // The region is inactive, clear the cache immediately.
            self.cache.compact_to(apply_idx + 1);
            return;
        }
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

    /// Evict entries from the cache.
    pub fn evict_cache(&mut self, half: bool) {
        if !self.cache.cache.is_empty() {
            let cache = &mut self.cache;
            let cache_len = cache.cache.len();
            let drain_to = if half { cache_len / 2 } else { cache_len - 1 };
            let idx = cache.cache[drain_to].index;
            let mem_size_change = cache.compact_to(idx + 1);
            RAFT_ENTRIES_EVICT_BYTES.inc_by(mem_size_change);
        }
    }

    pub fn cache_is_empty(&self) -> bool {
        self.cache.cache.is_empty()
    }

    #[inline]
    pub fn flush_cache_metrics(&mut self) {
        // NOTE: memory usage of entry cache is flushed realtime.
        self.cache.flush_stats();
        self.raftlog_fetch_stats.flush_stats();
        if self.engines.raft.has_builtin_entry_cache() {
            if let Some(stats) = self.engines.raft.flush_stats() {
                RAFT_ENTRIES_CACHES_GAUGE.set(stats.cache_size as i64);
                RAFT_ENTRY_FETCHES.hit.inc_by(stats.hit as u64);
                RAFT_ENTRY_FETCHES.miss.inc_by(stats.miss as u64);
            }
        }
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot(
        &mut self,
        snap: &Snapshot,
        task: &mut WriteTask<EK, ER>,
        destroy_regions: &[metapb::Region],
    ) -> Result<metapb::Region> {
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

        if task.raft_wb.is_none() {
            task.raft_wb = Some(self.engines.raft.log_batch(64));
        }
        if task.kv_wb.is_none() {
            task.kv_wb = Some(self.engines.kv.write_batch());
        }
        let raft_wb = task.raft_wb.as_mut().unwrap();
        let kv_wb = task.kv_wb.as_mut().unwrap();

        if self.is_initialized() {
            // we can only delete the old data when the peer is initialized.
            let first_index = self.first_index();
            // It's possible that logs between `last_compacted_idx` and `first_index` are
            // being deleted in raftlog_gc worker. But it's OK as:
            // 1. If the peer accepts a new snapshot, it must start with an index larger than
            //    this `first_index`;
            // 2. If the peer accepts new entries after this snapshot or new snapshot, it must
            //    start with the new applied index, which is larger than `first_index`.
            // So new logs won't be deleted by on going raftlog_gc task accidentally.
            // It's possible that there will be some logs between `last_compacted_idx` and
            // `first_index` are not deleted. So a cleanup task for the range should be triggered
            // after applying the snapshot.
            self.clear_meta(first_index, kv_wb, raft_wb)?;
        }
        // Write its source peers' `RegionLocalState` together with itself for atomicity
        for r in destroy_regions {
            write_peer_state(kv_wb, r, PeerState::Tombstone, None)?;
        }
        write_peer_state(kv_wb, &region, PeerState::Applying, None)?;

        let last_index = snap.get_metadata().get_index();

        self.raft_state.set_last_index(last_index);
        self.last_term = snap.get_metadata().get_term();
        self.apply_state.set_applied_index(last_index);
        self.applied_index_term = self.last_term;

        // The snapshot only contains log which index > applied index, so
        // here the truncate state's (index, term) is in snapshot metadata.
        self.apply_state.mut_truncated_state().set_index(last_index);
        self.apply_state
            .mut_truncated_state()
            .set_term(snap.get_metadata().get_term());

        // `region` will be updated after persisting.
        // Although there is an interval that other metadata are updated while `region`
        // is not after handing snapshot from ready, at the time of writing, it's no
        // problem for now.
        // The reason why the update of `region` is delayed is that we expect `region` stays
        // consistent with the one in `StoreMeta::regions` which should be updated after
        // persisting due to atomic snapshot and peer create process. So if we can fix
        // these issues in future(maybe not?), the `region` and `StoreMeta::regions`
        // can updated here immediately.

        info!(
            "apply snapshot with state ok";
            "region_id" => self.region.get_id(),
            "peer_id" => self.peer_id,
            "region" => ?region,
            "state" => ?self.apply_state,
        );

        Ok(region)
    }

    /// Delete all meta belong to the region. Results are stored in `wb`.
    pub fn clear_meta(
        &mut self,
        first_index: u64,
        kv_wb: &mut EK::WriteBatch,
        raft_wb: &mut ER::LogBatch,
    ) -> Result<()> {
        let region_id = self.get_region_id();
        clear_meta(
            &self.engines,
            kv_wb,
            raft_wb,
            region_id,
            first_index,
            &self.raft_state,
        )?;
        self.cache = EntryCache::default();
        Ok(())
    }

    /// Delete all data belong to the region.
    /// If return Err, data may get partial deleted.
    pub fn clear_data(&self) -> Result<()> {
        let (start_key, end_key) = (enc_start_key(self.region()), enc_end_key(self.region()));
        let region_id = self.get_region_id();
        box_try!(
            self.region_scheduler
                .schedule(RegionTask::destroy(region_id, start_key, end_key))
        );
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
            box_try!(self.region_scheduler.schedule(RegionTask::destroy(
                old_region.get_id(),
                old_start_key,
                new_start_key
            )));
        }
        if new_end_key < old_end_key {
            box_try!(self.region_scheduler.schedule(RegionTask::destroy(
                old_region.get_id(),
                new_end_key,
                old_end_key
            )));
        }
        Ok(())
    }

    /// Delete all extra split data from the `start_key` to `end_key`.
    pub fn clear_extra_split_data(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<()> {
        box_try!(self.region_scheduler.schedule(RegionTask::destroy(
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
        matches!(*self.snap_state.borrow(), SnapState::Generating { .. })
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

    /// Cancel applying snapshot, return true if the job can be considered not be run again.
    pub fn cancel_applying_snap(&mut self) -> bool {
        let is_canceled = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                if status
                    .compare_exchange(
                        JOB_STATUS_PENDING,
                        JOB_STATUS_CANCELLING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    true
                } else if status
                    .compare_exchange(
                        JOB_STATUS_RUNNING,
                        JOB_STATUS_CANCELLING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    return false;
                } else {
                    false
                }
            }
            _ => return false,
        };
        if is_canceled {
            *self.snap_state.borrow_mut() = SnapState::ApplyAborted;
            return true;
        }
        // now status can only be JOB_STATUS_CANCELLING, JOB_STATUS_CANCELLED,
        // JOB_STATUS_FAILED and JOB_STATUS_FINISHED.
        self.check_applying_snap() != CheckApplyingSnapStatus::Applying
    }

    /// Cancel generating snapshot.
    pub fn cancel_generating_snap(&mut self, compact_to: Option<u64>) {
        let snap_state = self.snap_state.borrow();
        if let SnapState::Generating {
            ref canceled,
            ref index,
            ..
        } = *snap_state
        {
            if !canceled.load(Ordering::SeqCst) {
                if let Some(idx) = compact_to {
                    let snap_index = index.load(Ordering::SeqCst);
                    if snap_index == 0 || idx <= snap_index + 1 {
                        return;
                    }
                }
                canceled.store(true, Ordering::SeqCst);
            }
        }
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
        if let Err(e) = self.region_scheduler.schedule(task) {
            info!(
                "failed to to schedule apply job, are we shutting down?";
                "region_id" => self.region.get_id(),
                "peer_id" => self.peer_id,
                "err" => ?e,
            );
        }
    }

    /// Handle raft ready then generate `HandleReadyResult` and `WriteTask`.
    ///
    /// It's caller's duty to write `WriteTask` explicitly to disk.
    pub fn handle_raft_ready(
        &mut self,
        ready: &mut Ready,
        destroy_regions: Vec<metapb::Region>,
    ) -> Result<(HandleReadyResult, WriteTask<EK, ER>)> {
        let region_id = self.get_region_id();
        let prev_raft_state = self.raft_state.clone();

        let mut write_task = WriteTask::new(region_id, self.peer_id, ready.number());

        let mut res = HandleReadyResult::SendIOTask;
        if !ready.snapshot().is_empty() {
            fail_point!("raft_before_apply_snap");
            let last_first_index = self.first_index();
            let snap_region =
                self.apply_snapshot(ready.snapshot(), &mut write_task, &destroy_regions)?;

            res = HandleReadyResult::Snapshot {
                msgs: ready.take_persisted_messages(),
                snap_region,
                destroy_regions,
                last_first_index,
            };
            fail_point!("raft_after_apply_snap");
        };

        if !ready.entries().is_empty() {
            self.append(ready.take_entries(), &mut write_task);
        }

        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if self.raft_state.get_last_index() > 0 {
            if let Some(hs) = ready.hs() {
                self.raft_state.set_hard_state(hs.clone());
            }
        }

        // Save raft state if it has changed or there is a snapshot.
        if prev_raft_state != self.raft_state || !ready.snapshot().is_empty() {
            write_task.raft_state = Some(self.raft_state.clone());
        }

        if !ready.snapshot().is_empty() {
            // In case of restart happens when we just write region state to Applying,
            // but not write raft_local_state to raft db in time.
            // We write raft state to kv db, with last index set to snap index,
            // in case of recv raft log after snapshot.
            self.save_snapshot_raft_state_to(
                ready.snapshot().get_metadata().get_index(),
                write_task.kv_wb.as_mut().unwrap(),
            )?;
            self.save_apply_state_to(write_task.kv_wb.as_mut().unwrap())?;
        }

        if !write_task.has_data() {
            res = HandleReadyResult::NoIOTask;
        }

        Ok((res, write_task))
    }

    pub fn update_cache_persisted(&mut self, persisted: u64) {
        self.cache.update_persisted(persisted);
    }

    pub fn persist_snapshot(&mut self, res: &PersistSnapshotResult) {
        // cleanup data before scheduling apply task
        if self.is_initialized() {
            if let Err(e) = self.clear_extra_data(self.region(), &res.region) {
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
        for r in &res.destroy_regions {
            if let Err(e) = self.clear_extra_data(r, &res.region) {
                error!(?e;
                    "failed to cleanup data, may leave some dirty data";
                    "region_id" => r.get_id(),
                );
            }
        }

        self.schedule_applying_snapshot();

        // The `region` is updated after persisting in order to stay consistent with the one
        // in `StoreMeta::regions` (will be updated soon).
        // See comments in `apply_snapshot` for more details.
        self.set_region(res.region.clone());
    }

    pub fn trace_cached_entries(&mut self, entries: CachedEntries) {
        self.cache.trace_cached_entries(entries);
    }
}

/// Delete all meta belong to the region. Results are stored in `wb`.
pub fn clear_meta<EK, ER>(
    engines: &Engines<EK, ER>,
    kv_wb: &mut EK::WriteBatch,
    raft_wb: &mut ER::LogBatch,
    region_id: u64,
    first_index: u64,
    raft_state: &RaftLocalState,
) -> Result<()>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    let t = Instant::now();
    box_try!(kv_wb.delete_cf(CF_RAFT, &keys::region_state_key(region_id)));
    box_try!(kv_wb.delete_cf(CF_RAFT, &keys::apply_state_key(region_id)));
    box_try!(
        engines
            .raft
            .clean(region_id, first_index, raft_state, raft_wb)
    );

    info!(
        "finish clear peer meta";
        "region_id" => region_id,
        "meta_key" => 1,
        "apply_key" => 1,
        "raft_key" => 1,
        "takes" => ?t.saturating_elapsed(),
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
    for_balance: bool,
    allow_multi_files_snapshot: bool,
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
        allow_multi_files_snapshot,
    )?;
    snap_data.mut_meta().set_for_balance(for_balance);
    let v = snap_data.write_to_bytes()?;
    snapshot.set_data(v.into());

    SNAPSHOT_KV_COUNT_HISTOGRAM.observe(stat.kv_count as f64);
    SNAPSHOT_SIZE_HISTOGRAM.observe(stat.size as f64);

    Ok(snapshot)
}

// When we bootstrap the region we must call this to initialize region local state first.
pub fn write_initial_raft_state<W: RaftLogBatch>(raft_wb: &mut W, region_id: u64) -> Result<()> {
    let mut raft_state = RaftLocalState {
        last_index: RAFT_INIT_LOG_INDEX,
        ..Default::default()
    };
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

/// Committed entries sent to apply threads.
#[derive(Clone)]
pub struct CachedEntries {
    pub range: Range<u64>,
    // Entries and dangle size for them. `dangle` means not in entry cache.
    entries: Arc<Mutex<(Vec<Entry>, usize)>>,
}

impl CachedEntries {
    pub fn new(entries: Vec<Entry>) -> Self {
        assert!(!entries.is_empty());
        let start = entries.first().map(|x| x.index).unwrap();
        let end = entries.last().map(|x| x.index).unwrap() + 1;
        let range = Range { start, end };
        CachedEntries {
            entries: Arc::new(Mutex::new((entries, 0))),
            range,
        }
    }

    /// Take cached entries and dangle size for them. `dangle` means not in entry cache.
    pub fn take_entries(&self) -> (Vec<Entry>, usize) {
        mem::take(&mut *self.entries.lock().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        path::Path,
        sync::{atomic::*, mpsc::*, *},
        time::Duration,
    };

    use engine_test::{
        kv::{KvTestEngine, KvTestSnapshot},
        raft::RaftTestEngine,
    };
    use engine_traits::{
        Engines, Iterable, RaftEngineDebug, RaftEngineReadOnly, SyncMutable, WriteBatch,
        WriteBatchExt, ALL_CFS, CF_DEFAULT,
    };
    use kvproto::raft_serverpb::RaftSnapshotData;
    use metapb::{Peer, Store, StoreLabel};
    use pd_client::PdClient;
    use raft::{
        eraftpb::{ConfState, Entry, HardState},
        Error as RaftError, GetEntriesContext, StorageError,
    };
    use tempfile::{Builder, TempDir};
    use tikv_util::worker::{dummy_scheduler, LazyWorker, Scheduler, Worker};

    use super::*;
    use crate::{
        coprocessor::CoprocessorHost,
        store::{
            async_io::write::write_to_db_for_test,
            bootstrap_store,
            fsm::apply::compact_raft_log,
            initial_region, prepare_bootstrap_cluster,
            worker::{RaftlogFetchRunner, RegionRunner, RegionTask},
        },
    };

    impl EntryCache {
        fn new_with_cb(cb: impl Fn(i64) + Send + 'static) -> Self {
            let entry_cache = EntryCache {
                persisted: 0,
                cache: Default::default(),
                trace: Default::default(),
                hit: Cell::new(0),
                miss: Cell::new(0),
                size_change_cb: Some(Box::new(cb) as Box<dyn Fn(i64) + Send + 'static>),
            };
            entry_cache.flush_mem_size_change(entry_cache.get_total_mem_size());
            entry_cache
        }
    }

    fn new_storage(
        region_scheduler: Scheduler<RegionTask<KvTestSnapshot>>,
        raftlog_fetch_scheduler: Scheduler<RaftlogFetchTask>,
        path: &TempDir,
    ) -> PeerStorage<KvTestEngine, RaftTestEngine> {
        let kv_db = engine_test::kv::new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None)
            .unwrap();
        let raft_path = path.path().join(Path::new("raft"));
        let raft_db = engine_test::raft::new_engine(raft_path.to_str().unwrap(), None).unwrap();
        let engines = Engines::new(kv_db, raft_db);
        bootstrap_store(&engines, 1, 1).unwrap();

        let region = initial_region(1, 1, 1);
        prepare_bootstrap_cluster(&engines, &region).unwrap();

        // write some data into CF_DEFAULT cf
        let mut p = Peer::default();
        p.set_store_id(1);
        p.set_id(1_u64);
        for k in 0..100 {
            let key = keys::data_key(format!("akey{}", k).as_bytes());
            engines.kv.put_msg_cf(CF_DEFAULT, &key[..], &p).unwrap();
        }
        PeerStorage::new(
            engines,
            &region,
            region_scheduler,
            raftlog_fetch_scheduler,
            1,
            "".to_owned(),
        )
        .unwrap()
    }

    fn new_storage_from_ents(
        region_scheduler: Scheduler<RegionTask<KvTestSnapshot>>,
        raftlog_fetch_scheduler: Scheduler<RaftlogFetchTask>,
        path: &TempDir,
        ents: &[Entry],
    ) -> PeerStorage<KvTestEngine, RaftTestEngine> {
        let mut store = new_storage(region_scheduler, raftlog_fetch_scheduler, path);
        let mut write_task = WriteTask::new(store.get_region_id(), store.peer_id, 1);
        store.append(ents[1..].to_vec(), &mut write_task);
        store.update_cache_persisted(ents.last().unwrap().get_index());
        store
            .apply_state
            .mut_truncated_state()
            .set_index(ents[0].get_index());
        store
            .apply_state
            .mut_truncated_state()
            .set_term(ents[0].get_term());
        store
            .apply_state
            .set_applied_index(ents.last().unwrap().get_index());
        if write_task.kv_wb.is_none() {
            write_task.kv_wb = Some(store.engines.kv.write_batch());
        }
        store
            .save_apply_state_to(write_task.kv_wb.as_mut().unwrap())
            .unwrap();
        write_task.raft_state = Some(store.raft_state.clone());
        write_to_db_for_test(&store.engines, write_task);
        store
    }

    fn append_ents(store: &mut PeerStorage<KvTestEngine, RaftTestEngine>, ents: &[Entry]) {
        if ents.is_empty() {
            return;
        }
        let mut write_task = WriteTask::new(store.get_region_id(), store.peer_id, 1);
        store.append(ents.to_vec(), &mut write_task);
        write_task.raft_state = Some(store.raft_state.clone());
        write_to_db_for_test(&store.engines, write_task);
    }

    fn validate_cache(store: &PeerStorage<KvTestEngine, RaftTestEngine>, exp_ents: &[Entry]) {
        assert_eq!(store.cache.cache, exp_ents);
        for e in exp_ents {
            let entry = store
                .engines
                .raft
                .get_entry(store.get_region_id(), e.get_index())
                .unwrap()
                .unwrap();
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

    pub struct TestPdClient {
        stores: Vec<metapb::Store>,
    }

    impl TestPdClient {
        pub fn new() -> TestPdClient {
            TestPdClient {
                stores: vec![metapb::Store::default(); 4],
            }
        }

        pub fn add_store(&mut self, store: metapb::Store) {
            let id = store.get_id();
            self.stores[id as usize] = store;
        }
    }

    impl PdClient for TestPdClient {
        fn get_store(&self, store_id: u64) -> pd_client::Result<metapb::Store> {
            if store_id < 4 {
                return Ok(self.stores[store_id as usize].clone());
            }
            Err(pd_client::Error::StoreTombstone(format!("{:?}", store_id)))
        }
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
            let (dummy_scheduler, _) = dummy_scheduler();
            let store = new_storage_from_ents(sched, dummy_scheduler, &td, &ents);
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
            .scan_entries(region_id, |_| {
                count += 1;
                Ok(true)
            })
            .unwrap();

        if store
            .engines
            .raft
            .get_raft_state(region_id)
            .unwrap()
            .is_some()
        {
            count += 1;
        }

        count
    }

    #[test]
    fn test_storage_clear_meta() {
        let worker = Worker::new("snap-manager").lazy_build("snap-manager");
        let cases = vec![(0, 0), (3, 0)];
        for (first_index, left) in cases {
            let td = Builder::new().prefix("tikv-store").tempdir().unwrap();
            let sched = worker.scheduler();
            let (dummy_scheduler, _) = dummy_scheduler();
            let mut store = new_storage_from_ents(
                sched,
                dummy_scheduler,
                &td,
                &[new_entry(3, 3), new_entry(4, 4)],
            );
            append_ents(&mut store, &[new_entry(5, 5), new_entry(6, 6)]);

            assert_eq!(6, get_meta_key_count(&store));

            let mut kv_wb = store.engines.kv.write_batch();
            let mut raft_wb = store.engines.raft.log_batch(0);
            store
                .clear_meta(first_index, &mut kv_wb, &mut raft_wb)
                .unwrap();
            kv_wb.write().unwrap();
            store
                .engines
                .raft
                .consume(&mut raft_wb, false /*sync*/)
                .unwrap();

            assert_eq!(left, get_meta_key_count(&store));
        }
    }

    use crate::{
        store::{SignificantMsg, SignificantRouter},
        Result as RaftStoreResult,
    };

    pub struct TestRouter<EK: KvEngine> {
        ch: SyncSender<SignificantMsg<EK::Snapshot>>,
    }

    impl<EK: KvEngine> TestRouter<EK> {
        pub fn new() -> (Self, Receiver<SignificantMsg<EK::Snapshot>>) {
            let (tx, rx) = sync_channel(1);
            (Self { ch: tx }, rx)
        }
    }

    impl<EK> SignificantRouter<EK> for TestRouter<EK>
    where
        EK: KvEngine,
    {
        /// Sends a significant message. We should guarantee that the message can't be dropped.
        fn significant_send(
            &self,
            _: u64,
            msg: SignificantMsg<EK::Snapshot>,
        ) -> RaftStoreResult<()> {
            self.ch.send(msg).unwrap();
            Ok(())
        }
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

        let mut count = 0;
        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let (router, rx) = TestRouter::new();
            let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
            let region_worker = Worker::new("snap-manager").lazy_build("snap-manager");
            let region_scheduler = region_worker.scheduler();
            let mut raftlog_fetch_worker =
                Worker::new("raftlog-fetch-worker").lazy_build("raftlog-fetch-worker");
            let raftlog_fetch_scheduler = raftlog_fetch_worker.scheduler();
            let mut store =
                new_storage_from_ents(region_scheduler, raftlog_fetch_scheduler, &td, &ents);
            raftlog_fetch_worker.start(RaftlogFetchRunner::<KvTestEngine, RaftTestEngine, _>::new(
                router,
                store.engines.raft.clone(),
            ));
            store.compact_cache_to(5);
            let mut e = store.entries(lo, hi, maxsize, GetEntriesContext::empty(true));
            if e == Err(raft::Error::Store(
                raft::StorageError::LogTemporarilyUnavailable,
            )) {
                let res = rx.recv().unwrap();
                match res {
                    SignificantMsg::RaftlogFetched { res, context } => {
                        store.update_async_fetch_res(lo, Some(res));
                        count += 1;
                        e = store.entries(lo, hi, maxsize, context);
                    }
                    _ => unreachable!(),
                };
            }
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }

        assert_ne!(count, 0);
    }

    #[test]
    fn test_async_fetch() {
        let ents = vec![
            new_entry(2, 2),
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];

        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let region_worker = Worker::new("snap-manager").lazy_build("snap-manager");
        let region_scheduler = region_worker.scheduler();
        let (dummy_scheduler, _rx) = dummy_scheduler();
        let mut store = new_storage_from_ents(region_scheduler, dummy_scheduler, &td, &ents);

        let max_u64 = u64::max_value();
        let mut tests = vec![
            // already compacted
            (
                3,
                7,
                max_u64,
                1,
                RaftlogFetchResult {
                    ents: Err(RaftError::Store(StorageError::Compacted)),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Err(RaftError::Store(StorageError::Compacted)),
                vec![],
            ),
            // fetch partial entries due to max size limit
            (
                3,
                7,
                30,
                1,
                RaftlogFetchResult {
                    ents: Ok(ents[1..4].to_vec()),
                    low: 3,
                    max_size: 30,
                    hit_size_limit: true,
                    tried_cnt: 1,
                    term: 1,
                },
                Ok(3),
                ents[1..4].to_vec(),
            ),
            // fetch all entries
            (
                2,
                7,
                max_u64,
                1,
                RaftlogFetchResult {
                    ents: Ok(ents.clone()),
                    low: 2,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Ok(5),
                ents.clone(),
            ),
            // high is smaller than before
            (
                3,
                5,
                max_u64,
                1,
                RaftlogFetchResult {
                    ents: Ok(ents[1..].to_vec()),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Ok(2),
                ents[1..3].to_vec(),
            ),
            // high is larger than before, second try
            (
                3,
                7,
                max_u64,
                1,
                RaftlogFetchResult {
                    ents: Ok(ents[1..4].to_vec()),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Err(RaftError::Store(StorageError::LogTemporarilyUnavailable)),
                vec![],
            ),
            // high is larger than before, thrid try
            (
                3,
                7,
                max_u64,
                1,
                RaftlogFetchResult {
                    ents: Ok(ents[1..4].to_vec()),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 2,
                    term: 1,
                },
                Ok(4),
                ents[1..].to_vec(),
            ),
            // max size is smaller than before
            (
                2,
                7,
                10,
                1,
                RaftlogFetchResult {
                    ents: Ok(ents.clone()),
                    low: 2,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Ok(2),
                ents[..2].to_vec(),
            ),
            // max size is larger than before but with lower high
            (
                2,
                5,
                40,
                1,
                RaftlogFetchResult {
                    ents: Ok(ents.clone()),
                    low: 2,
                    max_size: 30,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Ok(3),
                ents[..3].to_vec(),
            ),
            // low index is smaller than before
            (
                2,
                7,
                max_u64,
                1,
                RaftlogFetchResult {
                    ents: Err(RaftError::Store(StorageError::Compacted)),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Err(RaftError::Store(StorageError::LogTemporarilyUnavailable)),
                vec![],
            ),
            // low index is larger than before
            (
                4,
                7,
                max_u64,
                1,
                RaftlogFetchResult {
                    ents: Ok(vec![]),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Err(RaftError::Store(StorageError::LogTemporarilyUnavailable)),
                vec![],
            ),
            // hit tried several lmit
            (
                3,
                7,
                max_u64,
                1,
                RaftlogFetchResult {
                    ents: Ok(ents[1..4].to_vec()),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: MAX_ASYNC_FETCH_TRY_CNT,
                    term: 1,
                },
                Ok(4),
                ents[1..5].to_vec(),
            ),
            // term is changed
            (
                3,
                7,
                max_u64,
                2,
                RaftlogFetchResult {
                    ents: Ok(ents[1..4].to_vec()),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: MAX_ASYNC_FETCH_TRY_CNT,
                    term: 1,
                },
                Ok(4),
                ents[1..5].to_vec(),
            ),
        ];

        for (i, (lo, hi, maxsize, term, async_res, expected_res, expected_ents)) in
            tests.drain(..).enumerate()
        {
            if async_res.low != lo {
                store.clean_async_fetch_res(lo);
            } else {
                store.update_async_fetch_res(lo, Some(Box::new(async_res)));
            }
            let mut ents = vec![];
            store.raft_state.mut_hard_state().set_term(term);
            let res = store.async_fetch(
                store.get_region_id(),
                lo,
                hi,
                maxsize,
                GetEntriesContext::empty(true),
                &mut ents,
            );
            if res != expected_res {
                panic!("#{}: expect result {:?}, got {:?}", i, expected_res, res);
            }
            if ents != expected_ents {
                panic!("#{}: expect ents {:?}, got {:?}", i, expected_ents, ents);
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
            let (dummy_scheduler, _) = dummy_scheduler();
            let mut store = new_storage_from_ents(sched, dummy_scheduler, &td, &ents);
            let res = store
                .term(idx)
                .map_err(From::from)
                .and_then(|term| compact_raft_log(&store.tag, &mut store.apply_state, idx, term));
            // TODO check exact error type after refactoring error.
            if res.is_err() ^ werr.is_err() {
                panic!("#{}: want {:?}, got {:?}", i, werr, res);
            }
            if res.is_ok() {
                let mut kv_wb = store.engines.kv.write_batch();
                store.save_apply_state_to(&mut kv_wb).unwrap();
                kv_wb.write().unwrap();
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
            .get_entry(gen_task.region_id, idx)
            .unwrap()
            .unwrap();
        gen_task.generate_and_schedule_snapshot::<KvTestEngine>(
            engines.kv.clone().snapshot(),
            entry.get_term(),
            apply_state,
            sched,
        )
    }

    fn new_store(id: u64, labels: Vec<StoreLabel>) -> Store {
        let mut store = Store {
            id,
            ..Default::default()
        };
        store.set_labels(labels.into());
        store
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::default();
        cs.set_voters(vec![1, 2, 3]);

        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let mut worker = Worker::new("region-worker").lazy_build("region-worker");
        let sched = worker.scheduler();
        let (dummy_scheduler, _) = dummy_scheduler();
        let mut s = new_storage_from_ents(sched.clone(), dummy_scheduler, &td, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let runner = RegionRunner::new(
            s.engines.kv.clone(),
            mgr,
            0,
            true,
            2,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
            Option::<Arc<TestPdClient>>::None,
        );
        worker.start_with_timer(runner);
        let snap = s.snapshot(0, 0);
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap();
        let snap = match *s.snap_state.borrow() {
            SnapState::Generating { ref receiver, .. } => {
                receiver.recv_timeout(Duration::from_secs(3)).unwrap()
            }
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
        s.set_snap_state(gen_snap_for_test(rx));
        // Empty channel should cause snapshot call to wait.
        assert_eq!(s.snapshot(0, 0).unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        tx.send(snap.clone()).unwrap();
        assert_eq!(s.snapshot(0, 0), Ok(snap.clone()));
        assert_eq!(*s.snap_tried_cnt.borrow(), 0);

        let (tx, rx) = channel();
        tx.send(snap.clone()).unwrap();
        s.set_snap_state(gen_snap_for_test(rx));
        // stale snapshot should be abandoned, snapshot index < request index.
        assert_eq!(
            s.snapshot(snap.get_metadata().get_index() + 1, 0)
                .unwrap_err(),
            unavailable
        );
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);
        // Drop the task.
        let _ = s.gen_snap_task.borrow_mut().take().unwrap();

        let mut write_task = WriteTask::new(s.get_region_id(), s.peer_id, 1);
        s.append([new_entry(6, 5), new_entry(7, 5)].to_vec(), &mut write_task);
        let mut hs = HardState::default();
        hs.set_commit(7);
        hs.set_term(5);
        s.raft_state.set_hard_state(hs);
        s.raft_state.set_last_index(7);
        s.apply_state.set_applied_index(7);
        write_task.raft_state = Some(s.raft_state.clone());
        if write_task.kv_wb.is_none() {
            write_task.kv_wb = Some(s.engines.kv.write_batch());
        }
        s.save_apply_state_to(write_task.kv_wb.as_mut().unwrap())
            .unwrap();
        write_to_db_for_test(&s.engines, write_task);
        let term = s.term(7).unwrap();
        compact_raft_log(&s.tag, &mut s.apply_state, 7, term).unwrap();
        let mut kv_wb = s.engines.kv.write_batch();
        s.save_apply_state_to(&mut kv_wb).unwrap();
        kv_wb.write().unwrap();

        let (tx, rx) = channel();
        tx.send(snap).unwrap();
        s.set_snap_state(gen_snap_for_test(rx));
        *s.snap_tried_cnt.borrow_mut() = 1;
        // stale snapshot should be abandoned, snapshot index < truncated index.
        assert_eq!(s.snapshot(0, 0).unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);

        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap();
        match *s.snap_state.borrow() {
            SnapState::Generating { ref receiver, .. } => {
                receiver.recv_timeout(Duration::from_secs(3)).unwrap();
                worker.stop();
                match receiver.recv_timeout(Duration::from_secs(3)) {
                    Err(RecvTimeoutError::Disconnected) => {}
                    res => panic!("unexpected result: {:?}", res),
                }
            }
            ref s => panic!("unexpected state {:?}", s),
        }
        // Disconnected channel should trigger another try.
        assert_eq!(s.snapshot(0, 0).unwrap_err(), unavailable);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap_err();
        assert_eq!(*s.snap_tried_cnt.borrow(), 2);

        for cnt in 2..super::MAX_SNAP_TRY_CNT + 10 {
            if cnt < 12 {
                // Canceled generating won't be counted in `snap_tried_cnt`.
                s.cancel_generating_snap(None);
                assert_eq!(*s.snap_tried_cnt.borrow(), 2);
            } else {
                assert_eq!(*s.snap_tried_cnt.borrow(), cnt - 10);
            }

            // Scheduled job failed should trigger .
            assert_eq!(s.snapshot(0, 0).unwrap_err(), unavailable);
            let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
            generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap_err();
        }

        // When retry too many times, it should report a different error.
        match s.snapshot(0, 0) {
            Err(RaftError::Store(StorageError::Other(_))) => {}
            res => panic!("unexpected res: {:?}", res),
        }
    }

    fn test_storage_create_snapshot_for_role(role: &str, expected_snapshot_file_count: usize) {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::default();
        cs.set_voters(vec![1, 2, 3]);

        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mut mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        mgr.set_enable_multi_snapshot_files(true);
        mgr.set_max_per_file_size(500);
        let mut worker = Worker::new("region-worker").lazy_build("region-worker");
        let sched = worker.scheduler();
        let (dummy_scheduler, _) = dummy_scheduler();
        let s = new_storage_from_ents(sched.clone(), dummy_scheduler, &td, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let mut pd_client = TestPdClient::new();
        let labels = vec![StoreLabel {
            key: "engine".to_string(),
            value: role.to_string(),
            ..Default::default()
        }];
        let store = new_store(1, labels);
        pd_client.add_store(store);
        let pd_mock = Arc::new(pd_client);
        let runner = RegionRunner::new(
            s.engines.kv.clone(),
            mgr,
            0,
            true,
            2,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
            Some(pd_mock),
        );
        worker.start_with_timer(runner);
        let snap = s.snapshot(0, 1);
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s.engines, &sched).unwrap();
        let snap = match *s.snap_state.borrow() {
            SnapState::Generating { ref receiver, .. } => {
                receiver.recv_timeout(Duration::from_secs(3)).unwrap()
            }
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(snap.get_metadata().get_index(), 5);
        assert_eq!(snap.get_metadata().get_term(), 5);
        assert!(!snap.get_data().is_empty());

        let mut data = RaftSnapshotData::default();
        protobuf::Message::merge_from_bytes(&mut data, snap.get_data()).unwrap();
        assert_eq!(data.get_region().get_id(), 1);
        assert_eq!(data.get_region().get_peers().len(), 1);
        let files = data.get_meta().get_cf_files();
        assert_eq!(files.len(), expected_snapshot_file_count);
    }

    #[test]
    fn test_storage_create_snapshot_for_tiflash() {
        // each cf will have one cf file
        test_storage_create_snapshot_for_role("TiFlash" /* case does not matter */, 3);
    }

    #[test]
    fn test_storage_create_snapshot_for_tikv() {
        // default cf will have 3 sst files
        test_storage_create_snapshot_for_role("tikv", 5);
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(4, 6), new_entry(5, 6)],
                vec![new_entry(4, 6), new_entry(5, 6)],
            ),
            (
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
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
            let (dummy_scheduler, _) = dummy_scheduler();
            let mut store = new_storage_from_ents(sched, dummy_scheduler, &td, &ents);
            append_ents(&mut store, &entries);
            let li = store.last_index();
            let actual_entries = store
                .entries(4, li + 1, u64::max_value(), GetEntriesContext::empty(false))
                .unwrap();
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
        let (dummy_scheduler, _) = dummy_scheduler();
        let mut store = new_storage_from_ents(sched, dummy_scheduler, &td, &ents);
        store.cache.cache.clear();
        // empty cache should fetch data from rocksdb directly.
        let mut res = store
            .entries(4, 6, u64::max_value(), GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(*res, ents[1..]);

        let entries = vec![new_entry(6, 5), new_entry(7, 5)];
        append_ents(&mut store, &entries);
        validate_cache(&store, &entries);

        // direct cache access
        res = store
            .entries(6, 8, u64::max_value(), GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(res, entries);

        // size limit should be supported correctly.
        res = store
            .entries(4, 8, 0, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(res, vec![new_entry(4, 4)]);
        let mut size = ents[1..].iter().map(|e| u64::from(e.compute_size())).sum();
        res = store
            .entries(4, 8, size, GetEntriesContext::empty(false))
            .unwrap();
        let mut exp_res = ents[1..].to_vec();
        assert_eq!(res, exp_res);
        for e in &entries {
            size += u64::from(e.compute_size());
            exp_res.push(e.clone());
            res = store
                .entries(4, 8, size, GetEntriesContext::empty(false))
                .unwrap();
            assert_eq!(res, exp_res);
        }

        // range limit should be supported correctly.
        for low in 4..9 {
            for high in low..9 {
                let res = store
                    .entries(low, high, u64::max_value(), GetEntriesContext::empty(false))
                    .unwrap();
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
        let (dummy_scheduler, _) = dummy_scheduler();
        let mut store = new_storage_from_ents(sched, dummy_scheduler, &td, &ents);
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

        // compact to min(5 + 1, 7)
        store.cache.persisted = 5;
        store.compact_to(7);
        exp_res = vec![new_entry(6, 7), new_entry(7, 8)];
        validate_cache(&store, &exp_res);

        // compact to min(7 + 1, 7)
        store.cache.persisted = 7;
        store.compact_to(7);
        exp_res = vec![new_entry(7, 8)];
        validate_cache(&store, &exp_res);
        // compact all
        store.compact_to(8);
        validate_cache(&store, &[]);
        // invalid compaction should be ignored.
        store.compact_to(6);
    }

    #[test]
    fn test_storage_cache_size_change() {
        let new_padded_entry = |index: u64, term: u64, pad_len: usize| {
            let mut e = new_entry(index, term);
            e.data = vec![b'x'; pad_len].into();
            e
        };

        // Test the initial data structure size.
        let (tx, rx) = mpsc::sync_channel(8);
        let mut cache = EntryCache::new_with_cb(move |c: i64| tx.send(c).unwrap());
        assert_eq!(rx.try_recv().unwrap(), 896);

        cache.append(
            "",
            &[new_padded_entry(101, 1, 1), new_padded_entry(102, 1, 2)],
        );
        assert_eq!(rx.try_recv().unwrap(), 3);

        // Test size change for one overlapped entry.
        cache.append("", &[new_padded_entry(102, 2, 3)]);
        assert_eq!(rx.try_recv().unwrap(), 1);

        // Test size change for all overlapped entries.
        cache.append(
            "",
            &[new_padded_entry(101, 3, 4), new_padded_entry(102, 3, 5)],
        );
        assert_eq!(rx.try_recv().unwrap(), 5);

        cache.append("", &[new_padded_entry(103, 3, 6)]);
        assert_eq!(rx.try_recv().unwrap(), 6);

        // Test trace a dangle entry.
        let cached_entries = CachedEntries::new(vec![new_padded_entry(100, 1, 1)]);
        cache.trace_cached_entries(cached_entries);
        assert_eq!(rx.try_recv().unwrap(), 1);

        // Test trace an entry which is still in cache.
        let cached_entries = CachedEntries::new(vec![new_padded_entry(102, 3, 5)]);
        cache.trace_cached_entries(cached_entries);
        assert_eq!(rx.try_recv().unwrap(), 0);

        // Test compare `cached_last` with `trunc_to_idx` in `EntryCache::append_impl`.
        cache.append("", &[new_padded_entry(103, 4, 7)]);
        assert_eq!(rx.try_recv().unwrap(), 1);

        // Test compact one traced dangle entry and one entry in cache.
        cache.persisted = 101;
        cache.compact_to(102);
        assert_eq!(rx.try_recv().unwrap(), -5);

        // Test compact the last traced dangle entry.
        cache.persisted = 102;
        cache.compact_to(103);
        assert_eq!(rx.try_recv().unwrap(), -5);

        // Test compact all entries.
        cache.persisted = 103;
        cache.compact_to(104);
        assert_eq!(rx.try_recv().unwrap(), -7);

        drop(cache);
        assert_eq!(rx.try_recv().unwrap(), -896);
    }

    #[test]
    fn test_storage_cache_entry() {
        let mut cache = EntryCache::default();
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 4),
            new_entry(6, 6),
        ];
        cache.append("", &ents);
        assert!(cache.entry(1).is_none());
        assert!(cache.entry(2).is_none());
        for e in &ents {
            assert_eq!(e, cache.entry(e.get_index()).unwrap());
        }
        let res = panic_hook::recover_safe(|| cache.entry(7));
        assert!(res.is_err());
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
        let (dummy_scheduler, _) = dummy_scheduler();
        let s1 = new_storage_from_ents(sched.clone(), dummy_scheduler.clone(), &td1, &ents);
        let (router, _) = mpsc::sync_channel(100);
        let runner = RegionRunner::new(
            s1.engines.kv.clone(),
            mgr,
            0,
            true,
            2,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
            Option::<Arc<TestPdClient>>::None,
        );
        worker.start(runner);
        assert!(s1.snapshot(0, 0).is_err());
        let gen_task = s1.gen_snap_task.borrow_mut().take().unwrap();
        generate_and_schedule_snapshot(gen_task, &s1.engines, &sched).unwrap();

        let snap1 = match *s1.snap_state.borrow() {
            SnapState::Generating { ref receiver, .. } => {
                receiver.recv_timeout(Duration::from_secs(3)).unwrap()
            }
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(s1.truncated_index(), 3);
        assert_eq!(s1.truncated_term(), 3);
        worker.stop();

        let td2 = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let mut s2 = new_storage(sched.clone(), dummy_scheduler.clone(), &td2);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        let mut write_task = WriteTask::new(s2.get_region_id(), s2.peer_id, 1);
        let snap_region = s2.apply_snapshot(&snap1, &mut write_task, &[]).unwrap();
        let mut snap_data = RaftSnapshotData::default();
        snap_data.merge_from_bytes(snap1.get_data()).unwrap();
        assert_eq!(snap_region, snap_data.take_region(),);
        assert_eq!(s2.last_term, snap1.get_metadata().get_term());
        assert_eq!(s2.apply_state.get_applied_index(), 6);
        assert_eq!(s2.raft_state.get_last_index(), 6);
        assert_eq!(s2.apply_state.get_truncated_state().get_index(), 6);
        assert_eq!(s2.apply_state.get_truncated_state().get_term(), 6);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        validate_cache(&s2, &[]);

        let td3 = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let ents = &[new_entry(3, 3), new_entry(4, 3)];
        let mut s3 = new_storage_from_ents(sched, dummy_scheduler, &td3, ents);
        validate_cache(&s3, &ents[1..]);
        let mut write_task = WriteTask::new(s3.get_region_id(), s3.peer_id, 1);
        let snap_region = s3.apply_snapshot(&snap1, &mut write_task, &[]).unwrap();
        let mut snap_data = RaftSnapshotData::default();
        snap_data.merge_from_bytes(snap1.get_data()).unwrap();
        assert_eq!(snap_region, snap_data.take_region(),);
        assert_eq!(s3.last_term, snap1.get_metadata().get_term());
        assert_eq!(s3.apply_state.get_applied_index(), 6);
        assert_eq!(s3.raft_state.get_last_index(), 6);
        assert_eq!(s3.apply_state.get_truncated_state().get_index(), 6);
        assert_eq!(s3.apply_state.get_truncated_state().get_term(), 6);
        validate_cache(&s3, &[]);
    }

    #[test]
    fn test_canceling_apply_snapshot() {
        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let worker = LazyWorker::new("snap-manager");
        let sched = worker.scheduler();
        let (dummy_scheduler, _) = dummy_scheduler();
        let mut s = new_storage(sched, dummy_scheduler, &td);

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
        let (dummy_scheduler, _) = dummy_scheduler();
        let mut s = new_storage(sched, dummy_scheduler, &td);

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
    fn test_validate_states() {
        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let region_worker = LazyWorker::new("snap-manager");
        let region_sched = region_worker.scheduler();
        let raftlog_fetch_worker = LazyWorker::new("raftlog-fetch-worker");
        let raftlog_fetch_sched = raftlog_fetch_worker.scheduler();
        let kv_db =
            engine_test::kv::new_engine(td.path().to_str().unwrap(), None, ALL_CFS, None).unwrap();
        let raft_path = td.path().join(Path::new("raft"));
        let raft_db = engine_test::raft::new_engine(raft_path.to_str().unwrap(), None).unwrap();
        let engines = Engines::new(kv_db, raft_db);
        bootstrap_store(&engines, 1, 1).unwrap();

        let region = initial_region(1, 1, 1);
        prepare_bootstrap_cluster(&engines, &region).unwrap();
        let build_storage = || -> Result<PeerStorage<KvTestEngine, RaftTestEngine>> {
            PeerStorage::new(
                engines.clone(),
                &region,
                region_sched.clone(),
                raftlog_fetch_sched.clone(),
                0,
                "".to_owned(),
            )
        };
        let mut s = build_storage().unwrap();
        let mut raft_state = RaftLocalState::default();
        raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
        raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
        raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *raft_state.get_hard_state());

        // last_index < commit_index is invalid.
        raft_state.set_last_index(11);
        engines
            .raft
            .append(1, vec![new_entry(11, RAFT_INIT_LOG_TERM)])
            .unwrap();
        raft_state.mut_hard_state().set_commit(12);
        engines.raft.put_raft_state(1, &raft_state).unwrap();
        assert!(build_storage().is_err());

        raft_state.set_last_index(20);
        let entries = (12..=20)
            .map(|index| new_entry(index, RAFT_INIT_LOG_TERM))
            .collect();
        engines.raft.append(1, entries).unwrap();
        engines.raft.put_raft_state(1, &raft_state).unwrap();
        s = build_storage().unwrap();
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *raft_state.get_hard_state());

        // Missing last log is invalid.
        raft_state.set_last_index(21);
        engines.raft.put_raft_state(1, &raft_state).unwrap();
        assert!(build_storage().is_err());
        raft_state.set_last_index(20);
        engines.raft.put_raft_state(1, &raft_state).unwrap();

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
        engines.raft.gc(1, 14, 15).unwrap();
        apply_state.set_commit_index(14);
        apply_state.set_commit_term(RAFT_INIT_LOG_TERM);
        engines
            .kv
            .put_msg_cf(CF_RAFT, &apply_state_key, &apply_state)
            .unwrap();
        assert!(build_storage().is_err());

        let entries = (14..=20)
            .map(|index| new_entry(index, RAFT_INIT_LOG_TERM))
            .collect();
        engines.raft.gc(1, 0, 21).unwrap();
        engines.raft.append(1, entries).unwrap();
        raft_state.mut_hard_state().set_commit(14);
        s = build_storage().unwrap();
        let initial_state = s.initial_state().unwrap();
        assert_eq!(initial_state.hard_state, *raft_state.get_hard_state());

        // log term mismatch is invalid.
        let mut entries: Vec<_> = (14..=20)
            .map(|index| new_entry(index, RAFT_INIT_LOG_TERM))
            .collect();
        entries[0].set_term(RAFT_INIT_LOG_TERM - 1);
        engines.raft.append(1, entries).unwrap();
        assert!(build_storage().is_err());

        // hard state term miss match is invalid.
        let entries = (14..=20)
            .map(|index| new_entry(index, RAFT_INIT_LOG_TERM))
            .collect();
        engines.raft.append(1, entries).unwrap();
        raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM - 1);
        engines.raft.put_raft_state(1, &raft_state).unwrap();
        assert!(build_storage().is_err());

        // last index < recorded_commit_index is invalid.
        engines.raft.gc(1, 0, 21).unwrap();
        raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
        raft_state.set_last_index(13);
        engines
            .raft
            .append(1, vec![new_entry(13, RAFT_INIT_LOG_TERM)])
            .unwrap();
        engines.raft.put_raft_state(1, &raft_state).unwrap();
        assert!(build_storage().is_err());
    }

    fn gen_snap_for_test(rx: Receiver<Snapshot>) -> SnapState {
        SnapState::Generating {
            canceled: Arc::new(AtomicBool::new(false)),
            index: Arc::new(AtomicU64::new(0)),
            receiver: rx,
        }
    }
}
