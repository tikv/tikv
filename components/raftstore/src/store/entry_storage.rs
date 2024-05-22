// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains the implementation of the `EntryStorage`, which covers
//! a subset of raft storage. This module will be shared between raftstore v1
//! and v2.

use std::{
    cell::{Cell, RefCell},
    cmp,
    collections::VecDeque,
    mem,
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};

use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine, RAFT_LOG_MULTI_GET_CNT};
use fail::fail_point;
use kvproto::{
    metapb,
    raft_serverpb::{RaftApplyState, RaftLocalState},
};
use protobuf::Message;
use raft::{prelude::*, util::limit_size, GetEntriesContext, StorageError, INVALID_INDEX};
use tikv_alloc::TraceEvent;
use tikv_util::{box_err, debug, error, info, time::Instant, warn, worker::Scheduler};

use super::{
    metrics::*, peer_storage::storage_error, WriteTask, MEMTRACE_ENTRY_CACHE, RAFT_INIT_LOG_INDEX,
    RAFT_INIT_LOG_TERM,
};
use crate::{bytes_capacity, store::ReadTask, Result};

const MAX_ASYNC_FETCH_TRY_CNT: usize = 3;
const SHRINK_CACHE_CAPACITY: usize = 64;
const ENTRY_MEM_SIZE: usize = mem::size_of::<Entry>();

pub const MAX_WARMED_UP_CACHE_KEEP_TIME: Duration = Duration::from_secs(10);
pub const MAX_INIT_ENTRY_COUNT: usize = 1024;

#[inline]
pub fn first_index(state: &RaftApplyState) -> u64 {
    state.get_truncated_state().get_index() + 1
}

#[inline]
pub fn last_index(state: &RaftLocalState) -> u64 {
    state.get_last_index()
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

    pub fn iter_entries(&self, mut f: impl FnMut(&Entry)) {
        let entries = self.entries.lock().unwrap();
        for entry in &entries.0 {
            f(entry);
        }
    }

    /// Take cached entries and dangle size for them. `dangle` means not in
    /// entry cache.
    pub fn take_entries(&self) -> (Vec<Entry>, usize) {
        mem::take(&mut *self.entries.lock().unwrap())
    }

    #[cfg(test)]
    pub fn has_entries(&self) -> bool {
        !self.entries.lock().unwrap().0.is_empty()
    }
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
        // Cache either is empty or contains latest log. Hence we don't need to fetch
        // log from rocksdb anymore.
        assert!(end_idx == limit_idx || fetched_size > max_size);
        let (first, second) = tikv_util::slices_in_range(&self.cache, start_idx, end_idx);
        ents.extend_from_slice(first);
        ents.extend_from_slice(second);
    }

    fn append(&mut self, region_id: u64, peer_id: u64, entries: &[Entry]) {
        if !entries.is_empty() {
            let mut mem_size_change = 0;
            let old_capacity = self.cache.capacity();
            mem_size_change += self.append_impl(region_id, peer_id, entries);
            let new_capacity = self.cache.capacity();
            mem_size_change += Self::cache_vec_mem_size_change(new_capacity, old_capacity);
            mem_size_change += self.shrink_if_necessary();
            self.flush_mem_size_change(mem_size_change);
        }
    }

    /// Push entries to the left of the cache.
    ///
    /// When cache is not empty, the index of the last entry in entries
    /// should be equal to `cache first index - 1`. When cache is
    /// empty, it should be equal to the store's last index. Otherwise,
    /// append new entries may fail due to unexpected hole.
    fn prepend(&mut self, entries: Vec<Entry>) {
        let mut mem_size_change = 0;
        let old_capacity = self.cache.capacity();
        for e in entries.into_iter().rev() {
            mem_size_change += (bytes_capacity(&e.data) + bytes_capacity(&e.context)) as i64;
            self.cache.push_front(e);
        }
        let new_capacity = self.cache.capacity();
        mem_size_change += Self::cache_vec_mem_size_change(new_capacity, old_capacity);
        mem_size_change += self.shrink_if_necessary();
        self.flush_mem_size_change(mem_size_change);
    }

    fn append_impl(&mut self, region_id: u64, peer_id: u64, entries: &[Entry]) -> i64 {
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
                    "[region {}] {} unexpected hole: {} < {}",
                    region_id, peer_id, cache_last_index, first_index
                );
            }
        }

        for e in entries {
            self.cache.push_back(e.to_owned());
            mem_size_change += (bytes_capacity(&e.data) + bytes_capacity(&e.context)) as i64;
        }
        // In the past, the entry cache will be truncated if its size exceeds a certain
        // number. However, after introducing async write io, the entry must stay in
        // cache if it's not persisted to raft db because the raft-rs may need to read
        // entries.(e.g. leader sends MsgAppend to followers)

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

        // Clean cached entries which have been persisted. For example, if entries
        // [1, 10), [10, 20), [20, 30) are sent to apply threads and `compact_to(15)`
        // is called, both [10, 20), [20, 30) will still be kept in cache.
        let old_trace_cap = self.trace.capacity();
        while let Some(cached_entries) = self.trace.pop_front() {
            // Do not evict cached entries if not all of them are persisted.
            // After PR #16626, it is possible that applying entries are not
            // yet fully persisted. Therefore, it should not free these
            // entries until they are completely persisted.
            if cached_entries.range.end > idx {
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
        }
        let new_trace_cap = self.trace.capacity();
        mem_size_change += Self::trace_vec_mem_size_change(new_trace_cap, old_trace_cap);

        let cache_first_idx = self.first_index().unwrap_or(u64::MAX);
        if cache_first_idx >= idx {
            self.flush_mem_size_change(mem_size_change);
            assert!(mem_size_change <= 0);
            return -mem_size_change as u64;
        }

        let cache_last_idx = self.cache.back().unwrap().get_index();
        // Use `cache_last_idx + 1` to make sure cache can be cleared completely if
        // necessary.
        let compact_to = (cmp::min(cache_last_idx + 1, idx) - cache_first_idx) as usize;
        for e in self.cache.drain(..compact_to) {
            mem_size_change -= (bytes_capacity(&e.data) + bytes_capacity(&e.context)) as i64
        }

        mem_size_change += self.shrink_if_necessary();
        self.flush_mem_size_change(mem_size_change);
        assert!(mem_size_change <= 0);
        -mem_size_change as u64
    }

    fn total_mem_size(&self) -> i64 {
        let data_size: i64 = self
            .cache
            .iter()
            .map(|e| (bytes_capacity(&e.data) + bytes_capacity(&e.context)) as i64)
            .sum();
        let cache_vec_size = Self::cache_vec_mem_size_change(self.cache.capacity(), 0);
        let trace_vec_size = Self::trace_vec_mem_size_change(self.trace.capacity(), 0);
        data_size + cache_vec_size + trace_vec_size
    }

    fn cache_vec_mem_size_change(new_capacity: usize, old_capacity: usize) -> i64 {
        ENTRY_MEM_SIZE as i64 * (new_capacity as i64 - old_capacity as i64)
    }

    fn trace_vec_mem_size_change(new_capacity: usize, old_capacity: usize) -> i64 {
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
        let diff = Self::trace_vec_mem_size_change(new_capacity, old_capacity);

        self.flush_mem_size_change(diff + dangle_size as i64);
    }

    fn shrink_if_necessary(&mut self) -> i64 {
        if self.cache.len() < SHRINK_CACHE_CAPACITY && self.cache.capacity() > SHRINK_CACHE_CAPACITY
        {
            let old_capacity = self.cache.capacity();
            self.cache.shrink_to_fit();
            let new_capacity = self.cache.capacity();
            return Self::cache_vec_mem_size_change(new_capacity, old_capacity);
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
        entry_cache.flush_mem_size_change(entry_cache.total_mem_size());
        entry_cache
    }
}

impl Drop for EntryCache {
    fn drop(&mut self) {
        let mem_size_change = self.total_mem_size();
        self.flush_mem_size_change(-mem_size_change);
        self.flush_stats();
    }
}

#[derive(Debug)]
pub enum RaftlogFetchState {
    // The Instant records the start time of the fetching.
    Fetching(Instant),
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

fn validate_states<ER: RaftEngine>(
    region_id: u64,
    raft_engine: &ER,
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
        let entry = raft_engine.get_entry(region_id, recorded_commit_index)?;
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
    if apply_state.get_applied_index() > commit_index {
        info!("applied index is larger than recorded commit index"; "apply" => apply_state.get_applied_index(), "commit" => commit_index);
    }
    // Invariant: max(commit index, recorded commit index) <= last index
    if commit_index > last_index {
        return Err(box_err!(
            "max(commit index, recorded commit index) > last index, {}",
            state_str()
        ));
    }
    // Since the entries must be persisted before applying, the term of raft state
    // should also be persisted. So it should be greater than the commit term of
    // apply state.
    if raft_state.get_hard_state().get_term() < apply_state.get_commit_term() {
        return Err(box_err!(
            "term of raft state < commit term of apply state, {}",
            state_str()
        ));
    }

    raft_state.mut_hard_state().set_commit(commit_index);

    Ok(())
}

pub fn init_last_term<ER: RaftEngine>(
    raft_engine: &ER,
    region: &metapb::Region,
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
    let entry = raft_engine.get_entry(region.get_id(), last_idx)?;
    match entry {
        None => Err(box_err!(
            "[region {}] entry at {} doesn't exist, may lose data.",
            region.get_id(),
            last_idx
        )),
        Some(e) => Ok(e.get_term()),
    }
}

pub fn init_applied_term<ER: RaftEngine>(
    raft_engine: &ER,
    region: &metapb::Region,
    raft_state: &RaftLocalState,
    apply_state: &RaftApplyState,
) -> Result<u64> {
    if apply_state.applied_index == RAFT_INIT_LOG_INDEX {
        return Ok(RAFT_INIT_LOG_TERM);
    }
    let truncated_state = apply_state.get_truncated_state();
    if apply_state.applied_index == truncated_state.get_index() {
        return Ok(truncated_state.get_term());
    }

    // Applied index > last index means that some committed entries have applied but
    // not persisted, in this case, the raft term must not be changed, so we use the
    // term persisted in apply_state.
    if apply_state.applied_index > raft_state.get_last_index() {
        return Ok(apply_state.commit_term);
    }

    match raft_engine.get_entry(region.get_id(), apply_state.applied_index)? {
        Some(e) => Ok(e.term),
        None => Err(box_err!(
            "[region {}] entry at apply index {} doesn't exist, may lose data.",
            region.get_id(),
            apply_state.applied_index
        )),
    }
}

/// When a peer(follower) receives a TransferLeaderMsg, it enters the
/// CacheWarmupState. When the peer becomes leader or it doesn't
/// become leader before a deadline, it exits the state.
#[derive(Clone, Debug)]
pub struct CacheWarmupState {
    range: (u64, u64),
    is_task_timeout: bool,
    is_stale: bool,
    started_at: Instant,
}

impl CacheWarmupState {
    pub fn new() -> Self {
        CacheWarmupState::new_with_range(INVALID_INDEX, INVALID_INDEX)
    }

    pub fn new_with_range(low: u64, high: u64) -> Self {
        CacheWarmupState {
            range: (low, high),
            is_task_timeout: false,
            is_stale: false,
            started_at: Instant::now(),
        }
    }

    pub fn range(&self) -> (u64, u64) {
        self.range
    }

    /// How long has it been in this state.
    pub fn elapsed(&self) -> Duration {
        self.started_at.saturating_elapsed()
    }

    /// Whether the warmup task is already timeout.
    pub fn is_task_timeout(&self) -> bool {
        self.is_task_timeout
    }

    /// Check whether the task is timeout.
    pub fn check_task_timeout(&mut self, duration: Duration) -> bool {
        if self.is_task_timeout {
            return true;
        }
        if self.elapsed() > duration {
            WARM_UP_ENTRY_CACHE_COUNTER.timeout.inc();
            self.is_task_timeout = true;
        }
        self.is_task_timeout
    }

    /// Check whether this state is stale.
    pub fn check_stale(&mut self, duration: Duration) -> bool {
        fail_point!("entry_cache_warmed_up_state_is_stale", |_| true);
        if self.is_stale {
            return true;
        }
        if self.elapsed() > duration {
            self.is_stale = true;
        }
        self.is_stale
    }
}

impl Default for CacheWarmupState {
    fn default() -> Self {
        Self::new()
    }
}

/// A subset of `PeerStorage` that focus on accessing log entries.
pub struct EntryStorage<EK: KvEngine, ER> {
    region_id: u64,
    peer_id: u64,
    raft_engine: ER,
    cache: EntryCache,
    raft_state: RaftLocalState,
    apply_state: RaftApplyState,
    last_term: u64,
    applied_term: u64,
    read_scheduler: Scheduler<ReadTask<EK>>,
    raftlog_fetch_stats: AsyncFetchStats,
    async_fetch_results: RefCell<HashMap<u64, RaftlogFetchState>>,
    cache_warmup_state: Option<CacheWarmupState>,
}

impl<EK: KvEngine, ER: RaftEngine> EntryStorage<EK, ER> {
    pub fn new(
        peer_id: u64,
        raft_engine: ER,
        mut raft_state: RaftLocalState,
        apply_state: RaftApplyState,
        region: &metapb::Region,
        read_scheduler: Scheduler<ReadTask<EK>>,
    ) -> Result<Self> {
        if let Err(e) = validate_states(region.id, &raft_engine, &mut raft_state, &apply_state) {
            return Err(box_err!(
                "[region {}] {} validate state fail: {:?}",
                region.id,
                peer_id,
                e
            ));
        }
        let last_term = init_last_term(&raft_engine, region, &raft_state, &apply_state)?;
        let applied_term = init_applied_term(&raft_engine, region, &raft_state, &apply_state)?;
        Ok(Self {
            region_id: region.id,
            peer_id,
            raft_engine,
            cache: EntryCache::default(),
            raft_state,
            apply_state,
            last_term,
            applied_term,
            read_scheduler,
            raftlog_fetch_stats: AsyncFetchStats::default(),
            async_fetch_results: RefCell::new(HashMap::default()),
            cache_warmup_state: None,
        })
    }

    fn check_range(&self, low: u64, high: u64) -> raft::Result<()> {
        if low > high {
            return Err(storage_error(format!(
                "low: {} is greater that high: {}",
                low, high
            )));
        } else if low <= self.truncated_index() {
            return Err(raft::Error::Store(StorageError::Compacted));
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
        if let Some(RaftlogFetchState::Fetching(_)) = self.async_fetch_results.borrow().get(&low) {
            if res.is_none() {
                return;
            }
        }

        match res {
            Some(res) => {
                match self
                    .async_fetch_results
                    .borrow_mut()
                    .insert(low, RaftlogFetchState::Fetched(res))
                {
                    Some(RaftlogFetchState::Fetching(start)) => {
                        RAFT_ENTRY_FETCHES_TASK_DURATION_HISTOGRAM
                            .observe(start.saturating_elapsed_secs());
                    }
                    Some(RaftlogFetchState::Fetched(prev)) => {
                        info!(
                            "unconsumed async fetch res";
                            "region_id" => self.region_id,
                            "peer_id" => self.peer_id,
                            "res" => ?prev,
                            "low" => low,
                        );
                    }
                    _ => {
                        warn!(
                            "unknown async fetch res";
                            "region_id" => self.region_id,
                            "peer_id" => self.peer_id,
                            "low" => low,
                        );
                    }
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
        if let Some(RaftlogFetchState::Fetching(_)) = self.async_fetch_results.borrow().get(&low) {
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

                // the count of left entries isn't too large, fetch the remaining entries
                // synchronously one by one
                for idx in last + 1..high {
                    let ent = self.raft_engine.get_entry(region_id, idx)?;
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
                "region_id" => self.region_id,
                "peer_id" => self.peer_id,
                "first" => first,
                "last" => last,
                "low" => low,
                "high" => high,
                "max_size" => max_size,
                "res_max_size" => res.max_size,
            );
            // low index or max size is changed, the result is not fit for the current
            // range, so refetch again.
            self.raftlog_fetch_stats.fetch_invalid.update(|m| m + 1);
            res.tried_cnt + 1
        } else {
            1
        };

        // the first/second try: get [low, high) asynchronously
        // the third try:
        //  - if term and low are matched: use result of [low, persisted) and get
        //    [persisted, high) synchronously
        //  - else: get [low, high) synchronously
        if tried_cnt >= MAX_ASYNC_FETCH_TRY_CNT {
            // even the larger range is invalid again, fallback to fetch in sync way
            self.raftlog_fetch_stats.fallback_fetch.update(|m| m + 1);
            let count = self.raft_engine.fetch_entries_to(
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
            .insert(low, RaftlogFetchState::Fetching(Instant::now_coarse()));
        self.read_scheduler
            .schedule(ReadTask::FetchLogs {
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
        let cache_low = self.cache.first_index().unwrap_or(u64::MAX);
        if high <= cache_low {
            self.cache.miss.update(|m| m + 1);
            return if context.can_async() {
                self.async_fetch(self.region_id, low, high, max_size, context, &mut ents)?;
                Ok(ents)
            } else {
                self.raftlog_fetch_stats.sync_fetch.update(|m| m + 1);
                self.raft_engine.fetch_entries_to(
                    self.region_id,
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
                self.async_fetch(self.region_id, low, cache_low, max_size, context, &mut ents)?
            } else {
                self.raftlog_fetch_stats.sync_fetch.update(|m| m + 1);
                self.raft_engine.fetch_entries_to(
                    self.region_id,
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
                .raft_engine
                .get_entry(self.region_id, idx)
                .unwrap()
                .unwrap_or_else(|| {
                    panic!(
                        "region_id={}, peer_id={}, idx={idx}",
                        self.region_id, self.peer_id
                    )
                })
                .get_term())
        }
    }

    #[inline]
    pub fn set_truncated_index(&mut self, index: u64) {
        self.apply_state.mut_truncated_state().set_index(index)
    }

    #[inline]
    pub fn set_truncated_term(&mut self, term: u64) {
        self.apply_state.mut_truncated_state().set_term(term)
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
    pub fn set_last_term(&mut self, term: u64) {
        self.last_term = term;
    }

    #[inline]
    pub fn set_applied_term(&mut self, applied_term: u64) {
        self.applied_term = applied_term;
    }

    #[inline]
    pub fn applied_term(&self) -> u64 {
        self.applied_term
    }

    #[inline]
    pub fn raft_state(&self) -> &RaftLocalState {
        &self.raft_state
    }

    #[inline]
    pub fn raft_state_mut(&mut self) -> &mut RaftLocalState {
        &mut self.raft_state
    }

    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.apply_state.get_applied_index()
    }

    #[inline]
    pub fn set_apply_state(&mut self, apply_state: RaftApplyState) {
        self.apply_state = apply_state;
    }

    #[inline]
    pub fn apply_state(&self) -> &RaftApplyState {
        &self.apply_state
    }

    #[inline]
    pub fn apply_state_mut(&mut self) -> &mut RaftApplyState {
        &mut self.apply_state
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

    // Append the given entries to the raft log using previous last index or
    // self.last_index.
    pub fn append(&mut self, entries: Vec<Entry>, task: &mut WriteTask<EK, ER>) {
        if entries.is_empty() {
            return;
        }
        debug!(
            "append entries";
            "region_id" => self.region_id,
            "peer_id" => self.peer_id,
            "count" => entries.len(),
        );
        let prev_last_index = self.raft_state.get_last_index();

        let (last_index, last_term) = {
            let e = entries.last().unwrap();
            (e.get_index(), e.get_term())
        };

        self.cache.append(self.region_id, self.peer_id, &entries);

        // Delete any previously appended log entries which never committed.
        task.set_append(Some(prev_last_index + 1), entries);

        self.raft_state.set_last_index(last_index);
        self.last_term = last_term;
    }

    pub fn entry_cache_warmup_state(&self) -> &Option<CacheWarmupState> {
        &self.cache_warmup_state
    }

    pub fn entry_cache_warmup_state_mut(&mut self) -> &mut Option<CacheWarmupState> {
        &mut self.cache_warmup_state
    }

    pub fn clear_entry_cache_warmup_state(&mut self) {
        self.cache_warmup_state = None;
    }

    /// Trigger a task to warm up the entry cache.
    ///
    /// This will ensure the range [low..=last_index] are loaded into
    /// cache. Return the high index of the warmup range if a task is
    /// successfully triggered.
    pub fn async_warm_up_entry_cache(&mut self, low: u64) -> Option<u64> {
        let high = if let Some(first_index) = self.entry_cache_first_index() {
            if low >= first_index {
                // Already warmed up.
                self.cache_warmup_state = Some(CacheWarmupState::new());
                return None;
            }
            // Partially warmed up.
            first_index
        } else {
            self.last_index() + 1
        };

        // Fetch entries [low, high) to trigger an async fetch task in background.
        self.cache_warmup_state = Some(CacheWarmupState::new_with_range(low, high));
        match self.entries(low, high, u64::MAX, GetEntriesContext::empty(true)) {
            Ok(_) => {
                // This should not happen, but it's OK :)
                debug_assert!(false, "entries should not have been fetched");
                error!("entries are fetched unexpectedly during warming up");
                None
            }
            Err(raft::Error::Store(raft::StorageError::LogTemporarilyUnavailable)) => {
                WARM_UP_ENTRY_CACHE_COUNTER.started.inc();
                Some(high)
            }
            Err(e) => {
                error!(
                    "fetching entries met unexpected error during warming up";
                    "err" => ?e,
                );
                None
            }
        }
    }

    /// Warm up entry cache if the result is valid.
    ///
    /// Return true when the warmup operation succeed within the timeout.
    pub fn maybe_warm_up_entry_cache(&mut self, res: RaftlogFetchResult) -> bool {
        let low = res.low;
        // Warm up the entry cache if the low and high index are
        // exactly the same as the warmup range.
        let state = self.entry_cache_warmup_state().as_ref().unwrap();
        let range = state.range();
        let is_task_timeout = state.is_task_timeout();

        if range.0 != low {
            return false;
        }

        match res.ents {
            Ok(mut entries) => {
                let last_entry_index = entries.last().map(|e| e.index);
                if let Some(index) = last_entry_index {
                    // Generally speaking, when the res.low is the same as the warmup
                    // range start, the fetch result is exactly used for warmup.
                    // As the low index of each async_fetch task is different.
                    // There should exist only one exception. A async fetch task
                    // with same low index is triggered before the warmup task.
                    if index + 1 >= range.1 {
                        let is_valid = if let Some(first_index) = self.entry_cache_first_index() {
                            range.1 == first_index
                        } else {
                            range.1 == self.last_index() + 1
                        };
                        // FIXME: the assertion below doesn't hold.
                        // assert!(is_valid, "the warmup range should still be valid");
                        if !is_valid {
                            error!(
                                "unexpected warmup state";
                                "region_id" => self.region_id,
                                "peer_id" => self.peer_id,
                                "cache_first" => ?self.entry_cache_first_index(),
                                "last_index" => self.last_index(),
                                "warmup_state_high" => range.1,
                                "last_entry_index" => index,
                            );
                            return false;
                        }
                        entries.truncate((range.1 - range.0) as usize);
                        self.cache.prepend(entries);
                        WARM_UP_ENTRY_CACHE_COUNTER.finished.inc();
                        fail_point!("on_entry_cache_warmed_up");
                        return !is_task_timeout;
                    }
                }
                warn!(
                    "warm up the entry cache failed";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer_id,
                    "last_entry_index" => last_entry_index.unwrap_or(0),
                    "expected_high" => range.1,
                );
            }
            Err(e) => {
                warn!(
                    "warm up the entry cache failed";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer_id,
                    "err" => ?e,
                );
            }
        }
        false
    }

    pub fn compact_entry_cache(&mut self, idx: u64) {
        let mut can_compact = true;
        if let Some(state) = self.entry_cache_warmup_state_mut() {
            if state.check_stale(MAX_WARMED_UP_CACHE_KEEP_TIME) {
                self.clear_entry_cache_warmup_state();
            } else {
                can_compact = false;
            }
        }
        if can_compact {
            self.cache.compact_to(idx);
        }
    }

    #[inline]
    pub fn is_entry_cache_empty(&self) -> bool {
        self.cache.is_empty()
    }

    #[inline]
    pub fn entry_cache_first_index(&self) -> Option<u64> {
        self.cache.first_index()
    }

    /// Evict entries from the cache.
    pub fn evict_entry_cache(&mut self, half: bool) {
        if !self.is_entry_cache_empty() {
            let cache = &mut self.cache;
            let cache_len = cache.cache.len();
            let drain_to = if half { cache_len / 2 } else { cache_len - 1 };
            let idx = cache.cache[drain_to].index;
            let mem_size_change = cache.compact_to(idx + 1);
            RAFT_ENTRIES_EVICT_BYTES.inc_by(mem_size_change);
        } else if !half {
            let cache = &mut self.cache;
            let mem_size_change = cache.compact_to(u64::MAX);
            RAFT_ENTRIES_EVICT_BYTES.inc_by(mem_size_change);
        }
    }

    #[inline]
    pub fn flush_entry_cache_metrics(&mut self) {
        // NOTE: memory usage of entry cache is flushed realtime.
        self.cache.flush_stats();
        self.raftlog_fetch_stats.flush_stats();
    }

    pub fn raft_engine(&self) -> &ER {
        &self.raft_engine
    }

    pub fn update_cache_persisted(&mut self, persisted: u64) {
        self.cache.update_persisted(persisted);
    }

    pub fn trace_cached_entries(&mut self, entries: CachedEntries) {
        self.cache.trace_cached_entries(entries);
    }

    pub fn clear(&mut self) {
        self.cache = EntryCache::default();
    }

    pub fn read_scheduler(&self) -> Scheduler<ReadTask<EK>> {
        self.read_scheduler.clone()
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::mpsc;

    use engine_test::{kv::KvTestEngine, raft::RaftTestEngine};
    use engine_traits::RaftEngineReadOnly;
    use protobuf::Message;
    use raft::{GetEntriesContext, StorageError};
    use tempfile::Builder;
    use tikv_util::worker::{dummy_scheduler, LazyWorker, Worker};

    use super::*;
    use crate::store::peer_storage::tests::{append_ents, new_entry, new_storage_from_ents};

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
            entry_cache.flush_mem_size_change(entry_cache.total_mem_size());
            entry_cache
        }
    }

    pub fn validate_cache(store: &EntryStorage<KvTestEngine, RaftTestEngine>, exp_ents: &[Entry]) {
        assert_eq!(store.cache.cache, exp_ents);
        for e in exp_ents {
            let entry = store
                .raft_engine
                .get_entry(store.region_id, e.get_index())
                .unwrap()
                .unwrap();
            assert_eq!(entry, *e);
        }
    }

    #[test]
    fn test_storage_cache_size_change() {
        let new_padded_entry = |index: u64, term: u64, pad_len: usize| {
            let mut e = new_entry(index, term);
            e.data = vec![b'x'; pad_len].into();
            e
        };

        // Test the initial data structure size.
        let (tx, rx) = mpsc::sync_channel(1);
        let check_mem_size_change = |expect: i64| {
            assert_eq!(rx.try_recv().unwrap(), expect);
            rx.try_recv().unwrap_err();
        };
        let mut cache = EntryCache::new_with_cb(move |c: i64| tx.send(c).unwrap());
        check_mem_size_change(0);

        cache.append(
            0,
            0,
            &[new_padded_entry(101, 1, 1), new_padded_entry(102, 1, 2)],
        );
        check_mem_size_change(419);

        cache.prepend(vec![new_padded_entry(100, 1, 1)]);
        check_mem_size_change(1);
        cache.persisted = 100;
        cache.compact_to(101);
        check_mem_size_change(-1);

        // Test size change for one overlapped entry.
        cache.append(0, 0, &[new_padded_entry(102, 2, 3)]);
        check_mem_size_change(1);

        // Test size change for all overlapped entries.
        cache.append(
            0,
            0,
            &[new_padded_entry(101, 3, 4), new_padded_entry(102, 3, 5)],
        );
        check_mem_size_change(5);

        cache.append(0, 0, &[new_padded_entry(103, 3, 6)]);
        check_mem_size_change(6);

        // Test trace a dangle entry.
        let cached_entries = CachedEntries::new(vec![new_padded_entry(100, 1, 1)]);
        cache.trace_cached_entries(cached_entries);
        check_mem_size_change(97);

        // Test trace an entry which is still in cache.
        let cached_entries = CachedEntries::new(vec![new_padded_entry(102, 3, 5)]);
        cache.trace_cached_entries(cached_entries);
        check_mem_size_change(0);

        // Test compare `cached_last` with `trunc_to_idx` in `EntryCache::append_impl`.
        cache.append(0, 0, &[new_padded_entry(103, 4, 7)]);
        check_mem_size_change(1);

        // Test compact one traced dangle entry and one entry in cache.
        cache.persisted = 101;
        cache.compact_to(102);
        check_mem_size_change(-5);

        // Test compact the last traced dangle entry.
        cache.persisted = 102;
        cache.compact_to(103);
        check_mem_size_change(-5);

        // Test compact all entries.
        cache.persisted = 103;
        cache.compact_to(104);
        check_mem_size_change(-7);

        drop(cache);
        check_mem_size_change(-512);
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
        cache.append(0, 0, &ents);
        assert!(cache.entry(1).is_none());
        assert!(cache.entry(2).is_none());
        for e in &ents {
            assert_eq!(e, cache.entry(e.get_index()).unwrap());
        }
        let res = panic_hook::recover_safe(|| cache.entry(7));
        res.unwrap_err();
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
                    ents: Err(raft::Error::Store(StorageError::Compacted)),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Err(raft::Error::Store(StorageError::Compacted)),
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
                Err(raft::Error::Store(StorageError::LogTemporarilyUnavailable)),
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
                    ents: Err(raft::Error::Store(StorageError::Compacted)),
                    low: 3,
                    max_size: max_u64,
                    hit_size_limit: false,
                    tried_cnt: 1,
                    term: 1,
                },
                Err(raft::Error::Store(StorageError::LogTemporarilyUnavailable)),
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
                Err(raft::Error::Store(StorageError::LogTemporarilyUnavailable)),
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
            let li = store.last_index().unwrap();
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
        let mut size: u64 = ents[1..].iter().map(|e| u64::from(e.compute_size())).sum();
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
        store.cache.prepend(vec![new_entry(6, 5)]);

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
        store.compact_entry_cache(7);
        exp_res = vec![new_entry(6, 7), new_entry(7, 8)];
        validate_cache(&store, &exp_res);

        // compact to min(7 + 1, 7)
        store.cache.persisted = 7;
        store.compact_entry_cache(7);
        exp_res = vec![new_entry(7, 8)];
        validate_cache(&store, &exp_res);
        // compact all
        store.compact_entry_cache(8);
        validate_cache(&store, &[]);
        // invalid compaction should be ignored.
        store.compact_entry_cache(6);
    }

    #[test]
    fn test_async_warm_up_entry_cache() {
        let ents = vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];

        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let region_worker = Worker::new("snap-manager").lazy_build("snap-manager");
        let region_scheduler = region_worker.scheduler();
        let (dummy_scheduler, _rx) = dummy_scheduler();

        let mut store = new_storage_from_ents(region_scheduler, dummy_scheduler, &td, &ents);
        store.cache.compact_to(6);
        assert_eq!(store.entry_cache_first_index().unwrap(), 6);

        // The return value should be None when it is already warmed up.
        assert!(store.async_warm_up_entry_cache(6).is_none());

        // The high index should be equal to the entry_cache_first_index.
        assert_eq!(store.async_warm_up_entry_cache(5).unwrap(), 6);

        store.cache.compact_to(7); // Clean cache.
        // The high index should be equal to the last_index + 1.
        assert_eq!(store.async_warm_up_entry_cache(5).unwrap(), 7);
    }

    #[test]
    fn test_warmup_entry_cache() {
        let ents = vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];

        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let region_worker = Worker::new("snap-manager").lazy_build("snap-manager");
        let region_scheduler = region_worker.scheduler();
        let (dummy_scheduler, _rx) = dummy_scheduler();
        let mut store = new_storage_from_ents(region_scheduler, dummy_scheduler, &td, &ents);
        store.cache.compact_to(6);
        store.cache_warmup_state = Some(CacheWarmupState::new_with_range(5, 6));

        let res = RaftlogFetchResult {
            ents: Ok(ents[1..3].to_vec()),
            low: 5,
            max_size: u64::MAX,
            hit_size_limit: false,
            tried_cnt: MAX_ASYNC_FETCH_TRY_CNT,
            term: 1,
        };
        store.maybe_warm_up_entry_cache(res);
        // Cache should be warmed up.
        assert_eq!(store.entry_cache_first_index().unwrap(), 5);
    }

    #[test]
    fn test_evict_cached_entries() {
        let ents = vec![new_entry(3, 3)];
        let td = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let worker = LazyWorker::new("snap-manager");
        let sched = worker.scheduler();
        let (dummy_scheduler, _) = dummy_scheduler();
        let mut store = new_storage_from_ents(sched, dummy_scheduler, &td, &ents);

        // initial cache
        for i in 4..10 {
            append_ents(&mut store, &[new_entry(i, 4)]);
        }

        let cached_entries = vec![
            CachedEntries::new(vec![new_entry(4, 4)]),
            CachedEntries::new(vec![new_entry(5, 4)]),
            CachedEntries::new(vec![new_entry(6, 4), new_entry(7, 4), new_entry(8, 4)]),
            CachedEntries::new(vec![new_entry(9, 4)]),
        ];
        for ents in &cached_entries {
            store.trace_cached_entries(ents.clone());
        }
        assert_eq!(store.cache.first_index().unwrap(), 4);

        store.evict_entry_cache(false);
        assert_eq!(store.cache.first_index().unwrap(), 4);
        assert!(cached_entries[0].has_entries());

        store.cache.persisted = 4;
        store.evict_entry_cache(false);
        assert_eq!(store.cache.first_index().unwrap(), 5);
        assert!(!cached_entries[0].has_entries());
        assert!(cached_entries[1].has_entries());

        store.cache.persisted = 5;
        store.evict_entry_cache(false);
        assert_eq!(store.cache.first_index().unwrap(), 6);
        assert!(!cached_entries[1].has_entries());
        assert!(cached_entries[2].has_entries());

        for idx in [6, 7] {
            store.cache.persisted = idx;
            store.evict_entry_cache(false);
            assert_eq!(store.cache.first_index().unwrap(), idx + 1);
            assert!(cached_entries[2].has_entries());
        }

        store.cache.persisted = 8;
        store.evict_entry_cache(false);
        assert_eq!(store.cache.first_index().unwrap(), 9);
        assert!(!cached_entries[2].has_entries());

        store.cache.persisted = 9;
        store.evict_entry_cache(false);
        assert!(store.cache.first_index().is_none());
        assert!(!cached_entries[3].has_entries());
    }
}
