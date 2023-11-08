// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::Arc;

use fail::fail_point;
use lazy_static::lazy_static;
use tikv_alloc::{
    mem_trace,
    trace::{Id, MemoryTrace},
};
use tikv_util::sys::memory_usage_reaches_high_water;

lazy_static! {
    pub static ref MEMTRACE_ROOT: Arc<MemoryTrace> = mem_trace!(
        raftstore,
        [
            peers,
            applys,
            entry_cache,
            (raft_router, [alive, leak]),
            (apply_router, [alive, leak]),
            raft_messages,
            raft_entries
        ]
    );
    /// Memory usage for raft peers fsms.
    pub static ref MEMTRACE_PEERS: Arc<MemoryTrace> =
        MEMTRACE_ROOT.sub_trace(Id::Name("peers"));

    /// Memory usage for apply fsms.
    pub static ref MEMTRACE_APPLYS: Arc<MemoryTrace> =
        MEMTRACE_ROOT.sub_trace(Id::Name("applys"));

    pub static ref MEMTRACE_ENTRY_CACHE: Arc<MemoryTrace> =
        MEMTRACE_ROOT.sub_trace(Id::Name("entry_cache"));

    pub static ref MEMTRACE_RAFT_ROUTER_ALIVE: Arc<MemoryTrace> = MEMTRACE_ROOT
        .sub_trace(Id::Name("raft_router"))
        .sub_trace(Id::Name("alive"));
    pub static ref MEMTRACE_RAFT_ROUTER_LEAK: Arc<MemoryTrace> = MEMTRACE_ROOT
        .sub_trace(Id::Name("raft_router"))
        .sub_trace(Id::Name("leak"));
    pub static ref MEMTRACE_APPLY_ROUTER_ALIVE: Arc<MemoryTrace> = MEMTRACE_ROOT
        .sub_trace(Id::Name("apply_router"))
        .sub_trace(Id::Name("alive"));
    pub static ref MEMTRACE_APPLY_ROUTER_LEAK: Arc<MemoryTrace> = MEMTRACE_ROOT
        .sub_trace(Id::Name("apply_router"))
        .sub_trace(Id::Name("leak"));

    /// Heap size trace for received raft messages.
    pub static ref MEMTRACE_RAFT_MESSAGES: Arc<MemoryTrace> =
        MEMTRACE_ROOT.sub_trace(Id::Name("raft_messages"));

    /// Heap size trace for appended raft entries.
    pub static ref MEMTRACE_RAFT_ENTRIES: Arc<MemoryTrace> =
        MEMTRACE_ROOT.sub_trace(Id::Name("raft_entries"));
}

pub fn needs_evict_entry_cache(evict_cache_on_memory_ratio: f64) -> bool {
    fail_point!("needs_evict_entry_cache", |_| true);
    if evict_cache_on_memory_ratio < f64::EPSILON {
        return false;
    }

    let mut usage = 0;
    if memory_usage_reaches_high_water(&mut usage) {
        let ec_usage = MEMTRACE_ENTRY_CACHE.sum() as u64;
        if ec_usage as f64 > usage as f64 * evict_cache_on_memory_ratio {
            return true;
        }
    }
    false
}
