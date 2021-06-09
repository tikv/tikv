// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use fail::fail_point;
use lazy_static::lazy_static;
use std::sync::Arc;
use tikv_alloc::{
    mem_trace,
    trace::{Id, MemoryTrace, MemoryTraceNode},
};
use tikv_util::config::GIB;
use tikv_util::sys::{get_global_memory_usage, memory_usage_reaches_high_water};

lazy_static! {
    pub static ref MEMTRACE_ROOT: Arc<MemoryTraceNode> = mem_trace!(
        raftstore,
        [
            peers,
            applys,
            entry_cache,
            (raft_router, [alive, leak]),
            (apply_router, [alive, leak])
        ]
    );
    /// Memory usage for raft peers fsms.
    pub static ref MEMTRACE_PEERS: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("peers"));

    /// Memory usage for apply fsms.
    pub static ref MEMTRACE_APPLYS: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("applys"));

    pub static ref MEMTRACE_ENTRY_CACHE: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("entry_cache"));

    pub static ref MEMTRACE_RAFT_ROUTER_ALIVE: Arc<dyn MemoryTrace + Send + Sync> = MEMTRACE_ROOT
        .sub_trace(Id::Name("raft_router"))
        .sub_trace(Id::Name("alive"));
    pub static ref MEMTRACE_RAFT_ROUTER_LEAK: Arc<dyn MemoryTrace + Send + Sync> = MEMTRACE_ROOT
        .sub_trace(Id::Name("raft_router"))
        .sub_trace(Id::Name("leak"));
    pub static ref MEMTRACE_APPLY_ROUTER_ALIVE: Arc<dyn MemoryTrace + Send + Sync> = MEMTRACE_ROOT
        .sub_trace(Id::Name("apply_router"))
        .sub_trace(Id::Name("alive"));
    pub static ref MEMTRACE_APPLY_ROUTER_LEAK: Arc<dyn MemoryTrace + Send + Sync> = MEMTRACE_ROOT
        .sub_trace(Id::Name("apply_router"))
        .sub_trace(Id::Name("leak"));
}

pub fn needs_evict_entry_cache() -> bool {
    fail_point!("needs_evict_entry_cache", |_| true);
    if memory_usage_reaches_high_water() {
        let usage = get_global_memory_usage();
        let ec_usage = MEMTRACE_ENTRY_CACHE.sum() as u64;
        // Evict cache if entry cache memory usage reaches 1/3 of global,
        // or 1GiB for small instances.
        if ec_usage > GIB && (ec_usage > usage / 3 || usage <= 3 * GIB) {
            return true;
        }
    }
    false
}
