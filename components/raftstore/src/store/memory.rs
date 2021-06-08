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
            raft_read_only,
            raft_progress,
            raft_entries,
            apply_pending_cmds,
            apply_merge_yield,
            entry_cache,
            (raft_router, [alive, leak]),
            (apply_router, [alive, leak])
        ]
    );


    /// Memory usage for `ReadOnly`s in all raft groups.
    pub static ref MEMTRACE_PEER_READ_ONLY: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("raft_read_only"));

    /// Memory usage for `Progress` in all raft groups.
    pub static ref MEMTRACE_PEER_PROGRESS: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("raft_progress"));

    /// Memory usage for unstale raft entries in all raft groups.
    pub static ref MEMTRACE_PEER_ENTRIES: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("raft_entries"));

    /// Memory usage for pending commands in all `ApplyFsm`.
    pub static ref MEMTRACE_APPLY_COMMANDS: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("apply_pending_cmds"));

    /// Memory usage for merge yield state in all `ApplyFsm`.
    pub static ref MEMTRACE_APPLY_YIELD: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("apply_merge_yield"));

    /// Memory usage for raft entry cache.
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
