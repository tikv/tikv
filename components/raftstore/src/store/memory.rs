// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use memory_trace_macros::MemoryTraceHelper;
use std::sync::Arc;
use tikv_alloc::{
    mem_trace,
    trace::{Id, MemoryTrace, MemoryTraceNode},
};

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
    pub static ref MEMTRACE_PEERS: Arc<dyn MemoryTrace + Send + Sync> =
        MEMTRACE_ROOT.sub_trace(Id::Name("peers"));
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

#[derive(MemoryTraceHelper, Default)]
pub struct PeerMemoryTrace {
    pub raft_machine: usize,
    pub proposals: usize,
    pub rest: usize,
}

#[derive(MemoryTraceHelper, Default, Debug)]
pub struct ApplyMemoryTrace {
    pub pending_cmds: usize,
    pub rest: usize,
}
