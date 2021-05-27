// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use std::sync::Arc;
use tikv_alloc::{
    mem_trace,
    trace::{Id, MemoryTrace, MemoryTraceNode},
};

lazy_static! {
    pub static ref MEMTRACE_ROOT: Arc<MemoryTraceNode> = mem_trace!(
        raftstore,
        [(raft_router, [alive, leak]), (apply_router, [alive, leak])]
    );
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
