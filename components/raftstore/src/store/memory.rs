// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use memory_trace_macros::MemoryTraceHelper;
use std::sync::Arc;
use tikv_alloc::{mem_trace, trace::MemoryTraceNode};

lazy_static! {
    pub static ref RAFTSTORE_MEM_TRACE: Arc<MemoryTraceNode> = mem_trace!(
        raftstore,
        [(raft_router, [alive, leak]), (apply_router, [alive, leak])]
    );
}

#[derive(MemoryTraceHelper, Default)]
pub struct RaftContextTrace {
    pub write_batch: usize,
    pub rest: usize,
}

#[derive(MemoryTraceHelper, Default)]
pub struct ApplyContextTrace {
    pub cbs_size: usize,
    pub rest: usize,
}
