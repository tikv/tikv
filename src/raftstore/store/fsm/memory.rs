// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::store::StoreMeta;
use super::{ApplyFsm, PeerFsm};
use batch_system::RouterTrace;
use memory_trace_macros::MemoryTraceHelper;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use tikv_alloc::trace::{MemoryTrace, MemoryTraceProvider};
use tikv_util::collections::HashMap;
use tikv_util::memory::HeapSize;

#[derive(MemoryTraceHelper, Default)]
pub struct PeerMemoryTrace {
    pub log_cache: AtomicUsize,
    pub raft_machine: AtomicUsize,
    pub proposals: AtomicUsize,
    pub rest: AtomicUsize,
}

#[derive(MemoryTraceHelper, Default, Debug)]
pub struct ApplyMemoryTrace {
    pub pending_cmds: AtomicUsize,
    pub rest: AtomicUsize,
}

#[derive(MemoryTraceHelper, Default)]
pub struct RaftContextTrace {
    pub write_batch: AtomicUsize,
    pub rest: AtomicUsize,
}

#[derive(MemoryTraceHelper, Default)]
pub struct ApplyContextTrace {
    pub cbs_size: AtomicUsize,
    pub rest: AtomicUsize,
}

#[derive(Default)]
pub struct RaftStoreMemoryTrace {
    // TODO: Maybe slab is a better choice?
    pub peers: Mutex<HashMap<u64, Arc<PeerMemoryTrace>>>,
    pub applys: Mutex<HashMap<u64, Arc<ApplyMemoryTrace>>>,
    pub raft_context: Mutex<Vec<(ThreadId, Arc<RaftContextTrace>)>>,
    pub apply_context: Mutex<Vec<(ThreadId, Arc<ApplyContextTrace>)>>,
    pub raft_router: RouterTrace,
    pub apply_router: RouterTrace,
}

impl RaftStoreMemoryTrace {
    pub fn new(raft_router: RouterTrace, apply_router: RouterTrace) -> RaftStoreMemoryTrace {
        RaftStoreMemoryTrace {
            peers: Default::default(),
            applys: Default::default(),
            raft_context: Default::default(),
            apply_context: Default::default(),
            raft_router,
            apply_router,
        }
    }
}

pub struct RaftStoreMemoryProvider {
    meta: Arc<Mutex<StoreMeta>>,
    components: Arc<RaftStoreMemoryTrace>,
}

impl RaftStoreMemoryProvider {
    pub fn new(
        meta: Arc<Mutex<StoreMeta>>,
        components: Arc<RaftStoreMemoryTrace>,
    ) -> RaftStoreMemoryProvider {
        RaftStoreMemoryProvider { meta, components }
    }
}

impl MemoryTraceProvider for RaftStoreMemoryProvider {
    fn trace(&mut self, dump: &mut MemoryTrace) {
        let sub_trace = dump.add_sub_trace("raft store");
        let store_meta_trace = sub_trace.add_sub_trace("store meta");
        let mut size = std::mem::size_of::<Mutex<StoreMeta>>();
        let meta = self.meta.lock().unwrap();
        size += meta.region_ranges.heap_size()
            + meta.regions.heap_size()
            + meta.readers.heap_size()
            + meta.pending_votes.heap_size()
            + meta.pending_snapshot_regions.heap_size()
            + meta.pending_merge_targets.heap_size()
            + meta.targets_map.heap_size()
            + meta.mem_size;
        drop(meta);
        store_meta_trace.set_size(size);
        sub_trace.add_size(size);

        let peers = self.components.peers.lock().unwrap();
        let peer_trace = sub_trace.add_sub_trace_with_capacity("peers", peers.len());
        // Arbitrary engine is OK.
        size = mem::size_of::<PeerFsm>() * peers.len();
        for (id, p) in peers.iter() {
            size += p.trace(*id, peer_trace);
        }
        peer_trace.set_size(size);
        drop(peers);
        sub_trace.add_size(size);

        let applys = self.components.applys.lock().unwrap();
        // Arbitrary engine is OK.
        size = mem::size_of::<ApplyFsm>() * applys.len();
        let apply_trace = sub_trace.add_sub_trace_with_capacity("applys", applys.len());
        for (id, p) in applys.iter() {
            size += p.trace(*id, apply_trace);
        }
        apply_trace.set_size(size);
        drop(applys);
        sub_trace.add_size(size);

        let raft_context = self.components.raft_context.lock().unwrap();
        size = 0;
        let raft_context_trace =
            sub_trace.add_sub_trace_with_capacity("Raft Context", raft_context.len());
        for (id, (_, p)) in raft_context.iter().enumerate() {
            size += p.trace(id as u64, raft_context_trace);
        }
        raft_context_trace.set_size(size);
        drop(raft_context);
        sub_trace.add_size(size);

        let apply_context = self.components.apply_context.lock().unwrap();
        size = 0;
        let apply_context_trace =
            sub_trace.add_sub_trace_with_capacity("Apply Context", apply_context.len());
        for (id, (_, p)) in apply_context.iter().enumerate() {
            size += p.trace(id as u64, apply_context_trace);
        }
        apply_context_trace.set_size(size);
        drop(apply_context);
        sub_trace.add_size(size);

        size = self.components.raft_router.trace("Raft Router", sub_trace);
        sub_trace.add_size(size);
        size = self
            .components
            .apply_router
            .trace("Apply Router", sub_trace);
        sub_trace.add_size(size);

        size = sub_trace.size();
        dump.add_size(size);
    }
}
