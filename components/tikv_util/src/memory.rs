// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{
    kvrpcpb::LockInfo,
    metapb::{Peer, Region, RegionEpoch},
    raft_cmdpb::{self, RaftCmdRequest, ReadIndexRequest},
};
use std::mem;

pub trait HeapSize {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for Region {
    fn heap_size(&self) -> usize {
        let mut size = self.start_key.capacity() + self.end_key.capacity();
        size += mem::size_of::<RegionEpoch>();
        size += self.peers.capacity() * mem::size_of::<Peer>();
        size
    }
}

impl HeapSize for ReadIndexRequest {
    fn heap_size(&self) -> usize {
        self.key_ranges
            .iter()
            .map(|r| r.start_key.capacity() + r.end_key.capacity())
            .sum()
    }
}

impl HeapSize for LockInfo {
    fn heap_size(&self) -> usize {
        self.primary_lock.capacity()
            + self.key.capacity()
            + self.secondaries.iter().map(|k| k.len()).sum::<usize>()
    }
}

impl HeapSize for RaftCmdRequest {
    fn heap_size(&self) -> usize {
        mem::size_of::<raft_cmdpb::RaftRequestHeader>()
            + self.requests.capacity() * mem::size_of::<raft_cmdpb::Request>()
            + mem::size_of_val(&self.admin_request)
            + mem::size_of_val(&self.status_request)
    }
}
