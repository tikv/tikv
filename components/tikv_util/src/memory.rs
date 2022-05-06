// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use kvproto::{
    encryptionpb::EncryptionMeta,
    kvrpcpb::LockInfo,
    metapb::{Peer, Region, RegionEpoch},
    raft_cmdpb::{self, RaftCmdRequest, ReadIndexRequest},
};

/// Transmute vec from one type to the other type.
///
/// # Safety
///
/// The two types should be with same memory layout.
#[inline]
pub unsafe fn vec_transmute<F, T>(from: Vec<F>) -> Vec<T> {
    debug_assert!(mem::size_of::<F>() == mem::size_of::<T>());
    debug_assert!(mem::align_of::<F>() == mem::align_of::<T>());
    let (ptr, len, cap) = from.into_raw_parts();
    Vec::from_raw_parts(ptr as _, len, cap)
}

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
        // There is still a `bytes` in `EncryptionMeta`. Ignore it becaure it could be shared.
        size += mem::size_of::<EncryptionMeta>();
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
