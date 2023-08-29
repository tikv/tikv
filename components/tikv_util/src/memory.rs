// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    sync::atomic::{AtomicUsize, Ordering},
};

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

impl HeapSize for [u8] {
    fn heap_size(&self) -> usize {
        self.len() * mem::size_of::<u8>()
    }
}

impl HeapSize for Region {
    fn heap_size(&self) -> usize {
        let mut size = self.start_key.capacity() + self.end_key.capacity();
        size += mem::size_of::<RegionEpoch>();
        size += self.peers.capacity() * mem::size_of::<Peer>();
        // There is still a `bytes` in `EncryptionMeta`. Ignore it because it could be
        // shared.
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

pub struct MemoryQuota {
    capacity: AtomicUsize,
    in_use: AtomicUsize,
}

impl MemoryQuota {
    pub fn new(capacity: usize) -> MemoryQuota {
        MemoryQuota {
            capacity: AtomicUsize::new(capacity),
            in_use: AtomicUsize::new(0),
        }
    }

    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Acquire)
    }

    pub fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Release)
    }

    pub fn alloc(&self, bytes: usize) -> bool {
        let mut in_use_bytes = self.in_use.load(Ordering::Relaxed);
        let capacity = self.capacity.load(Ordering::Acquire);
        loop {
            if in_use_bytes + bytes > capacity {
                return false;
            }
            let new_in_use_bytes = in_use_bytes + bytes;
            match self.in_use.compare_exchange_weak(
                in_use_bytes,
                new_in_use_bytes,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => in_use_bytes = current,
            }
        }
    }

    pub fn free(&self, bytes: usize) {
        let mut in_use_bytes = self.in_use.load(Ordering::Relaxed);
        loop {
            // Saturating at the numeric bounds instead of overflowing.
            let new_in_use_bytes = in_use_bytes - std::cmp::min(bytes, in_use_bytes);
            match self.in_use.compare_exchange_weak(
                in_use_bytes,
                new_in_use_bytes,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => in_use_bytes = current,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_quota() {
        let quota = MemoryQuota::new(100);
        assert!(quota.alloc(10));
        assert_eq!(quota.in_use(), 10);
        assert!(!quota.alloc(100));
        assert_eq!(quota.in_use(), 10);
        quota.free(5);
        assert_eq!(quota.in_use(), 5);
        assert!(quota.alloc(95));
        assert_eq!(quota.in_use(), 100);
        quota.free(95);
        assert_eq!(quota.in_use(), 5);
    }

    #[test]
    fn test_resize_memory_quota() {
        let quota = MemoryQuota::new(100);
        assert!(quota.alloc(10));
        assert_eq!(quota.in_use(), 10);
        assert!(!quota.alloc(100));
        assert_eq!(quota.in_use(), 10);
        quota.set_capacity(200);
        assert!(quota.alloc(100));
        assert_eq!(quota.in_use(), 110);
        quota.set_capacity(50);
        assert!(!quota.alloc(100));
        assert_eq!(quota.in_use(), 110);
        quota.free(100);
        assert_eq!(quota.in_use(), 10);
        assert!(quota.alloc(40));
        assert_eq!(quota.in_use(), 50);
    }
}
