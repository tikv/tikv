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
        mem::size_of_val(self)
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

#[derive(Debug)]
pub struct MemoryQuotaExceeded;

impl std::error::Error for MemoryQuotaExceeded {}

impl_display_as_debug!(MemoryQuotaExceeded);

pub struct MemoryQuota {
    in_use: AtomicUsize,
    capacity: AtomicUsize,
}

impl MemoryQuota {
    pub fn new(capacity: usize) -> MemoryQuota {
        MemoryQuota {
            in_use: AtomicUsize::new(0),
            capacity: AtomicUsize::new(capacity),
        }
    }

    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    pub fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Relaxed);
    }

    pub fn alloc(&self, bytes: usize) -> Result<(), MemoryQuotaExceeded> {
        let capacity = self.capacity.load(Ordering::Relaxed);
        let mut in_use_bytes = self.in_use.load(Ordering::Relaxed);
        loop {
            if in_use_bytes + bytes > capacity {
                return Err(MemoryQuotaExceeded);
            }
            let new_in_use_bytes = in_use_bytes + bytes;
            match self.in_use.compare_exchange_weak(
                in_use_bytes,
                new_in_use_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
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
                Ordering::Relaxed,
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
        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 10);
        quota.alloc(100).unwrap_err();
        assert_eq!(quota.in_use(), 10);
        quota.free(5);
        assert_eq!(quota.in_use(), 5);
        quota.alloc(95).unwrap();
        assert_eq!(quota.in_use(), 100);
        quota.free(95);
        assert_eq!(quota.in_use(), 5);
    }

    #[test]
    fn test_resize_memory_quota() {
        let quota = MemoryQuota::new(100);
        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 10);
        quota.alloc(100).unwrap_err();
        assert_eq!(quota.in_use(), 10);
        quota.set_capacity(200);
        quota.alloc(100).unwrap();
        assert_eq!(quota.in_use(), 110);
        quota.set_capacity(50);
        quota.alloc(100).unwrap_err();
        assert_eq!(quota.in_use(), 110);
        quota.free(100);
        assert_eq!(quota.in_use(), 10);
        quota.alloc(40).unwrap();
        assert_eq!(quota.in_use(), 50);
    }
}
