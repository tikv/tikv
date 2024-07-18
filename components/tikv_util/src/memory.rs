// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use collections::HashMap;
use kvproto::{
    coprocessor as coppb,
    encryptionpb::EncryptionMeta,
    kvrpcpb::{self, LockInfo},
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
    fn approximate_heap_size(&self) -> usize {
        0
    }

    fn approximate_mem_size(&self) -> usize
    where
        Self: Sized,
    {
        mem::size_of::<Self>() + self.approximate_heap_size()
    }
}

macro_rules! impl_zero_heap_size{
    ( $($typ: ty,)+ ) => {
        $(
            impl HeapSize for $typ {
                fn approximate_heap_size(&self) -> usize { 0 }
            }
        )+
    }
}
impl_zero_heap_size! {
    bool, u8, u64,
}
// Do not impl HeapSize for [T], because type coercions make it error-prone.
// E.g., Vec[u8] may be casted to &[u8] which does not own any byte in heap.
impl<T: HeapSize> HeapSize for Vec<T> {
    fn approximate_heap_size(&self) -> usize {
        let cap_bytes = self.capacity() * std::mem::size_of::<T>();
        if self.is_empty() {
            cap_bytes
        } else {
            // Prefer an approximation of its actually heap size, because we
            // want the time complexity to be O(1).
            self.len() * self[0].approximate_heap_size() + cap_bytes
        }
    }
}
impl<A: HeapSize, B: HeapSize> HeapSize for (A, B) {
    fn approximate_heap_size(&self) -> usize {
        self.0.approximate_heap_size() + self.1.approximate_heap_size()
    }
}
impl<T: HeapSize> HeapSize for Option<T> {
    fn approximate_heap_size(&self) -> usize {
        match self {
            Some(t) => t.approximate_heap_size(),
            None => 0,
        }
    }
}

impl<K: HeapSize, V: HeapSize> HeapSize for HashMap<K, V> {
    fn approximate_heap_size(&self) -> usize {
        let cap_bytes = self.capacity() * (mem::size_of::<K>() + mem::size_of::<V>());
        if self.is_empty() {
            cap_bytes
        } else {
            let kv = self.iter().next().unwrap();
            // Prefer an approximation of its actually heap size, because we
            // want the time complexity to be O(1).
            cap_bytes + self.len() * (kv.0.approximate_heap_size() + kv.1.approximate_heap_size())
        }
    }
}

impl HeapSize for Region {
    fn approximate_heap_size(&self) -> usize {
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
    fn approximate_heap_size(&self) -> usize {
        self.key_ranges
            .iter()
            .map(|r| r.start_key.capacity() + r.end_key.capacity())
            .sum()
    }
}

impl HeapSize for LockInfo {
    fn approximate_heap_size(&self) -> usize {
        self.primary_lock.capacity()
            + self.key.capacity()
            + self.secondaries.iter().map(|k| k.len()).sum::<usize>()
    }
}

impl HeapSize for RaftCmdRequest {
    fn approximate_heap_size(&self) -> usize {
        mem::size_of::<raft_cmdpb::RaftRequestHeader>()
            + self.requests.capacity() * mem::size_of::<raft_cmdpb::Request>()
            + mem::size_of_val(&self.admin_request)
            + mem::size_of_val(&self.status_request)
    }
}

impl HeapSize for coppb::KeyRange {
    fn approximate_heap_size(&self) -> usize {
        self.start.capacity() + self.end.capacity()
    }
}

impl HeapSize for kvrpcpb::Context {
    fn approximate_heap_size(&self) -> usize {
        self.resolved_locks.capacity() * mem::size_of::<u64>()
            + self.committed_locks.capacity() * mem::size_of::<u64>()
            + self.resource_group_tag.capacity()
            + self.request_source.as_bytes().len()
            + self
                .get_resource_control_context()
                .resource_group_name
                .as_bytes()
                .len()
            + self.get_source_stmt().session_alias.as_bytes().len()
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

pub struct OwnedAllocated {
    allocated: usize,
    from: Arc<MemoryQuota>,
}

impl OwnedAllocated {
    pub fn new(target: Arc<MemoryQuota>) -> Self {
        Self {
            allocated: 0,
            from: target,
        }
    }

    pub fn alloc(&mut self, bytes: usize) -> Result<(), MemoryQuotaExceeded> {
        self.from.alloc(bytes)?;
        self.allocated += bytes;
        Ok(())
    }
}

impl Drop for OwnedAllocated {
    fn drop(&mut self) {
        self.from.free(self.allocated)
    }
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

    #[test]
    fn test_allocated() {
        let quota = Arc::new(MemoryQuota::new(100));
        let mut allocated = OwnedAllocated::new(Arc::clone(&quota));
        allocated.alloc(42).unwrap();
        assert_eq!(quota.in_use(), 42);
        quota.alloc(59).unwrap_err();
        allocated.alloc(16).unwrap();
        assert_eq!(quota.in_use(), 58);
        let mut allocated2 = OwnedAllocated::new(Arc::clone(&quota));
        allocated2.alloc(8).unwrap();
        allocated2.alloc(40).unwrap_err();
        assert_eq!(quota.in_use(), 66);
        quota.alloc(4).unwrap();
        assert_eq!(quota.in_use(), 70);
        drop(allocated);
        assert_eq!(quota.in_use(), 12);
        drop(allocated2);
        assert_eq!(quota.in_use(), 4);
    }
}
