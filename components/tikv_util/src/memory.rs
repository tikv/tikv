// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    sync::{
        atomic::{AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
};

use collections::HashMap;
use kvproto::{
    encryptionpb::EncryptionMeta,
    kvrpcpb::LockInfo,
    metapb::{Peer, Region, RegionEpoch},
    raft_cmdpb::{self, RaftCmdRequest, ReadIndexRequest},
};

// The maxinum memory size in byte for a single `alloc` or `alloc_force`.
// We set this hard limit to avoid the `in_use` counter overflow that may
// lead to undefined behavior.
// When passes a higher value, the result will depend on the called function:
// - alloc. Return an error.
// - alloc_force. Ignore this call and do nothing.
// - free. Ignore this call and do nothing.
const MAX_MEMORY_ALLOC_SIZE: usize = 1 << 48;

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

/// Query the number of bytes of an object.
pub trait HeapSize {
    /// Return the approximate number of bytes it owns in heap.
    ///
    /// N.B. the implementation should be performant, as it is often called on
    /// performance critical path.
    fn approximate_heap_size(&self) -> usize {
        0
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

#[derive(Debug)]
pub struct MemoryQuotaExceeded;

impl std::error::Error for MemoryQuotaExceeded {}

impl_display_as_debug!(MemoryQuotaExceeded);

pub struct MemoryQuota {
    // We suppose the memory quota should not exceed the upper bound of `isize`.
    // As we don't support 32bit(or lower) arch, this won't be a problem.
    in_use: AtomicIsize,
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

    pub fn source(&self) -> &MemoryQuota {
        &self.from
    }
}

impl Drop for OwnedAllocated {
    fn drop(&mut self) {
        self.from.free(self.allocated)
    }
}

impl MemoryQuota {
    pub fn new(capacity: usize) -> MemoryQuota {
        let capacity = Self::adjust_capacity(capacity);
        MemoryQuota {
            in_use: AtomicIsize::new(0),
            capacity: AtomicUsize::new(capacity),
        }
    }

    #[inline]
    fn adjust_capacity(capacity: usize) -> usize {
        // Value bigger than isize::MAX just means unlimited,
        // so replace it with isize::MAX to avoid overflow.
        std::cmp::min(capacity, isize::MAX as usize)
    }

    #[inline]
    pub fn in_use(&self) -> usize {
        let value = self.in_use.load(Ordering::Relaxed);
        // Saturating at the numeric bounds instead of overflowing.
        // we handle negative overflow here to make `free` implementation simpler.
        std::cmp::max(value, 0) as usize
    }

    /// Returns a floating number between [0, 1] presents the current memory
    /// status.
    pub fn used_ratio(&self) -> f64 {
        self.in_use() as f64 / self.capacity() as f64
    }

    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    pub fn set_capacity(&self, capacity: usize) {
        let capacity = Self::adjust_capacity(capacity);
        self.capacity.store(capacity, Ordering::Relaxed);
    }

    pub fn alloc_force(&self, bytes: usize) {
        if bytes > MAX_MEMORY_ALLOC_SIZE {
            return;
        }
        self.in_use.fetch_add(bytes as isize, Ordering::Relaxed);
    }

    pub fn alloc(&self, bytes: usize) -> Result<(), MemoryQuotaExceeded> {
        let capacity = self.capacity.load(Ordering::Relaxed) as isize;
        let in_use_bytes = self.in_use.load(Ordering::Relaxed);
        if bytes > capacity.saturating_sub(in_use_bytes) as usize || bytes > MAX_MEMORY_ALLOC_SIZE {
            return Err(MemoryQuotaExceeded);
        }
        let bytes = bytes as isize;
        let new_in_use_bytes = self.in_use.fetch_add(bytes, Ordering::Relaxed);
        if bytes > capacity - new_in_use_bytes {
            self.in_use.fetch_sub(bytes, Ordering::Relaxed);
            return Err(MemoryQuotaExceeded);
        }
        Ok(())
    }

    pub fn free(&self, bytes: usize) {
        // bytes higher than `MAX_MEMORY_ALLOC_SIZE` means the memory is acquired with
        // `alloc_force`, so also ignore the `free` call.
        if bytes > MAX_MEMORY_ALLOC_SIZE {
            return;
        }
        let bytes = bytes as isize;
        let new_in_use_bytes = self.in_use.fetch_sub(bytes, Ordering::Relaxed) - bytes;
        if new_in_use_bytes < 0 {
            // handle overflow.
            self.in_use
                .fetch_add(std::cmp::min(-new_in_use_bytes, bytes), Ordering::Relaxed);
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
    fn test_memory_quota_multi_thread() {
        let cap = 1000;
        // use a number not devided by the cap.
        let req = 6;
        let num_threads = 10;
        let quota = MemoryQuota::new(cap);

        // first alloc more than free, the in_use should reach near the capacity but not
        // exceed.
        std::thread::scope(|s| {
            let mut handlers = vec![];
            for _i in 0..num_threads {
                let h = s.spawn(|| {
                    for j in 0..100 {
                        let res = quota.alloc(req);
                        if res.is_err() {
                            let in_use = quota.in_use.load(Ordering::Relaxed) as usize;
                            assert!(
                                cap - num_threads * req < in_use
                                    && in_use < cap + num_threads * req
                            );
                        } else if j % 3 == 0 {
                            // do free randomly.
                            quota.free(req);
                        }
                    }
                });
                handlers.push(h);
            }
            for h in handlers {
                h.join().unwrap();
            }
        });
        assert_eq!(quota.in_use(), cap - cap % req);

        // test free more the alloc, the final result should be 0.
        quota.free(cap / 2);
        std::thread::scope(|s| {
            let mut handlers = vec![];
            for _i in 0..num_threads {
                let h = s.spawn(|| {
                    for j in 0..100 {
                        if quota.alloc(req).is_ok() {
                            quota.free(req);
                        }
                        // do random more free.
                        if j % 2 == 0 {
                            quota.free(req);
                            let in_use = quota.in_use.load(Ordering::Relaxed);
                            assert!(
                                in_use < cap as isize && in_use > -((num_threads * req) as isize)
                            );
                        }
                    }
                });
                handlers.push(h);
            }
            for h in handlers {
                h.join().unwrap();
            }
        });
        let in_use = quota.in_use.load(Ordering::Relaxed);
        assert_eq!(in_use, 0);
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

        // test out of range alloc and free.
        assert!(quota.alloc(MAX_MEMORY_ALLOC_SIZE * 2).is_err());

        // in_use should not change after force_alloc with extreme big value.
        let in_use = quota.in_use();
        assert!(in_use > 0);
        quota.alloc_force(MAX_MEMORY_ALLOC_SIZE * 2);
        quota.free(MAX_MEMORY_ALLOC_SIZE * 2);
        assert_eq!(quota.in_use(), in_use);
    }

    #[test]
    fn test_alloc_force() {
        let quota = MemoryQuota::new(100);
        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 10);
        quota.alloc_force(100);
        assert_eq!(quota.in_use(), 110);

        quota.free(10);
        assert_eq!(quota.in_use(), 100);
        quota.alloc(10).unwrap_err();
        assert_eq!(quota.in_use(), 100);

        quota.alloc_force(20);
        assert_eq!(quota.in_use(), 120);
        quota.free(110);
        assert_eq!(quota.in_use(), 10);

        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 20);
        quota.free(10);
        assert_eq!(quota.in_use(), 10);

        // Resize to a smaller capacity
        quota.set_capacity(10);
        quota.alloc(100).unwrap_err();
        assert_eq!(quota.in_use(), 10);
        quota.alloc_force(100);
        assert_eq!(quota.in_use(), 110);
        // Resize to a larger capacity
        quota.set_capacity(120);
        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 120);
        quota.alloc_force(100);
        assert_eq!(quota.in_use(), 220);
        // Free more then it has.
        quota.free(230);
        assert_eq!(quota.in_use(), 0);
    }

    #[test]
    fn test_approximate_heap_size() {
        let mut vu8 = Vec::with_capacity(16);
        assert_eq!(vu8.approximate_heap_size(), 16);
        vu8.extend([1u8, 2, 3]);
        assert_eq!(vu8.approximate_heap_size(), 16);

        let ovu8 = Some(vu8);
        assert_eq!(ovu8.approximate_heap_size(), 16);

        let ovu82 = (ovu8, Some(Vec::<u8>::with_capacity(16)));
        assert_eq!(ovu82.approximate_heap_size(), 16 * 2);

        let mut mu8u64 = HashMap::<u8, u64>::default();
        mu8u64.reserve(16);
        assert_eq!(mu8u64.approximate_heap_size(), mu8u64.capacity() * (1 + 8));

        let mut mu8vu64 = HashMap::<u8, Vec<u64>>::default();
        mu8vu64.reserve(16);
        mu8vu64.insert(1, Vec::with_capacity(2));
        mu8vu64.insert(2, Vec::with_capacity(2));
        assert_eq!(
            mu8vu64.approximate_heap_size(),
            mu8vu64.capacity() * (1 + mem::size_of::<Vec<u64>>())
                + 2 * (Vec::<u64>::with_capacity(2).approximate_heap_size())
        );
    }
}
