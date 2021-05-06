// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::{HashMap, HashSet};
use kvproto::{
    kvrpcpb::LockInfo,
    metapb::{Region, RegionEpoch},
    raft_cmdpb::ReadIndexRequest,
};
use std::collections::{BTreeMap, VecDeque};
use std::hash::Hash;
use std::mem;

pub trait HeapSize {
    fn heap_size(&self) -> usize;
}

impl HeapSize for Region {
    fn heap_size(&self) -> usize {
        self.start_key.len() + self.end_key.len() + mem::size_of::<RegionEpoch>()
    }
}

impl HeapSize for ReadIndexRequest {
    fn heap_size(&self) -> usize {
        self.key_ranges
            .iter()
            .map(|r| r.start_key.len() + r.end_key.len())
            .sum()
    }
}

impl HeapSize for LockInfo {
    fn heap_size(&self) -> usize {
        self.primary_lock.len()
            + self.key.len()
            + self.secondaries.iter().map(|k| k.len()).sum::<usize>()
    }
}

impl<T> HeapSize for Option<T>
where
    T: HeapSize,
{
    fn heap_size(&self) -> usize {
        self.as_ref().map_or(0, |s| s.heap_size())
    }
}

impl<T> HeapSize for Box<T>
where
    T: HeapSize,
{
    fn heap_size(&self) -> usize {
        self.as_ref().heap_size() + mem::size_of::<T>()
    }
}

impl<K: Eq + Hash, V> HeapSize for HashMap<K, V> {
    fn heap_size(&self) -> usize {
        // hashbrown uses 7/8 of allocated memory.
        self.capacity() * (mem::size_of::<K>() + mem::size_of::<V>()) * 8 / 7
    }
}

impl<K: Eq + Hash> HeapSize for HashSet<K> {
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>() * 8 / 7
    }
}

impl<K, V> HeapSize for BTreeMap<K, V> {
    fn heap_size(&self) -> usize {
        self.len() * (mem::size_of::<K>() + mem::size_of::<V>())
    }
}

impl<K> HeapSize for Vec<K> {
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>()
    }
}

impl<K> HeapSize for VecDeque<K> {
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>()
    }
}
