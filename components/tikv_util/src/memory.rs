// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::time::Instant;
use collections::{HashMap, HashSet};
use kvproto::{
    kvrpcpb::LockInfo,
    metapb::{Peer, Region, RegionEpoch},
    raft_cmdpb::{self, RaftCmdRequest, ReadIndexRequest},
};
use std::collections::{BTreeMap, VecDeque};
use std::hash::Hash;
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

impl HeapSize for RaftCmdRequest {
    fn heap_size(&self) -> usize {
        mem::size_of::<raft_cmdpb::RaftRequestHeader>()
            + self.requests.len() * mem::size_of::<raft_cmdpb::Request>()
            + self
                .admin_request
                .as_ref()
                .map_or(0, |r| mem::size_of_val(r))
            + self
                .status_request
                .as_ref()
                .map_or(0, |r| mem::size_of_val(r))
    }
}

impl<T1, T2> HeapSize for (T1, T2)
where
    T1: HeapSize,
    T2: HeapSize,
{
    fn heap_size(&self) -> usize {
        self.0.heap_size() + self.1.heap_size()
    }
}

impl<T1, T2, T3> HeapSize for (T1, T2, T3)
where
    T1: HeapSize,
    T2: HeapSize,
    T3: HeapSize,
{
    fn heap_size(&self) -> usize {
        self.0.heap_size() + self.1.heap_size() + self.2.heap_size()
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

impl<K: Eq + Hash, V> HeapSize for HashMap<K, V>
where
    K: HeapSize,
    V: HeapSize,
{
    fn heap_size(&self) -> usize {
        // hashbrown uses 7/8 of allocated memory.
        let size = self.capacity() * (mem::size_of::<K>() + mem::size_of::<V>()) * 8 / 7;
        let children_size: usize = self
            .iter()
            .map(|(k, v)| k.heap_size() + v.heap_size())
            .sum();
        size + children_size
    }
}

impl<K: Eq + Hash> HeapSize for HashSet<K>
where
    K: HeapSize,
{
    fn heap_size(&self) -> usize {
        let size = self.capacity() * mem::size_of::<K>() * 8 / 7;
        let children_size: usize = self.iter().map(|k| k.heap_size()).sum();
        size + children_size
    }
}

impl<K, V> HeapSize for BTreeMap<K, V>
where
    K: HeapSize,
    V: HeapSize,
{
    fn heap_size(&self) -> usize {
        let size = self.len() * (mem::size_of::<K>() + mem::size_of::<V>());
        let children_size: usize = self
            .iter()
            .map(|(k, v)| k.heap_size() + v.heap_size())
            .sum();
        size + children_size
    }
}

impl<T> HeapSize for Vec<T>
where
    T: HeapSize,
{
    fn heap_size(&self) -> usize {
        let size = self.capacity() * mem::size_of::<T>();
        let children_size: usize = self.iter().map(|v| v.heap_size()).sum();
        size + children_size
    }
}

impl<T> HeapSize for VecDeque<T>
where
    T: HeapSize,
{
    fn heap_size(&self) -> usize {
        let size = self.capacity() * mem::size_of::<T>();
        let children_size: usize = self.iter().map(|v| v.heap_size()).sum();
        size + children_size
    }
}

#[macro_export]
macro_rules! known_heap_size(
    ($size:expr, $($ty:ty),+) => (
        $(
            impl $crate::memory::HeapSize for $ty {
                fn heap_size(&self) -> usize {
                    $size
                }
            }
        )+
    );
    ($size: expr, $($ty:ident<$($gen:ident),+>),+) => (
        $(
        impl<$($gen: $crate::memory::HeapSize),+> $crate::HeapSize for $ty<$($gen),+> {
            fn heap_size(&self) -> usize {
                $size
            }
        }
        )+
    );
);

known_heap_size!(0, char, str);
known_heap_size!(0, u8, u16, u32, u64, usize);
known_heap_size!(0, i8, i16, i32, i64, isize);
known_heap_size!(0, bool, f32, f64);
known_heap_size!(0, Peer, Instant, std::time::Instant);
