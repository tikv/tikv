// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collections::{HashMap, HashSet};
use kvproto::metapb::{Region, RegionEpoch};
use std::collections::{BTreeMap, VecDeque};
use std::hash::Hash;
use std::mem;

pub trait HeapSize {
    fn heap_size(&self) -> usize;
}

impl HeapSize for Region {
    #[inline]
    fn heap_size(&self) -> usize {
        self.start_key.len() + self.end_key.len() + mem::size_of::<RegionEpoch>()
    }
}

impl<K: Eq + Hash, V> HeapSize for HashMap<K, V> {
    #[inline]
    fn heap_size(&self) -> usize {
        // hashbrown uses 7/8 of allocated memory.
        self.capacity() * (mem::size_of::<K>() + mem::size_of::<V>()) * 8 / 7
    }
}

impl<K, V, H> HeapSize for std::collections::HashMap<K, V, H> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.capacity() * (mem::size_of::<K>() + mem::size_of::<V>())
    }
}

impl<K: Eq + Hash> HeapSize for HashSet<K> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>() * 8 / 7
    }
}

impl<K, V> HeapSize for BTreeMap<K, V> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.len() * (mem::size_of::<K>() + mem::size_of::<V>())
    }
}

impl<K> HeapSize for Vec<K> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>()
    }
}

impl<K> HeapSize for VecDeque<K> {
    #[inline]
    fn heap_size(&self) -> usize {
        self.capacity() * mem::size_of::<K>()
    }
}
