// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::{cmp::Eq, hash::Hash};

pub type HashMap<K, V> =
    std::collections::HashMap<K, V, std::hash::BuildHasherDefault<fxhash::FxHasher>>;
pub type HashSet<T> = std::collections::HashSet<T, std::hash::BuildHasherDefault<fxhash::FxHasher>>;
pub use std::collections::hash_map::Entry as HashMapEntry;

pub fn hash_set_with_capacity<T: Hash + Eq>(capacity: usize) -> HashSet<T> {
    HashSet::with_capacity_and_hasher(capacity, fxhash::FxBuildHasher::default())
}
