// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
extern crate fxhash;
pub type HashMap<K, V> =
    std::collections::HashMap<K, V, std::hash::BuildHasherDefault<fxhash::FxHasher>>;
pub type HashSet<T> = std::collections::HashSet<T, std::hash::BuildHasherDefault<fxhash::FxHasher>>;
pub use std::collections::hash_map::Entry as HashMapEntry;

pub use indexmap::map::Entry as OrderMapEntry;
pub use indexmap::IndexMap as OrderMap;
