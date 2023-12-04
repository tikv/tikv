// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(hash_drain_filter)]

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::{
    borrow::Borrow,
    cmp::Eq,
    collections,
    fmt::{self, Debug},
    hash::{self, Hash},
    ops::Index,
};

type HashBuilder = hash::BuildHasherDefault<fxhash::FxHasher>;
pub struct HashMap<K, V> {
    base: collections::HashMap<K, V, HashBuilder>,
}
pub type HashSet<T> = collections::HashSet<T, HashBuilder>;
pub use collections::hash_map::Entry as HashMapEntry;

pub fn hash_set_with_capacity<T: Hash + Eq>(capacity: usize) -> HashSet<T> {
    HashSet::with_capacity_and_hasher(capacity, fxhash::FxBuildHasher::default())
}

impl<K, Q: ?Sized, V> Index<&Q> for HashMap<K, V>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash + Debug,
{
    type Output = V;

    fn index(&self, key: &Q) -> &V {
        self.base
            .get(key)
            .unwrap_or_else(|| panic!("no entry found for key: {:?}", key))
    }
}

impl<K, V> Debug for HashMap<K, V>
where
    K: Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.base.fmt(f)
    }
}

impl<K, V> Clone for HashMap<K, V>
where
    K: Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
        }
    }

    fn clone_from(&mut self, other: &Self) {
        self.base.clone_from(&other.base);
    }
}

impl<K, V> PartialEq for HashMap<K, V>
where
    K: Eq + Hash,
    V: PartialEq,
{
    fn eq(&self, other: &HashMap<K, V>) -> bool {
        self.base.eq(&other.base)
    }
}

impl<K, V> Default for HashMap<K, V> {
    /// Creates an empty `HashMap<K, V>`, with the `Default` value for the
    /// hasher.
    fn default() -> HashMap<K, V> {
        HashMap {
            base: collections::HashMap::with_hasher(fxhash::FxBuildHasher::default()),
        }
    }
}

impl<K, V> HashMap<K, V>
where
    K: Eq + Hash,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        self.base.extend(iter)
    }

    pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.base.contains_key(k)
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.base.get(k)
    }

    pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.base.get_mut(k)
    }

    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.base.remove(k)
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.base.insert(k, v)
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.base.retain(f)
    }

    pub fn entry(&mut self, key: K) -> collections::hash_map::Entry<'_, K, V> {
        self.base.entry(key)
    }

    pub fn get_key_value<Q: ?Sized>(&self, k: &Q) -> Option<(&K, &V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.base.get_key_value(k)
    }

    pub fn remove_entry<Q: ?Sized>(&mut self, k: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.base.remove_entry(k)
    }

    pub fn iter(&self) -> collections::hash_map::Iter<'_, K, V> {
        self.base.iter()
    }

    pub fn iter_mut(&mut self) -> collections::hash_map::IterMut<'_, K, V> {
        self.base.iter_mut()
    }

    pub fn shrink_to_fit(&mut self) {
        self.base.shrink_to_fit();
    }

    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.base.shrink_to(min_capacity);
    }

    pub fn drain_filter<F>(&mut self, pred: F) -> collections::hash_map::DrainFilter<'_, K, V, F>
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.base.drain_filter(pred)
    }
}

impl<K, V> HashMap<K, V> {
    pub fn new() -> HashMap<K, V> {
        HashMap::default()
    }

    pub fn is_empty(&self) -> bool {
        self.base.is_empty()
    }

    pub fn len(&self) -> usize {
        self.base.len()
    }

    pub fn capacity(&self) -> usize {
        self.base.capacity()
    }

    pub fn clear(&mut self) {
        self.base.clear();
    }

    pub fn keys(&self) -> collections::hash_map::Keys<'_, K, V> {
        self.base.keys()
    }

    pub fn into_keys(self) -> collections::hash_map::IntoKeys<K, V> {
        self.base.into_keys()
    }

    pub fn values(&self) -> collections::hash_map::Values<'_, K, V> {
        self.base.values()
    }

    pub fn values_mut(&mut self) -> collections::hash_map::ValuesMut<'_, K, V> {
        self.base.values_mut()
    }

    pub fn into_values(self) -> collections::hash_map::IntoValues<K, V> {
        self.base.into_values()
    }

    pub fn with_capacity(capacity: usize) -> HashMap<K, V> {
        HashMap {
            base: collections::HashMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }

    pub fn drain(&mut self) -> collections::hash_map::Drain<'_, K, V> {
        self.base.drain()
    }
}

impl<'a, K, V> IntoIterator for &'a HashMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = collections::hash_map::Iter<'a, K, V>;

    fn into_iter(self) -> collections::hash_map::Iter<'a, K, V> {
        self.base.iter()
    }
}

impl<'a, K, V> IntoIterator for &'a mut HashMap<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = collections::hash_map::IterMut<'a, K, V>;

    fn into_iter(self) -> collections::hash_map::IterMut<'a, K, V> {
        self.base.iter_mut()
    }
}

impl<K, V> IntoIterator for HashMap<K, V> {
    type Item = (K, V);
    type IntoIter = collections::hash_map::IntoIter<K, V>;

    fn into_iter(self) -> collections::hash_map::IntoIter<K, V> {
        self.base.into_iter()
    }
}

impl<K, V> FromIterator<(K, V)> for HashMap<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> HashMap<K, V> {
        let mut map = HashMap::default();
        map.extend(iter);
        map
    }
}

impl<K, V> serde::Serialize for HashMap<K, V>
where
    K: serde::Serialize + Eq + Hash,
    V: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.base.serialize(serializer)
    }
}

impl<'de, K, V> serde::Deserialize<'de> for HashMap<K, V>
where
    K: serde::Deserialize<'de> + Eq + Hash,
    V: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let base = <collections::HashMap<K, V, HashBuilder> as serde::Deserialize>::deserialize(
            deserializer,
        )?;
        Ok(HashMap { base })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_panic() {
        let map: HashMap<String, usize> = HashMap::new();
        let result = std::panic::catch_unwind(|| {
            let _ = map["KeyNotFound;&="];
        });
        let panic_msg = result.unwrap_err().downcast::<String>().unwrap();
        assert!(panic_msg.contains("KeyNotFound;&="), "{:?}", panic_msg);
    }
}
