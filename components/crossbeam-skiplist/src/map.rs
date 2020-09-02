//! TODO: docs

use std::borrow::Borrow;
use std::fmt;
use std::iter::FromIterator;
use std::mem::ManuallyDrop;
use std::ops::{Bound, RangeBounds};
use std::ptr;

use crate::base::{self, try_pin_loop};
use crate::epoch;

/// A map based on a lock-free skip list.
pub struct SkipMap<K, V> {
    inner: base::SkipList<K, V>,
}

impl<K, V> SkipMap<K, V> {
    /// Returns a new, empty map.
    pub fn new() -> SkipMap<K, V> {
        SkipMap {
            inner: base::SkipList::new(epoch::default_collector().clone()),
        }
    }

    /// Returns `true` if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of entries in the map.
    ///
    /// If the map is being concurrently modified, consider the returned number just an
    /// approximation without any guarantees.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K, V> SkipMap<K, V>
where
    K: Ord,
{
    /// Returns the entry with the smallest key.
    pub fn front(&self) -> Option<Entry<'_, K, V>> {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.front(guard)).map(Entry::new)
    }

    /// Returns the entry with the largest key.
    pub fn back(&self) -> Option<Entry<'_, K, V>> {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.back(guard)).map(Entry::new)
    }

    /// Returns `true` if the map contains a value for the specified key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        self.inner.contains_key(key, guard)
    }

    /// Returns an entry with the specified `key`.
    pub fn get<Q>(&self, key: &Q) -> Option<Entry<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.get(key, guard)).map(Entry::new)
    }

    /// Returns an `Entry` pointing to the lowest element whose key is above
    /// the given bound. If no such element is found then `None` is
    /// returned.
    pub fn lower_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<Entry<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.lower_bound(bound, guard)).map(Entry::new)
    }

    /// Returns an `Entry` pointing to the highest element whose key is below
    /// the given bound. If no such element is found then `None` is
    /// returned.
    pub fn upper_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<Entry<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.upper_bound(bound, guard)).map(Entry::new)
    }

    /// Finds an entry with the specified key, or inserts a new `key`-`value` pair if none exist.
    pub fn get_or_insert(&self, key: K, value: V) -> Entry<'_, K, V> {
        let guard = &epoch::pin();
        Entry::new(self.inner.get_or_insert(key, value, guard))
    }

    /// Returns an iterator over all entries in the map.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            inner: self.inner.ref_iter(),
        }
    }

    /// Returns an iterator over a subset of entries in the skip list.
    pub fn range<Q, R>(&self, range: R) -> Range<'_, Q, R, K, V>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        Range {
            inner: self.inner.ref_range(range),
        }
    }
}

impl<K, V> SkipMap<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    /// Inserts a `key`-`value` pair into the map and returns the new entry.
    ///
    /// If there is an existing entry with this key, it will be removed before inserting the new
    /// one.
    pub fn insert(&self, key: K, value: V) -> Entry<'_, K, V> {
        let guard = &epoch::pin();
        Entry::new(self.inner.insert(key, value, guard))
    }

    /// Removes an entry with the specified `key` from the map and returns it.
    pub fn remove<Q>(&self, key: &Q) -> Option<Entry<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        self.inner.remove(key, guard).map(Entry::new)
    }

    /// Removes an entry from the front of the map.
    pub fn pop_front(&self) -> Option<Entry<'_, K, V>> {
        let guard = &epoch::pin();
        self.inner.pop_front(guard).map(Entry::new)
    }

    /// Removes an entry from the back of the map.
    pub fn pop_back(&self) -> Option<Entry<'_, K, V>> {
        let guard = &epoch::pin();
        self.inner.pop_back(guard).map(Entry::new)
    }

    /// Iterates over the map and removes every entry.
    pub fn clear(&self) {
        let guard = &mut epoch::pin();
        self.inner.clear(guard);
    }
}

impl<K, V> Default for SkipMap<K, V> {
    fn default() -> SkipMap<K, V> {
        SkipMap::new()
    }
}

impl<K, V> fmt::Debug for SkipMap<K, V>
where
    K: Ord + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("SkipMap { .. }")
    }
}

impl<K, V> IntoIterator for SkipMap<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter {
            inner: self.inner.into_iter(),
        }
    }
}

impl<'a, K, V> IntoIterator for &'a SkipMap<K, V>
where
    K: Ord,
{
    type Item = Entry<'a, K, V>;
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

impl<K, V> FromIterator<(K, V)> for SkipMap<K, V>
where
    K: Ord,
{
    fn from_iter<I>(iter: I) -> SkipMap<K, V>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let s = SkipMap::new();
        for (k, v) in iter {
            s.get_or_insert(k, v);
        }
        s
    }
}

/// A reference-counted entry in a map.
pub struct Entry<'a, K, V> {
    inner: ManuallyDrop<base::RefEntry<'a, K, V>>,
}

impl<'a, K, V> Entry<'a, K, V> {
    fn new(inner: base::RefEntry<'a, K, V>) -> Entry<'a, K, V> {
        Entry {
            inner: ManuallyDrop::new(inner),
        }
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        self.inner.key()
    }

    /// Returns a reference to the value.
    pub fn value(&self) -> &V {
        self.inner.value()
    }

    /// Returns `true` if the entry is removed from the map.
    pub fn is_removed(&self) -> bool {
        self.inner.is_removed()
    }
}

impl<K, V> Drop for Entry<'_, K, V> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::into_inner(ptr::read(&self.inner)).release_with_pin(epoch::pin);
        }
    }
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: Ord,
{
    /// Moves to the next entry in the map.
    pub fn move_next(&mut self) -> bool {
        let guard = &epoch::pin();
        self.inner.move_next(guard)
    }

    /// Moves to the previous entry in the map.
    pub fn move_prev(&mut self) -> bool {
        let guard = &epoch::pin();
        self.inner.move_prev(guard)
    }

    /// Returns the next entry in the map.
    pub fn next(&self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next(guard).map(Entry::new)
    }

    /// Returns the previous entry in the map.
    pub fn prev(&self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.prev(guard).map(Entry::new)
    }
}

impl<K, V> Entry<'_, K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    /// Removes the entry from the map.
    ///
    /// Returns `true` if this call removed the entry and `false` if it was already removed.
    pub fn remove(&self) -> bool {
        let guard = &epoch::pin();
        self.inner.remove(guard)
    }
}

impl<'a, K, V> Clone for Entry<'a, K, V> {
    fn clone(&self) -> Entry<'a, K, V> {
        Entry {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> fmt::Debug for Entry<'_, K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Entry")
            .field(self.key())
            .field(self.value())
            .finish()
    }
}

/// An owning iterator over the entries of a `SkipMap`.
pub struct IntoIter<K, V> {
    inner: base::IntoIter<K, V>,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        self.inner.next()
    }
}

impl<K, V> fmt::Debug for IntoIter<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("IntoIter { .. }")
    }
}

/// An iterator over the entries of a `SkipMap`.
pub struct Iter<'a, K, V> {
    inner: base::RefIter<'a, K, V>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Ord,
{
    type Item = Entry<'a, K, V>;

    fn next(&mut self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next(guard).map(Entry::new)
    }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V>
where
    K: Ord,
{
    fn next_back(&mut self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next_back(guard).map(Entry::new)
    }
}

impl<K, V> fmt::Debug for Iter<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Iter { .. }")
    }
}

/// An iterator over the entries of a `SkipMap`.
pub struct Range<'a, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    pub(crate) inner: base::RefRange<'a, Q, R, K, V>,
}

impl<'a, Q, R, K, V> Iterator for Range<'a, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Entry<'a, K, V>;

    fn next(&mut self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next(guard).map(Entry::new)
    }
}

impl<'a, Q, R, K, V> DoubleEndedIterator for Range<'a, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn next_back(&mut self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next_back(guard).map(Entry::new)
    }
}

impl<Q, R, K, V> fmt::Debug for Range<'_, Q, R, K, V>
where
    K: Ord + Borrow<Q> + fmt::Debug,
    V: fmt::Debug,
    R: RangeBounds<Q> + fmt::Debug,
    Q: Ord + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Range")
            .field("range", &self.inner.range)
            .field("head", &self.inner.head)
            .field("tail", &self.inner.tail)
            .finish()
    }
}
