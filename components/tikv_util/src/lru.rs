// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    hash::Hash,
    mem::MaybeUninit,
    ptr::{self, NonNull},
};

use collections::{HashMap, HashMapEntry};

struct Record<K> {
    prev: NonNull<Record<K>>,
    next: NonNull<Record<K>>,
    key: MaybeUninit<K>,
}

struct ValueEntry<K, V> {
    value: V,
    record: NonNull<Record<K>>,
}

struct Trace<K> {
    head: NonNull<Record<K>>,
    tail: NonNull<Record<K>>,
    tick: usize,
    sample_mask: usize,
}

#[inline]
unsafe fn suture<K>(mut leading: NonNull<Record<K>>, mut following: NonNull<Record<K>>) {
    leading.as_mut().next = following;
    following.as_mut().prev = leading;
}

#[inline]
unsafe fn cut_out<K>(record: NonNull<Record<K>>) {
    suture(record.as_ref().prev, record.as_ref().next)
}

impl<K> Trace<K> {
    fn new(sample_mask: usize) -> Trace<K> {
        unsafe {
            let head = Box::leak(Box::new(Record {
                prev: NonNull::dangling(),
                next: NonNull::dangling(),
                key: MaybeUninit::uninit(),
            }))
            .into();
            let tail = Box::leak(Box::new(Record {
                prev: NonNull::dangling(),
                next: NonNull::dangling(),
                key: MaybeUninit::uninit(),
            }))
            .into();
            suture(head, tail);

            Trace {
                head,
                tail,
                sample_mask,
                tick: 0,
            }
        }
    }

    #[inline]
    fn maybe_promote(&mut self, record: NonNull<Record<K>>) {
        self.tick += 1;
        if self.tick & self.sample_mask == 0 {
            self.promote(record);
        }
    }

    fn promote(&mut self, record: NonNull<Record<K>>) {
        unsafe {
            cut_out(record);
            suture(record, self.head.as_ref().next);
            suture(self.head, record);
        }
    }

    fn delete(&mut self, record: NonNull<Record<K>>) {
        unsafe {
            cut_out(record);

            ptr::drop_in_place(Box::from_raw(record.as_ptr()).key.as_mut_ptr());
        }
    }

    fn create(&mut self, key: K) -> NonNull<Record<K>> {
        let record = Box::leak(Box::new(Record {
            prev: self.head,
            next: unsafe { self.head.as_ref().next },
            key: MaybeUninit::new(key),
        }))
        .into();
        unsafe {
            self.head.as_mut().next.as_mut().prev = record;
            self.head.as_mut().next = record;
        }
        record
    }

    fn reuse_tail(&mut self, key: K) -> (K, NonNull<Record<K>>) {
        unsafe {
            let mut record = self.tail.as_ref().prev;
            cut_out(record);
            suture(record, self.head.as_ref().next);
            suture(self.head, record);

            let old_key = record.as_mut().key.as_ptr().read();
            record.as_mut().key = MaybeUninit::new(key);
            (old_key, record)
        }
    }

    fn clear(&mut self) {
        unsafe {
            let mut cur = self.head.as_ref().next;
            while cur != self.tail {
                let tmp = cur.as_ref().next;
                ptr::drop_in_place(Box::from_raw(cur.as_ptr()).key.as_mut_ptr());
                cur = tmp;
            }
            suture(self.head, self.tail);
        }
    }

    fn remove_tail(&mut self) -> K {
        unsafe {
            let record = self.tail.as_ref().prev;
            cut_out(record);

            let r = Box::from_raw(record.as_ptr());
            r.key.as_ptr().read()
        }
    }

    fn get_tail(&self) -> &K {
        unsafe { self.tail.as_ref().prev.as_ref().key.assume_init_ref() }
    }
}

impl<K> Drop for Trace<K> {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.head.as_ptr()));
            drop(Box::from_raw(self.tail.as_ptr()));
        }
    }
}

pub trait SizePolicy<K, V> {
    fn current(&self) -> usize;
    fn on_insert(&mut self, key: &K, value: &V);
    fn on_remove(&mut self, key: &K, value: &V);
    fn on_reset(&mut self, val: usize);
}

#[derive(Default)]
pub struct CountTracker(usize);

impl<K, V> SizePolicy<K, V> for CountTracker {
    fn current(&self) -> usize {
        self.0
    }

    fn on_insert(&mut self, _: &K, _: &V) {
        self.0 += 1;
    }

    fn on_remove(&mut self, _: &K, _: &V) {
        self.0 -= 1;
    }

    fn on_reset(&mut self, val: usize) {
        self.0 = val;
    }
}

/// Some [`EvictPolicy`] (e.g. the `TxnStatusCache` in
/// `tikv::storage::txn::txn_status_cache` module) may need to know what the
/// entry bing popped out is to determine if it really can be popped. But there
/// is performance cost to always get the tail entry. So we pass this interface
/// to the `should_evict` function. An implementation of `EvictPolicy` can read
/// the tail entry only when it really needs.
pub trait GetTailEntry<K, V> {
    fn get_tail_entry(&self) -> Option<(&K, &V)>;
}

/// An [`EvictPolicy`] defines how the [`LruCache`] should determine an entry
/// at the tail should be popped out.
pub trait EvictPolicy<K, V> {
    fn should_evict(
        &self,
        current_size: usize,
        capacity: usize,
        get_tail_entry: &impl GetTailEntry<K, V>,
    ) -> bool;
}

/// The default [`EvictPolicy`] of [`LruCache`], which pops out entries at the
/// tail when the limit specified by `capacity` is exceeded.
pub struct EvictOnFull;

impl<K, V> EvictPolicy<K, V> for EvictOnFull {
    fn should_evict(
        &self,
        current_size: usize,
        capacity: usize,
        _: &impl GetTailEntry<K, V>,
    ) -> bool {
        capacity < current_size
    }
}

pub struct LruCache<K, V, T = CountTracker, E = EvictOnFull>
where
    T: SizePolicy<K, V>,
    E: EvictPolicy<K, V>,
{
    map: HashMap<K, ValueEntry<K, V>>,
    trace: Trace<K>,
    capacity: usize,
    size_policy: T,
    evict_policy: E,
}

impl<K, V, T> LruCache<K, V, T>
where
    T: SizePolicy<K, V>,
{
    pub fn with_capacity_sample_and_trace(
        capacity: usize,
        sample_mask: usize,
        size_policy: T,
    ) -> LruCache<K, V, T> {
        Self::new(capacity, sample_mask, size_policy, EvictOnFull)
    }
}

impl<K, V, T, E> LruCache<K, V, T, E>
where
    T: SizePolicy<K, V>,
    E: EvictPolicy<K, V>,
{
    pub fn new(mut capacity: usize, sample_mask: usize, size_policy: T, evict_policy: E) -> Self {
        // The capacity is at least 1.
        if capacity == 0 {
            capacity = 1;
        }
        Self {
            map: HashMap::default(),
            trace: Trace::new(sample_mask),
            capacity,
            size_policy,
            evict_policy,
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size_policy.current()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.map.clear();
        self.trace.clear();
        self.size_policy.on_reset(0);
    }

    /// Get the capacity limited on the `LruCache`.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the capacity actually allocated by the internal data structure.
    #[inline]
    pub fn internal_allocated_capacity(&self) -> usize {
        self.map.capacity()
    }
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
{
    pub fn with_capacity(capacity: usize) -> LruCache<K, V> {
        LruCache::with_capacity_and_sample(capacity, 0)
    }

    pub fn with_capacity_and_sample(capacity: usize, sample_mask: usize) -> LruCache<K, V> {
        Self::with_capacity_sample_and_trace(capacity, sample_mask, CountTracker::default())
    }
}

impl<K, V, T, E> LruCache<K, V, T, E>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    T: SizePolicy<K, V>,
    E: EvictPolicy<K, V>,
{
    #[inline]
    fn insert_impl(&mut self, key: K, value: V, replace: bool) -> bool {
        let mut inserted = true;
        let mut old_key = None;
        let current_size = SizePolicy::<K, V>::current(&self.size_policy);
        // In case the current size exactly equals to capacity, we also expect to reuse
        // tail when inserting. Use `current_size + 1` to include the case.
        let should_evict_on_insert =
            self.evict_policy
                .should_evict(current_size + 1, self.capacity, self);
        match self.map.entry(key) {
            HashMapEntry::Occupied(mut e) => {
                if replace {
                    self.size_policy.on_remove(e.key(), &e.get().value);
                    self.size_policy.on_insert(e.key(), &value);
                    let entry = e.get_mut();
                    self.trace.promote(entry.record);
                    entry.value = value;
                } else {
                    inserted = false;
                }
            }
            HashMapEntry::Vacant(v) => {
                let record = if should_evict_on_insert {
                    let res = self.trace.reuse_tail(v.key().clone());
                    old_key = Some(res.0);
                    res.1
                } else {
                    self.trace.create(v.key().clone())
                };

                self.size_policy.on_insert(v.key(), &value);
                v.insert(ValueEntry { value, record });
            }
        }
        if let Some(o) = old_key {
            let entry = self.map.remove(&o).unwrap();
            self.size_policy.on_remove(&o, &entry.value);
        }

        // NOTE: now when inserting a value larger than the capacity, actually this
        // implementation will clean the whole cache.
        // Perhaps we can reject entries larger than capacity goes in the LRU cache, but
        // that is impossible for now: the `SizePolicy` trait doesn't provide the
        // interface of querying the actual size of an item.
        self.evict_until_fit();
        inserted
    }

    fn evict_until_fit(&mut self) {
        let cap = self.capacity;
        loop {
            let current_size = self.size_policy.current();
            // Should we keep at least one entry? So our users won't lose their fresh record
            // once it exceeds the capacity.
            if !self.evict_policy.should_evict(current_size, cap, self) || self.map.is_empty() {
                break;
            }
            let key = self.trace.remove_tail();
            let val = self.map.remove(&key).unwrap();
            self.size_policy.on_remove(&key, &val.value);
        }
    }

    #[inline]
    pub fn insert(&mut self, key: K, value: V) {
        self.insert_impl(key, value, true);
    }

    /// Insert an entry if the key doesn't exist before. The existing entry
    /// won't be replaced and won't be promoted to the most-recent place.
    #[inline]
    pub fn insert_if_not_exist(&mut self, key: K, value: V) -> bool {
        self.insert_impl(key, value, false)
    }

    #[inline]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(v) = self.map.remove(key) {
            self.trace.delete(v.record);
            self.size_policy.on_remove(key, &v.value);
            return Some(v.value);
        }
        None
    }

    #[inline]
    pub fn get(&mut self, key: &K) -> Option<&V> {
        match self.map.get(key) {
            Some(v) => {
                self.trace.maybe_promote(v.record);
                Some(&v.value)
            }
            None => None,
        }
    }

    /// Get an item by key without promoting the item.
    #[inline]
    pub fn get_no_promote(&self, key: &K) -> Option<&V> {
        self.map.get(key).map(|v| &v.value)
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.get_no_promote(key).is_some()
    }

    #[inline]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        match self.map.get_mut(key) {
            Some(v) => {
                self.trace.maybe_promote(v.record);
                Some(&mut v.value)
            }
            None => None,
        }
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            base: self.map.iter(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    #[inline]
    pub fn resize(&mut self, mut new_cap: usize) {
        if new_cap == 0 {
            new_cap = 1;
        }
        if new_cap < self.capacity && self.size() > new_cap {
            while self.size() > new_cap {
                let key = self.trace.remove_tail();
                let entry = self.map.remove(&key).unwrap();
                self.size_policy.on_remove(&key, &entry.value);
            }
            self.map.shrink_to_fit();
        }
        self.capacity = new_cap;
    }
}

impl<K, V, T, E> GetTailEntry<K, V> for LruCache<K, V, T, E>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    T: SizePolicy<K, V>,
    E: EvictPolicy<K, V>,
{
    fn get_tail_entry(&self) -> Option<(&K, &V)> {
        if self.is_empty() {
            return None;
        }

        let k = self.trace.get_tail();
        self.map
            .get_key_value(k)
            .map(|(k, entry)| (k, &entry.value))
    }
}

unsafe impl<K, V, T, E> Send for LruCache<K, V, T, E>
where
    K: Send,
    V: Send,
    T: Send + SizePolicy<K, V>,
    E: Send + EvictPolicy<K, V>,
{
}

impl<K, V, T, E> Drop for LruCache<K, V, T, E>
where
    T: SizePolicy<K, V>,
    E: EvictPolicy<K, V>,
{
    fn drop(&mut self) {
        self.clear();
    }
}

pub struct Iter<'a, K, V> {
    base: std::collections::hash_map::Iter<'a, K, ValueEntry<K, V>>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.base.next().map(|(k, v)| (k, &v.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 0..5 {
            map.insert(i, i);
        }
        for i in 10..15 {
            map.insert(i, i);
        }
        for i in 0..5 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 10..15 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 5..10 {
            assert_eq!(map.get(&i), None);
        }
    }

    #[test]
    fn test_query() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 0..5 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 10..15 {
            map.insert(i, i);
        }
        for i in 0..5 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 10..15 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 5..10 {
            assert_eq!(map.get(&i), None);
        }
    }

    #[test]
    fn test_empty() {
        let mut map = LruCache::with_capacity(0);
        map.insert(2, 4);
        assert_eq!(map.get(&2), Some(&4));
        map.insert(3, 5);
        assert_eq!(map.get(&3), Some(&5));
        assert_eq!(map.get(&2), None);

        map.resize(1);
        map.insert(2, 4);
        assert_eq!(map.get(&2), Some(&4));
        assert_eq!(map.get(&3), None);
        map.insert(3, 5);
        assert_eq!(map.get(&3), Some(&5));
        assert_eq!(map.get(&2), None);

        map.remove(&3);
        assert_eq!(map.get(&3), None);
        map.insert(2, 4);
        assert_eq!(map.get(&2), Some(&4));

        map.resize(0);
        assert_eq!(map.get(&2), Some(&4));
        map.insert(3, 5);
        assert_eq!(map.get(&3), Some(&5));
        assert_eq!(map.get(&2), None);
    }

    #[test]
    fn test_remove() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in (0..3).chain(8..10) {
            map.remove(&i);
        }
        for i in 10..15 {
            map.insert(i, i);
        }
        for i in (0..3).chain(8..10) {
            assert_eq!(map.get(&i), None);
        }
        for i in (3..8).chain(10..15) {
            assert_eq!(map.get(&i), Some(&i));
        }
    }

    #[test]
    fn test_resize() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        map.resize(5);
        for i in 0..5 {
            assert_eq!(map.get(&i), None);
        }
        for i in 5..10 {
            assert_eq!(map.get(&i), Some(&i));
        }
        assert_eq!(map.capacity(), 5);

        map.resize(10);
        assert_eq!(map.capacity(), 10);

        map.resize(5);
        for i in 5..10 {
            assert_eq!(map.get(&i), Some(&i));
        }
        assert_eq!(map.capacity(), 5);

        map.resize(10);
        for i in 10..15 {
            map.insert(i, i);
        }
        for i in 5..15 {
            assert_eq!(map.get(&i), Some(&i));
        }
        assert_eq!(map.capacity(), 10);
    }

    #[test]
    fn test_sample() {
        let mut map = LruCache::with_capacity_and_sample(10, 7);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 0..10 {
            assert_eq!(map.get(&i), Some(&i));
        }
        for i in 10..19 {
            map.insert(i, i);
        }
        for i in (0..7).chain(8..10) {
            assert_eq!(map.get(&i), None);
        }
        for i in (7..8).chain(10..19) {
            assert_eq!(map.get(&i), Some(&i));
        }
    }

    #[test]
    fn test_clear() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 0..10 {
            assert_eq!(map.get(&i), Some(&i));
        }
        map.clear();
        for i in 0..10 {
            assert_eq!(map.get(&i), None);
        }
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 0..10 {
            assert_eq!(map.get(&i), Some(&i));
        }
    }

    struct TestTracker(usize);
    impl SizePolicy<usize, Vec<u8>> for TestTracker {
        fn current(&self) -> usize {
            self.0
        }

        fn on_insert(&mut self, _: &usize, value: &Vec<u8>) {
            let size = value.len();
            self.0 += size;
        }

        fn on_remove(&mut self, _: &usize, value: &Vec<u8>) {
            let size = value.len();
            self.0 -= size;
        }

        fn on_reset(&mut self, val: usize) {
            self.0 = val;
        }
    }

    #[test]
    fn test_tracker() {
        let mut map = LruCache::with_capacity_sample_and_trace(10, 0, TestTracker(0));
        for i in 0..10 {
            map.insert(i, vec![b' ']);
            assert_eq!(
                SizePolicy::<usize, Vec<u8>>::current(&map.size_policy),
                i + 1
            )
        }
        for i in 0..10 {
            assert_eq!(map.get(&i), Some(&vec![b' ']));
        }
        for i in 10..20 {
            map.insert(i, vec![b' ']);
            assert_eq!(SizePolicy::<usize, Vec<u8>>::current(&map.size_policy), 10)
        }
        for i in 0..10 {
            assert_eq!(map.get(&i), None);
        }
        for i in 10..20 {
            assert_eq!(map.get(&i), Some(&vec![b' ']));
        }
    }

    #[test]
    fn test_oversized() {
        let mut cache = LruCache::with_capacity_sample_and_trace(42, 0, TestTracker(0));
        cache.insert(
            42,
            b"this is the answer... but will it being inserted?".to_vec(),
        );
        assert!(cache.size() <= 42);
        cache.insert(42, b"Aha, perhaps an shorter answer.".to_vec());
        assert!(cache.size() <= 42);
        cache.insert(43, b"Yet a new challenger.".to_vec());
        assert!(cache.size() <= 42);

        for i in 0..100 {
            cache.insert(i, vec![i as _]);
            assert!(cache.size() <= 42);
        }
        for i in 90..200 {
            cache.insert(i, vec![i as _; 8]);
            assert!(cache.size() <= 42);
        }
    }

    #[test]
    fn test_get_no_promote() {
        let mut cache = LruCache::with_capacity_sample_and_trace(3, 0, CountTracker::default());
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);
        assert_eq!(cache.size(), 3);
        assert_eq!(*cache.get_no_promote(&1).unwrap(), 1);
        cache.insert(4, 4);
        assert_eq!(cache.size(), 3);
        // Key 1 is not promoted, so it's popped out first.
        assert!(cache.get_no_promote(&1).is_none());
        // Other entries are not affected.
        assert_eq!(*cache.get_no_promote(&2).unwrap(), 2);
        assert_eq!(*cache.get_no_promote(&3).unwrap(), 3);
        assert_eq!(*cache.get_no_promote(&4).unwrap(), 4);
    }

    #[test]
    fn test_insert_if_not_exist() {
        let mut cache = LruCache::with_capacity_sample_and_trace(4, 0, CountTracker::default());
        assert!(cache.insert_if_not_exist(1, 1));
        assert!(cache.insert_if_not_exist(2, 2));
        assert!(cache.insert_if_not_exist(3, 3));
        assert_eq!(cache.size(), 3);
        assert_eq!(*cache.get_no_promote(&1).unwrap(), 1);
        assert_eq!(*cache.get_no_promote(&2).unwrap(), 2);
        assert_eq!(*cache.get_no_promote(&3).unwrap(), 3);

        assert!(!cache.insert_if_not_exist(1, 11));
        // Not updated.
        assert_eq!(*cache.get_no_promote(&1).unwrap(), 1);

        assert!(cache.insert_if_not_exist(4, 4));
        assert!(!cache.insert_if_not_exist(2, 22));
        // Not updated.
        assert_eq!(*cache.get_no_promote(&2).unwrap(), 2);

        assert_eq!(cache.size(), 4);
        assert!(cache.insert_if_not_exist(5, 5));
        assert_eq!(cache.size(), 4);
        // key 1 is not promoted, so it's first popped out.
        assert!(cache.get_no_promote(&1).is_none());
        assert_eq!(*cache.get_no_promote(&2).unwrap(), 2);

        assert!(cache.insert_if_not_exist(6, 6));
        assert_eq!(cache.size(), 4);
        // key 2 is not promoted either, so it's first popped out.
        assert!(cache.get_no_promote(&2).is_none());
        assert_eq!(*cache.get_no_promote(&3).unwrap(), 3);

        assert!(cache.insert_if_not_exist(7, 7));
        assert_eq!(cache.size(), 4);
        assert!(cache.get_no_promote(&3).is_none());
        assert_eq!(*cache.get_no_promote(&4).unwrap(), 4);
    }
}
