// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collections::{HashMap, HashMapEntry};
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};

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
    head: Box<Record<K>>,
    tail: Box<Record<K>>,
    tick: usize,
    sample_mask: usize,
}

#[inline]
unsafe fn suture<K>(leading: &mut Record<K>, following: &mut Record<K>) {
    leading.next = NonNull::new_unchecked(following);
    following.prev = NonNull::new_unchecked(leading);
}

#[inline]
unsafe fn cut_out<K>(record: &mut Record<K>) {
    suture(record.prev.as_mut(), record.next.as_mut())
}

impl<K> Trace<K> {
    fn new(sample_mask: usize) -> Trace<K> {
        unsafe {
            let mut head = Box::new(Record {
                prev: NonNull::new_unchecked(1usize as _),
                next: NonNull::new_unchecked(1usize as _),
                key: MaybeUninit::uninit(),
            });
            let mut tail = Box::new(Record {
                prev: NonNull::new_unchecked(1usize as _),
                next: NonNull::new_unchecked(1usize as _),
                key: MaybeUninit::uninit(),
            });
            suture(&mut head, &mut tail);

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

    fn promote(&mut self, mut record: NonNull<Record<K>>) {
        unsafe {
            cut_out(record.as_mut());
            suture(record.as_mut(), &mut self.head.next.as_mut());
            suture(&mut self.head, record.as_mut());
        }
    }

    fn delete(&mut self, mut record: NonNull<Record<K>>) {
        unsafe {
            cut_out(record.as_mut());

            ptr::drop_in_place(Box::from_raw(record.as_ptr()).key.as_mut_ptr());
        }
    }

    fn create(&mut self, key: K) -> NonNull<Record<K>> {
        let record = Box::leak(Box::new(Record {
            prev: unsafe { NonNull::new_unchecked(&mut *self.head) },
            next: self.head.next,
            key: MaybeUninit::new(key),
        }))
        .into();
        unsafe {
            self.head.next.as_mut().prev = record;
            self.head.next = record;
        }
        record
    }

    fn reuse_tail(&mut self, key: K) -> (K, NonNull<Record<K>>) {
        unsafe {
            let mut record = self.tail.prev;
            cut_out(record.as_mut());
            suture(record.as_mut(), self.head.next.as_mut());
            suture(&mut self.head, record.as_mut());

            let old_key = record.as_mut().key.as_ptr().read();
            record.as_mut().key = MaybeUninit::new(key);
            (old_key, record)
        }
    }

    fn clear(&mut self) {
        let mut cur = self.head.next;
        unsafe {
            while cur.as_ptr() != &mut *self.tail {
                let tmp = cur.as_mut().next;
                ptr::drop_in_place(Box::from_raw(cur.as_ptr()).key.as_mut_ptr());
                cur = tmp;
            }
            suture(&mut self.head, &mut self.tail);
        }
    }

    fn remove_tail(&mut self) -> K {
        unsafe {
            let mut record = self.tail.prev;
            cut_out(record.as_mut());

            let r = Box::from_raw(record.as_ptr());
            r.key.as_ptr().read()
        }
    }
}

pub struct LruCache<K, V> {
    map: HashMap<K, ValueEntry<K, V>>,
    trace: Trace<K>,
    capacity: usize,
}

impl<K, V> LruCache<K, V> {
    pub fn with_capacity(capacity: usize) -> LruCache<K, V> {
        LruCache::with_capacity_and_sample(capacity, 0)
    }

    pub fn with_capacity_and_sample(mut capacity: usize, sample_mask: usize) -> LruCache<K, V> {
        if capacity == 0 {
            capacity = 1;
        }
        LruCache {
            map: HashMap::default(),
            trace: Trace::new(sample_mask),
            capacity,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.map.clear();
        self.trace.clear();
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
{
    #[inline]
    pub fn resize(&mut self, mut new_cap: usize) {
        if new_cap == 0 {
            new_cap = 1;
        }
        if new_cap < self.capacity && self.map.len() > new_cap {
            for _ in new_cap..self.map.len() {
                let key = self.trace.remove_tail();
                self.map.remove(&key);
            }
            self.map.shrink_to_fit();
        }
        self.capacity = new_cap;
    }

    #[inline]
    pub fn insert(&mut self, key: K, value: V) {
        let mut old_key = None;
        let map_len = self.map.len();
        match self.map.entry(key) {
            HashMapEntry::Occupied(mut e) => {
                let mut entry = e.get_mut();
                self.trace.promote(entry.record);
                entry.value = value;
            }
            HashMapEntry::Vacant(v) => {
                let record = if self.capacity == map_len {
                    let res = self.trace.reuse_tail(v.key().clone());
                    old_key = Some(res.0);
                    res.1
                } else {
                    self.trace.create(v.key().clone())
                };

                v.insert(ValueEntry { value, record });
            }
        }
        if let Some(o) = old_key {
            self.map.remove(&o);
        }
    }

    #[inline]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(v) = self.map.remove(key) {
            self.trace.delete(v.record);
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

    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            base: self.map.iter(),
        }
    }
}

unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        self.clear();
    }
}

pub struct Iter<'a, K: 'a, V: 'a> {
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
}
