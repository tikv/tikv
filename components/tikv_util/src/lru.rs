// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collections::{HashMap, HashMapEntry};
use std::hash::Hash;
use std::mem;
use std::ptr::NonNull;

struct Record<K> {
    prev: Option<NonNull<Record<K>>>,
    next: Option<NonNull<Record<K>>>,
    key: K,
}

struct ValueEntry<K, V> {
    value: V,
    record: NonNull<Record<K>>,
}

struct Trace<K> {
    head: Option<NonNull<Record<K>>>,
    tail: Option<NonNull<Record<K>>>,
}

impl<K> Trace<K> {
    fn promote(&mut self, mut record: NonNull<Record<K>>) {
        unsafe {
            if let Some(mut prev) = record.as_mut().prev {
                prev.as_mut().next = record.as_mut().next;
                if let Some(mut next) = record.as_mut().next {
                    next.as_mut().prev = Some(prev);
                } else {
                    self.tail = Some(prev);
                }
                self.head.unwrap().as_mut().prev = Some(record);
                record.as_mut().next = self.head;
                record.as_mut().prev = None;
                self.head = Some(record);
            }
        }
    }

    fn delete(&mut self, mut record: NonNull<Record<K>>) {
        unsafe {
            if let Some(mut prev) = record.as_mut().prev {
                prev.as_mut().next = record.as_mut().next;
                if let Some(mut next) = record.as_mut().next {
                    next.as_mut().prev = Some(prev);
                } else {
                    self.tail = Some(prev);
                }
            } else {
                self.head = record.as_mut().next;
                if self.head.is_none() {
                    self.tail = None;
                }
            }
            Box::from_raw(record.as_ptr());
        }
    }

    fn create(&mut self, key: K) -> NonNull<Record<K>> {
        let record = Box::leak(Box::new(Record {
            prev: None,
            next: self.head,
            key,
        }))
        .into();
        if let Some(mut head) = self.head {
            unsafe {
                head.as_mut().prev = Some(record);
            }
        }
        self.head = Some(record);
        if self.tail.is_none() {
            self.tail = self.head;
        }
        record
    }

    fn reuse_tail(&mut self, key: K) -> (K, NonNull<Record<K>>) {
        let mut tail = self.tail.unwrap();
        unsafe {
            if self.tail != self.head {
                tail.as_mut().prev.unwrap().as_mut().next = None;
                self.tail = tail.as_mut().prev;
                self.head.unwrap().as_mut().prev = Some(tail);

                tail.as_mut().prev = None;
                tail.as_mut().next = self.head;
                self.head = Some(tail);
            }
            let old_key = mem::replace(&mut tail.as_mut().key, key);
            (old_key, tail)
        }
    }

    fn clear(&mut self) {
        let mut cur = self.head;
        while cur.is_some() {
            unsafe {
                let tmp = cur.unwrap().as_mut().next;
                Box::from_raw(cur.unwrap().as_ptr());
                cur = tmp;
            }
        }
    }
}

pub struct LruCache<K, V> {
    map: HashMap<K, ValueEntry<K, V>>,
    trace: Trace<K>,
    capacity: usize,
}

impl<K, V> LruCache<K, V> {
    pub fn with_capacity(mut capacity: usize) -> LruCache<K, V> {
        if capacity < 2 {
            capacity = 2;
        }
        LruCache {
            map: HashMap::with_capacity_and_hasher(capacity, Default::default()),
            trace: Trace {
                head: None,
                tail: None,
            },
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
        if new_cap < 2 {
            new_cap = 2;
        }
        if new_cap < self.capacity && self.map.len() > new_cap {
            for _ in new_cap..self.map.len() {
                let record = self.trace.tail.unwrap();
                self.map.remove(unsafe { &record.as_ref().key });
                self.trace.delete(record);
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
    pub fn remove(&mut self, key: &K) {
        if let Some(v) = self.map.remove(key) {
            self.trace.delete(v.record);
        }
    }

    #[inline]
    pub fn get(&mut self, key: &K) -> Option<&V> {
        match self.map.get(key) {
            Some(v) => {
                self.trace.promote(v.record);
                Some(&v.value)
            }
            None => None,
        }
    }
}

unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        self.clear();
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
    fn test_remove() {
        let mut map = LruCache::with_capacity(10);
        for i in 0..10 {
            map.insert(i, i);
        }
        for i in 5..10 {
            map.remove(&i);
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
        assert_eq!(map.capacity(), 10);

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
}
