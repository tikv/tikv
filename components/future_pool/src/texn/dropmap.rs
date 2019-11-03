use std::sync::atomic::{AtomicPtr, Ordering::SeqCst};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dashmap::{DashMap, DashMapRef, DashMapRefAny};

// swap interval (in secs)
// const SWAP_INTERVAL: u64 = 20;

struct MapPointer<K, V>
where
    K: std::cmp::Eq + std::hash::Hash + Clone + 'static,
    V: 'static,
{
    pub inner: AtomicPtr<DashMap<K, V>>,
}

impl<K, V> MapPointer<K, V>
where
    K: std::cmp::Eq + std::hash::Hash + Clone + 'static,
    V: 'static,
{
    fn new() -> MapPointer<K, V> {
        let map = Box::new(DashMap::default());
        let inner = AtomicPtr::new(Box::into_raw(map));
        MapPointer { inner }
    }
}

impl<K, V> Drop for MapPointer<K, V>
where
    K: std::cmp::Eq + std::hash::Hash + Clone + 'static,
    V: 'static,
{
    fn drop(&mut self) {
        unsafe {
            let _: DashMap<K, V> = *self.inner.load(SeqCst);
        }
    }
}

#[derive(Clone)]
pub struct DropMap<K, V>
where
    K: std::cmp::Eq + std::hash::Hash + Clone + 'static,
    V: 'static,
{
    new: Arc<MapPointer<K, V>>,
    old: Arc<MapPointer<K, V>>,
}

impl<K, V> DropMap<K, V>
where
    K: std::cmp::Eq + std::hash::Hash + Clone + 'static,
    V: 'static,
{
    pub fn new(swap_interval: u64) -> Self {
        let new = Arc::new(MapPointer::new());
        let old = Arc::new(MapPointer::new());
        let new_cpy = new.clone();
        let old_cpy = old.clone();
        thread::spawn(move || loop {
            unsafe {
                thread::sleep(Duration::from_secs(swap_interval));
                let old_ptr = old_cpy.inner.swap(new_cpy.inner.load(SeqCst), SeqCst);
                (*old_ptr).clear();
                new_cpy.inner.store(old_ptr, SeqCst);
            }
        });
        DropMap { new, old }
    }
    pub fn contains_key(&self, key: &K) -> bool {
        unsafe {
            let new = self.new.inner.load(SeqCst);
            if (*new).contains_key(key) {
                true
            } else {
                let old = self.old.inner.load(SeqCst);
                if let Some((_, v)) = (*old).remove(key) {
                    (*new).insert(key.clone(), v);
                    true
                } else {
                    false
                }
            }
        }
    }
    pub fn get_or_insert(&self, key: &K, value: V) -> DashMapRefAny<'_, K, V> {
        unsafe {
            let new = self.new.inner.load(SeqCst);
            (*new).get_or_insert(key, value)
        }
    }
    pub fn get(&self, key: &K) -> Option<DashMapRef<'_, K, V>> {
        unsafe {
            let new = &*self.new.inner.load(SeqCst);
            if let Some(v) = new.get(key) {
                return Some(v);
            }
            let old = &*self.old.inner.load(SeqCst);
            if let Some((_, v)) = old.remove(key) {
                new.insert(key.clone(), v);
                return new.get(key);
            }
            None
        }
    }
}
