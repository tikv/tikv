// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Borrow,
    fmt::Display,
    hash::Hash,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::{mapref::entry::Entry, DashMap};
use futures::Future;

use crate::metrics::EXT_STORAGE_CACHE_COUNT;

#[derive(Clone)]
pub struct CacheMap<K: Hash + Eq, M: MakeCache>(Arc<CacheMapInner<K, M>>);

impl<K: Hash + Eq, M: MakeCache> Default for CacheMap<K, M> {
    fn default() -> Self {
        Self(Arc::default())
    }
}

impl<K: Hash + Eq, M: MakeCache> CacheMap<K, M> {
    #[cfg(test)]
    pub fn with_inner(inner: CacheMapInner<M>) -> Self {
        Self(Arc::new(inner))
    }
}

pub trait ShareOwned {
    type Shared: 'static;

    fn share_owned(&self) -> Self::Shared;
}

impl<T: Copy + 'static> ShareOwned for T {
    type Shared = T;

    fn share_owned(&self) -> Self::Shared {
        *self
    }
}

pub trait MakeCache: 'static {
    type Cached: std::fmt::Debug + ShareOwned + Send + Sync + 'static;
    type Error;

    fn make_cache(&self) -> std::result::Result<Self::Cached, Self::Error>;
}

#[derive(Debug)]
pub struct CacheMapInner<K: Hash + Eq, C: MakeCache> {
    cached: DashMap<K, Cached<C::Cached>>,
    now: AtomicUsize,

    gc_threshold: usize,
}

impl<K: Hash + Eq, C: MakeCache> Default for CacheMapInner<K, C> {
    fn default() -> Self {
        Self {
            cached: DashMap::default(),
            now: Default::default(),
            gc_threshold: 20,
        }
    }
}

impl<K: Hash + Eq, M: MakeCache> CacheMapInner<K, M> {
    #[cfg(test)]
    pub fn with_gc_threshold(n: usize) -> Self {
        Self {
            gc_threshold: n,
            ..Self::default()
        }
    }
}

#[derive(Debug)]
struct Cached<R> {
    resource: R,
    last_used: usize,
}

impl<R: ShareOwned> Cached<R> {
    fn new(resource: R) -> Self {
        Self {
            resource,
            last_used: 0,
        }
    }

    fn resource_owned(&mut self, now: usize) -> <R as ShareOwned>::Shared {
        self.last_used = now;
        self.resource.share_owned()
    }
}

impl<K: Hash + Eq + Display, M: MakeCache> CacheMapInner<K, M> {
    fn now(&self) -> usize {
        self.now.load(Ordering::SeqCst)
    }

    fn tick(&self) {
        let now = self.now.fetch_add(1usize, Ordering::SeqCst);
        self.cached.retain(|name, cache| {
            let need_hold = now.saturating_sub(cache.last_used) < self.gc_threshold;
            if !need_hold {
                info!("Removing cache due to expired."; "name" => %name, "entry" => ?cache);
            }
            need_hold
        });
    }
}

impl<K: Hash + Eq + Send + Sync + Display + 'static, M: MakeCache> CacheMap<K, M> {
    pub fn gc_loop(&self) -> impl Future<Output = ()> + Send + 'static {
        let this = Arc::downgrade(&self.0);
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                match this.upgrade() {
                    Some(inner) => inner.tick(),
                    None => return,
                }
            }
        }
    }

    pub fn cached_or_create<Key>(
        &self,
        cache_key: &Key,
        backend: &M,
    ) -> std::result::Result<<M::Cached as ShareOwned>::Shared, M::Error>
    where
        Key: ToOwned<Owned = K> + ?Sized + Display + Eq + Hash,
        K: Borrow<Key>,
    {
        let s = self.0.cached.get_mut(cache_key);
        match s {
            Some(mut s) => {
                EXT_STORAGE_CACHE_COUNT.with_label_values(&["hit"]).inc();
                Ok(s.value_mut().resource_owned(self.0.now()))
            }
            None => {
                drop(s);
                let e = self.0.cached.entry(cache_key.to_owned());
                match e {
                    Entry::Occupied(mut v) => {
                        EXT_STORAGE_CACHE_COUNT.with_label_values(&["hit"]).inc();
                        Ok(v.get_mut().resource_owned(self.0.now()))
                    }
                    Entry::Vacant(v) => {
                        EXT_STORAGE_CACHE_COUNT.with_label_values(&["miss"]).inc();
                        let pool = backend.make_cache()?;
                        info!("Insert storage cache."; "name" => %cache_key, "cached" => ?pool);
                        let shared = pool.share_owned();
                        v.insert(Cached::new(pool));
                        Ok(shared)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicBool, Ordering},
    };

    use super::{CacheMap, CacheMapInner, MakeCache};

    #[derive(Default)]
    struct CacheChecker(AtomicBool);

    impl MakeCache for CacheChecker {
        type Cached = ();
        type Error = Infallible;

        fn make_cache(&self) -> std::result::Result<Self::Cached, Self::Error> {
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl CacheChecker {
        fn made_cache(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn test_basic() {
        let cached = CacheMapInner::with_gc_threshold(1);
        let cached = CacheMap::with_inner(cached);

        let check_cache = |key: &str, should_make_cache: bool| {
            let c = CacheChecker::default();
            cached.cached_or_create(key, &c).unwrap();
            assert_eq!(c.made_cache(), should_make_cache);
        };

        check_cache("hello", true);
        check_cache("hello", false);
        check_cache("world", true);

        cached.0.tick();
        check_cache("hello", false);

        cached.0.tick();
        check_cache("world", true);

        cached.0.tick();
        check_cache("hello", true);
    }
}
