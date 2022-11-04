use std::{sync::Arc, time::Duration};

use dashmap::{mapref::entry::Entry, DashMap};
use external_storage_export::ExternalStorage;
use futures::Future;
use kvproto::brpb::StorageBackend;
use tikv_util::time::Instant;

use crate::{metrics::EXT_STORAGE_CACHE_COUNT, Result};

#[derive(Clone, Default)]
pub struct StorageCache(Arc<StorageCacheInner>);

#[derive(Debug, Default)]
struct StorageCacheInner {
    cached: DashMap<String, Cached>,
}

#[derive(Debug)]
struct Cached {
    storage: StoragePool,
    last_used: Instant,
}

impl Cached {
    fn new(pool: StoragePool) -> Self {
        Self {
            storage: pool,
            last_used: Instant::now_coarse(),
        }
    }

    fn storage(&mut self) -> Arc<dyn ExternalStorage> {
        self.last_used = Instant::now_coarse();
        self.storage.get()
    }
}

struct StoragePool(Box<[Arc<dyn ExternalStorage>]>);

impl StoragePool {
    fn create(backend: &StorageBackend, size: usize) -> Result<Self> {
        let mut r = Vec::with_capacity(size);
        for _ in 0..size {
            let s = external_storage_export::create_storage(backend, Default::default())?;
            r.push(Arc::from(s));
        }
        Ok(Self(r.into_boxed_slice()))
    }

    fn get(&self) -> Arc<dyn ExternalStorage> {
        use rand::Rng;
        let idx = rand::thread_rng().gen_range(0..self.0.len());
        Arc::clone(&self.0[idx])
    }
}

impl std::fmt::Debug for StoragePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let url = self
            .get()
            .url()
            .map(|u| u.to_string())
            .unwrap_or_else(|_| "<unknown>".to_owned());
        f.debug_tuple("StoragePool")
            .field(&format_args!("{}", url))
            .finish()
    }
}

impl StorageCache {
    pub fn gc_loop(&self) -> impl Future<Output = ()> + Send + 'static {
        let this = Arc::downgrade(&self.0);
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                match this.upgrade() {
                    Some(inner) => inner.cached.retain(|name, cache| {
                        let need_hold = Instant::now_coarse()
                            .checked_sub(cache.last_used)
                            .unwrap_or(Duration::ZERO)
                            < Duration::from_secs(600);
                        if !need_hold {
                            info!("Removing cache due to expired."; "name" => %name, "entry" => ?cache);
                        }
                        need_hold
                    }),
                    None => return,
                }
            }
        }
    }

    pub fn cached_or_create(
        &self,
        cache_key: &str,
        backend: &StorageBackend,
    ) -> Result<Arc<dyn ExternalStorage>> {
        let s = self.0.cached.get_mut(cache_key);
        match s {
            Some(mut s) => {
                EXT_STORAGE_CACHE_COUNT.with_label_values(&["hit"]).inc();
                Ok(s.value_mut().storage())
            }
            None => {
                drop(s);
                let e = self.0.cached.entry(cache_key.to_owned());
                match e {
                    Entry::Occupied(mut v) => Ok(v.get_mut().storage()),
                    Entry::Vacant(v) => {
                        EXT_STORAGE_CACHE_COUNT.with_label_values(&["miss"]).inc();
                        let pool = StoragePool::create(backend, 16)?;
                        info!("Insert storage cache."; "name" => %cache_key, "pool" => ?pool);
                        let cached = pool.get();
                        v.insert(Cached::new(pool));
                        Ok(cached)
                    }
                }
            }
        }
    }
}
