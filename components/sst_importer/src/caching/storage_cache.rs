// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

<<<<<<< HEAD
use external_storage_export::ExternalStorage;
=======
use external_storage::{BackendConfig, ExternalStorage};
>>>>>>> 3387bea551 (BR: add new storage type using google offical rust package. (#19315))
use kvproto::brpb::StorageBackend;

use super::cache_map::{MakeCache, ShareOwned};
use crate::{Error, Result};

impl ShareOwned for StoragePool {
    type Shared = Arc<dyn ExternalStorage>;

    fn share_owned(&self) -> Self::Shared {
        self.get()
    }
}

#[derive(Clone, Default)]
pub struct StorageBackendFactory {
    backend: StorageBackend,
    backend_config: BackendConfig,
}

impl StorageBackendFactory {
    pub fn new(backend: StorageBackend, backend_config: BackendConfig) -> Self {
        Self {
            backend,
            backend_config,
        }
    }
}

impl MakeCache for StorageBackendFactory {
    type Cached = StoragePool;
    type Error = Error;

    fn make_cache(&self) -> Result<Self::Cached> {
        StoragePool::create(&self.backend, &self.backend_config, 16)
    }
}

pub struct StoragePool(Box<[Arc<dyn ExternalStorage>]>);

impl StoragePool {
    fn create(
        backend: &StorageBackend,
        backend_config: &BackendConfig,
        size: usize,
    ) -> Result<Self> {
        let mut r = Vec::with_capacity(size);
        for _ in 0..size {
<<<<<<< HEAD
            let s = external_storage_export::create_storage(backend, Default::default())?;
=======
            let s = external_storage::create_storage(backend, backend_config.clone())?;
>>>>>>> 3387bea551 (BR: add new storage type using google offical rust package. (#19315))
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
