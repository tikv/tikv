// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use coprocessor_plugin_api::*;
use kvproto::kvrpcpb::Context;
use std::ops::Range;
use tikv_util::future::paired_future_callback;

use crate::storage::errors::{extract_kv_pairs, extract_region_error};
use crate::storage::lock_manager::LockManager;
use crate::storage::{Engine, Storage};

/// Implementation of the [`RawStorage`] trait.
///
/// It wraps TiKV's [`Storage`] into an API that is exposed to coprocessor plugins.
/// The `RawStorageImpl` should be constructed for every invocation of a [`CoprocessorPlugin`] as
/// it wraps a [`Context`] that is unique for every request.
pub struct RawStorageImpl<'a, E: Engine, L: LockManager> {
    context: &'a Context,
    storage: &'a Storage<E, L>,
}

impl<'a, E: Engine, L: LockManager> RawStorageImpl<'a, E, L> {
    /// Constructs a new `RawStorageImpl` that wraps a given [`Context`] and [`Storage`].
    pub fn new(context: &'a Context, storage: &'a Storage<E, L>) -> Self {
        RawStorageImpl { context, storage }
    }
}

#[async_trait(?Send)]
impl<E: Engine, L: LockManager> RawStorage for RawStorageImpl<'_, E, L> {
    async fn get(&self, key: Key) -> StorageResult<Option<Value>> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();

        let res = self.storage.raw_get(ctx, cf, key);

        let v = res.await;
        construct_result(v, |x| x)
    }

    async fn batch_get(&self, keys: Vec<Key>) -> StorageResult<Vec<KvPair>> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();

        let res = self.storage.raw_batch_get(ctx, cf, keys);

        let v = res.await;
        construct_result(v, |x| {
            extract_kv_pairs(Ok(x))
                .into_iter()
                .map(|kv| (kv.key, kv.value))
                .collect()
        })
    }

    async fn scan(&self, key_range: Range<Key>) -> StorageResult<Vec<Value>> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let key_only = false;
        let reverse = false;

        let res = self.storage.raw_scan(
            ctx,
            cf,
            key_range.start,
            Some(key_range.end),
            usize::MAX,
            key_only,
            reverse,
        );

        let v = res.await;
        construct_result(v, |x| {
            extract_kv_pairs(Ok(x))
                .into_iter()
                .map(|kv| kv.value)
                .collect()
        })
    }

    async fn put(&self, key: Key, value: Value) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let ttl = u64::MAX;
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_put(ctx, cf, key, value, ttl, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        construct_result(v, |x| x)
    }

    async fn batch_put(&self, kv_pairs: Vec<KvPair>) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let ttl = u64::MAX;
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_batch_put(ctx, cf, kv_pairs, ttl, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        construct_result(v, |x| x)
    }

    async fn delete(&self, key: Key) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_delete(ctx, cf, key, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        construct_result(v, |x| x)
    }

    async fn batch_delete(&self, keys: Vec<Key>) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_batch_delete(ctx, cf, keys, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        construct_result(v, |x| x)
    }

    async fn delete_range(&self, key_range: Range<Key>) -> StorageResult<()> {
        let ctx = self.context.clone();
        let cf = engine_traits::CF_DEFAULT.to_string();

        let (cb, f) = paired_future_callback();

        let res = self
            .storage
            .raw_delete_range(ctx, cf, key_range.start, key_range.end, cb);

        let v = match res {
            Err(e) => Err(e),
            Ok(_) => f.await.expect("future got canceled"),
        };
        construct_result(v, |x| x)
    }
}

fn construct_result<T, R>(
    storage_result: crate::storage::Result<T>,
    on_ok: impl FnOnce(T) -> R,
) -> StorageResult<R> {
    if let Some(err) = extract_region_error(&storage_result) {
        // TODO: map specific error to StorageError
        match err {
            _ => todo!(),
        }
    } else if let Err(e) = storage_result {
        Err(StorageError::OtherError(format!("{}", e)))
    } else if let Ok(v) = storage_result {
        Ok(on_ok(v))
    } else {
        unreachable!()
    }
}
