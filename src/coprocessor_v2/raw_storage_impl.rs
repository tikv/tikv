// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::KvFormat;
use async_trait::async_trait;
use coprocessor_plugin_api::*;
use futures::channel::oneshot::Canceled;
use kvproto::kvrpcpb::Context;
use std::ops::Range;
use tikv_util::future::paired_future_callback;

use crate::storage::errors::extract_kv_pairs;
use crate::storage::kv::{Error as KvError, ErrorInner as KvErrorInner};
use crate::storage::{self, lock_manager::LockManager, Engine, Storage};

/// Implementation of the [`RawStorage`] trait.
///
/// It wraps TiKV's [`Storage`] into an API that is exposed to coprocessor plugins.
/// The `RawStorageImpl` should be constructed for every invocation of a [`CoprocessorPlugin`] as
/// it wraps a [`Context`] that is unique for every request.
pub struct RawStorageImpl<'a, E: Engine, L: LockManager, F: KvFormat> {
    context: Context,
    storage: &'a Storage<E, L, F>,
}

impl<'a, E: Engine, L: LockManager, F: KvFormat> RawStorageImpl<'a, E, L, F> {
    /// Constructs a new `RawStorageImpl` that wraps a given [`Context`] and [`Storage`].
    pub fn new(context: Context, storage: &'a Storage<E, L, F>) -> Self {
        RawStorageImpl { context, storage }
    }
}

#[async_trait(?Send)]
impl<E: Engine, L: LockManager, F: KvFormat> RawStorage for RawStorageImpl<'_, E, L, F> {
    async fn get(&self, key: Key) -> PluginResult<Option<Value>> {
        let ctx = self.context.clone();

        let res = self.storage.raw_get(ctx, String::new(), key);

        let value = res.await.map_err(PluginErrorShim::from)?;
        Ok(value)
    }

    async fn batch_get(&self, keys: Vec<Key>) -> PluginResult<Vec<KvPair>> {
        let ctx = self.context.clone();

        let res = self.storage.raw_batch_get(ctx, String::new(), keys);

        let v = res.await.map_err(PluginErrorShim::from)?;
        let kv_pairs = extract_kv_pairs(Ok(v))
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();
        Ok(kv_pairs)
    }

    async fn scan(&self, key_range: Range<Key>) -> PluginResult<Vec<Value>> {
        let ctx = self.context.clone();
        let key_only = false;
        let reverse = false;

        let res = self.storage.raw_scan(
            ctx,
            String::new(),
            key_range.start,
            Some(key_range.end),
            usize::MAX,
            key_only,
            reverse,
        );

        let v = res.await.map_err(PluginErrorShim::from)?;
        let values = extract_kv_pairs(Ok(v))
            .into_iter()
            .map(|kv| kv.value)
            .collect();
        Ok(values)
    }

    async fn put(&self, key: Key, value: Value) -> PluginResult<()> {
        let ctx = self.context.clone();
        let ttl = 0; // unlimited
        let (cb, f) = paired_future_callback();

        let res = self
            .storage
            .raw_put(ctx, String::new(), key, value, ttl, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(PluginErrorShim::from)?,
        }
        .map_err(PluginErrorShim::from)?;
        Ok(())
    }

    async fn batch_put(&self, kv_pairs: Vec<KvPair>) -> PluginResult<()> {
        let ctx = self.context.clone();
        let ttls = vec![0; kv_pairs.len()]; // unlimited
        let (cb, f) = paired_future_callback();

        let res = self
            .storage
            .raw_batch_put(ctx, String::new(), kv_pairs, ttls, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(PluginErrorShim::from)?,
        }
        .map_err(PluginErrorShim::from)?;
        Ok(())
    }

    async fn delete(&self, key: Key) -> PluginResult<()> {
        let ctx = self.context.clone();
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_delete(ctx, String::new(), key, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(PluginErrorShim::from)?,
        }
        .map_err(PluginErrorShim::from)?;
        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<Key>) -> PluginResult<()> {
        let ctx = self.context.clone();
        let (cb, f) = paired_future_callback();

        let res = self.storage.raw_batch_delete(ctx, String::new(), keys, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(PluginErrorShim::from)?,
        }
        .map_err(PluginErrorShim::from)?;
        Ok(())
    }

    async fn delete_range(&self, key_range: Range<Key>) -> PluginResult<()> {
        let ctx = self.context.clone();

        let (cb, f) = paired_future_callback();

        let res =
            self.storage
                .raw_delete_range(ctx, String::new(), key_range.start, key_range.end, cb);

        match res {
            Err(e) => Err(e),
            Ok(_) => f.await.map_err(PluginErrorShim::from)?,
        }
        .map_err(PluginErrorShim::from)?;
        Ok(())
    }
}

/// Helper struct for converting between [`storage::errors::Error`] and
/// [`coprocessor_plugin_api::PluginError`].
struct PluginErrorShim(PluginError);

impl From<PluginErrorShim> for PluginError {
    fn from(err_shim: PluginErrorShim) -> Self {
        err_shim.0
    }
}

impl From<storage::errors::Error> for PluginErrorShim {
    fn from(error: storage::errors::Error) -> Self {
        let inner = match *error.0 {
            // Key not in region
            storage::errors::ErrorInner::Kv(KvError(box KvErrorInner::Request(ref req_err)))
                if req_err.has_key_not_in_region() =>
            {
                let key_err = req_err.get_key_not_in_region();
                PluginError::KeyNotInRegion {
                    key: key_err.get_key().to_owned(),
                    region_id: key_err.get_region_id(),
                    start_key: key_err.get_start_key().to_owned(),
                    end_key: key_err.get_end_key().to_owned(),
                }
            }
            // Timeout
            storage::errors::ErrorInner::Kv(KvError(box KvErrorInner::Timeout(duration))) => {
                PluginError::Timeout(duration)
            }
            // Other errors are passed as-is inside their `Result` so we get a `&Result` when using `Any::downcast_ref`.
            _ => PluginError::Other(
                format!("{}", &error),
                Box::new(storage::Result::<()>::Err(error)),
            ),
        };
        PluginErrorShim(inner)
    }
}

impl From<Canceled> for PluginErrorShim {
    fn from(_c: Canceled) -> Self {
        PluginErrorShim(PluginError::Canceled)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::{lock_manager::DummyLockManager, TestStorageBuilder};
    use api_version::ApiV2;
    use kvproto::kvrpcpb::{ApiVersion, Context};

    #[tokio::test]
    async fn test_storage_api() {
        let storage = TestStorageBuilder::<_, _, ApiV2>::new(DummyLockManager)
            .build()
            .unwrap();
        let ctx = Context {
            api_version: ApiVersion::V2,
            ..Default::default()
        };

        let raw_storage = RawStorageImpl::new(ctx, &storage);

        let key = b"r\0k1".to_vec();
        let val1 = vec![42];
        let val2 = vec![43];

        // Put
        raw_storage.put(key.clone(), val1.clone()).await.unwrap();

        // Get
        let r = raw_storage.get(key.clone()).await.unwrap();
        assert_eq!(r, Some(val1.clone()));

        // Put overwrite
        raw_storage.put(key.clone(), val2.clone()).await.unwrap();
        let r = raw_storage.get(key.clone()).await.unwrap();
        assert_eq!(r, Some(val2.clone()));

        // Delete
        raw_storage.delete(key.clone()).await.unwrap();

        // Get non-existent
        let r = raw_storage.get(key.clone()).await.unwrap();
        assert_eq!(r, None);
    }

    #[tokio::test]
    async fn test_storage_api_batch() {
        let storage = TestStorageBuilder::<_, _, ApiV2>::new(DummyLockManager)
            .build()
            .unwrap();
        let ctx = Context {
            api_version: ApiVersion::V2,
            ..Default::default()
        };

        let raw_storage = RawStorageImpl::new(ctx, &storage);

        let keys = vec![b"r\0k1".to_vec(), b"r\0k4".to_vec(), b"r\0k8".to_vec()];
        let values = vec![42, 99, 128].into_iter().map(|v| vec![v]);
        let non_existing_key = vec![b"r\0k3".to_vec()];

        let full_scan = Range {
            start: b"r\x00".to_vec(),
            end: b"r\x01".to_vec(),
        };

        // Batch put
        raw_storage
            .batch_put(keys.clone().into_iter().zip(values.clone()).collect())
            .await
            .unwrap();

        // Batch get
        let r = raw_storage
            .batch_get(keys.clone().into_iter().take(2).collect())
            .await
            .unwrap();
        assert_eq!(
            r,
            keys.clone()
                .into_iter()
                .take(2)
                .zip(values.clone())
                .collect::<Vec<(Vec<u8>, Vec<u8>)>>()
        );

        // Batch get (one non-existent)
        let r = raw_storage
            .batch_get(
                keys.clone()
                    .into_iter()
                    .take(1)
                    .chain(non_existing_key.clone())
                    .collect(),
            )
            .await
            .unwrap();
        assert_eq!(
            r,
            keys.clone()
                .into_iter()
                .take(1)
                .zip(values.clone())
                .collect::<Vec<(Vec<u8>, Vec<u8>)>>()
        );

        // Full scan
        let r = raw_storage.scan(full_scan.clone()).await.unwrap();
        assert_eq!(r.len(), 3);

        // Batch delete (one non-existent)
        raw_storage
            .batch_delete(
                keys.clone()
                    .into_iter()
                    .take(2)
                    .chain(non_existing_key.clone())
                    .collect(),
            )
            .await
            .unwrap();
        let r = raw_storage.scan(full_scan.clone()).await.unwrap();
        assert_eq!(r.len(), 1);

        // Batch put (one overwrite)
        raw_storage
            .batch_put(keys.clone().into_iter().zip(values.clone()).collect())
            .await
            .unwrap();
        let r = raw_storage.scan(full_scan.clone()).await.unwrap();
        assert_eq!(r.len(), 3);

        // Delete range (all)
        raw_storage.delete_range(full_scan.clone()).await.unwrap();
        let r = raw_storage.scan(full_scan.clone()).await.unwrap();
        assert_eq!(r.len(), 0);
    }
}
