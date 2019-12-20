// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use futures::Future;

use kvproto::kvrpcpb::{Context, LockInfo};
use tikv::server::gc_worker::{AutoGcConfig, GcConfig, GcSafePointProvider, GcWorker};
use tikv::storage::config::Config;
use tikv::storage::kv::RocksEngine;
use tikv::storage::lock_manager::DummyLockManager;
use tikv::storage::{
    Engine, RegionInfoProvider, Result, Storage, TestEngineBuilder, TestStorageBuilder, TxnStatus,
};
use tikv_util::collections::HashMap;
use txn_types::{Key, KvPair, Mutation, TimeStamp, Value};

/// A builder to build a `SyncTestStorage`.
///
/// Only used for test purpose.
pub struct SyncTestStorageBuilder<E: Engine> {
    engine: E,
    config: Option<Config>,
    gc_config: Option<GcConfig>,
}

impl SyncTestStorageBuilder<RocksEngine> {
    pub fn new() -> Self {
        Self {
            engine: TestEngineBuilder::new().build().unwrap(),
            config: None,
            gc_config: None,
        }
    }
}

impl<E: Engine> SyncTestStorageBuilder<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            config: None,
            gc_config: None,
        }
    }

    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn gc_config(mut self, gc_config: GcConfig) -> Self {
        self.gc_config = Some(gc_config);
        self
    }

    pub fn build(mut self) -> Result<SyncTestStorage<E>> {
        let mut builder = TestStorageBuilder::from_engine(self.engine.clone());
        if let Some(config) = self.config.take() {
            builder = builder.config(config);
        }
        let mut gc_worker =
            GcWorker::new(self.engine, None, None, self.gc_config.unwrap_or_default());
        gc_worker.start()?;

        Ok(SyncTestStorage {
            store: builder.build()?,
            gc_worker,
        })
    }
}

/// A `Storage` like structure with sync API.
///
/// Only used for test purpose.
#[derive(Clone)]
pub struct SyncTestStorage<E: Engine> {
    gc_worker: GcWorker<E>,
    store: Storage<E, DummyLockManager>,
}

impl<E: Engine> SyncTestStorage<E> {
    pub fn start_auto_gc<S: GcSafePointProvider, R: RegionInfoProvider>(
        &mut self,
        cfg: AutoGcConfig<S, R>,
    ) {
        self.gc_worker.start_auto_gc(cfg).unwrap();
    }

    pub fn get_storage(&self) -> Storage<E, DummyLockManager> {
        self.store.clone()
    }

    pub fn get_engine(&self) -> E {
        self.store.get_engine()
    }

    pub fn get(
        &self,
        ctx: Context,
        key: &Key,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<Option<Value>> {
        self.store.get(ctx, key.to_owned(), start_ts.into()).wait()
    }

    #[allow(dead_code)]
    pub fn batch_get(
        &self,
        ctx: Context,
        keys: &[Key],
        start_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<KvPair>>> {
        self.store
            .batch_get(ctx, keys.to_owned(), start_ts.into())
            .wait()
    }

    pub fn scan(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        key_only: bool,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<KvPair>>> {
        self.store
            .scan(
                ctx,
                start_key,
                end_key,
                limit,
                start_ts.into(),
                key_only,
                false,
            )
            .wait()
    }

    pub fn reverse_scan(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        key_only: bool,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<KvPair>>> {
        self.store
            .scan(
                ctx,
                start_key,
                end_key,
                limit,
                start_ts.into(),
                key_only,
                true,
            )
            .wait()
    }

    pub fn prewrite(
        &self,
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<Vec<Result<()>>> {
        wait_op!(|cb| self.store.prewrite(
            ctx,
            mutations,
            primary,
            start_ts.into(),
            0,
            false,
            0,
            TimeStamp::default(),
            cb
        ))
        .unwrap()
    }

    pub fn commit(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) -> Result<TxnStatus> {
        wait_op!(|cb| self
            .store
            .commit(ctx, keys, start_ts.into(), commit_ts.into(), cb))
        .unwrap()
    }

    pub fn cleanup(
        &self,
        ctx: Context,
        key: Key,
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        wait_op!(|cb| self
            .store
            .cleanup(ctx, key, start_ts.into(), current_ts.into(), cb))
        .unwrap()
    }

    pub fn rollback(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.rollback(ctx, keys, start_ts.into(), cb)).unwrap()
    }

    pub fn scan_locks(
        &self,
        ctx: Context,
        max_ts: impl Into<TimeStamp>,
        start_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        wait_op!(|cb| self
            .store
            .scan_locks(ctx, max_ts.into(), start_key, limit, cb))
        .unwrap()
    }

    pub fn resolve_lock(
        &self,
        ctx: Context,
        start_ts: impl Into<TimeStamp>,
        commit_ts: Option<impl Into<TimeStamp>>,
    ) -> Result<()> {
        let mut txn_status = HashMap::default();
        txn_status.insert(
            start_ts.into(),
            commit_ts.map(Into::into).unwrap_or_else(TimeStamp::zero),
        );
        wait_op!(|cb| self.store.resolve_lock(ctx, txn_status, cb)).unwrap()
    }

    pub fn resolve_lock_batch(
        &self,
        ctx: Context,
        txns: Vec<(TimeStamp, TimeStamp)>,
    ) -> Result<()> {
        let txn_status: HashMap<TimeStamp, TimeStamp> = txns.into_iter().collect();
        wait_op!(|cb| self.store.resolve_lock(ctx, txn_status, cb)).unwrap()
    }

    pub fn gc(&self, ctx: Context, safe_point: impl Into<TimeStamp>) -> Result<()> {
        wait_op!(|cb| self.gc_worker.gc(ctx, safe_point.into(), cb)).unwrap()
    }

    pub fn raw_get(&self, ctx: Context, cf: String, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.store.raw_get(ctx, cf, key).wait()
    }

    pub fn raw_put(&self, ctx: Context, cf: String, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.raw_put(ctx, cf, key, value, cb)).unwrap()
    }

    pub fn raw_delete(&self, ctx: Context, cf: String, key: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.raw_delete(ctx, cf, key, cb)).unwrap()
    }

    pub fn raw_scan(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<Result<KvPair>>> {
        self.store
            .raw_scan(ctx, cf, start_key, end_key, limit, false, false)
            .wait()
    }

    pub fn reverse_raw_scan(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<Result<KvPair>>> {
        self.store
            .raw_scan(ctx, cf, start_key, end_key, limit, false, true)
            .wait()
    }
}
