// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use futures::Future;

use kvproto::kvrpcpb::{Context, LockInfo};
use tikv::storage::config::Config;
use tikv::storage::kv::RocksEngine;
use tikv::storage::lock_manager::DummyLockMgr;
use tikv::storage::{
    AutoGCConfig, Engine, GCSafePointProvider, Key, KvPair, Mutation, Options, RegionInfoProvider,
    Result, Storage, Value,
};
use tikv::storage::{TestEngineBuilder, TestStorageBuilder};
use tikv_util::collections::HashMap;

/// A builder to build a `SyncTestStorage`.
///
/// Only used for test purpose.
pub struct SyncTestStorageBuilder<E: Engine> {
    engine: E,
    config: Option<Config>,
}

impl SyncTestStorageBuilder<RocksEngine> {
    pub fn new() -> Self {
        Self {
            engine: TestEngineBuilder::new().build().unwrap(),
            config: None,
        }
    }
}

impl<E: Engine> SyncTestStorageBuilder<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            config: None,
        }
    }

    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn build(mut self) -> Result<SyncTestStorage<E>> {
        let mut builder = TestStorageBuilder::from_engine(self.engine);
        if let Some(config) = self.config.take() {
            builder = builder.config(config);
        }
        Ok(SyncTestStorage {
            store: builder.build()?,
        })
    }
}

/// A `Storage` like structure with sync API.
///
/// Only used for test purpose.
#[derive(Clone)]
pub struct SyncTestStorage<E: Engine> {
    store: Storage<E, DummyLockMgr>,
}

impl<E: Engine> SyncTestStorage<E> {
    pub fn start_auto_gc<S: GCSafePointProvider, R: RegionInfoProvider>(
        &mut self,
        cfg: AutoGCConfig<S, R>,
    ) {
        self.store.start_auto_gc(cfg).unwrap();
    }

    pub fn get_storage(&self) -> Storage<E, DummyLockMgr> {
        self.store.clone()
    }

    pub fn get_engine(&self) -> E {
        self.store.get_engine()
    }

    pub fn get(&self, ctx: Context, key: &Key, start_ts: u64) -> Result<Option<Value>> {
        self.store.async_get(ctx, key.to_owned(), start_ts).wait()
    }

    #[allow(dead_code)]
    pub fn batch_get(
        &self,
        ctx: Context,
        keys: &[Key],
        start_ts: u64,
    ) -> Result<Vec<Result<KvPair>>> {
        self.store
            .async_batch_get(ctx, keys.to_owned(), start_ts)
            .wait()
    }

    pub fn scan(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        key_only: bool,
        start_ts: u64,
    ) -> Result<Vec<Result<KvPair>>> {
        self.store
            .async_scan(
                ctx,
                start_key,
                end_key,
                limit,
                start_ts,
                Options::new(0, false, key_only),
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
        start_ts: u64,
    ) -> Result<Vec<Result<KvPair>>> {
        self.store
            .async_scan(
                ctx,
                start_key,
                end_key,
                limit,
                start_ts,
                Options::new(0, false, key_only).reverse_scan(),
            )
            .wait()
    }

    pub fn prewrite(
        &self,
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
    ) -> Result<Vec<Result<()>>> {
        wait_op!(|cb| self.store.async_prewrite(
            ctx,
            mutations,
            primary,
            start_ts,
            Options::default(),
            cb
        ))
        .unwrap()
    }

    pub fn commit(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
        commit_ts: u64,
    ) -> Result<()> {
        wait_op!(|cb| self.store.async_commit(ctx, keys, start_ts, commit_ts, cb)).unwrap()
    }

    pub fn cleanup(&self, ctx: Context, key: Key, start_ts: u64) -> Result<()> {
        wait_op!(|cb| self.store.async_cleanup(ctx, key, start_ts, cb)).unwrap()
    }

    pub fn rollback(&self, ctx: Context, keys: Vec<Key>, start_ts: u64) -> Result<()> {
        wait_op!(|cb| self.store.async_rollback(ctx, keys, start_ts, cb)).unwrap()
    }

    pub fn scan_locks(
        &self,
        ctx: Context,
        max_ts: u64,
        start_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        wait_op!(|cb| self
            .store
            .async_scan_locks(ctx, max_ts, start_key, limit, cb))
        .unwrap()
    }

    pub fn resolve_lock(&self, ctx: Context, start_ts: u64, commit_ts: Option<u64>) -> Result<()> {
        let mut txn_status = HashMap::default();
        txn_status.insert(start_ts, commit_ts.unwrap_or(0));
        wait_op!(|cb| self.store.async_resolve_lock(ctx, txn_status, cb)).unwrap()
    }

    pub fn resolve_lock_batch(&self, ctx: Context, txns: Vec<(u64, u64)>) -> Result<()> {
        let txn_status: HashMap<u64, u64> = txns.into_iter().collect();
        wait_op!(|cb| self.store.async_resolve_lock(ctx, txn_status, cb)).unwrap()
    }

    pub fn gc(&self, ctx: Context, safe_point: u64) -> Result<()> {
        wait_op!(|cb| self.store.async_gc(ctx, safe_point, cb)).unwrap()
    }

    pub fn raw_get(&self, ctx: Context, cf: String, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.store.async_raw_get(ctx, cf, key).wait()
    }

    pub fn raw_put(&self, ctx: Context, cf: String, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.async_raw_put(ctx, cf, key, value, cb)).unwrap()
    }

    pub fn raw_delete(&self, ctx: Context, cf: String, key: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.async_raw_delete(ctx, cf, key, cb)).unwrap()
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
            .async_raw_scan(ctx, cf, start_key, end_key, limit, false, false)
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
            .async_raw_scan(ctx, cf, start_key, end_key, limit, false, true)
            .wait()
    }
}
