// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
};

use api_version::{ApiV1, KvFormat};
use collections::HashMap;
use futures::executor::block_on;
use kvproto::kvrpcpb::{ChecksumAlgorithm, Context, GetRequest, KeyRange, LockInfo, RawGetRequest};
use raftstore::{coprocessor::RegionInfoProvider, router::RaftStoreBlackHole};
use tikv::{
    server::gc_worker::{AutoGcConfig, GcConfig, GcSafePointProvider, GcWorker},
    storage::{
        config::Config, kv::RocksEngine, lock_manager::DummyLockManager, test_util::GetConsumer,
        txn::commands, Engine, KvGetStatistics, PrewriteResult, Result, Storage, TestEngineBuilder,
        TestStorageBuilder, TxnStatus,
    },
};
use tikv_util::time::Instant;
use txn_types::{Key, KvPair, Mutation, TimeStamp, Value};

/// A builder to build a `SyncTestStorage`.
///
/// Only used for test purpose.
pub struct SyncTestStorageBuilder<E: Engine, F: KvFormat> {
    engine: E,
    config: Option<Config>,
    gc_config: Option<GcConfig>,
    _phantom: PhantomData<F>,
}

/// SyncTestStorageBuilder for Api V1
/// To be convenience for test cases unrelated to RawKV.
pub type SyncTestStorageBuilderApiV1<E> = SyncTestStorageBuilder<E, ApiV1>;

impl<F: KvFormat> SyncTestStorageBuilder<RocksEngine, F> {
    pub fn new() -> Self {
        Self {
            engine: TestEngineBuilder::new()
                .api_version(F::TAG)
                .build()
                .unwrap(),
            config: None,
            gc_config: None,
            _phantom: PhantomData,
        }
    }
}

impl Default for SyncTestStorageBuilder<RocksEngine, ApiV1> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Engine, F: KvFormat> SyncTestStorageBuilder<E, F> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            config: None,
            gc_config: None,
            _phantom: PhantomData,
        }
    }

    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    #[must_use]
    pub fn gc_config(mut self, gc_config: GcConfig) -> Self {
        self.gc_config = Some(gc_config);
        self
    }

    pub fn build(mut self) -> Result<SyncTestStorage<E, F>> {
        let mut builder = TestStorageBuilder::<_, _, F>::from_engine_and_lock_mgr(
            self.engine.clone(),
            DummyLockManager,
        );
        if let Some(config) = self.config.take() {
            builder = builder.config(config);
        }
        builder = builder.set_api_version(F::TAG);
        SyncTestStorage::from_storage(builder.build()?, self.gc_config.unwrap_or_default())
    }
}

/// A `Storage` like structure with sync API.
///
/// Only used for test purpose.
#[derive(Clone)]
pub struct SyncTestStorage<E: Engine, F: KvFormat> {
    gc_worker: GcWorker<E, RaftStoreBlackHole>,
    store: Storage<E, DummyLockManager, F>,
}

/// SyncTestStorage for Api V1
/// To be convenience for test cases unrelated to RawKV.
pub type SyncTestStorageApiV1<E> = SyncTestStorage<E, ApiV1>;

impl<E: Engine, F: KvFormat> SyncTestStorage<E, F> {
    pub fn from_storage(
        storage: Storage<E, DummyLockManager, F>,
        config: GcConfig,
    ) -> Result<Self> {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut gc_worker = GcWorker::new(
            storage.get_engine(),
            RaftStoreBlackHole,
            tx,
            config,
            Default::default(),
        );
        gc_worker.start()?;
        Ok(Self {
            gc_worker,
            store: storage,
        })
    }

    pub fn start_auto_gc<S: GcSafePointProvider, R: RegionInfoProvider + Clone + 'static>(
        &mut self,
        cfg: AutoGcConfig<S, R>,
    ) {
        self.gc_worker
            .start_auto_gc(cfg, Arc::new(AtomicU64::new(0)))
            .unwrap();
    }

    pub fn get_storage(&self) -> Storage<E, DummyLockManager, F> {
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
    ) -> Result<(Option<Value>, KvGetStatistics)> {
        block_on(self.store.get(ctx, key.to_owned(), start_ts.into()))
    }

    #[allow(dead_code)]
    pub fn batch_get(
        &self,
        ctx: Context,
        keys: &[Key],
        start_ts: impl Into<TimeStamp>,
    ) -> Result<(Vec<Result<KvPair>>, KvGetStatistics)> {
        block_on(self.store.batch_get(ctx, keys.to_owned(), start_ts.into()))
    }

    #[allow(clippy::type_complexity)]
    pub fn batch_get_command(
        &self,
        ctx: Context,
        keys: &[&[u8]],
        start_ts: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut ids = vec![];
        let requests: Vec<GetRequest> = keys
            .iter()
            .copied()
            .map(|key| {
                let mut req = GetRequest::default();
                req.set_context(ctx.clone());
                req.set_key(key.to_owned());
                req.set_version(start_ts);
                ids.push(ids.len() as u64);
                req
            })
            .collect();
        let p = GetConsumer::new();
        block_on(
            self.store
                .batch_get_command(requests, ids, p.clone(), Instant::now()),
        )?;
        let mut values = vec![];
        for value in p.take_data().into_iter() {
            values.push(value?);
        }
        Ok(values)
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
        block_on(self.store.scan(
            ctx,
            start_key,
            end_key,
            limit,
            0,
            start_ts.into(),
            key_only,
            false,
        ))
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
        block_on(self.store.scan(
            ctx,
            start_key,
            end_key,
            limit,
            0,
            start_ts.into(),
            key_only,
            true,
        ))
    }

    pub fn prewrite(
        &self,
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<PrewriteResult> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Prewrite::with_context(mutations, primary, start_ts.into(), ctx),
            cb,
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
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Commit::new(keys, start_ts.into(), commit_ts.into(), ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn cleanup(
        &self,
        ctx: Context,
        key: Key,
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.sched_txn_command(
            commands::Cleanup::new(key, start_ts.into(), current_ts.into(), ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn rollback(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        wait_op!(|cb| self
            .store
            .sched_txn_command(commands::Rollback::new(keys, start_ts.into(), ctx), cb))
        .unwrap()
    }

    pub fn scan_locks(
        &self,
        ctx: Context,
        max_ts: impl Into<TimeStamp>,
        start_key: Option<Key>,
        end_key: Option<Key>,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        block_on(
            self.store
                .scan_lock(ctx, max_ts.into(), start_key, end_key, limit),
        )
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
        wait_op!(|cb| self.store.sched_txn_command(
            commands::ResolveLockReadPhase::new(txn_status, None, ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn resolve_lock_batch(
        &self,
        ctx: Context,
        txns: Vec<(TimeStamp, TimeStamp)>,
    ) -> Result<()> {
        let txn_status: HashMap<TimeStamp, TimeStamp> = txns.into_iter().collect();
        wait_op!(|cb| self.store.sched_txn_command(
            commands::ResolveLockReadPhase::new(txn_status, None, ctx),
            cb,
        ))
        .unwrap()
    }

    pub fn gc(&self, _: Context, safe_point: impl Into<TimeStamp>) -> Result<()> {
        wait_op!(|cb| self.gc_worker.gc(safe_point.into(), cb)).unwrap()
    }

    pub fn delete_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        notify_only: bool,
    ) -> Result<()> {
        wait_op!(|cb| self
            .store
            .delete_range(ctx, start_key, end_key, notify_only, cb))
        .unwrap()
    }

    pub fn raw_get(&self, ctx: Context, cf: String, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        block_on(self.store.raw_get(ctx, cf, key))
    }

    pub fn raw_get_key_ttl(&self, ctx: Context, cf: String, key: Vec<u8>) -> Result<Option<u64>> {
        block_on(self.store.raw_get_key_ttl(ctx, cf, key))
    }

    pub fn raw_batch_get(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(self.store.raw_batch_get(ctx, cf, keys))
    }

    pub fn raw_batch_get_command(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut ids = vec![];
        let requests: Vec<RawGetRequest> = keys
            .into_iter()
            .map(|key| {
                let mut req = RawGetRequest::default();
                req.set_context(ctx.clone());
                req.set_key(key);
                req.set_cf(cf.to_owned());
                ids.push(ids.len() as u64);
                req
            })
            .collect();
        let p = GetConsumer::new();
        block_on(self.store.raw_batch_get_command(requests, ids, p.clone()))?;
        let mut values = vec![];
        for value in p.take_data().into_iter() {
            values.push(value?);
        }
        Ok(values)
    }

    pub fn raw_put(&self, ctx: Context, cf: String, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.raw_put(ctx, cf, key, value, 0, cb)).unwrap()
    }

    pub fn raw_batch_put(&self, ctx: Context, cf: String, pairs: Vec<KvPair>) -> Result<()> {
        let ttls = vec![0; pairs.len()];
        wait_op!(|cb| self.store.raw_batch_put(ctx, cf, pairs, ttls, cb)).unwrap()
    }

    pub fn raw_delete(&self, ctx: Context, cf: String, key: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.raw_delete(ctx, cf, key, cb)).unwrap()
    }

    pub fn raw_delete_range(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.raw_delete_range(ctx, cf, start_key, end_key, cb)).unwrap()
    }

    pub fn raw_batch_delete(&self, ctx: Context, cf: String, keys: Vec<Vec<u8>>) -> Result<()> {
        wait_op!(|cb| self.store.raw_batch_delete(ctx, cf, keys, cb)).unwrap()
    }

    pub fn raw_scan(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(
            self.store
                .raw_scan(ctx, cf, start_key, end_key, limit, false, false),
        )
    }

    pub fn reverse_raw_scan(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(
            self.store
                .raw_scan(ctx, cf, start_key, end_key, limit, false, true),
        )
    }

    pub fn raw_batch_scan(
        &self,
        ctx: Context,
        cf: String,
        ranges: Vec<KeyRange>,
        limit: usize,
    ) -> Result<Vec<Result<KvPair>>> {
        block_on(
            self.store
                .raw_batch_scan(ctx, cf, ranges, limit, false, false),
        )
    }

    pub fn raw_compare_and_swap_atomic(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        previous_value: Option<Vec<u8>>,
        value: Vec<u8>,
        ttl: u64,
    ) -> Result<(Option<Vec<u8>>, bool)> {
        wait_op!(|cb| self.store.raw_compare_and_swap_atomic(
            ctx,
            cf,
            key,
            previous_value,
            value,
            ttl,
            cb
        ))
        .unwrap()
    }

    pub fn raw_batch_put_atomic(
        &self,
        ctx: Context,
        cf: String,
        pairs: Vec<KvPair>,
        ttls: Vec<u64>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.raw_batch_put_atomic(ctx, cf, pairs, ttls, cb)).unwrap()
    }

    pub fn raw_batch_delete_atomic(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> Result<()> {
        wait_op!(|cb| self.store.raw_batch_delete_atomic(ctx, cf, keys, cb)).unwrap()
    }

    pub fn raw_checksum(&self, ctx: Context, ranges: Vec<KeyRange>) -> Result<(u64, u64, u64)> {
        block_on(
            self.store
                .raw_checksum(ctx, ChecksumAlgorithm::Crc64Xor, ranges),
        )
    }
}
