// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Interact with persistent storage.
//!
//! The [`Storage`](storage::Storage) structure provides raw and transactional APIs on top of
//! a lower-level [`Engine`](storage::kv::Engine).
//!
//! There are multiple [`Engine`](storage::kv::Engine) implementations, [`RaftKv`](server::raftkv::RaftKv)
//! is used by the [`Server`](server::Server). The [`BTreeEngine`](storage::kv::BTreeEngine) and
//! [`RocksEngine`](storage::RocksEngine) are used for testing only.

pub mod config;
pub mod errors;
pub mod kv;
pub mod lock_manager;
pub(crate) mod metrics;
pub mod mvcc;
pub mod txn;

mod read_pool;
mod types;

pub use self::{
    errors::{get_error_kind_from_header, get_tag_from_header, Error, ErrorHeaderKind, ErrorInner},
    kv::{
        CfStatistics, Cursor, Engine, FlowStatistics, FlowStatsReporter, Iterator, RocksEngine,
        ScanMode, Snapshot, Statistics, TestEngineBuilder,
    },
    read_pool::{build_read_pool, build_read_pool_for_test},
    txn::{ProcessResult, Scanner, SnapshotStore, Store},
    types::{PessimisticLockRes, StorageCallback, TxnStatus},
};

use crate::read_pool::{ReadPool, ReadPoolHandle};
use crate::storage::metrics::CommandKind;
use crate::storage::{
    config::Config,
    kv::{with_tls_engine, Error as EngineError, ErrorInner as EngineErrorInner, Modify},
    lock_manager::{DummyLockManager, LockManager},
    metrics::*,
    txn::{
        commands::{Command, TypedCommand},
        scheduler::Scheduler as TxnScheduler,
    },
    types::StorageCallbackType,
};
use engine_traits::{CfName, ALL_CFS, CF_DEFAULT, DATA_CFS};
use engine_traits::{IterOptions, DATA_KEY_PREFIX_LEN};
use futures::Future;
use futures03::prelude::*;
use kvproto::kvrpcpb::{CommandPri, Context, GetRequest, KeyRange, RawGetRequest};
use raftstore::store::util::build_key_range;
use rand::prelude::*;
use std::sync::{atomic, Arc};
use tikv_util::time::Instant;
use txn_types::{Key, KvPair, TimeStamp, TsSet, Value};

pub type Result<T> = std::result::Result<T, Error>;
pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;

/// [`Storage`] implements transactional KV APIs and raw KV APIs on a given [`Engine`]. An [`Engine`]
/// provides low level KV functionality. [`Engine`] has multiple implementations. When a TiKV server
/// is running, a [`RaftKv`] will be the underlying [`Engine`] of [`Storage`]. The other two types of
/// engines are for test purpose.
///
///[`Storage`] is reference counted and cloning [`Storage`] will just increase the reference counter.
/// Storage resources (i.e. threads, engine) will be released when all references are dropped.
///
/// Notice that read and write methods may not be performed over full data in most cases, i.e. when
/// underlying engine is [`RaftKv`], which limits data access in the range of a single region
/// according to specified `ctx` parameter. However,
/// [`unsafe_destroy_range`](Storage::unsafe_destroy_range) is the only exception. It's
/// always performed on the whole TiKV.
///
/// Operations of [`Storage`] can be divided into two types: MVCC operations and raw operations.
/// MVCC operations uses MVCC keys, which usually consist of several physical keys in different
/// CFs. In default CF and write CF, the key will be memcomparable-encoded and append the timestamp
/// to it, so that multiple versions can be saved at the same time.
/// Raw operations use raw keys, which are saved directly to the engine without memcomparable-
/// encoding and appending timestamp.
pub struct Storage<E: Engine, L: LockManager> {
    // TODO: Too many Arcs, would be slow when clone.
    engine: E,

    sched: TxnScheduler<E, L>,

    /// The thread pool used to run most read operations.
    read_pool: ReadPoolHandle,

    /// How many strong references. Thread pool and workers will be stopped
    /// once there are no more references.
    // TODO: This should be implemented in thread pool and worker.
    refs: Arc<atomic::AtomicUsize>,

    // Fields below are storage configurations.
    max_key_size: usize,

    pessimistic_txn_enabled: bool,
}

impl<E: Engine, L: LockManager> Clone for Storage<E, L> {
    #[inline]
    fn clone(&self) -> Self {
        let refs = self.refs.fetch_add(1, atomic::Ordering::SeqCst);

        trace!(
            "Storage referenced"; "original_ref" => refs
        );

        Self {
            engine: self.engine.clone(),
            sched: self.sched.clone(),
            read_pool: self.read_pool.clone(),
            refs: self.refs.clone(),
            max_key_size: self.max_key_size,
            pessimistic_txn_enabled: self.pessimistic_txn_enabled,
        }
    }
}

impl<E: Engine, L: LockManager> Drop for Storage<E, L> {
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, atomic::Ordering::SeqCst);

        trace!(
            "Storage de-referenced"; "original_ref" => refs
        );

        if refs != 1 {
            return;
        }

        info!("Storage stopped.");
    }
}

macro_rules! check_key_size {
    ($key_iter: expr, $max_key_size: expr, $callback: ident) => {
        for k in $key_iter {
            let key_size = k.len();
            if key_size > $max_key_size {
                $callback(Err(Error::from(ErrorInner::KeyTooLarge(
                    key_size,
                    $max_key_size,
                ))));
                return Ok(());
            }
        }
    };
}

impl<E: Engine, L: LockManager> Storage<E, L> {
    /// Create a `Storage` from given engine.
    pub fn from_engine(
        engine: E,
        config: &Config,
        read_pool: ReadPoolHandle,
        lock_mgr: Option<L>,
        pipelined_pessimistic_lock: bool,
    ) -> Result<Self> {
        let pessimistic_txn_enabled = lock_mgr.is_some();
        let sched = TxnScheduler::new(
            engine.clone(),
            lock_mgr,
            config.scheduler_concurrency,
            config.scheduler_worker_pool_size,
            config.scheduler_pending_write_threshold.0 as usize,
            pipelined_pessimistic_lock,
        );

        info!("Storage started.");

        Ok(Storage {
            engine,
            sched,
            read_pool,
            refs: Arc::new(atomic::AtomicUsize::new(1)),
            max_key_size: config.max_key_size,
            pessimistic_txn_enabled,
        })
    }

    /// Get the underlying `Engine` of the `Storage`.
    pub fn get_engine(&self) -> E {
        self.engine.clone()
    }

    /// Get a snapshot of `engine`.
    fn snapshot(engine: &E, ctx: &Context) -> impl std::future::Future<Output = Result<E::Snap>> {
        let (callback, future) = tikv_util::future::paired_std_future_callback();
        let val = engine.async_snapshot(ctx, callback);
        // make engine not cross yield point
        async move {
            val?; // propagate error
            let (_ctx, result) = future
                .map_err(|cancel| EngineError::from(EngineErrorInner::Other(box_err!(cancel))))
                .await?;
            // map storage::kv::Error -> storage::txn::Error -> storage::Error
            result.map_err(txn::Error::from).map_err(Error::from)
        }
    }

    #[inline]
    fn with_tls_engine<F, R>(f: F) -> R
    where
        F: FnOnce(&E) -> R,
    {
        // Safety: the read pools ensure that a TLS engine exists.
        unsafe { with_tls_engine(f) }
    }

    /// Get value of the given key from a snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn get(
        &self,
        mut ctx: Context,
        key: Key,
        start_ts: TimeStamp,
    ) -> impl Future<Item = Option<Value>, Error = Error> {
        const CMD: CommandKind = CommandKind::get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);

        let res = self.read_pool.spawn_handle(
            async move {
                if let Ok(key) = key.to_owned().into_raw() {
                    tls_collect_qps(ctx.get_region_id(), ctx.get_peer(), &key, &key, false);
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                // The bypass_locks set will be checked at most once. `TsSet::vec` is more efficient
                // here.
                let bypass_locks = TsSet::vec_from_u64s(ctx.take_resolved_locks());
                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut statistics = Statistics::default();
                    let snap_store = SnapshotStore::new(
                        snapshot,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                        bypass_locks,
                        false,
                    );
                    let result = snap_store
                        .get(&key, &mut statistics)
                        // map storage::txn::Error -> storage::Error
                        .map_err(Error::from)
                        .map(|r| {
                            KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                            r
                        });

                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(ctx.get_region_id(), &statistics);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());

                    result
                }
            },
            priority,
            start_ts.into_inner(),
        );

        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }

    /// Get values of a set of keys with seperate context from a snapshot, return a list of `Result`s.
    ///
    /// Only writes that are committed before their respective `start_ts` are visible.
    pub fn batch_get_command(
        &self,
        gets: Vec<PointGetCommand>,
    ) -> impl Future<Item = Vec<Result<Option<Vec<u8>>>>, Error = Error> {
        const CMD: CommandKind = CommandKind::batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let ctx = gets[0].ctx.clone();
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let res = self.read_pool.spawn_handle(
            async move {
                for get in &gets {
                    if let Ok(key) = get.key.to_owned().into_raw() {
                        tls_collect_qps(
                            get.ctx.get_region_id(),
                            get.ctx.get_peer(),
                            &key,
                            &key,
                            false,
                        );
                    }
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut statistics = Statistics::default();
                    let mut snap_store = SnapshotStore::new(
                        snapshot,
                        TimeStamp::zero(),
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                        Default::default(),
                        false,
                    );
                    let mut results = vec![];
                    // TODO: optimize using seek.
                    for mut get in gets {
                        snap_store.set_start_ts(get.ts.unwrap());
                        snap_store.set_isolation_level(get.ctx.get_isolation_level());
                        // The bypass_locks set will be checked at most once. `TsSet::vec`
                        // is more efficient here.
                        snap_store
                            .set_bypass_locks(TsSet::vec_from_u64s(get.ctx.take_resolved_locks()));
                        results.push(
                            snap_store
                                .get(&get.key, &mut statistics)
                                .map_err(Error::from),
                        );
                    }
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());

                    Ok(results)
                }
            },
            priority,
            thread_rng().next_u64(),
        );
        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }

    /// Get values of a set of keys in a batch from the snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn batch_get(
        &self,
        mut ctx: Context,
        keys: Vec<Key>,
        start_ts: TimeStamp,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: CommandKind = CommandKind::batch_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);

        let res = self.read_pool.spawn_handle(
            async move {
                let mut key_ranges = vec![];
                for key in &keys {
                    if let Ok(key) = key.to_owned().into_raw() {
                        key_ranges.push(build_key_range(&key, &key, false));
                    }
                }
                tls_collect_qps_batch(ctx.get_region_id(), ctx.get_peer(), key_ranges);

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                let bypass_locks = TsSet::from_u64s(ctx.take_resolved_locks());
                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();

                    let mut statistics = Statistics::default();
                    let snap_store = SnapshotStore::new(
                        snapshot,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                        bypass_locks,
                        false,
                    );
                    let result = snap_store
                        .batch_get(&keys, &mut statistics)
                        .map_err(Error::from)
                        .map(|v| {
                            let kv_pairs: Vec<_> = v
                                .into_iter()
                                .zip(keys)
                                .filter(|&(ref v, ref _k)| {
                                    !(v.is_ok() && v.as_ref().unwrap().is_none())
                                })
                                .map(|(v, k)| match v {
                                    Ok(Some(x)) => Ok((k.into_raw().unwrap(), x)),
                                    Err(e) => Err(Error::from(e)),
                                    _ => unreachable!(),
                                })
                                .collect();
                            KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                                .get(CMD)
                                .observe(kv_pairs.len() as f64);
                            kv_pairs
                        });

                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(ctx.get_region_id(), &statistics);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());
                    result
                }
            },
            priority,
            start_ts.into_inner(),
        );

        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }

    /// Scan keys in [`start_key`, `end_key`) up to `limit` keys from the snapshot.
    ///
    /// If `end_key` is `None`, it means the upper bound is unbounded.
    ///
    /// Only writes committed before `start_ts` are visible.
    pub fn scan(
        &self,
        mut ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        start_ts: TimeStamp,
        key_only: bool,
        reverse_scan: bool,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: CommandKind = CommandKind::scan;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);

        let res = self.read_pool.spawn_handle(
            async move {
                if let Ok(start_key) = start_key.to_owned().into_raw() {
                    let mut key = vec![];
                    if let Some(end_key) = &end_key {
                        if let Ok(end_key) = end_key.to_owned().into_raw() {
                            key = end_key;
                        }
                    }
                    tls_collect_qps(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        &start_key,
                        &key,
                        reverse_scan,
                    );
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                let bypass_locks = TsSet::from_u64s(ctx.take_resolved_locks());
                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();

                    let snap_store = SnapshotStore::new(
                        snapshot,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                        bypass_locks,
                        false,
                    );

                    let mut scanner;
                    if !reverse_scan {
                        scanner =
                            snap_store.scanner(false, key_only, false, Some(start_key), end_key)?;
                    } else {
                        scanner =
                            snap_store.scanner(true, key_only, false, end_key, Some(start_key))?;
                    };
                    let res = scanner.scan(limit);

                    let statistics = scanner.take_statistics();
                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(ctx.get_region_id(), &statistics);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());

                    res.map_err(Error::from).map(|results| {
                        KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                            .get(CMD)
                            .observe(results.len() as f64);
                        results
                            .into_iter()
                            .map(|x| x.map_err(Error::from))
                            .collect()
                    })
                }
            },
            priority,
            start_ts.into_inner(),
        );

        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }

    pub fn sched_txn_command<T: StorageCallbackType>(
        &self,
        cmd: TypedCommand<T>,
        callback: Callback<T>,
    ) -> Result<()> {
        use crate::storage::txn::commands::{
            AcquirePessimisticLock, CommandKind, Prewrite, PrewritePessimistic,
        };

        let cmd: Command = cmd.into();

        if cmd.requires_pessimistic_txn() && !self.pessimistic_txn_enabled {
            callback(Err(Error::from(ErrorInner::PessimisticTxnNotEnabled)));
            return Ok(());
        }

        match &cmd.kind {
            CommandKind::Prewrite(Prewrite { mutations, .. }) => {
                check_key_size!(
                    mutations.iter().map(|m| m.key().as_encoded()),
                    self.max_key_size,
                    callback
                );
            }
            CommandKind::PrewritePessimistic(PrewritePessimistic { mutations, .. }) => {
                check_key_size!(
                    mutations.iter().map(|(m, _)| m.key().as_encoded()),
                    self.max_key_size,
                    callback
                );
            }
            CommandKind::AcquirePessimisticLock(AcquirePessimisticLock { keys, .. }) => {
                check_key_size!(
                    keys.iter().map(|k| k.0.as_encoded()),
                    self.max_key_size,
                    callback
                );
            }
            _ => {}
        }

        fail_point!("storage_drop_message", |_| Ok(()));
        cmd.incr_cmd_metric();
        self.sched.run_cmd(cmd, T::callback(callback));

        Ok(())
    }

    /// Delete all keys in the range [`start_key`, `end_key`).
    ///
    /// All keys in the range will be deleted permanently regardless of their timestamps.
    /// This means that deleted keys will not be retrievable by specifying an older timestamp.
    /// If `notify_only` is set, the data will not be immediately deleted, but the operation will
    /// still be replicated via Raft. This is used to notify that the data will be deleted by
    /// `unsafe_destroy_range` soon.
    pub fn delete_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        notify_only: bool,
        callback: Callback<()>,
    ) -> Result<()> {
        let mut modifies = Vec::with_capacity(DATA_CFS.len());
        for cf in DATA_CFS {
            modifies.push(Modify::DeleteRange(
                cf,
                start_key.clone(),
                end_key.clone(),
                notify_only,
            ));
        }

        self.engine.async_write(
            &ctx,
            modifies,
            Box::new(|(_, res): (_, kv::Result<_>)| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.delete_range.inc();
        Ok(())
    }

    /// Get the value of a raw key.
    pub fn raw_get(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
    ) -> impl Future<Item = Option<Vec<u8>>, Error = Error> {
        const CMD: CommandKind = CommandKind::raw_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);

        let res = self.read_pool.spawn_handle(
            async move {
                tls_collect_qps(ctx.get_region_id(), ctx.get_peer(), &key, &key, false);

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();
                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let cf = Self::rawkv_cf(&cf)?;
                    // no scan_count for this kind of op.

                    let key_len = key.len();
                    let r = snapshot.get_cf(cf, &Key::from_encoded(key))?;
                    if let Some(ref value) = r {
                        let mut stats = Statistics::default();
                        stats.data.flow_stats.read_keys = 1;
                        stats.data.flow_stats.read_bytes = key_len + value.len();
                        tls_collect_read_flow(ctx.get_region_id(), &stats);
                        KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                    }

                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());

                    Ok(r)
                }
            },
            priority,
            thread_rng().next_u64(),
        );

        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }

    /// Get the values of a set of raw keys, return a list of `Result`s.
    pub fn raw_batch_get_command(
        &self,
        cf: String,
        gets: Vec<PointGetCommand>,
    ) -> impl Future<Item = Vec<Result<Option<Vec<u8>>>>, Error = Error> {
        const CMD: CommandKind = CommandKind::raw_batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let ctx = gets[0].ctx.clone();
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let res = self.read_pool.spawn_handle(
            async move {
                for get in &gets {
                    if let Ok(key) = get.key.to_owned().into_raw() {
                        // todo no raw?
                        tls_collect_qps(
                            get.ctx.get_region_id(),
                            get.ctx.get_peer(),
                            &key,
                            &key,
                            false,
                        );
                    }
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();
                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();

                    let cf = Self::rawkv_cf(&cf)?;
                    let mut results = vec![];
                    // TODO: optimize using seek.
                    for get in gets {
                        results.push(snapshot.get_cf(cf, &get.key).map_err(Error::from));
                    }
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());

                    Ok(results)
                }
            },
            priority,
            thread_rng().next_u64(),
        );
        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }

    /// Get the values of some raw keys in a batch.
    pub fn raw_batch_get(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: CommandKind = CommandKind::raw_batch_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);

        let res = self.read_pool.spawn_handle(
            async move {
                let mut key_ranges = vec![];
                for key in &keys {
                    key_ranges.push(build_key_range(key, key, false));
                }
                tls_collect_qps_batch(ctx.get_region_id(), ctx.get_peer(), key_ranges);

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();
                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let keys: Vec<Key> = keys.into_iter().map(Key::from_encoded).collect();
                    let cf = Self::rawkv_cf(&cf)?;
                    // no scan_count for this kind of op.
                    let mut stats = Statistics::default();
                    let result: Vec<Result<KvPair>> = keys
                        .into_iter()
                        .map(|k| {
                            let v = snapshot.get_cf(cf, &k);
                            (k, v)
                        })
                        .filter(|&(_, ref v)| !(v.is_ok() && v.as_ref().unwrap().is_none()))
                        .map(|(k, v)| match v {
                            Ok(Some(v)) => {
                                stats.data.flow_stats.read_keys += 1;
                                stats.data.flow_stats.read_bytes += k.as_encoded().len() + v.len();
                                Ok((k.into_encoded(), v))
                            }
                            Err(e) => Err(Error::from(e)),
                            _ => unreachable!(),
                        })
                        .collect();

                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(stats.data.flow_stats.read_keys as f64);
                    tls_collect_read_flow(ctx.get_region_id(), &stats);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());
                    Ok(result)
                }
            },
            priority,
            thread_rng().next_u64(),
        );

        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }

    /// Write a raw key to the storage.
    pub fn raw_put(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        check_key_size!(Some(&key).into_iter(), self.max_key_size, callback);

        self.engine.async_write(
            &ctx,
            vec![Modify::Put(
                Self::rawkv_cf(&cf)?,
                Key::from_encoded(key),
                value,
            )],
            Box::new(|(_, res): (_, kv::Result<_>)| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.raw_put.inc();
        Ok(())
    }

    /// Write some keys to the storage in a batch.
    pub fn raw_batch_put(
        &self,
        ctx: Context,
        cf: String,
        pairs: Vec<KvPair>,
        callback: Callback<()>,
    ) -> Result<()> {
        let cf = Self::rawkv_cf(&cf)?;

        check_key_size!(
            pairs.iter().map(|(ref k, _)| k),
            self.max_key_size,
            callback
        );

        let requests = pairs
            .into_iter()
            .map(|(k, v)| Modify::Put(cf, Key::from_encoded(k), v))
            .collect();
        self.engine.async_write(
            &ctx,
            requests,
            Box::new(|(_, res): (_, kv::Result<_>)| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.raw_batch_put.inc();
        Ok(())
    }

    /// Delete a raw key from the storage.
    pub fn raw_delete(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        check_key_size!(Some(&key).into_iter(), self.max_key_size, callback);

        self.engine.async_write(
            &ctx,
            vec![Modify::Delete(Self::rawkv_cf(&cf)?, Key::from_encoded(key))],
            Box::new(|(_, res): (_, kv::Result<_>)| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.raw_delete.inc();
        Ok(())
    }

    /// Delete all raw keys in [`start_key`, `end_key`).
    pub fn raw_delete_range(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        check_key_size!(
            Some(&start_key)
                .into_iter()
                .chain(Some(&end_key).into_iter()),
            self.max_key_size,
            callback
        );

        let cf = Self::rawkv_cf(&cf)?;
        let start_key = Key::from_encoded(start_key);
        let end_key = Key::from_encoded(end_key);

        self.engine.async_write(
            &ctx,
            vec![Modify::DeleteRange(cf, start_key, end_key, false)],
            Box::new(|(_, res): (_, kv::Result<_>)| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.raw_delete_range.inc();
        Ok(())
    }

    /// Delete some raw keys in a batch.
    pub fn raw_batch_delete(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
        callback: Callback<()>,
    ) -> Result<()> {
        let cf = Self::rawkv_cf(&cf)?;
        check_key_size!(keys.iter(), self.max_key_size, callback);

        let requests = keys
            .into_iter()
            .map(|k| Modify::Delete(cf, Key::from_encoded(k)))
            .collect();
        self.engine.async_write(
            &ctx,
            requests,
            Box::new(|(_, res): (_, kv::Result<_>)| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.raw_batch_delete.inc();
        Ok(())
    }

    /// Scan raw keys in [`start_key`, `end_key`), returns at most `limit` keys. If `end_key` is
    /// `None`, it means unbounded.
    ///
    /// If `key_only` is true, the value corresponding to the key will not be read. Only scanned
    /// keys will be returned.
    fn forward_raw_scan(
        snapshot: &E::Snap,
        cf: &str,
        start_key: &Key,
        end_key: Option<Key>,
        limit: usize,
        statistics: &mut Statistics,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        let mut option = IterOptions::default();
        if let Some(end) = end_key {
            option.set_upper_bound(end.as_encoded(), DATA_KEY_PREFIX_LEN);
        }
        let mut cursor = snapshot.iter_cf(Self::rawkv_cf(cf)?, option, ScanMode::Forward)?;
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        while cursor.valid()? && pairs.len() < limit {
            pairs.push(Ok((
                cursor.key(statistics).to_owned(),
                if key_only {
                    vec![]
                } else {
                    cursor.value(statistics).to_owned()
                },
            )));
            cursor.next(statistics);
        }
        Ok(pairs)
    }

    /// Scan raw keys in [`end_key`, `start_key`) in reverse order, returns at most `limit` keys. If
    /// `start_key` is `None`, it means it's unbounded.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only scanned keys will be returned.
    fn reverse_raw_scan(
        snapshot: &E::Snap,
        cf: &str,
        start_key: &Key,
        end_key: Option<Key>,
        limit: usize,
        statistics: &mut Statistics,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        let mut option = IterOptions::default();
        if let Some(end) = end_key {
            option.set_lower_bound(end.as_encoded(), DATA_KEY_PREFIX_LEN);
        }
        let mut cursor = snapshot.iter_cf(Self::rawkv_cf(cf)?, option, ScanMode::Backward)?;
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.reverse_seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        while cursor.valid()? && pairs.len() < limit {
            pairs.push(Ok((
                cursor.key(statistics).to_owned(),
                if key_only {
                    vec![]
                } else {
                    cursor.value(statistics).to_owned()
                },
            )));
            cursor.prev(statistics);
        }
        Ok(pairs)
    }

    /// Scan raw keys in a range.
    ///
    /// If `reverse_scan` is false, the range is [`start_key`, `end_key`); otherwise, the range is
    /// [`end_key`, `start_key`) and it scans from `start_key` and goes backwards. If `end_key` is `None`, it
    /// means unbounded.
    ///
    /// This function scans at most `limit` keys.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only scanned keys will be returned.
    pub fn raw_scan(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
        key_only: bool,
        reverse_scan: bool,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: CommandKind = CommandKind::raw_scan;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);

        let res = self.read_pool.spawn_handle(
            async move {
                {
                    let end_key = match &end_key {
                        Some(end_key) => end_key.to_vec(),
                        None => vec![],
                    };
                    tls_collect_qps(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        &start_key,
                        &end_key,
                        reverse_scan,
                    );
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();

                    let end_key = end_key.map(Key::from_encoded);

                    let mut statistics = Statistics::default();
                    let result = if reverse_scan {
                        Self::reverse_raw_scan(
                            &snapshot,
                            &cf,
                            &Key::from_encoded(start_key),
                            end_key,
                            limit,
                            &mut statistics,
                            key_only,
                        )
                        .map_err(Error::from)
                    } else {
                        Self::forward_raw_scan(
                            &snapshot,
                            &cf,
                            &Key::from_encoded(start_key),
                            end_key,
                            limit,
                            &mut statistics,
                            key_only,
                        )
                        .map_err(Error::from)
                    };

                    metrics::tls_collect_read_flow(ctx.get_region_id(), &statistics);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(statistics.write.flow_stats.read_keys as f64);
                    metrics::tls_collect_scan_details(CMD, &statistics);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());

                    result
                }
            },
            priority,
            thread_rng().next_u64(),
        );

        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }

    /// Check the given raw kv CF name. Return the CF name, or `Err` if given CF name is invalid.
    /// The CF name can be one of `"default"`, `"write"` and `"lock"`. If given `cf` is empty,
    /// `CF_DEFAULT` (`"default"`) will be returned.
    fn rawkv_cf(cf: &str) -> Result<CfName> {
        if cf.is_empty() {
            return Ok(CF_DEFAULT);
        }
        for c in DATA_CFS {
            if cf == *c {
                return Ok(c);
            }
        }
        Err(Error::from(ErrorInner::InvalidCf(cf.to_owned())))
    }

    /// Check if key range is valid
    ///
    /// - If `reverse` is true, `end_key` is less than `start_key`. `end_key` is the lower bound.
    /// - If `reverse` is false, `end_key` is greater than `start_key`. `end_key` is the upper bound.
    fn check_key_ranges(ranges: &[KeyRange], reverse: bool) -> bool {
        let ranges_len = ranges.len();
        for i in 0..ranges_len {
            let start_key = ranges[i].get_start_key();
            let mut end_key = ranges[i].get_end_key();
            if end_key.is_empty() && i + 1 != ranges_len {
                end_key = ranges[i + 1].get_start_key();
            }
            if !end_key.is_empty()
                && (!reverse && start_key >= end_key || reverse && start_key <= end_key)
            {
                return false;
            }
        }
        true
    }

    /// Scan raw keys in multiple ranges in a batch.
    pub fn raw_batch_scan(
        &self,
        ctx: Context,
        cf: String,
        mut ranges: Vec<KeyRange>,
        each_limit: usize,
        key_only: bool,
        reverse_scan: bool,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: CommandKind = CommandKind::raw_batch_scan;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);

        let res = self.read_pool.spawn_handle(
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();
                let command_duration = tikv_util::time::Instant::now_coarse();

                let snapshot = Self::with_tls_engine(|engine| Self::snapshot(engine, &ctx)).await?;
                {
                    let begin_instant = Instant::now();
                    let mut statistics = Statistics::default();
                    if !Self::check_key_ranges(&ranges, reverse_scan) {
                        return Err(box_err!("Invalid KeyRanges"));
                    };
                    let mut result = Vec::new();
                    let ranges_len = ranges.len();
                    for i in 0..ranges_len {
                        let start_key = Key::from_encoded(ranges[i].take_start_key());
                        let end_key = ranges[i].take_end_key();
                        let end_key = if end_key.is_empty() {
                            if i + 1 == ranges_len {
                                None
                            } else {
                                Some(Key::from_encoded_slice(ranges[i + 1].get_start_key()))
                            }
                        } else {
                            Some(Key::from_encoded(end_key))
                        };
                        let pairs = if reverse_scan {
                            Self::reverse_raw_scan(
                                &snapshot,
                                &cf,
                                &start_key,
                                end_key,
                                each_limit,
                                &mut statistics,
                                key_only,
                            )?
                        } else {
                            Self::forward_raw_scan(
                                &snapshot,
                                &cf,
                                &start_key,
                                end_key,
                                each_limit,
                                &mut statistics,
                                key_only,
                            )?
                        };
                        result.extend(pairs.into_iter());
                    }
                    let mut key_ranges = vec![];
                    for range in ranges {
                        key_ranges.push(build_key_range(
                            &range.start_key,
                            &range.end_key,
                            reverse_scan,
                        ));
                    }
                    tls_collect_qps_batch(ctx.get_region_id(), ctx.get_peer(), key_ranges);
                    metrics::tls_collect_read_flow(ctx.get_region_id(), &statistics);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(statistics.write.flow_stats.read_keys as f64);
                    metrics::tls_collect_scan_details(CMD, &statistics);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());
                    Ok(result)
                }
            },
            priority,
            thread_rng().next_u64(),
        );

        res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
            .flatten()
    }
}

/// Get a single value.
pub struct PointGetCommand {
    pub ctx: Context,
    pub key: Key,
    /// None if this is a raw get, Some if this is a transactional get.
    pub ts: Option<TimeStamp>,
}

impl PointGetCommand {
    pub fn from_get(request: &mut GetRequest) -> Self {
        PointGetCommand {
            ctx: request.take_context(),
            key: Key::from_raw(request.get_key()),
            ts: Some(request.get_version().into()),
        }
    }

    pub fn from_raw_get(request: &mut RawGetRequest) -> Self {
        PointGetCommand {
            ctx: request.take_context(),
            // FIXME: It is weird in semantics because the key in the request is actually in the
            // raw format. We should fix it when the meaning of type `Key` is well defined.
            key: Key::from_encoded(request.take_key()),
            ts: None,
        }
    }

    #[cfg(test)]
    pub fn from_key_ts(key: Key, ts: Option<TimeStamp>) -> Self {
        PointGetCommand {
            ctx: Context::default(),
            key,
            ts,
        }
    }
}

fn get_priority_tag(priority: CommandPri) -> CommandPriority {
    match priority {
        CommandPri::Low => CommandPriority::low,
        CommandPri::Normal => CommandPriority::normal,
        CommandPri::High => CommandPriority::high,
    }
}

/// A builder to build a temporary `Storage<E>`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestStorageBuilder<E: Engine> {
    engine: E,
    config: Config,
    pessimistic_txn_enabled: bool,
    pipelined_pessimistic_lock: bool,
}

impl TestStorageBuilder<RocksEngine> {
    /// Build `Storage<RocksEngine>`.
    pub fn new() -> Self {
        Self {
            engine: TestEngineBuilder::new().build().unwrap(),
            config: Config::default(),
            pessimistic_txn_enabled: false,
            pipelined_pessimistic_lock: false,
        }
    }
}

impl<E: Engine> TestStorageBuilder<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            config: Config::default(),
            pessimistic_txn_enabled: false,
            pipelined_pessimistic_lock: false,
        }
    }

    /// Customize the config of the `Storage`.
    ///
    /// By default, `Config::default()` will be used.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn set_pipelined_pessimistic_lock(mut self, enabled: bool) -> Self {
        self.pipelined_pessimistic_lock = enabled;
        self
    }

    pub fn enable_pessimistic_txn(mut self) -> Self {
        self.pessimistic_txn_enabled = true;
        self
    }

    /// Build a `Storage<E>`.
    pub fn build(self) -> Result<Storage<E, DummyLockManager>> {
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            self.engine.clone(),
        );
        let lock_manager = if self.pessimistic_txn_enabled || self.pipelined_pessimistic_lock {
            Some(DummyLockManager {})
        } else {
            None
        };
        Storage::from_engine(
            self.engine,
            &self.config,
            ReadPool::from(read_pool).handle(),
            lock_manager,
            self.pipelined_pessimistic_lock,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::txn::{commands, Error as TxnError, ErrorInner as TxnErrorInner};
    use futures03::executor::block_on;
    use kvproto::kvrpcpb::{CommandPri, LockInfo};
    use std::{
        fmt::Debug,
        sync::mpsc::{channel, Sender},
    };
    use tikv_util::collections::HashMap;
    use tikv_util::config::ReadableSize;
    use txn_types::Mutation;

    fn expect_none(x: Result<Option<Value>>) {
        assert_eq!(x.unwrap(), None);
    }

    fn expect_value(v: Vec<u8>, x: Result<Option<Value>>) {
        assert_eq!(x.unwrap().unwrap(), v);
    }

    fn expect_multi_values(v: Vec<Option<KvPair>>, x: Result<Vec<Result<KvPair>>>) {
        let x: Vec<Option<KvPair>> = x.unwrap().into_iter().map(Result::ok).collect();
        assert_eq!(x, v);
    }

    fn expect_error<T, F>(err_matcher: F, x: Result<T>)
    where
        F: FnOnce(Error) + Send + 'static,
    {
        match x {
            Err(e) => err_matcher(e),
            _ => panic!("expect result to be an error"),
        }
    }

    fn expect_ok_callback<T: Debug>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            x.unwrap();
            done.send(id).unwrap();
        })
    }

    fn expect_fail_callback<T, F>(done: Sender<i32>, id: i32, err_matcher: F) -> Callback<T>
    where
        F: FnOnce(Error) + Send + 'static,
    {
        Box::new(move |x: Result<T>| {
            expect_error(err_matcher, x);
            done.send(id).unwrap();
        })
    }

    fn expect_too_busy_callback<T>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            expect_error(
                |err| match err {
                    Error(box ErrorInner::SchedTooBusy) => {}
                    e => panic!("unexpected error chain: {:?}, expect too busy", e),
                },
                x,
            );
            done.send(id).unwrap();
        })
    }

    fn expect_value_callback<T: PartialEq + Debug + Send + 'static>(
        done: Sender<i32>,
        id: i32,
        value: T,
    ) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert_eq!(x.unwrap(), value);
            done.send(id).unwrap();
        })
    }

    #[test]
    fn test_get_put() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"x"), 100.into())
                .wait(),
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::KeyIsLocked { .. },
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .get(Context::default(), Key::from_raw(b"x"), 101.into())
                .wait(),
        );
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    101.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"x"), 100.into())
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .get(Context::default(), Key::from_raw(b"x"), 101.into())
                .wait(),
        );
    }

    #[test]
    fn test_cf_error() {
        // New engine lacks normal column families.
        let engine = TestEngineBuilder::new().cfs(["foo"]).build().unwrap();
        let storage = TestStorageBuilder::from_engine(engine).build().unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"aa".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"bb".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"cc".to_vec())),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::Engine(EngineError(box EngineErrorInner::Request(
                            ..,
                        ))),
                    ))))) => {}
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Engine(EngineError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .get(Context::default(), Key::from_raw(b"x"), 1.into())
                .wait(),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Engine(EngineError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"x"),
                    None,
                    1000,
                    1.into(),
                    false,
                    false,
                )
                .wait(),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Engine(EngineError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .batch_get(
                    Context::default(),
                    vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                    1.into(),
                )
                .wait(),
        );
        let x = storage
            .batch_get_command(vec![
                PointGetCommand::from_key_ts(Key::from_raw(b"c"), Some(1.into())),
                PointGetCommand::from_key_ts(Key::from_raw(b"d"), Some(1.into())),
            ])
            .wait()
            .unwrap();
        for v in x {
            expect_error(
                |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::Engine(EngineError(box EngineErrorInner::Request(
                            ..,
                        ))),
                    ))))) => {}
                    e => panic!("unexpected error chain: {:?}", e),
                },
                v,
            );
        }
    }

    #[test]
    fn test_scan() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"aa".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"bb".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"cc".to_vec())),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![None, None, None],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    None,
                    1000,
                    5.into(),
                    false,
                    false,
                )
                .wait(),
        );
        // Backward
        expect_multi_values(
            vec![None, None, None],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    None,
                    1000,
                    5.into(),
                    false,
                    true,
                )
                .wait(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    Some(Key::from_raw(b"c")),
                    1000,
                    5.into(),
                    false,
                    false,
                )
                .wait(),
        );
        // Backward with bound
        expect_multi_values(
            vec![None, None],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    Some(Key::from_raw(b"b")),
                    1000,
                    5.into(),
                    false,
                    true,
                )
                .wait(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    None,
                    2,
                    5.into(),
                    false,
                    false,
                )
                .wait(),
        );
        // Backward with limit
        expect_multi_values(
            vec![None, None],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    None,
                    2,
                    5.into(),
                    false,
                    true,
                )
                .wait(),
        );

        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    None,
                    1000,
                    5.into(),
                    false,
                    false,
                )
                .wait(),
        );
        // Backward
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    None,
                    1000,
                    5.into(),
                    false,
                    true,
                )
                .wait(),
        );
        // Forward with bound
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    Some(Key::from_raw(b"c")),
                    1000,
                    5.into(),
                    false,
                    false,
                )
                .wait(),
        );
        // Backward with bound
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    Some(Key::from_raw(b"b")),
                    1000,
                    5.into(),
                    false,
                    true,
                )
                .wait(),
        );

        // Forward with limit
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    None,
                    2,
                    5.into(),
                    false,
                    false,
                )
                .wait(),
        );
        // Backward with limit
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            storage
                .scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    None,
                    2,
                    5.into(),
                    false,
                    true,
                )
                .wait(),
        );
    }

    #[test]
    fn test_batch_get() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"aa".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"bb".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"cc".to_vec())),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![None],
            storage
                .batch_get(
                    Context::default(),
                    vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                    2.into(),
                )
                .wait(),
        );
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            storage
                .batch_get(
                    Context::default(),
                    vec![
                        Key::from_raw(b"c"),
                        Key::from_raw(b"x"),
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                    ],
                    5.into(),
                )
                .wait(),
        );
    }

    #[test]
    fn test_batch_get_command() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"aa".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"bb".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"cc".to_vec())),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut x = storage
            .batch_get_command(vec![
                PointGetCommand::from_key_ts(Key::from_raw(b"c"), Some(2.into())),
                PointGetCommand::from_key_ts(Key::from_raw(b"d"), Some(2.into())),
            ])
            .wait()
            .unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::KeyIsLocked(..),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            x.remove(0),
        );
        assert_eq!(x.remove(0).unwrap(), None);
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                        Key::from_raw(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let x: Vec<Option<Vec<u8>>> = storage
            .batch_get_command(vec![
                PointGetCommand::from_key_ts(Key::from_raw(b"c"), Some(5.into())),
                PointGetCommand::from_key_ts(Key::from_raw(b"x"), Some(5.into())),
                PointGetCommand::from_key_ts(Key::from_raw(b"a"), Some(5.into())),
                PointGetCommand::from_key_ts(Key::from_raw(b"b"), Some(5.into())),
            ])
            .wait()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(
            x,
            vec![
                Some(b"cc".to_vec()),
                None,
                Some(b"aa".to_vec()),
                Some(b"bb".to_vec())
            ]
        );
    }

    #[test]
    fn test_txn() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(b"y"), b"101".to_vec()))],
                    b"y".to_vec(),
                    101.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    110.into(),
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 2, TxnStatus::committed(110.into())),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"y")],
                    101.into(),
                    111.into(),
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 3, TxnStatus::committed(111.into())),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            storage
                .get(Context::default(), Key::from_raw(b"x"), 120.into())
                .wait(),
        );
        expect_value(
            b"101".to_vec(),
            storage
                .get(Context::default(), Key::from_raw(b"y"), 120.into())
                .wait(),
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(b"x"), b"105".to_vec()))],
                    b"x".to_vec(),
                    105.into(),
                ),
                expect_fail_callback(tx, 6, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::WriteConflict { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_sched_too_busy() {
        let mut config = Config::default();
        config.scheduler_pending_write_threshold = ReadableSize(1);
        let storage = TestStorageBuilder::new().config(config).build().unwrap();
        let (tx, rx) = channel();
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"x"), 100.into())
                .wait(),
        );
        storage
            .sched_txn_command::<()>(
                commands::Pause::new(vec![Key::from_raw(b"x")], 1000, Context::default()).into(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(b"y"), b"101".to_vec()))],
                    b"y".to_vec(),
                    101.into(),
                ),
                expect_too_busy_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(b"z"), b"102".to_vec()))],
                    b"y".to_vec(),
                    102.into(),
                ),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    100.into(),
                    TimeStamp::zero(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"x"), 105.into())
                .wait(),
        );
    }

    #[test]
    fn test_cleanup_check_ttl() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let ts = TimeStamp::compose;
        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::Put((Key::from_raw(b"x"), b"110".to_vec()))],
                    b"x".to_vec(),
                    ts(110, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    ts(110, 0),
                    ts(120, 0),
                    Context::default(),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::KeyIsLocked(info),
                    ))))) => assert_eq!(info.get_lock_ttl(), 100),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Cleanup::new(
                    Key::from_raw(b"x"),
                    ts(110, 0),
                    ts(220, 0),
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"x"), ts(230, 0))
                .wait(),
        );
    }

    #[test]
    fn test_high_priority_get_put() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(storage.get(ctx, Key::from_raw(b"x"), 100.into()).wait());
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .sched_txn_command(
                commands::Prewrite::with_context(
                    vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                    b"x".to_vec(),
                    100.into(),
                    ctx,
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .sched_txn_command(
                commands::Commit::new(vec![Key::from_raw(b"x")], 100.into(), 101.into(), ctx),
                expect_ok_callback(tx, 2),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(storage.get(ctx, Key::from_raw(b"x"), 100.into()).wait());
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            storage.get(ctx, Key::from_raw(b"x"), 101.into()).wait(),
        );
    }

    #[test]
    fn test_high_priority_no_block() {
        let mut config = Config::default();
        config.scheduler_worker_pool_size = 1;
        let storage = TestStorageBuilder::new().config(config).build().unwrap();
        let (tx, rx) = channel();
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"x"), 100.into())
                .wait(),
        );
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"x")],
                    100.into(),
                    101.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Pause::new(vec![Key::from_raw(b"y")], 1000, Context::default()),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            storage.get(ctx, Key::from_raw(b"x"), 101.into()).wait(),
        );
        // Command Get with high priority not block by command Pause.
        assert_eq!(rx.recv().unwrap(), 3);
    }

    #[test]
    fn test_delete_range() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        // Write x and y.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"x"), b"100".to_vec())),
                        Mutation::Put((Key::from_raw(b"y"), b"100".to_vec())),
                        Mutation::Put((Key::from_raw(b"z"), b"100".to_vec())),
                    ],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![
                        Key::from_raw(b"x"),
                        Key::from_raw(b"y"),
                        Key::from_raw(b"z"),
                    ],
                    100.into(),
                    101.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            storage
                .get(Context::default(), Key::from_raw(b"x"), 101.into())
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .get(Context::default(), Key::from_raw(b"y"), 101.into())
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .get(Context::default(), Key::from_raw(b"z"), 101.into())
                .wait(),
        );

        // Delete range [x, z)
        storage
            .delete_range(
                Context::default(),
                Key::from_raw(b"x"),
                Key::from_raw(b"z"),
                false,
                expect_ok_callback(tx.clone(), 5),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"x"), 101.into())
                .wait(),
        );
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"y"), 101.into())
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .get(Context::default(), Key::from_raw(b"z"), 101.into())
                .wait(),
        );

        storage
            .delete_range(
                Context::default(),
                Key::from_raw(b""),
                Key::from_raw(&[255]),
                false,
                expect_ok_callback(tx, 9),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .get(Context::default(), Key::from_raw(b"z"), 101.into())
                .wait(),
        );
    }

    #[test]
    fn test_raw_delete_range() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let test_data = [
            (b"a", b"001"),
            (b"b", b"002"),
            (b"c", b"003"),
            (b"d", b"004"),
            (b"e", b"005"),
        ];

        // Write some key-value pairs to the db
        for kv in &test_data {
            storage
                .raw_put(
                    Context::default(),
                    "".to_string(),
                    kv.0.to_vec(),
                    kv.1.to_vec(),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }

        expect_value(
            b"004".to_vec(),
            storage
                .raw_get(Context::default(), "".to_string(), b"d".to_vec())
                .wait(),
        );

        // Delete ["d", "e")
        storage
            .raw_delete_range(
                Context::default(),
                "".to_string(),
                b"d".to_vec(),
                b"e".to_vec(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert key "d" has gone
        expect_value(
            b"003".to_vec(),
            storage
                .raw_get(Context::default(), "".to_string(), b"c".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .raw_get(Context::default(), "".to_string(), b"d".to_vec())
                .wait(),
        );
        expect_value(
            b"005".to_vec(),
            storage
                .raw_get(Context::default(), "".to_string(), b"e".to_vec())
                .wait(),
        );

        // Delete ["aa", "ab")
        storage
            .raw_delete_range(
                Context::default(),
                "".to_string(),
                b"aa".to_vec(),
                b"ab".to_vec(),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert nothing happened
        expect_value(
            b"001".to_vec(),
            storage
                .raw_get(Context::default(), "".to_string(), b"a".to_vec())
                .wait(),
        );
        expect_value(
            b"002".to_vec(),
            storage
                .raw_get(Context::default(), "".to_string(), b"b".to_vec())
                .wait(),
        );

        // Delete all
        storage
            .raw_delete_range(
                Context::default(),
                "".to_string(),
                b"a".to_vec(),
                b"z".to_vec(),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert now no key remains
        for kv in &test_data {
            expect_none(
                storage
                    .raw_get(Context::default(), "".to_string(), kv.0.to_vec())
                    .wait(),
            );
        }

        rx.recv().unwrap();
    }

    #[test]
    fn test_raw_batch_put() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let test_data = vec![
            (b"a".to_vec(), b"aa".to_vec()),
            (b"b".to_vec(), b"bb".to_vec()),
            (b"c".to_vec(), b"cc".to_vec()),
            (b"d".to_vec(), b"dd".to_vec()),
            (b"e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs in a batch
        storage
            .raw_batch_put(
                Context::default(),
                "".to_string(),
                test_data.clone(),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs one by one
        for (key, val) in test_data {
            expect_value(
                val,
                storage
                    .raw_get(Context::default(), "".to_string(), key)
                    .wait(),
            );
        }
    }

    #[test]
    fn test_raw_batch_get() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let test_data = vec![
            (b"a".to_vec(), b"aa".to_vec()),
            (b"b".to_vec(), b"bb".to_vec()),
            (b"c".to_vec(), b"cc".to_vec()),
            (b"d".to_vec(), b"dd".to_vec()),
            (b"e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs one by one
        for &(ref key, ref value) in &test_data {
            storage
                .raw_put(
                    Context::default(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }
        rx.recv().unwrap();

        // Verify pairs in a batch
        let keys = test_data.iter().map(|&(ref k, _)| k.clone()).collect();
        let results = test_data.into_iter().map(|(k, v)| Some((k, v))).collect();
        expect_multi_values(
            results,
            storage
                .raw_batch_get(Context::default(), "".to_string(), keys)
                .wait(),
        );
    }

    #[test]
    fn test_batch_raw_get() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let test_data = vec![
            (b"a".to_vec(), b"aa".to_vec()),
            (b"b".to_vec(), b"bb".to_vec()),
            (b"c".to_vec(), b"cc".to_vec()),
            (b"d".to_vec(), b"dd".to_vec()),
            (b"e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs one by one
        for &(ref key, ref value) in &test_data {
            storage
                .raw_put(
                    Context::default(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }
        rx.recv().unwrap();

        // Verify pairs in a batch
        let cmds = test_data
            .iter()
            .map(|&(ref k, _)| {
                let mut req = RawGetRequest::default();
                req.set_key(k.clone());
                PointGetCommand::from_raw_get(&mut req)
            })
            .collect();
        let results: Vec<Option<Vec<u8>>> = test_data.into_iter().map(|(_, v)| Some(v)).collect();
        let x: Vec<Option<Vec<u8>>> = storage
            .raw_batch_get_command("".to_string(), cmds)
            .wait()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(x, results);
    }

    #[test]
    fn test_raw_batch_delete() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let test_data = vec![
            (b"a".to_vec(), b"aa".to_vec()),
            (b"b".to_vec(), b"bb".to_vec()),
            (b"c".to_vec(), b"cc".to_vec()),
            (b"d".to_vec(), b"dd".to_vec()),
            (b"e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs in batch
        storage
            .raw_batch_put(
                Context::default(),
                "".to_string(),
                test_data.clone(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs exist
        let keys = test_data.iter().map(|&(ref k, _)| k.clone()).collect();
        let results = test_data
            .iter()
            .map(|&(ref k, ref v)| Some((k.clone(), v.clone())))
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_batch_get(Context::default(), "".to_string(), keys)
                .wait(),
        );

        // Delete ["b", "d"]
        storage
            .raw_batch_delete(
                Context::default(),
                "".to_string(),
                vec![b"b".to_vec(), b"d".to_vec()],
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert "b" and "d" are gone
        expect_value(
            b"aa".to_vec(),
            storage
                .raw_get(Context::default(), "".to_string(), b"a".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .raw_get(Context::default(), "".to_string(), b"b".to_vec())
                .wait(),
        );
        expect_value(
            b"cc".to_vec(),
            storage
                .raw_get(Context::default(), "".to_string(), b"c".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .raw_get(Context::default(), "".to_string(), b"d".to_vec())
                .wait(),
        );
        expect_value(
            b"ee".to_vec(),
            storage
                .raw_get(Context::default(), "".to_string(), b"e".to_vec())
                .wait(),
        );

        // Delete ["a", "c", "e"]
        storage
            .raw_batch_delete(
                Context::default(),
                "".to_string(),
                vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()],
                expect_ok_callback(tx, 2),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert no key remains
        for (k, _) in test_data {
            expect_none(
                storage
                    .raw_get(Context::default(), "".to_string(), k)
                    .wait(),
            );
        }
    }

    #[test]
    fn test_raw_scan() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let test_data = vec![
            (b"a".to_vec(), b"aa".to_vec()),
            (b"a1".to_vec(), b"aa11".to_vec()),
            (b"a2".to_vec(), b"aa22".to_vec()),
            (b"a3".to_vec(), b"aa33".to_vec()),
            (b"b".to_vec(), b"bb".to_vec()),
            (b"b1".to_vec(), b"bb11".to_vec()),
            (b"b2".to_vec(), b"bb22".to_vec()),
            (b"b3".to_vec(), b"bb33".to_vec()),
            (b"c".to_vec(), b"cc".to_vec()),
            (b"c1".to_vec(), b"cc11".to_vec()),
            (b"c2".to_vec(), b"cc22".to_vec()),
            (b"c3".to_vec(), b"cc33".to_vec()),
            (b"d".to_vec(), b"dd".to_vec()),
            (b"d1".to_vec(), b"dd11".to_vec()),
            (b"d2".to_vec(), b"dd22".to_vec()),
            (b"d3".to_vec(), b"dd33".to_vec()),
            (b"e".to_vec(), b"ee".to_vec()),
            (b"e1".to_vec(), b"ee11".to_vec()),
            (b"e2".to_vec(), b"ee22".to_vec()),
            (b"e3".to_vec(), b"ee33".to_vec()),
        ];

        // Write key-value pairs in batch
        storage
            .raw_batch_put(
                Context::default(),
                "".to_string(),
                test_data.clone(),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Scan pairs with key only
        let mut results: Vec<Option<KvPair>> = test_data
            .iter()
            .map(|&(ref k, _)| Some((k.clone(), vec![])))
            .collect();
        expect_multi_values(
            results.clone(),
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    vec![],
                    None,
                    20,
                    true,
                    false,
                )
                .wait(),
        );
        results = results.split_off(10);
        expect_multi_values(
            results,
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    b"c2".to_vec(),
                    None,
                    20,
                    true,
                    false,
                )
                .wait(),
        );
        let mut results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results.clone(),
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    vec![],
                    None,
                    20,
                    false,
                    false,
                )
                .wait(),
        );
        results = results.split_off(10);
        expect_multi_values(
            results,
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    b"c2".to_vec(),
                    None,
                    20,
                    false,
                    false,
                )
                .wait(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .rev()
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    b"z".to_vec(),
                    None,
                    20,
                    false,
                    true,
                )
                .wait(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .rev()
            .take(5)
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    b"z".to_vec(),
                    None,
                    5,
                    false,
                    true,
                )
                .wait(),
        );

        // Scan with end_key
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .skip(6)
            .take(4)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    b"b2".to_vec(),
                    Some(b"c2".to_vec()),
                    20,
                    false,
                    false,
                )
                .wait(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .skip(6)
            .take(1)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    b"b2".to_vec(),
                    Some(b"b2\x00".to_vec()),
                    20,
                    false,
                    false,
                )
                .wait(),
        );

        // Reverse scan with end_key
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .rev()
            .skip(10)
            .take(4)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    b"c2".to_vec(),
                    Some(b"b2".to_vec()),
                    20,
                    false,
                    true,
                )
                .wait(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .into_iter()
            .skip(6)
            .take(1)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_scan(
                    Context::default(),
                    "".to_string(),
                    b"b2\x00".to_vec(),
                    Some(b"b2".to_vec()),
                    20,
                    false,
                    true,
                )
                .wait(),
        );

        // End key tests. Confirm that lower/upper bound works correctly.
        let ctx = Context::default();
        let results = vec![
            (b"c1".to_vec(), b"cc11".to_vec()),
            (b"c2".to_vec(), b"cc22".to_vec()),
            (b"c3".to_vec(), b"cc33".to_vec()),
            (b"d".to_vec(), b"dd".to_vec()),
            (b"d1".to_vec(), b"dd11".to_vec()),
            (b"d2".to_vec(), b"dd22".to_vec()),
        ]
        .into_iter()
        .map(|(k, v)| Some((k, v)));
        let engine = storage.get_engine();
        expect_multi_values(
            results.clone().collect(),
            block_on(async {
                let snapshot =
                    <Storage<RocksEngine, DummyLockManager>>::snapshot(&engine, &ctx).await?;
                <Storage<RocksEngine, DummyLockManager>>::forward_raw_scan(
                    &snapshot,
                    &"".to_string(),
                    &Key::from_encoded(b"c1".to_vec()),
                    Some(Key::from_encoded(b"d3".to_vec())),
                    20,
                    &mut Statistics::default(),
                    false,
                )
            }),
        );
        expect_multi_values(
            results.rev().collect(),
            block_on(async move {
                let snapshot =
                    <Storage<RocksEngine, DummyLockManager>>::snapshot(&engine, &ctx).await?;
                <Storage<RocksEngine, DummyLockManager>>::reverse_raw_scan(
                    &snapshot,
                    &"".to_string(),
                    &Key::from_encoded(b"d3".to_vec()),
                    Some(Key::from_encoded(b"c1".to_vec())),
                    20,
                    &mut Statistics::default(),
                    false,
                )
            }),
        );
    }

    #[test]
    fn test_check_key_ranges() {
        fn make_ranges(ranges: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<KeyRange> {
            ranges
                .into_iter()
                .map(|(s, e)| {
                    let mut range = KeyRange::default();
                    range.set_start_key(s);
                    if !e.is_empty() {
                        range.set_end_key(e);
                    }
                    range
                })
                .collect()
        }

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, false),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"c".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, false),
            true
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, false),
            false
        );

        // if end_key is omitted, the next start_key is used instead. so, false is returned.
        let ranges = make_ranges(vec![
            (b"c".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"a".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, false),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, true),
            true
        );

        let ranges = make_ranges(vec![
            (b"c3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"a3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, true),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, true),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"c3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, true),
            false
        );
    }

    #[test]
    fn test_raw_batch_scan() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let test_data = vec![
            (b"a".to_vec(), b"aa".to_vec()),
            (b"a1".to_vec(), b"aa11".to_vec()),
            (b"a2".to_vec(), b"aa22".to_vec()),
            (b"a3".to_vec(), b"aa33".to_vec()),
            (b"b".to_vec(), b"bb".to_vec()),
            (b"b1".to_vec(), b"bb11".to_vec()),
            (b"b2".to_vec(), b"bb22".to_vec()),
            (b"b3".to_vec(), b"bb33".to_vec()),
            (b"c".to_vec(), b"cc".to_vec()),
            (b"c1".to_vec(), b"cc11".to_vec()),
            (b"c2".to_vec(), b"cc22".to_vec()),
            (b"c3".to_vec(), b"cc33".to_vec()),
            (b"d".to_vec(), b"dd".to_vec()),
            (b"d1".to_vec(), b"dd11".to_vec()),
            (b"d2".to_vec(), b"dd22".to_vec()),
            (b"d3".to_vec(), b"dd33".to_vec()),
            (b"e".to_vec(), b"ee".to_vec()),
            (b"e1".to_vec(), b"ee11".to_vec()),
            (b"e2".to_vec(), b"ee22".to_vec()),
            (b"e3".to_vec(), b"ee33".to_vec()),
        ];

        // Write key-value pairs in batch
        storage
            .raw_batch_put(
                Context::default(),
                "".to_string(),
                test_data.clone(),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs exist
        let keys = test_data.iter().map(|&(ref k, _)| k.clone()).collect();
        let results = test_data.into_iter().map(|(k, v)| Some((k, v))).collect();
        expect_multi_values(
            results,
            storage
                .raw_batch_get(Context::default(), "".to_string(), keys)
                .wait(),
        );

        let results = vec![
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"a1".to_vec(), b"aa11".to_vec())),
            Some((b"a2".to_vec(), b"aa22".to_vec())),
            Some((b"a3".to_vec(), b"aa33".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"b1".to_vec(), b"bb11".to_vec())),
            Some((b"b2".to_vec(), b"bb22".to_vec())),
            Some((b"b3".to_vec(), b"bb33".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
            Some((b"c1".to_vec(), b"cc11".to_vec())),
            Some((b"c2".to_vec(), b"cc22".to_vec())),
            Some((b"c3".to_vec(), b"cc33".to_vec())),
            Some((b"d".to_vec(), b"dd".to_vec())),
        ];
        let ranges: Vec<KeyRange> = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
            .into_iter()
            .map(|k| {
                let mut range = KeyRange::default();
                range.set_start_key(k);
                range
            })
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_batch_scan(
                    Context::default(),
                    "".to_string(),
                    ranges.clone(),
                    5,
                    false,
                    false,
                )
                .wait(),
        );

        let results = vec![
            Some((b"a".to_vec(), vec![])),
            Some((b"a1".to_vec(), vec![])),
            Some((b"a2".to_vec(), vec![])),
            Some((b"a3".to_vec(), vec![])),
            Some((b"b".to_vec(), vec![])),
            Some((b"b1".to_vec(), vec![])),
            Some((b"b2".to_vec(), vec![])),
            Some((b"b3".to_vec(), vec![])),
            Some((b"c".to_vec(), vec![])),
            Some((b"c1".to_vec(), vec![])),
            Some((b"c2".to_vec(), vec![])),
            Some((b"c3".to_vec(), vec![])),
            Some((b"d".to_vec(), vec![])),
        ];
        expect_multi_values(
            results,
            storage
                .raw_batch_scan(
                    Context::default(),
                    "".to_string(),
                    ranges.clone(),
                    5,
                    true,
                    false,
                )
                .wait(),
        );

        let results = vec![
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"a1".to_vec(), b"aa11".to_vec())),
            Some((b"a2".to_vec(), b"aa22".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"b1".to_vec(), b"bb11".to_vec())),
            Some((b"b2".to_vec(), b"bb22".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
            Some((b"c1".to_vec(), b"cc11".to_vec())),
            Some((b"c2".to_vec(), b"cc22".to_vec())),
        ];
        expect_multi_values(
            results,
            storage
                .raw_batch_scan(
                    Context::default(),
                    "".to_string(),
                    ranges.clone(),
                    3,
                    false,
                    false,
                )
                .wait(),
        );

        let results = vec![
            Some((b"a".to_vec(), vec![])),
            Some((b"a1".to_vec(), vec![])),
            Some((b"a2".to_vec(), vec![])),
            Some((b"b".to_vec(), vec![])),
            Some((b"b1".to_vec(), vec![])),
            Some((b"b2".to_vec(), vec![])),
            Some((b"c".to_vec(), vec![])),
            Some((b"c1".to_vec(), vec![])),
            Some((b"c2".to_vec(), vec![])),
        ];
        expect_multi_values(
            results,
            storage
                .raw_batch_scan(Context::default(), "".to_string(), ranges, 3, true, false)
                .wait(),
        );

        let results = vec![
            Some((b"a2".to_vec(), b"aa22".to_vec())),
            Some((b"a1".to_vec(), b"aa11".to_vec())),
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"b2".to_vec(), b"bb22".to_vec())),
            Some((b"b1".to_vec(), b"bb11".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"c2".to_vec(), b"cc22".to_vec())),
            Some((b"c1".to_vec(), b"cc11".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
        ];
        let ranges: Vec<KeyRange> = vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]
        .into_iter()
        .map(|(s, e)| {
            let mut range = KeyRange::default();
            range.set_start_key(s);
            range.set_end_key(e);
            range
        })
        .collect();
        expect_multi_values(
            results,
            storage
                .raw_batch_scan(Context::default(), "".to_string(), ranges, 5, false, true)
                .wait(),
        );

        let results = vec![
            Some((b"c2".to_vec(), b"cc22".to_vec())),
            Some((b"c1".to_vec(), b"cc11".to_vec())),
            Some((b"b2".to_vec(), b"bb22".to_vec())),
            Some((b"b1".to_vec(), b"bb11".to_vec())),
            Some((b"a2".to_vec(), b"aa22".to_vec())),
            Some((b"a1".to_vec(), b"aa11".to_vec())),
        ];
        let ranges: Vec<KeyRange> = vec![b"c3".to_vec(), b"b3".to_vec(), b"a3".to_vec()]
            .into_iter()
            .map(|s| {
                let mut range = KeyRange::default();
                range.set_start_key(s);
                range
            })
            .collect();
        expect_multi_values(
            results,
            storage
                .raw_batch_scan(Context::default(), "".to_string(), ranges, 2, false, true)
                .wait(),
        );

        let results = vec![
            Some((b"a2".to_vec(), vec![])),
            Some((b"a1".to_vec(), vec![])),
            Some((b"a".to_vec(), vec![])),
            Some((b"b2".to_vec(), vec![])),
            Some((b"b1".to_vec(), vec![])),
            Some((b"b".to_vec(), vec![])),
            Some((b"c2".to_vec(), vec![])),
            Some((b"c1".to_vec(), vec![])),
            Some((b"c".to_vec(), vec![])),
        ];
        let ranges: Vec<KeyRange> = vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]
        .into_iter()
        .map(|(s, e)| {
            let mut range = KeyRange::default();
            range.set_start_key(s);
            range.set_end_key(e);
            range
        })
        .collect();
        expect_multi_values(
            results,
            storage
                .raw_batch_scan(Context::default(), "".to_string(), ranges, 5, true, true)
                .wait(),
        );
    }

    #[test]
    fn test_scan_lock() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"x"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"y"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"z"), b"foo".to_vec())),
                    ],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                    ],
                    b"c".to_vec(),
                    101.into(),
                    123,
                    false,
                    3,
                    TimeStamp::default(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let (lock_a, lock_b, lock_c, lock_x, lock_y, lock_z) = (
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"a".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"b".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"c".to_vec());
                lock.set_lock_ttl(123);
                lock.set_txn_size(3);
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"x".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"y".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"z".to_vec());
                lock
            },
        );

        storage
            .sched_txn_command(
                commands::ScanLock::new(99.into(), None, 10, Context::default()),
                expect_value_callback(tx.clone(), 0, vec![]),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ScanLock::new(100.into(), None, 10, Context::default()),
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![lock_x.clone(), lock_y.clone(), lock_z.clone()],
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ScanLock::new(
                    100.into(),
                    Some(Key::from_raw(b"a")),
                    10,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![lock_x.clone(), lock_y.clone(), lock_z.clone()],
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ScanLock::new(
                    100.into(),
                    Some(Key::from_raw(b"y")),
                    10,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, vec![lock_y.clone(), lock_z.clone()]),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ScanLock::new(101.into(), None, 10, Context::default()),
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![
                        lock_a.clone(),
                        lock_b.clone(),
                        lock_c.clone(),
                        lock_x.clone(),
                        lock_y.clone(),
                        lock_z.clone(),
                    ],
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ScanLock::new(101.into(), None, 4, Context::default()),
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![lock_a, lock_b.clone(), lock_c.clone(), lock_x.clone()],
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ScanLock::new(
                    101.into(),
                    Some(Key::from_raw(b"b")),
                    4,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![
                        lock_b.clone(),
                        lock_c.clone(),
                        lock_x.clone(),
                        lock_y.clone(),
                    ],
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ScanLock::new(
                    101.into(),
                    Some(Key::from_raw(b"b")),
                    0,
                    Context::default(),
                ),
                expect_value_callback(tx, 0, vec![lock_b, lock_c, lock_x, lock_y, lock_z]),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_resolve_lock() {
        use crate::storage::txn::RESOLVE_LOCK_BATCH_SIZE;

        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        // These locks (transaction ts=99) are not going to be resolved.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                    ],
                    b"c".to_vec(),
                    99.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let (lock_a, lock_b, lock_c) = (
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"a".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"b".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::default();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"c".to_vec());
                lock
            },
        );

        // We should be able to resolve all locks for transaction ts=100 when there are this
        // many locks.
        let scanned_locks_coll = vec![
            1,
            RESOLVE_LOCK_BATCH_SIZE,
            RESOLVE_LOCK_BATCH_SIZE - 1,
            RESOLVE_LOCK_BATCH_SIZE + 1,
            RESOLVE_LOCK_BATCH_SIZE * 2,
            RESOLVE_LOCK_BATCH_SIZE * 2 - 1,
            RESOLVE_LOCK_BATCH_SIZE * 2 + 1,
        ];

        let is_rollback_coll = vec![
            false, // commit
            true,  // rollback
        ];
        let mut ts = 100.into();

        for scanned_locks in scanned_locks_coll {
            for is_rollback in &is_rollback_coll {
                let mut mutations = vec![];
                for i in 0..scanned_locks {
                    mutations.push(Mutation::Put((
                        Key::from_raw(format!("x{:08}", i).as_bytes()),
                        b"foo".to_vec(),
                    )));
                }

                storage
                    .sched_txn_command(
                        commands::Prewrite::with_defaults(mutations, b"x".to_vec(), ts),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let mut txn_status = HashMap::default();
                txn_status.insert(
                    ts,
                    if *is_rollback {
                        TimeStamp::zero() // rollback
                    } else {
                        (ts.into_inner() + 5).into() // commit, commit_ts = start_ts + 5
                    },
                );
                storage
                    .sched_txn_command(
                        commands::ResolveLock::new(txn_status, None, vec![], Context::default()),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                // All locks should be resolved except for a, b and c.
                storage
                    .sched_txn_command(
                        commands::ScanLock::new(ts, None, 0, Context::default()),
                        expect_value_callback(
                            tx.clone(),
                            0,
                            vec![lock_a.clone(), lock_b.clone(), lock_c.clone()],
                        ),
                    )
                    .unwrap();
                rx.recv().unwrap();

                ts = (ts.into_inner() + 10).into();
            }
        }
    }

    #[test]
    fn test_resolve_lock_lite() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                    ],
                    b"c".to_vec(),
                    99.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Rollback key 'b' and key 'c' and left key 'a' still locked.
        let resolve_keys = vec![Key::from_raw(b"b"), Key::from_raw(b"c")];
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    99.into(),
                    TimeStamp::zero(),
                    resolve_keys,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Check lock for key 'a'.
        let lock_a = {
            let mut lock = LockInfo::default();
            lock.set_primary_lock(b"c".to_vec());
            lock.set_lock_version(99);
            lock.set_key(b"a".to_vec());
            lock
        };
        storage
            .sched_txn_command(
                commands::ScanLock::new(99.into(), None, 0, Context::default()),
                expect_value_callback(tx.clone(), 0, vec![lock_a]),
            )
            .unwrap();
        rx.recv().unwrap();

        // Resolve lock for key 'a'.
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    99.into(),
                    TimeStamp::zero(),
                    vec![Key::from_raw(b"a")],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                    ],
                    b"c".to_vec(),
                    101.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Commit key 'b' and key 'c' and left key 'a' still locked.
        let resolve_keys = vec![Key::from_raw(b"b"), Key::from_raw(b"c")];
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    101.into(),
                    102.into(),
                    resolve_keys,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Check lock for key 'a'.
        let lock_a = {
            let mut lock = LockInfo::default();
            lock.set_primary_lock(b"c".to_vec());
            lock.set_lock_version(101);
            lock.set_key(b"a".to_vec());
            lock
        };
        storage
            .sched_txn_command(
                commands::ScanLock::new(101.into(), None, 0, Context::default()),
                expect_value_callback(tx, 0, vec![lock_a]),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_txn_heart_beat() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let k = Key::from_raw(b"k");
        let v = b"v".to_vec();

        let uncommitted = TxnStatus::uncommitted;

        // No lock.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 100, Context::default()),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnLockNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::Put((k.clone(), v))],
                    k.as_encoded().to_vec(),
                    10.into(),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // `advise_ttl` = 90, which is less than current ttl 100. The lock's ttl will remains 100.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 90, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(100, TimeStamp::zero())),
            )
            .unwrap();
        rx.recv().unwrap();

        // `advise_ttl` = 110, which is greater than current ttl. The lock's ttl will be updated to
        // 110.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 110, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(110, TimeStamp::zero())),
            )
            .unwrap();
        rx.recv().unwrap();

        // Lock not match. Nothing happens except throwing an error.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k, 11.into(), 150, Context::default()),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnLockNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_check_txn_status() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let k = Key::from_raw(b"k");
        let v = b"b".to_vec();

        let ts = TimeStamp::compose;
        use TxnStatus::*;
        let uncommitted = TxnStatus::uncommitted;
        let committed = TxnStatus::committed;

        // No lock and no commit info. Gets an error.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(9, 0),
                    ts(9, 1),
                    ts(9, 1),
                    false,
                    Context::default(),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        // No lock and no commit info. If specified rollback_if_not_exist, the key will be rolled
        // back.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(9, 0),
                    ts(9, 1),
                    ts(9, 1),
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, LockNotExist),
            )
            .unwrap();
        rx.recv().unwrap();

        // A rollback will be written, so an later-arriving prewrite will fail.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((k.clone(), v.clone()))],
                    k.as_encoded().to_vec(),
                    ts(9, 0),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::WriteConflict { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::Put((k.clone(), v.clone()))],
                    k.as_encoded().to_vec(),
                    ts(10, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If lock exists and not expired, returns the lock's TTL.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    ts(12, 0),
                    ts(15, 0),
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, uncommitted(100, TimeStamp::zero())),
            )
            .unwrap();
        rx.recv().unwrap();

        // TODO: Check the lock's min_commit_ts field.

        storage
            .sched_txn_command(
                commands::Commit::new(vec![k.clone()], ts(10, 0), ts(20, 0), Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the transaction is committed, returns the commit_ts.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    ts(12, 0),
                    ts(15, 0),
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, committed(ts(20, 0))),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::Put((k.clone(), v))],
                    k.as_encoded().to_vec(),
                    ts(25, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the lock has expired, cleanup it.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(25, 0),
                    ts(126, 0),
                    ts(127, 0),
                    true,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Commit::new(vec![k], ts(25, 0), ts(28, 0), Context::default()),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnLockNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_construct_point_get_command_from_get_request() {
        let mut context = Context::default();
        context.set_region_id(1);
        let raw_key = b"raw_key".to_vec();
        let version = 10;
        let mut req = GetRequest::default();
        req.set_context(context.clone());
        req.set_key(raw_key.clone());
        req.set_version(version);
        let cmd = PointGetCommand::from_get(&mut req);
        assert_eq!(cmd.ctx, context);
        assert_eq!(cmd.key, Key::from_raw(&raw_key));
        assert_eq!(cmd.ts, Some(TimeStamp::new(version)));
    }

    #[test]
    fn test_construct_point_get_command_from_raw_get_request() {
        let mut context = Context::default();
        context.set_region_id(1);
        let raw_key = b"raw_key".to_vec();
        let mut req = RawGetRequest::default();
        req.set_context(context.clone());
        req.set_key(raw_key.clone());
        let cmd = PointGetCommand::from_raw_get(&mut req);
        assert_eq!(cmd.ctx, context);
        assert_eq!(cmd.key.into_encoded(), raw_key);
        assert_eq!(cmd.ts, None);
    }

    fn test_pessimistic_lock_impl(pipelined_pessimistic_lock: bool) {
        type PessimisticLockCommand = TypedCommand<Result<PessimisticLockRes>>;
        fn new_acquire_pessimistic_lock_command(
            keys: Vec<(Key, bool)>,
            start_ts: impl Into<TimeStamp>,
            for_update_ts: impl Into<TimeStamp>,
            return_values: bool,
        ) -> PessimisticLockCommand {
            let primary = keys[0].0.clone().to_raw().unwrap();
            let for_update_ts: TimeStamp = for_update_ts.into();
            commands::AcquirePessimisticLock::new(
                keys,
                primary,
                start_ts.into(),
                3000,
                false,
                for_update_ts,
                None,
                return_values,
                for_update_ts.next(),
                Context::default(),
            )
        }

        fn delete_pessimistic_lock<E: Engine, L: LockManager>(
            storage: &Storage<E, L>,
            key: Key,
            start_ts: u64,
            for_update_ts: u64,
        ) {
            let (tx, rx) = channel();
            storage
                .sched_txn_command(
                    commands::PessimisticRollback::new(
                        vec![key],
                        start_ts.into(),
                        for_update_ts.into(),
                        Context::default(),
                    ),
                    expect_ok_callback(tx, 0),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        fn expect_pessimistic_lock_res_callback(
            done: Sender<i32>,
            pessimistic_lock_res: PessimisticLockRes,
        ) -> Callback<Result<PessimisticLockRes>> {
            Box::new(move |res: Result<Result<PessimisticLockRes>>| {
                assert_eq!(res.unwrap().unwrap(), pessimistic_lock_res);
                done.send(0).unwrap();
            })
        }

        let storage = TestStorageBuilder::new()
            .enable_pessimistic_txn()
            .set_pipelined_pessimistic_lock(pipelined_pessimistic_lock)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let (key, val) = (Key::from_raw(b"key"), b"val".to_vec());
        let (key2, val2) = (Key::from_raw(b"key2"), b"val2".to_vec());

        // Key not exist
        for &return_values in &[false, true] {
            let pessimistic_lock_res = if return_values {
                PessimisticLockRes::Values(vec![None])
            } else {
                PessimisticLockRes::Empty
            };

            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        10,
                        10,
                        return_values,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res.clone()),
                )
                .unwrap();
            rx.recv().unwrap();

            // Duplicated command
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        10,
                        10,
                        return_values,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res.clone()),
                )
                .unwrap();
            rx.recv().unwrap();

            delete_pessimistic_lock(&storage, key.clone(), 10, 10);
        }

        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 10, 10, false),
                expect_pessimistic_lock_res_callback(tx.clone(), PessimisticLockRes::Empty),
            )
            .unwrap();
        rx.recv().unwrap();

        // KeyIsLocked
        for &return_values in &[false, true] {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        20,
                        20,
                        return_values,
                    ),
                    expect_fail_callback(tx.clone(), 0, |e| match e {
                        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            mvcc::Error(box mvcc::ErrorInner::KeyIsLocked(_)),
                        )))) => (),
                        e => panic!("unexpected error chain: {:?}", e),
                    }),
                )
                .unwrap();
            // The DummyLockManager consumes the Msg::WaitForLock.
            rx.recv_timeout(std::time::Duration::from_millis(100))
                .unwrap_err();
        }

        // Put key and key2.
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (Mutation::Put((key.clone(), val.clone())), true),
                        (Mutation::Put((key2.clone(), val2.clone())), false),
                    ],
                    key.to_raw().unwrap(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    TimeStamp::zero(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![key.clone(), key2.clone()],
                    10.into(),
                    20.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // WriteConflict
        for &return_values in &[false, true] {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![(key.clone(), false)],
                        15,
                        15,
                        return_values,
                    ),
                    expect_fail_callback(tx.clone(), 0, |e| match e {
                        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            mvcc::Error(box mvcc::ErrorInner::WriteConflict { .. }),
                        )))) => (),
                        e => panic!("unexpected error chain: {:?}", e),
                    }),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        // Return multiple values
        for &return_values in &[false, true] {
            let pessimistic_lock_res = if return_values {
                PessimisticLockRes::Values(vec![Some(val.clone()), Some(val2.clone()), None])
            } else {
                PessimisticLockRes::Empty
            };
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        vec![
                            (key.clone(), false),
                            (key2.clone(), false),
                            (Key::from_raw(b"key3"), false),
                        ],
                        30,
                        30,
                        return_values,
                    ),
                    expect_pessimistic_lock_res_callback(tx.clone(), pessimistic_lock_res),
                )
                .unwrap();
            rx.recv().unwrap();

            delete_pessimistic_lock(&storage, key.clone(), 30, 30);
        }
    }

    #[test]
    fn test_pessimistic_lock() {
        test_pessimistic_lock_impl(false);
        test_pessimistic_lock_impl(true);
    }
}
