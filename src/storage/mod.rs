// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains TiKV's transaction layer. It lowers high-level, transactional
//! commands to low-level (raw key-value) interactions with persistent storage.
//!
//! This module is further split into layers: [`txn`](txn) lowers transactional commands to
//! key-value operations on an MVCC abstraction. [`mvcc`](mvcc) is our MVCC implementation.
//! [`kv`](kv) is an abstraction layer over persistent storage.
//!
//! Other responsibilities of this module are managing latches (see [`latch`](txn::latch)), deadlock
//! and wait handling (see [`lock_manager`](lock_manager)), scheduling command execution (see
//! [`txn::scheduler`](txn::scheduler)), and handling commands from the raw and versioned APIs (in
//! the [`Storage`](Storage) struct).
//!
//! For more information about TiKV's transactions, see the [sig-txn docs](https://github.com/tikv/sig-transaction/tree/master/doc).
//!
//! Some important types are:
//!
//! * the [`Engine`](kv::Engine) trait and related traits, which abstracts over underlying storage,
//! * the [`MvccTxn`](mvcc::txn::MvccTxn) struct, which is the primary object in the MVCC
//!   implementation,
//! * the commands in the [`commands`](txn::commands) module, which are how each command is implemented,
//! * the [`Storage`](Storage) struct, which is the primary entry point for this module.
//!
//! Related code:
//!
//! * the [`kv`](crate::server::service::kv) module, which is the interface for TiKV's APIs,
//! * the [`lock_manager](crate::server::lock_manager), which takes part in lock and deadlock
//!   management,
//! * [`gc_worker`](crate::server::gc_worker), which drives garbage collection of old values,
//! * the [`txn_types](::txn_types) crate, some important types for this module's interface,
//! * the [`kvproto`](::kvproto) crate, which defines TiKV's protobuf API and includes some
//!   documentation of the commands implemented here,
//! * the [`test_storage`](::test_storage) crate, integration tests for this module,
//! * the [`engine_traits`](::engine_traits) crate, more detail of the engine abstraction.

pub mod config;
pub mod errors;
pub mod kv;
pub mod lock_manager;
pub(crate) mod metrics;
pub mod mvcc;
pub mod raw;
pub mod txn;

mod read_pool;
mod types;

use self::kv::SnapContext;
pub use self::{
    errors::{get_error_kind_from_header, get_tag_from_header, Error, ErrorHeaderKind, ErrorInner},
    kv::{
        CbContext, CfStatistics, Cursor, CursorBuilder, Engine, FlowStatistics, FlowStatsReporter,
        Iterator, PerfStatisticsDelta, PerfStatisticsInstant, RocksEngine, ScanMode, Snapshot,
        Statistics, TestEngineBuilder,
    },
    raw::{RawStore, TTLSnapshot},
    read_pool::{build_read_pool, build_read_pool_for_test},
    txn::{Latches, Lock as LatchLock, ProcessResult, Scanner, SnapshotStore, Store},
    types::{PessimisticLockRes, PrewriteResult, SecondaryLocksStatus, StorageCallback, TxnStatus},
};

use crate::read_pool::{ReadPool, ReadPoolHandle};
use crate::storage::metrics::CommandKind;
use crate::storage::mvcc::MvccReader;
use crate::storage::txn::commands::{RawAtomicStore, RawCompareAndSwap};

use crate::server::lock_manager::waiter_manager;
use crate::storage::{
    config::Config,
    kv::{with_tls_engine, Modify, WriteData},
    lock_manager::{DummyLockManager, LockManager},
    metrics::*,
    mvcc::PointGetterBuilder,
    raw::ttl::convert_to_expire_ts,
    txn::{commands::TypedCommand, scheduler::Scheduler as TxnScheduler, Command},
    types::StorageCallbackType,
};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, CF_DEFAULT, DATA_CFS};
use futures::prelude::*;
use kvproto::kvrpcpb::{
    CommandPri, Context, GetRequest, IsolationLevel, KeyRange, LockInfo, RawGetRequest,
};
use kvproto::pdpb::QueryKind;
use raftstore::store::util::build_key_range;
use rand::prelude::*;
use resource_metering::{cpu::FutureExt, ResourceMeteringTag};
use std::{
    borrow::Cow,
    iter,
    sync::{atomic, Arc},
};
use tikv_util::time::{Instant, ThreadReadId};
use txn_types::{Key, KvPair, Lock, Mutation, OldValues, TimeStamp, TsSet, Value};

pub type Result<T> = std::result::Result<T, Error>;
pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;

/// [`Storage`](Storage) implements transactional KV APIs and raw KV APIs on a given [`Engine`].
/// An [`Engine`] provides low level KV functionality. [`Engine`] has multiple implementations.
/// When a TiKV server is running, a [`RaftKv`](crate::server::raftkv::RaftKv) will be the
/// underlying [`Engine`] of [`Storage`]. The other two types of engines are for test purpose.
///
///[`Storage`] is reference counted and cloning [`Storage`] will just increase the reference counter.
/// Storage resources (i.e. threads, engine) will be released when all references are dropped.
///
/// Notice that read and write methods may not be performed over full data in most cases, i.e. when
/// underlying engine is [`RaftKv`](crate::server::raftkv::RaftKv),
/// which limits data access in the range of a single region
/// according to specified `ctx` parameter. However,
/// [`unsafe_destroy_range`](crate::server::gc_worker::GcTask::UnsafeDestroyRange) is the only exception.
/// It's always performed on the whole TiKV.
///
/// Operations of [`Storage`](Storage) can be divided into two types: MVCC operations and raw operations.
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

    concurrency_manager: ConcurrencyManager,

    /// How many strong references. Thread pool and workers will be stopped
    /// once there are no more references.
    // TODO: This should be implemented in thread pool and worker.
    refs: Arc<atomic::AtomicUsize>,

    // Fields below are storage configurations.
    max_key_size: usize,

    enable_ttl: bool,
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
            concurrency_manager: self.concurrency_manager.clone(),
            enable_ttl: self.enable_ttl,
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
                $callback(Err(Error::from(ErrorInner::KeyTooLarge {
                    size: key_size,
                    limit: $max_key_size,
                })));
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
        lock_mgr: L,
        concurrency_manager: ConcurrencyManager,
        pipelined_pessimistic_lock: Arc<atomic::AtomicBool>,
    ) -> Result<Self> {
        let sched = TxnScheduler::new(
            engine.clone(),
            lock_mgr,
            concurrency_manager.clone(),
            config.scheduler_concurrency,
            config.scheduler_worker_pool_size,
            config.scheduler_pending_write_threshold.0 as usize,
            pipelined_pessimistic_lock,
            config.enable_async_apply_prewrite,
        );

        info!("Storage started.");

        Ok(Storage {
            engine,
            sched,
            read_pool,
            concurrency_manager,
            refs: Arc::new(atomic::AtomicUsize::new(1)),
            max_key_size: config.max_key_size,
            enable_ttl: config.enable_ttl,
        })
    }

    /// Get the underlying `Engine` of the `Storage`.
    pub fn get_engine(&self) -> E {
        self.engine.clone()
    }

    pub fn get_concurrency_manager(&self) -> ConcurrencyManager {
        self.concurrency_manager.clone()
    }

    pub fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.sched.dump_wait_for_entries(cb);
    }

    /// Get a snapshot of `engine`.
    fn snapshot(
        engine: &E,
        ctx: SnapContext<'_>,
    ) -> impl std::future::Future<Output = Result<E::Snap>> {
        kv::snapshot(engine, ctx)
            .map_err(txn::Error::from)
            .map_err(Error::from)
    }

    #[cfg(test)]
    pub fn get_snapshot(&self) -> E::Snap {
        self.engine.snapshot(Default::default()).unwrap()
    }

    pub fn release_snapshot(&self) {
        self.engine.release_snapshot();
    }

    pub fn get_readpool_queue_per_worker(&self) -> usize {
        self.read_pool.get_queue_size_per_worker()
    }

    pub fn get_normal_pool_size(&self) -> usize {
        self.read_pool.get_normal_pool_size()
    }

    #[inline]
    fn with_tls_engine<F, R>(f: F) -> R
    where
        F: FnOnce(&E) -> R,
    {
        // Safety: the read pools ensure that a TLS engine exists.
        unsafe { with_tls_engine(f) }
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

    /// Get value of the given key from a snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn get(
        &self,
        mut ctx: Context,
        key: Key,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Option<Value>, Statistics, PerfStatisticsDelta)>> {
        const CMD: CommandKind = CommandKind::get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = ResourceMeteringTag::from_rpc_context(&ctx);
        let concurrency_manager = self.concurrency_manager.clone();

        let res = self.read_pool.spawn_handle(
            async move {
                tls_collect_query(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key.as_encoded(),
                    key.as_encoded(),
                    false,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                // The bypass_locks set will be checked at most once. `TsSet::vec` is more efficient
                // here.
                let bypass_locks = TsSet::vec_from_u64s(ctx.take_resolved_locks());

                let snap_ctx = prepare_snap_ctx(
                    &ctx,
                    iter::once(&key),
                    start_ts,
                    &bypass_locks,
                    &concurrency_manager,
                    CMD,
                )?;
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut statistics = Statistics::default();
                    let perf_statistics = PerfStatisticsInstant::new();
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

                    Ok((result?, statistics, perf_statistics.delta()))
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get values of a set of keys with separate context from a snapshot, return a list of `Result`s.
    ///
    /// Only writes that are committed before their respective `start_ts` are visible.
    pub fn batch_get_command<
        P: 'static + ResponseBatchConsumer<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>,
    >(
        &self,
        requests: Vec<GetRequest>,
        ids: Vec<u64>,
        consumer: P,
    ) -> impl Future<Output = Result<()>> {
        const CMD: CommandKind = CommandKind::batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let priority = requests[0].get_context().get_priority();
        let concurrency_manager = self.concurrency_manager.clone();
        let res = self.read_pool.spawn_handle(
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(requests.len() as f64);
                let command_duration = tikv_util::time::Instant::now_coarse();
                let read_id = Some(ThreadReadId::new());
                let mut statistics = Statistics::default();
                let mut req_snaps = vec![];

                for (mut req, id) in requests.into_iter().zip(ids) {
                    let mut ctx = req.take_context();
                    let region_id = ctx.get_region_id();
                    let peer = ctx.get_peer();
                    let key = Key::from_raw(req.get_key());
                    tls_collect_query(
                        region_id,
                        peer,
                        key.as_encoded(),
                        key.as_encoded(),
                        false,
                        QueryKind::Get,
                    );
                    let start_ts = req.get_version().into();
                    let isolation_level = ctx.get_isolation_level();
                    let fill_cache = !ctx.get_not_fill_cache();
                    let bypass_locks = TsSet::vec_from_u64s(ctx.take_resolved_locks());
                    let region_id = ctx.get_region_id();

                    let snap_ctx = match prepare_snap_ctx(
                        &ctx,
                        iter::once(&key),
                        start_ts,
                        &bypass_locks,
                        &concurrency_manager,
                        CMD,
                    ) {
                        Ok(mut snap_ctx) => {
                            snap_ctx.read_id = if ctx.get_stale_read() {
                                None
                            } else {
                                read_id.clone()
                            };
                            snap_ctx
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e));
                            continue;
                        }
                    };

                    let resource_tag = ResourceMeteringTag::from_rpc_context(&ctx);
                    let snap = Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx));
                    req_snaps.push((
                        snap,
                        key,
                        start_ts,
                        isolation_level,
                        fill_cache,
                        bypass_locks,
                        region_id,
                        id,
                        resource_tag,
                    ));
                }
                Self::with_tls_engine(|engine| engine.release_snapshot());
                for req_snap in req_snaps {
                    let (
                        snap,
                        key,
                        start_ts,
                        isolation_level,
                        fill_cache,
                        bypass_locks,
                        region_id,
                        id,
                        resource_tag,
                    ) = req_snap;
                    match snap.await {
                        Ok(snapshot) => {
                            let _g = resource_tag.attach();
                            match PointGetterBuilder::new(snapshot, start_ts)
                                .fill_cache(fill_cache)
                                .isolation_level(isolation_level)
                                .multi(false)
                                .bypass_locks(bypass_locks)
                                .build()
                            {
                                Ok(mut point_getter) => {
                                    let perf_statistics = PerfStatisticsInstant::new();
                                    let v = point_getter.get(&key);
                                    let stat = point_getter.take_statistics();
                                    metrics::tls_collect_read_flow(region_id, &stat);
                                    statistics.add(&stat);
                                    consumer.consume(
                                        id,
                                        v.map_err(|e| Error::from(txn::Error::from(e)))
                                            .map(|v| (v, stat, perf_statistics.delta())),
                                    );
                                }
                                Err(e) => {
                                    consumer.consume(id, Err(Error::from(txn::Error::from(e))));
                                }
                            }
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e));
                        }
                    }
                }
                metrics::tls_collect_scan_details(CMD, &statistics);
                SCHED_HISTOGRAM_VEC_STATIC
                    .get(CMD)
                    .observe(command_duration.elapsed_secs());

                Ok(())
            },
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get values of a set of keys in a batch from the snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn batch_get(
        &self,
        mut ctx: Context,
        keys: Vec<Key>,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Vec<Result<KvPair>>, Statistics, PerfStatisticsDelta)>> {
        const CMD: CommandKind = CommandKind::batch_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = ResourceMeteringTag::from_rpc_context(&ctx);
        let concurrency_manager = self.concurrency_manager.clone();

        let res = self.read_pool.spawn_handle(
            async move {
                let mut key_ranges = vec![];
                for key in &keys {
                    key_ranges.push(build_key_range(key.as_encoded(), key.as_encoded(), false));
                }
                tls_collect_query_batch(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key_ranges,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                let bypass_locks = TsSet::from_u64s(ctx.take_resolved_locks());

                let snap_ctx = prepare_snap_ctx(
                    &ctx,
                    &keys,
                    start_ts,
                    &bypass_locks,
                    &concurrency_manager,
                    CMD,
                )?;
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut statistics = Statistics::default();
                    let perf_statistics = PerfStatisticsInstant::new();
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

                    Ok((result?, statistics, perf_statistics.delta()))
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
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
        sample_step: usize,
        start_ts: TimeStamp,
        key_only: bool,
        reverse_scan: bool,
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::scan;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = ResourceMeteringTag::from_rpc_context(&ctx);
        let concurrency_manager = self.concurrency_manager.clone();

        let res = self.read_pool.spawn_handle(
            async move {
                {
                    let end_key = match &end_key {
                        Some(k) => k.as_encoded().as_slice(),
                        None => &[],
                    };
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key,
                        reverse_scan,
                        QueryKind::Scan,
                    );
                }
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                let bypass_locks = TsSet::from_u64s(ctx.take_resolved_locks());

                // Update max_ts and check the in-memory lock table before getting the snapshot
                if !ctx.get_stale_read() {
                    concurrency_manager.update_max_ts(start_ts);
                }
                if ctx.get_isolation_level() == IsolationLevel::Si {
                    let begin_instant = Instant::now();
                    concurrency_manager
                        .read_range_check(Some(&start_key), end_key.as_ref(), |key, lock| {
                            Lock::check_ts_conflict(
                                Cow::Borrowed(lock),
                                &key,
                                start_ts,
                                &bypass_locks,
                            )
                        })
                        .map_err(|e| {
                            CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                                .get(CMD)
                                .locked
                                .observe(begin_instant.elapsed().as_secs_f64());
                            txn::Error::from_mvcc(e)
                        })?;
                    CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                        .get(CMD)
                        .unlocked
                        .observe(begin_instant.elapsed().as_secs_f64());
                }

                let mut snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    start_ts,
                    ..Default::default()
                };
                if need_check_locks_in_replica_read(&ctx) {
                    let mut key_range = KeyRange::default();
                    key_range.set_start_key(start_key.as_encoded().to_vec());
                    if let Some(end_key) = &end_key {
                        key_range.set_end_key(end_key.as_encoded().to_vec());
                    }
                    snap_ctx.key_ranges = vec![key_range];
                }

                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
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
                    let res = scanner.scan(limit, sample_step);

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
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    pub fn scan_lock(
        &self,
        mut ctx: Context,
        max_ts: TimeStamp,
        start_key: Option<Key>,
        end_key: Option<Key>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<LockInfo>>> {
        const CMD: CommandKind = CommandKind::scan_lock;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = ResourceMeteringTag::from_rpc_context(&ctx);
        let concurrency_manager = self.concurrency_manager.clone();
        // Do not allow replica read for scan_lock.
        ctx.set_replica_read(false);

        let res = self.read_pool.spawn_handle(
            async move {
                if let Some(start_key) = &start_key {
                    let end_key = match &end_key {
                        Some(k) => k.as_encoded().as_slice(),
                        None => &[],
                    };
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key,
                        false,
                        QueryKind::Scan,
                    );
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();

                concurrency_manager.update_max_ts(max_ts);
                let begin_instant = Instant::now();
                // TODO: Though it's very unlikely to find a conflicting memory lock here, it's not
                // a good idea to return an error to the client, making the GC fail. A better
                // approach is to wait for these locks to be unlocked.
                concurrency_manager.read_range_check(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |key, lock| {
                        // `Lock::check_ts_conflict` can't be used here, because LockType::Lock
                        // can't be ignored in this case.
                        if lock.ts <= max_ts {
                            CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                                .get(CMD)
                                .locked
                                .observe(begin_instant.elapsed().as_secs_f64());
                            Err(txn::Error::from_mvcc(mvcc::ErrorInner::KeyIsLocked(
                                lock.clone().into_lock_info(key.to_raw()?),
                            )))
                        } else {
                            Ok(())
                        }
                    },
                )?;
                CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                    .get(CMD)
                    .unlocked
                    .observe(begin_instant.elapsed().as_secs_f64());

                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };

                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut statistics = Statistics::default();
                    let mut reader = MvccReader::new(
                        snapshot,
                        Some(ScanMode::Forward),
                        !ctx.get_not_fill_cache(),
                    );
                    let result = reader
                        .scan_locks(
                            start_key.as_ref(),
                            end_key.as_ref(),
                            |lock| lock.ts <= max_ts,
                            limit,
                        )
                        .map_err(txn::Error::from);
                    statistics.add(&reader.statistics);
                    let (kv_pairs, _) = result?;
                    let mut locks = Vec::with_capacity(kv_pairs.len());
                    for (key, lock) in kv_pairs {
                        let lock_info =
                            lock.into_lock_info(key.into_raw().map_err(txn::Error::from)?);
                        locks.push(lock_info);
                    }

                    metrics::tls_collect_scan_details(CMD, &statistics);
                    metrics::tls_collect_read_flow(ctx.get_region_id(), &statistics);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());

                    Ok(locks)
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    pub fn sched_txn_command<T: StorageCallbackType>(
        &self,
        cmd: TypedCommand<T>,
        callback: Callback<T>,
    ) -> Result<()> {
        use crate::storage::txn::commands::{
            AcquirePessimisticLock, Prewrite, PrewritePessimistic,
        };

        let cmd: Command = cmd.into();

        match &cmd {
            Command::Prewrite(Prewrite { mutations, .. }) => {
                check_key_size!(
                    mutations.iter().map(|m| m.key().as_encoded()),
                    self.max_key_size,
                    callback
                );
            }
            Command::PrewritePessimistic(PrewritePessimistic { mutations, .. }) => {
                check_key_size!(
                    mutations.iter().map(|(m, _)| m.key().as_encoded()),
                    self.max_key_size,
                    callback
                );
            }
            Command::AcquirePessimisticLock(AcquirePessimisticLock { keys, .. }) => {
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
    /// [`unsafe_destroy_range`](crate::server::gc_worker::GcTask::UnsafeDestroyRange) soon.
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
            WriteData::from_modifies(modifies),
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
    ) -> impl Future<Output = Result<Option<Vec<u8>>>> {
        const CMD: CommandKind = CommandKind::raw_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let enable_ttl = self.enable_ttl;

        let res = self.read_pool.spawn_handle(
            async move {
                tls_collect_query(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    &key,
                    &key,
                    false,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let store = RawStore::new(snapshot, enable_ttl);
                let cf = Self::rawkv_cf(&cf)?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut stats = Statistics::default();
                    let r = store.raw_get_key_value(cf, &Key::from_encoded(key), &mut stats);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                    tls_collect_read_flow(ctx.get_region_id(), &stats);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());
                    r
                }
            },
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get the values of a set of raw keys, return a list of `Result`s.
    pub fn raw_batch_get_command<P: 'static + ResponseBatchConsumer<Option<Vec<u8>>>>(
        &self,
        gets: Vec<RawGetRequest>,
        ids: Vec<u64>,
        consumer: P,
    ) -> impl Future<Output = Result<()>> {
        const CMD: CommandKind = CommandKind::raw_batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let priority = gets[0].get_context().get_priority();
        let priority_tag = get_priority_tag(priority);
        let enable_ttl = self.enable_ttl;

        let res = self.read_pool.spawn_handle(
            async move {
                for get in &gets {
                    let key = get.key.to_owned();
                    let region_id = get.get_context().get_region_id();
                    let peer = get.get_context().get_peer();
                    tls_collect_query(region_id, peer, &key, &key, false, QueryKind::Get);
                }
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();
                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(gets.len() as f64);
                let command_duration = tikv_util::time::Instant::now_coarse();
                let read_id = Some(ThreadReadId::new());
                let mut snaps = vec![];
                for (req, id) in gets.into_iter().zip(ids) {
                    let snap_ctx = SnapContext {
                        pb_ctx: req.get_context(),
                        read_id: read_id.clone(),
                        ..Default::default()
                    };
                    let snap = Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx));
                    snaps.push((id, req, snap));
                }
                Self::with_tls_engine(|engine| engine.release_snapshot());
                let begin_instant = Instant::now_coarse();
                for (id, mut req, snap) in snaps {
                    let ctx = req.take_context();
                    let cf = req.take_cf();
                    let key = req.take_key();
                    match snap.await {
                        Ok(snapshot) => {
                            let mut stats = Statistics::default();
                            let store = RawStore::new(snapshot, enable_ttl);
                            match Self::rawkv_cf(&cf) {
                                Ok(cf) => {
                                    consumer.consume(
                                        id,
                                        store.raw_get_key_value(
                                            cf,
                                            &Key::from_encoded(key),
                                            &mut stats,
                                        ),
                                    );
                                    tls_collect_read_flow(ctx.get_region_id(), &stats);
                                }
                                Err(e) => {
                                    consumer.consume(id, Err(e));
                                }
                            }
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e));
                        }
                    }
                }

                SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(begin_instant.elapsed_secs());
                SCHED_HISTOGRAM_VEC_STATIC
                    .get(CMD)
                    .observe(command_duration.elapsed_secs());
                Ok(())
            },
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get the values of some raw keys in a batch.
    pub fn raw_batch_get(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::raw_batch_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let enable_ttl = self.enable_ttl;

        let res = self.read_pool.spawn_handle(
            async move {
                let mut key_ranges = vec![];
                for key in &keys {
                    key_ranges.push(build_key_range(key, key, false));
                }
                tls_collect_query_batch(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key_ranges,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let store = RawStore::new(snapshot, enable_ttl);
                {
                    let begin_instant = Instant::now_coarse();
                    let keys: Vec<Key> = keys.into_iter().map(Key::from_encoded).collect();
                    let cf = Self::rawkv_cf(&cf)?;
                    // no scan_count for this kind of op.
                    let mut stats = Statistics::default();
                    let result: Vec<Result<KvPair>> = keys
                        .into_iter()
                        .map(|k| {
                            let v = store.raw_get_key_value(cf, &k, &mut stats);
                            (k, v)
                        })
                        .filter(|&(_, ref v)| !(v.is_ok() && v.as_ref().unwrap().is_none()))
                        .map(|(k, v)| match v {
                            Ok(v) => Ok((k.into_encoded(), v.unwrap())),
                            Err(v) => Err(v),
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

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Write a raw key to the storage.
    pub fn raw_put(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        check_key_size!(Some(&key).into_iter(), self.max_key_size, callback);
        let mut m = Modify::Put(Self::rawkv_cf(&cf)?, Key::from_encoded(key), value);
        if self.enable_ttl {
            let expire_ts = convert_to_expire_ts(ttl);
            m.with_ttl(expire_ts);
        } else if ttl != 0 {
            return Err(Error::from(ErrorInner::TTLNotEnabled));
        }

        self.engine.async_write(
            &ctx,
            WriteData::from_modifies(vec![m]),
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
        ttl: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cf = Self::rawkv_cf(&cf)?;

        check_key_size!(
            pairs.iter().map(|(ref k, _)| k),
            self.max_key_size,
            callback
        );

        if !self.enable_ttl && ttl != 0 {
            return Err(Error::from(ErrorInner::TTLNotEnabled));
        }
        let expire_ts = convert_to_expire_ts(ttl);

        let modifies = pairs
            .into_iter()
            .map(|(k, v)| Modify::Put(cf, Key::from_encoded(k), v))
            .map(|mut m| {
                if self.enable_ttl {
                    m.with_ttl(expire_ts)
                }
                m
            })
            .collect();
        self.engine.async_write(
            &ctx,
            WriteData::from_modifies(modifies),
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
            WriteData::from_modifies(vec![Modify::Delete(
                Self::rawkv_cf(&cf)?,
                Key::from_encoded(key),
            )]),
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
            WriteData::from_modifies(vec![Modify::DeleteRange(cf, start_key, end_key, false)]),
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

        let modifies = keys
            .into_iter()
            .map(|k| Modify::Delete(cf, Key::from_encoded(k)))
            .collect();
        self.engine.async_write(
            &ctx,
            WriteData::from_modifies(modifies),
            Box::new(|(_, res): (_, kv::Result<_>)| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.raw_batch_delete.inc();
        Ok(())
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
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::raw_scan;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let enable_ttl = self.enable_ttl;

        let res = self.read_pool.spawn_handle(
            async move {
                {
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        &start_key,
                        end_key.as_ref().unwrap_or(&vec![]),
                        reverse_scan,
                        QueryKind::Scan,
                    );
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let cf = Self::rawkv_cf(&cf)?;
                {
                    let store = RawStore::new(snapshot, enable_ttl);
                    let begin_instant = Instant::now_coarse();

                    let start_key = Key::from_encoded(start_key);
                    let end_key = end_key.map(Key::from_encoded);

                    let mut statistics = Statistics::default();
                    let result = if reverse_scan {
                        store
                            .reverse_raw_scan(
                                cf,
                                &start_key,
                                end_key.as_ref(),
                                limit,
                                &mut statistics,
                                key_only,
                            )
                            .await
                    } else {
                        store
                            .forward_raw_scan(
                                cf,
                                &start_key,
                                end_key.as_ref(),
                                limit,
                                &mut statistics,
                                key_only,
                            )
                            .await
                    }
                    .map_err(Error::from);

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

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
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
    ) -> impl Future<Output = Result<Vec<Result<KvPair>>>> {
        const CMD: CommandKind = CommandKind::raw_batch_scan;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let enable_ttl = self.enable_ttl;

        let res = self.read_pool.spawn_handle(
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();
                let command_duration = tikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let cf = Self::rawkv_cf(&cf)?;
                {
                    let store = RawStore::new(snapshot, enable_ttl);
                    let begin_instant = Instant::now();
                    let mut statistics = Statistics::default();
                    if !Self::check_key_ranges(&ranges, reverse_scan) {
                        return Err(box_err!("Invalid KeyRanges"));
                    };
                    let mut result = Vec::new();
                    let mut key_ranges = vec![];
                    for range in &ranges {
                        key_ranges.push(build_key_range(
                            &range.start_key,
                            &range.end_key,
                            reverse_scan,
                        ));
                    }
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
                        let pairs: Vec<Result<KvPair>> = if reverse_scan {
                            store
                                .reverse_raw_scan(
                                    &cf,
                                    &start_key,
                                    end_key.as_ref(),
                                    each_limit,
                                    &mut statistics,
                                    key_only,
                                )
                                .await
                        } else {
                            store
                                .forward_raw_scan(
                                    &cf,
                                    &start_key,
                                    end_key.as_ref(),
                                    each_limit,
                                    &mut statistics,
                                    key_only,
                                )
                                .await
                        }?;
                        result.extend(pairs.into_iter());
                    }
                    tls_collect_query_batch(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        key_ranges,
                        QueryKind::Scan,
                    );
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

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get the value of a raw key.
    pub fn raw_get_key_ttl(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Option<u64>>> {
        const CMD: CommandKind = CommandKind::raw_get_key_ttl;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let enable_ttl = self.enable_ttl;

        let res = self.read_pool.spawn_handle(
            async move {
                tls_collect_query(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    &key,
                    &key,
                    false,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                let command_duration = tikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;
                let store = RawStore::new(snapshot, enable_ttl);
                let cf = Self::rawkv_cf(&cf)?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut stats = Statistics::default();
                    let r = store.raw_get_key_ttl(cf, &Key::from_encoded(key), &mut stats);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                    tls_collect_read_flow(ctx.get_region_id(), &stats);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.elapsed_secs());
                    r
                }
            },
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    pub fn raw_compare_and_swap_atomic(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        previous_value: Option<Vec<u8>>,
        value: Vec<u8>,
        ttl: u64,
        cb: Callback<(Option<Value>, bool)>,
    ) -> Result<()> {
        let cf = Self::rawkv_cf(&cf)?;
        let ttl = if self.enable_ttl {
            Some(ttl)
        } else {
            if ttl != 0 {
                return Err(Error::from(ErrorInner::TTLNotEnabled));
            }
            None
        };
        let cmd =
            RawCompareAndSwap::new(cf, Key::from_encoded(key), previous_value, value, ttl, ctx);
        self.sched_txn_command(cmd, cb)
    }

    pub fn raw_batch_put_atomic(
        &self,
        ctx: Context,
        cf: String,
        pairs: Vec<KvPair>,
        ttl: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cf = Self::rawkv_cf(&cf)?;
        let muations = pairs
            .into_iter()
            .map(|(k, v)| Mutation::Put((Key::from_encoded(k), v)))
            .collect();
        let ttl = if self.enable_ttl {
            Some(ttl)
        } else {
            if ttl != 0 {
                return Err(Error::from(ErrorInner::TTLNotEnabled));
            }
            None
        };
        let cmd = RawAtomicStore::new(cf, muations, ttl, ctx);
        self.sched_txn_command(cmd, callback)
    }

    pub fn raw_batch_delete_atomic(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
        callback: Callback<()>,
    ) -> Result<()> {
        let cf = Self::rawkv_cf(&cf)?;
        let muations = keys
            .into_iter()
            .map(|k| Mutation::Delete(Key::from_encoded(k)))
            .collect();
        let cmd = RawAtomicStore::new(cf, muations, None, ctx);
        self.sched_txn_command(cmd, callback)
    }
}

fn get_priority_tag(priority: CommandPri) -> CommandPriority {
    match priority {
        CommandPri::Low => CommandPriority::low,
        CommandPri::Normal => CommandPriority::normal,
        CommandPri::High => CommandPriority::high,
    }
}

fn prepare_snap_ctx<'a>(
    pb_ctx: &'a Context,
    keys: impl IntoIterator<Item = &'a Key> + Clone,
    start_ts: TimeStamp,
    bypass_locks: &'a TsSet,
    concurrency_manager: &ConcurrencyManager,
    cmd: CommandKind,
) -> Result<SnapContext<'a>> {
    // Update max_ts and check the in-memory lock table before getting the snapshot
    if !pb_ctx.get_stale_read() {
        concurrency_manager.update_max_ts(start_ts);
    }
    fail_point!("before-storage-check-memory-locks");
    let isolation_level = pb_ctx.get_isolation_level();
    if isolation_level == IsolationLevel::Si {
        let begin_instant = Instant::now();
        for key in keys.clone() {
            concurrency_manager
                .read_key_check(&key, |lock| {
                    Lock::check_ts_conflict(Cow::Borrowed(lock), &key, start_ts, bypass_locks)
                })
                .map_err(|e| {
                    CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                        .get(cmd)
                        .locked
                        .observe(begin_instant.elapsed().as_secs_f64());
                    txn::Error::from_mvcc(e)
                })?;
        }
        CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
            .get(cmd)
            .unlocked
            .observe(begin_instant.elapsed().as_secs_f64());
    }

    let mut snap_ctx = SnapContext {
        pb_ctx,
        start_ts,
        ..Default::default()
    };
    if need_check_locks_in_replica_read(pb_ctx) {
        snap_ctx.key_ranges = keys
            .into_iter()
            .map(|k| point_key_range(k.clone()))
            .collect();
    }
    Ok(snap_ctx)
}

pub fn need_check_locks_in_replica_read(ctx: &Context) -> bool {
    ctx.get_replica_read() && ctx.get_isolation_level() == IsolationLevel::Si
}

pub fn point_key_range(key: Key) -> KeyRange {
    let mut end_key = key.as_encoded().to_vec();
    end_key.push(0);
    let end_key = Key::from_encoded(end_key);
    let mut key_range = KeyRange::default();
    key_range.set_start_key(key.into_encoded());
    key_range.set_end_key(end_key.into_encoded());
    key_range
}

/// A builder to build a temporary `Storage<E>`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestStorageBuilder<E: Engine, L: LockManager> {
    engine: E,
    config: Config,
    pipelined_pessimistic_lock: Arc<atomic::AtomicBool>,
    lock_mgr: L,
}

impl TestStorageBuilder<RocksEngine, DummyLockManager> {
    /// Build `Storage<RocksEngine>`.
    pub fn new(lock_mgr: DummyLockManager, enable_ttl: bool) -> Self {
        let config = Config {
            enable_ttl,
            ..Default::default()
        };
        Self {
            engine: TestEngineBuilder::new().ttl(enable_ttl).build().unwrap(),
            config,
            pipelined_pessimistic_lock: Arc::new(atomic::AtomicBool::new(false)),
            lock_mgr,
        }
    }
}

impl<E: Engine, L: LockManager> TestStorageBuilder<E, L> {
    pub fn from_engine_and_lock_mgr(engine: E, lock_mgr: L) -> Self {
        let config = Config::default();
        Self {
            engine,
            config,
            pipelined_pessimistic_lock: Arc::new(atomic::AtomicBool::new(false)),
            lock_mgr,
        }
    }

    /// Customize the config of the `Storage`.
    ///
    /// By default, `Config::default()` will be used.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn set_pipelined_pessimistic_lock(self, enabled: bool) -> Self {
        self.pipelined_pessimistic_lock
            .store(enabled, atomic::Ordering::Relaxed);
        self
    }

    pub fn set_async_apply_prewrite(mut self, enabled: bool) -> Self {
        self.config.enable_async_apply_prewrite = enabled;
        self
    }

    /// Build a `Storage<E>`.
    pub fn build(self) -> Result<Storage<E, L>> {
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            self.engine.clone(),
        );

        Storage::from_engine(
            self.engine,
            &self.config,
            ReadPool::from(read_pool).handle(),
            self.lock_mgr,
            ConcurrencyManager::new(1.into()),
            self.pipelined_pessimistic_lock,
        )
    }
}

pub trait ResponseBatchConsumer<ConsumeResponse: Sized>: Send {
    fn consume(&self, id: u64, res: Result<ConsumeResponse>);
}

pub mod test_util {
    use super::*;
    use crate::storage::txn::commands;
    use std::sync::Mutex;
    use std::{
        fmt::Debug,
        sync::mpsc::{channel, Sender},
    };

    pub fn expect_none(x: Option<Value>) {
        assert_eq!(x, None);
    }

    pub fn expect_value(v: Vec<u8>, x: Option<Value>) {
        assert_eq!(x.unwrap(), v);
    }

    pub fn expect_multi_values(v: Vec<Option<KvPair>>, x: Vec<Result<KvPair>>) {
        let x: Vec<Option<KvPair>> = x.into_iter().map(Result::ok).collect();
        assert_eq!(x, v);
    }

    pub fn expect_error<T, F>(err_matcher: F, x: Result<T>)
    where
        F: FnOnce(Error) + Send + 'static,
    {
        match x {
            Err(e) => err_matcher(e),
            _ => panic!("expect result to be an error"),
        }
    }

    pub fn expect_ok_callback<T: Debug>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            x.unwrap();
            done.send(id).unwrap();
        })
    }

    pub fn expect_fail_callback<T, F>(done: Sender<i32>, id: i32, err_matcher: F) -> Callback<T>
    where
        F: FnOnce(Error) + Send + 'static,
    {
        Box::new(move |x: Result<T>| {
            expect_error(err_matcher, x);
            done.send(id).unwrap();
        })
    }

    pub fn expect_too_busy_callback<T>(done: Sender<i32>, id: i32) -> Callback<T> {
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

    pub fn expect_value_callback<T: PartialEq + Debug + Send + 'static>(
        done: Sender<i32>,
        id: i32,
        value: T,
    ) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert_eq!(x.unwrap(), value);
            done.send(id).unwrap();
        })
    }

    pub fn expect_pessimistic_lock_res_callback(
        done: Sender<i32>,
        pessimistic_lock_res: PessimisticLockRes,
    ) -> Callback<Result<PessimisticLockRes>> {
        Box::new(move |res: Result<Result<PessimisticLockRes>>| {
            assert_eq!(res.unwrap().unwrap(), pessimistic_lock_res);
            done.send(0).unwrap();
        })
    }

    pub fn expect_secondary_locks_status_callback(
        done: Sender<i32>,
        secondary_locks_status: SecondaryLocksStatus,
    ) -> Callback<SecondaryLocksStatus> {
        Box::new(move |res: Result<SecondaryLocksStatus>| {
            assert_eq!(res.unwrap(), secondary_locks_status);
            done.send(0).unwrap();
        })
    }

    type PessimisticLockCommand = TypedCommand<Result<PessimisticLockRes>>;

    pub fn new_acquire_pessimistic_lock_command(
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
            OldValues::default(),
            Context::default(),
        )
    }

    pub fn delete_pessimistic_lock<E: Engine, L: LockManager>(
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

    pub struct GetResult {
        id: u64,
        res: Result<Option<Vec<u8>>>,
    }

    #[derive(Clone)]
    pub struct GetConsumer {
        pub data: Arc<Mutex<Vec<GetResult>>>,
    }

    impl GetConsumer {
        pub fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(vec![])),
            }
        }

        pub fn take_data(self) -> Vec<Result<Option<Vec<u8>>>> {
            let mut data = self.data.lock().unwrap();
            let mut results = std::mem::take(&mut *data);
            results.sort_by_key(|k| k.id);
            results.into_iter().map(|v| v.res).collect()
        }
    }

    impl ResponseBatchConsumer<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)> for GetConsumer {
        fn consume(
            &self,
            id: u64,
            res: Result<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>,
        ) {
            self.data.lock().unwrap().push(GetResult {
                id,
                res: res.map(|(v, ..)| v),
            });
        }
    }

    impl ResponseBatchConsumer<Option<Vec<u8>>> for GetConsumer {
        fn consume(&self, id: u64, res: Result<Option<Vec<u8>>>) {
            self.data.lock().unwrap().push(GetResult { id, res });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        mvcc::tests::{must_unlocked, must_written},
        test_util::*,
        *,
    };

    use crate::config::TitanDBConfig;
    use crate::storage::kv::{ExpectedWrite, MockEngineBuilder};
    use crate::storage::lock_manager::DiagnosticContext;
    use crate::storage::mvcc::LockType;
    use crate::storage::txn::commands::{AcquirePessimisticLock, Prewrite};
    use crate::storage::{
        config::BlockCacheConfig,
        kv::{Error as EngineError, ErrorInner as EngineErrorInner},
        lock_manager::{Lock, WaitTimeout},
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        raw::ttl::current_ts,
        txn::{commands, Error as TxnError, ErrorInner as TxnErrorInner},
    };
    use collections::HashMap;
    use engine_rocks::raw_util::CFOptions;
    use engine_traits::{ALL_CFS, CF_LOCK, CF_RAFT, CF_WRITE};
    use errors::extract_key_error;
    use futures::executor::block_on;
    use kvproto::kvrpcpb::{CommandPri, Op};
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc::{channel, Sender},
            Arc,
        },
        time::Duration,
    };
    use tikv_util::config::ReadableSize;
    use txn_types::{Mutation, WriteType};

    #[test]
    fn test_prewrite_blocks_read() {
        use kvproto::kvrpcpb::ExtraOp;
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();

        // We have to do the prewrite manually so that the mem locks don't get released.
        let snapshot = storage.engine.snapshot(Default::default()).unwrap();
        let mutations = vec![Mutation::Put((Key::from_raw(b"x"), b"z".to_vec()))];
        let mut cmd = commands::Prewrite::with_defaults(mutations, vec![1, 2, 3], 10.into());
        if let Command::Prewrite(p) = &mut cmd.cmd {
            p.secondary_keys = Some(vec![]);
        }
        let wr = cmd
            .cmd
            .process_write(
                snapshot,
                commands::WriteContext {
                    lock_mgr: &DummyLockManager {},
                    concurrency_manager: storage.concurrency_manager.clone(),
                    extra_op: ExtraOp::Noop,
                    statistics: &mut Statistics::default(),
                    async_apply_prewrite: false,
                },
            )
            .unwrap();
        assert_eq!(wr.lock_guards.len(), 1);

        let result = block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()));
        assert!(matches!(
            result,
            Err(Error(box ErrorInner::Txn(txn::Error(
                box txn::ErrorInner::Mvcc(mvcc::Error(box mvcc::ErrorInner::KeyIsLocked { .. }))
            ))))
        ));
    }

    #[test]
    fn test_get_put() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
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
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into())),
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
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cf_error() {
        // New engine lacks normal column families.
        let engine = TestEngineBuilder::new().cfs(["foo"]).build().unwrap();
        let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
            engine,
            DummyLockManager {},
        )
        .build()
        .unwrap();
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
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 1.into())),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Engine(EngineError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"x"),
                None,
                1000,
                0,
                1.into(),
                false,
                false,
            )),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::Engine(EngineError(box EngineErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            block_on(storage.batch_get(
                Context::default(),
                vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                1.into(),
            )),
        );
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![create_get_request(b"c", 1), create_get_request(b"d", 1)],
            vec![1, 2],
            consumer.clone(),
        ))
        .unwrap();
        let data = consumer.take_data();
        for v in data {
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
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
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
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with sample step
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                2,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with sample step
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                2,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with sample step and limit
        expect_multi_values(
            vec![Some((b"a".to_vec(), b"aa".to_vec()))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1,
                2,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with sample step and limit
        expect_multi_values(
            vec![Some((b"c".to_vec(), b"cc".to_vec()))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1,
                2,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );

        // Forward with limit
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_scan_with_key_only() {
        let db_config = crate::config::DbConfig {
            titan: TitanDBConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = {
            let path = "".to_owned();
            let cfs = ALL_CFS.to_vec();
            let cfg_rocksdb = db_config;
            let cache = BlockCacheConfig::default().build_shared_cache();
            let cfs_opts = vec![
                CFOptions::new(
                    CF_DEFAULT,
                    cfg_rocksdb.defaultcf.build_opt(&cache, None, false),
                ),
                CFOptions::new(CF_LOCK, cfg_rocksdb.lockcf.build_opt(&cache)),
                CFOptions::new(CF_WRITE, cfg_rocksdb.writecf.build_opt(&cache, None)),
                CFOptions::new(CF_RAFT, cfg_rocksdb.raftcf.build_opt(&cache)),
            ];
            RocksEngine::new(
                &path,
                &cfs,
                Some(cfs_opts),
                cache.is_some(),
                None, /*io_rate_limiter*/
            )
        }
        .unwrap();
        let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
            engine,
            DummyLockManager {},
        )
        .build()
        .unwrap();
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
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![None, None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![None, None],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
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
                Some((b"a".to_vec(), vec![])),
                Some((b"b".to_vec(), vec![])),
                Some((b"c".to_vec(), vec![])),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), vec![])),
                Some((b"b".to_vec(), vec![])),
                Some((b"a".to_vec(), vec![])),
            ],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![Some((b"a".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                Some(Key::from_raw(b"c")),
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with bound
        expect_multi_values(
            vec![Some((b"c".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                Some(Key::from_raw(b"b")),
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );

        // Forward with limit
        expect_multi_values(
            vec![Some((b"a".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\x00"),
                None,
                2,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // Backward with limit
        expect_multi_values(
            vec![Some((b"c".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            block_on(storage.scan(
                Context::default(),
                Key::from_raw(b"\xff"),
                None,
                2,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_batch_get() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
            block_on(storage.batch_get(
                Context::default(),
                vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                2.into(),
            ))
            .unwrap()
            .0,
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
            block_on(storage.batch_get(
                Context::default(),
                vec![
                    Key::from_raw(b"c"),
                    Key::from_raw(b"x"),
                    Key::from_raw(b"a"),
                    Key::from_raw(b"b"),
                ],
                5.into(),
            ))
            .unwrap()
            .0,
        );
    }

    fn create_get_request(key: &[u8], start_ts: u64) -> GetRequest {
        let mut req = GetRequest::default();
        req.set_key(key.to_owned());
        req.set_version(start_ts);
        req
    }

    #[test]
    fn test_batch_get_command() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![create_get_request(b"c", 2), create_get_request(b"d", 2)],
            vec![1, 2],
            consumer.clone(),
        ))
        .unwrap();
        let mut x = consumer.take_data();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::KeyIsLocked(..),
                ))))) => {}
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
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(
            vec![
                create_get_request(b"c", 5),
                create_get_request(b"x", 5),
                create_get_request(b"a", 5),
                create_get_request(b"b", 5),
            ],
            vec![1, 2, 3, 4],
            consumer.clone(),
        ))
        .unwrap();

        let x: Vec<Option<Vec<u8>>> = consumer
            .take_data()
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
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 120.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"101".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 120.into()))
                .unwrap()
                .0,
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
        let config = Config {
            scheduler_pending_write_threshold: ReadableSize(1),
            ..Default::default()
        };
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .config(config)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_txn_command::<()>(
                commands::Pause::new(vec![Key::from_raw(b"x")], 1000, Context::default()),
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
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
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
        assert_eq!(cm.max_ts(), 100.into());
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 105.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cleanup_check_ttl() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), ts(230, 0)))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_high_priority_get_put() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(
            block_on(storage.get(ctx, Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
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
        expect_none(
            block_on(storage.get(ctx, Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(ctx, Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_high_priority_no_block() {
        let config = Config {
            scheduler_worker_pool_size: 1,
            ..Default::default()
        };
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .config(config)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 100.into()))
                .unwrap()
                .0,
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
            block_on(storage.get(ctx, Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        // Command Get with high priority not block by command Pause.
        assert_eq!(rx.recv().unwrap(), 3);
    }

    #[test]
    fn test_delete_range() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
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
            block_on(storage.get(Context::default(), Key::from_raw(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        expect_none(
            block_on(storage.get(Context::default(), Key::from_raw(b"y"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
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
            block_on(storage.get(Context::default(), Key::from_raw(b"z"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_raw_delete_range() {
        test_raw_delete_range_impl(false)
    }

    #[test]
    fn test_raw_delete_range_ttl() {
        test_raw_delete_range_impl(true)
    }

    fn test_raw_delete_range_impl(ttl: bool) {
        let storage = TestStorageBuilder::new(DummyLockManager {}, ttl)
            .build()
            .unwrap();
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
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }

        expect_value(
            b"004".to_vec(),
            block_on(storage.raw_get(Context::default(), "".to_string(), b"d".to_vec())).unwrap(),
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
            block_on(storage.raw_get(Context::default(), "".to_string(), b"c".to_vec())).unwrap(),
        );
        expect_none(
            block_on(storage.raw_get(Context::default(), "".to_string(), b"d".to_vec())).unwrap(),
        );
        expect_value(
            b"005".to_vec(),
            block_on(storage.raw_get(Context::default(), "".to_string(), b"e".to_vec())).unwrap(),
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
            block_on(storage.raw_get(Context::default(), "".to_string(), b"a".to_vec())).unwrap(),
        );
        expect_value(
            b"002".to_vec(),
            block_on(storage.raw_get(Context::default(), "".to_string(), b"b".to_vec())).unwrap(),
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
                block_on(storage.raw_get(Context::default(), "".to_string(), kv.0.to_vec()))
                    .unwrap(),
            );
        }

        rx.recv().unwrap();
    }

    #[test]
    fn test_raw_batch_put() {
        test_raw_batch_put_impl(false)
    }

    #[test]
    fn test_raw_batch_put_ttl() {
        test_raw_batch_put_impl(true)
    }

    fn test_raw_batch_put_impl(ttl: bool) {
        let storage = TestStorageBuilder::new(DummyLockManager {}, ttl)
            .build()
            .unwrap();
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
                0,
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs one by one
        for (key, val) in test_data {
            expect_value(
                val,
                block_on(storage.raw_get(Context::default(), "".to_string(), key)).unwrap(),
            );
        }
    }

    #[test]
    fn test_raw_batch_get() {
        test_raw_batch_get_impl(false)
    }

    #[test]
    fn test_raw_batch_get_ttl() {
        test_raw_batch_get_impl(true)
    }

    fn test_raw_batch_get_impl(ttl: bool) {
        let storage = TestStorageBuilder::new(DummyLockManager {}, ttl)
            .build()
            .unwrap();
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
                    0,
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
            block_on(storage.raw_batch_get(Context::default(), "".to_string(), keys)).unwrap(),
        );
    }

    #[test]
    fn test_batch_raw_get() {
        test_batch_raw_get_impl(false)
    }

    #[test]
    fn test_batch_raw_get_ttl() {
        test_batch_raw_get_impl(true)
    }

    fn test_batch_raw_get_impl(ttl: bool) {
        let storage = TestStorageBuilder::new(DummyLockManager {}, ttl)
            .build()
            .unwrap();
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
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }
        rx.recv().unwrap();

        // Verify pairs in a batch
        let mut ids = vec![];
        let cmds = test_data
            .iter()
            .map(|&(ref k, _)| {
                let mut req = RawGetRequest::default();
                req.set_key(k.clone());
                ids.push(ids.len() as u64);
                req
            })
            .collect();
        let results: Vec<Option<Vec<u8>>> = test_data.into_iter().map(|(_, v)| Some(v)).collect();
        let consumer = GetConsumer::new();
        block_on(storage.raw_batch_get_command(cmds, ids, consumer.clone())).unwrap();
        let x: Vec<Option<Vec<u8>>> = consumer
            .take_data()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(x, results);
    }

    #[test]
    fn test_raw_batch_delete() {
        test_raw_batch_delete_impl(false)
    }

    #[test]
    fn test_raw_batch_delete_ttl() {
        test_raw_batch_delete_impl(true)
    }

    fn test_raw_batch_delete_impl(ttl: bool) {
        let storage = TestStorageBuilder::new(DummyLockManager {}, ttl)
            .build()
            .unwrap();
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
                0,
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
            block_on(storage.raw_batch_get(Context::default(), "".to_string(), keys)).unwrap(),
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
            block_on(storage.raw_get(Context::default(), "".to_string(), b"a".to_vec())).unwrap(),
        );
        expect_none(
            block_on(storage.raw_get(Context::default(), "".to_string(), b"b".to_vec())).unwrap(),
        );
        expect_value(
            b"cc".to_vec(),
            block_on(storage.raw_get(Context::default(), "".to_string(), b"c".to_vec())).unwrap(),
        );
        expect_none(
            block_on(storage.raw_get(Context::default(), "".to_string(), b"d".to_vec())).unwrap(),
        );
        expect_value(
            b"ee".to_vec(),
            block_on(storage.raw_get(Context::default(), "".to_string(), b"e".to_vec())).unwrap(),
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
            expect_none(block_on(storage.raw_get(Context::default(), "".to_string(), k)).unwrap());
        }
    }

    #[test]
    fn test_raw_scan() {
        test_raw_scan_impl(false)
    }

    #[test]
    fn test_raw_scan_ttl() {
        test_raw_scan_impl(true)
    }

    fn test_raw_scan_impl(ttl: bool) {
        let storage = TestStorageBuilder::new(DummyLockManager {}, ttl)
            .build()
            .unwrap();
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
                0,
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
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                vec![],
                None,
                20,
                true,
                false,
            ))
            .unwrap(),
        );
        results = results.split_off(10);
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                b"c2".to_vec(),
                None,
                20,
                true,
                false,
            ))
            .unwrap(),
        );
        let mut results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results.clone(),
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                vec![],
                None,
                20,
                false,
                false,
            ))
            .unwrap(),
        );
        results = results.split_off(10);
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                b"c2".to_vec(),
                None,
                20,
                false,
                false,
            ))
            .unwrap(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .rev()
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                b"z".to_vec(),
                None,
                20,
                false,
                true,
            ))
            .unwrap(),
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
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                b"z".to_vec(),
                None,
                5,
                false,
                true,
            ))
            .unwrap(),
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
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                b"b2".to_vec(),
                Some(b"c2".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
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
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                b"b2".to_vec(),
                Some(b"b2\x00".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
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
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                b"c2".to_vec(),
                Some(b"b2".to_vec()),
                20,
                false,
                true,
            ))
            .unwrap(),
        );
        let results: Vec<Option<KvPair>> = test_data
            .into_iter()
            .skip(6)
            .take(1)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            block_on(storage.raw_scan(
                Context::default(),
                "".to_string(),
                b"b2\x00".to_vec(),
                Some(b"b2".to_vec()),
                20,
                false,
                true,
            ))
            .unwrap(),
        );

        // End key tests. Confirm that lower/upper bound works correctly.
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
        expect_multi_values(
            results.clone().collect(),
            block_on(async {
                storage
                    .raw_scan(
                        Context::default(),
                        "".to_string(),
                        b"c1".to_vec(),
                        Some(b"d3".to_vec()),
                        20,
                        false,
                        false,
                    )
                    .await
            })
            .unwrap(),
        );
        expect_multi_values(
            results.rev().collect(),
            block_on(async {
                storage
                    .raw_scan(
                        Context::default(),
                        "".to_string(),
                        b"d3".to_vec(),
                        Some(b"c1".to_vec()),
                        20,
                        false,
                        true,
                    )
                    .await
            })
            .unwrap(),
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
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, false,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"c".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, false,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, false,),
            false
        );

        // if end_key is omitted, the next start_key is used instead. so, false is returned.
        let ranges = make_ranges(vec![
            (b"c".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"a".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, false,),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, true,),
            true
        );

        let ranges = make_ranges(vec![
            (b"c3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"a3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, true,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, true,),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"c3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine, DummyLockManager>>::check_key_ranges(&ranges, true,),
            false
        );
    }

    #[test]
    fn test_raw_batch_scan() {
        test_raw_batch_scan_impl(false)
    }

    #[test]
    fn test_raw_batch_scan_ttl() {
        test_raw_batch_scan_impl(true)
    }

    fn test_raw_batch_scan_impl(ttl: bool) {
        let storage = TestStorageBuilder::new(DummyLockManager {}, ttl)
            .build()
            .unwrap();
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
                0,
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs exist
        let keys = test_data.iter().map(|&(ref k, _)| k.clone()).collect();
        let results = test_data.into_iter().map(|(k, v)| Some((k, v))).collect();
        expect_multi_values(
            results,
            block_on(storage.raw_batch_get(Context::default(), "".to_string(), keys)).unwrap(),
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
            block_on(storage.raw_batch_scan(
                Context::default(),
                "".to_string(),
                ranges.clone(),
                5,
                false,
                false,
            ))
            .unwrap(),
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
            block_on(storage.raw_batch_scan(
                Context::default(),
                "".to_string(),
                ranges.clone(),
                5,
                true,
                false,
            ))
            .unwrap(),
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
            block_on(storage.raw_batch_scan(
                Context::default(),
                "".to_string(),
                ranges.clone(),
                3,
                false,
                false,
            ))
            .unwrap(),
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
            block_on(storage.raw_batch_scan(
                Context::default(),
                "".to_string(),
                ranges,
                3,
                true,
                false,
            ))
            .unwrap(),
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
            block_on(storage.raw_batch_scan(
                Context::default(),
                "".to_string(),
                ranges,
                5,
                false,
                true,
            ))
            .unwrap(),
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
            block_on(storage.raw_batch_scan(
                Context::default(),
                "".to_string(),
                ranges,
                2,
                false,
                true,
            ))
            .unwrap(),
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
            block_on(storage.raw_batch_scan(
                Context::default(),
                "".to_string(),
                ranges,
                5,
                true,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_raw_get_key_ttl() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, true)
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let test_data = vec![
            (b"a".to_vec(), b"aa".to_vec(), 10),
            (b"b".to_vec(), b"bb".to_vec(), 20),
            (b"c".to_vec(), b"cc".to_vec(), 0),
            (b"d".to_vec(), b"dd".to_vec(), 10),
            (b"e".to_vec(), b"ee".to_vec(), 20),
            (b"f".to_vec(), b"ff".to_vec(), u64::MAX),
        ];

        // Write key-value pairs one by one
        for &(ref key, ref value, ttl) in &test_data {
            storage
                .raw_put(
                    Context::default(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    ttl,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }
        rx.recv().unwrap();

        for &(ref key, _, ttl) in &test_data {
            let res =
                block_on(storage.raw_get_key_ttl(Context::default(), "".to_string(), key.clone()))
                    .unwrap();
            if ttl != 0 {
                if ttl > u64::MAX - current_ts() {
                    assert_eq!(res, Some(u64::MAX - current_ts()));
                } else {
                    assert_eq!(res, Some(ttl));
                }
            } else {
                assert_eq!(res, Some(0));
            }
        }
    }

    #[test]
    fn test_scan_lock() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
                    TimeStamp::default(),
                    None,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
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

        let cm = storage.concurrency_manager.clone();

        let res =
            block_on(storage.scan_lock(Context::default(), 99.into(), None, None, 10)).unwrap();
        assert_eq!(res, vec![]);
        assert_eq!(cm.max_ts(), 99.into());

        let res =
            block_on(storage.scan_lock(Context::default(), 100.into(), None, None, 10)).unwrap();
        assert_eq!(res, vec![lock_x.clone(), lock_y.clone(), lock_z.clone()]);
        assert_eq!(cm.max_ts(), 100.into());

        let res = block_on(storage.scan_lock(
            Context::default(),
            100.into(),
            Some(Key::from_raw(b"a")),
            None,
            10,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_x.clone(), lock_y.clone(), lock_z.clone()]);

        let res = block_on(storage.scan_lock(
            Context::default(),
            100.into(),
            Some(Key::from_raw(b"y")),
            None,
            10,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_y.clone(), lock_z.clone()]);

        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 10)).unwrap();
        assert_eq!(
            res,
            vec![
                lock_a.clone(),
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
                lock_z.clone(),
            ]
        );
        assert_eq!(cm.max_ts(), 101.into());

        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 4)).unwrap();
        assert_eq!(
            res,
            vec![lock_a, lock_b.clone(), lock_c.clone(), lock_x.clone()]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            None,
            4,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            None,
            0,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone(),
                lock_z
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"c")),
            0,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b.clone()]);

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            4,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone()
            ]
        );

        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            3,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b.clone(), lock_c.clone(), lock_x.clone()]);

        let mem_lock = |k: &[u8], ts: u64, lock_type| {
            let key = Key::from_raw(k);
            let guard = block_on(cm.lock_key(&key));
            guard.with_lock(|lock| {
                *lock = Some(txn_types::Lock::new(
                    lock_type,
                    k.to_vec(),
                    ts.into(),
                    100,
                    None,
                    0.into(),
                    1,
                    20.into(),
                ));
            });
            guard
        };

        let guard = mem_lock(b"z", 80, LockType::Put);
        block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 1)).unwrap_err();

        let guard2 = mem_lock(b"a", 80, LockType::Put);
        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            0,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                lock_b.clone(),
                lock_c.clone(),
                lock_x.clone(),
                lock_y.clone()
            ]
        );
        drop(guard);
        drop(guard2);

        // LockType::Lock can't be ignored by scan_lock
        let guard = mem_lock(b"c", 80, LockType::Lock);
        block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            1,
        ))
        .unwrap_err();
        drop(guard);

        let guard = mem_lock(b"c", 102, LockType::Put);
        let res = block_on(storage.scan_lock(
            Context::default(),
            101.into(),
            Some(Key::from_raw(b"b")),
            Some(Key::from_raw(b"z")),
            0,
        ))
        .unwrap();
        assert_eq!(res, vec![lock_b, lock_c, lock_x, lock_y]);
        drop(guard);
    }

    #[test]
    fn test_resolve_lock() {
        use crate::storage::txn::RESOLVE_LOCK_BATCH_SIZE;

        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
                        commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                // All locks should be resolved except for a, b and c.
                let res =
                    block_on(storage.scan_lock(Context::default(), ts, None, None, 0)).unwrap();
                assert_eq!(res, vec![lock_a.clone(), lock_b.clone(), lock_c.clone()]);

                ts = (ts.into_inner() + 10).into();
            }
        }
    }

    #[test]
    fn test_resolve_lock_lite() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
        let res =
            block_on(storage.scan_lock(Context::default(), 99.into(), None, None, 0)).unwrap();
        assert_eq!(res, vec![lock_a]);

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
                expect_ok_callback(tx, 0),
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
        let res =
            block_on(storage.scan_lock(Context::default(), 101.into(), None, None, 0)).unwrap();
        assert_eq!(res, vec![lock_a]);
    }

    #[test]
    fn test_txn_heart_beat() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
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
                        box mvcc::ErrorInner::TxnNotFound { .. },
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
                    b"k".to_vec(),
                    10.into(),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let lock_with_ttl = |ttl| {
            txn_types::Lock::new(
                LockType::Put,
                b"k".to_vec(),
                10.into(),
                ttl,
                Some(v.clone()),
                0.into(),
                0,
                0.into(),
            )
        };

        // `advise_ttl` = 90, which is less than current ttl 100. The lock's ttl will remains 100.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 90, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(lock_with_ttl(100), false)),
            )
            .unwrap();
        rx.recv().unwrap();

        // `advise_ttl` = 110, which is greater than current ttl. The lock's ttl will be updated to
        // 110.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 110, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(lock_with_ttl(110), false)),
            )
            .unwrap();
        rx.recv().unwrap();

        // Lock not match. Nothing happens except throwing an error.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k, 11.into(), 150, Context::default()),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                        box mvcc::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_check_txn_status() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
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
                    false,
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

        assert_eq!(cm.max_ts(), ts(9, 1));

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
                    false,
                    false,
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
                commands::Prewrite::new(
                    vec![Mutation::Put((k.clone(), v.clone()))],
                    b"k".to_vec(),
                    ts(10, 0),
                    100,
                    false,
                    3,
                    ts(10, 1),
                    TimeStamp::default(),
                    Some(vec![b"k1".to_vec(), b"k2".to_vec()]),
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If lock exists and not expired, returns the lock's information.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    0.into(),
                    0.into(),
                    true,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    uncommitted(
                        txn_types::Lock::new(
                            LockType::Put,
                            b"k".to_vec(),
                            ts(10, 0),
                            100,
                            Some(v.clone()),
                            0.into(),
                            3,
                            ts(10, 1),
                        )
                        .use_async_commit(vec![b"k1".to_vec(), b"k2".to_vec()]),
                        false,
                    ),
                ),
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
                    false,
                    false,
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
                    false,
                    false,
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
    fn test_check_secondary_locks() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();

        let k1 = Key::from_raw(b"k1");
        let k2 = Key::from_raw(b"k2");

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::Lock(k1.clone()), Mutation::Lock(k2.clone())],
                    b"k".to_vec(),
                    10.into(),
                    100,
                    false,
                    2,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // All locks exist

        let mut lock1 = LockInfo::default();
        lock1.set_primary_lock(b"k".to_vec());
        lock1.set_lock_version(10);
        lock1.set_key(b"k1".to_vec());
        lock1.set_txn_size(2);
        lock1.set_lock_ttl(100);
        lock1.set_lock_type(Op::Lock);
        let mut lock2 = lock1.clone();
        lock2.set_key(b"k2".to_vec());

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(
                    vec![k1.clone(), k2.clone()],
                    10.into(),
                    Context::default(),
                ),
                expect_secondary_locks_status_callback(
                    tx.clone(),
                    SecondaryLocksStatus::Locked(vec![lock1, lock2]),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        // One of the locks are committed

        storage
            .sched_txn_command(
                commands::Commit::new(vec![k1.clone()], 10.into(), 20.into(), Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![k1, k2], 10.into(), Context::default()),
                expect_secondary_locks_status_callback(
                    tx.clone(),
                    SecondaryLocksStatus::Committed(20.into()),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(cm.max_ts(), 10.into());

        // Some of the locks do not exist
        let k3 = Key::from_raw(b"k3");
        let k4 = Key::from_raw(b"k4");

        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::Lock(k3.clone())],
                    b"k".to_vec(),
                    30.into(),
                    100,
                    false,
                    2,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![k3, k4], 10.into(), Context::default()),
                expect_secondary_locks_status_callback(tx, SecondaryLocksStatus::RolledBack),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn test_pessimistic_lock_impl(pipelined_pessimistic_lock: bool) {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .set_pipelined_pessimistic_lock(pipelined_pessimistic_lock)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
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

            if return_values {
                assert_eq!(cm.max_ts(), 10.into());
            }

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
            rx.recv_timeout(Duration::from_millis(100)).unwrap_err();
        }

        // Needn't update max_ts when failing to read value
        assert_eq!(cm.max_ts(), 10.into());

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
                    TimeStamp::default(),
                    None,
                    false,
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

        // Needn't update max_ts when failing to read value
        assert_eq!(cm.max_ts(), 10.into());

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

            if return_values {
                assert_eq!(cm.max_ts(), 30.into());
            }

            delete_pessimistic_lock(&storage, key.clone(), 30, 30);
        }
    }

    #[test]
    fn test_pessimistic_lock() {
        test_pessimistic_lock_impl(false);
        test_pessimistic_lock_impl(true);
    }

    pub enum Msg {
        WaitFor {
            start_ts: TimeStamp,
            cb: StorageCallback,
            pr: ProcessResult,
            lock: Lock,
            is_first_lock: bool,
            timeout: Option<WaitTimeout>,
            diag_ctx: DiagnosticContext,
        },

        WakeUp {
            lock_ts: TimeStamp,
            hashes: Vec<u64>,
            commit_ts: TimeStamp,
            is_pessimistic_txn: bool,
        },
    }

    // `ProxyLockMgr` sends all msgs it received to `Sender`.
    // It's used to check whether we send right messages to lock manager.
    #[derive(Clone)]
    pub struct ProxyLockMgr {
        tx: Sender<Msg>,
        has_waiter: Arc<AtomicBool>,
    }

    impl ProxyLockMgr {
        pub fn new(tx: Sender<Msg>) -> Self {
            Self {
                tx,
                has_waiter: Arc::new(AtomicBool::new(false)),
            }
        }

        pub fn set_has_waiter(&mut self, has_waiter: bool) {
            self.has_waiter.store(has_waiter, Ordering::Relaxed);
        }
    }

    impl LockManager for ProxyLockMgr {
        fn wait_for(
            &self,
            start_ts: TimeStamp,
            cb: StorageCallback,
            pr: ProcessResult,
            lock: Lock,
            is_first_lock: bool,
            timeout: Option<WaitTimeout>,
            diag_ctx: DiagnosticContext,
        ) {
            self.tx
                .send(Msg::WaitFor {
                    start_ts,
                    cb,
                    pr,
                    lock,
                    is_first_lock,
                    timeout,
                    diag_ctx,
                })
                .unwrap();
        }

        fn wake_up(
            &self,
            lock_ts: TimeStamp,
            hashes: Vec<u64>,
            commit_ts: TimeStamp,
            is_pessimistic_txn: bool,
        ) {
            self.tx
                .send(Msg::WakeUp {
                    lock_ts,
                    hashes,
                    commit_ts,
                    is_pessimistic_txn,
                })
                .unwrap();
        }

        fn has_waiter(&self) -> bool {
            self.has_waiter.load(Ordering::Relaxed)
        }

        fn dump_wait_for_entries(&self, _cb: waiter_manager::Callback) {
            unimplemented!()
        }
    }

    // Test whether `Storage` sends right wait-for-lock msgs to `LockManager`.
    #[test]
    fn validate_wait_for_lock_msg() {
        let (msg_tx, msg_rx) = channel();
        let storage = TestStorageBuilder::from_engine_and_lock_mgr(
            TestEngineBuilder::new().build().unwrap(),
            ProxyLockMgr::new(msg_tx),
        )
        .build()
        .unwrap();

        let (k, v) = (b"k".to_vec(), b"v".to_vec());
        let (tx, rx) = channel();
        // Write lock-k.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::Put((Key::from_raw(&k), v))],
                    k.clone(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // No wait for msg
        assert!(msg_rx.try_recv().is_err());

        // Meet lock-k.
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(Key::from_raw(b"foo"), false), (Key::from_raw(&k), false)],
                    k.clone(),
                    20.into(),
                    3000,
                    true,
                    20.into(),
                    Some(WaitTimeout::Millis(100)),
                    false,
                    21.into(),
                    OldValues::default(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        // The transaction should be waiting for lock released so cb won't be called.
        rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

        let msg = msg_rx.try_recv().unwrap();
        // Check msg validation.
        match msg {
            Msg::WaitFor {
                start_ts,
                pr,
                lock,
                is_first_lock,
                timeout,
                ..
            } => {
                assert_eq!(start_ts, TimeStamp::new(20));
                assert_eq!(
                    lock,
                    Lock {
                        ts: 10.into(),
                        hash: Key::from_raw(&k).gen_hash(),
                    }
                );
                assert_eq!(is_first_lock, true);
                assert_eq!(timeout, Some(WaitTimeout::Millis(100)));
                match pr {
                    ProcessResult::PessimisticLockRes { res } => match res {
                        Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                            MvccError(box MvccErrorInner::KeyIsLocked(info)),
                        ))))) => {
                            assert_eq!(info.get_key(), k.as_slice());
                            assert_eq!(info.get_primary_lock(), k.as_slice());
                            assert_eq!(info.get_lock_version(), 10);
                        }
                        _ => panic!("unexpected error"),
                    },
                    _ => panic!("unexpected process result"),
                };
            }

            _ => panic!("unexpected msg"),
        }
    }

    // Test whether `Storage` sends right wake-up msgs to `LockManager`
    #[test]
    fn validate_wake_up_msg() {
        fn assert_wake_up_msg_eq(
            msg: Msg,
            expected_lock_ts: TimeStamp,
            expected_hashes: Vec<u64>,
            expected_commit_ts: TimeStamp,
            expected_is_pessimistic_txn: bool,
        ) {
            match msg {
                Msg::WakeUp {
                    lock_ts,
                    hashes,
                    commit_ts,
                    is_pessimistic_txn,
                } => {
                    assert_eq!(lock_ts, expected_lock_ts);
                    assert_eq!(hashes, expected_hashes);
                    assert_eq!(commit_ts, expected_commit_ts);
                    assert_eq!(is_pessimistic_txn, expected_is_pessimistic_txn);
                }
                _ => panic!("unexpected msg"),
            }
        }

        let (msg_tx, msg_rx) = channel();
        let mut lock_mgr = ProxyLockMgr::new(msg_tx);
        lock_mgr.set_has_waiter(true);
        let storage = TestStorageBuilder::from_engine_and_lock_mgr(
            TestEngineBuilder::new().build().unwrap(),
            lock_mgr,
        )
        .build()
        .unwrap();

        let (tx, rx) = channel();
        let prewrite_locks = |keys: &[Key], ts: TimeStamp| {
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(
                        keys.iter()
                            .map(|k| Mutation::Put((k.clone(), b"v".to_vec())))
                            .collect(),
                        keys[0].to_raw().unwrap(),
                        ts,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        };
        let acquire_pessimistic_locks = |keys: &[Key], ts: TimeStamp| {
            storage
                .sched_txn_command(
                    new_acquire_pessimistic_lock_command(
                        keys.iter().map(|k| (k.clone(), false)).collect(),
                        ts,
                        ts,
                        false,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        };

        let keys = vec![
            Key::from_raw(b"a"),
            Key::from_raw(b"b"),
            Key::from_raw(b"c"),
        ];
        let key_hashes: Vec<u64> = keys.iter().map(|k| k.gen_hash()).collect();

        // Commit
        prewrite_locks(&keys, 10.into());
        // If locks don't exsit, hashes of released locks should be empty.
        for empty_hashes in &[false, true] {
            storage
                .sched_txn_command(
                    commands::Commit::new(keys.clone(), 10.into(), 20.into(), Context::default()),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            let msg = msg_rx.recv().unwrap();
            let hashes = if *empty_hashes {
                Vec::new()
            } else {
                key_hashes.clone()
            };
            assert_wake_up_msg_eq(msg, 10.into(), hashes, 20.into(), false);
        }

        // Cleanup
        for pessimistic in &[false, true] {
            let mut ts = TimeStamp::new(30);
            if *pessimistic {
                ts.incr();
                acquire_pessimistic_locks(&keys[..1], ts);
            } else {
                prewrite_locks(&keys[..1], ts);
            }
            for empty_hashes in &[false, true] {
                storage
                    .sched_txn_command(
                        commands::Cleanup::new(
                            keys[0].clone(),
                            ts,
                            TimeStamp::max(),
                            Context::default(),
                        ),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let msg = msg_rx.recv().unwrap();
                let (hashes, pessimistic) = if *empty_hashes {
                    (Vec::new(), false)
                } else {
                    (key_hashes[..1].to_vec(), *pessimistic)
                };
                assert_wake_up_msg_eq(msg, ts, hashes, 0.into(), pessimistic);
            }
        }

        // Rollback
        for pessimistic in &[false, true] {
            let mut ts = TimeStamp::new(40);
            if *pessimistic {
                ts.incr();
                acquire_pessimistic_locks(&keys, ts);
            } else {
                prewrite_locks(&keys, ts);
            }
            for empty_hashes in &[false, true] {
                storage
                    .sched_txn_command(
                        commands::Rollback::new(keys.clone(), ts, Context::default()),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let msg = msg_rx.recv().unwrap();
                let (hashes, pessimistic) = if *empty_hashes {
                    (Vec::new(), false)
                } else {
                    (key_hashes.clone(), *pessimistic)
                };
                assert_wake_up_msg_eq(msg, ts, hashes, 0.into(), pessimistic);
            }
        }

        // PessimisticRollback
        acquire_pessimistic_locks(&keys, 50.into());
        for empty_hashes in &[false, true] {
            storage
                .sched_txn_command(
                    commands::PessimisticRollback::new(
                        keys.clone(),
                        50.into(),
                        50.into(),
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            let msg = msg_rx.recv().unwrap();
            let (hashes, pessimistic) = if *empty_hashes {
                (Vec::new(), false)
            } else {
                (key_hashes.clone(), true)
            };
            assert_wake_up_msg_eq(msg, 50.into(), hashes, 0.into(), pessimistic);
        }

        // ResolveLockLite
        for commit in &[false, true] {
            let mut start_ts = TimeStamp::new(60);
            let commit_ts = if *commit {
                start_ts.incr();
                start_ts.next()
            } else {
                TimeStamp::zero()
            };
            prewrite_locks(&keys, start_ts);
            for empty_hashes in &[false, true] {
                storage
                    .sched_txn_command(
                        commands::ResolveLockLite::new(
                            start_ts,
                            commit_ts,
                            keys.clone(),
                            Context::default(),
                        ),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let msg = msg_rx.recv().unwrap();
                let hashes = if *empty_hashes {
                    Vec::new()
                } else {
                    key_hashes.clone()
                };
                assert_wake_up_msg_eq(msg, start_ts, hashes, commit_ts, false);
            }
        }

        // ResolveLock
        let mut txn_status = HashMap::default();
        acquire_pessimistic_locks(&keys, 70.into());
        // Rollback start_ts=70
        txn_status.insert(TimeStamp::new(70), TimeStamp::zero());
        let committed_keys = vec![
            Key::from_raw(b"d"),
            Key::from_raw(b"e"),
            Key::from_raw(b"f"),
        ];
        let committed_key_hashes: Vec<u64> = committed_keys.iter().map(|k| k.gen_hash()).collect();
        // Commit start_ts=75
        prewrite_locks(&committed_keys, 75.into());
        txn_status.insert(TimeStamp::new(75), TimeStamp::new(76));
        storage
            .sched_txn_command(
                commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut msg1 = msg_rx.recv().unwrap();
        let mut msg2 = msg_rx.recv().unwrap();
        match msg1 {
            Msg::WakeUp { lock_ts, .. } => {
                if lock_ts != TimeStamp::new(70) {
                    // Let msg1 be the msg of rolled back transaction.
                    std::mem::swap(&mut msg1, &mut msg2);
                }
                assert_wake_up_msg_eq(msg1, 70.into(), key_hashes, 0.into(), true);
                assert_wake_up_msg_eq(msg2, 75.into(), committed_key_hashes, 76.into(), false);
            }
            _ => panic!("unexpect msg"),
        }

        // CheckTxnStatus
        let key = Key::from_raw(b"k");
        let start_ts = TimeStamp::compose(100, 0);
        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::Put((key.clone(), b"v".to_vec()))],
                    key.to_raw().unwrap(),
                    start_ts,
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Not expire
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key.clone(),
                    start_ts,
                    TimeStamp::compose(110, 0),
                    TimeStamp::compose(150, 0),
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    TxnStatus::uncommitted(
                        txn_types::Lock::new(
                            LockType::Put,
                            b"k".to_vec(),
                            start_ts,
                            100,
                            Some(b"v".to_vec()),
                            0.into(),
                            0,
                            0.into(),
                        ),
                        false,
                    ),
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        // No msg
        assert!(msg_rx.try_recv().is_err());

        // Expired
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key.clone(),
                    start_ts,
                    TimeStamp::compose(110, 0),
                    TimeStamp::compose(201, 0),
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, TxnStatus::TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_wake_up_msg_eq(
            msg_rx.recv().unwrap(),
            start_ts,
            vec![key.gen_hash()],
            0.into(),
            false,
        );
    }

    #[test]
    fn test_check_memory_locks() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let cm = storage.get_concurrency_manager();
        let key = Key::from_raw(b"key");
        let guard = block_on(cm.lock_key(&key));
        guard.with_lock(|lock| {
            *lock = Some(txn_types::Lock::new(
                LockType::Put,
                b"key".to_vec(),
                10.into(),
                100,
                Some(vec![]),
                0.into(),
                1,
                20.into(),
            ));
        });

        let mut ctx = Context::default();
        ctx.set_isolation_level(IsolationLevel::Si);

        // Test get
        let key_error = extract_key_error(
            &block_on(storage.get(ctx.clone(), key.clone(), 100.into())).unwrap_err(),
        );
        assert_eq!(key_error.get_locked().get_key(), b"key");

        // Test batch_get
        let key_error = extract_key_error(
            &block_on(storage.batch_get(ctx.clone(), vec![Key::from_raw(b"a"), key], 100.into()))
                .unwrap_err(),
        );
        assert_eq!(key_error.get_locked().get_key(), b"key");

        // Test scan
        let key_error = extract_key_error(
            &block_on(storage.scan(
                ctx.clone(),
                Key::from_raw(b"a"),
                None,
                10,
                0,
                100.into(),
                false,
                false,
            ))
            .unwrap_err(),
        );
        assert_eq!(key_error.get_locked().get_key(), b"key");

        // Test batch_get_command
        let mut req1 = GetRequest::default();
        req1.set_context(ctx.clone());
        req1.set_key(b"a".to_vec());
        req1.set_version(50);
        let mut req2 = GetRequest::default();
        req2.set_context(ctx);
        req2.set_key(b"key".to_vec());
        req2.set_version(100);
        let consumer = GetConsumer::new();
        block_on(storage.batch_get_command(vec![req1, req2], vec![1, 2], consumer.clone()))
            .unwrap();
        let res = consumer.take_data();
        assert!(res[0].is_ok());
        let key_error = extract_key_error(&res[1].as_ref().unwrap_err());
        assert_eq!(key_error.get_locked().get_key(), b"key");
    }

    #[test]
    fn test_async_commit_prewrite() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        cm.update_max_ts(10.into());

        // Optimistic prewrite
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::Put((Key::from_raw(b"a"), b"v".to_vec())),
                        Mutation::Put((Key::from_raw(b"b"), b"v".to_vec())),
                        Mutation::Put((Key::from_raw(b"c"), b"v".to_vec())),
                    ],
                    b"c".to_vec(),
                    100.into(),
                    1000,
                    false,
                    3,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    Some(vec![b"a".to_vec(), b"b".to_vec()]),
                    false,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 101.into());

        // Pessimistic prewrite
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(Key::from_raw(b"d"), false), (Key::from_raw(b"e"), false)],
                    200,
                    300,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        cm.update_max_ts(1000.into());

        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (Mutation::Put((Key::from_raw(b"d"), b"v".to_vec())), true),
                        (Mutation::Put((Key::from_raw(b"e"), b"v".to_vec())), true),
                    ],
                    b"d".to_vec(),
                    200.into(),
                    1000,
                    400.into(),
                    2,
                    401.into(),
                    TimeStamp::default(),
                    Some(vec![b"e".to_vec()]),
                    false,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 1001.into());
    }

    // This is one of the series of tests to test overlapped timestamps.
    // Overlapped ts means there is a rollback record and a commit record with the same ts.
    // In this test we check that if rollback happens before commit, then they should not have overlapped ts,
    // which is an expected property.
    #[test]
    fn test_overlapped_ts_rollback_before_prewrite() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
            engine.clone(),
            DummyLockManager {},
        )
        .build()
        .unwrap();

        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");
        let key1 = Key::from_raw(k1);
        let key2 = Key::from_raw(k2);
        let value1 = v1.to_vec();
        let value2 = v2.to_vec();

        let (tx, rx) = channel();

        // T1 acquires lock on k1, start_ts = 1, for_update_ts = 3
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(key1.clone(), false)],
                    k1.to_vec(),
                    1.into(),
                    0,
                    true,
                    3.into(),
                    None,
                    false,
                    0.into(),
                    OldValues::default(),
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T2 acquires lock on k2, start_ts = 10, for_update_ts = 15
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(key2.clone(), false)],
                    k2.to_vec(),
                    10.into(),
                    0,
                    true,
                    15.into(),
                    None,
                    false,
                    0.into(),
                    OldValues::default(),
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T2 pessimistically prewrites, start_ts = 10, lock ttl = 0
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(Mutation::Put((key2.clone(), value2.clone())), true)],
                    k2.to_vec(),
                    10.into(),
                    0,
                    15.into(),
                    1,
                    0.into(),
                    100.into(),
                    None,
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T3 checks T2, which rolls back key2 and pushes max_ts to 10
        // use a large timestamp to make the lock expire so key2 will be rolled back.
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    key2.clone(),
                    10.into(),
                    ((1 << 18) + 8).into(),
                    ((1 << 18) + 8).into(),
                    true,
                    false,
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        must_unlocked(&engine, k2);
        must_written(&engine, k2, 10, 10, WriteType::Rollback);

        // T1 prewrites, start_ts = 1, for_update_ts = 3
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (Mutation::Put((key1.clone(), value1)), true),
                        (Mutation::Put((key2.clone(), value2)), false),
                    ],
                    k1.to_vec(),
                    1.into(),
                    0,
                    3.into(),
                    2,
                    0.into(),
                    (1 << 19).into(),
                    Some(vec![k2.to_vec()]),
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T1.commit_ts must be pushed to be larger than T2.start_ts (if we resolve T1)
        storage
            .sched_txn_command(
                commands::CheckSecondaryLocks::new(vec![key1, key2], 1.into(), Default::default()),
                Box::new(move |res| {
                    let pr = res.unwrap();
                    match pr {
                        SecondaryLocksStatus::Locked(l) => {
                            let min_commit_ts = l
                                .iter()
                                .map(|lock_info| lock_info.min_commit_ts)
                                .max()
                                .unwrap();
                            tx.send(min_commit_ts as i32).unwrap();
                        }
                        _ => unreachable!(),
                    }
                }),
            )
            .unwrap();
        assert!(rx.recv().unwrap() > 10);
    }
    // this test shows that the scheduler take `response_policy` in `WriteResult` serious,
    // ie. call the callback at expected stage when writing to the engine
    #[test]
    fn test_scheduler_response_policy() {
        struct Case<T: 'static + StorageCallbackType + Send> {
            expected_writes: Vec<ExpectedWrite>,

            command: TypedCommand<T>,
            pipelined_pessimistic_lock: bool,
        }

        impl<T: 'static + StorageCallbackType + Send> Case<T> {
            fn run(self) {
                let mut builder =
                    MockEngineBuilder::from_rocks_engine(TestEngineBuilder::new().build().unwrap());
                for expected_write in self.expected_writes {
                    builder = builder.add_expected_write(expected_write)
                }
                let engine = builder.build();
                let mut builder =
                    TestStorageBuilder::from_engine_and_lock_mgr(engine, DummyLockManager {});
                builder.config.enable_async_apply_prewrite = true;
                if self.pipelined_pessimistic_lock {
                    builder
                        .pipelined_pessimistic_lock
                        .store(true, Ordering::Relaxed);
                }
                let storage = builder.build().unwrap();
                let (tx, rx) = channel();
                storage
                    .sched_txn_command(
                        self.command,
                        Box::new(move |res| {
                            tx.send(res).unwrap();
                        }),
                    )
                    .unwrap();
                rx.recv().unwrap().unwrap();
            }
        }

        let keys = [b"k1", b"k2"];
        let values = [b"v1", b"v2"];
        let mutations = vec![
            Mutation::Put((Key::from_raw(keys[0]), keys[0].to_vec())),
            Mutation::Put((Key::from_raw(keys[1]), values[1].to_vec())),
        ];

        let on_applied_case = Case {
            // this case's command return ResponsePolicy::OnApplied
            // tested by `test_response_stage` in command::prewrite
            expected_writes: vec![
                ExpectedWrite::new()
                    .expect_no_committed_cb()
                    .expect_no_proposed_cb(),
                ExpectedWrite::new()
                    .expect_no_committed_cb()
                    .expect_no_proposed_cb(),
            ],

            command: Prewrite::new(
                mutations.clone(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                None,
                false,
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };
        let on_commited_case = Case {
            // this case's command return ResponsePolicy::OnCommitted
            // tested by `test_response_stage` in command::prewrite
            expected_writes: vec![
                ExpectedWrite::new().expect_committed_cb(),
                ExpectedWrite::new().expect_committed_cb(),
            ],

            command: Prewrite::new(
                mutations,
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };
        let on_proposed_case = Case {
            // this case's command return ResponsePolicy::OnProposed
            // untested, but all AcquirePessimisticLock should return ResponsePolicy::OnProposed now
            // and the scheduler expected to take OnProposed serious when
            // enable pipelined pessimistic lock
            expected_writes: vec![
                ExpectedWrite::new().expect_proposed_cb(),
                ExpectedWrite::new().expect_proposed_cb(),
            ],

            command: AcquirePessimisticLock::new(
                keys.iter().map(|&it| (Key::from_raw(it), true)).collect(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                TimeStamp::new(11),
                None,
                false,
                TimeStamp::new(12),
                OldValues::default(),
                Context::default(),
            ),
            pipelined_pessimistic_lock: true,
        };
        let on_proposed_fallback_case = Case {
            // this case's command return ResponsePolicy::OnProposed
            // but when pipelined pessimistic lock is off,
            // the scheduler should fallback to use OnApplied
            expected_writes: vec![
                ExpectedWrite::new().expect_no_proposed_cb(),
                ExpectedWrite::new().expect_no_proposed_cb(),
            ],

            command: AcquirePessimisticLock::new(
                keys.iter().map(|&it| (Key::from_raw(it), true)).collect(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                TimeStamp::new(11),
                None,
                false,
                TimeStamp::new(12),
                OldValues::default(),
                Context::default(),
            ),
            pipelined_pessimistic_lock: false,
        };

        on_applied_case.run();
        on_commited_case.run();
        on_proposed_case.run();
        on_proposed_fallback_case.run();
    }
}
