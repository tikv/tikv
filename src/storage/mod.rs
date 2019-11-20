// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Interact with persistent storage.
//!
//! The [`Storage`](storage::Storage) structure provides raw and transactional APIs on top of
//! a lower-level [`Engine`](storage::kv::Engine).
//!
//! There are multiple [`Engine`](storage::kv::Engine) implementations, [`RaftKv`](server::raftkv::RaftKv)
//! is used by the [`Server`](server::Server). The [`BTreeEngine`](storage::kv::BTreeEngine) and
//! [`RocksEngine`](storage::RocksEngine) are used for testing only.

pub mod commands;
pub mod config;
pub mod errors;
pub mod kv;
pub mod lock_manager;
pub mod metrics;
pub mod mvcc;
pub mod readpool_impl;
pub mod txn;
pub mod types;

use std::cmp;
use std::sync::{atomic, Arc};

use engine::{
    CfName, IterOption, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS, DATA_KEY_PREFIX_LEN,
};
use futures::{future, Future};
use kvproto::kvrpcpb::{Context, KeyRange, LockInfo};
use tikv_util::collections::HashMap;
use tikv_util::future_pool::FuturePool;

use self::commands::{get_priority_tag, Command, CommandKind};
use self::kv::with_tls_engine;
use self::metrics::*;
use self::mvcc::{Lock, TsSet};
use self::txn::scheduler::Scheduler as TxnScheduler;

pub use self::commands::{Options, PointGetCommand};
pub use self::config::{BlockCacheConfig, Config, DEFAULT_DATA_DIR, DEFAULT_ROCKSDB_SUB_DIR};
pub use self::errors::{get_error_kind_from_header, get_tag_from_header, Error, ErrorHeaderKind};
pub use self::kv::{
    CFStatistics, Cursor, CursorBuilder, Engine, Error as EngineError, FlowStatistics,
    FlowStatsReporter, Iterator, Modify, RegionInfoProvider, RocksEngine, ScanMode, Snapshot,
    Statistics, StatisticsSummary, TestEngineBuilder,
};
pub use self::lock_manager::{DummyLockManager, LockManager};
pub use self::mvcc::Scanner as StoreScanner;
pub use self::readpool_impl::*;
pub use self::txn::{FixtureStore, FixtureStoreScanner};
pub use self::txn::{Msg, Scanner, Scheduler, SnapshotStore, Store};
pub use self::types::{Key, KvPair, Mutation, MvccInfo, StorageCb, TxnStatus, Value};

pub type Result<T> = std::result::Result<T, Error>;
pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;

// Short value max len must <= 255.
pub const SHORT_VALUE_MAX_LEN: usize = 255;
pub const SHORT_VALUE_PREFIX: u8 = b'v';
pub const FOR_UPDATE_TS_PREFIX: u8 = b'f';
pub const TXN_SIZE_PREFIX: u8 = b't';
pub const MIN_COMMIT_TS_PREFIX: u8 = b'c';

pub fn is_short_value(value: &[u8]) -> bool {
    value.len() <= SHORT_VALUE_MAX_LEN
}

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
/// [`async_unsafe_destroy_range`](Storage::async_unsafe_destroy_range) is the only exception. It's
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
    read_pool_low: FuturePool,
    read_pool_normal: FuturePool,
    read_pool_high: FuturePool,

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
            read_pool_low: self.read_pool_low.clone(),
            read_pool_normal: self.read_pool_normal.clone(),
            read_pool_high: self.read_pool_high.clone(),
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

impl<E: Engine, L: LockManager> Storage<E, L> {
    /// Get concurrency of normal readpool.
    pub fn readpool_normal_concurrency(&self) -> usize {
        self.read_pool_normal.get_pool_size()
    }

    /// Create a `Storage` from given engine.
    pub fn from_engine(
        engine: E,
        config: &Config,
        mut read_pool: Vec<FuturePool>,
        lock_mgr: Option<L>,
    ) -> Result<Self> {
        let pessimistic_txn_enabled = lock_mgr.is_some();
        let sched = TxnScheduler::new(
            engine.clone(),
            lock_mgr,
            config.scheduler_concurrency,
            config.scheduler_worker_pool_size,
            config.scheduler_pending_write_threshold.0 as usize,
        );

        let read_pool_high = read_pool.remove(2);
        let read_pool_normal = read_pool.remove(1);
        let read_pool_low = read_pool.remove(0);

        info!("Storage started.");

        Ok(Storage {
            engine,
            sched,
            read_pool_low,
            read_pool_normal,
            read_pool_high,
            refs: Arc::new(atomic::AtomicUsize::new(1)),
            max_key_size: config.max_key_size,
            pessimistic_txn_enabled,
        })
    }

    /// Get the underlying `Engine` of the `Storage`.
    pub fn get_engine(&self) -> E {
        self.engine.clone()
    }

    /// Schedule a command to the transaction scheduler. `cb` will be invoked after finishing
    /// running the command.
    #[inline]
    fn schedule(&self, cmd: Command, cb: StorageCb) -> Result<()> {
        fail_point!("storage_drop_message", |_| Ok(()));
        self.sched.run_cmd(cmd, cb);
        Ok(())
    }

    /// Get a snapshot of `engine`.
    fn async_snapshot(engine: &E, ctx: &Context) -> impl Future<Item = E::Snap, Error = Error> {
        let (callback, future) = tikv_util::future::paired_future_callback();
        let val = engine.async_snapshot(ctx, callback);

        future::result(val)
            .and_then(|_| future.map_err(|cancel| EngineError::Other(box_err!(cancel))))
            .and_then(|(_ctx, result)| result)
            // map storage::kv::Error -> storage::txn::Error -> storage::Error
            .map_err(txn::Error::from)
            .map_err(Error::from)
    }

    fn get_read_pool(&self, priority: CommandPriority) -> &FuturePool {
        match priority {
            CommandPriority::high => &self.read_pool_high,
            CommandPriority::normal => &self.read_pool_normal,
            CommandPriority::low => &self.read_pool_low,
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
    pub fn async_get(
        &self,
        mut ctx: Context,
        key: Key,
        start_ts: u64,
    ) -> impl Future<Item = Option<Value>, Error = Error> {
        const CMD: &str = "get";
        let priority = get_priority_tag(ctx.get_priority());

        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();

            // The bypass_locks set will be checked at most once. `TsSet::vec` is more efficient
            // here.
            let bypass_locks = TsSet::vec(ctx.take_resolved_locks());
            Self::with_tls_engine(|engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let mut statistics = Statistics::default();
                            let snap_store = SnapshotStore::new(
                                snapshot,
                                start_ts,
                                ctx.get_isolation_level(),
                                !ctx.get_not_fill_cache(),
                                bypass_locks,
                            );
                            let result = snap_store
                                .get(&key, &mut statistics)
                                // map storage::txn::Error -> storage::Error
                                .map_err(Error::from)
                                .map(|r| {
                                    tls_collect_key_reads(CMD, 1);
                                    r
                                });

                            tls_collect_scan_details(CMD, &statistics);
                            tls_collect_read_flow(ctx.get_region_id(), &statistics);

                            result
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Get values of a set of keys with seperate context from a snapshot, return a list of `Result`s.
    ///
    /// Only writes that are committed before their respective `start_ts` are visible.
    pub fn async_batch_get_command(
        &self,
        gets: Vec<PointGetCommand>,
    ) -> impl Future<Item = Vec<Result<Option<Vec<u8>>>>, Error = Error> {
        const CMD: &str = "batch_get_command";
        // all requests in a batch have the same region, epoch, term, replica_read
        let ctx = gets[0].ctx.clone();
        let priority = get_priority_tag(ctx.get_priority());
        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();

            Self::with_tls_engine(move |engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let mut statistics = Statistics::default();
                            let mut snap_store = SnapshotStore::new(
                                snapshot,
                                0,
                                ctx.get_isolation_level(),
                                !ctx.get_not_fill_cache(),
                                Default::default(),
                            );
                            let mut results = vec![];
                            // TODO: optimize using seek.
                            for mut get in gets {
                                snap_store.set_start_ts(get.ts.unwrap());
                                snap_store.set_isolation_level(get.ctx.get_isolation_level());
                                // The bypass_locks set will be checked at most once. `TsSet::vec`
                                // is more efficient here.
                                snap_store
                                    .set_bypass_locks(TsSet::vec(get.ctx.take_resolved_locks()));
                                results.push(
                                    snap_store
                                        .get(&get.key, &mut statistics)
                                        .map_err(Error::from),
                                );
                            }
                            future::ok(results)
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });
        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Get values of a set of keys in a batch from the snapshot.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn async_batch_get(
        &self,
        mut ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "batch_get";
        let priority = get_priority_tag(ctx.get_priority());

        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();

            let bypass_locks = TsSet::new(ctx.take_resolved_locks());
            Self::with_tls_engine(|engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let mut statistics = Statistics::default();
                            let snap_store = SnapshotStore::new(
                                snapshot,
                                start_ts,
                                ctx.get_isolation_level(),
                                !ctx.get_not_fill_cache(),
                                bypass_locks,
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
                                    tls_collect_key_reads(CMD, kv_pairs.len());
                                    kv_pairs
                                });

                            tls_collect_scan_details(CMD, &statistics);
                            tls_collect_read_flow(ctx.get_region_id(), &statistics);

                            result
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Scan keys in [`start_key`, `end_key`) up to `limit` keys from the snapshot.
    ///
    /// If `end_key` is `None`, it means the upper bound is unbounded.
    ///
    /// Only writes committed before `start_ts` are visible.
    pub fn async_scan(
        &self,
        mut ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        start_ts: u64,
        options: Options,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "scan";
        let priority = get_priority_tag(ctx.get_priority());

        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();

            let bypass_locks = TsSet::new(ctx.take_resolved_locks());
            Self::with_tls_engine(|engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let snap_store = SnapshotStore::new(
                                snapshot,
                                start_ts,
                                ctx.get_isolation_level(),
                                !ctx.get_not_fill_cache(),
                                bypass_locks,
                            );

                            let mut scanner;
                            if !options.reverse_scan {
                                scanner = snap_store.scanner(
                                    false,
                                    options.key_only,
                                    Some(start_key),
                                    end_key,
                                )?;
                            } else {
                                scanner = snap_store.scanner(
                                    true,
                                    options.key_only,
                                    end_key,
                                    Some(start_key),
                                )?;
                            };
                            let res = scanner.scan(limit);

                            let statistics = scanner.take_statistics();
                            tls_collect_scan_details(CMD, &statistics);
                            tls_collect_read_flow(ctx.get_region_id(), &statistics);

                            res.map_err(Error::from).map(|results| {
                                tls_collect_key_reads(CMD, results.len());
                                results
                                    .into_iter()
                                    .map(|x| x.map_err(Error::from))
                                    .collect()
                            })
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// **Testing functionality:** Latch the given keys for given duration.
    ///
    /// This means other write operations that involve these keys will be blocked.
    pub fn async_pause(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        duration: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::Pause { keys, duration },
        };
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.pause.inc();
        Ok(())
    }

    /// The prewrite phase of a transaction. The first phase of 2PC.
    ///
    /// Schedules a [`CommandKind::Prewrite`].
    pub fn async_prewrite(
        &self,
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
        options: Options,
        callback: Callback<Vec<Result<()>>>,
    ) -> Result<()> {
        for m in &mutations {
            let key_size = m.key().as_encoded().len();
            if key_size > self.max_key_size {
                callback(Err(Error::KeyTooLarge(key_size, self.max_key_size)));
                return Ok(());
            }
        }
        let cmd = Command {
            ctx,
            kind: CommandKind::Prewrite {
                mutations,
                primary,
                start_ts,
                options,
            },
        };
        self.schedule(cmd, StorageCb::Booleans(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.prewrite.inc();
        Ok(())
    }

    /// Acquire a Pessimistic lock on the keys.
    /// Schedules a [`CommandKind::AcquirePessimisticLock`].
    pub fn async_acquire_pessimistic_lock(
        &self,
        ctx: Context,
        keys: Vec<(Key, bool)>,
        primary: Vec<u8>,
        start_ts: u64,
        options: Options,
        callback: Callback<Vec<Result<()>>>,
    ) -> Result<()> {
        if !self.pessimistic_txn_enabled {
            callback(Err(Error::PessimisticTxnNotEnabled));
            return Ok(());
        }

        for k in &keys {
            let key_size = k.0.as_encoded().len();
            if key_size > self.max_key_size {
                callback(Err(Error::KeyTooLarge(key_size, self.max_key_size)));
                return Ok(());
            }
        }
        let cmd = Command {
            ctx,
            kind: CommandKind::AcquirePessimisticLock {
                keys,
                primary,
                start_ts,
                options,
            },
        };
        self.schedule(cmd, StorageCb::Booleans(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.acquire_pessimistic_lock.inc();
        Ok(())
    }

    /// Commit the transaction that started at `lock_ts`.
    ///
    /// Schedules a [`CommandKind::Commit`].
    pub fn async_commit(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::Commit {
                keys,
                lock_ts,
                commit_ts,
            },
        };
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.commit.inc();
        Ok(())
    }

    /// Delete all keys in the range [`start_key`, `end_key`).
    ///
    /// All keys in the range will be deleted permanently regardless of their timestamps.
    /// This means that deleted keys will not be retrievable by specifying an older timestamp.
    /// If `notify_only` is set, the data will not be immediately deleted, but the operation will
    /// still be replicated via Raft. This is used to notify that the data will be deleted by
    /// `unsafe_destroy_range` soon.
    ///
    /// Schedules a [`CommandKind::DeleteRange`].
    pub fn async_delete_range(
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

    /// Rollback mutations on a single key.
    ///
    /// Schedules a [`CommandKind::Cleanup`].
    pub fn async_cleanup(
        &self,
        ctx: Context,
        key: Key,
        start_ts: u64,
        current_ts: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::Cleanup {
                key,
                start_ts,
                current_ts,
            },
        };
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.cleanup.inc();
        Ok(())
    }

    /// Rollback from the transaction that was started at `start_ts`.
    ///
    /// Schedules a [`CommandKind::Rollback`].
    pub fn async_rollback(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::Rollback { keys, start_ts },
        };
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.rollback.inc();
        Ok(())
    }

    /// Rollback pessimistic locks identified by `start_ts` and `for_update_ts`.
    ///
    /// Schedules a [`CommandKind::PessimisticRollback`].
    pub fn async_pessimistic_rollback(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
        for_update_ts: u64,
        callback: Callback<Vec<Result<()>>>,
    ) -> Result<()> {
        if !self.pessimistic_txn_enabled {
            callback(Err(Error::PessimisticTxnNotEnabled));
            return Ok(());
        }

        let cmd = Command {
            ctx,
            kind: CommandKind::PessimisticRollback {
                keys,
                start_ts,
                for_update_ts,
            },
        };
        self.schedule(cmd, StorageCb::Booleans(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.pessimistic_rollback.inc();
        Ok(())
    }

    /// Check the specified primary key and enlarge it's TTL if necessary. Returns the new TTL.
    ///
    /// Schedules a [`CommandKind::TxnHeartBeat`].
    pub fn async_txn_heart_beat(
        &self,
        ctx: Context,
        primary_key: Key,
        start_ts: u64,
        advise_ttl: u64,
        callback: Callback<TxnStatus>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::TxnHeartBeat {
                primary_key,
                start_ts,
                advise_ttl,
            },
        };
        self.schedule(cmd, StorageCb::TxnStatus(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.txn_heart_beat.inc();
        Ok(())
    }

    /// Check the status of a transaction.
    ///
    /// This operation checks whether a transaction has expired it's Lock's TTL, rollback the
    /// transaction if expired, and update the transaction's min_commit_ts according to the metadata
    /// in the primary lock.
    /// After checking, if the lock is still alive, it retrieves the Lock's TTL; if the transaction
    /// is committed, get the commit_ts; otherwise, if the transaction is rolled back or there's
    /// no information about the transaction, results will be both 0.
    ///
    /// Schedules a [`CommandKind::CheckTxnStatus`].
    pub fn async_check_txn_status(
        &self,
        ctx: Context,
        primary_key: Key,
        lock_ts: u64,
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_exist: bool,
        callback: Callback<TxnStatus>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::CheckTxnStatus {
                primary_key,
                lock_ts,
                caller_start_ts,
                current_ts,
                rollback_if_not_exist,
            },
        };
        self.schedule(cmd, StorageCb::TxnStatus(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.check_txn_status.inc();
        Ok(())
    }

    /// Scan locks from `start_key`, and find all locks whose timestamp is before `max_ts`.
    ///
    /// Schedules a [`CommandKind::ScanLock`].
    pub fn async_scan_locks(
        &self,
        ctx: Context,
        max_ts: u64,
        start_key: Vec<u8>,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::ScanLock {
                max_ts,
                start_key: if start_key.is_empty() {
                    None
                } else {
                    Some(Key::from_raw(&start_key))
                },
                limit,
            },
        };
        self.schedule(cmd, StorageCb::Locks(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.scan_lock.inc();
        Ok(())
    }

    /// Resolve locks according to `txn_status`.
    ///
    /// During the GC operation, this should be called to clean up stale locks whose timestamp is
    /// before the safe point.
    ///
    /// `txn_status` maps lock_ts to commit_ts. If a transaction is rolled back, it is mapped to 0.
    /// For an example, check the [`CommandKind::ResolveLock`] docs.
    ///
    /// Schedules a [`CommandKind::ResolveLock`].
    pub fn async_resolve_lock(
        &self,
        ctx: Context,
        txn_status: HashMap<u64, u64>,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::ResolveLock {
                txn_status,
                scan_key: None,
                key_locks: vec![],
            },
        };
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.resolve_lock.inc();
        Ok(())
    }

    /// Resolve locks on `resolve_keys` according to `start_ts` and `commit_ts`.
    ///
    /// During the GC operation, this should be called to clean up stale locks whose timestamp is
    /// before the safe point.
    ///
    /// Schedules a [`CommandKind::ResolveLockLite`].
    pub fn async_resolve_lock_lite(
        &self,
        ctx: Context,
        start_ts: u64,
        commit_ts: u64,
        resolve_keys: Vec<Key>,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::ResolveLockLite {
                start_ts,
                commit_ts,
                resolve_keys,
            },
        };
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.resolve_lock_lite.inc();
        Ok(())
    }

    /// Get the value of a raw key.
    pub fn async_raw_get(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
    ) -> impl Future<Item = Option<Vec<u8>>, Error = Error> {
        const CMD: &str = "raw_get";
        let priority = get_priority_tag(ctx.get_priority());

        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();

            Self::with_tls_engine(|engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let cf = match Self::rawkv_cf(&cf) {
                                Ok(x) => x,
                                Err(e) => return future::err(e),
                            };
                            // no scan_count for this kind of op.

                            let key_len = key.len();
                            let result = snapshot
                                .get_cf(cf, &Key::from_encoded(key))
                                // map storage::engine::Error -> storage::Error
                                .map_err(Error::from)
                                .map(|r| {
                                    if let Some(ref value) = r {
                                        let mut stats = Statistics::default();
                                        stats.data.flow_stats.read_keys = 1;
                                        stats.data.flow_stats.read_bytes = key_len + value.len();
                                        tls_collect_read_flow(ctx.get_region_id(), &stats);
                                        tls_collect_key_reads(CMD, 1);
                                    }
                                    r
                                });
                            future::result(result)
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Get the values of a set of raw keys, return a list of `Result`s.
    pub fn async_raw_batch_get_command(
        &self,
        cf: String,
        gets: Vec<PointGetCommand>,
    ) -> impl Future<Item = Vec<Result<Option<Vec<u8>>>>, Error = Error> {
        const CMD: &str = "raw_batch_get_command";
        // all requests in a batch have the same region, epoch, term, replica_read
        let ctx = gets[0].ctx.clone();
        let priority = get_priority_tag(ctx.get_priority());
        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();
            Self::with_tls_engine(move |engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let cf = match Self::rawkv_cf(&cf) {
                                Ok(x) => x,
                                Err(e) => return future::err(e),
                            };
                            let mut results = vec![];
                            // TODO: optimize using seek.
                            for get in gets {
                                results.push(snapshot.get_cf(cf, &get.key).map_err(Error::from));
                            }
                            future::ok(results)
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });
        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Get the values of some raw keys in a batch.
    pub fn async_raw_batch_get(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "raw_batch_get";
        let priority = get_priority_tag(ctx.get_priority());

        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();

            Self::with_tls_engine(|engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let keys: Vec<Key> = keys.into_iter().map(Key::from_encoded).collect();
                            let cf = match Self::rawkv_cf(&cf) {
                                Ok(x) => x,
                                Err(e) => return future::err(e),
                            };
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
                                        stats.data.flow_stats.read_bytes +=
                                            k.as_encoded().len() + v.len();
                                        Ok((k.into_encoded(), v))
                                    }
                                    Err(e) => Err(Error::from(e)),
                                    _ => unreachable!(),
                                })
                                .collect();

                            tls_collect_key_reads(CMD, stats.data.flow_stats.read_keys as usize);
                            tls_collect_read_flow(ctx.get_region_id(), &stats);
                            future::ok(result)
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Write a raw key to the storage.
    pub fn async_raw_put(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        if key.len() > self.max_key_size {
            callback(Err(Error::KeyTooLarge(key.len(), self.max_key_size)));
            return Ok(());
        }
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
    pub fn async_raw_batch_put(
        &self,
        ctx: Context,
        cf: String,
        pairs: Vec<KvPair>,
        callback: Callback<()>,
    ) -> Result<()> {
        let cf = Self::rawkv_cf(&cf)?;
        for &(ref key, _) in &pairs {
            if key.len() > self.max_key_size {
                callback(Err(Error::KeyTooLarge(key.len(), self.max_key_size)));
                return Ok(());
            }
        }
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
    pub fn async_raw_delete(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        if key.len() > self.max_key_size {
            callback(Err(Error::KeyTooLarge(key.len(), self.max_key_size)));
            return Ok(());
        }
        self.engine.async_write(
            &ctx,
            vec![Modify::Delete(Self::rawkv_cf(&cf)?, Key::from_encoded(key))],
            Box::new(|(_, res): (_, kv::Result<_>)| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.raw_delete.inc();
        Ok(())
    }

    /// Delete all raw keys in [`start_key`, `end_key`).
    pub fn async_raw_delete_range(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        if start_key.len() > self.max_key_size || end_key.len() > self.max_key_size {
            callback(Err(Error::KeyTooLarge(
                cmp::max(start_key.len(), end_key.len()),
                self.max_key_size,
            )));
            return Ok(());
        }

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
    pub fn async_raw_batch_delete(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
        callback: Callback<()>,
    ) -> Result<()> {
        let cf = Self::rawkv_cf(&cf)?;
        for key in &keys {
            if key.len() > self.max_key_size {
                callback(Err(Error::KeyTooLarge(key.len(), self.max_key_size)));
                return Ok(());
            }
        }
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
    fn raw_scan(
        snapshot: &E::Snap,
        cf: &str,
        start_key: &Key,
        end_key: Option<Key>,
        limit: usize,
        statistics: &mut Statistics,
        key_only: bool,
    ) -> Result<Vec<Result<KvPair>>> {
        let mut option = IterOption::default();
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
        let mut option = IterOption::default();
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
    /// If `reverse` is false, the range is [`key`, `end_key`); otherwise, the range is
    /// [`end_key`, `key`) and it scans from `key` and goes backwards. If `end_key` is `None`, it
    /// means unbounded.
    ///
    /// This function scans at most `limit` keys.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only scanned keys will be returned.
    pub fn async_raw_scan(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
        key_only: bool,
        reverse: bool,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "raw_scan";
        let priority = get_priority_tag(ctx.get_priority());

        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();

            Self::with_tls_engine(|engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let end_key = end_key.map(Key::from_encoded);

                            let mut statistics = Statistics::default();
                            let result = if reverse {
                                Self::reverse_raw_scan(
                                    &snapshot,
                                    &cf,
                                    &Key::from_encoded(key),
                                    end_key,
                                    limit,
                                    &mut statistics,
                                    key_only,
                                )
                                .map_err(Error::from)
                            } else {
                                Self::raw_scan(
                                    &snapshot,
                                    &cf,
                                    &Key::from_encoded(key),
                                    end_key,
                                    limit,
                                    &mut statistics,
                                    key_only,
                                )
                                .map_err(Error::from)
                            };

                            tls_collect_read_flow(ctx.get_region_id(), &statistics);
                            tls_collect_key_reads(
                                CMD,
                                statistics.write.flow_stats.read_keys as usize,
                            );
                            tls_collect_scan_details(CMD, &statistics);
                            future::result(result)
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
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
        Err(Error::InvalidCf(cf.to_owned()))
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
    pub fn async_raw_batch_scan(
        &self,
        ctx: Context,
        cf: String,
        mut ranges: Vec<KeyRange>,
        each_limit: usize,
        key_only: bool,
        reverse: bool,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "raw_batch_scan";
        let priority = get_priority_tag(ctx.get_priority());

        let res = self.get_read_pool(priority).spawn_handle(move || {
            tls_collect_command_count(CMD, priority);
            let command_duration = tikv_util::time::Instant::now_coarse();

            Self::with_tls_engine(|engine| {
                Self::async_snapshot(engine, &ctx)
                    .and_then(move |snapshot: E::Snap| {
                        tls_processing_read_observe_duration(CMD, || {
                            let mut statistics = Statistics::default();
                            if !Self::check_key_ranges(&ranges, reverse) {
                                return future::result(Err(box_err!("Invalid KeyRanges")));
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
                                let pairs = if reverse {
                                    match Self::reverse_raw_scan(
                                        &snapshot,
                                        &cf,
                                        &start_key,
                                        end_key,
                                        each_limit,
                                        &mut statistics,
                                        key_only,
                                    ) {
                                        Ok(x) => x,
                                        Err(e) => return future::err(e),
                                    }
                                } else {
                                    match Self::raw_scan(
                                        &snapshot,
                                        &cf,
                                        &start_key,
                                        end_key,
                                        each_limit,
                                        &mut statistics,
                                        key_only,
                                    ) {
                                        Ok(x) => x,
                                        Err(e) => return future::err(e),
                                    }
                                };
                                result.extend(pairs.into_iter());
                            }

                            tls_collect_read_flow(ctx.get_region_id(), &statistics);
                            tls_collect_key_reads(
                                CMD,
                                statistics.write.flow_stats.read_keys as usize,
                            );
                            tls_collect_scan_details(CMD, &statistics);
                            future::ok(result)
                        })
                    })
                    .then(move |r| {
                        tls_collect_command_duration(CMD, command_duration.elapsed());
                        r
                    })
            })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Get MVCC info of a transactional key.
    pub fn async_mvcc_by_key(
        &self,
        ctx: Context,
        key: Key,
        callback: Callback<MvccInfo>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::MvccByKey { key },
        };
        self.schedule(cmd, StorageCb::MvccInfoByKey(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.key_mvcc.inc();

        Ok(())
    }

    /// Find the first key that has a version with its `start_ts` equal to the given `start_ts`, and
    /// return its MVCC info.
    pub fn async_mvcc_by_start_ts(
        &self,
        ctx: Context,
        start_ts: u64,
        callback: Callback<Option<(Key, MvccInfo)>>,
    ) -> Result<()> {
        let cmd = Command {
            ctx,
            kind: CommandKind::MvccByStartTs { start_ts },
        };
        self.schedule(cmd, StorageCb::MvccInfoByStartTs(callback))?;
        KV_COMMAND_COUNTER_VEC_STATIC.start_ts_mvcc.inc();
        Ok(())
    }
}

/// A builder to build a temporary `Storage<E>`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestStorageBuilder<E: Engine> {
    engine: E,
    config: Config,
}

impl TestStorageBuilder<RocksEngine> {
    /// Build `Storage<RocksEngine>`.
    pub fn new() -> Self {
        Self {
            engine: TestEngineBuilder::new().build().unwrap(),
            config: Config::default(),
        }
    }
}

impl<E: Engine> TestStorageBuilder<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            config: Config::default(),
        }
    }

    /// Customize the config of the `Storage`.
    ///
    /// By default, `Config::default()` will be used.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Build a `Storage<E>`.
    pub fn build(self) -> Result<Storage<E, DummyLockManager>> {
        let read_pool = self::readpool_impl::build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            self.engine.clone(),
        );
        Storage::from_engine(self.engine, &self.config, read_pool, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use kvproto::kvrpcpb::{CommandPri, Context, LockInfo};
    use tikv_util::config::ReadableSize;

    use std::fmt::Debug;
    use std::sync::mpsc::{channel, Sender};

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
                    Error::SchedTooBusy => {}
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
                .async_get(Context::default(), Key::from_raw(b"x"), 100)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error::Txn(txn::Error::Mvcc(mvcc::Error::KeyIsLocked { .. })) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .async_get(Context::default(), Key::from_raw(b"x"), 101)
                .wait(),
        );
        storage
            .async_commit(
                Context::default(),
                vec![Key::from_raw(b"x")],
                100,
                101,
                expect_ok_callback(tx.clone(), 3),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::default(), Key::from_raw(b"x"), 100)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::default(), Key::from_raw(b"x"), 101)
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
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"aa".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"bb".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"cc".to_vec())),
                ],
                b"a".to_vec(),
                1,
                Options::default(),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Request(..)))) => {
                    }
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Request(..)))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .async_get(Context::default(), Key::from_raw(b"x"), 1)
                .wait(),
        );
        expect_error(
            |e| match e {
                Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Request(..)))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"x"),
                    None,
                    1000,
                    1,
                    Options::default(),
                )
                .wait(),
        );
        expect_error(
            |e| match e {
                Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Request(..)))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .async_batch_get(
                    Context::default(),
                    vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                    1,
                )
                .wait(),
        );
        let x = storage
            .async_batch_get_command(vec![
                PointGetCommand::from_key_ts(Key::from_raw(b"c"), Some(1)),
                PointGetCommand::from_key_ts(Key::from_raw(b"d"), Some(1)),
            ])
            .wait()
            .unwrap();
        for v in x {
            expect_error(
                |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Request(..)))) => {
                    }
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
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"aa".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"bb".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"cc".to_vec())),
                ],
                b"a".to_vec(),
                1,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![None, None, None],
            storage
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    None,
                    1000,
                    5,
                    Options::default(),
                )
                .wait(),
        );
        // Backward
        expect_multi_values(
            vec![None, None, None],
            storage
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    None,
                    1000,
                    5,
                    Options::default().reverse_scan(),
                )
                .wait(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            storage
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    Some(Key::from_raw(b"c")),
                    1000,
                    5,
                    Options::default(),
                )
                .wait(),
        );
        // Backward with bound
        expect_multi_values(
            vec![None, None],
            storage
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    Some(Key::from_raw(b"b")),
                    1000,
                    5,
                    Options::default().reverse_scan(),
                )
                .wait(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            storage
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    None,
                    2,
                    5,
                    Options::default(),
                )
                .wait(),
        );
        // Backward with limit
        expect_multi_values(
            vec![None, None],
            storage
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    None,
                    2,
                    5,
                    Options::default().reverse_scan(),
                )
                .wait(),
        );

        storage
            .async_commit(
                Context::default(),
                vec![
                    Key::from_raw(b"a"),
                    Key::from_raw(b"b"),
                    Key::from_raw(b"c"),
                ],
                1,
                2,
                expect_ok_callback(tx.clone(), 1),
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
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    None,
                    1000,
                    5,
                    Options::default(),
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
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    None,
                    1000,
                    5,
                    Options::default().reverse_scan(),
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
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    Some(Key::from_raw(b"c")),
                    1000,
                    5,
                    Options::default(),
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
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    Some(Key::from_raw(b"b")),
                    1000,
                    5,
                    Options::default().reverse_scan(),
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
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\x00"),
                    None,
                    2,
                    5,
                    Options::default(),
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
                .async_scan(
                    Context::default(),
                    Key::from_raw(b"\xff"),
                    None,
                    2,
                    5,
                    Options::default().reverse_scan(),
                )
                .wait(),
        );
    }

    #[test]
    fn test_batch_get() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"aa".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"bb".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"cc".to_vec())),
                ],
                b"a".to_vec(),
                1,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![None],
            storage
                .async_batch_get(
                    Context::default(),
                    vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                    2,
                )
                .wait(),
        );
        storage
            .async_commit(
                Context::default(),
                vec![
                    Key::from_raw(b"a"),
                    Key::from_raw(b"b"),
                    Key::from_raw(b"c"),
                ],
                1,
                2,
                expect_ok_callback(tx.clone(), 1),
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
                .async_batch_get(
                    Context::default(),
                    vec![
                        Key::from_raw(b"c"),
                        Key::from_raw(b"x"),
                        Key::from_raw(b"a"),
                        Key::from_raw(b"b"),
                    ],
                    5,
                )
                .wait(),
        );
    }

    #[test]
    fn test_async_batch_get_command() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"aa".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"bb".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"cc".to_vec())),
                ],
                b"a".to_vec(),
                1,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut x = storage
            .async_batch_get_command(vec![
                PointGetCommand::from_key_ts(Key::from_raw(b"c"), Some(2)),
                PointGetCommand::from_key_ts(Key::from_raw(b"d"), Some(2)),
            ])
            .wait()
            .unwrap();
        expect_error(
            |e| match e {
                Error::Txn(txn::Error::Mvcc(mvcc::Error::KeyIsLocked(..))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            x.remove(0),
        );
        assert_eq!(x.remove(0).unwrap(), None);
        storage
            .async_commit(
                Context::default(),
                vec![
                    Key::from_raw(b"a"),
                    Key::from_raw(b"b"),
                    Key::from_raw(b"c"),
                ],
                1,
                2,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let x: Vec<Option<Vec<u8>>> = storage
            .async_batch_get_command(vec![
                PointGetCommand::from_key_ts(Key::from_raw(b"c"), Some(5)),
                PointGetCommand::from_key_ts(Key::from_raw(b"x"), Some(5)),
                PointGetCommand::from_key_ts(Key::from_raw(b"a"), Some(5)),
                PointGetCommand::from_key_ts(Key::from_raw(b"b"), Some(5)),
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
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"y"), b"101".to_vec()))],
                b"y".to_vec(),
                101,
                Options::default(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .async_commit(
                Context::default(),
                vec![Key::from_raw(b"x")],
                100,
                110,
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        storage
            .async_commit(
                Context::default(),
                vec![Key::from_raw(b"y")],
                101,
                111,
                expect_ok_callback(tx.clone(), 3),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::default(), Key::from_raw(b"x"), 120)
                .wait(),
        );
        expect_value(
            b"101".to_vec(),
            storage
                .async_get(Context::default(), Key::from_raw(b"y"), 120)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"x"), b"105".to_vec()))],
                b"x".to_vec(),
                105,
                Options::default(),
                expect_fail_callback(tx.clone(), 6, |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::WriteConflict { .. })) => (),
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
                .async_get(Context::default(), Key::from_raw(b"x"), 100)
                .wait(),
        );
        storage
            .async_pause(
                Context::default(),
                vec![Key::from_raw(b"x")],
                1000,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"y"), b"101".to_vec()))],
                b"y".to_vec(),
                101,
                Options::default(),
                expect_too_busy_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"z"), b"102".to_vec()))],
                b"y".to_vec(),
                102,
                Options::default(),
                expect_ok_callback(tx.clone(), 3),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_cleanup(
                Context::default(),
                Key::from_raw(b"x"),
                100,
                0,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::default(), Key::from_raw(b"x"), 105)
                .wait(),
        );
    }

    #[test]
    fn test_cleanup_check_ttl() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        let mut options = Options::default();
        options.lock_ttl = 100;
        let ts = mvcc::compose_ts;
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"x"), b"110".to_vec()))],
                b"x".to_vec(),
                ts(110, 0),
                options,
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .async_cleanup(
                Context::default(),
                Key::from_raw(b"x"),
                ts(110, 0),
                ts(120, 0),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::KeyIsLocked(info))) => {
                        assert_eq!(info.get_lock_ttl(), 100)
                    }
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .async_cleanup(
                Context::default(),
                Key::from_raw(b"x"),
                ts(110, 0),
                ts(220, 0),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::default(), Key::from_raw(b"x"), ts(230, 0))
                .wait(),
        );
    }

    #[test]
    fn test_high_priority_get_put() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(storage.async_get(ctx, Key::from_raw(b"x"), 100).wait());
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .async_prewrite(
                ctx,
                vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .async_commit(
                ctx,
                vec![Key::from_raw(b"x")],
                100,
                101,
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(storage.async_get(ctx, Key::from_raw(b"x"), 100).wait());
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            storage.async_get(ctx, Key::from_raw(b"x"), 101).wait(),
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
                .async_get(Context::default(), Key::from_raw(b"x"), 100)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_commit(
                Context::default(),
                vec![Key::from_raw(b"x")],
                100,
                101,
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .async_pause(
                Context::default(),
                vec![Key::from_raw(b"y")],
                1000,
                expect_ok_callback(tx.clone(), 3),
            )
            .unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            storage.async_get(ctx, Key::from_raw(b"x"), 101).wait(),
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
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"x"), b"100".to_vec())),
                    Mutation::Put((Key::from_raw(b"y"), b"100".to_vec())),
                    Mutation::Put((Key::from_raw(b"z"), b"100".to_vec())),
                ],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_commit(
                Context::default(),
                vec![
                    Key::from_raw(b"x"),
                    Key::from_raw(b"y"),
                    Key::from_raw(b"z"),
                ],
                100,
                101,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::default(), Key::from_raw(b"x"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::default(), Key::from_raw(b"y"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::default(), Key::from_raw(b"z"), 101)
                .wait(),
        );

        // Delete range [x, z)
        storage
            .async_delete_range(
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
                .async_get(Context::default(), Key::from_raw(b"x"), 101)
                .wait(),
        );
        expect_none(
            storage
                .async_get(Context::default(), Key::from_raw(b"y"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::default(), Key::from_raw(b"z"), 101)
                .wait(),
        );

        storage
            .async_delete_range(
                Context::default(),
                Key::from_raw(b""),
                Key::from_raw(&[255]),
                false,
                expect_ok_callback(tx.clone(), 9),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::default(), Key::from_raw(b"z"), 101)
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
                .async_raw_put(
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
                .async_raw_get(Context::default(), "".to_string(), b"d".to_vec())
                .wait(),
        );

        // Delete ["d", "e")
        storage
            .async_raw_delete_range(
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
                .async_raw_get(Context::default(), "".to_string(), b"c".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .async_raw_get(Context::default(), "".to_string(), b"d".to_vec())
                .wait(),
        );
        expect_value(
            b"005".to_vec(),
            storage
                .async_raw_get(Context::default(), "".to_string(), b"e".to_vec())
                .wait(),
        );

        // Delete ["aa", "ab")
        storage
            .async_raw_delete_range(
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
                .async_raw_get(Context::default(), "".to_string(), b"a".to_vec())
                .wait(),
        );
        expect_value(
            b"002".to_vec(),
            storage
                .async_raw_get(Context::default(), "".to_string(), b"b".to_vec())
                .wait(),
        );

        // Delete all
        storage
            .async_raw_delete_range(
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
                    .async_raw_get(Context::default(), "".to_string(), kv.0.to_vec())
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
            .async_raw_batch_put(
                Context::default(),
                "".to_string(),
                test_data.clone(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs one by one
        for (key, val) in test_data {
            expect_value(
                val,
                storage
                    .async_raw_get(Context::default(), "".to_string(), key)
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
                .async_raw_put(
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
                .async_raw_batch_get(Context::default(), "".to_string(), keys)
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
                .async_raw_put(
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
            .map(|&(ref k, _)| PointGetCommand::from_key_ts(Key::from_encoded(k.clone()), Some(0)))
            .collect();
        let results: Vec<Option<Vec<u8>>> = test_data.into_iter().map(|(_, v)| Some(v)).collect();
        let x: Vec<Option<Vec<u8>>> = storage
            .async_raw_batch_get_command("".to_string(), cmds)
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
            .async_raw_batch_put(
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
                .async_raw_batch_get(Context::default(), "".to_string(), keys)
                .wait(),
        );

        // Delete ["b", "d"]
        storage
            .async_raw_batch_delete(
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
                .async_raw_get(Context::default(), "".to_string(), b"a".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .async_raw_get(Context::default(), "".to_string(), b"b".to_vec())
                .wait(),
        );
        expect_value(
            b"cc".to_vec(),
            storage
                .async_raw_get(Context::default(), "".to_string(), b"c".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .async_raw_get(Context::default(), "".to_string(), b"d".to_vec())
                .wait(),
        );
        expect_value(
            b"ee".to_vec(),
            storage
                .async_raw_get(Context::default(), "".to_string(), b"e".to_vec())
                .wait(),
        );

        // Delete ["a", "c", "e"]
        storage
            .async_raw_batch_delete(
                Context::default(),
                "".to_string(),
                vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()],
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert no key remains
        for (k, _) in test_data {
            expect_none(
                storage
                    .async_raw_get(Context::default(), "".to_string(), k)
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
            .async_raw_batch_put(
                Context::default(),
                "".to_string(),
                test_data.clone(),
                expect_ok_callback(tx.clone(), 0),
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
                .async_raw_scan(
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
                .async_raw_scan(
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
                .async_raw_scan(
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
                .async_raw_scan(
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
                .async_raw_scan(
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
                .async_raw_scan(
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
            results.clone(),
            storage
                .async_raw_scan(
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
            results.clone(),
            storage
                .async_raw_scan(
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
            results.clone(),
            storage
                .async_raw_scan(
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
            .clone()
            .into_iter()
            .skip(6)
            .take(1)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results.clone(),
            storage
                .async_raw_scan(
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
            <Storage<RocksEngine, DummyLockManager>>::async_snapshot(&engine, &ctx)
                .and_then(move |snapshot| {
                    <Storage<RocksEngine, DummyLockManager>>::raw_scan(
                        &snapshot,
                        &"".to_string(),
                        &Key::from_encoded(b"c1".to_vec()),
                        Some(Key::from_encoded(b"d3".to_vec())),
                        20,
                        &mut Statistics::default(),
                        false,
                    )
                })
                .wait(),
        );
        expect_multi_values(
            results.rev().collect(),
            <Storage<RocksEngine, DummyLockManager>>::async_snapshot(&engine, &ctx)
                .and_then(move |snapshot| {
                    <Storage<RocksEngine, DummyLockManager>>::reverse_raw_scan(
                        &snapshot,
                        &"".to_string(),
                        &Key::from_encoded(b"d3".to_vec()),
                        Some(Key::from_encoded(b"c1".to_vec())),
                        20,
                        &mut Statistics::default(),
                        false,
                    )
                })
                .wait(),
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
            .async_raw_batch_put(
                Context::default(),
                "".to_string(),
                test_data.clone(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs exist
        let keys = test_data.iter().map(|&(ref k, _)| k.clone()).collect();
        let results = test_data.into_iter().map(|(k, v)| Some((k, v))).collect();
        expect_multi_values(
            results,
            storage
                .async_raw_batch_get(Context::default(), "".to_string(), keys)
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
                .async_raw_batch_scan(
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
                .async_raw_batch_scan(
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
                .async_raw_batch_scan(
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
                .async_raw_batch_scan(Context::default(), "".to_string(), ranges, 3, true, false)
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
                .async_raw_batch_scan(Context::default(), "".to_string(), ranges, 5, false, true)
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
                .async_raw_batch_scan(Context::default(), "".to_string(), ranges, 2, false, true)
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
                .async_raw_batch_scan(Context::default(), "".to_string(), ranges, 5, true, true)
                .wait(),
        );
    }

    #[test]
    fn test_scan_lock() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"x"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"y"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"z"), b"foo".to_vec())),
                ],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut options = Options::default();
        options.lock_ttl = 123;
        options.txn_size = 3;
        storage
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                ],
                b"c".to_vec(),
                101,
                options,
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
            .async_scan_locks(
                Context::default(),
                99,
                vec![],
                10,
                expect_value_callback(tx.clone(), 0, vec![]),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_scan_locks(
                Context::default(),
                100,
                vec![],
                10,
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![lock_x.clone(), lock_y.clone(), lock_z.clone()],
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_scan_locks(
                Context::default(),
                100,
                b"a".to_vec(),
                10,
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![lock_x.clone(), lock_y.clone(), lock_z.clone()],
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_scan_locks(
                Context::default(),
                100,
                b"y".to_vec(),
                10,
                expect_value_callback(tx.clone(), 0, vec![lock_y.clone(), lock_z.clone()]),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_scan_locks(
                Context::default(),
                101,
                vec![],
                10,
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
            .async_scan_locks(
                Context::default(),
                101,
                vec![],
                4,
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![
                        lock_a.clone(),
                        lock_b.clone(),
                        lock_c.clone(),
                        lock_x.clone(),
                    ],
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_scan_locks(
                Context::default(),
                101,
                b"b".to_vec(),
                4,
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
            .async_scan_locks(
                Context::default(),
                101,
                b"b".to_vec(),
                0,
                expect_value_callback(
                    tx.clone(),
                    0,
                    vec![
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
    }

    #[test]
    fn test_resolve_lock() {
        use crate::storage::txn::RESOLVE_LOCK_BATCH_SIZE;

        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        // These locks (transaction ts=99) are not going to be resolved.
        storage
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                ],
                b"c".to_vec(),
                99,
                Options::default(),
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
        let mut ts = 100;

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
                    .async_prewrite(
                        Context::default(),
                        mutations,
                        b"x".to_vec(),
                        ts,
                        Options::default(),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let mut txn_status = HashMap::default();
                txn_status.insert(
                    ts,
                    if *is_rollback {
                        0 // rollback
                    } else {
                        ts + 5 // commit, commit_ts = start_ts + 5
                    },
                );
                storage
                    .async_resolve_lock(
                        Context::default(),
                        txn_status,
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                // All locks should be resolved except for a, b and c.
                storage
                    .async_scan_locks(
                        Context::default(),
                        ts,
                        vec![],
                        0,
                        expect_value_callback(
                            tx.clone(),
                            0,
                            vec![lock_a.clone(), lock_b.clone(), lock_c.clone()],
                        ),
                    )
                    .unwrap();
                rx.recv().unwrap();

                ts += 10;
            }
        }
    }

    #[test]
    fn test_resolve_lock_lite() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        storage
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                ],
                b"c".to_vec(),
                99,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Rollback key 'b' and key 'c' and left key 'a' still locked.
        let resolve_keys = vec![Key::from_raw(b"b"), Key::from_raw(b"c")];
        storage
            .async_resolve_lock_lite(
                Context::default(),
                99,
                0,
                resolve_keys,
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
            .async_scan_locks(
                Context::default(),
                99,
                vec![],
                0,
                expect_value_callback(tx.clone(), 0, vec![lock_a]),
            )
            .unwrap();
        rx.recv().unwrap();

        // Resolve lock for key 'a'.
        storage
            .async_resolve_lock_lite(
                Context::default(),
                99,
                0,
                vec![Key::from_raw(b"a")],
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                ],
                b"c".to_vec(),
                101,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Commit key 'b' and key 'c' and left key 'a' still locked.
        let resolve_keys = vec![Key::from_raw(b"b"), Key::from_raw(b"c")];
        storage
            .async_resolve_lock_lite(
                Context::default(),
                101,
                102,
                resolve_keys,
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
            .async_scan_locks(
                Context::default(),
                101,
                vec![],
                0,
                expect_value_callback(tx.clone(), 0, vec![lock_a]),
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
            .async_txn_heart_beat(
                Context::default(),
                k.clone(),
                10,
                100,
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::TxnLockNotFound { .. })) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut options = Options::default();
        options.lock_ttl = 100;
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((k.clone(), v))],
                k.as_encoded().to_vec(),
                10,
                options,
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // `advise_ttl` = 90, which is less than current ttl 100. The lock's ttl will remains 100.
        storage
            .async_txn_heart_beat(
                Context::default(),
                k.clone(),
                10,
                90,
                expect_value_callback(tx.clone(), 0, uncommitted(100, 0)),
            )
            .unwrap();
        rx.recv().unwrap();

        // `advise_ttl` = 110, which is greater than current ttl. The lock's ttl will be updated to
        // 110.
        storage
            .async_txn_heart_beat(
                Context::default(),
                k.clone(),
                10,
                110,
                expect_value_callback(tx.clone(), 0, uncommitted(110, 0)),
            )
            .unwrap();
        rx.recv().unwrap();

        // Lock not match. Nothing happens except throwing an error.
        storage
            .async_txn_heart_beat(
                Context::default(),
                k.clone(),
                11,
                150,
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::TxnLockNotFound { .. })) => (),
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

        let ts = mvcc::compose_ts;
        use TxnStatus::*;
        let uncommitted = TxnStatus::uncommitted;
        let committed = TxnStatus::committed;

        // No lock and no commit info. Gets an error.
        storage
            .async_check_txn_status(
                Context::default(),
                k.clone(),
                ts(9, 0),
                ts(9, 1),
                ts(9, 1),
                false,
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::TxnNotFound { .. })) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        // No lock and no commit info. If specified rollback_if_not_exist, the key will be rolled
        // back.
        storage
            .async_check_txn_status(
                Context::default(),
                k.clone(),
                ts(9, 0),
                ts(9, 1),
                ts(9, 1),
                true,
                expect_value_callback(tx.clone(), 0, LockNotExist),
            )
            .unwrap();
        rx.recv().unwrap();

        // A rollback will be written, so an later-arriving prewrite will fail.
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((k.clone(), v.clone()))],
                k.as_encoded().to_vec(),
                ts(9, 0),
                Options::default(),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::WriteConflict { .. })) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut options = Options::default();
        options.lock_ttl = 100;
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((k.clone(), v.clone()))],
                k.as_encoded().to_vec(),
                ts(10, 0),
                options.clone(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If lock exists and not expired, returns the lock's TTL.
        storage
            .async_check_txn_status(
                Context::default(),
                k.clone(),
                ts(10, 0),
                ts(12, 0),
                ts(15, 0),
                true,
                expect_value_callback(tx.clone(), 0, uncommitted(100, 0)),
            )
            .unwrap();
        rx.recv().unwrap();

        // TODO: Check the lock's min_commit_ts field.

        storage
            .async_commit(
                Context::default(),
                vec![k.clone()],
                ts(10, 0),
                ts(20, 0),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the transaction is committed, returns the commit_ts.
        storage
            .async_check_txn_status(
                Context::default(),
                k.clone(),
                ts(10, 0),
                ts(12, 0),
                ts(15, 0),
                true,
                expect_value_callback(tx.clone(), 0, committed(ts(20, 0))),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((k.clone(), v))],
                k.as_encoded().to_vec(),
                ts(25, 0),
                options,
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the lock has expired, cleanup it.
        storage
            .async_check_txn_status(
                Context::default(),
                k.clone(),
                ts(25, 0),
                ts(126, 0),
                ts(127, 0),
                true,
                expect_value_callback(tx.clone(), 0, TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .async_commit(
                Context::default(),
                vec![k.clone()],
                ts(25, 0),
                ts(28, 0),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error::Txn(txn::Error::Mvcc(mvcc::Error::TxnLockNotFound { .. })) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }
}
