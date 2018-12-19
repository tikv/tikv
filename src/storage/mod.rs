// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod config;
pub mod engine;
pub mod gc_worker;
mod metrics;
pub mod mvcc;
mod readpool_context;
pub mod txn;
pub mod types;

use std::boxed::FnBox;
use std::cmp;
use std::error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;
use std::sync::{atomic, Arc, Mutex};
use std::u64;

use futures::{future, Future};
use kvproto::errorpb;
use kvproto::kvrpcpb::{CommandPri, Context, KeyRange, LockInfo};

use rocksdb::DB;

use raftstore::store::engine::IterOption;
use server::readpool::{self, ReadPool};
use server::ServerRaftStoreRouter;
use util;
use util::collections::HashMap;
use util::worker::{self, Builder, ScheduleError, Worker};

use self::gc_worker::GCWorker;
use self::metrics::*;
use self::mvcc::Lock;
use self::txn::CMD_BATCH_SIZE;

pub use self::config::{Config, DEFAULT_DATA_DIR, DEFAULT_ROCKSDB_SUB_DIR};
pub use self::engine::raftkv::RaftKv;
pub use self::engine::{
    CFStatistics, Cursor, CursorBuilder, Engine, Error as EngineError, FlowStatistics, Iterator,
    Modify, RocksEngine, ScanMode, Snapshot, Statistics, StatisticsSummary, TestEngineBuilder,
};
pub use self::readpool_context::Context as ReadPoolContext;
pub use self::txn::{FixtureStore, FixtureStoreScanner};
pub use self::txn::{Msg, Scanner, Scheduler, SnapshotStore, Store, StoreScanner};
pub use self::types::{Key, KvPair, MvccInfo, Value};
pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";
// Cfs that should be very large generally.
pub const LARGE_CFS: &[CfName] = &[CF_DEFAULT, CF_WRITE];
pub const ALL_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];
pub const DATA_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

// Short value max len must <= 255.
pub const SHORT_VALUE_MAX_LEN: usize = 64;
pub const SHORT_VALUE_PREFIX: u8 = b'v';

pub fn is_short_value(value: &[u8]) -> bool {
    value.len() <= SHORT_VALUE_MAX_LEN
}

#[derive(Debug, Clone)]
pub enum Mutation {
    Put((Key, Value)),
    Delete(Key),
    Lock(Key),
}

#[cfg_attr(feature = "cargo-clippy", allow(match_same_arms))]
impl Mutation {
    pub fn key(&self) -> &Key {
        match *self {
            Mutation::Put((ref key, _)) => key,
            Mutation::Delete(ref key) => key,
            Mutation::Lock(ref key) => key,
        }
    }
}

pub enum StorageCb {
    Boolean(Callback<()>),
    Booleans(Callback<Vec<Result<()>>>),
    MvccInfoByKey(Callback<MvccInfo>),
    MvccInfoByStartTs(Callback<Option<(Key, MvccInfo)>>),
    Locks(Callback<Vec<LockInfo>>),
}

pub enum Command {
    Prewrite {
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
        options: Options,
    },
    Commit {
        ctx: Context,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
    },
    Cleanup {
        ctx: Context,
        key: Key,
        start_ts: u64,
    },
    Rollback {
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
    },
    ScanLock {
        ctx: Context,
        max_ts: u64,
        start_key: Option<Key>,
        limit: usize,
    },
    ResolveLock {
        ctx: Context,
        txn_status: HashMap<u64, u64>,
        scan_key: Option<Key>,
        key_locks: Vec<(Key, Lock)>,
    },
    DeleteRange {
        ctx: Context,
        start_key: Key,
        end_key: Key,
    },
    // only for test, keep the latches of keys for a while
    Pause {
        ctx: Context,
        keys: Vec<Key>,
        duration: u64,
    },
    MvccByKey {
        ctx: Context,
        key: Key,
    },
    MvccByStartTs {
        ctx: Context,
        start_ts: u64,
    },
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Command::Prewrite {
                ref ctx,
                ref mutations,
                start_ts,
                ..
            } => write!(
                f,
                "kv::command::prewrite mutations({}) @ {} | {:?}",
                mutations.len(),
                start_ts,
                ctx
            ),
            Command::Commit {
                ref ctx,
                ref keys,
                lock_ts,
                commit_ts,
                ..
            } => write!(
                f,
                "kv::command::commit {} {} -> {} | {:?}",
                keys.len(),
                lock_ts,
                commit_ts,
                ctx
            ),
            Command::Cleanup {
                ref ctx,
                ref key,
                start_ts,
                ..
            } => write!(f, "kv::command::cleanup {} @ {} | {:?}", key, start_ts, ctx),
            Command::Rollback {
                ref ctx,
                ref keys,
                start_ts,
                ..
            } => write!(
                f,
                "kv::command::rollback keys({}) @ {} | {:?}",
                keys.len(),
                start_ts,
                ctx
            ),
            Command::ScanLock {
                ref ctx,
                max_ts,
                ref start_key,
                limit,
                ..
            } => write!(
                f,
                "kv::scan_lock {:?} {} @ {} | {:?}",
                start_key, limit, max_ts, ctx
            ),
            Command::ResolveLock { .. } => write!(f, "kv::resolve_lock"),
            Command::DeleteRange {
                ref ctx,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "kv::command::delete range [{:?}, {:?}) | {:?}",
                start_key, end_key, ctx
            ),
            Command::Pause {
                ref ctx,
                ref keys,
                duration,
            } => write!(
                f,
                "kv::command::pause keys:({}) {} ms | {:?}",
                keys.len(),
                duration,
                ctx
            ),
            Command::MvccByKey { ref ctx, ref key } => {
                write!(f, "kv::command::mvccbykey {:?} | {:?}", key, ctx)
            }
            Command::MvccByStartTs {
                ref ctx,
                ref start_ts,
            } => write!(f, "kv::command::mvccbystartts {:?} | {:?}", start_ts, ctx),
        }
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub const CMD_TAG_GC: &str = "gc";
pub const CMD_TAG_UNSAFE_DESTROY_RANGE: &str = "unsafe_destroy_range";

impl Command {
    pub fn readonly(&self) -> bool {
        match *self {
            Command::ScanLock { .. } |
            // DeleteRange only called by DDL bg thread after table is dropped and
            // must guarantee that there is no other read or write on these keys, so
            // we can treat DeleteRange as readonly Command.
            Command::DeleteRange { .. } |
            Command::MvccByKey { .. } |
            Command::MvccByStartTs { .. } => true,
            Command::ResolveLock { ref key_locks, .. } => key_locks.is_empty(),
            _ => false,
        }
    }

    pub fn priority(&self) -> CommandPri {
        self.get_context().get_priority()
    }

    pub fn priority_tag(&self) -> &'static str {
        match self.get_context().get_priority() {
            CommandPri::Low => "low",
            CommandPri::Normal => "normal",
            CommandPri::High => "high",
        }
    }

    pub fn need_flow_control(&self) -> bool {
        !self.readonly() && self.priority() != CommandPri::High
    }

    pub fn tag(&self) -> &'static str {
        match *self {
            Command::Prewrite { .. } => "prewrite",
            Command::Commit { .. } => "commit",
            Command::Cleanup { .. } => "cleanup",
            Command::Rollback { .. } => "rollback",
            Command::ScanLock { .. } => "scan_lock",
            Command::ResolveLock { .. } => "resolve_lock",
            Command::DeleteRange { .. } => "delete_range",
            Command::Pause { .. } => "pause",
            Command::MvccByKey { .. } => "key_mvcc",
            Command::MvccByStartTs { .. } => "start_ts_mvcc",
        }
    }

    pub fn ts(&self) -> u64 {
        match *self {
            Command::Prewrite { start_ts, .. }
            | Command::Cleanup { start_ts, .. }
            | Command::Rollback { start_ts, .. }
            | Command::MvccByStartTs { start_ts, .. } => start_ts,
            Command::Commit { lock_ts, .. } => lock_ts,
            Command::ScanLock { max_ts, .. } => max_ts,
            Command::ResolveLock { .. }
            | Command::DeleteRange { .. }
            | Command::Pause { .. }
            | Command::MvccByKey { .. } => 0,
        }
    }

    pub fn get_context(&self) -> &Context {
        match *self {
            Command::Prewrite { ref ctx, .. }
            | Command::Commit { ref ctx, .. }
            | Command::Cleanup { ref ctx, .. }
            | Command::Rollback { ref ctx, .. }
            | Command::ScanLock { ref ctx, .. }
            | Command::ResolveLock { ref ctx, .. }
            | Command::DeleteRange { ref ctx, .. }
            | Command::Pause { ref ctx, .. }
            | Command::MvccByKey { ref ctx, .. }
            | Command::MvccByStartTs { ref ctx, .. } => ctx,
        }
    }

    pub fn mut_context(&mut self) -> &mut Context {
        match *self {
            Command::Prewrite { ref mut ctx, .. }
            | Command::Commit { ref mut ctx, .. }
            | Command::Cleanup { ref mut ctx, .. }
            | Command::Rollback { ref mut ctx, .. }
            | Command::ScanLock { ref mut ctx, .. }
            | Command::ResolveLock { ref mut ctx, .. }
            | Command::DeleteRange { ref mut ctx, .. }
            | Command::Pause { ref mut ctx, .. }
            | Command::MvccByKey { ref mut ctx, .. }
            | Command::MvccByStartTs { ref mut ctx, .. } => ctx,
        }
    }

    pub fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        match *self {
            Command::Prewrite { ref mutations, .. } => for m in mutations {
                match *m {
                    Mutation::Put((ref key, ref value)) => {
                        bytes += key.as_encoded().len();
                        bytes += value.len();
                    }
                    Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                        bytes += key.as_encoded().len();
                    }
                }
            },
            Command::Commit { ref keys, .. } | Command::Rollback { ref keys, .. } => {
                for key in keys {
                    bytes += key.as_encoded().len();
                }
            }
            Command::ResolveLock { ref key_locks, .. } => for lock in key_locks {
                bytes += lock.0.as_encoded().len();
            },
            Command::Cleanup { ref key, .. } => {
                bytes += key.as_encoded().len();
            }
            Command::Pause { ref keys, .. } => {
                for key in keys {
                    bytes += key.as_encoded().len();
                }
            }
            _ => {}
        }
        bytes
    }
}

#[derive(Clone, Default)]
pub struct Options {
    pub lock_ttl: u64,
    pub skip_constraint_check: bool,
    pub key_only: bool,
    pub reverse_scan: bool,
}

impl Options {
    pub fn new(lock_ttl: u64, skip_constraint_check: bool, key_only: bool) -> Options {
        Options {
            lock_ttl,
            skip_constraint_check,
            key_only,
            reverse_scan: false,
        }
    }

    pub fn reverse_scan(mut self) -> Options {
        self.reverse_scan = true;
        self
    }
}

/// A builder to build a temporary `Storage<E>`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestStorageBuilder<E: Engine> {
    engine: E,
    config: Config,
    local_storage: Option<Arc<DB>>,
    raft_store_router: Option<ServerRaftStoreRouter>,
}

impl TestStorageBuilder<RocksEngine> {
    /// Build `Storage<RocksEngine>`.
    pub fn new() -> Self {
        Self {
            engine: TestEngineBuilder::new().build().unwrap(),
            config: Config::default(),
            local_storage: None,
            raft_store_router: None,
        }
    }
}

impl<E: Engine> TestStorageBuilder<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            config: Config::default(),
            local_storage: None,
            raft_store_router: None,
        }
    }

    /// Customize the config of the `Storage`.
    ///
    /// By default, `Config::default()` will be used.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Set local storage for GCWorker.
    ///
    /// By default, `None` will be used.
    pub fn local_storage(mut self, local_storage: Arc<DB>) -> Self {
        self.local_storage = Some(local_storage);
        self
    }

    /// Set raft store router for GCWorker.
    ///
    /// By default, `None` will be used.
    pub fn raft_store_router(mut self, raft_store_router: ServerRaftStoreRouter) -> Self {
        self.raft_store_router = Some(raft_store_router);
        self
    }

    /// Build a `Storage<E>`.
    pub fn build(self) -> Result<Storage<E>> {
        use util::worker::FutureWorker;

        let read_pool = {
            let pd_worker = FutureWorker::new("test-future–worker");
            ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
                || ReadPoolContext::new(pd_worker.scheduler())
            })
        };
        Storage::from_engine(
            self.engine,
            &self.config,
            read_pool,
            self.local_storage,
            self.raft_store_router,
        )
    }
}

pub struct Storage<E: Engine> {
    // TODO: Too many Arcs, would be slow when clone.
    engine: E,

    /// To schedule the execution of storage commands
    worker: Arc<Mutex<Worker<Msg>>>,
    worker_scheduler: worker::Scheduler<Msg>,

    read_pool: ReadPool<ReadPoolContext>,
    gc_worker: GCWorker<E>,

    /// How many strong references. Thread pool and workers will be stopped
    /// once there are no more references.
    refs: Arc<atomic::AtomicUsize>,

    // Fields below are storage configurations.
    max_key_size: usize,
}

impl<E: Engine> Clone for Storage<E> {
    #[inline]
    fn clone(&self) -> Self {
        let refs = self.refs.fetch_add(1, atomic::Ordering::SeqCst);

        debug!(
            "Storage ({} engine) referenced (reference count before operation: {})",
            self.engine, refs
        );

        Self {
            engine: self.engine.clone(),
            worker: self.worker.clone(),
            worker_scheduler: self.worker_scheduler.clone(),
            read_pool: self.read_pool.clone(),
            gc_worker: self.gc_worker.clone(),
            refs: self.refs.clone(),
            max_key_size: self.max_key_size,
        }
    }
}

impl<E: Engine> Drop for Storage<E> {
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, atomic::Ordering::SeqCst);

        debug!(
            "Storage ({} engine) de-referenced (reference count before operation: {})",
            self.engine, refs
        );

        if refs != 1 {
            return;
        }

        let mut worker = self.worker.lock().unwrap();
        if let Err(e) = worker.schedule(Msg::Quit) {
            error!("Failed to ask scheduler to quit: {:?}", e);
        }

        let h = worker.stop().unwrap();
        if let Err(e) = h.join() {
            error!("Failed to join sched_handle: {:?}", e);
        }

        let r = self.gc_worker.stop();
        if let Err(e) = r {
            error!("Failed to stop gc_worker: {:?}", e);
        }

        info!("Storage ({} engine) stopped.", self.engine);
    }
}

impl<E: Engine> Storage<E> {
    pub fn from_engine(
        engine: E,
        config: &Config,
        read_pool: ReadPool<ReadPoolContext>,
        local_storage: Option<Arc<DB>>,
        raft_store_router: Option<ServerRaftStoreRouter>,
    ) -> Result<Self> {
        let worker = Arc::new(Mutex::new(
            Builder::new("storage-scheduler")
                .batch_size(CMD_BATCH_SIZE)
                .pending_capacity(config.scheduler_notify_capacity)
                .create(),
        ));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        let runner = Scheduler::new(
            engine.clone(),
            worker_scheduler.clone(),
            config.scheduler_concurrency,
            config.scheduler_worker_pool_size,
            config.scheduler_pending_write_threshold.0 as usize,
        );
        let mut gc_worker = GCWorker::new(
            engine.clone(),
            local_storage,
            raft_store_router,
            config.gc_ratio_threshold,
        );

        worker.lock().unwrap().start(runner)?;
        gc_worker.start()?;

        info!("Storage ({} engine) started.", engine);

        Ok(Storage {
            engine,
            worker,
            worker_scheduler,
            read_pool,
            gc_worker,
            refs: Arc::new(atomic::AtomicUsize::new(1)),
            max_key_size: config.max_key_size,
        })
    }

    pub fn get_engine(&self) -> E {
        self.engine.clone()
    }

    #[inline]
    fn schedule(&self, cmd: Command, cb: StorageCb) -> Result<()> {
        fail_point!("storage_drop_message", |_| Ok(()));
        match self.worker_scheduler.schedule(Msg::RawCmd { cmd, cb }) {
            Ok(()) => Ok(()),
            Err(ScheduleError::Full(_)) => Err(Error::SchedTooBusy),
            Err(ScheduleError::Stopped(_)) => Err(Error::Closed),
        }
    }

    fn async_snapshot(engine: E, ctx: &Context) -> impl Future<Item = E::Snap, Error = Error> {
        let (callback, future) = util::future::paired_future_callback();
        let val = engine.async_snapshot(ctx, callback);

        future::result(val)
            .and_then(|_| future.map_err(|cancel| EngineError::Other(box_err!(cancel))))
            .and_then(|(_ctx, result)| result)
            // map storage::engine::Error -> storage::txn::Error -> storage::Error
            .map_err(txn::Error::from)
            .map_err(Error::from)
    }

    /// Get from the snapshot.
    pub fn async_get(
        &self,
        ctx: Context,
        key: Key,
        start_ts: u64,
    ) -> impl Future<Item = Option<Value>, Error = Error> {
        const CMD: &str = "get";
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: E::Snap| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);

                    let mut statistics = Statistics::default();
                    let snap_store = SnapshotStore::new(
                        snapshot,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                    );
                    let result = snap_store
                        .get(&key, &mut statistics)
                        // map storage::txn::Error -> storage::Error
                        .map_err(Error::from)
                        .map(|r| {
                            thread_ctx.collect_key_reads(CMD, 1);
                            r
                        });

                    thread_ctx.collect_scan_count(CMD, &statistics);
                    thread_ctx.collect_read_flow(ctx.get_region_id(), &statistics);

                    result
                })
                .then(move |r| {
                    _timer.observe_duration();
                    r
                })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Batch get from the snapshot.
    pub fn async_batch_get(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "batch_get";
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: E::Snap| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);

                    let mut statistics = Statistics::default();
                    let snap_store = SnapshotStore::new(
                        snapshot,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                    );
                    let kv_pairs: Vec<_> = snap_store
                        .batch_get(&keys, &mut statistics)
                        .into_iter()
                        .zip(keys)
                        .filter(|&(ref v, ref _k)| !(v.is_ok() && v.as_ref().unwrap().is_none()))
                        .map(|(v, k)| match v {
                            Ok(Some(x)) => Ok((k.into_raw().unwrap(), x)),
                            Err(e) => Err(Error::from(e)),
                            _ => unreachable!(),
                        })
                        .collect();

                    thread_ctx.collect_key_reads(CMD, kv_pairs.len() as u64);
                    thread_ctx.collect_scan_count(CMD, &statistics);
                    thread_ctx.collect_read_flow(ctx.get_region_id(), &statistics);

                    Ok(kv_pairs)
                })
                .then(move |r| {
                    _timer.observe_duration();
                    r
                })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    /// Scan a range starting with `start_key` up to `limit` rows from the snapshot.
    pub fn async_scan(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        start_ts: u64,
        options: Options,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "scan";
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: E::Snap| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);

                    let snap_store = SnapshotStore::new(
                        snapshot,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                    );

                    let mut scanner;
                    if !options.reverse_scan {
                        scanner =
                            snap_store.scanner(false, options.key_only, Some(start_key), end_key)?;
                    } else {
                        scanner =
                            snap_store.scanner(true, options.key_only, end_key, Some(start_key))?;
                    };
                    let res = scanner.scan(limit);

                    let statistics = scanner.take_statistics();
                    thread_ctx.collect_scan_count(CMD, &statistics);
                    thread_ctx.collect_read_flow(ctx.get_region_id(), &statistics);

                    res.map_err(Error::from).map(|results| {
                        thread_ctx.collect_key_reads(CMD, results.len() as u64);
                        results
                            .into_iter()
                            .map(|x| x.map_err(Error::from))
                            .collect()
                    })
                })
                .then(move |r| {
                    _timer.observe_duration();
                    r
                })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    pub fn async_pause(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        duration: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command::Pause {
            ctx,
            keys,
            duration,
        };
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        Ok(())
    }

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
            let size = m.key().as_encoded().len();
            if size > self.max_key_size {
                callback(Err(Error::KeyTooLarge(size, self.max_key_size)));
                return Ok(());
            }
        }
        let cmd = Command::Prewrite {
            ctx,
            mutations,
            primary,
            start_ts,
            options,
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Booleans(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_commit(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command::Commit {
            ctx,
            keys,
            lock_ts,
            commit_ts,
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_delete_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        let mut modifies = Vec::with_capacity(DATA_CFS.len());
        for cf in DATA_CFS {
            // We enable memtable prefix bloom for CF_WRITE column family, for delete_range
            // operation, RocksDB will add start key to the prefix bloom, and the start key
            // will go through function prefix_extractor. In our case the prefix_extractor
            // is FixedSuffixSliceTransform, which will trim the timestamp at the tail. If the
            // length of start key is less than 8, we will encounter index out of range error.
            let s = if *cf == CF_WRITE {
                start_key.clone().append_ts(u64::MAX)
            } else {
                start_key.clone()
            };
            modifies.push(Modify::DeleteRange(cf, s, end_key.clone()));
        }

        self.engine
            .async_write(&ctx, modifies, box |(_, res): (_, engine::Result<_>)| {
                callback(res.map_err(Error::from))
            })?;
        KV_COMMAND_COUNTER_VEC
            .with_label_values(&["delete_range"])
            .inc();
        Ok(())
    }

    pub fn async_cleanup(
        &self,
        ctx: Context,
        key: Key,
        start_ts: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command::Cleanup { ctx, key, start_ts };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_rollback(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command::Rollback {
            ctx,
            keys,
            start_ts,
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_scan_locks(
        &self,
        ctx: Context,
        max_ts: u64,
        start_key: Vec<u8>,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    ) -> Result<()> {
        let cmd = Command::ScanLock {
            ctx,
            max_ts,
            start_key: if start_key.is_empty() {
                None
            } else {
                Some(Key::from_raw(&start_key))
            },
            limit,
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Locks(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_resolve_lock(
        &self,
        ctx: Context,
        txn_status: HashMap<u64, u64>,
        callback: Callback<()>,
    ) -> Result<()> {
        let cmd = Command::ResolveLock {
            ctx,
            txn_status,
            scan_key: None,
            key_locks: vec![],
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        self.gc_worker.async_gc(ctx, safe_point, callback)?;
        KV_COMMAND_COUNTER_VEC
            .with_label_values(&[CMD_TAG_GC])
            .inc();
        Ok(())
    }

    pub fn async_unsafe_destroy_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        self.gc_worker
            .async_unsafe_destroy_range(ctx, start_key, end_key, callback)?;
        KV_COMMAND_COUNTER_VEC
            .with_label_values(&[CMD_TAG_UNSAFE_DESTROY_RANGE])
            .inc();
        Ok(())
    }

    pub fn async_raw_get(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
    ) -> impl Future<Item = Option<Vec<u8>>, Error = Error> {
        const CMD: &str = "raw_get";
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: E::Snap| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);
                    let cf = Self::rawkv_cf(&cf)?;
                    // no scan_count for this kind of op.

                    let key_len = key.len();
                    snapshot.get_cf(cf, &Key::from_encoded(key))
                        // map storage::engine::Error -> storage::Error
                        .map_err(Error::from)
                        .map(|r| {
                            if let Some(ref value) = r {
                                let mut stats = Statistics::default();
                                stats.data.flow_stats.read_keys = 1;
                                stats.data.flow_stats.read_bytes = key_len + value.len();
                                thread_ctx.collect_read_flow(ctx.get_region_id(), &stats);
                                thread_ctx.collect_key_reads(CMD, 1);
                            }
                            r
                        })
                })
                .then(move |r| {
                    _timer.observe_duration();
                    r
                })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    pub fn async_raw_batch_get(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "raw_batch_get";
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let keys: Vec<Key> = keys.into_iter().map(Key::from_encoded).collect();

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: E::Snap| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);
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
                    thread_ctx.collect_key_reads(CMD, stats.data.flow_stats.read_keys as u64);
                    thread_ctx.collect_read_flow(ctx.get_region_id(), &stats);
                    Ok(result)
                })
                .then(move |r| {
                    _timer.observe_duration();
                    r
                })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

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
            box |(_, res): (_, engine::Result<_>)| callback(res.map_err(Error::from)),
        )?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&["raw_put"]).inc();
        Ok(())
    }

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
        self.engine
            .async_write(&ctx, requests, box |(_, res): (_, engine::Result<_>)| {
                callback(res.map_err(Error::from))
            })?;
        KV_COMMAND_COUNTER_VEC
            .with_label_values(&["raw_batch_put"])
            .inc();
        Ok(())
    }

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
            box |(_, res): (_, engine::Result<_>)| callback(res.map_err(Error::from)),
        )?;
        KV_COMMAND_COUNTER_VEC
            .with_label_values(&["raw_delete"])
            .inc();
        Ok(())
    }

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

        self.engine.async_write(
            &ctx,
            vec![Modify::DeleteRange(
                Self::rawkv_cf(&cf)?,
                Key::from_encoded(start_key),
                Key::from_encoded(end_key),
            )],
            box |(_, res): (_, engine::Result<_>)| callback(res.map_err(Error::from)),
        )?;
        KV_COMMAND_COUNTER_VEC
            .with_label_values(&["raw_delete_range"])
            .inc();
        Ok(())
    }

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
        self.engine
            .async_write(&ctx, requests, box |(_, res): (_, engine::Result<_>)| {
                callback(res.map_err(Error::from))
            })?;
        KV_COMMAND_COUNTER_VEC
            .with_label_values(&["raw_batch_delete"])
            .inc();
        Ok(())
    }

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
            option.set_upper_bound(end.into_encoded());
        }
        let mut cursor = snapshot.iter_cf(Self::rawkv_cf(cf)?, option, ScanMode::Forward)?;
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        while cursor.valid() && pairs.len() < limit {
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
            option.set_lower_bound(end.into_encoded());
        }
        let mut cursor = snapshot.iter_cf(Self::rawkv_cf(cf)?, option, ScanMode::Backward)?;
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.reverse_seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        while cursor.valid() && pairs.len() < limit {
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
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: E::Snap| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);

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
                        ).map_err(Error::from)
                    } else {
                        Self::raw_scan(
                            &snapshot,
                            &cf,
                            &Key::from_encoded(key),
                            end_key,
                            limit,
                            &mut statistics,
                            key_only,
                        ).map_err(Error::from)
                    };

                    thread_ctx.collect_read_flow(ctx.get_region_id(), &statistics);
                    thread_ctx.collect_key_reads(CMD, statistics.write.flow_stats.read_keys as u64);
                    thread_ctx.collect_scan_count(CMD, &statistics);

                    result
                })
                .then(move |r| {
                    _timer.observe_duration();
                    r
                })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

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
    /// - if reverse, endKey is less than startKey. endKey is lowerBound.
    /// - if not reverse, endKey is greater than startKey. endKey is upperBound.
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
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: E::Snap| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);

                    let mut statistics = Statistics::default();
                    if !Self::check_key_ranges(&ranges, reverse) {
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
                        let pairs = if reverse {
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
                            Self::raw_scan(
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

                    thread_ctx.collect_read_flow(ctx.get_region_id(), &statistics);
                    thread_ctx.collect_key_reads(CMD, statistics.write.flow_stats.read_keys as u64);
                    thread_ctx.collect_scan_count(CMD, &statistics);

                    Ok(result)
                })
                .then(move |r| {
                    _timer.observe_duration();
                    r
                })
        });

        future::result(res)
            .map_err(|_| Error::SchedTooBusy)
            .flatten()
    }

    pub fn async_mvcc_by_key(
        &self,
        ctx: Context,
        key: Key,
        callback: Callback<MvccInfo>,
    ) -> Result<()> {
        let cmd = Command::MvccByKey { ctx, key };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::MvccInfoByKey(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_mvcc_by_start_ts(
        &self,
        ctx: Context,
        start_ts: u64,
        callback: Callback<Option<(Key, MvccInfo)>>,
    ) -> Result<()> {
        let cmd = Command::MvccByStartTs { ctx, start_ts };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::MvccInfoByStartTs(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: EngineError) {
            from()
            cause(err)
            description(err.description())
        }
        Txn(err: txn::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Mvcc(err: mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Closed {
            description("storage is closed.")
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        SchedTooBusy {
            description("scheduler is too busy")
        }
        GCWorkerTooBusy {
            description("gc worker is too busy")
        }
        KeyTooLarge(size: usize, limit: usize) {
            description("max key size exceeded")
            display("max key size exceeded, size: {}, limit: {}", size, limit)
        }
        InvalidCf (cf_name: String) {
            description("invalid cf name")
            display("invalid cf name: {}", cf_name)
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

pub enum ErrorHeaderKind {
    NotLeader,
    RegionNotFound,
    KeyNotInRegion,
    StaleEpoch,
    ServerIsBusy,
    StaleCommand,
    StoreNotMatch,
    RaftEntryTooLarge,
    Other,
}

impl ErrorHeaderKind {
    /// TODO: This function is only used for bridging existing & legacy metric tags.
    /// It should be removed once Coprocessor starts using new static metrics.
    pub fn get_str(&self) -> &'static str {
        match *self {
            ErrorHeaderKind::NotLeader => "not_leader",
            ErrorHeaderKind::RegionNotFound => "region_not_found",
            ErrorHeaderKind::KeyNotInRegion => "key_not_in_region",
            ErrorHeaderKind::StaleEpoch => "stale_epoch",
            ErrorHeaderKind::ServerIsBusy => "server_is_busy",
            ErrorHeaderKind::StaleCommand => "stale_command",
            ErrorHeaderKind::StoreNotMatch => "store_not_match",
            ErrorHeaderKind::RaftEntryTooLarge => "raft_entry_too_large",
            ErrorHeaderKind::Other => "other",
        }
    }
}

impl Display for ErrorHeaderKind {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.get_str())
    }
}

pub fn get_error_kind_from_header(header: &errorpb::Error) -> ErrorHeaderKind {
    if header.has_not_leader() {
        ErrorHeaderKind::NotLeader
    } else if header.has_region_not_found() {
        ErrorHeaderKind::RegionNotFound
    } else if header.has_key_not_in_region() {
        ErrorHeaderKind::KeyNotInRegion
    } else if header.has_stale_epoch() {
        ErrorHeaderKind::StaleEpoch
    } else if header.has_server_is_busy() {
        ErrorHeaderKind::ServerIsBusy
    } else if header.has_stale_command() {
        ErrorHeaderKind::StaleCommand
    } else if header.has_store_not_match() {
        ErrorHeaderKind::StoreNotMatch
    } else if header.has_raft_entry_too_large() {
        ErrorHeaderKind::RaftEntryTooLarge
    } else {
        ErrorHeaderKind::Other
    }
}

pub fn get_tag_from_header(header: &errorpb::Error) -> &'static str {
    get_error_kind_from_header(header).get_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::kvrpcpb::{Context, LockInfo};
    use std::sync::mpsc::{channel, Sender};
    use util::config::ReadableSize;

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
                .async_get(Context::new(), Key::from_raw(b"x"), 100)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::new(),
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
                .async_get(Context::new(), Key::from_raw(b"x"), 101)
                .wait(),
        );
        storage
            .async_commit(
                Context::new(),
                vec![Key::from_raw(b"x")],
                100,
                101,
                expect_ok_callback(tx.clone(), 3),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::new(), Key::from_raw(b"x"), 100)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), Key::from_raw(b"x"), 101)
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
                Context::new(),
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
                        ()
                    }
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Other(..)))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .async_get(Context::new(), Key::from_raw(b"x"), 1)
                .wait(),
        );
        expect_error(
            |e| match e {
                Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Request(..)))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .async_scan(
                    Context::new(),
                    Key::from_raw(b"x"),
                    None,
                    1000,
                    1,
                    Options::default(),
                )
                .wait(),
        );
        expect_multi_values(
            vec![None, None],
            storage
                .async_batch_get(
                    Context::new(),
                    vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                    1,
                )
                .wait(),
        );
    }

    #[test]
    fn test_scan() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                Context::new(),
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
                    Context::new(),
                    vec![Key::from_raw(b"c"), Key::from_raw(b"d")],
                    2,
                )
                .wait(),
        );
        storage
            .async_commit(
                Context::new(),
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
                    Context::new(),
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
    fn test_txn() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((Key::from_raw(b"x"), b"100".to_vec()))],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        storage
            .async_prewrite(
                Context::new(),
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
                Context::new(),
                vec![Key::from_raw(b"x")],
                100,
                110,
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        storage
            .async_commit(
                Context::new(),
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
                .async_get(Context::new(), Key::from_raw(b"x"), 120)
                .wait(),
        );
        expect_value(
            b"101".to_vec(),
            storage
                .async_get(Context::new(), Key::from_raw(b"y"), 120)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::new(),
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
                .async_get(Context::new(), Key::from_raw(b"x"), 100)
                .wait(),
        );
        storage
            .async_pause(
                Context::new(),
                vec![Key::from_raw(b"x")],
                1000,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        storage
            .async_prewrite(
                Context::new(),
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
                Context::new(),
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
                Context::new(),
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
                Context::new(),
                Key::from_raw(b"x"),
                100,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::new(), Key::from_raw(b"x"), 105)
                .wait(),
        );
    }

    #[test]
    fn test_high_priority_get_put() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        expect_none(storage.async_get(ctx, Key::from_raw(b"x"), 100).wait());
        let mut ctx = Context::new();
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
        let mut ctx = Context::new();
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
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        expect_none(storage.async_get(ctx, Key::from_raw(b"x"), 100).wait());
        let mut ctx = Context::new();
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
                .async_get(Context::new(), Key::from_raw(b"x"), 100)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::new(),
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
                Context::new(),
                vec![Key::from_raw(b"x")],
                100,
                101,
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .async_pause(
                Context::new(),
                vec![],
                1000,
                expect_ok_callback(tx.clone(), 3),
            )
            .unwrap();
        let mut ctx = Context::new();
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
                Context::new(),
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
                Context::new(),
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
                .async_get(Context::new(), Key::from_raw(b"x"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), Key::from_raw(b"y"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), Key::from_raw(b"z"), 101)
                .wait(),
        );

        // Delete range [x, z)
        storage
            .async_delete_range(
                Context::new(),
                Key::from_raw(b"x"),
                Key::from_raw(b"z"),
                expect_ok_callback(tx.clone(), 5),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::new(), Key::from_raw(b"x"), 101)
                .wait(),
        );
        expect_none(
            storage
                .async_get(Context::new(), Key::from_raw(b"y"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), Key::from_raw(b"z"), 101)
                .wait(),
        );

        storage
            .async_delete_range(
                Context::new(),
                Key::from_raw(b""),
                Key::from_raw(&[255]),
                expect_ok_callback(tx.clone(), 9),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::new(), Key::from_raw(b"z"), 101)
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
                    Context::new(),
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
                .async_raw_get(Context::new(), "".to_string(), b"d".to_vec())
                .wait(),
        );

        // Delete ["d", "e")
        storage
            .async_raw_delete_range(
                Context::new(),
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
                .async_raw_get(Context::new(), "".to_string(), b"c".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .async_raw_get(Context::new(), "".to_string(), b"d".to_vec())
                .wait(),
        );
        expect_value(
            b"005".to_vec(),
            storage
                .async_raw_get(Context::new(), "".to_string(), b"e".to_vec())
                .wait(),
        );

        // Delete ["aa", "ab")
        storage
            .async_raw_delete_range(
                Context::new(),
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
                .async_raw_get(Context::new(), "".to_string(), b"a".to_vec())
                .wait(),
        );
        expect_value(
            b"002".to_vec(),
            storage
                .async_raw_get(Context::new(), "".to_string(), b"b".to_vec())
                .wait(),
        );

        // Delete all
        storage
            .async_raw_delete_range(
                Context::new(),
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
                    .async_raw_get(Context::new(), "".to_string(), kv.0.to_vec())
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
                Context::new(),
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
                    .async_raw_get(Context::new(), "".to_string(), key)
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
                    Context::new(),
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
                .async_raw_batch_get(Context::new(), "".to_string(), keys)
                .wait(),
        );
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
                Context::new(),
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
                .async_raw_batch_get(Context::new(), "".to_string(), keys)
                .wait(),
        );

        // Delete ["b", "d"]
        storage
            .async_raw_batch_delete(
                Context::new(),
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
                .async_raw_get(Context::new(), "".to_string(), b"a".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .async_raw_get(Context::new(), "".to_string(), b"b".to_vec())
                .wait(),
        );
        expect_value(
            b"cc".to_vec(),
            storage
                .async_raw_get(Context::new(), "".to_string(), b"c".to_vec())
                .wait(),
        );
        expect_none(
            storage
                .async_raw_get(Context::new(), "".to_string(), b"d".to_vec())
                .wait(),
        );
        expect_value(
            b"ee".to_vec(),
            storage
                .async_raw_get(Context::new(), "".to_string(), b"e".to_vec())
                .wait(),
        );

        // Delete ["a", "c", "e"]
        storage
            .async_raw_batch_delete(
                Context::new(),
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
                    .async_raw_get(Context::new(), "".to_string(), k)
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
                Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
        let ctx = Context::new();
        let results = vec![
            (b"c1".to_vec(), b"cc11".to_vec()),
            (b"c2".to_vec(), b"cc22".to_vec()),
            (b"c3".to_vec(), b"cc33".to_vec()),
            (b"d".to_vec(), b"dd".to_vec()),
            (b"d1".to_vec(), b"dd11".to_vec()),
            (b"d2".to_vec(), b"dd22".to_vec()),
        ].into_iter()
            .map(|(k, v)| Some((k, v)));
        expect_multi_values(
            results.clone().collect(),
            <Storage<RocksEngine>>::async_snapshot(storage.get_engine(), &ctx)
                .and_then(move |snapshot| {
                    <Storage<RocksEngine>>::raw_scan(
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
            <Storage<RocksEngine>>::async_snapshot(storage.get_engine(), &ctx)
                .and_then(move |snapshot| {
                    <Storage<RocksEngine>>::reverse_raw_scan(
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
                    let mut range = KeyRange::new();
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
            <Storage<RocksEngine>>::check_key_ranges(&ranges, false),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"c".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine>>::check_key_ranges(&ranges, false),
            true
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine>>::check_key_ranges(&ranges, false),
            false
        );

        // if end_key is omitted, the next start_key is used instead. so, false is returned.
        let ranges = make_ranges(vec![
            (b"c".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"a".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine>>::check_key_ranges(&ranges, false),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine>>::check_key_ranges(&ranges, true),
            true
        );

        let ranges = make_ranges(vec![
            (b"c3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"a3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine>>::check_key_ranges(&ranges, true),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        assert_eq!(
            <Storage<RocksEngine>>::check_key_ranges(&ranges, true),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"c3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<RocksEngine>>::check_key_ranges(&ranges, true),
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
                Context::new(),
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
                .async_raw_batch_get(Context::new(), "".to_string(), keys)
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
                let mut range = KeyRange::new();
                range.set_start_key(k);
                range
            })
            .collect();
        expect_multi_values(
            results,
            storage
                .async_raw_batch_scan(
                    Context::new(),
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
                    Context::new(),
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
                    Context::new(),
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
                .async_raw_batch_scan(Context::new(), "".to_string(), ranges, 3, true, false)
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
        ].into_iter()
            .map(|(s, e)| {
                let mut range = KeyRange::new();
                range.set_start_key(s);
                range.set_end_key(e);
                range
            })
            .collect();
        expect_multi_values(
            results,
            storage
                .async_raw_batch_scan(Context::new(), "".to_string(), ranges, 5, false, true)
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
                let mut range = KeyRange::new();
                range.set_start_key(s);
                range
            })
            .collect();
        expect_multi_values(
            results,
            storage
                .async_raw_batch_scan(Context::new(), "".to_string(), ranges, 2, false, true)
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
        ].into_iter()
            .map(|(s, e)| {
                let mut range = KeyRange::new();
                range.set_start_key(s);
                range.set_end_key(e);
                range
            })
            .collect();
        expect_multi_values(
            results,
            storage
                .async_raw_batch_scan(Context::new(), "".to_string(), ranges, 5, true, true)
                .wait(),
        );
    }

    #[test]
    fn test_scan_lock() {
        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::new(),
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
        storage
            .async_prewrite(
                Context::new(),
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
        let (lock_a, lock_b, lock_c, lock_x, lock_y, lock_z) = (
            {
                let mut lock = LockInfo::new();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"a".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::new();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"b".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::new();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(101);
                lock.set_key(b"c".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::new();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"x".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::new();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"y".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::new();
                lock.set_primary_lock(b"x".to_vec());
                lock.set_lock_version(100);
                lock.set_key(b"z".to_vec());
                lock
            },
        );
        storage
            .async_scan_locks(
                Context::new(),
                99,
                vec![],
                10,
                expect_value_callback(tx.clone(), 0, vec![]),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_scan_locks(
                Context::new(),
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
                Context::new(),
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
                Context::new(),
                100,
                b"y".to_vec(),
                10,
                expect_value_callback(tx.clone(), 0, vec![lock_y.clone(), lock_z.clone()]),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_scan_locks(
                Context::new(),
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
                Context::new(),
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
                Context::new(),
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
                Context::new(),
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
        use storage::txn::RESOLVE_LOCK_BATCH_SIZE;

        let storage = TestStorageBuilder::new().build().unwrap();
        let (tx, rx) = channel();

        // These locks (transaction ts=99) are not going to be resolved.
        storage
            .async_prewrite(
                Context::new(),
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
                let mut lock = LockInfo::new();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"a".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::new();
                lock.set_primary_lock(b"c".to_vec());
                lock.set_lock_version(99);
                lock.set_key(b"b".to_vec());
                lock
            },
            {
                let mut lock = LockInfo::new();
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
                        Context::new(),
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
                        Context::new(),
                        txn_status,
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                // All locks should be resolved except for a, b and c.
                storage
                    .async_scan_locks(
                        Context::new(),
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
}
