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

use std::boxed::FnBox;
use std::sync::{Arc, Mutex};
use std::fmt::{self, Debug, Display, Formatter};
use std::error;
use std::io::Error as IoError;
use std::u64;
use std::cmp;
use kvproto::kvrpcpb::{CommandPri, Context, LockInfo};
use kvproto::errorpb;
use util::collections::HashMap;
use futures::{future, Future};
use server::readpool::{self, ReadPool};
use self::metrics::*;
use self::mvcc::Lock;
use self::txn::CMD_BATCH_SIZE;
use util;
use util::worker::{self, Builder, Worker};
use raftstore::store::engine::IterOption;

pub mod engine;
pub mod mvcc;
pub mod txn;
pub mod config;
pub mod types;
mod metrics;
mod readpool_context;

pub use self::config::{Config, DEFAULT_DATA_DIR, DEFAULT_ROCKSDB_SUB_DIR};
pub use self::engine::{new_local_engine, CFStatistics, Cursor, Engine, Error as EngineError,
                       FlowStatistics, Iterator, Modify, ScanMode, Snapshot, Statistics,
                       StatisticsSummary, TEMP_DIR};
pub use self::engine::raftkv::RaftKv;
pub use self::txn::{Msg, Scheduler, SnapshotStore, StoreScanner};
pub use self::types::{make_key, Key, KvPair, MvccInfo, Value};
pub use self::readpool_context::Context as ReadPoolContext;
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

#[allow(match_same_arms)]
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
    SingleValue(Callback<Option<Value>>),
    KvPairs(Callback<Vec<Result<KvPair>>>),
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
    Gc {
        ctx: Context,
        safe_point: u64,
        ratio_threshold: f64,
        scan_key: Option<Key>,
        keys: Vec<Key>,
    },
    DeleteRange {
        ctx: Context,
        start_key: Key,
        end_key: Key,
    },
    Pause {
        ctx: Context,
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
            Command::Gc {
                ref ctx,
                safe_point,
                ref scan_key,
                ..
            } => write!(
                f,
                "kv::command::gc scan {:?} @ {} | {:?}",
                scan_key, safe_point, ctx
            ),
            Command::DeleteRange {
                ref ctx,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "kv::command::delete range [{:?}, {:?}) | {:?}",
                start_key, end_key, ctx
            ),
            Command::Pause { ref ctx, duration } => {
                write!(f, "kv::command::pause {} ms | {:?}", duration, ctx)
            }
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

impl Command {
    pub fn readonly(&self) -> bool {
        match *self {
            Command::ScanLock { .. } |
            // DeleteRange only called by DDL bg thread after table is dropped and
            // must guarantee that there is no other read or write on these keys, so
            // we can treat DeleteRange as readonly Command.
            Command::DeleteRange { .. } |
            Command::Pause { .. } |
            Command::MvccByKey { .. } |
            Command::MvccByStartTs { .. } => true,
            Command::ResolveLock { ref key_locks, .. } => key_locks.is_empty(),
            Command::Gc { ref keys, .. } => keys.is_empty(),
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
            Command::Gc { .. } => CMD_TAG_GC,
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
            Command::Gc { safe_point, .. } => safe_point,
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
            | Command::Gc { ref ctx, .. }
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
            | Command::Gc { ref mut ctx, .. }
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
                        bytes += key.encoded().len();
                        bytes += value.len();
                    }
                    Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                        bytes += key.encoded().len();
                    }
                }
            },
            Command::Commit { ref keys, .. } | Command::Rollback { ref keys, .. } => {
                for key in keys {
                    bytes += key.encoded().len();
                }
            }
            Command::ResolveLock { ref key_locks, .. } => for lock in key_locks {
                bytes += lock.0.encoded().len();
            },
            Command::Cleanup { ref key, .. } => {
                bytes += key.encoded().len();
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
}

impl Options {
    pub fn new(lock_ttl: u64, skip_constraint_check: bool, key_only: bool) -> Options {
        Options {
            lock_ttl: lock_ttl,
            skip_constraint_check: skip_constraint_check,
            key_only: key_only,
        }
    }
}

pub struct Storage {
    engine: Box<Engine>,

    // to schedule the execution of storage commands
    worker: Arc<Mutex<Worker<Msg>>>,
    worker_scheduler: worker::Scheduler<Msg>,

    read_pool: ReadPool<ReadPoolContext>,

    // Storage configurations.
    gc_ratio_threshold: f64,
    max_key_size: usize,
}

impl Storage {
    pub fn from_engine(
        engine: Box<Engine>,
        config: &Config,
        read_pool: ReadPool<ReadPoolContext>,
    ) -> Result<Storage> {
        info!("storage {:?} started.", engine);

        let worker = Arc::new(Mutex::new(
            Builder::new("storage-scheduler")
                .batch_size(CMD_BATCH_SIZE)
                .pending_capacity(config.scheduler_notify_capacity)
                .create(),
        ));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        Ok(Storage {
            read_pool,
            engine: engine,
            worker: worker,
            worker_scheduler: worker_scheduler,
            gc_ratio_threshold: config.gc_ratio_threshold,
            max_key_size: config.max_key_size,
        })
    }

    pub fn new(config: &Config, read_pool: ReadPool<ReadPoolContext>) -> Result<Storage> {
        let engine = engine::new_local_engine(&config.data_dir, ALL_CFS)?;
        Storage::from_engine(engine, config, read_pool)
    }

    pub fn start(&mut self, config: &Config) -> Result<()> {
        let sched_concurrency = config.scheduler_concurrency;
        let sched_worker_pool_size = config.scheduler_worker_pool_size;
        let sched_pending_write_threshold = config.scheduler_pending_write_threshold.0 as usize;
        let mut worker = self.worker.lock().unwrap();
        let scheduler = Scheduler::new(
            self.engine.clone(),
            worker.scheduler(),
            sched_concurrency,
            sched_worker_pool_size,
            sched_pending_write_threshold,
        );
        worker.start(scheduler)?;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let mut worker = self.worker.lock().unwrap();
        if let Err(e) = worker.schedule(Msg::Quit) {
            error!("send quit cmd to scheduler failed, error:{:?}", e);
            return Err(box_err!("failed to ask sched to quit: {:?}", e));
        }

        let h = worker.stop().unwrap();
        if let Err(e) = h.join() {
            return Err(box_err!("failed to join sched_handle, err:{:?}", e));
        }

        info!("storage {:?} closed.", self.engine);
        Ok(())
    }

    pub fn get_engine(&self) -> Box<Engine> {
        self.engine.clone()
    }

    fn schedule(&self, cmd: Command, cb: StorageCb) -> Result<()> {
        fail_point!("storage_drop_message", |_| Ok(()));
        box_try!(
            self.worker_scheduler
                .schedule(Msg::RawCmd { cmd: cmd, cb: cb })
        );
        Ok(())
    }

    fn async_snapshot(
        engine: Box<Engine>,
        ctx: &Context,
    ) -> impl Future<Item = Box<Snapshot + 'static>, Error = Error> {
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
                thread_ctx.start_command_duration_timer(CMD, priority, false)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: Box<Snapshot>| {
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
                thread_ctx.start_command_duration_timer(CMD, priority, false)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: Box<Snapshot>| {
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
                        .batch_get(&keys, &mut statistics)
                        // map storage::txn::Error -> storage::Error
                        .map_err(Error::from)
                        .map(|results| results
                            .into_iter()
                            .zip(keys)
                            .filter(|&(ref v, ref _k)|
                                !(v.is_ok() && v.as_ref().unwrap().is_none())
                            )
                            .map(|(v, k)| match v {
                                Ok(Some(x)) => Ok((k.raw().unwrap(), x)),
                                Err(e) => Err(Error::from(e)),
                                _ => unreachable!(),
                            })
                            .collect()
                        )
                        .map(|r: Vec<Result<KvPair>>| {
                            thread_ctx.collect_key_reads(CMD, r.len() as u64);
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

    /// Scan a range starting with `start_key` up to `limit` rows from the snapshot.
    pub fn async_scan(
        &self,
        ctx: Context,
        start_key: Key,
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
                thread_ctx.start_command_duration_timer(CMD, priority, false)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: Box<Snapshot>| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);

                    let snap_store = SnapshotStore::new(
                        snapshot,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                    );
                    snap_store
                        .scanner(ScanMode::Forward, options.key_only, None, None)
                        .and_then(|mut scanner| {
                            let res = scanner.scan(start_key, limit);
                            let statistics = scanner.get_statistics();
                            thread_ctx.collect_scan_count(CMD, statistics);
                            thread_ctx.collect_read_flow(ctx.get_region_id(), statistics);
                            res
                        })
                        .map_err(Error::from)
                        .map(|results| {
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

    pub fn async_pause(&self, ctx: Context, duration: u64, callback: Callback<()>) -> Result<()> {
        let cmd = Command::Pause {
            ctx: ctx,
            duration: duration,
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
            let size = m.key().encoded().len();
            if size > self.max_key_size {
                callback(Err(Error::KeyTooLarge(size, self.max_key_size)));
                return Ok(());
            }
        }
        let cmd = Command::Prewrite {
            ctx: ctx,
            mutations: mutations,
            primary: primary,
            start_ts: start_ts,
            options: options,
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
            ctx: ctx,
            keys: keys,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
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
                start_key.append_ts(u64::MAX)
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
        let cmd = Command::Cleanup {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
        };
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
            ctx: ctx,
            keys: keys,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_scan_lock(
        &self,
        ctx: Context,
        max_ts: u64,
        start_key: Vec<u8>,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    ) -> Result<()> {
        let cmd = Command::ScanLock {
            ctx: ctx,
            max_ts: max_ts,
            start_key: if start_key.is_empty() {
                None
            } else {
                Some(Key::from_raw(&start_key))
            },
            limit: limit,
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
            ctx: ctx,
            txn_status: txn_status,
            scan_key: None,
            key_locks: vec![],
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        let cmd = Command::Gc {
            ctx: ctx,
            safe_point: safe_point,
            ratio_threshold: self.gc_ratio_threshold,
            scan_key: None,
            keys: vec![],
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::Boolean(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_raw_get(
        &self,
        ctx: Context,
        key: Vec<u8>,
    ) -> impl Future<Item = Option<Vec<u8>>, Error = Error> {
        const CMD: &str = "raw_get";
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority, true)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: Box<Snapshot>| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);

                    // no scan_count for this kind of op.

                    let key_len = key.len();
                    snapshot.get(&Key::from_encoded(key))
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

    pub fn async_raw_put(
        &self,
        ctx: Context,
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
            vec![Modify::Put(CF_DEFAULT, Key::from_encoded(key), value)],
            box |(_, res): (_, engine::Result<_>)| callback(res.map_err(Error::from)),
        )?;
        RAWKV_COMMAND_COUNTER_VEC
            .with_label_values(&["raw_put"])
            .inc();
        Ok(())
    }

    pub fn async_raw_delete(
        &self,
        ctx: Context,
        key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        if key.len() > self.max_key_size {
            callback(Err(Error::KeyTooLarge(key.len(), self.max_key_size)));
            return Ok(());
        }
        self.engine.async_write(
            &ctx,
            vec![Modify::Delete(CF_DEFAULT, Key::from_encoded(key))],
            box |(_, res): (_, engine::Result<_>)| callback(res.map_err(Error::from)),
        )?;
        RAWKV_COMMAND_COUNTER_VEC
            .with_label_values(&["raw_delete"])
            .inc();
        Ok(())
    }

    pub fn async_raw_delete_range(
        &self,
        ctx: Context,
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
            vec![
                Modify::DeleteRange(
                    CF_DEFAULT,
                    Key::from_encoded(start_key),
                    Key::from_encoded(end_key),
                ),
            ],
            box |(_, res): (_, engine::Result<_>)| callback(res.map_err(Error::from)),
        )?;
        RAWKV_COMMAND_COUNTER_VEC
            .with_label_values(&["raw_delete_range"])
            .inc();
        Ok(())
    }

    fn raw_scan(
        snapshot: Box<Snapshot>,
        start_key: &Key,
        limit: usize,
        stats: &mut Statistics,
    ) -> engine::Result<Vec<Result<KvPair>>> {
        let mut cursor = snapshot.iter(IterOption::default(), ScanMode::Forward)?;
        if !cursor.seek(start_key, &mut stats.data)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        while cursor.valid() && pairs.len() < limit {
            pairs.push(Ok((cursor.key().to_owned(), cursor.value().to_owned())));
            cursor.next(&mut stats.data);
        }
        Ok(pairs)
    }

    pub fn async_raw_scan(
        &self,
        ctx: Context,
        key: Vec<u8>,
        limit: usize,
    ) -> impl Future<Item = Vec<Result<KvPair>>, Error = Error> {
        const CMD: &str = "raw_scan";
        let engine = self.get_engine();
        let priority = readpool::Priority::from(ctx.get_priority());

        let res = self.read_pool.future_execute(priority, move |ctxd| {
            let mut _timer = {
                let ctxd = ctxd.clone();
                let mut thread_ctx = ctxd.current_thread_context_mut();
                thread_ctx.start_command_duration_timer(CMD, priority, true)
            };

            Self::async_snapshot(engine, &ctx)
                .and_then(move |snapshot: Box<Snapshot>| {
                    let mut thread_ctx = ctxd.current_thread_context_mut();
                    let _t_process = thread_ctx.start_processing_read_duration_timer(CMD);

                    let mut statistics = Statistics::default();
                    let result = Storage::raw_scan(
                        snapshot,
                        &Key::from_encoded(key),
                        limit,
                        &mut statistics,
                    ).map_err(Error::from)
                        .map(|r| {
                            let mut valid_keys = 0;
                            let mut bytes_read = 0;
                            let mut stats = Statistics::default();
                            r.iter().for_each(|r| {
                                if let Ok(ref pair) = *r {
                                    valid_keys += 1;
                                    bytes_read += pair.0.len() + pair.1.len();
                                }
                            });
                            stats.data.flow_stats.read_keys = valid_keys;
                            stats.data.flow_stats.read_bytes = bytes_read;
                            thread_ctx.collect_read_flow(ctx.get_region_id(), &stats);
                            thread_ctx.collect_key_reads(CMD, valid_keys as u64);
                            r
                        });

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

    pub fn async_mvcc_by_key(
        &self,
        ctx: Context,
        key: Key,
        callback: Callback<MvccInfo>,
    ) -> Result<()> {
        let cmd = Command::MvccByKey { ctx: ctx, key: key };
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
        let cmd = Command::MvccByStartTs {
            ctx: ctx,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        self.schedule(cmd, StorageCb::MvccInfoByStartTs(callback))?;
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }
}

impl Clone for Storage {
    fn clone(&self) -> Storage {
        Storage {
            read_pool: self.read_pool.clone(),
            engine: self.engine.clone(),
            worker: Arc::clone(&self.worker),
            worker_scheduler: self.worker_scheduler.clone(),
            gc_ratio_threshold: self.gc_ratio_threshold,
            max_key_size: self.max_key_size,
        }
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
        KeyTooLarge(size: usize, limit: usize) {
            description("max key size exceeded")
            display("max key size exceeded, size: {}, limit: {}", size, limit)
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

pub fn get_tag_from_header(header: &errorpb::Error) -> &'static str {
    if header.has_not_leader() {
        "not_leader"
    } else if header.has_region_not_found() {
        "region_not_found"
    } else if header.has_key_not_in_region() {
        "key_not_in_region"
    } else if header.has_stale_epoch() {
        "stale_epoch"
    } else if header.has_server_is_busy() {
        "server_is_busy"
    } else if header.has_stale_command() {
        "stale_command"
    } else if header.has_store_not_match() {
        "store_not_match"
    } else if header.has_raft_entry_too_large() {
        "raft_entry_too_large"
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{channel, Sender};
    use kvproto::kvrpcpb::Context;
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

    fn expect_ok_callback<T>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_ok());
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

    fn new_read_pool() -> ReadPool<ReadPoolContext> {
        ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        })
    }

    #[test]
    fn test_get_put() {
        let read_pool = new_read_pool();
        let config = Config::default();
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        expect_none(
            storage
                .async_get(Context::new(), make_key(b"x"), 100)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
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
                .async_get(Context::new(), make_key(b"x"), 101)
                .wait(),
        );
        storage
            .async_commit(
                Context::new(),
                vec![make_key(b"x")],
                100,
                101,
                expect_ok_callback(tx.clone(), 3),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::new(), make_key(b"x"), 100)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), make_key(b"x"), 101)
                .wait(),
        );
        storage.stop().unwrap();
    }

    #[test]
    fn test_cf_error() {
        let read_pool = new_read_pool();
        let config = Config::default();
        // New engine lacks normal column families.
        let engine = engine::new_local_engine(&config.data_dir, &["foo"]).unwrap();
        let mut storage = Storage::from_engine(engine, &config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::new(),
                vec![
                    Mutation::Put((make_key(b"a"), b"aa".to_vec())),
                    Mutation::Put((make_key(b"b"), b"bb".to_vec())),
                    Mutation::Put((make_key(b"c"), b"cc".to_vec())),
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
                Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Request(..)))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage.async_get(Context::new(), make_key(b"x"), 1).wait(),
        );
        expect_error(
            |e| match e {
                Error::Txn(txn::Error::Mvcc(mvcc::Error::Engine(EngineError::Request(..)))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            storage
                .async_scan(Context::new(), make_key(b"x"), 1000, 1, Options::default())
                .wait(),
        );
        expect_multi_values(
            vec![None, None],
            storage
                .async_batch_get(Context::new(), vec![make_key(b"c"), make_key(b"d")], 1)
                .wait(),
        );
        storage.stop().unwrap();
    }

    #[test]
    fn test_scan() {
        let read_pool = new_read_pool();
        let config = Config::default();
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::new(),
                vec![
                    Mutation::Put((make_key(b"a"), b"aa".to_vec())),
                    Mutation::Put((make_key(b"b"), b"bb".to_vec())),
                    Mutation::Put((make_key(b"c"), b"cc".to_vec())),
                ],
                b"a".to_vec(),
                1,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![None, None, None],
            storage
                .async_scan(
                    Context::new(),
                    make_key(b"\x00"),
                    1000,
                    5,
                    Options::default(),
                )
                .wait(),
        );
        storage
            .async_commit(
                Context::new(),
                vec![make_key(b"a"), make_key(b"b"), make_key(b"c")],
                1,
                2,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            storage
                .async_scan(
                    Context::new(),
                    make_key(b"\x00"),
                    1000,
                    5,
                    Options::default(),
                )
                .wait(),
        );
        storage.stop().unwrap();
    }

    #[test]
    fn test_batch_get() {
        let read_pool = new_read_pool();
        let config = Config::default();
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::new(),
                vec![
                    Mutation::Put((make_key(b"a"), b"aa".to_vec())),
                    Mutation::Put((make_key(b"b"), b"bb".to_vec())),
                    Mutation::Put((make_key(b"c"), b"cc".to_vec())),
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
                .async_batch_get(Context::new(), vec![make_key(b"c"), make_key(b"d")], 2)
                .wait(),
        );
        storage
            .async_commit(
                Context::new(),
                vec![make_key(b"a"), make_key(b"b"), make_key(b"c")],
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
                        make_key(b"c"),
                        make_key(b"x"),
                        make_key(b"a"),
                        make_key(b"b"),
                    ],
                    5,
                )
                .wait(),
        );
        storage.stop().unwrap();
    }

    #[test]
    fn test_txn() {
        let read_pool = new_read_pool();
        let config = Config::default();
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((make_key(b"y"), b"101".to_vec()))],
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
                vec![make_key(b"x")],
                100,
                110,
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        storage
            .async_commit(
                Context::new(),
                vec![make_key(b"y")],
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
                .async_get(Context::new(), make_key(b"x"), 120)
                .wait(),
        );
        expect_value(
            b"101".to_vec(),
            storage
                .async_get(Context::new(), make_key(b"y"), 120)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((make_key(b"x"), b"105".to_vec()))],
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
        storage.stop().unwrap();
    }

    #[test]
    fn test_sched_too_busy() {
        let read_pool = new_read_pool();
        let mut config = Config::default();
        config.scheduler_pending_write_threshold = ReadableSize(1);
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        expect_none(
            storage
                .async_get(Context::new(), make_key(b"x"), 100)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                b"x".to_vec(),
                100,
                Options::default(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((make_key(b"y"), b"101".to_vec()))],
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
                vec![Mutation::Put((make_key(b"z"), b"102".to_vec()))],
                b"y".to_vec(),
                102,
                Options::default(),
                expect_ok_callback(tx.clone(), 3),
            )
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let read_pool = new_read_pool();
        let config = Config::default();
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
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
                make_key(b"x"),
                100,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::new(), make_key(b"x"), 105)
                .wait(),
        );
        storage.stop().unwrap();
    }

    #[test]
    fn test_high_priority_get_put() {
        let read_pool = new_read_pool();
        let config = Config::default();
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        expect_none(storage.async_get(ctx, make_key(b"x"), 100).wait());
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        storage
            .async_prewrite(
                ctx,
                vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
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
                vec![make_key(b"x")],
                100,
                101,
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        expect_none(storage.async_get(ctx, make_key(b"x"), 100).wait());
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            storage.async_get(ctx, make_key(b"x"), 101).wait(),
        );
        storage.stop().unwrap();
    }

    #[test]
    fn test_high_priority_no_block() {
        let read_pool = new_read_pool();
        let mut config = Config::default();
        config.scheduler_worker_pool_size = 1;
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        expect_none(
            storage
                .async_get(Context::new(), make_key(b"x"), 100)
                .wait(),
        );
        storage
            .async_prewrite(
                Context::new(),
                vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
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
                vec![make_key(b"x")],
                100,
                101,
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .async_pause(Context::new(), 1000, expect_ok_callback(tx.clone(), 3))
            .unwrap();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            storage.async_get(ctx, make_key(b"x"), 101).wait(),
        );
        // Command Get with high priority not block by command Pause.
        assert_eq!(rx.recv().unwrap(), 3);

        storage.stop().unwrap();
    }

    #[test]
    fn test_delete_range() {
        let read_pool = new_read_pool();
        let config = Config::default();
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        // Write x and y.
        storage
            .async_prewrite(
                Context::new(),
                vec![
                    Mutation::Put((make_key(b"x"), b"100".to_vec())),
                    Mutation::Put((make_key(b"y"), b"100".to_vec())),
                    Mutation::Put((make_key(b"z"), b"100".to_vec())),
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
                vec![make_key(b"x"), make_key(b"y"), make_key(b"z")],
                100,
                101,
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), make_key(b"x"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), make_key(b"y"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), make_key(b"z"), 101)
                .wait(),
        );

        // Delete range [x, z)
        storage
            .async_delete_range(
                Context::new(),
                make_key(b"x"),
                make_key(b"z"),
                expect_ok_callback(tx.clone(), 5),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::new(), make_key(b"x"), 101)
                .wait(),
        );
        expect_none(
            storage
                .async_get(Context::new(), make_key(b"y"), 101)
                .wait(),
        );
        expect_value(
            b"100".to_vec(),
            storage
                .async_get(Context::new(), make_key(b"z"), 101)
                .wait(),
        );

        // Delete range ["", ""), it means delete all
        storage
            .async_delete_range(
                Context::new(),
                make_key(b""),
                make_key(b""),
                expect_ok_callback(tx.clone(), 9),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            storage
                .async_get(Context::new(), make_key(b"z"), 101)
                .wait(),
        );
        storage.stop().unwrap();
    }

    #[test]
    fn test_raw_delete_range() {
        let read_pool = new_read_pool();
        let config = Config::default();
        let mut storage = Storage::new(&config, read_pool).unwrap();
        storage.start(&config).unwrap();
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
                    kv.0.to_vec(),
                    kv.1.to_vec(),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }

        expect_value(
            b"004".to_vec(),
            storage.async_raw_get(Context::new(), b"d".to_vec()).wait(),
        );

        // Delete ["d", "e")
        storage
            .async_raw_delete_range(
                Context::new(),
                b"d".to_vec(),
                b"e".to_vec(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert key "d" has gone
        expect_value(
            b"003".to_vec(),
            storage.async_raw_get(Context::new(), b"c".to_vec()).wait(),
        );
        expect_none(storage.async_raw_get(Context::new(), b"d".to_vec()).wait());
        expect_value(
            b"005".to_vec(),
            storage.async_raw_get(Context::new(), b"e".to_vec()).wait(),
        );

        // Delete ["aa", "ab")
        storage
            .async_raw_delete_range(
                Context::new(),
                b"aa".to_vec(),
                b"ab".to_vec(),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert nothing happened
        expect_value(
            b"001".to_vec(),
            storage.async_raw_get(Context::new(), b"a".to_vec()).wait(),
        );
        expect_value(
            b"002".to_vec(),
            storage.async_raw_get(Context::new(), b"b".to_vec()).wait(),
        );

        // Delete all
        storage
            .async_raw_delete_range(
                Context::new(),
                b"a".to_vec(),
                b"z".to_vec(),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert now no key remains
        for kv in &test_data {
            expect_none(storage.async_raw_get(Context::new(), kv.0.to_vec()).wait());
        }

        rx.recv().unwrap();
    }
}
