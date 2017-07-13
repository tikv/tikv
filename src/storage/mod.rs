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

use std::thread;
use std::boxed::FnBox;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::mpsc::{self, Receiver};
use std::error;
use std::sync::{Arc, Mutex};
use std::io::Error as IoError;
use kvproto::kvrpcpb::{LockInfo, CommandPri};
use kvproto::errorpb;
use self::metrics::*;

pub mod engine;
pub mod mvcc;
pub mod txn;
pub mod config;
pub mod types;
mod metrics;

pub use self::config::Config;
pub use self::engine::{Engine, Snapshot, TEMP_DIR, new_local_engine, Modify, Cursor,
                       Error as EngineError, ScanMode, Statistics};
pub use self::engine::raftkv::RaftKv;
pub use self::txn::{SnapshotStore, Scheduler, Msg};
pub use self::types::{Key, Value, KvPair, make_key};
pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";
// Cfs that should be very large generally.
pub const LARGE_CFS: &'static [CfName] = &[CF_DEFAULT, CF_WRITE];
pub const ALL_CFS: &'static [CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];

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

use kvproto::kvrpcpb::Context;

pub enum StorageCb {
    Boolean(Callback<()>),
    Booleans(Callback<Vec<Result<()>>>),
    SingleValue(Callback<Option<Value>>),
    KvPairs(Callback<Vec<Result<KvPair>>>),
    Locks(Callback<Vec<LockInfo>>),
}

pub enum Command {
    Get {
        ctx: Context,
        key: Key,
        start_ts: u64,
    },
    BatchGet {
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
    },
    Scan {
        ctx: Context,
        start_key: Key,
        limit: usize,
        start_ts: u64,
        options: Options,
    },
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
    Import {
        ctx: Context,
        mutations: Vec<Mutation>,
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
    ScanLock { ctx: Context, max_ts: u64 },
    ResolveLock {
        ctx: Context,
        start_ts: u64,
        commit_ts: Option<u64>,
        scan_key: Option<Key>,
        keys: Vec<Key>,
    },
    Gc {
        ctx: Context,
        safe_point: u64,
        scan_key: Option<Key>,
        keys: Vec<Key>,
    },
    RawGet { ctx: Context, key: Key },
    Pause { ctx: Context, duration: u64 },
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Command::Get { ref ctx, ref key, start_ts, .. } => {
                write!(f, "kv::command::get {} @ {} | {:?}", key, start_ts, ctx)
            }
            Command::BatchGet { ref ctx, ref keys, start_ts, .. } => {
                write!(f,
                       "kv::command_batch_get {} @ {} | {:?}",
                       keys.len(),
                       start_ts,
                       ctx)
            }
            Command::Scan { ref ctx, ref start_key, limit, start_ts, .. } => {
                write!(f,
                       "kv::command::scan {}({}) @ {} | {:?}",
                       start_key,
                       limit,
                       start_ts,
                       ctx)
            }
            Command::Prewrite { ref ctx, ref mutations, start_ts, .. } => {
                write!(f,
                       "kv::command::prewrite mutations({}) @ {} | {:?}",
                       mutations.len(),
                       start_ts,
                       ctx)
            }
            Command::Commit { ref ctx, ref keys, lock_ts, commit_ts, .. } => {
                write!(f,
                       "kv::command::commit {} {} -> {} | {:?}",
                       keys.len(),
                       lock_ts,
                       commit_ts,
                       ctx)
            }
            Command::Cleanup { ref ctx, ref key, start_ts, .. } => {
                write!(f, "kv::command::cleanup {} @ {} | {:?}", key, start_ts, ctx)
            }
            Command::Rollback { ref ctx, ref keys, start_ts, .. } => {
                write!(f,
                       "kv::command::rollback keys({}) @ {} | {:?}",
                       keys.len(),
                       start_ts,
                       ctx)
            }
            Command::ScanLock { ref ctx, max_ts, .. } => {
                write!(f, "kv::scan_lock {} | {:?}", max_ts, ctx)
            }
            Command::ResolveLock { ref ctx, start_ts, commit_ts, .. } => {
                write!(f,
                       "kv::resolve_txn {} -> {:?} | {:?}",
                       start_ts,
                       commit_ts,
                       ctx)
            }
            Command::Gc { ref ctx, safe_point, ref scan_key, .. } => {
                write!(f,
                       "kv::command::gc scan {:?} @ {} | {:?}",
                       scan_key,
                       safe_point,
                       ctx)
            }
            Command::Import { ref ctx, ref mutations, commit_ts, .. } => {
                write!(f,
                       "kv::command::import mutations({}) @ {} | {:?}",
                       mutations.len(),
                       commit_ts,
                       ctx)
            }
            Command::RawGet { ref ctx, ref key } => {
                write!(f, "kv::command::rawget {:?} | {:?}", key, ctx)
            }
            Command::Pause { ref ctx, duration } => {
                write!(f, "kv::command::pause {} ms | {:?}", duration, ctx)
            }
        }
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub const CMD_TAG_GC: &'static str = "gc";

impl Command {
    pub fn readonly(&self) -> bool {
        match *self {
            Command::Get { .. } |
            Command::BatchGet { .. } |
            Command::Scan { .. } |
            Command::ScanLock { .. } |
            Command::RawGet { .. } |
            Command::Pause { .. } => true,
            Command::ResolveLock { ref keys, .. } |
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
            Command::Get { .. } => "get",
            Command::BatchGet { .. } => "batch_get",
            Command::Scan { .. } => "scan",
            Command::Prewrite { .. } => "prewrite",
            Command::Commit { .. } => "commit",
            Command::Cleanup { .. } => "cleanup",
            Command::Rollback { .. } => "rollback",
            Command::ScanLock { .. } => "scan_lock",
            Command::ResolveLock { .. } => "resolve_lock",
            Command::Gc { .. } => CMD_TAG_GC,
            Command::Import { .. } => "import",
            Command::RawGet { .. } => "raw_get",
            Command::Pause { .. } => "pause",
        }
    }

    pub fn ts(&self) -> u64 {
        match *self {
            Command::Get { start_ts, .. } |
            Command::BatchGet { start_ts, .. } |
            Command::Scan { start_ts, .. } |
            Command::Prewrite { start_ts, .. } |
            Command::Cleanup { start_ts, .. } |
            Command::Rollback { start_ts, .. } |
            Command::ResolveLock { start_ts, .. } => start_ts,
            Command::Commit { lock_ts, .. } => lock_ts,
            Command::ScanLock { max_ts, .. } => max_ts,
            Command::Gc { safe_point, .. } => safe_point,
            Command::Import { commit_ts, .. } => commit_ts,
            Command::RawGet { .. } |
            Command::Pause { .. } => 0,
        }
    }

    pub fn get_context(&self) -> &Context {
        match *self {
            Command::Get { ref ctx, .. } |
            Command::BatchGet { ref ctx, .. } |
            Command::Scan { ref ctx, .. } |
            Command::Prewrite { ref ctx, .. } |
            Command::Commit { ref ctx, .. } |
            Command::Cleanup { ref ctx, .. } |
            Command::Rollback { ref ctx, .. } |
            Command::ScanLock { ref ctx, .. } |
            Command::ResolveLock { ref ctx, .. } |
            Command::Gc { ref ctx, .. } |
            Command::Import { ref ctx, .. } |
            Command::RawGet { ref ctx, .. } |
            Command::Pause { ref ctx, .. } => ctx,
        }
    }

    pub fn mut_context(&mut self) -> &mut Context {
        match *self {
            Command::Get { ref mut ctx, .. } |
            Command::BatchGet { ref mut ctx, .. } |
            Command::Scan { ref mut ctx, .. } |
            Command::Prewrite { ref mut ctx, .. } |
            Command::Commit { ref mut ctx, .. } |
            Command::Cleanup { ref mut ctx, .. } |
            Command::Rollback { ref mut ctx, .. } |
            Command::ScanLock { ref mut ctx, .. } |
            Command::ResolveLock { ref mut ctx, .. } |
            Command::Gc { ref mut ctx, .. } |
            Command::Import { ref mut ctx, .. } |
            Command::RawGet { ref mut ctx, .. } |
            Command::Pause { ref mut ctx, .. } => ctx,
        }
    }
}

use util::transport::SyncSendCh;

#[derive(Default)]
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

struct StorageHandle {
    handle: Option<thread::JoinHandle<()>>,
    receiver: Option<Receiver<Msg>>,
}

pub struct Storage {
    engine: Box<Engine>,
    sendch: SyncSendCh<Msg>,
    handle: Arc<Mutex<StorageHandle>>,
}

impl Storage {
    pub fn from_engine(engine: Box<Engine>, config: &Config) -> Result<Storage> {
        let (tx, rx) = mpsc::sync_channel(config.sched_notify_capacity);
        let sendch = SyncSendCh::new(tx, "kv-storage");

        info!("storage {:?} started.", engine);
        Ok(Storage {
            engine: engine,
            sendch: sendch,
            handle: Arc::new(Mutex::new(StorageHandle {
                handle: None,
                receiver: Some(rx),
            })),
        })
    }

    pub fn new(config: &Config) -> Result<Storage> {
        let engine = try!(engine::new_local_engine(&config.path, ALL_CFS));
        Storage::from_engine(engine, config)
    }

    pub fn start(&mut self, config: &Config) -> Result<()> {
        let mut handle = self.handle.lock().unwrap();
        if handle.handle.is_some() {
            return Err(box_err!("scheduler is already running"));
        }

        let engine = self.engine.clone();
        let builder = thread::Builder::new().name(thd_name!("storage-scheduler"));
        let rx = handle.receiver.take().unwrap();
        let sched_concurrency = config.sched_concurrency;
        let sched_worker_pool_size = config.sched_worker_pool_size;
        let sched_too_busy_threshold = config.sched_too_busy_threshold;
        let ch = self.sendch.clone();
        let h = try!(builder.spawn(move || {
            let mut sched = Scheduler::new(engine,
                                           ch,
                                           sched_concurrency,
                                           sched_worker_pool_size,
                                           sched_too_busy_threshold);
            if let Err(e) = sched.run(rx) {
                panic!("scheduler run err:{:?}", e);
            }
            info!("scheduler stopped");
        }));
        handle.handle = Some(h);

        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let mut handle = self.handle.lock().unwrap();
        if handle.handle.is_none() {
            return Ok(());
        }

        if let Err(e) = self.sendch.send(Msg::Quit) {
            error!("send quit cmd to scheduler failed, error:{:?}", e);
            return Err(box_err!("failed to ask sched to quit: {:?}", e));
        }

        let h = handle.handle.take().unwrap();
        if let Err(e) = h.join() {
            return Err(box_err!("failed to join sched_handle, err:{:?}", e));
        }

        info!("storage {:?} closed.", self.engine);
        Ok(())
    }

    pub fn get_engine(&self) -> Box<Engine> {
        self.engine.clone()
    }

    fn send(&self, cmd: Command, cb: StorageCb) -> Result<()> {
        box_try!(self.sendch.try_send(Msg::RawCmd { cmd: cmd, cb: cb }));
        Ok(())
    }

    pub fn async_get(&self,
                     ctx: Context,
                     key: Key,
                     start_ts: u64,
                     callback: Callback<Option<Value>>)
                     -> Result<()> {
        let cmd = Command::Get {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::SingleValue(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_batch_get(&self,
                           ctx: Context,
                           keys: Vec<Key>,
                           start_ts: u64,
                           callback: Callback<Vec<Result<KvPair>>>)
                           -> Result<()> {
        let cmd = Command::BatchGet {
            ctx: ctx,
            keys: keys,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::KvPairs(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_scan(&self,
                      ctx: Context,
                      start_key: Key,
                      limit: usize,
                      start_ts: u64,
                      options: Options,
                      callback: Callback<Vec<Result<KvPair>>>)
                      -> Result<()> {
        let cmd = Command::Scan {
            ctx: ctx,
            start_key: start_key,
            limit: limit,
            start_ts: start_ts,
            options: options,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::KvPairs(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_pause(&self, ctx: Context, duration: u64, callback: Callback<()>) -> Result<()> {
        let cmd = Command::Pause {
            ctx: ctx,
            duration: duration,
        };
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        Ok(())
    }

    pub fn async_prewrite(&self,
                          ctx: Context,
                          mutations: Vec<Mutation>,
                          primary: Vec<u8>,
                          start_ts: u64,
                          options: Options,
                          callback: Callback<Vec<Result<()>>>)
                          -> Result<()> {
        let cmd = Command::Prewrite {
            ctx: ctx,
            mutations: mutations,
            primary: primary,
            start_ts: start_ts,
            options: options,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::Booleans(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_commit(&self,
                        ctx: Context,
                        keys: Vec<Key>,
                        lock_ts: u64,
                        commit_ts: u64,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Command::Commit {
            ctx: ctx,
            keys: keys,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_cleanup(&self,
                         ctx: Context,
                         key: Key,
                         start_ts: u64,
                         callback: Callback<()>)
                         -> Result<()> {
        let cmd = Command::Cleanup {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_rollback(&self,
                          ctx: Context,
                          keys: Vec<Key>,
                          start_ts: u64,
                          callback: Callback<()>)
                          -> Result<()> {
        let cmd = Command::Rollback {
            ctx: ctx,
            keys: keys,
            start_ts: start_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_scan_lock(&self,
                           ctx: Context,
                           max_ts: u64,
                           callback: Callback<Vec<LockInfo>>)
                           -> Result<()> {
        let cmd = Command::ScanLock {
            ctx: ctx,
            max_ts: max_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::Locks(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_resolve_lock(&self,
                              ctx: Context,
                              start_ts: u64,
                              commit_ts: Option<u64>,
                              callback: Callback<()>)
                              -> Result<()> {
        let cmd = Command::ResolveLock {
            ctx: ctx,
            start_ts: start_ts,
            commit_ts: commit_ts,
            scan_key: None,
            keys: vec![],
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        let cmd = Command::Gc {
            ctx: ctx,
            safe_point: safe_point,
            scan_key: None,
            keys: vec![],
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_import(&self,
                        ctx: Context,
                        mutations: Vec<Mutation>,
                        commit_ts: u64,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Command::Import {
            ctx: ctx,
            mutations: mutations,
            commit_ts: commit_ts,
        };
        let tag = cmd.tag();
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        KV_COMMAND_COUNTER_VEC.with_label_values(&[tag]).inc();
        Ok(())
    }

    pub fn async_raw_get(&self,
                         ctx: Context,
                         key: Vec<u8>,
                         callback: Callback<Option<Vec<u8>>>)
                         -> Result<()> {
        let cmd = Command::RawGet {
            ctx: ctx,
            key: Key::from_encoded(key),
        };
        try!(self.send(cmd, StorageCb::SingleValue(callback)));
        RAWKV_COMMAND_COUNTER_VEC.with_label_values(&["get"]).inc();
        Ok(())
    }

    pub fn async_raw_put(&self,
                         ctx: Context,
                         key: Vec<u8>,
                         value: Vec<u8>,
                         callback: Callback<()>)
                         -> Result<()> {
        try!(self.engine
            .async_write(&ctx,
                         vec![Modify::Put(CF_DEFAULT, Key::from_encoded(key), value)],
                         box |(_, res): (_, engine::Result<_>)| {
                             callback(res.map_err(Error::from))
                         }));
        RAWKV_COMMAND_COUNTER_VEC.with_label_values(&["put"]).inc();
        Ok(())
    }

    pub fn async_raw_delete(&self,
                            ctx: Context,
                            key: Vec<u8>,
                            callback: Callback<()>)
                            -> Result<()> {
        try!(self.engine
            .async_write(&ctx,
                         vec![Modify::Delete(CF_DEFAULT, Key::from_encoded(key))],
                         box |(_, res): (_, engine::Result<_>)| {
                             callback(res.map_err(Error::from))
                         }));
        RAWKV_COMMAND_COUNTER_VEC.with_label_values(&["delete"]).inc();
        Ok(())
    }
}

impl Clone for Storage {
    fn clone(&self) -> Storage {
        Storage {
            engine: self.engine.clone(),
            sendch: self.sendch.clone(),
            handle: self.handle.clone(),
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
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{channel, Sender};
    use kvproto::kvrpcpb::Context;

    fn expect_get_none(done: Sender<i32>, id: i32) -> Callback<Option<Value>> {
        Box::new(move |x: Result<Option<Value>>| {
            assert_eq!(x.unwrap(), None);
            done.send(id).unwrap();
        })
    }

    fn expect_get_val(done: Sender<i32>, v: Vec<u8>, id: i32) -> Callback<Option<Value>> {
        Box::new(move |x: Result<Option<Value>>| {
            assert_eq!(x.unwrap().unwrap(), v);
            done.send(id).unwrap();
        })
    }

    fn expect_ok<T>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_ok());
            done.send(id).unwrap();
        })
    }

    fn expect_fail<T>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_err());
            done.send(id).unwrap();
        })
    }

    fn expect_too_busy<T>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_err());
            match x {
                Err(Error::SchedTooBusy) => {}
                _ => panic!("expect too busy"),
            }
            done.send(id).unwrap();
        })
    }

    fn expect_scan(done: Sender<i32>,
                   pairs: Vec<Option<KvPair>>,
                   id: i32)
                   -> Callback<Vec<Result<KvPair>>> {
        Box::new(move |rlt: Result<Vec<Result<KvPair>>>| {
            let rlt: Vec<Option<KvPair>> = rlt.unwrap()
                .into_iter()
                .map(Result::ok)
                .collect();
            assert_eq!(rlt, pairs);
            done.send(id).unwrap()
        })
    }

    fn expect_batch_get_vals(done: Sender<i32>,
                             pairs: Vec<Option<KvPair>>,
                             id: i32)
                             -> Callback<Vec<Result<KvPair>>> {
        Box::new(move |rlt: Result<Vec<Result<KvPair>>>| {
            let rlt: Vec<Option<KvPair>> = rlt.unwrap()
                .into_iter()
                .map(Result::ok)
                .collect();
            assert_eq!(rlt, pairs);
            done.send(id).unwrap()
        })
    }

    #[test]
    fn test_get_put() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone(), 0))
            .unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone(), 1))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"x")],
                          100,
                          101,
                          expect_ok(tx.clone(), 2))
            .unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone(), 3))
            .unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       101,
                       expect_get_val(tx.clone(), b"100".to_vec(), 4))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_put_with_err() {
        let config = Config::new();
        // New engine lack of some column families.
        let engine = engine::new_local_engine(&config.path, &["default"]).unwrap();
        let mut storage = Storage::from_engine(engine, &config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![
            Mutation::Put((make_key(b"a"), b"aa".to_vec())),
            Mutation::Put((make_key(b"b"), b"bb".to_vec())),
            Mutation::Put((make_key(b"c"), b"cc".to_vec())),
            ],
                            b"a".to_vec(),
                            1,
                            Options::default(),
                            expect_fail(tx.clone(), 0))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_scan() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![
            Mutation::Put((make_key(b"a"), b"aa".to_vec())),
            Mutation::Put((make_key(b"b"), b"bb".to_vec())),
            Mutation::Put((make_key(b"c"), b"cc".to_vec())),
            ],
                            b"a".to_vec(),
                            1,
                            Options::default(),
                            expect_ok(tx.clone(), 0))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"a"),make_key(b"b"),make_key(b"c"),],
                          1,
                          2,
                          expect_ok(tx.clone(), 1))
            .unwrap();
        rx.recv().unwrap();
        storage.async_scan(Context::new(),
                        make_key(b"\x00"),
                        1000,
                        5,
                        Options::default(),
                        expect_scan(tx.clone(),
                                    vec![
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
            ],
                                    2))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_batch_get() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![
            Mutation::Put((make_key(b"a"), b"aa".to_vec())),
            Mutation::Put((make_key(b"b"), b"bb".to_vec())),
            Mutation::Put((make_key(b"c"), b"cc".to_vec())),
            ],
                            b"a".to_vec(),
                            1,
                            Options::default(),
                            expect_ok(tx.clone(), 0))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"a"),make_key(b"b"),make_key(b"c"),],
                          1,
                          2,
                          expect_ok(tx.clone(), 1))
            .unwrap();
        rx.recv().unwrap();
        storage.async_batch_get(Context::new(),
                             vec![make_key(b"a"), make_key(b"b"), make_key(b"c")],
                             5,
                             expect_batch_get_vals(tx.clone(),
                                                   vec![
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
            ],
                                                   2))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_txn() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone(), 0))
            .unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"y"), b"101".to_vec()))],
                            b"y".to_vec(),
                            101,
                            Options::default(),
                            expect_ok(tx.clone(), 1))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"x")],
                          100,
                          110,
                          expect_ok(tx.clone(), 2))
            .unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"y")],
                          101,
                          111,
                          expect_ok(tx.clone(), 3))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       120,
                       expect_get_val(tx.clone(), b"100".to_vec(), 4))
            .unwrap();
        storage.async_get(Context::new(),
                       make_key(b"y"),
                       120,
                       expect_get_val(tx.clone(), b"101".to_vec(), 5))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"105".to_vec()))],
                            b"x".to_vec(),
                            105,
                            Options::default(),
                            expect_fail(tx.clone(), 6))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_sched_too_busy() {
        let mut config = Config::new();
        config.sched_too_busy_threshold = 1;
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone(), 0))
            .unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone(), 1))
            .unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"y"), b"101".to_vec()))],
                            b"y".to_vec(),
                            101,
                            Options::default(),
                            expect_too_busy(tx.clone(), 2))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"z"), b"102".to_vec()))],
                            b"y".to_vec(),
                            102,
                            Options::default(),
                            expect_ok(tx.clone(), 3))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone(), 0))
            .unwrap();
        rx.recv().unwrap();
        storage.async_cleanup(Context::new(),
                           make_key(b"x"),
                           100,
                           expect_ok(tx.clone(), 1))
            .unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       105,
                       expect_get_none(tx.clone(), 2))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_high_priority_get_put() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        storage.async_get(ctx, make_key(b"x"), 100, expect_get_none(tx.clone(), 0))
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        storage.async_prewrite(ctx,
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone(), 1))
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        storage.async_commit(ctx,
                          vec![make_key(b"x")],
                          100,
                          101,
                          expect_ok(tx.clone(), 2))
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        storage.async_get(ctx, make_key(b"x"), 100, expect_get_none(tx.clone(), 3))
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        storage.async_get(ctx,
                       make_key(b"x"),
                       101,
                       expect_get_val(tx.clone(), b"100".to_vec(), 4))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_high_priority_no_block() {
        let mut config = Config::new();
        config.sched_worker_pool_size = 1;
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone(), 0))
            .unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            Options::default(),
                            expect_ok(tx.clone(), 1))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"x")],
                          100,
                          101,
                          expect_ok(tx.clone(), 2))
            .unwrap();
        rx.recv().unwrap();

        storage.async_pause(Context::new(), 1000, expect_ok(tx.clone(), 3)).unwrap();
        let mut ctx = Context::new();
        ctx.set_priority(CommandPri::High);
        storage.async_get(ctx,
                       make_key(b"x"),
                       101,
                       expect_get_val(tx.clone(), b"100".to_vec(), 4))
            .unwrap();
        // Command Get with high priority not block by command Pause.
        assert_eq!(rx.recv().unwrap(), 4);
        assert_eq!(rx.recv().unwrap(), 3);

        storage.stop().unwrap();
    }

    #[test]
    fn test_import() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        let data = vec![(make_key(b"y"), b"101".to_vec()), (make_key(b"x"), b"100".to_vec())];
        let mutations = data.clone()
            .into_iter()
            .map(|(key, value)| Mutation::Put((key.clone(), value.clone())))
            .collect();
        storage.async_import(Context::new(), mutations, 100, expect_ok(tx.clone(), 0))
            .unwrap();
        rx.recv().unwrap();
        for (key, value) in data {
            storage.async_get(Context::new(),
                           key.clone(),
                           120,
                           expect_get_val(tx.clone(), value.clone(), 0))
                .unwrap();
            rx.recv().unwrap();
        }
        storage.stop().unwrap();
    }
}
