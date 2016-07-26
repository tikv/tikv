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

use std::time::Duration;
use std::boxed::Box;
use std::sync::Arc;
use threadpool::ThreadPool;
use storage::{Engine, Command, Snapshot, CallbackType, Result as StorageResult,
              Error as StorageError};
use kvproto::kvrpcpb::Context;
use storage::mvcc::{MvccTxn, Error as MvccError};
use storage::{Key, Value, KvPair};
use std::collections::HashMap;
use mio::{self, EventLoop};
use storage::engine::{Result as EngineResult, Callback as EngineCallback, Modify};
use super::Result;
use super::Error;
use super::SchedCh;
use super::store::SnapshotStore;
use super::latch::{Latches, Lock};

const REPORT_STATISTIC_INTERVAL: u64 = 60000; // 60 seconds
const DEFAULT_WOKER_POOL_SIZES: usize = 4;

pub enum Tick {
    ReportStatistic,
}

pub enum ProcessResult {
    Res,
    MultiRes {
        results: Vec<StorageResult<()>>,
    },
    MultiKvpairs {
        pairs: Vec<StorageResult<KvPair>>,
    },
    Value {
        value: Option<Value>,
    },
    Failed {
        err: StorageError,
    },
}

pub enum Msg {
    Quit,
    RawCmd {
        cmd: Command,
        cb: CallbackType,
    },
    SnapshotFinished {
        cid: u64,
        snapshot: EngineResult<Box<Snapshot>>,
    },
    ReadFinished {
        cid: u64,
        pr: ProcessResult,
    },
    PrepareWriteFinished {
        cid: u64,
        pr: ProcessResult,
        to_be_write: Vec<Modify>,
    },
    PrepareWriteFailed {
        cid: u64,
        err: Error,
    },
    WriteFinished {
        cid: u64,
        pr: ProcessResult,
        result: EngineResult<()>,
    },
}

fn execute_callback(callback: CallbackType, pr: ProcessResult) {
    match callback {
        CallbackType::Result(cb) => {
            match pr {
                ProcessResult::Res => cb(Ok(())),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
        CallbackType::MultiResults(cb) => {
            match pr {
                ProcessResult::MultiRes { results } => cb(Ok(results)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
        CallbackType::Value(cb) => {
            match pr {
                ProcessResult::Value { value } => cb(Ok(value)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
        CallbackType::KvPairs(cb) => {
            match pr {
                ProcessResult::MultiKvpairs { pairs } => cb(Ok(pairs)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
    }
}

pub struct RunningCtx {
    cid: u64,
    cmd: Command,
    lock: Lock,
    callback: Option<CallbackType>,
}

impl RunningCtx {
    pub fn new(cid: u64, cmd: Command, lock: Lock, cb: CallbackType) -> RunningCtx {
        RunningCtx {
            cid: cid,
            cmd: cmd,
            lock: lock,
            callback: Some(cb),
        }
    }
}

fn make_engine_cb(cid: u64, pr: ProcessResult, ch: SchedCh) -> EngineCallback<()> {
    Box::new(move |result: EngineResult<()>| {
        if let Err(e) = ch.send(Msg::WriteFinished {
            cid: cid,
            pr: pr,
            result: result,
        }) {
            error!("send write finished to scheduler failed cid={}, err:{:?}",
                   cid,
                   e);
        }
    })
}

pub struct Scheduler {
    engine: Arc<Box<Engine>>,

    // cid -> context
    cmd_ctxs: HashMap<u64, RunningCtx>,

    schedch: SchedCh,

    // cmd id generator
    id_alloc: u64,

    // write concurrency control
    latches: Latches,

    // worker pool
    worker_pool: ThreadPool,
}

fn process_read(cid: u64, cmd: Command, ch: SchedCh, snapshot: Box<Snapshot>) {
    debug!("process read cmd(cid={}) in worker pool.", cid);
    let pr = match cmd {
        Command::Get { ref key, start_ts, .. } => {
            let snap_store = SnapshotStore::new(snapshot.as_ref(), start_ts);
            let res = snap_store.get(key);
            match res {
                Ok(val) => ProcessResult::Value { value: val },
                Err(e) => ProcessResult::Failed { err: StorageError::from(e) },
            }
        }
        Command::BatchGet { ref keys, start_ts, .. } => {
            let snap_store = SnapshotStore::new(snapshot.as_ref(), start_ts);
            match snap_store.batch_get(keys) {
                Ok(results) => {
                    let mut res = vec![];
                    for (k, v) in keys.into_iter().zip(results.into_iter()) {
                        match v {
                            Ok(Some(x)) => res.push(Ok((k.raw().unwrap(), x))),
                            Ok(None) => {}
                            Err(e) => res.push(Err(StorageError::from(e))),
                        }
                    }
                    ProcessResult::MultiKvpairs { pairs: res }
                }
                Err(e) => ProcessResult::Failed { err: StorageError::from(e) },
            }
        }
        Command::Scan { ref start_key, limit, start_ts, .. } => {
            let snap_store = SnapshotStore::new(snapshot.as_ref(), start_ts);
            let scanner = snap_store.scanner();
            match scanner {
                Ok(mut scanner) => {
                    let key = start_key.clone();
                    match scanner.scan(key, limit) {
                        Ok(mut results) => {
                            let pairs = results.drain(..)
                                .map(|x| x.map_err(StorageError::from))
                                .collect();
                            ProcessResult::MultiKvpairs { pairs: pairs }
                        }
                        Err(e) => ProcessResult::Failed { err: StorageError::from(e) },
                    }
                }
                Err(e) => ProcessResult::Failed { err: StorageError::from(e) },
            }
        }
        _ => panic!("unsupported read command"),
    };

    if let Err(e) = ch.send(Msg::ReadFinished { cid: cid, pr: pr }) {
        error!("send read finished failed, cid={}, err={:?}", cid, e);
    }
}

fn process_write(cid: u64, cmd: Command, ch: SchedCh, snapshot: Box<Snapshot>) {
    if let Err(e) = process_write_impl(cid, cmd, ch.clone(), snapshot.as_ref()) {
        if let Err(err) = ch.send(Msg::PrepareWriteFailed { cid: cid, err: e }) {
            error!("send PrepareWriteFailed message to channel failed. cid={}, err={:?}",
                   cid,
                   err);
        }
    }
}

fn process_write_impl(cid: u64, cmd: Command, ch: SchedCh, snapshot: &Snapshot) -> Result<()> {
    if let Err(e) = match cmd {
        Command::Prewrite { ref mutations, ref primary, start_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, start_ts);
            let mut results = vec![];
            for m in mutations {
                match txn.prewrite(m.clone(), primary) {
                    Ok(_) => results.push(Ok(())),
                    e @ Err(MvccError::KeyIsLocked { .. }) => results.push(e.map_err(Error::from)),
                    Err(e) => return Err(Error::from(e)),
                }
            }
            let res = results.drain(..).map(|x| x.map_err(StorageError::from)).collect();
            let pr = ProcessResult::MultiRes { results: res };
            ch.send(Msg::PrepareWriteFinished {
                cid: cid,
                pr: pr,
                to_be_write: txn.modifies(),
            })
        }
        Command::Commit { ref keys, lock_ts, commit_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, lock_ts);
            for k in keys {
                try!(txn.commit(&k, commit_ts));
            }

            let pr = ProcessResult::Res;
            ch.send(Msg::PrepareWriteFinished {
                cid: cid,
                pr: pr,
                to_be_write: txn.modifies(),
            })
        }
        Command::CommitThenGet { ref key, lock_ts, commit_ts, get_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, lock_ts);
            let val = try!(txn.commit_then_get(&key, commit_ts, get_ts));

            let pr = ProcessResult::Value { value: val };
            ch.send(Msg::PrepareWriteFinished {
                cid: cid,
                pr: pr,
                to_be_write: txn.modifies(),
            })
        }
        Command::Cleanup { ref key, start_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, start_ts);
            try!(txn.rollback(&key));

            let pr = ProcessResult::Res;
            ch.send(Msg::PrepareWriteFinished {
                cid: cid,
                pr: pr,
                to_be_write: txn.modifies(),
            })
        }
        Command::Rollback { ref keys, start_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, start_ts);
            for k in keys {
                try!(txn.rollback(&k));
            }

            let pr = ProcessResult::Res;
            ch.send(Msg::PrepareWriteFinished {
                cid: cid,
                pr: pr,
                to_be_write: txn.modifies(),
            })
        }
        Command::RollbackThenGet { ref key, lock_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, lock_ts);
            let val = try!(txn.rollback_then_get(&key));

            let pr = ProcessResult::Value { value: val };
            ch.send(Msg::PrepareWriteFinished {
                cid: cid,
                pr: pr,
                to_be_write: txn.modifies(),
            })
        }
        _ => panic!("unsupported write command"),
    } {
        error!("process write impl send msg to channel failed, cid={}", cid);
        return Err(e);
    }

    Ok(())
}

impl Scheduler {
    pub fn new(engine: Arc<Box<Engine>>, schedch: SchedCh, concurrency: usize) -> Scheduler {
        Scheduler {
            engine: engine,
            cmd_ctxs: HashMap::new(),
            schedch: schedch,
            id_alloc: 0,
            latches: Latches::new(concurrency),
            worker_pool: ThreadPool::new_with_name(thd_name!("sched-worker-pool"),
                                                   DEFAULT_WOKER_POOL_SIZES),
        }
    }

    fn gen_id(&mut self) -> u64 {
        self.id_alloc += 1;
        self.id_alloc
    }

    fn gen_lock(&self, cmd: &Command) -> Lock {
        match *cmd {
            Command::Prewrite { ref mutations, .. } => {
                let keys: Vec<&Key> = mutations.iter().map(|x| x.key()).collect();
                self.latches.gen_lock(&keys)
            }
            Command::Commit { ref keys, .. } |
            Command::Rollback { ref keys, .. } => self.latches.gen_lock(keys),
            Command::CommitThenGet { ref key, .. } |
            Command::Cleanup { ref key, .. } |
            Command::RollbackThenGet { ref key, .. } => self.latches.gen_lock(&[key]),
            _ => Lock::new(vec![]),
        }
    }

    fn process_by_worker(&mut self, cid: u64, snapshot: Box<Snapshot>) {
        debug!("process cmd with snapshot, cid={}", cid);
        let cmd = {
            let ctx = self.cmd_ctxs.get(&cid).unwrap();
            assert_eq!(ctx.cid, cid);
            ctx.cmd.clone()
        };
        let ch = self.schedch.clone();
        let readcmd = cmd.readonly();
        if readcmd {
            self.worker_pool.execute(move || process_read(cid, cmd, ch, snapshot));
        } else {
            self.worker_pool.execute(move || process_write(cid, cmd, ch, snapshot));
        }
    }

    fn finish_with_err(&mut self, cid: u64, err: Error) {
        debug!("command cid={}, finished with error", cid);

        let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let cb = ctx.callback.take().unwrap();
        let pr = ProcessResult::Failed { err: StorageError::from(err) };
        execute_callback(cb, pr);

        self.release_lock(&ctx.lock, cid);
    }

    fn extract_context(&self, cid: u64) -> &Context {
        let ctx = &self.cmd_ctxs.get(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        match ctx.cmd {
            Command::Get { ref ctx, .. } |
            Command::BatchGet { ref ctx, .. } |
            Command::Scan { ref ctx, .. } |
            Command::Prewrite { ref ctx, .. } |
            Command::Commit { ref ctx, .. } |
            Command::CommitThenGet { ref ctx, .. } |
            Command::Cleanup { ref ctx, .. } |
            Command::Rollback { ref ctx, .. } |
            Command::RollbackThenGet { ref ctx, .. } => ctx,
        }
    }

    fn register_report_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::ReportStatistic,
                                       REPORT_STATISTIC_INTERVAL) {
            error!("register report statistic err: {:?}", e);
        };
    }

    fn on_report_staticstic_tick(&self, event_loop: &mut EventLoop<Self>) {
        info!("all running cmd count = {}", self.cmd_ctxs.len());

        self.register_report_tick(event_loop);
    }

    fn on_received_new_cmd(&mut self, cmd: Command, callback: CallbackType) {
        let cid = self.gen_id();
        let lock = self.gen_lock(&cmd);
        let ctx = RunningCtx::new(cid, cmd, lock, callback);
        if self.cmd_ctxs.insert(cid, ctx).is_some() {
            panic!("command cid={} shouldn't exist", cid);
        }

        debug!("received new command, cid={}", cid);

        if self.acquire_lock(cid) {
            self.get_snapshot(cid);
        }
    }

    fn acquire_lock(&mut self, cid: u64) -> bool {
        let ctx = &mut self.cmd_ctxs.get_mut(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        self.latches.acquire(&mut ctx.lock, cid)
    }

    fn get_snapshot(&mut self, cid: u64) {
        let ch = self.schedch.clone();
        let cb = box move |snapshot: EngineResult<Box<Snapshot>>| {
            if let Err(e) = ch.send(Msg::SnapshotFinished {
                cid: cid,
                snapshot: snapshot,
            }) {
                error!("send SnapshotFinish failed cmd id {}, err {:?}", cid, e);
            }
        };

        if let Err(e) = self.engine.async_snapshot(self.extract_context(cid), cb) {
            self.finish_with_err(cid, Error::from(e));
        }
    }

    fn on_snapshot_finished(&mut self, cid: u64, snapshot: EngineResult<Box<Snapshot>>) {
        debug!("receive snapshot finish msg for cid={}", cid);
        match snapshot {
            Ok(snapshot) => self.process_by_worker(cid, snapshot),
            Err(e) => self.finish_with_err(cid, Error::from(e)),
        }
    }

    fn on_read_finished(&mut self, cid: u64, pr: ProcessResult) {
        debug!("read command(cid={}) finished", cid);

        let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let cb = ctx.callback.take().unwrap();
        match ctx.cmd {
            Command::Get { .. } |
            Command::BatchGet { .. } |
            Command::Scan { .. } => execute_callback(cb, pr),
            _ => panic!("unsupported read command"),
        }
    }

    fn on_prepare_write_failed(&mut self, cid: u64, e: Error) {
        debug!("write command(cid={}) failed at prewrite.", cid);

        // execute callback
        let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let cb = ctx.callback.take().unwrap();
        let pr = ProcessResult::Failed { err: StorageError::from(e) };
        execute_callback(cb, pr);

        // release lock
        self.release_lock(&ctx.lock, cid);
    }

    fn on_prepare_write_finished(&mut self,
                                 cid: u64,
                                 pr: ProcessResult,
                                 to_be_write: Vec<Modify>) {
        let res = {
            let ctx = self.cmd_ctxs.get(&cid).unwrap();
            assert_eq!(ctx.cid, cid);
            match ctx.cmd {
                Command::Prewrite { ref ctx, .. } |
                Command::Commit { ref ctx, .. } |
                Command::CommitThenGet { ref ctx, .. } |
                Command::Cleanup { ref ctx, .. } |
                Command::Rollback { ref ctx, .. } |
                Command::RollbackThenGet { ref ctx, .. } => {
                    let engine_cb = make_engine_cb(cid, pr, self.schedch.clone());
                    self.engine.async_write(ctx, to_be_write, engine_cb)
                }
                _ => panic!("unsupported write command"),
            }
        };
        if let Err(e) = res {
            let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
            assert_eq!(ctx.cid, cid);
            let cb = ctx.callback.take().unwrap();
            execute_callback(cb, ProcessResult::Failed { err: StorageError::from(e) });

            self.release_lock(&ctx.lock, cid);
        }
    }

    fn on_write_finished(&mut self, cid: u64, pr: ProcessResult, result: EngineResult<()>) {
        debug!("write finished for command, cid={}", cid);
        let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let cb = ctx.callback.take().unwrap();
        let pr = match result {
            Ok(()) => pr,
            Err(e) => ProcessResult::Failed { err: ::storage::Error::from(e) },
        };
        execute_callback(cb, pr);

        self.release_lock(&ctx.lock, cid);
    }

    fn release_lock(&mut self, lock: &Lock, cid: u64) {
        let wakeup_list = self.latches.release(lock, cid);
        for wcid in wakeup_list {
            self.wakeup_cmd(wcid);
        }
    }

    fn wakeup_cmd(&mut self, cid: u64) {
        if self.acquire_lock(cid) {
            self.get_snapshot(cid);
        }
    }

    fn shutdown(&mut self, event_loop: &mut EventLoop<Self>) {
        info!("receive shutdown command");
        event_loop.shutdown();
    }
}

fn register_timer(event_loop: &mut EventLoop<Scheduler>,
                  tick: Tick,
                  delay: u64)
                  -> Result<mio::Timeout> {
    event_loop.timeout(tick, Duration::from_millis(delay))
        .map_err(|e| box_err!("register timer err: {:?}", e))
}

impl mio::Handler for Scheduler {
    type Timeout = Tick;
    type Message = Msg;

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        match timeout {
            Tick::ReportStatistic => self.on_report_staticstic_tick(event_loop),
        }
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => self.shutdown(event_loop),
            Msg::RawCmd { cmd, cb } => self.on_received_new_cmd(cmd, cb),
            Msg::SnapshotFinished { cid, snapshot } => self.on_snapshot_finished(cid, snapshot),
            Msg::ReadFinished { cid, pr } => self.on_read_finished(cid, pr),
            Msg::PrepareWriteFinished { cid, pr, to_be_write } => {
                self.on_prepare_write_finished(cid, pr, to_be_write)
            }
            Msg::PrepareWriteFailed { cid, err } => self.on_prepare_write_failed(cid, err),
            Msg::WriteFinished { cid, pr, result } => self.on_write_finished(cid, pr, result),
        }
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            // stop work threads if has
        }
    }
}
