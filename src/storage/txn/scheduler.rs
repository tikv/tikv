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
use storage::{Engine, Command, Snapshot};
use kvproto::kvrpcpb::Context;
use storage::mvcc::{MvccTxn, Error as MvccError};
use storage::{Key, Value};
use std::collections::HashMap;
use mio::{self, EventLoop};

use storage::engine::{Result as EngineResult, Callback};
use super::Result;
use super::Error;
use super::SchedCh;
use super::store::SnapshotStore;
use super::latch::Latches;

const REPORT_STATISTIC_INTERVAL: u64 = 60000; // 60 seconds

pub enum Tick {
    ReportStatistic,
}

pub enum ProcessResult {
    ResultSet {
        result: Vec<Result<()>>,
    },
    Value {
        value: Option<Value>,
    },
    Nothing,
}

pub enum Msg {
    // outer message
    Quit,
    RawCmd {
        cmd: Command,
    },

    // inner message
    AcquireLatches {
        cid: u64,
    },
    GetSnapshot {
        cid: u64,
    },
    SnapshotFinish {
        cid: u64,
        snapshot: EngineResult<Box<Snapshot>>,
    },
    WriteFinish {
        cid: u64,
        pr: ProcessResult,
        result: EngineResult<()>,
    },
}

type LatchIndices = Vec<usize>;

struct RunningCtx {
    pub cid: u64,
    pub cmd: Command,
    needed_latches: LatchIndices,
    pub owned_latches_count: usize,
}

impl RunningCtx {
    pub fn new(cid: u64, cmd: Command, needed_latches: LatchIndices) -> RunningCtx {
        RunningCtx {
            cid: cid,
            cmd: cmd,
            needed_latches: needed_latches,
            owned_latches_count: 0,
        }
    }

    pub fn needed_latches(&self) -> &LatchIndices {
        &self.needed_latches
    }

    pub fn all_latches_acquired(&self) -> bool {
        self.needed_latches.len() == self.owned_latches_count
    }
}

fn make_write_cb(pr: ProcessResult, cid: u64, ch: SchedCh) -> Callback<()> {
    Box::new(move |result: EngineResult<()>| {
        if let Err(e) = ch.send(Msg::WriteFinish{
            cid: cid,
            pr: pr,
            result: result,
        }) {
            error!("write engine failed cmd id {}, err {:?}", cid, e);
        }
    })
}

pub struct Scheduler {
    engine: Arc<Box<Engine>>,

    // cid -> context
    cmd_ctxs: HashMap<u64, RunningCtx>,

    //
    schedch: SchedCh,

    // cmd id generator
    idalloc: u64,

    // write concurrency control
    latches: Latches,
}

impl Scheduler {
    pub fn new(engine: Arc<Box<Engine>>, schedch: SchedCh, concurrency: usize) -> Scheduler {
        Scheduler {
            engine: engine,
            cmd_ctxs: HashMap::new(),
            schedch: schedch,
            idalloc: 0,
            latches: Latches::new(concurrency),
        }
    }

    fn gen_id(&mut self) -> u64 {
        self.idalloc += 1;
        self.idalloc
    }

    fn save_cmd_context(&mut self, cid: u64, ctx: RunningCtx) {
        if self.cmd_ctxs.insert(cid, ctx).is_some() {
            panic!("cid = {} shouldn't exist", cid);
        }
    }

    fn finish_cmd(&mut self, cid: u64) {
        {
            // running ctx must existed
            let rctx = self.cmd_ctxs.get(&cid).unwrap();
            assert_eq!(cid, rctx.cid);

            // release latches and wake up waiting commands
            if !rctx.needed_latches().is_empty() {
                let wakeup_list = self.latches.release_by_indices(rctx.needed_latches(), cid);
                for wcid in wakeup_list {
                    if let Err(e) = self.schedch.send(Msg::AcquireLatches { cid: wcid }) {
                        error!("wake up cmd failed, cid = {}, error = {:?}", wcid, e);
                    } else {
                        debug!("wake up cmd for acquire latches, cid = {}", wcid);
                    }
                }
            }
        }

        self.cmd_ctxs.remove(&cid);
    }

    pub fn dispatch_cmd(&self, cmd: Command) {
        if let Err(e) = self.schedch.send(Msg::RawCmd{ cmd: cmd }) {
            error!("dispatch cmd failed, error = {:?}", e);
        }
    }

    fn calc_latches_indices(&self, cmd: &Command) -> Vec<usize> {
        match *cmd {
            Command::Prewrite { ref mutations, .. } => {
                let keys: Vec<&Key> = mutations.iter().map(|x| x.key()).collect();
                self.latches.calc_latches_indices(&keys)
            }
            Command::Commit { ref keys, .. } |
            Command::Rollback { ref keys, .. } => {
                self.latches.calc_latches_indices(keys)
            }
            Command::CommitThenGet { ref key, .. } |
            Command::Cleanup { ref key, .. } |
            Command::RollbackThenGet { ref key, .. } => {
                self.latches.calc_latches_indices(&[key])
            }
            _ => {
                panic!("unsupported cmd that need latches");
            }
        }
    }

    fn process_cmd_with_snapshot(&mut self, cid: u64, snapshot: &Snapshot) -> Result<()> {
        debug!("process cmd with snapshot, cid = {}", cid);

        let rctx = self.cmd_ctxs.get_mut(&cid).unwrap();
        match rctx.cmd {
            Command::Get { ref key, start_ts, ref mut callback, .. } => {
                let snap_store = SnapshotStore::new(snapshot, start_ts);
                callback.take().unwrap()(snap_store.get(key).map_err(::storage::Error::from));
            }
            Command::BatchGet { ref keys, start_ts, ref mut callback, .. } => {
                let snap_store = SnapshotStore::new(snapshot, start_ts);
                callback.take().unwrap()(match snap_store.batch_get(keys) {
                    Ok(results) => {
                        let mut res = vec![];
                        for (k, v) in keys.into_iter().zip(results.into_iter()) {
                            match v {
                                Ok(Some(x)) => res.push(Ok((k.raw().unwrap(), x))),
                                Ok(None) => {}
                                Err(e) => res.push(Err(::storage::Error::from(e))),
                            }
                        }
                        Ok(res)
                    }
                    Err(e) => Err(e.into()),
                });
            }
            Command::Scan { ref start_key, limit, start_ts, ref mut callback, .. } => {
                let snap_store = SnapshotStore::new(snapshot, start_ts);
                let mut scanner = try!(snap_store.scanner());
                let key = start_key.clone();
                callback.take().unwrap()(match scanner.scan(key, limit) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(e.into()),
                });
            }
            Command::Prewrite { ref ctx, ref mutations, ref primary, start_ts, .. } => {
                let mut txn = MvccTxn::new(snapshot, start_ts);
                let mut results = vec![];
                for m in mutations {
                    match txn.prewrite(m.clone(), primary) {
                        Ok(_) => results.push(Ok(())),
                        e @ Err(MvccError::KeyIsLocked { .. }) => results.push(e.map_err(Error::from)),
                        Err(e) => return Err(Error::from(e)),
                    }
                }

                let pr: ProcessResult = ProcessResult::ResultSet { result: results };
                let cb = make_write_cb(pr, cid, self.schedch.clone());

                try!(self.engine.async_write(ctx, txn.modifies(), cb));
            }
            Command::Commit { ref ctx, ref keys, lock_ts, commit_ts, .. } => {
                let mut txn = MvccTxn::new(snapshot, lock_ts);
                for k in keys {
                    try!(txn.commit(&k, commit_ts));
                }

                let pr: ProcessResult = ProcessResult::Nothing;
                let cb = make_write_cb(pr, cid, self.schedch.clone());
                try!(self.engine.async_write(ctx, txn.modifies(), cb));
            }
            Command::CommitThenGet { ref ctx, ref key, lock_ts, commit_ts, get_ts, .. } => {
                let mut txn = MvccTxn::new(snapshot, lock_ts);
                let val = try!(txn.commit_then_get(&key, commit_ts, get_ts));

                let pr: ProcessResult = ProcessResult::Value { value: val };
                let cb = make_write_cb(pr, cid, self.schedch.clone());
                try!(self.engine.async_write(ctx, txn.modifies(), cb));
            }
            Command::Cleanup { ref ctx, ref key, start_ts, .. } => {
                let mut txn = MvccTxn::new(snapshot, start_ts);
                try!(txn.rollback(&key));

                let pr: ProcessResult = ProcessResult::Nothing;
                let cb = make_write_cb(pr, cid, self.schedch.clone());
                try!(self.engine.async_write(&ctx, txn.modifies(), cb));
            }
            Command::Rollback { ref ctx, ref keys, start_ts, .. } => {
                let mut txn = MvccTxn::new(snapshot, start_ts);
                for k in keys {
                    try!(txn.rollback(&k));
                }

                let pr: ProcessResult = ProcessResult::Nothing;
                let cb = make_write_cb(pr, cid, self.schedch.clone());
                try!(self.engine.async_write(ctx, txn.modifies(), cb));
            }
            Command::RollbackThenGet { ref ctx, ref key, lock_ts, .. } => {
                let mut txn = MvccTxn::new(snapshot, lock_ts);
                let val = try!(txn.rollback_then_get(&key));

                let pr: ProcessResult = ProcessResult::Value { value: val };
                let cb = make_write_cb(pr, cid, self.schedch.clone());
                try!(self.engine.async_write(ctx, txn.modifies(), cb));
            }
        }

        Ok(())
    }

    fn process_failed_cmd(&mut self, cid: u64, err: Error) {
        let rctx = self.cmd_ctxs.get_mut(&cid).unwrap();

        match rctx.cmd {
            Command::Get { ref mut callback, .. } |
            Command::CommitThenGet { ref mut callback, .. } |
            Command::RollbackThenGet { ref mut callback, .. } => {
                callback.take().unwrap()(Err(err.into()));
            }
            Command::BatchGet { ref mut callback, .. } => {
                callback.take().unwrap()(Err(err.into()));
            }
            Command::Scan { ref mut callback, .. } => {
                callback.take().unwrap()(Err(err.into()));
            }
            Command::Prewrite { ref mut callback, .. } => {
                callback.take().unwrap()(Err(err.into()));
            }
            Command::Commit { ref mut callback, .. } => {
                callback.take().unwrap()(Err(err.into()));
            }
            Command::Cleanup { ref mut callback, .. } => {
                callback.take().unwrap()(Err(err.into()));
            }
            Command::Rollback { ref mut callback, .. } => {
                callback.take().unwrap()(Err(err.into()));
            }
        }
    }

    fn extract_cmd_context_by_id(&self, cid: u64) -> &Context {
        let rctx: &RunningCtx = self.cmd_ctxs.get(&cid).unwrap();
        match rctx.cmd {
            Command::Get { ref ctx, .. } |
            Command::BatchGet { ref ctx, .. } |
            Command::Scan { ref ctx, .. } |
            Command::Prewrite { ref ctx, .. } |
            Command::Commit { ref ctx, .. } |
            Command::CommitThenGet { ref ctx, .. } |
            Command::Cleanup { ref ctx, .. } |
            Command::Rollback { ref ctx, .. } |
            Command::RollbackThenGet { ref ctx, .. } => {
                ctx
            }
        }
    }

    fn register_report_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop, Tick::ReportStatistic, REPORT_STATISTIC_INTERVAL) {
            error!("register report statistic err: {:?}", e);
        };
    }

    fn on_report_staticstic_tick(&self, event_loop: &mut EventLoop<Self>) {
        info!("all running cmd count = {}", self.cmd_ctxs.len());

        self.register_report_tick(event_loop);
    }

    fn received_new_cmd(&mut self, cmd: Command) {
        let cid = self.gen_id();

        if cmd.readonly() {
            // readonly command don't need latch, step to GetSnapshot
            let ctx: RunningCtx = RunningCtx::new(cid, cmd, vec![]);
            self.save_cmd_context(cid, ctx);

            if let Err(e) = self.schedch.send(Msg::GetSnapshot{ cid: cid }) {
                error!("send msg get snapshot failed, cid = {}, error = {:?}", cid, e);
                self.process_failed_cmd(cid, e);
                self.finish_cmd(cid);
            }

        } else {
            // write command need acquire latches first, if acquire all latches
            // then can step forward to get snapshot, or this command will be
            // wake up by who currently owned this latches when it release latches
            let indices = self.calc_latches_indices(&cmd);
            let mut ctx: RunningCtx = RunningCtx::new(cid, cmd, indices);

            let acquire_count = self.latches.acquire_by_indices(ctx.needed_latches(), cid);
            ctx.owned_latches_count += acquire_count;
            if ctx.all_latches_acquired() {
                if let Err(e) = self.schedch.send(Msg::GetSnapshot { cid: cid }) {
                    error!("send msg getsnapshot failed, cid = {}, error = {:?}", cid, e);
                    self.process_failed_cmd(cid, e);
                    self.finish_cmd(cid);
                }
            }

            self.save_cmd_context(cid, ctx);
        }
    }

    fn acquire_latches_for_cmd(&mut self, cid: u64) {
        let mut all_latches_acquired = false;
        {
            let ctx = &mut self.cmd_ctxs.get_mut(&cid).unwrap();

            let new_acquired = {
                let needed_latches = ctx.needed_latches();
                let owned_count = ctx.owned_latches_count;
                self.latches.acquire_by_indices(&needed_latches[owned_count..], cid)
            };
            ctx.owned_latches_count += new_acquired;
            if ctx.all_latches_acquired() {
                all_latches_acquired = true;
            }
        }

        if all_latches_acquired {
            if let Err(e) = self.schedch.send(Msg::GetSnapshot{ cid: cid }) {
                error!("send msg getsnapshot failed, cid = {}, error = {:?}", cid, e);
                self.process_failed_cmd(cid, e);
                self.finish_cmd(cid);
            }
        }
    }

    fn get_snapshot_for_cmd(&mut self, cid: u64) {
        let ch = self.schedch.clone();
        let cb = box move |snapshot: EngineResult<Box<Snapshot>>| {
            if let Err(e) = ch.send(Msg::SnapshotFinish{ cid: cid, snapshot: snapshot, }) {
                error!("send SnapshotFinish failed cmd id {}, err {:?}", cid, e);
            }
        };

        match self.engine.async_snapshot(self.extract_cmd_context_by_id(cid), cb) {
            Ok(()) => { }
            Err(e) => {
                self.process_failed_cmd(cid, Error::from(e));
                self.finish_cmd(cid);
            }
        }
    }

    fn snapshot_finished_for_cmd(&mut self, cid: u64, snapshot: EngineResult<Box<Snapshot>>) {
        debug!("receive snapshot finish msg for cid = {}", cid);

        match snapshot {
            Ok(snapshot) => {
                let res = self.process_cmd_with_snapshot(cid, snapshot.as_ref());
                let mut finished: bool = false;
                match res {
                    Ok(()) => {
                        let rctx = self.cmd_ctxs.get(&cid).unwrap();
                        if rctx.cmd.readonly() { finished = true; }
                    }
                    Err(e) => {
                        self.process_failed_cmd(cid, Error::from(e));
                        finished = true;
                    }
                }

                if finished { self.finish_cmd(cid); }
            }
            Err(e) => {
                self.process_failed_cmd(cid, Error::from(e));
                self.finish_cmd(cid);
            }
        }
    }

    fn write_finished_for_cmd(&mut self, cid: u64, pr: ProcessResult, result: EngineResult<()>) {
        let rctx = self.cmd_ctxs.get_mut(&cid).unwrap();
        assert_eq!(cid, rctx.cid);

        match rctx.cmd {
            Command::Prewrite { ref mut callback, .. } => {
                callback.take().unwrap()(match result {
                    Ok(()) => {
                        match pr {
                            ProcessResult::ResultSet { mut result } => {
                                Ok(result.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                            }
                            _ => { panic!("prewrite return but process result is not result set."); }
                        }
                    }
                    Err(e) => Err(e.into())
                });
            }
            Command::CommitThenGet { ref mut callback, .. } => {
                callback.take().unwrap()(match result {
                    Ok(()) => {
                        match pr {
                            ProcessResult::Value { value } => { Ok(value) }
                            _ => { panic!("commit then get return but process result is not value."); }
                        }
                    }
                    Err(e) => {
                        Err(::storage::Error::from(e))
                    }
                });
            }
            Command::Commit { ref mut callback, .. } |
            Command::Cleanup { ref mut callback, .. } |
            Command::Rollback { ref mut callback, .. } => {
                callback.take().unwrap()(result.map_err(::storage::Error::from));
            }
            Command::RollbackThenGet { ref mut callback, .. } => {
                callback.take().unwrap()(match result {
                    Ok(()) => {
                        match pr {
                            ProcessResult::Value { value } => { Ok(value) }
                            _ => { panic!("rollback then get return but process result is not value."); }
                        }
                    }
                    Err(e) => {
                        Err(::storage::Error::from(e))
                    }
                });
            }
            _ => { panic!("unsupported write cmd"); }
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
            // message from outer
            Msg::Quit => self.shutdown(event_loop),
            Msg::RawCmd { cmd } => self.received_new_cmd(cmd),

            // inner message
            Msg::AcquireLatches { cid } => self.acquire_latches_for_cmd(cid),
            Msg::GetSnapshot { cid } => self.get_snapshot_for_cmd(cid),
            Msg::SnapshotFinish { cid, snapshot } => self.snapshot_finished_for_cmd(cid, snapshot),
            Msg::WriteFinish { cid, pr, result } => {
                self.write_finished_for_cmd(cid, pr, result);
                self.finish_cmd(cid);
            }
        }
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            // stop work threads if has
        }
    }
}
