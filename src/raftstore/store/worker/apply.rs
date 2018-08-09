// Copyright 2017 PingCAP, Inc.
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

#![allow(dead_code)]

use std::cmp;
use std::collections::VecDeque;
use std::fmt::{self, Debug, Display, Formatter};
use std::mem;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::sync::oneshot;
use futures::{self, future, Future, Stream};
use protobuf::RepeatedField;
use rocksdb::rocksdb_options::WriteOptions;
use rocksdb::{Writable, WriteBatch, DB};
use uuid::Uuid;

use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb::{Peer as PeerMeta, Region};
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, AdminResponse, ChangePeerRequest, CmdType, CommitMergeRequest,
    RaftCmdRequest, RaftCmdResponse, Request, Response,
};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftApplyState, RaftTruncatedState, RegionLocalState,
};
use raft::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType};

use import::SSTImporter;
use raft::NO_LIMIT;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::engine::{Mutable, Peekable, Snapshot};
use raftstore::store::metrics::*;
use raftstore::store::msg::Callback;
use raftstore::store::peer::Peer;
use raftstore::store::peer_storage::{
    self, compact_raft_log, write_initial_apply_state, write_peer_state,
};
use raftstore::store::util::check_region_epoch;
use raftstore::store::{cmd_resp, keys, util, Engines, Store};
use raftstore::{Error, Result};
use storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use util::collections::HashMap;
use util::futurepool::{Context as FuturePoolContext, FuturePool};
use util::time::{duration_to_sec, Instant, SlowTimer};
use util::worker::{Runnable, Scheduler};
use util::{escape, rocksdb, MustConsumeVec};

use super::metrics::*;

const WRITE_BATCH_MAX_KEYS: usize = 128;
const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;
const SHRINK_PENDING_CMD_QUEUE_CAP: usize = 64;

const STACK_SIZE: usize = 10 * 1024 * 1024;

pub struct PendingCmd {
    pub index: u64,
    pub term: u64,
    pub cb: Option<Callback>,
}

impl PendingCmd {
    fn new(index: u64, term: u64, cb: Callback) -> PendingCmd {
        PendingCmd {
            index,
            term,
            cb: Some(cb),
        }
    }
}

impl Drop for PendingCmd {
    fn drop(&mut self) {
        if self.cb.is_some() {
            panic!(
                "callback of pending command at [index: {}, term: {}] is leak.",
                self.index, self.term
            );
        }
    }
}

impl Debug for PendingCmd {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "PendingCmd [index: {}, term: {}, has_cb: {}]",
            self.index,
            self.term,
            self.cb.is_some()
        )
    }
}

#[derive(Default, Debug)]
pub struct PendingCmdQueue {
    normals: VecDeque<PendingCmd>,
    conf_change: Option<PendingCmd>,
}

impl PendingCmdQueue {
    fn pop_normal(&mut self, term: u64) -> Option<PendingCmd> {
        self.normals.pop_front().and_then(|cmd| {
            if self.normals.capacity() > SHRINK_PENDING_CMD_QUEUE_CAP
                && self.normals.len() < SHRINK_PENDING_CMD_QUEUE_CAP
            {
                self.normals.shrink_to_fit();
            }
            if cmd.term > term {
                self.normals.push_front(cmd);
                return None;
            }
            Some(cmd)
        })
    }

    fn append_normal(&mut self, cmd: PendingCmd) {
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PendingCmd> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        self.conf_change.take()
    }

    // TODO: seems we don't need to separate conf change from normal entries.
    fn set_conf_change(&mut self, cmd: PendingCmd) {
        self.conf_change = Some(cmd);
    }
}

#[derive(Default, Debug)]
pub struct ChangePeer {
    pub conf_change: ConfChange,
    pub peer: PeerMeta,
    pub region: Region,
}

#[derive(Debug)]
pub struct Range {
    pub cf: String,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl Range {
    fn new(cf: String, start_key: Vec<u8>, end_key: Vec<u8>) -> Range {
        Range {
            cf,
            start_key,
            end_key,
        }
    }
}

#[derive(Debug)]
pub enum ExecResult {
    ChangePeer(ChangePeer),
    CompactLog {
        state: RaftTruncatedState,
        first_index: u64,
    },
    SplitRegion {
        left: Region,
        right: Region,
        right_derive: bool,
    },
    PrepareMerge {
        region: Region,
        state: MergeState,
    },
    CommitMerge {
        region: Region,
        source: Region,
    },
    RollbackMerge {
        region: Region,
        commit: u64,
    },
    ComputeHash {
        region: Region,
        index: u64,
        snap: Snapshot,
    },
    VerifyHash {
        index: u64,
        hash: Vec<u8>,
    },
    DeleteRange {
        ranges: Vec<Range>,
    },
    IngestSST {
        ssts: Vec<SSTMeta>,
    },
}

struct ApplyCallback {
    region: Region,
    cbs: Vec<(Option<Callback>, RaftCmdResponse)>,
}

impl ApplyCallback {
    fn new(region: Region) -> ApplyCallback {
        let cbs = vec![];
        ApplyCallback { region, cbs }
    }

    fn invoke_all(&mut self, host: &CoprocessorHost) {
        for (cb, mut resp) in self.cbs.drain(..) {
            host.post_apply(&self.region, &mut resp);
            if let Some(cb) = cb {
                cb.invoke_with_response(resp)
            };
        }
    }

    fn push(&mut self, cb: Option<Callback>, resp: RaftCmdResponse) {
        self.cbs.push((cb, resp));
    }
}

pub struct ApplyContext {
    host: Arc<CoprocessorHost>,
    importer: Arc<SSTImporter>,
    wb: Option<WriteBatch>,
    cbs: MustConsumeVec<ApplyCallback>,
    merged_regions: Vec<u64>,
    exec_results: Vec<ExecResult>,
    apply_res: Vec<ApplyRes>,
    wb_last_bytes: u64,
    wb_last_keys: u64,
    last_applied_index: u64,
    committed_count: usize,
    // `enable_sync_log` indicates that wal can be synchronized when data
    // is written to kv engine.
    enable_sync_log: bool,
    // `sync_log_hint` indicates whether synchronize wal is prefered.
    sync_log_hint: bool,
    exec_ctx: Option<ExecContext>,
}

impl ApplyContext {
    pub fn new(host: Arc<CoprocessorHost>, importer: Arc<SSTImporter>) -> Self {
        Self {
            host,
            importer,
            wb: None,
            cbs: MustConsumeVec::new("callback of apply context"),
            merged_regions: vec![],
            exec_results: vec![],
            apply_res: vec![],
            wb_last_bytes: 0,
            wb_last_keys: 0,
            last_applied_index: 0,
            committed_count: 0,
            enable_sync_log: false,
            sync_log_hint: false,
            exec_ctx: None,
        }
    }

    pub fn push_exec_result(&mut self, res: ExecResult) {
        self.exec_results.push(res);
    }

    pub fn enable_sync_log(mut self, enabled: bool) -> ApplyContext {
        self.enable_sync_log = enabled;
        self
    }

    pub fn apply_res_capacity(mut self, cap: usize) -> ApplyContext {
        self.apply_res = Vec::with_capacity(cap);
        self
    }

    /// Prepare for applying entries for `delegate`.
    ///
    /// A general apply progress for a delegate is:
    /// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
    /// After all delegates are handled, `write_to_db` method should be called.
    pub fn prepare_for(&mut self, delegate: &ApplyDelegate) {
        if self.wb.is_none() {
            self.wb = Some(WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE));
            self.wb_last_bytes = 0;
            self.wb_last_keys = 0;
        }
        self.cbs.push(ApplyCallback::new(delegate.region.clone()));
        self.last_applied_index = delegate.apply_state.get_applied_index();
    }

    /// Commit all changes have done for delegate. `persistent` indicates whether
    /// write the changes into rocksdb.
    ///
    /// This call is valid only when it's between a `prepare_for` and `finish_for`.
    pub fn commit(&mut self, delegate: &mut ApplyDelegate) {
        if self.last_applied_index < delegate.apply_state.get_applied_index() {
            delegate.write_apply_state(self.wb_mut());
        }
        // last_applied_index doesn't need to be updated, set persistent to true will
        // force it call `prepare_for` automatically.
        self.commit_opt(delegate, true);
    }

    fn commit_opt(&mut self, delegate: &mut ApplyDelegate, persistent: bool) {
        delegate.update_metrics(self);
        if persistent {
            self.write_to_db(&delegate.engines.kv);
            self.prepare_for(delegate);
        }
        self.wb_last_bytes = self.wb().data_size() as u64;
        self.wb_last_keys = self.wb().count() as u64;
    }

    /// Write all the changes into rocksdb.
    pub fn write_to_db(&mut self, engine: &DB) {
        if self.wb.as_ref().map_or(false, |wb| !wb.is_empty()) {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.enable_sync_log && self.sync_log_hint);
            engine
                .write_opt(self.wb.take().unwrap(), &write_opts)
                .unwrap_or_else(|e| {
                    panic!("failed to write to engine: {:?}", e);
                });
        }
        for mut cbs in self.cbs.drain(..) {
            cbs.invoke_all(&self.host);
        }
    }

    /// Finish applys for the delegate.
    pub fn finish_for(&mut self, delegate: &mut ApplyDelegate) {
        if !delegate.pending_remove {
            delegate.write_apply_state(self.wb_mut());
        }
        self.commit_opt(delegate, false);
        let mut exec_results = vec![];
        mem::swap(&mut exec_results, &mut self.exec_results);
        self.apply_res.push(ApplyRes {
            region_id: delegate.region_id(),
            apply_state: delegate.apply_state.clone(),
            exec_res: exec_results,
            metrics: delegate.metrics.clone(),
            applied_index_term: delegate.applied_index_term,
            merged: false,
        });
    }

    pub fn delta_bytes(&self) -> u64 {
        self.wb().data_size() as u64 - self.wb_last_bytes
    }

    pub fn delta_keys(&self) -> u64 {
        self.wb().count() as u64 - self.wb_last_keys
    }

    #[inline]
    pub fn wb(&self) -> &WriteBatch {
        self.wb.as_ref().unwrap()
    }

    #[inline]
    pub fn wb_mut(&mut self) -> &mut WriteBatch {
        self.wb.as_mut().unwrap()
    }
}

/// Call the callback of `cmd` that the region is removed.
fn notify_region_removed(region_id: u64, peer_id: u64, mut cmd: PendingCmd) {
    debug!(
        "[region {}] {} is removed, notify cmd at [index: {}, term: {}].",
        region_id, peer_id, cmd.index, cmd.term
    );
    notify_req_region_removed(region_id, cmd.cb.take().unwrap());
}

pub fn notify_req_region_removed(region_id: u64, cb: Callback) {
    let region_not_found = Error::RegionNotFound(region_id);
    let resp = cmd_resp::new_error(region_not_found);
    cb.invoke_with_response(resp);
}

/// Call the callback of `cmd` when it can not be processed further.
fn notify_stale_command(tag: &str, term: u64, mut cmd: PendingCmd) {
    info!(
        "{} command at [index: {}, term: {}] is stale, skip",
        tag, cmd.index, cmd.term
    );
    notify_stale_req(term, cmd.cb.take().unwrap());
}

pub fn notify_stale_req(term: u64, cb: Callback) {
    let resp = cmd_resp::err_resp(Error::StaleCommand, term);
    cb.invoke_with_response(resp);
}

/// Check if a write is needed to be issued before handle the command.
fn should_write_to_engine(cmd: &RaftCmdRequest, wb_keys: usize) -> bool {
    if cmd.has_admin_request() {
        match cmd.get_admin_request().get_cmd_type() {
            // ComputeHash require an up to date snapshot.
            AdminCmdType::ComputeHash |
            // Merge needs to get the latest apply index.
            AdminCmdType::CommitMerge |
            AdminCmdType::RollbackMerge => return true,
            _ => {}
        }
    }

    // When write batch contains more than `recommended` keys, write the batch to engine.
    if wb_keys >= WRITE_BATCH_MAX_KEYS {
        return true;
    }

    // Some commands may modify keys covered by the current write batch, so we
    // must write the current write batch to the engine first.
    for req in cmd.get_requests() {
        if req.has_delete_range() {
            return true;
        }
        if req.has_ingest_sst() {
            return true;
        }
    }

    false
}

pub struct ApplyDelegate {
    // peer_id
    id: u64,
    // peer_tag, "[region region_id] peer_id"
    tag: String,
    engines: Engines,
    region: Region,
    // if we remove ourself in ChangePeer remove, we should set this flag, then
    // any following committed logs in same Ready should be applied failed.
    pending_remove: bool,
    // we write apply_state to kv rocksdb, in one writebatch together with kv data.
    // because if we write it to raft rocksdb, apply_state and kv data (Put, Delete) are in
    // separate WAL file. when power failure, for current raft log, apply_index may synced
    // to file, but kv data may not synced to file, so we will lose data.
    apply_state: RaftApplyState,
    applied_index_term: u64,
    term: u64,
    is_merging: bool,
    pending_cmds: PendingCmdQueue,
    metrics: ApplyMetrics,
    last_merge_version: u64,

    scheduler: Scheduler<Task>,
    notifier: Sender<TaskRes>,
    host: Arc<CoprocessorHost>,
    importer: Arc<SSTImporter>,
    use_delete_range: bool,

    stopping: bool,
}

impl Debug for ApplyDelegate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ApplyDelegate")
            .field("tag", &self.tag)
            .field("id", &self.id)
            .finish()
    }
}

type RaftEntryFutureItem = (ApplyDelegate, ApplyContext, Option<ExecResult>);
type AdminCmdFutureItem = (
    ApplyDelegate,
    ApplyContext,
    AdminResponse,
    Option<ExecResult>,
);
type RaftCmdFutureItem = (
    ApplyDelegate,
    ApplyContext,
    RaftCmdResponse,
    Option<ExecResult>,
);
type FutureError = (ApplyDelegate, ApplyContext, Error);

impl ApplyDelegate {
    fn from_peer(
        peer: &Peer,
        scheduler: Scheduler<Task>,
        notifier: Sender<TaskRes>,
        host: Arc<CoprocessorHost>,
        importer: Arc<SSTImporter>,
        use_delete_range: bool,
    ) -> ApplyDelegate {
        let reg = Registration::new(peer);
        ApplyDelegate::from_registration(
            peer.engines(),
            reg,
            scheduler,
            notifier,
            host,
            importer,
            use_delete_range,
        )
    }

    fn from_registration(
        engines: Engines,
        reg: Registration,
        scheduler: Scheduler<Task>,
        notifier: Sender<TaskRes>,
        host: Arc<CoprocessorHost>,
        importer: Arc<SSTImporter>,
        use_delete_range: bool,
    ) -> ApplyDelegate {
        ApplyDelegate {
            id: reg.id,
            tag: format!("[region {}] {}", reg.region.get_id(), reg.id),
            engines,
            region: reg.region,
            pending_remove: false,
            apply_state: reg.apply_state,
            applied_index_term: reg.applied_index_term,
            term: reg.term,
            is_merging: false,
            pending_cmds: Default::default(),
            metrics: Default::default(),
            last_merge_version: 0,
            scheduler,
            notifier,
            host,
            importer,
            use_delete_range,
            stopping: false,
        }
    }

    pub fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn handle_raft_committed_entries(
        self,
        mut apply_ctx: ApplyContext,
        committed_entries: Vec<Entry>,
    ) -> Box<Future<Item = (Self, ApplyContext), Error = (Self, ApplyContext, Error)> + Send> {
        if committed_entries.is_empty() {
            return box future::ok((self, apply_ctx));
        }
        apply_ctx.prepare_for(&self);
        apply_ctx.committed_count += committed_entries.len();
        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        apply_ctx.committed_count += committed_entries.len();

        let f = futures::stream::iter_ok(committed_entries).fold(
            (self, apply_ctx),
            |(delegate, apply_ctx), entry| {
                if delegate.pending_remove {
                    let tag = delegate.tag.clone();
                    return box future::err((
                        delegate,
                        apply_ctx,
                        box_err!("{} delegate is pending remove", tag),
                    ))
                        as Box<Future<Item = (Self, ApplyContext), Error = FutureError> + Send>;
                }
                let expect_index = delegate.apply_state.get_applied_index() + 1;
                if expect_index != entry.get_index() {
                    panic!(
                        "{} expect index {}, but got {}",
                        delegate.tag,
                        expect_index,
                        entry.get_index()
                    );
                }
                box delegate.handle_raft_entry(apply_ctx, entry).and_then(
                    move |(delegate, mut apply_ctx, exec_result)| {
                        if let Some(res) = exec_result {
                            apply_ctx.push_exec_result(res);
                        }
                        future::ok((delegate, apply_ctx))
                    },
                )
            },
        );

        box f.then(move |res| {
            let (mut delegate, mut apply_ctx) = match res {
                Ok((delegate, apply_ctx)) => (delegate, apply_ctx),
                Err((delegate, apply_ctx, e)) => {
                    if let Error::Canceled = e {
                        return future::err((delegate, apply_ctx, e));
                    }
                    warn!("handle committed entries error: {:?}", e);
                    (delegate, apply_ctx)
                }
            };
            apply_ctx.finish_for(&mut delegate);
            future::ok((delegate, apply_ctx))
        })
    }

    fn update_metrics(&mut self, apply_ctx: &ApplyContext) {
        self.metrics.written_bytes += apply_ctx.delta_bytes();
        self.metrics.written_keys += apply_ctx.delta_keys();
    }

    fn write_apply_state(&self, wb: &WriteBatch) {
        rocksdb::get_cf_handle(&self.engines.kv, CF_RAFT)
            .map_err(From::from)
            .and_then(|handle| {
                wb.put_msg_cf(
                    handle,
                    &keys::apply_state_key(self.region.get_id()),
                    &self.apply_state,
                )
            })
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to save apply state to write batch, error: {:?}",
                    self.tag, e
                );
            });
    }

    fn handle_raft_entry(
        mut self,
        mut apply_ctx: ApplyContext,
        entry: Entry,
    ) -> Box<Future<Item = RaftEntryFutureItem, Error = FutureError> + Send> {
        let index = entry.get_index();
        let term = entry.get_term();
        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                let data = entry.get_data();
                if !data.is_empty() {
                    let cmd = util::parse_data_at(data, index, &self.tag);

                    if should_write_to_engine(&cmd, apply_ctx.wb().count()) {
                        apply_ctx.commit(&mut self);
                    }

                    return box self.process_raft_cmd(apply_ctx, index, term, cmd);
                }

                // when a peer become leader, it will send an empty entry.
                let mut state = self.apply_state.clone();
                state.set_applied_index(index);
                self.apply_state = state;
                self.applied_index_term = term;
                assert!(term > 0);
                while let Some(mut cmd) = self.pending_cmds.pop_normal(term - 1) {
                    // apprently, all the callbacks whose term is less than entry's term are stale.
                    apply_ctx
                        .cbs
                        .last_mut()
                        .unwrap()
                        .push(cmd.cb.take(), cmd_resp::err_resp(Error::StaleCommand, term));
                }

                box future::ok((self, apply_ctx, None))
            }
            EntryType::EntryConfChange => {
                let conf_change: ConfChange =
                    util::parse_data_at(entry.get_data(), index, &self.tag);
                let cmd = util::parse_data_at(conf_change.get_context(), index, &self.tag);
                box self.process_raft_cmd(apply_ctx, index, term, cmd).and_then(
                    move |(delegate, ctx, exec_result)| match exec_result {
                        Some(mut res) => {
                            if let ExecResult::ChangePeer(ref mut cp) = res {
                                cp.conf_change = conf_change;
                            } else {
                                panic!(
                                    "{} unexpected result {:?} for conf change {:?} at {}",
                                    delegate.tag, res, conf_change, index
                                );
                            }
                            future::ok((delegate, ctx, Some(res)))
                        }
                        None => future::ok((
                            delegate,
                            ctx,
                            Some(ExecResult::ChangePeer(Default::default())),
                        )),
                    },
                )
            }
        }
    }

    fn find_cb(&mut self, index: u64, term: u64, cmd: &RaftCmdRequest) -> Option<Callback> {
        if get_change_peer_cmd(cmd).is_some() {
            if let Some(mut cmd) = self.pending_cmds.take_conf_change() {
                if cmd.index == index && cmd.term == term {
                    return Some(cmd.cb.take().unwrap());
                } else {
                    notify_stale_command(&self.tag, self.term, cmd);
                }
            }
            return None;
        }
        while let Some(mut head) = self.pending_cmds.pop_normal(term) {
            if head.index == index && head.term == term {
                return Some(head.cb.take().unwrap());
            }
            // Because of the lack of original RaftCmdRequest, we skip calling
            // coprocessor here.
            notify_stale_command(&self.tag, self.term, head);
        }
        None
    }

    fn process_raft_cmd(
        mut self,
        mut apply_ctx: ApplyContext,
        index: u64,
        term: u64,
        cmd: RaftCmdRequest,
    ) -> impl Future<Item = RaftEntryFutureItem, Error = FutureError> {
        if index == 0 {
            panic!(
                "{} processing raft command needs a none zero index",
                self.tag
            );
        }

        if cmd.has_admin_request() {
            apply_ctx.sync_log_hint = true;
        }

        let cmd_cb = self.find_cb(index, term, &cmd);
        apply_ctx.host.pre_apply(&self.region, &cmd);

        let tag = self.tag.clone();
        self.apply_raft_cmd(apply_ctx, index, term, cmd).and_then(
            move |(delegate, mut ctx, mut resp, exec_result)| {
                debug!("{} applied command at log index {}", tag, index);

                // TODO: if we have exec_result, maybe we should return this callback too. Outer
                // store will call it after handing exec result.
                cmd_resp::bind_term(&mut resp, delegate.term);
                ctx.cbs.last_mut().unwrap().push(cmd_cb, resp);

                future::ok((delegate, ctx, exec_result))
            },
        )
    }

    // apply operation can fail as following situation:
    //   1. encouter an error that will occur on all store, it can continue
    // applying next entry safely, like stale epoch for example;
    //   2. encouter an error that may not occur on all store, in this case
    // we should try to apply the entry again or panic. Considering that this
    // usually due to disk operation fail, which is rare, so just panic is ok.
    fn apply_raft_cmd(
        self,
        mut ctx: ApplyContext,
        index: u64,
        term: u64,
        req: RaftCmdRequest,
    ) -> impl Future<Item = RaftCmdFutureItem, Error = FutureError> {
        // if pending remove, apply should be aborted already.
        assert!(!self.pending_remove);

        ctx.exec_ctx = Some(self.new_ctx(index, term, req));
        ctx.wb_mut().set_save_point();

        self.exec_raft_cmd(ctx)
            .then(move |res| match res {
                Ok((delegate, ctx, resp, exec_result)) => {
                    future::ok((delegate, ctx, resp, exec_result))
                }
                Err((delegate, mut ctx, e)) => {
                    // clear dirty values.
                    ctx.wb_mut().rollback_to_save_point().unwrap();
                    match e {
                        Error::StaleEpoch(..) => {
                            debug!("{} stale epoch err: {:?}", delegate.tag, e)
                        }
                        Error::Canceled => return future::err((delegate, ctx, e)),
                        _ => error!("{} execute raft command err: {:?}", delegate.tag, e),
                    }
                    future::ok((delegate, ctx, cmd_resp::new_error(e), None))
                }
            })
            .and_then(move |(mut delegate, mut ctx, resp, exec_result)| {
                let mut exec_ctx = ctx.exec_ctx.take().unwrap();
                exec_ctx.apply_state.set_applied_index(index);

                delegate.apply_state = exec_ctx.apply_state;
                delegate.applied_index_term = term;

                if let Some(ref exec_result) = exec_result {
                    match *exec_result {
                        ExecResult::ChangePeer(ref cp) => {
                            delegate.region = cp.region.clone();
                        }
                        ExecResult::ComputeHash { .. }
                        | ExecResult::VerifyHash { .. }
                        | ExecResult::CompactLog { .. }
                        | ExecResult::DeleteRange { .. }
                        | ExecResult::IngestSST { .. } => {}
                        ExecResult::SplitRegion {
                            ref left,
                            ref right,
                            right_derive,
                        } => {
                            if right_derive {
                                delegate.region = right.clone();
                            } else {
                                delegate.region = left.clone();
                            }
                            delegate.metrics.size_diff_hint = 0;
                            delegate.metrics.delete_keys_hint = 0;
                        }
                        ExecResult::PrepareMerge { ref region, .. } => {
                            delegate.region = region.clone();
                            delegate.is_merging = true;
                        }
                        ExecResult::CommitMerge {
                            ref region,
                            ref source,
                        } => {
                            delegate.region = region.clone();
                            delegate.last_merge_version = region.get_region_epoch().get_version();
                            ctx.merged_regions.push(source.get_id());
                        }
                        ExecResult::RollbackMerge { ref region, .. } => {
                            delegate.region = region.clone();
                            delegate.is_merging = false;
                        }
                    }
                }

                future::ok((delegate, ctx, resp, exec_result))
            })
    }

    /// Clear all the pending commands.
    ///
    /// Please note that all the pending callbacks will be lost.
    /// Should not do this when dropping a peer in case of possible leak.
    fn clear_pending_commands(&mut self) {
        if !self.pending_cmds.normals.is_empty() {
            info!(
                "{} clear {} commands",
                self.tag,
                self.pending_cmds.normals.len()
            );
            while let Some(mut cmd) = self.pending_cmds.normals.pop_front() {
                cmd.cb.take();
            }
        }
        if let Some(mut cmd) = self.pending_cmds.conf_change.take() {
            info!("{} clear pending conf change", self.tag);
            cmd.cb.take();
        }
    }

    fn destroy(&mut self) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
    }

    fn clear_all_commands_as_stale(&mut self) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_command(&self.tag, self.term, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_command(&self.tag, self.term, cmd);
        }
    }

    fn new_ctx(&self, index: u64, term: u64, req: RaftCmdRequest) -> ExecContext {
        ExecContext::new(self.apply_state.clone(), req, index, term)
    }
}

struct ExecContext {
    apply_state: RaftApplyState,
    req: RaftCmdRequest,
    index: u64,
    term: u64,
}

impl ExecContext {
    pub fn new(
        apply_state: RaftApplyState,
        req: RaftCmdRequest,
        index: u64,
        term: u64,
    ) -> ExecContext {
        ExecContext {
            apply_state,
            req,
            index,
            term,
        }
    }
}

// Here we implement all commands.
impl ApplyDelegate {
    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd(
        self,
        ctx: ApplyContext,
    ) -> Box<Future<Item = RaftCmdFutureItem, Error = FutureError> + Send> {
        let mut req = ctx.exec_ctx.as_ref().unwrap().req.clone();
        // Include region for stale epoch after merge may cause key not in range.
        let include_region =
            req.get_header().get_region_epoch().get_version() >= self.last_merge_version;
        if let Err(e) = check_region_epoch(&req, &self.region, include_region) {
            return box future::err((self, ctx, e));
        }

        if req.has_admin_request() {
            box self.exec_admin_cmd(ctx, req.take_admin_request())
        } else {
            box self.exec_write_cmd(ctx, req.take_requests().into_vec())
        }
    }

    fn exec_admin_cmd(
        self,
        ctx: ApplyContext,
        request: AdminRequest,
    ) -> impl Future<Item = RaftCmdFutureItem, Error = FutureError> {
        let cmd_type = request.get_cmd_type();
        info!(
            "{} execute admin command {:?} at [term: {}, index: {}]",
            self.tag,
            request,
            ctx.exec_ctx.as_ref().unwrap().term,
            ctx.exec_ctx.as_ref().unwrap().index
        );

        let exec_res: Box<Future<Item = AdminCmdFutureItem, Error = FutureError> + Send> =
            match cmd_type {
                AdminCmdType::ChangePeer => box self.exec_change_peer(ctx, &request),
                AdminCmdType::Split => box self.exec_split(ctx, &request),
                AdminCmdType::CompactLog => box self.exec_compact_log(ctx, &request),
                AdminCmdType::TransferLeader => {
                    box future::err((self, ctx, box_err!("transfer leader won't exec")))
                }
                AdminCmdType::ComputeHash => box self.exec_compute_hash(ctx, &request),
                AdminCmdType::VerifyHash => box self.exec_verify_hash(ctx, &request),
                // TODO: is it backward compatible to add new cmd_type?
                AdminCmdType::PrepareMerge => box self.exec_prepare_merge(ctx, &request),
                AdminCmdType::CommitMerge => box self.exec_commit_merge(ctx, &request),
                AdminCmdType::RollbackMerge => box self.exec_rollback_merge(ctx, &request),
                AdminCmdType::InvalidAdmin => {
                    box future::err((self, ctx, box_err!("unsupported admin command type")))
                }
            };
        exec_res.then(move |res| match res {
            Ok((delegate, ctx, mut response, exec_result)) => {
                response.set_cmd_type(cmd_type);
                let mut resp = RaftCmdResponse::new();
                let uuid = ctx
                    .exec_ctx
                    .as_ref()
                    .unwrap()
                    .req
                    .get_header()
                    .get_uuid()
                    .to_vec();
                resp.mut_header().set_uuid(uuid);
                resp.set_admin_response(response);
                future::ok((delegate, ctx, resp, exec_result))
            }
            Err((delegate, ctx, e)) => future::err((delegate, ctx, e)),
        })
    }

    fn exec_change_peer(
        mut self,
        mut ctx: ApplyContext,
        request: &AdminRequest,
    ) -> future::FutureResult<AdminCmdFutureItem, FutureError> {
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let store_id = peer.get_store_id();
        let change_type = request.get_change_type();
        let mut region = self.region.clone();

        info!(
            "{} exec ConfChange {:?}, epoch: {:?}",
            self.tag,
            util::conf_change_type_str(change_type),
            region.get_region_epoch()
        );

        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let conf_ver = region.get_region_epoch().get_conf_ver() + 1;
        region.mut_region_epoch().set_conf_ver(conf_ver);

        match change_type {
            ConfChangeType::AddNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_peer", "all"])
                    .inc();

                let mut exists = false;
                if let Some(p) = util::find_peer_mut(&mut region, store_id) {
                    exists = true;
                    if !p.get_is_learner() || p.get_id() != peer.get_id() {
                        error!(
                            "{} can't add duplicated peer {:?} to region {:?}",
                            self.tag, peer, self.region
                        );
                        let region = self.region.clone();
                        return future::err((
                            self,
                            ctx,
                            box_err!(
                                "can't add duplicated peer {:?} to region {:?}",
                                peer,
                                region
                            ),
                        ));
                    } else {
                        p.set_is_learner(false);
                    }
                }
                if !exists {
                    // TODO: Do we allow adding peer in same node?
                    region.mut_peers().push(peer.clone());
                }

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_peer", "success"])
                    .inc();
                info!(
                    "{} add peer {:?} to region {:?}",
                    self.tag, peer, self.region
                );
            }
            ConfChangeType::RemoveNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["remove_peer", "all"])
                    .inc();

                if let Some(p) = util::remove_peer(&mut region, store_id) {
                    // Considering `is_learner` flag in `Peer` here is by design.
                    if &p != peer {
                        error!(
                            "{} remove unmatched peer: expect: {:?}, get {:?}, ignore",
                            self.tag, peer, p
                        );
                        return future::err((
                            self,
                            ctx,
                            box_err!(
                                "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                                peer,
                                p
                            ),
                        ));
                    }
                    if self.id == peer.get_id() {
                        // Remove ourself, we will destroy all region data later.
                        // So we need not to apply following logs.
                        self.pending_remove = true;
                    }
                } else {
                    error!(
                        "{} remove missing peer {:?} from region {:?}",
                        self.tag, peer, self.region
                    );
                    let region = self.region.clone();
                    return future::err((
                        self,
                        ctx,
                        box_err!("remove missing peer {:?} from region {:?}", peer, region),
                    ));
                }

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["remove_peer", "success"])
                    .inc();
                info!(
                    "{} remove {} from region:{:?}",
                    self.tag,
                    peer.get_id(),
                    self.region
                );
            }
            ConfChangeType::AddLearnerNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "all"])
                    .inc();

                if util::find_peer(&region, store_id).is_some() {
                    error!(
                        "{} can't add duplicated learner {:?} to region {:?}",
                        self.tag, peer, self.region
                    );
                    let region = self.region.clone();
                    return future::err((
                        self,
                        ctx,
                        box_err!(
                            "can't add duplicated learner {:?} to region {:?}",
                            peer,
                            region
                        ),
                    ));
                }
                region.mut_peers().push(peer.clone());

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "success"])
                    .inc();
                info!(
                    "{} add learner {:?} to region {:?}",
                    self.tag, peer, self.region
                );
            }
        }

        let state = if self.pending_remove {
            PeerState::Tombstone
        } else {
            PeerState::Normal
        };
        if let Err(e) = write_peer_state(&self.engines.kv, ctx.wb_mut(), &region, state, None) {
            panic!("{} failed to update region state: {:?}", self.tag, e);
        }

        let mut resp = AdminResponse::new();
        resp.mut_change_peer().set_region(region.clone());

        future::ok((
            self,
            ctx,
            resp,
            Some(ExecResult::ChangePeer(ChangePeer {
                conf_change: Default::default(),
                peer: peer.clone(),
                region,
            })),
        ))
    }

    fn exec_split(
        self,
        mut ctx: ApplyContext,
        req: &AdminRequest,
    ) -> future::FutureResult<AdminCmdFutureItem, FutureError> {
        let _id = self.id;
        let _region_id = self.region_id();
        let apply_before_split = || {
            fail_point!(
                "apply_before_split_1_3",
                { _id == 3 && _region_id == 1 },
                |_| {}
            );
        };
        apply_before_split();

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["split", "all"])
            .inc();

        let split_req = req.get_split();
        let right_derive = split_req.get_right_derive();
        if split_req.get_split_key().is_empty() {
            return future::err((self, ctx, box_err!("missing split key")));
        }

        let split_key = split_req.get_split_key();
        let mut region = self.region.clone();
        if split_key <= region.get_start_key() {
            return future::err((
                self,
                ctx,
                box_err!("invalid split request: {:?}", split_req),
            ));
        }

        if let Err(e) = util::check_key_in_region(split_key, &region) {
            return future::err((self, ctx, e));
        }

        info!(
            "{} split at key: {}, region: {:?}",
            self.tag,
            escape(split_key),
            region
        );

        // TODO: check new region id validation.
        let new_region_id = split_req.get_new_region_id();

        // After split, the left region key range is [start_key, split_key),
        // the right region is [split_key, end).
        let mut new_region = region.clone();
        new_region.set_id(new_region_id);
        if right_derive {
            region.set_start_key(split_key.to_vec());
            new_region.set_end_key(split_key.to_vec());
        } else {
            region.set_end_key(split_key.to_vec());
            new_region.set_start_key(split_key.to_vec());
        }

        // Update new region peer ids.
        let new_peer_ids = split_req.get_new_peer_ids();
        if new_peer_ids.len() != new_region.get_peers().len() {
            return future::err((
                self,
                ctx,
                box_err!(
                    "invalid new peer id count, need {}, but got {}",
                    new_region.get_peers().len(),
                    new_peer_ids.len()
                ),
            ));
        }

        for (peer, &peer_id) in new_region.mut_peers().iter_mut().zip(new_peer_ids) {
            peer.set_id(peer_id);
        }

        // update region version
        let region_ver = region.get_region_epoch().get_version() + 1;
        region.mut_region_epoch().set_version(region_ver);
        new_region.mut_region_epoch().set_version(region_ver);
        write_peer_state(
            &self.engines.kv,
            ctx.wb_mut(),
            &region,
            PeerState::Normal,
            None,
        ).and_then(|_| {
            write_peer_state(
                &self.engines.kv,
                ctx.wb_mut(),
                &new_region,
                PeerState::Normal,
                None,
            )
        })
            .and_then(|_| {
                write_initial_apply_state(&self.engines.kv, ctx.wb_mut(), new_region.get_id())
            })
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to save split region {:?}: {:?}",
                    self.tag, new_region, e
                )
            });

        let mut resp = AdminResponse::new();
        if right_derive {
            resp.mut_split().set_left(new_region.clone());
            resp.mut_split().set_right(region.clone());
        } else {
            resp.mut_split().set_left(region.clone());
            resp.mut_split().set_right(new_region.clone());
        }

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["split", "success"])
            .inc();

        if right_derive {
            future::ok((
                self,
                ctx,
                resp,
                Some(ExecResult::SplitRegion {
                    left: new_region,
                    right: region,
                    right_derive: true,
                }),
            ))
        } else {
            future::ok((
                self,
                ctx,
                resp,
                Some(ExecResult::SplitRegion {
                    left: region,
                    right: new_region,
                    right_derive: false,
                }),
            ))
        }
    }

    fn exec_prepare_merge(
        self,
        ctx: ApplyContext,
        req: &AdminRequest,
    ) -> future::FutureResult<AdminCmdFutureItem, FutureError> {
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["prepare_merge", "all"])
            .inc();

        let (region, merging_state) = {
            let prepare_merge = req.get_prepare_merge();
            let index = prepare_merge.get_min_index();
            let exec_ctx = ctx.exec_ctx.as_ref().unwrap();
            let first_index = peer_storage::first_index(&exec_ctx.apply_state);
            if index < first_index {
                // We filter `CompactLog` command before.
                panic!(
                    "{} first index {} > min_index {}, skip pre merge.",
                    self.tag, first_index, index
                );
            }
            let mut region = self.region.clone();
            let region_version = region.get_region_epoch().get_version() + 1;
            region.mut_region_epoch().set_version(region_version);
            // In theory conf version should not be increased when executing prepare_merge.
            // However, we don't want to do conf change after prepare_merge is committed.
            // This can also be done by iterating all proposal to find if prepare_merge is
            // proposed before proposing conf change, but it make things complicated.
            // Another way is make conf change also check region version, but this is not
            // backward compatible.
            let conf_version = region.get_region_epoch().get_conf_ver() + 1;
            region.mut_region_epoch().set_conf_ver(conf_version);
            let mut merging_state = MergeState::new();
            merging_state.set_min_index(index);
            merging_state.set_target(prepare_merge.get_target().to_owned());
            merging_state.set_commit(exec_ctx.index);
            write_peer_state(
                &self.engines.kv,
                ctx.wb(),
                &region,
                PeerState::Merging,
                Some(merging_state.clone()),
            ).unwrap_or_else(|e| {
                panic!(
                    "{} failed to save merging state {:?} for region {:?}: {:?}",
                    self.tag, merging_state, region, e
                )
            });

            (region, merging_state)
        };

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["prepare_merge", "success"])
            .inc();

        future::ok((
            self,
            ctx,
            AdminResponse::new(),
            Some(ExecResult::PrepareMerge {
                region,
                state: merging_state,
            }),
        ))
    }

    fn load_entries_for_merge(&self, merge: &CommitMergeRequest, apply_index: u64) -> Vec<Entry> {
        // Entries from [first_index, last_index) need to be loaded.
        let first_index = apply_index + 1;
        let last_index = merge.get_commit() + 1;
        if first_index >= last_index {
            return vec![];
        }
        let exist_first_index = merge
            .get_entries()
            .get(0)
            .map_or(last_index, |e| e.get_index());
        if first_index >= exist_first_index {
            return merge.get_entries()[(first_index - exist_first_index) as usize..].to_vec();
        }
        let source_region = merge.get_source();
        let mut entries = Vec::with_capacity((last_index - first_index) as usize);
        peer_storage::fetch_entries_to(
            &self.engines.raft,
            source_region.get_id(),
            first_index,
            exist_first_index,
            NO_LIMIT,
            &mut entries,
        ).unwrap_or_else(|e| {
            panic!(
                "{} failed to load entries [{}:{}) from region {}: {:?}",
                self.tag,
                first_index,
                exist_first_index,
                source_region.get_id(),
                e
            );
        });
        entries.extend_from_slice(merge.get_entries());
        entries
    }

    fn wait_source_apply_to(
        self,
        merge: CommitMergeRequest,
        exist_region: Region,
    ) -> impl Future<Item = (Self, Region), Error = (Self, Error)> {
        let region_id = exist_region.get_id();
        let (tx, rx) = oneshot::channel();

        // Send message to the source region first.
        self.scheduler
            .schedule(Task::apply_to(region_id, merge.clone(), tx))
            .unwrap();

        // Continue after the source region has applied to the specified position.
        rx.then(move |res| match res {
            Ok(region) => future::ok((self, region)),
            _e @ Err(oneshot::Canceled) => future::err((self, Error::Canceled)),
        })
    }

    fn exec_commit_merge(
        self,
        ctx: ApplyContext,
        req: &AdminRequest,
    ) -> impl Future<Item = AdminCmdFutureItem, Error = FutureError> {
        {
            let apply_before_commit_merge = || {
                fail_point!(
                    "apply_before_commit_merge_except_1_4",
                    { self.region_id() == 1 && self.id != 4 },
                    |_| {}
                );
            };
            apply_before_commit_merge();
        }

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["commit_merge", "all"])
            .inc();

        let merge = req.get_commit_merge().to_owned();
        let source_region = merge.get_source().to_owned();
        let region_state_key = keys::region_state_key(source_region.get_id());
        let state: RegionLocalState = match self.engines.kv.get_msg_cf(CF_RAFT, &region_state_key) {
            Ok(Some(s)) => s,
            e => panic!(
                "{} failed to get regions state of {:?}: {:?}",
                self.tag, source_region, e
            ),
        };
        match state.get_state() {
            PeerState::Normal | PeerState::Merging => {}
            _ => panic!(
                "{} unexpected state of merging region {:?}",
                self.tag, state
            ),
        }
        let exist_region = state.get_region().to_owned();
        self.wait_source_apply_to(merge, exist_region)
            .then(move |res| match res {
                Ok((delegate, exist_region)) => {
                    if source_region != exist_region {
                        panic!(
                            "{} source_region {:?} not match exist region {:?}",
                            delegate.tag, source_region, exist_region
                        );
                    }

                    let target_region_before_merge = delegate.region.clone();
                    let mut region = delegate.region.clone();
                    // Use a max value so that pd can ensure overlapped region has a priority.
                    let version = cmp::max(
                        source_region.get_region_epoch().get_version(),
                        region.get_region_epoch().get_version(),
                    ) + 1;
                    region.mut_region_epoch().set_version(version);
                    if keys::enc_end_key(&region) == keys::enc_start_key(&source_region) {
                        region.set_end_key(source_region.get_end_key().to_vec());
                    } else {
                        region.set_start_key(source_region.get_start_key().to_vec());
                    }
                    write_peer_state(
                        &delegate.engines.kv,
                        ctx.wb(),
                        &region,
                        PeerState::Normal,
                        None,
                    ).and_then(|_| {
                        // TODO: maybe all information needs to be filled?
                        let mut merging_state = MergeState::new();
                        merging_state.set_target(target_region_before_merge);
                        write_peer_state(
                            &delegate.engines.kv,
                            ctx.wb(),
                            &source_region,
                            PeerState::Tombstone,
                            Some(merging_state),
                        )
                    })
                        .unwrap_or_else(|e| {
                            panic!(
                                "{} failed to save merge region {:?}: {:?}",
                                delegate.tag, region, e
                            )
                        });

                    PEER_ADMIN_CMD_COUNTER_VEC
                        .with_label_values(&["commit_merge", "success"])
                        .inc();

                    let resp = AdminResponse::new();
                    future::ok((
                        delegate,
                        ctx,
                        resp,
                        Some(ExecResult::CommitMerge {
                            region,
                            source: source_region.to_owned(),
                        }),
                    ))
                }
                Err((delegate, e)) => future::err((delegate, ctx, e)),
            })
    }

    fn exec_rollback_merge(
        self,
        ctx: ApplyContext,
        req: &AdminRequest,
    ) -> future::FutureResult<AdminCmdFutureItem, FutureError> {
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["rollback_merge", "all"])
            .inc();
        let region_state_key = keys::region_state_key(self.region_id());
        let state: RegionLocalState = match self.engines.kv.get_msg_cf(CF_RAFT, &region_state_key) {
            Ok(Some(s)) => s,
            e => panic!("{} failed to get regions state: {:?}", self.tag, e),
        };
        assert_eq!(state.get_state(), PeerState::Merging, "{}", self.tag);
        let rollback = req.get_rollback_merge();
        assert_eq!(
            state.get_merge_state().get_commit(),
            rollback.get_commit(),
            "{}",
            self.tag
        );
        let mut region = self.region.clone();
        let version = region.get_region_epoch().get_version();
        // Update version to avoid duplicated rollback requests.
        region.mut_region_epoch().set_version(version + 1);
        write_peer_state(&self.engines.kv, ctx.wb(), &region, PeerState::Normal, None)
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to rollback merge {:?}: {:?}",
                    self.tag, rollback, e
                )
            });

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["rollback_merge", "success"])
            .inc();
        let resp = AdminResponse::new();
        future::ok((
            self,
            ctx,
            resp,
            Some(ExecResult::RollbackMerge {
                region,
                commit: rollback.get_commit(),
            }),
        ))
    }

    fn exec_compact_log(
        self,
        mut ctx: ApplyContext,
        req: &AdminRequest,
    ) -> future::FutureResult<AdminCmdFutureItem, FutureError> {
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["compact", "all"])
            .inc();

        let first_index = {
            let apply_state = &ctx.exec_ctx.as_mut().unwrap().apply_state;
            peer_storage::first_index(apply_state)
        };

        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::new();
        if compact_index <= first_index {
            debug!(
                "{} compact index {} <= first index {}, no need to compact",
                self.tag, compact_index, first_index
            );
            return future::ok((self, ctx, resp, None));
        }
        if self.is_merging {
            info!(
                "{} is in merging mode, skip compact {}",
                self.tag, compact_index
            );
            return future::ok((self, ctx, resp, None));
        }

        let compact_term = req.get_compact_log().get_compact_term();
        // TODO: add unit tests to cover all the message integrity checks.
        if compact_term == 0 {
            info!(
                "{} compact term missing in {:?}, skip.",
                self.tag,
                req.get_compact_log()
            );
            // old format compact log command, safe to ignore.
            return future::err((
                self,
                ctx,
                box_err!("command format is outdated, please upgrade leader."),
            ));
        }

        // compact failure is safe to be omitted, no need to assert.
        let compact_raft_log_res = {
            let apply_state = &mut ctx.exec_ctx.as_mut().unwrap().apply_state;
            compact_raft_log(&self.tag, apply_state, compact_index, compact_term)
        };
        if let Err(e) = compact_raft_log_res {
            return future::err((self, ctx, e));
        }

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["compact", "success"])
            .inc();

        let state = {
            let apply_state = &ctx.exec_ctx.as_mut().unwrap().apply_state;
            apply_state.get_truncated_state().clone()
        };
        future::ok((
            self,
            ctx,
            resp,
            Some(ExecResult::CompactLog { state, first_index }),
        ))
    }

    fn exec_write_cmd(
        mut self,
        ctx: ApplyContext,
        requests: Vec<Request>,
    ) -> impl Future<Item = RaftCmdFutureItem, Error = FutureError> {
        let mut responses = Vec::with_capacity(requests.len());

        let mut ranges = vec![];
        let mut ssts = vec![];
        for req in &requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Put => self.handle_put(&ctx, req),
                CmdType::Delete => self.handle_delete(&ctx, req),
                CmdType::DeleteRange => self.handle_delete_range(req, &mut ranges),
                CmdType::IngestSST => self.handle_ingest_sst(&ctx, req, &mut ssts),
                // Readonly commands are handled in raftstore directly.
                // Don't panic here in case there are old entries need to be applied.
                // It's also safe to skip them here, because a restart must have happened,
                // hence there is no callback to be called.
                CmdType::Snap | CmdType::Get => {
                    warn!("{} skip readonly command: {:?}", self.tag, req);
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid => {
                    Err(box_err!("invalid cmd type, message maybe currupted"))
                }
            };

            let mut resp = match resp {
                Ok(r) => r,
                Err(e) => return future::err((self, ctx, e)),
            };
            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        let uuid = ctx
            .exec_ctx
            .as_ref()
            .unwrap()
            .req
            .get_header()
            .get_uuid()
            .to_vec();
        resp.mut_header().set_uuid(uuid);
        resp.set_responses(RepeatedField::from_vec(responses));

        assert!(ranges.is_empty() || ssts.is_empty());
        let exec_res = if !ranges.is_empty() {
            Some(ExecResult::DeleteRange { ranges })
        } else if !ssts.is_empty() {
            Some(ExecResult::IngestSST { ssts })
        } else {
            None
        };

        future::ok((self, ctx, resp, exec_res))
    }

    fn handle_put(&mut self, ctx: &ApplyContext, req: &Request) -> Result<Response> {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, &self.region)?;

        let resp = Response::new();
        let key = keys::data_key(key);
        self.metrics.size_diff_hint += key.len() as i64;
        self.metrics.size_diff_hint += value.len() as i64;
        if !req.get_put().get_cf().is_empty() {
            let cf = req.get_put().get_cf();
            // TODO: don't allow write preseved cfs.
            if cf == CF_LOCK {
                self.metrics.lock_cf_written_bytes += key.len() as u64;
                self.metrics.lock_cf_written_bytes += value.len() as u64;
            }
            // TODO: check whether cf exists or not.
            rocksdb::get_cf_handle(&self.engines.kv, cf)
                .and_then(|handle| ctx.wb().put_cf(handle, &key, value))
                .unwrap_or_else(|e| {
                    panic!(
                        "{} failed to write ({}, {}) to cf {}: {:?}",
                        self.tag,
                        escape(&key),
                        escape(value),
                        cf,
                        e
                    )
                });
        } else {
            ctx.wb().put(&key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}): {:?}",
                    self.tag,
                    escape(&key),
                    escape(value),
                    e
                );
            });
        }
        Ok(resp)
    }

    fn handle_delete(&mut self, ctx: &ApplyContext, req: &Request) -> Result<Response> {
        let key = req.get_delete().get_key();
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, &self.region)?;

        let key = keys::data_key(key);
        // since size_diff_hint is not accurate, so we just skip calculate the value size.
        self.metrics.size_diff_hint -= key.len() as i64;
        let resp = Response::new();
        if !req.get_delete().get_cf().is_empty() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            rocksdb::get_cf_handle(&self.engines.kv, cf)
                .and_then(|handle| ctx.wb().delete_cf(handle, &key))
                .unwrap_or_else(|e| {
                    panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
                });

            if cf == CF_LOCK {
                // delete is a kind of write for RocksDB.
                self.metrics.lock_cf_written_bytes += key.len() as u64;
            } else {
                self.metrics.delete_keys_hint += 1;
            }
        } else {
            ctx.wb().delete(&key).unwrap_or_else(|e| {
                panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
            });
            self.metrics.delete_keys_hint += 1;
        }

        Ok(resp)
    }

    fn handle_delete_range(&mut self, req: &Request, ranges: &mut Vec<Range>) -> Result<Response> {
        let s_key = req.get_delete_range().get_start_key();
        let e_key = req.get_delete_range().get_end_key();
        if !e_key.is_empty() && s_key >= e_key {
            return Err(box_err!(
                "invalid delete range command, start_key: {:?}, end_key: {:?}",
                s_key,
                e_key
            ));
        }
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(s_key, &self.region)?;
        let end_key = keys::data_end_key(e_key);
        let region_end_key = keys::data_end_key(self.region.get_end_key());
        if end_key > region_end_key {
            return Err(Error::KeyNotInRegion(e_key.to_vec(), self.region.clone()));
        }

        let resp = Response::new();
        let mut cf = req.get_delete_range().get_cf();
        if cf.is_empty() {
            cf = CF_DEFAULT;
        }
        if ALL_CFS.iter().find(|x| **x == cf).is_none() {
            return Err(box_err!("invalid delete range command, cf: {:?}", cf));
        }
        let handle = rocksdb::get_cf_handle(&self.engines.kv, cf).unwrap();

        let start_key = keys::data_key(s_key);
        // Use delete_files_in_range to drop as many sst files as possible, this
        // is a way to reclaim disk space quickly after drop a table/index.
        self.engines
            .kv
            .delete_files_in_range_cf(handle, &start_key, &end_key, /* include_end */ false)
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete files in range [{}, {}): {:?}",
                    self.tag,
                    escape(&start_key),
                    escape(&end_key),
                    e
                )
            });

        // Delete all remaining keys.
        // If it's not CF_LOCK and use_delete_range is false, skip this step to speed up (#3034)
        util::delete_all_in_range_cf(
            &self.engines.kv,
            cf,
            &start_key,
            &end_key,
            self.use_delete_range,
        ).unwrap_or_else(|e| {
            panic!(
                "{} failed to delete all in range [{}, {}), cf: {}, err: {:?}",
                self.tag,
                escape(&start_key),
                escape(&end_key),
                cf,
                e
            );
        });

        ranges.push(Range::new(cf.to_owned(), start_key, end_key));

        Ok(resp)
    }

    fn handle_ingest_sst(
        &mut self,
        ctx: &ApplyContext,
        req: &Request,
        ssts: &mut Vec<SSTMeta>,
    ) -> Result<Response> {
        let sst = req.get_ingest_sst().get_sst();

        if let Err(e) = check_sst_for_ingestion(sst, &self.region) {
            error!("ingest {:?} to region {:?}: {:?}", sst, self.region, e);
            // This file is not valid, we can delete it here.
            let _ = ctx.importer.delete(sst);
            return Err(e);
        }

        ctx.importer
            .ingest(sst, &self.engines.kv)
            .unwrap_or_else(|e| {
                // If this failed, it means that the file is corrupted or something
                // is wrong with the engine, but we can do nothing about that.
                panic!("{} ingest {:?}: {:?}", self.tag, sst, e);
            });

        ssts.push(sst.clone());
        Ok(Response::new())
    }
}

pub fn get_change_peer_cmd(msg: &RaftCmdRequest) -> Option<&ChangePeerRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_change_peer() {
        return None;
    }

    Some(req.get_change_peer())
}

fn check_sst_for_ingestion(sst: &SSTMeta, region: &Region) -> Result<()> {
    let uuid = sst.get_uuid();
    if let Err(e) = Uuid::from_bytes(uuid) {
        return Err(box_err!("invalid uuid {:?}: {:?}", uuid, e));
    }

    let cf_name = sst.get_cf_name();
    if cf_name != CF_DEFAULT && cf_name != CF_WRITE {
        return Err(box_err!("invalid cf name {}", cf_name));
    }

    let region_id = sst.get_region_id();
    if region_id != region.get_id() {
        return Err(Error::RegionNotFound(region_id));
    }

    let epoch = sst.get_region_epoch();
    let region_epoch = region.get_region_epoch();
    if epoch.get_conf_ver() != region_epoch.get_conf_ver()
        || epoch.get_version() != region_epoch.get_version()
    {
        let error = format!("{:?} != {:?}", epoch, region_epoch);
        return Err(Error::StaleEpoch(error, vec![region.clone()]));
    }

    let range = sst.get_range();
    util::check_key_in_region(range.get_start(), region)?;
    util::check_key_in_region(range.get_end(), region)?;

    Ok(())
}

// Consistency Check
impl ApplyDelegate {
    fn exec_compute_hash(
        self,
        ctx: ApplyContext,
        _: &AdminRequest,
    ) -> future::FutureResult<AdminCmdFutureItem, FutureError> {
        let resp = AdminResponse::new();
        let region = self.region.clone();
        let index = ctx.exec_ctx.as_ref().unwrap().index;
        let kv_engine = Arc::clone(&self.engines.kv);
        future::ok((
            self,
            ctx,
            resp,
            Some(ExecResult::ComputeHash {
                region,
                index,
                // This snapshot may be held for a long time, which may cause too many
                // open files in rocksdb.
                // TODO: figure out another way to do consistency check without snapshot
                // or short life snapshot.
                snap: Snapshot::new(kv_engine),
            }),
        ))
    }

    fn exec_verify_hash(
        self,
        ctx: ApplyContext,
        req: &AdminRequest,
    ) -> future::FutureResult<AdminCmdFutureItem, FutureError> {
        let verify_req = req.get_verify_hash();
        let index = verify_req.get_index();
        let hash = verify_req.get_hash().to_vec();
        let resp = AdminResponse::new();
        future::ok((
            self,
            ctx,
            resp,
            Some(ExecResult::VerifyHash { index, hash }),
        ))
    }
}

pub struct Apply {
    region_id: u64,
    term: u64,
    entries: Vec<Entry>,
}

impl Apply {
    pub fn new(region_id: u64, term: u64, entries: Vec<Entry>) -> Apply {
        Apply {
            region_id,
            term,
            entries,
        }
    }
}

#[derive(Default, Clone)]
pub struct Registration {
    pub id: u64,
    pub term: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub region: Region,
}

impl Registration {
    pub fn new(peer: &Peer) -> Registration {
        Registration {
            id: peer.peer_id(),
            term: peer.term(),
            apply_state: peer.get_store().apply_state.clone(),
            applied_index_term: peer.get_store().applied_index_term,
            region: peer.region().clone(),
        }
    }
}

pub struct Proposal {
    is_conf_change: bool,
    index: u64,
    term: u64,
    pub cb: Callback,
}

impl Proposal {
    pub fn new(is_conf_change: bool, index: u64, term: u64, cb: Callback) -> Proposal {
        Proposal {
            is_conf_change,
            index,
            term,
            cb,
        }
    }
}

pub struct RegionProposal {
    id: u64,
    region_id: u64,
    props: Vec<Proposal>,
}

impl RegionProposal {
    pub fn new(id: u64, region_id: u64, props: Vec<Proposal>) -> RegionProposal {
        RegionProposal {
            id,
            region_id,
            props,
        }
    }
}

pub struct ApplyBatch {
    vec: Vec<Apply>,
    start: Instant,
}

pub struct AppliesRes {
    delegates: Vec<ApplyDelegate>,
    merged_regions: Vec<u64>,
    apply_res: Vec<ApplyRes>,
}

/// When an existing region receive an registration message, it
/// should update to the latest registration.
pub struct Update {
    pub region_id: u64,
    pub new_delegate: ApplyDelegate,
}

/// When two regions are going to merge, at the commit_merge stage,
/// the target region will send a `SourceMustApplyTo` message to
/// the source region and wait util the source region apply to the
/// specified position.
pub struct SourceMustApplyTo {
    pub source_region: u64,

    pub merge_req: CommitMergeRequest,

    // The source region will be send back to the target region
    // throw this sender.
    pub tx: oneshot::Sender<Region>,
}

/// RegionTask represents tasks received by region. They are send from
/// the apply scheduler thread.
pub enum RegionTask {
    Apply((Apply, Instant)),
    Destroy(u64),
    Proposal(RegionProposal),

    Update(Update),
    ApplyTo(SourceMustApplyTo),
    Stop((u64, Sender<()>)),
}

impl RegionTask {
    pub fn apply(apply: Apply, start: Instant) -> RegionTask {
        RegionTask::Apply((apply, start))
    }

    pub fn proposal(proposal: RegionProposal) -> RegionTask {
        RegionTask::Proposal(proposal)
    }

    pub fn destroy(region_id: u64) -> RegionTask {
        RegionTask::Destroy(region_id)
    }

    pub fn update(region_id: u64, new_delegate: ApplyDelegate) -> RegionTask {
        RegionTask::Update(Update {
            region_id,
            new_delegate,
        })
    }
}

pub struct Destroy {
    pub region_id: u64,
}

/// Task represents tasks received by the apply scheduler thread. These
/// tasks are mainly send from RaftStore thread.
pub enum Task {
    // Applies, Registration, Proposals and Destroy are tasks send
    // from the RaftStore thread.
    Applies(ApplyBatch),
    Registration(Registration),
    Proposals(Vec<RegionProposal>),
    Destroy(Destroy),

    ApplyTo(SourceMustApplyTo),
}

impl Task {
    pub fn applies(applies: Vec<Apply>) -> Task {
        Task::Applies(ApplyBatch {
            vec: applies,
            start: Instant::now_coarse(),
        })
    }

    pub fn register(peer: &Peer) -> Task {
        Task::Registration(Registration::new(peer))
    }

    pub fn destroy(region_id: u64) -> Task {
        Task::Destroy(Destroy { region_id })
    }

    pub fn apply_to(
        source_region: u64,
        merge_req: CommitMergeRequest,
        tx: oneshot::Sender<Region>,
    ) -> Task {
        Task::ApplyTo(SourceMustApplyTo {
            source_region,
            merge_req,
            tx,
        })
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Applies(ref a) => write!(f, "async applies count {}", a.vec.len()),
            Task::Proposals(ref p) => write!(f, "region proposal count {}", p.len()),
            Task::Registration(ref r) => {
                write!(f, "[region {}] Reg {:?}", r.region.get_id(), r.apply_state)
            }
            Task::Destroy(ref d) => write!(f, "[region {}] destroy", d.region_id),
            Task::ApplyTo(ref a) => write!(
                f,
                "wait source region [{}] apply to [{}]",
                a.source_region,
                a.merge_req.get_commit()
            ),
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct ApplyMetrics {
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: i64,
    /// delete keys' count since last reset.
    pub delete_keys_hint: u64,

    pub written_bytes: u64,
    pub written_keys: u64,
    pub lock_cf_written_bytes: u64,
}

#[derive(Debug)]
pub struct ApplyRes {
    pub region_id: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub exec_res: Vec<ExecResult>,
    pub metrics: ApplyMetrics,
    pub merged: bool,
}

#[derive(Debug)]
pub enum TaskRes {
    Applys(Vec<ApplyRes>),
    Destroy(ApplyDelegate),
}

#[derive(Debug)]
struct PoolContext {}

impl FuturePoolContext for PoolContext {}

pub struct Runner {
    engines: Engines,
    host: Arc<CoprocessorHost>,
    importer: Arc<SSTImporter>,
    notifier: Sender<TaskRes>,
    sync_log: bool,
    use_delete_range: bool,
    tag: String,

    apply_pool: FuturePool<PoolContext>,
    apply_scheduler: Scheduler<Task>,
    region_task_senders: HashMap<u64, UnboundedSender<RegionTask>>,
}

impl Runner {
    pub fn new<T, C>(
        store: &Store<T, C>,
        notifier: Sender<TaskRes>,
        sync_log: bool,
        use_delete_range: bool,
        apply_pool_size: usize,
        apply_scheduler: Scheduler<Task>,
    ) -> Runner {
        // All apply tasks will execute in this FuturePool.
        let apply_pool = FuturePool::new(
            apply_pool_size,
            STACK_SIZE,
            "apply-pool",
            Duration::from_secs(1),
            move || PoolContext {},
        );

        // Each region has a channel to receive apply tasks.
        // Apply tasks will send to scheduler thread first, and then the scheduler
        // thread dispatch these tasks to related regions throw `region_task_senders`.
        let mut region_task_senders =
            HashMap::with_capacity_and_hasher(store.get_peers().len(), Default::default());
        for (&region_id, p) in store.get_peers() {
            let (tx, rx) = unbounded();
            assert!(region_task_senders.insert(region_id, tx).is_none());

            let mut delegate = ApplyDelegate::from_peer(
                p,
                apply_scheduler.clone(),
                notifier.clone(),
                Arc::clone(&store.coprocessor_host),
                Arc::clone(&store.importer),
                use_delete_range,
            );

            // Spawn an endless future in the pool for each region.
            Self::spawn_region(&apply_pool, delegate, rx);
        }

        Runner {
            engines: store.engines(),
            host: Arc::clone(&store.coprocessor_host),
            importer: Arc::clone(&store.importer),
            notifier,
            sync_log,
            use_delete_range,
            tag: format!("[store {}]", store.store_id()),
            apply_pool,
            apply_scheduler,
            region_task_senders,
        }
    }

    fn handle_proposals(&mut self, proposals: Vec<RegionProposal>) {
        let mut propose_num = 0;
        for mut region_proposal in proposals {
            propose_num += region_proposal.props.len();

            let mut removed = false;
            let mut stale_proposal = None;
            let region_id = region_proposal.region_id;
            match self.region_task_senders.get(&region_id) {
                // Try to dispatch the proposal message to related region.
                Some(tx) => {
                    tx.unbounded_send(RegionTask::proposal(region_proposal))
                        .unwrap_or_else(|e| {
                            warn!("Send proposal task to {} failed, err {:?}, it is a stale proposal.", region_id, e);
                            removed = true;
                            let task = e.into_inner();
                            if let RegionTask::Proposal(p) = task {
                                stale_proposal = Some(p);
                            } else {
                                panic!("Proposal task is expected.")
                            }
                        });
                }
                // These is no related sender, must be a stale proposal.
                None => {
                    info!("Receive a stale proposal message for {}", region_id);
                    removed = true;
                    stale_proposal = Some(region_proposal);
                }
            }

            // For stale proposal message, notify the caller.
            if removed {
                let stale_proposal = stale_proposal.unwrap();
                for p in stale_proposal.props {
                    let cmd = PendingCmd::new(p.index, p.term, p.cb);
                    notify_region_removed(stale_proposal.region_id, stale_proposal.id, cmd);
                }
                self.region_task_senders.remove(&stale_proposal.region_id);
            }
        }

        APPLY_PROPOSAL.observe(propose_num as f64);
    }

    // Spawn an endless future on the apply pool for the given region.
    fn spawn_region(
        apply_pool: &FuturePool<PoolContext>,
        delegate: ApplyDelegate,
        rx: UnboundedReceiver<RegionTask>,
    ) {
        apply_pool
            .spawn(move |_| {
                rx.fold(delegate, move |mut delegate, task| {
                    match task {
                        RegionTask::Apply((apply, start)) => {
                            if delegate.stopping {
                                info!("{} is stopping, ignore new apply tasks", delegate.tag);
                                return box future::ok(delegate)
                                    as Box<Future<Item = ApplyDelegate, Error = ()> + Send>;
                            }

                            assert_eq!(apply.region_id, delegate.region_id());

                            let elapsed = duration_to_sec(start.elapsed());
                            APPLY_TASK_WAIT_TIME_HISTOGRAM.observe(elapsed);

                            delegate.metrics = ApplyMetrics::default();
                            delegate.term = apply.term;

                            if apply.entries.is_empty() {
                                return box future::ok(delegate)
                                    as Box<Future<Item = ApplyDelegate, Error = ()> + Send>;
                            }

                            let t = SlowTimer::new();
                            let ctx =
                                ApplyContext::new(delegate.host.clone(), delegate.importer.clone());
                            box delegate
                                .handle_raft_committed_entries(ctx, apply.entries)
                                .then(move |res| {
                                    let (mut delegate, ctx, stopping) = match res {
                                        Ok((delegate, mut ctx)) => {
                                            ctx.write_to_db(&delegate.engines.kv);
                                            (delegate, ctx, false)
                                        }
                                        Err((delegate, ctx, e)) => {
                                            let mut stopping = false;
                                            if let Error::Canceled = e {
                                                stopping = true;
                                            }
                                            (delegate, ctx, stopping)
                                        }
                                    };
                                    if stopping {
                                        delegate.stopping = true;
                                    }

                                    if !ctx.apply_res.is_empty() {
                                        delegate
                                            .notifier
                                            .send(TaskRes::Applys(ctx.apply_res))
                                            .unwrap();
                                    }

                                    if delegate.pending_remove {
                                        info!("{} destroyed due to pending_remove", delegate.tag);
                                        delegate.destroy();
                                        return Err(());
                                    }

                                    STORE_APPLY_LOG_HISTOGRAM
                                        .observe(duration_to_sec(t.elapsed()) as f64);

                                    slow_log!(
                                        t,
                                        "{} handle ready {} committed entries",
                                        delegate.tag,
                                        ctx.committed_count
                                    );

                                    Ok(delegate)
                                })
                                as Box<Future<Item = ApplyDelegate, Error = ()> + Send>
                        }
                        RegionTask::Destroy(region_id) => {
                            assert_eq!(delegate.region_id(), region_id);
                            info!("{} remove from apply pool", delegate.tag);
                            delegate.destroy();
                            let notifier = delegate.notifier.clone();
                            notifier.send(TaskRes::Destroy(delegate)).unwrap();

                            // Exit current task
                            box future::err(())
                        }
                        RegionTask::Update(u) => {
                            assert_eq!(delegate.region_id(), u.region_id);
                            delegate.term = u.new_delegate.term;
                            delegate.clear_all_commands_as_stale();

                            box future::ok(u.new_delegate)
                        }
                        RegionTask::Stop((region_id, stop_tx)) => {
                            assert_eq!(delegate.region_id(), region_id);
                            delegate.destroy();
                            stop_tx.send(()).unwrap();

                            // Exit current task
                            box future::err(())
                        }
                        RegionTask::ApplyTo(a) => {
                            let need_apply_to = a.merge_req.get_commit();
                            let applied_index = delegate.apply_state.applied_index;
                            assert_eq!(delegate.region_id(), a.source_region);

                            if applied_index >= need_apply_to {
                                // Current applied index >= need_apply_to, don't need to catch up log.
                                a.tx.send(delegate.region.clone()).unwrap();
                                delegate.destroy();

                                // Exit current task
                                box future::err(())
                            } else {
                                // Apply left entries.
                                let entries =
                                    delegate.load_entries_for_merge(&a.merge_req, applied_index);
                                assert_eq!(entries.len() as u64, need_apply_to - applied_index);
                                let ctx = ApplyContext::new(
                                    delegate.host.clone(),
                                    delegate.importer.clone(),
                                );
                                box delegate.handle_raft_committed_entries(ctx, entries).then(
                                    move |res| {
                                        let (mut delegate, mut ctx) = match res {
                                            Ok((delegate, mut ctx)) => {
                                                ctx.write_to_db(&delegate.engines.kv);
                                                (delegate, ctx)
                                            }
                                            Err((delegate, _ctx, e)) => panic!(
                                                "{} catch up log failed, error {:?}",
                                                delegate.tag, e
                                            ),
                                        };

                                        if !ctx.apply_res.is_empty() {
                                            for res in &mut ctx.apply_res {
                                                res.merged = true;
                                            }
                                            delegate
                                                .notifier
                                                .send(TaskRes::Applys(ctx.apply_res))
                                                .unwrap();
                                        }

                                        // Tell target region, has applied to specified position, and destroy itself.
                                        a.tx.send(delegate.region.clone()).unwrap();
                                        delegate.destroy();

                                        Err(())
                                    },
                                )
                                    as Box<Future<Item = ApplyDelegate, Error = ()> + Send>
                            }
                        }
                        RegionTask::Proposal(region_proposal) => {
                            assert_eq!(delegate.id, region_proposal.id);
                            for p in region_proposal.props {
                                let cmd = PendingCmd::new(p.index, p.term, p.cb);
                                if p.is_conf_change {
                                    if let Some(cmd) = delegate.pending_cmds.take_conf_change() {
                                        // if it loses leadership before conf change is replicated, there may be
                                        // a stale pending conf change before next conf change is applied. If it
                                        // becomes leader again with the stale pending conf change, will enter
                                        // this block, so we notify leadership may have been changed.
                                        notify_stale_command(&delegate.tag, delegate.term, cmd);
                                    }
                                    delegate.pending_cmds.set_conf_change(cmd);
                                } else {
                                    delegate.pending_cmds.append_normal(cmd);
                                }
                            }

                            box future::ok(delegate)
                        }
                    }
                })
            })
            .forget();
    }

    fn handle_registration(&mut self, s: Registration) {
        let peer_id = s.id;
        let region_id = s.region.get_id();
        let delegate = ApplyDelegate::from_registration(
            self.engines.clone(),
            s,
            self.apply_scheduler.clone(),
            self.notifier.clone(),
            Arc::clone(&self.host),
            Arc::clone(&self.importer),
            self.use_delete_range,
        );

        let mut d = None;

        // If there existing related sender, try to send update message.
        if let Some(tx) = self.region_task_senders.get(&region_id) {
            info!(
                "{} peer {} update delegate at term {}",
                delegate.tag, peer_id, delegate.term
            );
            tx.unbounded_send(RegionTask::update(region_id, delegate))
                .unwrap_or_else(|e| {
                    error!(
                        "send update message failed for region {}, error {:?}",
                        region_id, e
                    );
                    let task = e.into_inner();
                    if let RegionTask::Update(u) = task {
                        d = Some(u.new_delegate);
                    } else {
                        panic!("Proposal task is expected.")
                    }
                });
        } else {
            d = Some(delegate);
        }

        // Spawn a future for this region.
        if let Some(delegate) = d {
            info!(
                "{} peer {} register to apply pool at term {}",
                delegate.tag, peer_id, delegate.term
            );
            let (tx, rx) = unbounded();
            Self::spawn_region(&self.apply_pool, delegate, rx);
            self.region_task_senders.insert(region_id, tx);
        }
    }

    fn handle_destroy(&mut self, d: Destroy) {
        // Only respond when the meta exists. Otherwise if destroy is triggered
        // multiple times, the store may destroy wrong target peer.
        if let Some(tx) = self.region_task_senders.remove(&d.region_id) {
            tx.unbounded_send(RegionTask::destroy(d.region_id))
                .unwrap_or_else(|e| {
                    warn!(
                        "[region {}] has been removed before destroy message arrived, error {:?}",
                        d.region_id, e
                    );
                });
        }
    }

    fn handle_apply_to(&mut self, apply_to: SourceMustApplyTo) {
        // Dispatch the wait message to the `source_region`.
        // When source region received this message, it will apply left entries
        // by itself, so we remove its sender here.
        let region = apply_to.source_region;
        let tx = self.region_task_senders.remove(&region).unwrap_or_else(|| {
            panic!(
                "Can't send ApplyTo message to region {}, no related message channel",
                region
            );
        });

        tx.unbounded_send(RegionTask::ApplyTo(apply_to))
            .unwrap_or_else(|e| {
                panic!(
                    "Can't send ApplyTo message to region {}, caused by err: {:?}",
                    region, e
                );
            });
    }

    fn handle_shutdown(&mut self) {
        // Send stop message to each region.
        let mut total: u64 = 0;
        let (stop_tx, stop_rx) = mpsc::channel();
        for (region_id, tx) in self.region_task_senders.drain() {
            if let Ok(()) = tx.unbounded_send(RegionTask::Stop((region_id, stop_tx.clone()))) {
                total += 1;
            }
        }

        // Wait all regions have stopped.
        for _ in 0..total {
            let _ = stop_rx.recv();
        }
    }

    fn handle_applies(&mut self, applies: ApplyBatch) {
        let start = applies.start;
        let vec = applies.vec;
        for apply in vec {
            if apply.entries.is_empty() {
                continue;
            }

            // Dispatch the apply task to related region.
            let region_id = apply.region_id;
            let mut removed = false;
            if let Some(tx) = self.region_task_senders.get(&region_id) {
                tx.unbounded_send(RegionTask::apply(apply, start))
                    .unwrap_or_else(|_e| {
                        error!("[region {}] has been removed", region_id);
                        removed = true;
                    });
            } else {
                error!("[region {}] is missing", region_id);
                continue;
            }

            if removed {
                self.region_task_senders.remove(&region_id).unwrap();
            }
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Applies(a) => self.handle_applies(a),
            Task::Proposals(props) => self.handle_proposals(props),
            Task::Registration(s) => self.handle_registration(s),
            Task::Destroy(d) => self.handle_destroy(d),
            Task::ApplyTo(a) => self.handle_apply_to(a),
        }
    }

    fn shutdown(&mut self) {
        self.handle_shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::*;
    use std::sync::*;

    use kvproto::metapb::RegionEpoch;
    use kvproto::raft_cmdpb::*;
    use protobuf::Message;
    use raftstore::coprocessor::*;
    use raftstore::store::msg::WriteResponse;
    use rocksdb::{Writable, WriteBatch};
    use tempdir::TempDir;

    use super::*;
    use import::test_helpers::*;
    use util::collections::HashMap;
    use util::worker::Worker;

    impl Debug for ApplyContext {
        fn fmt(&self, f: &mut Formatter) -> fmt::Result {
            write!(f, "ApplyDelegate")
        }
    }

    pub fn create_tmp_engine(path: &str) -> (TempDir, Engines) {
        let path = TempDir::new(path).unwrap();
        let db = Arc::new(
            rocksdb::new_engine(path.path().join("db").to_str().unwrap(), ALL_CFS, None).unwrap(),
        );
        let raft_db = Arc::new(
            rocksdb::new_engine(path.path().join("raft").to_str().unwrap(), &[], None).unwrap(),
        );
        (path, Engines::new(db, raft_db))
    }

    pub fn create_tmp_importer(path: &str) -> (TempDir, Arc<SSTImporter>) {
        let dir = TempDir::new(path).unwrap();
        let importer = Arc::new(SSTImporter::new(dir.path()).unwrap());
        (dir, importer)
    }

    fn new_runner(
        engines: Engines,
        host: Arc<CoprocessorHost>,
        importer: Arc<SSTImporter>,
        tx: Sender<TaskRes>,
        apply_scheduler: Scheduler<Task>,
    ) -> Runner {
        let apply_pool = FuturePool::new(
            4,
            STACK_SIZE,
            "apply-pool",
            Duration::from_secs(1),
            move || PoolContext {},
        );

        Runner {
            engines,
            host,
            importer,
            notifier: tx,
            sync_log: false,
            tag: "".to_owned(),
            use_delete_range: true,
            apply_pool,
            apply_scheduler,
            region_task_senders: HashMap::default(),
        }
    }

    pub fn new_entry(term: u64, index: u64, req: Option<RaftCmdRequest>) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(term);
        if let Some(r) = req {
            e.set_data(r.write_to_bytes().unwrap())
        };
        e
    }

    #[test]
    fn test_should_write_to_engine() {
        // ComputeHash command
        let mut req = RaftCmdRequest::new();
        req.mut_admin_request()
            .set_cmd_type(AdminCmdType::ComputeHash);
        let wb = WriteBatch::new();
        assert_eq!(should_write_to_engine(&req, wb.count()), true);

        // IngestSST command
        let mut req = Request::new();
        req.set_cmd_type(CmdType::IngestSST);
        req.set_ingest_sst(IngestSSTRequest::new());
        let mut cmd = RaftCmdRequest::new();
        cmd.mut_requests().push(req);
        let wb = WriteBatch::new();
        assert_eq!(should_write_to_engine(&cmd, wb.count()), true);

        // Write batch keys reach WRITE_BATCH_MAX_KEYS
        let req = RaftCmdRequest::new();
        let wb = WriteBatch::new();
        for i in 0..WRITE_BATCH_MAX_KEYS {
            let key = format!("key_{}", i);
            wb.put(key.as_bytes(), b"value").unwrap();
        }
        assert_eq!(should_write_to_engine(&req, wb.count()), true);

        // Write batch keys not reach WRITE_BATCH_MAX_KEYS
        let req = RaftCmdRequest::new();
        let wb = WriteBatch::new();
        for i in 0..WRITE_BATCH_MAX_KEYS - 1 {
            let key = format!("key_{}", i);
            wb.put(key.as_bytes(), b"value").unwrap();
        }
        assert_eq!(should_write_to_engine(&req, wb.count()), false);
    }

    #[test]
    fn test_basic_flow() {
        let (tx, rx) = mpsc::channel();
        let (_tmp, engines) = create_tmp_engine("apply-basic");
        let host = Arc::new(CoprocessorHost::default());
        let (_dir, importer) = create_tmp_importer("apply-basic");
        let mut apply_worker = Worker::new("apply-worker");
        let runner = new_runner(
            engines.clone(),
            Arc::clone(&host),
            Arc::clone(&importer),
            tx,
            apply_worker.scheduler(),
        );

        apply_worker.start(runner).unwrap();

        // registration task
        let mut reg = Registration::default();
        reg.id = 1; // peer id
        reg.region.set_id(2);
        reg.apply_state.set_applied_index(3);
        reg.term = 4;
        reg.applied_index_term = 5;
        apply_worker
            .schedule(Task::Registration(reg.clone()))
            .unwrap();
        // {
        //     let delegates = runner.delegates.lock().unwrap();
        //     let delegate = &delegates[&2].as_ref().unwrap();
        //     assert_eq!(delegate.id, 1);
        //     assert_eq!(delegate.tag, "[region 2] 1");
        //     assert_eq!(delegate.region, reg.region);
        //     assert!(!delegate.pending_remove);
        //     assert_eq!(delegate.apply_state, reg.apply_state);
        //     assert_eq!(delegate.term, reg.term);
        //     assert_eq!(delegate.applied_index_term, reg.applied_index_term);
        // }

        // proposal task
        let (resp_tx, resp_rx) = mpsc::channel();
        let p = Proposal::new(
            false,
            1,
            0,
            Callback::Write(box move |resp: WriteResponse| {
                resp_tx.send(resp.response).unwrap();
            }),
        );
        let region_proposal = RegionProposal::new(1, 1, vec![p]);
        apply_worker
            .schedule(Task::Proposals(vec![region_proposal]))
            .unwrap();

        // unregistered region should be ignored and notify failed.
        assert!(rx.try_recv().is_err());
        let resp = resp_rx.recv().unwrap();
        assert!(resp.get_header().get_error().has_region_not_found());

        let (cc_tx, cc_rx) = mpsc::channel();
        let pops = vec![
            Proposal::new(
                false, /* is conf change */
                2,     /* index */
                0,     /* term */
                Callback::None,
            ),
            Proposal::new(
                true,
                3,
                0,
                Callback::Write(box move |write: WriteResponse| {
                    cc_tx.send(write.response).unwrap();
                }),
            ),
        ];
        let region_proposal = RegionProposal::new(1, 2, pops);
        apply_worker
            .schedule(Task::Proposals(vec![region_proposal]))
            .unwrap();
        assert!(rx.try_recv().is_err());
        // {
        //     let delegates = runner.delegates.lock().unwrap();
        //     let normals = &delegates[&2].as_ref().unwrap().pending_cmds.normals;
        //     assert_eq!(normals.back().map(|c| c.index), Some(2));
        // }
        assert!(rx.try_recv().is_err());
        // {
        //     let delegates = runner.delegates.lock().unwrap();
        //     let cc = &delegates[&2].as_ref().unwrap().pending_cmds.conf_change;
        //     assert_eq!(cc.as_ref().map(|c| c.index), Some(3));
        // }

        let p = Proposal::new(true, 4, 0, Callback::None);
        let region_proposal = RegionProposal::new(1, 2, vec![p]);
        apply_worker
            .schedule(Task::Proposals(vec![region_proposal]))
            .unwrap();
        assert!(rx.try_recv().is_err());
        // {
        //     let delegates = runner.delegates.lock().unwrap();
        //     let cc = &delegates[&2].as_ref().unwrap().pending_cmds.conf_change;
        //     assert_eq!(cc.as_ref().map(|c| c.index), Some(4));
        // }
        // propose another conf change should mark previous stale.
        let cc_resp = cc_rx.recv().unwrap();
        assert!(cc_resp.get_header().get_error().has_stale_command());

        apply_worker
            .schedule(Task::applies(vec![Apply::new(
                1, /* region id */
                1, /* term */
                vec![new_entry(2, 3, None)],
            )]))
            .unwrap();
        // non registered region should be ignored.
        assert!(rx.try_recv().is_err());

        apply_worker
            .schedule(Task::applies(vec![Apply::new(2, 11, vec![])]))
            .unwrap();
        // empty entries should be ignored.
        assert!(rx.try_recv().is_err());
        // {
        //     let delegates = runner.delegates.lock().unwrap();
        //     assert_eq!(delegates[&2].as_ref().unwrap().term, reg.term);
        // }

        let apply_state_key = keys::apply_state_key(2);
        assert!(engines.kv.get(&apply_state_key).unwrap().is_none());
        apply_worker
            .schedule(Task::applies(vec![Apply::new(
                2,  /* region id */
                11, /* term */
                vec![new_entry(5 /* term */, 4 /* index */, None)],
            )]))
            .unwrap();
        let res = match rx.recv().unwrap() {
            TaskRes::Applys(res) => res,
            e => panic!("Expected apply results, but got {:?}", e),
        };
        assert_eq!(res.len(), 1);
        let apply_res = &res[0];
        assert_eq!(apply_res.region_id, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 4);
        assert!(apply_res.exec_res.is_empty());
        // empty entry will make applied_index step forward and should write apply state to engine.
        assert_eq!(apply_res.metrics.written_keys, 1);
        assert_eq!(apply_res.applied_index_term, 5);
        // {
        //     let delegates = runner.delegates.lock().unwrap();
        //     let delegate = &delegates[&2].as_ref().unwrap();
        //     assert_eq!(delegate.term, 11);
        //     assert_eq!(delegate.applied_index_term, 5);
        //     assert_eq!(delegate.apply_state.get_applied_index(), 4);
        //     let apply_state: RaftApplyState =
        //         db.get_msg_cf(CF_RAFT, &apply_state_key).unwrap().unwrap();
        //     assert_eq!(apply_state, delegate.apply_state);
        // }

        apply_worker.schedule(Task::destroy(2)).unwrap();
        let destroy_res = match rx.recv().unwrap() {
            TaskRes::Destroy(d) => d,
            e => panic!("Expected destroy result, but get {:?}", e),
        };
        assert_eq!(destroy_res.id, 1);
        assert_eq!(destroy_res.applied_index_term, 5);

        let handle = apply_worker.stop().unwrap();
        handle.join().unwrap();
    }

    struct EntryBuilder {
        entry: Entry,
        req: RaftCmdRequest,
    }

    impl EntryBuilder {
        fn new(index: u64, term: u64) -> EntryBuilder {
            let req = RaftCmdRequest::new();
            let mut entry = Entry::new();
            entry.set_index(index);
            entry.set_term(term);
            EntryBuilder { entry, req }
        }

        fn capture_resp(
            self,
            delegate: &mut ApplyDelegate,
            tx: Sender<RaftCmdResponse>,
        ) -> EntryBuilder {
            let cmd = PendingCmd::new(
                self.entry.get_index(),
                self.entry.get_term(),
                Callback::Write(box move |resp: WriteResponse| {
                    tx.send(resp.response).unwrap();
                }),
            );
            delegate.pending_cmds.append_normal(cmd);
            self
        }

        fn epoch(mut self, conf_ver: u64, version: u64) -> EntryBuilder {
            let mut epoch = RegionEpoch::new();
            epoch.set_version(version);
            epoch.set_conf_ver(conf_ver);
            self.req.mut_header().set_region_epoch(epoch);
            self
        }

        fn put(self, key: &[u8], value: &[u8]) -> EntryBuilder {
            self.add_put_req(None, key, value)
        }

        fn put_cf(self, cf: &str, key: &[u8], value: &[u8]) -> EntryBuilder {
            self.add_put_req(Some(cf), key, value)
        }

        fn add_put_req(mut self, cf: Option<&str>, key: &[u8], value: &[u8]) -> EntryBuilder {
            let mut cmd = Request::new();
            cmd.set_cmd_type(CmdType::Put);
            if let Some(cf) = cf {
                cmd.mut_put().set_cf(cf.to_owned());
            }
            cmd.mut_put().set_key(key.to_vec());
            cmd.mut_put().set_value(value.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn delete(self, key: &[u8]) -> EntryBuilder {
            self.add_delete_req(None, key)
        }

        fn delete_cf(self, cf: &str, key: &[u8]) -> EntryBuilder {
            self.add_delete_req(Some(cf), key)
        }

        fn delete_range(self, start_key: &[u8], end_key: &[u8]) -> EntryBuilder {
            self.add_delete_range_req(None, start_key, end_key)
        }

        fn delete_range_cf(self, cf: &str, start_key: &[u8], end_key: &[u8]) -> EntryBuilder {
            self.add_delete_range_req(Some(cf), start_key, end_key)
        }

        fn add_delete_req(mut self, cf: Option<&str>, key: &[u8]) -> EntryBuilder {
            let mut cmd = Request::new();
            cmd.set_cmd_type(CmdType::Delete);
            if let Some(cf) = cf {
                cmd.mut_delete().set_cf(cf.to_owned());
            }
            cmd.mut_delete().set_key(key.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn add_delete_range_req(
            mut self,
            cf: Option<&str>,
            start_key: &[u8],
            end_key: &[u8],
        ) -> EntryBuilder {
            let mut cmd = Request::new();
            cmd.set_cmd_type(CmdType::DeleteRange);
            if let Some(cf) = cf {
                cmd.mut_delete_range().set_cf(cf.to_owned());
            }
            cmd.mut_delete_range().set_start_key(start_key.to_vec());
            cmd.mut_delete_range().set_end_key(end_key.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn ingest_sst(mut self, meta: &SSTMeta) -> EntryBuilder {
            let mut cmd = Request::new();
            cmd.set_cmd_type(CmdType::IngestSST);
            cmd.mut_ingest_sst().set_sst(meta.clone());
            self.req.mut_requests().push(cmd);
            self
        }

        fn build(mut self) -> Entry {
            self.entry.set_data(self.req.write_to_bytes().unwrap());
            self.entry
        }
    }

    #[derive(Clone, Default)]
    struct ApplyObserver {
        pre_admin_count: Arc<AtomicUsize>,
        pre_query_count: Arc<AtomicUsize>,
        post_admin_count: Arc<AtomicUsize>,
        post_query_count: Arc<AtomicUsize>,
    }

    impl Coprocessor for ApplyObserver {}

    impl QueryObserver for ApplyObserver {
        fn pre_apply_query(&self, _: &mut ObserverContext, _: &[Request]) {
            self.pre_query_count.fetch_add(1, Ordering::SeqCst);
        }

        fn post_apply_query(&self, _: &mut ObserverContext, _: &mut RepeatedField<Response>) {
            self.post_query_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_handle_raft_committed_entries() {
        let (_path, engines) = create_tmp_engine("test-delegate");
        let (import_dir, importer) = create_tmp_importer("test-delegate");
        let importer = Arc::new(importer);
        let mut host = CoprocessorHost::default();
        let obs = ApplyObserver::default();
        host.registry
            .register_query_observer(1, Box::new(obs.clone()));

        let apply_worker = Worker::new("apply-worker");
        let (notifier, _receiver) = mpsc::channel();

        let mut reg = Registration::default();
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_version(3);
        let mut delegate = ApplyDelegate::from_registration(
            engines.clone(),
            reg,
            apply_worker.scheduler(),
            notifier.clone(),
            Arc::new(host),
            Arc::clone(&importer),
            true, /* use_delete_range */
        );

        let (tx, rx) = mpsc::channel();
        let put_entry = EntryBuilder::new(1, 1)
            .put(b"k1", b"v1")
            .put(b"k2", b"v1")
            .put(b"k3", b"v1")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let apply_ctx = ApplyContext::new(delegate.host.clone(), delegate.importer.clone());
        let (delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, vec![put_entry])
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        assert!(apply_ctx.apply_res.last().unwrap().exec_res.is_empty());
        let resp = rx.try_recv().unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(resp.get_responses().len(), 3);
        let dk_k1 = keys::data_key(b"k1");
        let dk_k2 = keys::data_key(b"k2");
        let dk_k3 = keys::data_key(b"k3");
        assert_eq!(engines.kv.get(&dk_k1).unwrap().unwrap(), b"v1");
        assert_eq!(engines.kv.get(&dk_k2).unwrap().unwrap(), b"v1");
        assert_eq!(engines.kv.get(&dk_k3).unwrap().unwrap(), b"v1");
        assert_eq!(delegate.applied_index_term, 1);
        assert_eq!(delegate.apply_state.get_applied_index(), 1);

        let lock_written_bytes = delegate.metrics.lock_cf_written_bytes;
        let written_bytes = delegate.metrics.written_bytes;
        let written_keys = delegate.metrics.written_keys;
        let size_diff_hint = delegate.metrics.size_diff_hint;
        let put_entry = EntryBuilder::new(2, 2)
            .put_cf(CF_LOCK, b"k1", b"v1")
            .epoch(1, 3)
            .build();
        let (mut delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, vec![put_entry])
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        let lock_handle = engines.kv.cf_handle(CF_LOCK).unwrap();
        assert_eq!(
            engines.kv.get_cf(lock_handle, &dk_k1).unwrap().unwrap(),
            b"v1"
        );
        assert_eq!(
            delegate.metrics.lock_cf_written_bytes,
            lock_written_bytes + 5
        );
        assert!(delegate.metrics.written_bytes >= written_bytes + 5);
        assert_eq!(delegate.metrics.written_keys, written_keys + 2);
        assert_eq!(delegate.metrics.size_diff_hint, size_diff_hint + 5);
        assert_eq!(delegate.applied_index_term, 2);
        assert_eq!(delegate.apply_state.get_applied_index(), 2);

        let put_entry = EntryBuilder::new(3, 2)
            .put(b"k2", b"v2")
            .epoch(1, 1)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let (mut delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, vec![put_entry])
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_stale_epoch());
        assert_eq!(delegate.applied_index_term, 2);
        assert_eq!(delegate.apply_state.get_applied_index(), 3);

        let put_entry = EntryBuilder::new(4, 2)
            .put(b"k3", b"v3")
            .put(b"k5", b"v5")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let (mut delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, vec![put_entry])
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());
        assert_eq!(delegate.applied_index_term, 2);
        assert_eq!(delegate.apply_state.get_applied_index(), 4);
        // a writebatch should be atomic.
        assert_eq!(engines.kv.get(&dk_k3).unwrap().unwrap(), b"v1");

        EntryBuilder::new(5, 2)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let put_entry = EntryBuilder::new(5, 3)
            .delete(b"k1")
            .delete_cf(CF_LOCK, b"k1")
            .delete_cf(CF_WRITE, b"k1")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let lock_written_bytes = delegate.metrics.lock_cf_written_bytes;
        let delete_keys_hint = delegate.metrics.delete_keys_hint;
        let size_diff_hint = delegate.metrics.size_diff_hint;
        let (mut delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, vec![put_entry])
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        let resp = rx.try_recv().unwrap();
        // stale command should be cleared.
        assert!(resp.get_header().get_error().has_stale_command());
        let resp = rx.try_recv().unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert!(engines.kv.get(&dk_k1).unwrap().is_none());
        assert_eq!(
            delegate.metrics.lock_cf_written_bytes,
            lock_written_bytes + 3
        );
        assert_eq!(delegate.metrics.delete_keys_hint, delete_keys_hint + 2);
        assert_eq!(delegate.metrics.size_diff_hint, size_diff_hint - 9);

        let delete_entry = EntryBuilder::new(6, 3)
            .delete(b"k5")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let (mut delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, vec![delete_entry])
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());

        let delete_range_entry = EntryBuilder::new(7, 3)
            .delete_range(b"", b"")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let (mut delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, vec![delete_range_entry])
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());
        assert_eq!(engines.kv.get(&dk_k3).unwrap().unwrap(), b"v1");

        let delete_range_entry = EntryBuilder::new(8, 3)
            .delete_range_cf(CF_DEFAULT, b"", b"k5")
            .delete_range_cf(CF_LOCK, b"", b"k5")
            .delete_range_cf(CF_WRITE, b"", b"k5")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let (mut delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, vec![delete_range_entry])
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        let resp = rx.try_recv().unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert!(engines.kv.get(&dk_k1).unwrap().is_none());
        assert!(engines.kv.get(&dk_k2).unwrap().is_none());
        assert!(engines.kv.get(&dk_k3).unwrap().is_none());

        // UploadSST
        let sst_path = import_dir.path().join("test.sst");
        let sst_epoch = delegate.region.get_region_epoch().clone();
        let sst_range = (0, 100);
        let (mut meta1, data1) = gen_sst_file(&sst_path, sst_range);
        meta1.set_region_epoch(sst_epoch);
        let mut file1 = importer.create(&meta1).unwrap();
        file1.append(&data1).unwrap();
        file1.finish().unwrap();
        let (mut meta2, data2) = gen_sst_file(&sst_path, sst_range);
        meta2.mut_region_epoch().set_version(1234);
        let mut file2 = importer.create(&meta2).unwrap();
        file2.append(&data2).unwrap();
        file2.finish().unwrap();

        // IngestSST
        let put_ok = EntryBuilder::new(9, 3)
            .capture_resp(&mut delegate, tx.clone())
            .put(&[sst_range.0], &[sst_range.1])
            .epoch(0, 3)
            .build();
        // Add a put above to test flush before ingestion.
        let ingest_ok = EntryBuilder::new(10, 3)
            .capture_resp(&mut delegate, tx.clone())
            .ingest_sst(&meta1)
            .epoch(0, 3)
            .build();
        let ingest_stale_epoch = EntryBuilder::new(11, 3)
            .capture_resp(&mut delegate, tx.clone())
            .ingest_sst(&meta2)
            .epoch(0, 3)
            .build();
        let entries = vec![put_ok, ingest_ok, ingest_stale_epoch];
        let (mut delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, entries)
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        let resp = rx.try_recv().unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        let resp = rx.try_recv().unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        check_db_range(&engines.kv, sst_range);
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().has_error());
        assert_eq!(delegate.applied_index_term, 3);
        assert_eq!(delegate.apply_state.get_applied_index(), 11);

        let mut entries = vec![];
        for i in 0..WRITE_BATCH_MAX_KEYS {
            let put_entry = EntryBuilder::new(i as u64 + 12, 3)
                .put(b"k", b"v")
                .epoch(1, 3)
                .capture_resp(&mut delegate, tx.clone())
                .build();
            entries.push(put_entry);
        }
        let (delegate, mut apply_ctx) = delegate
            .handle_raft_committed_entries(apply_ctx, entries)
            .wait()
            .unwrap();
        apply_ctx.write_to_db(&engines.kv);
        for _ in 0..WRITE_BATCH_MAX_KEYS {
            rx.try_recv().unwrap();
        }
        let index = WRITE_BATCH_MAX_KEYS + 11;
        assert_eq!(delegate.apply_state.get_applied_index(), index as u64);
        assert_eq!(obs.pre_query_count.load(Ordering::SeqCst), index);
        assert_eq!(obs.post_query_count.load(Ordering::SeqCst), index);
    }

    #[test]
    fn test_check_sst_for_ingestion() {
        let mut sst = SSTMeta::new();
        let mut region = Region::new();

        // Check uuid and cf name
        assert!(check_sst_for_ingestion(&sst, &region).is_err());
        sst.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        sst.set_cf_name(CF_DEFAULT.to_owned());
        check_sst_for_ingestion(&sst, &region).unwrap();
        sst.set_cf_name("test".to_owned());
        assert!(check_sst_for_ingestion(&sst, &region).is_err());
        sst.set_cf_name(CF_WRITE.to_owned());
        check_sst_for_ingestion(&sst, &region).unwrap();

        // Check region id
        region.set_id(1);
        sst.set_region_id(2);
        assert!(check_sst_for_ingestion(&sst, &region).is_err());
        sst.set_region_id(1);
        check_sst_for_ingestion(&sst, &region).unwrap();

        // Check region epoch
        region.mut_region_epoch().set_conf_ver(1);
        assert!(check_sst_for_ingestion(&sst, &region).is_err());
        sst.mut_region_epoch().set_conf_ver(1);
        check_sst_for_ingestion(&sst, &region).unwrap();
        region.mut_region_epoch().set_version(1);
        assert!(check_sst_for_ingestion(&sst, &region).is_err());
        sst.mut_region_epoch().set_version(1);
        check_sst_for_ingestion(&sst, &region).unwrap();

        // Check region range
        region.set_start_key(vec![2]);
        region.set_end_key(vec![8]);
        sst.mut_range().set_start(vec![1]);
        sst.mut_range().set_end(vec![8]);
        assert!(check_sst_for_ingestion(&sst, &region).is_err());
        sst.mut_range().set_start(vec![2]);
        assert!(check_sst_for_ingestion(&sst, &region).is_err());
        sst.mut_range().set_end(vec![7]);
        check_sst_for_ingestion(&sst, &region).unwrap();
    }
}
