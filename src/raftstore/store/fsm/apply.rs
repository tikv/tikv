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

use std::cmp;
use std::collections::VecDeque;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::mpsc::{SendError, TryRecvError};
use std::sync::Arc;

use protobuf::RepeatedField;
use rocksdb::rocksdb_options::WriteOptions;
use rocksdb::{Writable, WriteBatch};
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

use super::apply_transport::{OneshotNotifier, OneshotPoller, Router, Scheduler};
use super::transport::Router as PeerRouter;
use import::SSTImporter;
use prometheus::{exponential_buckets, Histogram};
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
use raftstore::store::{cmd_resp, keys, util, Config, Engines, PeerMsg};
use raftstore::{Error, Result};
use storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use util::mpsc::Receiver;
use util::time::{duration_to_sec, Instant};
use util::{escape, rocksdb, MustConsumeVec};

lazy_static! {
    pub static ref APPLY_PROPOSAL: Histogram = register_histogram!(
        "tikv_raftstore_apply_proposal",
        "Proposal count of all regions in a mio tick",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    ).unwrap();
}

const WRITE_BATCH_MAX_KEYS: usize = 128;
const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;
const SHRINK_PENDING_CMD_QUEUE_CAP: usize = 64;

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

/*impl Drop for PendingCmd {
    fn drop(&mut self) {
        if self.cb.is_some() {
            panic!(
                "callback of pending command at [index: {}, term: {}] is leak.",
                self.index, self.term
            );
        }
    }
}*/

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
        regions: Vec<Region>,
        derived: Region,
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

pub struct ApplyCallback {
    region: Region,
    cbs: Vec<(Option<Callback>, RaftCmdResponse)>,
}

impl ApplyCallback {
    fn new(region: Region) -> ApplyCallback {
        let cbs = vec![];
        ApplyCallback { region, cbs }
    }

    fn invoke_all(self, host: &CoprocessorHost) {
        for (cb, mut resp) in self.cbs {
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

/// Stash keeps the informations that are needed to restore an appropriate
/// applying context for the `ApplyContextCore::stash` call.
pub struct Stash {
    region: Option<Region>,
    exec_ctx: Option<ExecContext>,
    last_applied_index: u64,
}

pub struct PollContext {
    engines: Engines,
    cfg: Arc<Config>,
    host: Arc<CoprocessorHost>,
    importer: Arc<SSTImporter>,
    notifier: PeerRouter,
    router: Router,
    wb: Option<WriteBatch>,
    cbs: MustConsumeVec<ApplyCallback>,
    merged_regions: Vec<u64>,
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
    use_delete_range: bool,
}

impl<'a> PollContext {
    pub fn new(
        engines: Engines,
        cfg: Arc<Config>,
        router: Router,
        notifier: PeerRouter,
        host: Arc<CoprocessorHost>,
        importer: Arc<SSTImporter>,
    ) -> PollContext {
        PollContext {
            engines,
            host,
            importer,
            notifier,
            router,
            wb: None,
            cbs: MustConsumeVec::new("callback of apply context"),
            merged_regions: vec![],
            apply_res: vec![],
            wb_last_bytes: 0,
            wb_last_keys: 0,
            last_applied_index: 0,
            committed_count: 0,
            enable_sync_log: cfg.sync_log,
            sync_log_hint: false,
            exec_ctx: None,
            use_delete_range: cfg.use_delete_range,
            cfg,
        }
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
            self.write_to_db();
            self.prepare_for(delegate);
        }
        self.wb_last_bytes = self.wb().data_size() as u64;
        self.wb_last_keys = self.wb().count() as u64;
    }

    /// Write all the changes into rocksdb.
    pub fn write_to_db(&mut self) {
        if self.wb.as_ref().map_or(false, |wb| !wb.is_empty()) {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.enable_sync_log && self.sync_log_hint);
            self.engines
                .kv
                .write_opt(self.wb.take().unwrap(), &write_opts)
                .unwrap_or_else(|e| {
                    panic!("failed to write to engine: {:?}", e);
                });
            self.sync_log_hint = false;
        }
        for cbs in self.cbs.drain(..) {
            cbs.invoke_all(&self.host);
        }
    }

    /// Finish applys for the delegate.
    pub fn finish_for(
        &mut self,
        delegate: &mut ApplyDelegate,
        results: Option<VecDeque<ExecResult>>,
    ) {
        if !delegate.pending_remove {
            delegate.write_apply_state(self.wb_mut());
        }
        self.commit_opt(delegate, false);
        self.apply_res.push(ApplyRes {
            region_id: delegate.region_id(),
            apply_state: delegate.apply_state.clone(),
            exec_res: results,
            metrics: delegate.metrics.clone(),
            applied_index_term: delegate.applied_index_term,
            merged: false,
        });
    }

    /// Stash the dirty state away for a `ApplyDelegate`, so
    /// the context is ready to switch to apply other `ApplyDelegate`.
    pub fn stash(&mut self, delegate: &mut ApplyDelegate) -> Stash {
        self.commit_opt(delegate, false);
        Stash {
            // last cbs should not be popped, because if the ApplyContext
            // is flushed, the callbacks can be flushed too.
            region: self.cbs.last().map(|cbs| cbs.region.clone()),
            exec_ctx: self.exec_ctx.take(),
            last_applied_index: self.last_applied_index,
        }
    }

    /// Restore the dirty state, so context can resume applying from
    /// last stash point.
    pub fn restore_stash(&mut self, stash: Stash) {
        if let Some(region) = stash.region {
            self.cbs.push(ApplyCallback::new(region));
        }
        self.exec_ctx = stash.exec_ctx;
        self.last_applied_index = stash.last_applied_index;
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

    #[inline]
    pub fn flush(&mut self) {
        self.write_to_db();
        if !self.apply_res.is_empty() {
            for res in self.apply_res.drain(..) {
                let region_id = res.region_id;
                // TODO: verify if it's really shutting down.
                let _ = self
                    .notifier
                    .force_send_peer_message(region_id, PeerMsg::ApplyRes(TaskRes::Apply(res)));
            }
        }
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

#[derive(Debug)]
struct MergeAsyncWait {
    pending_entries: Vec<Entry>,
    pending_tasks: Vec<Task>,
    poller: OneshotPoller,
}

#[derive(Debug)]
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
    metrics: ApplyMetrics,
    term: u64,
    is_merging: bool,
    stopped: bool,
    pending_cmds: PendingCmdQueue,
    pending_merge_apply: Option<MergeAsyncWait>,
    last_merge_version: u64,
}

enum EntryResult {
    None,
    Res(ExecResult),
    Paused,
}

impl ApplyDelegate {
    pub fn from_peer(peer: &Peer) -> ApplyDelegate {
        let reg = Registration::new(peer);
        ApplyDelegate::from_registration(peer.engines(), reg)
    }

    fn from_registration(engines: Engines, reg: Registration) -> ApplyDelegate {
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
            stopped: false,
            pending_cmds: Default::default(),
            metrics: Default::default(),
            last_merge_version: 0,
            pending_merge_apply: None,
        }
    }

    pub fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    // Return true means it's ready to handle next entries.
    fn handle_raft_committed_entries(
        &mut self,
        apply_ctx: &mut PollContext,
        mut committed_entries: Vec<Entry>,
    ) -> bool {
        if committed_entries.is_empty() {
            return true;
        }
        apply_ctx.prepare_for(self);
        apply_ctx.committed_count += committed_entries.len();
        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        apply_ctx.committed_count += committed_entries.len();
        let mut results = None;
        let mut drainer = committed_entries.drain(..);
        let mut finished = true;
        while let Some(entry) = drainer.next() {
            if self.pending_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }

            let expect_index = self.apply_state.get_applied_index() + 1;
            if expect_index == entry.get_index() {

            } else {
                if expect_index > entry.get_index() && self.is_merging {
                    info!(
                        "{} skip log at {} for already applied.",
                        self.tag,
                        entry.get_index()
                    );
                    continue;
                }
                panic!(
                    "{} expect index {}, but got {}",
                    self.tag,
                    expect_index,
                    entry.get_index()
                );
            }

            let res = match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_raft_entry_normal(apply_ctx, &entry),
                EntryType::EntryConfChange => self.handle_raft_entry_conf_change(apply_ctx, &entry),
            };

            match res {
                EntryResult::None => {}
                EntryResult::Res(r) => results.get_or_insert_with(VecDeque::new).push_back(r),
                EntryResult::Paused => {
                    apply_ctx.committed_count -= drainer.len() + 1;
                    let merge_apply = self.pending_merge_apply.as_mut().unwrap();
                    merge_apply.pending_entries = Vec::with_capacity(drainer.len() + 1);
                    merge_apply.pending_entries.push(entry);
                    merge_apply.pending_entries.extend(drainer);
                    finished = false;
                    break;
                }
            }
        }

        apply_ctx.finish_for(self, results);
        finished
    }

    fn update_metrics(&mut self, apply_ctx: &PollContext) {
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

    fn handle_raft_entry_normal(
        &mut self,
        apply_ctx: &mut PollContext,
        entry: &Entry,
    ) -> EntryResult {
        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if !data.is_empty() {
            let cmd = util::parse_data_at(data, index, &self.tag);

            if should_write_to_engine(&cmd, apply_ctx.wb().count()) {
                apply_ctx.commit(self);
            }

            return self.process_raft_cmd(apply_ctx, index, term, cmd);
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
        EntryResult::None
    }

    fn handle_raft_entry_conf_change(
        &mut self,
        apply_ctx: &mut PollContext,
        entry: &Entry,
    ) -> EntryResult {
        let index = entry.get_index();
        let term = entry.get_term();
        let conf_change: ConfChange = util::parse_data_at(entry.get_data(), index, &self.tag);
        let cmd = util::parse_data_at(conf_change.get_context(), index, &self.tag);
        let res = match self.process_raft_cmd(apply_ctx, index, term, cmd) {
            EntryResult::Res(mut res) => {
                if let ExecResult::ChangePeer(ref mut cp) = res {
                    cp.conf_change = conf_change;
                } else {
                    panic!(
                        "{} unexpected result {:?} for conf change {:?} at {}",
                        self.tag, res, conf_change, index
                    );
                }
                res
            }
            EntryResult::None => ExecResult::ChangePeer(Default::default()),
            EntryResult::Paused => unreachable!(),
        };
        EntryResult::Res(res)
    }

    fn find_cb(&mut self, index: u64, term: u64, is_conf_change: bool) -> Option<Callback> {
        if is_conf_change {
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
        &mut self,
        apply_ctx: &mut PollContext,
        index: u64,
        term: u64,
        cmd: RaftCmdRequest,
    ) -> EntryResult {
        if index == 0 {
            panic!(
                "{} processing raft command needs a none zero index",
                self.tag
            );
        }

        if cmd.has_admin_request() {
            apply_ctx.sync_log_hint = true;
        }

        let is_conf_change = get_change_peer_cmd(&cmd).is_some();
        apply_ctx.host.pre_apply(&self.region, &cmd);
        let (mut resp, exec_result) = self.apply_raft_cmd(apply_ctx, index, term, cmd);
        if let EntryResult::Paused = exec_result {
            return exec_result;
        }

        let cmd_cb = self.find_cb(index, term, is_conf_change);

        debug!("{} applied command at log index {}", self.tag, index);

        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        cmd_resp::bind_term(&mut resp, self.term);
        apply_ctx.cbs.last_mut().unwrap().push(cmd_cb, resp);

        exec_result
    }

    // apply operation can fail as following situation:
    //   1. encouter an error that will occur on all store, it can continue
    // applying next entry safely, like stale epoch for example;
    //   2. encouter an error that may not occur on all store, in this case
    // we should try to apply the entry again or panic. Considering that this
    // usually due to disk operation fail, which is rare, so just panic is ok.
    fn apply_raft_cmd(
        &mut self,
        ctx: &mut PollContext,
        index: u64,
        term: u64,
        req: RaftCmdRequest,
    ) -> (RaftCmdResponse, EntryResult) {
        // if pending remove, apply should be aborted already.
        assert!(!self.pending_remove);

        ctx.exec_ctx = Some(self.new_ctx(index, term));
        ctx.wb_mut().set_save_point();
        let (resp, exec_result) = self.exec_raft_cmd(ctx, req).unwrap_or_else(|e| {
            // clear dirty values.
            ctx.wb_mut().rollback_to_save_point().unwrap();
            match e {
                Error::StaleEpoch(..) => debug!("{} stale epoch err: {:?}", self.tag, e),
                _ => error!("{} execute raft command err: {:?}", self.tag, e),
            }
            (cmd_resp::new_error(e), EntryResult::None)
        });
        if let EntryResult::Paused = exec_result {
            return (resp, exec_result);
        }

        let mut exec_ctx = ctx.exec_ctx.take().unwrap();
        exec_ctx.apply_state.set_applied_index(index);

        self.apply_state = exec_ctx.apply_state;
        self.applied_index_term = term;

        if let EntryResult::Res(ref exec_result) = exec_result {
            match *exec_result {
                ExecResult::ChangePeer(ref cp) => {
                    self.region = cp.region.clone();
                }
                ExecResult::ComputeHash { .. }
                | ExecResult::VerifyHash { .. }
                | ExecResult::CompactLog { .. }
                | ExecResult::DeleteRange { .. }
                | ExecResult::IngestSST { .. } => {}
                ExecResult::SplitRegion { ref derived, .. } => {
                    self.region = derived.clone();
                    self.metrics.size_diff_hint = 0;
                    self.metrics.delete_keys_hint = 0;
                }
                ExecResult::PrepareMerge { ref region, .. } => {
                    self.region = region.clone();
                    self.is_merging = true;
                }
                ExecResult::CommitMerge {
                    ref region,
                    ref source,
                } => {
                    self.region = region.clone();
                    self.last_merge_version = region.get_region_epoch().get_version();
                    ctx.merged_regions.push(source.get_id());
                }
                ExecResult::RollbackMerge { ref region, .. } => {
                    self.region = region.clone();
                    self.is_merging = false;
                }
            }
        }

        (resp, exec_result)
    }

    /// Clear all the pending commands.
    ///
    /// Please note that all the pending callbacks will be lost.
    /// Should not do this when dropping a peer in case of possible leak.
    /// TODO: find a good way to clear pending commands.
    #[allow(dead_code)]
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

    fn destroy(&mut self, ctx: &mut PollContext) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
        ctx.router.stop(self.region.get_id());
        self.stopped = true;
    }

    fn clear_all_commands_as_stale(&mut self) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_command(&self.tag, self.term, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_command(&self.tag, self.term, cmd);
        }
    }

    fn new_ctx(&self, index: u64, term: u64) -> ExecContext {
        ExecContext::new(self.apply_state.clone(), index, term)
    }
}

pub struct ExecContext {
    apply_state: RaftApplyState,
    index: u64,
    term: u64,
}

impl ExecContext {
    pub fn new(apply_state: RaftApplyState, index: u64, term: u64) -> ExecContext {
        ExecContext {
            apply_state,
            index,
            term,
        }
    }
}

// Here we implement all commands.
impl ApplyDelegate {
    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd(
        &mut self,
        ctx: &mut PollContext,
        req: RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, EntryResult)> {
        // Include region for stale epoch after merge may cause key not in range.
        let include_region =
            req.get_header().get_region_epoch().get_version() >= self.last_merge_version;
        check_region_epoch(&req, &self.region, include_region)?;
        if req.has_admin_request() {
            self.exec_admin_cmd(ctx, req.get_admin_request())
        } else {
            self.exec_write_cmd(ctx, req.get_requests())
        }
    }

    fn exec_admin_cmd(
        &mut self,
        ctx: &mut PollContext,
        request: &AdminRequest,
    ) -> Result<(RaftCmdResponse, EntryResult)> {
        let cmd_type = request.get_cmd_type();
        info!(
            "{} execute admin command {:?} at [term: {}, index: {}]",
            self.tag,
            request,
            ctx.exec_ctx.as_ref().unwrap().term,
            ctx.exec_ctx.as_ref().unwrap().index
        );

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::Split => self.exec_split(ctx, request),
            AdminCmdType::BatchSplit => self.exec_batch_split(ctx, request),
            AdminCmdType::CompactLog => self.exec_compact_log(ctx, request),
            AdminCmdType::TransferLeader => Err(box_err!("transfer leader won't exec")),
            AdminCmdType::ComputeHash => self.exec_compute_hash(ctx, request),
            AdminCmdType::VerifyHash => self.exec_verify_hash(ctx, request),
            // TODO: is it backward compatible to add new cmd_type?
            AdminCmdType::PrepareMerge => self.exec_prepare_merge(ctx, request),
            AdminCmdType::CommitMerge => self.exec_commit_merge(ctx, request),
            AdminCmdType::RollbackMerge => self.exec_rollback_merge(ctx, request),
            AdminCmdType::InvalidAdmin => Err(box_err!("unsupported admin command type")),
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn exec_change_peer(
        &mut self,
        ctx: &mut PollContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
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
                let add_ndoe_fp = || {
                    fail_point!(
                        "apply_on_add_node_1_2",
                        { self.id == 2 && self.region_id() == 1 },
                        |_| {}
                    )
                };
                add_ndoe_fp();

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
                        return Err(box_err!(
                            "can't add duplicated peer {:?} to region {:?}",
                            peer,
                            self.region
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
                        return Err(box_err!(
                            "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                            peer,
                            p
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
                    return Err(box_err!(
                        "remove missing peer {:?} from region {:?}",
                        peer,
                        self.region
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
                    return Err(box_err!(
                        "can't add duplicated learner {:?} to region {:?}",
                        peer,
                        self.region
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

        Ok((
            resp,
            EntryResult::Res(ExecResult::ChangePeer(ChangePeer {
                conf_change: Default::default(),
                peer: peer.clone(),
                region,
            })),
        ))
    }

    fn exec_split(
        &mut self,
        ctx: &mut PollContext,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
        info!(
            "{} split is deprecated, redirect to use batch split.",
            self.tag
        );
        let split = req.get_split().to_owned();
        let mut admin_req = AdminRequest::new();
        admin_req
            .mut_splits()
            .set_right_derive(split.get_right_derive());
        admin_req.mut_splits().mut_requests().push(split);
        // This method is executed only when there are unapplied entries after being restarted.
        // So there will be no callback, it's OK to return a response that does not matched
        // with its request.
        self.exec_batch_split(ctx, &admin_req)
    }

    fn exec_batch_split(
        &mut self,
        ctx: &mut PollContext,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
        let apply_before_split = || {
            fail_point!(
                "apply_before_split_1_3",
                { self.id == 3 && self.region_id() == 1 },
                |_| {}
            );
        };
        apply_before_split();

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["batch-split", "all"])
            .inc();

        let split_reqs = req.get_splits();
        let right_derive = split_reqs.get_right_derive();
        if split_reqs.get_requests().is_empty() {
            return Err(box_err!("missing split requests"));
        }
        let mut derived = self.region.clone();
        let new_region_cnt = split_reqs.get_requests().len();
        let mut regions = Vec::with_capacity(new_region_cnt + 1);
        let mut keys: VecDeque<Vec<u8>> = VecDeque::with_capacity(new_region_cnt + 1);
        for req in split_reqs.get_requests() {
            let split_key = req.get_split_key();
            if split_key.is_empty() {
                return Err(box_err!("missing split key"));
            }
            if split_key
                <= keys
                    .back()
                    .map_or_else(|| derived.get_start_key(), Vec::as_slice)
            {
                return Err(box_err!("invalid split request: {:?}", split_reqs));
            }
            if req.get_new_peer_ids().len() != derived.get_peers().len() {
                return Err(box_err!(
                    "invalid new peer id count, need {}, but got {}",
                    derived.get_peers().len(),
                    req.get_new_peer_ids().len()
                ));
            }
            keys.push_back(split_key.to_vec());
        }

        util::check_key_in_region(keys.back().unwrap(), &self.region)?;

        info!("{} split region {:?} with {:?}", self.tag, derived, keys);
        let new_version = derived.get_region_epoch().get_version() + new_region_cnt as u64;
        derived.mut_region_epoch().set_version(new_version);
        // Note that the split requests only contain ids for new regions, so we need
        // to handle new regions and old region seperately.
        if right_derive {
            // So the range of new regions is [old_start_key, split_key1, ..., last_split_key].
            keys.push_front(derived.get_start_key().to_vec());
        } else {
            // So the range of new regions is [split_key1, ..., last_split_key, old_end_key].
            keys.push_back(derived.get_end_key().to_vec());
            derived.set_end_key(keys.front().unwrap().to_vec());
            regions.push(derived.clone());
        }
        for req in split_reqs.get_requests() {
            let mut new_region = Region::new();
            // TODO: check new region id validation.
            new_region.set_id(req.get_new_region_id());
            new_region.set_region_epoch(derived.get_region_epoch().to_owned());
            new_region.set_start_key(keys.pop_front().unwrap());
            new_region.set_end_key(keys.front().unwrap().to_vec());
            new_region.set_peers(RepeatedField::from_slice(derived.get_peers()));
            for (peer, peer_id) in new_region
                .mut_peers()
                .iter_mut()
                .zip(req.get_new_peer_ids())
            {
                peer.set_id(*peer_id);
            }
            write_peer_state(
                &self.engines.kv,
                ctx.wb_mut(),
                &new_region,
                PeerState::Normal,
                None,
            ).and_then(|_| {
                write_initial_apply_state(&self.engines.kv, ctx.wb_mut(), new_region.get_id())
            })
                .unwrap_or_else(|e| {
                    panic!(
                        "{} fails to save split region {:?}: {:?}",
                        self.tag, new_region, e
                    )
                });
            regions.push(new_region);
        }
        if right_derive {
            derived.set_start_key(keys.pop_front().unwrap());
            regions.push(derived.clone());
        }
        write_peer_state(
            &self.engines.kv,
            ctx.wb_mut(),
            &derived,
            PeerState::Normal,
            None,
        ).unwrap_or_else(|e| panic!("{} fails to update region {:?}: {:?}", self.tag, derived, e));
        let mut resp = AdminResponse::new();
        resp.mut_splits()
            .set_regions(RepeatedField::from_slice(&regions));
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["batch-split", "success"])
            .inc();

        Ok((
            resp,
            EntryResult::Res(ExecResult::SplitRegion { regions, derived }),
        ))
    }

    fn exec_prepare_merge(
        &mut self,
        ctx: &mut PollContext,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["prepare_merge", "all"])
            .inc();

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

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["prepare_merge", "success"])
            .inc();

        Ok((
            AdminResponse::new(),
            EntryResult::Res(ExecResult::PrepareMerge {
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

    fn check_log_uptodate_for_merge(
        &mut self,
        merge: &CommitMergeRequest,
        source_region_id: u64,
    ) -> bool {
        let apply_state_key = keys::apply_state_key(source_region_id);
        let apply_state: RaftApplyState =
            match self.engines.kv.get_msg_cf(CF_RAFT, &apply_state_key) {
                Ok(Some(s)) => s,
                e => panic!(
                    "{} failed to get apply state of {}: {:?}",
                    self.tag, source_region_id, e
                ),
            };
        let apply_index = apply_state.get_applied_index();
        apply_index >= merge.get_commit()
    }

    fn catch_up_log_for_merge(&mut self, ctx: &mut PollContext, merge: CommitMergeRequest) -> bool {
        let apply_index = self.apply_state.get_applied_index();
        if apply_index >= merge.get_commit() {
            return true;
        }
        let entries = self.load_entries_for_merge(&merge, apply_index);
        if entries.is_empty() {
            // TODO: is it reachable?
            return true;
        }
        let b = self.handle_raft_committed_entries(ctx, entries);
        ctx.apply_res.last_mut().unwrap().merged = b;
        b
    }

    fn exec_commit_merge(
        &mut self,
        ctx: &mut PollContext,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
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

        let merge = req.get_commit_merge();
        let source_region = merge.get_source();
        if !self.check_log_uptodate_for_merge(merge, source_region.get_id()) {
            let (tx, rx) = ctx.router.one_shot(self.region_id());
            self.pending_merge_apply = Some(MergeAsyncWait {
                poller: rx,
                pending_entries: vec![],
                pending_tasks: vec![],
            });
            // TODO: maybe it's better to move.
            ctx.router
                .force_send_task(
                    source_region.get_id(),
                    Task::CatchUpLogs {
                        req: merge.to_owned(),
                        notifier: tx,
                    },
                )
                .unwrap();
            return Ok((AdminResponse::default(), EntryResult::Paused));
        }
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
        if *source_region != exist_region {
            panic!(
                "{} source_region {:?} not match exist region {:?}",
                self.tag, source_region, exist_region
            );
        }
        let mut region = self.region.clone();
        // Use a max value so that pd can ensure overlapped region has a priority.
        let version = cmp::max(
            source_region.get_region_epoch().get_version(),
            region.get_region_epoch().get_version(),
        ) + 1;
        region.mut_region_epoch().set_version(version);
        if keys::enc_end_key(&region) == keys::enc_start_key(source_region) {
            region.set_end_key(source_region.get_end_key().to_vec());
        } else {
            region.set_start_key(source_region.get_start_key().to_vec());
        }
        write_peer_state(&self.engines.kv, ctx.wb(), &region, PeerState::Normal, None)
            .and_then(|_| {
                // TODO: maybe all information needs to be filled?
                let mut merging_state = MergeState::new();
                merging_state.set_target(self.region.clone());
                write_peer_state(
                    &self.engines.kv,
                    ctx.wb(),
                    source_region,
                    PeerState::Tombstone,
                    Some(merging_state),
                )
            })
            .unwrap_or_else(|e| {
                panic!(
                    "{} failed to save merge region {:?}: {:?}",
                    self.tag, region, e
                )
            });

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["commit_merge", "success"])
            .inc();

        let resp = AdminResponse::new();
        Ok((
            resp,
            EntryResult::Res(ExecResult::CommitMerge {
                region,
                source: source_region.to_owned(),
            }),
        ))
    }

    fn exec_rollback_merge(
        &mut self,
        ctx: &mut PollContext,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
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
        Ok((
            resp,
            EntryResult::Res(ExecResult::RollbackMerge {
                region,
                commit: rollback.get_commit(),
            }),
        ))
    }

    fn exec_compact_log(
        &mut self,
        ctx: &mut PollContext,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["compact", "all"])
            .inc();

        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::new();
        let apply_state = &mut ctx.exec_ctx.as_mut().unwrap().apply_state;
        let first_index = peer_storage::first_index(apply_state);
        if compact_index <= first_index {
            debug!(
                "{} compact index {} <= first index {}, no need to compact",
                self.tag, compact_index, first_index
            );
            return Ok((resp, EntryResult::None));
        }
        if self.is_merging {
            info!(
                "{} is in merging mode, skip compact {}",
                self.tag, compact_index
            );
            return Ok((resp, EntryResult::None));
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
            return Err(box_err!(
                "command format is outdated, please upgrade leader."
            ));
        }

        // compact failure is safe to be omitted, no need to assert.
        compact_raft_log(&self.tag, apply_state, compact_index, compact_term)?;

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["compact", "success"])
            .inc();

        Ok((
            resp,
            EntryResult::Res(ExecResult::CompactLog {
                state: apply_state.get_truncated_state().clone(),
                first_index,
            }),
        ))
    }

    fn exec_write_cmd(
        &mut self,
        ctx: &PollContext,
        requests: &[Request],
    ) -> Result<(RaftCmdResponse, EntryResult)> {
        let mut responses = Vec::with_capacity(requests.len());

        let mut ranges = vec![];
        let mut ssts = vec![];
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Put => self.handle_put(ctx, req),
                CmdType::Delete => self.handle_delete(ctx, req),
                CmdType::DeleteRange => {
                    self.handle_delete_range(req, &mut ranges, ctx.use_delete_range)
                }
                CmdType::IngestSST => self.handle_ingest_sst(ctx, req, &mut ssts),
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
            }?;

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(RepeatedField::from_vec(responses));

        assert!(ranges.is_empty() || ssts.is_empty());
        let exec_res = if !ranges.is_empty() {
            EntryResult::Res(ExecResult::DeleteRange { ranges })
        } else if !ssts.is_empty() {
            EntryResult::Res(ExecResult::IngestSST { ssts })
        } else {
            EntryResult::None
        };

        Ok((resp, exec_res))
    }

    fn handle_put(&mut self, ctx: &PollContext, req: &Request) -> Result<Response> {
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

    fn handle_delete(&mut self, ctx: &PollContext, req: &Request) -> Result<Response> {
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

    fn handle_delete_range(
        &mut self,
        req: &Request,
        ranges: &mut Vec<Range>,
        use_delete_range: bool,
    ) -> Result<Response> {
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
        // TODO: Remove the `if` line after apply pool is implemented
        if cf == CF_LOCK || use_delete_range {
            util::delete_all_in_range_cf(
                &self.engines.kv,
                cf,
                &start_key,
                &end_key,
                use_delete_range,
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
        }

        ranges.push(Range::new(cf.to_owned(), start_key, end_key));

        Ok(resp)
    }

    fn handle_ingest_sst(
        &mut self,
        ctx: &PollContext,
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
        &self,
        ctx: &PollContext,
        _: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
        let resp = AdminResponse::new();
        Ok((
            resp,
            EntryResult::Res(ExecResult::ComputeHash {
                region: self.region.clone(),
                index: ctx.exec_ctx.as_ref().unwrap().index,
                // This snapshot may be held for a long time, which may cause too many
                // open files in rocksdb.
                // TODO: figure out another way to do consistency check without snapshot
                // or short life snapshot.
                snap: Snapshot::new(Arc::clone(&self.engines.kv)),
            }),
        ))
    }

    fn exec_verify_hash(
        &self,
        _: &PollContext,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, EntryResult)> {
        let verify_req = req.get_verify_hash();
        let index = verify_req.get_index();
        let hash = verify_req.get_hash().to_vec();
        let resp = AdminResponse::new();
        Ok((
            resp,
            EntryResult::Res(ExecResult::VerifyHash { index, hash }),
        ))
    }
}

#[derive(Debug)]
pub struct Apply {
    pub region_id: u64,
    term: u64,
    entries: Vec<Entry>,
    send_time: Instant,
}

impl Apply {
    pub fn new(region_id: u64, term: u64, entries: Vec<Entry>) -> Apply {
        Apply {
            region_id,
            term,
            entries,
            send_time: Instant::now(),
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
            apply_state: peer.get_store().apply_state().clone(),
            applied_index_term: peer.get_store().applied_index_term(),
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
    pub region_id: u64,
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

pub struct Destroy {
    region_id: u64,
}

/// region related task.
pub enum Task {
    Apply(Apply),
    CatchUpLogs {
        req: CommitMergeRequest,
        notifier: OneshotNotifier,
    },
    Registration(Registration),
    Proposal(RegionProposal),
    Destroy(Destroy),
    Noop,
}

impl Debug for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Task {
    pub fn register(peer: &Peer) -> Task {
        Task::Registration(Registration::new(peer))
    }

    pub fn destroy(region_id: u64) -> Task {
        Task::Destroy(Destroy { region_id })
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Apply(ref a) => write!(f, "[region {}] async apply", a.region_id),
            Task::Proposal(ref p) => write!(f, "[region {}] proposal", p.region_id),
            Task::Registration(ref r) => {
                write!(f, "[region {}] Reg {:?}", r.region.get_id(), r.apply_state)
            }
            Task::CatchUpLogs { ref req, .. } => {
                write!(f, "[region {}] CatchUpLogs", req.get_source().get_id())
            }
            Task::Destroy(ref d) => write!(f, "[region {}] destroy", d.region_id),
            Task::Noop => write!(f, "Noop"),
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
    pub exec_res: Option<VecDeque<ExecResult>>,
    pub metrics: ApplyMetrics,
    pub merged: bool,
}

#[derive(Debug)]
pub enum TaskRes {
    Apply(ApplyRes),
    Destroy { region_id: u64, id: u64 },
}

pub struct FallbackDelegate {
    pub stopped: bool,
}

impl FallbackDelegate {
    pub fn new() -> FallbackDelegate {
        FallbackDelegate { stopped: false }
    }
}

pub struct FallbackPoller<'a> {
    delegate: &'a mut FallbackDelegate,
    ctx: &'a mut PollContext,
    scheduler: &'a Scheduler,
}

impl<'a> FallbackPoller<'a> {
    pub fn new(
        delegate: &'a mut FallbackDelegate,
        ctx: &'a mut PollContext,
        scheduler: &'a Scheduler,
    ) -> FallbackPoller<'a> {
        FallbackPoller {
            delegate,
            ctx,
            scheduler,
        }
    }

    fn handle_tasks(&mut self, buf: &mut Vec<Task>) {
        for task in buf.drain(..) {
            match task {
                Task::Registration(reg) => match self
                    .ctx
                    .router
                    .try_send_task(reg.region.get_id(), Task::Registration(reg))
                {
                    Ok(()) => return,
                    Err(SendError(Task::Registration(reg))) => {
                        let delegate =
                            ApplyDelegate::from_registration(self.ctx.engines.clone(), reg);
                        self.scheduler.schedule(delegate);
                    }
                    _ => unreachable!(),
                },
                Task::Destroy(d) => {
                    let region_id = d.region_id;
                    let res = self.ctx.router.try_send_task(region_id, Task::Destroy(d));
                    error!("[region {}] failed to send destroy: {:?}", region_id, res);
                }
                Task::Apply(a) => {
                    let region_id = a.region_id;
                    let res = self.ctx.router.try_send_task(region_id, Task::Apply(a));
                    error!("[region {}] failed to send apply: {:?}", region_id, res);
                }
                Task::CatchUpLogs { req, notifier } => {
                    let region_id = req.get_source().get_id();
                    let res = self
                        .ctx
                        .router
                        .try_send_task(region_id, Task::CatchUpLogs { req, notifier });
                    error!(
                        "[region {}] failed to send catchuplogs: {:?}",
                        region_id, res
                    );
                }
                Task::Proposal(p) => {
                    let region_id = p.region_id;
                    let res = self.ctx.router.try_send_task(region_id, Task::Proposal(p));
                    error!("[region {}] failed to send proposal: {:?}", region_id, res);
                }
                Task::Noop => {}
            }
        }
    }

    pub fn poll(&mut self, receiver: &Receiver<Task>, buf: &mut Vec<Task>) -> Option<usize> {
        let mut mark = None;
        while buf.len() < self.ctx.cfg.messages_per_tick {
            match receiver.try_recv() {
                Ok(msg) => buf.push(msg),
                Err(TryRecvError::Empty) => {
                    mark = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    self.delegate.stopped = true;
                    mark = Some(0);
                    break;
                }
            }
        }
        self.handle_tasks(buf);
        mark
    }
}

pub struct ApplyPoller<'a> {
    delegate: &'a mut ApplyDelegate,
    ctx: &'a mut PollContext,
}

impl<'a> ApplyPoller<'a> {
    pub fn new(delegate: &'a mut ApplyDelegate, ctx: &'a mut PollContext) -> ApplyPoller<'a> {
        ApplyPoller { delegate, ctx }
    }

    // Return true means poller is ready to handle next task.
    fn on_apply_task(&mut self, task: Task) -> bool {
        match task {
            Task::Apply(a) => {
                let elapsed = duration_to_sec(a.send_time.elapsed());
                APPLY_TASK_WAIT_TIME_HISTOGRAM.observe(elapsed);
                self.handle_apply(a)
            }
            Task::Proposal(p) => self.handle_proposal(p),
            Task::Registration(s) => self.handle_registration(s),
            Task::Destroy(d) => self.handle_destroy(d),
            Task::CatchUpLogs { req, notifier } => {
                let _notifier = notifier;
                self.delegate.catch_up_log_for_merge(&mut self.ctx, req)
            }
            Task::Noop => true,
        }
    }

    /// Return true means merge is handled.
    fn resume_handling_pending_apply(&mut self) -> bool {
        let mut merge_apply = self.delegate.pending_merge_apply.take().unwrap();
        if !merge_apply.poller.waken() {
            self.delegate.pending_merge_apply = Some(merge_apply);
            return false;
        }
        if !merge_apply.pending_entries.is_empty()
            && !self
                .delegate
                .handle_raft_committed_entries(&mut self.ctx, merge_apply.pending_entries)
        {
            let pending_merge = self.delegate.pending_merge_apply.as_mut().unwrap();
            pending_merge.pending_tasks = merge_apply.pending_tasks;
            return false;
        }
        if !merge_apply.pending_tasks.is_empty() {
            self.handle_tasks(&mut merge_apply.pending_tasks)
        } else {
            true
        }
    }

    /// Return true means all tasks are handled.
    fn handle_tasks(&mut self, tasks: &mut Vec<Task>) -> bool {
        let mut drainer = tasks.drain(..);
        while let Some(m) = drainer.next() {
            if !self.on_apply_task(m) {
                break;
            }
        }
        if let Some(ref mut apply) = self.delegate.pending_merge_apply {
            apply.pending_tasks = drainer.collect();
            false
        } else {
            true
        }
    }

    // Return true means poller should try to release the fsm.
    pub fn poll(&mut self, receiver: &Receiver<Task>, buf: &mut Vec<Task>) -> Option<usize> {
        let mut mark = None;
        if self.delegate.pending_merge_apply.is_some() {
            mark = Some(receiver.len());
            if !self.resume_handling_pending_apply() {
                // TODO: this will occupy one thread, we need to figure out a way to solve the problem.
                return mark;
            }
            mark = None;
        }
        while buf.len() < self.ctx.cfg.messages_per_tick {
            match receiver.try_recv() {
                Ok(msg) => buf.push(msg),
                Err(TryRecvError::Empty) => {
                    mark = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    self.delegate.stopped = true;
                    mark = Some(0);
                    break;
                }
            }
        }
        if !self.handle_tasks(buf) {
            // Merge should be checked later.
            mark = Some(0);
        }
        if self.delegate.stopped {
            self.delegate.destroy(&mut self.ctx);
        }
        mark
    }

    pub fn stopped(&self) -> bool {
        self.delegate.stopped
    }

    // Return true means it's OK to handle next apply.
    fn handle_apply(&mut self, apply: Apply) -> bool {
        if apply.entries.is_empty() || self.ctx.merged_regions.contains(&apply.region_id) {
            return true;
        }

        self.delegate.metrics = ApplyMetrics::default();
        self.delegate.term = apply.term;

        self.delegate
            .handle_raft_committed_entries(&mut self.ctx, apply.entries);

        if self.delegate.pending_merge_apply.is_some() {
            return false;
        }

        if self.delegate.pending_remove {
            self.delegate.destroy(self.ctx);
            false
        } else {
            true
        }
    }

    fn handle_proposal(&mut self, region_proposal: RegionProposal) -> bool {
        assert_eq!(self.delegate.id, region_proposal.id);
        let propose_num = region_proposal.props.len();
        for p in region_proposal.props {
            let cmd = PendingCmd::new(p.index, p.term, p.cb);
            if p.is_conf_change {
                if let Some(cmd) = self.delegate.pending_cmds.take_conf_change() {
                    // if it loses leadership before conf change is replicated, there may be
                    // a stale pending conf change before next conf change is applied. If it
                    // becomes leader again with the stale pending conf change, will enter
                    // this block, so we notify leadership may have been changed.
                    notify_stale_command(&self.delegate.tag, self.delegate.term, cmd);
                }
                self.delegate.pending_cmds.set_conf_change(cmd);
            } else {
                self.delegate.pending_cmds.append_normal(cmd);
            }
        }
        APPLY_PROPOSAL.observe(propose_num as f64);
        true
    }

    fn handle_registration(&mut self, s: Registration) -> bool {
        let peer_id = s.id;
        let term = s.term;
        let delegate = ApplyDelegate::from_registration(self.ctx.engines.clone(), s);
        info!(
            "{} register to apply delegates at term {}",
            delegate.tag, delegate.term
        );
        assert_eq!(self.delegate.id, peer_id);
        self.delegate.term = term;
        self.delegate.clear_all_commands_as_stale();
        *self.delegate = delegate;
        true
    }

    fn handle_destroy(&mut self, d: Destroy) -> bool {
        // Only respond when the meta exists. Otherwise if destroy is triggered
        // multiple times, the store may destroy wrong target peer.
        info!("{} remove from apply delegates", self.delegate.tag);
        self.delegate.destroy(self.ctx);
        self.ctx
            .notifier
            .force_send_peer_message(
                d.region_id,
                PeerMsg::ApplyRes(TaskRes::Destroy {
                    region_id: self.delegate.region_id(),
                    id: self.delegate.id(),
                }),
            )
            .unwrap();
        false
    }
}
/*
#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::atomic::*;
    use std::sync::*;
    use std::time::*;

    use kvproto::metapb::{self, RegionEpoch};
    use kvproto::raft_cmdpb::*;
    use protobuf::Message;
    use raftstore::coprocessor::*;
    use raftstore::store::fsm::apply_transport::Router as ApplyRouter;
    use raftstore::store::msg::WriteResponse;
    use raftstore::store::peer_storage::RAFT_INIT_LOG_INDEX;
    use raftstore::store::util::{new_learner_peer, new_peer};
    use rocksdb::{Writable, WriteBatch, DB};
    use tempdir::TempDir;

    use super::*;
    use import::test_helpers::*;
    use util::collections::HashMap;

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
        tx: Router,
    ) -> PollContext {
        let cfg = Arc::new(Config::default());
        let (notifier, _) = ApplyRouter::new_for_test(1);
        PollContext::new(engines, cfg, tx, notifier, host, importer)
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
        let (router, rx) = Router::new_for_test(2);
        let (_tmp, engines) = create_tmp_engine("apply-basic");
        let host = Arc::new(CoprocessorHost::default());
        let (_dir, importer) = create_tmp_importer("apply-basic");
        let mut runner = new_runner(
            engines.clone(),
            Arc::clone(&host),
            Arc::clone(&importer),
            router,
        );

        let mut reg = Registration::default();
        reg.id = 1;
        reg.region.set_id(2);
        reg.apply_state.set_applied_index(3);
        reg.term = 4;
        reg.applied_index_term = 5;
        runner.handle_tasks(&mut vec![Task::Registration(reg.clone())]);
        assert!(runner.delegates.get(&2).is_some());
        {
            let delegate = &runner.delegates[&2].as_ref().unwrap();
            assert_eq!(delegate.id, 1);
            assert_eq!(delegate.tag, "[region 2] 1");
            assert_eq!(delegate.region, reg.region);
            assert!(!delegate.pending_remove);
            assert_eq!(delegate.apply_state, reg.apply_state);
            assert_eq!(delegate.term, reg.term);
            assert_eq!(delegate.applied_index_term, reg.applied_index_term);
        }

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
        runner.run_batch(&mut vec![Task::Proposals(vec![region_proposal])]);
        // unregistered region should be ignored and notify failed.
        assert!(rx.try_recv().is_err());
        let resp = resp_rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_region_not_found());

        let (cc_tx, cc_rx) = mpsc::channel();
        let pops = vec![
            Proposal::new(false, 2, 0, Callback::None),
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
        runner.run_batch(&mut vec![Task::Proposals(vec![region_proposal])]);
        assert!(rx.try_recv().is_err());
        {
            let normals = &runner.delegates[&2].as_ref().unwrap().pending_cmds.normals;
            assert_eq!(normals.back().map(|c| c.index), Some(2));
        }
        assert!(rx.try_recv().is_err());
        {
            let cc = &runner.delegates[&2]
                .as_ref()
                .unwrap()
                .pending_cmds
                .conf_change;
            assert_eq!(cc.as_ref().map(|c| c.index), Some(3));
        }

        let p = Proposal::new(true, 4, 0, Callback::None);
        let region_proposal = RegionProposal::new(1, 2, vec![p]);
        runner.run_batch(&mut vec![Task::Proposals(vec![region_proposal])]);
        assert!(rx.try_recv().is_err());
        {
            let cc = &runner.delegates[&2]
                .as_ref()
                .unwrap()
                .pending_cmds
                .conf_change;
            assert_eq!(cc.as_ref().map(|c| c.index), Some(4));
        }
        // propose another conf change should mark previous stale.
        let cc_resp = cc_rx.try_recv().unwrap();
        assert!(cc_resp.get_header().get_error().has_stale_command());

        runner.run_batch(&mut vec![Task::applies(vec![Apply::new(
            1,
            1,
            vec![new_entry(2, 3, None)],
        )])]);
        // non registered region should be ignored.
        assert!(rx.try_recv().is_err());

        runner.run_batch(&mut vec![Task::applies(vec![Apply::new(2, 11, vec![])])]);
        // empty entries should be ignored.
        assert!(rx.try_recv().is_err());
        assert_eq!(runner.delegates[&2].as_ref().unwrap().term, reg.term);

        let apply_state_key = keys::apply_state_key(2);
        assert!(engines.kv.get(&apply_state_key).unwrap().is_none());
        runner.run_batch(&mut vec![Task::applies(vec![Apply::new(
            2,
            11,
            vec![new_entry(5, 4, None)],
        )])]);
        let apply_res = match rx.try_recv() {
            Ok(PeerMsg::ApplyRes(TaskRes::Apply(res))) => res,
            e => panic!("unexpected apply result: {:?}", e),
        };
        assert_eq!(apply_res.region_id, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 4);
        assert!(apply_res.exec_res.is_none());
        // empty entry will make applied_index step forward and should write apply state to engine.
        assert_eq!(apply_res.metrics.written_keys, 1);
        assert_eq!(apply_res.applied_index_term, 5);
        {
            let delegate = &runner.delegates[&2].as_ref().unwrap();
            assert_eq!(delegate.term, 11);
            assert_eq!(delegate.applied_index_term, 5);
            assert_eq!(delegate.apply_state.get_applied_index(), 4);
            let apply_state: RaftApplyState = engines
                .kv
                .get_msg_cf(CF_RAFT, &apply_state_key)
                .unwrap()
                .unwrap();
            assert_eq!(apply_state, delegate.apply_state);
        }

        runner.run_batch(&mut vec![Task::destroy(2)]);
        let destroy_res = match rx.try_recv() {
            Ok(PeerMsg::ApplyRes(TaskRes::Destroy(d))) => d,
            e => panic!("expected destroy result, but got {:?}", e),
        };
        assert_eq!(destroy_res.id, 1);
        assert_eq!(destroy_res.applied_index_term, 5);

        runner.shutdown();
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
            tx: ::std::sync::mpsc::Sender<RaftCmdResponse>,
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

        fn split(mut self, splits: BatchSplitRequest) -> EntryBuilder {
            let mut req = AdminRequest::new();
            req.set_cmd_type(AdminCmdType::BatchSplit);
            req.set_splits(splits);
            self.req.set_admin_request(req);
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
        let mut reg = Registration::default();
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_version(3);
        let mut delegate = ApplyDelegate::from_registration(engines.clone(), reg);
        let mut delegates = HashMap::default();
        let (tx, rx) = mpsc::channel();
        let (router, _) = Router::new_for_test(1);

        let put_entry = EntryBuilder::new(1, 1)
            .put(b"k1", b"v1")
            .put(b"k2", b"v1")
            .put(b"k3", b"v1")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let mut host = Arc::new(CoprocessorHost::default());
        let obs = ApplyObserver::default();
        host.registry
            .register_query_observer(1, Box::new(obs.clone()));
        let mut apply_ctx = new_runner(engines.clone(), host.clone(), importer.clone(), router);
        apply_ctx.use_delete_range(true);
        delegate.handle_raft_committed_entries(&mut apply_ctx, vec![put_entry]);
        apply_ctx.write_to_db();
        assert!(apply_ctx.apply_res.last().unwrap().exec_res.is_none());
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
        delegate.handle_raft_committed_entries(&mut apply_ctx, vec![put_entry]);
        apply_ctx.write_to_db();
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
        delegate.handle_raft_committed_entries(&mut apply_ctx, vec![put_entry]);
        apply_ctx.write_to_db();
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
        delegate.handle_raft_committed_entries(&mut apply_ctx, vec![put_entry]);
        apply_ctx.write_to_db();
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
        delegate.handle_raft_committed_entries(&mut apply_ctx, vec![put_entry]);
        apply_ctx.write_to_db();
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
        delegate.handle_raft_committed_entries(&mut apply_ctx, vec![delete_entry]);
        apply_ctx.write_to_db();
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());

        let delete_range_entry = EntryBuilder::new(7, 3)
            .delete_range(b"", b"")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        delegate.handle_raft_committed_entries(&mut apply_ctx, vec![delete_range_entry]);
        apply_ctx.write_to_db();
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
        delegate.handle_raft_committed_entries(&mut apply_ctx, vec![delete_range_entry]);
        apply_ctx.write_to_db();
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
        delegate.handle_raft_committed_entries(&mut apply_ctx, entries);
        apply_ctx.write_to_db();
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
        delegate.handle_raft_committed_entries(&mut apply_ctx, entries);
        apply_ctx.write_to_db();
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

    #[test]
    fn test_stash() {
        let (_path, engines) = create_tmp_engine("test-delegate");
        let (_import_dir, importer) = create_tmp_importer("test-delegate");
        let mut reg = Registration::default();
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_version(3);
        let mut delegate1 = ApplyDelegate::from_registration(engines.clone(), reg);
        delegate1.apply_state.set_applied_index(3);
        reg = Registration::default();
        reg.region.set_start_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_version(3);
        let mut delegate2 = ApplyDelegate::from_registration(engines.clone(), reg);
        delegate2.apply_state.set_applied_index(1);

        let host = Arc::new(CoprocessorHost::default());
        let (router, _) = Router::new_for_test(1);
        let (tx, rx) = mpsc::channel();
        let core = new_runner(engines, host, importer, router);
        core.prepare_for(&delegate1);
        assert_eq!(core.last_applied_index, 3);
        let state = delegate1.apply_state.clone();
        core.exec_ctx = Some(ExecContext::new(state, RaftCmdRequest::new(), 4, 2));
        core.wb.as_mut().unwrap().put(b"k1", b"v1").unwrap();
        let tx1 = tx.clone();
        assert_eq!(core.cbs.last().unwrap().region, delegate1.region);
        core.cbs.last_mut().unwrap().push(
            Some(Callback::Write(Box::new(move |_| tx1.send(1).unwrap()))),
            RaftCmdResponse::new(),
        );
        core.exec_ctx
            .as_mut()
            .unwrap()
            .apply_state
            .set_applied_index(4);

        let stash = core.stash(&mut delegate1);
        core.prepare_for(&delegate2);
        assert!(core.exec_ctx.is_none());
        assert_eq!(core.last_applied_index, 1);
        let state = delegate2.apply_state.clone();
        core.exec_ctx = Some(ExecContext::new(state, 2, 3));
        core.wb.as_mut().unwrap().put(b"k2", b"v2").unwrap();
        let tx1 = tx.clone();
        assert_eq!(core.cbs.last().unwrap().region, delegate2.region);
        core.cbs.last_mut().unwrap().push(
            Some(Callback::Write(Box::new(move |_| tx1.send(2).unwrap()))),
            RaftCmdResponse::new(),
        );
        delegate2.apply_state = core.exec_ctx.take().unwrap().apply_state;
        core.finish_for(&mut delegate2, None);

        core.restore_stash(stash);
        assert_eq!(core.last_applied_index, 3);
        core.wb.as_mut().unwrap().put(b"k3", b"v3").unwrap();
        let tx1 = tx.clone();
        assert_eq!(core.cbs.last().unwrap().region, delegate1.region);
        core.cbs.last_mut().unwrap().push(
            Some(Callback::Write(Box::new(move |_| tx1.send(3).unwrap()))),
            RaftCmdResponse::new(),
        );
        delegate1.apply_state = core.exec_ctx.take().unwrap().apply_state;
        core.finish_for(&mut delegate1, None);
        core.write_to_db();

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 3);
        assert_eq!(delegate1.apply_state.get_applied_index(), 4);
        assert_eq!(delegate2.apply_state.get_applied_index(), 1);
        let written_bytes1 = delegate1.metrics.written_bytes;
        let written_bytes2 = delegate2.metrics.written_bytes;
        assert!(
            written_bytes1 > written_bytes2,
            "{} > {}",
            written_bytes1,
            written_bytes2
        );
        assert_eq!(delegate1.metrics.written_keys, 3); // 2 kvs + 1 state
        assert_eq!(delegate2.metrics.written_keys, 2); // 1 kv + 1 state
    }

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::new();
        req.set_split_key(key.to_vec());
        req.set_new_region_id(id);
        req.set_new_peer_ids(children);
        req
    }

    struct SplitResultChecker<'a> {
        db: &'a DB,
        origin_peers: &'a [metapb::Peer],
        epoch: Rc<RefCell<RegionEpoch>>,
    }

    impl<'a> SplitResultChecker<'a> {
        fn check(&self, start: &[u8], end: &[u8], id: u64, children: &[u64], check_initial: bool) {
            let key = keys::region_state_key(id);
            let state: RegionLocalState = self.db.get_msg_cf(CF_RAFT, &key).unwrap().unwrap();
            assert_eq!(state.get_state(), PeerState::Normal);
            assert_eq!(state.get_region().get_id(), id);
            assert_eq!(state.get_region().get_start_key(), start);
            assert_eq!(state.get_region().get_end_key(), end);
            let expect_peers: Vec<_> = self
                .origin_peers
                .iter()
                .zip(children)
                .map(|(p, new_id)| {
                    let mut new_peer = metapb::Peer::clone(p);
                    new_peer.set_id(*new_id);
                    new_peer
                })
                .collect();
            assert_eq!(state.get_region().get_peers(), expect_peers.as_slice());
            assert!(!state.has_merge_state(), "{:?}", state);
            let epoch = self.epoch.borrow();
            assert_eq!(*state.get_region().get_region_epoch(), *epoch);
            if !check_initial {
                return;
            }
            let key = keys::apply_state_key(id);
            let initial_state: RaftApplyState = self.db.get_msg_cf(CF_RAFT, &key).unwrap().unwrap();
            assert_eq!(initial_state.get_applied_index(), RAFT_INIT_LOG_INDEX);
            assert_eq!(
                initial_state.get_truncated_state().get_index(),
                RAFT_INIT_LOG_INDEX
            );
            assert_eq!(
                initial_state.get_truncated_state().get_term(),
                RAFT_INIT_LOG_INDEX
            );
        }
    }

    fn error_msg(resp: &RaftCmdResponse) -> &str {
        resp.get_header().get_error().get_message()
    }

    #[test]
    fn test_split() {
        let (_path, engines) = create_tmp_engine("test-delegate");
        let (_import_dir, importer) = create_tmp_importer("test-delegate");
        let mut reg = Registration::default();
        reg.region.set_id(1);
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_version(3);
        let peers = vec![new_peer(2, 3), new_peer(4, 5), new_learner_peer(6, 7)];
        reg.region.set_peers(RepeatedField::from_vec(peers.clone()));
        let mut delegate = ApplyDelegate::from_registration(engines.clone(), reg);
        let mut delegates = HashMap::default();
        let (tx, rx) = mpsc::channel();
        let host = CoprocessorHost::default();

        let mut index_id = 1;
        let mut exec_split = |delegate: &mut ApplyDelegate, reqs| {
            let (router, _) = Router::new_for_test(1);
            let mut apply_ctx = new_runner(engines, host, importer, router);
            apply_ctx.use_delete_range(true);
            let epoch = delegate.region.get_region_epoch().to_owned();
            let split = EntryBuilder::new(index_id, 1)
                .split(reqs)
                .epoch(epoch.get_conf_ver(), epoch.get_version())
                .capture_resp(delegate, tx.clone())
                .build();
            delegate.handle_raft_committed_entries(&mut apply_ctx, vec![split]);
            apply_ctx.write_to_db();
            index_id += 1;
            rx.try_recv().unwrap()
        };

        let mut splits = BatchSplitRequest::new();
        splits.set_right_derive(true);
        splits.mut_requests().push(new_split_req(b"k1", 8, vec![]));
        let resp = exec_split(&mut delegate, splits.clone());
        // 3 followers are required.
        assert!(error_msg(&resp).contains("id count"), "{:?}", resp);

        splits.mut_requests().clear();
        let resp = exec_split(&mut delegate, splits.clone());
        // Empty requests should be rejected.
        assert!(error_msg(&resp).contains("missing"), "{:?}", resp);

        splits
            .mut_requests()
            .push(new_split_req(b"k6", 8, vec![9, 10, 11]));
        let resp = exec_split(&mut delegate, splits.clone());
        // Out of range keys should be rejected.
        assert!(
            resp.get_header().get_error().has_key_not_in_region(),
            "{:?}",
            resp
        );

        splits
            .mut_requests()
            .push(new_split_req(b"", 8, vec![9, 10, 11]));
        let resp = exec_split(&mut delegate, splits.clone());
        // Empty key should be rejected.
        assert!(error_msg(&resp).contains("missing"), "{:?}", resp);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 8, vec![9, 10, 11]));
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        let resp = exec_split(&mut delegate, splits.clone());
        // keys should be in ascend order.
        assert!(error_msg(&resp).contains("invalid"), "{:?}", resp);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 8, vec![9, 10]));
        let resp = exec_split(&mut delegate, splits.clone());
        // All requests should be checked.
        assert!(error_msg(&resp).contains("id count"), "{:?}", resp);

        let epoch = Rc::new(RefCell::new(delegate.region.get_region_epoch().clone()));
        let mut new_version = epoch.borrow().get_version() + 1;
        epoch.borrow_mut().set_version(new_version);
        let checker = SplitResultChecker {
            db: &engines.kv,
            origin_peers: &peers,
            epoch: epoch.clone(),
        };

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        let resp = exec_split(&mut delegate, splits.clone());
        // Split should succeed.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        checker.check(b"", b"k1", 8, &[9, 10, 11], true);
        checker.check(b"k1", b"k5", 1, &[3, 5, 7], false);

        new_version = epoch.borrow().get_version() + 1;
        epoch.borrow_mut().set_version(new_version);
        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k4", 12, vec![13, 14, 15]));
        splits.set_right_derive(false);
        let resp = exec_split(&mut delegate, splits.clone());
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        checker.check(b"k4", b"k5", 12, &[13, 14, 15], true);
        checker.check(b"k1", b"k4", 1, &[3, 5, 7], false);

        new_version = epoch.borrow().get_version() + 2;
        epoch.borrow_mut().set_version(new_version);
        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 16, vec![17, 18, 19]));
        splits
            .mut_requests()
            .push(new_split_req(b"k3", 20, vec![21, 22, 23]));
        splits.set_right_derive(true);
        let resp = exec_split(&mut delegate, splits.clone());
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        checker.check(b"k1", b"k2", 16, &[17, 18, 19], true);
        checker.check(b"k2", b"k3", 20, &[21, 22, 23], true);
        checker.check(b"k3", b"k4", 1, &[3, 5, 7], false);

        new_version = epoch.borrow().get_version() + 2;
        epoch.borrow_mut().set_version(new_version);
        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k31", 24, vec![25, 26, 27]));
        splits
            .mut_requests()
            .push(new_split_req(b"k32", 28, vec![29, 30, 31]));
        splits.set_right_derive(false);
        let resp = exec_split(&mut delegate, splits.clone());
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        checker.check(b"k3", b"k31", 1, &[3, 5, 7], false);
        checker.check(b"k31", b"k32", 24, &[25, 26, 27], true);
        checker.check(b"k32", b"k4", 28, &[29, 30, 31], true);
    }
}
*/
