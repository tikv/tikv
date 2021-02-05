// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::cmp::{Ord, Ordering as CmpOrdering};
use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::mpsc::Sender;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec::Drain;
use std::{cmp, usize};

use batch_system::{BasicMailbox, BatchRouter, BatchSystem, Fsm, HandlerBuilder, PollHandler};
use collections::{HashMap, HashMapEntry, HashSet};
use crossbeam::channel::{TryRecvError, TrySendError};
use engine_traits::PerfContext;
use engine_traits::PerfContextKind;
use engine_traits::{
    DeleteStrategy, KvEngine, RaftEngine, Range as EngineRange, Snapshot, WriteBatch,
};
use engine_traits::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use kvproto::import_sstpb::SstMeta;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::{PeerRole, Region, RegionEpoch};
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, AdminResponse, ChangePeerRequest, CmdType, CommitMergeRequest,
    RaftCmdRequest, RaftCmdResponse, Request, Response,
};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftApplyState, RaftTruncatedState, RegionLocalState,
};
use raft::eraftpb::{
    ConfChange, ConfChangeType, ConfChangeV2, Entry, EntryType, Snapshot as RaftSnapshot,
};
use raft_proto::ConfChangeI;
use sst_importer::SSTImporter;
use tikv_util::config::{Tracker, VersionTrack};
use tikv_util::mpsc::{loose_bounded, LooseBoundedSender, Receiver};
use tikv_util::time::{duration_to_sec, Instant};
use tikv_util::worker::Scheduler;
use tikv_util::{Either, MustConsumeVec};
use time::Timespec;
use uuid::Builder as UuidBuilder;

use crate::coprocessor::{Cmd, CoprocessorHost};
use crate::store::fsm::RaftPollerBuilder;
use crate::store::metrics::*;
use crate::store::msg::{Callback, PeerMsg, ReadResponse, SignificantMsg};
use crate::store::peer::Peer;
use crate::store::peer_storage::{
    self, write_initial_apply_state, write_peer_state, ENTRY_MEM_SIZE,
};
use crate::store::util::{
    check_region_epoch, compare_region_epoch, is_learner, ChangePeerI, ConfChangeKind,
    KeysInfoFormatter, ADMIN_CMD_EPOCH_MAP,
};
use crate::store::{cmd_resp, util, Config, RegionSnapshot, RegionTask};
use crate::{Error, Result};

use super::metrics::*;

const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;
const APPLY_WB_SHRINK_SIZE: usize = 1024 * 1024;
const SHRINK_PENDING_CMD_QUEUE_CAP: usize = 64;

pub struct PendingCmd<S>
where
    S: Snapshot,
{
    pub index: u64,
    pub term: u64,
    pub cb: Option<Callback<S>>,
}

impl<S> PendingCmd<S>
where
    S: Snapshot,
{
    fn new(index: u64, term: u64, cb: Callback<S>) -> PendingCmd<S> {
        PendingCmd {
            index,
            term,
            cb: Some(cb),
        }
    }
}

impl<S> Drop for PendingCmd<S>
where
    S: Snapshot,
{
    fn drop(&mut self) {
        if self.cb.is_some() {
            safe_panic!(
                "callback of pending command at [index: {}, term: {}] is leak",
                self.index,
                self.term
            );
        }
    }
}

impl<S> Debug for PendingCmd<S>
where
    S: Snapshot,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PendingCmd [index: {}, term: {}, has_cb: {}]",
            self.index,
            self.term,
            self.cb.is_some()
        )
    }
}

/// Commands waiting to be committed and applied.
#[derive(Debug)]
pub struct PendingCmdQueue<S>
where
    S: Snapshot,
{
    normals: VecDeque<PendingCmd<S>>,
    conf_change: Option<PendingCmd<S>>,
}

impl<S> PendingCmdQueue<S>
where
    S: Snapshot,
{
    fn new() -> PendingCmdQueue<S> {
        PendingCmdQueue {
            normals: VecDeque::new(),
            conf_change: None,
        }
    }

    fn pop_normal(&mut self, index: u64, term: u64) -> Option<PendingCmd<S>> {
        self.normals.pop_front().and_then(|cmd| {
            if self.normals.capacity() > SHRINK_PENDING_CMD_QUEUE_CAP
                && self.normals.len() < SHRINK_PENDING_CMD_QUEUE_CAP
            {
                self.normals.shrink_to_fit();
            }
            if (cmd.term, cmd.index) > (term, index) {
                self.normals.push_front(cmd);
                return None;
            }
            Some(cmd)
        })
    }

    fn append_normal(&mut self, cmd: PendingCmd<S>) {
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PendingCmd<S>> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        self.conf_change.take()
    }

    // TODO: seems we don't need to separate conf change from normal entries.
    fn set_conf_change(&mut self, cmd: PendingCmd<S>) {
        self.conf_change = Some(cmd);
    }
}

#[derive(Default, Debug)]
pub struct ChangePeer {
    pub index: u64,
    // The proposed ConfChangeV2 or (legacy) ConfChange
    // ConfChange (if it is) will convert to ConfChangeV2
    pub conf_change: ConfChangeV2,
    // The change peer requests come along with ConfChangeV2
    // or (legacy) ConfChange, for ConfChange, it only contains
    // one element
    pub changes: Vec<ChangePeerRequest>,
    pub region: Region,
}

pub struct Range {
    pub cf: String,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl Debug for Range {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{{ cf: {:?}, start_key: {:?}, end_key: {:?} }}",
            self.cf,
            log_wrappers::Value::key(&self.start_key),
            log_wrappers::Value::key(&self.end_key)
        )
    }
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
pub enum ExecResult<S> {
    ChangePeer(ChangePeer),
    CompactLog {
        state: RaftTruncatedState,
        first_index: u64,
    },
    SplitRegion {
        regions: Vec<Region>,
        derived: Region,
        new_split_regions: HashMap<u64, NewSplitPeer>,
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
        context: Vec<u8>,
        snap: S,
    },
    VerifyHash {
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    },
    DeleteRange {
        ranges: Vec<Range>,
    },
    IngestSst {
        ssts: Vec<SstMeta>,
    },
}

/// The possible returned value when applying logs.
pub enum ApplyResult<S> {
    None,
    Yield,
    /// Additional result that needs to be sent back to raftstore.
    Res(ExecResult<S>),
    /// It is unable to apply the `CommitMerge` until the source peer
    /// has applied to the required position and sets the atomic boolean
    /// to true.
    WaitMergeSource(Arc<AtomicU64>),
}

struct ExecContext {
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

struct ApplyCallback<EK>
where
    EK: KvEngine,
{
    region: Region,
    cbs: Vec<(Option<Callback<EK::Snapshot>>, Cmd)>,
}

impl<EK> ApplyCallback<EK>
where
    EK: KvEngine,
{
    fn new(region: Region) -> Self {
        let cbs = vec![];
        ApplyCallback { region, cbs }
    }

    fn invoke_all(self, host: &CoprocessorHost<EK>) {
        for (cb, mut cmd) in self.cbs {
            host.post_apply(&self.region, &mut cmd);
            if let Some(cb) = cb {
                cb.invoke_with_response(cmd.response)
            };
        }
    }

    fn push(&mut self, cb: Option<Callback<EK::Snapshot>>, cmd: Cmd) {
        self.cbs.push((cb, cmd));
    }
}

pub trait Notifier<EK: KvEngine>: Send {
    fn notify(&self, apply_res: Vec<ApplyRes<EK::Snapshot>>);
    fn notify_one(&self, region_id: u64, msg: PeerMsg<EK>);
    fn clone_box(&self) -> Box<dyn Notifier<EK>>;
}

struct ApplyContext<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    tag: String,
    timer: Option<Instant>,
    host: CoprocessorHost<EK>,
    importer: Arc<SSTImporter>,
    region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    router: ApplyRouter<EK>,
    notifier: Box<dyn Notifier<EK>>,
    engine: EK,
    cbs: MustConsumeVec<ApplyCallback<EK>>,
    apply_res: Vec<ApplyRes<EK::Snapshot>>,
    exec_ctx: Option<ExecContext>,

    kv_wb: W,
    kv_wb_last_bytes: u64,
    kv_wb_last_keys: u64,

    last_applied_index: u64,
    committed_count: usize,

    // Whether synchronize WAL is preferred.
    sync_log_hint: bool,
    // Whether to use the delete range API instead of deleting one by one.
    use_delete_range: bool,

    perf_context: EK::PerfContext,

    yield_duration: Duration,

    store_id: u64,
    /// region_id -> (peer_id, is_splitting)
    /// Used for handling race between splitting and creating new peer.
    /// An uninitialized peer can be replaced to the one from splitting iff they are exactly the same peer.
    pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,

    /// We must delete the ingested file before calling `callback` so that any ingest-request reaching this
    /// peer could see this update if leader had changed. We must also delete them after the applied-index
    /// has been persisted to kvdb because this entry may replay because of panic or power-off, which
    /// happened before `WriteBatch::write` and after `SSTImporter::delete`. We shall make sure that
    /// this entry will never apply again at first, then we can delete the ssts files.
    delete_ssts: Vec<SstMeta>,
}

impl<EK, W> ApplyContext<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    pub fn new(
        tag: String,
        host: CoprocessorHost<EK>,
        importer: Arc<SSTImporter>,
        region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
        engine: EK,
        router: ApplyRouter<EK>,
        notifier: Box<dyn Notifier<EK>>,
        cfg: &Config,
        store_id: u64,
        pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
    ) -> ApplyContext<EK, W> {
        // If `enable_multi_batch_write` was set true, we create `RocksWriteBatchVec`.
        // Otherwise create `RocksWriteBatch`.
        let kv_wb = W::with_capacity(&engine, DEFAULT_APPLY_WB_SIZE);

        ApplyContext {
            tag,
            timer: None,
            host,
            importer,
            region_scheduler,
            engine: engine.clone(),
            router,
            notifier,
            kv_wb,
            cbs: MustConsumeVec::new("callback of apply context"),
            apply_res: vec![],
            kv_wb_last_bytes: 0,
            kv_wb_last_keys: 0,
            last_applied_index: 0,
            committed_count: 0,
            sync_log_hint: false,
            exec_ctx: None,
            use_delete_range: cfg.use_delete_range,
            perf_context: engine.get_perf_context(cfg.perf_level, PerfContextKind::RaftstoreApply),
            yield_duration: cfg.apply_yield_duration.0,
            delete_ssts: vec![],
            store_id,
            pending_create_peers,
        }
    }

    /// Prepares for applying entries for `delegate`.
    ///
    /// A general apply progress for a delegate is:
    /// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
    /// After all delegates are handled, `write_to_db` method should be called.
    pub fn prepare_for(&mut self, delegate: &mut ApplyDelegate<EK>) {
        self.cbs.push(ApplyCallback::new(delegate.region.clone()));
        self.last_applied_index = delegate.apply_state.get_applied_index();

        if let Some(observe_cmd) = &delegate.observe_cmd {
            let region_id = delegate.region_id();
            if observe_cmd.enabled.load(Ordering::Acquire) {
                self.host.prepare_for_apply(observe_cmd.id, region_id);
            } else {
                info!("region is no longer observerd";
                    "region_id" => region_id);
                delegate.observe_cmd.take();
            }
        }
    }

    /// Commits all changes have done for delegate. `persistent` indicates whether
    /// write the changes into rocksdb.
    ///
    /// This call is valid only when it's between a `prepare_for` and `finish_for`.
    pub fn commit(&mut self, delegate: &mut ApplyDelegate<EK>) {
        if self.last_applied_index < delegate.apply_state.get_applied_index() {
            delegate.write_apply_state(self.kv_wb_mut());
        }
        // last_applied_index doesn't need to be updated, set persistent to true will
        // force it call `prepare_for` automatically.
        self.commit_opt(delegate, true);
    }

    fn commit_opt(&mut self, delegate: &mut ApplyDelegate<EK>, persistent: bool) {
        delegate.update_metrics(self);
        if persistent {
            self.write_to_db();
            self.prepare_for(delegate);
        }
        self.kv_wb_last_bytes = self.kv_wb().data_size() as u64;
        self.kv_wb_last_keys = self.kv_wb().count() as u64;
    }

    /// Writes all the changes into RocksDB.
    /// If it returns true, all pending writes are persisted in engines.
    pub fn write_to_db(&mut self) -> bool {
        let need_sync = self.sync_log_hint;
        if !self.kv_wb_mut().is_empty() {
            let mut write_opts = engine_traits::WriteOptions::new();
            write_opts.set_sync(need_sync);
            self.kv_wb().write_opt(&write_opts).unwrap_or_else(|e| {
                panic!("failed to write to engine: {:?}", e);
            });
            self.perf_context.report_metrics();
            self.sync_log_hint = false;
            let data_size = self.kv_wb().data_size();
            if data_size > APPLY_WB_SHRINK_SIZE {
                // Control the memory usage for the WriteBatch. Whether it's `RocksWriteBatch` or
                // `RocksWriteBatchVec` depends on the `enable_multi_batch_write` configuration.
                self.kv_wb = W::with_capacity(&self.engine, DEFAULT_APPLY_WB_SIZE);
            } else {
                // Clear data, reuse the WriteBatch, this can reduce memory allocations and deallocations.
                self.kv_wb_mut().clear();
            }
            self.kv_wb_last_bytes = 0;
            self.kv_wb_last_keys = 0;
        }
        if !self.delete_ssts.is_empty() {
            let tag = self.tag.clone();
            for sst in self.delete_ssts.drain(..) {
                self.importer.delete(&sst).unwrap_or_else(|e| {
                    panic!("{} cleanup ingested file {:?}: {:?}", tag, sst, e);
                });
            }
        }
        // Call it before invoking callback for preventing Commit is executed before Prewrite is observed.
        self.host.on_flush_apply(self.engine.clone());

        for cbs in self.cbs.drain(..) {
            cbs.invoke_all(&self.host);
        }
        need_sync
    }

    /// Finishes `Apply`s for the delegate.
    pub fn finish_for(
        &mut self,
        delegate: &mut ApplyDelegate<EK>,
        results: VecDeque<ExecResult<EK::Snapshot>>,
    ) {
        if !delegate.pending_remove {
            delegate.write_apply_state(self.kv_wb_mut());
        }
        self.commit_opt(delegate, false);
        self.apply_res.push(ApplyRes {
            region_id: delegate.region_id(),
            apply_state: delegate.apply_state.clone(),
            exec_res: results,
            metrics: delegate.metrics.clone(),
            applied_index_term: delegate.applied_index_term,
        });
    }

    pub fn delta_bytes(&self) -> u64 {
        self.kv_wb().data_size() as u64 - self.kv_wb_last_bytes
    }

    pub fn delta_keys(&self) -> u64 {
        self.kv_wb().count() as u64 - self.kv_wb_last_keys
    }

    #[inline]
    pub fn kv_wb(&self) -> &W {
        &self.kv_wb
    }

    #[inline]
    pub fn kv_wb_mut(&mut self) -> &mut W {
        &mut self.kv_wb
    }

    /// Flush all pending writes to engines.
    /// If it returns true, all pending writes are persisted in engines.
    pub fn flush(&mut self) -> bool {
        // TODO: this check is too hacky, need to be more verbose and less buggy.
        let t = match self.timer.take() {
            Some(t) => t,
            None => return false,
        };

        // Write to engine
        // raftstore.sync-log = true means we need prevent data loss when power failure.
        // take raft log gc for example, we write kv WAL first, then write raft WAL,
        // if power failure happen, raft WAL may synced to disk, but kv WAL may not.
        // so we use sync-log flag here.
        let is_synced = self.write_to_db();

        if !self.apply_res.is_empty() {
            let apply_res = std::mem::replace(&mut self.apply_res, vec![]);
            self.notifier.notify(apply_res);
        }

        let elapsed = t.elapsed();
        STORE_APPLY_LOG_HISTOGRAM.observe(duration_to_sec(elapsed) as f64);

        slow_log!(
            elapsed,
            "{} handle ready {} committed entries",
            self.tag,
            self.committed_count
        );
        self.committed_count = 0;
        is_synced
    }
}

/// Calls the callback of `cmd` when the Region is removed.
fn notify_region_removed(region_id: u64, peer_id: u64, mut cmd: PendingCmd<impl Snapshot>) {
    debug!(
        "region is removed, notify commands";
        "region_id" => region_id,
        "peer_id" => peer_id,
        "index" => cmd.index,
        "term" => cmd.term
    );
    notify_req_region_removed(region_id, cmd.cb.take().unwrap());
}

pub fn notify_req_region_removed(region_id: u64, cb: Callback<impl Snapshot>) {
    let region_not_found = Error::RegionNotFound(region_id);
    let resp = cmd_resp::new_error(region_not_found);
    cb.invoke_with_response(resp);
}

/// Calls the callback of `cmd` when it can not be processed further.
fn notify_stale_command(
    region_id: u64,
    peer_id: u64,
    term: u64,
    mut cmd: PendingCmd<impl Snapshot>,
) {
    info!(
        "command is stale, skip";
        "region_id" => region_id,
        "peer_id" => peer_id,
        "index" => cmd.index,
        "term" => cmd.term
    );
    notify_stale_req(term, cmd.cb.take().unwrap());
}

pub fn notify_stale_req(term: u64, cb: Callback<impl Snapshot>) {
    let resp = cmd_resp::err_resp(Error::StaleCommand, term);
    cb.invoke_with_response(resp);
}

/// Checks if a write is needed to be issued before handling the command.
fn should_write_to_engine(cmd: &RaftCmdRequest) -> bool {
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

/// Checks if a write is needed to be issued after handling the command.
fn should_sync_log(cmd: &RaftCmdRequest) -> bool {
    if cmd.has_admin_request() {
        if cmd.get_admin_request().get_cmd_type() == AdminCmdType::CompactLog {
            // We do not need to sync WAL before compact log, because this request will send a msg to
            // raft_gc_log thread to delete the entries before this index instead of deleting them in
            // apply thread directly.
            return false;
        }
        return true;
    }

    for req in cmd.get_requests() {
        // After ingest sst, sst files are deleted quickly. As a result,
        // ingest sst command can not be handled again and must be synced.
        // See more in Cleanup worker.
        if req.has_ingest_sst() {
            return true;
        }
    }

    false
}

/// A struct that stores the state related to Merge.
///
/// When executing a `CommitMerge`, the source peer may have not applied
/// to the required index, so the target peer has to abort current execution
/// and wait for it asynchronously.
///
/// When rolling the stack, all states required to recover are stored in
/// this struct.
/// TODO: check whether generator/coroutine is a good choice in this case.
struct WaitSourceMergeState {
    /// A flag that indicates whether the source peer has applied to the required
    /// index. If the source peer is ready, this flag should be set to the region id
    /// of source peer.
    logs_up_to_date: Arc<AtomicU64>,
}

struct YieldState<EK>
where
    EK: KvEngine,
{
    /// All of the entries that need to continue to be applied after
    /// the source peer has applied its logs.
    pending_entries: Vec<Entry>,
    /// All of messages that need to continue to be handled after
    /// the source peer has applied its logs and pending entries
    /// are all handled.
    pending_msgs: Vec<Msg<EK>>,
}

impl<EK> Debug for YieldState<EK>
where
    EK: KvEngine,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("YieldState")
            .field("pending_entries", &self.pending_entries.len())
            .field("pending_msgs", &self.pending_msgs.len())
            .finish()
    }
}

impl Debug for WaitSourceMergeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitSourceMergeState")
            .field("logs_up_to_date", &self.logs_up_to_date)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct NewSplitPeer {
    pub peer_id: u64,
    // `None` => success,
    // `Some(s)` => fail due to `s`.
    pub result: Option<String>,
}

/// The apply delegate of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// `Delegate` is just a structure to congregate all apply related fields of a
/// Region. The apply worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding apply delegate to
/// handle the apply task to make the code logic more clear.
#[derive(Debug)]
pub struct ApplyDelegate<EK>
where
    EK: KvEngine,
{
    /// The ID of the peer.
    id: u64,
    /// The term of the Region.
    term: u64,
    /// The Region information of the peer.
    region: Region,
    /// Peer_tag, "[region region_id] peer_id".
    tag: String,

    /// If the delegate should be stopped from polling.
    /// A delegate can be stopped in conf change, merge or requested by destroy message.
    stopped: bool,
    /// The start time of the current round to execute commands.
    handle_start: Option<Instant>,
    /// Set to true when removing itself because of `ConfChangeType::RemoveNode`, and then
    /// any following committed logs in same Ready should be applied failed.
    pending_remove: bool,

    /// The commands waiting to be committed and applied
    pending_cmds: PendingCmdQueue<EK::Snapshot>,
    /// The counter of pending request snapshots. See more in `Peer`.
    pending_request_snapshot_count: Arc<AtomicUsize>,

    /// Indicates the peer is in merging, if that compact log won't be performed.
    is_merging: bool,
    /// Records the epoch version after the last merge.
    last_merge_version: u64,
    yield_state: Option<YieldState<EK>>,
    /// A temporary state that keeps track of the progress of the source peer state when
    /// CommitMerge is unable to be executed.
    wait_merge_state: Option<WaitSourceMergeState>,
    // ID of last region that reports ready.
    ready_source_region_id: u64,

    /// TiKV writes apply_state to KV RocksDB, in one write batch together with kv data.
    ///
    /// If we write it to Raft RocksDB, apply_state and kv data (Put, Delete) are in
    /// separate WAL file. When power failure, for current raft log, apply_index may synced
    /// to file, but KV data may not synced to file, so we will lose data.
    apply_state: RaftApplyState,
    /// The term of the raft log at applied index.
    applied_index_term: u64,
    /// The latest synced apply index.
    last_sync_apply_index: u64,

    /// Info about cmd observer.
    observe_cmd: Option<ObserveCmd>,

    /// The local metrics, and it will be flushed periodically.
    metrics: ApplyMetrics,
}

impl<EK> ApplyDelegate<EK>
where
    EK: KvEngine,
{
    fn from_registration(reg: Registration) -> ApplyDelegate<EK> {
        ApplyDelegate {
            id: reg.id,
            tag: format!("[region {}] {}", reg.region.get_id(), reg.id),
            region: reg.region,
            pending_remove: false,
            last_sync_apply_index: reg.apply_state.get_applied_index(),
            apply_state: reg.apply_state,
            applied_index_term: reg.applied_index_term,
            term: reg.term,
            stopped: false,
            handle_start: None,
            ready_source_region_id: 0,
            yield_state: None,
            wait_merge_state: None,
            is_merging: reg.is_merging,
            pending_cmds: PendingCmdQueue::new(),
            metrics: Default::default(),
            last_merge_version: 0,
            pending_request_snapshot_count: reg.pending_request_snapshot_count,
            observe_cmd: None,
        }
    }

    pub fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Handles all the committed_entries, namely, applies the committed entries.
    fn handle_raft_committed_entries<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        mut committed_entries_drainer: Drain<Entry>,
    ) {
        if committed_entries_drainer.len() == 0 {
            return;
        }
        apply_ctx.prepare_for(self);
        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        apply_ctx.committed_count += committed_entries_drainer.len();
        let mut results = VecDeque::new();
        while let Some(entry) = committed_entries_drainer.next() {
            if self.pending_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }

            let expect_index = self.apply_state.get_applied_index() + 1;
            if expect_index != entry.get_index() {
                panic!(
                    "{} expect index {}, but got {}",
                    self.tag,
                    expect_index,
                    entry.get_index()
                );
            }

            // NOTE: before v5.0, `EntryType::EntryConfChangeV2` entry is handled by `unimplemented!()`,
            // which can break compatibility (i.e. old version tikv running on data written by new version tikv),
            // but PD will reject old version tikv join the cluster, so this should not happen.
            let res = match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_raft_entry_normal(apply_ctx, &entry),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_raft_entry_conf_change(apply_ctx, &entry)
                }
            };

            match res {
                ApplyResult::None => {}
                ApplyResult::Res(res) => results.push_back(res),
                ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => {
                    // Both cancel and merge will yield current processing.
                    apply_ctx.committed_count -= committed_entries_drainer.len() + 1;
                    let mut pending_entries =
                        Vec::with_capacity(committed_entries_drainer.len() + 1);
                    // Note that current entry is skipped when yield.
                    pending_entries.push(entry);
                    pending_entries.extend(committed_entries_drainer);
                    apply_ctx.finish_for(self, results);
                    self.yield_state = Some(YieldState {
                        pending_entries,
                        pending_msgs: Vec::default(),
                    });
                    if let ApplyResult::WaitMergeSource(logs_up_to_date) = res {
                        self.wait_merge_state = Some(WaitSourceMergeState { logs_up_to_date });
                    }
                    return;
                }
            }
        }

        apply_ctx.finish_for(self, results);

        if self.pending_remove {
            self.destroy(apply_ctx);
        }
    }

    fn update_metrics<W: WriteBatch<EK>>(&mut self, apply_ctx: &ApplyContext<EK, W>) {
        self.metrics.written_bytes += apply_ctx.delta_bytes();
        self.metrics.written_keys += apply_ctx.delta_keys();
    }

    fn write_apply_state<W: WriteBatch<EK>>(&self, wb: &mut W) {
        wb.put_msg_cf(
            CF_RAFT,
            &keys::apply_state_key(self.region.get_id()),
            &self.apply_state,
        )
        .unwrap_or_else(|e| {
            panic!(
                "{} failed to save apply state to write batch, error: {:?}",
                self.tag, e
            );
        });
    }

    fn handle_raft_entry_normal<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        entry: &Entry,
    ) -> ApplyResult<EK::Snapshot> {
        fail_point!("yield_apply_1000", self.region_id() == 1000, |_| {
            ApplyResult::Yield
        });

        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if !data.is_empty() {
            let cmd = util::parse_data_at(data, index, &self.tag);

            if should_write_to_engine(&cmd) || apply_ctx.kv_wb().should_write_to_engine() {
                apply_ctx.commit(self);
                if let Some(start) = self.handle_start.as_ref() {
                    if start.elapsed() >= apply_ctx.yield_duration {
                        return ApplyResult::Yield;
                    }
                }
            }

            return self.process_raft_cmd(apply_ctx, index, term, cmd);
        }
        // TOOD(cdc): should we observe empty cmd, aka leader change?

        self.apply_state.set_applied_index(index);
        self.applied_index_term = term;
        assert!(term > 0);

        // 1. When a peer become leader, it will send an empty entry.
        // 2. When a leader tries to read index during transferring leader,
        //    it will also propose an empty entry. But that entry will not contain
        //    any associated callback. So no need to clear callback.
        while let Some(mut cmd) = self.pending_cmds.pop_normal(std::u64::MAX, term - 1) {
            apply_ctx.cbs.last_mut().unwrap().push(
                cmd.cb.take(),
                Cmd::new(
                    cmd.index,
                    RaftCmdRequest::default(),
                    cmd_resp::err_resp(Error::StaleCommand, term),
                ),
            );
        }
        ApplyResult::None
    }

    fn handle_raft_entry_conf_change<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        entry: &Entry,
    ) -> ApplyResult<EK::Snapshot> {
        // Although conf change can't yield in normal case, it is convenient to
        // simulate yield before applying a conf change log.
        fail_point!("yield_apply_conf_change_3", self.id() == 3, |_| {
            ApplyResult::Yield
        });
        let (index, term) = (entry.get_index(), entry.get_term());
        let conf_change: ConfChangeV2 = match entry.get_entry_type() {
            EntryType::EntryConfChange => {
                let conf_change: ConfChange =
                    util::parse_data_at(entry.get_data(), index, &self.tag);
                conf_change.into_v2()
            }
            EntryType::EntryConfChangeV2 => util::parse_data_at(entry.get_data(), index, &self.tag),
            _ => unreachable!(),
        };
        let cmd = util::parse_data_at(conf_change.get_context(), index, &self.tag);
        match self.process_raft_cmd(apply_ctx, index, term, cmd) {
            ApplyResult::None => {
                // If failed, tell Raft that the `ConfChange` was aborted.
                ApplyResult::Res(ExecResult::ChangePeer(Default::default()))
            }
            ApplyResult::Res(mut res) => {
                if let ExecResult::ChangePeer(ref mut cp) = res {
                    cp.conf_change = conf_change;
                } else {
                    panic!(
                        "{} unexpected result {:?} for conf change {:?} at {}",
                        self.tag, res, conf_change, index
                    );
                }
                ApplyResult::Res(res)
            }
            ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => unreachable!(),
        }
    }

    fn find_pending(
        &mut self,
        index: u64,
        term: u64,
        is_conf_change: bool,
    ) -> Option<Callback<EK::Snapshot>> {
        let (region_id, peer_id) = (self.region_id(), self.id());
        if is_conf_change {
            if let Some(mut cmd) = self.pending_cmds.take_conf_change() {
                if cmd.index == index && cmd.term == term {
                    return Some(cmd.cb.take().unwrap());
                } else {
                    notify_stale_command(region_id, peer_id, self.term, cmd);
                }
            }
            return None;
        }
        while let Some(mut head) = self.pending_cmds.pop_normal(index, term) {
            if head.term == term {
                if head.index == index {
                    return Some(head.cb.take().unwrap());
                } else {
                    panic!(
                        "{} unexpected callback at term {}, found index {}, expected {}",
                        self.tag, term, head.index, index
                    );
                }
            } else {
                // Because of the lack of original RaftCmdRequest, we skip calling
                // coprocessor here.
                notify_stale_command(region_id, peer_id, self.term, head);
            }
        }
        None
    }

    fn process_raft_cmd<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        index: u64,
        term: u64,
        cmd: RaftCmdRequest,
    ) -> ApplyResult<EK::Snapshot> {
        if index == 0 {
            panic!(
                "{} processing raft command needs a none zero index",
                self.tag
            );
        }

        // Set sync log hint if the cmd requires so.
        apply_ctx.sync_log_hint |= should_sync_log(&cmd);

        apply_ctx.host.pre_apply(&self.region, &cmd);
        let (mut resp, exec_result) = self.apply_raft_cmd(apply_ctx, index, term, &cmd);
        if let ApplyResult::WaitMergeSource(_) = exec_result {
            return exec_result;
        }

        debug!(
            "applied command";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "index" => index
        );

        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        cmd_resp::bind_term(&mut resp, self.term);
        let cmd_cb = self.find_pending(index, term, is_conf_change_cmd(&cmd));
        let cmd = Cmd::new(index, cmd, resp);
        if let Some(observe_cmd) = self.observe_cmd.as_ref() {
            apply_ctx
                .host
                .on_apply_cmd(observe_cmd.id, self.region_id(), cmd.clone());
        }

        apply_ctx.cbs.last_mut().unwrap().push(cmd_cb, cmd);

        exec_result
    }

    /// Applies raft command.
    ///
    /// An apply operation can fail in the following situations:
    ///   1. it encounters an error that will occur on all stores, it can continue
    /// applying next entry safely, like epoch not match for example;
    ///   2. it encounters an error that may not occur on all stores, in this case
    /// we should try to apply the entry again or panic. Considering that this
    /// usually due to disk operation fail, which is rare, so just panic is ok.
    fn apply_raft_cmd<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        index: u64,
        term: u64,
        req: &RaftCmdRequest,
    ) -> (RaftCmdResponse, ApplyResult<EK::Snapshot>) {
        // if pending remove, apply should be aborted already.
        assert!(!self.pending_remove);

        ctx.exec_ctx = Some(self.new_ctx(index, term));
        ctx.kv_wb_mut().set_save_point();
        let mut origin_epoch = None;
        let (resp, exec_result) = match self.exec_raft_cmd(ctx, &req) {
            Ok(a) => {
                ctx.kv_wb_mut().pop_save_point().unwrap();
                if req.has_admin_request() {
                    origin_epoch = Some(self.region.get_region_epoch().clone());
                }
                a
            }
            Err(e) => {
                // clear dirty values.
                ctx.kv_wb_mut().rollback_to_save_point().unwrap();
                match e {
                    Error::EpochNotMatch(..) => debug!(
                        "epoch not match";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "err" => ?e
                    ),
                    _ => error!(?e;
                        "execute raft command";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                    ),
                }
                (cmd_resp::new_error(e), ApplyResult::None)
            }
        };
        if let ApplyResult::WaitMergeSource(_) = exec_result {
            return (resp, exec_result);
        }

        let mut exec_ctx = ctx.exec_ctx.take().unwrap();
        exec_ctx.apply_state.set_applied_index(index);

        self.apply_state = exec_ctx.apply_state;
        self.applied_index_term = term;

        if let ApplyResult::Res(ref exec_result) = exec_result {
            match *exec_result {
                ExecResult::ChangePeer(ref cp) => {
                    self.region = cp.region.clone();
                }
                ExecResult::ComputeHash { .. }
                | ExecResult::VerifyHash { .. }
                | ExecResult::CompactLog { .. }
                | ExecResult::DeleteRange { .. }
                | ExecResult::IngestSst { .. } => {}
                ExecResult::SplitRegion { ref derived, .. } => {
                    self.region = derived.clone();
                    self.metrics.size_diff_hint = 0;
                    self.metrics.delete_keys_hint = 0;
                }
                ExecResult::PrepareMerge { ref region, .. } => {
                    self.region = region.clone();
                    self.is_merging = true;
                }
                ExecResult::CommitMerge { ref region, .. } => {
                    self.region = region.clone();
                    self.last_merge_version = region.get_region_epoch().get_version();
                }
                ExecResult::RollbackMerge { ref region, .. } => {
                    self.region = region.clone();
                    self.is_merging = false;
                }
            }
        }
        if let Some(epoch) = origin_epoch {
            let cmd_type = req.get_admin_request().get_cmd_type();
            let epoch_state = *ADMIN_CMD_EPOCH_MAP.get(&cmd_type).unwrap();
            // The chenge-epoch behavior **MUST BE** equal to the settings in `ADMIN_CMD_EPOCH_MAP`
            if (epoch_state.change_ver
                && epoch.get_version() == self.region.get_region_epoch().get_version())
                || (epoch_state.change_conf_ver
                    && epoch.get_conf_ver() == self.region.get_region_epoch().get_conf_ver())
            {
                panic!("{} apply admin cmd {:?} but epoch change is not expected, epoch state {:?}, before {:?}, after {:?}",
                        self.tag, req, epoch_state, epoch, self.region.get_region_epoch());
            }
        }

        (resp, exec_result)
    }

    fn destroy<W: WriteBatch<EK>>(&mut self, apply_ctx: &mut ApplyContext<EK, W>) {
        self.stopped = true;
        apply_ctx.router.close(self.region_id());
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
    }

    fn clear_all_commands_as_stale(&mut self) {
        let (region_id, peer_id) = (self.region_id(), self.id());
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_command(region_id, peer_id, self.term, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_command(region_id, peer_id, self.term, cmd);
        }
    }

    fn new_ctx(&self, index: u64, term: u64) -> ExecContext {
        ExecContext::new(self.apply_state.clone(), index, term)
    }
}

impl<EK> ApplyDelegate<EK>
where
    EK: KvEngine,
{
    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult<EK::Snapshot>)> {
        // Include region for epoch not match after merge may cause key not in range.
        let include_region =
            req.get_header().get_region_epoch().get_version() >= self.last_merge_version;
        check_region_epoch(req, &self.region, include_region)?;
        if req.has_admin_request() {
            self.exec_admin_cmd(ctx, req)
        } else {
            self.exec_write_cmd(ctx, req)
        }
    }

    fn exec_admin_cmd<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult<EK::Snapshot>)> {
        let request = req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        if cmd_type != AdminCmdType::CompactLog && cmd_type != AdminCmdType::CommitMerge {
            info!(
                "execute admin command";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "term" => ctx.exec_ctx.as_ref().unwrap().term,
                "index" => ctx.exec_ctx.as_ref().unwrap().index,
                "command" => ?request,
            );
        }

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::ChangePeerV2 => self.exec_change_peer_v2(ctx, request),
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

        let mut resp = RaftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn exec_write_cmd<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!(
            "on_apply_write_cmd",
            cfg!(release) || self.id() == 3,
            |_| {
                unimplemented!();
            }
        );

        let requests = req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        let mut ranges = vec![];
        let mut ssts = vec![];
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Put => self.handle_put(ctx.kv_wb_mut(), req),
                CmdType::Delete => self.handle_delete(ctx.kv_wb_mut(), req),
                CmdType::DeleteRange => {
                    assert!(ctx.kv_wb.is_empty());
                    self.handle_delete_range(&ctx.engine, req, &mut ranges, ctx.use_delete_range)
                }
                CmdType::IngestSst => {
                    assert!(ctx.kv_wb.is_empty());
                    self.handle_ingest_sst(&ctx.importer, &ctx.engine, req, &mut ssts)
                }
                // Readonly commands are handled in raftstore directly.
                // Don't panic here in case there are old entries need to be applied.
                // It's also safe to skip them here, because a restart must have happened,
                // hence there is no callback to be called.
                CmdType::Snap | CmdType::Get => {
                    warn!(
                        "skip readonly command";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "command" => ?req,
                    );
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
                    Err(box_err!("invalid cmd type, message maybe corrupted"))
                }
            }?;

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }
        resp.set_responses(responses.into());

        assert!(ranges.is_empty() || ssts.is_empty());
        let exec_res = if !ranges.is_empty() {
            ApplyResult::Res(ExecResult::DeleteRange { ranges })
        } else if !ssts.is_empty() {
            ctx.delete_ssts.append(&mut ssts.clone());
            ApplyResult::Res(ExecResult::IngestSst { ssts })
        } else {
            ApplyResult::None
        };

        Ok((resp, exec_res))
    }
}

// Write commands related.
impl<EK> ApplyDelegate<EK>
where
    EK: KvEngine,
{
    fn handle_put<W: WriteBatch<EK>>(&mut self, wb: &mut W, req: &Request) -> Result<Response> {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, &self.region)?;

        let resp = Response::default();
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
            wb.put_cf(cf, &key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}) to cf {}: {:?}",
                    self.tag,
                    log_wrappers::Value::key(&key),
                    log_wrappers::Value::value(&value),
                    cf,
                    e
                )
            });
        } else {
            wb.put(&key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}): {:?}",
                    self.tag,
                    log_wrappers::Value::key(&key),
                    log_wrappers::Value::value(&value),
                    e
                );
            });
        }
        Ok(resp)
    }

    fn handle_delete<W: WriteBatch<EK>>(&mut self, wb: &mut W, req: &Request) -> Result<Response> {
        let key = req.get_delete().get_key();
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, &self.region)?;

        let key = keys::data_key(key);
        // since size_diff_hint is not accurate, so we just skip calculate the value size.
        self.metrics.size_diff_hint -= key.len() as i64;
        let resp = Response::default();
        if !req.get_delete().get_cf().is_empty() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            wb.delete_cf(cf, &key).unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete {}: {}",
                    self.tag,
                    log_wrappers::Value::key(&key),
                    e
                )
            });

            if cf == CF_LOCK {
                // delete is a kind of write for RocksDB.
                self.metrics.lock_cf_written_bytes += key.len() as u64;
            } else {
                self.metrics.delete_keys_hint += 1;
            }
        } else {
            wb.delete(&key).unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete {}: {}",
                    self.tag,
                    log_wrappers::Value::key(&key),
                    e
                )
            });
            self.metrics.delete_keys_hint += 1;
        }

        Ok(resp)
    }

    fn handle_delete_range(
        &mut self,
        engine: &EK,
        req: &Request,
        ranges: &mut Vec<Range>,
        use_delete_range: bool,
    ) -> Result<Response> {
        let s_key = req.get_delete_range().get_start_key();
        let e_key = req.get_delete_range().get_end_key();
        let notify_only = req.get_delete_range().get_notify_only();
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

        let resp = Response::default();
        let mut cf = req.get_delete_range().get_cf();
        if cf.is_empty() {
            cf = CF_DEFAULT;
        }
        if ALL_CFS.iter().find(|x| **x == cf).is_none() {
            return Err(box_err!("invalid delete range command, cf: {:?}", cf));
        }

        let start_key = keys::data_key(s_key);
        // Use delete_files_in_range to drop as many sst files as possible, this
        // is a way to reclaim disk space quickly after drop a table/index.
        if !notify_only {
            let range = vec![EngineRange::new(&start_key, &end_key)];
            let fail_f = |e: engine_traits::Error, strategy: DeleteStrategy| {
                panic!(
                    "{} failed to delete {:?} in ranges [{}, {}): {:?}",
                    self.tag,
                    strategy,
                    &log_wrappers::Value::key(&start_key),
                    &log_wrappers::Value::key(&end_key),
                    e
                )
            };
            engine
                .delete_ranges_cf(cf, DeleteStrategy::DeleteFiles, &range)
                .unwrap_or_else(|e| fail_f(e, DeleteStrategy::DeleteFiles));

            let strategy = if use_delete_range {
                DeleteStrategy::DeleteByRange
            } else {
                DeleteStrategy::DeleteByKey
            };
            // Delete all remaining keys.
            engine
                .delete_ranges_cf(cf, strategy.clone(), &range)
                .unwrap_or_else(move |e| fail_f(e, strategy));
            engine
                .delete_ranges_cf(cf, DeleteStrategy::DeleteBlobs, &range)
                .unwrap_or_else(move |e| fail_f(e, DeleteStrategy::DeleteBlobs));
        }

        // TODO: Should this be executed when `notify_only` is set?
        ranges.push(Range::new(cf.to_owned(), start_key, end_key));

        Ok(resp)
    }

    fn handle_ingest_sst(
        &mut self,
        importer: &Arc<SSTImporter>,
        engine: &EK,
        req: &Request,
        ssts: &mut Vec<SstMeta>,
    ) -> Result<Response> {
        let sst = req.get_ingest_sst().get_sst();

        if let Err(e) = check_sst_for_ingestion(sst, &self.region) {
            error!(?e;
                 "ingest fail";
                 "region_id" => self.region_id(),
                 "peer_id" => self.id(),
                 "sst" => ?sst,
                 "region" => ?&self.region,
            );
            // This file is not valid, we can delete it here.
            let _ = importer.delete(sst);
            return Err(e);
        }

        importer.ingest(sst, engine).unwrap_or_else(|e| {
            // If this failed, it means that the file is corrupted or something
            // is wrong with the engine, but we can do nothing about that.
            panic!("{} ingest {:?}: {:?}", self.tag, sst, e);
        });

        ssts.push(sst.clone());
        Ok(Response::default())
    }
}

mod confchange_cmd_metric {
    use super::*;

    fn write_metric(cct: ConfChangeType, kind: &str) {
        let metric = match cct {
            ConfChangeType::AddNode => "add_peer",
            ConfChangeType::RemoveNode => "remove_peer",
            ConfChangeType::AddLearnerNode => "add_learner",
        };
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&[metric, kind])
            .inc();
    }

    pub fn inc_all(cct: ConfChangeType) {
        write_metric(cct, "all")
    }

    pub fn inc_success(cct: ConfChangeType) {
        write_metric(cct, "success")
    }
}

// Admin commands related.
impl<EK> ApplyDelegate<EK>
where
    EK: KvEngine,
{
    fn exec_change_peer<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        assert!(request.has_change_peer());
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let store_id = peer.get_store_id();
        let change_type = request.get_change_type();
        let mut region = self.region.clone();

        fail_point!(
            "apply_on_conf_change_1_3_1",
            (self.id == 1 || self.id == 3) && self.region_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_3_1",
            self.id == 3 && self.region_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_all_1",
            self.region_id() == 1,
            |_| panic!("should not use return")
        );
        info!(
            "exec ConfChange";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "type" => util::conf_change_type_str(change_type),
            "epoch" => ?region.get_region_epoch(),
        );

        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let conf_ver = region.get_region_epoch().get_conf_ver() + 1;
        region.mut_region_epoch().set_conf_ver(conf_ver);

        match change_type {
            ConfChangeType::AddNode => {
                let add_ndoe_fp = || {
                    fail_point!(
                        "apply_on_add_node_1_2",
                        self.id == 2 && self.region_id() == 1,
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
                    if !is_learner(p) || p.get_id() != peer.get_id() {
                        error!(
                            "can't add duplicated peer";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "peer" => ?peer,
                            "region" => ?&self.region
                        );
                        return Err(box_err!(
                            "can't add duplicated peer {:?} to region {:?}",
                            peer,
                            self.region
                        ));
                    } else {
                        p.set_role(PeerRole::Voter);
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
                    "add peer successfully";
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "region" => ?&self.region
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
                            "ignore remove unmatched peer";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "expect_peer" => ?peer,
                            "get_peeer" => ?p
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
                        self.stopped = true;
                        self.pending_remove = true;
                    }
                } else {
                    error!(
                        "remove missing peer";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "peer" => ?peer,
                        "region" => ?&self.region
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
                    "remove peer successfully";
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "region" => ?&self.region
                );
            }
            ConfChangeType::AddLearnerNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "all"])
                    .inc();

                if util::find_peer(&region, store_id).is_some() {
                    error!(
                        "can't add duplicated learner";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "peer" => ?peer,
                        "region" => ?&self.region
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
                    "add learner successfully";
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                    "peer" => ?peer,
                    "region" => ?&self.region
                );
            }
        }

        let state = if self.pending_remove {
            PeerState::Tombstone
        } else {
            PeerState::Normal
        };
        if let Err(e) = write_peer_state(ctx.kv_wb_mut(), &region, state, None) {
            panic!("{} failed to update region state: {:?}", self.tag, e);
        }

        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_region(region.clone());

        Ok((
            resp,
            ApplyResult::Res(ExecResult::ChangePeer(ChangePeer {
                index: ctx.exec_ctx.as_ref().unwrap().index,
                conf_change: Default::default(),
                changes: vec![request.clone()],
                region,
            })),
        ))
    }

    fn exec_change_peer_v2<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        assert!(request.has_change_peer_v2());
        let changes = request.get_change_peer_v2().get_change_peers().to_vec();

        info!(
            "exec ConfChangeV2";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "kind" => ?ConfChangeKind::confchange_kind(changes.len()),
            "epoch" => ?self.region.get_region_epoch(),
        );

        let region = match ConfChangeKind::confchange_kind(changes.len()) {
            ConfChangeKind::LeaveJoint => self.apply_leave_joint()?,
            kind => self.apply_conf_change(kind, changes.as_slice())?,
        };

        let state = if self.pending_remove {
            PeerState::Tombstone
        } else {
            PeerState::Normal
        };

        if let Err(e) = write_peer_state(ctx.kv_wb_mut(), &region, state, None) {
            panic!("{} failed to update region state: {:?}", self.tag, e);
        }

        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_region(region.clone());
        Ok((
            resp,
            ApplyResult::Res(ExecResult::ChangePeer(ChangePeer {
                index: ctx.exec_ctx.as_ref().unwrap().index,
                conf_change: Default::default(),
                changes,
                region,
            })),
        ))
    }

    fn apply_conf_change(
        &mut self,
        kind: ConfChangeKind,
        changes: &[ChangePeerRequest],
    ) -> Result<Region> {
        let mut region = self.region.clone();
        for cp in changes.iter() {
            let (change_type, peer) = (cp.get_change_type(), cp.get_peer());
            let store_id = peer.get_store_id();

            confchange_cmd_metric::inc_all(change_type);

            if let Some(exist_peer) = util::find_peer(&region, store_id) {
                let r = exist_peer.get_role();
                if r == PeerRole::IncomingVoter || r == PeerRole::DemotingVoter {
                    panic!(
                        "{} can't apply confchange because configuration is still in joint state, confchange: {:?}, region: {:?}",
                        self.tag, cp, self.region
                    );
                }
            }
            match (util::find_peer_mut(&mut region, store_id), change_type) {
                (None, ConfChangeType::AddNode) => {
                    let mut peer = peer.clone();
                    match kind {
                        ConfChangeKind::Simple => peer.set_role(PeerRole::Voter),
                        ConfChangeKind::EnterJoint => peer.set_role(PeerRole::IncomingVoter),
                        _ => unreachable!(),
                    }
                    region.mut_peers().push(peer);
                }
                (None, ConfChangeType::AddLearnerNode) => {
                    let mut peer = peer.clone();
                    peer.set_role(PeerRole::Learner);
                    region.mut_peers().push(peer);
                }
                (None, ConfChangeType::RemoveNode) => {
                    error!(
                        "remove missing peer";
                        "region_id" => self.region_id(),
                        "peer_id" => self.id(),
                        "peer" => ?peer,
                        "region" => ?&self.region,
                    );
                    return Err(box_err!(
                        "remove missing peer {:?} from region {:?}",
                        peer,
                        self.region
                    ));
                }
                // Add node
                (Some(exist_peer), ConfChangeType::AddNode)
                | (Some(exist_peer), ConfChangeType::AddLearnerNode) => {
                    let (role, exist_id, incoming_id) =
                        (exist_peer.get_role(), exist_peer.get_id(), peer.get_id());

                    if exist_id != incoming_id // Add peer with different id to the same store
                            // The peer is already the requested role
                            || (role, change_type) == (PeerRole::Voter, ConfChangeType::AddNode)
                            || (role, change_type) == (PeerRole::Learner, ConfChangeType::AddLearnerNode)
                    {
                        error!(
                            "can't add duplicated peer";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "peer" => ?peer,
                            "exist peer" => ?exist_peer,
                            "confchnage type" => ?change_type,
                            "region" => ?&self.region
                        );
                        return Err(box_err!(
                                "can't add duplicated peer {:?} to region {:?}, duplicated with exist peer {:?}",
                                peer,
                                self.region,
                                exist_peer
                            ));
                    }
                    match (role, change_type) {
                        (PeerRole::Voter, ConfChangeType::AddLearnerNode) => match kind {
                            ConfChangeKind::Simple => exist_peer.set_role(PeerRole::Learner),
                            ConfChangeKind::EnterJoint => {
                                exist_peer.set_role(PeerRole::DemotingVoter)
                            }
                            _ => unreachable!(),
                        },
                        (PeerRole::Learner, ConfChangeType::AddNode) => match kind {
                            ConfChangeKind::Simple => exist_peer.set_role(PeerRole::Voter),
                            ConfChangeKind::EnterJoint => {
                                exist_peer.set_role(PeerRole::IncomingVoter)
                            }
                            _ => unreachable!(),
                        },
                        _ => unreachable!(),
                    }
                }
                // Remove node
                (Some(exist_peer), ConfChangeType::RemoveNode) => {
                    if kind == ConfChangeKind::EnterJoint
                        && exist_peer.get_role() == PeerRole::Voter
                    {
                        error!(
                            "can't remove voter directly";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "peer" => ?peer,
                            "region" => ?&self.region
                        );
                        return Err(box_err!(
                            "can not remove voter {:?} directly from region {:?}",
                            peer,
                            self.region
                        ));
                    }
                    match util::remove_peer(&mut region, store_id) {
                        Some(p) => {
                            if &p != peer {
                                error!(
                                    "ignore remove unmatched peer";
                                    "region_id" => self.region_id(),
                                    "peer_id" => self.id(),
                                    "expect_peer" => ?peer,
                                    "get_peeer" => ?p
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
                                self.stopped = true;
                                self.pending_remove = true;
                            }
                        }
                        None => unreachable!(),
                    }
                }
            }
            confchange_cmd_metric::inc_success(change_type);
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + changes.len() as u64;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        info!(
            "conf change successfully";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "changes" => ?changes,
            "original region" => ?&self.region,
            "current region" => ?&region,
        );
        Ok(region)
    }

    fn apply_leave_joint(&self) -> Result<Region> {
        let mut region = self.region.clone();
        let mut change_num = 0;
        for peer in region.mut_peers().iter_mut() {
            match peer.get_role() {
                PeerRole::IncomingVoter => peer.set_role(PeerRole::Voter),
                PeerRole::DemotingVoter => peer.set_role(PeerRole::Learner),
                _ => continue,
            }
            change_num += 1;
        }
        if change_num == 0 {
            panic!(
                "{} can't leave a non-joint config, region: {:?}",
                self.tag, self.region
            );
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + change_num;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        info!(
            "leave joint state successfully";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "region" => ?&region,
        );
        Ok(region)
    }

    fn exec_split<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        info!(
            "split is deprecated, redirect to use batch split";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
        );
        let split = req.get_split().to_owned();
        let mut admin_req = AdminRequest::default();
        admin_req
            .mut_splits()
            .set_right_derive(split.get_right_derive());
        admin_req.mut_splits().mut_requests().push(split);
        // This method is executed only when there are unapplied entries after being restarted.
        // So there will be no callback, it's OK to return a response that does not matched
        // with its request.
        self.exec_batch_split(ctx, &admin_req)
    }

    fn exec_batch_split<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_split");
        fail_point!(
            "apply_before_split_1_3",
            self.id == 3 && self.region_id() == 1,
            |_| { unreachable!() }
        );

        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

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
                    "invalid new peer id count, need {:?}, but got {:?}",
                    derived.get_peers(),
                    req.get_new_peer_ids()
                ));
            }
            keys.push_back(split_key.to_vec());
        }

        util::check_key_in_region(keys.back().unwrap(), &self.region)?;

        info!(
            "split region";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "region" => ?derived,
            "keys" => %KeysInfoFormatter(keys.iter()),
        );
        let new_version = derived.get_region_epoch().get_version() + new_region_cnt as u64;
        derived.mut_region_epoch().set_version(new_version);
        // Note that the split requests only contain ids for new regions, so we need
        // to handle new regions and old region separately.
        if right_derive {
            // So the range of new regions is [old_start_key, split_key1, ..., last_split_key].
            keys.push_front(derived.get_start_key().to_vec());
        } else {
            // So the range of new regions is [split_key1, ..., last_split_key, old_end_key].
            keys.push_back(derived.get_end_key().to_vec());
            derived.set_end_key(keys.front().unwrap().to_vec());
            regions.push(derived.clone());
        }

        let mut new_split_regions: HashMap<u64, NewSplitPeer> = HashMap::default();
        for req in split_reqs.get_requests() {
            let mut new_region = Region::default();
            new_region.set_id(req.get_new_region_id());
            new_region.set_region_epoch(derived.get_region_epoch().to_owned());
            new_region.set_start_key(keys.pop_front().unwrap());
            new_region.set_end_key(keys.front().unwrap().to_vec());
            new_region.set_peers(derived.get_peers().to_vec().into());
            for (peer, peer_id) in new_region
                .mut_peers()
                .iter_mut()
                .zip(req.get_new_peer_ids())
            {
                peer.set_id(*peer_id);
            }
            new_split_regions.insert(
                new_region.get_id(),
                NewSplitPeer {
                    peer_id: util::find_peer(&new_region, ctx.store_id).unwrap().get_id(),
                    result: None,
                },
            );
            regions.push(new_region);
        }

        if right_derive {
            derived.set_start_key(keys.pop_front().unwrap());
            regions.push(derived.clone());
        }

        let mut replace_regions = HashSet::default();
        {
            let mut pending_create_peers = ctx.pending_create_peers.lock().unwrap();
            for (region_id, new_split_peer) in new_split_regions.iter_mut() {
                match pending_create_peers.entry(*region_id) {
                    HashMapEntry::Occupied(mut v) => {
                        if *v.get() != (new_split_peer.peer_id, false) {
                            new_split_peer.result =
                                Some(format!("status {:?} is not expected", v.get()));
                        } else {
                            replace_regions.insert(*region_id);
                            v.insert((new_split_peer.peer_id, true));
                        }
                    }
                    HashMapEntry::Vacant(v) => {
                        v.insert((new_split_peer.peer_id, true));
                    }
                }
            }
        }

        // region_id -> peer_id
        let mut already_exist_regions = Vec::new();
        for (region_id, new_split_peer) in new_split_regions.iter_mut() {
            let region_state_key = keys::region_state_key(*region_id);
            match ctx
                .engine
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
            {
                Ok(None) => (),
                Ok(Some(state)) => {
                    if replace_regions.get(region_id).is_some() {
                        // This peer must be the first one on local store. So if this peer is created on the other side,
                        // it means no `RegionLocalState` in kv engine.
                        panic!("{} failed to replace region {} peer {} because state {:?} alread exist in kv engine",
                            self.tag, region_id, new_split_peer.peer_id, state);
                    }
                    already_exist_regions.push((*region_id, new_split_peer.peer_id));
                    new_split_peer.result = Some(format!("state {:?} exist in kv engine", state));
                }
                e => panic!(
                    "{} failed to get regions state of {}: {:?}",
                    self.tag, region_id, e
                ),
            }
        }

        if !already_exist_regions.is_empty() {
            let mut pending_create_peers = ctx.pending_create_peers.lock().unwrap();
            for (region_id, peer_id) in &already_exist_regions {
                assert_eq!(
                    pending_create_peers.remove(region_id),
                    Some((*peer_id, true))
                );
            }
        }

        let kv_wb_mut = ctx.kv_wb_mut();
        for new_region in &regions {
            if new_region.get_id() == derived.get_id() {
                continue;
            }
            let new_split_peer = new_split_regions.get(&new_region.get_id()).unwrap();
            if let Some(ref r) = new_split_peer.result {
                warn!(
                    "new region from splitting already exists";
                    "new_region_id" => new_region.get_id(),
                    "new_peer_id" => new_split_peer.peer_id,
                    "reason" => r,
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                );
                continue;
            }
            write_peer_state(kv_wb_mut, new_region, PeerState::Normal, None)
                .and_then(|_| write_initial_apply_state(kv_wb_mut, new_region.get_id()))
                .unwrap_or_else(|e| {
                    panic!(
                        "{} fails to save split region {:?}: {:?}",
                        self.tag, new_region, e
                    )
                });
        }
        write_peer_state(kv_wb_mut, &derived, PeerState::Normal, None).unwrap_or_else(|e| {
            panic!("{} fails to update region {:?}: {:?}", self.tag, derived, e)
        });
        let mut resp = AdminResponse::default();
        resp.mut_splits().set_regions(regions.clone().into());
        PEER_ADMIN_CMD_COUNTER.batch_split.success.inc();

        fail_point!(
            "apply_after_split_1_3",
            self.id == 3 && self.region_id() == 1,
            |_| { unreachable!() }
        );

        Ok((
            resp,
            ApplyResult::Res(ExecResult::SplitRegion {
                regions,
                derived,
                new_split_regions,
            }),
        ))
    }

    fn exec_prepare_merge<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_prepare_merge");

        PEER_ADMIN_CMD_COUNTER.prepare_merge.all.inc();

        let prepare_merge = req.get_prepare_merge();
        let index = prepare_merge.get_min_index();
        let exec_ctx = ctx.exec_ctx.as_ref().unwrap();
        let first_index = peer_storage::first_index(&exec_ctx.apply_state);
        if index < first_index {
            // We filter `CompactLog` command before.
            panic!(
                "{} first index {} > min_index {}, skip pre merge",
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
        let mut merging_state = MergeState::default();
        merging_state.set_min_index(index);
        merging_state.set_target(prepare_merge.get_target().to_owned());
        merging_state.set_commit(exec_ctx.index);
        write_peer_state(
            ctx.kv_wb_mut(),
            &region,
            PeerState::Merging,
            Some(merging_state.clone()),
        )
        .unwrap_or_else(|e| {
            panic!(
                "{} failed to save merging state {:?} for region {:?}: {:?}",
                self.tag, merging_state, region, e
            )
        });
        fail_point!("apply_after_prepare_merge");
        PEER_ADMIN_CMD_COUNTER.prepare_merge.success.inc();

        Ok((
            AdminResponse::default(),
            ApplyResult::Res(ExecResult::PrepareMerge {
                region,
                state: merging_state,
            }),
        ))
    }

    // The target peer should send missing log entries to the source peer.
    //
    // So, the merge process order would be:
    // 1.   `exec_commit_merge` in target apply fsm and send `CatchUpLogs` to source peer fsm
    // 2.   `on_catch_up_logs_for_merge` in source peer fsm
    // 3.   if the source peer has already executed the corresponding `on_ready_prepare_merge`, set pending_remove and jump to step 6
    // 4.   ... (raft append and apply logs)
    // 5.   `on_ready_prepare_merge` in source peer fsm and set pending_remove (means source region has finished applying all logs)
    // 6.   `logs_up_to_date_for_merge` in source apply fsm (destroy its apply fsm and send Noop to trigger the target apply fsm)
    // 7.   resume `exec_commit_merge` in target apply fsm
    // 8.   `on_ready_commit_merge` in target peer fsm and send `MergeResult` to source peer fsm
    // 9.   `on_merge_result` in source peer fsm (destroy itself)
    fn exec_commit_merge<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        {
            fail_point!("apply_before_commit_merge");
            let apply_before_commit_merge = || {
                fail_point!(
                    "apply_before_commit_merge_except_1_4",
                    self.region_id() == 1 && self.id != 4,
                    |_| {}
                );
            };
            apply_before_commit_merge();
        }

        PEER_ADMIN_CMD_COUNTER.commit_merge.all.inc();

        let merge = req.get_commit_merge();
        let source_region = merge.get_source();
        let source_region_id = source_region.get_id();

        // No matter whether the source peer has applied to the required index,
        // it's a race to write apply state in both source delegate and target
        // delegate. So asking the source delegate to stop first.
        if self.ready_source_region_id != source_region_id {
            if self.ready_source_region_id != 0 {
                panic!(
                    "{} unexpected ready source region {}, expecting {}",
                    self.tag, self.ready_source_region_id, source_region_id
                );
            }
            info!(
                "asking delegate to stop";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "source_region_id" => source_region_id
            );
            fail_point!("before_handle_catch_up_logs_for_merge");
            // Sends message to the source peer fsm and pause `exec_commit_merge` process
            let logs_up_to_date = Arc::new(AtomicU64::new(0));
            let msg = SignificantMsg::CatchUpLogs(CatchUpLogs {
                target_region_id: self.region_id(),
                merge: merge.to_owned(),
                logs_up_to_date: logs_up_to_date.clone(),
            });
            ctx.notifier
                .notify_one(source_region_id, PeerMsg::SignificantMsg(msg));
            return Ok((
                AdminResponse::default(),
                ApplyResult::WaitMergeSource(logs_up_to_date),
            ));
        }

        info!(
            "execute CommitMerge";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "commit" => merge.get_commit(),
            "entries" => merge.get_entries().len(),
            "term" => ctx.exec_ctx.as_ref().unwrap().term,
            "index" => ctx.exec_ctx.as_ref().unwrap().index,
            "source_region" => ?source_region
        );

        self.ready_source_region_id = 0;

        let region_state_key = keys::region_state_key(source_region_id);
        let state: RegionLocalState = match ctx.engine.get_msg_cf(CF_RAFT, &region_state_key) {
            Ok(Some(s)) => s,
            e => panic!(
                "{} failed to get regions state of {:?}: {:?}",
                self.tag, source_region, e
            ),
        };
        if state.get_state() != PeerState::Merging {
            panic!(
                "{} unexpected state of merging region {:?}",
                self.tag, state
            );
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
        let kv_wb_mut = ctx.kv_wb_mut();
        write_peer_state(kv_wb_mut, &region, PeerState::Normal, None)
            .and_then(|_| {
                // TODO: maybe all information needs to be filled?
                let mut merging_state = MergeState::default();
                merging_state.set_target(self.region.clone());
                write_peer_state(
                    kv_wb_mut,
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

        PEER_ADMIN_CMD_COUNTER.commit_merge.success.inc();

        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::CommitMerge {
                region,
                source: source_region.to_owned(),
            }),
        ))
    }

    fn exec_rollback_merge<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_rollback_merge");

        PEER_ADMIN_CMD_COUNTER.rollback_merge.all.inc();
        let region_state_key = keys::region_state_key(self.region_id());
        let state: RegionLocalState = match ctx.engine.get_msg_cf(CF_RAFT, &region_state_key) {
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
        write_peer_state(ctx.kv_wb_mut(), &region, PeerState::Normal, None).unwrap_or_else(|e| {
            panic!(
                "{} failed to rollback merge {:?}: {:?}",
                self.tag, rollback, e
            )
        });

        PEER_ADMIN_CMD_COUNTER.rollback_merge.success.inc();
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::RollbackMerge {
                region,
                commit: rollback.get_commit(),
            }),
        ))
    }

    fn exec_compact_log<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.compact.all.inc();

        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::default();
        let apply_state = &mut ctx.exec_ctx.as_mut().unwrap().apply_state;
        let first_index = peer_storage::first_index(apply_state);
        if compact_index <= first_index {
            debug!(
                "compact index <= first index, no need to compact";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "compact_index" => compact_index,
                "first_index" => first_index,
            );
            return Ok((resp, ApplyResult::None));
        }
        if self.is_merging {
            info!(
                "in merging mode, skip compact";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "compact_index" => compact_index
            );
            return Ok((resp, ApplyResult::None));
        }

        let compact_term = req.get_compact_log().get_compact_term();
        // TODO: add unit tests to cover all the message integrity checks.
        if compact_term == 0 {
            info!(
                "compact term missing, skip";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "command" => ?req.get_compact_log()
            );
            // old format compact log command, safe to ignore.
            return Err(box_err!(
                "command format is outdated, please upgrade leader"
            ));
        }

        // compact failure is safe to be omitted, no need to assert.
        compact_raft_log(&self.tag, apply_state, compact_index, compact_term)?;

        PEER_ADMIN_CMD_COUNTER.compact.success.inc();

        Ok((
            resp,
            ApplyResult::Res(ExecResult::CompactLog {
                state: apply_state.get_truncated_state().clone(),
                first_index,
            }),
        ))
    }

    fn exec_compute_hash<W: WriteBatch<EK>>(
        &self,
        ctx: &ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::ComputeHash {
                region: self.region.clone(),
                index: ctx.exec_ctx.as_ref().unwrap().index,
                context: req.get_compute_hash().get_context().to_vec(),
                // This snapshot may be held for a long time, which may cause too many
                // open files in rocksdb.
                // TODO: figure out another way to do consistency check without snapshot
                // or short life snapshot.
                snap: ctx.engine.snapshot(),
            }),
        ))
    }

    fn exec_verify_hash<W: WriteBatch<EK>>(
        &self,
        _: &ApplyContext<EK, W>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let verify_req = req.get_verify_hash();
        let index = verify_req.get_index();
        let context = verify_req.get_context().to_vec();
        let hash = verify_req.get_hash().to_vec();
        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::VerifyHash {
                index,
                context,
                hash,
            }),
        ))
    }
}

pub fn is_conf_change_cmd(msg: &RaftCmdRequest) -> bool {
    if !msg.has_admin_request() {
        return false;
    }
    let req = msg.get_admin_request();
    req.has_change_peer() || req.has_change_peer_v2()
}

fn check_sst_for_ingestion(sst: &SstMeta, region: &Region) -> Result<()> {
    let uuid = sst.get_uuid();
    if let Err(e) = UuidBuilder::from_slice(uuid) {
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
        return Err(Error::EpochNotMatch(error, vec![region.clone()]));
    }

    let range = sst.get_range();
    util::check_key_in_region(range.get_start(), region)?;
    util::check_key_in_region(range.get_end(), region)?;

    Ok(())
}

/// Updates the `state` with given `compact_index` and `compact_term`.
///
/// Remember the Raft log is not deleted here.
pub fn compact_raft_log(
    tag: &str,
    state: &mut RaftApplyState,
    compact_index: u64,
    compact_term: u64,
) -> Result<()> {
    debug!("{} compact log entries to prior to {}", tag, compact_index);

    if compact_index <= state.get_truncated_state().get_index() {
        return Err(box_err!("try to truncate compacted entries"));
    } else if compact_index > state.get_applied_index() {
        return Err(box_err!(
            "compact index {} > applied index {}",
            compact_index,
            state.get_applied_index()
        ));
    }

    // we don't actually delete the logs now, we add an async task to do it.

    state.mut_truncated_state().set_index(compact_index);
    state.mut_truncated_state().set_term(compact_term);

    Ok(())
}

pub struct Apply<S>
where
    S: Snapshot,
{
    pub peer_id: u64,
    pub region_id: u64,
    pub term: u64,
    pub entries: Vec<Entry>,
    pub cbs: Vec<Proposal<S>>,
    entries_mem_size: i64,
    entries_count: i64,
}

impl<S: Snapshot> Apply<S> {
    pub(crate) fn new(
        peer_id: u64,
        region_id: u64,
        term: u64,
        entries: Vec<Entry>,
        cbs: Vec<Proposal<S>>,
    ) -> Apply<S> {
        let entries_mem_size =
            (ENTRY_MEM_SIZE * entries.capacity()) as i64 + get_entries_mem_size(&entries);
        APPLY_PENDING_BYTES_GAUGE.add(entries_mem_size);
        let entries_count = entries.len() as i64;
        APPLY_PENDING_ENTRIES_GAUGE.add(entries_count);
        Apply {
            peer_id,
            region_id,
            term,
            entries,
            cbs,
            entries_mem_size,
            entries_count,
        }
    }
}

impl<S: Snapshot> Drop for Apply<S> {
    fn drop(&mut self) {
        APPLY_PENDING_BYTES_GAUGE.sub(self.entries_mem_size);
        APPLY_PENDING_ENTRIES_GAUGE.sub(self.entries_count);
    }
}

fn get_entries_mem_size(entries: &[Entry]) -> i64 {
    if entries.is_empty() {
        return 0;
    }
    let data_size: i64 = entries
        .iter()
        .map(|e| (e.data.capacity() + e.context.capacity()) as i64)
        .sum();
    data_size
}

#[derive(Default, Clone)]
pub struct Registration {
    pub id: u64,
    pub term: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub region: Region,
    pub pending_request_snapshot_count: Arc<AtomicUsize>,
    pub is_merging: bool,
}

impl Registration {
    pub fn new<EK: KvEngine, ER: RaftEngine>(peer: &Peer<EK, ER>) -> Registration {
        Registration {
            id: peer.peer_id(),
            term: peer.term(),
            apply_state: peer.get_store().apply_state().clone(),
            applied_index_term: peer.get_store().applied_index_term(),
            region: peer.region().clone(),
            pending_request_snapshot_count: peer.pending_request_snapshot_count.clone(),
            is_merging: peer.pending_merge_state.is_some(),
        }
    }
}

pub struct Proposal<S>
where
    S: Snapshot,
{
    pub is_conf_change: bool,
    pub index: u64,
    pub term: u64,
    pub cb: Callback<S>,
    /// `renew_lease_time` contains the last time when a peer starts to renew lease.
    pub renew_lease_time: Option<Timespec>,
    pub must_pass_epoch_check: bool,
}

pub struct Destroy {
    region_id: u64,
    merge_from_snapshot: bool,
}

/// A message that asks the delegate to apply to the given logs and then reply to
/// target mailbox.
#[derive(Default, Debug)]
pub struct CatchUpLogs {
    /// The target region to be notified when given logs are applied.
    pub target_region_id: u64,
    /// Merge request that contains logs to be applied.
    pub merge: CommitMergeRequest,
    /// A flag indicate that all source region's logs are applied.
    ///
    /// This is still necessary although we have a mailbox field already.
    /// Mailbox is used to notify target region, and trigger a round of polling.
    /// But due to the FIFO natural of channel, we need a flag to check if it's
    /// ready when polling.
    pub logs_up_to_date: Arc<AtomicU64>,
}

pub struct GenSnapTask {
    pub(crate) region_id: u64,
    snap_notifier: SyncSender<RaftSnapshot>,
    // indicates whether the snapshot is triggered due to load balance
    for_balance: bool,
}

impl GenSnapTask {
    pub fn new(region_id: u64, snap_notifier: SyncSender<RaftSnapshot>) -> GenSnapTask {
        GenSnapTask {
            region_id,
            snap_notifier,
            for_balance: false,
        }
    }

    pub fn set_for_balance(&mut self) {
        self.for_balance = true;
    }

    pub fn generate_and_schedule_snapshot<EK>(
        self,
        kv_snap: EK::Snapshot,
        last_applied_index_term: u64,
        last_applied_state: RaftApplyState,
        region_sched: &Scheduler<RegionTask<EK::Snapshot>>,
    ) -> Result<()>
    where
        EK: KvEngine,
    {
        let snapshot = RegionTask::Gen {
            region_id: self.region_id,
            notifier: self.snap_notifier,
            for_balance: self.for_balance,
            last_applied_index_term,
            last_applied_state,
            // This snapshot may be held for a long time, which may cause too many
            // open files in rocksdb.
            kv_snap,
        };
        box_try!(region_sched.schedule(snapshot));
        Ok(())
    }
}

impl Debug for GenSnapTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenSnapTask")
            .field("region_id", &self.region_id)
            .finish()
    }
}

static OBSERVE_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier for checking stale observed commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObserveID(usize);

impl ObserveID {
    pub fn new() -> ObserveID {
        ObserveID(OBSERVE_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

struct ObserveCmd {
    id: ObserveID,
    enabled: Arc<AtomicBool>,
}

impl Debug for ObserveCmd {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserveCmd").field("id", &self.id).finish()
    }
}

#[derive(Debug)]
pub enum ChangeCmd {
    RegisterObserver {
        observe_id: ObserveID,
        region_id: u64,
        enabled: Arc<AtomicBool>,
    },
    Snapshot {
        observe_id: ObserveID,
        region_id: u64,
    },
}

pub enum Msg<EK>
where
    EK: KvEngine,
{
    Apply {
        start: Instant,
        apply: Apply<EK::Snapshot>,
    },
    Registration(Registration),
    LogsUpToDate(CatchUpLogs),
    Noop,
    Destroy(Destroy),
    Snapshot(GenSnapTask),
    Change {
        cmd: ChangeCmd,
        region_epoch: RegionEpoch,
        cb: Callback<EK::Snapshot>,
    },
    #[cfg(any(test, feature = "testexport"))]
    #[allow(clippy::type_complexity)]
    Validate(u64, Box<dyn FnOnce(*const u8) + Send>),
}

impl<EK> Msg<EK>
where
    EK: KvEngine,
{
    pub fn apply(apply: Apply<EK::Snapshot>) -> Msg<EK> {
        Msg::Apply {
            start: Instant::now(),
            apply,
        }
    }

    pub fn register<ER: RaftEngine>(peer: &Peer<EK, ER>) -> Msg<EK> {
        Msg::Registration(Registration::new(peer))
    }

    pub fn destroy(region_id: u64, merge_from_snapshot: bool) -> Msg<EK> {
        Msg::Destroy(Destroy {
            region_id,
            merge_from_snapshot,
        })
    }
}

impl<EK> Debug for Msg<EK>
where
    EK: KvEngine,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Msg::Apply { apply, .. } => write!(f, "[region {}] async apply", apply.region_id),
            Msg::Registration(ref r) => {
                write!(f, "[region {}] Reg {:?}", r.region.get_id(), r.apply_state)
            }
            Msg::LogsUpToDate(_) => write!(f, "logs are updated"),
            Msg::Noop => write!(f, "noop"),
            Msg::Destroy(ref d) => write!(f, "[region {}] destroy", d.region_id),
            Msg::Snapshot(GenSnapTask { region_id, .. }) => {
                write!(f, "[region {}] requests a snapshot", region_id)
            }
            Msg::Change {
                cmd: ChangeCmd::RegisterObserver { region_id, .. },
                ..
            } => write!(f, "[region {}] registers cmd observer", region_id),
            Msg::Change {
                cmd: ChangeCmd::Snapshot { region_id, .. },
                ..
            } => write!(f, "[region {}] cmd snapshot", region_id),
            #[cfg(any(test, feature = "testexport"))]
            Msg::Validate(region_id, _) => write!(f, "[region {}] validate", region_id),
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
pub struct ApplyRes<S>
where
    S: Snapshot,
{
    pub region_id: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub exec_res: VecDeque<ExecResult<S>>,
    pub metrics: ApplyMetrics,
}

#[derive(Debug)]
pub enum TaskRes<S>
where
    S: Snapshot,
{
    Apply(ApplyRes<S>),
    Destroy {
        // ID of region that has been destroyed.
        region_id: u64,
        // ID of peer that has been destroyed.
        peer_id: u64,
        // Whether destroy request is from its target region's snapshot
        merge_from_snapshot: bool,
    },
}

pub struct ApplyFsm<EK>
where
    EK: KvEngine,
{
    delegate: ApplyDelegate<EK>,
    receiver: Receiver<Msg<EK>>,
    mailbox: Option<BasicMailbox<ApplyFsm<EK>>>,
}

impl<EK> ApplyFsm<EK>
where
    EK: KvEngine,
{
    fn from_peer<ER: RaftEngine>(
        peer: &Peer<EK, ER>,
    ) -> (LooseBoundedSender<Msg<EK>>, Box<ApplyFsm<EK>>) {
        let reg = Registration::new(peer);
        ApplyFsm::from_registration(reg)
    }

    fn from_registration(reg: Registration) -> (LooseBoundedSender<Msg<EK>>, Box<ApplyFsm<EK>>) {
        let (tx, rx) = loose_bounded(usize::MAX);
        let delegate = ApplyDelegate::from_registration(reg);
        (
            tx,
            Box::new(ApplyFsm {
                delegate,
                receiver: rx,
                mailbox: None,
            }),
        )
    }

    /// Handles peer registration. When a peer is created, it will register an apply delegate.
    fn handle_registration(&mut self, reg: Registration) {
        info!(
            "re-register to apply delegates";
            "region_id" => self.delegate.region_id(),
            "peer_id" => self.delegate.id(),
            "term" => reg.term
        );
        assert_eq!(self.delegate.id, reg.id);
        self.delegate.term = reg.term;
        self.delegate.clear_all_commands_as_stale();
        self.delegate = ApplyDelegate::from_registration(reg);
    }

    /// Handles apply tasks, and uses the apply delegate to handle the committed entries.
    fn handle_apply<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        mut apply: Apply<EK::Snapshot>,
    ) {
        if apply_ctx.timer.is_none() {
            apply_ctx.timer = Some(Instant::now_coarse());
        }

        fail_point!("on_handle_apply_1003", self.delegate.id() == 1003, |_| {});
        fail_point!("on_handle_apply_2", self.delegate.id() == 2, |_| {});
        fail_point!("on_handle_apply", |_| {});

        if apply.entries.is_empty() || self.delegate.pending_remove || self.delegate.stopped {
            return;
        }

        self.delegate.metrics = ApplyMetrics::default();
        self.delegate.term = apply.term;
        if let Some(entry) = apply.entries.last() {
            let prev_state = (
                self.delegate.apply_state.get_commit_index(),
                self.delegate.apply_state.get_commit_term(),
            );
            let cur_state = (entry.get_index(), entry.get_term());
            if prev_state.0 > cur_state.0 || prev_state.1 > cur_state.1 {
                panic!(
                    "{} commit state jump backward {:?} -> {:?}",
                    self.delegate.tag, prev_state, cur_state
                );
            }
            self.delegate.apply_state.set_commit_index(cur_state.0);
            self.delegate.apply_state.set_commit_term(cur_state.1);
        }

        self.append_proposal(apply.cbs.drain(..));
        self.delegate
            .handle_raft_committed_entries(apply_ctx, apply.entries.drain(..));
        fail_point!("post_handle_apply_1003", self.delegate.id() == 1003, |_| {});
    }

    /// Handles proposals, and appends the commands to the apply delegate.
    fn append_proposal(&mut self, props_drainer: Drain<Proposal<EK::Snapshot>>) {
        let (region_id, peer_id) = (self.delegate.region_id(), self.delegate.id());
        let propose_num = props_drainer.len();
        if self.delegate.stopped {
            for p in props_drainer {
                let cmd = PendingCmd::<EK::Snapshot>::new(p.index, p.term, p.cb);
                notify_stale_command(region_id, peer_id, self.delegate.term, cmd);
            }
            return;
        }
        for p in props_drainer {
            let cmd = PendingCmd::new(p.index, p.term, p.cb);
            if p.is_conf_change {
                if let Some(cmd) = self.delegate.pending_cmds.take_conf_change() {
                    // if it loses leadership before conf change is replicated, there may be
                    // a stale pending conf change before next conf change is applied. If it
                    // becomes leader again with the stale pending conf change, will enter
                    // this block, so we notify leadership may have been changed.
                    notify_stale_command(region_id, peer_id, self.delegate.term, cmd);
                }
                self.delegate.pending_cmds.set_conf_change(cmd);
            } else {
                self.delegate.pending_cmds.append_normal(cmd);
            }
        }
        // TODO: observe it in batch.
        APPLY_PROPOSAL.observe(propose_num as f64);
    }

    fn destroy<W: WriteBatch<EK>>(&mut self, ctx: &mut ApplyContext<EK, W>) {
        let region_id = self.delegate.region_id();
        if ctx.apply_res.iter().any(|res| res.region_id == region_id) {
            // Flush before destroying to avoid reordering messages.
            ctx.flush();
        }
        fail_point!(
            "before_peer_destroy_1003",
            self.delegate.id() == 1003,
            |_| {}
        );
        info!(
            "remove delegate from apply delegates";
            "region_id" => self.delegate.region_id(),
            "peer_id" => self.delegate.id(),
        );
        self.delegate.destroy(ctx);
    }

    /// Handles peer destroy. When a peer is destroyed, the corresponding apply delegate should be removed too.
    fn handle_destroy<W: WriteBatch<EK>>(&mut self, ctx: &mut ApplyContext<EK, W>, d: Destroy) {
        assert_eq!(d.region_id, self.delegate.region_id());
        if d.merge_from_snapshot {
            assert_eq!(self.delegate.stopped, false);
        }
        if !self.delegate.stopped {
            self.destroy(ctx);
            ctx.notifier.notify_one(
                self.delegate.region_id(),
                PeerMsg::ApplyRes {
                    res: TaskRes::Destroy {
                        region_id: self.delegate.region_id(),
                        peer_id: self.delegate.id,
                        merge_from_snapshot: d.merge_from_snapshot,
                    },
                },
            );
        }
    }

    fn resume_pending<W: WriteBatch<EK>>(&mut self, ctx: &mut ApplyContext<EK, W>) {
        if let Some(ref state) = self.delegate.wait_merge_state {
            let source_region_id = state.logs_up_to_date.load(Ordering::SeqCst);
            if source_region_id == 0 {
                return;
            }
            self.delegate.ready_source_region_id = source_region_id;
        }
        self.delegate.wait_merge_state = None;

        let mut state = self.delegate.yield_state.take().unwrap();

        if ctx.timer.is_none() {
            ctx.timer = Some(Instant::now_coarse());
        }
        if !state.pending_entries.is_empty() {
            self.delegate
                .handle_raft_committed_entries(ctx, state.pending_entries.drain(..));
            if let Some(ref mut s) = self.delegate.yield_state {
                // So the delegate is expected to yield the CPU.
                // It can either be executing another `CommitMerge` in pending_msgs
                // or has been written too much data.
                s.pending_msgs = state.pending_msgs;
                return;
            }
        }

        if !state.pending_msgs.is_empty() {
            self.handle_tasks(ctx, &mut state.pending_msgs);
        }
    }

    fn logs_up_to_date_for_merge<W: WriteBatch<EK>>(
        &mut self,
        ctx: &mut ApplyContext<EK, W>,
        catch_up_logs: CatchUpLogs,
    ) {
        fail_point!("after_handle_catch_up_logs_for_merge");
        fail_point!(
            "after_handle_catch_up_logs_for_merge_1003",
            self.delegate.id() == 1003,
            |_| {}
        );

        let region_id = self.delegate.region_id();
        info!(
            "source logs are all applied now";
            "region_id" => region_id,
            "peer_id" => self.delegate.id(),
        );
        // The source peer fsm will be destroyed when the target peer executes `on_ready_commit_merge`
        // and sends `merge result` to the source peer fsm.
        self.destroy(ctx);
        catch_up_logs
            .logs_up_to_date
            .store(region_id, Ordering::SeqCst);
        // To trigger the target apply fsm
        if let Some(mailbox) = ctx.router.mailbox(catch_up_logs.target_region_id) {
            let _ = mailbox.force_send(Msg::Noop);
        } else {
            error!(
                "failed to get mailbox, are we shutting down?";
                "region_id" => region_id,
                "peer_id" => self.delegate.id(),
            );
        }
    }

    #[allow(unused_mut)]
    fn handle_snapshot<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        snap_task: GenSnapTask,
    ) {
        if self.delegate.pending_remove || self.delegate.stopped {
            return;
        }
        let applied_index = self.delegate.apply_state.get_applied_index();
        let mut need_sync = apply_ctx
            .apply_res
            .iter()
            .any(|res| res.region_id == self.delegate.region_id())
            && self.delegate.last_sync_apply_index != applied_index;
        #[cfg(feature = "failpoint")]
        (|| fail_point!("apply_on_handle_snapshot_sync", |_| { need_sync = true }))();
        if need_sync {
            if apply_ctx.timer.is_none() {
                apply_ctx.timer = Some(Instant::now_coarse());
            }
            self.delegate.write_apply_state(apply_ctx.kv_wb_mut());
            fail_point!(
                "apply_on_handle_snapshot_1_1",
                self.delegate.id == 1 && self.delegate.region_id() == 1,
                |_| unimplemented!()
            );

            apply_ctx.flush();
            // For now, it's more like last_flush_apply_index.
            // TODO: Update it only when `flush()` returns true.
            self.delegate.last_sync_apply_index = applied_index;
        }

        if let Err(e) = snap_task.generate_and_schedule_snapshot::<EK>(
            apply_ctx.engine.snapshot(),
            self.delegate.applied_index_term,
            self.delegate.apply_state.clone(),
            &apply_ctx.region_scheduler,
        ) {
            error!(
                "schedule snapshot failed";
                "error" => ?e,
                "region_id" => self.delegate.region_id(),
                "peer_id" => self.delegate.id()
            );
        }
        self.delegate
            .pending_request_snapshot_count
            .fetch_sub(1, Ordering::SeqCst);
        fail_point!(
            "apply_on_handle_snapshot_finish_1_1",
            self.delegate.id == 1 && self.delegate.region_id() == 1,
            |_| unimplemented!()
        );
    }

    fn handle_change<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        cmd: ChangeCmd,
        region_epoch: RegionEpoch,
        cb: Callback<EK::Snapshot>,
    ) {
        let (observe_id, region_id, enabled) = match cmd {
            ChangeCmd::RegisterObserver {
                observe_id,
                region_id,
                enabled,
            } => (observe_id, region_id, Some(enabled)),
            ChangeCmd::Snapshot {
                observe_id,
                region_id,
            } => (observe_id, region_id, None),
        };
        if let Some(observe_cmd) = self.delegate.observe_cmd.as_mut() {
            if observe_cmd.id > observe_id {
                notify_stale_req(self.delegate.term, cb);
                return;
            }
        }

        assert_eq!(self.delegate.region_id(), region_id);
        let resp = match compare_region_epoch(
            &region_epoch,
            &self.delegate.region,
            false, /* check_conf_ver */
            true,  /* check_ver */
            true,  /* include_region */
        ) {
            Ok(()) => {
                // Commit the writebatch for ensuring the following snapshot can get all previous writes.
                if apply_ctx.kv_wb().count() > 0 {
                    apply_ctx.commit(&mut self.delegate);
                }
                ReadResponse {
                    response: Default::default(),
                    snapshot: Some(RegionSnapshot::from_snapshot(
                        Arc::new(apply_ctx.engine.snapshot()),
                        Arc::new(self.delegate.region.clone()),
                    )),
                    txn_extra_op: TxnExtraOp::Noop,
                }
            }
            Err(e) => {
                // Return error if epoch not match
                cb.invoke_read(ReadResponse {
                    response: cmd_resp::new_error(e),
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                });
                return;
            }
        };
        if let Some(enabled) = enabled {
            assert!(
                !self
                    .delegate
                    .observe_cmd
                    .as_ref()
                    .map_or(false, |o| o.enabled.load(Ordering::SeqCst)),
                "{} observer already exists {:?} {:?}",
                self.delegate.tag,
                self.delegate.observe_cmd,
                observe_id
            );
            // TODO(cdc): take observe_cmd when enabled is false.
            self.delegate.observe_cmd = Some(ObserveCmd {
                id: observe_id,
                enabled,
            });
        }

        cb.invoke_read(resp);
    }

    fn handle_tasks<W: WriteBatch<EK>>(
        &mut self,
        apply_ctx: &mut ApplyContext<EK, W>,
        msgs: &mut Vec<Msg<EK>>,
    ) {
        let mut channel_timer = None;
        let mut drainer = msgs.drain(..);
        loop {
            match drainer.next() {
                Some(Msg::Apply { start, apply }) => {
                    if channel_timer.is_none() {
                        channel_timer = Some(start);
                    }
                    self.handle_apply(apply_ctx, apply);
                    if let Some(ref mut state) = self.delegate.yield_state {
                        state.pending_msgs = drainer.collect();
                        break;
                    }
                }
                Some(Msg::Registration(reg)) => self.handle_registration(reg),
                Some(Msg::Destroy(d)) => self.handle_destroy(apply_ctx, d),
                Some(Msg::LogsUpToDate(cul)) => self.logs_up_to_date_for_merge(apply_ctx, cul),
                Some(Msg::Noop) => {}
                Some(Msg::Snapshot(snap_task)) => self.handle_snapshot(apply_ctx, snap_task),
                Some(Msg::Change {
                    cmd,
                    region_epoch,
                    cb,
                }) => self.handle_change(apply_ctx, cmd, region_epoch, cb),
                #[cfg(any(test, feature = "testexport"))]
                Some(Msg::Validate(_, f)) => {
                    let delegate: *const u8 = unsafe { std::mem::transmute(&self.delegate) };
                    f(delegate)
                }
                None => break,
            }
        }
        if let Some(timer) = channel_timer {
            let elapsed = duration_to_sec(timer.elapsed());
            APPLY_TASK_WAIT_TIME_HISTOGRAM.observe(elapsed);
        }
    }
}

impl<EK> Fsm for ApplyFsm<EK>
where
    EK: KvEngine,
{
    type Message = Msg<EK>;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.delegate.stopped
    }

    #[inline]
    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    #[inline]
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        self.mailbox.take()
    }
}

impl<EK> Drop for ApplyFsm<EK>
where
    EK: KvEngine,
{
    fn drop(&mut self) {
        self.delegate.clear_all_commands_as_stale();
    }
}

pub struct ControlMsg;

pub struct ControlFsm;

impl Fsm for ControlFsm {
    type Message = ControlMsg;

    #[inline]
    fn is_stopped(&self) -> bool {
        true
    }
}

pub struct ApplyPoller<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    msg_buf: Vec<Msg<EK>>,
    apply_ctx: ApplyContext<EK, W>,
    messages_per_tick: usize,
    cfg_tracker: Tracker<Config>,
}

impl<EK, W> PollHandler<ApplyFsm<EK>, ControlFsm> for ApplyPoller<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    fn begin(&mut self, _batch_size: usize) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            match Ord::cmp(&incoming.messages_per_tick, &self.messages_per_tick) {
                CmpOrdering::Greater => {
                    self.msg_buf.reserve(incoming.messages_per_tick);
                    self.messages_per_tick = incoming.messages_per_tick;
                }
                CmpOrdering::Less => {
                    self.msg_buf.shrink_to(incoming.messages_per_tick);
                    self.messages_per_tick = incoming.messages_per_tick;
                }
                _ => {}
            }
        }
        self.apply_ctx.perf_context.start_observe();
    }

    /// There is no control fsm in apply poller.
    fn handle_control(&mut self, _: &mut ControlFsm) -> Option<usize> {
        unimplemented!()
    }

    fn handle_normal(&mut self, normal: &mut ApplyFsm<EK>) -> Option<usize> {
        let mut expected_msg_count = None;
        normal.delegate.handle_start = Some(Instant::now_coarse());
        if normal.delegate.yield_state.is_some() {
            if normal.delegate.wait_merge_state.is_some() {
                // We need to query the length first, otherwise there is a race
                // condition that new messages are queued after resuming and before
                // query the length.
                expected_msg_count = Some(normal.receiver.len());
            }
            normal.resume_pending(&mut self.apply_ctx);
            if normal.delegate.wait_merge_state.is_some() {
                // Yield due to applying CommitMerge, this fsm can be released if its
                // channel msg count equals to expected_msg_count because it will receive
                // a new message if its source region has applied all needed logs.
                return expected_msg_count;
            } else if normal.delegate.yield_state.is_some() {
                // Yield due to other reasons, this fsm must not be released because
                // it's possible that no new message will be sent to itself.
                // The remaining messages will be handled in next rounds.
                return None;
            }
            expected_msg_count = None;
        }
        fail_point!("before_handle_normal_3", normal.delegate.id() == 3, |_| {
            None
        });
        fail_point!(
            "before_handle_normal_1003",
            normal.delegate.id() == 1003,
            |_| { None }
        );
        while self.msg_buf.len() < self.messages_per_tick {
            match normal.receiver.try_recv() {
                Ok(msg) => self.msg_buf.push(msg),
                Err(TryRecvError::Empty) => {
                    expected_msg_count = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    normal.delegate.stopped = true;
                    expected_msg_count = Some(0);
                    break;
                }
            }
        }
        normal.handle_tasks(&mut self.apply_ctx, &mut self.msg_buf);
        if normal.delegate.wait_merge_state.is_some() {
            // Check it again immediately as catching up logs can be very fast.
            expected_msg_count = Some(0);
        } else if normal.delegate.yield_state.is_some() {
            // Let it continue to run next time.
            expected_msg_count = None;
        }
        expected_msg_count
    }

    fn end(&mut self, fsms: &mut [Box<ApplyFsm<EK>>]) {
        let is_synced = self.apply_ctx.flush();
        if is_synced {
            for fsm in fsms {
                fsm.delegate.last_sync_apply_index = fsm.delegate.apply_state.get_applied_index();
            }
        }
    }
}

pub struct Builder<EK: KvEngine, W: WriteBatch<EK>> {
    tag: String,
    cfg: Arc<VersionTrack<Config>>,
    coprocessor_host: CoprocessorHost<EK>,
    importer: Arc<SSTImporter>,
    region_scheduler: Scheduler<RegionTask<<EK as KvEngine>::Snapshot>>,
    engine: EK,
    sender: Box<dyn Notifier<EK>>,
    router: ApplyRouter<EK>,
    _phantom: PhantomData<W>,
    store_id: u64,
    pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
}

impl<EK: KvEngine, W> Builder<EK, W>
where
    W: WriteBatch<EK>,
{
    pub fn new<T, ER: RaftEngine>(
        builder: &RaftPollerBuilder<EK, ER, T>,
        sender: Box<dyn Notifier<EK>>,
        router: ApplyRouter<EK>,
    ) -> Builder<EK, W> {
        Builder {
            tag: format!("[store {}]", builder.store.get_id()),
            cfg: builder.cfg.clone(),
            coprocessor_host: builder.coprocessor_host.clone(),
            importer: builder.importer.clone(),
            region_scheduler: builder.region_scheduler.clone(),
            engine: builder.engines.kv.clone(),
            _phantom: PhantomData,
            sender,
            router,
            store_id: builder.store.get_id(),
            pending_create_peers: builder.pending_create_peers.clone(),
        }
    }
}

impl<EK, W> HandlerBuilder<ApplyFsm<EK>, ControlFsm> for Builder<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    type Handler = ApplyPoller<EK, W>;

    fn build(&mut self) -> ApplyPoller<EK, W> {
        let cfg = self.cfg.value();
        ApplyPoller {
            msg_buf: Vec::with_capacity(cfg.messages_per_tick),
            apply_ctx: ApplyContext::new(
                self.tag.clone(),
                self.coprocessor_host.clone(),
                self.importer.clone(),
                self.region_scheduler.clone(),
                self.engine.clone(),
                self.router.clone(),
                self.sender.clone_box(),
                &cfg,
                self.store_id,
                self.pending_create_peers.clone(),
            ),
            messages_per_tick: cfg.messages_per_tick,
            cfg_tracker: self.cfg.clone().tracker(self.tag.clone()),
        }
    }
}

#[derive(Clone)]
pub struct ApplyRouter<EK>
where
    EK: KvEngine,
{
    pub router: BatchRouter<ApplyFsm<EK>, ControlFsm>,
}

impl<EK> Deref for ApplyRouter<EK>
where
    EK: KvEngine,
{
    type Target = BatchRouter<ApplyFsm<EK>, ControlFsm>;

    fn deref(&self) -> &BatchRouter<ApplyFsm<EK>, ControlFsm> {
        &self.router
    }
}

impl<EK> DerefMut for ApplyRouter<EK>
where
    EK: KvEngine,
{
    fn deref_mut(&mut self) -> &mut BatchRouter<ApplyFsm<EK>, ControlFsm> {
        &mut self.router
    }
}

impl<EK> ApplyRouter<EK>
where
    EK: KvEngine,
{
    pub fn schedule_task(&self, region_id: u64, msg: Msg<EK>) {
        let reg = match self.try_send(region_id, msg) {
            Either::Left(Ok(())) => return,
            Either::Left(Err(TrySendError::Disconnected(msg))) | Either::Right(msg) => match msg {
                Msg::Registration(reg) => reg,
                Msg::Apply { mut apply, .. } => {
                    info!(
                        "target region is not found, drop proposals";
                        "region_id" => region_id
                    );
                    for p in apply.cbs.drain(..) {
                        let cmd = PendingCmd::<EK::Snapshot>::new(p.index, p.term, p.cb);
                        notify_region_removed(apply.region_id, apply.peer_id, cmd);
                    }
                    return;
                }
                Msg::Destroy(_) | Msg::Noop => {
                    info!(
                        "target region is not found, drop messages";
                        "region_id" => region_id
                    );
                    return;
                }
                Msg::Snapshot(_) => {
                    warn!(
                        "region is removed before taking snapshot, are we shutting down?";
                        "region_id" => region_id
                    );
                    return;
                }
                Msg::LogsUpToDate(cul) => {
                    warn!(
                        "region is removed before merged, are we shutting down?";
                        "region_id" => region_id,
                        "merge" => ?cul.merge,
                    );
                    return;
                }
                Msg::Change {
                    cmd: ChangeCmd::RegisterObserver { region_id, .. },
                    cb,
                    ..
                }
                | Msg::Change {
                    cmd: ChangeCmd::Snapshot { region_id, .. },
                    cb,
                    ..
                } => {
                    warn!("target region is not found";
                            "region_id" => region_id);
                    let resp = ReadResponse {
                        response: cmd_resp::new_error(Error::RegionNotFound(region_id)),
                        snapshot: None,
                        txn_extra_op: TxnExtraOp::Noop,
                    };
                    cb.invoke_read(resp);
                    return;
                }
                #[cfg(any(test, feature = "testexport"))]
                Msg::Validate(_, _) => return,
            },
            Either::Left(Err(TrySendError::Full(_))) => unreachable!(),
        };

        // Messages in one region are sent in sequence, so there is no race here.
        // However, this can't be handled inside control fsm, as messages can be
        // queued inside both queue of control fsm and normal fsm, which can reorder
        // messages.
        let (sender, apply_fsm) = ApplyFsm::from_registration(reg);
        let mailbox = BasicMailbox::new(sender, apply_fsm);
        self.register(region_id, mailbox);
    }
}

pub struct ApplyBatchSystem<EK: KvEngine> {
    system: BatchSystem<ApplyFsm<EK>, ControlFsm>,
}

impl<EK: KvEngine> Deref for ApplyBatchSystem<EK> {
    type Target = BatchSystem<ApplyFsm<EK>, ControlFsm>;

    fn deref(&self) -> &BatchSystem<ApplyFsm<EK>, ControlFsm> {
        &self.system
    }
}

impl<EK: KvEngine> DerefMut for ApplyBatchSystem<EK> {
    fn deref_mut(&mut self) -> &mut BatchSystem<ApplyFsm<EK>, ControlFsm> {
        &mut self.system
    }
}

impl<EK: KvEngine> ApplyBatchSystem<EK> {
    pub fn schedule_all<'a, ER: RaftEngine>(&self, peers: impl Iterator<Item = &'a Peer<EK, ER>>) {
        let mut mailboxes = Vec::with_capacity(peers.size_hint().0);
        for peer in peers {
            let (tx, fsm) = ApplyFsm::from_peer(peer);
            mailboxes.push((peer.region().get_id(), BasicMailbox::new(tx, fsm)));
        }
        self.router().register_all(mailboxes);
    }
}

pub fn create_apply_batch_system<EK: KvEngine>(
    cfg: &Config,
) -> (ApplyRouter<EK>, ApplyBatchSystem<EK>) {
    let (tx, _) = loose_bounded(usize::MAX);
    let (router, system) =
        batch_system::create_system(&cfg.apply_batch_system, tx, Box::new(ControlFsm));
    (ApplyRouter { router }, ApplyBatchSystem { system })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::*;
    use std::sync::*;
    use std::thread;
    use std::time::*;

    use crate::coprocessor::*;
    use crate::store::msg::WriteResponse;
    use crate::store::peer_storage::RAFT_INIT_LOG_INDEX;
    use crate::store::util::{new_learner_peer, new_peer};
    use engine_test::kv::{new_engine, KvTestEngine, KvTestSnapshot, KvTestWriteBatch};
    use engine_traits::{Peekable as PeekableTrait, WriteBatchExt};
    use kvproto::metapb::{self, RegionEpoch};
    use kvproto::raft_cmdpb::*;
    use protobuf::Message;
    use tempfile::{Builder, TempDir};
    use uuid::Uuid;

    use crate::store::{Config, RegionTask};
    use test_sst_importer::*;
    use tikv_util::config::VersionTrack;
    use tikv_util::worker::dummy_scheduler;

    use super::*;

    pub fn create_tmp_engine(path: &str) -> (TempDir, KvTestEngine) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let engine = new_engine(
            path.path().join("db").to_str().unwrap(),
            None,
            ALL_CFS,
            None,
        )
        .unwrap();
        (path, engine)
    }

    pub fn create_tmp_importer(path: &str) -> (TempDir, Arc<SSTImporter>) {
        let dir = Builder::new().prefix(path).tempdir().unwrap();
        let importer = Arc::new(SSTImporter::new(dir.path(), None).unwrap());
        (dir, importer)
    }

    pub fn new_entry(term: u64, index: u64, set_data: bool) -> Entry {
        let mut e = Entry::default();
        e.set_index(index);
        e.set_term(term);
        if set_data {
            let mut cmd = Request::default();
            cmd.set_cmd_type(CmdType::Put);
            cmd.mut_put().set_key(b"key".to_vec());
            cmd.mut_put().set_value(b"value".to_vec());
            let mut req = RaftCmdRequest::default();
            req.mut_requests().push(cmd);
            e.set_data(req.write_to_bytes().unwrap())
        }
        e
    }

    #[derive(Clone)]
    pub struct TestNotifier<EK: KvEngine> {
        tx: Sender<PeerMsg<EK>>,
    }

    impl<EK: KvEngine> Notifier<EK> for TestNotifier<EK> {
        fn notify(&self, apply_res: Vec<ApplyRes<EK::Snapshot>>) {
            for r in apply_res {
                let res = TaskRes::Apply(r);
                let _ = self.tx.send(PeerMsg::ApplyRes { res });
            }
        }
        fn notify_one(&self, _: u64, msg: PeerMsg<EK>) {
            let _ = self.tx.send(msg);
        }
        fn clone_box(&self) -> Box<dyn Notifier<EK>> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_should_sync_log() {
        // Admin command
        let mut req = RaftCmdRequest::default();
        req.mut_admin_request()
            .set_cmd_type(AdminCmdType::ComputeHash);
        assert_eq!(should_sync_log(&req), true);

        // IngestSst command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::IngestSst);
        req.set_ingest_sst(IngestSstRequest::default());
        let mut cmd = RaftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(should_write_to_engine(&cmd), true);
        assert_eq!(should_sync_log(&cmd), true);

        // Normal command
        let req = RaftCmdRequest::default();
        assert_eq!(should_sync_log(&req), false);
    }

    #[test]
    fn test_should_write_to_engine() {
        // ComputeHash command
        let mut req = RaftCmdRequest::default();
        req.mut_admin_request()
            .set_cmd_type(AdminCmdType::ComputeHash);
        assert_eq!(should_write_to_engine(&req), true);

        // IngestSst command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::IngestSst);
        req.set_ingest_sst(IngestSstRequest::default());
        let mut cmd = RaftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(should_write_to_engine(&cmd), true);
    }

    fn validate<F, E>(router: &ApplyRouter<E>, region_id: u64, validate: F)
    where
        F: FnOnce(&ApplyDelegate<E>) + Send + 'static,
        E: KvEngine,
    {
        let (validate_tx, validate_rx) = mpsc::channel();
        router.schedule_task(
            region_id,
            Msg::Validate(
                region_id,
                Box::new(move |delegate: *const u8| {
                    let delegate = unsafe { &*(delegate as *const ApplyDelegate<E>) };
                    validate(delegate);
                    validate_tx.send(()).unwrap();
                }),
            ),
        );
        validate_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }

    // Make sure msgs are handled in the same batch.
    fn batch_messages<E>(router: &ApplyRouter<E>, region_id: u64, msgs: Vec<Msg<E>>)
    where
        E: KvEngine,
    {
        let (notify1, wait1) = mpsc::channel();
        let (notify2, wait2) = mpsc::channel();
        router.schedule_task(
            region_id,
            Msg::Validate(
                region_id,
                Box::new(move |_| {
                    notify1.send(()).unwrap();
                    wait2.recv().unwrap();
                }),
            ),
        );
        wait1.recv().unwrap();

        for msg in msgs {
            router.schedule_task(region_id, msg);
        }

        notify2.send(()).unwrap();
    }

    fn fetch_apply_res<E>(
        receiver: &::std::sync::mpsc::Receiver<PeerMsg<E>>,
    ) -> ApplyRes<E::Snapshot>
    where
        E: KvEngine,
    {
        match receiver.recv_timeout(Duration::from_secs(3)) {
            Ok(PeerMsg::ApplyRes {
                res: TaskRes::Apply(res),
                ..
            }) => res,
            e => panic!("unexpected res {:?}", e),
        }
    }

    fn proposal<S: Snapshot>(
        is_conf_change: bool,
        index: u64,
        term: u64,
        cb: Callback<S>,
    ) -> Proposal<S> {
        Proposal {
            is_conf_change,
            index,
            term,
            cb,
            renew_lease_time: None,
            must_pass_epoch_check: false,
        }
    }

    fn apply<S: Snapshot>(
        peer_id: u64,
        region_id: u64,
        term: u64,
        entries: Vec<Entry>,
        cbs: Vec<Proposal<S>>,
    ) -> Apply<S> {
        Apply::new(peer_id, region_id, term, entries, cbs)
    }

    #[test]
    fn test_basic_flow() {
        let (tx, rx) = mpsc::channel();
        let sender = Box::new(TestNotifier { tx });
        let (_tmp, engine) = create_tmp_engine("apply-basic");
        let (_dir, importer) = create_tmp_importer("apply-basic");
        let (region_scheduler, mut snapshot_rx) = dummy_scheduler();
        let cfg = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&cfg.value());
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine, KvTestWriteBatch> {
            tag: "test-store".to_owned(),
            cfg,
            coprocessor_host: CoprocessorHost::<KvTestEngine>::default(),
            importer,
            region_scheduler,
            sender,
            engine,
            router: router.clone(),
            _phantom: Default::default(),
            store_id: 1,
            pending_create_peers,
        };
        system.spawn("test-basic".to_owned(), builder);

        let mut reg = Registration {
            id: 1,
            term: 4,
            applied_index_term: 5,
            ..Default::default()
        };
        reg.region.set_id(2);
        reg.apply_state.set_applied_index(3);
        router.schedule_task(2, Msg::Registration(reg.clone()));
        validate(&router, 2, move |delegate| {
            assert_eq!(delegate.id, 1);
            assert_eq!(delegate.tag, "[region 2] 1");
            assert_eq!(delegate.region, reg.region);
            assert!(!delegate.pending_remove);
            assert_eq!(delegate.apply_state, reg.apply_state);
            assert_eq!(delegate.term, reg.term);
            assert_eq!(delegate.applied_index_term, reg.applied_index_term);
        });

        let (resp_tx, resp_rx) = mpsc::channel();
        let p = proposal(
            false,
            1,
            0,
            Callback::write(Box::new(move |resp: WriteResponse| {
                resp_tx.send(resp.response).unwrap();
            })),
        );
        router.schedule_task(
            1,
            Msg::apply(apply(1, 1, 0, vec![new_entry(0, 1, true)], vec![p])),
        );
        // unregistered region should be ignored and notify failed.
        let resp = resp_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_region_not_found());
        assert!(rx.try_recv().is_err());

        let (cc_tx, cc_rx) = mpsc::channel();
        let pops = vec![
            proposal(
                false,
                4,
                4,
                Callback::write(Box::new(move |write: WriteResponse| {
                    cc_tx.send(write.response).unwrap();
                })),
            ),
            proposal(false, 4, 5, Callback::None),
        ];
        router.schedule_task(
            2,
            Msg::apply(apply(1, 2, 11, vec![new_entry(5, 4, true)], pops)),
        );
        // proposal with not commit entry should be ignore
        validate(&router, 2, move |delegate| {
            assert_eq!(delegate.term, 11);
        });
        let cc_resp = cc_rx.try_recv().unwrap();
        assert!(cc_resp.get_header().get_error().has_stale_command());
        assert!(rx.recv_timeout(Duration::from_secs(3)).is_ok());

        // Make sure Apply and Snapshot are in the same batch.
        let (snap_tx, _) = mpsc::sync_channel(0);
        batch_messages(
            &router,
            2,
            vec![
                Msg::apply(apply(1, 2, 11, vec![new_entry(5, 5, false)], vec![])),
                Msg::Snapshot(GenSnapTask::new(2, snap_tx)),
            ],
        );
        let apply_res = match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(PeerMsg::ApplyRes {
                res: TaskRes::Apply(res),
                ..
            }) => res,
            e => panic!("unexpected apply result: {:?}", e),
        };
        let apply_state_key = keys::apply_state_key(2);
        let apply_state = match snapshot_rx.recv_timeout(Duration::from_secs(3)) {
            Ok(Some(RegionTask::Gen { kv_snap, .. })) => kv_snap
                .get_msg_cf(CF_RAFT, &apply_state_key)
                .unwrap()
                .unwrap(),
            e => panic!("unexpected apply result: {:?}", e),
        };
        assert_eq!(apply_res.region_id, 2);
        assert_eq!(apply_res.apply_state, apply_state);
        assert_eq!(apply_res.apply_state.get_applied_index(), 5);
        assert!(apply_res.exec_res.is_empty());
        // empty entry will make applied_index step forward and should write apply state to engine.
        assert_eq!(apply_res.metrics.written_keys, 1);
        assert_eq!(apply_res.applied_index_term, 5);
        validate(&router, 2, |delegate| {
            assert_eq!(delegate.term, 11);
            assert_eq!(delegate.applied_index_term, 5);
            assert_eq!(delegate.apply_state.get_applied_index(), 5);
            assert_eq!(
                delegate.apply_state.get_applied_index(),
                delegate.last_sync_apply_index
            );
        });

        router.schedule_task(2, Msg::destroy(2, false));
        let (region_id, peer_id) = match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(PeerMsg::ApplyRes {
                res: TaskRes::Destroy {
                    region_id, peer_id, ..
                },
                ..
            }) => (region_id, peer_id),
            e => panic!("expected destroy result, but got {:?}", e),
        };
        assert_eq!(peer_id, 1);
        assert_eq!(region_id, 2);

        // Stopped peer should be removed.
        let (resp_tx, resp_rx) = mpsc::channel();
        let p = proposal(
            false,
            1,
            0,
            Callback::write(Box::new(move |resp: WriteResponse| {
                resp_tx.send(resp.response).unwrap();
            })),
        );
        router.schedule_task(
            2,
            Msg::apply(apply(1, 1, 0, vec![new_entry(0, 1, true)], vec![p])),
        );
        // unregistered region should be ignored and notify failed.
        let resp = resp_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            resp.get_header().get_error().has_region_not_found(),
            "{:?}",
            resp
        );
        assert!(rx.try_recv().is_err());

        system.shutdown();
    }

    fn cb<S: Snapshot>(idx: u64, term: u64, tx: Sender<RaftCmdResponse>) -> Proposal<S> {
        proposal(
            false,
            idx,
            term,
            Callback::write(Box::new(move |resp: WriteResponse| {
                tx.send(resp.response).unwrap();
            })),
        )
    }

    struct EntryBuilder {
        entry: Entry,
        req: RaftCmdRequest,
    }

    impl EntryBuilder {
        fn new(index: u64, term: u64) -> EntryBuilder {
            let req = RaftCmdRequest::default();
            let mut entry = Entry::default();
            entry.set_index(index);
            entry.set_term(term);
            EntryBuilder { entry, req }
        }

        fn epoch(mut self, conf_ver: u64, version: u64) -> EntryBuilder {
            let mut epoch = RegionEpoch::default();
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
            let mut cmd = Request::default();
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
            let mut cmd = Request::default();
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
            let mut cmd = Request::default();
            cmd.set_cmd_type(CmdType::DeleteRange);
            if let Some(cf) = cf {
                cmd.mut_delete_range().set_cf(cf.to_owned());
            }
            cmd.mut_delete_range().set_start_key(start_key.to_vec());
            cmd.mut_delete_range().set_end_key(end_key.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn ingest_sst(mut self, meta: &SstMeta) -> EntryBuilder {
            let mut cmd = Request::default();
            cmd.set_cmd_type(CmdType::IngestSst);
            cmd.mut_ingest_sst().set_sst(meta.clone());
            self.req.mut_requests().push(cmd);
            self
        }

        fn split(mut self, splits: BatchSplitRequest) -> EntryBuilder {
            let mut req = AdminRequest::default();
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
        cmd_batches: RefCell<Vec<CmdBatch>>,
        cmd_sink: Option<Arc<Mutex<Sender<CmdBatch>>>>,
    }

    impl Coprocessor for ApplyObserver {}

    impl QueryObserver for ApplyObserver {
        fn pre_apply_query(&self, _: &mut ObserverContext<'_>, _: &[Request]) {
            self.pre_query_count.fetch_add(1, Ordering::SeqCst);
        }

        fn post_apply_query(&self, _: &mut ObserverContext<'_>, _: &mut Cmd) {
            self.post_query_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl<E> CmdObserver<E> for ApplyObserver
    where
        E: KvEngine,
    {
        fn on_prepare_for_apply(&self, observe_id: ObserveID, region_id: u64) {
            self.cmd_batches
                .borrow_mut()
                .push(CmdBatch::new(observe_id, region_id));
        }

        fn on_apply_cmd(&self, observe_id: ObserveID, region_id: u64, cmd: Cmd) {
            self.cmd_batches
                .borrow_mut()
                .last_mut()
                .expect("should exist some cmd batch")
                .push(observe_id, region_id, cmd);
        }

        fn on_flush_apply(&self, _: E) {
            if !self.cmd_batches.borrow().is_empty() {
                let batches = self.cmd_batches.replace(Vec::default());
                for b in batches {
                    if let Some(sink) = self.cmd_sink.as_ref() {
                        sink.lock().unwrap().send(b).unwrap();
                    }
                }
            }
        }
    }

    #[test]
    fn test_handle_raft_committed_entries() {
        let (_path, engine) = create_tmp_engine("test-delegate");
        let (import_dir, importer) = create_tmp_importer("test-delegate");
        let obs = ApplyObserver::default();
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(obs.clone()));

        let (tx, rx) = mpsc::channel();
        let (region_scheduler, _) = dummy_scheduler();
        let sender = Box::new(TestNotifier { tx });
        let cfg = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&cfg.value());
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine, KvTestWriteBatch> {
            tag: "test-store".to_owned(),
            cfg,
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer: importer.clone(),
            engine: engine.clone(),
            router: router.clone(),
            _phantom: Default::default(),
            store_id: 1,
            pending_create_peers,
        };
        system.spawn("test-handle-raft".to_owned(), builder);

        let peer_id = 3;
        let mut reg = Registration {
            id: peer_id,
            ..Default::default()
        };
        reg.region.set_id(1);
        reg.region.mut_peers().push(new_peer(2, 3));
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_conf_ver(1);
        reg.region.mut_region_epoch().set_version(3);
        router.schedule_task(1, Msg::Registration(reg));

        let (capture_tx, capture_rx) = mpsc::channel();
        let put_entry = EntryBuilder::new(1, 1)
            .put(b"k1", b"v1")
            .put(b"k2", b"v1")
            .put(b"k3", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                1,
                vec![put_entry],
                vec![cb(1, 1, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(resp.get_responses().len(), 3);
        let dk_k1 = keys::data_key(b"k1");
        let dk_k2 = keys::data_key(b"k2");
        let dk_k3 = keys::data_key(b"k3");
        assert_eq!(engine.get_value(&dk_k1).unwrap().unwrap(), b"v1");
        assert_eq!(engine.get_value(&dk_k2).unwrap().unwrap(), b"v1");
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");
        validate(&router, 1, |delegate| {
            assert_eq!(delegate.applied_index_term, 1);
            assert_eq!(delegate.apply_state.get_applied_index(), 1);
        });
        fetch_apply_res(&rx);

        let put_entry = EntryBuilder::new(2, 2)
            .put_cf(CF_LOCK, b"k1", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 2, vec![put_entry], vec![])));
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.region_id, 1);
        assert_eq!(apply_res.apply_state.get_applied_index(), 2);
        assert_eq!(apply_res.applied_index_term, 2);
        assert!(apply_res.exec_res.is_empty());
        assert!(apply_res.metrics.written_bytes >= 5);
        assert_eq!(apply_res.metrics.written_keys, 2);
        assert_eq!(apply_res.metrics.size_diff_hint, 5);
        assert_eq!(apply_res.metrics.lock_cf_written_bytes, 5);
        assert_eq!(
            engine.get_value_cf(CF_LOCK, &dk_k1).unwrap().unwrap(),
            b"v1"
        );

        let put_entry = EntryBuilder::new(3, 2)
            .put(b"k2", b"v2")
            .epoch(1, 1)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                2,
                vec![put_entry],
                vec![cb(3, 2, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_epoch_not_match());
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_index_term, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 3);

        let put_entry = EntryBuilder::new(4, 2)
            .put(b"k3", b"v3")
            .put(b"k5", b"v5")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                2,
                vec![put_entry],
                vec![cb(4, 2, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_index_term, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 4);
        // a writebatch should be atomic.
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");

        let put_entry = EntryBuilder::new(5, 3)
            .delete(b"k1")
            .delete_cf(CF_LOCK, b"k1")
            .delete_cf(CF_WRITE, b"k1")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                vec![put_entry],
                vec![cb(5, 2, capture_tx.clone()), cb(5, 3, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        // stale command should be cleared.
        assert!(resp.get_header().get_error().has_stale_command());
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert!(engine.get_value(&dk_k1).unwrap().is_none());
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.metrics.lock_cf_written_bytes, 3);
        assert_eq!(apply_res.metrics.delete_keys_hint, 2);
        assert_eq!(apply_res.metrics.size_diff_hint, -9);

        let delete_entry = EntryBuilder::new(6, 3).delete(b"k5").epoch(1, 3).build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                vec![delete_entry],
                vec![cb(6, 3, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());
        fetch_apply_res(&rx);

        let delete_range_entry = EntryBuilder::new(7, 3)
            .delete_range(b"", b"")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                vec![delete_range_entry],
                vec![cb(7, 3, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");
        fetch_apply_res(&rx);

        let delete_range_entry = EntryBuilder::new(8, 3)
            .delete_range_cf(CF_DEFAULT, b"", b"k5")
            .delete_range_cf(CF_LOCK, b"", b"k5")
            .delete_range_cf(CF_WRITE, b"", b"k5")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                vec![delete_range_entry],
                vec![cb(8, 3, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert!(engine.get_value(&dk_k1).unwrap().is_none());
        assert!(engine.get_value(&dk_k2).unwrap().is_none());
        assert!(engine.get_value(&dk_k3).unwrap().is_none());
        fetch_apply_res(&rx);

        // UploadSST
        let sst_path = import_dir.path().join("test.sst");
        let mut sst_epoch = RegionEpoch::default();
        sst_epoch.set_conf_ver(1);
        sst_epoch.set_version(3);
        let sst_range = (0, 100);
        let (mut meta1, data1) = gen_sst_file(&sst_path, sst_range);
        meta1.set_region_id(1);
        meta1.set_region_epoch(sst_epoch);
        let mut file1 = importer.create(&meta1).unwrap();
        file1.append(&data1).unwrap();
        file1.finish().unwrap();
        let (mut meta2, data2) = gen_sst_file(&sst_path, sst_range);
        meta2.set_region_id(1);
        meta2.mut_region_epoch().set_conf_ver(1);
        meta2.mut_region_epoch().set_version(1234);
        let mut file2 = importer.create(&meta2).unwrap();
        file2.append(&data2).unwrap();
        file2.finish().unwrap();

        // IngestSst
        let put_ok = EntryBuilder::new(9, 3)
            .put(&[sst_range.0], &[sst_range.1])
            .epoch(0, 3)
            .build();
        // Add a put above to test flush before ingestion.
        let capture_tx_clone = capture_tx.clone();
        let ingest_ok = EntryBuilder::new(10, 3)
            .ingest_sst(&meta1)
            .epoch(0, 3)
            .build();
        let ingest_epoch_not_match = EntryBuilder::new(11, 3)
            .ingest_sst(&meta2)
            .epoch(0, 3)
            .build();
        let entries = vec![put_ok, ingest_ok, ingest_epoch_not_match];
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                3,
                entries,
                vec![
                    cb(9, 3, capture_tx.clone()),
                    proposal(
                        false,
                        10,
                        3,
                        Callback::write(Box::new(move |resp: WriteResponse| {
                            // Sleep until yield timeout.
                            thread::sleep(Duration::from_millis(500));
                            capture_tx_clone.send(resp.response).unwrap();
                        })),
                    ),
                    cb(11, 3, capture_tx.clone()),
                ],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        check_db_range(&engine, sst_range);
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().has_error());
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_index_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 10);
        // The region will yield after timeout.
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_index_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 11);

        let write_batch_max_keys = <KvTestEngine as WriteBatchExt>::WRITE_BATCH_MAX_KEYS;

        let mut props = vec![];
        let mut entries = vec![];
        for i in 0..write_batch_max_keys {
            let put_entry = EntryBuilder::new(i as u64 + 12, 3)
                .put(b"k", b"v")
                .epoch(1, 3)
                .build();
            entries.push(put_entry);
            props.push(cb(i as u64 + 12, 3, capture_tx.clone()));
        }
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 3, entries, props)));
        for _ in 0..write_batch_max_keys {
            capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        let index = write_batch_max_keys + 11;
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.apply_state.get_applied_index(), index as u64);
        assert_eq!(obs.pre_query_count.load(Ordering::SeqCst), index);
        assert_eq!(obs.post_query_count.load(Ordering::SeqCst), index);

        system.shutdown();
    }

    #[test]
    fn test_cmd_observer() {
        let (_path, engine) = create_tmp_engine("test-delegate");
        let (_import_dir, importer) = create_tmp_importer("test-delegate");
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        let mut obs = ApplyObserver::default();
        let (sink, cmdbatch_rx) = mpsc::channel();
        obs.cmd_sink = Some(Arc::new(Mutex::new(sink)));
        host.registry
            .register_cmd_observer(1, BoxCmdObserver::new(obs));

        let (tx, rx) = mpsc::channel();
        let (region_scheduler, _) = dummy_scheduler();
        let sender = Box::new(TestNotifier { tx });
        let cfg = Config::default();
        let (router, mut system) = create_apply_batch_system(&cfg);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine, KvTestWriteBatch> {
            tag: "test-store".to_owned(),
            cfg: Arc::new(VersionTrack::new(cfg)),
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer,
            engine,
            router: router.clone(),
            _phantom: Default::default(),
            store_id: 1,
            pending_create_peers,
        };
        system.spawn("test-handle-raft".to_owned(), builder);

        let peer_id = 3;
        let mut reg = Registration {
            id: peer_id,
            ..Default::default()
        };
        reg.region.set_id(1);
        reg.region.mut_peers().push(new_peer(2, 3));
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_conf_ver(1);
        reg.region.mut_region_epoch().set_version(3);
        let region_epoch = reg.region.get_region_epoch().clone();
        router.schedule_task(1, Msg::Registration(reg));

        let put_entry = EntryBuilder::new(1, 1)
            .put(b"k1", b"v1")
            .put(b"k2", b"v1")
            .put(b"k3", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 1, vec![put_entry], vec![])));
        fetch_apply_res(&rx);
        // It must receive nothing because no region registered.
        cmdbatch_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();
        let (block_tx, block_rx) = mpsc::channel::<()>();
        router.schedule_task(
            1,
            Msg::Validate(
                1,
                Box::new(move |_| {
                    // Block the apply worker
                    block_rx.recv().unwrap();
                }),
            ),
        );
        let put_entry = EntryBuilder::new(2, 2)
            .put(b"k0", b"v0")
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 2, vec![put_entry], vec![])));
        // Register cmd observer to region 1.
        let enabled = Arc::new(AtomicBool::new(true));
        let observe_id = ObserveID::new();
        router.schedule_task(
            1,
            Msg::Change {
                region_epoch: region_epoch.clone(),
                cmd: ChangeCmd::RegisterObserver {
                    observe_id,
                    region_id: 1,
                    enabled: enabled.clone(),
                },
                cb: Callback::Read(Box::new(|resp: ReadResponse<KvTestSnapshot>| {
                    assert!(!resp.response.get_header().has_error());
                    assert!(resp.snapshot.is_some());
                    let snap = resp.snapshot.unwrap();
                    assert_eq!(snap.get_value(b"k0").unwrap().unwrap(), b"v0");
                })),
            },
        );
        // Unblock the apply worker
        block_tx.send(()).unwrap();
        fetch_apply_res(&rx);
        let (capture_tx, capture_rx) = mpsc::channel();
        let put_entry = EntryBuilder::new(3, 2)
            .put_cf(CF_LOCK, b"k1", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                2,
                vec![put_entry],
                vec![cb(3, 2, capture_tx)],
            )),
        );
        fetch_apply_res(&rx);
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(resp.get_responses().len(), 1);
        let cmd_batch = cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(resp, cmd_batch.into_iter(1).next().unwrap().response);

        let put_entry1 = EntryBuilder::new(4, 2)
            .put(b"k2", b"v2")
            .epoch(1, 3)
            .build();
        let put_entry2 = EntryBuilder::new(5, 2)
            .put(b"k2", b"v2")
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(peer_id, 1, 2, vec![put_entry1, put_entry2], vec![])),
        );
        let cmd_batch = cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(2, cmd_batch.len());

        // Stop observer regoin 1.
        enabled.store(false, Ordering::SeqCst);
        let put_entry = EntryBuilder::new(6, 2)
            .put(b"k2", b"v2")
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 2, vec![put_entry], vec![])));
        // Must not receive new cmd.
        cmdbatch_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // Must response a RegionNotFound error.
        router.schedule_task(
            2,
            Msg::Change {
                region_epoch,
                cmd: ChangeCmd::RegisterObserver {
                    observe_id,
                    region_id: 2,
                    enabled,
                },
                cb: Callback::Read(Box::new(|resp: ReadResponse<_>| {
                    assert!(resp
                        .response
                        .get_header()
                        .get_error()
                        .has_region_not_found());
                    assert!(resp.snapshot.is_none());
                })),
            },
        );

        system.shutdown();
    }

    #[test]
    fn test_check_sst_for_ingestion() {
        let mut sst = SstMeta::default();
        let mut region = Region::default();

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

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::default();
        req.set_split_key(key.to_vec());
        req.set_new_region_id(id);
        req.set_new_peer_ids(children);
        req
    }

    struct SplitResultChecker<'a, E>
    where
        E: KvEngine,
    {
        engine: E,
        origin_peers: &'a [metapb::Peer],
        epoch: Rc<RefCell<RegionEpoch>>,
    }

    impl<'a, E> SplitResultChecker<'a, E>
    where
        E: KvEngine,
    {
        fn check(&self, start: &[u8], end: &[u8], id: u64, children: &[u64], check_initial: bool) {
            let key = keys::region_state_key(id);
            let state: RegionLocalState = self.engine.get_msg_cf(CF_RAFT, &key).unwrap().unwrap();
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
            let initial_state: RaftApplyState =
                self.engine.get_msg_cf(CF_RAFT, &key).unwrap().unwrap();
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
        let (_path, engine) = create_tmp_engine("test-delegate");
        let (_import_dir, importer) = create_tmp_importer("test-delegate");
        let peer_id = 3;
        let mut reg = Registration {
            id: peer_id,
            term: 1,
            ..Default::default()
        };
        reg.region.set_id(1);
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_version(3);
        let region_epoch = reg.region.get_region_epoch().clone();
        let peers = vec![new_peer(2, 3), new_peer(4, 5), new_learner_peer(6, 7)];
        reg.region.set_peers(peers.clone().into());
        let (tx, _rx) = mpsc::channel();
        let sender = Box::new(TestNotifier { tx });
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        let mut obs = ApplyObserver::default();
        let (sink, cmdbatch_rx) = mpsc::channel();
        obs.cmd_sink = Some(Arc::new(Mutex::new(sink)));
        host.registry
            .register_cmd_observer(1, BoxCmdObserver::new(obs));
        let (region_scheduler, _) = dummy_scheduler();
        let cfg = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&cfg.value());
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine, KvTestWriteBatch> {
            tag: "test-store".to_owned(),
            cfg,
            sender,
            importer,
            region_scheduler,
            coprocessor_host: host,
            engine: engine.clone(),
            router: router.clone(),
            _phantom: Default::default(),
            store_id: 2,
            pending_create_peers,
        };
        system.spawn("test-split".to_owned(), builder);

        router.schedule_task(1, Msg::Registration(reg.clone()));
        let enabled = Arc::new(AtomicBool::new(true));
        let observe_id = ObserveID::new();
        router.schedule_task(
            1,
            Msg::Change {
                region_epoch: region_epoch.clone(),
                cmd: ChangeCmd::RegisterObserver {
                    observe_id,
                    region_id: 1,
                    enabled: enabled.clone(),
                },
                cb: Callback::Read(Box::new(|resp: ReadResponse<_>| {
                    assert!(!resp.response.get_header().has_error(), "{:?}", resp);
                    assert!(resp.snapshot.is_some());
                })),
            },
        );

        let mut index_id = 1;
        let (capture_tx, capture_rx) = mpsc::channel();
        let epoch = Rc::new(RefCell::new(reg.region.get_region_epoch().to_owned()));
        let epoch_ = epoch.clone();
        let mut exec_split = |router: &ApplyRouter<KvTestEngine>, reqs| {
            let epoch = epoch_.borrow();
            let split = EntryBuilder::new(index_id, 1)
                .split(reqs)
                .epoch(epoch.get_conf_ver(), epoch.get_version())
                .build();
            router.schedule_task(
                1,
                Msg::apply(apply(
                    peer_id,
                    1,
                    1,
                    vec![split],
                    vec![cb(index_id, 1, capture_tx.clone())],
                )),
            );
            index_id += 1;
            capture_rx.recv_timeout(Duration::from_secs(3)).unwrap()
        };

        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(true);
        splits.mut_requests().push(new_split_req(b"k1", 8, vec![]));
        let resp = exec_split(&router, splits.clone());
        // 3 followers are required.
        assert!(error_msg(&resp).contains("id count"), "{:?}", resp);
        cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();

        splits.mut_requests().clear();
        let resp = exec_split(&router, splits.clone());
        // Empty requests should be rejected.
        assert!(error_msg(&resp).contains("missing"), "{:?}", resp);

        splits
            .mut_requests()
            .push(new_split_req(b"k6", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // Out of range keys should be rejected.
        assert!(
            resp.get_header().get_error().has_key_not_in_region(),
            "{:?}",
            resp
        );

        splits
            .mut_requests()
            .push(new_split_req(b"", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // Empty key should be rejected.
        assert!(error_msg(&resp).contains("missing"), "{:?}", resp);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 8, vec![9, 10, 11]));
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // keys should be in ascend order.
        assert!(error_msg(&resp).contains("invalid"), "{:?}", resp);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 8, vec![9, 10]));
        let resp = exec_split(&router, splits.clone());
        // All requests should be checked.
        assert!(error_msg(&resp).contains("id count"), "{:?}", resp);
        let checker = SplitResultChecker {
            engine,
            origin_peers: &peers,
            epoch: epoch.clone(),
        };

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // Split should succeed.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        let mut new_version = epoch.borrow().get_version() + 1;
        epoch.borrow_mut().set_version(new_version);
        checker.check(b"", b"k1", 8, &[9, 10, 11], true);
        checker.check(b"k1", b"k5", 1, &[3, 5, 7], false);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k4", 12, vec![13, 14, 15]));
        splits.set_right_derive(false);
        let resp = exec_split(&router, splits.clone());
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        new_version = epoch.borrow().get_version() + 1;
        epoch.borrow_mut().set_version(new_version);
        checker.check(b"k4", b"k5", 12, &[13, 14, 15], true);
        checker.check(b"k1", b"k4", 1, &[3, 5, 7], false);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 16, vec![17, 18, 19]));
        splits
            .mut_requests()
            .push(new_split_req(b"k3", 20, vec![21, 22, 23]));
        splits.set_right_derive(true);
        let resp = exec_split(&router, splits.clone());
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        new_version = epoch.borrow().get_version() + 2;
        epoch.borrow_mut().set_version(new_version);
        checker.check(b"k1", b"k2", 16, &[17, 18, 19], true);
        checker.check(b"k2", b"k3", 20, &[21, 22, 23], true);
        checker.check(b"k3", b"k4", 1, &[3, 5, 7], false);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k31", 24, vec![25, 26, 27]));
        splits
            .mut_requests()
            .push(new_split_req(b"k32", 28, vec![29, 30, 31]));
        splits.set_right_derive(false);
        let resp = exec_split(&router, splits);
        // Right derive should be respected.
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        new_version = epoch.borrow().get_version() + 2;
        epoch.borrow_mut().set_version(new_version);
        checker.check(b"k3", b"k31", 1, &[3, 5, 7], false);
        checker.check(b"k31", b"k32", 24, &[25, 26, 27], true);
        checker.check(b"k32", b"k4", 28, &[29, 30, 31], true);

        let (tx, rx) = mpsc::channel();
        enabled.store(false, Ordering::SeqCst);
        router.schedule_task(
            1,
            Msg::Change {
                region_epoch,
                cmd: ChangeCmd::RegisterObserver {
                    observe_id,
                    region_id: 1,
                    enabled: Arc::new(AtomicBool::new(true)),
                },
                cb: Callback::Read(Box::new(move |resp: ReadResponse<_>| {
                    assert!(
                        resp.response.get_header().get_error().has_epoch_not_match(),
                        "{:?}",
                        resp
                    );
                    assert!(resp.snapshot.is_none());
                    tx.send(()).unwrap();
                })),
            },
        );
        rx.recv_timeout(Duration::from_millis(500)).unwrap();

        system.shutdown();
    }

    #[test]
    fn pending_cmd_leak() {
        let res = panic_hook::recover_safe(|| {
            let _cmd = PendingCmd::<KvTestSnapshot>::new(1, 1, Callback::None);
        });
        res.unwrap_err();
    }

    #[test]
    fn pending_cmd_leak_dtor_not_abort() {
        let res = panic_hook::recover_safe(|| {
            let _cmd = PendingCmd::<KvTestSnapshot>::new(1, 1, Callback::None);
            panic!("Don't abort");
            // It would abort and fail if there was a double-panic in PendingCmd dtor.
        });
        res.unwrap_err();
    }
}
