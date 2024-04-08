// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
#[cfg(test)]
use std::sync::mpsc::Sender;
use std::{
    borrow::Cow,
    cmp,
    cmp::{Ord, Ordering as CmpOrdering},
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    io::BufRead,
    mem,
    ops::{Deref, DerefMut, Range as StdRange},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::SyncSender,
        Arc, Mutex,
    },
    time::Duration,
    usize,
    vec::Drain,
};

use batch_system::{
    BasicMailbox, BatchRouter, BatchSystem, Config as BatchSystemConfig, Fsm, HandleResult,
    HandlerBuilder, PollHandler, Priority,
};
use collections::{HashMap, HashMapEntry, HashSet};
use crossbeam::channel::{TryRecvError, TrySendError};
use engine_traits::{
    util::SequenceNumber, CacheRange, DeleteStrategy, KvEngine, Mutable, PerfContext,
    PerfContextKind, RaftEngine, RaftEngineReadOnly, Range as EngineRange, Snapshot, SstMetaInfo,
    WriteBatch, WriteOptions, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE,
};
use fail::fail_point;
use health_controller::types::LatencyInspector;
use kvproto::{
    import_sstpb::SstMeta,
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb::{self, PeerRole, Region, RegionEpoch},
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, ChangePeerRequest, CmdType, CommitMergeRequest,
        RaftCmdRequest, RaftCmdResponse, Request, SplitRequest, SwitchWitnessRequest,
    },
    raft_serverpb::{MergeState, PeerState, RaftApplyState, RaftTruncatedState, RegionLocalState},
};
use pd_client::{BucketMeta, BucketStat};
use prometheus::local::LocalHistogram;
use protobuf::{wire_format::WireType, CodedInputStream, Message};
use raft::eraftpb::{
    ConfChange, ConfChangeType, ConfChangeV2, Entry, EntryType, Snapshot as RaftSnapshot,
};
use raft_proto::ConfChangeI;
use resource_control::{ResourceConsumeType, ResourceController, ResourceMetered};
use smallvec::{smallvec, SmallVec};
use sst_importer::SstImporter;
use tikv_alloc::trace::TraceEvent;
use tikv_util::{
    box_err, box_try,
    config::{Tracker, VersionTrack},
    debug, error, info,
    memory::HeapSize,
    mpsc::{loose_bounded, LooseBoundedSender, Receiver},
    safe_panic, slow_log,
    store::{find_peer, find_peer_by_id, find_peer_mut, is_learner, remove_peer},
    time::{duration_to_sec, Instant},
    warn,
    worker::Scheduler,
    Either, MustConsumeVec,
};
use time::Timespec;
use tracker::GLOBAL_TRACKERS;
use uuid::Builder as UuidBuilder;

use self::memtrace::*;
use super::metrics::*;
use crate::{
    bytes_capacity,
    coprocessor::{
        ApplyCtxInfo, Cmd, CmdBatch, CmdObserveInfo, CoprocessorHost, ObserveHandle, ObserveLevel,
        RegionState,
    },
    store::{
        cmd_resp,
        entry_storage::{self, CachedEntries},
        fsm::RaftPollerBuilder,
        local_metrics::RaftMetrics,
        memory::*,
        metrics::*,
        msg::{Callback, ErrorCallback, PeerMsg, ReadResponse, SignificantMsg},
        peer::Peer,
        peer_storage::{write_initial_apply_state, write_peer_state},
        util::{
            self, admin_cmd_epoch_lookup, check_flashback_state, check_req_region_epoch,
            compare_region_epoch, ChangePeerI, ConfChangeKind, KeysInfoFormatter,
        },
        Config, RegionSnapshot, RegionTask, WriteCallback,
    },
    Error, Result,
};

// These consts are shared in both v1 and v2.
pub const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;
pub const APPLY_WB_SHRINK_SIZE: usize = 1024 * 1024;
pub const SHRINK_PENDING_CMD_QUEUE_CAP: usize = 64;
pub const MAX_APPLY_BATCH_SIZE: usize = 64 * 1024 * 1024;

pub struct PendingCmd<C> {
    pub index: u64,
    pub term: u64,
    pub cb: Option<C>,
}

impl<C> PendingCmd<C> {
    fn new(index: u64, term: u64, cb: C) -> PendingCmd<C> {
        PendingCmd {
            index,
            term,
            cb: Some(cb),
        }
    }
}

impl<C> Drop for PendingCmd<C> {
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

impl<C> Debug for PendingCmd<C> {
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

impl<C> HeapSize for PendingCmd<C> {}

/// Commands waiting to be committed and applied.
#[derive(Debug)]
pub struct PendingCmdQueue<C> {
    normals: VecDeque<PendingCmd<C>>,
    conf_change: Option<PendingCmd<C>>,
    compacts: VecDeque<PendingCmd<C>>,
}

impl<C> PendingCmdQueue<C> {
    fn new() -> PendingCmdQueue<C> {
        PendingCmdQueue {
            normals: VecDeque::new(),
            conf_change: None,
            compacts: VecDeque::new(),
        }
    }

    fn pop_normal(&mut self, index: u64, term: u64) -> Option<PendingCmd<C>> {
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

    fn append_normal(&mut self, cmd: PendingCmd<C>) {
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PendingCmd<C>> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        self.conf_change.take()
    }

    // TODO: seems we don't need to separate conf change from normal entries.
    fn set_conf_change(&mut self, cmd: PendingCmd<C>) {
        self.conf_change = Some(cmd);
    }

    fn push_compact(&mut self, cmd: PendingCmd<C>) {
        self.compacts.push_back(cmd);
    }

    fn pop_compact(&mut self, index: u64) -> Option<PendingCmd<C>> {
        let mut front = None;
        while self.compacts.front().map_or(false, |c| c.index < index) {
            front = self.compacts.pop_front();
            front.as_mut().unwrap().cb.take().unwrap();
        }
        front
    }

    fn has_compact(&mut self) -> bool {
        !self.compacts.is_empty()
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

#[derive(Default, Debug)]
pub struct SwitchWitness {
    pub index: u64,
    pub switches: Vec<SwitchWitnessRequest>,
    pub region: Region,
}

#[derive(Debug)]
pub enum ExecResult<S> {
    ChangePeer(ChangePeer),
    CompactLog {
        state: RaftTruncatedState,
        first_index: u64,
        has_pending: bool,
    },
    SplitRegion {
        regions: Vec<Region>,
        derived: Region,
        new_split_regions: HashMap<u64, NewSplitPeer>,
        share_source_region_size: bool,
    },
    PrepareMerge {
        region: Region,
        state: MergeState,
    },
    CommitMerge {
        index: u64,
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
        ssts: Vec<SstMetaInfo>,
    },
    TransferLeader {
        term: u64,
    },
    Flashback {
        region: Region,
    },
    BatchSwitchWitness(SwitchWitness),
    // The raftstore thread will use it to update the internal state of `PeerFsm`. If it is
    // `true`, when the raftstore detects that the raft log has not been gc for a long time,
    // the raftstore thread will actively pull the `voter_replicated_index` from the leader
    // and try to compact pending gc. If false, raftstore does not do any additional
    // processing.
    HasPendingCompactCmd(bool),
}

/// The possible returned value when applying logs.
#[derive(Debug)]
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

// The applied command and their callback
struct ApplyCallbackBatch<S>
where
    S: Snapshot,
{
    cmd_batch: Vec<CmdBatch>,
    // The max observe level of current `Vec<CmdBatch>`
    batch_max_level: ObserveLevel,
    cb_batch: MustConsumeVec<(Callback<S>, RaftCmdResponse)>,
}

impl<S: Snapshot> ApplyCallbackBatch<S> {
    fn new() -> ApplyCallbackBatch<S> {
        ApplyCallbackBatch {
            cmd_batch: vec![],
            batch_max_level: ObserveLevel::None,
            cb_batch: MustConsumeVec::new("callback of apply callback batch"),
        }
    }

    fn push_batch(&mut self, observe_info: &CmdObserveInfo, region_id: u64) {
        let cb = CmdBatch::new(observe_info, region_id);
        self.batch_max_level = cmp::max(self.batch_max_level, cb.level);
        self.cmd_batch.push(cb);
    }

    fn push_cb(&mut self, cb: Callback<S>, resp: RaftCmdResponse) {
        self.cb_batch.push((cb, resp));
    }

    fn push(
        &mut self,
        cb: Option<Callback<S>>,
        cmd: Cmd,
        observe_info: &CmdObserveInfo,
        region_id: u64,
    ) {
        if let Some(cb) = cb {
            self.cb_batch.push((cb, cmd.response.clone()));
        }
        self.cmd_batch
            .last_mut()
            .unwrap()
            .push(observe_info, region_id, cmd);
    }
}

pub trait Notifier<EK: KvEngine>: Send {
    fn notify(&self, apply_res: Vec<ApplyRes<EK::Snapshot>>);
    fn notify_one(&self, region_id: u64, msg: PeerMsg<EK>);
    fn clone_box(&self) -> Box<dyn Notifier<EK>>;
}

struct ApplyContext<EK>
where
    EK: KvEngine,
{
    tag: String,
    timer: Option<Instant>,
    host: CoprocessorHost<EK>,
    importer: Arc<SstImporter<EK>>,
    region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    router: ApplyRouter<EK>,
    notifier: Box<dyn Notifier<EK>>,
    engine: EK,
    applied_batch: ApplyCallbackBatch<EK::Snapshot>,
    apply_res: Vec<ApplyRes<EK::Snapshot>>,
    exec_log_index: u64,
    exec_log_term: u64,

    kv_wb: EK::WriteBatch,
    kv_wb_last_bytes: u64,
    kv_wb_last_keys: u64,

    committed_count: usize,

    // Whether synchronize WAL is preferred.
    sync_log_hint: bool,
    // Whether to use the delete range API instead of deleting one by one.
    use_delete_range: bool,

    perf_context: EK::PerfContext,

    yield_duration: Duration,
    yield_msg_size: u64,

    store_id: u64,
    /// region_id -> (peer_id, is_splitting)
    /// Used for handling race between splitting and creating new peer.
    /// An uninitialized peer can be replaced to the one from splitting iff they
    /// are exactly the same peer.
    pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,

    /// We must delete the ingested file before calling `callback` so that any
    /// ingest-request reaching this peer could see this update if leader
    /// had changed. We must also delete them after the applied-index
    /// has been persisted to kvdb because this entry may replay because of
    /// panic or power-off, which happened before `WriteBatch::write` and
    /// after `SstImporter::delete`. We shall make sure that this entry will
    /// never apply again at first, then we can delete the ssts files.
    delete_ssts: Vec<SstMetaInfo>,

    /// A self-defined engine may be slow to ingest ssts.
    /// It may move some elements of `delete_ssts` into `pending_delete_ssts` to
    /// delay deletion. Otherwise we may lost data.
    pending_delete_ssts: Vec<SstMetaInfo>,

    /// The priority of this Handler.
    priority: Priority,
    /// Whether to yield high-latency operation to low-priority handler.
    yield_high_latency_operation: bool,

    /// The ssts waiting to be ingested in `write_to_db`.
    pending_ssts: Vec<SstMetaInfo>,

    /// The pending inspector should be cleaned at the end of a write.
    pending_latency_inspect: Vec<LatencyInspector>,
    apply_wait: LocalHistogram,
    apply_time: LocalHistogram,
    key_size: LocalHistogram,
    value_size: LocalHistogram,

    key_buffer: Vec<u8>,

    // Whether to disable WAL.
    disable_wal: bool,

    /// A general apply progress for a delegate is:
    /// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
    /// Sometimes an `ApplyRes` is created with an applied_index, but data
    /// before the applied index is still not written to kvdb. Let's call the
    /// `ApplyRes` uncommitted. Data will finally be written to kvdb in
    /// `flush`.
    uncommitted_res_count: usize,

    enable_v2_compatible_learner: bool,
}

impl<EK> ApplyContext<EK>
where
    EK: KvEngine,
{
    pub fn new(
        tag: String,
        host: CoprocessorHost<EK>,
        importer: Arc<SstImporter<EK>>,
        region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
        engine: EK,
        router: ApplyRouter<EK>,
        notifier: Box<dyn Notifier<EK>>,
        cfg: &Config,
        store_id: u64,
        pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
        priority: Priority,
    ) -> ApplyContext<EK> {
        let kv_wb = engine.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE);

        ApplyContext {
            tag,
            timer: None,
            host,
            importer,
            region_scheduler,
            engine,
            router,
            notifier,
            kv_wb,
            applied_batch: ApplyCallbackBatch::new(),
            apply_res: vec![],
            exec_log_index: 0,
            exec_log_term: 0,
            kv_wb_last_bytes: 0,
            kv_wb_last_keys: 0,
            committed_count: 0,
            sync_log_hint: false,
            use_delete_range: cfg.use_delete_range,
            perf_context: EK::get_perf_context(cfg.perf_level, PerfContextKind::RaftstoreApply),
            yield_duration: cfg.apply_yield_duration.0,
            yield_msg_size: cfg.apply_yield_write_size.0,
            delete_ssts: vec![],
            pending_delete_ssts: vec![],
            store_id,
            pending_create_peers,
            priority,
            yield_high_latency_operation: cfg.apply_batch_system.low_priority_pool_size > 0,
            pending_ssts: vec![],
            pending_latency_inspect: vec![],
            apply_wait: APPLY_TASK_WAIT_TIME_HISTOGRAM.local(),
            apply_time: APPLY_TIME_HISTOGRAM.local(),
            key_size: STORE_APPLY_KEY_SIZE_HISTOGRAM.local(),
            value_size: STORE_APPLY_VALUE_SIZE_HISTOGRAM.local(),
            key_buffer: Vec::with_capacity(1024),
            disable_wal: false,
            uncommitted_res_count: 0,
            enable_v2_compatible_learner: cfg.enable_v2_compatible_learner,
        }
    }

    /// Prepares for applying entries for `delegate`.
    ///
    /// A general apply progress for a delegate is:
    /// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
    /// After all delegates are handled, `write_to_db` method should be called.
    pub fn prepare_for(&mut self, delegate: &mut ApplyDelegate<EK>) {
        self.applied_batch
            .push_batch(&delegate.observe_info, delegate.region.get_id());
        let range = CacheRange::from_region(&delegate.region);
        self.kv_wb.prepare_for_range(range);
    }

    /// Commits all changes have done for delegate. `persistent` indicates
    /// whether write the changes into rocksdb.
    ///
    /// This call is valid only when it's between a `prepare_for` and
    /// `finish_for`.
    pub fn commit(&mut self, delegate: &mut ApplyDelegate<EK>) {
        if delegate.last_flush_applied_index < delegate.apply_state.get_applied_index() {
            delegate.maybe_write_apply_state(self);
        }
        self.commit_opt(delegate, true);
    }

    fn commit_opt(&mut self, delegate: &mut ApplyDelegate<EK>, persistent: bool) {
        delegate.update_metrics(self);
        if persistent {
            if let (_, Some(seqno)) = self.write_to_db() {
                delegate.unfinished_write_seqno.push(seqno);
            }
            self.prepare_for(delegate);
            delegate.last_flush_applied_index = delegate.apply_state.get_applied_index();
            delegate.has_pending_ssts = false;
        }
        self.kv_wb_last_bytes = self.kv_wb().data_size() as u64;
        self.kv_wb_last_keys = self.kv_wb().count() as u64;
    }

    /// Writes all the changes into RocksDB.
    /// If it returns true, all pending writes are persisted in engines.
    pub fn write_to_db(&mut self) -> (bool, Option<SequenceNumber>) {
        let need_sync = self.sync_log_hint && !self.disable_wal;
        let mut seqno = None;
        // There may be put and delete requests after ingest request in the same fsm.
        // To guarantee the correct order, we must ingest the pending_sst first, and
        // then persist the kv write batch to engine.
        if !self.pending_ssts.is_empty() {
            let tag = self.tag.clone();
            self.importer
                .ingest(&self.pending_ssts, &self.engine)
                .unwrap_or_else(|e| {
                    panic!(
                        "{} failed to ingest ssts {:?}: {:?}",
                        tag, self.pending_ssts, e
                    );
                });
            self.pending_ssts = vec![];
        }
        if !self.kv_wb_mut().is_empty() {
            self.perf_context.start_observe();
            let mut write_opts = engine_traits::WriteOptions::new();
            write_opts.set_sync(need_sync);
            write_opts.set_disable_wal(self.disable_wal);
            if self.disable_wal {
                let sn = SequenceNumber::pre_write();
                seqno = Some(sn);
            }
            let seq = self.kv_wb_mut().write_opt(&write_opts).unwrap_or_else(|e| {
                panic!("failed to write to engine: {:?}", e);
            });
            if let Some(seqno) = seqno.as_mut() {
                seqno.post_write(seq)
            }
            let trackers: Vec<_> = self
                .applied_batch
                .cb_batch
                .iter()
                .flat_map(|(cb, _)| cb.write_trackers())
                .flat_map(|trackers| trackers.as_tracker_token())
                .collect();
            self.perf_context.report_metrics(&trackers);
            self.sync_log_hint = false;
            let data_size = self.kv_wb().data_size();
            if data_size > APPLY_WB_SHRINK_SIZE {
                // Control the memory usage for the WriteBatch.
                self.kv_wb = self.engine.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE);
            } else {
                // Clear data, reuse the WriteBatch, this can reduce memory allocations and
                // deallocations.
                self.kv_wb_mut().clear();
            }
            self.kv_wb_last_bytes = 0;
            self.kv_wb_last_keys = 0;
        }
        if !self.delete_ssts.is_empty() {
            let tag = self.tag.clone();
            for sst in self.delete_ssts.drain(..) {
                self.importer.delete(&sst.meta).unwrap_or_else(|e| {
                    panic!("{} cleanup ingested file {:?}: {:?}", tag, sst, e);
                });
            }
        }
        // Take the applied commands and their callback
        let ApplyCallbackBatch {
            cmd_batch,
            batch_max_level,
            mut cb_batch,
        } = mem::replace(&mut self.applied_batch, ApplyCallbackBatch::new());
        // Call it before invoking callback for preventing Commit is executed before
        // Prewrite is observed.
        self.host
            .on_flush_applied_cmd_batch(batch_max_level, cmd_batch, &self.engine);
        // Invoke callbacks
        let now = std::time::Instant::now();
        for (cb, resp) in cb_batch.drain(..) {
            for tracker in cb.write_trackers() {
                tracker.observe(now, &self.apply_time, |t| &mut t.metrics.apply_time_nanos);
            }
            cb.invoke_with_response(resp);
        }
        self.apply_time.flush();
        self.apply_wait.flush();
        self.key_size.flush();
        self.value_size.flush();
        let res_count = self.uncommitted_res_count;
        self.uncommitted_res_count = 0;
        if let Some(seqno) = seqno {
            for res in self.apply_res.iter_mut().rev().take(res_count) {
                res.write_seqno.push(seqno);
            }
        }
        (need_sync, seqno)
    }

    /// Finishes `Apply`s for the delegate.
    pub fn finish_for(
        &mut self,
        delegate: &mut ApplyDelegate<EK>,
        results: VecDeque<ExecResult<EK::Snapshot>>,
    ) {
        if self.host.pre_persist(&delegate.region, true, None) {
            delegate.maybe_write_apply_state(self);
            self.commit_opt(delegate, false);
        } else {
            debug!("do not persist when finish_for";
                "region" => ?delegate.region,
                "tag" => &delegate.tag,
            );
        }
        self.apply_res.push(ApplyRes {
            region_id: delegate.region_id(),
            apply_state: delegate.apply_state.clone(),
            write_seqno: mem::take(&mut delegate.unfinished_write_seqno),
            exec_res: results,
            metrics: mem::take(&mut delegate.metrics),
            applied_term: delegate.applied_term,
            bucket_stat: delegate.buckets.clone(),
        });
        if !self.kv_wb().is_empty() {
            // Pending writes not flushed, need to set seqno to following ApplyRes later
            // after flushing
            self.uncommitted_res_count += 1;
        }
    }

    pub fn delta_bytes(&self) -> u64 {
        self.kv_wb().data_size() as u64 - self.kv_wb_last_bytes
    }

    pub fn delta_keys(&self) -> u64 {
        self.kv_wb().count() as u64 - self.kv_wb_last_keys
    }

    #[inline]
    pub fn kv_wb(&self) -> &EK::WriteBatch {
        &self.kv_wb
    }

    #[inline]
    pub fn kv_wb_mut(&mut self) -> &mut EK::WriteBatch {
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
        let (is_synced, _) = self.write_to_db();

        if !self.apply_res.is_empty() {
            fail_point!("before_nofity_apply_res");
            let apply_res = mem::take(&mut self.apply_res);
            self.notifier.notify(apply_res);
        }

        let elapsed = t.saturating_elapsed();
        STORE_APPLY_LOG_HISTOGRAM.observe(duration_to_sec(elapsed));
        for mut inspector in std::mem::take(&mut self.pending_latency_inspect) {
            inspector.record_apply_process(elapsed);
            inspector.finish();
        }

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
fn notify_region_removed(region_id: u64, peer_id: u64, mut cmd: PendingCmd<impl ErrorCallback>) {
    debug!(
        "region is removed, notify commands";
        "region_id" => region_id,
        "peer_id" => peer_id,
        "index" => cmd.index,
        "term" => cmd.term
    );
    notify_req_region_removed(region_id, cmd.cb.take().unwrap());
}

pub fn notify_req_region_removed(region_id: u64, cb: impl ErrorCallback) {
    let region_not_found = Error::RegionNotFound(region_id);
    let resp = cmd_resp::new_error(region_not_found);
    cb.report_error(resp);
}

/// Calls the callback of `cmd` when it can not be processed further.
fn notify_stale_command(
    region_id: u64,
    peer_id: u64,
    term: u64,
    mut cmd: PendingCmd<impl ErrorCallback>,
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

pub fn notify_stale_req(term: u64, cb: impl ErrorCallback) {
    let resp = cmd_resp::err_resp(Error::StaleCommand, term);
    cb.report_error(resp);
}

pub fn notify_stale_req_with_msg(term: u64, msg: String, cb: impl ErrorCallback) {
    let mut resp = cmd_resp::err_resp(Error::StaleCommand, term);
    resp.mut_header().mut_error().set_message(msg);
    cb.report_error(resp);
}

/// Checks if a write is needed to be issued before handling the command.
fn should_write_to_engine(has_pending_writes: bool, cmd: &RaftCmdRequest) -> bool {
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
        if req.has_ingest_sst() && has_pending_writes {
            return true;
        }
    }

    false
}

/// Checks if a write has high-latency operation.
fn has_high_latency_operation(cmd: &RaftCmdRequest) -> bool {
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
            // We do not need to sync WAL before compact log, because this request will send
            // a msg to raft_gc_log thread to delete the entries before this
            // index instead of deleting them in apply thread directly.
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

fn can_witness_skip(entry: &Entry) -> bool {
    // need to handle ConfChange entry type
    if entry.get_entry_type() != EntryType::EntryNormal {
        return false;
    }

    // HACK: check admin request field in serialized data from `RaftCmdRequest`
    // without deserializing all. It's done by checking the existence of the
    // field number of `admin_request`.
    // See the encoding in `write_to_with_cached_sizes()` of `RaftCmdRequest` in
    // `raft_cmdpb.rs` for reference.
    let mut is = CodedInputStream::from_bytes(entry.get_data());
    if is.eof().unwrap() {
        return true;
    }
    let (mut field_number, wire_type) = is.read_tag_unpack().unwrap();
    // Header field is of number 1
    if field_number == 1 {
        if wire_type != WireType::WireTypeLengthDelimited {
            panic!("unexpected wire type");
        }
        let len = is.read_raw_varint32().unwrap();
        // skip parsing the content of `Header`
        is.consume(len as usize);
        // read next field number
        (field_number, _) = is.read_tag_unpack().unwrap();
    }

    // `Requests` field is of number 2 and `AdminRequest` field is of number 3.
    // - If the next field is 2, there must be no admin request as in one
    //   `RaftCmdRequest`, either requests or admin_request is filled.
    // - If the next field is 3, it's exactly an admin request.
    // - If the next field is others, neither requests nor admin_request is filled,
    //   so there is no admin request.
    field_number != 3
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
    /// A flag that indicates whether the source peer has applied to the
    /// required index. If the source peer is ready, this flag should be set
    /// to the region id of source peer.
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

    /// Cache heap size for itself.
    heap_size: Option<usize>,
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
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ApplyDelegate<EK>
where
    EK: KvEngine,
{
    /// The term of the Region.
    term: u64,
    /// The Region information of the peer.
    region: Region,
    /// The Peer information.
    peer: metapb::Peer,
    /// Peer_tag, "[region region_id] peer_id".
    tag: String,

    /// If the delegate should be stopped from polling.
    /// A delegate can be stopped in conf change, merge or requested by destroy
    /// message.
    stopped: bool,
    /// The start time of the current round to execute commands.
    handle_start: Option<Instant>,
    /// Set to true when removing itself because of
    /// `ConfChangeType::RemoveNode`, and then any following committed logs
    /// in same Ready should be applied failed.
    pending_remove: bool,

    /// Indicates whether the peer is waiting data. See more in `Peer`.
    wait_data: bool,

    /// The commands waiting to be committed and applied
    pending_cmds: PendingCmdQueue<Callback<EK::Snapshot>>,
    /// The counter of pending request snapshots. See more in `Peer`.
    pending_request_snapshot_count: Arc<AtomicUsize>,

    /// Indicates the peer is in merging, if that compact log won't be
    /// performed.
    is_merging: bool,
    /// Records the epoch version after the last merge.
    last_merge_version: u64,
    yield_state: Option<YieldState<EK>>,
    /// A temporary state that keeps track of the progress of the source peer
    /// state when CommitMerge is unable to be executed.
    wait_merge_state: Option<WaitSourceMergeState>,
    // ID of last region that reports ready.
    ready_source_region_id: u64,

    /// TiKV writes apply_state to KV RocksDB, in one write batch together with
    /// kv data.
    ///
    /// If we write it to Raft RocksDB, apply_state and kv data (Put, Delete)
    /// are in separate WAL file. When power failure, for current raft log,
    /// apply_index may synced to file, but KV data may not synced to file,
    /// so we will lose data.
    apply_state: RaftApplyState,
    /// The term of the raft log at applied index.
    applied_term: u64,
    /// The latest flushed applied index.
    last_flush_applied_index: u64,

    /// Info about cmd observer.
    observe_info: CmdObserveInfo,

    /// The local metrics, and it will be flushed periodically.
    metrics: ApplyMetrics,

    /// Priority in batch system. When applying some commands which have high
    /// latency, we decrease the priority of current fsm to reduce the
    /// impact on other normal commands.
    priority: Priority,

    /// To fetch Raft entries for applying if necessary.
    #[derivative(Debug = "ignore")]
    raft_engine: Box<dyn RaftEngineReadOnly>,

    trace: ApplyMemoryTrace,

    buckets: Option<BucketStat>,

    unfinished_write_seqno: Vec<SequenceNumber>,

    has_pending_ssts: bool,
}

impl<EK> ApplyDelegate<EK>
where
    EK: KvEngine,
{
    fn from_registration(reg: Registration) -> ApplyDelegate<EK> {
        ApplyDelegate {
            tag: format!("[region {}] {}", reg.region.get_id(), reg.id),
            peer: find_peer_by_id(&reg.region, reg.id).unwrap().clone(),
            region: reg.region,
            pending_remove: false,
            wait_data: false,
            last_flush_applied_index: reg.apply_state.get_applied_index(),
            apply_state: reg.apply_state,
            applied_term: reg.applied_term,
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
            // use a default `CmdObserveInfo` because observing is disable by default
            observe_info: CmdObserveInfo::default(),
            priority: Priority::Normal,
            raft_engine: reg.raft_engine,
            trace: ApplyMemoryTrace::default(),
            buckets: None,
            unfinished_write_seqno: vec![],
            has_pending_ssts: false,
        }
    }

    pub fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn id(&self) -> u64 {
        self.peer.get_id()
    }

    /// Handles all the committed_entries, namely, applies the committed
    /// entries.
    fn handle_raft_committed_entries(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        mut committed_entries_drainer: Drain<'_, Entry>,
    ) {
        if committed_entries_drainer.len() == 0 {
            return;
        }
        apply_ctx.prepare_for(self);
        // If we send multiple ConfChange commands, only first one will be proposed
        // correctly, others will be saved as a normal entry with no data, so we
        // must re-propose these commands again.
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
                    "{} expect index {}, but got {}, ctx {}",
                    self.tag,
                    expect_index,
                    entry.get_index(),
                    apply_ctx.tag,
                );
            }

            // NOTE: before v5.0, `EntryType::EntryConfChangeV2` entry is handled by
            // `unimplemented!()`, which can break compatibility (i.e. old version tikv
            // running on data written by new version tikv), but PD will reject old version
            // tikv join the cluster, so this should not happen.
            let res = match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_raft_entry_normal(apply_ctx, &entry),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_raft_entry_conf_change(apply_ctx, &entry)
                }
            };

            match res {
                ApplyResult::None => {}
                ApplyResult::Res(res) => {
                    results.push_back(res);
                    if self.wait_data {
                        apply_ctx.committed_count -= committed_entries_drainer.len();
                        break;
                    }
                }
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
                        heap_size: None,
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

    fn update_metrics(&mut self, apply_ctx: &ApplyContext<EK>) {
        self.metrics.written_bytes += apply_ctx.delta_bytes();
        self.metrics.written_keys += apply_ctx.delta_keys();
    }

    fn write_apply_state(&self, wb: &mut EK::WriteBatch) {
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

    fn maybe_write_apply_state(&self, apply_ctx: &mut ApplyContext<EK>) {
        let can_write = apply_ctx.host.pre_write_apply_state(&self.region);
        if can_write {
            self.write_apply_state(apply_ctx.kv_wb_mut());
        }
    }

    fn handle_raft_entry_normal(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        entry: &Entry,
    ) -> ApplyResult<EK::Snapshot> {
        fail_point!(
            "yield_apply_first_region",
            self.region.get_start_key().is_empty() && !self.region.get_end_key().is_empty(),
            |_| ApplyResult::Yield
        );

        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if !data.is_empty() {
            if !self.peer.is_witness || !can_witness_skip(entry) {
                let cmd = match util::parse_raft_cmd_request(data, index, term, &self.tag) {
                    util::RaftCmd::V1(cmd) => cmd,
                    util::RaftCmd::V2(simple_write_decoder) => {
                        if !apply_ctx.enable_v2_compatible_learner {
                            panic!(
                                "{} can not handle v2 command when enable_v2_compatible_learner is false",
                                self.tag
                            );
                        }
                        simple_write_decoder.to_raft_cmd_request()
                    }
                };
                if apply_ctx.yield_high_latency_operation && has_high_latency_operation(&cmd) {
                    self.priority = Priority::Low;
                }
                if self.has_pending_ssts {
                    // we are in low priority handler and to avoid overlapped ssts with same region
                    // just return Yield
                    return ApplyResult::Yield;
                }
                let mut has_unflushed_data =
                    self.last_flush_applied_index != self.apply_state.get_applied_index();
                if (has_unflushed_data
                    && should_write_to_engine(!apply_ctx.kv_wb().is_empty(), &cmd)
                    || apply_ctx.kv_wb().should_write_to_engine())
                    && apply_ctx.host.pre_persist(&self.region, false, Some(&cmd))
                {
                    apply_ctx.commit(self);
                    if self.metrics.written_bytes >= apply_ctx.yield_msg_size
                        || self
                            .handle_start
                            .as_ref()
                            .map_or(Duration::ZERO, Instant::saturating_elapsed)
                            >= apply_ctx.yield_duration
                    {
                        return ApplyResult::Yield;
                    }
                    has_unflushed_data = false;
                }
                if self.priority != apply_ctx.priority {
                    if has_unflushed_data {
                        apply_ctx.commit(self);
                    }
                    return ApplyResult::Yield;
                }

                return self.process_raft_cmd(apply_ctx, index, term, cmd);
            }
        } else {
            // we should observe empty cmd, aka leader change,
            // read index during confchange, or other situations.
            apply_ctx.host.on_empty_cmd(&self.region, index, term);

            // 1. When a peer become leader, it will send an empty entry.
            // 2. When a leader tries to read index during transferring leader, it will also
            //    propose an empty entry. But that entry will not contain any associated
            //    callback. So no need to clear callback.
            while let Some(mut cmd) = self.pending_cmds.pop_normal(u64::MAX, term - 1) {
                if let Some(cb) = cmd.cb.take() {
                    apply_ctx
                        .applied_batch
                        .push_cb(cb, cmd_resp::err_resp(Error::StaleCommand, term));
                }
            }
        }

        self.apply_state.set_applied_index(index);
        self.applied_term = term;
        assert!(term > 0);

        ApplyResult::None
    }

    fn handle_raft_entry_conf_change(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
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

    fn process_raft_cmd(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        index: u64,
        term: u64,
        req: RaftCmdRequest,
    ) -> ApplyResult<EK::Snapshot> {
        if index == 0 {
            panic!(
                "{} processing raft command needs a none zero index",
                self.tag
            );
        }

        // Set sync log hint if the cmd requires so.
        apply_ctx.sync_log_hint |= should_sync_log(&req);

        apply_ctx.host.pre_apply(&self.region, &req);
        let (mut cmd, exec_result, should_write) = self.apply_raft_cmd(apply_ctx, index, term, req);
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
        cmd_resp::bind_term(&mut cmd.response, self.term);
        let cmd_cb = self.find_pending(index, term, is_conf_change_cmd(&cmd.request));
        apply_ctx
            .applied_batch
            .push(cmd_cb, cmd, &self.observe_info, self.region_id());
        if should_write {
            // An observer shall prevent a write_apply_state here by not return true
            // when `post_exec`.
            self.write_apply_state(apply_ctx.kv_wb_mut());
            apply_ctx.commit(self);
        }
        exec_result
    }

    /// Applies raft command.
    ///
    /// An apply operation can fail in the following situations:
    ///   - it encounters an error that will occur on all stores, it can
    /// continue applying next entry safely, like epoch not match for
    /// example;
    ///   - it encounters an error that may not occur on all stores, in this
    ///     case we should try to apply the entry again or panic. Considering
    ///     that this usually due to disk operation fail, which is rare, so just
    ///     panic is ok.
    fn apply_raft_cmd(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        index: u64,
        term: u64,
        req: RaftCmdRequest,
    ) -> (Cmd, ApplyResult<EK::Snapshot>, bool) {
        // if pending remove, apply should be aborted already.
        assert!(!self.pending_remove);

        // Remember if the raft cmd fails to be applied, it must have no side effects.
        // E.g. `RaftApplyState` must not be changed.

        let mut origin_epoch = None;
        let (resp, exec_result) = if ctx.host.pre_exec(&self.region, &req, index, term) {
            // One of the observers want to filter execution of the command.
            let mut resp = RaftCmdResponse::default();
            if !req.get_header().get_uuid().is_empty() {
                let uuid = req.get_header().get_uuid().to_vec();
                resp.mut_header().set_uuid(uuid);
            }
            (resp, ApplyResult::None)
        } else {
            ctx.exec_log_index = index;
            ctx.exec_log_term = term;
            ctx.kv_wb_mut().set_save_point();
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
                        Error::FlashbackInProgress(..) => debug!(
                            "flashback is in process";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "err" => ?e
                        ),
                        Error::FlashbackNotPrepared(..) => debug!(
                            "flashback is not prepared";
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
            (resp, exec_result)
        };

        let cmd = Cmd::new(index, term, req, resp);
        if let ApplyResult::WaitMergeSource(_) = exec_result {
            return (cmd, exec_result, false);
        }

        self.apply_state.set_applied_index(index);
        self.applied_term = term;

        let (modified_region, mut pending_handle_ssts) = match exec_result {
            ApplyResult::Res(ref e) => match e {
                ExecResult::SplitRegion { ref derived, .. } => (Some(derived.clone()), None),
                ExecResult::PrepareMerge { ref region, .. } => (Some(region.clone()), None),
                ExecResult::CommitMerge { ref region, .. } => (Some(region.clone()), None),
                ExecResult::RollbackMerge { ref region, .. } => (Some(region.clone()), None),
                ExecResult::IngestSst { ref ssts } => (None, Some(ssts.clone())),
                ExecResult::Flashback { ref region } => (Some(region.clone()), None),
                _ => (None, None),
            },
            _ => (None, None),
        };
        let mut apply_ctx_info = ApplyCtxInfo {
            pending_handle_ssts: &mut pending_handle_ssts,
            delete_ssts: &mut ctx.delete_ssts,
            pending_delete_ssts: &mut ctx.pending_delete_ssts,
        };
        let should_write = ctx.host.post_exec(
            &self.region,
            &cmd,
            &self.apply_state,
            &RegionState {
                peer_id: self.id(),
                pending_remove: self.pending_remove,
                modified_region,
            },
            &mut apply_ctx_info,
        );
        match pending_handle_ssts {
            None => (),
            Some(mut v) => {
                if !v.is_empty() {
                    // All elements in `pending_handle_ssts` should be moved into either
                    // `delete_ssts` or `pending_delete_ssts`, once handled by by any of the
                    // `post_exec` observers. So a non-empty
                    // `pending_handle_ssts` here indicates no `post_exec` handled.
                    ctx.delete_ssts.append(&mut v);
                }
                RAFT_APPLYING_SST_GAUGE
                    .with_label_values(&["pending_delete"])
                    .set(ctx.pending_delete_ssts.len() as i64);
            }
        }

        if let ApplyResult::Res(ref exec_result) = exec_result {
            match *exec_result {
                ExecResult::ChangePeer(ref cp) => {
                    self.region = cp.region.clone();
                    if let Some(p) = find_peer_by_id(&self.region, self.id()) {
                        self.peer = p.clone();
                    }
                }
                ExecResult::ComputeHash { .. }
                | ExecResult::VerifyHash { .. }
                | ExecResult::CompactLog { .. }
                | ExecResult::DeleteRange { .. }
                | ExecResult::IngestSst { .. }
                | ExecResult::TransferLeader { .. }
                | ExecResult::HasPendingCompactCmd(..) => {}
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
                ExecResult::Flashback { ref region } => {
                    self.region = region.clone();
                }
                ExecResult::BatchSwitchWitness(ref switches) => {
                    self.region = switches.region.clone();
                    if let Some(p) = find_peer_by_id(&self.region, self.id()) {
                        self.peer = p.clone();
                    }
                }
            }
        }
        if let Some(epoch) = origin_epoch {
            let cmd_type = cmd.request.get_admin_request().get_cmd_type();
            let epoch_state = admin_cmd_epoch_lookup(cmd_type);
            // The change-epoch behavior **MUST BE** equal to the settings in
            // `admin_cmd_epoch_lookup`
            if (epoch_state.change_ver
                && epoch.get_version() == self.region.get_region_epoch().get_version())
                || (epoch_state.change_conf_ver
                    && epoch.get_conf_ver() == self.region.get_region_epoch().get_conf_ver())
            {
                panic!(
                    "{} apply admin cmd {:?} but epoch change is not expected, epoch state {:?}, before {:?}, after {:?}",
                    self.tag,
                    cmd.request,
                    epoch_state,
                    epoch,
                    self.region.get_region_epoch()
                );
            }
        }

        (cmd, exec_result, should_write)
    }

    fn destroy(&mut self, apply_ctx: &mut ApplyContext<EK>) {
        self.stopped = true;
        apply_ctx.router.close(self.region_id());
        let id = self.id();
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region.get_id(), id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region.get_id(), id, cmd);
        }
        for cmd in self.pending_cmds.compacts.drain(..) {
            notify_region_removed(self.region.get_id(), id, cmd);
        }
        self.yield_state = None;

        let mut event = TraceEvent::default();
        if let Some(e) = self.trace.reset(ApplyMemoryTrace::default()) {
            event = event + e;
        }
        MEMTRACE_APPLYS.trace(event);
    }

    fn clear_all_commands_as_stale(&mut self) {
        let (region_id, peer_id) = (self.region_id(), self.id());
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_command(region_id, peer_id, self.term, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_command(region_id, peer_id, self.term, cmd);
        }
        for cmd in self.pending_cmds.compacts.drain(..) {
            notify_region_removed(self.region.get_id(), peer_id, cmd);
        }
    }

    fn clear_all_commands_silently(&mut self) {
        for mut cmd in self.pending_cmds.normals.drain(..) {
            cmd.cb.take();
        }
        if let Some(mut cmd) = self.pending_cmds.conf_change.take() {
            cmd.cb.take();
        }
        for mut cmd in self.pending_cmds.compacts.drain(..) {
            cmd.cb.take();
        }
    }
}

impl<EK> ApplyDelegate<EK>
where
    EK: KvEngine,
{
    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult<EK::Snapshot>)> {
        // Include region for epoch not match after merge may cause key not in range.
        let include_region =
            req.get_header().get_region_epoch().get_version() >= self.last_merge_version;
        check_req_region_epoch(req, &self.region, include_region)?;
        let header = req.get_header();
        let admin_type = req.admin_request.as_ref().map(|req| req.get_cmd_type());
        check_flashback_state(
            self.region.is_in_flashback,
            self.region.flashback_start_ts,
            header,
            admin_type,
            self.region_id(),
            false,
        )?;
        if req.has_admin_request() {
            self.exec_admin_cmd(ctx, req)
        } else {
            self.exec_write_cmd(ctx, req)
        }
    }

    fn exec_admin_cmd(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult<EK::Snapshot>)> {
        let request = req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        if cmd_type != AdminCmdType::CompactLog && cmd_type != AdminCmdType::CommitMerge {
            info!(
                "execute admin command";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "term" => ctx.exec_log_term,
                "index" => ctx.exec_log_index,
                "command" => ?request,
            );
        }

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::ChangePeerV2 => self.exec_change_peer_v2(ctx, request),
            AdminCmdType::Split => self.exec_split(ctx, request),
            AdminCmdType::BatchSplit => self.exec_batch_split(ctx, request),
            AdminCmdType::CompactLog => self.exec_compact_log(request),
            AdminCmdType::TransferLeader => self.exec_transfer_leader(request, ctx.exec_log_term),
            AdminCmdType::ComputeHash => self.exec_compute_hash(ctx, request),
            AdminCmdType::VerifyHash => self.exec_verify_hash(ctx, request),
            AdminCmdType::PrepareMerge => self.exec_prepare_merge(ctx, request),
            AdminCmdType::CommitMerge => self.exec_commit_merge(ctx, request),
            AdminCmdType::RollbackMerge => self.exec_rollback_merge(ctx, request),
            AdminCmdType::PrepareFlashback | AdminCmdType::FinishFlashback => {
                self.exec_flashback(ctx, request)
            }
            AdminCmdType::UpdateGcPeer => Err(box_err!("v2 only command and it's safe to skip")),
            AdminCmdType::BatchSwitchWitness => self.exec_batch_switch_witness(ctx, request),
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

    fn exec_write_cmd(
        &mut self,
        ctx: &mut ApplyContext<EK>,
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

        let mut ranges = vec![];
        let mut ssts = vec![];
        for req in requests {
            let cmd_type = req.get_cmd_type();
            match cmd_type {
                CmdType::Put => self.handle_put(ctx, req),
                CmdType::Delete => self.handle_delete(ctx, req),
                CmdType::DeleteRange => {
                    self.handle_delete_range(&ctx.engine, req, &mut ranges, ctx.use_delete_range)
                }
                CmdType::IngestSst => self.handle_ingest_sst(ctx, req, &mut ssts),
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
        }

        let mut resp = RaftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }

        assert!(ranges.is_empty() || ssts.is_empty());
        let exec_res = if !ranges.is_empty() {
            ApplyResult::Res(ExecResult::DeleteRange { ranges })
        } else if !ssts.is_empty() {
            #[cfg(feature = "failpoints")]
            {
                let mut dont_delete_ingested_sst_fp = || {
                    fail_point!("dont_delete_ingested_sst", |_| {
                        ssts.clear();
                    });
                };
                dont_delete_ingested_sst_fp();
            }
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
    fn handle_put(&mut self, ctx: &mut ApplyContext<EK>, req: &Request) -> Result<()> {
        fail::fail_point!("on_handle_put");
        PEER_WRITE_CMD_COUNTER.put.inc();
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, &self.region)?;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(key, value.len() as u64);
        }

        keys::data_key_with_buffer(key, &mut ctx.key_buffer);
        let key = ctx.key_buffer.as_slice();

        self.metrics.size_diff_hint += key.len() as i64;
        self.metrics.size_diff_hint += value.len() as i64;
        ctx.key_size.observe(key.len() as f64);
        ctx.value_size.observe(value.len() as f64);
        if !req.get_put().get_cf().is_empty() {
            let cf = req.get_put().get_cf();
            // TODO: don't allow write preseved cfs.
            if cf == CF_LOCK {
                self.metrics.lock_cf_written_bytes += key.len() as u64;
                self.metrics.lock_cf_written_bytes += value.len() as u64;
            }
            // TODO: check whether cf exists or not.
            ctx.kv_wb.put_cf(cf, key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}) to cf {}: {:?}",
                    self.tag,
                    log_wrappers::Value::key(key),
                    log_wrappers::Value::value(value),
                    cf,
                    e
                )
            });
        } else {
            ctx.kv_wb.put(key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}): {:?}",
                    self.tag,
                    log_wrappers::Value::key(key),
                    log_wrappers::Value::value(value),
                    e
                );
            });
        }
        Ok(())
    }

    fn handle_delete(&mut self, ctx: &mut ApplyContext<EK>, req: &Request) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.delete.inc();
        let key = req.get_delete().get_key();
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, &self.region)?;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(key, 0);
        }

        keys::data_key_with_buffer(key, &mut ctx.key_buffer);
        let key = ctx.key_buffer.as_slice();

        // since size_diff_hint is not accurate, so we just skip calculate the value
        // size.
        self.metrics.size_diff_hint -= key.len() as i64;
        if !req.get_delete().get_cf().is_empty() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            ctx.kv_wb.delete_cf(cf, key).unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete {}: {}",
                    self.tag,
                    log_wrappers::Value::key(key),
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
            ctx.kv_wb.delete(key).unwrap_or_else(|e| {
                panic!(
                    "{} failed to delete {}: {}",
                    self.tag,
                    log_wrappers::Value::key(key),
                    e
                )
            });
            self.metrics.delete_keys_hint += 1;
        }

        Ok(())
    }

    fn handle_delete_range(
        &mut self,
        engine: &EK,
        req: &Request,
        ranges: &mut Vec<Range>,
        use_delete_range: bool,
    ) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.delete_range.inc();
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

        let mut cf = req.get_delete_range().get_cf();
        if cf.is_empty() {
            cf = CF_DEFAULT;
        }
        if !ALL_CFS.iter().any(|x| *x == cf) {
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
            let wopts = WriteOptions::default();
            engine
                .delete_ranges_cf(&wopts, cf, DeleteStrategy::DeleteFiles, &range)
                .unwrap_or_else(|e| fail_f(e, DeleteStrategy::DeleteFiles));

            let strategy = if use_delete_range {
                DeleteStrategy::DeleteByRange
            } else {
                DeleteStrategy::DeleteByKey
            };
            // Delete all remaining keys.
            engine
                .delete_ranges_cf(&wopts, cf, strategy.clone(), &range)
                .unwrap_or_else(move |e| fail_f(e, strategy));
            engine
                .delete_ranges_cf(&wopts, cf, DeleteStrategy::DeleteBlobs, &range)
                .unwrap_or_else(move |e| fail_f(e, DeleteStrategy::DeleteBlobs));
        }

        // TODO: Should this be executed when `notify_only` is set?
        ranges.push(Range::new(cf.to_owned(), start_key, end_key));

        Ok(())
    }

    fn handle_ingest_sst(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &Request,
        ssts: &mut Vec<SstMetaInfo>,
    ) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.ingest_sst.inc();
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
            let _ = ctx.importer.delete(sst);
            return Err(e);
        }

        match ctx.importer.validate(sst) {
            Ok(meta_info) => {
                ctx.pending_ssts.push(meta_info.clone());
                self.has_pending_ssts = true;
                ssts.push(meta_info)
            }
            Err(e) => {
                // If this failed, it means that the file is corrupted or something
                // is wrong with the engine, but we can do nothing about that.
                panic!("{} ingest {:?}: {:?}", self.tag, sst, e);
            }
        };
        Ok(())
    }
}

mod confchange_cmd_metric {
    use super::*;

    pub fn inc_all(cct: ConfChangeType) {
        let metrics = match cct {
            ConfChangeType::AddNode => &PEER_ADMIN_CMD_COUNTER.add_peer,
            ConfChangeType::RemoveNode => &PEER_ADMIN_CMD_COUNTER.remove_peer,
            ConfChangeType::AddLearnerNode => &PEER_ADMIN_CMD_COUNTER.add_learner,
        };
        metrics.all.inc();
    }

    pub fn inc_success(cct: ConfChangeType) {
        let metrics = match cct {
            ConfChangeType::AddNode => &PEER_ADMIN_CMD_COUNTER.add_peer,
            ConfChangeType::RemoveNode => &PEER_ADMIN_CMD_COUNTER.remove_peer,
            ConfChangeType::AddLearnerNode => &PEER_ADMIN_CMD_COUNTER.add_learner,
        };
        metrics.success.inc();
    }
}

pub fn validate_batch_split(req: &AdminRequest, region: &Region) -> Result<()> {
    if req.get_splits().get_requests().is_empty() {
        return Err(box_err!("missing split requests"));
    }

    let split_reqs: &[SplitRequest] = req.get_splits().get_requests();
    let mut last_key = region.get_start_key();
    for req in split_reqs {
        let split_key = req.get_split_key();
        if split_key.is_empty() {
            return Err(box_err!("missing split key"));
        }

        if split_key <= last_key {
            return Err(box_err!("invalid split request: {:?}", split_reqs));
        }

        if req.get_new_peer_ids().len() != region.get_peers().len() {
            return Err(box_err!(
                "invalid new peer id count, need {:?}, but got {:?}",
                region.get_peers(),
                req.get_new_peer_ids()
            ));
        }

        last_key = req.get_split_key();
    }

    util::check_key_in_region_exclusive(last_key, region)?;

    Ok(())
}

// Admin commands related.
impl<EK> ApplyDelegate<EK>
where
    EK: KvEngine,
{
    // Legacy code for compatibility. All new conf changes are dispatched by
    // ChangePeerV2 now.
    fn exec_change_peer(
        &mut self,
        ctx: &mut ApplyContext<EK>,
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
            (self.id() == 1 || self.id() == 3) && self.region_id() == 1,
            |_| panic!("should not use return")
        );
        fail_point!(
            "apply_on_conf_change_3_1",
            self.id() == 3 && self.region_id() == 1,
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
                let add_node_fp = || {
                    fail_point!(
                        "apply_on_add_node_1_2",
                        self.id() == 2 && self.region_id() == 1,
                        |_| {}
                    )
                };
                add_node_fp();

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_peer", "all"])
                    .inc();

                let mut exists = false;
                if let Some(p) = find_peer_mut(&mut region, store_id) {
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

                if let Some(p) = remove_peer(&mut region, store_id) {
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
                    if self.id() == peer.get_id() {
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

                if find_peer(&region, store_id).is_some() {
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
                index: ctx.exec_log_index,
                conf_change: Default::default(),
                changes: vec![request.clone()],
                region,
            })),
        ))
    }

    fn exec_change_peer_v2(
        &mut self,
        ctx: &mut ApplyContext<EK>,
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
                index: ctx.exec_log_index,
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

            if let Some(exist_peer) = find_peer(&region, store_id) {
                let r = exist_peer.get_role();
                if r == PeerRole::IncomingVoter || r == PeerRole::DemotingVoter {
                    panic!(
                        "{} can't apply confchange because configuration is still in joint state, confchange: {:?}, region: {:?}",
                        self.tag, cp, self.region
                    );
                }
            }
            match (find_peer_mut(&mut region, store_id), change_type) {
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
                            || exist_peer.get_is_witness() != peer.get_is_witness()
                    {
                        error!(
                            "can't add duplicated peer";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "peer" => ?peer,
                            "exist_peer" => ?exist_peer,
                            "confchange_type" => ?change_type,
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
                    match remove_peer(&mut region, store_id) {
                        Some(p) => {
                            if &p != peer {
                                error!(
                                    "ignore remove unmatched peer";
                                    "region_id" => self.region_id(),
                                    "peer_id" => self.id(),
                                    "expect_peer" => ?peer,
                                    "get_peer" => ?p
                                );
                                return Err(box_err!(
                                    "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                                    peer,
                                    p
                                ));
                            }
                            if self.id() == peer.get_id() {
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
            "original_region" => ?&self.region,
            "current_region" => ?&region,
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

    fn exec_split(
        &mut self,
        ctx: &mut ApplyContext<EK>,
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
        admin_req
            .mut_split()
            .set_share_source_region_size(split.get_share_source_region_size());
        admin_req.mut_splits().mut_requests().push(split);
        // This method is executed only when there are unapplied entries after being
        // restarted. So there will be no callback, it's OK to return a response
        // that does not matched with its request.
        self.exec_batch_split(ctx, &admin_req)
    }

    fn exec_batch_split(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_split");
        fail_point!(
            "apply_before_split_1_3",
            self.id() == 3 && self.region_id() == 1,
            |_| { unreachable!() }
        );

        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

        let mut derived = self.region.clone();
        validate_batch_split(req, &derived)?;

        let split_reqs = req.get_splits();
        let mut keys: VecDeque<_> = split_reqs
            .get_requests()
            .iter()
            .map(|req| req.get_split_key().to_vec())
            .collect();

        info!(
            "split region";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "region" => ?derived,
            "keys" => %KeysInfoFormatter(keys.iter()),
        );

        let new_region_cnt = split_reqs.get_requests().len();
        let new_version = derived.get_region_epoch().get_version() + new_region_cnt as u64;
        derived.mut_region_epoch().set_version(new_version);

        let right_derive = split_reqs.get_right_derive();
        let share_source_region_size = split_reqs.get_share_source_region_size();
        let mut regions = Vec::with_capacity(new_region_cnt + 1);
        // Note that the split requests only contain ids for new regions, so we need
        // to handle new regions and old region separately.
        if right_derive {
            // So the range of new regions is [old_start_key, split_key1, ...,
            // last_split_key].
            keys.push_front(derived.get_start_key().to_vec());
        } else {
            // So the range of new regions is [split_key1, ..., last_split_key,
            // old_end_key].
            keys.push_back(derived.get_end_key().to_vec());
            derived.set_end_key(keys.front().unwrap().to_vec());
            regions.push(derived.clone());
        }

        // Init split regions' meta info
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
                    peer_id: find_peer(&new_region, ctx.store_id).unwrap().get_id(),
                    result: None,
                },
            );
            regions.push(new_region);
        }

        if right_derive {
            derived.set_start_key(keys.pop_front().unwrap());
            regions.push(derived.clone());
        }

        // Generally, a peer is created in pending_create_peers when it is
        // created by raft_message (or by split here) and removed from
        // pending_create_peers when it has applied the snapshot. So, if the
        // peer of the split region is already created by raft_message in
        // pending_create_peers ,we decide to replace it.
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

        fail_point!(
            "on_handle_apply_split_2_after_mem_check",
            self.id() == 2,
            |_| unimplemented!()
        );

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
                        // It's marked replaced, then further destroy will skip cleanup, so there
                        // should be no region local state.
                        panic!(
                            "{} failed to replace region {} peer {} because state {:?} alread exist in kv engine",
                            self.tag, region_id, new_split_peer.peer_id, state
                        )
                    }
                    // If the peer's state is already persisted, add some info in
                    // new_split_peer.result so that we will skip this region in later
                    // executions.
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
            self.id() == 3 && self.region_id() == 1,
            |_| { unreachable!() }
        );

        Ok((
            resp,
            ApplyResult::Res(ExecResult::SplitRegion {
                regions,
                derived,
                new_split_regions,
                share_source_region_size,
            }),
        ))
    }

    fn exec_prepare_merge(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!("apply_before_prepare_merge");
        fail_point!(
            "apply_before_prepare_merge_2_3",
            ctx.store_id == 2 || ctx.store_id == 3,
            |_| { unreachable!() }
        );

        PEER_ADMIN_CMD_COUNTER.prepare_merge.all.inc();

        let prepare_merge = req.get_prepare_merge();
        let index = prepare_merge.get_min_index();
        let first_index = entry_storage::first_index(&self.apply_state);
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
        merging_state.set_commit(ctx.exec_log_index);
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
    // - `exec_commit_merge` in target apply fsm and send `CatchUpLogs` to source
    //   peer fsm
    // - `on_catch_up_logs_for_merge` in source peer fsm
    // - if the source peer has already executed the corresponding
    //   `on_ready_prepare_merge`, set pending_remove and jump to step 6
    // - ... (raft append and apply logs)
    // - `on_ready_prepare_merge` in source peer fsm and set pending_remove (means
    //   source region has finished applying all logs)
    // - `logs_up_to_date_for_merge` in source apply fsm (destroy its apply fsm and
    //   send Noop to trigger the target apply fsm)
    // - resume `exec_commit_merge` in target apply fsm
    // - `on_ready_commit_merge` in target peer fsm and send `MergeResult` to source
    //   peer fsm
    // - `on_merge_result` in source peer fsm (destroy itself)
    fn exec_commit_merge(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        {
            fail_point!("apply_before_commit_merge");
            let apply_before_commit_merge = || {
                fail_point!(
                    "apply_before_commit_merge_except_1_4",
                    self.region_id() == 1 && self.id() != 4,
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
            "term" => ctx.exec_log_term,
            "index" => ctx.exec_log_index,
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
                index: ctx.exec_log_index,
                region,
                source: source_region.to_owned(),
            }),
        ))
    }

    fn exec_rollback_merge(
        &mut self,
        ctx: &mut ApplyContext<EK>,
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

    fn exec_flashback(
        &self,
        ctx: &mut ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        // Modify flashback fields in region state.
        let mut region = self.region.clone();
        match req.get_cmd_type() {
            AdminCmdType::PrepareFlashback => {
                PEER_ADMIN_CMD_COUNTER.prepare_flashback.success.inc();
                // First time enter into the flashback state, inc the counter.
                if !region.is_in_flashback {
                    PEER_IN_FLASHBACK_STATE.inc()
                }

                region.set_is_in_flashback(true);
                region.set_flashback_start_ts(req.get_prepare_flashback().get_start_ts());
            }
            AdminCmdType::FinishFlashback => {
                PEER_ADMIN_CMD_COUNTER.finish_flashback.success.inc();
                // Leave the flashback state, dec the counter.
                if region.is_in_flashback {
                    PEER_IN_FLASHBACK_STATE.dec()
                }

                region.set_is_in_flashback(false);
                region.clear_flashback_start_ts();
            }
            _ => unreachable!(),
        }

        // Modify the `RegionLocalState` persisted in disk.
        write_peer_state(ctx.kv_wb_mut(), &region, PeerState::Normal, None).unwrap_or_else(|e| {
            panic!(
                "{} failed to change the flashback state to {:?} for region {:?}: {:?}",
                self.tag,
                req.get_cmd_type(),
                region,
                e
            )
        });
        Ok((
            AdminResponse::default(),
            ApplyResult::Res(ExecResult::Flashback { region }),
        ))
    }

    // When the first return value is true, it means that we have updated
    // `RaftApplyState`, and the caller needs to do persistence.
    fn try_compact_log(
        &mut self,
        voter_replicated_index: u64,
        voter_replicated_term: u64,
    ) -> Result<(bool, Option<ExecResult<EK::Snapshot>>)> {
        PEER_ADMIN_CMD_COUNTER.compact.all.inc();
        let first_index = entry_storage::first_index(&self.apply_state);

        if self.is_merging {
            info!(
                "in merging mode, skip compact";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "voter_replicated_index" => voter_replicated_index,
            );
            return Ok((false, None));
        }

        // When the witness restarted, the pending compact cmd has been lost, so use
        // `voter_replicated_index` for gc to avoid log accumulation.
        if !self.pending_cmds.has_compact() {
            if voter_replicated_index <= first_index {
                debug!(
                    "voter_replicated_index <= first index, no need to compact";
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                    "compact_index" => voter_replicated_index,
                    "first_index" => first_index,
                );
                return Ok((false, Some(ExecResult::HasPendingCompactCmd(false))));
            }
            // compact failure is safe to be omitted, no need to assert.
            compact_raft_log(
                &self.tag,
                &mut self.apply_state,
                voter_replicated_index,
                voter_replicated_term,
            )?;
            PEER_ADMIN_CMD_COUNTER.compact.success.inc();
            return Ok((true, Some(ExecResult::HasPendingCompactCmd(false))));
        }

        match self.pending_cmds.pop_compact(voter_replicated_index) {
            Some(cmd) => {
                // compact failure is safe to be omitted, no need to assert.
                compact_raft_log(&self.tag, &mut self.apply_state, cmd.index, cmd.term)?;
                PEER_ADMIN_CMD_COUNTER.compact.success.inc();
                Ok((
                    true,
                    Some(ExecResult::CompactLog {
                        state: self.apply_state.get_truncated_state().clone(),
                        first_index,
                        has_pending: self.pending_cmds.has_compact(),
                    }),
                ))
            }
            None => {
                info!(
                    "latest voter_replicated_index < compact_index, skip";
                    "region_id" => self.region_id(),
                    "peer_id" => self.id(),
                    "voter_replicated_index" => voter_replicated_index,
                );
                Ok((false, None))
            }
        }
    }

    fn exec_compact_log(
        &mut self,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.compact.all.inc();

        let mut compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::default();
        let first_index = entry_storage::first_index(&self.apply_state);
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

        let mut compact_term = req.get_compact_log().get_compact_term();
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

        let voter_replicated_index = req.get_compact_log().get_voter_replicated_index();
        // If there is any voter lagging behind, the log truncation of the witness
        // shouldn't be triggered even if it's force mode(raft log size/count exceeds
        // the threshold or raft engine purge), otherwise the witness can't help the
        // lagging voter catch up logs when leader is down. In this situation Compact
        // index should be queued. If witness receives a voter_replicated_index
        // that is larger than the pending compact index, logs can be deleted.
        if self.peer.is_witness {
            if voter_replicated_index < compact_index {
                self.pending_cmds.push_compact(PendingCmd::new(
                    compact_index,
                    compact_term,
                    Callback::None,
                ));
                match self.pending_cmds.pop_compact(voter_replicated_index) {
                    Some(cmd) => {
                        compact_index = cmd.index;
                        compact_term = cmd.term;
                    }
                    None => {
                        info!(
                            "voter_replicated_index < compact_index, skip";
                            "region_id" => self.region_id(),
                            "peer_id" => self.id(),
                            "command" => ?req.get_compact_log()
                        );
                        return Ok((
                            resp,
                            ApplyResult::Res(ExecResult::HasPendingCompactCmd(true)),
                        ));
                    }
                }
            } else {
                for mut cmd in self.pending_cmds.compacts.drain(..) {
                    cmd.cb.take().unwrap();
                }
            }
        }
        // compact failure is safe to be omitted, no need to assert.
        compact_raft_log(
            &self.tag,
            &mut self.apply_state,
            compact_index,
            compact_term,
        )?;

        PEER_ADMIN_CMD_COUNTER.compact.success.inc();

        Ok((
            resp,
            ApplyResult::Res(ExecResult::CompactLog {
                state: self.apply_state.get_truncated_state().clone(),
                first_index,
                has_pending: self.pending_cmds.has_compact(),
            }),
        ))
    }

    fn exec_transfer_leader(
        &mut self,
        req: &AdminRequest,
        term: u64,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.transfer_leader.all.inc();
        let resp = AdminResponse::default();

        let peer = req.get_transfer_leader().get_peer();
        // Only execute TransferLeader if the expected new leader is self.
        if peer.get_id() == self.id() {
            Ok((resp, ApplyResult::Res(ExecResult::TransferLeader { term })))
        } else {
            Ok((resp, ApplyResult::None))
        }
    }

    fn exec_compute_hash(
        &self,
        ctx: &ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let resp = AdminResponse::default();
        Ok((
            resp,
            if self.peer.is_witness {
                ApplyResult::None
            } else {
                ApplyResult::Res(ExecResult::ComputeHash {
                    region: self.region.clone(),
                    index: ctx.exec_log_index,
                    context: req.get_compute_hash().get_context().to_vec(),
                    // This snapshot may be held for a long time, which may cause too many
                    // open files in rocksdb.
                    // TODO: figure out another way to do consistency check without snapshot
                    // or short life snapshot.
                    snap: ctx.engine.snapshot(None),
                })
            },
        ))
    }

    fn exec_verify_hash(
        &self,
        _: &ApplyContext<EK>,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        let resp = AdminResponse::default();
        if self.peer.is_witness {
            return Ok((resp, ApplyResult::None));
        }
        let verify_req = req.get_verify_hash();
        let index = verify_req.get_index();
        let context = verify_req.get_context().to_vec();
        let hash = verify_req.get_hash().to_vec();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::VerifyHash {
                index,
                context,
                hash,
            }),
        ))
    }

    fn exec_batch_switch_witness(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        fail_point!(
            "before_exec_batch_switch_witness",
            self.id() == 2,
            |_| unimplemented!()
        );
        assert!(request.has_switch_witnesses());
        let switches = request
            .get_switch_witnesses()
            .get_switch_witnesses()
            .to_vec();

        info!(
            "exec BatchSwitchWitness";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "epoch" => ?self.region.get_region_epoch(),
        );

        let mut region = self.region.clone();
        for s in switches.as_slice() {
            PEER_ADMIN_CMD_COUNTER.batch_switch_witness.all.inc();
            let (peer_id, is_witness) = (s.get_peer_id(), s.get_is_witness());
            let mut peer_is_exist = false;
            for p in region.mut_peers().iter_mut() {
                if p.id == peer_id {
                    if p.is_witness == is_witness {
                        return Err(box_err!(
                            "switch peer {:?} on region {:?} is no-op",
                            p,
                            self.region
                        ));
                    }
                    p.is_witness = is_witness;
                    peer_is_exist = true;
                    break;
                }
            }
            if !peer_is_exist {
                return Err(box_err!(
                    "switch peer {} on region {:?} failed: peer does not exist",
                    peer_id,
                    self.region
                ));
            }
            PEER_ADMIN_CMD_COUNTER.batch_switch_witness.success.inc();
            if self.id() == peer_id && !is_witness {
                self.wait_data = true;
                self.peer.is_witness = false;
            }
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + switches.len() as u64;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        info!(
            "switch witness successfully";
            "region_id" => self.region_id(),
            "peer_id" => self.id(),
            "switches" => ?switches,
            "original_region" => ?&self.region,
            "current_region" => ?&region,
        );

        let state = if self.pending_remove {
            PeerState::Tombstone
        } else if self.wait_data {
            PeerState::Unavailable
        } else {
            PeerState::Normal
        };

        if let Err(e) = write_peer_state(ctx.kv_wb_mut(), &region, state, None) {
            panic!("{} failed to update region state: {:?}", self.tag, e);
        }

        let resp = AdminResponse::default();
        Ok((
            resp,
            ApplyResult::Res(ExecResult::BatchSwitchWitness(SwitchWitness {
                index: ctx.exec_log_index,
                switches,
                region,
            })),
        ))
    }

    fn update_memory_trace(&mut self, event: &mut TraceEvent) {
        let pending_cmds = self.pending_cmds.approximate_heap_size();
        let merge_yield = if let Some(ref mut state) = self.yield_state {
            if state.heap_size.is_none() {
                state.heap_size = Some(state.approximate_heap_size());
            }
            state.heap_size.unwrap()
        } else {
            0
        };

        let task = ApplyMemoryTrace {
            pending_cmds,
            merge_yield,
        };
        if let Some(e) = self.trace.reset(task) {
            *event = *event + e;
        }
    }
}

pub fn is_conf_change_cmd(msg: &RaftCmdRequest) -> bool {
    if !msg.has_admin_request() {
        return false;
    }
    let req = msg.get_admin_request();
    req.has_change_peer() || req.has_change_peer_v2()
}

/// This function is used to check whether an sst is valid for ingestion.
///
/// The `sst` must have epoch and range matched with `region`.
pub fn check_sst_for_ingestion(sst: &SstMeta, region: &Region) -> Result<()> {
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

pub struct Apply<C> {
    pub peer_id: u64,
    pub region_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub entries: SmallVec<[CachedEntries; 1]>,
    pub entries_size: usize,
    pub cbs: Vec<Proposal<C>>,
    pub bucket_meta: Option<Arc<BucketMeta>>,
}

impl<C: WriteCallback> Apply<C> {
    pub(crate) fn new(
        peer_id: u64,
        region_id: u64,
        term: u64,
        commit_index: u64,
        commit_term: u64,
        entries: Vec<Entry>,
        cbs: Vec<Proposal<C>>,
        buckets: Option<Arc<BucketMeta>>,
    ) -> Apply<C> {
        let mut entries_size = 0;
        for e in &entries {
            entries_size += bytes_capacity(&e.data) + bytes_capacity(&e.context);
        }
        let cached_entries = CachedEntries::new(entries);
        Apply {
            peer_id,
            region_id,
            term,
            commit_index,
            commit_term,
            entries: smallvec![cached_entries],
            entries_size,
            cbs,
            bucket_meta: buckets,
        }
    }

    pub fn on_schedule(&mut self, metrics: &RaftMetrics) {
        let now = std::time::Instant::now();
        for cb in &mut self.cbs {
            for tracker in cb.cb.write_trackers_mut() {
                tracker.observe(now, &metrics.store_time, |t| {
                    t.metrics.write_instant = Some(now);
                    &mut t.metrics.store_time_nanos
                });
                tracker.reset(now);
            }
        }
    }

    fn try_batch(&mut self, other: &mut Apply<C>) -> bool {
        assert_eq!(self.region_id, other.region_id);
        assert_eq!(self.peer_id, other.peer_id);
        if self.entries_size + other.entries_size <= MAX_APPLY_BATCH_SIZE {
            if other.bucket_meta.is_some() {
                self.bucket_meta = other.bucket_meta.take();
            }

            assert!(other.term >= self.term);
            self.term = other.term;

            assert!(other.commit_index >= self.commit_index);
            self.commit_index = other.commit_index;
            assert!(other.commit_term >= self.commit_term);
            self.commit_term = other.commit_term;

            self.entries.append(&mut other.entries);
            self.entries_size += other.entries_size;

            self.cbs.append(&mut other.cbs);
            true
        } else {
            false
        }
    }
}

pub struct Registration {
    pub id: u64,
    pub term: u64,
    pub apply_state: RaftApplyState,
    pub applied_term: u64,
    pub region: Region,
    pub pending_request_snapshot_count: Arc<AtomicUsize>,
    pub is_merging: bool,
    raft_engine: Box<dyn RaftEngineReadOnly>,
}

impl Registration {
    pub fn new<EK: KvEngine, ER: RaftEngine>(peer: &Peer<EK, ER>) -> Registration {
        Registration {
            id: peer.peer_id(),
            term: peer.term(),
            apply_state: peer.get_store().apply_state().clone(),
            applied_term: peer.get_store().applied_term(),
            region: peer.region().clone(),
            pending_request_snapshot_count: peer.pending_request_snapshot_count.clone(),
            is_merging: peer.pending_merge_state.is_some(),
            raft_engine: Box::new(peer.get_store().engines.raft.clone()),
        }
    }
}

#[derive(Debug)]
pub struct Proposal<C> {
    pub is_conf_change: bool,
    pub index: u64,
    pub term: u64,
    pub cb: C,
    /// `propose_time` is set to the last time when a peer starts to renew
    /// lease.
    pub propose_time: Option<Timespec>,
    pub must_pass_epoch_check: bool,
    pub sent: bool,
}

impl<C> Proposal<C> {
    pub fn new(index: u64, term: u64, cb: C) -> Self {
        Self {
            index,
            term,
            cb,
            propose_time: None,
            must_pass_epoch_check: false,
            is_conf_change: false,
            sent: false,
        }
    }
}

impl<C> HeapSize for Proposal<C> {}

pub struct Destroy {
    region_id: u64,
    merge_from_snapshot: bool,
}

/// A message that asks the delegate to apply to the given logs and then reply
/// to target mailbox.
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
    // Fill it after the RocksDB snapshot is taken.
    pub index: Arc<AtomicU64>,
    // Fetch it to cancel the task if necessary.
    pub canceled: Arc<AtomicBool>,

    snap_notifier: SyncSender<RaftSnapshot>,
    // indicates whether the snapshot is triggered due to load balance
    for_balance: bool,
    // the store id the snapshot will be sent to
    to_store_id: u64,
}

impl GenSnapTask {
    pub fn new(
        region_id: u64,
        index: Arc<AtomicU64>,
        canceled: Arc<AtomicBool>,
        snap_notifier: SyncSender<RaftSnapshot>,
        to_store_id: u64,
    ) -> GenSnapTask {
        GenSnapTask {
            region_id,
            index,
            canceled,
            snap_notifier,
            for_balance: false,
            to_store_id,
        }
    }

    pub fn set_for_balance(&mut self) {
        self.for_balance = true;
    }

    pub fn generate_and_schedule_snapshot<EK>(
        self,
        kv_snap: EK::Snapshot,
        last_applied_term: u64,
        last_applied_state: RaftApplyState,
        region_sched: &Scheduler<RegionTask<EK::Snapshot>>,
    ) -> Result<()>
    where
        EK: KvEngine,
    {
        self.index
            .store(last_applied_state.applied_index, Ordering::SeqCst);
        let snapshot = RegionTask::Gen {
            region_id: self.region_id,
            notifier: self.snap_notifier,
            for_balance: self.for_balance,
            last_applied_term,
            last_applied_state,
            canceled: self.canceled,
            // This snapshot may be held for a long time, which may cause too many
            // open files in rocksdb.
            kv_snap,
            to_store_id: self.to_store_id,
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

#[derive(Debug)]
pub enum ObserverType {
    Cdc(ObserveHandle),
    Rts(ObserveHandle),
    Pitr(ObserveHandle),
}

impl ObserverType {
    pub fn handle(&self) -> &ObserveHandle {
        match self {
            ObserverType::Cdc(h) => h,
            ObserverType::Rts(h) => h,
            ObserverType::Pitr(h) => h,
        }
    }
}

#[derive(Debug)]
pub struct ChangeObserver {
    pub ty: ObserverType,
    pub region_id: u64,
}

impl ChangeObserver {
    pub fn from_cdc(region_id: u64, id: ObserveHandle) -> Self {
        Self {
            ty: ObserverType::Cdc(id),
            region_id,
        }
    }

    pub fn from_rts(region_id: u64, id: ObserveHandle) -> Self {
        Self {
            ty: ObserverType::Rts(id),
            region_id,
        }
    }

    pub fn from_pitr(region_id: u64, id: ObserveHandle) -> Self {
        Self {
            ty: ObserverType::Pitr(id),
            region_id,
        }
    }
}

pub enum Msg<EK>
where
    EK: KvEngine,
{
    Apply {
        start: Instant,
        apply: Apply<Callback<EK::Snapshot>>,
    },
    Registration(Registration),
    LogsUpToDate(CatchUpLogs),
    Noop,
    Destroy(Destroy),
    Snapshot(GenSnapTask),
    Change {
        cmd: ChangeObserver,
        region_epoch: RegionEpoch,
        cb: Callback<EK::Snapshot>,
    },
    #[cfg(any(test, feature = "testexport"))]
    #[allow(clippy::type_complexity)]
    Validate(u64, Box<dyn FnOnce(*const u8) + Send>),
    Recover(u64),
    CheckCompact {
        region_id: u64,
        voter_replicated_index: u64,
        voter_replicated_term: u64,
    },
}

impl<EK: KvEngine> ResourceMetered for Msg<EK> {
    fn consume_resource(&self, resource_ctl: &Arc<ResourceController>) -> Option<String> {
        match self {
            Msg::Apply { apply, .. } => {
                let mut dominant_group = "".to_owned();
                let mut max_write_bytes = 0;
                for cached_entries in &apply.entries {
                    cached_entries.iter_entries(|entry| {
                        let header = util::get_entry_header(entry);
                        let group_name = header.get_resource_group_name().to_owned();
                        let write_bytes = entry.compute_size() as u64;
                        resource_ctl.consume(
                            group_name.as_bytes(),
                            ResourceConsumeType::IoBytes(write_bytes),
                        );
                        if write_bytes > max_write_bytes {
                            dominant_group = group_name;
                            max_write_bytes = write_bytes;
                        }
                    });
                }
                Some(dominant_group)
            }
            _ => None,
        }
    }
}

impl<EK> Msg<EK>
where
    EK: KvEngine,
{
    pub fn apply(apply: Apply<Callback<EK::Snapshot>>) -> Msg<EK> {
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
                cmd: ChangeObserver { region_id, .. },
                ..
            } => write!(f, "[region {}] change cmd", region_id),
            #[cfg(any(test, feature = "testexport"))]
            Msg::Validate(region_id, _) => write!(f, "[region {}] validate", region_id),
            Msg::Recover(region_id) => write!(f, "recover [region {}] apply", region_id),
            Msg::CheckCompact {
                region_id,
                voter_replicated_index,
                voter_replicated_term,
            } => {
                write!(
                    f,
                    "[region {}] check compact, voter_replicated_index: {}, voter_replicated_term: {}",
                    region_id, voter_replicated_index, voter_replicated_term
                )
            }
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
    pub applied_term: u64,
    pub exec_res: VecDeque<ExecResult<S>>,
    pub metrics: ApplyMetrics,
    pub bucket_stat: Option<BucketStat>,
    pub write_seqno: Vec<SequenceNumber>,
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

    /// Handles peer registration. When a peer is created, it will register an
    /// apply delegate.
    fn handle_registration(&mut self, reg: Registration) {
        info!(
            "re-register to apply delegates";
            "region_id" => self.delegate.region_id(),
            "peer_id" => self.delegate.id(),
            "term" => reg.term
        );
        assert_eq!(self.delegate.id(), reg.id);
        self.delegate.term = reg.term;
        self.delegate.clear_all_commands_as_stale();
        self.delegate = ApplyDelegate::from_registration(reg);
    }

    /// Handles apply tasks, and uses the apply delegate to handle the committed
    /// entries.
    fn handle_apply(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        mut apply: Apply<Callback<EK::Snapshot>>,
    ) {
        if apply_ctx.timer.is_none() {
            apply_ctx.timer = Some(Instant::now_coarse());
        }

        fail_point!("on_handle_apply_1003", self.delegate.id() == 1003, |_| {});
        fail_point!("on_handle_apply_2", self.delegate.id() == 2, |_| {});
        fail_point!("on_handle_apply", |_| {});
        fail_point!("on_handle_apply_store_1", apply_ctx.store_id == 1, |_| {});

        if self.delegate.pending_remove || self.delegate.stopped {
            return;
        }

        if self.delegate.wait_data {
            return;
        }

        let mut entries = Vec::new();

        let mut dangle_size = 0;
        for cached_entries in apply.entries {
            let (e, sz) = cached_entries.take_entries();
            dangle_size += sz;
            if e.is_empty() {
                let rid = self.delegate.region_id();
                let StdRange { start, end } = cached_entries.range;
                self.delegate
                    .raft_engine
                    .fetch_entries_to(rid, start, end, None, &mut entries)
                    .unwrap();
            } else if entries.is_empty() {
                entries = e;
            } else {
                entries.extend(e);
            }
        }
        if dangle_size > 0 {
            MEMTRACE_ENTRY_CACHE.trace(TraceEvent::Sub(dangle_size));
            RAFT_ENTRIES_CACHES_GAUGE.sub(dangle_size as i64);
        }

        self.delegate.term = apply.term;
        if let Some(meta) = apply.bucket_meta.clone() {
            if let Some(old) = &mut self.delegate.buckets {
                old.set_meta(meta);
            } else {
                let new = BucketStat::from_meta(meta);
                self.delegate.buckets.replace(new);
            }
        }

        let prev_state = (
            self.delegate.apply_state.get_commit_index(),
            self.delegate.apply_state.get_commit_term(),
        );
        let cur_state = (apply.commit_index, apply.commit_term);
        if prev_state.0 > cur_state.0 || prev_state.1 > cur_state.1 {
            panic!(
                "{} commit state jump backward {:?} -> {:?}",
                self.delegate.tag, prev_state, cur_state
            );
        }
        self.delegate.apply_state.set_commit_index(cur_state.0);
        self.delegate.apply_state.set_commit_term(cur_state.1);

        self.append_proposal(apply.cbs.drain(..));
        // If there is any apply task, we change this fsm to normal-priority.
        // When it meets a ingest-request or a delete-range request, it will change to
        // low-priority.
        self.delegate.priority = Priority::Normal;
        self.delegate
            .handle_raft_committed_entries(apply_ctx, entries.drain(..));
        fail_point!("post_handle_apply_1003", self.delegate.id() == 1003, |_| {});
    }

    /// Handles proposals, and appends the commands to the apply delegate.
    fn append_proposal(&mut self, props_drainer: Drain<'_, Proposal<Callback<EK::Snapshot>>>) {
        let (region_id, peer_id) = (self.delegate.region_id(), self.delegate.id());
        let propose_num = props_drainer.len();
        if self.delegate.stopped {
            for p in props_drainer {
                let cmd = PendingCmd::new(p.index, p.term, p.cb);
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

    fn destroy(&mut self, ctx: &mut ApplyContext<EK>) {
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

    /// Handles peer destroy. When a peer is destroyed, the corresponding apply
    /// delegate should be removed too.
    fn handle_destroy(&mut self, ctx: &mut ApplyContext<EK>, d: Destroy) {
        fail_point!("on_apply_handle_destroy");
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
                        peer_id: self.delegate.id(),
                        merge_from_snapshot: d.merge_from_snapshot,
                    },
                },
            );
        }
    }

    fn resume_pending(&mut self, ctx: &mut ApplyContext<EK>) {
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

    fn logs_up_to_date_for_merge(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        catch_up_logs: CatchUpLogs,
    ) {
        fail_point!("after_handle_catch_up_logs_for_merge", |_| {});
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
        // The source peer fsm will be destroyed when the target peer executes
        // `on_ready_commit_merge` and sends `merge result` to the source peer
        // fsm.
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

    fn handle_snapshot(&mut self, apply_ctx: &mut ApplyContext<EK>, snap_task: GenSnapTask) {
        if self.delegate.pending_remove || self.delegate.stopped {
            return;
        }
        if self.delegate.peer.is_witness || self.delegate.wait_data {
            // witness or non-witness hasn't finish applying snapshot shouldn't generate
            // snapshot.
            return;
        }
        let applied_index = self.delegate.apply_state.get_applied_index();
        let need_sync = apply_ctx
            .apply_res
            .iter()
            .any(|res| res.region_id == self.delegate.region_id())
            && self.delegate.last_flush_applied_index != applied_index;
        let force_sync_fp = || {
            fail_point!("apply_on_handle_snapshot_sync", |_| true);
            false
        };
        if need_sync || force_sync_fp() {
            if apply_ctx.timer.is_none() {
                apply_ctx.timer = Some(Instant::now_coarse());
            }
            self.delegate.maybe_write_apply_state(apply_ctx);
            fail_point!(
                "apply_on_handle_snapshot_1_1",
                self.delegate.id() == 1 && self.delegate.region_id() == 1,
                |_| unimplemented!()
            );

            apply_ctx.flush();
            self.delegate.last_flush_applied_index = applied_index;
        }

        if let Err(e) = snap_task.generate_and_schedule_snapshot::<EK>(
            apply_ctx.engine.snapshot(None),
            self.delegate.applied_term,
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
            self.delegate.id() == 1 && self.delegate.region_id() == 1,
            |_| unimplemented!()
        );
    }

    fn handle_change(
        &mut self,
        apply_ctx: &mut ApplyContext<EK>,
        cmd: ChangeObserver,
        region_epoch: RegionEpoch,
        cb: Callback<EK::Snapshot>,
    ) {
        let ChangeObserver { region_id, ty } = cmd;

        let is_stale_cmd = match ty {
            ObserverType::Cdc(ObserveHandle { id, .. }) => {
                self.delegate.observe_info.cdc_id.id > id
            }
            ObserverType::Rts(ObserveHandle { id, .. }) => {
                self.delegate.observe_info.rts_id.id > id
            }
            ObserverType::Pitr(ObserveHandle { id, .. }) => {
                self.delegate.observe_info.pitr_id.id > id
            }
        };
        if is_stale_cmd {
            notify_stale_req_with_msg(
                self.delegate.term,
                format!(
                    "stale observe id {:?}, current id: {:?}",
                    ty.handle().id,
                    self.delegate.observe_info.pitr_id.id
                ),
                cb,
            );
            return;
        }

        assert_eq!(self.delegate.region_id(), region_id);
        let resp = match compare_region_epoch(
            &region_epoch,
            &self.delegate.region,
            false, // check_conf_ver
            true,  // check_ver
            true,  // include_region
        ) {
            Ok(()) => {
                // Commit the writebatch for ensuring the following snapshot can get all
                // previous writes.
                if apply_ctx.kv_wb().count() > 0 {
                    apply_ctx.commit(&mut self.delegate);
                }
                ReadResponse {
                    response: Default::default(),
                    snapshot: Some(RegionSnapshot::from_snapshot(
                        Arc::new(apply_ctx.engine.snapshot(None)),
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

        match ty {
            ObserverType::Cdc(id) => {
                self.delegate.observe_info.cdc_id = id;
            }
            ObserverType::Rts(id) => {
                self.delegate.observe_info.rts_id = id;
            }
            ObserverType::Pitr(id) => {
                self.delegate.observe_info.pitr_id = id;
            }
        }
        cb.invoke_read(resp);
    }

    fn check_pending_compact_log(
        &mut self,
        ctx: &mut ApplyContext<EK>,
        voter_replicated_index: u64,
        voter_replicated_term: u64,
    ) {
        if self.delegate.pending_remove || self.delegate.stopped {
            return;
        }

        let res = self
            .delegate
            .try_compact_log(voter_replicated_index, voter_replicated_term);
        match res {
            Ok((should_write, res)) => {
                if let Some(res) = res {
                    if ctx.timer.is_none() {
                        ctx.timer = Some(Instant::now_coarse());
                    }
                    ctx.prepare_for(&mut self.delegate);
                    let mut result = VecDeque::new();
                    // If modified `truncated_state` in `try_compact_log`, the apply state should be
                    // persisted.
                    if should_write {
                        self.delegate.write_apply_state(ctx.kv_wb_mut());
                        ctx.commit_opt(&mut self.delegate, true);
                    }
                    result.push_back(res);
                    ctx.finish_for(&mut self.delegate, result);
                }
            }
            Err(e) => error!(?e;
                "failed to compact log";
                "region_id" => self.delegate.region.get_id(),
                "peer_id" => self.delegate.id(),
            ),
        }
    }

    fn handle_tasks(&mut self, apply_ctx: &mut ApplyContext<EK>, msgs: &mut Vec<Msg<EK>>) {
        let mut drainer = msgs.drain(..);
        let mut batch_apply = None;
        loop {
            let msg = match drainer.next() {
                Some(m) => m,
                None => {
                    if let Some(apply) = batch_apply {
                        self.handle_apply(apply_ctx, apply);
                    }
                    break;
                }
            };

            if batch_apply.is_some() {
                match &msg {
                    Msg::Apply { .. } => (),
                    _ => {
                        self.handle_apply(apply_ctx, batch_apply.take().unwrap());
                        if let Some(ref mut state) = self.delegate.yield_state {
                            state.pending_msgs.push(msg);
                            state.pending_msgs.extend(drainer);
                            break;
                        }
                    }
                }
            }

            match msg {
                Msg::Apply { start, mut apply } => {
                    let apply_wait = start.saturating_elapsed();
                    apply_ctx.apply_wait.observe(apply_wait.as_secs_f64());
                    for tracker in apply
                        .cbs
                        .iter()
                        .flat_map(|p| p.cb.write_trackers())
                        .flat_map(|ts| ts.as_tracker_token())
                    {
                        GLOBAL_TRACKERS.with_tracker(tracker, |t| {
                            t.metrics.apply_wait_nanos = apply_wait.as_nanos() as u64;
                        });
                    }

                    if let Some(batch) = batch_apply.as_mut() {
                        if batch.try_batch(&mut apply) {
                            continue;
                        } else {
                            self.handle_apply(apply_ctx, batch_apply.take().unwrap());
                            if let Some(ref mut state) = self.delegate.yield_state {
                                state.pending_msgs.push(Msg::Apply { start, apply });
                                state.pending_msgs.extend(drainer);
                                break;
                            }
                        }
                    }
                    if !self.delegate.wait_data {
                        batch_apply = Some(apply);
                    }
                }
                Msg::Recover(..) => self.delegate.wait_data = false,
                Msg::Registration(reg) => self.handle_registration(reg),
                Msg::Destroy(d) => self.handle_destroy(apply_ctx, d),
                Msg::LogsUpToDate(cul) => self.logs_up_to_date_for_merge(apply_ctx, cul),
                Msg::Noop => {}
                Msg::Snapshot(snap_task) => self.handle_snapshot(apply_ctx, snap_task),
                Msg::Change {
                    cmd,
                    region_epoch,
                    cb,
                } => self.handle_change(apply_ctx, cmd, region_epoch, cb),
                #[cfg(any(test, feature = "testexport"))]
                Msg::Validate(_, f) => {
                    let delegate = &self.delegate as *const ApplyDelegate<EK> as *const u8;
                    f(delegate)
                }
                Msg::CheckCompact {
                    voter_replicated_index,
                    voter_replicated_term,
                    ..
                } => {
                    self.check_pending_compact_log(
                        apply_ctx,
                        voter_replicated_index,
                        voter_replicated_term,
                    );
                }
            }
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

    #[inline]
    fn get_priority(&self) -> Priority {
        self.delegate.priority
    }
}

impl<EK> Drop for ApplyFsm<EK>
where
    EK: KvEngine,
{
    fn drop(&mut self) {
        if tikv_util::thread_group::is_shutdown(!cfg!(test)) {
            self.delegate.clear_all_commands_silently()
        } else {
            self.delegate.clear_all_commands_as_stale();
        }
        let mut event = TraceEvent::default();
        if let Some(e) = self.delegate.trace.reset(ApplyMemoryTrace::default()) {
            event = event + e;
        }
        MEMTRACE_APPLYS.trace(event);
    }
}

pub enum ControlMsg {
    LatencyInspect {
        send_time: Instant,
        inspector: LatencyInspector,
    },
}

impl ResourceMetered for ControlMsg {}

pub struct ControlFsm {
    receiver: Receiver<ControlMsg>,
    stopped: bool,
}

impl ControlFsm {
    pub fn new() -> (LooseBoundedSender<ControlMsg>, Box<ControlFsm>) {
        let (tx, rx) = loose_bounded(std::usize::MAX);
        let fsm = Box::new(ControlFsm {
            stopped: false,
            receiver: rx,
        });
        (tx, fsm)
    }

    pub fn handle_messages(&mut self, pending_latency_inspect: &mut Vec<LatencyInspector>) {
        // Usually there will be only 1 control message.
        loop {
            match self.receiver.try_recv() {
                Ok(ControlMsg::LatencyInspect {
                    send_time,
                    mut inspector,
                }) => {
                    inspector.record_apply_wait(send_time.saturating_elapsed());
                    pending_latency_inspect.push(inspector);
                }
                Err(TryRecvError::Empty) => {
                    return;
                }
                Err(TryRecvError::Disconnected) => {
                    self.stopped = true;
                    return;
                }
            }
        }
    }
}

impl Fsm for ControlFsm {
    type Message = ControlMsg;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.stopped
    }
}

pub struct ApplyPoller<EK>
where
    EK: KvEngine,
{
    msg_buf: Vec<Msg<EK>>,
    apply_ctx: ApplyContext<EK>,
    messages_per_tick: usize,
    cfg_tracker: Tracker<Config>,

    trace_event: TraceEvent,
}

impl<EK> PollHandler<ApplyFsm<EK>, ControlFsm> for ApplyPoller<EK>
where
    EK: KvEngine,
{
    fn begin<F>(&mut self, _batch_size: usize, update_cfg: F)
    where
        for<'a> F: FnOnce(&'a BatchSystemConfig),
    {
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
            self.apply_ctx.yield_msg_size = incoming.apply_yield_write_size.0;
            update_cfg(&incoming.apply_batch_system);
        }
    }

    fn handle_control(&mut self, control: &mut ControlFsm) -> Option<usize> {
        control.handle_messages(&mut self.apply_ctx.pending_latency_inspect);
        if !self.apply_ctx.pending_latency_inspect.is_empty() && self.apply_ctx.timer.is_none() {
            self.apply_ctx.timer = Some(Instant::now_coarse());
        }
        Some(0)
    }

    fn handle_normal(&mut self, normal: &mut impl DerefMut<Target = ApplyFsm<EK>>) -> HandleResult {
        let mut handle_result = HandleResult::KeepProcessing;
        normal.delegate.handle_start = Some(Instant::now_coarse());
        if normal.delegate.yield_state.is_some() {
            if normal.delegate.wait_merge_state.is_some() {
                // We need to query the length first, otherwise there is a race
                // condition that new messages are queued after resuming and before
                // query the length.
                handle_result = HandleResult::stop_at(normal.receiver.len(), false);
            }
            normal.resume_pending(&mut self.apply_ctx);
            if normal.delegate.wait_merge_state.is_some() {
                // Yield due to applying CommitMerge, this fsm can be released if its
                // channel msg count equals to last count because it will receive
                // a new message if its source region has applied all needed logs.
                return handle_result;
            } else if normal.delegate.yield_state.is_some() {
                // Yield due to other reasons, this fsm must not be released because
                // it's possible that no new message will be sent to itself.
                // The remaining messages will be handled in next rounds.
                return HandleResult::KeepProcessing;
            }
            handle_result = HandleResult::KeepProcessing;
        }
        fail_point!("before_handle_normal_3", normal.delegate.id() == 3, |_| {
            HandleResult::KeepProcessing
        });
        fail_point!(
            "before_handle_normal_1003",
            normal.delegate.id() == 1003,
            |_| { HandleResult::KeepProcessing }
        );
        while self.msg_buf.len() < self.messages_per_tick {
            match normal.receiver.try_recv() {
                Ok(msg) => self.msg_buf.push(msg),
                Err(TryRecvError::Empty) => {
                    handle_result = HandleResult::stop_at(0, false);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    normal.delegate.stopped = true;
                    handle_result = HandleResult::stop_at(0, false);
                    break;
                }
            }
        }

        normal.handle_tasks(&mut self.apply_ctx, &mut self.msg_buf);

        if normal.delegate.wait_merge_state.is_some() {
            // Check it again immediately as catching up logs can be very fast.
            handle_result = HandleResult::stop_at(0, false);
        } else if normal.delegate.yield_state.is_some() {
            // Let it continue to run next time.
            handle_result = HandleResult::KeepProcessing;
        }
        handle_result
    }

    fn end(&mut self, fsms: &mut [Option<impl DerefMut<Target = ApplyFsm<EK>>>]) {
        self.apply_ctx.flush();
        for fsm in fsms.iter_mut().flatten() {
            fsm.delegate.last_flush_applied_index = fsm.delegate.apply_state.get_applied_index();
            fsm.delegate.has_pending_ssts = false;
            fsm.delegate.update_memory_trace(&mut self.trace_event);
        }
        MEMTRACE_APPLYS.trace(mem::take(&mut self.trace_event));
    }

    fn get_priority(&self) -> Priority {
        self.apply_ctx.priority
    }
}

pub struct Builder<EK: KvEngine> {
    tag: String,
    cfg: Arc<VersionTrack<Config>>,
    coprocessor_host: CoprocessorHost<EK>,
    importer: Arc<SstImporter<EK>>,
    region_scheduler: Scheduler<RegionTask<<EK as KvEngine>::Snapshot>>,
    engine: EK,
    sender: Box<dyn Notifier<EK>>,
    router: ApplyRouter<EK>,
    store_id: u64,
    pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
}

impl<EK: KvEngine> Builder<EK> {
    pub fn new<T, ER: RaftEngine>(
        builder: &RaftPollerBuilder<EK, ER, T>,
        sender: Box<dyn Notifier<EK>>,
        router: ApplyRouter<EK>,
    ) -> Builder<EK> {
        Builder {
            tag: format!("[store {}]", builder.store.get_id()),
            cfg: builder.cfg.clone(),
            coprocessor_host: builder.coprocessor_host.clone(),
            importer: builder.importer.clone(),
            region_scheduler: builder.region_scheduler.clone(),
            engine: builder.engines.kv.clone(),
            sender,
            router,
            store_id: builder.store.get_id(),
            pending_create_peers: builder.pending_create_peers.clone(),
        }
    }
}

impl<EK> HandlerBuilder<ApplyFsm<EK>, ControlFsm> for Builder<EK>
where
    EK: KvEngine,
{
    type Handler = ApplyPoller<EK>;

    fn build(&mut self, priority: Priority) -> ApplyPoller<EK> {
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
                priority,
            ),
            messages_per_tick: cfg.messages_per_tick,
            cfg_tracker: self.cfg.clone().tracker(self.tag.clone()),
            trace_event: Default::default(),
        }
    }
}

impl<EK> Clone for Builder<EK>
where
    EK: KvEngine,
{
    fn clone(&self) -> Self {
        Builder {
            tag: self.tag.clone(),
            cfg: self.cfg.clone(),
            coprocessor_host: self.coprocessor_host.clone(),
            importer: self.importer.clone(),
            region_scheduler: self.region_scheduler.clone(),
            engine: self.engine.clone(),
            sender: self.sender.clone_box(),
            router: self.router.clone(),
            store_id: self.store_id,
            pending_create_peers: self.pending_create_peers.clone(),
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
                    // Invoking callback can release txn latch, if it's still leader, following
                    // command may not read the writes of previous commands and break ACID. If
                    // it's still leader, there are two possibility that mailbox is closed:
                    // 1. The process is shutting down.
                    // 2. The leader is destroyed. A leader won't propose to destroy itself, so it
                    //    should either destroyed by older leaders or newer leaders. Leader won't
                    //    respond to read until it has applied to current term, so no command will
                    //    be proposed until command from older leaders have applied, which will then
                    //    stop it from accepting proposals. If the command is proposed by new
                    //    leader, then it won't be able to propose new proposals.
                    // So only shutdown needs to be checked here.
                    if !tikv_util::thread_group::is_shutdown(!cfg!(test)) {
                        for p in apply.cbs.drain(..) {
                            let cmd = PendingCmd::new(p.index, p.term, p.cb);
                            notify_region_removed(apply.region_id, apply.peer_id, cmd);
                        }
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
                    cmd: ChangeObserver { region_id, .. },
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
                Msg::Validate(..) => return,
                Msg::Recover(region_id) => {
                    info!("recover apply";
                          "region_id" => region_id);
                    return;
                }
                Msg::CheckCompact { region_id, .. } => {
                    info!("target region is not found";
                            "region_id" => region_id);
                    return;
                }
            },
            Either::Left(Err(TrySendError::Full(_))) => unreachable!(),
        };

        // Messages in one region are sent in sequence, so there is no race here.
        // However, this can't be handled inside control fsm, as messages can be
        // queued inside both queue of control fsm and normal fsm, which can reorder
        // messages.
        let (sender, apply_fsm) = ApplyFsm::from_registration(reg);
        let mailbox = BasicMailbox::new(sender, apply_fsm, self.state_cnt().clone());
        self.register(region_id, mailbox);
    }

    pub fn register(&self, region_id: u64, mailbox: BasicMailbox<ApplyFsm<EK>>) {
        self.router.register(region_id, mailbox);
        self.update_trace();
    }

    pub fn register_all(&self, mailboxes: Vec<(u64, BasicMailbox<ApplyFsm<EK>>)>) {
        self.router.register_all(mailboxes);
        self.update_trace();
    }

    pub fn close(&self, region_id: u64) {
        self.router.close(region_id);
        self.update_trace();
    }

    fn update_trace(&self) {
        let router_trace = self.router.trace();
        MEMTRACE_APPLY_ROUTER_ALIVE.trace(TraceEvent::Reset(router_trace.alive));
        MEMTRACE_APPLY_ROUTER_LEAK.trace(TraceEvent::Reset(router_trace.leak));
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
            mailboxes.push((
                peer.region().get_id(),
                BasicMailbox::new(tx, fsm, self.router().state_cnt().clone()),
            ));
        }
        self.router().register_all(mailboxes);
    }
}

pub fn create_apply_batch_system<EK: KvEngine>(
    cfg: &Config,
    resource_ctl: Option<Arc<ResourceController>>,
) -> (ApplyRouter<EK>, ApplyBatchSystem<EK>) {
    let (control_tx, control_fsm) = ControlFsm::new();
    let (router, system) = batch_system::create_system(
        &cfg.apply_batch_system,
        control_tx,
        control_fsm,
        resource_ctl,
    );
    (ApplyRouter { router }, ApplyBatchSystem { system })
}

mod memtrace {
    use memory_trace_macros::MemoryTraceHelper;

    use super::*;

    #[derive(MemoryTraceHelper, Default, Debug)]
    pub struct ApplyMemoryTrace {
        pub pending_cmds: usize,
        pub merge_yield: usize,
    }

    impl<C> HeapSize for PendingCmdQueue<C> {
        fn approximate_heap_size(&self) -> usize {
            // Some fields of `PendingCmd` are on stack, but ignore them because they are
            // just some small boxed closures.
            self.normals.capacity() * mem::size_of::<PendingCmd<C>>()
        }
    }

    impl<EK> HeapSize for YieldState<EK>
    where
        EK: KvEngine,
    {
        fn approximate_heap_size(&self) -> usize {
            let mut size = self.pending_entries.capacity() * mem::size_of::<Entry>();
            for e in &self.pending_entries {
                size += bytes_capacity(&e.data) + bytes_capacity(&e.context);
            }

            size += self.pending_msgs.capacity() * mem::size_of::<Msg<EK>>();
            for msg in &self.pending_msgs {
                size += msg.approximate_heap_size();
            }

            size
        }
    }

    impl<EK> HeapSize for Msg<EK>
    where
        EK: KvEngine,
    {
        /// Only consider large fields in `Msg`.
        fn approximate_heap_size(&self) -> usize {
            match self {
                Msg::LogsUpToDate(l) => l.approximate_heap_size(),
                // For entries in `Msg::Apply`, heap size is already updated when fetching them
                // from `raft::Storage`. So use `0` here.
                Msg::Apply { .. } => 0,
                Msg::Registration(_)
                | Msg::Snapshot(_)
                | Msg::Destroy(_)
                | Msg::Noop
                | Msg::Change { .. } => 0,
                #[cfg(any(test, feature = "testexport"))]
                Msg::Validate(..) => 0,
                Msg::Recover(..) => 0,
                Msg::CheckCompact { .. } => 0,
            }
        }
    }

    impl HeapSize for CatchUpLogs {
        fn approximate_heap_size(&self) -> usize {
            let mut size: usize = 0;
            for e in &self.merge.entries {
                size += bytes_capacity(&e.data) + bytes_capacity(&e.context);
            }
            size
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{atomic::*, *},
        thread,
        time::*,
    };

    use bytes::Bytes;
    use engine_panic::PanicEngine;
    use engine_test::kv::{new_engine, KvTestEngine, KvTestSnapshot};
    use engine_traits::{Peekable as PeekableTrait, SyncMutable, WriteBatchExt};
    use kvproto::{
        kvrpcpb::ApiVersion,
        metapb::{self, RegionEpoch},
        raft_cmdpb::*,
    };
    use protobuf::Message;
    use raft::eraftpb::{ConfChange, ConfChangeV2};
    use sst_importer::Config as ImportConfig;
    use tempfile::{Builder, TempDir};
    use test_sst_importer::*;
    use tikv_util::{
        config::{ReadableSize, VersionTrack},
        store::{new_learner_peer, new_peer},
        worker::dummy_scheduler,
    };
    use txn_types::WriteBatchFlags;
    use uuid::Uuid;

    use super::*;
    use crate::{
        coprocessor::*,
        store::{
            msg::WriteResponse,
            peer_storage::RAFT_INIT_LOG_INDEX,
            simple_write::{SimpleWriteEncoder, SimpleWriteReqEncoder},
            Config, RegionTask,
        },
    };

    impl GenSnapTask {
        fn new_for_test(region_id: u64, snap_notifier: SyncSender<RaftSnapshot>) -> GenSnapTask {
            let index = Arc::new(AtomicU64::new(0));
            let canceled = Arc::new(AtomicBool::new(false));
            Self::new(region_id, index, canceled, snap_notifier, 0)
        }
    }

    pub fn create_tmp_engine(path: &str) -> (TempDir, KvTestEngine) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let engine = new_engine(path.path().join("db").to_str().unwrap(), ALL_CFS).unwrap();
        (path, engine)
    }

    pub fn create_tmp_importer(path: &str) -> (TempDir, Arc<SstImporter<KvTestEngine>>) {
        let dir = Builder::new().prefix(path).tempdir().unwrap();
        let importer = Arc::new(
            SstImporter::new(
                &ImportConfig::default(),
                dir.path(),
                None,
                ApiVersion::V1,
                false,
            )
            .unwrap(),
        );
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
            req.set_header(RaftRequestHeader::default());
            req.mut_requests().push(cmd);
            e.set_data(req.write_to_bytes().unwrap().into())
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

    impl Default for Registration {
        fn default() -> Self {
            Registration {
                id: Default::default(),
                term: Default::default(),
                apply_state: Default::default(),
                applied_term: Default::default(),
                region: Default::default(),
                pending_request_snapshot_count: Default::default(),
                is_merging: Default::default(),
                raft_engine: Box::new(PanicEngine),
            }
        }
    }

    impl Registration {
        fn dup(&self) -> Self {
            Registration {
                id: self.id,
                term: self.term,
                apply_state: self.apply_state.clone(),
                applied_term: self.applied_term,
                region: self.region.clone(),
                pending_request_snapshot_count: self.pending_request_snapshot_count.clone(),
                is_merging: self.is_merging,
                raft_engine: Box::new(PanicEngine),
            }
        }
    }

    #[test]
    fn test_can_witness_skip() {
        let mut entry = Entry::new();
        let mut req = RaftCmdRequest::default();
        entry.set_entry_type(EntryType::EntryNormal);
        let data = req.write_to_bytes().unwrap();
        entry.set_data(Bytes::copy_from_slice(&data));
        assert!(can_witness_skip(&entry));

        req.mut_admin_request()
            .set_cmd_type(AdminCmdType::CompactLog);
        let data = req.write_to_bytes().unwrap();
        entry.set_data(Bytes::copy_from_slice(&data));
        assert!(!can_witness_skip(&entry));

        let mut req = RaftCmdRequest::default();
        let mut request = Request::default();
        request.set_cmd_type(CmdType::Put);
        req.set_requests(vec![request].into());
        let data = req.write_to_bytes().unwrap();
        entry.set_data(Bytes::copy_from_slice(&data));
        assert!(can_witness_skip(&entry));

        entry.set_entry_type(EntryType::EntryConfChange);
        let conf_change = ConfChange::new();
        let data = conf_change.write_to_bytes().unwrap();
        entry.set_data(Bytes::copy_from_slice(&data));
        assert!(!can_witness_skip(&entry));

        entry.set_entry_type(EntryType::EntryConfChangeV2);
        let conf_change_v2 = ConfChangeV2::new();
        let data = conf_change_v2.write_to_bytes().unwrap();
        entry.set_data(Bytes::copy_from_slice(&data));
        assert!(!can_witness_skip(&entry));
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
        assert_eq!(should_write_to_engine(true, &cmd), true);
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
        assert_eq!(should_write_to_engine(true, &req), true);
        assert_eq!(should_write_to_engine(false, &req), true);

        // DeleteRange command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::DeleteRange);
        req.set_delete_range(DeleteRangeRequest::default());
        let mut cmd = RaftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(should_write_to_engine(true, &cmd), true);
        assert_eq!(should_write_to_engine(false, &cmd), true);

        // IngestSst command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::IngestSst);
        req.set_ingest_sst(IngestSstRequest::default());
        let mut cmd = RaftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(should_write_to_engine(true, &cmd), true);
        assert_eq!(should_write_to_engine(false, &cmd), false);
    }

    #[test]
    fn test_has_high_latency_operation() {
        // Normal command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Put);
        req.set_put(PutRequest::default());
        let mut cmd = RaftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(has_high_latency_operation(&cmd), false);

        // IngestSst command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::IngestSst);
        req.set_ingest_sst(IngestSstRequest::default());
        let mut cmd = RaftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(has_high_latency_operation(&cmd), true);

        // DeleteRange command
        let mut req = Request::default();
        req.set_cmd_type(CmdType::DeleteRange);
        req.set_delete_range(DeleteRangeRequest::default());
        let mut cmd = RaftCmdRequest::default();
        cmd.mut_requests().push(req);
        assert_eq!(has_high_latency_operation(&cmd), true);
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
    ) -> Proposal<Callback<S>> {
        Proposal {
            is_conf_change,
            index,
            term,
            cb,
            propose_time: None,
            must_pass_epoch_check: false,
            sent: true,
        }
    }

    fn apply<C: WriteCallback>(
        peer_id: u64,
        region_id: u64,
        term: u64,
        entries: Vec<Entry>,
        cbs: Vec<Proposal<C>>,
    ) -> Apply<C> {
        let (commit_index, commit_term) = entries
            .last()
            .map(|e| (e.get_index(), e.get_term()))
            .unwrap();
        Apply::new(
            peer_id,
            region_id,
            term,
            commit_index,
            commit_term,
            entries,
            cbs,
            None,
        )
    }

    #[test]
    fn test_basic_flow() {
        let (tx, rx) = mpsc::channel();
        let sender = Box::new(TestNotifier { tx });
        let (_tmp, engine) = create_tmp_engine("apply-basic");
        let (_dir, importer) = create_tmp_importer("apply-basic");
        let (region_scheduler, mut snapshot_rx) = dummy_scheduler();
        let cfg = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg,
            coprocessor_host: CoprocessorHost::<KvTestEngine>::default(),
            importer,
            region_scheduler,
            sender,
            engine,
            router: router.clone(),
            store_id: 1,
            pending_create_peers,
        };
        system.spawn("test-basic".to_owned(), builder);

        let mut reg = Registration {
            id: 1,
            term: 4,
            applied_term: 5,
            ..Default::default()
        };
        reg.region.set_id(2);
        let mut peer = metapb::Peer::default();
        peer.set_id(1);
        reg.region.mut_peers().push(peer.clone());
        reg.apply_state.set_applied_index(3);
        router.schedule_task(2, Msg::Registration(reg.dup()));
        validate(&router, 2, move |delegate| {
            // Migrated to 2021 migration. This let statement is probably not needed, see
            //   https://doc.rust-lang.org/edition-guide/rust-2021/disjoint-capture-in-closures.html
            let _ = &reg;
            assert_eq!(delegate.id(), 1);
            assert_eq!(delegate.peer, peer);
            assert_eq!(delegate.tag, "[region 2] 1");
            assert_eq!(delegate.region, reg.region);
            assert!(!delegate.pending_remove);
            assert_eq!(delegate.apply_state, reg.apply_state);
            assert_eq!(delegate.term, reg.term);
            assert_eq!(delegate.applied_term, reg.applied_term);
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
        rx.try_recv().unwrap_err();

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
        rx.recv_timeout(Duration::from_secs(3)).unwrap();

        // Make sure Apply and Snapshot are in the same batch.
        let (snap_tx, _) = mpsc::sync_channel(0);
        batch_messages(
            &router,
            2,
            vec![
                Msg::apply(apply(1, 2, 11, vec![new_entry(5, 5, false)], vec![])),
                Msg::Snapshot(GenSnapTask::new_for_test(2, snap_tx)),
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
        // empty entry will make applied_index step forward and should write apply state
        // to engine.
        assert_eq!(apply_res.metrics.written_keys, 1);
        assert_eq!(apply_res.applied_term, 5);
        validate(&router, 2, |delegate| {
            assert_eq!(delegate.term, 11);
            assert_eq!(delegate.applied_term, 5);
            assert_eq!(delegate.apply_state.get_applied_index(), 5);
            assert_eq!(
                delegate.apply_state.get_applied_index(),
                delegate.last_flush_applied_index
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
        rx.try_recv().unwrap_err();

        system.shutdown();
    }

    fn cb<S: Snapshot>(idx: u64, term: u64, tx: Sender<RaftCmdResponse>) -> Proposal<Callback<S>> {
        proposal(
            false,
            idx,
            term,
            Callback::write(Box::new(move |resp: WriteResponse| {
                tx.send(resp.response).unwrap();
            })),
        )
    }

    fn cb_conf_change<S: Snapshot>(
        idx: u64,
        term: u64,
        tx: Sender<RaftCmdResponse>,
    ) -> Proposal<Callback<S>> {
        proposal(
            true,
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

        fn prepare_merge(mut self, target: metapb::Region) -> EntryBuilder {
            let mut request = AdminRequest::default();
            request.set_cmd_type(AdminCmdType::PrepareMerge);
            request.mut_prepare_merge().set_target(target);
            self.req.set_admin_request(request);
            self
        }

        fn compact_log(mut self, index: u64, term: u64) -> EntryBuilder {
            let mut req = AdminRequest::default();
            req.set_cmd_type(AdminCmdType::CompactLog);
            req.mut_compact_log().set_compact_index(index);
            req.mut_compact_log().set_compact_term(term);
            self.req.set_admin_request(req);
            self
        }

        fn compute_hash(mut self, context: Vec<u8>) -> EntryBuilder {
            let mut req = AdminRequest::default();
            req.set_cmd_type(AdminCmdType::ComputeHash);
            req.mut_compute_hash().set_context(context);
            self.req.set_admin_request(req);
            self
        }

        fn conf_change(mut self, changes: Vec<ChangePeerRequest>) -> EntryBuilder {
            let mut req = AdminRequest::default();
            req.set_cmd_type(AdminCmdType::ChangePeerV2);
            req.mut_change_peer_v2().set_changes(changes.into());
            self.req.set_admin_request(req);
            self
        }

        fn build(mut self) -> Entry {
            self.entry
                .set_data(self.req.write_to_bytes().unwrap().into());
            self.entry
        }
    }

    struct EntryBuilderUsingSimpleWrite {
        entry: Entry,
        header: Box<RaftRequestHeader>,
        encoder: SimpleWriteEncoder,
    }

    impl EntryBuilderUsingSimpleWrite {
        fn new(index: u64, term: u64) -> EntryBuilderUsingSimpleWrite {
            let encoder = SimpleWriteEncoder::with_capacity(64);
            let header = Box::<RaftRequestHeader>::default();
            let mut entry = Entry::default();
            entry.set_index(index);
            entry.set_term(term);
            EntryBuilderUsingSimpleWrite {
                entry,
                header,
                encoder,
            }
        }

        fn epoch(mut self, conf_ver: u64, version: u64) -> EntryBuilderUsingSimpleWrite {
            let mut epoch = RegionEpoch::default();
            epoch.set_version(version);
            epoch.set_conf_ver(conf_ver);
            self.header.set_region_epoch(epoch);
            self
        }

        fn put(mut self, key: &[u8], value: &[u8]) -> EntryBuilderUsingSimpleWrite {
            self.encoder.put(CF_DEFAULT, key, value);
            self
        }

        fn put_cf(mut self, cf: &str, key: &[u8], value: &[u8]) -> EntryBuilderUsingSimpleWrite {
            self.encoder.put(cf, key, value);
            self
        }

        fn delete(mut self, key: &[u8]) -> EntryBuilderUsingSimpleWrite {
            self.encoder.delete(CF_DEFAULT, key);
            self
        }

        fn delete_cf(mut self, cf: &str, key: &[u8]) -> EntryBuilderUsingSimpleWrite {
            self.encoder.delete(cf, key);
            self
        }

        fn delete_range(
            mut self,
            start_key: &[u8],
            end_key: &[u8],
        ) -> EntryBuilderUsingSimpleWrite {
            self.encoder
                .delete_range(CF_DEFAULT, start_key, end_key, false);
            self
        }

        fn delete_range_cf(
            mut self,
            cf: &str,
            start_key: &[u8],
            end_key: &[u8],
        ) -> EntryBuilderUsingSimpleWrite {
            self.encoder.delete_range(cf, start_key, end_key, false);
            self
        }

        fn ingest_sst(mut self, meta: &SstMeta) -> EntryBuilderUsingSimpleWrite {
            self.encoder.ingest(vec![meta.clone()]);
            self
        }

        fn build(mut self) -> Entry {
            let bin = self.encoder.encode();
            let req_encoder = SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
                self.header.clone(),
                bin,
                1000,
            );
            let (bytes, _) = req_encoder.encode();
            self.entry.set_data(bytes.into());
            self.entry
        }
    }

    #[derive(Clone, Default)]
    struct ApplyObserver {
        pre_query_count: Arc<AtomicUsize>,
        post_query_count: Arc<AtomicUsize>,
        cmd_sink: Option<Arc<Mutex<Sender<CmdBatch>>>>,
        filter_compact_log: Arc<AtomicBool>,
        filter_consistency_check: Arc<AtomicBool>,
        skip_persist_when_pre_commit: Arc<AtomicBool>,
        delay_remove_ssts: Arc<AtomicBool>,
        last_delete_sst_count: Arc<AtomicU64>,
        last_pending_delete_sst_count: Arc<AtomicU64>,
        last_pending_handle_sst_count: Arc<AtomicU64>,
    }

    impl Coprocessor for ApplyObserver {}

    impl QueryObserver for ApplyObserver {
        fn pre_apply_query(&self, _: &mut ObserverContext<'_>, _: &[Request]) {
            self.pre_query_count.fetch_add(1, Ordering::SeqCst);
        }

        fn post_apply_query(&self, _: &mut ObserverContext<'_>, _: &Cmd) {
            self.post_query_count.fetch_add(1, Ordering::SeqCst);
        }

        fn post_exec_query(
            &self,
            _: &mut ObserverContext<'_>,
            _: &Cmd,
            _: &RaftApplyState,
            _: &RegionState,
            apply_info: &mut ApplyCtxInfo<'_>,
        ) -> bool {
            match apply_info.pending_handle_ssts {
                Some(v) => {
                    // If it is a ingest sst
                    let mut ssts = std::mem::take(v);
                    assert_ne!(ssts.len(), 0);
                    if self.delay_remove_ssts.load(Ordering::SeqCst) {
                        apply_info.pending_delete_ssts.append(&mut ssts);
                    } else {
                        apply_info.delete_ssts.append(&mut ssts);
                    }
                }
                None => (),
            }
            self.last_delete_sst_count
                .store(apply_info.delete_ssts.len() as u64, Ordering::SeqCst);
            self.last_pending_delete_sst_count.store(
                apply_info.pending_delete_ssts.len() as u64,
                Ordering::SeqCst,
            );
            self.last_pending_handle_sst_count.store(
                match apply_info.pending_handle_ssts {
                    Some(ref v) => v.len() as u64,
                    None => 0,
                },
                Ordering::SeqCst,
            );
            false
        }
    }

    impl AdminObserver for ApplyObserver {
        fn post_exec_admin(
            &self,
            _: &mut ObserverContext<'_>,
            cmd: &Cmd,
            _: &RaftApplyState,
            region_state: &RegionState,
            _: &mut ApplyCtxInfo<'_>,
        ) -> bool {
            let request = cmd.request.get_admin_request();
            match request.get_cmd_type() {
                AdminCmdType::CompactLog => true,
                AdminCmdType::CommitMerge
                | AdminCmdType::PrepareMerge
                | AdminCmdType::RollbackMerge => {
                    assert!(region_state.modified_region.is_some());
                    true
                }
                AdminCmdType::BatchSplit => true,
                AdminCmdType::PrepareFlashback | AdminCmdType::FinishFlashback => true,
                _ => false,
            }
        }

        fn pre_exec_admin(
            &self,
            _: &mut ObserverContext<'_>,
            req: &AdminRequest,
            _: u64,
            _: u64,
        ) -> bool {
            let cmd_type = req.get_cmd_type();
            if cmd_type == AdminCmdType::CompactLog
                && self.filter_compact_log.deref().load(Ordering::SeqCst)
            {
                return true;
            };
            if (cmd_type == AdminCmdType::ComputeHash || cmd_type == AdminCmdType::VerifyHash)
                && self.filter_consistency_check.deref().load(Ordering::SeqCst)
            {
                return true;
            };
            false
        }
    }

    impl<E> CmdObserver<E> for ApplyObserver
    where
        E: KvEngine,
    {
        fn on_flush_applied_cmd_batch(
            &self,
            _: ObserveLevel,
            cmd_batches: &mut Vec<CmdBatch>,
            _: &E,
        ) {
            for b in std::mem::take(cmd_batches) {
                if b.is_empty() {
                    continue;
                }
                if let Some(sink) = self.cmd_sink.as_ref() {
                    sink.lock().unwrap().send(b).unwrap();
                }
            }
        }

        fn on_applied_current_term(&self, _: raft::StateRole, _: &Region) {}
    }

    impl RegionChangeObserver for ApplyObserver {
        fn pre_persist(
            &self,
            _: &mut ObserverContext<'_>,
            _is_finished: bool,
            _cmd: Option<&RaftCmdRequest>,
        ) -> bool {
            !self.skip_persist_when_pre_commit.load(Ordering::SeqCst)
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
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg,
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer: importer.clone(),
            engine: engine.clone(),
            router: router.clone(),
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
        let dk_k1 = keys::data_key(b"k1");
        let dk_k2 = keys::data_key(b"k2");
        let dk_k3 = keys::data_key(b"k3");
        assert_eq!(engine.get_value(&dk_k1).unwrap().unwrap(), b"v1");
        assert_eq!(engine.get_value(&dk_k2).unwrap().unwrap(), b"v1");
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");
        validate(&router, 1, |delegate| {
            assert_eq!(delegate.applied_term, 1);
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
        assert_eq!(apply_res.applied_term, 2);
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
        assert_eq!(apply_res.applied_term, 2);
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
        assert_eq!(apply_res.applied_term, 2);
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

        // The region was rescheduled from normal-priority handler to
        // low-priority handler, so the first apple_res.exec_res should be empty.
        let apply_res = fetch_apply_res(&rx);
        assert!(apply_res.exec_res.is_empty());
        // The entry should be applied now.
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 8);

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

        // The region was rescheduled to normal-priority handler because of
        // nomral put command, so the first apple_res.exec_res should be empty.
        let apply_res = fetch_apply_res(&rx);
        assert!(apply_res.exec_res.is_empty());
        // The region was rescheduled low-priority because of ingest command,
        // only put entry has been applied;
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 9);
        // The region will yield after timeout.
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 10);
        // The third entry should be applied now.
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_term, 3);
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
        // The region was rescheduled to normal-priority handler. Discard the first
        // apply_res.
        fetch_apply_res(&rx);
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.apply_state.get_applied_index(), index as u64);
        assert_eq!(obs.pre_query_count.load(Ordering::SeqCst), index);
        assert_eq!(obs.post_query_count.load(Ordering::SeqCst), index);

        system.shutdown();
    }

    #[test]
    fn test_handle_raft_committed_entries_from_v2() {
        let (_path, engine) = create_tmp_engine("test-delegate");
        let (import_dir, importer) = create_tmp_importer("test-delegate");
        let obs = ApplyObserver::default();
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(obs.clone()));

        let (tx, rx) = mpsc::channel();
        let (region_scheduler, _) = dummy_scheduler();
        let sender = Box::new(TestNotifier { tx });
        let mut config = Config::default();
        config.enable_v2_compatible_learner = true;
        let cfg = Arc::new(VersionTrack::new(config));
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg,
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer: importer.clone(),
            engine: engine.clone(),
            router: router.clone(),
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
        let put_entry = EntryBuilderUsingSimpleWrite::new(1, 1)
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
        let dk_k1 = keys::data_key(b"k1");
        let dk_k2 = keys::data_key(b"k2");
        let dk_k3 = keys::data_key(b"k3");
        assert_eq!(engine.get_value(&dk_k1).unwrap().unwrap(), b"v1");
        assert_eq!(engine.get_value(&dk_k2).unwrap().unwrap(), b"v1");
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");
        validate(&router, 1, |delegate| {
            assert_eq!(delegate.applied_term, 1);
            assert_eq!(delegate.apply_state.get_applied_index(), 1);
        });
        fetch_apply_res(&rx);

        let put_entry = EntryBuilderUsingSimpleWrite::new(2, 2)
            .put_cf(CF_LOCK, b"k1", b"v1")
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 2, vec![put_entry], vec![])));
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.region_id, 1);
        assert_eq!(apply_res.apply_state.get_applied_index(), 2);
        assert_eq!(apply_res.applied_term, 2);
        assert!(apply_res.exec_res.is_empty());
        assert!(apply_res.metrics.written_bytes >= 5);
        assert_eq!(apply_res.metrics.written_keys, 2);
        assert_eq!(apply_res.metrics.size_diff_hint, 5);
        assert_eq!(apply_res.metrics.lock_cf_written_bytes, 5);
        assert_eq!(
            engine.get_value_cf(CF_LOCK, &dk_k1).unwrap().unwrap(),
            b"v1"
        );

        let put_entry = EntryBuilderUsingSimpleWrite::new(3, 2)
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
        assert_eq!(apply_res.applied_term, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 3);

        let put_entry = EntryBuilderUsingSimpleWrite::new(4, 2)
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
        assert_eq!(apply_res.applied_term, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 4);
        // a writebatch should be atomic.
        assert_eq!(engine.get_value(&dk_k3).unwrap().unwrap(), b"v1");

        let put_entry = EntryBuilderUsingSimpleWrite::new(5, 3)
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

        let delete_entry = EntryBuilderUsingSimpleWrite::new(6, 3)
            .delete(b"k5")
            .epoch(1, 3)
            .build();
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

        let delete_range_entry = EntryBuilderUsingSimpleWrite::new(7, 3)
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

        let delete_range_entry = EntryBuilderUsingSimpleWrite::new(8, 3)
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

        // The region was rescheduled from normal-priority handler to
        // low-priority handler, so the first apple_res.exec_res should be empty.
        let apply_res = fetch_apply_res(&rx);
        assert!(apply_res.exec_res.is_empty());
        // The entry should be applied now.
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 8);

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
        let put_ok = EntryBuilderUsingSimpleWrite::new(9, 3)
            .put(&[sst_range.0], &[sst_range.1])
            .epoch(0, 3)
            .build();
        // Add a put above to test flush before ingestion.
        let capture_tx_clone = capture_tx.clone();
        let ingest_ok = EntryBuilderUsingSimpleWrite::new(10, 3)
            .ingest_sst(&meta1)
            .epoch(0, 3)
            .build();
        let ingest_epoch_not_match = EntryBuilderUsingSimpleWrite::new(11, 3)
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

        // The region was rescheduled to normal-priority handler because of
        // nomral put command, so the first apple_res.exec_res should be empty.
        let apply_res = fetch_apply_res(&rx);
        assert!(apply_res.exec_res.is_empty());
        // The region was rescheduled low-priority becasuee of ingest command,
        // only put entry has been applied;
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 9);
        // The region will yield after timeout.
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_term, 3);
        assert_eq!(apply_res.apply_state.get_applied_index(), 10);
        // The third entry should be applied now.
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.applied_term, 3);
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
        // The region was rescheduled to normal-priority handler. Discard the first
        // apply_res.
        fetch_apply_res(&rx);
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.apply_state.get_applied_index(), index as u64);
        assert_eq!(obs.pre_query_count.load(Ordering::SeqCst), index);
        assert_eq!(obs.post_query_count.load(Ordering::SeqCst), index);

        system.shutdown();
    }

    #[test]
    fn test_apply_yield_with_msg_size() {
        let (_path, engine) = create_tmp_engine("test-apply-yield");
        let (_import_dir, importer) = create_tmp_importer("test-apply-yield");
        let obs = ApplyObserver::default();
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(obs));

        let (tx, rx) = mpsc::channel();
        let (region_scheduler, _) = dummy_scheduler();
        let sender = Box::new(TestNotifier { tx });
        let cfg = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg: cfg.clone(),
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer,
            engine,
            router: router.clone(),
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

        let schedule_apply = |idx: u64, count: usize, size: usize| {
            let mut entries = Vec::with_capacity(count);
            for i in 0..count {
                let put_entry = EntryBuilder::new(idx + i as u64, 3)
                    .put(format!("k{:03}", i).as_ref(), &vec![0; size - 4])
                    .epoch(1, 3)
                    .build();
                entries.push(put_entry);
            }
            router.schedule_task(1, Msg::apply(apply(peer_id, 1, 3, entries, vec![])));
        };

        fn approximate_eq(a: u64, b: u64, delta: u64) {
            assert!(
                a >= b - delta && a <= b + delta,
                "left: {}, right: {}, delta: {}",
                a,
                b,
                delta
            );
        }

        // schedule a batch with 512 keys and 64k total size will trigger 2 flush and
        // yield.
        schedule_apply(1, 512, 128);
        let apply_res = fetch_apply_res(&rx);
        approximate_eq(apply_res.metrics.written_bytes, 32768, 2048);
        approximate_eq(apply_res.metrics.written_keys, 256, 15);
        // the second part, note that resume apply not clean up the metrics
        let apply_res = fetch_apply_res(&rx);
        approximate_eq(apply_res.metrics.written_bytes, 32768, 2048);
        approximate_eq(apply_res.metrics.written_keys, 256, 15);

        // update apply yeild size to 64kb
        _ = cfg.update(|c| {
            c.apply_yield_write_size = ReadableSize::kb(64);
            Ok::<(), ()>(())
        });
        // only trigger one time of
        schedule_apply(513, 512, 128);
        let apply_res = fetch_apply_res(&rx);
        approximate_eq(apply_res.metrics.written_bytes, 65536, 4096);
        approximate_eq(apply_res.metrics.written_keys, 512, 20);
    }

    #[test]
    fn test_handle_ingest_sst() {
        let (_path, engine) = create_tmp_engine("test-ingest");
        let (import_dir, importer) = create_tmp_importer("test-ingest");
        let obs = ApplyObserver::default();
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(obs));

        let (tx, rx) = mpsc::channel();
        let (region_scheduler, _) = dummy_scheduler();
        let sender = Box::new(TestNotifier { tx });
        let cfg = {
            let mut cfg = Config::default();
            cfg.apply_batch_system.pool_size = 1;
            cfg.apply_batch_system.low_priority_pool_size = 0;
            Arc::new(VersionTrack::new(cfg))
        };
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg,
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer: importer.clone(),
            engine: engine.clone(),
            router: router.clone(),
            store_id: 1,
            pending_create_peers,
        };
        system.spawn("test-ingest".to_owned(), builder);

        let mut reg = Registration {
            id: 1,
            ..Default::default()
        };
        reg.region.set_id(1);
        reg.region.mut_peers().push(new_peer(1, 1));
        reg.region.set_start_key(b"k1".to_vec());
        reg.region.set_end_key(b"k2".to_vec());
        reg.region.mut_region_epoch().set_conf_ver(1);
        reg.region.mut_region_epoch().set_version(3);
        router.schedule_task(1, Msg::Registration(reg));

        // Test whether put commands and ingest commands are applied to engine in a
        // correct order. We will generate 5 entries which are put, ingest, put,
        // ingest, put respectively. For a same key, it can exist in multiple
        // entries or in a single entries. We will test all all the possible
        // keys exsiting combinations.
        let mut keys = Vec::new();
        let keys_count = 1 << 5;
        for i in 0..keys_count {
            keys.push(format!("k1/{:02}", i).as_bytes().to_vec());
        }
        let mut expected_vals = Vec::new();
        expected_vals.resize(keys_count, Vec::new());

        let entry1 = {
            let mut entry = EntryBuilder::new(1, 1);
            for i in 0..keys_count {
                if (i & 1) > 0 {
                    entry = entry.put(&keys[i], b"1");
                    expected_vals[i] = b"1".to_vec();
                }
            }
            entry.epoch(1, 3).build()
        };
        let entry2 = {
            let mut kvs: Vec<(&[u8], &[u8])> = Vec::new();
            for i in 0..keys_count {
                if (i & 2) > 0 {
                    kvs.push((&keys[i], b"2"));
                    expected_vals[i] = b"2".to_vec();
                }
            }
            let sst_path = import_dir.path().join("test.sst");
            let (mut meta, data) = gen_sst_file_with_kvs(sst_path, &kvs);
            meta.set_region_id(1);
            meta.mut_region_epoch().set_conf_ver(1);
            meta.mut_region_epoch().set_version(3);
            let mut file = importer.create(&meta).unwrap();
            file.append(&data).unwrap();
            file.finish().unwrap();
            EntryBuilder::new(2, 1)
                .ingest_sst(&meta)
                .epoch(1, 3)
                .build()
        };
        let entry3 = {
            let mut entry = EntryBuilder::new(3, 1);
            for i in 0..keys_count {
                if (i & 4) > 0 {
                    entry = entry.put(&keys[i], b"3");
                    expected_vals[i] = b"3".to_vec();
                }
            }
            entry.epoch(1, 3).build()
        };
        let entry4 = {
            let mut kvs: Vec<(&[u8], &[u8])> = Vec::new();
            for i in 0..keys_count {
                if (i & 8) > 0 {
                    kvs.push((&keys[i], b"4"));
                    expected_vals[i] = b"4".to_vec();
                }
            }
            let sst_path = import_dir.path().join("test2.sst");
            let (mut meta, data) = gen_sst_file_with_kvs(sst_path, &kvs);
            meta.set_region_id(1);
            meta.mut_region_epoch().set_conf_ver(1);
            meta.mut_region_epoch().set_version(3);
            let mut file = importer.create(&meta).unwrap();
            file.append(&data).unwrap();
            file.finish().unwrap();
            EntryBuilder::new(4, 1)
                .ingest_sst(&meta)
                .epoch(1, 3)
                .build()
        };
        let entry5 = {
            let mut entry = EntryBuilder::new(5, 1);
            for i in 0..keys_count {
                if (i & 16) > 0 {
                    entry = entry.put(&keys[i], b"5");
                    expected_vals[i] = b"5".to_vec();
                }
            }
            entry.epoch(1, 3).build()
        };

        let (capture_tx, capture_rx) = mpsc::channel();
        router.schedule_task(
            1,
            Msg::apply(apply(
                1,
                1,
                1,
                vec![entry1, entry2, entry3],
                vec![
                    cb(1, 1, capture_tx.clone()),
                    cb(2, 1, capture_tx.clone()),
                    cb(3, 1, capture_tx.clone()),
                ],
            )),
        );
        router.schedule_task(
            1,
            Msg::apply(apply(
                1,
                1,
                1,
                vec![entry4, entry5],
                vec![cb(4, 1, capture_tx.clone()), cb(5, 1, capture_tx)],
            )),
        );
        for _ in 0..3 {
            let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert!(!resp.get_header().has_error(), "{:?}", resp);
        }
        for _ in 0..2 {
            let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert!(!resp.get_header().has_error(), "{:?}", resp);
        }
        let mut res = fetch_apply_res(&rx);
        // There are five entries [put, ingest, put, ingest, put] in one region.
        // so the apply results should be notified at index 2/4.
        if res.apply_state.get_applied_index() == 2 {
            res = fetch_apply_res(&rx);
        }
        if res.apply_state.get_applied_index() == 4 {
            res = fetch_apply_res(&rx);
        }
        assert_eq!(res.apply_state.get_applied_index(), 5);

        // Verify the engine keys.
        for i in 1..keys_count {
            let dk = keys::data_key(&keys[i]);
            assert_eq!(engine.get_value(&dk).unwrap().unwrap(), &expected_vals[i]);
        }
    }

    #[test]
    fn test_bucket_version_change_in_try_batch() {
        let (_path, engine) = create_tmp_engine("test-bucket");
        let (_, importer) = create_tmp_importer("test-bucket");
        let obs = ApplyObserver::default();
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(obs));

        let (tx, rx) = mpsc::channel();
        let (region_scheduler, _) = dummy_scheduler();
        let sender = Box::new(TestNotifier { tx });
        let cfg = {
            let mut cfg = Config::default();
            cfg.apply_batch_system.pool_size = 1;
            cfg.apply_batch_system.low_priority_pool_size = 0;
            Arc::new(VersionTrack::new(cfg))
        };
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg,
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer,
            engine,
            router: router.clone(),
            store_id: 1,
            pending_create_peers,
        };
        system.spawn("test-bucket".to_owned(), builder);

        let mut reg = Registration {
            id: 1,
            ..Default::default()
        };
        reg.region.set_id(1);
        reg.region.mut_peers().push(new_peer(1, 1));
        reg.region.set_start_key(b"k1".to_vec());
        reg.region.set_end_key(b"k2".to_vec());
        reg.region.mut_region_epoch().set_conf_ver(1);
        reg.region.mut_region_epoch().set_version(3);
        router.schedule_task(1, Msg::Registration(reg));

        let entry1 = {
            let mut entry = EntryBuilder::new(1, 1);
            entry = entry.put(b"key1", b"value1");
            entry.epoch(1, 3).build()
        };

        let entry2 = {
            let mut entry = EntryBuilder::new(2, 1);
            entry = entry.put(b"key2", b"value2");
            entry.epoch(1, 3).build()
        };

        let (capture_tx, _capture_rx) = mpsc::channel();
        let mut apply1 = apply(1, 1, 1, vec![entry1], vec![cb(1, 1, capture_tx.clone())]);
        let bucket_meta = BucketMeta {
            region_id: 1,
            region_epoch: RegionEpoch::default(),
            version: 1,
            keys: vec![b"".to_vec(), b"".to_vec()],
            sizes: vec![0, 0],
        };
        apply1.bucket_meta = Some(Arc::new(bucket_meta));

        let mut apply2 = apply(1, 1, 1, vec![entry2], vec![cb(2, 1, capture_tx)]);
        let mut bucket_meta2 = BucketMeta {
            region_id: 1,
            region_epoch: RegionEpoch::default(),
            version: 2,
            keys: vec![b"".to_vec(), b"".to_vec()],
            sizes: vec![0, 0],
        };
        bucket_meta2.version = 2;
        apply2.bucket_meta = Some(Arc::new(bucket_meta2));

        router.schedule_task(1, Msg::apply(apply1));
        router.schedule_task(1, Msg::apply(apply2));

        let res = fetch_apply_res(&rx);
        let bucket_version = res.bucket_stat.unwrap().meta.version;

        assert_eq!(bucket_version, 2);

        validate(&router, 1, |delegate| {
            let bucket_version = delegate.buckets.as_ref().unwrap().meta.version;
            assert_eq!(bucket_version, 2);
        });
    }

    #[test]
    fn test_exec_observer() {
        let (_path, engine) = create_tmp_engine("test-exec-observer");
        let (import_dir, importer) = create_tmp_importer("test-exec-observer");
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        let obs = ApplyObserver::default();
        host.registry
            .register_admin_observer(1, BoxAdminObserver::new(obs.clone()));
        host.registry
            .register_region_change_observer(1, BoxRegionChangeObserver::new(obs.clone()));
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(obs.clone()));

        let (tx, rx) = mpsc::channel();
        let (region_scheduler, _) = dummy_scheduler();
        let sender = Box::new(TestNotifier { tx });
        let cfg = Config::default();
        let (router, mut system) = create_apply_batch_system(&cfg, None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-exec-observer".to_owned(),
            cfg: Arc::new(VersionTrack::new(cfg)),
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer: importer.clone(),
            engine: engine.clone(),
            router: router.clone(),
            store_id: 1,
            pending_create_peers,
        };
        system.spawn("test-exec-observer".to_owned(), builder);

        let peer_id = 3;
        let mut reg = Registration {
            id: peer_id,
            ..Default::default()
        };
        reg.region.set_id(1);
        reg.region.mut_peers().push(new_peer(1, peer_id));
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_conf_ver(1);
        reg.region.mut_region_epoch().set_version(3);
        router.schedule_task(1, Msg::Registration(reg));

        obs.skip_persist_when_pre_commit
            .store(true, Ordering::SeqCst);
        let mut index_id = 1;
        let put_entry = EntryBuilder::new(index_id, 1)
            .put(b"k1", b"v1")
            .put(b"k2", b"v2")
            .put(b"k3", b"v3")
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 1, vec![put_entry], vec![])));
        let apply_res = fetch_apply_res(&rx);

        // We don't persist at `finish_for`, since we disabled `pre_persist`.
        let state: RaftApplyState = engine
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        assert_eq!(
            apply_res.apply_state.get_applied_index(),
            state.get_applied_index() + 1
        );
        obs.skip_persist_when_pre_commit
            .store(false, Ordering::SeqCst);

        // Phase 1: we test if pre_exec will filter execution of commands correctly.
        index_id += 1;
        let compact_entry = EntryBuilder::new(index_id, 1)
            .compact_log(index_id - 1, 2)
            .epoch(1, 3)
            .build();
        // Filter CompactLog
        obs.filter_compact_log.store(true, Ordering::SeqCst);
        router.schedule_task(
            1,
            Msg::apply(apply(peer_id, 1, 1, vec![compact_entry], vec![])),
        );
        let apply_res = fetch_apply_res(&rx);
        // applied_index can still be advanced.
        assert_eq!(apply_res.apply_state.get_applied_index(), index_id);
        assert_eq!(apply_res.applied_term, 1);
        // Executing CompactLog is filtered and takes no effect.
        assert_eq!(apply_res.exec_res.len(), 0);
        assert_eq!(apply_res.apply_state.get_truncated_state().get_index(), 0);

        // We persist at `finish_for`, since we enabled `pre_persist`.
        let state: RaftApplyState = engine
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        assert_eq!(
            apply_res.apply_state.get_applied_index(),
            state.get_applied_index()
        );

        index_id += 1;
        // Don't filter CompactLog
        obs.filter_compact_log.store(false, Ordering::SeqCst);
        let compact_entry = EntryBuilder::new(index_id, 1)
            .compact_log(index_id - 1, 2)
            .epoch(1, 3)
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(peer_id, 1, 1, vec![compact_entry], vec![])),
        );
        let apply_res = fetch_apply_res(&rx);
        // applied_index can still be advanced.
        assert_eq!(apply_res.apply_state.get_applied_index(), index_id);
        assert_eq!(apply_res.applied_term, 1);
        // We can get exec result of CompactLog.
        assert_eq!(apply_res.exec_res.len(), 1);
        assert_eq!(
            apply_res.apply_state.get_truncated_state().get_index(),
            index_id - 1
        );

        index_id += 1;
        obs.filter_consistency_check.store(true, Ordering::SeqCst);
        let compute_hash_entry = EntryBuilder::new(index_id, 1).compute_hash(vec![]).build();
        router.schedule_task(
            1,
            Msg::apply(apply(peer_id, 1, 1, vec![compute_hash_entry], vec![])),
        );
        let apply_res = fetch_apply_res(&rx);
        // applied_index can still be advanced.
        assert_eq!(apply_res.apply_state.get_applied_index(), index_id);
        assert_eq!(apply_res.applied_term, 1);
        // We can't get exec result of ComputeHash.
        assert_eq!(apply_res.exec_res.len(), 0);
        obs.filter_consistency_check.store(false, Ordering::SeqCst);

        // Phase 2: we test if post_exec will persist when need.
        // We choose BatchSplit in order to make sure `modified_region` is filled.
        index_id += 1;
        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(true);
        splits.mut_requests().push(new_split_req(b"k2", 8, vec![7]));
        let split = EntryBuilder::new(index_id, 1)
            .split(splits)
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 1, vec![split], vec![])));
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.apply_state.get_applied_index(), index_id);
        assert_eq!(apply_res.applied_term, 1);
        let (r1, r8) = if let ExecResult::SplitRegion {
            regions,
            derived: _,
            new_split_regions: _,
            share_source_region_size: _,
        } = apply_res.exec_res.front().unwrap()
        {
            let r8 = regions.first().unwrap();
            let r1 = regions.get(1).unwrap();
            assert_eq!(r8.get_id(), 8);
            assert_eq!(r1.get_id(), 1);
            (r1, r8)
        } else {
            panic!("error split exec_res");
        };

        index_id += 1;
        let merge = EntryBuilder::new(index_id, 1)
            .prepare_merge(r8.clone())
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 1, vec![merge], vec![])));
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.apply_state.get_applied_index(), index_id);
        assert_eq!(apply_res.applied_term, 1);
        // PrepareMerge will trigger commit.
        let state: RaftApplyState = engine
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        assert_eq!(apply_res.apply_state, state);

        // Phase 3: we test if we can delay deletion of some sst files.
        let r1_epoch = r1.get_region_epoch();
        index_id += 1;
        let kvs: Vec<(&[u8], &[u8])> = vec![(b"k3", b"2")];
        let sst_path = import_dir.path().join("test.sst");
        let (mut meta, data) = gen_sst_file_with_kvs(&sst_path, &kvs);
        meta.set_region_id(1);
        meta.set_region_epoch(r1_epoch.clone());
        let mut file = importer.create(&meta).unwrap();
        file.append(&data).unwrap();
        file.finish().unwrap();
        let src = sst_path.clone();
        let dst = file.get_import_path().save.to_str().unwrap();
        std::fs::copy(src, dst).unwrap();
        assert!(sst_path.as_path().exists());
        let ingestsst = EntryBuilder::new(index_id, 1)
            .ingest_sst(&meta)
            .epoch(r1_epoch.get_conf_ver(), r1_epoch.get_version())
            .build();

        obs.delay_remove_ssts.store(true, Ordering::SeqCst);
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 1, vec![ingestsst], vec![])));
        fetch_apply_res(&rx);
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.exec_res.len(), 1);
        assert_eq!(obs.last_pending_handle_sst_count.load(Ordering::SeqCst), 0);
        assert_eq!(obs.last_delete_sst_count.load(Ordering::SeqCst), 0);
        assert_eq!(obs.last_pending_delete_sst_count.load(Ordering::SeqCst), 1);

        index_id += 1;
        let ingestsst = EntryBuilder::new(index_id, 1)
            .ingest_sst(&meta)
            .epoch(r1_epoch.get_conf_ver(), r1_epoch.get_version())
            .build();
        obs.delay_remove_ssts.store(false, Ordering::SeqCst);
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 1, vec![ingestsst], vec![])));
        let apply_res = fetch_apply_res(&rx);
        assert_eq!(apply_res.exec_res.len(), 1);
        assert_eq!(obs.last_pending_handle_sst_count.load(Ordering::SeqCst), 0);
        assert_eq!(obs.last_delete_sst_count.load(Ordering::SeqCst), 1);
        assert_eq!(obs.last_pending_delete_sst_count.load(Ordering::SeqCst), 1);

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
        let (router, mut system) = create_apply_batch_system(&cfg, None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg: Arc::new(VersionTrack::new(cfg)),
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer,
            engine,
            router: router.clone(),
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
        let cmd_batch = cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(cmd_batch.len(), 1);
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
        let observe_handle = ObserveHandle::with_id(1);
        router.schedule_task(
            1,
            Msg::Change {
                region_epoch: region_epoch.clone(),
                cmd: ChangeObserver::from_cdc(1, observe_handle.clone()),
                cb: Callback::read(Box::new(|resp: ReadResponse<KvTestSnapshot>| {
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
        let cmd_batch = cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(cmd_batch.cdc_id, ObserveHandle::with_id(0).id);
        assert_eq!(cmd_batch.rts_id, ObserveHandle::with_id(0).id);
        assert_eq!(cmd_batch.pitr_id, ObserveHandle::with_id(0).id);

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
        let cmd_batch = cmdbatch_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(cmd_batch.cdc_id, observe_handle.id);
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
        observe_handle.stop_observing();

        let observe_handle = ObserveHandle::new();
        let put_entry = EntryBuilder::new(6, 2)
            .put(b"k2", b"v2")
            .epoch(1, 3)
            .build();
        router.schedule_task(1, Msg::apply(apply(peer_id, 1, 2, vec![put_entry], vec![])));

        // Must response a RegionNotFound error.
        router.schedule_task(
            2,
            Msg::Change {
                region_epoch,
                cmd: ChangeObserver::from_cdc(2, observe_handle),
                cb: Callback::read(Box::new(|resp: ReadResponse<_>| {
                    assert!(
                        resp.response
                            .get_header()
                            .get_error()
                            .has_region_not_found()
                    );
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
        check_sst_for_ingestion(&sst, &region).unwrap_err();
        sst.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        sst.set_cf_name(CF_DEFAULT.to_owned());
        check_sst_for_ingestion(&sst, &region).unwrap();
        sst.set_cf_name("test".to_owned());
        check_sst_for_ingestion(&sst, &region).unwrap_err();
        sst.set_cf_name(CF_WRITE.to_owned());
        check_sst_for_ingestion(&sst, &region).unwrap();

        // Check region id
        region.set_id(1);
        sst.set_region_id(2);
        check_sst_for_ingestion(&sst, &region).unwrap_err();
        sst.set_region_id(1);
        check_sst_for_ingestion(&sst, &region).unwrap();

        // Check region epoch
        region.mut_region_epoch().set_conf_ver(1);
        check_sst_for_ingestion(&sst, &region).unwrap_err();
        sst.mut_region_epoch().set_conf_ver(1);
        check_sst_for_ingestion(&sst, &region).unwrap();
        region.mut_region_epoch().set_version(1);
        check_sst_for_ingestion(&sst, &region).unwrap_err();
        sst.mut_region_epoch().set_version(1);
        check_sst_for_ingestion(&sst, &region).unwrap();

        // Check region range
        region.set_start_key(vec![2]);
        region.set_end_key(vec![8]);
        sst.mut_range().set_start(vec![1]);
        sst.mut_range().set_end(vec![8]);
        check_sst_for_ingestion(&sst, &region).unwrap_err();
        sst.mut_range().set_start(vec![2]);
        check_sst_for_ingestion(&sst, &region).unwrap_err();
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
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg,
            sender,
            importer,
            region_scheduler,
            coprocessor_host: host,
            engine: engine.clone(),
            router: router.clone(),
            store_id: 2,
            pending_create_peers,
        };
        system.spawn("test-split".to_owned(), builder);

        router.schedule_task(1, Msg::Registration(reg.dup()));
        let observe_handle = ObserveHandle::new();
        router.schedule_task(
            1,
            Msg::Change {
                region_epoch: region_epoch.clone(),
                cmd: ChangeObserver::from_cdc(1, observe_handle.clone()),
                cb: Callback::read(Box::new(|resp: ReadResponse<_>| {
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

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"", 8, vec![9, 10, 11]));
        let resp = exec_split(&router, splits.clone());
        // Empty key will not in any region exclusively.
        assert!(error_msg(&resp).contains("missing split key"), "{:?}", resp);

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
        observe_handle.stop_observing();
        router.schedule_task(
            1,
            Msg::Change {
                region_epoch,
                cmd: ChangeObserver::from_cdc(1, observe_handle),
                cb: Callback::read(Box::new(move |resp: ReadResponse<_>| {
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

    // When a peer is removed, it is necessary to update its apply state because
    // this peer may be simultaneously taking a snapshot. An outdated apply state
    // invalidates the coprocessor cache assumption (apply state must match data
    // in the snapshot) and potentially lead to a violation of linearizability
    // (returning stale cache).
    #[test]
    fn test_conf_change_remove_node_update_apply_state() {
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
        let peers = vec![new_peer(2, 3), new_peer(4, 5), new_learner_peer(6, 7)];
        reg.region.set_peers(peers.into());
        let (tx, apply_res_rx) = mpsc::channel();
        let sender = Box::new(TestNotifier { tx });
        let coprocessor_host = CoprocessorHost::<KvTestEngine>::default();
        let (region_scheduler, _) = dummy_scheduler();
        let cfg = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "test-store".to_owned(),
            cfg,
            sender,
            importer,
            region_scheduler,
            coprocessor_host,
            engine: engine.clone(),
            router: router.clone(),
            store_id: 2,
            pending_create_peers,
        };
        system.spawn("test-conf-change".to_owned(), builder);

        router.schedule_task(1, Msg::Registration(reg.dup()));

        let mut index_id = 1;
        let epoch = reg.region.get_region_epoch().to_owned();

        // Write some data.
        let (capture_tx, capture_rx) = mpsc::channel();
        let put_entry = EntryBuilder::new(index_id, 1)
            .put(b"k1", b"v1")
            .epoch(epoch.get_conf_ver(), epoch.get_version())
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                1,
                vec![put_entry],
                vec![cb(index_id, 1, capture_tx)],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        let initial_state: RaftApplyState = engine
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap();
        assert_ne!(initial_state.get_applied_index(), 0);
        match apply_res_rx.recv_timeout(Duration::from_secs(3)) {
            Ok(PeerMsg::ApplyRes {
                res: TaskRes::Apply(apply_res),
            }) => assert_eq!(apply_res.apply_state, initial_state),
            e => panic!("unexpected result: {:?}", e),
        }
        index_id += 1;

        // Remove itself.
        let (capture_tx, capture_rx) = mpsc::channel();
        let mut remove_node = ChangePeerRequest::default();
        remove_node.set_change_type(ConfChangeType::RemoveNode);
        remove_node.set_peer(new_peer(2, 3));
        let conf_change = EntryBuilder::new(index_id, 1)
            .conf_change(vec![remove_node])
            .epoch(epoch.get_conf_ver(), epoch.get_version())
            .build();
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                1,
                vec![conf_change],
                vec![cb_conf_change(index_id, 1, capture_tx)],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);

        let apply_state: RaftApplyState = engine
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap();
        match apply_res_rx.recv_timeout(Duration::from_secs(3)) {
            Ok(PeerMsg::ApplyRes {
                res: TaskRes::Apply(apply_res),
            }) => assert_eq!(apply_res.apply_state, apply_state),
            e => panic!("unexpected result: {:?}", e),
        }
        assert!(
            apply_state.get_applied_index() > initial_state.get_applied_index(),
            "\n{:?}\n{:?}",
            apply_state,
            initial_state
        );

        system.shutdown();
    }

    #[test]
    fn pending_cmd_leak() {
        let res = panic_hook::recover_safe(|| {
            let _cmd = PendingCmd::new(1, 1, Callback::<KvTestSnapshot>::None);
        });
        res.unwrap_err();
    }

    #[test]
    fn pending_cmd_leak_dtor_not_abort() {
        let res = panic_hook::recover_safe(|| {
            let _cmd = PendingCmd::new(1, 1, Callback::<KvTestSnapshot>::None);
            panic!("Don't abort");
            // It would abort and fail if there was a double-panic in PendingCmd
            // dtor.
        });
        res.unwrap_err();
    }

    #[test]
    fn flashback_need_to_be_applied() {
        let (_path, engine) = create_tmp_engine("flashback_need_to_be_applied");
        let (_, importer) = create_tmp_importer("flashback_need_to_be_applied");
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(ApplyObserver::default()));

        let (tx, rx) = mpsc::channel();
        let (region_scheduler, _) = dummy_scheduler();
        let sender = Box::new(TestNotifier { tx });
        let cfg = Arc::new(VersionTrack::new(Config::default()));
        let (router, mut system) = create_apply_batch_system(&cfg.value(), None);
        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let builder = super::Builder::<KvTestEngine> {
            tag: "flashback_need_to_be_applied".to_owned(),
            cfg,
            sender,
            region_scheduler,
            coprocessor_host: host,
            importer,
            engine: engine.clone(),
            router: router.clone(),
            store_id: 1,
            pending_create_peers,
        };
        system.spawn("flashback_need_to_be_applied".to_owned(), builder);

        let peer_id = 3;
        let mut reg = Registration {
            id: peer_id,
            ..Default::default()
        };
        reg.region.set_id(1);
        reg.region.mut_peers().push(new_peer(2, 3));
        reg.region.mut_region_epoch().set_conf_ver(1);
        reg.region.mut_region_epoch().set_version(3);
        reg.region.set_is_in_flashback(true);
        router.schedule_task(1, Msg::Registration(reg));

        let (capture_tx, capture_rx) = mpsc::channel();
        let mut region_state = RegionLocalState::default();
        region_state.mut_region().set_is_in_flashback(false);
        let region_state_key = keys::region_state_key(1);
        engine
            .put_msg_cf(CF_RAFT, &region_state_key, &region_state)
            .unwrap();
        // Check for not flashback request.
        let mut cmd = AdminRequest::default();
        cmd.set_cmd_type(AdminCmdType::TransferLeader);
        let mut flashback_req = EntryBuilder::new(1, 1).epoch(1, 3);
        flashback_req.req.set_admin_request(cmd.clone());
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                1,
                vec![flashback_req.build()],
                vec![cb(1, 1, capture_tx.clone())],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().get_error().has_flashback_in_progress());
        // Check for flashback request.
        cmd.set_cmd_type(AdminCmdType::PrepareFlashback);
        region_state.mut_region().set_is_in_flashback(false);
        let mut flashback_req = EntryBuilder::new(2, 2).epoch(1, 3);
        flashback_req.req.set_admin_request(cmd.clone());
        flashback_req
            .req
            .mut_header()
            .set_flags(WriteBatchFlags::FLASHBACK.bits());
        router.schedule_task(
            1,
            Msg::apply(apply(
                peer_id,
                1,
                2,
                vec![flashback_req.build()],
                vec![cb(2, 2, capture_tx)],
            )),
        );
        let resp = capture_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);

        rx.recv_timeout(Duration::from_millis(500)).unwrap();
        system.shutdown();
    }

    fn new_batch_split_request(keys: Vec<Vec<u8>>) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::BatchSplit);
        for key in keys {
            let mut split_req = SplitRequest::default();
            split_req.set_split_key(key);
            split_req.set_new_peer_ids(vec![1]);
            req.mut_splits().mut_requests().push(split_req);
        }
        req
    }

    #[test]
    fn test_validate_batch_split() {
        let mut region = Region::default();
        region.set_start_key(b"k05".to_vec());
        region.set_end_key(b"k10".to_vec());
        region.set_peers(vec![new_peer(1, 2)].into());

        let missing_error = "missing split requests";
        let invalid_error = "invalid split request";
        let not_in_region_error = "not in region";
        let empty_error = "missing split key";
        let peer_id_error = "invalid new peer id count";

        // case: split is deprecated
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::Split);
        let mut split_req = SplitRequest::default();
        split_req.set_split_key(b"k06".to_vec());
        req.set_split(split_req);
        assert!(
            validate_batch_split(&req, &region)
                .unwrap_err()
                .to_string()
                .contains(missing_error)
        );

        // case: missing peer ids
        let mut req = new_batch_split_request(vec![b"k07".to_vec()]);
        req.mut_splits()
            .mut_requests()
            .get_mut(0)
            .unwrap()
            .new_peer_ids
            .clear();
        assert!(
            validate_batch_split(&req, &region)
                .unwrap_err()
                .to_string()
                .contains(peer_id_error)
        );

        let fail_cases = vec![
            // case: default admin request should be rejected
            (vec![], missing_error),
            // case: empty split key
            (vec![vec![]], empty_error),
            // case: out of order split keys
            (
                vec![b"k07".to_vec(), b"k08".to_vec(), b"k06".to_vec()],
                invalid_error,
            ),
            // case: split keys are not in region range
            (
                vec![b"k04".to_vec(), b"k07".to_vec(), b"k08".to_vec()],
                invalid_error,
            ),
            // case: split keys are not in region range
            (
                vec![b"k06".to_vec(), b"k07".to_vec(), b"k11".to_vec()],
                not_in_region_error,
            ),
            // case: duplicated split keys
            (vec![b"k06".to_vec(), b"k06".to_vec()], invalid_error),
        ];

        for (split_keys, fail_str) in fail_cases {
            let req = if split_keys.is_empty() {
                AdminRequest::default()
            } else {
                new_batch_split_request(split_keys)
            };
            assert!(
                validate_batch_split(&req, &region)
                    .unwrap_err()
                    .to_string()
                    .contains(fail_str)
            );
        }

        // case: pass the validation
        let req = new_batch_split_request(vec![b"k06".to_vec(), b"k07".to_vec(), b"k08".to_vec()]);
        validate_batch_split(&req, &region).unwrap();
    }
}
