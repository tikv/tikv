// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::errors::*;
use crate::store::cmd_resp::{bind_term, err_resp};
use crate::{mvcc, RaftRouter};
use bytes::{Buf, BufMut, Bytes};
use fail::fail_point;
use futures_util::AsyncReadExt;
use kvengine::{Item, SnapAccess};
use kvproto::metapb;
use kvproto::metapb::{PeerRole, Region};
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, AdminResponse, ChangePeerRequest, RaftCmdRequest, RaftCmdResponse,
    RaftResponseHeader,
};
use kvproto::raft_serverpb::{MergeState, RaftTruncatedState, PeerState};
use protobuf::ProtobufEnum;
use raft::eraftpb::{self, ConfChange, ConfChangeType, ConfChangeV2, EntryType};
use raft::{SoftState, StateRole};
use raftstore::store::fsm::metrics::*;
use raftstore::store::memory::*;
use raftstore::store::metrics::*;
use raftstore::store::util as outil;
use raftstore::store::QueryStats;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::vec::Drain;
use tikv_alloc::trace::TraceEvent;
use tikv_util::worker::Scheduler;
use tikv_util::{box_err, defer, error, info};
use time::Timespec;

pub(crate) struct PendingCmd {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) cb: Callback,
}

impl PendingCmd {
    pub(crate) fn new(index: u64, term: u64, cb: Callback) -> Self {
        Self { index, term, cb }
    }
}

#[derive(Default)]
pub(crate) struct PendingCmdQueue {
    pub(crate) normals: VecDeque<PendingCmd>,
    pub(crate) conf_change: Option<PendingCmd>,
}

impl PendingCmdQueue {
    pub(crate) fn pop_normal(&mut self, term: u64) -> Option<PendingCmd> {
        if self.normals.len() == 0 {
            return None;
        }
        if self.normals[0].term > term {
            return None;
        };
        self.normals.pop_front()
    }

    pub(crate) fn append_normal(&mut self, cmd: PendingCmd) {
        self.normals.push_back(cmd)
    }

    pub(crate) fn take_conf_change(&mut self) -> Option<PendingCmd> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        self.conf_change.take()
    }

    // TODO: seems we don't need to separate conf change from normal entries.
    pub(crate) fn set_conf_change(&mut self, cmd: PendingCmd) {
        self.conf_change = Some(cmd)
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

#[derive(Debug, Clone)]
pub struct NewSplitPeer {
    pub peer_id: u64,
    // `None` => success,
    // `Some(s)` => fail due to `s`.
    pub result: Option<String>,
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
        new_split_regions: HashMap<u64, NewSplitPeer>,
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
    },
    VerifyHash {
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    },
    DeleteRange {
        ranges: Vec<Range>,
    },
}

pub(crate) enum ApplyResult {
    None,
    Yield,
    /// Additional result that needs to be sent back to raftstore.
    Res(ExecResult),
    /// It is unable to apply the `CommitMerge` until the source peer
    /// has applied to the required position and sets the atomic boolean
    /// to true.
    WaitMergeSource(Arc<AtomicU64>),
}

#[derive(Default)]
pub(crate) struct ApplyExecContext {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) apply_state: RaftApplyState,
}

impl ApplyExecContext {
    pub(crate) fn new(index: u64, term: u64, apply_state: RaftApplyState) -> Self {
        Self {
            index,
            term,
            apply_state,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Proposal {
    pub(crate) is_conf_change: bool,
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) cb: Callback,

    /// `propose_time` is set to the last time when a peer starts to renew lease.
    pub propose_time: Option<Timespec>,
    pub must_pass_epoch_check: bool,
}

pub(crate) struct ApplyMsgs {
    pub(crate) msgs: Vec<ApplyMsg>,
}

#[derive(Default)]
pub(crate) struct ApplyBatch {
    pub(crate) msgs: Vec<ApplyMsg>,
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

struct YieldState {
    /// All of the entries that need to continue to be applied after
    /// the source peer has applied its logs.
    pending_entries: Vec<eraftpb::Entry>,
    /// All of messages that need to continue to be handled after
    /// the source peer has applied its logs and pending entries
    /// are all handled.
    pending_msgs: Vec<ApplyMsg>,

    /// Cache heap size for itself.
    heap_size: Option<usize>,
}

impl Debug for YieldState {
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

/// The Applier of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// The raft worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding applier to
/// handle the apply task to make the code logic more clear.
pub(crate) struct Applier {
    pub(crate) peer_idx: usize,
    pub(crate) term: u64,
    pub(crate) region: metapb::Region,
    pub(crate) tag: String,

    /// If the applier should be stopped from polling.
    /// A applier can be stopped in conf change, merge or requested by destroy message.
    pub(crate) stopped: bool,
    /// Set to true when removing itself because of `ConfChangeType::RemoveNode`, and then
    /// any following committed logs in same Ready should be applied failed.
    pub(crate) pending_remove: bool,

    /// The commands waiting to be committed and applied
    pub(crate) pending_cmds: PendingCmdQueue,

    /// Marks the applier as merged by CommitMerge.
    pub(crate) merged: bool,

    /// Indicates the peer is in merging, if that compact log won't be performed.
    pub(crate) is_merging: bool,

    /// Records the epoch version after the last merge.
    pub(crate) last_merge_version: u64,

    /// A temporary state that keeps track of the progress of the source peer state when
    /// CommitMerge is unable to be executed.
    pub(crate) wait_merge_state: Option<WaitSourceMergeState>,

    /// ID of last region that reports ready.
    pub(crate) ready_source_region: bool,

    pub(crate) apply_state: RaftApplyState,

    /// The local metrics, and it will be flushed periodically.
    pub(crate) metrics: ApplyMetrics,

    pub(crate) lock_cache: HashMap<Bytes, Bytes>,

    pub(crate) snap: Option<Arc<SnapAccess>>,

    pub(crate) yield_state: Option<YieldState>,

    pub(crate) recover_split: bool,
}

impl Applier {
    pub(crate) fn new_from_peer(peer: &PeerFsm) -> Self {
        let reg = MsgRegistration::new(&peer.peer);
        Self::new_from_reg(reg)
    }

    pub(crate) fn new_from_reg(reg: MsgRegistration) -> Self {
        let peer_idx = get_peer_idx_by_store_id(&reg.region, reg.peer.store_id);
        let tag = Self::make_tag(&reg.region);
        Self {
            peer_idx,
            term: reg.term,
            region: reg.region,
            tag,
            stopped: false,
            pending_remove: false,
            pending_cmds: Default::default(),
            merged: false,
            is_merging: false,
            last_merge_version: 0,
            wait_merge_state: None,
            ready_source_region: false,
            apply_state: reg.apply_state,
            metrics: Default::default(),
            lock_cache: Default::default(),
            snap: None,
            yield_state: None,
            recover_split: false,
        }
    }

    fn make_tag(region: &metapb::Region) -> String {
        format!(
            "({}:{})",
            region.get_id(),
            region.get_region_epoch().get_version()
        )
    }

    pub(crate) fn new_for_recover(
        store_id: u64,
        region: metapb::Region,
        snap: Arc<SnapAccess>,
        apply_state: RaftApplyState,
    ) -> Self {
        let peer_idx = get_peer_idx_by_store_id(&region, store_id);
        let tag = Self::make_tag(&region);
        Self {
            peer_idx,
            term: RAFT_INIT_LOG_TERM,
            region,
            tag,
            stopped: false,
            pending_remove: false,
            pending_cmds: Default::default(),
            merged: false,
            is_merging: false,
            last_merge_version: 0,
            wait_merge_state: None,
            ready_source_region: false,
            apply_state,
            metrics: Default::default(),
            lock_cache: Default::default(),
            snap: Some(snap),
            yield_state: None,
            recover_split: false,
        }
    }

    pub(crate) fn get_peer(&self) -> &metapb::Peer {
        &self.region.peers[self.peer_idx]
    }

    pub(crate) fn id(&self) -> u64 {
        self.get_peer().get_id()
    }

    pub(crate) fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub(crate) fn commit_lock(
        &mut self,
        kv: &kvengine::Engine,
        wb: &mut kvengine::WriteBatch,
        key: &[u8],
        val: &[u8],
        commit_ts: u64,
    ) {
        let mut lock: mvcc::Lock;
        if val.len() == 0 {
            let val = self.get_lock_for_commit(kv, key, commit_ts);
            if val.len() == 0 {
                return;
            }
            lock = mvcc::Lock::decode(val.chunk());
        } else {
            lock = mvcc::Lock::decode(val);
        }
        if lock.op as i32 == kvproto::kvrpcpb::Op::PessimisticLock.value() {
            lock.op = kvproto::kvrpcpb::Op::Lock.value() as u8;
        }
        let user_meta = &mvcc::UserMeta::new(lock.start_ts, commit_ts).to_array()[..];
        if lock.op as i32 != kvproto::kvrpcpb::Op::Lock.value() {
            wb.put(mvcc::WRITE_CF, key, val, 0, user_meta, commit_ts);
        } else {
            let op_lock_key = mvcc::encode_extra_txn_status_key(key, lock.start_ts);
            wb.put(
                mvcc::EXTRA_CF,
                op_lock_key.chunk(),
                &[],
                0,
                user_meta,
                commit_ts,
            )
        }
        wb.delete(mvcc::LOCK_CF, key, 0);
    }

    pub(crate) fn get_lock_for_commit(
        &mut self,
        kv: &kvengine::Engine,
        key: &[u8],
        commit_ts: u64,
    ) -> Bytes {
        if let Some((_, v)) = self.lock_cache.remove_entry(key) {
            return v;
        }
        let region_id = self.region.get_id();
        if self.snap.is_none() {
            self.snap = Some(kv.get_snap_access(region_id).unwrap());
        }
        let mut snap = self.snap.clone().unwrap();
        if let Some(item) = snap.get(mvcc::LOCK_CF, key, u64::MAX) {
            return Bytes::copy_from_slice(item.get_value());
        }
        // Maybe snap is stale, try to get snap access again.
        snap = kv.get_snap_access(region_id).unwrap();
        self.snap = Some(snap.clone());
        if let Some(item) = snap.get(mvcc::LOCK_CF, key, u64::MAX) {
            return Bytes::copy_from_slice(item.get_value());
        }
        // TODO: investigate why there is duplicated commit.
        let item = snap.get(mvcc::WRITE_CF, key, u64::MAX).unwrap();
        let user_meta = mvcc::UserMeta::from_slice(item.user_meta());
        assert_eq!(user_meta.commit_ts, commit_ts);
        Bytes::copy_from_slice(item.get_value())
    }

    pub(crate) fn rollback(
        &mut self,
        wb: &mut kvengine::WriteBatch,
        key: &[u8],
        start_ts: u64,
        delete_lock: bool,
    ) {
        let rollback_key = mvcc::encode_extra_txn_status_key(key, start_ts);
        let user_meta = &mvcc::UserMeta::new(start_ts, 0).to_array()[..];
        wb.put(
            mvcc::EXTRA_CF,
            rollback_key.chunk(),
            &[],
            0,
            user_meta,
            start_ts,
        );
        if delete_lock {
            wb.delete(mvcc::LOCK_CF, key, 0);
            self.lock_cache.remove(key);
        }
    }

    pub(crate) fn exec_delete_range(&mut self, ctx: &mut ApplyContext, cl: CustomRaftLog) {
        todo!()
    }

    fn exec_admin_cmd(
        &mut self,
        ctx: &mut ApplyContext,
        req: &RaftCmdRequest,
    ) -> Result<(RaftCmdResponse, ApplyResult)> {
        let request = req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        if cmd_type != AdminCmdType::CompactLog && cmd_type != AdminCmdType::CommitMerge {
            info!(
                "execute admin command";
                "region_id" => self.region_id(),
                "peer_id" => self.id(),
                "term" => ctx.exec_ctx.term,
                "index" => ctx.exec_ctx.index,
                "command" => ?request,
            );
        }

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::ChangePeerV2 => self.exec_change_peer_v2(ctx, request),
            AdminCmdType::Split => self.exec_split(ctx, request),
            AdminCmdType::BatchSplit => self.exec_split(ctx, request),
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

    pub(crate) fn exec_custom_log(&mut self, ctx: &mut ApplyContext, cl: &CustomRaftLog) {
        let wb = ctx.wb.get_engine_wb(self.region.get_id());
        wb.set_sequence(ctx.exec_ctx.index);
        match cl.get_type() {
            TYPE_PREWRITE => cl.iterate_lock(|k, v| {
                wb.put(mvcc::LOCK_CF, k.chunk(), v.chunk(), 0, &[], 0);
                self.lock_cache.insert(k, v);
            }),
            TYPE_PESSIMISTIC_LOCK => cl.iterate_lock(|k, v| {
                wb.put(mvcc::LOCK_CF, k.chunk(), v.chunk(), 0, &[], 0);
            }),
            TYPE_COMMIT => cl.iterate_commit(|k, v, commit_ts| {
                self.commit_lock(&ctx.engine, wb, k, v, commit_ts);
            }),
            TYPE_ROLLBACK => cl.iterate_rollback(|k, start_ts, delete_lock| {
                self.rollback(wb, k, start_ts, delete_lock);
            }),
            TYPE_PESSIMISTIC_ROLLBACK => {
                cl.iterate_keys_only(|k| {
                    wb.delete(mvcc::LOCK_CF, k, 0);
                });
            }
            TYPE_PRE_SPLIT => {
                let mut cs = cl.get_change_set().unwrap();
                cs.sequence = ctx.exec_ctx.index;
                if let Err(e) = ctx.engine.pre_split(cs) {
                    error!("failed to execute pre-split, maybe already split by ingest";
                        "region" => self.tag,
                    );
                }
            }
            TYPE_FLUSH | TYPE_COMPACTION | TYPE_SPLIT_FILES => {
                let mut cs = cl.get_change_set().unwrap();
                // Assign the raft log's index as the sequence number of the ChangeSet to ensure monotonic increase.
                cs.sequence = ctx.exec_ctx.index;
                if cs.has_flush() {
                    if let Some(shard) = ctx.engine.get_shard(self.region.get_id()) {
                        shard.mark_mem_table_applying_flush(cs.get_flush().get_commit_ts());
                    }
                }
                let task = RegionTask::ApplyChangeSet { change: cs };
                ctx.region_scheduler.as_ref().unwrap().schedule(task);
            }
            TYPE_NEX_MEM_TABLE_SIZE => {
                let mut cs = cl.get_change_set().unwrap();
                cs.sequence = ctx.exec_ctx.index;
                let bin = &mut [0u8; 8][..];
                bin.put_u64_le(cs.next_mem_table_size);
                wb.set_property(kvengine::MEM_TABLE_SIZE_KEY, bin);
            }
            TYPE_DELETE_RANGE => {
                todo!()
            }
            _ => panic!("unknown custom log type"),
        }
        ctx.engine.write(wb);
        self.metrics.written_bytes += wb.estimated_size() as u64;
        self.metrics.written_keys += wb.num_entries() as u64;
        wb.reset();
    }

    /// Applies raft command.
    ///
    /// An apply operation can fail in the following situations:
    ///   1. it encounters an error that will occur on all stores, it can continue
    /// applying next entry safely, like epoch not match for example;
    ///   2. it encounters an error that may not occur on all stores, in this case
    /// we should try to apply the entry again or panic. Considering that this
    /// usually due to disk operation fail, which is rare, so just panic is ok.
    fn apply_raft_log(
        &mut self,
        ctx: &mut ApplyContext,
        rlog: &RaftLog,
    ) -> (RaftCmdResponse, ApplyResult) {
        // Include region for epoch not match after merge may cause key not in range.
        let (ver, conf_ver) = rlog.get_epoch();
        let include_region = ver >= self.last_merge_version;
        if let Err(err) = check_region_epoch(rlog, &self.region, include_region) {
            let mut check_in_region_worker = false;
            if let RaftLog::Custom(custom) = rlog {
                match custom.get_type() {
                    rlog::TYPE_FLUSH | rlog::TYPE_COMPACTION | rlog::TYPE_SPLIT_FILES => {
                        check_in_region_worker = true;
                    }
                }
            }
            if !check_in_region_worker {
                return (err_resp(err, ctx.exec_ctx.term), ApplyResult::None);
            }
        }
        match rlog {
            RaftLog::Request(req) => {
                let res = self.exec_admin_cmd(ctx, req);
                match res {
                    Ok((resp, result)) => (resp, result),
                    Err(e) => (err_resp(e, ctx.exec_ctx.term), ApplyResult::None),
                }
            }
            RaftLog::Custom(custom) => {
                self.exec_custom_log(ctx, custom);
                let mut resp = RaftCmdResponse::default();
                let header = RaftResponseHeader::default();
                resp.set_header(header);
                (resp, ApplyResult::None)
            }
        }
    }

    fn handle_apply_result(
        &mut self,
        ctx: &mut ApplyContext,
        mut resp: RaftCmdResponse,
        result: &ApplyResult,
        is_conf_change: bool,
    ) {
        match result {
            ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => {
                return;
            }
            _ => {}
        }
        let index = ctx.exec_ctx.index;
        let term = ctx.exec_ctx.term;
        self.apply_state.applied_index = index;
        ctx.exec_ctx = ApplyExecContext::default();
        if let ApplyResult::Res(exec_result) = result {
            match exec_result {
                ExecResult::ChangePeer(cp) => {
                    self.region = cp.region.clone();
                    let peer_id = self.get_peer().get_id();
                    if peer_id == cp.changes[0].get_peer().get_id() {
                        self.peer_idx = get_peer_idx_by_peer_id(&self.region, peer_id);
                    }
                }
                ExecResult::CompactLog { .. } => {}
                ExecResult::SplitRegion { derived, .. } => {
                    self.region = derived.clone();
                }
                ExecResult::PrepareMerge { region, .. } => {
                    self.region = region.clone();
                    self.is_merging = true;
                }
                ExecResult::CommitMerge { region, .. } => {
                    self.region = region.clone();
                    self.last_merge_version = self.region.get_region_epoch().get_version();
                }
                ExecResult::RollbackMerge { region, .. } => {
                    self.region = region.clone();
                    self.is_merging = false;
                }
                ExecResult::ComputeHash { .. } => {}
                ExecResult::VerifyHash { .. } => {}
                ExecResult::DeleteRange { .. } => {}
            }
            self.tag = Self::make_tag(&self.region);
        }
        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        bind_term(&mut resp, term);
        if let Some(cmd_cb) = self.find_callback(index, term, is_conf_change) {
            cmd_cb.invoke_with_response(resp);
        }
    }

    fn find_callback(&mut self, index: u64, term: u64, is_conf_change: bool) -> Option<Callback> {
        let region_id = self.region.get_id();
        let peer_id = self.get_peer().get_id();
        if is_conf_change {
            if let Some(cmd) = self.pending_cmds.take_conf_change() {
                if cmd.index == index && cmd.term == term {
                    return Some(cmd.cb);
                }
                notify_stale_req(term, cmd.cb);
            }
            return None;
        }
        loop {
            let head = self.pending_cmds.pop_normal(term);
            if head.is_none() {
                break;
            }
            let head = head.unwrap();
            if head.term == term && head.index == index {
                return Some(head.cb);
            }
            // Because of the lack of original RaftCmdRequest, we skip calling
            // coprocessor here.
            notify_stale_req(term, head.cb);
        }
        None
    }

    fn handle_raft_entry_normal(
        &mut self,
        ctx: &mut ApplyContext,
        entry: eraftpb::Entry,
    ) -> ApplyResult {
        let index = entry.get_index();
        let term = entry.get_term();
        if entry.get_data().len() > 0 {
            let rlog = rlog::decode_log(Bytes::from(entry.get_data()));
            assert!(index > 0);
            // if pending remove, apply should be aborted already.
            assert!(!self.pending_remove);
            ctx.exec_ctx = ApplyExecContext::new(index, term, self.apply_state);
            let (resp, result) = self.apply_raft_log(ctx, &rlog);
            self.handle_apply_result(ctx, resp, &result, rlog.is_conf_change());
            return result;
        }
        // when a peer become leader, it will send an empty entry.
        self.apply_state.applied_index = index;
        assert!(term > 0);
        loop {
            let cmd = self.pending_cmds.pop_normal(term - 1);
            if cmd.is_none() {
                break;
            }
            // apparently, all the callbacks whose term is less than entry's term are stale.
            cmd.unwrap()
                .cb
                .invoke_with_response(err_resp(Error::StaleCommand, term));
        }
        ApplyResult::None
    }

    pub(crate) fn exec_change_peer(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
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
            "type" => outil::conf_change_type_str(change_type),
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
                if let Some(p) = outil::find_peer_mut(&mut region, store_id) {
                    exists = true;
                    if !outil::is_learner(p) || p.get_id() != peer.get_id() {
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

                if let Some(p) = outil::remove_peer(&mut region, store_id) {
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

                if outil::find_peer(&region, store_id).is_some() {
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

        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_region(region.clone());

        Ok((
            resp,
            ApplyResult::Res(ExecResult::ChangePeer(ChangePeer {
                index: ctx.exec_ctx.index,
                conf_change: Default::default(),
                changes: vec![request.clone()],
                region,
            })),
        ))
    }

    pub(crate) fn exec_change_peer_v2(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        todo!()
    }

    fn process_raft_cmd(
        &mut self,
        ctx: &mut ApplyContext,
        index: u64,
        term: u64,
        cmd: RaftCmdRequest,
    ) -> ApplyResult {
        todo!()
    }

    fn handle_raft_entry_conf_change(
        &mut self,
        apply_ctx: &mut ApplyContext,
        entry: &eraftpb::Entry,
    ) -> ApplyResult {
        // Although conf change can't yield in normal case, it is convenient to
        // simulate yield before applying a conf change log.
        fail_point!("yield_apply_conf_change_3", self.id() == 3, |_| {
            ApplyResult::Yield
        });
        let (index, term) = (entry.get_index(), entry.get_term());
        let conf_change: ConfChangeV2 = match entry.get_entry_type() {
            EntryType::EntryConfChange => {
                let conf_change: ConfChange =
                    outil::parse_data_at(entry.get_data(), index, &self.tag);
                use raft_proto::ConfChangeI;
                conf_change.into_v2()
            }
            EntryType::EntryConfChangeV2 => {
                outil::parse_data_at(entry.get_data(), index, &self.tag)
            }
            _ => unreachable!(),
        };
        let cmd = outil::parse_data_at(conf_change.get_context(), index, &self.tag);
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

    fn exec_split(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        todo!()
    }

    fn exec_compact_log(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        todo!()
    }

    fn exec_compute_hash(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        todo!()
    }

    fn exec_verify_hash(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        todo!()
    }

    fn exec_prepare_merge(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        todo!()
    }

    fn exec_commit_merge(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        todo!()
    }

    fn exec_rollback_merge(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        todo!()
    }

    /// Handles proposals, and appends the commands to the apply delegate.
    fn append_proposal(&mut self, props_drainer: Drain<Proposal>) {
        let (region_id, peer_id) = (self.region.get_id(), self.get_peer().get_id());
        let propose_num = props_drainer.len();
        if self.stopped {
            for p in props_drainer {
                notify_stale_req(p.term, p.cb);
            }
            return;
        }
        for p in props_drainer {
            let cmd = PendingCmd::new(p.index, p.term, p.cb);
            if p.is_conf_change {
                if let Some(cmd) = self.pending_cmds.take_conf_change() {
                    // if it loses leadership before conf change is replicated, there may be
                    // a stale pending conf change before next conf change is applied. If it
                    // becomes leader again with the stale pending conf change, will enter
                    // this block, so we notify leadership may have been changed.
                    notify_stale_req(self.term, cmd.cb);
                }
                self.pending_cmds.set_conf_change(cmd);
            } else {
                self.pending_cmds.append_normal(cmd);
            }
        }
        // TODO: observe it in batch.
        APPLY_PROPOSAL.observe(propose_num as f64);
    }

    /// Handles all the committed_entries, namely, applies the committed entries.
    fn handle_raft_committed_entries(
        &mut self,
        ctx: &mut ApplyContext,
        mut committed_entries_drainer: Drain<eraftpb::Entry>,
    ) {
        if committed_entries_drainer.len() == 0 {
            return;
        }
        defer!({
            self.snap.take();
        });
        if self.yield_state.is_some() {
            let yield_state = self.yield_state.as_mut().unwrap();
            yield_state
                .pending_entries
                .extend(committed_entries_drainer);
            return;
        }

        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let mut results = VecDeque::<ExecResult>::new();
        while let Some(entry) = committed_entries_drainer.next() {
            if self.pending_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }
            let expected_index = self.apply_state.applied_index + 1;
            if expected_index != entry.get_index() {
                // Msg::CatchUpLogs may have arrived before Msg::Apply.
                if expected_index > entry.get_index() && self.is_merging {
                    info!("skip log as it is already applied";
                        "region" => &self.tag,
                        "index" => entry.get_index(),
                    );
                    continue;
                }
                panic!(
                    "{} expect index {}, but got {}",
                    self.tag,
                    expected_index,
                    entry.get_index()
                );
            }
            let mut result: ApplyResult = ApplyResult::None;
            match entry.get_entry_type() {
                eraftpb::EntryType::EntryNormal => {
                    result = self.handle_raft_entry_normal(ctx, entry);
                }
                eraftpb::EntryType::EntryConfChange | eraftpb::EntryType::EntryConfChangeV2 => {
                    result = self.handle_raft_entry_conf_change(ctx, &entry);
                }
            }
            match result {
                ApplyResult::None => {}
                ApplyResult::Res(res) => {
                    results.push_back(res);
                }
                ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => {
                    // Both cancel and merge will yield current processing.
                    let mut pending_entries =
                        Vec::with_capacity(committed_entries_drainer.len() + 1);
                    // Note that current entry is skipped when yield.
                    pending_entries.push(entry);
                    pending_entries.extend(committed_entries_drainer);
                    ctx.finish_for(self, results);
                    self.yield_state = Some(YieldState {
                        pending_entries,
                        pending_msgs: Vec::default(),
                        heap_size: None,
                    });
                    if let ApplyResult::WaitMergeSource(logs_up_to_date) = result {
                        self.wait_merge_state = Some(WaitSourceMergeState { logs_up_to_date });
                    }
                    return;
                }
            }
        }
        ctx.finish_for(&self, results);
    }

    fn on_role_changed(&mut self, ctx: &mut ApplyContext, state: SoftState) {
        self.recover_split = false;
        if let Some(shard) = ctx.engine.get_shard(self.region.get_id()) {
            info!("shard set passive on role changed";
                "region" => &self.tag);
            shard.set_active(state.raft_state == StateRole::Leader);
            if state.raft_state == StateRole::Leader {
                ctx.engine.trigger_flush(shard);
                if shard.get_split_stage() != kvenginepb::SplitStage::Initial {
                    self.recover_split = true;
                }
            }
        }
    }

    fn handle_recover_split(&mut self, ctx: &mut ApplyContext) {
        if self.recover_split {
            let shard = ctx.engine.get_shard(self.region.get_id()).unwrap();
            if shard.get_split_stage().value() >= kvenginepb::SplitStage::PreSplitFlushDone.value()
            {
                self.recover_split = false;
                info!("shard recover split"; "region" => &self.tag);
                let task = RegionTask::RecoverSplit {
                    region: self.region.clone(),
                    peer: self.get_peer().clone(),
                    split_keys: shard.get_split_keys(),
                    stage: shard.get_split_stage(),
                };
                ctx.region_scheduler.as_ref().unwrap().schedule(task).unwrap();
            }
        }
    }

    fn handle_apply(&mut self, ctx: &mut ApplyContext, mut apply: MsgApply) {
        if (apply.entries.len() == 0 && apply.soft_state.is_none())
            || self.pending_remove
            || self.stopped
        {
            return;
        }
        self.metrics = ApplyMetrics::default();
        if let Some(shard) = ctx.engine.get_shard(apply.region_id) {
            self.metrics.approximate_size = shard.get_estimated_size();
        }
        self.term = apply.term;
        self.append_proposal(apply.cbs.drain(..));
        self.handle_raft_committed_entries(ctx, apply.entries.drain(..));
        if let Some(state) = apply.soft_state {
            self.on_role_changed(ctx, state);
        }
        if self.recover_split {
            self.handle_recover_split(ctx);
        }
        if self.wait_merge_state.is_some() {
            return;
        }
        if self.pending_remove {
            self.destroy(ctx);
        }
    }

    fn clear_all_commands_as_stale(&mut self) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_req(self.term, cmd.cb);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_req(self.term, cmd.cb);
        }
    }

    /// Handles peer registration. When a peer is created, it will register an apply delegate.
    fn handle_registration(&mut self, reg: MsgRegistration) {
        info!(
            "re-register to applier";
            "region_id" => self.region.get_id(),
            "peer_id" => self.get_peer().get_id(),
            "term" => reg.term
        );
        assert_eq!(self.get_peer().get_id(), reg.peer.id);
        self.term = reg.term;
        self.clear_all_commands_as_stale();
        *self = Applier::new_from_reg(reg);
    }

    fn destroy(&mut self, ctx: &mut ApplyContext) {
        let region_id = self.region.get_id();
        let peer_id = self.get_peer().get_id();
        fail_point!("before_peer_destroy_1003", peer_id == 1003, |_| {});
        info!(
            "remove applier";
            "region_id" => region_id,
            "peer_id" => peer_id,
        );
        self.stopped = true;
        if let Some(router) = &ctx.router {
            router.close(self.region_id());
        }
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_req_region_removed(self.region.get_id(), cmd.cb);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_req_region_removed(self.region.get_id(), cmd.cb);
        }
        self.yield_state = None;

        // TODO: add trace
        // let mut event = TraceEvent::default();
        // if let Some(e) = self.trace.reset(ApplyMemoryTrace::default()) {
        //     event = event + e;
        // }
        // MEMTRACE_APPLYS.trace(event);
    }

    fn handle_destroy(
        &mut self,
        ctx: &mut ApplyContext,
        region_id: u64,
        merge_from_snapshot: bool,
    ) {
        assert_eq!(region_id, self.region.get_id());
        if merge_from_snapshot {
            assert_eq!(self.stopped, false);
        }
        if !self.stopped {
            self.destroy(ctx);
            if let Some(router) = &ctx.router {
                let msg = PeerMsg::DestroyRes {
                    peer_id: self.get_peer().id,
                    merge_from_snapshot,
                };
                router.send(self.region.get_id(), msg);
            }
        }
    }

    pub(crate) fn handle_msg(&mut self, ctx: &mut ApplyContext, msg: ApplyMsg) {
        match msg {
            ApplyMsg::Apply(apply) => {
                self.handle_apply(ctx, apply);
            }
            ApplyMsg::Registration(reg) => {
                self.handle_registration(reg);
            }
            ApplyMsg::Destroy {
                region_id,
                merge_from_snapshot,
            } => {
                self.handle_destroy(ctx, region_id, merge_from_snapshot);
            }
            _ => {
                panic!("not supported")
            }
        }
    }
}

pub fn get_peer_idx_by_store_id(region: &metapb::Region, store_id: u64) -> usize {
    let mut peer_idx = region.peers.len();
    for (i, peer) in region.peers.iter().enumerate() {
        if peer.store_id == store_id {
            peer_idx = i;
            break;
        }
    }
    peer_idx
}

pub fn get_peer_idx_by_peer_id(region: &metapb::Region, peer_id: u64) -> usize {
    let mut peer_idx = region.peers.len();
    for (i, peer) in region.peers.iter().enumerate() {
        if peer.get_id() == peer_id {
            peer_idx = i;
            break;
        }
    }
    peer_idx
}

pub fn is_conf_change_cmd(msg: &RaftCmdRequest) -> bool {
    if !msg.has_admin_request() {
        return false;
    }
    let req = msg.get_admin_request();
    req.has_change_peer() || req.has_change_peer_v2()
}

#[derive(Clone)]
pub(crate) struct ApplyRouter {}

pub(crate) const APPLY_STATE_KEY: &'static str = "apply_state";

pub(crate) struct ApplyContext {
    pub(crate) engine: kvengine::Engine,
    pub(crate) region_scheduler: Option<Scheduler<RegionTask>>, // None in recover mode.
    pub(crate) router: Option<RaftRouter>,                      // None in recover mode.
    pub(crate) apply_batch: ApplyBatch,
    pub(crate) apply_task_res_list: Vec<ApplyResult>,
    pub(crate) exec_ctx: ApplyExecContext,
    pub(crate) wb: KVWriteBatch,
}

impl ApplyContext {
    pub fn new(
        engine: kvengine::Engine,
        region_scheduler: Option<Scheduler<RegionTask>>,
        router: Option<RaftRouter>,
    ) -> Self {
        Self {
            engine: engine.clone(),
            region_scheduler,
            router,
            apply_batch: ApplyBatch::default(),
            apply_task_res_list: Default::default(),
            exec_ctx: Default::default(),
            wb: KVWriteBatch::new(engine),
        }
    }

    pub fn finish_for(&self, applier: &Applier, results: VecDeque<ExecResult>) {
        if let Some(router) = &self.router {
            let apply_res = MsgApplyResult {
                results,
                apply_state: applier.apply_state,
                merged: false,
                metrics: applier.metrics.clone(),
            };
            let region_id = applier.region.get_id();
            let msg = PeerMsg::ApplyRes(apply_res);
            router.peer_sender.send((region_id, msg)).unwrap();
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct ApplyMetrics {
    pub approximate_size: u64,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub written_query_stats: QueryStats,
    pub lock_cf_written_bytes: u64,
}
