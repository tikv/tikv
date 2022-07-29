// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, VecDeque},
    fmt::{self, Debug, Formatter},
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::Duration,
    vec::Drain,
};

use bytes::Buf;
use fail::fail_point;
use kvengine::{ChangeSet, Engine, SnapAccess};
use kvproto::{
    metapb,
    metapb::{PeerRole, Region},
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, BatchSplitRequest, BatchSplitResponse,
        ChangePeerRequest, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, RaftResponseHeader,
    },
};
use prometheus::local::LocalHistogram;
use protobuf::RepeatedField;
use raft::{
    eraftpb::{ConfChange, ConfChangeType, ConfChangeV2, EntryType},
    StateRole,
};
use raft_proto::eraftpb;
use raftstore::store::{
    fsm::metrics::*,
    metrics::*,
    util,
    util::{ChangePeerI, ConfChangeKind},
    QueryStats,
};
use tikv_util::{box_err, error, info, time::Instant, warn};
use time::Timespec;
use txn_types::LockType;

use super::*;
use crate::{
    errors::*,
    mvcc,
    store::cmd_resp::{bind_term, err_resp},
    RaftRouter, RaftStoreRouter, UserMeta,
};

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
        if self.normals.is_empty() {
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

#[allow(unused)]
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
#[allow(clippy::large_enum_variant)]
pub enum ExecResult {
    ChangePeer(ChangePeer),
    SplitRegion { regions: Vec<Region> },
    DeleteRange { ranges: Vec<Range> },
    UnsafeDestroy,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ApplyResult {
    None,
    /// Additional result that needs to be sent back to raftstore.
    Res(ExecResult),
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

pub(crate) struct ApplyBatch {
    pub(crate) applier: Arc<Mutex<Applier>>,
    pub(crate) msgs: Vec<ApplyMsg>,
    pub(crate) applying_cnt: Arc<AtomicU64>,
    pub(crate) send_time: Instant,
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
#[derive(Default)]
pub(crate) struct Applier {
    pub(crate) peer: metapb::Peer,
    pub(crate) term: u64,
    pub(crate) region: metapb::Region,

    /// If the applier should be stopped from polling.
    /// A applier can be stopped in conf change, merge or requested by destroy message.
    pub(crate) stopped: bool,
    /// Set to true when removing itself because of `ConfChangeType::RemoveNode`, and then
    /// any following committed logs in same Ready should be applied failed.
    pub(crate) pending_remove: bool,

    /// The commands waiting to be committed and applied
    pub(crate) pending_cmds: PendingCmdQueue,

    pub(crate) apply_state: RaftApplyState,

    pub(crate) lock_cache: HashMap<Vec<u8>, Vec<u8>>,

    pub(crate) snap: Option<SnapAccess>,

    pub(crate) metrics: ApplyMetrics,

    pub(crate) pending_split: HashMap<u64, kvenginepb::ChangeSet>,

    pub(crate) paused: bool,

    pub(crate) paused_apply_queue: Vec<MsgApply>,

    pub(crate) ingest_callback: Option<Callback>,

    pub(crate) scheduled_change_sets: VecDeque<u64>,

    pub(crate) prepared_change_sets: HashMap<u64, kvengine::ChangeSet>,

    pub(crate) role: raft::StateRole,

    mem_table_state: Option<MemTableState>,

    last_property_term: u64,
}

impl Applier {
    pub(crate) fn new_from_peer(peer: &PeerFsm) -> Self {
        let reg = MsgRegistration::new(&peer.peer);
        Self::new_from_reg(reg)
    }

    pub(crate) fn new_from_reg(reg: MsgRegistration) -> Self {
        Self {
            peer: reg.peer,
            term: reg.term,
            region: reg.region,
            apply_state: reg.apply_state,
            ..Default::default()
        }
    }

    fn tag(&self) -> PeerTag {
        let id_ver = RegionIDVer::new(self.region.id, self.region.get_region_epoch().version);
        PeerTag::new(self.peer.store_id, id_ver)
    }

    pub(crate) fn new_for_recover(
        store_id: u64,
        region: metapb::Region,
        snap: SnapAccess,
        apply_state: RaftApplyState,
    ) -> Self {
        let peer_idx = get_peer_idx_by_store_id(&region, store_id);
        let peer = region.peers[peer_idx].clone();
        Self {
            peer,
            term: RAFT_INIT_LOG_TERM,
            region,
            apply_state,
            snap: Some(snap),
            ..Default::default()
        }
    }

    pub(crate) fn get_peer(&self) -> &metapb::Peer {
        &self.peer
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
        commit_ts: u64,
        log_index: u64,
    ) {
        let lock_val = self.get_lock_for_commit(kv, key, commit_ts, log_index);
        if lock_val.is_empty() {
            return;
        }
        let lock = txn_types::Lock::parse(&lock_val).unwrap_or_else(|x| {
            panic!(
                "failed to parse lock value {:?}, local_val {:?}",
                x, &lock_val
            );
        });
        let start_ts = lock.ts.into_inner();
        let user_meta = &mvcc::UserMeta::new(start_ts, commit_ts).to_array()[..];
        match lock.lock_type {
            LockType::Lock | LockType::Pessimistic => {
                let op_lock_key = mvcc::encode_extra_txn_status_key(key, start_ts);
                wb.put(
                    mvcc::EXTRA_CF,
                    op_lock_key.chunk(),
                    &[0],
                    0,
                    user_meta,
                    commit_ts,
                )
            }
            LockType::Put => {
                let val = lock.short_value.unwrap();
                wb.put(mvcc::WRITE_CF, key, &val, 0, user_meta, commit_ts);
            }
            LockType::Delete => {
                wb.put(mvcc::WRITE_CF, key, &[], 0, user_meta, commit_ts);
            }
        }
        wb.delete(mvcc::LOCK_CF, key, 0);
    }

    pub(crate) fn get_lock_for_commit(
        &mut self,
        kv: &kvengine::Engine,
        key: &[u8],
        commit_ts: u64,
        log_index: u64,
    ) -> Vec<u8> {
        if let Some((_, v)) = self.lock_cache.remove_entry(key) {
            return v;
        }
        let region_id = self.region.get_id();
        if self.snap.is_none() {
            self.snap = Some(kv.get_snap_access(region_id).unwrap());
        }
        let mut snap = self.snap.clone().unwrap();
        let item = snap.get(mvcc::LOCK_CF, key, u64::MAX);
        if item.value_len() > 0 {
            return item.get_value().to_vec();
        }
        // Maybe snap is stale, try to get snap access again.
        snap = kv.get_snap_access(region_id).unwrap();
        self.snap = Some(snap.clone());
        let item = snap.get(mvcc::LOCK_CF, key, u64::MAX);
        if item.value_len() > 0 {
            return item.get_value().to_vec();
        }
        // TODO: investigate why there is duplicated commit.
        let item = snap.get(mvcc::WRITE_CF, key, u64::MAX);
        if item.user_meta_len() == 0 {
            panic!(
                "failed to get lock for key {:?}, {}, snap_write_sequence: {}, snap_files: {:?}, log_index:{}",
                key,
                snap.get_tag(),
                snap.get_write_sequence(),
                snap.get_all_files(),
                log_index,
            );
        }
        let user_meta = mvcc::UserMeta::from_slice(item.user_meta());
        assert_eq!(user_meta.commit_ts, commit_ts);
        warn!("duplicated commit for key {:?}, {}", key, snap.get_tag(),);
        vec![]
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
            &[0],
            0,
            user_meta,
            start_ts,
        );
        if delete_lock {
            wb.delete(mvcc::LOCK_CF, key, 0);
            self.lock_cache.remove(key);
        }
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
                "tag" => self.tag(),
                "peer_id" => self.id(),
                "term" => ctx.exec_log_term,
                "index" => ctx.exec_log_index,
                "command" => ?request,
            );
        }

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::ChangePeerV2 => self.exec_change_peer_v2(ctx, request),
            AdminCmdType::BatchSplit => self.exec_split(ctx, request),
            AdminCmdType::TransferLeader => Err(box_err!("transfer leader won't exec")),
            // TODO: is it backward compatible to add new cmd_type?
            _ => Err(box_err!("unsupported admin command type")),
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

    pub(crate) fn exec_custom_log(
        &mut self,
        ctx: &mut ApplyContext,
        cl: &CustomRaftLog<'_>,
    ) -> Result<(RaftCmdResponse, ApplyResult)> {
        let wb = ctx.wb.get_engine_wb(self.region.get_id());
        let engine = &ctx.engine;
        let log_index = ctx.exec_log_index;
        wb.set_sequence(log_index);
        if ctx.exec_log_term != self.last_property_term {
            wb.set_property(TERM_KEY, &ctx.exec_log_term.to_le_bytes());
            self.last_property_term = ctx.exec_log_term;
        }
        let timer = Instant::now();
        match cl.get_type() {
            TYPE_PREWRITE => cl.iterate_lock(|k, v| {
                wb.put(mvcc::LOCK_CF, k, v, 0, &[], 0);
                self.metrics.written_keys += 1;
                self.metrics.written_bytes += (k.len() + v.len()) as u64;
                self.lock_cache.insert(k.to_vec(), v.to_vec());
            }),
            TYPE_PESSIMISTIC_LOCK => cl.iterate_lock(|k, v| {
                wb.put(mvcc::LOCK_CF, k, v, 0, &[], 0);
            }),
            TYPE_COMMIT => cl.iterate_commit(|k, commit_ts| {
                self.commit_lock(engine, wb, k, commit_ts, log_index);
            }),
            TYPE_ONE_PC => cl.iterate_one_pc(|k, v, is_extra, del_lock, start_ts, commit_ts| {
                self.metrics.written_keys += 1;
                self.metrics.written_bytes += (k.len() + v.len()) as u64;
                let user_meta = UserMeta::new(start_ts, commit_ts).to_array();
                if is_extra {
                    let op_lock_key = mvcc::encode_extra_txn_status_key(k, start_ts);
                    wb.put(mvcc::EXTRA_CF, &op_lock_key, &[0], 0, &user_meta, commit_ts);
                } else {
                    wb.put(mvcc::WRITE_CF, k, v, 0, &user_meta, commit_ts);
                }
                if del_lock {
                    wb.delete(mvcc::LOCK_CF, k, 0);
                }
            }),
            TYPE_ROLLBACK => cl.iterate_rollback(|k, start_ts, del_lock| {
                self.rollback(wb, k, start_ts, del_lock);
            }),
            TYPE_PESSIMISTIC_ROLLBACK => {
                cl.iterate_del_lock(|k| {
                    wb.delete(mvcc::LOCK_CF, k, 0);
                });
            }
            TYPE_ENGINE_META => {
                let cs = cl.get_change_set().unwrap();
                if !cs.get_property_key().is_empty() {
                    wb.set_property(cs.get_property_key(), cs.get_property_value());
                }
            }
            TYPE_RESOLVE_LOCK => cl.iterate_resolve_lock(|tp, k, ts, del_lock| match tp {
                TYPE_COMMIT => self.commit_lock(engine, wb, k, ts, log_index),
                TYPE_ROLLBACK => self.rollback(wb, k, ts, del_lock),
                _ => unreachable!("unexpected custom log type: {:?}", tp),
            }),
            TYPE_SWITCH_MEM_TABLE => {
                let switch_mem_table_size = cl.get_switch_mem_table();
                if self.mut_mem_table_state(engine).mem_table_size >= switch_mem_table_size {
                    wb.set_switch_mem_table();
                }
            }
            _ => panic!("unknown custom log type"),
        }
        let mem_table_size = ctx.engine.write(wb) as u64;
        wb.reset();
        let mem_states = self.mut_mem_table_state(engine);
        if mem_states.mem_table_size > 0 && mem_table_size == 0 {
            mem_states.set_switch_time(timer);
        }
        mem_states.mem_table_size = mem_table_size;
        mem_states.last_write_time = Some(timer);
        self.maybe_propose_switch_mem_table(ctx, timer);
        ctx.apply_time.observe(timer.saturating_elapsed_secs());
        // self.metrics.written_bytes += wb.estimated_size() as u64;
        // self.metrics.written_keys += wb.num_entries() as u64;
        let mut resp = RaftCmdResponse::default();
        let header = RaftResponseHeader::default();
        resp.set_header(header);
        Ok((resp, ApplyResult::None))
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
        req: &RaftCmdRequest,
    ) -> (RaftCmdResponse, ApplyResult) {
        if let Err(err) = check_region_epoch(req, &self.region, true) {
            let mut check_in_region_worker = false;
            if let Some(custom) = rlog::get_custom_log(req) {
                if custom.get_type() == rlog::TYPE_ENGINE_META {
                    check_in_region_worker = true;
                }
            }
            if !check_in_region_worker {
                return (err_resp(err, ctx.exec_log_term), ApplyResult::None);
            }
        }
        if req.has_admin_request() {
            return match self.exec_admin_cmd(ctx, req) {
                Ok((resp, result)) => (resp, result),
                Err(e) => (err_resp(e, ctx.exec_log_term), ApplyResult::None),
            };
        }
        let custom = rlog::get_custom_log(req).unwrap();
        match self.exec_custom_log(ctx, &custom) {
            Ok((resp, result)) => (resp, result),
            Err(e) => (err_resp(e, ctx.exec_log_term), ApplyResult::None),
        }
    }

    fn handle_apply_result(
        &mut self,
        ctx: &mut ApplyContext,
        mut resp: RaftCmdResponse,
        result: &ApplyResult,
        is_conf_change: bool,
    ) {
        self.apply_state.applied_index = ctx.exec_log_index;
        self.apply_state.applied_index_term = ctx.exec_log_term;
        if let ApplyResult::Res(exec_result) = result {
            match exec_result {
                ExecResult::ChangePeer(cp) => {
                    self.region = cp.region.clone();
                    for peer in cp.region.get_peers() {
                        if peer.id == self.peer.id {
                            self.peer = peer.clone();
                            break;
                        }
                    }
                }
                ExecResult::SplitRegion { regions } => {
                    self.region = regions.last().unwrap().clone();
                }
                ExecResult::DeleteRange { .. } => {}
                ExecResult::UnsafeDestroy { .. } => {}
            }
        }
        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        bind_term(&mut resp, ctx.exec_log_term);
        if let Some(cmd_cb) =
            self.find_callback(ctx.exec_log_index, ctx.exec_log_term, is_conf_change)
        {
            cmd_cb.invoke_with_response(resp);
        }
    }

    fn find_callback(&mut self, index: u64, term: u64, is_conf_change: bool) -> Option<Callback> {
        if is_conf_change {
            if let Some(cmd) = self.pending_cmds.take_conf_change() {
                if cmd.index == index && cmd.term == term {
                    return Some(cmd.cb);
                }
                notify_stale_req(term, cmd.cb, "conf change term not match");
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
            notify_stale_req(term, head.cb, "term not match");
        }
        None
    }

    fn handle_raft_entry_normal(
        &mut self,
        ctx: &mut ApplyContext,
        entry: &eraftpb::Entry,
    ) -> ApplyResult {
        // fail_point!(
        // "yield_apply_first_region",
        // self.region.get_start_key().is_empty() && !self.region.get_end_key().is_empty(),
        // |_| ApplyResult::Yield
        // );

        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if !data.is_empty() {
            let cmd = parse_data_at(data, index, self.tag());
            assert!(index > 0);
            // if pending remove, apply should be aborted already.
            assert!(!self.pending_remove);
            let (resp, result) = self.apply_raft_log(ctx, &cmd);
            self.handle_apply_result(ctx, resp, &result, false);
            return result;
        }
        // when a peer become leader, it will send an empty entry.
        self.apply_state.applied_index = index;
        self.apply_state.applied_index_term = term;
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
            "tag" => self.tag(),
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
                        self.id() == 2 && self.region_id() == 1,
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
                    if !util::is_learner(p) || p.get_id() != peer.get_id() {
                        error!(
                            "can't add duplicated peer";
                            "tag" => self.tag(),
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
                    "tag" => self.tag(),
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
                            "tag" => self.tag(),
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
                        "tag" => self.tag(),
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
                    "tag" => self.tag(),
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
                        "tag" => self.tag(),
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
                    "tag" => self.tag(),
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
                index: ctx.exec_log_index,
                conf_change: Default::default(),
                changes: vec![request.clone()],
                region,
            })),
        ))
    }

    fn exec_change_peer_v2(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        assert!(request.has_change_peer_v2());
        let changes = request.get_change_peer_v2().get_change_peers().to_vec();

        info!(
            "exec ConfChangeV2";
            "tag" => self.tag(),
            "peer_id" => self.id(),
            "kind" => ?ConfChangeKind::confchange_kind(changes.len()),
            "epoch" => ?self.region.get_region_epoch(),
        );

        let region = match ConfChangeKind::confchange_kind(changes.len()) {
            ConfChangeKind::LeaveJoint => self.apply_leave_joint()?,
            kind => self.apply_conf_change(kind, changes.as_slice())?,
        };

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

            // confchange_cmd_metric::inc_all(change_type);

            if let Some(exist_peer) = util::find_peer(&region, store_id) {
                let r = exist_peer.get_role();
                if r == PeerRole::IncomingVoter || r == PeerRole::DemotingVoter {
                    panic!(
                        "{} can't apply confchange because configuration is still in joint state, confchange: {:?}, region: {:?}",
                        self.tag(),
                        cp,
                        self.region
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
                        "tag" => self.region_id(),
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
                            "tag" => self.tag(),
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
                            "tag" => self.tag(),
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
                                    "tag" => self.tag(),
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
                        }
                        None => unreachable!(),
                    }
                }
            }
            // confchange_cmd_metric::inc_success(change_type);
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + changes.len() as u64;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        info!(
            "conf change successfully";
            "tag" => self.tag(),
            "peer_id" => self.id(),
            "changes" => ?changes,
            "original region" => ?&self.region,
            "current region" => ?&region,
        );
        Ok(region)
    }

    fn handle_raft_entry_conf_change(
        &mut self,
        ctx: &mut ApplyContext,
        entry: &eraftpb::Entry,
    ) -> ApplyResult {
        // Although conf change can't yield in normal case, it is convenient to
        // simulate yield before applying a conf change log.
        // fail_point!("yield_apply_conf_change_3", self.id() == 3, |_| {
        // ApplyResult::Yield
        // });
        let (index, _) = (entry.get_index(), entry.get_term());
        let conf_change: ConfChangeV2 = match entry.get_entry_type() {
            EntryType::EntryConfChange => {
                let conf_change: ConfChange = parse_data_at(entry.get_data(), index, self.tag());
                use raft_proto::ConfChangeI;
                conf_change.into_v2()
            }
            EntryType::EntryConfChangeV2 => parse_data_at(entry.get_data(), index, self.tag()),
            _ => unreachable!(),
        };
        let cmd = parse_data_at(conf_change.get_context(), index, self.tag());
        let (resp, result) = self.apply_raft_log(ctx, &cmd);
        self.handle_apply_result(ctx, resp, &result, true);
        match result {
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
                        self.tag(),
                        res,
                        conf_change,
                        index
                    );
                }
                ApplyResult::Res(res)
            }
        }
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
                self.tag(),
                self.region
            );
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + change_num;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        info!(
            "leave joint state successfully";
            "tag" => self.tag(),
            "peer_id" => self.id(),
            "region" => ?&region,
        );
        Ok(region)
    }

    fn exec_split(
        &mut self,
        ctx: &mut ApplyContext,
        request: &AdminRequest,
    ) -> Result<(AdminResponse, ApplyResult)> {
        // Write the engine before run finish split, or we will get shard not match error.
        let cs = self.pending_split.remove(&ctx.exec_log_index);
        if cs.is_none() {
            return Err(box_err!("split conflict with conf change"));
        }
        let cs = cs.unwrap();
        let mut resp = AdminResponse::default();
        if let Err(err) = ctx.engine.split(cs, RAFT_INIT_LOG_INDEX) {
            // This must be a follower that fall behind, we need to pause the apply and wait for split files to finish
            // in the background worker.
            panic!(
                "region {} failed to execute split operation, error {:?}",
                self.tag(),
                err
            );
        }
        self.snap.take(); // snapshot is outdated.
        // clear the cache here or the locks doesn't belong to the new range would never have chance to delete.
        self.lock_cache.clear();
        let mut splits = BatchSplitResponse::default();
        let regions =
            split_gen_new_region_metas(self.peer.store_id, &self.region, request.get_splits())
                .unwrap();
        splits.set_regions(RepeatedField::from(regions.clone()));
        resp.set_splits(splits);
        let result = ApplyResult::Res(ExecResult::SplitRegion { regions });
        Ok((resp, result))
    }

    /// Handles proposals, and appends the commands to the apply delegate.
    fn append_proposal(&mut self, props_drainer: Drain<'_, Proposal>) {
        let propose_num = props_drainer.len();
        if self.stopped {
            for p in props_drainer {
                notify_stale_req(p.term, p.cb, "stopped");
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
                    notify_stale_req(self.term, cmd.cb, "pending conf change");
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
        committed_entries_drainer: Drain<'_, eraftpb::Entry>,
    ) {
        if committed_entries_drainer.len() == 0 {
            return;
        }

        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let mut results = VecDeque::<ExecResult>::new();
        for entry in committed_entries_drainer {
            if self.pending_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }
            let expected_index = self.apply_state.applied_index + 1;
            if expected_index != entry.get_index() {
                panic!(
                    "{} expect index {}, but got {}",
                    self.tag(),
                    expected_index,
                    entry.get_index()
                );
            }
            ctx.exec_log_index = entry.index;
            ctx.exec_log_term = entry.term;
            let result = match entry.get_entry_type() {
                eraftpb::EntryType::EntryNormal => self.handle_raft_entry_normal(ctx, &entry),
                eraftpb::EntryType::EntryConfChange | eraftpb::EntryType::EntryConfChangeV2 => {
                    self.handle_raft_entry_conf_change(ctx, &entry)
                }
            };
            match result {
                ApplyResult::None => {}
                ApplyResult::Res(res) => {
                    results.push_back(res);
                }
            }
        }
        ctx.finish_for(self, results);
    }

    fn on_role_changed(&mut self, ctx: &mut ApplyContext, new_role: StateRole) {
        self.role = new_role;
        ctx.engine
            .set_shard_active(self.region.get_id(), self.is_leader());
    }

    fn is_leader(&self) -> bool {
        self.role == StateRole::Leader
    }

    fn handle_apply(&mut self, ctx: &mut ApplyContext, mut apply: MsgApply) {
        if (apply.entries.is_empty() && apply.new_role.is_none())
            || self.pending_remove
            || self.stopped
        {
            return;
        }
        if self.paused {
            self.paused_apply_queue.push(apply);
            return;
        }
        self.term = apply.term;
        self.append_proposal(apply.cbs.drain(..));
        self.metrics = ApplyMetrics::default();
        self.handle_raft_committed_entries(ctx, apply.entries.drain(..));
        self.snap.take();
        if let Some(state) = apply.new_role {
            self.on_role_changed(ctx, state);
        }
        if self.pending_remove {
            self.destroy(ctx);
        }
    }

    fn handle_apply_change_set(&mut self, ctx: &mut ApplyContext, cs: ChangeSet) {
        if !self
            .scheduled_change_sets
            .iter()
            .any(|&seq| seq == cs.sequence)
        {
            info!(
                "{} discard outdated change set {:?}",
                self.tag(),
                &cs.change_set
            );
            return;
        }
        self.prepared_change_sets.insert(cs.sequence, cs);
        while let Some(cs) = self.take_prepared_change_set() {
            let cs_pb = cs.change_set.clone();
            let is_ingest_files = cs.has_ingest_files();
            let result = if cs.has_snapshot() {
                let snap = cs.get_snapshot();
                self.apply_state.applied_index = snap.get_data_sequence();
                let term_val =
                    kvengine::get_shard_property(TERM_KEY, snap.get_properties()).unwrap();
                self.apply_state.applied_index_term = term_val.as_slice().get_u64_le();
                ctx.engine.ingest(cs, false).map(|()| cs_pb)
            } else {
                ctx.engine.apply_change_set(cs).map(|()| cs_pb)
            };
            let router = ctx.router.as_ref().unwrap();
            router.send(self.region_id(), PeerMsg::ApplyChangeSetResult(result));
            if is_ingest_files {
                if let Some(callback) = self.ingest_callback.take() {
                    let mut resp = RaftCmdResponse::default();
                    resp.mut_header().set_current_term(ctx.exec_log_term);
                    callback.invoke_with_response(resp);
                }
                self.paused = false;
                for apply in std::mem::take(&mut self.paused_apply_queue) {
                    self.handle_apply(ctx, apply);
                }
            }
        }
    }

    fn take_prepared_change_set(&mut self) -> Option<ChangeSet> {
        if let Some(sequence) = self.scheduled_change_sets.front() {
            if let Some(cs) = self.prepared_change_sets.remove(sequence) {
                self.scheduled_change_sets.pop_front();
                return Some(cs);
            }
        }
        None
    }

    fn clear_all_commands_as_stale(&mut self) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_req(self.term, cmd.cb, "reregistration");
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_req(self.term, cmd.cb, "reregistration");
        }
    }

    /// Handles peer registration. When a peer is created, it will register an apply delegate.
    fn handle_registration(&mut self, reg: MsgRegistration) {
        info!(
            "re-register to applier";
            "tag" => self.tag(),
            "peer_id" => self.get_peer().get_id(),
            "term" => reg.term,
            "reg_region_id" => reg.region.get_id(),
        );
        assert_eq!(self.get_peer().get_id(), reg.peer.id);
        self.term = reg.term;
        self.clear_all_commands_as_stale();
        *self = Applier::new_from_reg(reg);
    }

    fn destroy(&mut self, _: &mut ApplyContext) {
        let peer_id = self.get_peer().get_id();
        fail_point!("before_peer_destroy_1003", peer_id == 1003, |_| {});
        info!(
            "remove applier";
            "tag" => self.tag(),
            "peer_id" => peer_id,
        );
        self.stopped = true;
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_req_region_removed(self.region.get_id(), cmd.cb);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_req_region_removed(self.region.get_id(), cmd.cb);
        }
    }

    fn handle_unsafe_destroy(&mut self, ctx: &mut ApplyContext, region_id: u64) {
        assert_eq!(region_id, self.region.get_id());
        if !self.stopped {
            self.destroy(ctx);
        }
    }

    fn handle_prepare_change_set(&mut self, ctx: &mut ApplyContext, cs: kvenginepb::ChangeSet) {
        if cs.has_ingest_files() {
            self.ingest_callback = self.find_callback(cs.sequence, ctx.exec_log_term, false);
            self.paused = true;
        }
        self.scheduled_change_sets.push_back(cs.sequence);
        let engine = ctx.engine.clone();
        let router = ctx.router.clone().unwrap();
        let is_leader = self.is_leader();
        std::thread::spawn(move || {
            let id = cs.shard_id;
            let res = engine.prepare_change_set(cs, !is_leader);
            router.send(id, PeerMsg::PrepareChangeSetResult(res));
        });
    }

    fn mut_mem_table_state(&mut self, engine: &Engine) -> &mut MemTableState {
        let region_id = self.region_id();
        self.mem_table_state.get_or_insert_with(|| {
            let shard = engine.get_shard(region_id).unwrap();
            MemTableState::new(shard.get_writable_mem_table_size())
        })
    }

    fn maybe_propose_switch_mem_table(&mut self, ctx: &mut ApplyContext, now: Instant) {
        if !self.is_leader() {
            return;
        }
        let mem_state = self.mut_mem_table_state(&ctx.engine);
        if mem_state.need_switch(now) {
            let mut custom_builder = CustomBuilder::new();
            custom_builder.set_switch_mem_table(mem_state.mem_table_size);
            let mut req = self.new_raft_cmd_request();
            req.set_custom_request(custom_builder.build());
            ctx.router
                .as_ref()
                .unwrap()
                .send_command(req, Callback::None);
            self.mut_mem_table_state(&ctx.engine).proposed_time = Some(now);
        }
    }

    fn handle_check_switch_mem_table(&mut self, ctx: &mut ApplyContext, region_id: u64) {
        assert_eq!(self.region_id(), region_id);
        self.maybe_propose_switch_mem_table(ctx, Instant::now());
    }

    fn new_raft_cmd_request(&self) -> RaftCmdRequest {
        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(self.region_id());
        header.set_peer(self.peer.clone());
        header.set_region_epoch(self.region.get_region_epoch().clone());
        header.set_term(self.term);
        req.set_header(header);
        req
    }

    pub(crate) fn handle_msg(&mut self, ctx: &mut ApplyContext, msg: ApplyMsg) {
        match msg {
            ApplyMsg::Apply(apply) => {
                self.handle_apply(ctx, apply);
            }
            ApplyMsg::Registration(reg) => {
                self.handle_registration(reg);
            }
            ApplyMsg::UnsafeDestroy { region_id } => {
                self.handle_unsafe_destroy(ctx, region_id);
            }
            ApplyMsg::PendingSplit(pending_split) => {
                self.pending_split
                    .insert(pending_split.sequence, pending_split);
            }
            ApplyMsg::PrepareChangeSet(cs) => {
                self.handle_prepare_change_set(ctx, cs);
            }
            ApplyMsg::ApplyChangeSet(cs) => {
                self.handle_apply_change_set(ctx, cs);
            }
            ApplyMsg::CheckSwitchMemTable { region_id } => {
                self.handle_check_switch_mem_table(ctx, region_id);
            }
        }
    }
}

struct MemTableState {
    mem_table_size: u64,
    init_time: Instant,
    last_write_time: Option<Instant>,
    last_switch_time: Option<Instant>,
    proposed_time: Option<Instant>,
}

const BYTES_MB: u64 = 1024 * 1024;
const MEM_TABLE_SIZE_LOWER_LIMIT: u64 = 4 * BYTES_MB;

// 128MB mem-table flush at 10 seconds.
// 32MB mem-table flush at 40 seconds.
// 4MB mem-table flush at 320 seconds.
const STANDARD_MEMORY_SIZE_DURATION: u64 = 128 * BYTES_MB * 10;

// 1MB mem-table idle for 30 minutes get flushed.
const STANDARD_IDLE_SECONDS: u64 = 30 * 60;
const PROPOSE_SWITCH_TIMEOUT: Duration = Duration::from_secs(10);

impl MemTableState {
    fn new(mem_table_size: u64) -> Self {
        Self {
            mem_table_size,
            init_time: Instant::now(),
            last_write_time: None,
            last_switch_time: None,
            proposed_time: None,
        }
    }

    fn need_switch(&self, now: Instant) -> bool {
        if self.mem_table_size == 0 {
            return false;
        }
        if let Some(proposed_time) = self.proposed_time {
            if now.saturating_duration_since(proposed_time) < PROPOSE_SWITCH_TIMEOUT {
                return false;
            }
            // The proposal maybe failed for some reason, propose again.
            warn!("propose switch mem-table expired, propose again");
        }
        if self.mem_table_size < MEM_TABLE_SIZE_LOWER_LIMIT {
            let idle_duration = self.get_idle_duration(now);
            return idle_duration > self.max_idle_duration();
        }
        // We don't need to propose on hard limit, it is handled by the engine.
        let duration_secs_to_switch = STANDARD_MEMORY_SIZE_DURATION / self.mem_table_size;
        self.get_duration_since_last_switch(now).as_secs() > duration_secs_to_switch
    }

    fn get_idle_duration(&self, now: Instant) -> Duration {
        self.last_write_time
            .map(|time| now.saturating_duration_since(time))
            .unwrap_or_else(|| now.saturating_duration_since(self.init_time))
    }

    // idle duration applies to small mem-table
    // The large mem-table has less max idle time.
    fn max_idle_duration(&self) -> Duration {
        Duration::from_secs(STANDARD_IDLE_SECONDS * BYTES_MB / self.mem_table_size)
    }

    fn get_duration_since_last_switch(&self, now: Instant) -> Duration {
        self.last_switch_time
            .map(|time| now.saturating_duration_since(time))
            .unwrap_or_else(|| now.saturating_duration_since(self.init_time))
    }

    fn set_switch_time(&mut self, t: Instant) {
        self.last_switch_time = Some(t);
        self.proposed_time = None;
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

pub(crate) fn split_gen_new_region_metas(
    store_id: u64,
    old_region: &metapb::Region,
    splits: &BatchSplitRequest,
) -> Result<Vec<metapb::Region>> {
    let requests = splits.get_requests();
    if requests.is_empty() {
        return Err(box_err!("missing split key"));
    }
    let new_region_cnt = requests.len();
    let tag = PeerTag::new(store_id, RegionIDVer::from_region(old_region));

    let mut keys = Vec::with_capacity(new_region_cnt + 1);
    keys.push(old_region.start_key.clone());
    for request in requests {
        let split_key = &request.split_key;
        if split_key.is_empty() {
            return Err(box_err!("missing split key"));
        }
        if split_key <= keys.last().unwrap() {
            return Err(box_err!(
                "invalid split requests {:?}, old region {} start_key {:?}, end_key {:?}",
                splits,
                tag,
                old_region.start_key.clone(),
                old_region.end_key.clone()
            ));
        }
        if request.new_peer_ids.len() != old_region.peers.len() {
            return Err(box_err!(
                "invalid new id peer count need {} but got {}",
                old_region.peers.len(),
                request.new_peer_ids.len()
            ));
        }
        keys.push(split_key.clone());
    }
    let end_key = &old_region.end_key;
    if !end_key.is_empty() && end_key <= keys.last().unwrap() {
        return Err(box_err!("invalid split requests {:?}", splits));
    }
    keys.push(end_key.clone());

    let mut derived = old_region.clone();
    let mut new_regions = Vec::with_capacity(new_region_cnt + 1);
    let old_version = old_region.get_region_epoch().get_version();
    info!("split region"; "region" => tag);
    derived
        .mut_region_epoch()
        .set_version(old_version + new_region_cnt as u64);

    // Note that the split requests only contain ids for new regions, so we need
    // to handle new regions and old region separately.
    for (i, req) in requests.iter().enumerate() {
        let mut new_region = metapb::Region::new();
        new_region.set_id(req.new_region_id);
        new_region.set_region_epoch(derived.get_region_epoch().clone());
        new_region.set_start_key(keys[i].clone());
        new_region.set_end_key(keys[i + 1].clone());
        for j in 0..derived.peers.len() {
            let mut new_peer = metapb::Peer::new();
            new_peer.set_id(req.new_peer_ids[j]);
            new_peer.set_store_id(derived.peers[j].get_store_id());
            new_peer.set_role(derived.peers[j].get_role());
            new_region.mut_peers().push(new_peer);
        }
        new_regions.push(new_region);
    }
    derived.start_key = keys[keys.len() - 2].clone();
    new_regions.push(derived);
    Ok(new_regions)
}

pub(crate) fn build_split_pb(
    old: &metapb::Region,
    new_regions: &Vec<metapb::Region>,
    term: u64,
) -> kvenginepb::Split {
    let mut split = kvenginepb::Split::new();
    for new_region in new_regions {
        let mut props = kvenginepb::Properties::new();
        props.set_shard_id(new_region.get_id());
        props.mut_keys().push(TERM_KEY.to_string());
        if new_region.get_id() == old.get_id() {
            props.mut_values().push(term.to_le_bytes().to_vec());
        } else {
            props
                .mut_values()
                .push(RAFT_INIT_LOG_TERM.to_le_bytes().to_vec());
        }
        split.mut_new_shards().push(props);
    }
    for new_region in &new_regions[1..] {
        split.mut_keys().push(raw_start_key(new_region));
    }
    split
}

#[derive(Clone)]
pub(crate) struct ApplyRouter {}

pub(crate) const TERM_KEY: &str = "term";

pub(crate) struct ApplyContext {
    pub(crate) engine: kvengine::Engine,
    pub(crate) router: Option<RaftRouter>, // None in recover mode.
    pub(crate) exec_log_index: u64,
    pub(crate) exec_log_term: u64,
    pub(crate) wb: KVWriteBatch,
    pub(crate) apply_wait: LocalHistogram,
    pub(crate) apply_time: LocalHistogram,
}

impl ApplyContext {
    pub fn new(engine: kvengine::Engine, router: Option<RaftRouter>) -> Self {
        Self {
            engine,
            router,
            exec_log_index: Default::default(),
            exec_log_term: Default::default(),
            wb: KVWriteBatch::new(),
            apply_wait: APPLY_TASK_WAIT_TIME_HISTOGRAM.local(),
            apply_time: APPLY_TIME_HISTOGRAM.local(),
        }
    }

    pub fn finish_for(&self, applier: &Applier, results: VecDeque<ExecResult>) {
        if let Some(router) = &self.router {
            let apply_res = MsgApplyResult {
                results,
                apply_state: applier.apply_state,
                metrics: applier.metrics.clone(),
            };
            let region_id = applier.region.get_id();
            let msg = PeerMsg::ApplyResult(apply_res);
            if let Err(err) = router.peer_sender.send((region_id, msg)) {
                warn!("send apply result error {:?}", err);
            }
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

#[test]
fn test_mem_table_state() {
    // Test that
    #[derive(Clone, Copy, Debug)]
    struct Case {
        size_mb: u64,
        propose_time: Option<u64>,
        write_time: Option<u64>,
        switch_time: Option<u64>,
        check_time: u64,
        check_result: bool,
    }
    impl Case {
        fn new(size_mb: u64) -> Self {
            Case {
                size_mb,
                propose_time: None,
                write_time: None,
                switch_time: None,
                check_time: 0,
                check_result: false,
            }
        }

        fn propose_at(&self, secs: u64) -> Self {
            let mut c = *self;
            c.propose_time = Some(secs);
            c
        }

        fn write_at(&self, secs: u64) -> Self {
            let mut c = *self;
            c.write_time = Some(secs);
            c
        }

        fn switch_at(&self, secs: u64) -> Self {
            let mut c = *self;
            c.switch_time = Some(secs);
            c
        }

        fn check_at(&self, secs: u64) -> Self {
            let mut c = *self;
            c.check_time = secs;
            c
        }

        fn result(&self, b: bool) -> Self {
            let mut c = *self;
            c.check_result = b;
            c
        }
    }
    let cases = vec![
        // check mem size bound
        Case::new(0).result(false),
        // check propose
        Case::new(129).propose_at(0).check_at(3).result(false),
        Case::new(129).propose_at(0).check_at(11).result(true),
        // check idle
        Case::new(1).check_at(1700).result(false),
        Case::new(1).check_at(1900).result(true),
        Case::new(1).write_at(1000).check_at(1900).result(false),
        Case::new(1).write_at(1000).check_at(2900).result(true),
        Case::new(3).check_at(599).result(false),
        Case::new(3).check_at(601).result(true),
        // check switched
        Case::new(32).check_at(39).result(false),
        Case::new(32).check_at(41).result(true),
        Case::new(8).switch_at(100).check_at(259).result(false),
        Case::new(8).switch_at(100).check_at(261).result(true),
        Case::new(4).check_at(319).result(false),
        Case::new(4).check_at(321).result(true),
    ];
    for case in cases {
        let mut states = MemTableState::new(case.size_mb * BYTES_MB);
        states.proposed_time = case
            .propose_time
            .map(|secs| Instant::now() + Duration::from_secs(secs));
        states.last_switch_time = case
            .switch_time
            .map(|secs| Instant::now() + Duration::from_secs(secs));
        states.last_write_time = case
            .write_time
            .map(|secs| Instant::now() + Duration::from_secs(secs));
        let check_time = Instant::now() + Duration::from_secs(case.check_time);
        assert_eq!(
            states.need_switch(check_time),
            case.check_result,
            "{:?}",
            case
        );
    }
}
