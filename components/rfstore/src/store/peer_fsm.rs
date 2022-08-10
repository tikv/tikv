// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicU64, Arc},
    u64,
};

use fail::fail_point;
use kvengine::DEL_PREFIXES_KEY;
use kvproto::{
    import_sstpb::SwitchMode,
    metapb::{self, Region, RegionEpoch},
    raft_cmdpb::{
        CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, StatusCmdType,
        StatusResponse,
    },
    raft_serverpb::RaftMessage,
};
use raft::{self, eraftpb::MessageType, Storage};
use raft_proto::eraftpb;
use raftstore::store::util;
use rand::{thread_rng, Rng};
use tikv_util::{box_err, debug, error, info, time::duration_to_sec, trace, warn};
use txn_types::{Key, WriteBatchFlags};

use crate::{
    store::{
        apply::TERM_KEY,
        cmd_resp::{bind_term, new_error},
        ingest::convert_sst,
        msg::Callback,
        notify_req_region_removed,
        peer::Peer,
        util as _util, ApplyMetrics, ApplyMsg, CasualMessage, Config, CustomBuilder, Engines,
        MsgApplyResult, PdTask, PeerMsg, PersistReady, RaftApplyState, RaftContext, SignificantMsg,
        SnapState, StoreMsg, Ticker, PEER_TICK_PD_HEARTBEAT, PEER_TICK_RAFT, PEER_TICK_RAFT_LOG_GC,
        PEER_TICK_SPLIT_CHECK, PEER_TICK_SWITCH_MEM_TABLE_CHECK,
    },
    DiscardReason, Error, RaftStoreRouter, Result,
};

/// Limits the maximum number of regions returned by error.
///
/// Another choice is using coprocessor batch limit, but 10 should be a good fit in most case.
const _MAX_REGIONS_IN_ERROR: usize = 10;

pub struct PeerFsm {
    pub(crate) peer: Peer,
    pub(crate) stopped: bool,
    // apply_worker_idx is initialized randomly, and can be changed based on workload.
    pub(crate) apply_worker_idx: usize,
    // applying_cnt is increased by raft worker and decreased by apply worker.
    // When we need to change the worker idx, we need to make sure the applying_cnt is zero.
    pub(crate) applying_cnt: Arc<AtomicU64>,
    ticker: Ticker,
}

impl PeerFsm {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        region: &metapb::Region,
    ) -> Result<PeerFsm> {
        let meta_peer = match util::find_peer(region, store_id) {
            None => {
                return Err(box_err!(
                    "find no peer for store {} in region {:?}",
                    store_id,
                    region
                ));
            }
            Some(peer) => peer.clone(),
        };

        info!(
            "create peer";
            "region_id" => region.get_id(),
            "peer_id" => meta_peer.get_id(),
        );
        Ok(PeerFsm {
            peer: Peer::new(store_id, cfg, engines, region, meta_peer)?,
            stopped: false,
            apply_worker_idx: thread_rng().gen_range(0..cfg.apply_pool_size),
            applying_cnt: Arc::new(AtomicU64::new(0)),
            ticker: Ticker::new(cfg),
        })
    }

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after applying snapshot.
    pub fn replicate(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        region_id: u64,
        peer: metapb::Peer,
    ) -> Result<PeerFsm> {
        // We will remove tombstone key when apply snapshot
        info!(
            "replicate peer";
            "region_id" => region_id,
            "peer_id" => peer.get_id(),
        );

        let mut region = metapb::Region::default();
        region.set_id(region_id);
        Ok(PeerFsm {
            peer: Peer::new(store_id, cfg, engines, &region, peer)?,
            stopped: false,
            ticker: Ticker::new(cfg),
            apply_worker_idx: thread_rng().gen_range(0..cfg.apply_pool_size),
            applying_cnt: Arc::new(AtomicU64::new(0)),
        })
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.peer.region().get_id()
    }

    #[inline]
    pub(crate) fn get_peer(&self) -> &Peer {
        &self.peer
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer.peer_id()
    }

    #[inline]
    pub fn stop(&mut self) {
        self.stopped = true;
    }
}

pub(crate) struct PeerMsgHandler<'a> {
    fsm: &'a mut PeerFsm,
    pub(crate) ctx: &'a mut RaftContext,
}

impl<'a> Deref for PeerMsgHandler<'a> {
    type Target = PeerFsm;

    fn deref(&self) -> &Self::Target {
        self.fsm
    }
}

impl<'a> DerefMut for PeerMsgHandler<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.fsm
    }
}

impl<'a> PeerMsgHandler<'a> {
    pub(crate) fn new(fsm: &'a mut PeerFsm, ctx: &'a mut RaftContext) -> PeerMsgHandler<'a> {
        PeerMsgHandler { fsm, ctx }
    }

    pub fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg>) {
        for m in msgs.drain(..) {
            match m {
                PeerMsg::RaftMessage(msg) => {
                    if let Err(e) = self.on_raft_message(msg) {
                        error!(%e;
                            "handle raft message err";
                            "tag" => self.peer.tag(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                }
                PeerMsg::RaftCommand(cmd) => {
                    self.ctx
                        .raft_metrics
                        .propose
                        .request_wait_time
                        .observe(duration_to_sec(cmd.send_time.saturating_elapsed()) as f64);
                    self.propose_raft_command(cmd.request, cmd.callback);
                }
                PeerMsg::Tick => self.on_tick(),
                PeerMsg::ApplyResult(res) => {
                    self.on_apply_result(res);
                }
                PeerMsg::SignificantMsg(msg) => self.on_significant_msg(msg),
                PeerMsg::CasualMessage(msg) => self.on_casual_msg(msg),
                PeerMsg::Start => self.start(),
                PeerMsg::GenerateEngineChangeSet(cs) => self.on_generate_engine_change_set(cs),
                PeerMsg::ApplyChangeSetResult(res) => {
                    self.on_apply_change_set_result(res);
                }
                PeerMsg::Persisted(ready) => {
                    self.on_persisted(ready);
                }
                PeerMsg::PrepareChangeSetResult(res) => {
                    self.on_prepared_change_set(res);
                }
            }
        }
    }

    fn on_casual_msg(&mut self, msg: CasualMessage) {
        match msg {
            CasualMessage::SplitRegion {
                region_epoch,
                split_keys,
                callback,
                source,
            } => {
                self.on_prepare_split_region(region_epoch, split_keys, callback, &source);
            }
            CasualMessage::HalfSplitRegion {
                region_epoch: _,
                policy: _,
                source: _,
            } => {
                // TODO(x) handle half split region;
                warn!("ignore half split region");
            }
            CasualMessage::DeletePrefix {
                region_version,
                prefix,
                callback,
            } => self.on_delete_prefix(region_version, prefix, callback),
        }
    }

    fn on_tick(&mut self) {
        if self.fsm.stopped {
            return;
        }
        trace!(
            "tick";
            "peer_id" => self.fsm.peer_id(),
            "tag" => self.peer.tag(),
        );
        self.ticker.tick_clock();
        if self.ticker.is_on_tick(PEER_TICK_RAFT) {
            self.on_raft_base_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_PD_HEARTBEAT) {
            self.on_pd_heartbeat_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_SPLIT_CHECK) {
            self.on_split_region_check_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_SWITCH_MEM_TABLE_CHECK) {
            self.on_switch_mem_table_check_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_RAFT_LOG_GC) {
            self.on_raft_log_gc_tick();
        }
    }

    fn start(&mut self) {
        self.ticker.schedule(PEER_TICK_RAFT);
        self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
        self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
        self.ticker.schedule(PEER_TICK_SWITCH_MEM_TABLE_CHECK);
        self.ticker.schedule(PEER_TICK_RAFT_LOG_GC);
    }

    fn on_significant_msg(&mut self, msg: SignificantMsg) {
        match msg {
            SignificantMsg::StoreUnreachable { store_id } => {
                if let Some(peer_id) = util::find_peer(self.region(), store_id).map(|p| p.get_id())
                {
                    if self.fsm.peer.is_leader() {
                        self.fsm.peer.raft_group.report_unreachable(peer_id);
                    }
                }
            }
        }
    }

    #[inline]
    fn region_id(&self) -> u64 {
        self.fsm.peer.region().get_id()
    }

    #[inline]
    fn region(&self) -> &Region {
        self.fsm.peer.region()
    }

    #[inline]
    fn store_id(&self) -> u64 {
        self.fsm.peer.peer.get_store_id()
    }

    fn on_raft_base_tick(&mut self) {
        if self.peer.pending_remove {
            self.peer.mut_store().flush_cache_metrics();
            return;
        }
        self.ticker.schedule(PEER_TICK_RAFT);
        // When having pending snapshot, if election timeout is met, it can't pass
        // the pending conf change check because first index has been updated to
        // a value that is larger than last index.
        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_pending_snapshot() {
            // need to check if snapshot is applied.
            return;
        }
        let raft_election_timeout_ticks = self.ctx.cfg.raft_election_timeout_ticks;
        self.peer.retry_pending_reads(raft_election_timeout_ticks);
        self.fsm.peer.raft_group.tick();
        self.fsm.peer.mut_store().flush_cache_metrics();
        if self.peer.need_campaign {
            let _ = self.peer.raft_group.campaign();
            self.peer.need_campaign = false;
        }
    }

    fn on_apply_result(&mut self, res: MsgApplyResult) {
        if res.peer_id != self.peer.peer_id() {
            warn!(
                "{} mismatch apply result peer_id {}",
                self.peer.tag(),
                res.peer_id
            );
            return;
        }
        fail_point!("on_apply_res", |_| {});
        if !self.fsm.peer.pending_apply_results.is_empty() {
            // Apply results should be handled in order but there is a pending one, so
            // delay to handle it.
            self.fsm.peer.pending_apply_results.push(res);
            return;
        }
        if !res.results.is_empty() {
            self.fsm.peer.pending_apply_results.push(res);
            // Some metadata change need to be handled by store FSM, so send the result to it
            // and store FSM will handle all pending results of the peer.
            let region_id = self.region_id();
            let peer_id = self.peer_id();
            self.ctx
                .global
                .router
                .send_store(StoreMsg::ApplyResult { region_id, peer_id });
            return;
        }
        debug!(
            "async apply finish";
            "tag" => self.peer.tag(),
            "peer_id" => self.fsm.peer_id(),
            "res" => ?res,
        );
        // TODO(x) update metrics.
        self.fsm.peer.post_apply(self.ctx, &res);
        if self.fsm.stopped {}
    }

    fn on_transfer_leader_msg(&mut self, msg: &eraftpb::Message) {
        // log_term is set by original leader, represents the term last log is written
        // in, which should be equal to the original leader's term.
        if msg.get_log_term() != self.fsm.peer.term() {
            return;
        }
        if self.fsm.peer.is_leader() {
            let from = match self.fsm.peer.get_peer_from_cache(msg.get_from()) {
                Some(p) => p,
                None => return,
            };
            match self
                .fsm
                .peer
                .ready_to_transfer_leader(self.ctx, msg.get_index(), &from)
            {
                Some(reason) => {
                    info!(
                        "reject to transfer leader";
                        "tag" => self.peer.tag(),
                        "peer_id" => self.fsm.peer_id(),
                        "to" => ?from,
                        "reason" => reason,
                        "index" => msg.get_index(),
                        "last_index" => self.fsm.peer.get_store().last_index(),
                    );
                }
                None => {
                    self.fsm.peer.transfer_leader(&from);
                }
            }
        } else {
            self.fsm.peer.execute_transfer_leader(self.ctx, msg);
        }
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        let msg_debug = MsgDebug(&msg);
        if msg_debug.need_log() {
            debug!(
                "handle raft message";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
                "message" => %msg_debug,
            );
        }

        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }
        if self.fsm.peer.pending_remove || self.fsm.stopped {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if self.check_msg(&msg) {
            return Ok(());
        }

        if msg.has_extra_msg() {
            self.on_extra_message(msg);
            return Ok(());
        }

        let is_snapshot = msg.get_message().has_snapshot();
        let regions_to_destroy = self.check_snapshot(&msg)?;

        self.fsm.peer.insert_peer_cache(msg.take_from_peer());

        let result = if msg.get_message().get_msg_type() == MessageType::MsgTransferLeader {
            self.on_transfer_leader_msg(msg.get_message());
            Ok(())
        } else {
            self.fsm.peer.step(msg.take_message())
        };

        if is_snapshot && self.fsm.peer.has_pending_snapshot() {
            self.destroy_regions_for_snapshot(regions_to_destroy);
        }
        result
    }

    fn on_extra_message(&mut self, _msg: RaftMessage) {
        // TODO(x)
    }

    // return false means the message is invalid, and can be ignored.
    fn validate_raft_msg(&mut self, msg: &RaftMessage) -> bool {
        let region_id = msg.get_region_id();
        let to = msg.get_to_peer();

        if to.get_store_id() != self.store_id() {
            warn!(
                "store not match, ignore it";
                "tag" => self.peer.tag(),
                "region_id" => region_id,
                "to_store_id" => to.get_store_id(),
                "my_store_id" => self.store_id(),
            );
            self.ctx.raft_metrics.message_dropped.mismatch_store_id += 1;
            return false;
        }

        if !msg.has_region_epoch() {
            error!(
                "missing epoch in raft message, ignore it";
                "tag" => self.peer.tag(),
                "region_id" => region_id,
            );
            self.ctx.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return false;
        }

        true
    }

    /// Checks if the message is sent to the correct peer.
    ///
    /// Returns true means that the message can be dropped silently.
    fn check_msg(&mut self, msg: &RaftMessage) -> bool {
        let from_epoch = msg.get_region_epoch();
        let from_store_id = msg.get_from_peer().get_store_id();

        // Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
        // a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
        // c. 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
        // d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
        //  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
        //  rejoin the raft group again.
        // f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
        //  unlike case e, 2 will be stale forever.
        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
        // tell 2 is stale, so 2 can remove itself.
        if util::is_epoch_stale(from_epoch, self.fsm.peer.region().get_region_epoch())
            && util::find_peer(self.fsm.peer.region(), from_store_id).is_none()
        {
            self.ctx
                .handle_stale_msg(msg, self.fsm.peer.region().get_region_epoch().clone(), None);
            return true;
        }

        let target = msg.get_to_peer();
        match target.get_id().cmp(&self.fsm.peer.peer_id()) {
            cmp::Ordering::Less => {
                info!(
                    "target peer id is smaller, msg maybe stale";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_peer" => ?target,
                );
                self.ctx.raft_metrics.message_dropped.stale_msg += 1;
                true
            }
            cmp::Ordering::Greater => {
                if self.fsm.peer.maybe_destroy() {
                    self.ctx.apply_msgs.msgs.push(ApplyMsg::UnsafeDestroy {
                        region_id: self.region_id(),
                    });
                } else {
                    self.ctx.raft_metrics.message_dropped.applying_snap += 1;
                }
                true
            }
            cmp::Ordering::Equal => false,
        }
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let from_epoch = msg.get_region_epoch();
        if !util::is_epoch_stale(self.fsm.peer.region().get_region_epoch(), from_epoch) {
            return;
        }

        if self.fsm.peer.peer != *msg.get_to_peer() {
            info!(
                "receive stale gc message, ignore.";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }
        // TODO: ask pd to guarantee we are stale now.
        info!(
            "receives gc message, trying to remove";
            "tag" => self.peer.tag(),
            "peer_id" => self.fsm.peer_id(),
            "to_peer" => ?msg.get_to_peer(),
        );
        if self.fsm.peer.maybe_destroy() {
            // Destroy the apply fsm first, wait for the reply msg from apply fsm
            self.ctx.apply_msgs.msgs.push(ApplyMsg::UnsafeDestroy {
                region_id: self.region_id(),
            });
        } else {
            self.ctx.raft_metrics.message_dropped.applying_snap += 1;
        }
    }

    // Returns `Vec<(u64, bool)>` indicated (source_region_id, merge_to_this_peer) if the `msg`
    // doesn't contain a snapshot or this snapshot doesn't conflict with any other snapshots or regions.
    // Otherwise a `SnapKey` is returned.
    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<Vec<(u64, bool)>> {
        if !msg.get_message().has_snapshot() {
            return Ok(vec![]);
        }
        // TODO(x)
        Ok(vec![])
    }

    fn destroy_regions_for_snapshot(&mut self, regions_to_destroy: Vec<(u64, bool)>) {
        if regions_to_destroy.is_empty() {}
        // TODO(x)
    }

    fn pre_propose_raft_command(
        &mut self,
        msg: &RaftCmdRequest,
    ) -> Result<Option<RaftCmdResponse>> {
        // Check store_id, make sure that the msg is dispatched to the right place.
        if let Err(e) = _util::check_store_id(msg, self.store_id()) {
            self.ctx.raft_metrics.invalid_proposal.mismatch_store_id += 1;
            return Err(e);
        }
        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            let resp = self.execute_status_command(msg)?;
            return Ok(Some(resp));
        }

        // Check whether the store has the right peer to handle the request.
        let region_id = self.region_id();
        let leader_id = self.fsm.peer.leader_id();
        let request = msg.get_requests();

        // ReadIndex can be processed on the replicas.
        let is_read_index_request =
            request.len() == 1 && request[0].get_cmd_type() == CmdType::ReadIndex;
        let mut read_only = true;
        for r in msg.get_requests() {
            match r.get_cmd_type() {
                CmdType::Get | CmdType::Snap | CmdType::ReadIndex => (),
                _ => read_only = false,
            }
        }
        let allow_replica_read = read_only && msg.get_header().get_replica_read();
        let flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
        let allow_stale_read = read_only && flags.contains(WriteBatchFlags::STALE_READ);
        if !self.fsm.peer.is_leader()
            && !is_read_index_request
            && !allow_replica_read
            && !allow_stale_read
        {
            self.ctx.raft_metrics.invalid_proposal.not_leader += 1;
            let leader = self.fsm.peer.get_peer_from_cache(leader_id);
            return Err(Error::NotLeader(region_id, leader));
        }
        // peer_id must be the same as peer's.
        if let Err(e) = _util::check_peer_id(msg, self.fsm.peer.peer_id()) {
            self.ctx.raft_metrics.invalid_proposal.mismatch_peer_id += 1;
            return Err(e);
        }
        // check whether the peer is initialized.
        if !self.fsm.peer.is_initialized() {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .region_not_initialized += 1;
            return Err(Error::RegionNotInitialized(region_id));
        }
        // If the peer is applying snapshot, it may drop some sending messages, that could
        // make clients wait for response until timeout.
        if self.fsm.peer.is_applying_snapshot() {
            self.ctx.raft_metrics.invalid_proposal.is_applying_snapshot += 1;
            // TODO: replace to a more suitable error.
            return Err(Error::Other(box_err!(
                "{} peer is applying snapshot",
                self.fsm.peer.tag()
            )));
        }
        // Check whether the term is stale.
        if let Err(e) = _util::check_term(msg, self.fsm.peer.term()) {
            self.ctx.raft_metrics.invalid_proposal.stale_command += 1;
            return Err(e);
        }

        match _util::check_region_epoch(msg, self.fsm.peer.region(), true) {
            Err(Error::EpochNotMatch(m, new_regions)) => {
                // Attach the region which might be split from the current region. But it doesn't
                // matter if the region is not split from the current region. If the region meta
                // received by the TiKV driver is newer than the meta cached in the driver, the meta is
                // updated.
                // TODO(x) add sibling region.
                self.ctx.raft_metrics.invalid_proposal.epoch_not_match += 1;
                Err(Error::EpochNotMatch(m, new_regions))
            }
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    fn propose_raft_command(&mut self, msg: RaftCmdRequest, cb: Callback) {
        match self.pre_propose_raft_command(&msg) {
            Ok(Some(resp)) => {
                cb.invoke_with_response(resp);
                return;
            }
            Err(e) => {
                debug!(
                    "failed to propose";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                    "message" => ?msg,
                    "err" => %e,
                );
                cb.invoke_with_response(new_error(e));
                return;
            }
            _ => (),
        }

        if self.fsm.peer.pending_remove {
            notify_req_region_removed(self.region_id(), cb);
            return;
        }
        if !msg.get_requests().is_empty() && msg.get_requests()[0].has_ingest_sst() {
            self.propose_ingest_sst(msg, cb);
            return;
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.

        let mut resp = RaftCmdResponse::default();
        let term = self.fsm.peer.term();
        bind_term(&mut resp, term);
        self.fsm.peer.propose(self.ctx, cb, msg, resp);

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn propose_ingest_sst(&mut self, msg: RaftCmdRequest, cb: Callback) {
        // This is a ingest sst request, we need to redirect to worker thread and convert
        // it to cloud engine format.
        let importer = self.ctx.global.importer.clone();
        let router = self.ctx.global.router.clone();
        let kv = self.ctx.global.engines.kv.clone();
        let shard_meta = self.peer.get_store().shard_meta.as_ref().unwrap().clone();
        std::thread::spawn(move || {
            match convert_sst(kv, importer, &msg, shard_meta) {
                Ok(cs) => {
                    // Make ingest command.
                    let mut cmd = RaftCmdRequest::default();
                    cmd.set_header(msg.get_header().clone());
                    let mut custom_builder = CustomBuilder::new();
                    custom_builder.set_change_set(cs);
                    cmd.set_custom_request(custom_builder.build());
                    router.send_command(cmd, cb);
                }
                Err(e) => {
                    cb.invoke_with_response(new_error(e));
                }
            }
        });
    }

    fn on_split_region_check_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
        if let Some(shard) = self.ctx.global.engines.kv.get_shard(self.region_id()) {
            self.peer.peer_stat.approximate_size = shard.get_estimated_size();
            self.peer.peer_stat.approximate_keys = shard.get_estimated_entries();
            if !self.fsm.peer.is_leader() {
                return;
            }
            if !shard.get_initial_flushed() {
                return;
            }
            // When Lightning or BR is importing data to TiKV, their ingest-request may fail because of
            // region-epoch not matched. So we hope TiKV do not check region size and split region during
            // importing.
            if self.ctx.global.importer.get_mode() == SwitchMode::Import {
                return;
            }
            let region_max_size = self.ctx.cfg.region_split_size.0 * 3 / 2;
            let region_max_entries = region_max_size / 100;
            let estimated_size = shard.get_estimated_size();
            let estimated_entries = shard.get_estimated_entries();
            raftstore::coprocessor::metrics::REGION_SIZE_HISTOGRAM.observe(estimated_size as f64);
            raftstore::coprocessor::metrics::REGION_KEYS_HISTOGRAM
                .observe(estimated_entries as f64);
            if estimated_size < region_max_size && estimated_entries < region_max_entries {
                return;
            }
            if let Some(k) = shard.get_suggest_split_key() {
                info!(
                    "region {} split, estimated size {}, estimated entries {}, max_size {}",
                    self.peer.tag(),
                    estimated_size,
                    estimated_entries,
                    region_max_size,
                );
                let task = PdTask::AskBatchSplit {
                    region: self.region().clone(),
                    split_keys: vec![Key::from_raw(&k).as_encoded().to_vec()],
                    peer: self.peer.peer.clone(),
                    right_derive: true,
                    callback: Callback::None,
                };
                self.ctx.global.pd_scheduler.schedule(task).unwrap();
            }
        }
    }

    // For idle regions, we need tick to trigger switch mem-table to reduce memory consumption.
    // Only the applier knows how large the mem-table is and when the mem-table has been switched,
    // so we don't need to check anything, just send a message.
    fn on_switch_mem_table_check_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_SWITCH_MEM_TABLE_CHECK);
        if !self.peer.is_leader() {
            return;
        }
        let region_id = self.region_id();
        self.ctx
            .apply_msgs
            .msgs
            .push(ApplyMsg::CheckSwitchMemTable { region_id });
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        cb: Callback,
        source: &str,
    ) {
        if let Err(e) = self.validate_split_region(&region_epoch, &split_keys) {
            info!(
                "prepare split error";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
                "split_keys" => %util::KeysInfoFormatter(split_keys.iter()),
                "source" => source,
                "error" => ?e,
            );
            cb.invoke_with_response(new_error(e));
            return;
        }
        info!(
            "on split";
            "tag" => self.peer.tag(),
            "peer_id" => self.fsm.peer_id(),
            "split_keys" => %util::KeysInfoFormatter(split_keys.iter()),
            "source" => source,
        );
        let task = PdTask::AskBatchSplit {
            region: self.region().clone(),
            split_keys,
            peer: self.peer.peer.clone(),
            right_derive: true,
            callback: cb,
        };
        self.ctx.global.pd_scheduler.schedule(task).unwrap();
    }

    fn validate_split_region(
        &mut self,
        epoch: &metapb::RegionEpoch,
        split_keys: &[Vec<u8>],
    ) -> Result<()> {
        if split_keys.is_empty() {
            error!(
                "no split key is specified.";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
            );
            return Err(box_err!(
                "{} no split key is specified.",
                self.fsm.peer.tag()
            ));
        }
        for key in split_keys {
            if key.is_empty() {
                error!(
                    "split key should not be empty!!!";
                    "tag" => self.peer.tag(),
                    "peer_id" => self.fsm.peer_id(),
                );
                return Err(box_err!(
                    "{} split key should not be empty",
                    self.fsm.peer.tag()
                ));
            }
        }
        if !self.fsm.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            info!(
                "not leader, skip.";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
            );
            return Err(Error::NotLeader(
                self.region_id(),
                self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()),
            ));
        }

        let region = self.fsm.peer.region();
        let latest_epoch = region.get_region_epoch();

        // This is a little difference for `check_region_epoch` in region split case.
        // Here we just need to check `version` because `conf_ver` will be update
        // to the latest value of the peer, and then send to PD.
        if latest_epoch.get_version() != epoch.get_version() {
            info!(
                "epoch changed, retry later";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
                "prev_epoch" => ?region.get_region_epoch(),
                "epoch" => ?epoch,
            );
            return Err(Error::EpochNotMatch(
                format!(
                    "{} epoch changed {:?} != {:?}, retry later",
                    self.fsm.peer.tag(),
                    latest_epoch,
                    epoch
                ),
                vec![region.to_owned()],
            ));
        }
        if !self.peer.get_store().initial_flushed {
            return Err(Error::Transport(DiscardReason::Full));
        }
        Ok(())
    }

    fn on_delete_prefix(&mut self, region_version: u64, prefix: Vec<u8>, callback: Callback) {
        if !self.peer.is_leader() {
            callback.invoke_with_response(RaftCmdResponse::default());
            return;
        }
        let tag = self.peer.tag();
        let id_ver = tag.id_ver;
        if region_version != id_ver.ver() {
            warn!("{} delete prefix version not match", tag);
            callback.invoke_with_response(RaftCmdResponse::default());
            return;
        }
        let mut cmd = self.new_raft_cmd_request();
        let mut cs = kvengine::new_change_set(id_ver.id(), id_ver.ver());
        cs.set_property_key(DEL_PREFIXES_KEY.to_string());
        cs.set_property_value(prefix);
        cs.set_property_merge(true);
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_change_set(cs);
        cmd.set_custom_request(custom_builder.build());
        self.propose_raft_command(cmd, callback);
    }

    fn on_pd_heartbeat_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
        self.fsm.peer.check_peers();
        self.update_max_lag_metrics();

        if !self.fsm.peer.is_leader() {
            return;
        }
        self.fsm.peer.heartbeat_pd(self.ctx);
    }

    fn on_generate_engine_change_set(&mut self, cs: kvenginepb::ChangeSet) {
        let tag = self.peer.tag();
        info!("generate meta change event {:?}", &cs; "region" => tag);
        self.propose_change_set(cs);
    }

    fn propose_change_set(&mut self, cs: kvenginepb::ChangeSet) {
        let tag = self.peer.tag();
        let mut req = self.new_raft_cmd_request();
        let mut builder = CustomBuilder::new();
        builder.set_change_set(cs);
        let custom_req = builder.build();
        req.set_custom_request(custom_req);
        let cb = Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                let err_msg = resp.response.get_header().get_error().get_message();
                warn!("{} failed to propose engine change set {:?}", tag, err_msg);
                // TODO(x): handle the error.
                // We need to detect if this error can be retried and retry it.
                // Or we may lose data if it is a flush.
                // And we need to stop propose change set if previous change set propose failed.
            } else {
                info!("{} proposed meta change event", tag);
            }
        }));
        self.propose_raft_command(req, cb);
    }

    fn new_raft_cmd_request(&self) -> RaftCmdRequest {
        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(self.region_id());
        header.set_peer(self.peer.peer.clone());
        header.set_region_epoch(self.region().get_region_epoch().clone());
        header.set_term(self.peer.term());
        req.set_header(header);
        req
    }

    fn on_apply_change_set_result(&mut self, result: kvengine::Result<kvenginepb::ChangeSet>) {
        let tag = self.peer.tag();
        if let Err(err) = result {
            error!(
                "region failed to apply change set";
                "err" => ?err,
                "region" => tag,
            );
            if self.peer.mut_store().snap_state == SnapState::Applying {
                self.peer.mut_store().snap_state = SnapState::ApplyAborted;
            }
            return;
        }
        let change = result.unwrap();
        if change.shard_ver != self.region().get_region_epoch().get_version() {
            error!("change set version not match change {:?}", &change; "region" => tag);
            return;
        }
        if change.has_snapshot() {
            if self.peer.mut_store().is_applying_snapshot() {
                self.peer.mut_store().snap_state = SnapState::Relax;
                let apply_state = RaftApplyState::new(
                    self.peer.mut_store().snapshot_index(),
                    self.peer.mut_store().snapshot_term(),
                );
                let apply_result = MsgApplyResult {
                    peer_id: self.peer.peer_id(),
                    results: VecDeque::new(),
                    apply_state,
                    metrics: ApplyMetrics::default(),
                };
                if self.peer.raft_group.raft.raft_log.persisted >= apply_state.applied_index {
                    self.fsm.peer.post_apply(self.ctx, &apply_result);
                } else {
                    self.peer.mut_store().on_persist_apply_result = Some(apply_result);
                }
                let on_apply_snapshot_msgs =
                    std::mem::take(&mut self.peer.mut_store().on_apply_snapshot_msgs);
                for msg in on_apply_snapshot_msgs {
                    if let Err(err) = self.ctx.global.trans.send(msg) {
                        error!("failed to send on apply snapshot msg {:?}", err);
                    }
                }
            }
        }
        if change.has_snapshot() || change.has_initial_flush() {
            self.peer.mut_store().initial_flushed = true;
        }
    }

    fn on_persisted(&mut self, ready: PersistReady) {
        if ready.peer_id != self.fsm.peer_id() {
            error!(
                "peer id not match";
                "tag" => self.peer.tag(),
                "peer_id" => self.fsm.peer_id(),
                "persisted_peer_id" => ready.peer_id,
                "persisted_number" => ready.ready_number,
            );
            return;
        }
        self.peer.raft_group.on_persist_ready(ready.ready_number);
        if let Some(apply_result) = self.peer.mut_store().on_persist_apply_result.take() {
            self.fsm.peer.post_apply(self.ctx, &apply_result);
        }
    }

    pub(crate) fn on_prepared_change_set(&mut self, res: kvengine::Result<kvengine::ChangeSet>) {
        if res.is_err() {
            // TODO(x): properly handle this error.
            panic!(
                "{} failed to prepare change set {:?}",
                self.peer.tag(),
                res.unwrap_err()
            );
        }
        self.ctx
            .apply_msgs
            .msgs
            .push(ApplyMsg::ApplyChangeSet(res.unwrap()));
    }

    pub(crate) fn update_max_lag_metrics(&mut self) {
        if !self.fsm.peer.is_leader() {
            return;
        }
        let last_idx = self.peer.get_store().last_index();
        let replicated_idx = self
            .peer
            .raft_group
            .raft
            .prs()
            .iter()
            .map(|(_, pr)| pr.matched)
            .min()
            .unwrap_or_default();
        // When an election happened or a new peer is added, replicated_idx can be 0.
        if replicated_idx > 0 {
            assert!(
                last_idx >= replicated_idx,
                "expect last index {} >= replicated index {}",
                last_idx,
                replicated_idx
            );
            raftstore::store::metrics::REGION_MAX_LOG_LAG
                .observe((last_idx - replicated_idx) as f64);
        }
    }

    fn on_raft_log_gc_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_RAFT_LOG_GC);

        let region_id = self.region_id();
        let engines = &self.ctx.global.engines;
        if !self.peer.is_initialized()
            || self.peer.is_applying_snapshot()
            || !engines.raft.can_truncate(region_id)
        {
            return;
        }

        let last_idx = self.peer.get_store().last_index();
        let replicated_idx = if self.peer.is_leader() {
            self.peer
                .raft_group
                .raft
                .prs()
                .iter()
                .filter_map(|(peer_id, pr)| {
                    // Don't keep raft logs for down peer. It may be too long(default 10mins).
                    (!self.peer.down_peer_ids.contains(peer_id)).then(|| pr.matched)
                })
                .min()
                .unwrap_or(last_idx)
        } else {
            // Followers can't know the progress of other replicas, so they truncate logs as many
            // as possible, but it may result in snapshot transport when becomes leader.
            // One solution is that leader propagates truncated index to followers.
            last_idx
        };
        let size_limit_index = engines.raft.index_to_truncate_to_size(
            self.region_id(),
            self.ctx.cfg.raft_log_gc_size_limit.unwrap().0 as usize,
        );
        let applied_idx = self.peer.get_store().applied_index();

        // If raft logs occupies too much memory, truncates them regardless of lagged replicas to avoid OOM.
        let mut to_truncate_idx = cmp::max(size_limit_index, replicated_idx);
        // Shouldn't truncate unapplied logs.
        to_truncate_idx = cmp::min(to_truncate_idx, applied_idx);

        // persisted_log_idx is the upper limit of truncated_idx.
        let mut persisted_log_idx = self.peer.get_store().data_persisted_log_index().unwrap();
        if to_truncate_idx > persisted_log_idx {
            let applied_term = self.peer.get_store().applied_index_term();
            // Try to advance applied_idx.
            if persisted_log_idx < applied_idx {
                let shard_data_all_persisted = engines
                    .kv
                    .get_shard(region_id)
                    .unwrap()
                    .data_all_persisted();
                if shard_data_all_persisted {
                    // Advance persisted log index to applied index manually because some raft logs(ChangeSet
                    // and ConfChange) don't write data to kvengine, so they can't trigger memtable
                    // flush which results in unGCed logs.
                    let shard_meta = self.peer.mut_store().mut_engine_meta();
                    shard_meta.data_sequence = applied_idx;
                    shard_meta.set_property(TERM_KEY, &applied_term.to_le_bytes());
                    let meta_data = shard_meta.marshal();
                    info!(
                        "advance data sequence from {} to {}", persisted_log_idx, applied_idx;
                        "region" => self.peer.tag(),
                    );
                    self.ctx
                        .raft_wb
                        .set_state(region_id, _util::KV_ENGINE_META_KEY, &meta_data);
                    persisted_log_idx = applied_idx;
                }
            }
            to_truncate_idx = cmp::min(to_truncate_idx, persisted_log_idx);
        };
        assert!(to_truncate_idx <= persisted_log_idx && to_truncate_idx <= applied_idx);
        let truncated_idx = self.peer.get_store().truncated_index();
        if to_truncate_idx > truncated_idx {
            let to_truncate_term = self.peer.get_store().term(to_truncate_idx).unwrap();
            self.ctx
                .raft_wb
                .truncate_raft_log(region_id, to_truncate_idx, to_truncate_term);
            info!(
                "{} truncate raft logs to {}, term {}",
                self.peer.tag(),
                to_truncate_idx,
                to_truncate_term,
            );
        }
    }
}

pub fn new_read_index_request(
    region_id: u64,
    region_epoch: RegionEpoch,
    peer: metapb::Peer,
) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::default();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_region_epoch(region_epoch);
    request.mut_header().set_peer(peer);
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::ReadIndex);
    request
}

pub fn new_admin_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::default();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request
}

/// For status command.
impl PeerMsgHandler<'_> {
    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self, request: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();

        let mut response = match cmd_type {
            StatusCmdType::RegionLeader => self.execute_region_leader(),
            StatusCmdType::RegionDetail => self.execute_region_detail(request),
            StatusCmdType::InvalidStatus => {
                Err(box_err!("{} invalid status command!", self.fsm.peer.tag()))
            }
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::default();
        resp.set_status_response(response);
        // Bind peer current term here.
        bind_term(&mut resp, self.fsm.peer.term());
        Ok(resp)
    }

    fn execute_region_leader(&mut self) -> Result<StatusResponse> {
        let mut resp = StatusResponse::default();
        if let Some(leader) = self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }

    fn execute_region_detail(&mut self, request: &RaftCmdRequest) -> Result<StatusResponse> {
        if !self.fsm.peer.get_store().is_initialized() {
            let region_id = request.get_header().get_region_id();
            return Err(Error::RegionNotInitialized(region_id));
        }
        let mut resp = StatusResponse::default();
        resp.mut_region_detail()
            .set_region(self.fsm.peer.region().clone());
        if let Some(leader) = self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()) {
            resp.mut_region_detail().set_leader(leader);
        }

        Ok(resp)
    }
}

pub struct MsgDebug<'a>(pub &'a RaftMessage);

impl MsgDebug<'_> {
    fn need_log(&self) -> bool {
        match self.0.get_message().msg_type {
            MessageType::MsgHeartbeat | MessageType::MsgHeartbeatResponse => false,
            _ => true,
        }
    }
}

impl std::fmt::Display for MsgDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = self.0.get_message();
        write!(
            f,
            "{:?}, to: {}, from: {}, term: {}, log_term: {}, index: {}, commit: {}, reject: {}, num_entries: {}",
            msg.msg_type,
            msg.to,
            msg.from,
            msg.term,
            msg.log_term,
            msg.index,
            msg.commit,
            msg.reject,
            msg.get_entries().len(),
        )
    }
}
