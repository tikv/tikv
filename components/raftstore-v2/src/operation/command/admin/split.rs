// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains batch split related processing logic.
//!
//! Process Overview
//!
//! Propose:
//! - Nothing special except for validating batch split requests (ex: split keys
//!   are in ascending order).
//!
//! Apply:
//! - apply_batch_split: Create and initialize metapb::region for split regions
//!   and derived regions. Then, create checkpoints of the current talbet for
//!   split regions and derived region to make tablet physical isolated. Update
//!   the parent region's region state without persistency. Send the new regions
//!   (including derived region) back to raftstore.
//!
//! On Apply Result:
//! - on_ready_split_region: Update the relevant in memory meta info of the
//!   parent peer, then send to the store the relevant info needed to create and
//!   initialize the split regions.
//!
//! Split peer creation and initlization:
//! - on_split_init: In normal cases, the uninitialized split region will be
//!   created by the store, and here init it using the data sent from the parent
//!   peer.

use std::{borrow::Cow, cmp, path::PathBuf};

use collections::HashSet;
use crossbeam::channel::SendError;
use engine_traits::{
    Checkpointer, KvEngine, RaftEngine, RaftLogBatch, TabletContext, TabletRegistry,
};
use fail::fail_point;
use kvproto::{
    metapb::{self, Region, RegionEpoch},
    pdpb::CheckPolicy,
    raft_cmdpb::{AdminRequest, AdminResponse, RaftCmdRequest, SplitRequest},
    raft_serverpb::RaftSnapshotData,
};
use protobuf::Message;
use raft::{prelude::Snapshot, INVALID_ID};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{
        cmd_resp,
        fsm::{apply::validate_batch_split, ApplyMetrics},
        metrics::PEER_ADMIN_CMD_COUNTER,
        snap::TABLET_SNAPSHOT_VERSION,
        util::{self, KeysInfoFormatter},
        PeerPessimisticLocks, SplitCheckTask, Transport, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM,
    },
    Result,
};
use slog::info;

use crate::{
    batch::StoreContext,
    fsm::{ApplyResReporter, PeerFsmDelegate},
    operation::AdminCmdResult,
    raft::{Apply, Peer},
    router::{CmdResChannel, PeerMsg, PeerTick, StoreMsg},
    worker::tablet_gc,
    Error,
};

pub const SPLIT_PREFIX: &str = "split_";

#[derive(Debug)]
pub struct SplitResult {
    pub regions: Vec<Region>,
    // The index of the derived region in `regions`
    pub derived_index: usize,
    pub tablet_index: u64,
}

#[derive(Debug)]
pub struct SplitInit {
    /// Split region
    pub region: metapb::Region,
    pub check_split: bool,
    pub scheduled: bool,
    pub source_leader: bool,
    pub source_id: u64,

    /// In-memory pessimistic locks that should be inherited from parent region
    pub locks: PeerPessimisticLocks,
}

impl SplitInit {
    fn to_snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();
        // Set snapshot metadata.
        snapshot.mut_metadata().set_term(RAFT_INIT_LOG_TERM);
        snapshot.mut_metadata().set_index(RAFT_INIT_LOG_INDEX);
        let conf_state = util::conf_state_from_region(&self.region);
        snapshot.mut_metadata().set_conf_state(conf_state);
        // Set snapshot data.
        let mut snap_data = RaftSnapshotData::default();
        snap_data.set_region(self.region.clone());
        snap_data.set_version(TABLET_SNAPSHOT_VERSION);
        snap_data.mut_meta().set_for_balance(false);
        snapshot.set_data(snap_data.write_to_bytes().unwrap().into());
        snapshot
    }
}

#[derive(Debug)]
pub struct RequestSplit {
    pub epoch: RegionEpoch,
    pub split_keys: Vec<Vec<u8>>,
    pub source: Cow<'static, str>,
}

#[derive(Default, Debug)]
pub struct SplitFlowControl {
    size_diff_hint: i64,
    skip_split_count: u64,
    may_skip_split_check: bool,
}

pub fn temp_split_path<EK>(registry: &TabletRegistry<EK>, region_id: u64) -> PathBuf {
    let tablet_name = registry.tablet_name(SPLIT_PREFIX, region_id, RAFT_INIT_LOG_INDEX);
    registry.tablet_root().join(tablet_name)
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'_, EK, ER, T> {
    pub fn on_split_region_check(&mut self) {
        if !self.fsm.peer_mut().on_split_region_check(self.store_ctx) {
            self.schedule_tick(PeerTick::SplitRegionCheck)
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Handle split check.
    ///
    /// Returns true means the check tick is consumed, no need to schedule
    /// another tick.
    pub fn on_split_region_check<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) -> bool {
        if !self.is_leader() {
            return true;
        }
        let is_generating_snapshot = self.storage().is_generating_snapshot();
        let control = self.split_flow_control_mut();
        if control.may_skip_split_check
            && control.size_diff_hint < ctx.cfg.region_split_check_diff().0 as i64
        {
            return true;
        }
        if ctx.schedulers.split_check.is_busy() {
            return false;
        }
        if is_generating_snapshot && control.skip_split_count < 3 {
            control.skip_split_count += 1;
            return false;
        }
        let task =
            SplitCheckTask::split_check(self.region().clone(), true, CheckPolicy::Scan, None);
        if let Err(e) = ctx.schedulers.split_check.schedule(task) {
            info!(self.logger, "failed to schedule split check"; "err" => ?e);
        }
        let control = self.split_flow_control_mut();
        control.may_skip_split_check = true;
        control.size_diff_hint = 0;
        control.skip_split_count = 0;
        false
    }

    pub fn update_split_flow_control(&mut self, metrics: &ApplyMetrics) {
        let control = self.split_flow_control_mut();
        control.size_diff_hint += metrics.size_diff_hint;
    }

    pub fn on_request_split<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        rs: RequestSplit,
        ch: CmdResChannel,
    ) {
        info!(
            self.logger,
            "on split";
            "split_keys" => %KeysInfoFormatter(rs.split_keys.iter()),
            "source" => %&rs.source,
        );
        if !self.is_leader() {
            // region on this store is no longer leader, skipped.
            info!(self.logger, "not leader, skip.");
            ch.set_result(cmd_resp::new_error(Error::NotLeader(
                self.region_id(),
                self.leader(),
            )));
            return;
        }
        if let Err(e) = util::validate_split_region(
            self.region_id(),
            self.peer_id(),
            self.region(),
            &rs.epoch,
            &rs.split_keys,
        ) {
            info!(self.logger, "invalid split request"; "err" => ?e, "source" => %&rs.source);
            ch.set_result(cmd_resp::new_error(e));
            return;
        }
        self.ask_batch_split_pd(ctx, rs.split_keys, ch);
    }

    pub fn propose_split<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        validate_batch_split(req.get_admin_request(), self.region())?;
        // We rely on ConflictChecker to detect conflicts, so no need to set proposal
        // context.
        let data = req.write_to_bytes().unwrap();
        self.propose(store_ctx, data)
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_split(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        info!(
            self.logger,
            "split is deprecated, redirect to use batch split";
        );
        let split = req.get_split().to_owned();
        let mut admin_req = AdminRequest::default();
        admin_req
            .mut_splits()
            .set_right_derive(split.get_right_derive());
        admin_req.mut_splits().mut_requests().push(split);
        // This method is executed only when there are unapplied entries after being
        // restarted. So there will be no callback, it's OK to return a response
        // that does not matched with its request.
        self.apply_batch_split(req, log_index)
    }

    pub fn apply_batch_split(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

        let region = self.region_state().get_region();
        let region_id = region.get_id();
        validate_batch_split(req, self.region_state().get_region())?;

        let mut boundaries: Vec<&[u8]> = Vec::default();
        boundaries.push(self.region_state().get_region().get_start_key());
        for req in req.get_splits().get_requests() {
            boundaries.push(req.get_split_key());
        }
        boundaries.push(self.region_state().get_region().get_end_key());

        info!(
            self.logger,
            "split region";
            "region" => ?region,
            "boundaries" => %KeysInfoFormatter(boundaries.iter()),
        );

        let split_reqs = req.get_splits();
        let new_region_cnt = split_reqs.get_requests().len();
        let new_version = region.get_region_epoch().get_version() + new_region_cnt as u64;

        let mut derived_req = SplitRequest::default();
        derived_req.new_region_id = region.id;
        let derived_req = &[derived_req];

        let right_derive = split_reqs.get_right_derive();
        let reqs = if right_derive {
            split_reqs.get_requests().iter().chain(derived_req)
        } else {
            derived_req.iter().chain(split_reqs.get_requests())
        };

        let regions: Vec<_> = boundaries
            .array_windows::<2>()
            .zip(reqs)
            .map(|([start_key, end_key], req)| {
                let mut new_region = Region::default();
                new_region.set_id(req.get_new_region_id());
                new_region.set_region_epoch(region.get_region_epoch().to_owned());
                new_region.mut_region_epoch().set_version(new_version);
                new_region.set_start_key(start_key.to_vec());
                new_region.set_end_key(end_key.to_vec());
                new_region.set_peers(region.get_peers().to_vec().into());
                // If the `req` is the `derived_req`, the peers are already set correctly and
                // the following loop will not be executed due to the empty `new_peer_ids` in
                // the `derived_req`
                for (peer, peer_id) in new_region
                    .mut_peers()
                    .iter_mut()
                    .zip(req.get_new_peer_ids())
                {
                    peer.set_id(*peer_id);
                }
                new_region
            })
            .collect();

        let derived_index = if right_derive { regions.len() - 1 } else { 0 };

        // We will create checkpoint of the current tablet for both derived region and
        // split regions. Before the creation, we should flush the writes and remove the
        // write batch
        self.flush();

        // todo(SpadeA): Here: we use a temporary solution that we use checkpoint API to
        // clone new tablets. It may cause large jitter as we need to flush the
        // memtable. And more what is more important is that after removing WAL, the API
        // will never flush.
        // We will freeze the memtable rather than flush it in the following PR.
        let tablet = self.tablet().clone();
        let mut checkpointer = tablet.new_checkpointer().unwrap_or_else(|e| {
            panic!(
                "{:?} fails to create checkpoint object: {:?}",
                self.logger.list(),
                e
            )
        });

        let reg = self.tablet_registry();
        for new_region in &regions {
            let new_region_id = new_region.id;
            if new_region_id == region_id {
                continue;
            }

            let split_temp_path = temp_split_path(reg, new_region_id);
            checkpointer
                .create_at(&split_temp_path, None, 0)
                .unwrap_or_else(|e| {
                    panic!(
                        "{:?} fails to create checkpoint with path {:?}: {:?}",
                        self.logger.list(),
                        split_temp_path,
                        e
                    )
                });
        }

        let derived_path = self.tablet_registry().tablet_path(region_id, log_index);
        // If it's recovered from restart, it's possible the target path exists already.
        // And because checkpoint is atomic, so we don't need to worry about corruption.
        // And it's also wrong to delete it and remake as it may has applied and flushed
        // some data to the new checkpoint before being restarted.
        if !derived_path.exists() {
            checkpointer
                .create_at(&derived_path, None, 0)
                .unwrap_or_else(|e| {
                    panic!(
                        "{:?} fails to create checkpoint with path {:?}: {:?}",
                        self.logger.list(),
                        derived_path,
                        e
                    )
                });
        }
        // Remove the old write batch.
        self.write_batch.take();
        let reg = self.tablet_registry();
        let path = reg.tablet_path(region_id, log_index);
        let mut ctx = TabletContext::new(&regions[derived_index], Some(log_index));
        // Now the tablet is flushed, so all previous states should be persisted.
        // Reusing the tablet should not be a problem.
        // TODO: Should we avoid flushing for the old tablet?
        ctx.flush_state = Some(self.flush_state().clone());
        let tablet = reg.tablet_factory().open_tablet(ctx, &path).unwrap();
        self.publish_tablet(tablet);

        self.region_state_mut()
            .set_region(regions[derived_index].clone());
        self.region_state_mut().set_tablet_index(log_index);

        let mut resp = AdminResponse::default();
        resp.mut_splits().set_regions(regions.clone().into());
        PEER_ADMIN_CMD_COUNTER.batch_split.success.inc();

        Ok((
            resp,
            AdminCmdResult::SplitRegion(SplitResult {
                regions,
                derived_index,
                tablet_index: log_index,
            }),
        ))
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_apply_res_split<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        res: SplitResult,
    ) {
        fail_point!("on_split", self.peer().get_store_id() == 3, |_| {});

        let derived = &res.regions[res.derived_index];
        let derived_epoch = derived.get_region_epoch().clone();
        let region_id = derived.get_id();

        // Group in-memory pessimistic locks in the original region into new regions.
        // The locks of new regions will be put into the corresponding new regions
        // later. And the locks belonging to the old region will stay in the original
        // map.
        let region_locks = {
            let mut pessimistic_locks = self.txn_ext().pessimistic_locks.write();
            info!(self.logger, "moving {} locks to new regions", pessimistic_locks.len(););
            // Update the version so the concurrent reader will fail due to EpochNotMatch
            // instead of PessimisticLockNotFound.
            pessimistic_locks.version = derived_epoch.get_version();
            pessimistic_locks.group_by_regions(&res.regions, derived)
        };
        fail_point!("on_split_invalidate_locks");

        {
            let mut meta = store_ctx.store_meta.lock().unwrap();
            meta.set_region(derived, true, &self.logger);
            let reader = meta.readers.get_mut(&derived.get_id()).unwrap();
            self.set_region(
                &store_ctx.coprocessor_host,
                reader,
                derived.clone(),
                RegionChangeReason::Split,
                res.tablet_index,
            );
        }

        self.post_split();

        if self.is_leader() {
            self.region_heartbeat_pd(store_ctx);
            // Notify pd immediately to let it update the region meta.
            info!(
                self.logger,
                "notify pd with split";
                "split_count" => res.regions.len(),
            );
            // Now pd only uses ReportBatchSplit for history operation show,
            // so we send it independently here.
            self.report_batch_split_pd(store_ctx, res.regions.to_vec());
            self.add_pending_tick(PeerTick::SplitRegionCheck);
        }

        self.record_tablet_as_tombstone_and_refresh(res.tablet_index, store_ctx);
        let _ = store_ctx
            .schedulers
            .tablet_gc
            .schedule(tablet_gc::Task::trim(
                self.tablet().unwrap().clone(),
                derived,
            ));

        let last_region_id = res.regions.last().unwrap().get_id();
        let mut new_ids = HashSet::default();
        for (new_region, locks) in res.regions.into_iter().zip(region_locks) {
            let new_region_id = new_region.get_id();
            if new_region_id == region_id {
                continue;
            }

            new_ids.insert(new_region_id);
            let split_init = PeerMsg::SplitInit(Box::new(SplitInit {
                region: new_region,
                source_leader: self.is_leader(),
                source_id: region_id,
                check_split: last_region_id == new_region_id,
                scheduled: false,
                locks,
            }));

            // First, send init msg to peer directly. Returning error means the peer is not
            // existed in which case we should redirect it to the store.
            match store_ctx.router.force_send(new_region_id, split_init) {
                Ok(_) => {}
                Err(SendError(PeerMsg::SplitInit(msg))) => {
                    store_ctx
                        .router
                        .force_send_control(StoreMsg::SplitInit(msg))
                        .unwrap_or_else(|e| {
                            panic!(
                                "{:?} fails to send split peer intialization msg to store : {:?}",
                                self.logger.list(),
                                e
                            )
                        });
                }
                _ => unreachable!(),
            }
        }
        self.split_trace_mut().push((res.tablet_index, new_ids));
        let region_state = self.storage().region_state().clone();
        self.state_changes_mut()
            .put_region_state(region_id, res.tablet_index, &region_state)
            .unwrap();
        self.set_has_extra_write();
    }

    pub fn on_split_init<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut split_init: Box<SplitInit>,
    ) {
        let region_id = split_init.region.id;
        if self.storage().is_initialized() && self.persisted_index() >= RAFT_INIT_LOG_INDEX {
            // Race with split operation. The tablet created by split will eventually be
            // deleted (TODO). We don't trim it.
            let _ = store_ctx
                .router
                .force_send(split_init.source_id, PeerMsg::SplitInitFinish(region_id));
            return;
        }

        if self.storage().is_initialized() || self.raft_group().snap().is_some() {
            // It accepts a snapshot already but not finish applied yet.
            let prev = self.storage_mut().split_init_mut().replace(split_init);
            assert!(prev.is_none(), "{:?}", prev);
            return;
        }

        split_init.scheduled = true;
        let snap = split_init.to_snapshot();
        let mut msg = raft::eraftpb::Message::default();
        msg.set_to(self.peer_id());
        msg.set_from(self.leader_id());
        msg.set_msg_type(raft::eraftpb::MessageType::MsgSnapshot);
        msg.set_snapshot(snap);
        msg.set_term(cmp::max(self.term(), RAFT_INIT_LOG_TERM));
        let res = self.raft_group_mut().step(msg);
        let accept_snap = self.raft_group().snap().is_some();
        if res.is_err() || !accept_snap {
            panic!(
                "{:?} failed to accept snapshot {:?} with error {}",
                self.logger.list(),
                res,
                accept_snap
            );
        }
        let prev = self.storage_mut().split_init_mut().replace(split_init);
        assert!(prev.is_none(), "{:?}", prev);
        self.set_has_ready();
    }

    pub fn post_split_init<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        split_init: Box<SplitInit>,
    ) {
        let _ = store_ctx
            .schedulers
            .tablet_gc
            .schedule(tablet_gc::Task::trim(
                self.tablet().unwrap().clone(),
                self.region(),
            ));
        if split_init.source_leader
            && self.leader_id() == INVALID_ID
            && self.term() == RAFT_INIT_LOG_TERM
        {
            let _ = self.raft_group_mut().campaign();
            self.set_has_ready();

            *self.txn_ext().pessimistic_locks.write() = split_init.locks;
            // The new peer is likely to become leader, send a heartbeat immediately to
            // reduce client query miss.
            self.region_heartbeat_pd(store_ctx);
        }
        let region_id = self.region_id();

        if split_init.check_split {
            self.add_pending_tick(PeerTick::SplitRegionCheck);
        }
        let _ = store_ctx
            .router
            .force_send(split_init.source_id, PeerMsg::SplitInitFinish(region_id));
    }

    pub fn on_split_init_finish(&mut self, region_id: u64) {
        let mut found = false;
        for (_, ids) in self.split_trace_mut() {
            if ids.remove(&region_id) {
                found = true;
                break;
            }
        }
        assert!(found, "{:?} {}", self.logger.list(), region_id);
        let split_trace = self.split_trace_mut();
        let mut off = 0;
        let mut admin_flushed = 0;
        for (tablet_index, ids) in split_trace.iter() {
            if !ids.is_empty() {
                break;
            }
            admin_flushed = *tablet_index;
            off += 1;
        }
        if off > 0 {
            // There should be very few elements in the vector.
            split_trace.drain(..off);
            assert_ne!(admin_flushed, 0);
            self.storage_mut()
                .apply_trace_mut()
                .on_admin_flush(admin_flushed);
            // Persist admin flushed.
            self.set_has_extra_write();
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    };

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::TestTabletFactory,
    };
    use engine_traits::{
        Peekable, TabletContext, TabletRegistry, WriteBatch, CF_DEFAULT, DATA_CFS,
    };
    use kvproto::{
        metapb::RegionEpoch,
        raft_cmdpb::{BatchSplitRequest, SplitRequest},
        raft_serverpb::{PeerState, RegionLocalState},
    };
    use raftstore::store::cmd_resp::new_error;
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::{
        store::{new_learner_peer, new_peer},
        worker::dummy_scheduler,
    };

    use super::*;
    use crate::{fsm::ApplyResReporter, raft::Apply, router::ApplyRes};

    struct MockReporter {
        sender: Sender<ApplyRes>,
    }

    impl MockReporter {
        fn new() -> (Self, Receiver<ApplyRes>) {
            let (tx, rx) = channel();
            (MockReporter { sender: tx }, rx)
        }
    }

    impl ApplyResReporter for MockReporter {
        fn report(&self, apply_res: ApplyRes) {
            let _ = self.sender.send(apply_res);
        }
    }

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::default();
        req.set_split_key(key.to_vec());
        req.set_new_region_id(id);
        req.set_new_peer_ids(children);
        req
    }

    fn assert_split(
        apply: &mut Apply<engine_test::kv::KvTestEngine, MockReporter>,
        parent_id: u64,
        right_derived: bool,
        new_region_ids: Vec<u64>,
        split_keys: Vec<Vec<u8>>,
        children_peers: Vec<Vec<u64>>,
        log_index: u64,
        region_boundries: Vec<(Vec<u8>, Vec<u8>)>,
        expected_region_epoch: RegionEpoch,
        expected_derived_index: usize,
    ) {
        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(right_derived);

        for ((new_region_id, children), split_key) in new_region_ids
            .into_iter()
            .zip(children_peers.clone())
            .zip(split_keys)
        {
            splits
                .mut_requests()
                .push(new_split_req(&split_key, new_region_id, children));
        }

        let mut req = AdminRequest::default();
        req.set_splits(splits);

        // Exec batch split
        let (resp, apply_res) = apply.apply_batch_split(&req, log_index).unwrap();

        let regions = resp.get_splits().get_regions();
        assert!(regions.len() == region_boundries.len());

        let mut child_idx = 0;
        for (i, region) in regions.iter().enumerate() {
            assert_eq!(region.get_start_key().to_vec(), region_boundries[i].0);
            assert_eq!(region.get_end_key().to_vec(), region_boundries[i].1);
            assert_eq!(*region.get_region_epoch(), expected_region_epoch);

            if region.id == parent_id {
                let state = apply.region_state();
                assert_eq!(state.tablet_index, log_index);
                assert_eq!(state.get_region(), region);
                let reg = apply.tablet_registry();
                let tablet_path = reg.tablet_path(region.id, log_index);
                assert!(reg.tablet_factory().exists(&tablet_path));

                match apply_res {
                    AdminCmdResult::SplitRegion(SplitResult {
                        derived_index,
                        tablet_index,
                        ..
                    }) => {
                        assert_eq!(expected_derived_index, derived_index);
                        assert_eq!(tablet_index, log_index);
                    }
                    _ => panic!(),
                }
            } else {
                assert_eq! {
                    region.get_peers().iter().map(|peer| peer.id).collect::<Vec<_>>(),
                    children_peers[child_idx]
                }
                child_idx += 1;

                let reg = apply.tablet_registry();
                let tablet_name = reg.tablet_name(SPLIT_PREFIX, region.id, RAFT_INIT_LOG_INDEX);
                let path = reg.tablet_root().join(tablet_name);
                assert!(reg.tablet_factory().exists(&path));
            }
        }
    }

    #[test]
    fn test_split() {
        let store_id = 2;

        let mut region = Region::default();
        region.set_id(1);
        region.set_end_key(b"k10".to_vec());
        region.mut_region_epoch().set_version(3);
        let peers = vec![new_peer(2, 3), new_peer(4, 5), new_learner_peer(6, 7)];
        region.set_peers(peers.into());

        let logger = slog_global::borrow_global().new(o!());
        let path = TempDir::new().unwrap();
        let cf_opts = DATA_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let factory = Box::new(TestTabletFactory::new(DbOptions::default(), cf_opts));
        let reg = TabletRegistry::new(factory, path.path()).unwrap();
        let ctx = TabletContext::new(&region, Some(5));
        reg.load(ctx, true).unwrap();

        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region.clone());
        region_state.set_tablet_index(5);

        let (read_scheduler, _rx) = dummy_scheduler();
        let (reporter, _) = MockReporter::new();
        let mut apply = Apply::new(
            region
                .get_peers()
                .iter()
                .find(|p| p.store_id == store_id)
                .unwrap()
                .clone(),
            region_state,
            reporter,
            reg,
            read_scheduler,
            Arc::default(),
            None,
            0,
            0,
            logger.clone(),
        );

        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(true);
        splits.mut_requests().push(new_split_req(b"k1", 1, vec![]));
        let mut req = AdminRequest::default();
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // 3 followers are required.
        assert!(err.to_string().contains("invalid new peer id count"));

        splits.mut_requests().clear();
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // Empty requests should be rejected.
        assert!(err.to_string().contains("missing split requests"));

        splits
            .mut_requests()
            .push(new_split_req(b"k11", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let resp = new_error(apply.apply_batch_split(&req, 0).unwrap_err());
        // Out of range keys should be rejected.
        assert!(
            resp.get_header().get_error().has_key_not_in_region(),
            "{:?}",
            resp
        );

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // Empty key will not in any region exclusively.
        assert!(err.to_string().contains("missing split key"), "{:?}", err);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 1, vec![11, 12, 13]));
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // keys should be in ascend order.
        assert!(
            err.to_string().contains("invalid split request"),
            "{:?}",
            err
        );

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 1, vec![11, 12, 13]));
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 1, vec![11, 12]));
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // All requests should be checked.
        assert!(err.to_string().contains("id count"), "{:?}", err);

        let cases = vec![
            // region 1["", "k10"]
            // After split: region  1 ["", "k09"],
            //              region 10 ["k09", "k10"]
            (
                1,
                false,
                vec![10],
                vec![b"k09".to_vec()],
                vec![vec![11, 12, 13]],
                10,
                vec![
                    (b"".to_vec(), b"k09".to_vec()),
                    (b"k09".to_vec(), b"k10".to_vec()),
                ],
                4,
                0,
            ),
            // region 1 ["", "k09"]
            // After split: region 20 ["", "k01"],
            //               region 1 ["k01", "k09"]
            (
                1,
                true,
                vec![20],
                vec![b"k01".to_vec()],
                vec![vec![21, 22, 23]],
                20,
                vec![
                    (b"".to_vec(), b"k01".to_vec()),
                    (b"k01".to_vec(), b"k09".to_vec()),
                ],
                5,
                1,
            ),
            // region 1 ["k01", "k09"]
            // After split: region 30 ["k01", "k02"],
            //              region 40 ["k02", "k03"],
            //              region  1 ["k03", "k09"]
            (
                1,
                true,
                vec![30, 40],
                vec![b"k02".to_vec(), b"k03".to_vec()],
                vec![vec![31, 32, 33], vec![41, 42, 43]],
                30,
                vec![
                    (b"k01".to_vec(), b"k02".to_vec()),
                    (b"k02".to_vec(), b"k03".to_vec()),
                    (b"k03".to_vec(), b"k09".to_vec()),
                ],
                7,
                2,
            ),
            // region 1 ["k03", "k09"]
            // After split: region  1 ["k03", "k07"],
            //              region 50 ["k07", "k08"],
            //              region 60 ["k08", "k09"]
            (
                1,
                false,
                vec![50, 60],
                vec![b"k07".to_vec(), b"k08".to_vec()],
                vec![vec![51, 52, 53], vec![61, 62, 63]],
                40,
                vec![
                    (b"k03".to_vec(), b"k07".to_vec()),
                    (b"k07".to_vec(), b"k08".to_vec()),
                    (b"k08".to_vec(), b"k09".to_vec()),
                ],
                9,
                0,
            ),
        ];

        for (
            parent_id,
            right_derive,
            new_region_ids,
            split_keys,
            children_peers,
            log_index,
            region_boundries,
            version,
            expected_derived_index,
        ) in cases
        {
            let mut expected_epoch = RegionEpoch::new();
            expected_epoch.set_version(version);

            assert_split(
                &mut apply,
                parent_id,
                right_derive,
                new_region_ids,
                split_keys,
                children_peers,
                log_index,
                region_boundries,
                expected_epoch,
                expected_derived_index,
            );
        }

        // Split will create checkpoint tablet, so if there are some writes before
        // split, they should be flushed immediately.
        apply.apply_put(CF_DEFAULT, 50, b"k04", b"v4").unwrap();
        assert!(!WriteBatch::is_empty(apply.write_batch.as_ref().unwrap()));
        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k05", 70, vec![71, 72, 73]));
        req.set_splits(splits);
        apply.apply_batch_split(&req, 51).unwrap();
        assert!(apply.write_batch.is_none());
        assert_eq!(
            apply
                .tablet()
                .get_value(&keys::data_key(b"k04"))
                .unwrap()
                .unwrap(),
            b"v4"
        );
    }
}
