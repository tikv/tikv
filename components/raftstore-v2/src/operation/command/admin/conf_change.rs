// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the configuration change command.
//!
//! The command will go through the following steps:
//! - Propose conf change
//! - Apply after conf change is committed
//! - Update raft state using the result of conf change

use std::time::Instant;

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use fail::fail_point;
use kvproto::{
    metapb::{self, PeerRole},
    raft_cmdpb::{AdminRequest, AdminResponse, ChangePeerRequest, RaftCmdRequest},
    raft_serverpb::{PeerState, RegionLocalState},
};
use protobuf::Message;
use raft::prelude::*;
use raftstore::{
    coprocessor::{RegionChangeEvent, RegionChangeReason},
    store::{
        metrics::{PEER_ADMIN_CMD_COUNTER_VEC, PEER_PROPOSE_LOG_SIZE_HISTOGRAM},
        util::{self, ChangePeerI, ConfChangeKind},
        ProposalContext,
    },
    Error, Result,
};
use slog::{error, info, warn};
use tikv_util::{box_err, slog_panic};

use super::AdminCmdResult;
use crate::{
    batch::StoreContext,
    raft::{Apply, Peer},
};

/// The apply result of conf change.
#[derive(Default, Debug)]
pub struct ConfChangeResult {
    pub index: u64,
    // The proposed ConfChangeV2 or (legacy) ConfChange.
    // ConfChange (if it is) will be converted to ConfChangeV2.
    pub conf_change: ConfChangeV2,
    // The change peer requests come along with ConfChangeV2
    // or (legacy) ConfChange. For ConfChange, it only contains
    // one element.
    pub changes: Vec<ChangePeerRequest>,
    pub region_state: RegionLocalState,
}

#[derive(Debug)]
pub struct UpdateGcPeersResult {
    index: u64,
    region_state: RegionLocalState,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn propose_conf_change<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        if self.raft_group().raft.has_pending_conf() {
            info!(
                self.logger,
                "there is a pending conf change, try later";
            );
            return Err(box_err!("there is a pending conf change, try later"));
        }
        let data = req.write_to_bytes()?;
        let admin = req.get_admin_request();
        if admin.has_change_peer() {
            self.propose_conf_change_imp(ctx, admin.get_change_peer(), data)
        } else if admin.has_change_peer_v2() {
            self.propose_conf_change_imp(ctx, admin.get_change_peer_v2(), data)
        } else {
            unreachable!()
        }
    }

    /// Fails in following cases:
    ///
    /// 1. A pending conf change has not been applied yet;
    /// 2. Removing the leader is not allowed in the configuration;
    /// 3. The conf change makes the raft group not healthy;
    /// 4. The conf change is dropped by raft group internally.
    /// 5. There is a same peer on the same store in history record (TODO).
    fn propose_conf_change_imp<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        change_peer: impl ChangePeerI,
        data: Vec<u8>,
    ) -> Result<u64> {
        let data_size = data.len();
        let cc = change_peer.to_confchange(data);
        let changes = change_peer.get_change_peers();

        util::check_conf_change(
            &ctx.cfg,
            self.raft_group(),
            self.region(),
            self.peer(),
            changes.as_ref(),
            &cc,
            self.is_in_force_leader(),
        )?;

        // TODO: check if the new peer is already in history record.

        ctx.raft_metrics.propose.conf_change.inc();
        // TODO: use local histogram metrics
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data_size as f64);
        info!(
            self.logger,
            "propose conf change peer";
            "changes" => ?changes.as_ref(),
            "kind" => ?ConfChangeKind::confchange_kind(changes.as_ref().len()),
        );

        let last_index = self.raft_group().raft.raft_log.last_index();
        self.raft_group_mut()
            .propose_conf_change(ProposalContext::SYNC_LOG.to_vec(), cc)?;
        let proposal_index = self.raft_group().raft.raft_log.last_index();
        if proposal_index == last_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id(), None));
        }

        Ok(proposal_index)
    }

    pub fn on_apply_res_conf_change<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        conf_change: ConfChangeResult,
    ) {
        // TODO: cancel generating snapshot.

        // Snapshot is applied in memory without waiting for all entries being
        // applied. So it's possible conf_change.index < first_index.
        if conf_change.index >= self.raft_group().raft.raft_log.first_index() {
            match self.raft_group_mut().apply_conf_change(&conf_change.conf_change) {
                Ok(_)
                // PD could dispatch redundant conf changes.
                | Err(raft::Error::NotExists { .. }) | Err(raft::Error::Exists { .. }) => (),
                _ => unreachable!(),
            }
        }

        let remove_self = conf_change.region_state.get_state() == PeerState::Tombstone;
        self.storage_mut()
            .set_region_state(conf_change.region_state.clone());
        if self.is_leader() {
            info!(
                self.logger,
                "notify pd with change peer region";
                "region" => ?self.region(),
            );
            self.region_heartbeat_pd(ctx);
            let demote_self =
                tikv_util::store::is_learner(self.peer()) && !self.is_in_force_leader();
            if remove_self || demote_self {
                warn!(self.logger, "removing or demoting leader"; "remove" => remove_self, "demote" => demote_self);
                let term = self.term();
                self.raft_group_mut()
                    .raft
                    .become_follower(term, raft::INVALID_ID);
            }
            let mut has_new_peer = None;
            for c in conf_change.changes {
                let peer_id = c.get_peer().get_id();
                match c.get_change_type() {
                    ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                        if has_new_peer.is_none() {
                            has_new_peer = Some(Instant::now());
                        }
                        self.add_peer_heartbeat(peer_id, has_new_peer.unwrap());
                    }
                    ConfChangeType::RemoveNode => {
                        self.remove_peer_heartbeat(peer_id);
                    }
                }
            }
            if self.is_leader() {
                if has_new_peer.is_some() {
                    // Speed up snapshot instead of waiting another heartbeat.
                    self.raft_group_mut().ping();
                    self.set_has_ready();
                }
                self.maybe_schedule_gc_peer_tick();
            }
        }
        ctx.store_meta
            .lock()
            .unwrap()
            .set_region(self.region(), true, &self.logger);
        // Update leader's peer list after conf change.
        self.read_progress()
            .update_leader_info(self.leader_id(), self.term(), self.region());
        ctx.coprocessor_host.on_region_changed(
            self.region(),
            RegionChangeEvent::Update(RegionChangeReason::ChangePeer),
            self.raft_group().raft.state,
        );
        if remove_self {
            // When self is destroyed, all metas will be cleaned in `start_destroy`.
            self.mark_for_destroy(None);
        } else {
            let region_id = self.region_id();
            self.state_changes_mut()
                .put_region_state(region_id, conf_change.index, &conf_change.region_state)
                .unwrap();
            self.set_has_extra_write();
        }
    }

    pub fn on_apply_res_update_gc_peers(&mut self, result: UpdateGcPeersResult) {
        let region_id = self.region_id();
        self.state_changes_mut()
            .put_region_state(region_id, result.index, &result.region_state)
            .unwrap();
        self.set_has_extra_write();
        self.storage_mut().set_region_state(result.region_state);
    }
}

impl<EK: KvEngine, R> Apply<EK, R> {
    #[inline]
    pub fn apply_conf_change(
        &mut self,
        index: u64,
        req: &AdminRequest,
        cc: ConfChangeV2,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        assert!(req.has_change_peer());
        self.apply_conf_change_imp(index, std::slice::from_ref(req.get_change_peer()), cc, true)
    }

    #[inline]
    pub fn apply_conf_change_v2(
        &mut self,
        index: u64,
        req: &AdminRequest,
        cc: ConfChangeV2,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        assert!(req.has_change_peer_v2());
        self.apply_conf_change_imp(
            index,
            req.get_change_peer_v2().get_change_peers(),
            cc,
            false,
        )
    }

    #[inline]
    fn apply_conf_change_imp(
        &mut self,
        index: u64,
        changes: &[ChangePeerRequest],
        cc: ConfChangeV2,
        legacy: bool,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        let region = self.region();
        let change_kind = ConfChangeKind::confchange_kind(changes.len());
        info!(self.logger, "exec ConfChangeV2"; "kind" => ?change_kind, "legacy" => legacy, "epoch" => ?region.get_region_epoch(), "index" => index);
        let mut new_region = region.clone();
        match change_kind {
            ConfChangeKind::LeaveJoint => self.apply_leave_joint(&mut new_region),
            kind => {
                debug_assert!(!legacy || kind == ConfChangeKind::Simple, "{:?}", kind);
                debug_assert!(
                    kind != ConfChangeKind::Simple || changes.len() == 1,
                    "{:?}",
                    changes
                );
                for cp in changes {
                    let res = if legacy {
                        self.apply_single_change_legacy(cp, &mut new_region)
                    } else {
                        self.apply_single_change(kind, cp, &mut new_region)
                    };
                    if let Err(e) = res {
                        error!(self.logger, "failed to apply conf change";
                        "changes" => ?changes,
                        "legacy" => legacy,
                        "original_region" => ?region, "err" => ?e);
                        return Err(e);
                    }
                }
                let conf_ver = region.get_region_epoch().get_conf_ver() + changes.len() as u64;
                new_region.mut_region_epoch().set_conf_ver(conf_ver);
            }
        };

        info!(
            self.logger,
            "conf change successfully";
            "changes" => ?changes,
            "legacy" => legacy,
            "original_region" => ?region,
            "current_region" => ?new_region,
        );
        let my_id = self.peer().get_id();
        let state = self.region_state_mut();
        let mut removed_records: Vec<_> = state.take_removed_records().into();
        for p0 in state.get_region().get_peers() {
            // No matching store ID means the peer must be removed.
            if new_region
                .get_peers()
                .iter()
                .all(|p1| p1.get_store_id() != p0.get_store_id())
            {
                removed_records.push(p0.clone());
            }
        }
        // If a peer is replaced in the same store, the leader will keep polling the
        // new peer on the same store, which implies that the old peer must be
        // tombstone in the end.
        removed_records.retain(|p0| {
            new_region
                .get_peers()
                .iter()
                .all(|p1| p1.get_store_id() != p0.get_store_id())
        });
        state.set_region(new_region.clone());
        state.set_removed_records(removed_records.into());
        let new_peer = new_region
            .get_peers()
            .iter()
            .find(|p| p.get_id() == my_id)
            .cloned();
        if new_peer.is_none() {
            // A peer will reject any snapshot that doesn't include itself in the
            // configuration. So if it disappear from the configuration, it must
            // be removed by conf change.
            state.set_state(PeerState::Tombstone);
        }
        let mut resp = AdminResponse::default();
        resp.mut_change_peer().set_region(new_region);
        let conf_change = ConfChangeResult {
            index,
            conf_change: cc,
            changes: changes.to_vec(),
            region_state: state.clone(),
        };
        if state.get_state() == PeerState::Tombstone {
            self.mark_tombstone();
        }
        if let Some(peer) = new_peer {
            self.set_peer(peer);
        }
        Ok((resp, AdminCmdResult::ConfChange(conf_change)))
    }

    #[inline]
    fn apply_leave_joint(&self, region: &mut metapb::Region) {
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
            slog_panic!(
                self.logger,
                "can't leave a non-joint config";
                "region" => ?self.region_state()
            );
        }
        let conf_ver = region.get_region_epoch().get_conf_ver() + change_num;
        region.mut_region_epoch().set_conf_ver(conf_ver);
        info!(self.logger, "leave joint state successfully"; "region" => ?region);
    }

    /// This is used for conf change v1. Use a standalone function to avoid
    /// future refactor breaks consistency accidentally.
    #[inline]
    fn apply_single_change_legacy(
        &self,
        cp: &ChangePeerRequest,
        region: &mut metapb::Region,
    ) -> Result<()> {
        let peer = cp.get_peer();
        let store_id = peer.get_store_id();
        let change_type = cp.get_change_type();

        match change_type {
            ConfChangeType::AddNode => {
                let add_node_fp = || {
                    fail_point!(
                        "apply_on_add_node_1_2",
                        self.peer_id() == 2 && self.region_id() == 1,
                        |_| {}
                    )
                };
                add_node_fp();
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_peer", "all"])
                    .inc();

                let mut exists = false;
                if let Some(p) = tikv_util::store::find_peer_mut(region, store_id) {
                    exists = true;
                    if !tikv_util::store::is_learner(p) || p.get_id() != peer.get_id() {
                        return Err(box_err!(
                            "can't add duplicated peer {:?} to region {:?}",
                            peer,
                            self.region_state()
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
            }
            ConfChangeType::RemoveNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["remove_peer", "all"])
                    .inc();

                if let Some(p) = tikv_util::store::remove_peer(region, store_id) {
                    // Considering `is_learner` flag in `Peer` here is by design.
                    if &p != peer {
                        return Err(box_err!(
                            "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                            peer,
                            p
                        ));
                    }
                } else {
                    return Err(box_err!(
                        "remove missing peer {:?} from region {:?}",
                        peer,
                        self.region_state()
                    ));
                }

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["remove_peer", "success"])
                    .inc();
            }
            ConfChangeType::AddLearnerNode => {
                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "all"])
                    .inc();

                if tikv_util::store::find_peer(region, store_id).is_some() {
                    return Err(box_err!(
                        "can't add duplicated learner {:?} to region {:?}",
                        peer,
                        self.region_state()
                    ));
                }
                region.mut_peers().push(peer.clone());

                PEER_ADMIN_CMD_COUNTER_VEC
                    .with_label_values(&["add_learner", "success"])
                    .inc();
            }
        }
        Ok(())
    }

    #[inline]
    fn apply_single_change(
        &self,
        kind: ConfChangeKind,
        cp: &ChangePeerRequest,
        region: &mut metapb::Region,
    ) -> Result<()> {
        let (change_type, peer) = (cp.get_change_type(), cp.get_peer());
        let store_id = peer.get_store_id();

        let metric = match change_type {
            ConfChangeType::AddNode => "add_peer",
            ConfChangeType::RemoveNode => "remove_peer",
            ConfChangeType::AddLearnerNode => "add_learner",
        };
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&[metric, "all"])
            .inc();

        if let Some(exist_peer) = tikv_util::store::find_peer(region, store_id) {
            let r = exist_peer.get_role();
            if r == PeerRole::IncomingVoter || r == PeerRole::DemotingVoter {
                slog_panic!(
                    self.logger,
                    "can't apply confchange because configuration is still in joint state";
                    "confchange" => ?cp,
                    "region_state" => ?self.region_state()
                );
            }
        }
        match (
            tikv_util::store::find_peer_mut(region, store_id),
            change_type,
        ) {
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
                return Err(box_err!(
                    "remove missing peer {:?} from region {:?}",
                    peer,
                    self.region_state()
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
                    return Err(box_err!(
                        "can't add duplicated peer {:?} to region {:?}, duplicated with exist peer {:?}",
                        peer,
                        self.region_state(),
                        exist_peer
                    ));
                }
                match (role, change_type) {
                    (PeerRole::Voter, ConfChangeType::AddLearnerNode) => match kind {
                        ConfChangeKind::Simple => exist_peer.set_role(PeerRole::Learner),
                        ConfChangeKind::EnterJoint => exist_peer.set_role(PeerRole::DemotingVoter),
                        _ => unreachable!(),
                    },
                    (PeerRole::Learner, ConfChangeType::AddNode) => match kind {
                        ConfChangeKind::Simple => exist_peer.set_role(PeerRole::Voter),
                        ConfChangeKind::EnterJoint => exist_peer.set_role(PeerRole::IncomingVoter),
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            }
            // Remove node
            (Some(exist_peer), ConfChangeType::RemoveNode) => {
                if kind == ConfChangeKind::EnterJoint && exist_peer.get_role() == PeerRole::Voter {
                    return Err(box_err!(
                        "can not remove voter {:?} directly from region {:?}",
                        peer,
                        self.region_state()
                    ));
                }
                match tikv_util::store::remove_peer(region, store_id) {
                    Some(p) => {
                        if &p != peer {
                            return Err(box_err!(
                                "remove unmatched peer: expect: {:?}, get {:?}, ignore",
                                peer,
                                p
                            ));
                        }
                    }
                    None => unreachable!(),
                }
            }
        }
        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&[metric, "success"])
            .inc();
        Ok(())
    }

    pub fn apply_update_gc_peer(
        &mut self,
        log_index: u64,
        admin_req: &AdminRequest,
    ) -> (AdminResponse, AdminCmdResult) {
        let mut removed_records: Vec<_> = self.region_state_mut().take_removed_records().into();
        let mut merged_records: Vec<_> = self.region_state_mut().take_merged_records().into();
        let updates = admin_req.get_update_gc_peers().get_peer_id();
        info!(
            self.logger,
            "update gc peer";
            "index" => log_index,
            "updates" => ?updates,
            "gc_peers" => ?removed_records,
            "merged_peers" => ?merged_records
        );
        removed_records.retain(|p| !updates.contains(&p.get_id()));
        merged_records.retain_mut(|r| {
            let mut sources: Vec<_> = r.take_source_peers().into();
            sources.retain(|p| !updates.contains(&p.get_id()));
            r.set_source_peers(sources.into());
            !r.get_source_peers().is_empty()
        });
        self.region_state_mut()
            .set_removed_records(removed_records.into());
        self.region_state_mut()
            .set_merged_records(merged_records.into());
        (
            AdminResponse::default(),
            AdminCmdResult::UpdateGcPeers(UpdateGcPeersResult {
                index: log_index,
                region_state: self.region_state().clone(),
            }),
        )
    }
}
