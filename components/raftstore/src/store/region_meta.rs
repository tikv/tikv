// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use kvproto::{
    metapb::{self, PeerRole},
    raft_serverpb,
};
use raft::{Progress, ProgressState, StateRole, Status};
use serde::{Deserialize, Serialize};

use super::GroupState;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum RaftProgressState {
    Probe,
    Replicate,
    Snapshot,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RaftProgress {
    pub matched: u64,
    pub next_idx: u64,
    pub state: RaftProgressState,
    pub paused: bool,
    pub pending_snapshot: u64,
    pub pending_request_snapshot: u64,
    pub recent_active: bool,
}

impl RaftProgress {
    fn new(progress: &Progress) -> Self {
        Self {
            matched: progress.matched,
            next_idx: progress.next_idx,
            state: progress.state.into(),
            paused: progress.paused,
            pending_snapshot: progress.pending_snapshot,
            pending_request_snapshot: progress.pending_request_snapshot,
            recent_active: progress.recent_active,
        }
    }
}

impl From<ProgressState> for RaftProgressState {
    fn from(state: ProgressState) -> Self {
        match state {
            ProgressState::Probe => RaftProgressState::Probe,
            ProgressState::Replicate => RaftProgressState::Replicate,
            ProgressState::Snapshot => RaftProgressState::Snapshot,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RaftHardState {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub enum RaftStateRole {
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

impl From<StateRole> for RaftStateRole {
    fn from(role: StateRole) -> Self {
        match role {
            StateRole::Follower => RaftStateRole::Follower,
            StateRole::Candidate => RaftStateRole::Candidate,
            StateRole::Leader => RaftStateRole::Leader,
            StateRole::PreCandidate => RaftStateRole::PreCandidate,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RaftSoftState {
    pub leader_id: u64,
    pub raft_state: RaftStateRole,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftStatus {
    pub id: u64,
    pub hard_state: RaftHardState,
    pub soft_state: RaftSoftState,
    pub applied: u64,
    pub voters: HashMap<u64, RaftProgress>,
    pub learners: HashMap<u64, RaftProgress>,
    pub last_index: u64,
    pub persisted_index: u64,
}

impl<'a> From<raft::Status<'a>> for RaftStatus {
    fn from(status: raft::Status<'a>) -> Self {
        let id = status.id;
        let hard_state = RaftHardState {
            term: status.hs.get_term(),
            vote: status.hs.get_vote(),
            commit: status.hs.get_commit(),
        };
        let soft_state = RaftSoftState {
            leader_id: status.ss.leader_id,
            raft_state: status.ss.raft_state.into(),
        };
        let applied = status.applied;
        let mut voters = HashMap::new();
        let mut learners = HashMap::new();
        if let Some(progress) = status.progress {
            for (id, pr) in progress.iter() {
                if progress.conf().voters().contains(*id) {
                    voters.insert(*id, RaftProgress::new(pr));
                } else {
                    learners.insert(*id, RaftProgress::new(pr));
                }
            }
        }
        Self {
            id,
            hard_state,
            soft_state,
            applied,
            voters,
            learners,
            last_index: 0,
            persisted_index: 0,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum RaftPeerRole {
    Voter,
    Learner,
    IncomingVoter,
    DemotingVoter,
}

impl From<PeerRole> for RaftPeerRole {
    fn from(role: PeerRole) -> Self {
        match role {
            PeerRole::Voter => RaftPeerRole::Voter,
            PeerRole::Learner => RaftPeerRole::Learner,
            PeerRole::IncomingVoter => RaftPeerRole::IncomingVoter,
            PeerRole::DemotingVoter => RaftPeerRole::DemotingVoter,
        }
    }
}

impl From<RaftPeerRole> for PeerRole {
    fn from(role: RaftPeerRole) -> Self {
        match role {
            RaftPeerRole::Voter => PeerRole::Voter,
            RaftPeerRole::Learner => PeerRole::Learner,
            RaftPeerRole::IncomingVoter => PeerRole::IncomingVoter,
            RaftPeerRole::DemotingVoter => PeerRole::DemotingVoter,
        }
    }
}

impl PartialEq<PeerRole> for RaftPeerRole {
    fn eq(&self, other: &PeerRole) -> bool {
        let r: RaftPeerRole = (*other).into();
        *self == r
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Epoch {
    pub conf_ver: u64,
    pub version: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RegionPeer {
    pub id: u64,
    pub store_id: u64,
    pub role: RaftPeerRole,
    pub is_witness: bool,
}

impl PartialEq<metapb::Peer> for RegionPeer {
    #[inline]
    fn eq(&self, other: &metapb::Peer) -> bool {
        // May not be sufficent, but always correct.
        let s: metapb::Peer = (*self).into();
        s == *other
    }
}

impl From<RegionPeer> for metapb::Peer {
    fn from(p: RegionPeer) -> Self {
        metapb::Peer {
            id: p.id,
            store_id: p.store_id,
            role: Into::<PeerRole>::into(p.role) as i32,
            is_witness: p.is_witness,
            ..Default::default()
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RegionMergeState {
    pub min_index: u64,
    pub commit: u64,
    pub region_id: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RaftTruncatedState {
    pub index: u64,
    pub term: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RaftApplyState {
    pub applied_index: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub truncated_state: RaftTruncatedState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionLocalState {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub epoch: Epoch,
    pub peers: Vec<RegionPeer>,
    pub merge_state: Option<RegionMergeState>,
    pub tablet_index: u64,
}

/// A serializeable struct that exposes the internal debug information of a
/// peer. TODO: make protobuf generated code derive serde directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionMeta {
    pub group_state: GroupState,
    pub raft_status: RaftStatus,
    pub raft_apply: RaftApplyState,
    pub region_state: RegionLocalState,
    pub bucket_keys: Vec<Vec<u8>>,
}

impl RegionMeta {
    pub fn new(
        local_state: &raft_serverpb::RegionLocalState,
        apply_state: &raft_serverpb::RaftApplyState,
        group_state: GroupState,
        raft_status: Status<'_>,
        last_index: u64,
        persisted_index: u64,
    ) -> Self {
        let region = local_state.get_region();
        let epoch = region.get_region_epoch();
        let start_key = region.get_start_key();
        let end_key = region.get_end_key();
        let raw_peers = region.get_peers();
        let mut peers = Vec::with_capacity(raw_peers.len());
        for peer in raw_peers {
            peers.push(RegionPeer {
                id: peer.get_id(),
                store_id: peer.get_store_id(),
                role: peer.get_role().into(),
                is_witness: peer.is_witness,
            });
        }
        let merge_state = if local_state.has_merge_state() {
            Some(local_state.get_merge_state())
        } else {
            None
        };
        let mut raft_status: RaftStatus = raft_status.into();
        raft_status.last_index = last_index;
        raft_status.persisted_index = persisted_index;

        Self {
            group_state,
            raft_status,
            raft_apply: RaftApplyState {
                applied_index: apply_state.get_applied_index(),
                commit_index: apply_state.get_commit_index(),
                commit_term: apply_state.get_commit_term(),
                truncated_state: RaftTruncatedState {
                    index: apply_state.get_truncated_state().get_index(),
                    term: apply_state.get_truncated_state().get_term(),
                },
            },
            region_state: RegionLocalState {
                id: region.get_id(),
                start_key: start_key.to_owned(),
                end_key: end_key.to_owned(),
                epoch: Epoch {
                    conf_ver: epoch.get_conf_ver(),
                    version: epoch.get_version(),
                },
                peers,
                merge_state: merge_state.map(|state| RegionMergeState {
                    min_index: state.get_min_index(),
                    commit: state.get_commit(),
                    region_id: state.get_target().get_id(),
                }),
                tablet_index: local_state.get_tablet_index(),
            },
            bucket_keys: vec![],
        }
    }
}
