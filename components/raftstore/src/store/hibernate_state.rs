// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use collections::HashSet;
use kvproto::metapb::Region;
use pd_client::{Feature, FeatureGate};
use serde_derive::{Deserialize, Serialize};
use tikv_util::debug;

use crate::store::metrics::*;
/// Because negotiation protocol can't be recognized by old version of binaries,
/// so enabling it directly can cause a lot of connection reset.
const NEGOTIATION_HIBERNATE: Feature = Feature::require(5, 0, 0);

/// Represents state of the group.
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub enum GroupState {
    /// The group is working generally, leader keeps
    /// replicating data to followers.
    Ordered,
    /// The group is out of order. Leadership may not be hold.
    Chaos,
    /// The group is about to be out of order. It leave some
    /// safe space to avoid stepping chaos too often.
    PreChaos,
    /// The group is hibernated.
    Idle,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LeaderState {
    Awaken,
    Poll(Vec<u64>),
    Hibernated,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HibernateState {
    group: GroupState,
    leader: LeaderState,
}

macro_rules! update_metric {
    ($state:expr, $op:ident) => {
        let gauge = match $state {
            GroupState::Idle | GroupState::PreChaos => &HIBERNATED_PEER_STATE_GAUGE.hibernated,
            _ => &HIBERNATED_PEER_STATE_GAUGE.awaken,
        };
        gauge.$op();
    };
}

impl HibernateState {
    pub fn ordered() -> HibernateState {
        HibernateState {
            group: GroupState::Ordered,
            leader: LeaderState::Awaken,
        }
    }

    pub fn group_state(&self) -> GroupState {
        self.group
    }

    pub fn reset(&mut self, group_state: GroupState) {
        if group_state == self.group {
            return;
        }
        update_metric!(self.group, dec);
        update_metric!(group_state, inc);
        self.group = group_state;
        if group_state != GroupState::Idle {
            self.leader = LeaderState::Awaken;
        }
    }

    pub fn count_vote(&mut self, from: u64) {
        if let LeaderState::Poll(v) = &mut self.leader {
            if !v.contains(&from) {
                v.push(from);
            }
        }
    }

    pub fn should_bcast(&self, gate: &FeatureGate) -> bool {
        gate.can_enable(NEGOTIATION_HIBERNATE)
    }

    pub fn maybe_hibernate<F>(
        &mut self,
        has_quorum: F,
        my_id: u64,
        region: &Region,
        down_peer_ids: &[u64],
    ) -> (bool, Vec<u64>)
    where
        F: Fn(&HashSet<u64>) -> bool,
    {
        let mut hibernate_vote_peer_ids = HashSet::default();
        hibernate_vote_peer_ids.insert(my_id); // leader always votes for itself
        let peers = region.get_peers();
        let v = match &mut self.leader {
            LeaderState::Awaken => {
                self.leader = LeaderState::Poll(Vec::with_capacity(peers.len()));
                return (false, vec![]);
            }
            LeaderState::Poll(v) => v,
            LeaderState::Hibernated => {
                return (true, vec![]);
            }
        };
        hibernate_vote_peer_ids.extend(v.iter());
        let vote_peer_ids: Vec<u64> = hibernate_vote_peer_ids.iter().cloned().collect();
        let mut alive_non_hibernate_vote_peer = None; // whether alive non-hibernate-vote peer exists
        for peer in peers {
            let peer_id = peer.get_id();
            if peer_id == my_id {
                continue;
            }
            if !v.contains(&peer_id) && !down_peer_ids.contains(&peer_id) {
                debug!(
                    "peer is alive but not voted for hibernate";
                    "peer_id" => peer_id,
                    "my_id" => my_id,
                    "region_id" => region.get_id(),
                );
                alive_non_hibernate_vote_peer = Some(peer_id);
                break;
            }
        }
        // 1 is for leader itself, which is not counted into votes.
        if v.len() + 1 < peers.len() {
            if alive_non_hibernate_vote_peer.is_some() || !has_quorum(&hibernate_vote_peer_ids) {
                // Either:
                // - There is some alive non-hibernate-vote peer, leader cannot hibernate
                // - No alive non-hibernate-voter peer, but not enough votes to form a quorum
                return (false, vote_peer_ids);
            } else {
                // No alive non-hibernate-voter peer, and enough votes to form a quorum.
                // We can proceed to do the following final check.
                debug!(
                    "hibernate vote check passed by quorum with down peers";
                    "my_id" => my_id,
                    "votes" => v.len(),
                    "down_peer_ids" => ?down_peer_ids,
                    "vote_peer_ids" => ?v,
                    "region_id" => region.get_id(),
                );
            }
        }
        if peers.iter().all(|p| {
            p.get_id() == my_id || v.contains(&p.get_id()) || down_peer_ids.contains(&p.get_id())
        }) {
            self.leader = LeaderState::Hibernated;
            (true, vote_peer_ids)
        } else {
            (false, vote_peer_ids)
        }
    }
}
