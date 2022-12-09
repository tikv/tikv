// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use kvproto::metapb::Region;
use pd_client::{Feature, FeatureGate};
use serde_derive::{Deserialize, Serialize};

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

    pub fn maybe_hibernate(&mut self, my_id: u64, region: &Region) -> bool {
        let peers = region.get_peers();
        let v = match &mut self.leader {
            LeaderState::Awaken => {
                self.leader = LeaderState::Poll(Vec::with_capacity(peers.len()));
                return false;
            }
            LeaderState::Poll(v) => v,
            LeaderState::Hibernated => return true,
        };
        // 1 is for leader itself, which is not counted into votes.
        if v.len() + 1 < peers.len() {
            return false;
        }
        if peers
            .iter()
            .all(|p| p.get_id() == my_id || v.contains(&p.get_id()))
        {
            self.leader = LeaderState::Hibernated;
            true
        } else {
            false
        }
    }
}
