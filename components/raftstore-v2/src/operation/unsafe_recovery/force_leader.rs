// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use collections::HashSet;
use engine_traits::{KvEngine, RaftEngine};
use raft::{eraftpb::MessageType, StateRole, Storage};
use raftstore::store::{
    util::LeaseState, ForceLeaderState, UnsafeRecoveryForceLeaderSyncer, UnsafeRecoveryState,
};
use slog::{info, warn};
use tikv_util::time::Instant as TiInstant;

use crate::{batch::StoreContext, raft::Peer, router::PeerMsg};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_enter_pre_force_leader<T>(
        &mut self,
        ctx: &StoreContext<EK, ER, T>,
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
    ) {
        match self.force_leader() {
            Some(ForceLeaderState::PreForceLeader { .. }) => {
                self.on_force_leader_fail();
            }
            Some(ForceLeaderState::ForceLeader { .. }) => {
                // already is a force leader, do nothing
                return;
            }
            Some(ForceLeaderState::WaitTicks { .. }) => {
                *self.force_leader_mut() = None;
            }
            None => {}
        }

        if !self.storage().is_initialized() {
            warn!(self.logger,
                "Unsafe recovery, cannot force leader since this peer is not initialized";
            );
            return;
        }

        let ticks = if self.is_leader() {
            // wait two rounds of election timeout to trigger check quorum to
            // step down the leader.
            // Note: check quorum is triggered every `election_timeout` instead
            // of `randomized_election_timeout`
            Some(
                self.raft_group().raft.election_timeout() * 2
                    - self.raft_group().raft.election_elapsed,
            )
        // When election timeout is triggered, leader_id is set to INVALID_ID.
        // But learner(not promotable) is a exception here as it wouldn't tick
        // election.
        } else if self.raft_group().raft.promotable() && self.leader_id() != raft::INVALID_ID {
            // wait one round of election timeout to make sure leader_id is invalid
            if self.raft_group().raft.election_elapsed <= ctx.cfg.raft_election_timeout_ticks {
                warn!(
                    self.logger,
                    "Unsafe recovery, reject pre force leader due to leader lease may not expired"
                );
                return;
            }
            Some(
                self.raft_group().raft.randomized_election_timeout()
                    - self.raft_group().raft.election_elapsed,
            )
        } else {
            None
        };

        if let Some(ticks) = ticks {
            info!(self.logger,
                "Unsafe recovery, enter wait ticks";
                "ticks" => ticks,
            );
            *self.force_leader_mut() = Some(ForceLeaderState::WaitTicks {
                syncer,
                failed_stores,
                ticks,
            });
            self.set_has_ready();
            return;
        }

        let expected_alive_voter = self.get_force_leader_expected_alive_voter(&failed_stores);
        if !expected_alive_voter.is_empty()
            && self
                .raft_group()
                .raft
                .prs()
                .has_quorum(&expected_alive_voter)
        {
            warn!(self.logger,
                "Unsafe recovery, reject pre force leader due to has quorum";
            );
            return;
        }

        info!(self.logger,
            "Unsafe recovery, enter pre force leader state";
            "alive_voter" => ?expected_alive_voter,
        );

        // Do not use prevote as prevote won't set `vote` to itself.
        // When PD issues force leader on two different peer, it may cause
        // two force leader in same term.
        self.raft_group_mut().raft.pre_vote = false;
        // trigger vote request to all voters, will check the vote result in
        // `check_force_leader`
        if let Err(e) = self.raft_group_mut().campaign() {
            warn!(self.logger, "Unsafe recovery, campaign failed"; "err" => ?e);
        }
        assert_eq!(self.raft_group().raft.state, StateRole::Candidate);
        if !self
            .raft_group()
            .raft
            .prs()
            .votes()
            .get(&self.peer_id())
            .unwrap()
        {
            warn!(self.logger,
                "Unsafe recovery, pre force leader failed to campaign";
            );
            self.on_force_leader_fail();
            return;
        }

        *self.force_leader_mut() = Some(ForceLeaderState::PreForceLeader {
            syncer,
            failed_stores,
        });
        self.set_has_ready();
    }

    pub fn on_force_leader_fail(&mut self) {
        self.raft_group_mut().raft.pre_vote = true;
        self.raft_group_mut().raft.set_check_quorum(true);
        *self.force_leader_mut() = None;
    }

    fn on_enter_force_leader(&mut self) {
        info!(self.logger, "Unsafe recovery, enter force leader state");
        assert_eq!(self.raft_group().raft.state, StateRole::Candidate);

        let failed_stores = match self.force_leader_mut().take() {
            Some(ForceLeaderState::PreForceLeader { failed_stores, .. }) => failed_stores,
            _ => unreachable!(),
        };

        let peer_ids: Vec<_> = self.voters().iter().collect();
        for peer_id in peer_ids {
            let store_id = self
                .region()
                .get_peers()
                .iter()
                .find(|p| p.get_id() == peer_id)
                .unwrap()
                .get_store_id();
            if !failed_stores.contains(&store_id) {
                continue;
            }

            // make fake vote responses from peers on failed store.
            let mut msg = raft::eraftpb::Message::new();
            msg.msg_type = MessageType::MsgRequestVoteResponse;
            msg.reject = false;
            msg.term = self.term();
            msg.from = peer_id;
            msg.to = self.peer_id();
            self.raft_group_mut().step(msg).unwrap();
        }

        // after receiving all votes, should become leader
        assert!(self.is_leader());
        self.raft_group_mut().raft.set_check_quorum(false);

        *self.force_leader_mut() = Some(ForceLeaderState::ForceLeader {
            time: TiInstant::now_coarse(),
            failed_stores,
        });
        self.set_has_ready();
    }

    // TODO: add exit force leader check tick for raftstore v2
    pub fn on_exit_force_leader<T>(&mut self, ctx: &StoreContext<EK, ER, T>, force: bool) {
        if !self.has_force_leader() {
            return;
        }

        if let Some(UnsafeRecoveryState::Failed) = self.unsafe_recovery_state() && !force {
            // Skip force leader if the plan failed, so wait for the next retry of plan with force leader state holding
            info!(
                self.logger, "skip exiting force leader state"
            );
            return;
        }

        info!(self.logger, "exit force leader state");
        *self.force_leader_mut() = None;
        // leader lease shouldn't be renewed in force leader state.
        assert_eq!(self.leader_lease().inspect(None), LeaseState::Expired);
        let term = self.term();
        self.raft_group_mut()
            .raft
            .become_follower(term, raft::INVALID_ID);

        self.raft_group_mut().raft.set_check_quorum(true);
        self.raft_group_mut().raft.pre_vote = true;
        if self.raft_group().raft.promotable() {
            // Do not campaign directly here, otherwise on_role_changed() won't called for
            // follower state
            let _ = ctx
                .router
                .send(self.region_id(), PeerMsg::ExitForceLeaderStateCampaign);
        }
        self.set_has_ready();
    }

    pub fn on_exit_force_leader_campaign(&mut self) {
        let _ = self.raft_group_mut().campaign();
        self.set_has_ready();
    }

    fn get_force_leader_expected_alive_voter(&self, failed_stores: &HashSet<u64>) -> HashSet<u64> {
        let region = self.region();
        self.voters()
            .iter()
            .filter(|peer_id| {
                let store_id = region
                    .get_peers()
                    .iter()
                    .find(|p| p.get_id() == *peer_id)
                    .unwrap()
                    .get_store_id();
                !failed_stores.contains(&store_id)
            })
            .collect()
    }

    pub fn check_force_leader<T>(&mut self, ctx: &StoreContext<EK, ER, T>) {
        if let Some(ForceLeaderState::WaitTicks {
            syncer,
            failed_stores,
            ticks,
        }) = self.force_leader_mut()
        {
            if *ticks == 0 {
                let syncer_clone = syncer.clone();
                let s = mem::take(failed_stores);
                self.on_enter_pre_force_leader(ctx, syncer_clone, s);
            } else {
                *ticks -= 1;
            }
            return;
        };

        let failed_stores = match self.force_leader() {
            None => return,
            Some(ForceLeaderState::ForceLeader { .. }) => {
                if self.maybe_force_forward_commit_index() {
                    self.set_has_ready();
                }
                return;
            }
            Some(ForceLeaderState::PreForceLeader { failed_stores, .. }) => failed_stores,
            Some(ForceLeaderState::WaitTicks { .. }) => unreachable!(),
        };

        if self.raft_group().raft.election_elapsed + 1 < ctx.cfg.raft_election_timeout_ticks {
            // wait as longer as it can to collect responses of request vote
            return;
        }

        let expected_alive_voter = self.get_force_leader_expected_alive_voter(failed_stores);
        let check = || {
            if self.raft_group().raft.state != StateRole::Candidate {
                Err(format!(
                    "unexpected role {:?}",
                    self.raft_group().raft.state
                ))
            } else {
                let mut granted = 0;
                for (id, vote) in self.raft_group().raft.prs().votes() {
                    if expected_alive_voter.contains(id) {
                        if *vote {
                            granted += 1;
                        } else {
                            return Err(format!("receive reject response from {}", *id));
                        }
                    } else if *id == self.peer_id() {
                        // self may be a learner
                        continue;
                    } else {
                        return Err(format!(
                            "receive unexpected vote from {} vote {}",
                            *id, *vote
                        ));
                    }
                }
                Ok(granted)
            }
        };

        match check() {
            Err(err) => {
                warn!(self.logger,
                    "Unsafe recovery, pre force leader check failed";
                    "alive_voter" => ?expected_alive_voter,
                    "reason" => err,
                );
                self.on_force_leader_fail();
            }
            Ok(granted) => {
                info!(self.logger,
                    "Unsafe recovery, expected live voters:";
                    "voters" => ?expected_alive_voter,
                    "granted" => granted,
                );
                if granted == expected_alive_voter.len() {
                    self.on_enter_force_leader();
                }
            }
        }
    }

    pub fn maybe_force_forward_commit_index(&mut self) -> bool {
        let failed_stores = match self.force_leader() {
            Some(ForceLeaderState::ForceLeader { failed_stores, .. }) => failed_stores,
            _ => unreachable!(),
        };

        let region = self.region();
        let mut replicated_idx = self.raft_group().raft.raft_log.persisted;
        for (peer_id, p) in self.raft_group().raft.prs().iter() {
            let store_id = region
                .get_peers()
                .iter()
                .find(|p| p.get_id() == *peer_id)
                .unwrap()
                .get_store_id();
            if failed_stores.contains(&store_id) {
                continue;
            }
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
        }

        if self.raft_group().store().term(replicated_idx).unwrap_or(0) < self.term() {
            // do not commit logs of previous term directly
            return false;
        }

        let idx = std::cmp::max(self.raft_group().raft.raft_log.committed, replicated_idx);
        self.raft_group_mut().raft.raft_log.committed = idx;
        true
    }
}
