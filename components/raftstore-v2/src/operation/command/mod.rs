// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains implementations of commmands that will be replicated to
//! all replicas and executed in the same order. Typical commands include:
//! - normal writes like put, delete, etc.
//! - admin commands like split, compact, etc.

use std::cmp;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest};
use protobuf::Message;
use raft::eraftpb::Entry;
use raftstore::{
    store::{
        fsm::Proposal, local_metrics::RaftMetrics, metrics::*, msg::ErrorCallback, util,
        WriteCallback,
    },
    Error, Result,
};
use slog::error;
use tikv_util::{box_err, time::monotonic_raw_now};

use crate::{
    batch::StoreContext,
    fsm::{PeerFsm, PeerFsmDelegate},
    raft::Peer,
    router::CmdResChannel,
};

mod write;

pub use write::{SimpleWriteDecoder, SimpleWriteEncoder};

impl<'a, EK: KvEngine, ER: RaftEngine, T> PeerFsmDelegate<'a, EK, ER, T> {
    #[inline]
    pub fn on_command(&mut self, req: RaftCmdRequest, ch: CmdResChannel) {
        if !req.get_requests().is_empty() {
            self.fsm
                .peer_mut()
                .on_write_command(self.store_ctx, req, ch)
        } else if req.has_admin_request() {
            // self.on_admin_request(req, ch)
        } else if req.has_status_request() {
            error!(self.fsm.logger(), "status command should be sent by Query");
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    fn validate_command(&self, req: &RaftCmdRequest, metrics: &mut RaftMetrics) -> Result<()> {
        if let Err(e) = util::check_store_id(req, self.peer().get_store_id()) {
            metrics.invalid_proposal.mismatch_store_id.inc();
            return Err(e);
        }
        for r in req.get_requests() {
            if let CmdType::Get | CmdType::Snap | CmdType::ReadIndex = r.get_cmd_type() {
                return Err(box_err!("internal error: query can't be sent as command"));
            }
        }
        if let Err(e) = util::check_peer_id(req, self.peer().get_id()) {
            metrics.invalid_proposal.mismatch_peer_id.inc();
            return Err(e);
        }
        if !self.is_leader() {
            metrics.invalid_proposal.not_leader.inc();
            return Err(Error::NotLeader(self.region_id(), self.leader()));
        }
        if let Err(e) = util::check_term(req, self.term()) {
            metrics.invalid_proposal.stale_command.inc();
            return Err(e);
        }
        if let Err(mut e) = util::check_region_epoch(req, self.region(), true) {
            if let Error::EpochNotMatch(_, new_regions) = &mut e {
                // TODO: query sibling regions.
                metrics.invalid_proposal.epoch_not_match.inc();
            }
            return Err(e);
        }
        Ok(())
    }

    #[inline]
    fn propose_command<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        let data = req.write_to_bytes().unwrap();
        self.propose(ctx, data)
    }

    #[inline]
    fn propose<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>, data: Vec<u8>) -> Result<u64> {
        ctx.raft_metrics.propose.normal.inc();
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);
        if data.len() as u64 > ctx.cfg.raft_entry_max_size.0 {
            return Err(Error::RaftEntryTooLarge {
                region_id: self.region_id(),
                entry_size: data.len() as u64,
            });
        }
        let last_index = self.raft_group().raft.raft_log.last_index();
        self.raft_group_mut().propose(vec![], data)?;
        if self.raft_group().raft.raft_log.last_index() == last_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id(), None));
        }
        Ok(last_index + 1)
    }

    #[inline]
    fn enqueue_pending_proposal<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut proposal: Proposal<Vec<CmdResChannel>>,
    ) {
        let applied_to_current_term = self.applied_to_current_term();
        if applied_to_current_term {
            proposal.cb.notify_proposed();
        }
        proposal.must_pass_epoch_check = applied_to_current_term;
        proposal.propose_time = Some(*ctx.current_time.get_or_insert_with(monotonic_raw_now));
        self.proposals_mut().push(proposal);
    }

    #[inline]
    pub fn schedule_apply_committed_entries<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        committed_entries: Vec<Entry>,
    ) {
        let last_entry = match committed_entries.last() {
            Some(e) => e,
            None => return,
        };
        let current_term = self.term();
        let mut entry_and_proposals = vec![];
        let queue = self.proposals_mut();
        if !queue.is_empty() {
            for e in committed_entries {
                let mut proposal = queue.find_proposal(e.term, e.index, current_term);
                if let Some(p) = &mut proposal && p.must_pass_epoch_check {
                    // In this case the apply can be guaranteed to be successful. Invoke the
                    // on_committed callback if necessary.
                    p.cb.notify_committed();
                }
                entry_and_proposals.push((e, proposal));
            }
        } else {
            entry_and_proposals = committed_entries.into_iter().map(|e| (e, None)).collect();
        }
        // Note that the `commit_index` and `commit_term` here may be used to
        // forward the commit index after being restarted. So it must be less
        // than or equal to persisted index.
        let commit_index = cmp::min(
            self.raft_group().raft.raft_log.committed,
            self.raft_group().raft.raft_log.persisted,
        );
        let commit_term = self.raft_group().raft.raft_log.term(commit_index).unwrap();
        // TODO: schedule apply task
    }
}
