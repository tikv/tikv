// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::LinkedList, mem};

use kvproto::{metapb, raft_cmdpb::AdminCmdType};
use raftstore::{
    store::{
        cmd_resp,
        fsm::apply,
        msg::ErrorCallback,
        util::{
            admin_cmd_epoch_lookup, AdminCmdEpochState, NORMAL_REQ_CHECK_CONF_VER,
            NORMAL_REQ_CHECK_VER,
        },
    },
    Error,
};

use crate::router::CmdResChannel;

#[derive(Debug)]
pub struct ProposedAdminCmd {
    cmd_type: AdminCmdType,
    committed: bool,
    epoch_state: AdminCmdEpochState,
    index: u64,
    /// Callbacks of commands that are conflict with on going admin command.
    ///
    /// Callbacks are delayed to avoid making client retry with arbitrary
    /// backoff.
    delayed_chs: Vec<CmdResChannel>,
}

impl ProposedAdminCmd {
    fn new(
        cmd_type: AdminCmdType,
        epoch_state: AdminCmdEpochState,
        index: u64,
    ) -> ProposedAdminCmd {
        ProposedAdminCmd {
            cmd_type,
            committed: false,
            epoch_state,
            index,
            delayed_chs: Vec::new(),
        }
    }

    pub fn cmd_type(&self) -> AdminCmdType {
        self.cmd_type
    }

    /// Delay responding to channel until the command is applied so client won't
    /// retry with arbitrary timeout.
    pub fn delay_channel(&mut self, ch: CmdResChannel) {
        self.delayed_chs.push(ch);
    }

    /// Same as `delay_channel`, but accepts a batch.
    pub fn delay_channels(&mut self, chs: Vec<CmdResChannel>) {
        if self.delayed_chs.is_empty() {
            self.delayed_chs = chs;
        } else {
            self.delayed_chs.extend(chs);
        }
    }
}

/// `ProposalControl` is a rewrite of `CmdEpochChecker` from v1.
///
/// Admin command may change the epoch of a region. If a proposal is proposed
/// after the admin command is proposed but before the command is applied, the
/// proposal is probably to fail because of epoch not match. `ProposalControl`
/// aims to detect the failure early. With `ProposalControl`, users can assume
/// once a command is proposed, it's likely to succeed in the end.
///
/// Compared to `CmdEpochChecker`, `ProposalControl` also traces the whole
/// lifetime of prepare merge.
pub struct ProposalControl {
    // Use `LinkedList` to reduce memory footprint. In most cases, the list
    // should be empty or 1 element. And access speed is not a concern.
    proposed_admin_cmd: LinkedList<ProposedAdminCmd>,
    pending_merge_index: u64,
    term: u64,
}

impl ProposalControl {
    pub fn new(term: u64) -> ProposalControl {
        ProposalControl {
            proposed_admin_cmd: LinkedList::new(),
            pending_merge_index: 0,
            term,
        }
    }

    /// Clears all queued conflict callbacks if term changed.
    ///
    /// If term is changed, leader is probably changed. Clear all callbacks to
    /// notify clients to retry with new leader.
    #[inline]
    pub fn maybe_update_term(&mut self, term: u64) {
        match term.cmp(&self.term) {
            std::cmp::Ordering::Equal => (),
            std::cmp::Ordering::Greater => {
                for cmd in mem::take(&mut self.proposed_admin_cmd) {
                    for cb in cmd.delayed_chs {
                        apply::notify_stale_req(term, cb);
                    }
                }
                self.term = term;
            }
            std::cmp::Ordering::Less => {
                panic!("term should not decrease, old {}, new {}", self.term, term)
            }
        }
    }

    /// Check if a proposal is conflict with proposed admin commands in current
    /// term. If the proposal is an admin command, then its type should be
    /// passed, otherwise just provide `None`.
    ///
    /// Returns None if passing the epoch check, otherwise returns the last
    /// conflict conflict proposal meta.
    pub fn check_conflict(
        &mut self,
        cmd_type: Option<AdminCmdType>,
    ) -> Option<&mut ProposedAdminCmd> {
        let (check_ver, check_conf_ver) = match cmd_type {
            None => (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER),
            Some(ty) => {
                let epoch_state = admin_cmd_epoch_lookup(ty);
                (epoch_state.check_ver, epoch_state.check_conf_ver)
            }
        };
        self.proposed_admin_cmd.iter_mut().rev().find(|cmd| {
            (check_ver && cmd.epoch_state.change_ver)
                || (check_conf_ver && cmd.epoch_state.change_conf_ver)
        })
    }

    /// Record an admin proposal.
    ///
    /// Further requests that is conflict with the admin proposal will be
    /// rejected in `check_proposal_conflict`.
    pub fn record_proposed_admin(&mut self, cmd_type: AdminCmdType, index: u64) {
        let epoch_state = admin_cmd_epoch_lookup(cmd_type);
        if !epoch_state.change_conf_ver && !epoch_state.change_ver {
            return;
        }

        let conflict_cmd = self.proposed_admin_cmd.iter_mut().rev().find(|cmd| {
            (epoch_state.check_ver && cmd.epoch_state.change_ver)
                || (epoch_state.check_conf_ver && cmd.epoch_state.change_conf_ver)
        });
        assert!(conflict_cmd.is_none(), "{:?}", conflict_cmd);

        if let Some(cmd) = self.proposed_admin_cmd.back() {
            assert!(cmd.index < index, "{:?} {}", cmd, index);
        }
        self.proposed_admin_cmd
            .push_back(ProposedAdminCmd::new(cmd_type, epoch_state, index));
    }

    /// Commit the admin commands.
    #[inline]
    pub fn commit_to(&mut self, index: u64, mut on_commit: impl FnMut(&ProposedAdminCmd)) {
        if self.proposed_admin_cmd.is_empty() {
            return;
        }

        for cmd in &mut self.proposed_admin_cmd {
            if cmd.committed {
                continue;
            }
            if cmd.index <= index {
                cmd.committed = true;
                on_commit(cmd);
                continue;
            }
            return;
        }
    }

    pub fn advance_apply(&mut self, index: u64, term: u64, region: &metapb::Region) {
        while !self.proposed_admin_cmd.is_empty() {
            let cmd = self.proposed_admin_cmd.front_mut().unwrap();
            if cmd.index <= index {
                for ch in cmd.delayed_chs.drain(..) {
                    let mut resp = cmd_resp::new_error(Error::EpochNotMatch(
                        format!(
                            "current epoch of region {} is {:?}",
                            region.get_id(),
                            region.get_region_epoch(),
                        ),
                        vec![region.to_owned()],
                    ));
                    cmd_resp::bind_term(&mut resp, term);
                    ch.report_error(resp);
                }
            } else {
                break;
            }
            self.proposed_admin_cmd.pop_front();
        }
    }

    #[inline]
    pub fn enter_prepare_merge(&mut self, prepare_merge_index: u64) {
        self.pending_merge_index = prepare_merge_index;
    }

    #[inline]
    pub fn leave_prepare_merge(&mut self, prepare_merge_index: u64) {
        if self.pending_merge_index != 0 {
            assert_eq!(self.pending_merge_index, prepare_merge_index);
            self.pending_merge_index = 0;
        }
    }

    /// Check if there is an on-going split command on current term.
    ///
    /// The answer is reliable only when the peer is leader.
    #[inline]
    pub fn is_splitting(&self) -> bool {
        if self.proposed_admin_cmd.is_empty() {
            return false;
        }
        // Split is deprecated in v2, only needs to check `BatchSplit`.
        self.proposed_admin_cmd
            .iter()
            .any(|c| c.cmd_type == AdminCmdType::BatchSplit && c.committed)
    }

    /// Check if there the current peer is waiting for being merged.
    ///
    /// The answer is reliable only when the peer is leader or `PrepareMerge` is
    /// applied.
    #[inline]
    pub fn is_merging(&self) -> bool {
        if self.proposed_admin_cmd.is_empty() {
            return self.pending_merge_index != 0;
        }
        self.proposed_admin_cmd
            .iter()
            .any(|c| c.cmd_type == AdminCmdType::PrepareMerge && c.committed)
    }
}

impl Drop for ProposalControl {
    fn drop(&mut self) {
        for state in mem::take(&mut self.proposed_admin_cmd) {
            for ch in state.delayed_chs {
                apply::notify_stale_req(self.term, ch);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proposal_control() {
        let region = metapb::Region::default();

        let mut control = ProposalControl::new(10);
        assert_eq!(control.term, 10);
        assert!(
            control
                .check_conflict(Some(AdminCmdType::BatchSplit))
                .is_none()
        );
        control.record_proposed_admin(AdminCmdType::BatchSplit, 5);
        assert_eq!(control.proposed_admin_cmd.len(), 1);

        // Both conflict with the split admin cmd
        let conflict = control.check_conflict(None).unwrap();
        assert_eq!(conflict.index, 5);
        assert_eq!(conflict.cmd_type, AdminCmdType::BatchSplit);
        let conflict = control
            .check_conflict(Some(AdminCmdType::PrepareMerge))
            .unwrap();
        assert_eq!(conflict.index, 5);

        assert!(
            control
                .check_conflict(Some(AdminCmdType::ChangePeerV2))
                .is_none()
        );
        control.record_proposed_admin(AdminCmdType::ChangePeerV2, 6);
        assert_eq!(control.proposed_admin_cmd.len(), 2);

        assert!(!control.is_splitting());
        assert!(!control.is_merging());

        // Conflict with the change peer admin cmd
        let conflict = control
            .check_conflict(Some(AdminCmdType::ChangePeerV2))
            .unwrap();
        assert_eq!(conflict.index, 6);
        // Conflict with the split admin cmd
        let conflict = control.check_conflict(None).unwrap();
        assert_eq!(conflict.index, 5);
        // Conflict with the change peer admin cmd
        let conflict = control
            .check_conflict(Some(AdminCmdType::PrepareMerge))
            .unwrap();
        assert_eq!(conflict.index, 6);

        let mut commit_split = false;
        control.commit_to(4, |c| commit_split = c.cmd_type == AdminCmdType::BatchSplit);
        assert!(!commit_split);
        assert!(!control.is_splitting());
        control.commit_to(5, |c| commit_split = c.cmd_type == AdminCmdType::BatchSplit);
        assert!(commit_split);
        assert!(control.is_splitting());

        control.advance_apply(4, 10, &region);
        // Have no effect on `proposed_admin_cmd`
        assert_eq!(control.proposed_admin_cmd.len(), 2);
        assert!(control.is_splitting());

        control.advance_apply(5, 10, &region);
        // Left one change peer admin cmd
        assert_eq!(control.proposed_admin_cmd.len(), 1);
        assert!(!control.is_splitting());

        assert!(control.check_conflict(None).is_none());
        let conflict = control
            .check_conflict(Some(AdminCmdType::BatchSplit))
            .unwrap();
        assert_eq!(conflict.index, 6);

        // Change term to 11
        control.maybe_update_term(11);
        assert!(
            control
                .check_conflict(Some(AdminCmdType::BatchSplit))
                .is_none()
        );
        assert_eq!(control.term, 11);
        // Should be empty
        assert_eq!(control.proposed_admin_cmd.len(), 0);

        // Test attaching multiple callbacks.
        control.record_proposed_admin(AdminCmdType::BatchSplit, 7);
        let mut subs = vec![];
        for _ in 0..3 {
            let conflict = control.check_conflict(None).unwrap();
            let (ch, sub) = CmdResChannel::pair();
            conflict.delay_channel(ch);
            subs.push(sub);
        }
        // Delayed channel should not be notified immediately.
        for sub in &subs {
            assert!(!sub.has_result());
        }
        control.advance_apply(7, 12, &region);
        for sub in subs {
            assert!(sub.has_result());
            let res = futures::executor::block_on(sub.result()).unwrap();
            assert!(
                res.get_header().get_error().has_epoch_not_match(),
                "{:?}",
                res
            );
        }

        // Should invoke callbacks when term is increased.
        control.record_proposed_admin(AdminCmdType::BatchSplit, 8);
        let (ch, sub) = CmdResChannel::pair();
        control.check_conflict(None).unwrap().delay_channel(ch);
        control.maybe_update_term(13);
        assert!(control.check_conflict(None).is_none());
        let res = futures::executor::block_on(sub.result()).unwrap();
        assert!(
            res.get_header().get_error().has_stale_command(),
            "{:?}",
            res
        );

        // Should invoke callbacks when it's dropped.
        control.record_proposed_admin(AdminCmdType::BatchSplit, 9);
        let (ch, sub) = CmdResChannel::pair();
        control.check_conflict(None).unwrap().delay_channel(ch);
        drop(control);
        let res = futures::executor::block_on(sub.result()).unwrap();
        assert!(
            res.get_header().get_error().has_stale_command(),
            "{:?}",
            res
        );
    }

    #[test]
    fn test_proposal_control_merge() {
        let region = metapb::Region::default();

        let mut control = ProposalControl::new(5);
        assert!(!control.is_merging());
        control.record_proposed_admin(AdminCmdType::PrepareMerge, 5);
        assert!(!control.is_merging());
        control.commit_to(5, |_| ());
        assert!(control.is_merging());
        control.advance_apply(5, 5, &region);
        assert!(!control.is_merging());

        control.record_proposed_admin(AdminCmdType::PrepareMerge, 6);
        assert!(!control.is_merging());
        control.commit_to(6, |_| ());
        assert!(control.is_merging());
        control.enter_prepare_merge(6);
        control.advance_apply(6, 5, &region);
        assert!(control.is_merging());
        control.leave_prepare_merge(6);
        assert!(!control.is_merging());
    }
}
