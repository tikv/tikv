// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains merge related processing logic.
//!
//! ## Propose
//!
//! The proposal is initiated by the source region peer. It periodically checks
//! for the freshness of local target region peer (`Peer::on_check_merge`). Once
//! the target peer is up-to-date, it sends a `CommitMerge` command to local
//! target region peer. (For simplicity, we send this message regardless of
//! whether the target peer is leader.) The command will also carry some source
//! region logs that are potentially not yet committed by certain peers.
//!
//! ## Apply (`Apply::apply_commit_merge`)
//!
//! Firstly, target region applies the `CommitMerge` command without
//! changing region states. Instead it redirects the log entries from source
//! region, as a `CatchUpLogs` message, to the local source region peer. When
//! the source region peer has applied all logs up to the prior `PrepareMerge`
//! command, it will notify its [`Apply`] FSM to destroy itself and wake up
//! target peer (`Peer::update_merge_progress_on_ready_prepare_merge`,
//! `Apply::logs_up_to_date_for_merge`).
//!
//! The target [`Apply`] FSM is able to confirm that the local source region
//! peer has caught up logs, via an atomic counter. Once confirmed, it will
//! resume the apply of `CommitMerge` command.
//!
//! ## On Apply Result (`Peer::on_ready_commit_merge`)
//!
//! Update the target peer states and send a `MergeResult` to source peer to
//! destroy it.

use engine_traits::{KvEngine, RaftEngine};

use crate::raft::Peer;

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_merge_check_tick(&mut self) {
        // TODO
    }

    pub fn update_merge_progress_on_ready_prepare_merge(&mut self) {
        assert!(self.pending_merge_state.is_some());
        // TODO
    }
}
