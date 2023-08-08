// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use raftstore::store::{UnsafeRecoveryExecutePlanSyncer, UnsafeRecoveryState};
use slog::warn;

use crate::raft::Peer;

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_unsafe_recovery_destroy_peer(&mut self, syncer: UnsafeRecoveryExecutePlanSyncer) {
        if self.unsafe_recovery_state().is_some() {
            warn!(self.logger,
                "Unsafe recovery, can't destroy, another plan is executing in progress";
            );
            syncer.abort();
            return;
        }
        // Syncer will be dropped after peer finishing destroy process.
        *self.unsafe_recovery_state_mut() = Some(UnsafeRecoveryState::Destroy(syncer));
        self.mark_for_destroy(None);
    }
}
