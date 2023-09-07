// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::Bound::{Excluded, Unbounded};

use crossbeam::channel::SendError;
use engine_traits::{KvEngine, RaftEngine, TabletContext};
use keys::{data_end_key, data_key, enc_start_key};
use kvproto::metapb::Region;
use raftstore::store::{
    PeerPessimisticLocks, Transport, UnsafeRecoveryExecutePlanSyncer, UnsafeRecoveryState,
    RAFT_INIT_LOG_INDEX,
};
use slog::{error, info, warn};

use crate::{
    batch::StoreContext,
    fsm::Store,
    operation::{command::temp_split_path, SplitInit},
    raft::Peer,
    router::PeerMsg,
};

impl Store {
    // Reuse split mechanism to create peer. Because in v2 the only way to
    // create and initialize to peer is via a snapshot message.
    pub fn on_unsafe_recovery_create_peer<EK, ER, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        region: Region,
        syncer: UnsafeRecoveryExecutePlanSyncer,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
        T: Transport,
    {
        info!(self.logger(), "Unsafe recovery, creating a peer"; "peer" => ?region);
        // Check if the peer has been created already.
        let meta = ctx.store_meta.lock().unwrap();
        if let Some((_, id)) = meta
            .region_ranges
            .range((
                Excluded((data_key(region.get_start_key()), u64::MAX)),
                Unbounded::<(Vec<u8>, u64)>,
            ))
            .next()
        {
            let (exist_region, _) = &meta.regions[id];
            if enc_start_key(exist_region) < data_end_key(region.get_end_key()) {
                if exist_region.get_id() == region.get_id() {
                    warn!(self.logger(),
                        "Unsafe recovery, region has already been created";
                        "region" => ?region,
                        "exist_region" => ?exist_region,
                    );
                    return;
                } else {
                    error!(self.logger(),
                        "Unsafe recovery, region to be created overlaps with an existing region";
                        "region" => ?region,
                        "exist_region" => ?exist_region,
                    );
                    return;
                }
            }
        }
        drop(meta);

        // Create an empty split tablet.
        let region_id = region.get_id();
        let path = temp_split_path(&ctx.tablet_registry, region_id);
        let tctx = TabletContext::new(&region, Some(RAFT_INIT_LOG_INDEX));
        // TODO: make the follow line can recover from abort.
        if let Err(e) = ctx
            .tablet_registry
            .tablet_factory()
            .open_tablet(tctx, &path)
        {
            error!(self.logger(),
                "Unsafe recovery, region to be created due to fail to open tablet";
                "region" => ?region,
                "error" => ?e,
            );
            return;
        }

        let split_init = Box::new(SplitInit {
            region,
            derived_leader: false,
            derived_region_id: 0, // No derived region.
            check_split: false,
            scheduled: false,
            approximate_size: None,
            approximate_keys: None,
            locks: PeerPessimisticLocks::default(),
        });
        // Skip sending SplitInit if there exists a peer, because a peer can not
        // handle concurrent SplitInit messages.
        self.on_split_init(ctx, split_init, true /* skip_if_exists */);

        let wait = PeerMsg::UnsafeRecoveryWaitInitialized(syncer);
        if let Err(SendError(_)) = ctx.router.force_send(region_id, wait) {
            warn!(
                self.logger(),
                "Unsafe recovery, created peer is destroyed before sending wait msg";
                "region_id" => region_id,
            );
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_unsafe_recovery_wait_initialized(&mut self, syncer: UnsafeRecoveryExecutePlanSyncer) {
        if let Some(state) = self.unsafe_recovery_state() && !state.is_abort() {
            warn!(self.logger,
                "Unsafe recovery, can't wait initialize, another plan is executing in progress";
                "state" => ?state,
            );
            syncer.abort();
            return;
        }

        *self.unsafe_recovery_state_mut() = Some(UnsafeRecoveryState::WaitInitialize(syncer));
        self.unsafe_recovery_maybe_finish_wait_initialized(!self.serving());
    }

    pub fn unsafe_recovery_maybe_finish_wait_initialized(&mut self, force: bool) {
        if let Some(UnsafeRecoveryState::WaitInitialize(_)) = self.unsafe_recovery_state() {
            if self.storage().is_initialized() || force {
                info!(self.logger,
                    "Unsafe recovery, finish wait initialize";
                    "tablet_index" =>  self.storage().tablet_index(),
                    "force" => force,
                );
                *self.unsafe_recovery_state_mut() = None;
            }
        }
    }
}
