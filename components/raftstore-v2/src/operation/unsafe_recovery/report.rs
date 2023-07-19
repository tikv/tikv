// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::pdpb;
use raftstore::store::Transport;

use crate::{batch::StoreContext, fsm::Store};

impl Store {
    pub fn on_unsafe_recovery_report<EK, ER, T>(
        &self,
        ctx: &StoreContext<EK, ER, T>,
        report: pdpb::StoreReport,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
        T: Transport,
    {
        self.store_heartbeat_pd(ctx, Some(report))
    }
}
