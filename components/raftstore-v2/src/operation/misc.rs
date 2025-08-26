// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine};
use raftstore::{Result, store::TabletSnapKey};
use slog::warn;

use crate::{
    batch::StoreContext,
    fsm::{Store, StoreFsmDelegate},
    router::{PeerMsg, StoreTick},
    worker::tablet,
};

impl<EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'_, EK, ER, T> {
    #[inline]
    pub fn on_snapshot_gc(&mut self) {
        if let Err(e) = self.fsm.store.on_snapshot_gc(self.store_ctx) {
            warn!(self.fsm.store.logger(), "cleanup import sst failed"; "error" => ?e);
        }
        self.schedule_tick(
            StoreTick::SnapGc,
            self.store_ctx.cfg.snap_mgr_gc_tick_interval.0,
        );
    }
}

impl Store {
    #[inline]
    fn on_snapshot_gc<EK: KvEngine, ER: RaftEngine, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
    ) -> Result<()> {
        let paths = ctx.snap_mgr.list_snapshot()?;
        let mut region_keys: HashMap<u64, Vec<TabletSnapKey>> = HashMap::default();
        for path in paths {
            let key = TabletSnapKey::from_path(path)?;
            region_keys.entry(key.region_id).or_default().push(key);
        }
        for (region_id, keys) in region_keys {
            if let Err(TrySendError::Disconnected(msg)) =
                ctx.router.send(region_id, PeerMsg::SnapGc(keys.into()))
                && !ctx.router.is_shutdown()
            {
                let PeerMsg::SnapGc(keys) = msg else {
                    unreachable!()
                };
                let _ = ctx.schedulers.tablet.schedule(tablet::Task::SnapGc(keys));
            }
        }
        Ok(())
    }
}
