// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use parking_lot::lock_api::RawMutex;

use crate::{fsm::StoreFsmDelegate, router::StoreTick};

impl<'a, EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'a, EK, ER, T> {
    fn register_compact_check_tick(&self) {
        self.schedule_tick(
            StoreTick::CompactCheck,
            self.store_ctx.cfg.region_compact_check_interval.0,
        )
    }

    pub fn on_ompact_check_tick(&mut self) {
        let meta = self.store_ctx.store_meta.lock().unwrap();
        
    }
}
