// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Mutex;

use engine_rocks::{CompactedEventSender, RocksCompactedEvent, RocksEngine};
use engine_traits::RaftEngine;
use tikv_util::warn;

use crate::store::{fsm::store::RaftRouter, StoreMsg};

// raftstore v1's implementation
pub struct RaftRouterCompactedEventSender<ER: RaftEngine> {
    pub router: Mutex<RaftRouter<RocksEngine, ER>>,
}

impl<ER: RaftEngine> CompactedEventSender for RaftRouterCompactedEventSender<ER> {
    fn send(&self, event: RocksCompactedEvent) {
        let router = self.router.lock().unwrap();
        let event = StoreMsg::CompactedEvent(event);
        if let Err(e) = router.send_control(event) {
            warn!(
                "send compaction finished event to raftstore failed";
                "err" => ?e,
            );
        }
    }
}
