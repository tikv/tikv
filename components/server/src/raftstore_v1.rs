// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Mutex;

use engine_rocks::{CompactedEventSender, RocksCompactedEvent, RocksEngine};
use engine_traits::RaftEngine;
use raftstore::store::{fsm::store::RaftRouter, StoreMsg};

// raftstore v1's implementation
pub struct CompactedEventSenderV1<ER: RaftEngine> {
    pub router: Mutex<RaftRouter<RocksEngine, ER>>,
}

impl<ER: RaftEngine> CompactedEventSender for CompactedEventSenderV1<ER> {
    fn send(&self, event: RocksCompactedEvent) {
        let router = self.router.lock().unwrap();
        let event = StoreMsg::CompactedEvent(event);
        if let Err(e) = router.send_control(event) {
            error_unknown!(?e; "send compaction finished event to raftstore failed");
        }
    }

    fn clone(&self) -> Box<dyn CompactedEventSender + Send + Sync> {
        Box::new(Self {
            router: Mutex::new(self.router.lock().unwrap().clone()),
        })
    }
}
