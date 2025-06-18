// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Mutex;

use engine_rocks::{
    CompactedEventSender, RocksCompactedEvent, RocksStatsChangeEvent, RocksStatsChangeEventSender,
};
use engine_traits::{KvEngine, RaftEngine};
use tikv_util::error_unknown;

use crate::store::{StoreMsg, fsm::store::RaftRouter};

// raftstore v1's implementation
pub struct RaftRouterCompactedEventSender<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub router: Mutex<RaftRouter<EK, ER>>,
}

impl<EK, ER> CompactedEventSender for RaftRouterCompactedEventSender<EK, ER>
where
    EK: KvEngine<CompactedEvent = RocksCompactedEvent>,
    ER: RaftEngine,
{
    fn send(&self, event: RocksCompactedEvent) {
        let router: std::sync::MutexGuard<'_, RaftRouter<EK, ER>> = self.router.lock().unwrap();
        let event = StoreMsg::CompactedEvent(event);
        if let Err(e) = router.send_control(event) {
            error_unknown!(?e; "send compaction finished event to raftstore failed");
        }
    }
}

pub struct RaftRouterStatsChangeEventSender<EK, ER>
where
    EK: KvEngine<StatsChangeEvent = RocksStatsChangeEvent>,
    ER: RaftEngine,
{
    pub router: Mutex<RaftRouter<EK, ER>>,
}
impl<EK, ER> RocksStatsChangeEventSender for RaftRouterStatsChangeEventSender<EK, ER>
where
    EK: KvEngine<StatsChangeEvent = RocksStatsChangeEvent>,
    ER: RaftEngine,
{
    fn send(&self, event: RocksStatsChangeEvent) {
        let router: std::sync::MutexGuard<'_, RaftRouter<EK, ER>> = self.router.lock().unwrap();
        let event = StoreMsg::StatsChangeEvent(event);
        if let Err(e) = router.send_control(event) {
            error_unknown!(?e; "send stats change event to raftstore failed");
        }
    }
}
