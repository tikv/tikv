// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use raftstore::store::{FetchedLogs, LogFetchedNotifier};

use super::PeerMsg;
use crate::batch::StoreRouter;

impl<EK: KvEngine, ER: RaftEngine> LogFetchedNotifier for StoreRouter<EK, ER> {
    fn notify(&self, region_id: u64, fetched: FetchedLogs) {
        let _ = self.force_send(region_id, PeerMsg::FetchedLogs(fetched));
    }
}
