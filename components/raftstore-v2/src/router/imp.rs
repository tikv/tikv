// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

use engine_traits::{KvEngine, RaftEngine};
use futures::executor::block_on;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use raftstore::store::{FetchedLogs, LogFetchedNotifier, RegionSnapshot};
use slog::Logger;

use super::PeerMsg;
use crate::{
    batch::StoreRouter,
    operation::{LocalReader, StoreMetaDelegate},
    StoreMeta,
};

impl<EK: KvEngine, ER: RaftEngine> LogFetchedNotifier for StoreRouter<EK, ER> {
    fn notify(&self, region_id: u64, fetched: FetchedLogs) {
        let _ = self.force_send(region_id, PeerMsg::FetchedLogs(fetched));
    }
}

/// A router that routes messages to the raftstore
pub struct ServerRaftStoreRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    router: StoreRouter<EK, ER>,
    local_reader: RefCell<LocalReader<EK, StoreRouter<EK, ER>>>,
}

impl<EK, ER> Clone for ServerRaftStoreRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn clone(&self) -> Self {
        ServerRaftStoreRouter {
            router: self.router.clone(),
            local_reader: self.local_reader.clone(),
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> ServerRaftStoreRouter<EK, ER> {
    pub fn new(
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        router: StoreRouter<EK, ER>,
        logger: Logger,
    ) -> Self {
        let local_reader = RefCell::new(LocalReader::new(
            StoreMetaDelegate::new(store_meta),
            router.clone(),
            logger,
        ));
        ServerRaftStoreRouter {
            router,
            local_reader,
        }
    }

    pub fn router(&self) -> &StoreRouter<EK, ER> {
        &self.router
    }

    pub fn router_mut(&mut self) -> &mut StoreRouter<EK, ER> {
        &mut self.router
    }

    #[allow(clippy::await_holding_refcell_ref)]
    pub fn get_snapshot(
        &self,
        req: RaftCmdRequest,
    ) -> std::result::Result<RegionSnapshot<EK::Snapshot>, RaftCmdResponse> {
        block_on({
            async {
                let mut local_reader = self.local_reader.borrow_mut();
                local_reader.snapshot(req).await
            }
        })
    }
}
