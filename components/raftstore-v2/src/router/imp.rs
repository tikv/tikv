// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

use engine_traits::{KvEngine, RaftEngine};
use raftstore::store::{FetchedLogs, LogFetchedNotifier};
use slog::Logger;

use super::PeerMsg;
use crate::{
    batch::StoreRouter,
    operation::{CachedReadDelegate, LocalReader, StoreMetaDelegate},
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
    local_reader:
        RefCell<LocalReader<StoreRouter<EK, ER>, CachedReadDelegate<EK>, StoreMetaDelegate<EK>>>,
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
}
