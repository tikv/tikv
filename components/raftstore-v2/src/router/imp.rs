// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RaftMessage,
};
use raftstore::store::{FetchedLogs, LogFetchedNotifier, RegionSnapshot};
use slog::Logger;

use super::PeerMsg;
use crate::{batch::StoreRouter, operation::LocalReader, StoreMeta};

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
        let local_reader = RefCell::new(LocalReader::new(store_meta, router.clone(), logger));
        ServerRaftStoreRouter {
            router,
            local_reader,
        }
    }

    pub fn send(&self, addr: u64, msg: PeerMsg) -> Result<(), TrySendError<PeerMsg>> {
        self.router.send(addr, msg)
    }

    pub fn send_raft_message(
        &self,
        msg: Box<RaftMessage>,
    ) -> std::result::Result<(), TrySendError<Box<RaftMessage>>> {
        self.router.send_raft_message(msg)
    }

    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn get_snapshot(
        &self,
        req: RaftCmdRequest,
    ) -> std::result::Result<RegionSnapshot<EK::Snapshot>, RaftCmdResponse> {
        let mut local_reader = self.local_reader.borrow_mut();
        local_reader.snapshot(req).await
    }
}
