// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use crossbeam::channel::TrySendError;
use engine_rocks::{RocksEngine, RocksSnapshot};
use kvproto::raft_serverpb::RaftMessage;
use raft::SnapshotStatus;
use raftstore::errors::{Error as RaftStoreError, Result as RaftStoreResult};
use raftstore::router::{handle_send_error, RaftPeerRouter, RaftStoreRouter};
use raftstore::store::msg::{CasualMessage, PeerMsg, SignificantMsg};
use raftstore::store::{CasualRouter, ProposalRouter, RaftCommand, StoreMsg, StoreRouter};
use tikv_util::collections::HashMap;
use tikv_util::mpsc::{loose_bounded, LooseBoundedSender, Receiver};

#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct MockRaftStoreRouter {
    senders: Arc<Mutex<HashMap<u64, LooseBoundedSender<PeerMsg<RocksEngine>>>>>,
}

impl MockRaftStoreRouter {
    pub fn new() -> MockRaftStoreRouter {
        MockRaftStoreRouter {
            senders: Arc::default(),
        }
    }
    pub fn add_region(&self, region_id: u64, cap: usize) -> Receiver<PeerMsg<RocksEngine>> {
        let (tx, rx) = loose_bounded(cap);
        self.senders.lock().unwrap().insert(region_id, tx);
        rx
    }
}

impl StoreRouter for MockRaftStoreRouter {
    fn send(&self, _: StoreMsg) -> RaftStoreResult<()> {
        unimplemented!();
    }
}

impl ProposalRouter<RocksSnapshot> for MockRaftStoreRouter {
    fn send(
        &self,
        _: RaftCommand<RocksSnapshot>,
    ) -> std::result::Result<(), TrySendError<RaftCommand<RocksSnapshot>>> {
        unimplemented!();
    }
}

impl CasualRouter<RocksEngine> for MockRaftStoreRouter {
    fn send(&self, region_id: u64, msg: CasualMessage<RocksEngine>) -> RaftStoreResult<()> {
        let mut senders = self.senders.lock().unwrap();
        if let Some(tx) = senders.get_mut(&region_id) {
            tx.try_send(PeerMsg::CasualMessage(msg))
                .map_err(|e| handle_send_error(region_id, e))
        } else {
            Err(RaftStoreError::RegionNotFound(region_id))
        }
    }
}

impl RaftPeerRouter for MockRaftStoreRouter {
    fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
        unimplemented!()
    }
    fn clone_box(&self) -> Box<dyn RaftPeerRouter> {
        Box::new(self.clone())
    }
    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> RaftStoreResult<()> {
        self.significant_send(
            region_id,
            SignificantMsg::SnapshotStatus {
                region_id,
                to_peer_id,
                status,
            },
        )
    }
}

impl RaftStoreRouter<RocksEngine> for MockRaftStoreRouter {
    fn significant_send(
        &self,
        region_id: u64,
        msg: SignificantMsg<RocksSnapshot>,
    ) -> RaftStoreResult<()> {
        let mut senders = self.senders.lock().unwrap();
        if let Some(tx) = senders.get_mut(&region_id) {
            tx.force_send(PeerMsg::SignificantMsg(msg)).unwrap();
            Ok(())
        } else {
            error!("failed to send significant msg"; "msg" => ?msg);
            Err(RaftStoreError::RegionNotFound(region_id))
        }
    }

    fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<RocksEngine>) {}
}
