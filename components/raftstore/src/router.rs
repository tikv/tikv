// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::{SendError, TrySendError};
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::RaftMessage;

use crate::store::fsm::RaftRouter;
use crate::store::{
    Callback, CasualMessage, LocalReader, PeerMsg, RaftCommand, SignificantMsg, StoreMsg,
};
use crate::{DiscardReason, Error as RaftStoreError, Result as RaftStoreResult};
use engine_rocks::RocksEngine;
use raft::SnapshotStatus;

/// Routes messages to the raftstore.
pub trait RaftStoreRouter: Send + Clone {
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()>;

    /// Sends RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback<RocksEngine>) -> RaftStoreResult<()>;

    /// Sends a significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, region_id: u64, msg: SignificantMsg) -> RaftStoreResult<()>;

    /// Reports the peer being unreachable to the Region.
    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()> {
        self.significant_send(
            region_id,
            SignificantMsg::Unreachable {
                region_id,
                to_peer_id,
            },
        )
    }

    fn broadcast_unreachable(&self, store_id: u64);

    /// Reports the sending snapshot status to the peer of the Region.
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

    fn casual_send(&self, region_id: u64, msg: CasualMessage<RocksEngine>) -> RaftStoreResult<()>;
}

#[derive(Clone)]
pub struct RaftStoreBlackHole;

impl RaftStoreRouter for RaftStoreBlackHole {
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
        Ok(())
    }

    /// Sends RaftCmdRequest to local store.
    fn send_command(&self, _: RaftCmdRequest, _: Callback<RocksEngine>) -> RaftStoreResult<()> {
        Ok(())
    }

    /// Sends a significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, _: u64, _: SignificantMsg) -> RaftStoreResult<()> {
        Ok(())
    }

    fn broadcast_unreachable(&self, _: u64) {}

    fn casual_send(&self, _: u64, _: CasualMessage<RocksEngine>) -> RaftStoreResult<()> {
        Ok(())
    }
}

/// A router that routes messages to the raftstore
#[derive(Clone)]
pub struct ServerRaftStoreRouter {
    router: RaftRouter<RocksEngine>,
    local_reader: LocalReader<RaftRouter<RocksEngine>, RocksEngine>,
}

impl ServerRaftStoreRouter {
    /// Creates a new router.
    pub fn new(
        router: RaftRouter<RocksEngine>,
        local_reader: LocalReader<RaftRouter<RocksEngine>, RocksEngine>,
    ) -> ServerRaftStoreRouter {
        ServerRaftStoreRouter {
            router,
            local_reader,
        }
    }

    pub fn send_store(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        self.router.send_control(msg).map_err(|e| {
            RaftStoreError::Transport(match e {
                TrySendError::Full(_) => DiscardReason::Full,
                TrySendError::Disconnected(_) => DiscardReason::Disconnected,
            })
        })
    }
}

#[inline]
fn handle_error<T>(region_id: u64, e: TrySendError<T>) -> RaftStoreError {
    match e {
        TrySendError::Full(_) => RaftStoreError::Transport(DiscardReason::Full),
        TrySendError::Disconnected(_) => RaftStoreError::RegionNotFound(region_id),
    }
}

impl RaftStoreRouter for ServerRaftStoreRouter {
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let region_id = msg.get_region_id();
        self.router
            .send_raft_message(msg)
            .map_err(|e| handle_error(region_id, e))
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback<RocksEngine>) -> RaftStoreResult<()> {
        let cmd = RaftCommand::new(req, cb);
        if LocalReader::<RaftRouter<RocksEngine>, RocksEngine>::acceptable(&cmd.request) {
            self.local_reader.execute_raft_command(cmd);
            Ok(())
        } else {
            let region_id = cmd.request.get_header().get_region_id();
            self.router
                .send_raft_command(cmd)
                .map_err(|e| handle_error(region_id, e))
        }
    }

    fn significant_send(&self, region_id: u64, msg: SignificantMsg) -> RaftStoreResult<()> {
        if let Err(SendError(msg)) = self
            .router
            .force_send(region_id, PeerMsg::SignificantMsg(msg))
        {
            // TODO: panic here once we can detect system is shutting down reliably.
            error!("failed to send significant msg"; "msg" => ?msg);
            return Err(RaftStoreError::RegionNotFound(region_id));
        }

        Ok(())
    }

    fn casual_send(&self, region_id: u64, msg: CasualMessage<RocksEngine>) -> RaftStoreResult<()> {
        self.router
            .send(region_id, PeerMsg::CasualMessage(msg))
            .map_err(|e| handle_error(region_id, e))
    }

    fn broadcast_unreachable(&self, store_id: u64) {
        let _ = self
            .router
            .send_control(StoreMsg::StoreUnreachable { store_id });
    }
}
