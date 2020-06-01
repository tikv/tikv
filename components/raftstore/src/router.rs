// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::{SendError, TrySendError};
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::RaftMessage;

use crate::store::fsm::RaftRouter;
use crate::store::{
    Callback, CasualMessage, LocalReader, PeerMsg, RaftCommand, SignificantMsg, StoreMsg,
};
use crate::{DiscardReason, Error as RaftStoreError, Result as RaftStoreResult};
use engine_traits::{KvEngine, Snapshot};
use raft::SnapshotStatus;
use std::cell::RefCell;
use tikv_util::threadpool::ThreadReadId;

/// Routes messages to the raftstore.
pub trait RaftStoreRouter<S>: Send + Clone
where
    S: Snapshot,
{
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()>;

    /// Sends RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback<S>) -> RaftStoreResult<()>;

    /// Sends Snapshot to local store.
    fn read(
        &self,
        _read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) -> RaftStoreResult<()> {
        self.send_command(req, cb)
    }

    fn release_snapshot_cache(&self) {}

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

    fn casual_send(&self, region_id: u64, msg: CasualMessage<S>) -> RaftStoreResult<()>;
}

#[derive(Clone)]
pub struct RaftStoreBlackHole;

impl<S> RaftStoreRouter<S> for RaftStoreBlackHole
where
    S: Snapshot,
{
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
        Ok(())
    }

    /// Sends RaftCmdRequest to local store.
    fn send_command(&self, _: RaftCmdRequest, _: Callback<S>) -> RaftStoreResult<()> {
        Ok(())
    }

    /// Sends a significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, _: u64, _: SignificantMsg) -> RaftStoreResult<()> {
        Ok(())
    }

    fn broadcast_unreachable(&self, _: u64) {}

    fn casual_send(&self, _: u64, _: CasualMessage<S>) -> RaftStoreResult<()> {
        Ok(())
    }
}

/// A router that routes messages to the raftstore
pub struct ServerRaftStoreRouter<E>
where
    E: KvEngine,
{
    router: RaftRouter<E::Snapshot>,
    local_reader: LocalReader<RaftRouter<E::Snapshot>, E>,
}

impl<E> Clone for ServerRaftStoreRouter<E>
where
    E: KvEngine,
{
    fn clone(&self) -> Self {
        ServerRaftStoreRouter {
            router: self.router.clone(),
            local_reader: self.local_reader.clone(),
        }
    }
}

impl<E> ServerRaftStoreRouter<E>
where
    E: KvEngine,
{
    /// Creates a new router.
    pub fn new(
        router: RaftRouter<E::Snapshot>,
        reader: LocalReader<RaftRouter<E::Snapshot>, E>,
    ) -> ServerRaftStoreRouter<E> {
        let local_reader = RefCell::new(reader);
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
pub fn handle_send_error<T>(region_id: u64, e: TrySendError<T>) -> RaftStoreError {
    match e {
        TrySendError::Full(_) => RaftStoreError::Transport(DiscardReason::Full),
        TrySendError::Disconnected(_) => RaftStoreError::RegionNotFound(region_id),
    }
}

impl<E> RaftStoreRouter<E::Snapshot> for ServerRaftStoreRouter<E>
where
    E: KvEngine,
{
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let region_id = msg.get_region_id();
        self.router
            .send_raft_message(msg)
            .map_err(|e| handle_send_error(region_id, e))
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback<E::Snapshot>) -> RaftStoreResult<()> {
        let cmd = RaftCommand::new(req, cb);
        let region_id = cmd.request.get_header().get_region_id();
        self.router
            .send_raft_command(cmd)
            .map_err(|e| handle_send_error(region_id, e))
    }

    fn read(
        &self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) -> RaftStoreResult<()> {
        let cmd = RaftCommand::new(req, cb);
        let mut local_reader = self.local_reader.borrow_mut();
        local_reader.read(read_id, cmd);
        Ok(())
    }

    fn release_snapshot_cache(&self) {
        let mut local_reader = self.local_reader.borrow_mut();
        local_reader.release_snapshot_cache();
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

    fn casual_send(&self, region_id: u64, msg: CasualMessage<E::Snapshot>) -> RaftStoreResult<()> {
        self.router
            .send(region_id, PeerMsg::CasualMessage(msg))
            .map_err(|e| handle_send_error(region_id, e))
    }

    fn broadcast_unreachable(&self, store_id: u64) {
        let _ = self
            .router
            .send_control(StoreMsg::StoreUnreachable { store_id });
    }
}
