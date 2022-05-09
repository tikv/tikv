// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;

use kvproto::{raft_cmdpb::RaftCmdRequest, raft_serverpb::RaftMessage};
use tikv_util::{deadline::Deadline, mpsc::Sender, time::ThreadReadId};

use crate::{
    store::{
        Callback, CasualMessage, CasualRouter, LocalReader, PeerMsg, ProposalRouter, RaftCommand,
        SignificantMsg, StoreMsg, StoreRouter,
    },
    Result as RaftStoreResult,
};

/// Routes messages to the raftstore.
pub trait RaftStoreRouter: StoreRouter + ProposalRouter + CasualRouter + Send + Clone {
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage);

    /// Sends a significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, region_id: u64, msg: SignificantMsg);

    /// Send a casual message to the given region.
    fn send_casual_msg(&self, region_id: u64, msg: CasualMessage) {
        <Self as CasualRouter>::send(self, region_id, msg)
    }

    /// Send a store message to the backend raft batch system.
    fn send_store_msg(&self, msg: StoreMsg) {
        <Self as StoreRouter>::send(self, msg)
    }

    /// Sends RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) {
        send_command_impl(self, req, cb, None)
    }

    fn send_command_with_deadline(&self, req: RaftCmdRequest, cb: Callback, deadline: Deadline) {
        send_command_impl(self, req, cb, Some(deadline))
    }

    /// Reports the peer being unreachable to the Region.
    fn report_unreachable(&self, region_id: u64, store_id: u64) {
        let msg = SignificantMsg::StoreUnreachable { store_id };
        self.significant_send(region_id, msg)
    }

    /// Broadcast an `StoreUnreachable` event to all Raft groups.
    fn broadcast_unreachable(&self, store_id: u64) {
        self.send_store_msg(StoreMsg::StoreUnreachable { store_id });
    }
}

pub trait LocalReadRouter: Send + Clone {
    fn read(
        &self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback,
    ) -> RaftStoreResult<()>;
}

/// A router that routes messages to the raftstore
pub struct ServerRaftStoreRouter {
    router: RaftRouter,
    local_reader: RefCell<LocalReader>,
}

impl Clone for ServerRaftStoreRouter {
    fn clone(&self) -> Self {
        ServerRaftStoreRouter {
            router: self.router.clone(),
            local_reader: self.local_reader.clone(),
        }
    }
}

impl ServerRaftStoreRouter {
    /// Creates a new router.
    pub fn new(router: RaftRouter, reader: LocalReader) -> ServerRaftStoreRouter {
        let local_reader = RefCell::new(reader);
        ServerRaftStoreRouter {
            router,
            local_reader,
        }
    }
}

impl StoreRouter for ServerRaftStoreRouter {
    fn send(&self, msg: StoreMsg) {
        StoreRouter::send(&self.router, msg)
    }
}

impl ProposalRouter for ServerRaftStoreRouter {
    fn send(&self, cmd: RaftCommand) {
        ProposalRouter::send(&self.router, cmd)
    }
}

impl CasualRouter for ServerRaftStoreRouter {
    fn send(&self, region_id: u64, msg: CasualMessage) {
        CasualRouter::send(&self.router, region_id, msg)
    }
}

impl RaftStoreRouter for ServerRaftStoreRouter {
    fn send_raft_msg(&self, msg: RaftMessage) {
        RaftStoreRouter::send_raft_msg(&self.router, msg)
    }

    /// Sends a significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, region_id: u64, msg: SignificantMsg) {
        RaftStoreRouter::significant_send(&self.router, region_id, msg)
    }
}

impl LocalReadRouter for ServerRaftStoreRouter {
    fn read(
        &self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback,
    ) -> RaftStoreResult<()> {
        let mut local_reader = self.local_reader.borrow_mut();
        local_reader.read(read_id, req, cb);
        Ok(())
    }
}

#[derive(Clone)]
pub struct RaftStoreBlackHole;

impl CasualRouter for RaftStoreBlackHole {
    fn send(&self, _: u64, _: CasualMessage) {}
}

impl ProposalRouter for RaftStoreBlackHole {
    fn send(&self, _: RaftCommand) {}
}

impl StoreRouter for RaftStoreBlackHole {
    fn send(&self, _: StoreMsg) {}
}

impl RaftStoreRouter for RaftStoreBlackHole {
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, _: RaftMessage) {}

    /// Sends a significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, _: u64, _: SignificantMsg) {}
}

#[derive(Clone)]
pub struct RaftRouter {
    pub(crate) store_sender: Sender<StoreMsg>,
    pub(crate) peer_sender: Sender<(u64, PeerMsg)>,
}

impl RaftRouter {
    pub(crate) fn new(peer_sender: Sender<(u64, PeerMsg)>, store_sender: Sender<StoreMsg>) -> Self {
        Self {
            store_sender,
            peer_sender,
        }
    }

    pub(crate) fn send(&self, id: u64, msg: PeerMsg) {
        self.peer_sender.send((id, msg)).unwrap();
    }

    pub(crate) fn send_store(&self, msg: StoreMsg) {
        self.store_sender.send(msg).unwrap();
    }
}

impl RaftStoreRouter for RaftRouter {
    fn send_raft_msg(&self, msg: RaftMessage) {
        let region_id = msg.get_region_id();
        let raft_msg = PeerMsg::RaftMessage(msg);
        self.send(region_id, raft_msg);
    }

    fn significant_send(&self, region_id: u64, msg: SignificantMsg) {
        let msg = PeerMsg::SignificantMsg(msg);
        self.send(region_id, msg);
    }
}

fn send_command_impl(
    router: &impl ProposalRouter,
    req: RaftCmdRequest,
    cb: Callback,
    _deadline: Option<Deadline>,
) {
    let cmd = RaftCommand::new(req, cb);
    // TODO(x) handle deadline
    router.send(cmd)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_run() {
        println!("run")
    }
}
