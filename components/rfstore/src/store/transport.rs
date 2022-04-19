// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::{CasualMessage, PeerMsg, RaftCommand, StoreMsg};
use crate::{RaftRouter, Result};
use dyn_clone::DynClone;
use kvproto::raft_serverpb::RaftMessage;
use std::sync::mpsc;

/// Transports messages between different Raft peers.
pub trait Transport: Send + DynClone {
    fn send(&mut self, msg: RaftMessage) -> Result<()>;

    fn need_flush(&self) -> bool;

    fn flush(&mut self);
}

dyn_clone::clone_trait_object!(Transport);

/// Routes message to target region.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait CasualRouter: Send {
    fn send(&self, region_id: u64, msg: CasualMessage);
}

/// Routes proposal to target region.
pub trait ProposalRouter {
    fn send(&self, cmd: RaftCommand);
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter: Send {
    fn send(&self, msg: StoreMsg);
}

impl CasualRouter for RaftRouter {
    #[inline]
    fn send(&self, region_id: u64, msg: CasualMessage) {
        self.send(region_id, PeerMsg::CasualMessage(msg))
    }
}

impl ProposalRouter for RaftRouter {
    #[inline]
    fn send(&self, cmd: RaftCommand) {
        let region_id = cmd.request.get_header().get_region_id();
        let msg = PeerMsg::RaftCommand(cmd);
        self.send(region_id, msg)
    }
}

impl StoreRouter for RaftRouter {
    #[inline]
    fn send(&self, msg: StoreMsg) {
        self.send_store(msg)
    }
}

impl CasualRouter for mpsc::SyncSender<(u64, CasualMessage)> {
    fn send(&self, region_id: u64, msg: CasualMessage) {
        self.send((region_id, msg)).unwrap();
    }
}

impl ProposalRouter for mpsc::SyncSender<RaftCommand> {
    fn send(&self, cmd: RaftCommand) {
        self.send(cmd).unwrap();
    }
}

impl StoreRouter for mpsc::Sender<StoreMsg> {
    fn send(&self, msg: StoreMsg) {
        self.send(msg).unwrap()
    }
}
