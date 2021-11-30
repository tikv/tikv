// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::{Callback, CasualMessage, PeerMsg, RaftCommand, StoreMsg};
use crate::{DiscardReason, Error, RaftRouter, Result};
use crossbeam::channel::SendError;
use dyn_clone::DynClone;
use engine_traits::{KvEngine, RaftEngine, Snapshot};
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
    fn send(&self, region_id: u64, msg: CasualMessage) -> Result<()>;
}

/// Routes proposal to target region.
pub trait ProposalRouter {
    fn send(&self, cmd: RaftCommand) -> Result<()>;
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter: Send {
    fn send(&self, msg: StoreMsg);
}

impl CasualRouter for RaftRouter {
    #[inline]
    fn send(&self, region_id: u64, msg: CasualMessage) -> Result<()> {
        if let Err(e) = self.send(
            region_id,
            PeerMsg::CasualMessage(msg),
        ) {
            return Err(Error::RegionNotFound(region_id));
        }
        Ok(())
    }
}

impl ProposalRouter for RaftRouter {
    #[inline]
    fn send(&self, cmd: RaftCommand) -> Result<()> {
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
    fn send(&self, region_id: u64, msg: CasualMessage) -> Result<()> {
        match self.try_send((region_id, msg)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
        }
    }
}

impl ProposalRouter for mpsc::SyncSender<RaftCommand> {
    fn send(&self, cmd: RaftCommand) -> Result<()> {
        match self.send(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(_)) => Err(Error::Transport(DiscardReason::Disconnected)),
        }
    }
}

impl StoreRouter for mpsc::Sender<StoreMsg> {
    fn send(&self, msg: StoreMsg) {
        self.send(msg).unwrap()
    }
}
