// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::msg::{CasualMessage, RaftCommand, StoreMsg};
use crate::errors::{DiscardReason, Error, Result};
use crossbeam::TrySendError;
use kvproto::raft_serverpb::RaftMessage;
use std::sync::mpsc;

/// Transports messages between different Raft peers.
pub trait Transport: Send + Clone {
    fn send(&mut self, msg: RaftMessage) -> Result<()>;

    fn flush(&mut self);
}

/// Routes message to target region.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait CasualRouter {
    fn send(&self, region_id: u64, msg: CasualMessage) -> Result<()>;
}

/// Routes proposal to target region.
pub trait ProposalRouter {
    fn send(&self, cmd: RaftCommand) -> std::result::Result<(), TrySendError<RaftCommand>>;
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter {
    fn send(&self, msg: StoreMsg) -> Result<()>;
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
    fn send(&self, cmd: RaftCommand) -> std::result::Result<(), TrySendError<RaftCommand>> {
        match self.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(cmd)) => Err(TrySendError::Disconnected(cmd)),
            Err(mpsc::TrySendError::Full(cmd)) => Err(TrySendError::Full(cmd)),
        }
    }
}

impl StoreRouter for mpsc::Sender<StoreMsg> {
    fn send(&self, msg: StoreMsg) -> Result<()> {
        match self.send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(_)) => Err(Error::Transport(DiscardReason::Disconnected)),
        }
    }
}
