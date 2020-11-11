// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::{CasualMessage, PeerMsg, RaftCommand, RaftRouter, StoreMsg};
use crate::{DiscardReason, Error, Result};
use crossbeam::TrySendError;
use engine_traits::{KvEngine, RaftEngine, Snapshot};
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
pub trait CasualRouter<EK>: Send
where
    EK: KvEngine,
{
    fn send(&self, region_id: u64, msg: CasualMessage<EK>) -> Result<()>;
}

/// Routes proposal to target region.
pub trait ProposalRouter<S>
where
    S: Snapshot,
{
    fn send(&self, cmd: RaftCommand<S>) -> std::result::Result<(), TrySendError<RaftCommand<S>>>;
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter<EK>: Send
where
    EK: KvEngine,
{
    fn send(&self, msg: StoreMsg<EK>) -> Result<()>;
}

impl<EK, ER> CasualRouter<EK> for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    #[inline]
    fn send(&self, region_id: u64, msg: CasualMessage<EK>) -> Result<()> {
        match self.router.send(region_id, PeerMsg::CasualMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySendError::Disconnected(_)) => Err(Error::RegionNotFound(region_id)),
        }
    }
}

impl<EK, ER> ProposalRouter<EK::Snapshot> for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    #[inline]
    fn send(
        &self,
        cmd: RaftCommand<EK::Snapshot>,
    ) -> std::result::Result<(), TrySendError<RaftCommand<EK::Snapshot>>> {
        self.send_raft_command(cmd)
    }
}

impl<EK, ER> StoreRouter<EK> for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    #[inline]
    fn send(&self, msg: StoreMsg<EK>) -> Result<()> {
        match self.send_control(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySendError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
        }
    }
}

impl<EK> CasualRouter<EK> for mpsc::SyncSender<(u64, CasualMessage<EK>)>
where
    EK: KvEngine,
{
    fn send(&self, region_id: u64, msg: CasualMessage<EK>) -> Result<()> {
        match self.try_send((region_id, msg)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
        }
    }
}

impl<S: Snapshot> ProposalRouter<S> for mpsc::SyncSender<RaftCommand<S>> {
    fn send(&self, cmd: RaftCommand<S>) -> std::result::Result<(), TrySendError<RaftCommand<S>>> {
        match self.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(cmd)) => Err(TrySendError::Disconnected(cmd)),
            Err(mpsc::TrySendError::Full(cmd)) => Err(TrySendError::Full(cmd)),
        }
    }
}

impl<EK> StoreRouter<EK> for mpsc::Sender<StoreMsg<EK>>
where
    EK: KvEngine,
{
    fn send(&self, msg: StoreMsg<EK>) -> Result<()> {
        match self.send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(_)) => Err(Error::Transport(DiscardReason::Disconnected)),
        }
    }
}
