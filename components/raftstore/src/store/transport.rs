// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::{CasualMessage, PeerMsg, RaftCommand, RaftRouter, StoreMsg};
use crate::{DiscardReason, Error, Result};
use crossbeam::TrySendError;
use engine_rocks::RocksEngine;
use engine_traits::KvEngine;
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
pub trait CasualRouter<E: KvEngine> {
    fn send(&self, region_id: u64, msg: CasualMessage<E>) -> Result<()>;
}

/// Routes proposal to target region.
pub trait ProposalRouter<E: KvEngine> {
    fn send(&self, cmd: RaftCommand<E>) -> std::result::Result<(), TrySendError<RaftCommand<E>>>;
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter {
    fn send(&self, msg: StoreMsg) -> Result<()>;
}

impl<E: KvEngine> CasualRouter<E> for RaftRouter<E> {
    #[inline]
    fn send(&self, region_id: u64, msg: CasualMessage<E>) -> Result<()> {
        match self.router.send(region_id, PeerMsg::CasualMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySendError::Disconnected(_)) => Err(Error::RegionNotFound(region_id)),
        }
    }
}

impl<E: KvEngine> ProposalRouter<E> for RaftRouter<E> {
    #[inline]
    fn send(&self, cmd: RaftCommand<E>) -> std::result::Result<(), TrySendError<RaftCommand<E>>> {
        self.send_raft_command(cmd)
    }
}

impl StoreRouter for RaftRouter<RocksEngine> {
    #[inline]
    fn send(&self, msg: StoreMsg) -> Result<()> {
        match self.send_control(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySendError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
        }
    }
}

impl<E: KvEngine> CasualRouter<E> for mpsc::SyncSender<(u64, CasualMessage<E>)> {
    fn send(&self, region_id: u64, msg: CasualMessage<E>) -> Result<()> {
        match self.try_send((region_id, msg)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
        }
    }
}

impl ProposalRouter<RocksEngine> for mpsc::SyncSender<RaftCommand<RocksEngine>> {
    fn send(
        &self,
        cmd: RaftCommand<RocksEngine>,
    ) -> std::result::Result<(), TrySendError<RaftCommand<RocksEngine>>> {
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
