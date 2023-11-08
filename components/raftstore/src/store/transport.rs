// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::{mpsc, Mutex};

use crossbeam::channel::{SendError, TrySendError};
use engine_traits::{KvEngine, RaftEngine, Snapshot};
use kvproto::raft_serverpb::RaftMessage;
use tikv_util::{error, warn};

use super::{AsyncReadNotifier, FetchedLogs, GenSnapRes};
use crate::{
    store::{CasualMessage, PeerMsg, RaftCommand, RaftRouter, SignificantMsg, StoreMsg},
    DiscardReason, Error, Result,
};

/// Transports messages between different Raft peers.
pub trait Transport: Send + Clone {
    fn send(&mut self, msg: RaftMessage) -> Result<()>;

    // empty list means all stores are allowed to send.
    fn set_store_allowlist(&mut self, stores: Vec<u64>);

    fn need_flush(&self) -> bool;

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

/// Routes message to target region.
///
/// Messages aret guaranteed to be delivered by this trait.
pub trait SignificantRouter<EK>: Send
where
    EK: KvEngine,
{
    fn significant_send(&self, region_id: u64, msg: SignificantMsg<EK::Snapshot>) -> Result<()>;
}

impl<'a, T: SignificantRouter<EK>, EK: KvEngine> SignificantRouter<EK> for &'a Mutex<T> {
    #[inline]
    fn significant_send(&self, region_id: u64, msg: SignificantMsg<EK::Snapshot>) -> Result<()> {
        Mutex::lock(self).unwrap().significant_send(region_id, msg)
    }
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

impl<'a, EK: KvEngine, T: CasualRouter<EK>> CasualRouter<EK> for &'a Mutex<T> {
    #[inline]
    fn send(&self, region_id: u64, msg: CasualMessage<EK>) -> Result<()> {
        CasualRouter::send(&*Mutex::lock(self).unwrap(), region_id, msg)
    }
}

impl<EK, ER> SignificantRouter<EK> for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    #[inline]
    fn significant_send(&self, region_id: u64, msg: SignificantMsg<EK::Snapshot>) -> Result<()> {
        if let Err(SendError(msg)) = self
            .router
            .force_send(region_id, PeerMsg::SignificantMsg(msg))
        {
            // TODO: panic here once we can detect system is shutting down reliably.

            // Avoid printing error log if it's not a severe problem failing to send it.
            if msg.is_send_failure_ignorable() {
                warn!("failed to send significant msg"; "msg" => ?msg);
            } else {
                error!("failed to send significant msg"; "msg" => ?msg);
            }
            return Err(Error::RegionNotFound(region_id));
        }

        Ok(())
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

impl<EK: KvEngine, ER: RaftEngine> AsyncReadNotifier for RaftRouter<EK, ER> {
    #[inline]
    fn notify_logs_fetched(&self, region_id: u64, fetched: FetchedLogs) {
        // Ignore region not found as it may be removed.
        let _ = self.significant_send(region_id, SignificantMsg::RaftlogFetched(fetched));
    }

    #[inline]
    fn notify_snapshot_generated(&self, _region_id: u64, _snapshot: GenSnapRes) {
        unreachable!()
    }
}
