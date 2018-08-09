// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::raft_serverpb::RaftMessage;
use raftstore::store::metrics::*;
use raftstore::store::worker::ApplyTaskRes;
use raftstore::store::Transport;
use raftstore::store::{AllMsg, Msg, SignificantMsg};
use std::collections::BTreeMap;
use std::result;
use std::sync::mpsc::{SendError, TrySendError};
use std::sync::{Arc, Mutex};
use std::u64;
use util::collections::HashMap;
use util::mpsc::LooseBoundedSender;
use util::transport::{NotifyError, Sender};

#[derive(Default)]
pub struct RangeState {
    pub region_ranges: BTreeMap<Vec<u8>, u64>,
    pub region_peers: HashMap<u64, Region>,
    pub pending_cross_snap: HashMap<u64, metapb::RegionEpoch>,
    pub pending_snapshot_regions: Vec<Region>,
}

pub type InternalMailboxes = Arc<Mutex<HashMap<u64, LooseBoundedSender<AllMsg>>>>;

pub struct InternalTransport {
    mailboxes: InternalMailboxes,
}

impl InternalTransport {
    pub fn new(mailboxes: InternalMailboxes) -> InternalTransport {
        InternalTransport { mailboxes }
    }

    pub fn mailboxes(&self) -> &InternalMailboxes {
        &self.mailboxes
    }

    #[inline]
    fn try_send<F, E, EF>(&self, region_id: u64, msg: Msg, f: F, e: EF) -> result::Result<(), E>
    where
        F: FnOnce(&mut LooseBoundedSender<AllMsg>, AllMsg) -> result::Result<(), E>,
        EF: FnOnce(AllMsg) -> E,
    {
        // TODO: handle destroy for cache.
        let mut mailboxes = self.mailboxes.lock().unwrap();
        if let Some(sender) = mailboxes.get(&region_id) {
            let mut sender = sender.to_owned();
            let res = f(&mut sender, AllMsg::Msg(msg));
            return res;
        }

        if let Msg::RaftMessage(_) = msg {
            f(mailboxes.get_mut(&0).unwrap(), AllMsg::Msg(msg))
        } else {
            Err(e(AllMsg::Msg(msg)))
        }
    }

    pub fn send(&self, region_id: u64, msg: Msg) -> result::Result<(), TrySendError<Msg>> {
        self.try_send(
            region_id,
            msg,
            LooseBoundedSender::<AllMsg>::try_send,
            TrySendError::Disconnected,
        ).map_err(|e| match e {
            TrySendError::Full(AllMsg::Msg(msg)) => TrySendError::Full(msg),
            TrySendError::Disconnected(AllMsg::Msg(msg)) => TrySendError::Disconnected(msg),
            _ => unreachable!(),
        })
    }

    pub fn force_send(&self, region_id: u64, msg: Msg) -> result::Result<(), SendError<Msg>> {
        self.try_send(
            region_id,
            msg,
            LooseBoundedSender::<AllMsg>::force_send,
            SendError,
        ).map_err(|e| match e {
            SendError(AllMsg::Msg(msg)) => SendError(msg),
            _ => unreachable!(),
        })
    }

    pub fn significant_send(
        &self,
        region_id: u64,
        msg: SignificantMsg,
    ) -> result::Result<(), SendError<SignificantMsg>> {
        // TODO: handle destroy for cache.
        let mailboxes = self.mailboxes.lock().unwrap();
        if let Some(sender) = mailboxes.get(&region_id) {
            let mut sender = sender.to_owned();
            let res = sender
                .force_send(AllMsg::SignificantMsg(msg))
                .map_err(|e| match e {
                    SendError(AllMsg::SignificantMsg(msg)) => SendError(msg),
                    _ => unreachable!(),
                });
            return res;
        }

        Err(SendError(msg))
    }

    pub fn send_apply(
        &self,
        region_id: u64,
        msg: ApplyTaskRes,
    ) -> result::Result<(), SendError<ApplyTaskRes>> {
        // TODO: handle destroy for cache.
        let mailboxes = self.mailboxes.lock().unwrap();
        if let Some(sender) = mailboxes.get(&region_id) {
            let mut sender = sender.to_owned();
            let res = sender
                .force_send(AllMsg::ApplyRes(msg))
                .map_err(|e| match e {
                    SendError(AllMsg::ApplyRes(msg)) => SendError(msg),
                    _ => unreachable!(),
                });
            return res;
        }

        Err(SendError(msg))
    }

    pub fn stop(&self) {
        // TODO: support join.
        let _ = self.force_send(0, Msg::Quit);
    }
}

impl Sender<Msg> for InternalTransport {
    #[inline]
    fn send(&self, m: Msg) -> result::Result<(), NotifyError<Msg>> {
        // This trait is only used for store channel, hence use 0 for region id.
        match InternalTransport::send(self, 0, m) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(t)) => Err(NotifyError::Full(t)),
            Err(TrySendError::Disconnected(t)) => Err(NotifyError::Closed(Some(t))),
        }
    }
}

impl Clone for InternalTransport {
    #[inline]
    fn clone(&self) -> InternalTransport {
        InternalTransport {
            mailboxes: self.mailboxes.clone(),
        }
    }
}

pub fn handle_stale_msg<R: Transport>(
    trans: &R,
    msg: &RaftMessage,
    cur_epoch: &RegionEpoch,
    need_gc: bool,
    target_region: Option<Region>,
) {
    let region_id = msg.get_region_id();
    let from_peer = msg.get_from_peer();
    let to_peer = msg.get_to_peer();
    let msg_type = msg.get_message().get_msg_type();

    if !need_gc {
        info!(
            "[region {}] raft message {:?} is stale, current {:?}, ignore it",
            region_id, msg_type, cur_epoch
        );
        STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
            .with_label_values(&["stale_msg"])
            .inc_by(1);
        return;
    }

    info!(
        "[region {}] raft message {:?} is stale, current {:?}, tell to gc",
        region_id, msg_type, cur_epoch
    );

    let mut gc_msg = RaftMessage::new();
    gc_msg.set_region_id(region_id);
    gc_msg.set_from_peer(to_peer.clone());
    gc_msg.set_to_peer(from_peer.clone());
    gc_msg.set_region_epoch(cur_epoch.clone());
    if let Some(r) = target_region {
        gc_msg.set_merge_target(r);
    } else {
        gc_msg.set_is_tombstone(true);
    }
    if let Err(e) = trans.send(gc_msg) {
        error!("[region {}] send gc message failed {:?}", region_id, e);
    }
}
