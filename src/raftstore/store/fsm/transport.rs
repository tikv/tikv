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

//! Generally peers are state machines that represent a replica of a region,
//! and store is also a special state machine that handles all requests across
//! stores. They are mixed for now, will be separated in the future.

use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::RaftMessage;
use raftstore::store::metrics::RAFTSTORE_CHANNEL_FULL;
use raftstore::store::msg::{Callback, PeerMsg, StoreMsg};
use std::cell::Cell;
use std::sync::mpsc::{SendError, TrySendError};
use std::sync::{Arc, Mutex};
use util::collections::HashMap;
use util::mpsc;
use util::Either;

pub type MailBox = mpsc::LooseBoundedSender<PeerMsg>;

pub struct Router {
    mailboxes: Arc<Mutex<HashMap<u64, MailBox>>>,
    caches: Cell<HashMap<u64, MailBox>>,
    store_box: mpsc::LooseBoundedSender<StoreMsg>,
}

impl Router {
    pub fn new(store_box: mpsc::LooseBoundedSender<StoreMsg>) -> Router {
        Router {
            mailboxes: Arc::default(),
            caches: Cell::new(HashMap::default()),
            store_box,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(region_id: u64) -> (Router, mpsc::Receiver<PeerMsg>) {
        use util::mpsc::loose_bounded;

        let (tx, _) = loose_bounded(10);
        let router = Router::new(tx);
        let (tx, rx) = loose_bounded(10);
        router.register_mailbox(region_id, tx);
        (router, rx)
    }

    pub fn send_raft_message(&self, mut msg: RaftMessage) -> Result<(), TrySendError<RaftMessage>> {
        let id = msg.get_region_id();
        match self.try_send_peer_message(id, PeerMsg::RaftMessage(msg)) {
            Either::Left(Ok(())) => return Ok(()),
            Either::Left(Err(TrySendError::Full(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Full(m))
            }
            Either::Left(Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Disconnected(m))
            }
            Either::Right(PeerMsg::RaftMessage(m)) => msg = m,
            _ => unreachable!(),
        }
        match self.send_store_message(StoreMsg::RaftMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(StoreMsg::RaftMessage(m))) => Err(TrySendError::Full(m)),
            Err(TrySendError::Disconnected(StoreMsg::RaftMessage(m))) => {
                Err(TrySendError::Disconnected(m))
            }
            _ => unreachable!(),
        }
    }

    pub fn send_cmd(
        &self,
        req: RaftCmdRequest,
        cb: Callback,
    ) -> Result<(), TrySendError<(RaftCmdRequest, Callback)>> {
        let id = req.get_header().get_region_id();
        match self.send_peer_message(id, PeerMsg::new_raft_cmd(req, cb)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(PeerMsg::RaftCmd {
                request, callback, ..
            })) => Err(TrySendError::Full((request, callback))),
            Err(TrySendError::Disconnected(PeerMsg::RaftCmd {
                request, callback, ..
            })) => Err(TrySendError::Disconnected((request, callback))),
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn send_peer_message(
        &self,
        region_id: u64,
        msg: PeerMsg,
    ) -> Result<(), TrySendError<PeerMsg>> {
        match self.try_send_peer_message(region_id, msg) {
            Either::Left(res) => res,
            Either::Right(m) => Err(TrySendError::Disconnected(m)),
        }
    }

    #[inline]
    pub fn force_send_peer_message(
        &self,
        region_id: u64,
        msg: PeerMsg,
    ) -> Result<(), SendError<PeerMsg>> {
        match self.send_peer_message(region_id, msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(m)) => {
                let caches = unsafe { &mut *self.caches.as_ptr() };
                caches[&region_id].force_send(m)
            }
            Err(TrySendError::Disconnected(m)) => Err(SendError(m)),
        }
    }

    #[inline]
    pub fn try_send_peer_message(
        &self,
        region_id: u64,
        mut msg: PeerMsg,
    ) -> Either<Result<(), TrySendError<PeerMsg>>, PeerMsg> {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut disconnected = false;
        if let Some(mailbox) = caches.get(&region_id) {
            match mailbox.try_send(msg) {
                Ok(()) => return Either::Left(Ok(())),
                Err(TrySendError::Full(m)) => {
                    RAFTSTORE_CHANNEL_FULL.inc();
                    return Either::Left(Err(TrySendError::Full(m)));
                }
                Err(TrySendError::Disconnected(m)) => {
                    disconnected = true;
                    msg = m;
                }
            };
        }

        let mailbox = 'fetch_box: {
            let mut boxes = self.mailboxes.lock().unwrap();
            if let Some(mailbox) = boxes.get_mut(&region_id) {
                break 'fetch_box mailbox.clone();
            }
            drop(boxes);
            if disconnected {
                caches.remove(&region_id);
            }
            return Either::Right(msg);
        };

        match mailbox.try_send(msg) {
            r @ Ok(()) => {
                caches.insert(region_id, mailbox);
                Either::Left(r)
            }
            r @ Err(TrySendError::Full(_)) => {
                RAFTSTORE_CHANNEL_FULL.inc();
                caches.insert(region_id, mailbox);
                Either::Left(r)
            }
            r @ Err(TrySendError::Disconnected(_)) => {
                if disconnected {
                    caches.remove(&region_id);
                }
                Either::Left(r)
            }
        }
    }

    #[inline]
    pub fn send_store_message(&self, msg: StoreMsg) -> Result<(), TrySendError<StoreMsg>> {
        match self.store_box.try_send(msg) {
            Ok(()) => Ok(()),
            r @ Err(TrySendError::Full(_)) => {
                RAFTSTORE_CHANNEL_FULL.inc();
                r
            }
            r => r,
        }
    }

    pub fn broadcast_shutdown(&self) {
        self.store_box.close();
        let mut mailboxes = self.mailboxes.lock().unwrap();
        for (_, mailbox) in mailboxes.drain() {
            mailbox.close();
        }
    }

    pub fn register_mailbox(&self, region_id: u64, mailbox: MailBox) {
        let mut mailboxes = self.mailboxes.lock().unwrap();
        mailboxes.insert(region_id, mailbox);
    }

    pub fn register_mailboxes(&self, mailboxes: impl IntoIterator<Item = (u64, MailBox)>) {
        let mut mbs = self.mailboxes.lock().unwrap();
        mbs.extend(mailboxes);
    }

    pub fn unregister_mailbox(&self, region_id: u64) -> Option<MailBox> {
        let mut mailboxes = self.mailboxes.lock().unwrap();
        mailboxes.remove(&region_id)
    }

    pub fn store_mailbox(&self) -> mpsc::LooseBoundedSender<StoreMsg> {
        self.store_box.clone()
    }
}

impl Clone for Router {
    #[inline]
    fn clone(&self) -> Router {
        Router {
            mailboxes: self.mailboxes.clone(),
            caches: Cell::default(),
            store_box: self.store_box.clone(),
        }
    }
}
