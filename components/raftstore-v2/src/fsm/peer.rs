// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;

use batch_system::{BasicMailbox, Fsm};
use crossbeam::channel::TryRecvError;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::metapb;
use raftstore::store::Config;
use slog::{info, Logger};
use tikv_util::mpsc::{self, LooseBoundedSender, Receiver, Sender};

use crate::{batch::StoreContext, raft::Peer, PeerMsg, Result};

pub type SenderFsmPair<EK, ER> = (LooseBoundedSender<PeerMsg<EK>>, Box<PeerFsm<EK, ER>>);

pub struct PeerFsm<EK: KvEngine, ER: RaftEngine> {
    peer: Peer<EK, ER>,
    logger: Logger,
    mailbox: Option<BasicMailbox<PeerFsm<EK, ER>>>,
    receiver: Receiver<PeerMsg<EK>>,
    is_stopped: bool,
}

impl<EK: KvEngine, ER: RaftEngine> PeerFsm<EK, ER> {
    pub fn new(cfg: &Config, peer: Peer<EK, ER>) -> Result<SenderFsmPair<EK, ER>> {
        let logger = peer.logger().clone();
        info!(logger, "create peer");
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        let fsm = Box::new(PeerFsm {
            logger,
            peer,
            mailbox: None,
            receiver: rx,
            is_stopped: false,
        });
        Ok((tx, fsm))
    }

    #[inline]
    pub fn peer(&self) -> &Peer<EK, ER> {
        &self.peer
    }

    #[inline]
    pub fn logger(&self) -> &Logger {
        self.peer.logger()
    }

    /// Fetches messages to `peer_msg_buf`. It will stop when the buffer
    /// capacity is reached or there is no more pending messages.
    ///
    /// Returns how many messages are fetched.
    pub fn recv(&mut self, peer_msg_buf: &mut Vec<PeerMsg<EK>>) -> usize {
        let l = peer_msg_buf.len();
        for i in l..peer_msg_buf.capacity() {
            match self.receiver.try_recv() {
                Ok(msg) => peer_msg_buf.push(msg),
                Err(e) => {
                    if let TryRecvError::Disconnected = e {
                        self.is_stopped = true;
                    }
                    return i - l;
                }
            }
        }
        peer_msg_buf.capacity() - l
    }
}

impl<EK: KvEngine, ER: RaftEngine> Fsm for PeerFsm<EK, ER> {
    type Message = PeerMsg<EK>;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.is_stopped
    }

    /// Set a mailbox to Fsm, which should be used to send message to itself.
    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    /// Take the mailbox from Fsm. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        self.mailbox.take()
    }
}

pub struct PeerFsmDelegate<'a, EK: KvEngine, ER: RaftEngine, T> {
    fsm: &'a mut PeerFsm<EK, ER>,
    store_ctx: &'a mut StoreContext<T>,
}

impl<'a, EK: KvEngine, ER: RaftEngine, T> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn new(fsm: &'a mut PeerFsm<EK, ER>, store_ctx: &'a mut StoreContext<T>) -> Self {
        Self { fsm, store_ctx }
    }

    pub fn handle_msgs(&self, peer_msgs_buf: &mut Vec<PeerMsg<EK>>) {
        for msg in peer_msgs_buf.drain(..) {
            // TODO: handle the messages.
        }
    }
}
