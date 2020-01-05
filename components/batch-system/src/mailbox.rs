// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::fsm::{Fsm, FsmScheduler, FsmState};
use crossbeam::channel::{SendError, TrySendError};
use std::borrow::Cow;
use std::sync::Arc;
use tikv_util::mpsc;

/// A basic mailbox.
///
/// Every mailbox should have one and only one owner, who will receive all
/// messages sent to this mailbox.
///
/// When a message is sent to a mailbox, its owner will be checked whether it's
/// idle. An idle owner will be scheduled via `FsmScheduler` immediately, which
/// will drive the fsm to poll for messages.
pub struct BasicMailbox<Owner: Fsm> {
    sender: mpsc::LooseBoundedSender<Owner::Message>,
    state: Arc<FsmState<Owner>>,
}

impl<Owner: Fsm> BasicMailbox<Owner> {
    #[inline]
    pub fn new(
        sender: mpsc::LooseBoundedSender<Owner::Message>,
        fsm: Box<Owner>,
    ) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender,
            state: Arc::new(FsmState::new(fsm)),
        }
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.sender.is_sender_connected()
    }

    pub(crate) fn release(&self, fsm: Box<Owner>) {
        self.state.release(fsm)
    }

    pub(crate) fn take_fsm(&self) -> Option<Box<Owner>> {
        self.state.take_fsm()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    /// Force sending a message despite the capacity limit on channel.
    #[inline]
    pub fn force_send<S: FsmScheduler<Fsm = Owner>>(
        &self,
        msg: Owner::Message,
        scheduler: &S,
    ) -> Result<(), SendError<Owner::Message>> {
        self.sender.force_send(msg)?;
        self.state.notify(scheduler, Cow::Borrowed(self));
        Ok(())
    }

    /// Try to send a message to the mailbox.
    ///
    /// If there are too many pending messages, function may fail.
    #[inline]
    pub fn try_send<S: FsmScheduler<Fsm = Owner>>(
        &self,
        msg: Owner::Message,
        scheduler: &S,
    ) -> Result<(), TrySendError<Owner::Message>> {
        self.sender.try_send(msg)?;
        self.state.notify(scheduler, Cow::Borrowed(self));
        Ok(())
    }

    /// Close the mailbox explicitly.
    #[inline]
    pub(crate) fn close(&self) {
        self.sender.close_sender();
        self.state.clear();
    }
}

impl<Owner: Fsm> Clone for BasicMailbox<Owner> {
    #[inline]
    fn clone(&self) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender: self.sender.clone(),
            state: self.state.clone(),
        }
    }
}

/// A more high level mailbox.
pub struct Mailbox<Owner, Scheduler>
where
    Owner: Fsm,
    Scheduler: FsmScheduler<Fsm = Owner>,
{
    mailbox: BasicMailbox<Owner>,
    scheduler: Scheduler,
}

impl<Owner, Scheduler> Mailbox<Owner, Scheduler>
where
    Owner: Fsm,
    Scheduler: FsmScheduler<Fsm = Owner>,
{
    pub fn new(mailbox: BasicMailbox<Owner>, scheduler: Scheduler) -> Mailbox<Owner, Scheduler> {
        Mailbox { mailbox, scheduler }
    }

    /// Force sending a message despite channel capacity limit.
    #[inline]
    pub fn force_send(&self, msg: Owner::Message) -> Result<(), SendError<Owner::Message>> {
        self.mailbox.force_send(msg, &self.scheduler)
    }

    /// Try to send a message.
    #[inline]
    pub fn try_send(&self, msg: Owner::Message) -> Result<(), TrySendError<Owner::Message>> {
        self.mailbox.try_send(msg, &self.scheduler)
    }
}
