// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    borrow::Cow,
    sync::{atomic::AtomicUsize, Arc},
};

use crossbeam::channel::{SendError, TrySendError};
use tikv_util::{info, mpsc};

use crate::fsm::{Fsm, FsmScheduler, FsmState};

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

    // The global_sender is used to send requests to the parallel_pool loop.
    global_sender: Option<crossbeam::channel::Sender<Owner::Message>>,
}

impl<Owner: Fsm> BasicMailbox<Owner> {
    #[inline]
    pub fn new(
        sender: mpsc::LooseBoundedSender<Owner::Message>,
        fsm: Box<Owner>,
        state_cnt: Arc<AtomicUsize>,
    ) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender,
            state: Arc::new(FsmState::new(fsm, state_cnt)),
            global_sender: None,
        }
    }

    // Create a mailbox which sends apply requests to a global mpmc channel and these requests
    // would be processed concurrently by all the apply workers without considering region or apply
    // fsm peer. This is just for demo test.
    pub fn new_global(
        sender: mpsc::LooseBoundedSender<Owner::Message>,
        fsm: Box<Owner>,
        state_cnt: Arc<AtomicUsize>,
        global_sender: crossbeam::channel::Sender<Owner::Message>,
    ) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender,
            state: Arc::new(FsmState::new(fsm, state_cnt)),
            global_sender: Some(global_sender),
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
        if self.global_sender.is_none() {
            self.sender.force_send(msg)?;
            self.state.notify(scheduler, Cow::Borrowed(self));
        } else {
            self.global_sender.as_ref().unwrap().send(msg)?;
        }
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
        if self.global_sender.is_none() {
            self.sender.try_send(msg)?;
            self.state.notify(scheduler, Cow::Borrowed(self));
        } else {
            self.global_sender.as_ref().unwrap().try_send(msg)?;
        }
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
            global_sender: self.global_sender.clone(),
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
