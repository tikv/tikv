// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::{ptr, usize};

use crate::mailbox::BasicMailbox;

// The FSM is notified.
const NOTIFYSTATE_NOTIFIED: usize = 0;
// The FSM is idle.
const NOTIFYSTATE_IDLE: usize = 1;
// The FSM is expected to be dropped.
const NOTIFYSTATE_DROP: usize = 2;

/// `FsmScheduler` schedules `Fsm` for later handles.
pub trait FsmScheduler {
    type Fsm: Fsm;

    /// Schedule a Fsm for later handles.
    fn schedule(&self, fsm: Box<Self::Fsm>);
    /// Shutdown the scheduler, which indicates that resources like
    /// background thread pool should be released.
    fn shutdown(&self);
}

/// A Fsm is a finite state machine. It should be able to be notified for
/// updating internal state according to incoming messages.
pub trait Fsm {
    type Message: Send;

    fn is_stopped(&self) -> bool;

    /// Set a mailbox to Fsm, which should be used to send message to itself.
    fn set_mailbox(&mut self, _mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
    }
    /// Take the mailbox from Fsm. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        None
    }
}

/// FsmOwner is used when handling active `Fsm`.
pub trait FsmOwner {
    type Fsm: Fsm;
    /// Convert a `Fsm` to a `FsmOwner`. `fsm` is pinned in batch-system actually.
    fn from_pinned_fsm(fsm: Box<Self::Fsm>) -> Box<Self>;
    /// Convert a `FsmOwner` to a `Fsm`. `owner` is pinned in batch-system actually.
    fn into_pinned_fsm(owner: Box<Self>) -> Box<Self::Fsm>;
    fn fsm(&self) -> &Self::Fsm;
    fn fsm_mut(&mut self) -> &mut Self::Fsm;
}

pub struct FsmState<N> {
    status: AtomicUsize,
    data: AtomicPtr<N>,
}

impl<N: Fsm> FsmState<N> {
    fn fsm_into_raw(mut fsm: Box<N>) -> *mut N {
        let ptr = &mut *fsm.as_mut() as *mut N;
        std::mem::forget(fsm);
        ptr
    }

    pub fn new(data: Box<N>) -> FsmState<N> {
        FsmState {
            status: AtomicUsize::new(NOTIFYSTATE_IDLE),
            data: AtomicPtr::new(Self::fsm_into_raw(data)),
        }
    }

    /// Take the fsm if it's IDLE.
    pub fn take_fsm(&self) -> Option<Box<N>> {
        let previous_state =
            self.status
                .compare_and_swap(NOTIFYSTATE_IDLE, NOTIFYSTATE_NOTIFIED, Ordering::AcqRel);
        if previous_state != NOTIFYSTATE_IDLE {
            return None;
        }

        let p = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            Some(unsafe { Box::from_raw(p) })
        } else {
            panic!("inconsistent status and data, something should be wrong.");
        }
    }

    /// Notify fsm via a `FsmScheduler`.
    #[inline]
    pub fn notify<S: FsmScheduler<Fsm = N>>(
        &self,
        scheduler: &S,
        mailbox: Cow<'_, BasicMailbox<N>>,
    ) {
        match self.take_fsm() {
            None => {}
            Some(mut n) => {
                n.set_mailbox(mailbox);
                scheduler.schedule(n);
            }
        }
    }

    /// Put the owner back to the state.
    ///
    /// It's not required that all messages should be consumed before
    /// releasing a fsm. However, a fsm is guaranteed to be notified only
    /// when new messages arrives after it's released.
    #[inline]
    pub fn release(&self, fsm: Box<N>) {
        let previous = self.data.swap(Self::fsm_into_raw(fsm), Ordering::AcqRel);
        let mut previous_status = NOTIFYSTATE_NOTIFIED;
        if previous.is_null() {
            previous_status = self.status.compare_and_swap(
                NOTIFYSTATE_NOTIFIED,
                NOTIFYSTATE_IDLE,
                Ordering::AcqRel,
            );
            match previous_status {
                NOTIFYSTATE_NOTIFIED => return,
                NOTIFYSTATE_DROP => {
                    let ptr = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
                    unsafe { Box::from_raw(ptr) };
                    return;
                }
                _ => {}
            }
        }
        panic!("invalid release state: {:?} {}", previous, previous_status);
    }
}
