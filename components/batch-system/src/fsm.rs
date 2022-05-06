// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    ptr,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
    usize,
};

use crate::mailbox::BasicMailbox;

// The FSM is notified.
const NOTIFYSTATE_NOTIFIED: usize = 0;
// The FSM is idle.
const NOTIFYSTATE_IDLE: usize = 1;
// The FSM is expected to be dropped.
const NOTIFYSTATE_DROP: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Priority {
    Low,
    Normal,
}

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

    fn get_priority(&self) -> Priority {
        Priority::Normal
    }
}

pub struct FsmState<N> {
    status: AtomicUsize,
    data: AtomicPtr<N>,
    state_cnt: Arc<AtomicUsize>,
}

impl<N: Fsm> FsmState<N> {
    pub fn new(data: Box<N>, state_cnt: Arc<AtomicUsize>) -> FsmState<N> {
        state_cnt.fetch_add(1, Ordering::Relaxed);
        FsmState {
            status: AtomicUsize::new(NOTIFYSTATE_IDLE),
            data: AtomicPtr::new(Box::into_raw(data)),
            state_cnt,
        }
    }

    /// Take the fsm if it's IDLE.
    pub fn take_fsm(&self) -> Option<Box<N>> {
        let res = self.status.compare_exchange(
            NOTIFYSTATE_IDLE,
            NOTIFYSTATE_NOTIFIED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if res.is_err() {
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
        let previous = self.data.swap(Box::into_raw(fsm), Ordering::AcqRel);
        let mut previous_status = NOTIFYSTATE_NOTIFIED;
        if previous.is_null() {
            let res = self.status.compare_exchange(
                NOTIFYSTATE_NOTIFIED,
                NOTIFYSTATE_IDLE,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            previous_status = match res {
                Ok(_) => return,
                Err(NOTIFYSTATE_DROP) => {
                    let ptr = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
                    unsafe { Box::from_raw(ptr) };
                    return;
                }
                Err(s) => s,
            };
        }
        panic!("invalid release state: {:?} {}", previous, previous_status);
    }

    /// Clear the fsm.
    #[inline]
    pub fn clear(&self) {
        match self.status.swap(NOTIFYSTATE_DROP, Ordering::AcqRel) {
            NOTIFYSTATE_NOTIFIED | NOTIFYSTATE_DROP => return,
            _ => {}
        }

        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                Box::from_raw(ptr);
            }
        }
    }
}

impl<N> Drop for FsmState<N> {
    fn drop(&mut self) {
        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe { Box::from_raw(ptr) };
        }
        self.state_cnt.fetch_sub(1, Ordering::Relaxed);
    }
}
