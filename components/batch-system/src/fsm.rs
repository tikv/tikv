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

use resource_control::ResourceMetered;

use crate::mailbox::BasicMailbox;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Priority {
    Low,
    Normal,
}

/// `FsmScheduler` schedules `Fsm` for later handling.
pub trait FsmScheduler {
    type Fsm: Fsm;

    /// Schedule a Fsm for later handling.
    fn schedule(&self, fsm: Box<Self::Fsm>);

    /// Shutdown the scheduler, which indicates that resources like
    /// background thread pool should be released.
    fn shutdown(&self);

    /// Consume the resources of msg in resource controller if enabled,
    /// otherwise do nothing.
    fn consume_msg_resource(&self, msg: &<Self::Fsm as Fsm>::Message);
}

/// A `Fsm` is a finite state machine. It should be able to be notified for
/// updating internal state according to incoming messages.
pub trait Fsm: Send + 'static {
    type Message: Send + ResourceMetered;

    fn is_stopped(&self) -> bool;

    /// Set a mailbox to FSM, which should be used to send message to itself.
    fn set_mailbox(&mut self, _mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
    }

    /// Take the mailbox from FSM. Implementation should ensure there will be
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

/// A holder of FSM.
///
/// There are three possible states:
///
/// 1. NOTIFYSTATE_NOTIFIED: The FSM is taken by an external executor. `data`
///    holds a null pointer.
/// 2. NOTIFYSTATE_IDLE: No actor is using the FSM. `data` owns the FSM.
/// 3. NOTIFYSTATE_DROP: The FSM is dropped. `data` holds a null pointer.
pub struct FsmState<N> {
    status: AtomicUsize,
    data: AtomicPtr<N>,
    /// A counter shared with other `FsmState`s.
    state_cnt: Arc<AtomicUsize>,
}

impl<N: Fsm> FsmState<N> {
    const NOTIFYSTATE_NOTIFIED: usize = 0;
    const NOTIFYSTATE_IDLE: usize = 1;
    const NOTIFYSTATE_DROP: usize = 2;

    pub fn new(data: Box<N>, state_cnt: Arc<AtomicUsize>) -> FsmState<N> {
        state_cnt.fetch_add(1, Ordering::Relaxed);
        FsmState {
            status: AtomicUsize::new(Self::NOTIFYSTATE_IDLE),
            data: AtomicPtr::new(Box::into_raw(data)),
            state_cnt,
        }
    }

    /// Take the fsm if it's IDLE.
    pub fn take_fsm(&self) -> Option<Box<N>> {
        let res = self.status.compare_exchange(
            Self::NOTIFYSTATE_IDLE,
            Self::NOTIFYSTATE_NOTIFIED,
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

    /// Notifies FSM via a `FsmScheduler`.
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

    /// Releases the FSM ownership back to this state.
    ///
    /// It's not required that all messages should be consumed before
    /// releasing a FSM. However, a FSM is guaranteed to be notified only
    /// when new messages arrives after it's released.
    #[inline]
    pub fn release(&self, fsm: Box<N>) {
        let previous = self.data.swap(Box::into_raw(fsm), Ordering::AcqRel);
        let mut previous_status = Self::NOTIFYSTATE_NOTIFIED;
        if previous.is_null() {
            let res = self.status.compare_exchange(
                Self::NOTIFYSTATE_NOTIFIED,
                Self::NOTIFYSTATE_IDLE,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            previous_status = match res {
                Ok(_) => return,
                Err(Self::NOTIFYSTATE_DROP) => {
                    let ptr = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
                    unsafe {
                        let _ = Box::from_raw(ptr);
                    };
                    return;
                }
                Err(s) => s,
            };
        }
        panic!("invalid release state: {:?} {}", previous, previous_status);
    }

    /// Clears the FSM.
    #[inline]
    pub fn clear(&self) {
        match self.status.swap(Self::NOTIFYSTATE_DROP, Ordering::AcqRel) {
            Self::NOTIFYSTATE_NOTIFIED | Self::NOTIFYSTATE_DROP => return,
            _ => {}
        }

        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(ptr);
            }
        }
    }
}

impl<N> Drop for FsmState<N> {
    fn drop(&mut self) {
        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(ptr);
            };
        }
        self.state_cnt.fetch_sub(1, Ordering::Relaxed);
    }
}
