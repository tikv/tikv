// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::UnsafeCell;
use std::fmt::{self, Pointer};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{isize, mem, ptr, usize};

/// IDLE -> NOTIFIED, when new messages are put into mailbox.
/// NOTIFIED -> POLLING, when fsm is about to be processed.
/// POLLING -> NOTIFIED, when new messages are put into mailbox during processing.
/// POLLING -> IDLE, when the fsm's mailbox are drained.
const IDLE: usize = 0;
const NOTIFIED: usize = 1;
const POLLING: usize = 2;

/// SET when the fsm is about to be destroyed.
const CLEAR_BIT: usize = 4;

/// The state are maintained in one usize:
/// ```text
/// r...rcss
/// ```
/// IDLE, NOTIFIED, POLLING are maintained in the least significant 2 bits,
/// clear bit are maintained at the 3rd bit, the rest are ref count.
const REF_OFFSET: usize = 3;
const REF_BASE: usize = 1 << REF_OFFSET;
const STATE_MASK: usize = REF_BASE - 1;
const REF_MASK: usize = usize::MAX - STATE_MASK;
/// Uses isize to leave some space before overflow.
const MAX_REF_COUNT: usize = (isize::MAX >> REF_OFFSET) as usize;

/// An Fsm is a finite state machine. It should be able to be notified for
/// updating internal state according to incoming messages.
pub trait Fsm {
    type Message: Send;
}

/// `FsmScheduler` schedules `Fsm` for later handling.
pub trait FsmScheduler: Sized {
    type Fsm: Fsm;

    /// Schedules a Fsm for later handles.
    fn schedule(&self, fsm: FsmState<Self::Fsm>);
    /// Shutdowns the scheduler, which indicates that resources like
    /// background thread pool should be released.
    fn shutdown(&self);
}

/// Inner data for `FsmState`.
struct FsmStateInner<F> {
    state: AtomicUsize,
    fsm: UnsafeCell<Option<Box<F>>>,
}

/// A shared state that maintains `Fsm` between mailbox and polling thread.
pub struct FsmState<F> {
    inner: NonNull<FsmStateInner<F>>,
}

impl<F> FsmState<F> {
    pub fn new(fsm: Box<F>) -> FsmState<F> {
        let inner = Box::new(FsmStateInner {
            state: AtomicUsize::new(REF_BASE | IDLE),
            fsm: UnsafeCell::new(Some(fsm)),
        });
        FsmState {
            inner: unsafe { NonNull::new_unchecked(Box::into_raw(inner)) },
        }
    }

    fn inner(&self) -> &FsmStateInner<F> {
        unsafe { self.inner.as_ref() }
    }

    unsafe fn clear_fsm(&self) {
        (&mut *self.inner().fsm.get()).take();
    }
}

impl<F: Fsm> FsmState<F> {
    /// Schedules the fsm for later handling.
    pub fn notify<S: FsmScheduler<Fsm = F>>(&self, scheduler: &S) {
        let inner = self.inner();
        let mut state = inner.state.load(Ordering::SeqCst);
        loop {
            let old_state = state & STATE_MASK;
            let new_state = if old_state == IDLE {
                // Idle fsm will be scheduled later, increase reference count directly
                // to avoid another atomic operation.
                ((state & REF_MASK) + REF_BASE) | NOTIFIED
            } else if old_state == POLLING {
                // Marks it back to NOTIFIED to hint it needs to be processed again.
                state & REF_MASK | NOTIFIED
            } else {
                // It's NOTIFIED or cleared
                return;
            };

            match inner.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if old_state == IDLE {
                        // Don't use clone here as ref count has been changed.
                        scheduler.schedule(FsmState { inner: self.inner });
                    }
                    return;
                }
                Err(s) => state = s,
            }
        }
    }

    /// Starts handling the inner fsm.
    ///
    /// It returns `None` if it has been cleared.
    #[must_use]
    pub fn enter_polling(self) -> Option<Managed<F>> {
        let inner = self.inner();
        let mut state = inner.state.load(Ordering::SeqCst);
        loop {
            if state & CLEAR_BIT == CLEAR_BIT {
                unsafe {
                    self.clear_fsm();
                }
                return None;
            }

            assert_eq!(state & STATE_MASK, NOTIFIED);
            let new_state = state & REF_MASK | POLLING;
            match inner.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(Managed { state: self }),
                Err(s) => state = s,
            }
        }
    }

    /// Clears the inner fsm.
    ///
    /// This will prevent the fsm being processed next time. On going process
    /// will not be aborted.
    pub fn clear(&self) {
        let inner = self.inner();
        let mut state = inner.state.load(Ordering::SeqCst);
        loop {
            if state & CLEAR_BIT == CLEAR_BIT {
                return;
            }

            // Ref count can't be reduced here, as it's racy between dropping the state
            // inner and fsm.
            let new_state = state | CLEAR_BIT;
            match inner.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(s) => state = s,
            }
        }
        let s = state & STATE_MASK;
        // Both POLLING and NOTIFIED are valid state for a fsm being processed.
        if s != POLLING && s != NOTIFIED {
            unsafe { self.clear_fsm() };
        }
    }
}

impl<F> Pointer for FsmState<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fsm = unsafe { &*self.inner().fsm.get() };
        write!(f, "{:p}", fsm.as_ref().map_or_else(ptr::null, |b| &**b))
    }
}

impl<F> Clone for FsmState<F> {
    fn clone(&self) -> FsmState<F> {
        let old_state = self.inner().state.fetch_add(REF_BASE, Ordering::SeqCst);
        let ref_count = old_state >> REF_OFFSET;
        if ref_count < MAX_REF_COUNT {
            return FsmState { inner: self.inner };
        }
        panic!("ref count is too large: {} >= {}", ref_count, MAX_REF_COUNT);
    }
}

impl<F> Drop for FsmState<F> {
    fn drop(&mut self) {
        let old_state = self.inner().state.fetch_sub(REF_BASE, Ordering::SeqCst);
        if old_state & REF_MASK != REF_BASE {
            return;
        }
        unsafe {
            Box::from_raw(self.inner.as_ptr());
        }
    }
}

unsafe impl<F: Send> Send for FsmState<F> {}

/// A safe container for accessing fsm.
///
/// It's guaranteed that fsm will be valid as long as `Managed` is not dropped.
pub struct Managed<F: Fsm> {
    state: FsmState<F>,
}

impl<F: Fsm> Deref for Managed<F> {
    type Target = F;

    fn deref(&self) -> &F {
        unsafe { &*self.state.inner().fsm.get() }.as_ref().unwrap()
    }
}

impl<F: Fsm> DerefMut for Managed<F> {
    fn deref_mut(&mut self) -> &mut F {
        unsafe { &mut *self.state.inner().fsm.get() }
            .as_mut()
            .unwrap()
    }
}

impl<F: Fsm> Managed<F> {
    /// Tries to release the fsm so that other thread can process the fsm.
    ///
    /// Some is returned if there are new messages put into mailbox.
    pub fn try_release(self) -> Option<Managed<F>> {
        let inner = self.state.inner();
        let mut state = inner.state.load(Ordering::SeqCst);
        loop {
            if state & CLEAR_BIT == CLEAR_BIT {
                unsafe {
                    self.state.clear_fsm();
                }
                return None;
            }
            match state & STATE_MASK {
                POLLING => {
                    let new_state = (state & REF_MASK | IDLE) - REF_BASE;
                    match inner.state.compare_exchange_weak(
                        state,
                        new_state,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            if new_state & REF_MASK == 0 {
                                unsafe {
                                    Box::from_raw(self.state.inner.as_ptr());
                                }
                            }
                            mem::forget(self);
                            return None;
                        }
                        Err(s) => state = s,
                    }
                }
                NOTIFIED => {
                    let new_state = state & REF_MASK | POLLING;
                    match inner.state.compare_exchange_weak(
                        state,
                        new_state,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => return Some(self),
                        Err(s) => state = s,
                    }
                }
                s => panic!("unexpected state {}", s),
            }
        }
    }

    /// Release the fsm and reschedule it if there are new messages.
    pub fn reschedule<S: FsmScheduler<Fsm = F>>(self, scheduler: &S) {
        let inner = self.state.inner();
        let mut state = inner.state.load(Ordering::SeqCst);
        loop {
            if state & CLEAR_BIT == CLEAR_BIT {
                unsafe {
                    self.state.clear_fsm();
                }
                return;
            }
            match state & STATE_MASK {
                POLLING => {
                    let new_state = (state & REF_MASK | IDLE) - REF_BASE;
                    match inner.state.compare_exchange_weak(
                        state,
                        new_state,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            if new_state & REF_MASK == 0 {
                                unsafe {
                                    Box::from_raw(self.state.inner.as_ptr());
                                }
                            }
                            mem::forget(self);
                            return;
                        }
                        Err(s) => state = s,
                    }
                }
                NOTIFIED => {
                    scheduler.schedule(FsmState {
                        inner: self.state.inner,
                    });
                    mem::forget(self);
                    return;
                }
                s => panic!("unexpected state {}", s),
            }
        }
    }
}
