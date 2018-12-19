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

//! This is the core implementation of a batch system. Generally there will be two
//! different kind of FSMs in TiKV's FSM system. One is normal FSM, which usually
//! represents a peer, the other is control FSM, which usually represents something
//! that controls how the former is created or metrics are collected.

// TODO: remove this
#![allow(dead_code)]

use crossbeam::channel::{SendError, TrySendError};
use std::borrow::Cow;
use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use util::collections::HashMap;
use util::mpsc;
use util::Either;

// The FSM is notified.
const NOTIFYSTATE_NOTIFIED: usize = 0;
// The FSM is idle.
const NOTIFYSTATE_IDLE: usize = 1;
// The FSM is expected to be dropped.
const NOTIFYSTATE_DROP: usize = 2;

struct State<N> {
    status: AtomicUsize,
    data: AtomicPtr<N>,
}

impl<N> Drop for State<N> {
    fn drop(&mut self) {
        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe { Box::from_raw(ptr) };
        }
    }
}

/// `FsmScheduler` schedules `Fsm` for later handles.
pub trait FsmScheduler {
    type Fsm: Fsm;

    /// Schedule a Fsm for later handles.
    fn schedule(&self, fsm: Box<Self::Fsm>);
    /// Shutdown the scheduler, which indicates that resouces like
    /// background thread pool should be released.
    fn shutdown(&self);
}

/// A Fsm is a finite state machine. It should be able to be notified for
/// updating internal state according to incomming messages.
pub trait Fsm {
    type Message;

    /// Notify the Fsm for readiness.
    fn notify(&self);

    /// Set a mailbox to Fsm, which should be used to send message to itself.
    fn set_mailbox(&mut self, mailbox: Cow<BasicMailbox<Self>>)
    where
        Self: Sized;
    /// Take the mailbox from Fsm. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized;
}

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
    state: Arc<State<Owner>>,
}

impl<Owner: Fsm> BasicMailbox<Owner> {
    #[inline]
    fn new(
        sender: mpsc::LooseBoundedSender<Owner::Message>,
        fsm: Box<Owner>,
    ) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender,
            state: Arc::new(State {
                status: AtomicUsize::new(NOTIFYSTATE_IDLE),
                data: AtomicPtr::new(Box::into_raw(fsm)),
            }),
        }
    }

    /// Take the owner if it's IDLE.
    fn take_fsm(&self) -> Option<Box<Owner>> {
        let previous_state = self.state.status.compare_and_swap(
            NOTIFYSTATE_IDLE,
            NOTIFYSTATE_NOTIFIED,
            Ordering::AcqRel,
        );
        if previous_state != NOTIFYSTATE_IDLE {
            return None;
        }

        let p = self.state.data.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            Some(unsafe { Box::from_raw(p) })
        } else {
            panic!("inconsistent status and data, something should be wrong.");
        }
    }

    /// Notify owner via a `FsmScheduler`.
    #[inline]
    fn notify<S: FsmScheduler<Fsm = Owner>>(&self, scheduler: &S) {
        match self.take_fsm() {
            None => {}
            Some(mut n) => {
                n.set_mailbox(Cow::Borrowed(self));
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
    fn release(&self, fsm: Box<Owner>) {
        let previous = self.state.data.swap(Box::into_raw(fsm), Ordering::AcqRel);
        let mut previous_status = NOTIFYSTATE_NOTIFIED;
        if previous.is_null() {
            previous_status = self.state.status.compare_and_swap(
                NOTIFYSTATE_NOTIFIED,
                NOTIFYSTATE_IDLE,
                Ordering::AcqRel,
            );
            match previous_status {
                NOTIFYSTATE_NOTIFIED => return,
                NOTIFYSTATE_DROP => {
                    let ptr = self.state.data.swap(ptr::null_mut(), Ordering::AcqRel);
                    unsafe { Box::from_raw(ptr) };
                    return;
                }
                _ => {}
            }
        }
        panic!("invalid release state: {:?} {}", previous, previous_status);
    }

    /// Force sending a message despite the capacity limit on channel.
    #[inline]
    pub fn force_send<S: FsmScheduler<Fsm = Owner>>(
        &self,
        msg: Owner::Message,
        scheduler: &S,
    ) -> Result<(), SendError<Owner::Message>> {
        self.sender.force_send(msg)?;
        self.notify(scheduler);
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
        self.notify(scheduler);
        Ok(())
    }

    /// Close the mailbox explicitly.
    #[inline]
    fn close(&self) {
        self.sender.close_sender();
        match self.state.status.swap(NOTIFYSTATE_DROP, Ordering::AcqRel) {
            NOTIFYSTATE_NOTIFIED | NOTIFYSTATE_DROP => return,
            _ => {}
        }

        let ptr = self.state.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                Box::from_raw(ptr);
            }
        }
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
pub struct Mailbox<Owner: Fsm, Scheduler: FsmScheduler<Fsm = Owner>> {
    mailbox: BasicMailbox<Owner>,
    scheduler: Scheduler,
}

impl<Owner: Fsm, Scheduler: FsmScheduler<Fsm = Owner>> Mailbox<Owner, Scheduler> {
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

/// Router route messages to its target mailbox.
///
/// Every fsm has a mailbox, hence it's necessary to have an address book
/// that can deliver messages to specified fsm, which is exact router.
///
/// In our abstract model, every batch system has two different kind of
/// fsms. First is normal fsm, which does the common work like peers in a
/// raftstore model or apply delegate in apply model. Second is control fsm,
/// which does some work that requires a global view of resources or creates
/// missing fsm for specified address. Normal fsm and control fsm can have
/// different scheduler, but this is not required.
pub struct Router<N: Fsm, C: Fsm, Ns, Cs> {
    normals: Arc<Mutex<HashMap<u64, BasicMailbox<N>>>>,
    caches: Cell<HashMap<u64, BasicMailbox<N>>>,
    control_box: BasicMailbox<C>,
    // TODO: These two schedulers should be unified as single one. However
    // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
    // for now.
    normal_scheduler: Ns,
    control_scheduler: Cs,
}

impl<N, C, Ns, Cs> Router<N, C, Ns, Cs>
where
    N: Fsm,
    C: Fsm,
    Ns: FsmScheduler<Fsm = N> + Clone,
    Cs: FsmScheduler<Fsm = C> + Clone,
{
    fn new(
        control_box: BasicMailbox<C>,
        normal_scheduler: Ns,
        control_scheduler: Cs,
    ) -> Router<N, C, Ns, Cs> {
        Router {
            normals: Arc::default(),
            caches: Cell::default(),
            control_box,
            normal_scheduler,
            control_scheduler,
        }
    }

    /// A helper function that tries to unify a common access pattern to
    /// mailbox.
    ///
    /// Generally, when sending a message to a mailbox, cache should be
    /// check first, if not found, lock should be acquired.
    #[inline]
    fn check_do<F, R>(&self, addr: u64, mut f: F) -> Option<R>
    where
        F: FnMut(&BasicMailbox<N>) -> Option<R>,
    {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut connected = true;
        if let Some(mailbox) = caches.get(&addr) {
            match f(mailbox) {
                Some(r) => return Some(r),
                None => {
                    connected = false;
                }
            }
        }

        let mailbox = 'fetch_box: {
            let mut boxes = self.normals.lock().unwrap();
            if let Some(mailbox) = boxes.get_mut(&addr) {
                break 'fetch_box mailbox.clone();
            }
            drop(boxes);
            if !connected {
                caches.remove(&addr);
            }
            return None;
        };

        let res = f(&mailbox);
        if res.is_some() {
            caches.insert(addr, mailbox);
        } else if !connected {
            caches.remove(&addr);
        }
        res
    }

    /// Register a mailbox with given address.
    pub fn register(&self, addr: u64, mailbox: BasicMailbox<N>) {
        let mut normals = self.normals.lock().unwrap();
        if let Some(mailbox) = normals.insert(addr, mailbox) {
            mailbox.close();
        }
    }

    /// Get the mailbox of specified address.
    pub fn mailbox(&self, addr: u64) -> Option<Mailbox<N, Ns>> {
        self.check_do(addr, |mailbox| {
            if mailbox.sender.is_sender_connected() {
                Some(Mailbox {
                    mailbox: mailbox.clone(),
                    scheduler: self.normal_scheduler.clone(),
                })
            } else {
                None
            }
        })
    }

    /// Get the mailbox of control fsm.
    pub fn control_mailbox(&self) -> Mailbox<C, Cs> {
        Mailbox {
            mailbox: self.control_box.clone(),
            scheduler: self.control_scheduler.clone(),
        }
    }

    /// Try to send a message to specified address.
    ///
    /// If Either::Left is returned, then the message is sent. Otherwise,
    /// it indicates mailbox is not found.
    #[inline]
    pub fn try_send(
        &self,
        addr: u64,
        msg: N::Message,
    ) -> Either<Result<(), TrySendError<N::Message>>, N::Message> {
        let mut msg = Some(msg);
        let mut check_times = 0;
        let res = self.check_do(addr, |mailbox| {
            check_times += 1;
            let m = msg.take().unwrap();
            match mailbox.try_send(m, &self.normal_scheduler) {
                Ok(()) => Some(Ok(())),
                r @ Err(TrySendError::Full(_)) => {
                    // TODO: report channel full
                    Some(r)
                }
                Err(TrySendError::Disconnected(m)) => {
                    msg = Some(m);
                    None
                }
            }
        });
        match res {
            Some(r) => Either::Left(r),
            None => {
                if check_times == 1 {
                    Either::Right(msg.unwrap())
                } else {
                    Either::Left(Err(TrySendError::Disconnected(msg.unwrap())))
                }
            }
        }
    }

    /// Send the message to specified address.
    #[inline]
    pub fn send(&self, addr: u64, msg: N::Message) -> Result<(), TrySendError<N::Message>> {
        match self.try_send(addr, msg) {
            Either::Left(res) => res,
            Either::Right(m) => Err(TrySendError::Disconnected(m)),
        }
    }

    /// Force sending message to specified address despite the capacity
    /// limit of mailbox.
    #[inline]
    pub fn force_send(&self, addr: u64, msg: N::Message) -> Result<(), SendError<N::Message>> {
        match self.send(addr, msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(m)) => {
                let caches = unsafe { &mut *self.caches.as_ptr() };
                caches[&addr].force_send(m, &self.normal_scheduler)
            }
            Err(TrySendError::Disconnected(m)) => Err(SendError(m)),
        }
    }

    /// Force sending message to control fsm.
    #[inline]
    pub fn send_control(&self, msg: C::Message) -> Result<(), TrySendError<C::Message>> {
        match self.control_box.try_send(msg, &self.control_scheduler) {
            Ok(()) => Ok(()),
            r @ Err(TrySendError::Full(_)) => {
                // TODO: record metrics.
                r
            }
            r => r,
        }
    }

    /// Try to notify all fsm that the cluster is being shutdown.
    pub fn broadcast_shutdown(&self) {
        info!("broadcasting shutdown");
        unsafe { &mut *self.caches.as_ptr() }.clear();
        let mut mailboxes = self.normals.lock().unwrap();
        for (addr, mailbox) in mailboxes.drain() {
            debug!("[region {}] shutdown mailbox", addr);
            mailbox.close();
        }
        self.control_box.close();
        self.normal_scheduler.shutdown();
        self.control_scheduler.shutdown();
    }

    /// Close the mailbox of address.
    pub fn close(&self, addr: u64) {
        info!("[region {}] shutdown mailbox", addr);
        unsafe { &mut *self.caches.as_ptr() }.remove(&addr);
        let mut mailboxes = self.normals.lock().unwrap();
        if let Some(mb) = mailboxes.remove(&addr) {
            mb.close();
        }
    }
}

impl<N: Fsm, C: Fsm, Ns: Clone, Cs: Clone> Clone for Router<N, C, Ns, Cs> {
    fn clone(&self) -> Router<N, C, Ns, Cs> {
        Router {
            normals: self.normals.clone(),
            caches: Cell::default(),
            control_box: self.control_box.clone(),
            // These two schedulers should be unified as single one. However
            // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
            // for now.
            normal_scheduler: self.normal_scheduler.clone(),
            control_scheduler: self.control_scheduler.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::{SendError, TryRecvError, TrySendError};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::thread;
    use test::Bencher;
    use util::mpsc;

    struct Counter {
        counter: Arc<AtomicUsize>,
        recv: mpsc::Receiver<usize>,
        mailbox: Option<BasicMailbox<Counter>>,
    }

    impl Fsm for Counter {
        type Message = usize;

        fn notify(&self) {}

        fn set_mailbox(&mut self, mailbox: Cow<BasicMailbox<Self>>) {
            self.mailbox = Some(mailbox.into_owned());
        }

        fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>> {
            self.mailbox.take()
        }
    }

    impl Drop for Counter {
        fn drop(&mut self) {
            self.counter.store(0, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct CounterScheduler {
        sender: mpsc::Sender<Option<Box<Counter>>>,
    }

    impl FsmScheduler for CounterScheduler {
        type Fsm = Counter;

        fn schedule(&self, fsm: Box<Counter>) {
            self.sender.send(Some(fsm)).unwrap();
        }

        fn shutdown(&self) {
            self.sender.send(None).unwrap();
        }
    }

    fn poll_fsm(fsm_receiver: &mpsc::Receiver<Option<Box<Counter>>>, block: bool) -> bool {
        loop {
            let mut fsm = if block {
                match fsm_receiver.recv() {
                    Ok(Some(fsm)) => fsm,
                    Err(_) | Ok(None) => return true,
                }
            } else {
                match fsm_receiver.try_recv() {
                    Ok(Some(fsm)) => fsm,
                    Err(TryRecvError::Empty) => return false,
                    Err(_) | Ok(None) => return true,
                }
            };

            while let Ok(c) = fsm.recv.try_recv() {
                fsm.counter.fetch_add(c, Ordering::SeqCst);
            }
            let mailbox = fsm.mailbox.take().unwrap();
            mailbox.release(fsm);
        }
    }

    fn new_counter(cap: usize) -> (mpsc::LooseBoundedSender<usize>, Arc<AtomicUsize>, Counter) {
        let cnt = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::loose_bounded(cap);
        let fsm = Counter {
            counter: cnt.clone(),
            recv: rx,
            mailbox: None,
        };
        (tx, cnt, fsm)
    }

    #[test]
    fn test_basic() {
        let (schedule_tx, schedule_rx) = mpsc::unbounded();
        let (control_tx, control_cnt, control_fsm) = new_counter(10);
        let (normal_tx, normal_cnt, normal_fsm) = new_counter(10);
        let control_box = BasicMailbox::new(control_tx, Box::new(control_fsm));
        let scheduler = CounterScheduler {
            sender: schedule_tx,
        };
        let router = Router::new(control_box, scheduler.clone(), scheduler);
        let normal_box = BasicMailbox::new(normal_tx, Box::new(normal_fsm));
        router.register(2, normal_box);

        // Mising mailbox should report error.
        assert_eq!(router.force_send(1, 1), Err(SendError(1)));
        assert_eq!(router.send(1, 1), Err(TrySendError::Disconnected(1)));
        assert!(schedule_rx.is_empty());

        // Existing mailbox should trigger readiness.
        router.force_send(2, 1).unwrap();
        router.send(2, 4).unwrap();
        assert_eq!(schedule_rx.len(), 1);
        poll_fsm(&schedule_rx, false);
        assert_eq!(normal_cnt.load(Ordering::SeqCst), 5);

        // Control mailbox should also trigger readiness.
        router.send_control(2).unwrap();
        assert_eq!(schedule_rx.len(), 1);
        poll_fsm(&schedule_rx, false);
        assert_eq!(control_cnt.load(Ordering::SeqCst), 2);

        // send should respect capacity limit, while force_send not.
        let sent_cnt = (0..).take_while(|_| router.send(2, 20).is_ok()).count();
        assert_eq!(router.send(2, 20), Err(TrySendError::Full(20)));
        router.force_send(2, 20).unwrap();
        let old_value = normal_cnt.load(Ordering::SeqCst);
        poll_fsm(&schedule_rx, false);
        let diff = normal_cnt.load(Ordering::SeqCst) - old_value;
        assert_eq!(diff, 20 * (sent_cnt + 1));

        // close should release resources.
        router.close(2);
        assert_eq!(normal_cnt.load(Ordering::SeqCst), 0);
        assert_eq!(router.send(2, 20), Err(TrySendError::Disconnected(20)));
        assert_eq!(router.force_send(2, 20), Err(SendError(20)));
        assert!(schedule_rx.is_empty());

        router.broadcast_shutdown();
        assert_eq!(control_cnt.load(Ordering::SeqCst), 0);
    }

    #[bench]
    fn bench_send(b: &mut Bencher) {
        let (schedule_tx, schedule_rx) = mpsc::unbounded();
        let (control_tx, _, control_fsm) = new_counter(100000);
        let (normal_tx, _, normal_fsm) = new_counter(100000);
        let control_box = BasicMailbox::new(control_tx, Box::new(control_fsm));
        let scheduler = CounterScheduler {
            sender: schedule_tx,
        };
        let router = Router::new(control_box, scheduler.clone(), scheduler);
        let normal_box = BasicMailbox::new(normal_tx, Box::new(normal_fsm));
        router.register(2, normal_box);

        let handle = thread::spawn(move || poll_fsm(&schedule_rx, true));

        b.iter(|| {
            router.send(2, 2).unwrap();
        });
        router.broadcast_shutdown();
        handle.join().unwrap();
    }
}
