// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use super::batch::{Fsm, FsmScheduler};
use crossbeam::channel::{SendError, TrySendError};
use std::borrow::Cow;
use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tikv_util::collections::HashMap;
use tikv_util::mpsc;
use tikv_util::Either;

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
    pub fn new(
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
    pub(super) fn take_fsm(&self) -> Option<Box<Owner>> {
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

    #[inline]
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
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
    pub(super) fn release(&self, fsm: Box<Owner>) {
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

enum CheckDoResult<T> {
    NotExist,
    Invalid,
    Valid(T),
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
    pub(super) control_box: BasicMailbox<C>,
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
    pub(super) fn new(
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
    ///
    /// Returns None means there is no mailbox inside the normal registry.
    /// Some(None) means there is expected mailbox inside the normal registry
    /// but it returns None after apply the given function. Some(Some) means
    /// the given function returns Some and cache is updated if it's invalid.
    #[inline]
    fn check_do<F, R>(&self, addr: u64, mut f: F) -> CheckDoResult<R>
    where
        F: FnMut(&BasicMailbox<N>) -> Option<R>,
    {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut connected = true;
        if let Some(mailbox) = caches.get(&addr) {
            match f(mailbox) {
                Some(r) => return CheckDoResult::Valid(r),
                None => {
                    connected = false;
                }
            }
        }

        let mailbox = {
            let mut boxes = self.normals.lock().unwrap();
            match boxes.get_mut(&addr) {
                Some(mailbox) => mailbox.clone(),
                None => {
                    drop(boxes);
                    if !connected {
                        caches.remove(&addr);
                    }
                    return CheckDoResult::NotExist;
                }
            }
        };

        let res = f(&mailbox);
        match res {
            Some(r) => {
                caches.insert(addr, mailbox);
                CheckDoResult::Valid(r)
            }
            None => {
                if !connected {
                    caches.remove(&addr);
                }
                CheckDoResult::Invalid
            }
        }
    }

    /// Register a mailbox with given address.
    pub fn register(&self, addr: u64, mailbox: BasicMailbox<N>) {
        let mut normals = self.normals.lock().unwrap();
        if let Some(mailbox) = normals.insert(addr, mailbox) {
            mailbox.close();
        }
    }

    pub fn register_all(&self, mailboxes: Vec<(u64, BasicMailbox<N>)>) {
        let mut normals = self.normals.lock().unwrap();
        normals.reserve(mailboxes.len());
        for (addr, mailbox) in mailboxes {
            if let Some(m) = normals.insert(addr, mailbox) {
                m.close();
            }
        }
    }

    /// Get the mailbox of specified address.
    pub fn mailbox(&self, addr: u64) -> Option<Mailbox<N, Ns>> {
        let res = self.check_do(addr, |mailbox| {
            if mailbox.sender.is_sender_connected() {
                Some(Mailbox {
                    mailbox: mailbox.clone(),
                    scheduler: self.normal_scheduler.clone(),
                })
            } else {
                None
            }
        });
        match res {
            CheckDoResult::Valid(r) => Some(r),
            _ => None,
        }
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
        let res = self.check_do(addr, |mailbox| {
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
            CheckDoResult::Valid(r) => Either::Left(r),
            CheckDoResult::Invalid => Either::Left(Err(TrySendError::Disconnected(msg.unwrap()))),
            CheckDoResult::NotExist => Either::Right(msg.unwrap()),
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

    /// Try to notify all normal fsm a message.
    pub fn broadcast_normal(&self, mut msg_gen: impl FnMut() -> N::Message) {
        let mailboxes = self.normals.lock().unwrap();
        for mailbox in mailboxes.values() {
            let _ = mailbox.force_send(msg_gen(), &self.normal_scheduler);
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
    use crate::raftstore::store::fsm::batch::tests::Message;
    use crate::raftstore::store::fsm::batch::{self, tests::*};
    use crossbeam::channel::{RecvTimeoutError, SendError, TryRecvError, TrySendError};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::time::Duration;
    use test::Bencher;

    fn counter_closure(counter: &Arc<AtomicUsize>) -> Message {
        let c = counter.clone();
        Some(Box::new(move |_: &mut Runner| {
            c.fetch_add(1, Ordering::SeqCst);
        }))
    }

    fn noop() -> Message {
        None
    }

    fn unreachable() -> Message {
        Some(Box::new(|_: &mut Runner| unreachable!()))
    }

    #[test]
    fn test_basic() {
        let (control_tx, mut control_fsm) = new_runner(10);
        let (control_drop_tx, control_drop_rx) = mpsc::unbounded();
        control_fsm.sender = Some(control_drop_tx);
        let (router, mut system) = batch::create_system(2, 2, control_tx, control_fsm);
        let builder = Builder::new();
        system.spawn("test".to_owned(), builder);

        // Missing mailbox should report error.
        match router.force_send(1, unreachable()) {
            Err(SendError(_)) => (),
            Ok(_) => panic!("send should fail"),
        }
        match router.send(1, unreachable()) {
            Err(TrySendError::Disconnected(_)) => (),
            Ok(_) => panic!("send should fail"),
            Err(TrySendError::Full(_)) => panic!("expect disconnected."),
        }

        let (tx, rx) = mpsc::unbounded();
        let router_ = router.clone();
        // Control mailbox should be connected.
        router
            .send_control(Some(Box::new(move |_: &mut Runner| {
                let (sender, mut runner) = new_runner(10);
                let (tx1, rx1) = mpsc::unbounded();
                runner.sender = Some(tx1);
                let mailbox = BasicMailbox::new(sender, runner);
                router_.register(1, mailbox);
                tx.send(rx1).unwrap();
            })))
            .unwrap();
        let runner_drop_rx = rx.recv_timeout(Duration::from_secs(3)).unwrap();

        // Registered mailbox should be connected.
        router.force_send(1, noop()).unwrap();
        router.send(1, noop()).unwrap();

        // Send should respect capacity limit, while force_send not.
        let (tx, rx) = mpsc::unbounded();
        router
            .send(
                1,
                Some(Box::new(move |_: &mut Runner| {
                    rx.recv_timeout(Duration::from_secs(100)).unwrap();
                })),
            )
            .unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let sent_cnt = (0..)
            .take_while(|_| router.send(1, counter_closure(&counter)).is_ok())
            .count();
        match router.send(1, counter_closure(&counter)) {
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => panic!("mailbox should still be connected."),
            Ok(_) => panic!("send should fail"),
        }
        router.force_send(1, counter_closure(&counter)).unwrap();
        tx.send(1).unwrap();
        // Flush.
        let (tx, rx) = mpsc::unbounded();
        router
            .force_send(
                1,
                Some(Box::new(move |_: &mut Runner| {
                    tx.send(1).unwrap();
                })),
            )
            .unwrap();
        rx.recv_timeout(Duration::from_secs(100)).unwrap();

        let c = counter.load(Ordering::SeqCst);
        assert_eq!(c, sent_cnt + 1);

        // close should release resources.
        assert_eq!(runner_drop_rx.try_recv(), Err(TryRecvError::Empty));
        router.close(1);
        assert_eq!(
            runner_drop_rx.recv_timeout(Duration::from_secs(3)),
            Err(RecvTimeoutError::Disconnected)
        );
        match router.send(1, unreachable()) {
            Err(TrySendError::Disconnected(_)) => (),
            Ok(_) => panic!("send should fail."),
            Err(TrySendError::Full(_)) => panic!("sender should be closed"),
        }
        match router.force_send(1, unreachable()) {
            Err(SendError(_)) => (),
            Ok(_) => panic!("send should fail."),
        }
        assert_eq!(control_drop_rx.try_recv(), Err(TryRecvError::Empty));
        system.shutdown();
        assert_eq!(
            control_drop_rx.recv_timeout(Duration::from_secs(3)),
            Err(RecvTimeoutError::Disconnected)
        );
    }

    #[bench]
    fn bench_send(b: &mut Bencher) {
        let (control_tx, control_fsm) = new_runner(100000);
        let (router, mut system) = batch::create_system(2, 2, control_tx, control_fsm);
        let builder = Builder::new();
        system.spawn("test".to_owned(), builder);
        let (normal_tx, normal_fsm) = new_runner(100000);
        let normal_box = BasicMailbox::new(normal_tx, normal_fsm);
        router.register(1, normal_box);

        b.iter(|| {
            router.send(1, noop()).unwrap();
        });
        system.shutdown();
    }
}
