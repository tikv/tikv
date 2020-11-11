// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::fsm::{Fsm, FsmScheduler, FsmState};
use crate::mailbox::{BasicMailbox, Mailbox};
use crossbeam::channel::{SendError, TrySendError};
use std::cell::Cell;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tikv_alloc::trace::{Id, MemoryTrace};
use tikv_util::collections::HashMap;
use tikv_util::lru::LruCache;
use tikv_util::Either;

/// A struct that traces the approximate memory usage of router.
#[derive(Default)]
pub struct RouterTrace {
    pub alive: Arc<AtomicUsize>,
    pub total: Arc<AtomicUsize>,
    mailbox_unit: usize,
    state_unit: usize,
    message_unit: usize,
}

impl RouterTrace {
    pub fn trace(&self, id: impl Into<Id>, sub_trace: &mut MemoryTrace) -> usize {
        let total = self.total.load(Ordering::Relaxed);
        let alive = self.alive.load(Ordering::Relaxed);
        let leak = if total > alive + 1 {
            total - alive - 1
        } else {
            0
        };
        // hashbrown uses 7/8 of allocated memory.
        let alive_mem = alive * self.mailbox_unit * 8 / 7
            + self.state_unit * alive
            + (self.message_unit + 8) * 31 * alive;
        let leak_mem = self.state_unit * leak + (self.message_unit + 8) * 31 * leak;
        let s = sub_trace.add_sub_trace_with_capacity(id, 2);
        s.add_sub_trace("alive").set_size(alive_mem);
        s.add_sub_trace("leak").set_size(leak_mem);
        s.set_size(alive_mem + leak_mem);
        alive_mem + leak_mem
    }
}

enum CheckDoResult<T> {
    NotExist,
    Invalid,
    Valid(T),
}

struct NormalMailMap<N: Fsm> {
    map: HashMap<u64, BasicMailbox<N>>,
    /// Count of Mailbox that is stored in `map`.
    alive_cnt: Arc<AtomicUsize>,
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
    normals: Arc<Mutex<NormalMailMap<N>>>,
    caches: Cell<LruCache<u64, BasicMailbox<N>>>,
    pub(super) control_box: BasicMailbox<C>,
    // TODO: These two schedulers should be unified as single one. However
    // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
    // for now.
    pub(crate) normal_scheduler: Ns,
    control_scheduler: Cs,

    /// Count of Mailbox that is not destroyed.
    state_cnt: Arc<AtomicUsize>,
    // Indicates the router is shutdown down or not.
    shutdown: Arc<AtomicBool>,
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
        state_cnt: Arc<AtomicUsize>,
    ) -> Router<N, C, Ns, Cs> {
        Router {
            normals: Arc::new(Mutex::new(NormalMailMap {
                map: HashMap::default(),
                alive_cnt: Arc::default(),
            })),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            control_box,
            normal_scheduler,
            control_scheduler,
            state_cnt,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The `Router` has been already shutdown or not.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
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

        let (cnt, mailbox) = {
            let mut boxes = self.normals.lock().unwrap();
            let cnt = boxes.map.len();
            let b = match boxes.map.get_mut(&addr) {
                Some(mailbox) => mailbox.clone(),
                None => {
                    drop(boxes);
                    if !connected {
                        caches.remove(&addr);
                    }
                    return CheckDoResult::NotExist;
                }
            };
            (cnt, b)
        };
        if cnt > caches.capacity() || cnt < caches.capacity() / 2 {
            caches.resize(cnt);
        }

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
        if let Some(mailbox) = normals.map.insert(addr, mailbox) {
            mailbox.close();
        }
        normals
            .alive_cnt
            .store(normals.map.len(), Ordering::Relaxed);
    }

    pub fn register_all(&self, mailboxes: Vec<(u64, BasicMailbox<N>)>) {
        let mut normals = self.normals.lock().unwrap();
        normals.map.reserve(mailboxes.len());
        for (addr, mailbox) in mailboxes {
            if let Some(m) = normals.map.insert(addr, mailbox) {
                m.close();
            }
        }
        normals
            .alive_cnt
            .store(normals.map.len(), Ordering::Relaxed);
    }

    /// Get the mailbox of specified address.
    pub fn mailbox(&self, addr: u64) -> Option<Mailbox<N, Ns>> {
        let res = self.check_do(addr, |mailbox| {
            if mailbox.is_connected() {
                Some(Mailbox::new(mailbox.clone(), self.normal_scheduler.clone()))
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
        Mailbox::new(self.control_box.clone(), self.control_scheduler.clone())
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
                caches
                    .get(&addr)
                    .unwrap()
                    .force_send(m, &self.normal_scheduler)
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
        for mailbox in mailboxes.map.values() {
            let _ = mailbox.force_send(msg_gen(), &self.normal_scheduler);
        }
    }

    /// Try to notify all fsm that the cluster is being shutdown.
    pub fn broadcast_shutdown(&self) {
        info!("broadcasting shutdown");
        self.shutdown.store(true, Ordering::SeqCst);
        unsafe { &mut *self.caches.as_ptr() }.clear();
        let mut mailboxes = self.normals.lock().unwrap();
        for (addr, mailbox) in mailboxes.map.drain() {
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
        if let Some(mb) = mailboxes.map.remove(&addr) {
            mb.close();
        }
        mailboxes
            .alive_cnt
            .store(mailboxes.map.len(), Ordering::Relaxed);
    }

    pub fn state_cnt(&self) -> &Arc<AtomicUsize> {
        &self.state_cnt
    }

    pub fn alive_cnt(&self) -> Arc<AtomicUsize> {
        self.normals.lock().unwrap().alive_cnt.clone()
    }

    pub fn trace(&self) -> RouterTrace {
        RouterTrace {
            alive: self.normals.lock().unwrap().alive_cnt.clone(),
            total: self.state_cnt.clone(),
            mailbox_unit: mem::size_of::<(u64, BasicMailbox<N>)>(),
            state_unit: mem::size_of::<FsmState<N>>(),
            message_unit: mem::size_of::<N::Message>(),
        }
    }
}

impl<N: Fsm, C: Fsm, Ns: Clone, Cs: Clone> Clone for Router<N, C, Ns, Cs> {
    fn clone(&self) -> Router<N, C, Ns, Cs> {
        Router {
            normals: self.normals.clone(),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            control_box: self.control_box.clone(),
            // These two schedulers should be unified as single one. However
            // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
            // for now.
            normal_scheduler: self.normal_scheduler.clone(),
            control_scheduler: self.control_scheduler.clone(),
            shutdown: self.shutdown.clone(),
            state_cnt: self.state_cnt.clone(),
        }
    }
}
