// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use crossbeam::channel::{SendError, TrySendError};
use dashmap::DashMap;
use tikv_util::{
    debug, info,
    time::{duration_to_sec, Instant},
    Either,
};

use crate::{
    fsm::{Fsm, FsmScheduler},
    mailbox::{BasicMailbox, Mailbox},
    metrics::*,
};

/// A struct that traces the approximate memory usage of router.
#[derive(Default)]
pub struct RouterTrace {
    pub alive: usize,
    pub leak: usize,
}

enum CheckDoResult<T> {
    NotExist,
    Invalid,
    Valid(T),
}

const ROUTER_SHRINK_SIZE: usize = 1000;

/// Router routes messages to its target FSM's mailbox.
///
/// In our abstract model, every batch system has two different kind of
/// FSMs. First is normal FSM, which does the common work like peers in a
/// raftstore model or apply delegate in apply model. Second is control FSM,
/// which does some work that requires a global view of resources or creates
/// missing FSM for specified address.
///
/// There are one control FSM and multiple normal FSMs in a system. Each FSM
/// has its own mailbox. We maintain an address book to deliver messages to the
/// specified normal FSM.
///
/// Normal FSM and control FSM can have different scheduler, but this is not
/// required.
pub struct Router<N: Fsm, C: Fsm, Ns, Cs> {
    normals: Arc<DashMap<u64, BasicMailbox<N>>>,
    pub(super) control_box: BasicMailbox<C>,
    // TODO: These two schedulers should be unified as single one. However
    // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
    // for now.
    pub(crate) normal_scheduler: Ns,
    pub(crate) control_scheduler: Cs,

    // Number of active mailboxes.
    // Added when a mailbox is created, and subtracted it when a mailbox is
    // destroyed.
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
            normals: Arc::new(DashMap::default()),
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
    /// Returns None means there is no mailbox inside the normal registry.
    /// Some(None) means there is expected mailbox inside the normal registry
    /// but it returns None after apply the given function. Some(Some) means
    /// the given function returns Some.
    #[inline]
    fn check_do<F, R>(&self, addr: u64, mut f: F) -> CheckDoResult<R>
    where
        F: FnMut(&BasicMailbox<N>) -> Option<R>,
    {
        let mailbox = match self.normals.get_mut(&addr) {
            Some(mailbox) => mailbox,
            None => {
                return CheckDoResult::NotExist;
            }
        };
        match f(&mailbox) {
            Some(r) => CheckDoResult::Valid(r),
            None => CheckDoResult::Invalid,
        }
    }

    /// Register a mailbox with given address.
    pub fn register(&self, addr: u64, mailbox: BasicMailbox<N>) {
        if let Some(mailbox) = self.normals.insert(addr, mailbox) {
            mailbox.close();
        }
    }

    /// Same as send a message and then register the mailbox.
    ///
    /// The mailbox will not be registered if the message can't be sent.
    pub fn send_and_register(
        &self,
        addr: u64,
        mailbox: BasicMailbox<N>,
        msg: N::Message,
    ) -> Result<(), (BasicMailbox<N>, N::Message)> {
        if let Some(mailbox) = self.normals.insert(addr, mailbox.clone()) {
            mailbox.close();
        }
        if let Err(SendError(m)) = mailbox.force_send(msg, &self.normal_scheduler) {
            self.normals.remove(&addr);
            return Err((mailbox, m));
        }
        Ok(())
    }

    pub fn register_all(&self, mailboxes: Vec<(u64, BasicMailbox<N>)>) {
        for (addr, mailbox) in mailboxes {
            if let Some(m) = self.normals.insert(addr, mailbox) {
                m.close();
            }
        }
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

    /// Get the mailbox of control FSM.
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
                    CHANNEL_FULL_COUNTER_VEC
                        .with_label_values(&["normal"])
                        .inc();
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
            Err(TrySendError::Full(m)) => self
                .normals
                .get(&addr)
                .unwrap()
                .force_send(m, &self.normal_scheduler),
            Err(TrySendError::Disconnected(m)) => {
                if self.is_shutdown() {
                    Ok(())
                } else {
                    Err(SendError(m))
                }
            }
        }
    }

    /// Sending message to control FSM.
    #[inline]
    pub fn send_control(&self, msg: C::Message) -> Result<(), TrySendError<C::Message>> {
        match self.control_box.try_send(msg, &self.control_scheduler) {
            Ok(()) => Ok(()),
            r @ Err(TrySendError::Full(_)) => {
                CHANNEL_FULL_COUNTER_VEC
                    .with_label_values(&["control"])
                    .inc();
                r
            }
            r => r,
        }
    }

    /// Force sending message to control FSM.
    #[inline]
    pub fn force_send_control(&self, msg: C::Message) -> Result<(), SendError<C::Message>> {
        self.control_box.force_send(msg, &self.control_scheduler)
    }

    /// Try to notify all normal FSMs a message.
    pub fn broadcast_normal(&self, mut msg_gen: impl FnMut() -> N::Message) {
        let timer = Instant::now_coarse();
        self.normals.iter().for_each(|mailbox| {
            let _ = mailbox.force_send(msg_gen(), &self.normal_scheduler);
        });
        BROADCAST_NORMAL_DURATION.observe(duration_to_sec(timer.saturating_elapsed()));
    }

    /// Try to notify all FSMs that the cluster is being shutdown.
    pub fn broadcast_shutdown(&self) {
        info!("broadcasting shutdown");
        self.shutdown.store(true, Ordering::SeqCst);
        for e in self.normals.iter() {
            let addr = e.key();
            let mailbox = e.value();
            debug!("[region {}] shutdown mailbox", addr);
            mailbox.close();
        }
        self.normals.clear();
        self.control_box.close();
        self.normal_scheduler.shutdown();
        self.control_scheduler.shutdown();
    }

    /// Close the mailbox of address.
    pub fn close(&self, addr: u64) {
        info!("shutdown mailbox"; "region_id" => addr);
        if let Some((_, mb)) = self.normals.remove(&addr) {
            mb.close();
        }
        if self.normals.capacity() - self.normals.len() > ROUTER_SHRINK_SIZE {
            self.normals.shrink_to_fit();
        }
    }

    pub fn state_cnt(&self) -> &Arc<AtomicUsize> {
        &self.state_cnt
    }

    pub fn alive_cnt(&self) -> usize {
        self.normals.len()
    }

    pub fn trace(&self) -> RouterTrace {
        let alive = self.alive_cnt();
        let total = self.state_cnt.load(Ordering::Relaxed);
        // 1 represents the control fsm.
        let leak = if total > alive + 1 {
            total - alive - 1
        } else {
            0
        };
        RouterTrace { alive, leak }
    }
}

impl<N: Fsm, C: Fsm, Ns: Clone, Cs: Clone> Clone for Router<N, C, Ns, Cs> {
    fn clone(&self) -> Router<N, C, Ns, Cs> {
        Router {
            normals: self.normals.clone(),
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
