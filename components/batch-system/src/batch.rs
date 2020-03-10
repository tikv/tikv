// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This is the core implementation of a batch system. Generally there will be two
//! different kind of FSMs in TiKV's FSM system. One is normal FSM, which usually
//! represents a peer, the other is control FSM, which usually represents something
//! that controls how the former is created or metrics are collected.

use crate::fsm::{Fsm, FsmScheduler};
use crate::mailbox::BasicMailbox;
use crate::router::Router;
use crossbeam::channel::{self, SendError, TryRecvError};
use std::borrow::Cow;
use std::thread::{self, JoinHandle};
use tikv_util::mpsc;

/// A unify type for FSMs so that they can be sent to channel easily.
enum FsmTypes<N, C> {
    Normal(Box<N>),
    Control(Box<C>),
    // Used as a signal that scheduler should be shutdown.
    Empty,
}

// A macro to introduce common definition of scheduler.
macro_rules! impl_sched {
    ($name:ident) => {
        pub struct $name<N, C> {
            sender: channel::Sender<FsmTypes<N, C>>,
        }

        impl<N, C> Clone for $name<N, C> {
            #[inline]
            fn clone(&self) -> $name<N, C> {
                $name {
                    sender: self.sender.clone(),
                }
            }
        }
    };
}

impl_sched!(NormalScheduler);
impl_sched!(ControlScheduler);

impl<N: Fsm, C> FsmScheduler for NormalScheduler<N, C> {
    type Fsm = N;

    #[inline]
    fn schedule(&self, fsm: Box<N>) {
        match self.sender.send(FsmTypes::Normal(fsm)) {
            Ok(()) => return,
            // TODO: use debug instead.
            Err(SendError(FsmTypes::Normal(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
            _ => unreachable!(),
        }
    }

    fn shutdown(&self) {
        // TODO: close it explicitly once it's supported.
        // Magic number, actually any number greater than poll pool size works.
        for _ in 0..100 {
            let _ = self.sender.send(FsmTypes::Empty);
        }
    }
}

impl<N, C: Fsm> FsmScheduler for ControlScheduler<N, C> {
    type Fsm = C;

    #[inline]
    fn schedule(&self, fsm: Box<C>) {
        match self.sender.send(FsmTypes::Control(fsm)) {
            Ok(()) => return,
            Err(SendError(FsmTypes::Control(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
            _ => unreachable!(),
        }
    }

    fn shutdown(&self) {
        for _ in 0..100 {
            let _ = self.sender.send(FsmTypes::Empty);
        }
    }
}

/// A basic struct for a round of polling.
#[allow(clippy::vec_box)]
pub struct Batch<N, C> {
    normals: Vec<Box<N>>,
    counters: Vec<usize>,
    control: Option<Box<C>>,
}

impl<N: Fsm, C: Fsm> Batch<N, C> {
    /// Create a a batch with given batch size.
    pub fn with_capacity(cap: usize) -> Batch<N, C> {
        Batch {
            normals: Vec::with_capacity(cap),
            counters: Vec::with_capacity(cap),
            control: None,
        }
    }

    pub fn normals_mut(&mut self) -> &mut [Box<N>] {
        &mut self.normals
    }

    fn push(&mut self, fsm: FsmTypes<N, C>) -> bool {
        match fsm {
            FsmTypes::Normal(n) => {
                self.normals.push(n);
                self.counters.push(0);
            }
            FsmTypes::Control(c) => {
                assert!(self.control.is_none());
                self.control = Some(c);
            }
            FsmTypes::Empty => return false,
        }
        true
    }

    #[inline]
    fn len(&self) -> usize {
        self.normals.len() + self.control.is_some() as usize
    }

    fn is_empty(&self) -> bool {
        self.normals.is_empty() && self.control.is_none()
    }

    fn clear(&mut self) {
        self.normals.clear();
        self.counters.clear();
        self.control.take();
    }

    /// Put back the FSM located at index.
    ///
    /// Only when channel length is larger than `checked_len` will trigger
    /// further notification. This function may fail if channel length is
    /// larger than the given value before FSM is released.
    pub fn release(&mut self, index: usize, checked_len: usize) {
        let mut fsm = self.normals.swap_remove(index);
        let mailbox = fsm.take_mailbox().unwrap();
        mailbox.release(fsm);
        if mailbox.len() == checked_len {
            self.counters.swap_remove(index);
        } else {
            match mailbox.take_fsm() {
                None => (),
                Some(mut s) => {
                    s.set_mailbox(Cow::Owned(mailbox));
                    let last_index = self.normals.len();
                    self.normals.push(s);
                    self.normals.swap(index, last_index);
                }
            }
        }
    }

    /// Remove the normal FSM located at `index`.
    ///
    /// This method should only be called when the FSM is stopped.
    /// If there are still messages in channel, the FSM is untouched and
    /// the function will return false to let caller to keep polling.
    pub fn remove(&mut self, index: usize) {
        let mut fsm = self.normals.swap_remove(index);
        let mailbox = fsm.take_mailbox().unwrap();
        if mailbox.is_empty() {
            mailbox.release(fsm);
            self.counters.swap_remove(index);
        } else {
            fsm.set_mailbox(Cow::Owned(mailbox));
            let last_index = self.normals.len();
            self.normals.push(fsm);
            self.normals.swap(index, last_index);
        }
    }

    /// Schedule the normal FSM located at `index`.
    pub fn reschedule(&mut self, router: &BatchRouter<N, C>, index: usize) {
        let fsm = self.normals.swap_remove(index);
        self.counters.swap_remove(index);
        router.normal_scheduler.schedule(fsm);
    }

    /// Same as `release`, but working on control FSM.
    pub fn release_control(&mut self, control_box: &BasicMailbox<C>, checked_len: usize) -> bool {
        let s = self.control.take().unwrap();
        control_box.release(s);
        if control_box.len() == checked_len {
            true
        } else {
            match control_box.take_fsm() {
                None => true,
                Some(s) => {
                    self.control = Some(s);
                    false
                }
            }
        }
    }

    /// Same as `remove`, but working on control FSM.
    pub fn remove_control(&mut self, control_box: &BasicMailbox<C>) {
        if control_box.is_empty() {
            let s = self.control.take().unwrap();
            control_box.release(s);
        }
    }
}

/// A handler that poll all FSM in ready.
///
/// A General process works like following:
/// ```text
/// loop {
///     begin
///     if control is ready:
///         handle_control
///     foreach ready normal:
///         handle_normal
///     end
/// }
/// ```
///
/// Note that, every poll thread has its own handler, which doesn't have to be
/// Sync.
pub trait PollHandler<N, C> {
    /// This function is called at the very beginning of every round.
    fn begin(&mut self, batch_size: usize);

    /// This function is called when handling readiness for control FSM.
    ///
    /// If returned value is Some, then it represents a length of channel. This
    /// function will only be called for the same fsm after channel's lengh is
    /// larger than the value. If it returns None, then this function will
    /// still be called for the same FSM in the next loop unless the FSM is
    /// stopped.
    fn handle_control(&mut self, control: &mut C) -> Option<usize>;

    /// This function is called when handling readiness for normal FSM.
    ///
    /// The returned value is handled in the same way as `handle_control`.
    fn handle_normal(&mut self, normal: &mut N) -> Option<usize>;

    /// This function is called at the end of every round.
    fn end(&mut self, batch: &mut [Box<N>]);

    /// This function is called when batch system is going to sleep.
    fn pause(&mut self) {}
}

/// Internal poller that fetches batch and call handler hooks for readiness.
struct Poller<N: Fsm, C: Fsm, Handler> {
    router: Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>,
    fsm_receiver: channel::Receiver<FsmTypes<N, C>>,
    handler: Handler,
    max_batch_size: usize,
}

enum ReschedulePolicy {
    Release(usize),
    Remove,
    Schedule,
}

impl<N: Fsm, C: Fsm, Handler: PollHandler<N, C>> Poller<N, C, Handler> {
    fn fetch_batch(&mut self, batch: &mut Batch<N, C>, max_size: usize) {
        let curr_batch_len = batch.len();
        if batch.control.is_some() || curr_batch_len >= max_size {
            // Do nothing if there's a pending control fsm or the batch is already full.
            return;
        }

        let mut pushed = if curr_batch_len == 0 {
            match self.fsm_receiver.try_recv().or_else(|_| {
                self.handler.pause();
                // Block if the batch is empty.
                self.fsm_receiver.recv()
            }) {
                Ok(fsm) => batch.push(fsm),
                Err(_) => return,
            }
        } else {
            true
        };

        while pushed {
            if batch.len() < max_size {
                let fsm = match self.fsm_receiver.try_recv() {
                    Ok(fsm) => fsm,
                    Err(TryRecvError::Empty) => return,
                    Err(TryRecvError::Disconnected) => unreachable!(),
                };
                pushed = batch.push(fsm);
            } else {
                return;
            }
        }
        batch.clear();
    }

    // Poll for readiness and forward to handler. Remove stale peer if necessary.
    fn poll(&mut self) {
        let mut batch = Batch::with_capacity(self.max_batch_size);
        let mut reschedule_fsms = Vec::with_capacity(self.max_batch_size);

        self.fetch_batch(&mut batch, self.max_batch_size);
        while !batch.is_empty() {
            let mut hot_fsm_count = 0;
            self.handler.begin(batch.len());
            if batch.control.is_some() {
                let len = self.handler.handle_control(batch.control.as_mut().unwrap());
                if batch.control.as_ref().unwrap().is_stopped() {
                    batch.remove_control(&self.router.control_box);
                } else if let Some(len) = len {
                    batch.release_control(&self.router.control_box, len);
                }
            }
            if !batch.normals.is_empty() {
                for (i, p) in batch.normals.iter_mut().enumerate() {
                    let len = self.handler.handle_normal(p);
                    batch.counters[i] += 1;
                    if p.is_stopped() {
                        reschedule_fsms.push((i, ReschedulePolicy::Remove));
                    } else {
                        if batch.counters[i] > 3 {
                            hot_fsm_count += 1;
                            // We should only reschedule a half of the hot regions, otherwise,
                            // it's possible all the hot regions are fetched in a batch the
                            // next time.
                            if hot_fsm_count % 2 == 0 {
                                reschedule_fsms.push((i, ReschedulePolicy::Schedule));
                                continue;
                            }
                        }
                        if let Some(l) = len {
                            reschedule_fsms.push((i, ReschedulePolicy::Release(l)));
                        }
                    }
                }
            }
            self.handler.end(batch.normals_mut());
            // Because release use `swap_remove` internally, so using pop here
            // to remove the correct FSM.
            while let Some((r, mark)) = reschedule_fsms.pop() {
                match mark {
                    ReschedulePolicy::Release(l) => batch.release(r, l),
                    ReschedulePolicy::Remove => batch.remove(r),
                    ReschedulePolicy::Schedule => batch.reschedule(&self.router, r),
                }
            }
            // Fetch batch after every round is finished. It's helpful to protect regions
            // from becoming hungry if some regions are hot points.
            self.fetch_batch(&mut batch, self.max_batch_size);
        }
    }
}

/// A builder trait that can build up poll handlers.
pub trait HandlerBuilder<N, C> {
    type Handler: PollHandler<N, C>;

    fn build(&mut self) -> Self::Handler;
}

/// A system that can poll FSMs concurrently and in batch.
///
/// To use the system, two type of FSMs and their PollHandlers need
/// to be defined: Normal and Control. Normal FSM handles the general
/// task while Control FSM creates normal FSM instances.
pub struct BatchSystem<N: Fsm, C: Fsm> {
    name_prefix: Option<String>,
    router: BatchRouter<N, C>,
    receiver: channel::Receiver<FsmTypes<N, C>>,
    pool_size: usize,
    max_batch_size: usize,
    workers: Vec<JoinHandle<()>>,
}

impl<N, C> BatchSystem<N, C>
where
    N: Fsm + Send + 'static,
    C: Fsm + Send + 'static,
{
    pub fn router(&self) -> &BatchRouter<N, C> {
        &self.router
    }

    /// Start the batch system.
    pub fn spawn<B>(&mut self, name_prefix: String, mut builder: B)
    where
        B: HandlerBuilder<N, C>,
        B::Handler: Send + 'static,
    {
        for i in 0..self.pool_size {
            let handler = builder.build();
            let mut poller = Poller {
                router: self.router.clone(),
                fsm_receiver: self.receiver.clone(),
                handler,
                max_batch_size: self.max_batch_size,
            };
            let t = thread::Builder::new()
                .name(thd_name!(format!("{}-{}", name_prefix, i)))
                .spawn(move || poller.poll())
                .unwrap();
            self.workers.push(t);
        }
        self.name_prefix = Some(name_prefix);
    }

    /// Shutdown the batch system and wait till all background threads exit.
    pub fn shutdown(&mut self) {
        if self.name_prefix.is_none() {
            return;
        }
        let name_prefix = self.name_prefix.take().unwrap();
        info!("shutdown batch system {}", name_prefix);
        self.router.broadcast_shutdown();
        for h in self.workers.drain(..) {
            debug!("waiting for {}", h.thread().name().unwrap());
            h.join().unwrap();
        }
        info!("batch system {} is stopped.", name_prefix);
    }
}

pub type BatchRouter<N, C> = Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>;

/// Create a batch system with the given thread name prefix and pool size.
///
/// `sender` and `controller` should be paired.
pub fn create_system<N: Fsm, C: Fsm>(
    pool_size: usize,
    max_batch_size: usize,
    sender: mpsc::LooseBoundedSender<C::Message>,
    controller: Box<C>,
) -> (BatchRouter<N, C>, BatchSystem<N, C>) {
    let control_box = BasicMailbox::new(sender, controller);
    let (tx, rx) = channel::unbounded();
    let normal_scheduler = NormalScheduler { sender: tx.clone() };
    let control_scheduler = ControlScheduler { sender: tx };
    let router = Router::new(control_box, normal_scheduler, control_scheduler);
    let system = BatchSystem {
        name_prefix: None,
        router: router.clone(),
        receiver: rx,
        pool_size,
        max_batch_size,
        workers: vec![],
    };
    (router, system)
}
