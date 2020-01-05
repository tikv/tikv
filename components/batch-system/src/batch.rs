// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This is the core implementation of a batch system. Generally there will be two
//! different kind of FSMs in TiKV's FSM system. One is normal FSM, which usually
//! represents a peer, the other is control FSM, which usually represents something
//! that controls how the former is created or metrics are collected.

use crate::fsm::{Fsm, FsmScheduler, FsmState, Managed};
use crate::mailbox::BasicMailbox;
use crate::router::Router;
use crossbeam::channel::{self, SendError, TryRecvError};
use std::thread::{self, JoinHandle};
use tikv_util::mpsc;

/// A unify type for FSMs so that they can be sent to channel easily.
enum FsmTypes<N, C> {
    Normal(FsmState<N>),
    Control(FsmState<C>),
    // Used as a signal that scheduler should be shutdown.
    Empty,
}

// A macro to introduce common definition of scheduler.
macro_rules! impl_sched {
    ($name:ident, $ty:path, Fsm = $fsm:tt) => {
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

        impl<N, C> FsmScheduler for $name<N, C>
        where
            $fsm: Fsm,
        {
            type Fsm = $fsm;

            #[inline]
            fn schedule(&self, fsm: FsmState<Self::Fsm>) {
                match self.sender.send($ty(fsm)) {
                    Ok(()) => {}
                    // TODO: use debug instead.
                    Err(SendError($ty(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
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
    };
}

impl_sched!(NormalScheduler, FsmTypes::Normal, Fsm = N);
impl_sched!(ControlScheduler, FsmTypes::Control, Fsm = C);

/// A basic struct for a round of polling.
pub struct Batch<N: Fsm, C: Fsm> {
    normals: Vec<Managed<N>>,
    control: Option<Managed<C>>,
}

impl<N: Fsm, C: Fsm> Batch<N, C> {
    /// Create a a batch with given batch size.
    pub fn with_capacity(cap: usize) -> Batch<N, C> {
        Batch {
            normals: Vec::with_capacity(cap),
            control: None,
        }
    }

    pub fn normals_mut(&mut self) -> &mut [Managed<N>] {
        &mut self.normals
    }

    fn push(&mut self, fsm: FsmTypes<N, C>) -> bool {
        match fsm {
            FsmTypes::Normal(n) => {
                if let Some(n) = n.enter_polling() {
                    self.normals.push(n)
                }
            }
            FsmTypes::Control(c) => {
                if let Some(c) = c.enter_polling() {
                    assert!(self.control.is_none());
                    self.control = Some(c);
                }
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
        self.control.take();
    }

    /// Put back the FSM located at index.
    pub fn release(&mut self, index: usize) -> bool {
        let fsm = self.normals.swap_remove(index);
        if let Some(s) = fsm.try_release() {
            let last_index = self.normals.len();
            self.normals.push(s);
            self.normals.swap(index, last_index);
            false
        } else {
            true
        }
    }

    /// Remove the normal FSM located at `index`.
    #[allow(dead_code)]
    pub fn reschedule(&mut self, index: usize, scheduler: &NormalScheduler<N, C>) {
        let fsm = self.normals.swap_remove(index);
        fsm.reschedule(scheduler);
    }

    /// Same as `release`, but working on control FSM.
    pub fn release_control(&mut self) -> bool {
        let s = self.control.take().unwrap();
        if let Some(s) = s.try_release() {
            self.control = Some(s);
            false
        } else {
            true
        }
    }

    /// Same as `remove`, but working on control FSM.
    #[allow(dead_code)]
    pub fn reschedule_control(&mut self, scheduler: &ControlScheduler<N, C>) {
        self.control.take().unwrap().reschedule(scheduler)
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
pub trait PollHandler<N: Fsm, C: Fsm> {
    /// This function is called at the very beginning of every round.
    fn begin(&mut self, batch_size: usize);

    /// This function is called when handling readiness for control FSM.
    ///
    /// Returned value is a hint to show whether the control fsm needs another
    /// round of processing.
    fn handle_control(&mut self, control: &mut Managed<C>) -> bool;

    /// This function is called when handling readiness for normal FSM.
    ///
    /// The returned value is handled in the same way as `handle_control`.
    fn handle_normal(&mut self, normal: &mut Managed<N>) -> bool;

    /// This function is called at the end of every round.
    fn end(&mut self, batch: &mut [Managed<N>]);

    /// This function is called when batch system is going to sleep.
    fn pause(&mut self) {}
}

/// Internal poller that fetches batch and call handler hooks for readiness.
struct Poller<N: Fsm, C: Fsm, Handler> {
    _router: Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>,
    fsm_receiver: channel::Receiver<FsmTypes<N, C>>,
    handler: Handler,
    max_batch_size: usize,
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
        let mut exhausted_fsms = Vec::with_capacity(self.max_batch_size);

        self.fetch_batch(&mut batch, self.max_batch_size);
        while !batch.is_empty() {
            self.handler.begin(batch.len());
            if batch.control.is_some()
                && !self.handler.handle_control(batch.control.as_mut().unwrap())
            {
                batch.release_control();
            }
            if !batch.normals.is_empty() {
                for (i, p) in batch.normals.iter_mut().enumerate() {
                    if !self.handler.handle_normal(p) {
                        exhausted_fsms.push(i);
                    }
                }
            }
            self.handler.end(batch.normals_mut());
            // Because release use `swap_remove` internally, so using pop here
            // to remove the correct FSM.
            while let Some(r) = exhausted_fsms.pop() {
                batch.release(r);
            }
            // Fetch batch after every round is finished. It's helpful to protect regions
            // from becoming hungry if some regions are hot points.
            self.fetch_batch(&mut batch, self.max_batch_size);
        }
    }
}

/// A builder trait that can build up poll handlers.
pub trait HandlerBuilder<N: Fsm, C: Fsm> {
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
                _router: self.router.clone(),
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
