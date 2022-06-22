// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This is the core implementation of a batch system. Generally there will be two
//! different kind of FSMs in TiKV's FSM system. One is normal FSM, which usually
//! represents a peer, the other is control FSM, which usually represents something
//! that controls how the former is created or metrics are collected.

// #[PerformanceCriticalPath]
use std::{
    borrow::Cow,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicUsize, Arc, Mutex},
    thread::{self, current, JoinHandle, ThreadId},
    time::Duration,
};

use crossbeam::channel::{self, SendError};
use fail::fail_point;
use file_system::{set_io_type, IOType};
use tikv_util::{debug, error, info, mpsc, safe_panic, thd_name, time::Instant, warn};

use crate::{
    config::Config,
    fsm::{Fsm, FsmScheduler, Priority},
    mailbox::BasicMailbox,
    router::Router,
};

/// A unify type for FSMs so that they can be sent to channel easily.
pub enum FsmTypes<N, C> {
    Normal(Box<N>),
    Control(Box<C>),
    // Used as a signal that scheduler should be shutdown.
    Empty,
}

// A macro to introduce common definition of scheduler.
macro_rules! impl_sched {
    ($name:ident, $ty:path, Fsm = $fsm:tt) => {
        pub struct $name<N, C> {
            sender: channel::Sender<FsmTypes<N, C>>,
            low_sender: channel::Sender<FsmTypes<N, C>>,
        }

        impl<N, C> Clone for $name<N, C> {
            #[inline]
            fn clone(&self) -> $name<N, C> {
                $name {
                    sender: self.sender.clone(),
                    low_sender: self.low_sender.clone(),
                }
            }
        }

        impl<N, C> FsmScheduler for $name<N, C>
        where
            $fsm: Fsm,
        {
            type Fsm = $fsm;

            #[inline]
            fn schedule(&self, fsm: Box<Self::Fsm>) {
                let sender = match fsm.get_priority() {
                    Priority::Normal => &self.sender,
                    Priority::Low => &self.low_sender,
                };
                match sender.send($ty(fsm)) {
                    Ok(()) => {}
                    // TODO: use debug instead.
                    Err(SendError($ty(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
                    _ => unreachable!(),
                }
            }

            fn shutdown(&self) {
                // TODO: close it explicitly once it's supported.
                // Magic number, actually any number greater than poll pool size works.
                for _ in 0..256 {
                    let _ = self.sender.send(FsmTypes::Empty);
                    let _ = self.low_sender.send(FsmTypes::Empty);
                }
            }
        }
    };
}

impl_sched!(NormalScheduler, FsmTypes::Normal, Fsm = N);
impl_sched!(ControlScheduler, FsmTypes::Control, Fsm = C);

pub struct NormalFsm<N> {
    fsm: Box<N>,
    timer: Instant,
    policy: Option<ReschedulePolicy>,
}

impl<N> NormalFsm<N> {
    #[inline]
    fn new(fsm: Box<N>) -> NormalFsm<N> {
        NormalFsm {
            fsm,
            timer: Instant::now_coarse(),
            policy: None,
        }
    }
}

impl<N> Deref for NormalFsm<N> {
    type Target = N;

    #[inline]
    fn deref(&self) -> &N {
        &self.fsm
    }
}

impl<N> DerefMut for NormalFsm<N> {
    #[inline]
    fn deref_mut(&mut self) -> &mut N {
        &mut self.fsm
    }
}

/// A basic struct for a round of polling.
#[allow(clippy::vec_box)]
pub struct Batch<N, C> {
    normals: Vec<Option<NormalFsm<N>>>,
    control: Option<Box<C>>,
}

impl<N: Fsm, C: Fsm> Batch<N, C> {
    /// Create a a batch with given batch size.
    pub fn with_capacity(cap: usize) -> Batch<N, C> {
        Batch {
            normals: Vec::with_capacity(cap),
            control: None,
        }
    }

    fn push(&mut self, fsm: FsmTypes<N, C>) -> bool {
        match fsm {
            FsmTypes::Normal(n) => {
                self.normals.push(Some(NormalFsm::new(n)));
            }
            FsmTypes::Control(c) => {
                assert!(self.control.is_none());
                self.control = Some(c);
            }
            FsmTypes::Empty => return false,
        }
        true
    }

    fn is_empty(&self) -> bool {
        self.normals.is_empty() && self.control.is_none()
    }

    fn clear(&mut self) {
        self.normals.clear();
        self.control.take();
    }

    /// Put back the FSM located at index.
    ///
    /// Only when channel length is larger than `checked_len` will trigger
    /// further notification. This function may fail if channel length is
    /// larger than the given value before FSM is released.
    fn release(&mut self, mut fsm: NormalFsm<N>, checked_len: usize) -> Option<NormalFsm<N>> {
        let mailbox = fsm.take_mailbox().unwrap();
        mailbox.release(fsm.fsm);
        if mailbox.len() == checked_len {
            None
        } else {
            match mailbox.take_fsm() {
                // It's rescheduled by other thread.
                None => None,
                Some(mut s) => {
                    s.set_mailbox(Cow::Owned(mailbox));
                    fsm.fsm = s;
                    Some(fsm)
                }
            }
        }
    }

    /// Remove the normal FSM located at `index`.
    ///
    /// This method should only be called when the FSM is stopped.
    /// If there are still messages in channel, the FSM is untouched and
    /// the function will return false to let caller to keep polling.
    fn remove(&mut self, mut fsm: NormalFsm<N>) -> Option<NormalFsm<N>> {
        let mailbox = fsm.take_mailbox().unwrap();
        if mailbox.is_empty() {
            // It will be removed only when it's already closed, so no new messages can
            // be scheduled, hence don't need to consider rescheduling.
            mailbox.release(fsm.fsm);
            None
        } else {
            fsm.set_mailbox(Cow::Owned(mailbox));
            Some(fsm)
        }
    }

    /// Schedule the normal FSM located at `index`.
    ///
    /// If `inplace`, the relative position of all fsm will not be changed; otherwise, the fsm
    /// will be popped and the last fsm will be swap in to reduce memory copy.
    pub fn schedule(&mut self, router: &BatchRouter<N, C>, index: usize, inplace: bool) {
        let to_schedule = match self.normals[index].take() {
            Some(f) => f,
            None => {
                if !inplace {
                    self.normals.swap_remove(index);
                }
                return;
            }
        };
        let mut res = match to_schedule.policy {
            Some(ReschedulePolicy::Release(l)) => self.release(to_schedule, l),
            Some(ReschedulePolicy::Remove) => self.remove(to_schedule),
            Some(ReschedulePolicy::Schedule) => {
                router.normal_scheduler.schedule(to_schedule.fsm);
                None
            }
            None => Some(to_schedule),
        };
        if let Some(f) = &mut res {
            // failed to reschedule
            f.policy.take();
            self.normals[index] = res;
        } else if !inplace {
            self.normals.swap_remove(index);
        }
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

/// The result for `PollHandler::handle_control`.
pub enum HandleResult {
    /// The Fsm still needs to be processed.
    KeepProcessing,
    /// The Fsm should stop at the progress.
    StopAt {
        /// The count of messages that have been acknowledged by handler. The fsm should be
        /// released until new messages arrive.
        progress: usize,
        /// Whether the fsm should be released before `end`.
        skip_end: bool,
    },
}

impl HandleResult {
    #[inline]
    pub fn stop_at(progress: usize, skip_end: bool) -> HandleResult {
        HandleResult::StopAt { progress, skip_end }
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
pub trait PollHandler<N, C>: Send + 'static {
    /// This function is called at the very beginning of every round.
    fn begin<F>(&mut self, _batch_size: usize, update_cfg: F)
    where
        for<'a> F: FnOnce(&'a Config);

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
    fn handle_normal(&mut self, normal: &mut impl DerefMut<Target = N>) -> HandleResult;

    /// This function is called after `handle_normal` is called for all fsm and before calling
    /// `end`. The function is expected to run lightweight work.
    fn light_end(&mut self, _batch: &mut [Option<impl DerefMut<Target = N>>]) {}

    /// This function is called at the end of every round.
    fn end(&mut self, batch: &mut [Option<impl DerefMut<Target = N>>]);

    /// This function is called when batch system is going to sleep.
    fn pause(&mut self) {}

    /// This function returns the priority of this handler.
    fn get_priority(&self) -> Priority {
        Priority::Normal
    }
}

/// Internal poller that fetches batch and call handler hooks for readiness.
pub struct Poller<N: Fsm, C: Fsm, Handler> {
    pub router: Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>,
    pub fsm_receiver: channel::Receiver<FsmTypes<N, C>>,
    pub handler: Handler,
    pub max_batch_size: usize,
    pub reschedule_duration: Duration,
    pub joinable_workers: Option<Arc<Mutex<Vec<ThreadId>>>>,
}

impl<N, C, Handler> Drop for Poller<N, C, Handler>
where
    N: Fsm,
    C: Fsm,
{
    fn drop(&mut self) {
        if let Some(joinable_workers) = &self.joinable_workers {
            joinable_workers.lock().unwrap().push(current().id());
        }
    }
}

enum ReschedulePolicy {
    Release(usize),
    Remove,
    Schedule,
}

impl<N: Fsm, C: Fsm, Handler: PollHandler<N, C>> Poller<N, C, Handler> {
    fn fetch_fsm(&mut self, batch: &mut Batch<N, C>) -> bool {
        if batch.control.is_some() {
            return true;
        }

        if let Ok(fsm) = self.fsm_receiver.try_recv() {
            return batch.push(fsm);
        }

        if batch.is_empty() {
            self.handler.pause();
            if let Ok(fsm) = self.fsm_receiver.recv() {
                return batch.push(fsm);
            }
        }
        !batch.is_empty()
    }

    // Poll for readiness and forward to handler. Remove stale peer if necessary.
    pub fn poll(&mut self) {
        fail_point!("poll");
        let mut batch = Batch::with_capacity(self.max_batch_size);
        let mut reschedule_fsms = Vec::with_capacity(self.max_batch_size);
        let mut to_skip_end = Vec::with_capacity(self.max_batch_size);

        // Fetch batch after every round is finished. It's helpful to protect regions
        // from becoming hungry if some regions are hot points. Since we fetch new fsm every time
        // calling `poll`, we do not need to configure a large value for `self.max_batch_size`.
        let mut run = true;
        while run && self.fetch_fsm(&mut batch) {
            // If there is some region wait to be deal, we must deal with it even if it has overhead
            // max size of batch. It's helpful to protect regions from becoming hungry
            // if some regions are hot points.
            let mut max_batch_size = std::cmp::max(self.max_batch_size, batch.normals.len());
            // update some online config if needed.
            {
                // TODO: rust 2018 does not support capture disjoint field within a closure.
                // See https://github.com/rust-lang/rust/issues/53488 for more details.
                // We can remove this once we upgrade to rust 2021 or later edition.
                let batch_size = &mut self.max_batch_size;
                self.handler.begin(max_batch_size, |cfg| {
                    *batch_size = cfg.max_batch_size();
                });
            }
            max_batch_size = std::cmp::max(self.max_batch_size, batch.normals.len());

            if batch.control.is_some() {
                let len = self.handler.handle_control(batch.control.as_mut().unwrap());
                if batch.control.as_ref().unwrap().is_stopped() {
                    batch.remove_control(&self.router.control_box);
                } else if let Some(len) = len {
                    batch.release_control(&self.router.control_box, len);
                }
            }

            let mut hot_fsm_count = 0;
            for (i, p) in batch.normals.iter_mut().enumerate() {
                let p = p.as_mut().unwrap();
                let res = self.handler.handle_normal(p);
                if p.is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    reschedule_fsms.push(i);
                } else if p.get_priority() != self.handler.get_priority() {
                    p.policy = Some(ReschedulePolicy::Schedule);
                    reschedule_fsms.push(i);
                } else {
                    if p.timer.saturating_elapsed() >= self.reschedule_duration {
                        hot_fsm_count += 1;
                        // We should only reschedule a half of the hot regions, otherwise,
                        // it's possible all the hot regions are fetched in a batch the
                        // next time.
                        if hot_fsm_count % 2 == 0 {
                            p.policy = Some(ReschedulePolicy::Schedule);
                            reschedule_fsms.push(i);
                            continue;
                        }
                    }
                    if let HandleResult::StopAt { progress, skip_end } = res {
                        p.policy = Some(ReschedulePolicy::Release(progress));
                        reschedule_fsms.push(i);
                        if skip_end {
                            to_skip_end.push(i);
                        }
                    }
                }
            }
            let mut fsm_cnt = batch.normals.len();
            while batch.normals.len() < max_batch_size {
                if let Ok(fsm) = self.fsm_receiver.try_recv() {
                    run = batch.push(fsm);
                }
                // If we receive a ControlFsm, break this cycle and call `end`. Because ControlFsm
                // may change state of the handler, we shall deal with it immediately after
                // calling `begin` of `Handler`.
                if !run || fsm_cnt >= batch.normals.len() {
                    break;
                }
                let p = batch.normals[fsm_cnt].as_mut().unwrap();
                let res = self.handler.handle_normal(p);
                if p.is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    reschedule_fsms.push(fsm_cnt);
                } else if let HandleResult::StopAt { progress, skip_end } = res {
                    p.policy = Some(ReschedulePolicy::Release(progress));
                    reschedule_fsms.push(fsm_cnt);
                    if skip_end {
                        to_skip_end.push(fsm_cnt);
                    }
                }
                fsm_cnt += 1;
            }
            self.handler.light_end(&mut batch.normals);
            for offset in &to_skip_end {
                batch.schedule(&self.router, *offset, true);
            }
            to_skip_end.clear();
            self.handler.end(&mut batch.normals);

            // Because release use `swap_remove` internally, so using pop here
            // to remove the correct FSM.
            while let Some(r) = reschedule_fsms.pop() {
                batch.schedule(&self.router, r, false);
            }
        }
        if let Some(fsm) = batch.control.take() {
            self.router.control_scheduler.schedule(fsm);
            info!("poller will exit, release the left ControlFsm");
        }
        let left_fsm_cnt = batch.normals.len();
        if left_fsm_cnt > 0 {
            info!(
                "poller will exit, schedule {} left NormalFsms",
                left_fsm_cnt
            );
            for i in 0..left_fsm_cnt {
                let to_schedule = match batch.normals[i].take() {
                    Some(f) => f,
                    None => continue,
                };
                self.router.normal_scheduler.schedule(to_schedule.fsm);
            }
        }
        batch.clear();
    }
}

/// A builder trait that can build up poll handlers.
pub trait HandlerBuilder<N, C> {
    type Handler: PollHandler<N, C>;

    fn build(&mut self, priority: Priority) -> Self::Handler;
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
    low_receiver: channel::Receiver<FsmTypes<N, C>>,
    pool_size: usize,
    max_batch_size: usize,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    joinable_workers: Arc<Mutex<Vec<ThreadId>>>,
    reschedule_duration: Duration,
    low_priority_pool_size: usize,
    pool_state_builder: Option<PoolStateBuilder<N, C>>,
}

impl<N, C> BatchSystem<N, C>
where
    N: Fsm + Send + 'static,
    C: Fsm + Send + 'static,
{
    pub fn router(&self) -> &BatchRouter<N, C> {
        &self.router
    }

    pub fn build_pool_state<H: HandlerBuilder<N, C>>(
        &mut self,
        handler_builder: H,
    ) -> PoolState<N, C, H> {
        let pool_state_builder = self.pool_state_builder.take().unwrap();
        pool_state_builder.build(
            self.name_prefix.as_ref().unwrap().clone(),
            self.low_priority_pool_size,
            self.workers.clone(),
            self.joinable_workers.clone(),
            handler_builder,
            self.pool_size,
        )
    }

    fn start_poller<B>(&mut self, name: String, priority: Priority, builder: &mut B)
    where
        B: HandlerBuilder<N, C>,
        B::Handler: Send + 'static,
    {
        let handler = builder.build(priority);
        let receiver = match priority {
            Priority::Normal => self.receiver.clone(),
            Priority::Low => self.low_receiver.clone(),
        };
        let mut poller = Poller {
            router: self.router.clone(),
            fsm_receiver: receiver,
            handler,
            max_batch_size: self.max_batch_size,
            reschedule_duration: self.reschedule_duration,
            joinable_workers: if priority == Priority::Normal {
                Some(Arc::clone(&self.joinable_workers))
            } else {
                None
            },
        };
        let props = tikv_util::thread_group::current_properties();
        let t = thread::Builder::new()
            .name(name)
            .spawn(move || {
                tikv_util::thread_group::set_properties(props);
                set_io_type(IOType::ForegroundWrite);
                poller.poll();
            })
            .unwrap();
        self.workers.lock().unwrap().push(t);
    }

    /// Start the batch system.
    pub fn spawn<B>(&mut self, name_prefix: String, mut builder: B)
    where
        B: HandlerBuilder<N, C>,
        B::Handler: Send + 'static,
    {
        for i in 0..self.pool_size {
            self.start_poller(
                thd_name!(format!("{}-{}", name_prefix, i)),
                Priority::Normal,
                &mut builder,
            );
        }
        for i in 0..self.low_priority_pool_size {
            self.start_poller(
                thd_name!(format!("{}-low-{}", name_prefix, i)),
                Priority::Low,
                &mut builder,
            );
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
        let mut last_error = None;
        for h in self.workers.lock().unwrap().drain(..) {
            debug!("waiting for {}", h.thread().name().unwrap());
            if let Err(e) = h.join() {
                error!("failed to join worker thread: {:?}", e);
                last_error = Some(e);
            }
        }
        if let Some(e) = last_error {
            safe_panic!("failed to join worker thread: {:?}", e);
        }
        info!("batch system {} is stopped.", name_prefix);
    }
}

struct PoolStateBuilder<N, C> {
    max_batch_size: usize,
    reschedule_duration: Duration,
    fsm_receiver: channel::Receiver<FsmTypes<N, C>>,
    fsm_sender: channel::Sender<FsmTypes<N, C>>,
    pool_size: usize,
}

impl<N, C> PoolStateBuilder<N, C> {
    fn build<H: HandlerBuilder<N, C>>(
        self,
        name_prefix: String,
        low_priority_pool_size: usize,
        workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
        joinable_workers: Arc<Mutex<Vec<ThreadId>>>,
        handler_builder: H,
        id_base: usize,
    ) -> PoolState<N, C, H> {
        PoolState {
            name_prefix,
            handler_builder,
            fsm_receiver: self.fsm_receiver,
            fsm_sender: self.fsm_sender,
            low_priority_pool_size,
            workers,
            joinable_workers,
            expected_pool_size: self.pool_size,
            max_batch_size: self.max_batch_size,
            reschedule_duration: self.reschedule_duration,
            id_base,
        }
    }
}

pub struct PoolState<N, C, H: HandlerBuilder<N, C>> {
    pub name_prefix: String,
    pub handler_builder: H,
    pub fsm_receiver: channel::Receiver<FsmTypes<N, C>>,
    pub fsm_sender: channel::Sender<FsmTypes<N, C>>,
    pub low_priority_pool_size: usize,
    pub expected_pool_size: usize,
    pub workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    pub joinable_workers: Arc<Mutex<Vec<ThreadId>>>,
    pub max_batch_size: usize,
    pub reschedule_duration: Duration,
    pub id_base: usize,
}

pub type BatchRouter<N, C> = Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>;

/// Create a batch system with the given thread name prefix and pool size.
///
/// `sender` and `controller` should be paired.
pub fn create_system<N: Fsm, C: Fsm>(
    cfg: &Config,
    sender: mpsc::LooseBoundedSender<C::Message>,
    controller: Box<C>,
) -> (BatchRouter<N, C>, BatchSystem<N, C>) {
    let state_cnt = Arc::new(AtomicUsize::new(0));
    let control_box = BasicMailbox::new(sender, controller, state_cnt.clone());
    let (tx, rx) = channel::unbounded();
    let (tx2, rx2) = channel::unbounded();
    let normal_scheduler = NormalScheduler {
        sender: tx.clone(),
        low_sender: tx2.clone(),
    };
    let control_scheduler = ControlScheduler {
        sender: tx.clone(),
        low_sender: tx2,
    };
    let pool_state_builder = PoolStateBuilder {
        max_batch_size: cfg.max_batch_size(),
        reschedule_duration: cfg.reschedule_duration.0,
        fsm_receiver: rx.clone(),
        fsm_sender: tx,
        pool_size: cfg.pool_size,
    };
    let router = Router::new(control_box, normal_scheduler, control_scheduler, state_cnt);
    let system = BatchSystem {
        name_prefix: None,
        router: router.clone(),
        receiver: rx,
        low_receiver: rx2,
        pool_size: cfg.pool_size,
        max_batch_size: cfg.max_batch_size(),
        workers: Arc::new(Mutex::new(Vec::new())),
        joinable_workers: Arc::new(Mutex::new(Vec::new())),
        reschedule_duration: cfg.reschedule_duration.0,
        low_priority_pool_size: cfg.low_priority_pool_size,
        pool_state_builder: Some(pool_state_builder),
    };
    (router, system)
}
