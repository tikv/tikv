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

//! Generally peers are state machines that represent a replica of a region,
//! and store is also a special state machine that handles all requests across
//! stores. They are mixed for now, will be separated in the future.

use super::apply::*;
use super::transport::{BatchSystem as PeerBatchSystem, Router as PeerRouter};
use super::ConfigProvider;
use crossbeam_channel;
use import::SSTImporter;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::metrics::*;
use raftstore::store::{Config, Engines};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::mpsc::SendError;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::{cmp, mem, ptr};
use util::collections::HashMap;
use util::mpsc;
use util::time::{duration_to_sec, SlowTimer};

// TODO: remove this once memory leak is fixed.
const NOTIFYSTATE_NOTIFIED: usize = 0;
const NOTIFYSTATE_PENDING: usize = 1;
const NOTIFYSTATE_CLOSED: usize = 2;

pub enum FsmTypes {
    Empty,
    Apply(Box<ApplyFsm>),
    Fallback(Box<FallbackFsm>),
}

struct State {
    notified: AtomicUsize,
    ptr: AtomicPtr<FsmTypes>,
}

impl State {
    #[inline]
    fn new(fsm: Box<FsmTypes>) -> State {
        State {
            notified: AtomicUsize::new(NOTIFYSTATE_PENDING),
            ptr: AtomicPtr::new(Box::into_raw(fsm)),
        }
    }

    #[inline]
    fn notify(&self, mb: &MailBox<Task>, scheduler: &crossbeam_channel::Sender<Box<FsmTypes>>) {
        match self.maybe_catch_fsm() {
            None => return,
            Some(mut p) => {
                match &mut *p {
                    FsmTypes::Apply(p) => p.mail_box = Some(mb.to_owned()),
                    FsmTypes::Fallback(_) => {}
                    _ => unreachable!(),
                }
                scheduler.send(p)
            }
        }
    }

    #[inline]
    fn release_fsm(&self, fsm: Box<FsmTypes>) {
        let previous = self.ptr.swap(Box::into_raw(fsm), Ordering::AcqRel);
        let mut previous_notified = NOTIFYSTATE_NOTIFIED;
        if previous.is_null() {
            previous_notified = self.notified.compare_and_swap(
                NOTIFYSTATE_NOTIFIED,
                NOTIFYSTATE_PENDING,
                Ordering::AcqRel,
            );
            match previous_notified {
                NOTIFYSTATE_NOTIFIED => return,
                NOTIFYSTATE_CLOSED => {
                    let ptr = self.ptr.swap(ptr::null_mut(), Ordering::AcqRel);
                    unsafe { Box::from_raw(ptr) };
                    return;
                }
                _ => {}
            }
        }
        panic!(
            "invalid release state: {:p} {}",
            previous, previous_notified
        );
    }

    #[inline]
    fn maybe_catch_fsm(&self) -> Option<Box<FsmTypes>> {
        match self.notified.swap(NOTIFYSTATE_NOTIFIED, Ordering::AcqRel) {
            NOTIFYSTATE_NOTIFIED => return None,
            NOTIFYSTATE_PENDING => {}
            _ => return None,
        }

        let p = self.ptr.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            Some(unsafe { Box::from_raw(p) })
        } else {
            unreachable!()
        }
    }

    #[inline]
    fn close(&self) {
        match self.notified.swap(NOTIFYSTATE_CLOSED, Ordering::AcqRel) {
            NOTIFYSTATE_NOTIFIED | NOTIFYSTATE_CLOSED => return,
            _ => {}
        }

        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe { Box::from_raw(ptr) };
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe { Box::from_raw(ptr) };
        }
    }
}

pub struct MailBox<M> {
    sender: mpsc::Sender<M>,
    state: Arc<State>,
}

impl<M> MailBox<M> {
    #[inline]
    fn new(sender: mpsc::Sender<M>, state: Arc<State>) -> MailBox<M> {
        MailBox { sender, state }
    }
}

impl MailBox<Task> {
    #[inline]
    fn force_send(
        &self,
        msg: Task,
        scheduler: &crossbeam_channel::Sender<Box<FsmTypes>>,
    ) -> Result<(), SendError<Task>> {
        self.sender.send(msg)?;
        self.state.notify(self, scheduler);
        Ok(())
    }
}

impl<M> MailBox<M> {
    #[inline]
    fn close(&self) {
        self.sender.close();
        self.state.close();
    }
}

impl<M> Clone for MailBox<M> {
    #[inline]
    fn clone(&self) -> MailBox<M> {
        MailBox {
            sender: self.sender.clone(),
            state: self.state.clone(),
        }
    }
}

pub struct OneshotNotifier {
    waken: Arc<AtomicBool>,
    mail_box: MailBox<Task>,
    scheduler: crossbeam_channel::Sender<Box<FsmTypes>>,
}

impl Drop for OneshotNotifier {
    fn drop(&mut self) {
        self.waken.store(true, Ordering::Release);
        let _ = self.mail_box.force_send(Task::Noop, &self.scheduler);
    }
}

#[derive(Debug)]
pub struct OneshotPoller {
    waken: Arc<AtomicBool>,
}

impl OneshotPoller {
    #[inline]
    pub fn waken(&self) -> bool {
        self.waken.load(Ordering::Acquire)
    }
}

pub struct Router {
    mailboxes: Arc<Mutex<HashMap<u64, MailBox<Task>>>>,
    caches: Cell<HashMap<u64, MailBox<Task>>>,
    scheduler: crossbeam_channel::Sender<Box<FsmTypes>>,
    fallback: MailBox<Task>,
}

impl Router {
    fn new(fallback: MailBox<Task>, scheduler: crossbeam_channel::Sender<Box<FsmTypes>>) -> Router {
        Router {
            mailboxes: Arc::default(),
            caches: Cell::default(),
            scheduler,
            fallback,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(region_id: u64) -> (Router, mpsc::Receiver<Task>) {
        let (tx, _) = mpsc::unbounded();
        let (bs, _) = crossbeam_channel::unbounded();
        let state = State::new(Box::new(FsmTypes::Empty));
        state.notified.store(NOTIFYSTATE_NOTIFIED, Ordering::SeqCst);
        let store_box = MailBox {
            sender: tx,
            state: Arc::new(state),
        };
        let router = Router::new(store_box, bs);
        let (tx, rx) = mpsc::unbounded();
        router.register_mailbox(region_id, tx);
        (router, rx)
    }

    #[cfg(test)]
    pub fn register_mailbox(&self, region_id: u64, sender: mpsc::Sender<Task>) {
        let state = State::new(Box::new(FsmTypes::Empty));
        state.notified.store(NOTIFYSTATE_NOTIFIED, Ordering::SeqCst);
        let mut mail_boxes = self.mailboxes.lock().unwrap();
        mail_boxes.insert(
            region_id,
            MailBox {
                sender,
                state: Arc::new(state),
            },
        );
    }

    fn peer_notifier(&self, region_id: u64) -> Option<MailBox<Task>> {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut disconnected = false;
        if let Some(mail_box) = caches.get(&region_id) {
            disconnected = mail_box.sender.is_alive();
            if !disconnected {
                return Some(mail_box.clone());
            }
        }

        let mail_box = 'fetch_box: {
            let mut boxes = self.mailboxes.lock().unwrap();
            if let Some(mailbox) = boxes.get_mut(&region_id) {
                break 'fetch_box mailbox.clone();
            }
            drop(boxes);
            if disconnected {
                caches.remove(&region_id);
            }
            return None;
        };

        if mail_box.sender.is_alive() {
            caches.insert(region_id, mail_box.clone());
        }

        Some(mail_box)
    }

    pub fn one_shot(&self, region_id: u64) -> (OneshotNotifier, OneshotPoller) {
        let mailbox = self.peer_notifier(region_id).unwrap();
        let waken = Arc::new(AtomicBool::new(false));
        (
            OneshotNotifier {
                waken: waken.clone(),
                mail_box: mailbox,
                scheduler: self.scheduler.clone(),
            },
            OneshotPoller { waken },
        )
    }

    pub fn force_send_task(&self, region_id: u64, t: Task) -> Result<(), SendError<Task>> {
        match self.try_send_task(region_id, t) {
            Ok(()) => Ok(()),
            Err(SendError(t)) => self.fallback.force_send(t, &self.scheduler),
        }
    }

    #[inline]
    pub fn try_send_task(&self, region_id: u64, mut msg: Task) -> Result<(), SendError<Task>> {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut disconnected = false;
        if let Some(mailbox) = caches.get(&region_id) {
            match mailbox.force_send(msg, &self.scheduler) {
                Ok(()) => return Ok(()),
                Err(SendError(m)) => {
                    disconnected = true;
                    msg = m;
                }
            };
        }

        let mailbox = 'fetch_box: {
            let mut boxes = self.mailboxes.lock().unwrap();
            if let Some(mailbox) = boxes.get_mut(&region_id) {
                break 'fetch_box mailbox.clone();
            }
            drop(boxes);
            if disconnected {
                caches.remove(&region_id);
            }
            return Err(SendError(msg));
        };

        match mailbox.force_send(msg, &self.scheduler) {
            r @ Ok(()) => {
                caches.insert(region_id, mailbox);
                r
            }
            r @ Err(SendError(_)) => {
                if disconnected {
                    caches.remove(&region_id);
                }
                r
            }
        }
    }

    pub fn broadcast_shutdown(&self) {
        unsafe { &mut *self.caches.as_ptr() }.clear();
        let mut mailboxes = self.mailboxes.lock().unwrap();
        for (_, mailbox) in mailboxes.drain() {
            mailbox.close();
        }
        for _ in 0..100 {
            self.scheduler.send(Box::new(FsmTypes::Empty));
        }
    }

    pub fn stop(&self, region_id: u64) {
        let mut mailboxes = self.mailboxes.lock().unwrap();
        if let Some(mb) = mailboxes.remove(&region_id) {
            mb.close();
        }
    }
}

impl Clone for Router {
    #[inline]
    fn clone(&self) -> Router {
        Router {
            mailboxes: self.mailboxes.clone(),
            caches: Cell::default(),
            scheduler: self.scheduler.clone(),
            fallback: self.fallback.clone(),
        }
    }
}

pub struct FallbackFsm {
    delegate: FallbackDelegate,
    receiver: mpsc::Receiver<Task>,
}

pub struct ApplyFsm {
    delegate: ApplyDelegate,
    mail_box: Option<MailBox<Task>>,
    receiver: mpsc::Receiver<Task>,
}

struct Batch {
    fsm_holders: Vec<Box<FsmTypes>>,
    applys: Vec<Box<ApplyFsm>>,
    fallback: Option<Box<FallbackFsm>>,
}

impl Batch {
    fn with_capacity(cap: usize) -> Batch {
        Batch {
            fsm_holders: Vec::with_capacity(cap),
            applys: Vec::with_capacity(cap),
            fallback: None,
        }
    }

    #[inline]
    fn push(&mut self, mut fsm: Box<FsmTypes>) -> bool {
        match mem::replace(&mut *fsm, FsmTypes::Empty) {
            FsmTypes::Apply(a) => {
                self.applys.push(a);
            }
            FsmTypes::Fallback(f) => {
                assert!(self.fallback.is_none());
                self.fallback = Some(f);
            }
            FsmTypes::Empty => return false,
        }
        self.fsm_holders.push(fsm);
        true
    }

    fn remove(&mut self, index: usize) {
        self.fsm_holders.pop().unwrap();
        self.applys.swap_remove(index);
    }

    #[inline]
    fn release(&mut self, index: usize, pos: usize) -> bool {
        let mut holder = self.fsm_holders.pop().unwrap();
        let mut apply = self.applys.swap_remove(index);
        let mail_box = apply.mail_box.take().unwrap();
        mem::replace(&mut *holder, FsmTypes::Apply(apply));
        mail_box.state.release_fsm(holder);
        if mail_box.sender.len() == pos {
            true
        } else {
            match mail_box.state.maybe_catch_fsm() {
                None => true,
                Some(mut s) => {
                    match &mut *s {
                        FsmTypes::Apply(p) => p.mail_box = Some(mail_box),
                        _ => unreachable!(),
                    }
                    let last_index = self.applys.len();
                    self.push(s);
                    self.applys.swap(index, last_index);
                    false
                }
            }
        }
    }

    fn release_fallback(&mut self, fallback: &MailBox<Task>, pos: usize) -> bool {
        let mut holder = self.fsm_holders.pop().unwrap();
        let f = self.fallback.take().unwrap();
        mem::replace(&mut *holder, FsmTypes::Fallback(f));
        fallback.state.release_fsm(holder);
        if fallback.sender.len() == pos {
            true
        } else {
            match fallback.state.maybe_catch_fsm() {
                None => true,
                Some(f) => {
                    self.push(f);
                    false
                }
            }
        }
    }

    fn remove_fallback(&mut self) {
        self.fsm_holders.pop();
        self.fallback.take();
    }
}

pub struct Scheduler {
    router: Router,
    batch_receiver: crossbeam_channel::Receiver<Box<FsmTypes>>,
}

impl Scheduler {
    pub fn router(&self) -> &Router {
        &self.router
    }

    fn fetch_batch(&self, batch: &mut Batch, max_size: usize) {
        let mut pushed = match self.batch_receiver.recv() {
            Some(fsm) => batch.push(fsm),
            None => return,
        };
        let mut tried = 1;
        while pushed {
            if tried < max_size {
                let fsm = match self.batch_receiver.try_recv() {
                    Some(fsm) => fsm,
                    None => return,
                };
                pushed = batch.push(fsm);
                tried += 1;
            } else {
                return;
            }
        }
        batch.applys.clear();
        batch.fsm_holders.clear();
    }

    pub fn schedule(&self, delegate: ApplyDelegate) {
        let (sender, receiver) = mpsc::unbounded();
        let region_id = delegate.region_id();
        let fsm = ApplyFsm {
            delegate,
            mail_box: None,
            receiver,
        };
        let s = Arc::new(State::new(Box::new(FsmTypes::Apply(Box::new(fsm)))));
        let mail_box = MailBox::new(sender, s);
        let mut boxes = self.router.mailboxes.lock().unwrap();
        boxes.insert(region_id, mail_box);
    }

    pub fn schedule_all(&self, delegates: Vec<ApplyDelegate>) {
        let mut boxes = self.router.mailboxes.lock().unwrap();
        for delegate in delegates {
            let (sender, receiver) = mpsc::unbounded();
            let region_id = delegate.region_id();
            let fsm = ApplyFsm {
                delegate,
                mail_box: None,
                receiver,
            };
            let state = Arc::new(State::new(Box::new(FsmTypes::Apply(Box::new(fsm)))));
            let mail_box = MailBox::new(sender, state);
            boxes.insert(region_id, mail_box);
        }
    }

    pub fn stop(&self, region_id: u64) {
        self.router.stop(region_id);
    }
}

impl Clone for Scheduler {
    fn clone(&self) -> Scheduler {
        Scheduler {
            router: self.router.clone(),
            batch_receiver: self.batch_receiver.clone(),
        }
    }
}

struct Poller {
    tag: String,
    scheduler: Scheduler,
    cfg: Arc<Config>,
    ctx: PollContext,
    timer: SlowTimer,
}

impl Poller {
    fn poll(&mut self) {
        let mut batch_size = self.cfg.max_batch_size;
        let mut batches = Batch::with_capacity(batch_size);
        let mut msgs = Vec::with_capacity(self.cfg.messages_per_tick);
        let mut exhausted_peers = Vec::with_capacity(self.cfg.messages_per_tick);
        let batch_size_observer = POLL_BATCH_SIZE.local();
        self.scheduler.fetch_batch(&mut batches, batch_size);
        batch_size_observer.observe(batches.fsm_holders.len() as f64);
        while !batches.fsm_holders.is_empty() {
            self.timer = SlowTimer::new();
            if batches.fallback.is_some() {
                let mark = {
                    let mut f = batches.fallback.as_mut().unwrap();
                    let f: &mut FallbackFsm = &mut *f;
                    let mut fallback =
                        FallbackPoller::new(&mut f.delegate, &mut self.ctx, &self.scheduler);
                    fallback.poll(&f.receiver, &mut msgs)
                };
                if batches.fallback.as_ref().unwrap().delegate.stopped {
                    batches.remove_fallback();
                } else if let Some(pos) = mark {
                    batches.release_fallback(&self.scheduler.router.fallback, pos);
                }
            }
            for (i, p) in batches.applys.iter_mut().enumerate() {
                let p: &mut ApplyFsm = &mut *p;
                let mut apply_poller = ApplyPoller::new(&mut p.delegate, &mut self.ctx);
                let mark = apply_poller.poll(&p.receiver, &mut msgs);
                if apply_poller.stopped() {
                    exhausted_peers.push((i, None));
                } else if mark.is_some() {
                    exhausted_peers.push((i, mark));
                }
            }
            self.ctx.flush();
            STORE_APPLY_LOG_HISTOGRAM.observe(duration_to_sec(self.timer.elapsed()) as f64);
            slow_log!(self.timer, "{} handle ready committed entries", self.tag,);

            while let Some((r, mark)) = exhausted_peers.pop() {
                if let Some(pos) = mark {
                    batches.release(r, pos);
                } else {
                    batches.remove(r);
                }
            }
            if batches.fsm_holders.is_empty() {
                batch_size = cmp::min(batch_size + 1, self.cfg.max_batch_size);
                batch_size_observer.flush();
                self.scheduler.fetch_batch(&mut batches, batch_size);
                batch_size_observer.observe(batches.fsm_holders.len() as f64);
            } else {
                batch_size = cmp::max(1, batch_size - 1);
            }
        }
    }
}

pub struct BatchSystem {
    engines: Engines,
    cfg: Arc<Config>,
    router: Router,
    host: Arc<CoprocessorHost>,
    importer: Arc<SSTImporter>,
    delegates: Vec<ApplyDelegate>,
    notifier: PeerRouter,
    tag: String,
    workers: Vec<JoinHandle<()>>,
}

impl BatchSystem {
    pub fn new<T, C>(
        system: &PeerBatchSystem<T, C>,
        notifier: PeerRouter,
        router: Router,
        cfg: Arc<Config>,
    ) -> BatchSystem {
        let mut delegates = Vec::with_capacity(system.get_peers().len());
        for p in system.get_peers() {
            delegates.push(ApplyDelegate::from_peer(&p));
        }
        BatchSystem {
            engines: system.engines().clone(),
            router,
            host: system.coprocessor_host(),
            importer: system.importer(),
            delegates,
            notifier,
            cfg,
            tag: format!("[store {}]", system.store_id()),
            workers: vec![],
        }
    }

    pub fn poll_context(&self) -> PollContext {
        PollContext::new(
            self.engines.clone(),
            self.cfg.clone(),
            self.router.clone(),
            self.notifier.clone(),
            self.host.clone(),
            self.importer.clone(),
        )
    }

    pub fn spawn(
        &mut self,
        batch_receiver: crossbeam_channel::Receiver<Box<FsmTypes>>,
    ) -> ::raftstore::Result<()> {
        let scheduler = Scheduler {
            router: self.router.clone(),
            batch_receiver,
        };

        let delegates = mem::replace(&mut self.delegates, vec![]);
        scheduler.schedule_all(delegates);

        self.workers = Vec::with_capacity(self.cfg.apply_pool_size);
        for i in 0..self.cfg.apply_pool_size {
            let mut poller = Poller {
                scheduler: scheduler.clone(),
                tag: self.tag.clone(),
                cfg: self.cfg.clone(),
                ctx: self.poll_context(),
                timer: SlowTimer::new(),
            };
            self.workers.push(
                thread::Builder::new()
                    .name(thd_name!(format!("apply-{}", i)))
                    .spawn(move || {
                        poller.poll();
                    })
                    .unwrap(),
            );
        }

        Ok(())
    }

    pub fn shutdown(&mut self) {
        info!("{} start to stop apply pool.", self.tag);

        self.router.broadcast_shutdown();

        for h in self.workers.drain(..) {
            debug!("{} stop poller {:?}", self.tag, h.thread().name());
            h.join().unwrap();
        }

        info!("{} stop apply pool finished.", self.tag);
    }
}

pub fn create_router(_: &Config) -> (Router, crossbeam_channel::Receiver<Box<FsmTypes>>) {
    let (fallback_tx, fallback_rx) = mpsc::unbounded();
    let fallback_delegate = FallbackDelegate::new();
    let fallback_fsm = FallbackFsm {
        delegate: fallback_delegate,
        receiver: fallback_rx,
    };
    let state = Arc::new(State::new(Box::new(FsmTypes::Fallback(Box::new(
        fallback_fsm,
    )))));
    let fallback_box = MailBox::new(fallback_tx, state);
    let (tx, rx) = crossbeam_channel::unbounded();
    let router = Router::new(fallback_box, tx);
    (router, rx)
}
