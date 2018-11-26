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

use super::{ConfigProvider, GlobalStoreStat, LocalStoreStat, Peer, StoreMeta};
use crossbeam_channel;
use futures_cpupool::CpuPool;
use import::SSTImporter;
use kvproto::metapb;
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::{PeerState, RaftMessage, RegionLocalState};
use pd::{PdClient, PdRunner, PdTask};
use protobuf;
use raft::Ready;
use raftstore::coprocessor::split_observer::SplitObserver;
use raftstore::coprocessor::{CoprocessorHost, RegionChangeEvent};
use raftstore::store::engine::{Iterable, Mutable, Peekable};
use raftstore::store::fsm::apply_transport::{
    self, BatchSystem as ApplyBatchSystem, FsmTypes as ApplyFsmTypes, Router as ApplyRouter,
};
use raftstore::store::fsm::store::{Store, StoreCore};
use raftstore::store::fsm::{ApplyTask, RegionProposal};
use raftstore::store::keys::{self, enc_end_key};
use raftstore::store::local_metrics::RaftMetrics;
use raftstore::store::metrics::RAFTSTORE_CHANNEL_FULL;
use raftstore::store::metrics::*;
use raftstore::store::peer_storage::{self, HandleRaftReadyContext, InvokeContext};
use raftstore::store::worker::{
    CleanupSSTRunner, CleanupSSTTask, CompactRunner, CompactTask, ConsistencyCheckRunner,
    ConsistencyCheckTask, LocalReader, RaftlogGcRunner, RaftlogGcTask, ReadTask, RegionRunner,
    RegionTask, SplitCheckRunner, SplitCheckTask,
};
use raftstore::store::{
    peer, Callback, Config, Engines, PeerMsg, SnapManager, StoreMsg, Transport,
};
use rocksdb::{WriteBatch, WriteOptions};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::mpsc::{SendError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::{cmp, mem, ptr};
use storage::CF_RAFT;
use tokio_timer::timer::Handle;
use util::collections::{HashMap, HashSet};
use util::mpsc;
use util::rocksdb;
use util::sys as util_sys;
use util::time::{duration_to_sec, SlowTimer};
use util::timer::GLOBAL_TIMER_HANDLE;
use util::worker::{FutureScheduler, FutureWorker, Scheduler as WorkerScheduler, Worker};
use util::Either;

// TODO: remove this once memory leak is fixed.
const NOTIFYSTATE_NOTIFIED: usize = 0;
const NOTIFYSTATE_PENDING: usize = 1;
const NOTIFYSTATE_CLOSED: usize = 2;

pub enum FsmTypes {
    Empty,
    Peer(Box<PeerFsm>),
    Store(Box<StoreFsm>),
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
    fn notify_peer(
        &self,
        mb: &MailBox<PeerMsg>,
        scheduler: &crossbeam_channel::Sender<Box<FsmTypes>>,
    ) {
        match self.maybe_catch_fsm() {
            None => return,
            Some(mut p) => {
                match &mut *p {
                    FsmTypes::Peer(p) => p.mail_box = Some(mb.to_owned()),
                    _ => unreachable!(),
                }
                scheduler.send(p)
            }
        }
    }

    #[inline]
    fn notify_store(&self, scheduler: &crossbeam_channel::Sender<Box<FsmTypes>>) {
        match self.maybe_catch_fsm() {
            None => return,
            Some(p) => {
                match &*p {
                    FsmTypes::Store(_) => {}
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
    sender: mpsc::LooseBoundedSender<M>,
    state: Arc<State>,
}

impl<M> MailBox<M> {
    #[inline]
    fn new(sender: mpsc::LooseBoundedSender<M>, state: Arc<State>) -> MailBox<M> {
        MailBox { sender, state }
    }
}

impl MailBox<PeerMsg> {
    #[inline]
    fn force_send(
        &self,
        msg: PeerMsg,
        scheduler: &crossbeam_channel::Sender<Box<FsmTypes>>,
    ) -> Result<(), SendError<PeerMsg>> {
        self.sender.force_send(msg)?;
        self.state.notify_peer(self, scheduler);
        Ok(())
    }

    #[inline]
    fn try_send(
        &self,
        msg: PeerMsg,
        scheduler: &crossbeam_channel::Sender<Box<FsmTypes>>,
    ) -> Result<(), TrySendError<PeerMsg>> {
        self.sender.try_send(msg)?;
        self.state.notify_peer(self, scheduler);
        Ok(())
    }
}

impl MailBox<StoreMsg> {
    #[inline]
    fn force_send(
        &self,
        msg: StoreMsg,
        scheduler: &crossbeam_channel::Sender<Box<FsmTypes>>,
    ) -> Result<(), SendError<StoreMsg>> {
        self.sender.force_send(msg)?;
        self.state.notify_store(scheduler);
        Ok(())
    }

    #[inline]
    fn try_send(
        &self,
        msg: StoreMsg,
        scheduler: &crossbeam_channel::Sender<Box<FsmTypes>>,
    ) -> Result<(), TrySendError<StoreMsg>> {
        self.sender.try_send(msg)?;
        self.state.notify_store(scheduler);
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
    mail_box: MailBox<PeerMsg>,
    scheduler: crossbeam_channel::Sender<Box<FsmTypes>>,
}

impl Drop for OneshotNotifier {
    fn drop(&mut self) {
        self.waken.store(true, Ordering::Release);
        let _ = self.mail_box.force_send(PeerMsg::Noop, &self.scheduler);
    }
}

pub struct OneshotPoller {
    waken: Arc<AtomicBool>,
}

impl OneshotPoller {
    #[inline]
    pub fn waken(&self) -> bool {
        self.waken.load(Ordering::Acquire)
    }
}

pub struct PeerNotifier {
    mail_box: MailBox<PeerMsg>,
    scheduler: crossbeam_channel::Sender<Box<FsmTypes>>,
}

impl PeerNotifier {
    #[inline]
    pub fn force_send(&self, msg: PeerMsg) -> Result<(), SendError<PeerMsg>> {
        self.mail_box.force_send(msg, &self.scheduler)
    }

    #[inline]
    pub fn try_send(&self, msg: PeerMsg) -> Result<(), TrySendError<PeerMsg>> {
        self.mail_box.try_send(msg, &self.scheduler)
    }
}

pub struct StoreNotifier {
    mail_box: MailBox<StoreMsg>,
    scheduler: crossbeam_channel::Sender<Box<FsmTypes>>,
}

impl StoreNotifier {
    #[inline]
    pub fn force_send(&self, msg: StoreMsg) -> Result<(), SendError<StoreMsg>> {
        self.mail_box.force_send(msg, &self.scheduler)
    }

    #[inline]
    pub fn try_send(&self, msg: StoreMsg) -> Result<(), TrySendError<StoreMsg>> {
        self.mail_box.try_send(msg, &self.scheduler)
    }
}

pub struct Router {
    mailboxes: Arc<Mutex<HashMap<u64, MailBox<PeerMsg>>>>,
    caches: Cell<HashMap<u64, MailBox<PeerMsg>>>,
    store_box: MailBox<StoreMsg>,
    scheduler: crossbeam_channel::Sender<Box<FsmTypes>>,
}

impl Router {
    fn new(
        store_box: MailBox<StoreMsg>,
        scheduler: crossbeam_channel::Sender<Box<FsmTypes>>,
    ) -> Router {
        Router {
            mailboxes: Arc::default(),
            caches: Cell::default(),
            store_box,
            scheduler,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(region_id: u64) -> (Router, mpsc::Receiver<PeerMsg>) {
        let (tx, _) = mpsc::loose_bounded(10);
        let (bs, _) = crossbeam_channel::unbounded();
        let state = State::new(Box::new(FsmTypes::Empty));
        state.notified.store(NOTIFYSTATE_NOTIFIED, Ordering::SeqCst);
        let store_box = MailBox {
            sender: tx,
            state: Arc::new(state),
        };
        let router = Router::new(store_box, bs);
        let (tx, rx) = mpsc::loose_bounded(10);
        router.register_mailbox(region_id, tx);
        (router, rx)
    }

    #[cfg(test)]
    pub fn register_mailbox(&self, region_id: u64, sender: mpsc::LooseBoundedSender<PeerMsg>) {
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

    pub fn one_shot(&self, region_id: u64) -> (OneshotNotifier, OneshotPoller) {
        let peer_notifier = self.peer_notifier(region_id).unwrap();
        let waken = Arc::new(AtomicBool::new(false));
        (
            OneshotNotifier {
                waken: waken.clone(),
                mail_box: peer_notifier.mail_box,
                scheduler: self.scheduler.clone(),
            },
            OneshotPoller { waken },
        )
    }

    pub fn send_raft_message(&self, mut msg: RaftMessage) -> Result<(), TrySendError<RaftMessage>> {
        let id = msg.get_region_id();
        match self.try_send_peer_message(id, PeerMsg::RaftMessage(msg)) {
            Either::Left(Ok(())) => return Ok(()),
            Either::Left(Err(TrySendError::Full(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Full(m))
            }
            Either::Left(Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Disconnected(m))
            }
            Either::Right(PeerMsg::RaftMessage(m)) => msg = m,
            _ => unreachable!(),
        }
        match self.send_store_message(StoreMsg::RaftMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(StoreMsg::RaftMessage(m))) => Err(TrySendError::Full(m)),
            Err(TrySendError::Disconnected(StoreMsg::RaftMessage(m))) => {
                Err(TrySendError::Disconnected(m))
            }
            _ => unreachable!(),
        }
    }

    pub fn send_cmd(
        &self,
        req: RaftCmdRequest,
        cb: Callback,
    ) -> Result<(), TrySendError<(RaftCmdRequest, Callback)>> {
        let id = req.get_header().get_region_id();
        match self.send_peer_message(id, PeerMsg::new_raft_cmd(req, cb)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(PeerMsg::RaftCmd {
                request, callback, ..
            })) => Err(TrySendError::Full((request, callback))),
            Err(TrySendError::Disconnected(PeerMsg::RaftCmd {
                request, callback, ..
            })) => Err(TrySendError::Disconnected((request, callback))),
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn send_peer_message(
        &self,
        region_id: u64,
        msg: PeerMsg,
    ) -> Result<(), TrySendError<PeerMsg>> {
        match self.try_send_peer_message(region_id, msg) {
            Either::Left(res) => res,
            Either::Right(m) => Err(TrySendError::Disconnected(m)),
        }
    }

    #[inline]
    pub fn force_send_peer_message(
        &self,
        region_id: u64,
        msg: PeerMsg,
    ) -> Result<(), SendError<PeerMsg>> {
        match self.send_peer_message(region_id, msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(m)) => {
                let caches = unsafe { &mut *self.caches.as_ptr() };
                caches[&region_id].force_send(m, &self.scheduler)
            }
            Err(TrySendError::Disconnected(m)) => Err(SendError(m)),
        }
    }

    #[inline]
    pub fn try_send_peer_message(
        &self,
        region_id: u64,
        mut msg: PeerMsg,
    ) -> Either<Result<(), TrySendError<PeerMsg>>, PeerMsg> {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut disconnected = false;
        if let Some(mailbox) = caches.get(&region_id) {
            match mailbox.try_send(msg, &self.scheduler) {
                Ok(()) => return Either::Left(Ok(())),
                Err(TrySendError::Full(m)) => {
                    RAFTSTORE_CHANNEL_FULL.inc();
                    return Either::Left(Err(TrySendError::Full(m)));
                }
                Err(TrySendError::Disconnected(m)) => {
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
            return Either::Right(msg);
        };

        match mailbox.try_send(msg, &self.scheduler) {
            r @ Ok(()) => {
                caches.insert(region_id, mailbox);
                Either::Left(r)
            }
            r @ Err(TrySendError::Full(_)) => {
                RAFTSTORE_CHANNEL_FULL.inc();
                caches.insert(region_id, mailbox);
                Either::Left(r)
            }
            r @ Err(TrySendError::Disconnected(_)) => {
                if disconnected {
                    caches.remove(&region_id);
                }
                Either::Left(r)
            }
        }
    }

    #[inline]
    pub fn send_store_message(&self, msg: StoreMsg) -> Result<(), TrySendError<StoreMsg>> {
        match self.store_box.try_send(msg, &self.scheduler) {
            Ok(()) => Ok(()),
            r @ Err(TrySendError::Full(_)) => {
                RAFTSTORE_CHANNEL_FULL.inc();
                r
            }
            r => r,
        }
    }

    pub fn broadcast_shutdown(&self) {
        info!("broadcasting shutdown.");
        self.store_box.close();
        unsafe { &mut *self.caches.as_ptr() }.clear();
        let mut mailboxes = self.mailboxes.lock().unwrap();
        for (region_id, mailbox) in mailboxes.drain() {
            debug!("[region {}] shutdown mailbox", region_id);
            mailbox.close();
        }
        for _ in 0..100 {
            self.scheduler.send(Box::new(FsmTypes::Empty));
        }
    }

    pub fn stop(&self, region_id: u64) {
        info!("[region {}] shutdown mailbox", region_id);
        let mut mailboxes = self.mailboxes.lock().unwrap();
        if let Some(mb) = mailboxes.remove(&region_id) {
            mb.close();
        }
    }

    pub fn peer_notifier(&self, region_id: u64) -> Option<PeerNotifier> {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut connected = false;
        if let Some(mail_box) = caches.get(&region_id) {
            connected = mail_box.sender.is_alive();
            if connected {
                return Some(PeerNotifier {
                    mail_box: mail_box.clone(),
                    scheduler: self.scheduler.clone(),
                });
            }
        }

        let mail_box = 'fetch_box: {
            let mut boxes = self.mailboxes.lock().unwrap();
            if let Some(mailbox) = boxes.get_mut(&region_id) {
                break 'fetch_box mailbox.clone();
            }
            drop(boxes);
            if !connected {
                caches.remove(&region_id);
            }
            return None;
        };

        if mail_box.sender.is_alive() {
            caches.insert(region_id, mail_box.clone());
        }

        Some(PeerNotifier {
            mail_box,
            scheduler: self.scheduler.clone(),
        })
    }

    pub fn store_notifier(&self) -> StoreNotifier {
        StoreNotifier {
            mail_box: self.store_box.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

impl Clone for Router {
    #[inline]
    fn clone(&self) -> Router {
        Router {
            mailboxes: self.mailboxes.clone(),
            caches: Cell::default(),
            store_box: self.store_box.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

pub struct PeerFsm {
    peer: peer::Peer,
    mail_box: Option<MailBox<PeerMsg>>,
    receiver: mpsc::Receiver<PeerMsg>,
}

impl Drop for PeerFsm {
    fn drop(&mut self) {
        self.peer.stop();
    }
}

pub struct StoreFsm {
    store: StoreCore,
    receiver: mpsc::Receiver<StoreMsg>,
}

struct Batch {
    fsm_holders: Vec<Box<FsmTypes>>,
    peers: Vec<Box<PeerFsm>>,
    store: Option<Box<StoreFsm>>,
}

impl Batch {
    fn with_capacity(cap: usize) -> Batch {
        Batch {
            fsm_holders: Vec::with_capacity(cap),
            peers: Vec::with_capacity(cap),
            store: None,
        }
    }

    #[inline]
    fn push(&mut self, mut fsm: Box<FsmTypes>) -> bool {
        match mem::replace(&mut *fsm, FsmTypes::Empty) {
            FsmTypes::Peer(p) => {
                self.peers.push(p);
            }
            FsmTypes::Store(s) => {
                assert!(self.store.is_none());
                self.store = Some(s);
            }
            FsmTypes::Empty => return false,
        }
        self.fsm_holders.push(fsm);
        true
    }

    fn remove_peer(&mut self, index: usize) {
        let mut holder = self.fsm_holders.pop().unwrap();
        let mut peer = self.peers.swap_remove(index);
        let mail_box = peer.mail_box.take().unwrap();
        if mail_box.sender.is_empty() {
            mem::replace(&mut *holder, FsmTypes::Peer(peer));
            mail_box.state.release_fsm(holder);
        } else {
            peer.mail_box = Some(mail_box);
            mem::replace(&mut *holder, FsmTypes::Peer(peer));
            let last_index = self.peers.len();
            self.push(holder);
            self.peers.swap(index, last_index);
        }
    }

    #[inline]
    fn release_peer(&mut self, index: usize, pos: usize) -> bool {
        let mut holder = self.fsm_holders.pop().unwrap();
        let mut peer = self.peers.swap_remove(index);
        let mail_box = peer.mail_box.take().unwrap();
        mem::replace(&mut *holder, FsmTypes::Peer(peer));
        mail_box.state.release_fsm(holder);
        if mail_box.sender.len() == pos {
            true
        } else {
            match mail_box.state.maybe_catch_fsm() {
                None => true,
                Some(mut s) => {
                    match &mut *s {
                        FsmTypes::Peer(p) => p.mail_box = Some(mail_box),
                        _ => unreachable!(),
                    }
                    let last_index = self.peers.len();
                    self.push(s);
                    self.peers.swap(index, last_index);
                    false
                }
            }
        }
    }

    #[inline]
    fn release_store(&mut self, mail_box: &MailBox<StoreMsg>, pos: usize) -> bool {
        let mut holder = self.fsm_holders.pop().unwrap();
        let s = self.store.take().unwrap();
        mem::replace(&mut *holder, FsmTypes::Store(s));
        mail_box.state.release_fsm(holder);
        if mail_box.sender.len() == pos {
            true
        } else {
            match mail_box.state.maybe_catch_fsm() {
                None => true,
                Some(s) => {
                    self.push(s);
                    false
                }
            }
        }
    }

    fn remove_store(&mut self, mail_box: &MailBox<StoreMsg>) {
        let mut holder = self.fsm_holders.pop().unwrap();
        let s = self.store.take().unwrap();
        mem::replace(&mut *holder, FsmTypes::Store(s));
        if mail_box.sender.is_empty() {
            mail_box.state.release_fsm(holder);
        } else {
            self.push(holder);
        }
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
        batch.peers.clear();
        batch.store.take();
        batch.fsm_holders.clear();
    }

    pub fn schedule(&self, p: peer::Peer, state: Option<RegionLocalState>) {
        let (sender, receiver) = mpsc::loose_bounded(p.cfg.notify_capacity);
        let region_id = p.region().get_id();
        let fsm = PeerFsm {
            peer: p,
            mail_box: None,
            receiver,
        };
        let s = Arc::new(State::new(Box::new(FsmTypes::Peer(Box::new(fsm)))));
        let mail_box = MailBox::new(sender, s);
        let mut boxes = self.router.mailboxes.lock().unwrap();
        mail_box
            .force_send(PeerMsg::Start { state }, &self.router.scheduler)
            .unwrap();
        boxes.insert(region_id, mail_box);
    }

    pub fn schedule_all(&self, prs: Vec<(peer::Peer, RegionLocalState)>) {
        let mut boxes = self.router.mailboxes.lock().unwrap();
        for (p, r) in prs {
            let (sender, receiver) = mpsc::loose_bounded(p.cfg.notify_capacity);
            let region_id = p.region().get_id();
            let fsm = PeerFsm {
                peer: p,
                mail_box: None,
                receiver,
            };
            let state = Arc::new(State::new(Box::new(FsmTypes::Peer(Box::new(fsm)))));
            let mail_box = MailBox::new(sender, state);
            mail_box
                .force_send(PeerMsg::Start { state: Some(r) }, &self.router.scheduler)
                .unwrap();
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

pub struct PollContext<T, C: 'static> {
    pub pd_scheduler: FutureScheduler<PdTask>,
    pub raftlog_gc_scheduler: WorkerScheduler<RaftlogGcTask>,
    pub consistency_check_scheduler: WorkerScheduler<ConsistencyCheckTask>,
    pub split_check_scheduler: WorkerScheduler<SplitCheckTask>,
    pub cleanup_sst_scheduler: WorkerScheduler<CleanupSSTTask>,
    pub local_reader: WorkerScheduler<ReadTask>,
    pub region_scheduler: WorkerScheduler<RegionTask>,
    pub apply_router: ApplyRouter,
    pub compact_scheduler: WorkerScheduler<CompactTask>,
    pub importer: Arc<SSTImporter>,
    pub store_meta: Arc<Mutex<StoreMeta>>,
    pub poller: CpuPool,
    pub raft_metrics: RaftMetrics,
    pub snap_mgr: SnapManager,
    pub coprocessor_host: Arc<CoprocessorHost>,
    pub timer: Handle,
    pub trans: T,
    pub pd_client: Arc<C>,
    pub global_stat: Arc<GlobalStoreStat>,
    pub store_stat: LocalStoreStat,
    pub engines: Engines,
    pub kv_wb: WriteBatch,
    pub raft_wb: WriteBatch,
    pub sync_log: bool,
    pub is_busy: bool,
    pub ready_res: Vec<(Ready, InvokeContext)>,
    pub need_flush_trans: bool,
    pub queued_snapshot: HashSet<u64>,
}

impl<T, C> HandleRaftReadyContext for PollContext<T, C> {
    fn kv_wb(&self) -> &WriteBatch {
        &self.kv_wb
    }
    fn kv_wb_mut(&mut self) -> &mut WriteBatch {
        &mut self.kv_wb
    }
    fn raft_wb(&self) -> &WriteBatch {
        &self.raft_wb
    }
    fn raft_wb_mut(&mut self) -> &mut WriteBatch {
        &mut self.raft_wb
    }
    fn sync_log(&self) -> bool {
        self.sync_log
    }
    fn set_sync_log(&mut self, sync: bool) {
        self.sync_log = sync;
    }
}

struct Poller<T, C: 'static> {
    tag: String,
    scheduler: Scheduler,
    cfg: Arc<Config>,
    ctx: PollContext<T, C>,
    timer: SlowTimer,
}

impl<T: Transport + 'static, C: PdClient + 'static> Poller<T, C> {
    fn handle_raft_ready(
        &mut self,
        pending_cnt: usize,
        batches: &mut Vec<Box<PeerFsm>>,
        proposals: Vec<RegionProposal>,
        previous_ready_metrics: &RaftMetrics,
    ) {
        if !proposals.is_empty() {
            // TODO: verify if it's shutting down.
            for prop in proposals {
                if let Err(SendError(ApplyTask::Proposal(prop))) = self
                    .ctx
                    .apply_router
                    .force_send_task(prop.region_id, ApplyTask::Proposal(prop))
                {
                    prop.notify_region_removed();
                }
            }
            self.ctx.trans.flush();
        }
        self.ctx.raft_metrics.ready.has_ready_region += self.ctx.ready_res.len() as u64;
        fail_point!("raft_before_save");
        if !self.ctx.kv_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            let kv_wb = mem::replace(&mut self.ctx.kv_wb, WriteBatch::new());
            self.ctx
                .engines
                .kv
                .write_opt(kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save append state result: {:?}", self.tag, e);
                });
        }
        fail_point!("raft_between_save");
        if !self.ctx.raft_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.cfg.sync_log || self.ctx.sync_log);
            let raft_wb = mem::replace(&mut self.ctx.raft_wb, WriteBatch::with_capacity(4 * 1024));
            self.ctx
                .engines
                .raft
                .write_opt(raft_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save raft append result: {:?}", self.tag, e);
                });
        }
        fail_point!("raft_after_save");
        let ready_cnt = self.ctx.ready_res.len();
        if ready_cnt != 0 {
            let mut apply_tasks = Vec::with_capacity(ready_cnt);
            let mut batch_pos = 0;
            let mut ready_res = mem::replace(&mut self.ctx.ready_res, Vec::default());
            for (mut ready, invoke_ctx) in ready_res.drain(..) {
                let region_id = invoke_ctx.region_id;
                if batches[batch_pos].peer.region().get_id() == region_id {
                } else {
                    while batches[batch_pos].peer.region().get_id() != region_id {
                        batch_pos += 1;
                    }
                }
                Peer::new(&mut batches[batch_pos].peer, &mut self.ctx, &self.scheduler)
                    .post_raft_ready_append(ready, invoke_ctx, &mut apply_tasks);
            }
            for task in apply_tasks {
                let _ = self
                    .ctx
                    .apply_router
                    .force_send_task(task.region_id, ApplyTask::Apply(task));
            }
        }

        let dur = self.timer.elapsed();
        if !self.ctx.is_busy {
            let election_timeout = Duration::from_millis(
                self.cfg.raft_base_tick_interval.as_millis()
                    * self.cfg.raft_election_timeout_ticks as u64,
            );
            if dur >= election_timeout {
                self.ctx.is_busy = true;
            }
        }

        self.ctx
            .raft_metrics
            .append_log
            .observe(duration_to_sec(dur) as f64);

        self.ctx.trans.flush();
        self.ctx.need_flush_trans = false;

        slow_log!(
            self.timer,
            "{} handle {} pending peers include {} ready, {} entries, {} messages and {} \
             snapshots",
            self.tag,
            pending_cnt,
            ready_cnt,
            self.ctx.raft_metrics.ready.append - previous_ready_metrics.ready.append,
            self.ctx.raft_metrics.ready.message - previous_ready_metrics.ready.message,
            self.ctx.raft_metrics.ready.snapshot - previous_ready_metrics.ready.snapshot
        );
    }

    fn poll(&mut self) {
        let mut batch_size = self.cfg.max_batch_size;
        let mut batches = Batch::with_capacity(batch_size);
        let mut msgs = Vec::with_capacity(self.cfg.messages_per_tick);
        let mut store_msgs = Vec::with_capacity(self.cfg.messages_per_tick);
        let mut exhausted_peers = Vec::with_capacity(batch_size);
        let mut previous_metrics = self.ctx.raft_metrics.clone();
        let batch_size_observer = POLL_BATCH_SIZE.local();
        self.scheduler.fetch_batch(&mut batches, batch_size);
        batch_size_observer.observe(batches.fsm_holders.len() as f64);
        while !batches.fsm_holders.is_empty() {
            self.ctx.sync_log = false;
            self.timer = SlowTimer::new();
            let mut has_ready = false;
            if batches.store.is_some() {
                let mark = {
                    let mut s = batches.store.as_mut().unwrap();
                    let s: &mut StoreFsm = &mut *s;
                    let mut store = Store::new(&mut s.store, &mut self.ctx, &self.scheduler);
                    store.poll(&s.receiver, &mut store_msgs)
                };
                if batches.store.as_ref().unwrap().store.stopped {
                    batches.remove_store(&self.scheduler.router.store_box);
                } else if let Some(pos) = mark {
                    batches.release_store(&self.scheduler.router.store_box, pos);
                }
            }
            let mut pending_cnt = batches.peers.len();
            let mut pending_proposals = Vec::new();
            for (i, p) in batches.peers.iter_mut().enumerate() {
                let p: &mut PeerFsm = &mut *p;
                let mut peer = Peer::new(&mut p.peer, &mut self.ctx, &self.scheduler);
                let mark = peer.poll(&p.receiver, &mut msgs);
                if peer.peer.has_ready && !peer.peer.stopped {
                    has_ready = true;
                    if let Some(p) = peer.peer.take_apply_proposals() {
                        if pending_proposals.capacity() == 0 {
                            pending_proposals = Vec::with_capacity(pending_cnt);
                        }
                        pending_proposals.push(p);
                    }
                    peer.handle_raft_ready_append();
                } else {
                    pending_cnt -= 1;
                }
                if peer.peer.stopped {
                    exhausted_peers.push((i, None));
                } else if mark.is_some() {
                    exhausted_peers.push((i, mark));
                }
            }
            if has_ready {
                self.handle_raft_ready(
                    pending_cnt,
                    &mut batches.peers,
                    pending_proposals,
                    &previous_metrics,
                );
                previous_metrics = self.ctx.raft_metrics.clone();
            }
            if self.ctx.need_flush_trans {
                self.ctx.need_flush_trans = false;
                self.ctx.trans.flush();
            }
            if !self.ctx.queued_snapshot.is_empty() {
                let mut meta = self.ctx.store_meta.lock().unwrap();
                meta.pending_snapshot_regions
                    .retain(|r| !self.ctx.queued_snapshot.contains(&r.get_id()));
                self.ctx.queued_snapshot.clear();
            }

            while let Some((r, mark)) = exhausted_peers.pop() {
                if let Some(pos) = mark {
                    batches.release_peer(r, pos);
                } else {
                    batches.remove_peer(r);
                }
            }
            self.ctx
                .raft_metrics
                .process_ready
                .observe(duration_to_sec(self.timer.elapsed()) as f64);
            self.ctx.raft_metrics.flush();
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

pub struct BatchSystem<T: 'static, C: 'static> {
    meta: metapb::Store,
    tag: String,
    cfg: Arc<Config>,
    engines: Engines,
    store_meta: Arc<Mutex<StoreMeta>>,
    workers: Vec<JoinHandle<()>>,
    router: Router,
    apply_router: ApplyRouter,
    apply_receiver: Option<crossbeam_channel::Receiver<Box<ApplyFsmTypes>>>,

    coprocessor_host: Arc<CoprocessorHost>,

    importer: Arc<SSTImporter>,
    split_check_worker: Worker<SplitCheckTask>,
    raftlog_gc_worker: Worker<RaftlogGcTask>,
    region_worker: Worker<RegionTask>,
    compact_worker: Worker<CompactTask>,
    pd_worker: FutureWorker<PdTask>,
    consistency_check_worker: Worker<ConsistencyCheckTask>,
    cleanup_sst_worker: Worker<CleanupSSTTask>,
    local_reader: Worker<ReadTask>,
    poller: CpuPool,
    global_stat: Arc<GlobalStoreStat>,
    snap_manager: SnapManager,
    trans: T,
    pd_client: Arc<C>,
    peers: Vec<(peer::Peer, RegionLocalState)>,
}

impl<T, C> ConfigProvider for BatchSystem<T, C> {
    #[inline]
    fn store_id(&self) -> u64 {
        self.meta.get_id()
    }

    #[inline]
    fn snap_scheduler(&self) -> WorkerScheduler<RegionTask> {
        self.region_worker.scheduler()
    }

    #[inline]
    fn engines(&self) -> &Engines {
        &self.engines
    }

    #[inline]
    fn apply_scheduler(&self) -> ApplyRouter {
        self.apply_router.clone()
    }

    #[inline]
    fn read_scheduler(&self) -> WorkerScheduler<ReadTask> {
        self.local_reader.scheduler()
    }

    #[inline]
    fn coprocessor_host(&self) -> Arc<CoprocessorHost> {
        self.coprocessor_host.clone()
    }

    #[inline]
    fn config(&self) -> Arc<Config> {
        self.cfg.clone()
    }
}

impl<T, C> BatchSystem<T, C> {
    #[inline]
    pub fn meta(&self) -> &metapb::Store {
        &self.meta
    }

    #[inline]
    pub fn store_meta(&self) -> &Arc<Mutex<StoreMeta>> {
        &self.store_meta
    }

    pub fn get_peers(&self) -> impl ExactSizeIterator<Item = &peer::Peer> {
        (&self.peers).into_iter().map(|a| &a.0)
    }

    pub fn importer(&self) -> Arc<SSTImporter> {
        self.importer.clone()
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    pub fn snap_manager(&self) -> SnapManager {
        self.snap_manager.clone()
    }
}

impl<T: Transport, C: PdClient> BatchSystem<T, C> {
    pub fn new(
        meta: metapb::Store,
        mut cfg: Config,
        engines: Engines,
        trans: T,
        pd_client: Arc<C>,
        pd_worker: FutureWorker<PdTask>,
        local_reader: Worker<ReadTask>,
        snap_manager: SnapManager,
        importer: Arc<SSTImporter>,
        router: Router,
        mut coprocessor_host: CoprocessorHost,
    ) -> ::raftstore::Result<BatchSystem<T, C>> {
        let store_meta = StoreMeta::new(100);

        cfg.validate()?;
        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, box SplitObserver);

        let (apply_router, receiver) = apply_transport::create_router(&cfg);

        let poller = ::futures_cpupool::Builder::new()
            .name_prefix(thd_name!("timer"))
            .pool_size(1)
            .create();
        let mut system = BatchSystem {
            tag: format!("[store {}]", meta.get_id()),
            meta,
            engines,
            workers: Vec::with_capacity(cfg.store_pool_size),
            cfg: Arc::new(cfg),
            store_meta: Arc::new(Mutex::new(store_meta)),
            router,
            coprocessor_host: Arc::new(coprocessor_host),
            importer,
            split_check_worker: Worker::new("split-check"),
            region_worker: Worker::new("snapshot-worker"),
            raftlog_gc_worker: Worker::new("raft-gc-worker"),
            compact_worker: Worker::new("compact-worker"),
            consistency_check_worker: Worker::new("consistency-check"),
            cleanup_sst_worker: Worker::new("cleanup-sst"),
            apply_router,
            apply_receiver: Some(receiver),
            pd_worker,
            local_reader,
            poller,
            global_stat: Arc::new(GlobalStoreStat::default()),
            trans,
            snap_manager,
            pd_client,
            peers: vec![],
        };
        system.init_peers()?;
        Ok(system)
    }

    fn clear_stale_meta(
        &self,
        kv_wb: &mut WriteBatch,
        raft_wb: &mut WriteBatch,
        origin_state: &RegionLocalState,
    ) {
        let region = origin_state.get_region();
        let raft_key = keys::raft_state_key(region.get_id());
        let raft_state = match self.engines.raft.get_msg(&raft_key).unwrap() {
            // it has been cleaned up.
            None => return,
            Some(value) => value,
        };

        peer_storage::clear_meta(&self.engines, kv_wb, raft_wb, region.get_id(), &raft_state)
            .unwrap();
        let key = keys::region_state_key(region.get_id());
        let handle = rocksdb::get_cf_handle(&self.engines.kv, CF_RAFT).unwrap();
        kv_wb.put_msg_cf(handle, &key, origin_state).unwrap();
    }

    /// `clear_stale_data` clean up all possible garbage data.
    fn clear_stale_data(&self, meta: &mut StoreMeta) -> ::raftstore::Result<()> {
        let t = Instant::now();

        let mut ranges = Vec::new();
        let mut last_start_key = keys::data_key(b"");
        for region_id in meta.region_ranges.values() {
            let region = &meta.regions[region_id];
            let start_key = keys::enc_start_key(region);
            assert!(start_key >= last_start_key);
            ranges.push((last_start_key, start_key));
            last_start_key = keys::enc_end_key(region);
        }
        ranges.push((last_start_key, keys::DATA_MAX_KEY.to_vec()));

        rocksdb::roughly_cleanup_ranges(&self.engines.kv, &ranges)?;

        info!(
            "{} cleans up {} ranges garbage data, takes {:?}",
            self.tag,
            ranges.len(),
            t.elapsed()
        );

        Ok(())
    }

    /// Initialize this store. It scans the db engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn init_peers(&mut self) -> ::raftstore::Result<()> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let kv_engine = Arc::clone(&self.engines.kv);
        let mut total_cnt = 0;
        let mut tombstone_cnt = 0;

        let t = Instant::now();
        let mut kv_wb = WriteBatch::new();
        let mut raft_wb = WriteBatch::new();
        let mut local_states = vec![];
        kv_engine.scan_cf(CF_RAFT, start_key, end_key, false, |key, value| {
            let (region_id, suffix) = keys::decode_region_meta_key(key)?;
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            total_cnt += 1;

            let local_state = protobuf::parse_from_bytes::<RegionLocalState>(value)?;
            if local_state.get_state() == PeerState::Tombstone {
                tombstone_cnt += 1;
                debug!(
                    "{} region {:?} is tombstone",
                    self.tag,
                    local_state.get_region(),
                );
                self.clear_stale_meta(&mut kv_wb, &mut raft_wb, &local_state);
                return Ok(true);
            }
            if local_state.get_state() == PeerState::Applying {
                // in case of restart happen when we just write region state to Applying,
                // but not write raft_local_state to raft rocksdb in time.
                peer_storage::recover_from_applying_state(&self.engines, &raft_wb, region_id)?;
            }

            local_states.push(local_state);
            Ok(true)
        })?;

        if !kv_wb.is_empty() {
            self.engines.kv.write(kv_wb).unwrap();
            self.engines.kv.sync_wal().unwrap();
        }
        if !raft_wb.is_empty() {
            self.engines.raft.write(raft_wb).unwrap();
            self.engines.raft.sync_wal().unwrap();
        }

        let mut applying_cnt = 0;
        let mut merging_cnt = 0;
        self.peers = Vec::with_capacity(local_states.len());
        let mut meta = self.store_meta.lock().unwrap();
        for mut local_state in local_states {
            let mut peer = {
                let region = local_state.get_region();
                meta.region_ranges
                    .insert(enc_end_key(region), region.get_id());
                // No need to check duplicated here, because we use region id as the key
                // in DB.
                meta.regions.insert(region.get_id(), region.clone());
                peer::Peer::create(self, region)?
            };
            match local_state.get_state() {
                PeerState::Normal => {}
                PeerState::Applying => {
                    applying_cnt += 1;
                }
                PeerState::Merging => {
                    merging_cnt += 1;
                }
                PeerState::Tombstone => unreachable!(),
            }
            self.peers.push((peer, local_state));
        }

        info!(
            "{} starts with {} regions, including {} tombstones, {} applying \
             regions and {} merging regions, takes {:?}",
            self.tag,
            total_cnt,
            tombstone_cnt,
            applying_cnt,
            merging_cnt,
            t.elapsed()
        );

        self.clear_stale_data(&mut meta)?;

        Ok(())
    }

    pub fn poll_context(&self) -> PollContext<T, C> {
        PollContext {
            pd_scheduler: self.pd_worker.scheduler(),
            raftlog_gc_scheduler: self.raftlog_gc_worker.scheduler(),
            consistency_check_scheduler: self.consistency_check_worker.scheduler(),
            split_check_scheduler: self.split_check_worker.scheduler(),
            cleanup_sst_scheduler: self.cleanup_sst_worker.scheduler(),
            local_reader: self.local_reader.scheduler(),
            region_scheduler: self.region_worker.scheduler(),
            apply_router: self.apply_router.clone(),
            compact_scheduler: self.compact_worker.scheduler(),
            importer: self.importer.clone(),
            store_meta: self.store_meta.clone(),
            poller: self.poller.clone(),
            coprocessor_host: self.coprocessor_host(),
            raft_metrics: RaftMetrics::default(),
            snap_mgr: self.snap_manager.clone(),
            timer: GLOBAL_TIMER_HANDLE.clone(),
            trans: self.trans.clone(),
            pd_client: self.pd_client.clone(),
            global_stat: self.global_stat.clone(),
            store_stat: self.global_stat.local(),
            engines: self.engines.clone(),
            kv_wb: WriteBatch::default(),
            raft_wb: WriteBatch::with_capacity(4 * 1024),
            sync_log: false,
            is_busy: false,
            ready_res: Vec::new(),
            need_flush_trans: false,
            queued_snapshot: HashSet::default(),
        }
    }

    pub fn spawn(
        &mut self,
        batch_receiver: crossbeam_channel::Receiver<Box<FsmTypes>>,
    ) -> ::raftstore::Result<()> {
        self.router
            .send_store_message(StoreMsg::Start(self.meta.clone(), self.cfg.clone()))
            .unwrap();

        let scheduler = Scheduler {
            router: self.router.clone(),
            batch_receiver,
        };

        let mut system = ApplyBatchSystem::new(
            self,
            self.router.clone(),
            self.apply_router.clone(),
            self.cfg.clone(),
        );
        box_try!(system.spawn(self.apply_receiver.take().unwrap()));

        let reader = LocalReader::new(&self, self.router.clone());
        let timer = LocalReader::new_timer();
        box_try!(self.local_reader.start_with_timer(reader, timer));

        let peers = mem::replace(&mut self.peers, vec![]);
        let regions: Vec<_> = peers.iter().map(|r| r.0.region().to_owned()).collect();
        scheduler.schedule_all(peers);
        for r in regions {
            self.coprocessor_host
                .on_region_changed(&r, RegionChangeEvent::Create);
        }

        self.snap_manager.init()?;

        let split_check_runner = SplitCheckRunner::new(
            Arc::clone(&self.engines.kv),
            self.router.clone(),
            Arc::clone(&self.coprocessor_host),
        );

        box_try!(self.split_check_worker.start(split_check_runner));

        let region_runner = RegionRunner::new(
            self.engines.clone(),
            self.snap_manager(),
            self.cfg.snap_apply_batch_size.0 as usize,
            self.cfg.use_delete_range,
            self.cfg.clean_stale_peer_delay.0,
        );
        let timer = RegionRunner::new_timer();
        box_try!(self.region_worker.start_with_timer(region_runner, timer));

        let raftlog_gc_runner = RaftlogGcRunner::new(None);
        box_try!(self.raftlog_gc_worker.start(raftlog_gc_runner));

        let compact_runner = CompactRunner::new(Arc::clone(&self.engines.kv));
        box_try!(self.compact_worker.start(compact_runner));

        let pd_runner = PdRunner::new(
            self.meta.get_id(),
            Arc::clone(&self.pd_client),
            self.router.clone(),
            Arc::clone(&self.engines.kv),
            self.pd_worker.scheduler(),
        );
        box_try!(self.pd_worker.start(pd_runner));

        let consistency_check_runner = ConsistencyCheckRunner::new(self.router.clone());
        box_try!(
            self.consistency_check_worker
                .start(consistency_check_runner)
        );

        let cleanup_sst_runner = CleanupSSTRunner::new(
            self.meta.get_id(),
            self.router.clone(),
            Arc::clone(&self.importer),
            Arc::clone(&self.pd_client),
        );
        box_try!(self.cleanup_sst_worker.start(cleanup_sst_runner));

        if let Err(e) = util_sys::thread::set_priority(util_sys::HIGH_PRI) {
            warn!("set thread priority for raftstore failed, error: {:?}", e);
        }

        self.workers = Vec::with_capacity(self.cfg.store_pool_size);
        let sid = self.store_id();
        for i in 0..self.cfg.store_pool_size {
            let mut poller = Poller {
                scheduler: scheduler.clone(),
                tag: self.tag.clone(),
                cfg: self.cfg.clone(),
                ctx: self.poll_context(),
                timer: SlowTimer::new(),
            };
            self.workers.push(
                thread::Builder::new()
                    .name(thd_name!(format!("store_{}-{}", sid, i)))
                    .spawn(move || {
                        poller.poll();
                    })
                    .unwrap(),
            );
        }

        Ok(())
    }

    pub fn shutdown(&mut self) {
        info!("{} start to stop raftstore.", self.tag);

        // Wait all workers finish.
        let mut handles: Vec<Option<thread::JoinHandle<()>>> = vec![];
        handles.push(self.split_check_worker.stop());
        handles.push(self.region_worker.stop());
        handles.push(self.raftlog_gc_worker.stop());
        handles.push(self.compact_worker.stop());
        handles.push(self.pd_worker.stop());
        handles.push(self.consistency_check_worker.stop());
        handles.push(self.cleanup_sst_worker.stop());
        handles.push(self.local_reader.stop());

        self.apply_router.broadcast_shutdown();
        self.router.broadcast_shutdown();

        for h in self.workers.drain(..) {
            debug!("{} stop poller {:?}", self.tag, h.thread().name());
            h.join().unwrap();
        }

        for h in handles {
            if let Some(h) = h {
                h.join().unwrap();
            }
        }

        self.coprocessor_host.shutdown();

        info!("{} stop raftstore finished.", self.tag);
    }
}

pub fn create_router(cfg: &Config) -> (Router, crossbeam_channel::Receiver<Box<FsmTypes>>) {
    let (store_tx, store_rx) = mpsc::loose_bounded(cfg.messages_per_tick);

    let store = StoreCore::new();

    let store_fsm = StoreFsm {
        store,
        receiver: store_rx,
    };
    let state = Arc::new(State::new(Box::new(FsmTypes::Store(Box::new(store_fsm)))));
    let store_box = MailBox::new(store_tx, state);
    let (tx, rx) = crossbeam_channel::unbounded();
    let router = Router::new(store_box, tx);
    (router, rx)
}
