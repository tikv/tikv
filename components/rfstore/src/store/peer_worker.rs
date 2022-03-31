// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::RaftRouter;
use crossbeam::channel::RecvTimeoutError;
use raftstore::store::metrics::{
    STORE_WRITE_RAFTDB_DURATION_HISTOGRAM, STORE_WRITE_SEND_DURATION_HISTOGRAM,
    STORE_WRITE_TRIGGER_SIZE_HISTOGRAM,
};
use raftstore::store::util;
use std::collections::HashMap;
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tikv_util::mpsc::{Receiver, Sender};
use tikv_util::time::{duration_to_sec, InstantExt};
use tikv_util::worker::Scheduler;
use tikv_util::{debug, error};

#[derive(Clone)]
pub(crate) struct PeerStates {
    pub(crate) applier: Arc<Mutex<Applier>>,
    pub(crate) peer_fsm: Arc<Mutex<PeerFsm>>,
    pub(crate) closed: Arc<AtomicBool>,
}

impl PeerStates {
    pub(crate) fn new(applier: Applier, peer_fsm: PeerFsm) -> Self {
        Self {
            applier: Arc::new(Mutex::new(applier)),
            peer_fsm: Arc::new(Mutex::new(peer_fsm)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub(crate) struct PeerInbox {
    pub(crate) peer: PeerStates,
    pub(crate) msgs: Vec<PeerMsg>,
}

pub(crate) struct Inboxes {
    inboxes: HashMap<u64, PeerInbox>,
}

impl Inboxes {
    fn new() -> Self {
        Inboxes {
            inboxes: HashMap::new(),
        }
    }

    fn get_inbox(&mut self, router: &RaftRouter, region_id: u64) -> &mut PeerInbox {
        self.init_inbox(router, region_id);
        self.inboxes.get_mut(&region_id).unwrap()
    }

    fn init_inbox(&mut self, router: &RaftRouter, region_id: u64) {
        if self.inboxes.get_mut(&region_id).is_none() {
            let peer_state = router.peers.get(&region_id).unwrap();
            let inbox = PeerInbox {
                peer: peer_state.clone(),
                msgs: vec![],
            };
            self.inboxes.insert(region_id, inbox);
        }
    }

    fn append_msg(&mut self, router: &RaftRouter, region_id: u64, msg: PeerMsg) {
        self.get_inbox(router, region_id).msgs.push(msg)
    }
}

pub(crate) struct RaftWorker {
    ctx: RaftContext,
    receiver: Receiver<(u64, PeerMsg)>,
    router: RaftRouter,
    apply_senders: Vec<Sender<ApplyBatch>>,
    io_sender: Sender<IOTask>,
    sync_io_worker: Option<IOWorker>,
    last_tick: Instant,
    tick_millis: u64,
}

const MAX_BATCH_COUNT: usize = 1024;
const MAX_BATCH_SIZE: usize = 1 * 1024 * 1024;

impl RaftWorker {
    pub(crate) fn new(
        ctx: GlobalContext,
        receiver: Receiver<(u64, PeerMsg)>,
        router: RaftRouter,
        io_sender: Sender<IOTask>,
        sync_io_worker: Option<IOWorker>,
    ) -> (Self, Vec<Receiver<ApplyBatch>>) {
        let apply_pool_size = ctx.cfg.value().apply_pool_size;
        let mut apply_senders = Vec::with_capacity(apply_pool_size);
        let mut apply_receivers = Vec::with_capacity(apply_pool_size);
        for _ in 0..apply_pool_size {
            let (sender, receiver) = tikv_util::mpsc::unbounded();
            apply_senders.push(sender);
            apply_receivers.push(receiver);
        }
        let tick_millis = ctx.cfg.value().raft_base_tick_interval.as_millis();
        let ctx = RaftContext::new(ctx);
        (
            Self {
                ctx,
                receiver,
                router,
                apply_senders,
                io_sender,
                sync_io_worker,
                last_tick: Instant::now(),
                tick_millis,
            },
            apply_receivers,
        )
    }

    pub(crate) fn run(&mut self) {
        let mut inboxes = Inboxes::new();
        loop {
            let loop_start: tikv_util::time::Instant;
            match self.receive_msgs(&mut inboxes) {
                Ok(start_time) => {
                    loop_start = start_time;
                }
                Err(_) => return,
            }
            inboxes.inboxes.iter_mut().for_each(|(_, inbox)| {
                self.process_inbox(inbox);
            });
            if self.ctx.global.trans.need_flush() {
                self.ctx.global.trans.flush();
            }
            self.persist_state();
            self.ctx
                .raft_metrics
                .store_time
                .observe(duration_to_sec(loop_start.saturating_elapsed()));
            self.ctx.raft_metrics.flush();
            self.ctx.current_time = None;
        }
    }

    /// return true means channel is disconnected, return outer loop.
    fn receive_msgs(
        &mut self,
        inboxes: &mut Inboxes,
    ) -> std::result::Result<tikv_util::time::Instant, RecvTimeoutError> {
        inboxes.inboxes.retain(|_, inbox| -> bool {
            if inbox.msgs.len() == 0 {
                false
            } else {
                inbox.msgs.truncate(0);
                true
            }
        });
        let res = self.receiver.recv_timeout(Duration::from_millis(10));
        let receive_time = tikv_util::time::Instant::now();
        let router = &self.router;
        match res {
            Ok((region_id, msg)) => {
                let mut batch_size = msg.size();
                let mut batch_cnt = 1;
                inboxes.append_msg(router, region_id, msg);
                loop {
                    if let Ok((region_id, msg)) = self.receiver.try_recv() {
                        batch_size += msg.size();
                        batch_cnt += 1;
                        inboxes.append_msg(router, region_id, msg);
                        if batch_cnt > MAX_BATCH_COUNT || batch_size > MAX_BATCH_SIZE {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            Err(RecvTimeoutError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
            Err(RecvTimeoutError::Timeout) => {}
        }
        if self.last_tick.saturating_elapsed().as_millis() as u64 > self.tick_millis {
            self.last_tick = Instant::now();
            let peers = self.router.peers.clone();
            for x in peers.iter() {
                let region_id = *x.key();
                inboxes.append_msg(router, region_id, PeerMsg::Tick);
            }
        }
        return Ok(receive_time);
    }

    fn process_inbox(&mut self, inbox: &mut PeerInbox) {
        if inbox.msgs.is_empty() {
            return;
        }
        let mut peer_fsm = inbox.peer.peer_fsm.lock().unwrap();
        if peer_fsm.stopped {
            return;
        }
        PeerMsgHandler::new(&mut peer_fsm, &mut self.ctx).handle_msgs(&mut inbox.msgs);
        peer_fsm.peer.handle_raft_ready(&mut self.ctx);
        self.maybe_send_apply(&inbox.peer.applier, &peer_fsm);
    }

    fn maybe_send_apply(&mut self, applier: &Arc<Mutex<Applier>>, peer_fsm: &PeerFsm) {
        if !self.ctx.apply_msgs.msgs.is_empty() {
            let peer_batch = ApplyBatch {
                msgs: mem::take(&mut self.ctx.apply_msgs.msgs),
                applier: applier.clone(),
                applying_cnt: peer_fsm.applying_cnt.clone(),
                send_time: tikv_util::time::Instant::now(),
            };
            peer_batch
                .applying_cnt
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.apply_senders[peer_fsm.apply_worker_idx]
                .send(peer_batch)
                .unwrap();
        }
    }

    fn persist_state(&mut self) {
        if self.ctx.persist_readies.is_empty() && self.ctx.raft_wb.is_empty() {
            return;
        }
        let raft_wb = mem::take(&mut self.ctx.raft_wb);
        self.ctx.global.engines.raft.apply(&raft_wb);
        let readies = mem::take(&mut self.ctx.persist_readies);
        let io_task = IOTask { raft_wb, readies };
        if let Some(sync_io_worker) = self.sync_io_worker.as_mut() {
            sync_io_worker.handle_task(io_task);
        } else {
            self.io_sender.send(io_task).unwrap();
        }
    }
}

pub(crate) struct ApplyWorker {
    ctx: ApplyContext,
    receiver: Receiver<ApplyBatch>,
}

impl ApplyWorker {
    pub(crate) fn new(
        engine: kvengine::Engine,
        region_sched: Scheduler<RegionTask>,
        router: RaftRouter,
        receiver: Receiver<ApplyBatch>,
    ) -> Self {
        let ctx = ApplyContext::new(engine, Some(region_sched), Some(router));
        Self { ctx, receiver }
    }

    pub(crate) fn run(&mut self) {
        let mut loop_cnt = 0u64;
        loop {
            let res = self.receiver.recv();
            if res.is_err() {
                return;
            }
            let mut batch = res.unwrap();
            let timer = tikv_util::time::Instant::now();
            self.ctx.apply_wait.observe(duration_to_sec(
                timer.saturating_duration_since(batch.send_time),
            ));
            let mut applier = batch.applier.lock().unwrap();
            for msg in batch.msgs.drain(..) {
                applier.handle_msg(&mut self.ctx, msg);
            }
            batch
                .applying_cnt
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            loop_cnt += 1;
            if loop_cnt % 128 == 0 {
                self.ctx.apply_wait.flush();
                self.ctx.apply_time.flush();
            }
        }
    }
}

pub(crate) struct StoreWorker {
    handler: StoreMsgHandler,
    last_tick: Instant,
    tick_millis: u64,
}

impl StoreWorker {
    pub(crate) fn new(store_fsm: StoreFSM, ctx: GlobalContext) -> Self {
        let tick_millis = ctx.cfg.value().raft_base_tick_interval.as_millis();
        let handler = StoreMsgHandler::new(store_fsm, ctx);
        Self {
            handler,
            last_tick: Instant::now(),
            tick_millis,
        }
    }

    pub(crate) fn run(&mut self) {
        loop {
            let res = self
                .handler
                .get_receiver()
                .recv_timeout(Duration::from_millis(10));
            match res {
                Ok(msg) => self.handler.handle_msg(msg),
                Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => {}
            }
            if self.last_tick.saturating_elapsed().as_millis() as u64 > self.tick_millis {
                self.handler.handle_msg(StoreMsg::Tick);
                self.last_tick = Instant::now();
            }
        }
    }
}

pub(crate) struct IOWorker {
    engine: rfengine::RFEngine,
    receiver: Receiver<IOTask>,
    router: RaftRouter,
    trans: Box<dyn Transport>,
}

impl IOWorker {
    pub(crate) fn new(
        engine: rfengine::RFEngine,
        router: RaftRouter,
        trans: Box<dyn Transport>,
    ) -> (Self, Sender<IOTask>) {
        let (sender, receiver) = tikv_util::mpsc::bounded(0);
        (
            Self {
                engine,
                receiver,
                router,
                trans,
            },
            sender,
        )
    }

    pub(crate) fn run(&mut self) {
        loop {
            let res = self.receiver.recv_timeout(Duration::from_secs(1));
            match res {
                Ok(msg) => self.handle_task(msg),
                Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => {}
            }
        }
    }

    fn handle_task(&mut self, task: IOTask) {
        if !task.raft_wb.is_empty() {
            let timer = tikv_util::time::Instant::now();
            let write_size = self.engine.persist(task.raft_wb).unwrap();
            let write_raft_db_time = duration_to_sec(timer.saturating_elapsed());
            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(write_raft_db_time);
            STORE_WRITE_TRIGGER_SIZE_HISTOGRAM.observe(write_size as f64);
        }
        let timer = tikv_util::time::Instant::now();
        for mut ready in task.readies {
            let raft_messages = mem::take(&mut ready.raft_messages);
            for msg in raft_messages {
                debug!(
                    "follower send raft message";
                    "region_id" => msg.region_id,
                    "message_type" => %util::MsgType(&msg),
                    "from_peer_id" => msg.get_from_peer().get_id(),
                    "to_peer_id" => msg.get_to_peer().get_id(),
                );
                if let Err(err) = self.trans.send(msg) {
                    error!("failed to send persist raft message {:?}", err);
                }
            }
            let region_id = ready.region_id;
            let msg = PeerMsg::Persisted(ready);
            if let Err(err) = self.router.send(region_id, msg) {
                error!("failed to send persisted message {:?}", err);
            }
        }
        if self.trans.need_flush() {
            self.trans.flush();
        }
        let send_time = duration_to_sec(timer.saturating_elapsed());
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(send_time);
    }
}
