// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{hash_map::Entry, HashMap},
    mem,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crossbeam::channel::RecvTimeoutError;
use kvproto::{errorpb, raft_cmdpb::RaftCmdResponse};
use raftstore::store::{
    metrics::{
        STORE_WRITE_RAFTDB_DURATION_HISTOGRAM, STORE_WRITE_SEND_DURATION_HISTOGRAM,
        STORE_WRITE_TRIGGER_SIZE_HISTOGRAM,
    },
    util,
};
use tikv_util::{
    debug, error,
    mpsc::{Receiver, Sender},
    time::{duration_to_sec, InstantExt},
};

use super::*;
use crate::RaftRouter;

#[derive(Clone)]
pub(crate) struct PeerStates {
    pub(crate) applier: Arc<Mutex<Applier>>,
    pub(crate) peer_fsm: Arc<Mutex<PeerFsm>>,
}

impl PeerStates {
    pub(crate) fn new(applier: Applier, peer_fsm: PeerFsm) -> Self {
        Self {
            applier: Arc::new(Mutex::new(applier)),
            peer_fsm: Arc::new(Mutex::new(peer_fsm)),
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
}

pub(crate) struct RaftWorker {
    ctx: StoreContext,
    receiver: Receiver<(u64, PeerMsg)>,
    router: RaftRouter,
    apply_senders: Vec<Sender<ApplyBatch>>,
    io_sender: Sender<IOTask>,
    sync_io_worker: Option<IOWorker>,
    last_tick: Instant,
    tick_millis: u64,
    store_fsm: StoreFSM,
}

const MAX_BATCH_COUNT: usize = 1024;
const MAX_BATCH_SIZE: usize = 1024 * 1024;

impl RaftWorker {
    pub(crate) fn new(
        ctx: StoreContext,
        receiver: Receiver<(u64, PeerMsg)>,
        router: RaftRouter,
        io_sender: Sender<IOTask>,
        sync_io_worker: Option<IOWorker>,
        store_fsm: StoreFSM,
    ) -> (Self, Vec<Receiver<ApplyBatch>>) {
        let apply_pool_size = ctx.cfg.apply_pool_size;
        let mut apply_senders = Vec::with_capacity(apply_pool_size);
        let mut apply_receivers = Vec::with_capacity(apply_pool_size);
        for _ in 0..apply_pool_size {
            let (sender, receiver) = tikv_util::mpsc::unbounded();
            apply_senders.push(sender);
            apply_receivers.push(receiver);
        }
        let tick_millis = ctx.cfg.raft_base_tick_interval.as_millis();
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
                store_fsm,
            },
            apply_receivers,
        )
    }

    pub(crate) fn run(&mut self) {
        let mut inboxes = Inboxes::new();
        loop {
            self.handle_store_msg();
            let loop_start = match self.receive_msgs(&mut inboxes) {
                Ok(start_time) => start_time,
                Err(_) => return,
            };
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

    fn handle_store_msg(&mut self) {
        while let Ok(msg) = self.store_fsm.receiver.try_recv() {
            let mut store_handler = StoreMsgHandler::new(&mut self.store_fsm, &mut self.ctx);
            if let Some(apply_region) = store_handler.handle_msg(msg) {
                let peer = self.ctx.peers.get(&apply_region).unwrap().clone();
                let peer_fsm = peer.peer_fsm.lock().unwrap();
                let applier = peer.applier.clone();
                self.maybe_send_apply(&applier, &peer_fsm);
            }
        }
        if self.store_fsm.last_tick.saturating_elapsed().as_millis() as u64
            > self.store_fsm.tick_millis
        {
            let mut store_handler = StoreMsgHandler::new(&mut self.store_fsm, &mut self.ctx);
            store_handler.handle_msg(StoreMsg::Tick);
            self.store_fsm.last_tick = Instant::now();
        }
    }

    /// return true means channel is disconnected, return outer loop.
    fn receive_msgs(
        &mut self,
        inboxes: &mut Inboxes,
    ) -> std::result::Result<tikv_util::time::Instant, RecvTimeoutError> {
        inboxes.inboxes.retain(|_, inbox| -> bool {
            if inbox.msgs.is_empty() {
                false
            } else {
                inbox.msgs.truncate(0);
                true
            }
        });
        let res = self.receiver.recv_timeout(Duration::from_millis(10));
        let receive_time = tikv_util::time::Instant::now();
        match res {
            Ok((region_id, msg)) => {
                let mut batch_size = msg.size();
                let mut batch_cnt = 1;
                self.append_msg(inboxes, region_id, msg);
                while let Ok((region_id, msg)) = self.receiver.try_recv() {
                    batch_size += msg.size();
                    batch_cnt += 1;
                    self.append_msg(inboxes, region_id, msg);
                    if batch_cnt > MAX_BATCH_COUNT || batch_size > MAX_BATCH_SIZE {
                        break;
                    }
                }
            }
            Err(RecvTimeoutError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
            Err(RecvTimeoutError::Timeout) => {}
        }
        if self.last_tick.saturating_elapsed().as_millis() as u64 > self.tick_millis {
            self.last_tick = Instant::now();
            for (region_id, peer) in self.ctx.peers.iter() {
                match inboxes.inboxes.entry(*region_id) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().msgs.push(PeerMsg::Tick);
                    }
                    Entry::Vacant(mut entry) => {
                        entry.insert(PeerInbox {
                            peer: peer.clone(),
                            msgs: vec![PeerMsg::Tick],
                        });
                    }
                }
            }
        }
        Ok(receive_time)
    }

    fn append_msg(&mut self, inboxes: &mut Inboxes, region_id: u64, msg: PeerMsg) {
        if let Some(inbox) = inboxes.inboxes.get_mut(&region_id) {
            inbox.msgs.push(msg);
            return;
        }
        if let Some(peer) = self.ctx.peers.get(&region_id) {
            inboxes.inboxes.insert(
                region_id,
                PeerInbox {
                    peer: peer.clone(),
                    msgs: vec![msg],
                },
            );
            return;
        }
        match msg {
            PeerMsg::RaftMessage(msg) => {
                self.router.send_store(StoreMsg::RaftMessage(msg));
            }
            PeerMsg::RaftCommand(cmd) => {
                let mut resp = RaftCmdResponse::default();
                let mut err = errorpb::Error::default();
                err.set_message(format!("region {} is missing", region_id));
                resp.mut_header().set_error(err);
                cmd.callback.invoke_with_response(resp);
            }
            PeerMsg::Tick => {}
            PeerMsg::Start => {}
            PeerMsg::ApplyResult(_) => {}
            PeerMsg::CasualMessage(_) => {}
            PeerMsg::SignificantMsg(_) => {}
            PeerMsg::GenerateEngineChangeSet(_) => {}
            PeerMsg::ApplyChangeSetResult(_) => {}
            PeerMsg::PrepareChangeSetResult(_) => {}
            PeerMsg::Persisted(_) => {}
        }
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
        peer_fsm.peer.handle_raft_ready(&mut self.ctx, None);
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
        router: RaftRouter,
        receiver: Receiver<ApplyBatch>,
    ) -> Self {
        let ctx = ApplyContext::new(engine, Some(router));
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
            self.router.send(region_id, PeerMsg::Persisted(ready));
        }
        if self.trans.need_flush() {
            self.trans.flush();
        }
        let send_time = duration_to_sec(timer.saturating_elapsed());
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(send_time);
    }
}
