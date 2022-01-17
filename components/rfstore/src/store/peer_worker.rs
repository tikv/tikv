// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::RaftRouter;
use crossbeam::channel::RecvTimeoutError;
use std::collections::HashMap;
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tikv_util::debug;
use tikv_util::mpsc::{Receiver, Sender};
use tikv_util::worker::Scheduler;

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
    apply_sender: Sender<ApplyBatch>,
    last_tick: Instant,
    tick_millis: u64,
}

impl RaftWorker {
    pub(crate) fn new(
        ctx: GlobalContext,
        receiver: Receiver<(u64, PeerMsg)>,
        router: RaftRouter,
    ) -> (Self, Receiver<ApplyBatch>) {
        let (apply_sender, apply_receiver) = tikv_util::mpsc::unbounded();
        let tick_millis = ctx.cfg.value().raft_base_tick_interval.as_millis();
        let ctx = RaftContext::new(ctx);
        (
            Self {
                ctx,
                receiver,
                router,
                apply_sender,
                last_tick: Instant::now(),
                tick_millis,
            },
            apply_receiver,
        )
    }

    pub(crate) fn run(&mut self) {
        let mut inboxes = Inboxes::new();
        loop {
            if self.receive_msgs(&mut inboxes).is_err() {
                return;
            }
            inboxes.inboxes.iter_mut().for_each(|(_, inbox)| {
                self.process_inbox(inbox);
            });
            self.persist_state();
            self.handle_post_persist_tasks(&mut inboxes);
            self.round_end();
        }
    }

    /// return true means channel is disconnected, return outer loop.
    fn receive_msgs(&mut self, inboxes: &mut Inboxes) -> std::result::Result<(), RecvTimeoutError> {
        inboxes.inboxes.retain(|_, inbox| -> bool {
            if inbox.msgs.len() == 0 {
                false
            } else {
                inbox.msgs.truncate(0);
                true
            }
        });
        let res = self.receiver.recv_timeout(Duration::from_millis(10));
        let router = &self.router;
        match res {
            Ok((region_id, msg)) => {
                inboxes.append_msg(router, region_id, msg);
                loop {
                    if let Ok((region_id, msg)) = self.receiver.try_recv() {
                        inboxes.append_msg(router, region_id, msg);
                    } else {
                        break;
                    }
                }
            }
            Err(RecvTimeoutError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
            Err(RecvTimeoutError::Timeout) => {}
        }
        let now = Instant::now();
        if (now - self.last_tick).as_millis() as u64 > self.tick_millis {
            self.last_tick = now;
            let peers = self.router.peers.clone();
            for x in peers.iter() {
                let region_id = *x.key();
                inboxes.append_msg(router, region_id, PeerMsg::Tick);
            }
        }
        return Ok(());
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
        self.maybe_send_apply(&inbox.peer.applier);
        peer_fsm.peer.maybe_finish_split(&mut self.ctx);
    }

    fn maybe_send_apply(&mut self, applier: &Arc<Mutex<Applier>>) {
        if !self.ctx.apply_msgs.msgs.is_empty() {
            let peer_batch = ApplyBatch {
                msgs: mem::take(&mut self.ctx.apply_msgs.msgs),
                applier: applier.clone(),
            };
            self.apply_sender.send(peer_batch).unwrap();
        }
    }

    fn persist_state(&mut self) {
        if self.ctx.global.trans.need_flush() {
            self.ctx.global.trans.flush();
        }
        let raft_wb = &mut self.ctx.raft_wb;
        if !raft_wb.is_empty() {
            debug!("persist_state");
            self.ctx.global.engines.raft.write(raft_wb).unwrap()
        }
        raft_wb.reset();
    }

    fn handle_post_persist_tasks(&mut self, inboxes: &mut Inboxes) {
        let tasks = mem::take(&mut self.ctx.post_persist_tasks);
        for task in tasks {
            let inbox = inboxes.inboxes.get_mut(&task.region_id).unwrap();
            let peer = &mut inbox.peer.peer_fsm.lock().unwrap().peer;
            peer.handle_post_persist_task(&mut self.ctx, task);
        }
    }

    fn round_end(&mut self) {
        if self.ctx.global.trans.need_flush() {
            self.ctx.global.trans.flush();
        }
        self.ctx.current_time = None;
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
        split_scheduler: Scheduler<SplitTask>,
        router: RaftRouter,
        receiver: Receiver<ApplyBatch>,
    ) -> Self {
        let ctx = ApplyContext::new(
            engine,
            Some(region_sched),
            Some(split_scheduler),
            Some(router),
        );
        Self { ctx, receiver }
    }

    pub(crate) fn run(&mut self) {
        loop {
            let res = self.receiver.recv();
            if res.is_err() {
                return;
            }
            let mut batch = res.unwrap();
            let mut applier = batch.applier.lock().unwrap();
            for msg in batch.msgs.drain(..) {
                applier.handle_msg(&mut self.ctx, msg);
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
            let now = Instant::now();
            if (now - self.last_tick).as_millis() as u64 > self.tick_millis {
                self.handler.handle_msg(StoreMsg::Tick);
                self.last_tick = now;
            }
        }
    }
}
