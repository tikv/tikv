// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::RaftRouter;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tikv_util::mpsc::{Receiver, Sender};

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

pub(crate) struct RaftWorker {
    ctx: RaftContext,
    receiver: Receiver<(u64, PeerMsg)>,
    router: RaftRouter,
    apply_sender: Sender<ApplyBatch>,
    inboxes: HashMap<u64, PeerInbox>,
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
                inboxes: HashMap::new(),
                last_tick: Instant::now(),
                tick_millis,
            },
            apply_receiver,
        )
    }

    pub(crate) fn run(&mut self) {
        self.receive_msgs();
        let mut readies = vec![];
        self.inboxes.iter_mut().for_each( |(_, inbox) | {
            if let Some(ready) = self.process_inbox(inbox) {
                readies.push(ready);
            }
        });
        self.persist_state();
        self.post_persist_state(readies);
        self.ctx.flush_local_stats();
    }

    fn receive_msgs(&mut self) {
        self.inboxes.retain(|region_id, inbox| -> bool {
            if inbox.msgs.len() == 0 {
                false
            } else {
                inbox.msgs.truncate(0);
                true
            }
        });
        if let Ok((region_id, msg)) = self.receiver.recv_timeout(Duration::from_millis(10)) {
            self.append_msg(region_id, msg);
        }
        loop {
            if let Ok((region_id, msg)) = self.receiver.try_recv() {
                self.append_msg(region_id, msg);
            } else {
                break;
            }
        }
        let now = Instant::now();
        if (now - self.last_tick).as_millis() as u64 > self.tick_millis {
            self.last_tick = now;
            let peers = self.router.peers.clone();
            for x in peers.iter() {
                let region_id = *x.key();
                self.append_msg(region_id, PeerMsg::Tick);
            }
        }
    }

    fn append_msg(&mut self, region_id: u64,  msg: PeerMsg) {
        self.get_inbox(region_id).msgs.push(msg);
    }

    fn get_inbox(&mut self, region_id: u64) -> &mut PeerInbox {
        self.init_inbox(region_id);
        self.inboxes.get_mut(&region_id).unwrap()
    }

    fn init_inbox(&mut self, region_id: u64) {
        if self.inboxes.get_mut(&region_id).is_none() {
            let peer_state = self.router.peers.get(&region_id).unwrap();
            let inbox = PeerInbox {
                peer: peer_state.clone(),
                msgs: vec![],
            };
            self.inboxes.insert(region_id, inbox);
        }
    }

    fn process_inbox(&mut self, inbox: &mut PeerInbox) -> Option<CollectedReady> {
        let mut peer_fsm = inbox.peer.peer_fsm.lock().unwrap();
        let mut h = PeerMsgHandler::new(&mut peer_fsm, &mut self.ctx);
        if let Some(mut rd) = h.new_raft_ready() {
            h.handle_raft_ready(&mut rd);
            let mut apply_msgs = self.ctx.apply_msgs.msgs.drain(..);
            let mut peer_batch = ApplyBatch {
                msgs: vec![],
            };
            peer_batch.msgs.extend(apply_msgs);
            self.apply_sender.send(peer_batch);
            return Some(rd)
        }
        None
    }

    fn persist_state(&self) {}

    fn post_persist_state(&self, readies: Vec<CollectedReady>) {}
}

pub(crate) struct ApplyWorker {
    ctx: GlobalContext,
    receiver: Receiver<ApplyBatch>,
    router: RaftRouter,
}

impl ApplyWorker {
    pub(crate) fn new(
        ctx: GlobalContext,
        receiver: Receiver<ApplyBatch>,
        router: RaftRouter,
    ) -> Self {
        Self {
            ctx,
            receiver,
            router,
        }
    }

    pub(crate) fn run(&mut self) {
        todo!()
    }
}

pub(crate) struct StoreWorker {
    handler: StoreMsgHandler,
}

impl StoreWorker {
    pub(crate) fn new(store_fsm: StoreFSM, ctx: GlobalContext) -> Self {
        let handler = StoreMsgHandler::new(store_fsm, ctx);
        Self { handler }
    }

    pub(crate) fn run(&mut self) {
        todo!()
    }
}
