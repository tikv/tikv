// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::apply::Applier;
use super::*;
use kvproto::kvrpcpb::Op;
use slog_global::info;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub(crate) struct Router {
    pub(crate) store_sender: mpsc::Sender<StoreMsg>,
    pub(crate) store_fsm: Arc<StoreFSM>,
    pub(crate) peers: Arc<dashmap::DashMap<u64, PeerState>>,
    pub(crate) peer_sender: mpsc::Sender<PeerMsg>,
}

impl Router {
    pub(crate) fn new(
        peer_sender: mpsc::Sender<PeerMsg>,
        store_sender: mpsc::Sender<StoreMsg>,
        store_fsm: StoreFSM,
    ) -> Self {
        Self {
            store_sender,
            store_fsm: Arc::new(store_fsm),
            peers: Arc::new(dashmap::DashMap::new()),
            peer_sender,
        }
    }

    pub(crate) fn get(&self, region_id: u64) -> Option<dashmap::mapref::one::Ref<u64, PeerState>> {
        self.peers.get(&region_id)
    }

    pub(crate) fn register(&self, peer: PeerFSM) {
        let id = peer.peer.region().id;
        let ver = peer.peer.region().get_region_epoch().get_version();
        info!(
            "register region {}:{}, peer {}",
            id,
            ver,
            peer.peer.peer_id()
        );
        let applier = Applier::new_from_peer(&peer);
        let new_peer = PeerState::new(applier, peer);
        self.peers.insert(id, new_peer);
    }

    pub(crate) fn close(&self, id: u64) {
        if let Some(peer) = self.peers.get(&id) {
            peer.closed.store(true, Ordering::Release);
            self.peers.remove(&peer.peer_fsm.peer.region().id);
        }
    }

    pub(crate) fn send(&self, id: u64, mut msg: PeerMsg) {}

    pub(crate) fn send_store(&self, msg: StoreMsg) {}
}
