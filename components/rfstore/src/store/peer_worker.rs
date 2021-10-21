// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct PeerState {
    pub(crate) applier: Arc<Mutex<Applier>>,
    pub(crate) peer_fsm: Arc<Mutex<PeerFSM>>,
    pub(crate) closed: Arc<AtomicBool>,
}

impl PeerState {
    pub(crate) fn new(applier: Applier, peer_fsm: PeerFSM) -> Self {
        Self {
            applier: Arc::new(Mutex::new(applier)),
            peer_fsm: Arc::new(Mutex::new(peer_fsm)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}
