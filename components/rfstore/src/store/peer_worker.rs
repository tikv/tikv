// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use std::sync::atomic::AtomicBool;

pub(crate) struct PeerState {
    pub(crate) applier: Applier,
    pub(crate) peer_fsm: PeerFSM,
    pub(crate) closed: AtomicBool,
}

impl PeerState {
    pub(crate) fn new(applier: Applier, peer_fsm: PeerFSM) -> Self {
        Self {
            applier,
            peer_fsm,
            closed: AtomicBool::new(false),
        }
    }
}
