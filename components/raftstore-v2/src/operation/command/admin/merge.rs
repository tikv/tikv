// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains merge related processing logic.
//!
//! ## Propose (`propose_prepare_merge`)
//!
//!  
//!
//! ## Apply (`apply_prepare_merge`)
//!
//! ## On Apply Result (`on_ready_prepare_merge`)

use engine_traits::{KvEngine, RaftEngine};

use crate::raft::Peer;

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_merge_check_tick(&mut self) {
        // TODO
    }
}
