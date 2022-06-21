// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! Raftstore is the place where we implement multi-raft.
//!
//! The thread module of raftstore is batch-system, more check components/batch-system.
//! All state machines are defined in [`fsm`] module. Everything that wrapping raft is
//! implemented in [`raft`] module. And the commands are implemented in [`operation`] module.
//! All state machines are expected to communicate with messages. They are defined in
//! [`router`] module.

#![allow(unused)]

mod fsm;
mod operation;
mod raft;
mod router;

pub use raftstore::{Error, Result};
pub use router::{PeerMsg, PeerTick, StoreMsg, StoreTick};
