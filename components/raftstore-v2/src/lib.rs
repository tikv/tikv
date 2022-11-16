// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! Raftstore is the place where we implement multi-raft.
//!
//! The thread module of raftstore is batch-system, more check
//! components/batch-system. All state machines are defined in [`fsm`] module.
//! Everything that wrapping raft is implemented in [`raft`] module. And the
//! commands, including split/merge/confchange/read/write, are implemented in
//! [`operation`] module. All state machines are expected to communicate with
//! messages. They are defined in [`router`] module.

// You may get confused about the peer, or other structs like apply, in fsm and
// peer in raft module. The guideline is that if any field doesn't depend on
// the details of batch system, then it should be defined for peer in raft
// module.
//
// If we change to other concurrent programming solution, we can easily just
// change the peer in fsm.
//
// Any accessors should be defined in the file where the struct is defined.
// Functionalities like read, write, etc should be implemented in [`operation`]
// using a standalone modules.

#![allow(unused)]
#![feature(let_chains)]
#![feature(array_windows)]

mod batch;
mod bootstrap;
mod fsm;
mod operation;
mod raft;
pub mod router;
mod tablet;

pub(crate) use batch::StoreContext;
pub use batch::{create_store_batch_system, StoreRouter, StoreSystem};
pub use bootstrap::Bootstrap;
pub use fsm::StoreMeta;
pub use raftstore::{Error, Result};
