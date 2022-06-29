// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! FSM is short for finite state machine. There are three types of FSMs,
//! - StoreFsm, used for handling control messages and global initialization.
//! - PeerFsm, used for handling messages specific for one raft peer.
//! - ApplyFsm, used for handling apply task for one raft peer.

mod apply;
mod peer;
mod store;
