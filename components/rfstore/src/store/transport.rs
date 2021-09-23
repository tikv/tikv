// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;
use kvproto::*;

/// Transports messages between different Raft peers.
pub trait Transport: Send + Clone {
    fn send(&mut self, msg: raft_serverpb::RaftMessage) -> Result<()>;

    fn need_flush(&self) -> bool;

    fn flush(&mut self);
}
