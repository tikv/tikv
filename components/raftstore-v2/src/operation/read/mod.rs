// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! There are two types of read:
//! - If the ReadDelegate is in the leader lease status, the read is operated
//!   locally and need not to go through the raft layer (namely local read).
//! - Otherwise, redirect the request to the raftstore and proposed as a
//!   RaftCommand in the raft layer.

mod local;
