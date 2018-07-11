// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use raftstore;
use raftstore::store::msg::Msg;
use std::sync::mpsc::Sender;
use util::transport::SendCh;

pub trait MsgSender {
    fn send(&self, msg: Msg) -> raftstore::Result<()>;
    // same as send, but with retry.
    fn try_send(&self, msg: Msg) -> raftstore::Result<()>;
}

impl MsgSender for SendCh<Msg> {
    fn send(&self, msg: Msg) -> raftstore::Result<()> {
        SendCh::send(self, msg).map_err(|e| box_err!("{:?}", e))
    }

    fn try_send(&self, msg: Msg) -> raftstore::Result<()> {
        SendCh::try_send(self, msg).map_err(|e| box_err!("{:?}", e))
    }
}

impl MsgSender for Sender<Msg> {
    fn send(&self, msg: Msg) -> raftstore::Result<()> {
        Sender::send(self, msg).unwrap();
        Ok(())
    }

    fn try_send(&self, msg: Msg) -> raftstore::Result<()> {
        Sender::send(self, msg).unwrap();
        Ok(())
    }
}

pub mod apply;
mod cleanup_sst;
mod compact;
mod consistency_check;
mod metrics;
mod raftlog_gc;
mod region;
mod split_check;

pub use self::apply::{
    Apply, ApplyMetrics, ApplyRes, Proposal, RegionProposal, Registration, Runner as ApplyRunner,
    Task as ApplyTask, TaskRes as ApplyTaskRes,
};
pub use self::cleanup_sst::{Runner as CleanupSSTRunner, Task as CleanupSSTTask};
pub use self::compact::{Runner as CompactRunner, Task as CompactTask};
pub use self::consistency_check::{Runner as ConsistencyCheckRunner, Task as ConsistencyCheckTask};
pub use self::raftlog_gc::{Runner as RaftlogGcRunner, Task as RaftlogGcTask};
pub use self::region::{Runner as RegionRunner, Task as RegionTask, STALE_PEER_CHECK_INTERVAL};
pub use self::split_check::{Runner as SplitCheckRunner, Task as SplitCheckTask};
