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


use raftstore::store::msg::Msg;
use raftstore;
use util::transport::SendCh;
use std::sync::mpsc::Sender;

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

mod region;
mod split_check;
mod compact;
mod raftlog_gc;
mod pd;
mod metrics;
mod consistency_check;
pub mod apply;

pub use self::region::{Task as RegionTask, Runner as RegionRunner};
pub use self::split_check::{Task as SplitCheckTask, Runner as SplitCheckRunner};
pub use self::compact::{Task as CompactTask, Runner as CompactRunner};
pub use self::raftlog_gc::{Task as RaftlogGcTask, Tasks as RaftlogGcTasks,
                           Runner as RaftlogGcRunner};
pub use self::pd::{Task as PdTask, Runner as PdRunner};
pub use self::consistency_check::{Task as ConsistencyCheckTask, Runner as ConsistencyCheckRunner};
pub use self::apply::{Task as ApplyTask, Runner as ApplyRunner, TaskRes as ApplyTaskRes, ApplyRes,
                      ApplyMetrics, Registration, Apply};
