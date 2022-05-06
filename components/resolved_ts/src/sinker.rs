// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use engine_traits::Snapshot;
use raftstore::{coprocessor::ObserveID, store::RegionSnapshot};
use txn_types::TimeStamp;

use crate::cmd::ChangeLog;

pub struct SinkCmd {
    pub region_id: u64,
    pub observe_id: ObserveID,
    pub logs: Vec<ChangeLog>,
}

pub trait CmdSinker<S: Snapshot>: Send {
    fn sink_cmd(&mut self, sink_cmd: Vec<SinkCmd>);

    fn sink_cmd_with_old_value(&mut self, sink_cmd: Vec<SinkCmd>, snapshot: RegionSnapshot<S>);

    fn sink_resolved_ts(&mut self, regions: Vec<u64>, ts: TimeStamp);
}

pub struct DummySinker<S: Snapshot>(PhantomData<S>);

impl<S: Snapshot> DummySinker<S> {
    pub fn new() -> Self {
        Self(PhantomData::default())
    }
}

impl<S: Snapshot> Default for DummySinker<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Snapshot> CmdSinker<S> for DummySinker<S> {
    fn sink_cmd(&mut self, _sink_cmd: Vec<SinkCmd>) {}

    fn sink_cmd_with_old_value(&mut self, _sink_cmd: Vec<SinkCmd>, _snapshot: RegionSnapshot<S>) {}

    fn sink_resolved_ts(&mut self, _regions: Vec<u64>, _ts: TimeStamp) {}
}
