// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::Snapshot;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use txn_types::TimeStamp;

use crate::cmd::ChangeLog;

pub struct SinkCmd {
    pub region_id: u64,
    pub observe_id: ObserveID,
    pub logs: Vec<ChangeLog>,
}

pub trait CmdSinker<S: Snapshot>: Send {
    fn sink_cmd(&mut self, sink_cmd: Vec<SinkCmd>, snapshot: RegionSnapshot<S>);

    fn sink_resolved_ts(&mut self, regions: Vec<u64>, ts: TimeStamp);
}
