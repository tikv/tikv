// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;

use engine_traits::Snapshot;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::CmdBatch;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use txn_types::TimeStamp;

use crate::errors::Error;

pub enum Task<S: Snapshot> {
    RegionDestroyed(Region),
    RegionUpdated(Region),
    RegionRoleChanged {
        region: Region,
        role: StateRole,
    },
    RegionError {
        region: Region,
        observe_id: ObserveID,
        error: Box<Error>,
    },
    RegisterAdvanceEvent,
    AdvanceResolvedTs {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    ChangeLog {
        cmd_batch: Vec<CmdBatch>,
        snapshot: RegionSnapshot<S>,
    },
    // ScanLocks {
    //     region_id: u64,
    //     observe_id: ObserveID,
    //     entry: ScanEntry,
    // },
}

impl<S: Snapshot> fmt::Debug for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("ResolvedTsTask");
        match self {
            Task::RegionDestroyed(ref region) => de
                .field("name", &"region_destroyed")
                .field("region", &region)
                .finish(),
            Task::RegionUpdated(ref region) => de
                .field("name", &"region_updated")
                .field("region", &region)
                .finish(),
            Task::RegionRoleChanged {
                ref region,
                ref role,
            } => de
                .field("name", &"region_role_changed")
                .field("region", &region)
                .field("role", &role)
                .finish(),
            Task::RegionError {
                ref region,
                ref observe_id,
                ref error,
            } => de
                .field("name", &"region_error")
                .field("region", &region)
                .field("observe_id", &observe_id)
                .field("error", &error)
                .finish(),
            Task::AdvanceResolvedTs {
                ref regions,
                ref ts,
            } => de
                .field("name", &"advance_resolved_ts")
                .field("regions", &regions)
                .field("ts", &ts)
                .finish(),
            Task::ChangeLog { .. } => de.field("name", &"change_log").finish(),
            // Task::ScanLocks {
            //     ref region_id,
            //     ref observe_id,
            //     ..
            // } => de
            //     .field("name", &"scan_locks")
            //     .field("region_id", &region_id)
            //     .field("observe_id", &observe_id)
            //     .finish(),
            Task::RegisterAdvanceEvent => de.field("name", &"register_advance_event").finish(),
        }
    }
}

impl<S: Snapshot> fmt::Display for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
