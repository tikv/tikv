// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use raft::StateRole;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::mpsc;
use tikv_util::collections::HashMap;
use tikv_util::escape;

/// `RaftStoreEvent` Represents events dispatched from raftstore coprocessor.
#[derive(Debug)]
pub enum RaftStoreEvent {
    CreateRegion { region: Region, role: StateRole },
    UpdateRegion { region: Region, role: StateRole },
    DestroyRegion { region: Region },
    RoleChange { region: Region, role: StateRole },
}

impl RaftStoreEvent {
    pub fn get_region(&self) -> &Region {
        match self {
            RaftStoreEvent::CreateRegion { region, .. }
            | RaftStoreEvent::UpdateRegion { region, .. }
            | RaftStoreEvent::DestroyRegion { region, .. }
            | RaftStoreEvent::RoleChange { region, .. } => region,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RegionInfo {
    pub region: Region,
    pub role: StateRole,
}

impl RegionInfo {
    pub fn new(region: Region, role: StateRole) -> Self {
        Self { region, role }
    }
}

pub type RegionsMap = HashMap<u64, RegionInfo>;
pub type RegionRangesMap = BTreeMap<Vec<u8>, u64>;

pub type SeekRegionCallback = Box<dyn Fn(&mut dyn Iterator<Item = &RegionInfo>) + Send>;

/// `RegionInfoAccessor` has its own thread. Queries and updates are done by sending commands to the
/// thread.
pub enum RegionInfoQuery {
    RaftStoreEvent(RaftStoreEvent),
    SeekRegion {
        from: Vec<u8>,
        callback: SeekRegionCallback,
    },
    /// Gets all contents from the collection. Only used for testing.
    DebugDump(mpsc::Sender<(RegionsMap, RegionRangesMap)>),
}

impl Display for RegionInfoQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            RegionInfoQuery::RaftStoreEvent(e) => write!(f, "RaftStoreEvent({:?})", e),
            RegionInfoQuery::SeekRegion { from, .. } => {
                write!(f, "SeekRegion(from: {})", escape(from))
            }
            RegionInfoQuery::DebugDump(_) => write!(f, "DebugDump"),
        }
    }
}

