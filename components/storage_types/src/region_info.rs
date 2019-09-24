// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use raft::StateRole;

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

pub type SeekRegionCallback = Box<dyn Fn(&mut dyn Iterator<Item = &RegionInfo>) + Send>;
