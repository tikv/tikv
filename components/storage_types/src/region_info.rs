// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use raft::StateRole;
use std::error;

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

pub trait RegionInfoProvider: Send + Clone + 'static {
    /// Find the first region `r` whose range contains or greater than `from_key` and the peer on
    /// this TiKV satisfies `filter(peer)` returns true.
    fn seek_region(&self, from: &[u8], filter: SeekRegionCallback) -> RipResult<()>;
}

quick_error! {
    #[derive(Debug)]
    pub enum RipError {
        Other(err: Box<dyn error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

pub type RipResult<T> = std::result::Result<T, RipError>;

