#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate quick_error;

#[macro_use]
extern crate serde_derive;

use futures::Future;
use kvproto::metapb;
use kvproto::pdpb;
use std::ops::Deref;

pub use self::errors::{Error, Result};

pub mod config;
pub mod errors;
pub mod metrics;

pub type PdFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;

#[derive(Default, Clone)]
pub struct RegionStat {
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub read_bytes: u64,
    pub read_keys: u64,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub last_report_ts: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RegionInfo {
    pub region: metapb::Region,
    pub leader: Option<metapb::Peer>,
}

impl RegionInfo {
    pub fn new(region: metapb::Region, leader: Option<metapb::Peer>) -> RegionInfo {
        RegionInfo { region, leader }
    }
}

impl Deref for RegionInfo {
    type Target = metapb::Region;

    fn deref(&self) -> &Self::Target {
        &self.region
    }
}

pub const REQUEST_TIMEOUT: u64 = 2; // 2s
