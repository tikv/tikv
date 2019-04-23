#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use(
    kv,
    slog_kv,
    slog_error,
    slog_warn,
    slog_info,
    slog_debug,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate tikv_util;

use futures::Future;
use kvproto::metapb;
use kvproto::pdpb;
use std::ops::Deref;

pub use self::config::Config;
pub use self::errors::{Error, Result};

pub mod config;
pub mod errors;
pub mod metrics;
pub mod util;

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
