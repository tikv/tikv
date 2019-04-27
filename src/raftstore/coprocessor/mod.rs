// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::DB;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;

pub use raftstore2::coprocessor::config;
pub mod dispatcher;
pub use raftstore2::coprocessor::error;
pub use raftstore2::coprocessor::metrics;
pub use raftstore2::coprocessor::properties;
pub mod region_info_accessor;
mod split_check;
pub use raftstore2::coprocessor::split_observer;

pub use raftstore2::coprocessor::config::Config;
pub use self::dispatcher::{CoprocessorHost, Registry};
pub use raftstore2::coprocessor::error::{Error, Result};
pub use self::region_info_accessor::{RegionInfo, RegionInfoAccessor, SeekRegionCallback};
pub use self::split_check::{
    get_region_approximate_keys, get_region_approximate_keys_cf, get_region_approximate_middle,
    get_region_approximate_size, get_region_approximate_size_cf, HalfCheckObserver,
    Host as SplitCheckerHost, KeysCheckObserver, SizeCheckObserver, TableCheckObserver,
};

pub use crate::raftstore::store::KeyEntry;

pub use raftstore2::coprocessor::{
    Coprocessor, ObserverContext, AdminObserver,
    QueryObserver,
    RoleObserver, RegionChangeEvent, RegionChangeObserver,
};

/// SplitChecker is invoked during a split check scan, and decides to use
/// which keys to split a region.
pub trait SplitChecker {
    /// Hook to call for every kv scanned during split.
    ///
    /// Return true to abort scan early.
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, _: &KeyEntry) -> bool {
        false
    }

    /// Get the desired split keys.
    fn split_keys(&mut self) -> Vec<Vec<u8>>;

    /// Get approximate split keys without scan.
    fn approximate_split_keys(&mut self, _: &Region, _: &DB) -> Result<Vec<Vec<u8>>> {
        Ok(vec![])
    }

    /// Get split policy.
    fn policy(&self) -> CheckPolicy;
}

pub trait SplitCheckObserver: Coprocessor {
    /// Add a checker for a split scan.
    fn add_checker(
        &self,
        _: &mut ObserverContext<'_>,
        _: &mut SplitCheckerHost,
        _: &DB,
        policy: CheckPolicy,
    );
}
