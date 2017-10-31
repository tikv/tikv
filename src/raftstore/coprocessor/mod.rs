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

use rocksdb::DB;
use kvproto::raft_cmdpb::{AdminRequest, Request};
use kvproto::metapb::Region;
use protobuf::RepeatedField;

pub mod dispatcher;
pub mod split_observer;
pub mod config;
mod region_snapshot;
mod error;
mod metrics;
mod split_check;

pub use self::config::Config;
pub use self::region_snapshot::{RegionIterator, RegionSnapshot};
pub use self::dispatcher::{CoprocessorHost, Registry};
pub use self::error::{Error, Result};
pub use self::split_check::{SizeCheckObserver, Status as SplitCheckStatus,
                            SIZE_CHECK_OBSERVER_PRIORITY};

/// Coprocessor is used to provide a convient way to inject code to
/// KV processing.
pub trait Coprocessor {
    fn start(&self) {}
    fn stop(&self) {}
}

/// Context of observer.
pub struct ObserverContext<'a> {
    region: &'a Region,
    /// Whether to bypass following observer hook.
    pub bypass: bool,
}

impl<'a> ObserverContext<'a> {
    pub fn new(region: &Region) -> ObserverContext {
        ObserverContext {
            region: region,
            bypass: false,
        }
    }

    pub fn region(&self) -> &Region {
        self.region
    }
}

/// Observer hook of region level.
pub trait RegionObserver: Coprocessor {
    /// Hook to call before execute admin request.
    fn pre_admin(&self, _: &mut ObserverContext, _: &mut AdminRequest) -> Result<()> {
        Ok(())
    }

    /// Hook to call before execute read/write request.
    fn pre_query(&self, _: &mut ObserverContext, _: &mut RepeatedField<Request>) -> Result<()> {
        Ok(())
    }

    /// Hook to call before apply read/write request.
    ///
    /// Please note that improper implementation can lead to data inconsistency.
    fn pre_apply_query(&self, _: &mut ObserverContext, _: &mut RepeatedField<Request>) {}

    /// Hook to call before handle split region task. If it returns a None,
    /// then `on_split_check` can be skippped.
    //
    // This is a workaround for preserving status for split check observers.
    // TODO: Refactor RegionObserver, requires Send + Clone,
    //       so that ervery threads has its own RegionObservers.
    fn new_split_check_status(&self, _: &mut ObserverContext, _: &mut SplitCheckStatus, _: &DB) {}

    /// Hook to call for every check during split.
    fn on_split_check(
        &self,
        _: &mut ObserverContext,
        _: &mut SplitCheckStatus,
        _: &[u8],
        _: u64,
    ) -> Option<Vec<u8>> {
        None
    }
}
