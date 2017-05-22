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

mod region_snapshot;
pub mod dispatcher;
pub mod split_observer;
mod error;

pub use self::region_snapshot::{RegionSnapshot, RegionIterator};
pub use self::dispatcher::{CoprocessorHost, Registry};

use kvproto::raft_cmdpb::{AdminRequest, Request};
use kvproto::metapb::Region;
use protobuf::RepeatedField;

pub use self::error::{Error, Result};


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
    /// Please note that inproper implementation can lead to data inconsistency.
    fn pre_apply_query(&self, _: &mut ObserverContext, _: &mut RepeatedField<Request>) {}
}
