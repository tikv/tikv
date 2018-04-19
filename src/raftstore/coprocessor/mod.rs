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

use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, Request, Response};
use protobuf::RepeatedField;
use raft::StateRole;
use rocksdb::DB;

pub mod config;
pub mod dispatcher;
mod error;
mod metrics;
mod split_check;
pub mod split_observer;

pub use self::config::Config;
pub use self::dispatcher::{CoprocessorHost, Registry};
pub use self::error::{Error, Result};
pub use self::split_check::{HalfCheckObserver, SizeCheckObserver, Status as SplitCheckStatus,
                            TableCheckObserver, HALF_SPLIT_OBSERVER_PRIORITY,
                            SIZE_CHECK_OBSERVER_PRIORITY, TABLE_CHECK_OBSERVER_PRIORITY};

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
            region,
            bypass: false,
        }
    }

    pub fn region(&self) -> &Region {
        self.region
    }
}

pub trait AdminObserver: Coprocessor {
    /// Hook to call before proposing admin request.
    fn pre_propose_admin(&self, _: &mut ObserverContext, _: &mut AdminRequest) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying admin request.
    fn pre_apply_admin(&self, _: &mut ObserverContext, _: &AdminRequest) {}

    /// Hook to call after applying admin request.
    fn post_apply_admin(&self, _: &mut ObserverContext, _: &mut AdminResponse) {}
}

pub trait QueryObserver: Coprocessor {
    /// Hook to call before proposing write request.
    ///
    /// We don't propose read request, hence there is no hook for it yet.
    fn pre_propose_query(
        &self,
        _: &mut ObserverContext,
        _: &mut RepeatedField<Request>,
    ) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying write request.
    fn pre_apply_query(&self, _: &mut ObserverContext, _: &[Request]) {}

    /// Hook to call after applying write request.
    fn post_apply_query(&self, _: &mut ObserverContext, _: &mut RepeatedField<Response>) {}
}

pub trait SplitCheckObserver: Coprocessor {
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

pub trait RoleObserver: Coprocessor {
    /// Hook to call when role of a peer changes.
    ///
    /// Please note that, this hook is not called at realtime. There maybe a
    /// situation that the hook is not called yet, however the role of some peers
    /// have changed.
    fn on_role_change(&self, _: &mut ObserverContext, _: StateRole) {}
}
