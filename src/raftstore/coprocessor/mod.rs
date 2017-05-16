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

use kvproto::raft_cmdpb::{AdminRequest, Request, AdminResponse, Response};
use kvproto::metapb::Region;
use protobuf::RepeatedField;
use raftstore::store::PeerStorage;

pub use self::error::{Error, Result};


/// Coprocessor is used to provide a convient way to inject code to
/// KV processing.
pub trait Coprocessor {
    fn start(&mut self) -> ();
    fn stop(&mut self) -> ();
}

/// Context of observer.
pub struct ObserverContext<'a> {
    ps: &'a PeerStorage,
    /// A snapshot of requested region.
    snap: Option<RegionSnapshot>,
    /// Whether to bypass following observer hook.
    pub bypass: bool,
}

impl<'a> ObserverContext<'a> {
    pub fn new(peer: &PeerStorage) -> ObserverContext {
        ObserverContext {
            ps: peer,
            snap: None,
            bypass: false,
        }
    }

    pub fn region(&self) -> &Region {
        &self.ps.region
    }

    pub fn snap(&mut self) -> &RegionSnapshot {
        if self.snap.is_none() {
            self.snap = Some(RegionSnapshot::new(self.ps));
        }

        self.snap.as_ref().unwrap()
    }
}

/// Observer hook of region level.
pub trait RegionObserver: Coprocessor {
    /// Hook to call before execute admin request.
    fn pre_admin(&mut self, ctx: &mut ObserverContext, req: &mut AdminRequest) -> Result<()>;

    /// Hook to call before execute read/write request.
    fn pre_query(&mut self,
                 ctx: &mut ObserverContext,
                 req: &mut RepeatedField<Request>)
                 -> Result<()>;

    /// Hook to call after admin request being executed.
    fn post_admin(&mut self,
                  ctx: &mut ObserverContext,
                  req: &AdminRequest,
                  resp: &mut AdminResponse)
                  -> ();

    /// Hook to call after read/write request being executed.
    fn post_query(&mut self,
                  ctx: &mut ObserverContext,
                  req: &[Request],
                  resp: &mut RepeatedField<Response>)
                  -> ();
}
