// Copyright 2017 PingCAP, Inc.
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

use std::result;

use kvproto::pdpb::*;

mod service;
mod split;
mod bootstrap;
mod leader_change;
mod retry;

pub use self::service::Service;
pub use self::split::Split;
pub use self::bootstrap::AlreadyBootstrapped;
pub use self::leader_change::LeaderChange;
pub use self::retry::Retry;

pub const DEFAULT_CLUSTER_ID: u64 = 42;

pub type Result<T> = result::Result<T, String>;

pub trait Mocker {
    fn get_members(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        None
    }

    fn tso(&self, _: &TsoRequest) -> Option<Result<TsoResponse>> {
        None
    }

    fn bootstrap(&self, _: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        None
    }

    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        None
    }

    fn alloc_id(&self, _: &AllocIDRequest) -> Option<Result<AllocIDResponse>> {
        None
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        None
    }

    fn put_store(&self, _: &PutStoreRequest) -> Option<Result<PutStoreResponse>> {
        None
    }

    fn store_heartbeat(&self, _: &StoreHeartbeatRequest) -> Option<Result<StoreHeartbeatResponse>> {
        None
    }

    fn region_heartbeat(&self,
                        _: &RegionHeartbeatRequest)
                        -> Option<Result<RegionHeartbeatResponse>> {
        None
    }

    fn get_region(&self, _: &GetRegionRequest) -> Option<Result<GetRegionResponse>> {
        None
    }

    fn get_region_by_id(&self, _: &GetRegionByIDRequest) -> Option<Result<GetRegionResponse>> {
        None
    }

    fn ask_split(&self, _: &AskSplitRequest) -> Option<Result<AskSplitResponse>> {
        None
    }

    fn report_split(&self, _: &ReportSplitRequest) -> Option<Result<ReportSplitResponse>> {
        None
    }

    fn get_cluster_config(&self,
                          _: &GetClusterConfigRequest)
                          -> Option<Result<GetClusterConfigResponse>> {
        None
    }

    fn put_cluster_config(&self,
                          _: &PutClusterConfigRequest)
                          -> Option<Result<PutClusterConfigResponse>> {
        None
    }

    fn set_endpoints(&self, _: Vec<String>) {}
}
