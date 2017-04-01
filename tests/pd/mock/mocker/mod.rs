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

#![allow(non_snake_case)]

use std::result;

use grpc::error::GrpcError;

use kvproto::pdpb::*;

mod service;
mod split;
mod bootstrap;
mod leader_change;

pub use self::service::Service;
pub use self::split::Split;
pub use self::bootstrap::Bootstrap;
pub use self::leader_change::LeaderChange;

pub const DEFAULT_CLUSTER_ID: u64 = 42;

pub type Result<T> = result::Result<T, GrpcError>;

pub trait Mocker {
    fn GetMembers(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        None
    }

    fn Tso(&self, _: &TsoRequest) -> Option<Result<TsoResponse>> {
        None
    }

    fn Bootstrap(&self, _: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        None
    }

    fn IsBootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        None
    }

    fn AllocID(&self, _: &AllocIDRequest) -> Option<Result<AllocIDResponse>> {
        None
    }

    fn GetStore(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        None
    }

    fn PutStore(&self, _: &PutStoreRequest) -> Option<Result<PutStoreResponse>> {
        None
    }

    fn StoreHeartbeat(&self, _: &StoreHeartbeatRequest) -> Option<Result<StoreHeartbeatResponse>> {
        None
    }

    fn RegionHeartbeat(&self,
                       _: &RegionHeartbeatRequest)
                       -> Option<Result<RegionHeartbeatResponse>> {
        None
    }

    fn GetRegion(&self, _: &GetRegionRequest) -> Option<Result<GetRegionResponse>> {
        None
    }

    fn GetRegionByID(&self, _: &GetRegionByIDRequest) -> Option<Result<GetRegionResponse>> {
        None
    }

    fn AskSplit(&self, _: &AskSplitRequest) -> Option<Result<AskSplitResponse>> {
        None
    }

    fn ReportSplit(&self, _: &ReportSplitRequest) -> Option<Result<ReportSplitResponse>> {
        None
    }

    fn GetClusterConfig(&self,
                        _: &GetClusterConfigRequest)
                        -> Option<Result<GetClusterConfigResponse>> {
        None
    }

    fn PutClusterConfig(&self,
                        _: &PutClusterConfigRequest)
                        -> Option<Result<PutClusterConfigResponse>> {
        None
    }
}
