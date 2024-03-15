// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::result;

use kvproto::{
    meta_storagepb as mpb,
    pdpb::*,
    resource_manager::{TokenBucketsRequest, TokenBucketsResponse},
};

mod bootstrap;
pub mod etcd;
mod incompatible;
mod leader_change;
mod meta_storage;
mod retry;
mod service;
mod split;

pub use self::{
    bootstrap::AlreadyBootstrapped,
    incompatible::Incompatible,
    leader_change::LeaderChange,
    meta_storage::MetaStorage,
    retry::{NotRetry, Retry},
    service::Service,
    split::Split,
};

pub const DEFAULT_CLUSTER_ID: u64 = 42;

pub type Result<T> = result::Result<T, String>;

pub trait PdMocker {
    fn meta_store_get(&self, _req: mpb::GetRequest) -> Option<Result<mpb::GetResponse>> {
        None
    }

    fn meta_store_put(&self, _req: mpb::PutRequest) -> Option<Result<mpb::PutResponse>> {
        None
    }

    fn meta_store_delete(&self, _req: mpb::DeleteRequest) -> Option<Result<mpb::DeleteResponse>> {
        None
    }

    fn meta_store_watch(
        &self,
        _req: mpb::WatchRequest,
        _sink: grpcio::ServerStreamingSink<mpb::WatchResponse>,
        _ctx: &grpcio::RpcContext<'_>,
    ) -> bool {
        false
    }

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

    fn alloc_id(&self, _: &AllocIdRequest) -> Option<Result<AllocIdResponse>> {
        None
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        None
    }

    fn put_store(&self, _: &PutStoreRequest) -> Option<Result<PutStoreResponse>> {
        None
    }

    fn get_all_stores(&self, _: &GetAllStoresRequest) -> Option<Result<GetAllStoresResponse>> {
        None
    }

    fn store_heartbeat(&self, _: &StoreHeartbeatRequest) -> Option<Result<StoreHeartbeatResponse>> {
        None
    }

    fn region_heartbeat(
        &self,
        _: &RegionHeartbeatRequest,
    ) -> Option<Result<RegionHeartbeatResponse>> {
        None
    }

    fn report_buckets(&self, _: &ReportBucketsRequest) -> Option<Result<ReportBucketsResponse>> {
        None
    }

    fn get_region(&self, _: &GetRegionRequest) -> Option<Result<GetRegionResponse>> {
        None
    }

    fn get_region_by_id(&self, _: &GetRegionByIdRequest) -> Option<Result<GetRegionResponse>> {
        None
    }

    fn ask_split(&self, _: &AskSplitRequest) -> Option<Result<AskSplitResponse>> {
        None
    }

    fn ask_batch_split(&self, _: &AskBatchSplitRequest) -> Option<Result<AskBatchSplitResponse>> {
        None
    }

    fn report_batch_split(
        &self,
        _: &ReportBatchSplitRequest,
    ) -> Option<Result<ReportBatchSplitResponse>> {
        None
    }

    fn get_cluster_config(
        &self,
        _: &GetClusterConfigRequest,
    ) -> Option<Result<GetClusterConfigResponse>> {
        None
    }

    fn put_cluster_config(
        &self,
        _: &PutClusterConfigRequest,
    ) -> Option<Result<PutClusterConfigResponse>> {
        None
    }

    fn scatter_region(&self, _: &ScatterRegionRequest) -> Option<Result<ScatterRegionResponse>> {
        None
    }

    fn set_endpoints(&self, _: Vec<String>) {}

    fn update_gc_safe_point(
        &self,
        _: &UpdateGcSafePointRequest,
    ) -> Option<Result<UpdateGcSafePointResponse>> {
        None
    }

    fn get_gc_safe_point(
        &self,
        _: &GetGcSafePointRequest,
    ) -> Option<Result<GetGcSafePointResponse>> {
        None
    }

    fn get_operator(&self, _: &GetOperatorRequest) -> Option<Result<GetOperatorResponse>> {
        None
    }

    fn report_ru_metrics(&self, req: &TokenBucketsRequest) -> Option<Result<TokenBucketsResponse>> {
        req.get_requests().iter().for_each(|r| {
            assert_eq!(r.get_is_background(), true);
        });
        None
    }
}
