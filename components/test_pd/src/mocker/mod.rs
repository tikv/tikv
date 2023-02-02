// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::result;

use futures::executor::block_on;
use kvproto::pdpb::*;

mod bootstrap;
pub mod etcd;
mod incompatible;
mod leader_change;
mod retry;
mod service;
mod split;

use self::etcd::{EtcdClient, KeyValue, Keys, MetaKey};
pub use self::{
    bootstrap::AlreadyBootstrapped,
    incompatible::Incompatible,
    leader_change::LeaderChange,
    retry::{NotRetry, Retry},
    service::Service,
    split::Split,
};

pub const DEFAULT_CLUSTER_ID: u64 = 42;

pub type Result<T> = result::Result<T, String>;

pub trait PdMocker {
    fn load_global_config(
        &self,
        _req: &LoadGlobalConfigRequest,
        etcd_client: EtcdClient,
    ) -> Option<Result<LoadGlobalConfigResponse>> {
        let mut res = LoadGlobalConfigResponse::default();
        let mut items = Vec::new();
        let (resp, revision) = block_on(async move {
            etcd_client.lock().await.get_key(Keys::Range(
                MetaKey(b"".to_vec()),
                MetaKey(b"\xff".to_vec()),
            ))
        });

        let values: Vec<GlobalConfigItem> = resp
            .iter()
            .map(|kv| {
                let mut item = GlobalConfigItem::default();
                item.set_name(String::from_utf8(kv.key().to_vec()).unwrap());
                item.set_payload(kv.value().into());
                item
            })
            .collect();

        items.extend(values);
        res.set_revision(revision);
        res.set_items(items.into());
        Some(Ok(res))
    }

    fn store_global_config(
        &self,
        req: &StoreGlobalConfigRequest,
        etcd_client: EtcdClient,
    ) -> Option<Result<StoreGlobalConfigResponse>> {
        for item in req.get_changes() {
            let cli = etcd_client.clone();
            block_on(async move {
                match item.get_kind() {
                    EventType::Put => {
                        let kv =
                            KeyValue(MetaKey(item.get_name().into()), item.get_payload().into());
                        cli.lock().await.set(kv).await
                    }
                    EventType::Delete => {
                        let key = Keys::Key(MetaKey(item.get_name().into()));
                        cli.lock().await.delete(key).await
                    }
                }
            })
            .unwrap();
        }
        Some(Ok(StoreGlobalConfigResponse::default()))
    }

    fn watch_global_config(
        &self,
        _req: &WatchGlobalConfigRequest,
    ) -> Option<Result<WatchGlobalConfigResponse>> {
        unimplemented!()
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
}
