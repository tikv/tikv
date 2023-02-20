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

fn make_config_key(path: &str, name: &str) -> MetaKey {
    let key = format!(
        "{}/{}",
        path.trim_end_matches("/"),
        name.trim_start_matches("/")
    )
    .trim_end_matches("/")
    .to_owned()
    .into_bytes();
    MetaKey(key)
}

pub trait PdMocker {
    fn load_global_config(
        &self,
        req: &LoadGlobalConfigRequest,
        etcd_client: EtcdClient,
    ) -> Option<Result<LoadGlobalConfigResponse>> {
        let make_response = |kvs: Vec<KeyValue>, rev: i64| {
            let mut res = LoadGlobalConfigResponse::default();
            let values: Vec<GlobalConfigItem> = kvs
                .iter()
                .map(|kv| {
                    let mut item = GlobalConfigItem::default();
                    item.set_name(String::from_utf8(kv.key().to_vec()).unwrap());
                    item.set_payload(kv.value().into());
                    item
                })
                .collect();
            res.set_revision(rev);
            res.set_items(values.into());
            res
        };

        if req.get_names().is_empty() {
            let (resp, revision) = block_on(async move {
                etcd_client.lock().await.get_key(Keys::Prefix(MetaKey(
                    req.get_config_path().as_bytes().to_vec(),
                )))
            });
            let resp = make_response(resp, revision);
            return Some(Ok(resp));
        }

        let cfg_path = req.get_config_path();

        let values = req
            .get_names()
            .iter()
            .flat_map(|name| {
                let (resp, _) = block_on(etcd_client.lock())
                    .get_key(Keys::Key(make_config_key(cfg_path, name)));
                resp
            })
            .collect::<Vec<_>>();
        let resp = make_response(values, 0);

        Some(Ok(resp))
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
                        let kv = KeyValue(
                            make_config_key(req.get_config_path(), item.get_name()),
                            item.get_payload().into(),
                        );
                        cli.lock().await.set(kv).await
                    }
                    EventType::Delete => {
                        let key =
                            Keys::Key(make_config_key(req.get_config_path(), item.get_name()));
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
