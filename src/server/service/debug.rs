// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use futures::{
    future::{Future, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::{self, TryStreamExt},
};
use kvproto::debugpb::{self, get_range_properties_response::RangeProperty, *};
use raftstore::store::fsm::store::StoreRegionMeta;
use tikv_kv::RaftExtension;
use tikv_util::{future::paired_future_callback, metrics};
use tokio::runtime::Handle;

use crate::server::debug::{Debugger, Error, Result};

fn error_to_tonic_status(e: Error) -> tonic::Status {
    match e {
        Error::NotFound(msg) => tonic::Status::not_found(msg),
        Error::InvalidArgument(msg) => tonic::Status::invalid_argument(msg),
        Error::Other(e) => tonic::Status::unknown(format!("{:?}", e)),
        Error::EngineTrait(e) => tonic::Status::unknown(format!("{:?}", e)),
        Error::FlashbackFailed(msg) => tonic::Status::unknown(msg),
    }
}

pub type Callback<T> = Box<dyn FnOnce(T) + Send>;
pub type ResolvedTsDiagnosisCallback = Callback<
    Option<(
        bool, // stopped
        u64,  // resolved_ts
        u64,  // tracked index
        u64,  // num_locks
        u64,  // num_transactions
    )>,
>;
pub type ScheduleResolvedTsTask = Arc<
    dyn Fn(
            u64,  // region id
            bool, // log_locks
            u64,  // min_start_ts
            ResolvedTsDiagnosisCallback,
        ) -> bool
        + Send
        + Sync,
>;

/// Service handles the RPC messages for the `Debug` service.
pub struct Service<T, D, S>
where
    T: RaftExtension + Clone,
    D: Debugger + Clone,
    S: StoreRegionMeta,
{
    pool: Handle,
    debugger: D,
    raft_router: T,
    store_meta: Arc<Mutex<S>>,
    resolved_ts_scheduler: ScheduleResolvedTsTask,
}

impl<T, D, S> Clone for Service<T, D, S>
where
    T: RaftExtension + Clone,
    D: Debugger + Clone,
    S: StoreRegionMeta,
{
    fn clone(&self) -> Self {
        Service {
            pool: self.pool.clone(),
            debugger: self.debugger.clone(),
            raft_router: self.raft_router.clone(),
            store_meta: self.store_meta.clone(),
            resolved_ts_scheduler: self.resolved_ts_scheduler.clone(),
        }
    }
}

impl<T, D, S> Service<T, D, S>
where
    T: RaftExtension + Clone,
    D: Debugger + Clone,
    S: StoreRegionMeta,
{
    /// Constructs a new `Service` with `Engines`, a `RaftExtension`, a
    /// `GcWorker` and a `RegionInfoAccessor`.
    pub fn new(
        debugger: D,
        pool: Handle,
        raft_router: T,
        store_meta: Arc<Mutex<S>>,
        resolved_ts_scheduler: ScheduleResolvedTsTask,
    ) -> Self {
        Service {
            pool,
            debugger,
            raft_router,
            store_meta,
            resolved_ts_scheduler,
        }
    }
}

#[tonic::async_trait]
impl<T, D, S> debugpb::debug_server::Debug for Service<T, D, S>
where
    T: RaftExtension + Sync + 'static,
    D: Debugger + Clone + Send + Sync + 'static,
    S: StoreRegionMeta + Sync + 'static,
{
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> std::result::Result<tonic::Response<GetResponse>, tonic::Status> {
        const TAG: &str = "debug_get";
        let mut req = request.into_inner();
        let db = req.get_db();
        let cf = req.take_cf();
        let key = req.take_key();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.get(db, &cf, key.as_slice()) });
        match join.await.unwrap() {
            Ok(value) => {
                let mut resp = GetResponse::default();
                resp.set_value(value);
                Ok(tonic::Response::new(resp))
            }
            Err(e) => Err(error_to_tonic_status(e)),
        }
    }

    async fn raft_log(
        &self,
        request: tonic::Request<RaftLogRequest>,
    ) -> std::result::Result<tonic::Response<RaftLogResponse>, tonic::Status> {
        const TAG: &str = "debug_raft_log";
        let req = request.into_inner();
        let region_id = req.get_region_id();
        let log_index = req.get_log_index();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.raft_log(region_id, log_index) });
        match join.await.unwrap() {
            Ok(v) => {
                let mut resp = RaftLogResponse::default();
                resp.set_entry(v);
                Ok(tonic::Response::new(resp))
            }
            Err(e) => Err(error_to_tonic_status(e)),
        }
    }

    async fn region_info(
        &self,
        request: tonic::Request<RegionInfoRequest>,
    ) -> std::result::Result<tonic::Response<RegionInfoResponse>, tonic::Status> {
        const TAG: &str = "debug_region_log";
        let req = request.into_inner();
        let region_id = req.get_region_id();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.region_info(region_id) });
        match join.await.unwrap() {
            Ok(region_info) => {
                let mut resp = RegionInfoResponse::default();
                if let Some(raft_local_state) = region_info.raft_local_state {
                    resp.set_raft_local_state(raft_local_state);
                }
                if let Some(raft_apply_state) = region_info.raft_apply_state {
                    resp.set_raft_apply_state(raft_apply_state);
                }
                if let Some(region_state) = region_info.region_local_state {
                    resp.set_region_local_state(region_state);
                }
                Ok(tonic::Response::new(resp))
            }
            Err(e) => Err(error_to_tonic_status(e)),
        }
    }

    async fn region_size(
        &self,
        request: tonic::Request<RegionSizeRequest>,
    ) -> std::result::Result<tonic::Response<RegionSizeResponse>, tonic::Status> {
        const TAG: &str = "debug_region_size";
        let mut req = request.into_inner();
        let region_id = req.get_region_id();
        let cfs = req.take_cfs().into();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.region_size(region_id, cfs) });
        match join.await.unwrap() {
            Ok(entries) => {
                let mut resp = RegionSizeResponse::default();
                resp.set_entries(
                    entries
                        .into_iter()
                        .map(|(cf, size)| {
                            let mut entry = region_size_response::Entry::default();
                            entry.set_cf(cf);
                            entry.set_size(size as u64);
                            entry
                        })
                        .collect(),
                );
                Ok(tonic::Response::new(resp))
            }
            Err(e) => Err(error_to_tonic_status(e)),
        }
    }

    async fn scan_mvcc(
        &self,
        request: tonic::Request<ScanMvccRequest>,
    ) -> std::result::Result<
        tonic::Response<tonic::codegen::BoxStream<ScanMvccResponse>>,
        tonic::Status,
    > {
        let debugger = self.debugger.clone();
        let mut req = request.into_inner();
        let from = req.take_from_key();
        let to = req.take_to_key();
        let limit = req.get_limit();

        let iter = match debugger.scan_mvcc(&from, &to, limit) {
            Ok(i) => i,
            Err(e) => {
                return Err(error_to_tonic_status(e));
            }
        };

        let s = stream::iter(iter)
            .map_err(|e| box_err!(e))
            .map_err(|e| error_to_tonic_status(e))
            .map_ok(|(key, mvcc_info)| {
                let mut resp = ScanMvccResponse::default();
                resp.set_key(key);
                resp.set_info(mvcc_info);
                resp
            });
        Ok(tonic::Response::new(Box::pin(s) as _))
    }

    async fn compact(
        &self,
        request: tonic::Request<CompactRequest>,
    ) -> std::result::Result<tonic::Response<CompactResponse>, tonic::Status> {
        let debugger = self.debugger.clone();
        let req = request.into_inner();
        let res = self.pool.spawn(async move {
            debugger
                .compact(
                    req.get_db(),
                    req.get_cf(),
                    req.get_from_key(),
                    req.get_to_key(),
                    req.get_threads(),
                    req.get_bottommost_level_compaction().into(),
                )
                .map(|_| CompactResponse::default())
        });

        match res.await.unwrap() {
            Ok(r) => Ok(tonic::Response::new(r)),
            Err(e) => Err(error_to_tonic_status(e)),
        }
    }

    async fn inject_fail_point(
        &self,
        request: tonic::Request<InjectFailPointRequest>,
    ) -> std::result::Result<tonic::Response<InjectFailPointResponse>, tonic::Status> {
        const TAG: &str = "debug_inject_fail_point";
        let mut req = request.into_inner();
        self.pool
            .spawn(async move {
                let name = req.take_name();
                if name.is_empty() {
                    return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
                }
                let actions = req.get_actions();
                if let Err(e) = fail::cfg(name, actions) {
                    return Err(box_err!("{:?}", e));
                }
                Ok(InjectFailPointResponse::default())
            })
            .await
            .unwrap()
            .map(|r| tonic::Response::new(r))
            .map_err(error_to_tonic_status)
    }

    async fn recover_fail_point(
        &self,
        request: tonic::Request<RecoverFailPointRequest>,
    ) -> std::result::Result<tonic::Response<RecoverFailPointResponse>, tonic::Status> {
        const TAG: &str = "debug_recover_fail_point";
        let mut req = request.into_inner();
        self.pool
            .spawn(async move {
                let name = req.take_name();
                if name.is_empty() {
                    return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
                }
                fail::remove(name);
                Ok(RecoverFailPointResponse::default())
            })
            .await
            .unwrap()
            .map(|r| tonic::Response::new(r))
            .map_err(error_to_tonic_status)
    }

    async fn list_fail_points(
        &self,
        _request: tonic::Request<ListFailPointsRequest>,
    ) -> std::result::Result<tonic::Response<ListFailPointsResponse>, tonic::Status> {
        const TAG: &str = "debug_list_fail_points";

        self.pool
            .spawn(async move {
                let list = fail::list().into_iter().map(|(name, actions)| {
                    let mut entry = list_fail_points_response::Entry::default();
                    entry.set_name(name);
                    entry.set_actions(actions);
                    entry
                });
                let mut resp = ListFailPointsResponse::default();
                resp.set_entries(list.collect());
                Ok(resp)
            })
            .await
            .unwrap()
            .map(|r| tonic::Response::new(r))
            .map_err(error_to_tonic_status)
    }

    async fn get_metrics(
        &self,
        request: tonic::Request<GetMetricsRequest>,
    ) -> std::result::Result<tonic::Response<GetMetricsResponse>, tonic::Status> {
        const TAG: &str = "debug_get_metrics";
        let req = request.into_inner();
        let debugger = self.debugger.clone();
        self.pool
            .spawn(async move {
                let mut resp = GetMetricsResponse::default();
                resp.set_store_id(debugger.get_store_ident()?.store_id);
                resp.set_prometheus(metrics::dump(false));
                if req.get_all() {
                    resp.set_rocksdb_kv(debugger.dump_kv_stats()?);
                    resp.set_rocksdb_raft(debugger.dump_raft_stats()?);
                    resp.set_jemalloc(tikv_alloc::dump_stats());
                }
                Ok(resp)
            })
            .await
            .unwrap()
            .map(|r| tonic::Response::new(r))
            .map_err(error_to_tonic_status)
    }

    async fn check_region_consistency(
        &self,
        request: tonic::Request<RegionConsistencyCheckRequest>,
    ) -> std::result::Result<tonic::Response<RegionConsistencyCheckResponse>, tonic::Status> {
        let req = request.into_inner();
        let region_id = req.get_region_id();
        let f = self.raft_router.check_consistency(region_id);
        let task = async move {
            box_try!(f.await);
            Ok(())
        };
        self.pool
            .spawn(task)
            .await
            .unwrap()
            .map(|_| tonic::Response::new(RegionConsistencyCheckResponse::default()))
            .map_err(error_to_tonic_status)
    }

    async fn modify_tikv_config(
        &self,
        request: tonic::Request<ModifyTikvConfigRequest>,
    ) -> std::result::Result<tonic::Response<ModifyTikvConfigResponse>, tonic::Status> {
        const TAG: &str = "modify_tikv_config";
        let mut req = request.into_inner();
        let config_name = req.take_config_name();
        let config_value = req.take_config_value();
        let debugger = self.debugger.clone();

        self.pool
            .spawn(async move { debugger.modify_tikv_config(&config_name, &config_value) })
            .await
            .unwrap()
            .map(|_| tonic::Response::new(ModifyTikvConfigResponse::default()))
            .map_err(error_to_tonic_status)
    }

    async fn get_region_properties(
        &self,
        request: tonic::Request<GetRegionPropertiesRequest>,
    ) -> std::result::Result<tonic::Response<GetRegionPropertiesResponse>, tonic::Status> {
        const TAG: &str = "get_region_properties";
        let debugger = self.debugger.clone();
        let req = request.into_inner();
        self.pool
            .spawn(async move { debugger.get_region_properties(req.get_region_id()) })
            .await
            .unwrap()
            .map(|props| {
                let mut resp = GetRegionPropertiesResponse::default();
                for (name, value) in props {
                    let mut prop = Property::default();
                    prop.set_name(name);
                    prop.set_value(value);
                    resp.mut_props().push(prop);
                }
                tonic::Response::new(resp)
            })
            .map_err(error_to_tonic_status)
    }

    async fn get_range_properties(
        &self,
        request: tonic::Request<GetRangePropertiesRequest>,
    ) -> std::result::Result<tonic::Response<GetRangePropertiesResponse>, tonic::Status> {
        const TAG: &str = "get_range_properties";
        let debugger = self.debugger.clone();
        let req = request.into_inner();
        self.pool
            .spawn(async move {
                debugger.get_range_properties(req.get_start_key(), req.get_end_key())
            })
            .await
            .unwrap()
            .map(|props| {
                let mut resp = GetRangePropertiesResponse::default();
                for (key, value) in props {
                    let mut prop = RangeProperty::default();
                    prop.set_key(key);
                    prop.set_value(value);
                    resp.mut_properties().push(prop)
                }
                tonic::Response::new(resp)
            })
            .map_err(error_to_tonic_status)
    }

    async fn get_store_info(
        &self,
        _request: tonic::Request<GetStoreInfoRequest>,
    ) -> std::result::Result<tonic::Response<GetStoreInfoResponse>, tonic::Status> {
        const TAG: &str = "debug_get_store_id";
        let debugger = self.debugger.clone();

        self.pool
            .spawn(async move {
                let mut resp = GetStoreInfoResponse::default();
                match debugger.get_store_ident() {
                    Ok(ident) => {
                        resp.set_store_id(ident.get_store_id());
                        resp.set_api_version(ident.get_api_version());
                    }
                    Err(_) => resp.set_store_id(0),
                }
                Ok(resp)
            })
            .await
            .unwrap()
            .map(|resp| tonic::Response::new(resp))
            .map_err(error_to_tonic_status)
    }

    async fn get_cluster_info(
        &self,
        _request: tonic::Request<GetClusterInfoRequest>,
    ) -> std::result::Result<tonic::Response<GetClusterInfoResponse>, tonic::Status> {
        const TAG: &str = "debug_get_cluster_id";
        let debugger = self.debugger.clone();

        self.pool
            .spawn(async move {
                let mut resp = GetClusterInfoResponse::default();
                match debugger.get_store_ident() {
                    Ok(ident) => resp.set_cluster_id(ident.get_cluster_id()),
                    Err(_) => resp.set_cluster_id(0),
                }
                Ok(resp)
            })
            .await
            .unwrap()
            .map(|resp| tonic::Response::new(resp))
            .map_err(error_to_tonic_status)
    }

    async fn get_all_regions_in_store(
        &self,
        _request: tonic::Request<GetAllRegionsInStoreRequest>,
    ) -> std::result::Result<tonic::Response<GetAllRegionsInStoreResponse>, tonic::Status> {
        const TAG: &str = "debug_get_all_regions_in_store";
        let debugger = self.debugger.clone();

        self.pool
            .spawn(async move {
                let mut resp = GetAllRegionsInStoreResponse::default();
                match debugger.get_all_regions_in_store() {
                    Ok(regions) => resp.set_regions(regions),
                    Err(_) => resp.set_regions(vec![]),
                }
                Ok(resp)
            })
            .await
            .unwrap()
            .map(|resp| tonic::Response::new(resp))
            .map_err(error_to_tonic_status)
    }

    async fn reset_to_version(
        &self,
        request: tonic::Request<ResetToVersionRequest>,
    ) -> std::result::Result<tonic::Response<ResetToVersionResponse>, tonic::Status> {
        let req = request.into_inner();
        self.debugger.reset_to_version(req.get_ts());
        Ok(tonic::Response::new(ResetToVersionResponse::default()))
    }

    async fn flashback_to_version(
        &self,
        request: tonic::Request<FlashbackToVersionRequest>,
    ) -> std::result::Result<tonic::Response<FlashbackToVersionResponse>, tonic::Status> {
        let debugger = self.debugger.clone();
        let req = request.into_inner();
        self.pool
            .spawn(async move {
                let check = debugger.key_range_flashback_to_version(
                    req.get_version(),
                    req.get_region_id(),
                    req.get_start_key(),
                    req.get_end_key(),
                    req.get_start_ts(),
                    req.get_commit_ts(),
                );
                match check.await {
                    Ok(_) => Ok(FlashbackToVersionResponse::default()),
                    Err(err) => Err(err),
                }
            })
            .await
            .unwrap()
            .map(|resp| tonic::Response::new(resp))
            .map_err(error_to_tonic_status)
    }

    async fn get_region_read_progress(
        &self,
        request: tonic::Request<GetRegionReadProgressRequest>,
    ) -> std::result::Result<tonic::Response<GetRegionReadProgressResponse>, tonic::Status> {
        let req = request.into_inner();
        let mut resp = GetRegionReadProgressResponse::default();
        {
            let store_meta = self.store_meta.lock().unwrap();
            let rrp = store_meta.region_read_progress();

            rrp.with(|registry| {
                let region = registry.get(&req.get_region_id());
                if let Some(r) = region {
                    resp.set_region_read_progress_exist(true);
                    resp.set_safe_ts(r.safe_ts());
                    let core = r.get_core();
                    resp.set_applied_index(core.applied_index());
                    resp.set_region_read_progress_paused(core.paused());
                    if let Some(back) = core.pending_items().back() {
                        resp.set_pending_back_ts(back.ts);
                        resp.set_pending_back_applied_index(back.idx);
                    }
                    if let Some(front) = core.pending_items().front() {
                        resp.set_pending_front_ts(front.ts);
                        resp.set_pending_front_applied_index(front.idx)
                    }
                    resp.set_read_state_ts(core.read_state().ts);
                    resp.set_read_state_apply_index(core.read_state().idx);
                    resp.set_discard(core.discarding());
                    resp.set_duration_to_last_consume_leader_ms(
                        core.last_instant_of_consume_leader()
                            .map(|t| t.saturating_elapsed().as_millis() as u64)
                            .unwrap_or(u64::MAX),
                    );
                    resp.set_duration_to_last_update_safe_ts_ms(
                        core.last_instant_of_update_ts()
                            .map(|t| t.saturating_elapsed().as_millis() as u64)
                            .unwrap_or(u64::MAX),
                    );
                } else {
                    resp.set_region_read_progress_exist(false);
                }
            });
        }

        // get from resolver
        let (cb, f) = paired_future_callback();
        if (*self.resolved_ts_scheduler)(
            req.get_region_id(),
            req.get_log_locks(),
            req.get_min_start_ts(),
            cb,
        ) {
            let res = f.await;
            match res {
                Err(e) => {
                    resp.set_error("get resolved-ts info failed".to_owned());
                    error!("tikv-ctl get resolved-ts info failed"; "err" => ?e);
                }
                Ok(Some((
                    stopped,
                    resolved_ts,
                    resolver_tracked_index,
                    num_locks,
                    num_transactions,
                ))) => {
                    resp.set_resolver_exist(true);
                    resp.set_resolver_stopped(stopped);
                    resp.set_resolved_ts(resolved_ts);
                    resp.set_resolver_tracked_index(resolver_tracked_index);
                    resp.set_num_locks(num_locks);
                    resp.set_num_transactions(num_transactions);
                }
                Ok(None) => {
                    resp.set_resolver_exist(false);
                }
            }

            Ok(tonic::Response::new(resp))
        } else {
            resp.set_error("resolved-ts is not enabled".to_owned());
            Ok(tonic::Response::new(resp))
        }
    }
}

mod region_size_response {
    pub type Entry = kvproto::debugpb::region_size_response::Entry;
}

mod list_fail_points_response {
    pub type Entry = kvproto::debugpb::list_fail_points_response::Entry;
}
