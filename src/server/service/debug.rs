// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use futures::{
    future::{Future, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::{self, TryStreamExt},
};
use grpcio::{
    Error as GrpcError, RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink,
    WriteFlags,
};
use kvproto::debugpb::{self, get_range_properties_response::RangeProperty, *};
use raftstore::store::fsm::store::StoreRegionMeta;
use tikv_kv::RaftExtension;
use tikv_util::{future::paired_future_callback, metrics};
use tokio::runtime::Handle;

use crate::server::debug::{Debugger, Error, Result};

fn error_to_status(e: Error) -> RpcStatus {
    let (code, msg) = match e {
        Error::NotFound(msg) => (RpcStatusCode::NOT_FOUND, msg),
        Error::InvalidArgument(msg) => (RpcStatusCode::INVALID_ARGUMENT, msg),
        Error::Other(e) => (RpcStatusCode::UNKNOWN, format!("{:?}", e)),
        Error::EngineTrait(e) => (RpcStatusCode::UNKNOWN, format!("{:?}", e)),
        Error::FlashbackFailed(msg) => (RpcStatusCode::UNKNOWN, msg),
    };
    RpcStatus::with_message(code, msg)
}

fn on_grpc_error(tag: &'static str, e: &GrpcError) {
    error!("{} failed: {:?}", tag, e);
}

fn error_to_grpc_error(tag: &'static str, e: Error) -> GrpcError {
    let status = error_to_status(e);
    let e = GrpcError::RpcFailure(status);
    on_grpc_error(tag, &e);
    e
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

    fn handle_response<F, P>(
        &self,
        ctx: RpcContext<'_>,
        sink: UnarySink<P>,
        resp: F,
        tag: &'static str,
    ) where
        P: Send + 'static,
        F: Future<Output = Result<P>> + Send + 'static,
    {
        let ctx_task = async move {
            match resp.await {
                Ok(resp) => sink.success(resp).await?,
                Err(e) => sink.fail(error_to_status(e)).await?,
            }
            Ok(())
        };
        ctx.spawn(ctx_task.unwrap_or_else(move |e| on_grpc_error(tag, &e)));
    }
}

impl<T, D, S> debugpb::Debug for Service<T, D, S>
where
    T: RaftExtension + 'static,
    D: Debugger + Clone + Send + 'static,
    S: StoreRegionMeta,
{
    fn get(&mut self, ctx: RpcContext<'_>, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        const TAG: &str = "debug_get";

        let db = req.get_db();
        let cf = req.take_cf();
        let key = req.take_key();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.get(db, &cf, key.as_slice()) });
        let f = async move {
            let value = join.await.unwrap()?;
            let mut resp = GetResponse::default();
            resp.set_value(value);
            Ok(resp)
        };

        self.handle_response(ctx, sink, f, TAG);
    }

    fn raft_log(
        &mut self,
        ctx: RpcContext<'_>,
        req: RaftLogRequest,
        sink: UnarySink<RaftLogResponse>,
    ) {
        const TAG: &str = "debug_raft_log";

        let region_id = req.get_region_id();
        let log_index = req.get_log_index();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.raft_log(region_id, log_index) });
        let f = async move {
            let entry = join.await.unwrap()?;
            let mut resp = RaftLogResponse::default();
            resp.set_entry(entry);
            Ok(resp)
        };

        self.handle_response(ctx, sink, f, TAG);
    }

    fn region_info(
        &mut self,
        ctx: RpcContext<'_>,
        req: RegionInfoRequest,
        sink: UnarySink<RegionInfoResponse>,
    ) {
        const TAG: &str = "debug_region_log";

        let region_id = req.get_region_id();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.region_info(region_id) });
        let f = async move {
            let region_info = join.await.unwrap()?;
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
            Ok(resp)
        };

        self.handle_response(ctx, sink, f, TAG);
    }

    fn region_size(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: RegionSizeRequest,
        sink: UnarySink<RegionSizeResponse>,
    ) {
        const TAG: &str = "debug_region_size";

        let region_id = req.get_region_id();
        let cfs = req.take_cfs().into();
        let debugger = self.debugger.clone();

        let join = self
            .pool
            .spawn(async move { debugger.region_size(region_id, cfs) });
        let f = async move {
            let entries = join.await.unwrap()?;
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
            Ok(resp)
        };

        self.handle_response(ctx, sink, f, TAG);
    }

    fn scan_mvcc(
        &mut self,
        _: RpcContext<'_>,
        mut req: ScanMvccRequest,
        mut sink: ServerStreamingSink<ScanMvccResponse>,
    ) {
        let debugger = self.debugger.clone();
        let from = req.take_from_key();
        let to = req.take_to_key();
        let limit = req.get_limit();

        let future = async move {
            let iter = debugger.scan_mvcc(&from, &to, limit);
            if iter.is_err() {
                return;
            }
            let mut s = stream::iter(iter.unwrap())
                .map_err(|e| box_err!(e))
                .map_err(|e| error_to_grpc_error("scan_mvcc", e))
                .map_ok(|(key, mvcc_info)| {
                    let mut resp = ScanMvccResponse::default();
                    resp.set_key(key);
                    resp.set_info(mvcc_info);
                    (resp, WriteFlags::default())
                });
            if let Err(e) = sink.send_all(&mut s).await {
                on_grpc_error("scan_mvcc", &e);
                return;
            }
            let _ = sink.close().await;
        };
        self.pool.spawn(future);
    }

    fn compact(
        &mut self,
        ctx: RpcContext<'_>,
        req: CompactRequest,
        sink: UnarySink<CompactResponse>,
    ) {
        let debugger = self.debugger.clone();

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

        let f = async move { res.await.unwrap() };

        self.handle_response(ctx, sink, f, "debug_compact");
    }

    fn inject_fail_point(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: InjectFailPointRequest,
        sink: UnarySink<InjectFailPointResponse>,
    ) {
        const TAG: &str = "debug_inject_fail_point";

        let f = self
            .pool
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
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn recover_fail_point(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: RecoverFailPointRequest,
        sink: UnarySink<RecoverFailPointResponse>,
    ) {
        const TAG: &str = "debug_recover_fail_point";

        let f = self
            .pool
            .spawn(async move {
                let name = req.take_name();
                if name.is_empty() {
                    return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
                }
                fail::remove(name);
                Ok(RecoverFailPointResponse::default())
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn list_fail_points(
        &mut self,
        ctx: RpcContext<'_>,
        _: ListFailPointsRequest,
        sink: UnarySink<ListFailPointsResponse>,
    ) {
        const TAG: &str = "debug_list_fail_points";

        let f = self
            .pool
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
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_metrics(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetMetricsRequest,
        sink: UnarySink<GetMetricsResponse>,
    ) {
        const TAG: &str = "debug_get_metrics";

        let debugger = self.debugger.clone();
        let f = self
            .pool
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
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn check_region_consistency(
        &mut self,
        ctx: RpcContext<'_>,
        req: RegionConsistencyCheckRequest,
        sink: UnarySink<RegionConsistencyCheckResponse>,
    ) {
        let region_id = req.get_region_id();
        let f = self.raft_router.check_consistency(region_id);
        let task = async move {
            box_try!(f.await);
            Ok(())
        };
        let f = self
            .pool
            .spawn(task)
            .map(|res| res.unwrap())
            .map_ok(|_| RegionConsistencyCheckResponse::default());
        self.handle_response(ctx, sink, f, "check_region_consistency");
    }

    fn modify_tikv_config(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: ModifyTikvConfigRequest,
        sink: UnarySink<ModifyTikvConfigResponse>,
    ) {
        const TAG: &str = "modify_tikv_config";

        let config_name = req.take_config_name();
        let config_value = req.take_config_value();
        let debugger = self.debugger.clone();

        let f = self
            .pool
            .spawn(async move { debugger.modify_tikv_config(&config_name, &config_value) })
            .map(|res| res.unwrap())
            .map_ok(|_| ModifyTikvConfigResponse::default());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_region_properties(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetRegionPropertiesRequest,
        sink: UnarySink<GetRegionPropertiesResponse>,
    ) {
        const TAG: &str = "get_region_properties";
        let debugger = self.debugger.clone();

        let f = self
            .pool
            .spawn(async move { debugger.get_region_properties(req.get_region_id()) })
            .map(|res| res.unwrap())
            .map_ok(|props| {
                let mut resp = GetRegionPropertiesResponse::default();
                for (name, value) in props {
                    let mut prop = Property::default();
                    prop.set_name(name);
                    prop.set_value(value);
                    resp.mut_props().push(prop);
                }
                resp
            });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_range_properties(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetRangePropertiesRequest,
        sink: UnarySink<GetRangePropertiesResponse>,
    ) {
        const TAG: &str = "get_range_properties";
        let debugger = self.debugger.clone();

        let f =
            self.pool
                .spawn(async move {
                    debugger.get_range_properties(req.get_start_key(), req.get_end_key())
                })
                .map(|res| res.unwrap())
                .map_ok(|props| {
                    let mut resp = GetRangePropertiesResponse::default();
                    for (key, value) in props {
                        let mut prop = RangeProperty::default();
                        prop.set_key(key);
                        prop.set_value(value);
                        resp.mut_properties().push(prop)
                    }
                    resp
                });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_store_info(
        &mut self,
        ctx: RpcContext<'_>,
        _: GetStoreInfoRequest,
        sink: UnarySink<GetStoreInfoResponse>,
    ) {
        const TAG: &str = "debug_get_store_id";
        let debugger = self.debugger.clone();

        let f = self
            .pool
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
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_cluster_info(
        &mut self,
        ctx: RpcContext<'_>,
        _: GetClusterInfoRequest,
        sink: UnarySink<GetClusterInfoResponse>,
    ) {
        const TAG: &str = "debug_get_cluster_id";
        let debugger = self.debugger.clone();

        let f = self
            .pool
            .spawn(async move {
                let mut resp = GetClusterInfoResponse::default();
                match debugger.get_store_ident() {
                    Ok(ident) => resp.set_cluster_id(ident.get_cluster_id()),
                    Err(_) => resp.set_cluster_id(0),
                }
                Ok(resp)
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn get_all_regions_in_store(
        &mut self,
        ctx: RpcContext<'_>,
        _: GetAllRegionsInStoreRequest,
        sink: UnarySink<GetAllRegionsInStoreResponse>,
    ) {
        const TAG: &str = "debug_get_all_regions_in_store";
        let debugger = self.debugger.clone();

        let f = self
            .pool
            .spawn(async move {
                let mut resp = GetAllRegionsInStoreResponse::default();
                match debugger.get_all_regions_in_store() {
                    Ok(regions) => resp.set_regions(regions),
                    Err(_) => resp.set_regions(vec![]),
                }
                Ok(resp)
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, TAG);
    }

    fn reset_to_version(
        &mut self,
        _ctx: RpcContext<'_>,
        req: ResetToVersionRequest,
        sink: UnarySink<ResetToVersionResponse>,
    ) {
        self.debugger.reset_to_version(req.get_ts());
        sink.success(ResetToVersionResponse::default());
    }

    fn flashback_to_version(
        &mut self,
        ctx: RpcContext<'_>,
        req: FlashbackToVersionRequest,
        sink: UnarySink<FlashbackToVersionResponse>,
    ) {
        let debugger = self.debugger.clone();
        let f = self
            .pool
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
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, "debug_flashback_to_version");
    }

    fn get_region_read_progress(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetRegionReadProgressRequest,
        sink: UnarySink<GetRegionReadProgressResponse>,
    ) {
        let store_meta = self.store_meta.lock().unwrap();
        let rrp = store_meta.region_read_progress();
        let mut resp = GetRegionReadProgressResponse::default();
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

        // get from resolver
        let (cb, f) = paired_future_callback();
        if (*self.resolved_ts_scheduler)(
            req.get_region_id(),
            req.get_log_locks(),
            req.get_min_start_ts(),
            cb,
        ) {
            let f = async move {
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

                Ok(resp)
            };
            self.handle_response(ctx, sink, f, "debug_get_region_read_progress");
        } else {
            resp.set_error("resolved-ts is not enabled".to_owned());
            let f = async move { Ok(resp) };
            self.handle_response(ctx, sink, f, "debug_get_region_read_progress");
        }
    }
}

mod region_size_response {
    pub type Entry = kvproto::debugpb::region_size_response::Entry;
}

mod list_fail_points_response {
    pub type Entry = kvproto::debugpb::list_fail_points_response::Entry;
}
