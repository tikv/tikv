// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use engine_traits::{Engines, MiscExt, RaftEngine};
use futures::{
    future::{Future, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::{self, TryStreamExt},
};
use grpcio::{
    Error as GrpcError, RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink,
    WriteFlags,
};
use kvproto::debugpb::{self, *};
use tikv_kv::RaftExtension;
use tikv_util::metrics;
use tokio::runtime::Handle;

use crate::{
    config::ConfigController,
    server::debug::{Debugger, Error, Result},
};

fn error_to_status(e: Error) -> RpcStatus {
    let (code, msg) = match e {
        Error::NotFound(msg) => (RpcStatusCode::NOT_FOUND, msg),
        Error::InvalidArgument(msg) => (RpcStatusCode::INVALID_ARGUMENT, msg),
        Error::Other(e) => (RpcStatusCode::UNKNOWN, format!("{:?}", e)),
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

/// Service handles the RPC messages for the `Debug` service.
#[derive(Clone)]
pub struct Service<ER: RaftEngine, T: RaftExtension> {
    pool: Handle,
    debugger: Debugger<ER>,
    raft_router: T,
}

impl<ER: RaftEngine, T: RaftExtension> Service<ER, T> {
    /// Constructs a new `Service` with `Engines`, a `RaftExtension` and a
    /// `GcWorker`.
    pub fn new(
        engines: Engines<RocksEngine, ER>,
        pool: Handle,
        raft_router: T,
        cfg_controller: ConfigController,
    ) -> Self {
        let debugger = Debugger::new(engines, cfg_controller);
        Service {
            pool,
            debugger,
            raft_router,
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

impl<ER: RaftEngine, T: RaftExtension + 'static> debugpb::Debug for Service<ER, T> {
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
            let req = req;
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
                    let engines = debugger.get_engine();
                    resp.set_rocksdb_kv(box_try!(MiscExt::dump_stats(&engines.kv)));
                    resp.set_rocksdb_raft(box_try!(RaftEngine::dump_stats(&engines.raft)));
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
}

mod region_size_response {
    pub type Entry = kvproto::debugpb::RegionSizeResponseEntry;
}

mod list_fail_points_response {
    pub type Entry = kvproto::debugpb::ListFailPointsResponseEntry;
}
