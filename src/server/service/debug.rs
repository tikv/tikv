// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::util::stats as rocksdb_stats;
use engine::Engines;
use fail;
use futures::sync::oneshot;
use futures::{future, stream, Future, Stream};
use futures_cpupool::{Builder, CpuPool};
use grpcio::{Error as GrpcError, WriteFlags};
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink};
use kvproto::debugpb::{self, *};
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, RaftCmdRequest, RaftRequestHeader, RegionDetailResponse,
    StatusCmdType, StatusRequest,
};

use crate::raftstore::store::msg::Callback;
use crate::server::debug::{Debugger, Error};
use crate::server::transport::RaftStoreRouter;
use tikv_util::metrics;

use tikv_alloc;

fn error_to_status(e: Error) -> RpcStatus {
    let (code, msg) = match e {
        Error::NotFound(msg) => (RpcStatusCode::NOT_FOUND, Some(msg)),
        Error::InvalidArgument(msg) => (RpcStatusCode::INVALID_ARGUMENT, Some(msg)),
        Error::Other(e) => (RpcStatusCode::UNKNOWN, Some(format!("{:?}", e))),
    };
    RpcStatus::new(code, msg)
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
pub struct Service<T: RaftStoreRouter> {
    pool: CpuPool,
    debugger: Debugger,
    raft_router: T,
}

impl<T: RaftStoreRouter> Service<T> {
    /// Constructs a new `Service` with `Engines` and a `RaftStoreRouter`.
    pub fn new(engines: Engines, raft_router: T) -> Service<T> {
        let pool = Builder::new()
            .name_prefix(thd_name!("debugger"))
            .pool_size(1)
            .create();
        let debugger = Debugger::new(engines);
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
        F: Future<Item = P, Error = Error> + Send + 'static,
    {
        let f = resp.then(|v| match v {
            Ok(resp) => sink.success(resp),
            Err(e) => sink.fail(error_to_status(e)),
        });
        ctx.spawn(f.map_err(move |e| on_grpc_error(tag, &e)));
    }
}

impl<T: RaftStoreRouter + 'static> debugpb::Debug for Service<T> {
    fn get(&mut self, ctx: RpcContext<'_>, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        const TAG: &str = "debug_get";

        let db = req.get_db();
        let cf = req.take_cf();
        let key = req.take_key();

        let f = self
            .pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.get(db, &cf, key.as_slice())),
            )
            .map(|value| {
                let mut resp = GetResponse::default();
                resp.set_value(value);
                resp
            });

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

        let f = self
            .pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.raft_log(region_id, log_index)),
            )
            .map(|entry| {
                let mut resp = RaftLogResponse::default();
                resp.set_entry(entry);
                resp
            });

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

        let f = self
            .pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.region_info(region_id)),
            )
            .map(|region_info| {
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
                resp
            });

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

        let f = self
            .pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.region_size(region_id, cfs)),
            )
            .map(|entries| {
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
                resp
            });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn scan_mvcc(
        &mut self,
        _: RpcContext<'_>,
        mut req: ScanMvccRequest,
        sink: ServerStreamingSink<ScanMvccResponse>,
    ) {
        let debugger = self.debugger.clone();
        let from = req.take_from_key();
        let to = req.take_to_key();
        let limit = req.get_limit();
        let future = future::result(debugger.scan_mvcc(&from, &to, limit))
            .map_err(|e| error_to_grpc_error("scan_mvcc", e))
            .and_then(|iter| {
                stream::iter_result(iter)
                    .map_err(|e| error_to_grpc_error("scan_mvcc", e))
                    .map(|(key, mvcc_info)| {
                        let mut resp = ScanMvccResponse::default();
                        resp.set_key(key);
                        resp.set_info(mvcc_info);
                        (resp, WriteFlags::default())
                    })
                    .forward(sink)
                    .map(|_| ())
            })
            .map_err(|e| on_grpc_error("scan_mvcc", &e));
        self.pool.spawn(future).forget();
    }

    fn compact(
        &mut self,
        ctx: RpcContext<'_>,
        req: CompactRequest,
        sink: UnarySink<CompactResponse>,
    ) {
        let debugger = self.debugger.clone();
        let f = self.pool.spawn_fn(move || {
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
        self.handle_response(ctx, sink, f, "debug_compact");
    }

    fn inject_fail_point(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: InjectFailPointRequest,
        sink: UnarySink<InjectFailPointResponse>,
    ) {
        const TAG: &str = "debug_inject_fail_point";

        let f = self.pool.spawn_fn(move || {
            let name = req.take_name();
            if name.is_empty() {
                return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
            }
            let actions = req.get_actions();
            if let Err(e) = fail::cfg(name, actions) {
                return Err(box_err!("{:?}", e));
            }
            Ok(InjectFailPointResponse::default())
        });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn recover_fail_point(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: RecoverFailPointRequest,
        sink: UnarySink<RecoverFailPointResponse>,
    ) {
        const TAG: &str = "debug_recover_fail_point";

        let f = self.pool.spawn_fn(move || {
            let name = req.take_name();
            if name.is_empty() {
                return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
            }
            fail::remove(name);
            Ok(RecoverFailPointResponse::default())
        });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn list_fail_points(
        &mut self,
        ctx: RpcContext<'_>,
        _: ListFailPointsRequest,
        sink: UnarySink<ListFailPointsResponse>,
    ) {
        const TAG: &str = "debug_list_fail_points";

        let f = self.pool.spawn_fn(move || {
            let list = fail::list().into_iter().map(|(name, actions)| {
                let mut entry = list_fail_points_response::Entry::default();
                entry.set_name(name);
                entry.set_actions(actions);
                entry
            });
            let mut resp = ListFailPointsResponse::default();
            resp.set_entries(list.collect());
            Ok(resp)
        });

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
        let f = self.pool.spawn_fn(move || {
            let mut resp = GetMetricsResponse::default();
            resp.set_store_id(debugger.get_store_id()?);
            resp.set_prometheus(metrics::dump());
            if req.get_all() {
                let engines = debugger.get_engine();
                resp.set_rocksdb_kv(box_try!(rocksdb_stats::dump(&engines.kv)));
                resp.set_rocksdb_raft(box_try!(rocksdb_stats::dump(&engines.raft)));
                resp.set_jemalloc(tikv_alloc::dump_stats());
            }
            Ok(resp)
        });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn check_region_consistency(
        &mut self,
        ctx: RpcContext<'_>,
        req: RegionConsistencyCheckRequest,
        sink: UnarySink<RegionConsistencyCheckResponse>,
    ) {
        let region_id = req.get_region_id();
        let debugger = self.debugger.clone();
        let router1 = self.raft_router.clone();
        let router2 = self.raft_router.clone();

        let consistency_check = future::result(debugger.get_store_id())
            .and_then(move |store_id| region_detail(router2, region_id, store_id))
            .and_then(|detail| consistency_check(router1, detail));
        let f = self
            .pool
            .spawn(consistency_check)
            .map(|_| RegionConsistencyCheckResponse::default());
        self.handle_response(ctx, sink, f, "check_region_consistency");
    }

    fn modify_tikv_config(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: ModifyTikvConfigRequest,
        sink: UnarySink<ModifyTikvConfigResponse>,
    ) {
        const TAG: &str = "modify_tikv_config";

        let module = req.get_module();
        let config_name = req.take_config_name();
        let config_value = req.take_config_value();

        let f = self
            .pool
            .spawn(future::ok(self.debugger.clone()).and_then(move |debugger| {
                debugger.modify_tikv_config(module, &config_name, &config_value)
            }))
            .map(|_| ModifyTikvConfigResponse::default());

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
            .spawn_fn(move || debugger.get_region_properties(req.get_region_id()))
            .map(|props| {
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

        let f = self.pool.spawn_fn(move || {
            let mut resp = GetStoreInfoResponse::default();
            match debugger.get_store_id() {
                Ok(store_id) => resp.set_store_id(store_id),
                Err(_) => resp.set_store_id(0),
            }
            Ok(resp)
        });

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

        let f = self.pool.spawn_fn(move || {
            let mut resp = GetClusterInfoResponse::default();
            match debugger.get_cluster_id() {
                Ok(cluster_id) => resp.set_cluster_id(cluster_id),
                Err(_) => resp.set_cluster_id(0),
            }
            Ok(resp)
        });

        self.handle_response(ctx, sink, f, TAG);
    }
}

fn region_detail<T: RaftStoreRouter>(
    raft_router: T,
    region_id: u64,
    store_id: u64,
) -> impl Future<Item = RegionDetailResponse, Error = Error> {
    let mut header = RaftRequestHeader::default();
    header.set_region_id(region_id);
    header.mut_peer().set_store_id(store_id);
    let mut status_request = StatusRequest::default();
    status_request.set_cmd_type(StatusCmdType::RegionDetail);
    let mut raft_cmd = RaftCmdRequest::default();
    raft_cmd.set_header(header);
    raft_cmd.set_status_request(status_request);

    let (tx, rx) = oneshot::channel();
    let cb = Callback::Read(Box::new(|resp| tx.send(resp).unwrap()));
    future::result(raft_router.send_command(raft_cmd, cb))
        .map_err(|e| Error::Other(Box::new(e)))
        .and_then(move |_| {
            rx.map_err(|e| Error::Other(Box::new(e)))
                .and_then(move |mut r| {
                    if r.response.get_header().has_error() {
                        let e = r.response.get_header().get_error();
                        warn!("region_detail got error"; "err" => ?e);
                        return Err(Error::Other(e.message.clone().into()));
                    }
                    let detail = r.response.take_status_response().take_region_detail();
                    debug!("region_detail got region detail"; "detail" => ?detail);
                    let leader_store_id = detail.get_leader().get_store_id();
                    if leader_store_id != store_id {
                        let msg = format!("Leader is on store {}", leader_store_id);
                        return Err(Error::Other(msg.into()));
                    }
                    Ok(detail)
                })
        })
}

fn consistency_check<T: RaftStoreRouter>(
    raft_router: T,
    mut detail: RegionDetailResponse,
) -> impl Future<Item = (), Error = Error> {
    let mut header = RaftRequestHeader::default();
    header.set_region_id(detail.get_region().get_id());
    header.set_peer(detail.take_leader());
    let mut admin_request = AdminRequest::default();
    admin_request.set_cmd_type(AdminCmdType::ComputeHash);
    let mut raft_cmd = RaftCmdRequest::default();
    raft_cmd.set_header(header);
    raft_cmd.set_admin_request(admin_request);

    let (tx, rx) = oneshot::channel();
    let cb = Callback::Read(Box::new(|resp| tx.send(resp).unwrap()));
    future::result(raft_router.send_command(raft_cmd, cb))
        .map_err(|e| Error::Other(Box::new(e)))
        .and_then(move |_| {
            rx.map_err(|e| Error::Other(Box::new(e)))
                .and_then(move |r| {
                    if r.response.get_header().has_error() {
                        let e = r.response.get_header().get_error();
                        warn!("consistency-check got error"; "err" => ?e);
                        return Err(Error::Other(e.message.clone().into()));
                    }
                    Ok(())
                })
        })
}

mod region_size_response {
    pub type Entry = kvproto::debugpb::RegionSizeResponseEntry;
}

mod list_fail_points_response {
    pub type Entry = kvproto::debugpb::ListFailPointsResponseEntry;
}
