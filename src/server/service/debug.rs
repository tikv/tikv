// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::{
    future::{Future, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::{self, TryStreamExt},
};
use grpcio::{
    Error as GrpcError, RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink,
    WriteFlags,
};
use kvproto::{
    debugpb::{self, *},
    kvrpcpb::KeyRange,
};
use pd_client::PdClient;
use raftstore::{store::util::build_key_range, RegionInfoAccessor};
use tikv_kv::RaftExtension;
use tikv_util::metrics;
use tokio::{
    runtime::Handle,
    sync::{oneshot, Mutex},
};

use crate::{
    server::debug::{BoxFuture, Debugger, Error, Result, FLASHBACK_TIMEOUT},
    storage::mvcc::TimeStamp,
};

fn error_to_status(e: Error) -> RpcStatus {
    let (code, msg) = match e {
        Error::NotFound(msg) => (RpcStatusCode::NOT_FOUND, msg),
        Error::InvalidArgument(msg) => (RpcStatusCode::INVALID_ARGUMENT, msg),
        Error::Other(e) => (RpcStatusCode::UNKNOWN, format!("{:?}", e)),
        Error::FlashbackFailed(msg) => (RpcStatusCode::UNKNOWN, msg),
        Error::NotPreparedFlashback(msg) => (RpcStatusCode::UNKNOWN, msg),
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
pub struct Service<T, D>
where
    T: RaftExtension,
    D: Debugger,
{
    pool: Handle,
    debugger: D,
    raft_router: T,
    pd_client: Arc<dyn PdClient>,
    region_info_accessor: RegionInfoAccessor,
}

impl<T, D> Service<T, D>
where
    T: RaftExtension,
    D: Debugger,
{
    /// Constructs a new `Service` with `Engines`, a `RaftExtension`, a
    /// `GcWorker` and a `RegionInfoAccessor`.
    pub fn new(
        debugger: D,
        pool: Handle,
        raft_router: T,
        pd_client: Arc<dyn PdClient>,
        region_info_accessor: RegionInfoAccessor,
    ) -> Self {
        Service {
            pool,
            debugger,
            raft_router,
            pd_client,
            region_info_accessor,
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

impl<T, D> debugpb::Debug for Service<T, D>
where
    T: RaftExtension + 'static,
    D: Debugger + Clone + Send + 'static,
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
                        let mut prop = GetRangePropertiesResponseRangeProperty::default();
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
        let tmp = self.region_info_accessor.region_leaders();
        let region_leaders = tmp.read().unwrap();
        let key_range = build_key_range(req.get_start_key(), req.get_end_key(), false);

        let region_ids = req.get_region_ids();
        let filtered_region_ids = region_leaders
            .iter()
            .filter_map(|region_id| {
                if region_ids.is_empty() || region_ids.contains(region_id) {
                    let debugger = self.debugger.clone();
                    let r = debugger.region_info(*region_id).unwrap();
                    let region = r
                        .region_local_state
                        .as_ref()
                        .map(|s| s.get_region().clone())
                        .unwrap();

                    if check_intersect_of_range(
                        &build_key_range(region.get_start_key(), region.get_end_key(), false),
                        &key_range,
                    ) {
                        Some(*region_id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let version = req.get_version();
        let debugger = Arc::new(Mutex::new(self.debugger.clone()));
        let filtered_region_ids_clone = filtered_region_ids.clone();
        let pd_client = self.pd_client.clone();
        let runtime = self.pool.clone();
        let f = self
            .pool
            .spawn(async move {
                let start_ts = pd_client
                    .get_tso()
                    .await
                    .expect("failed to get commit_ts from PD");
                // Prepare flashback for specified regions.
                let prepare_wg = WaitGroup::new();
                filtered_region_ids_clone.iter().for_each(|&region_id| {
                    let debugger = debugger.clone();
                    let wg = prepare_wg.clone();
                    runtime.spawn(async move {
                        let work = wg.work();
                        let debugger = debugger.lock().await;
                        let check = debugger.region_flashback_to_version(
                            region_id,
                            version,
                            start_ts,
                            TimeStamp::zero(),
                        );
                        match check.await {
                            Ok(_) => {}
                            Err(err) => {
                                return Err(Error::NotPreparedFlashback(format!(
                                    "prepare flashback failed err is {}",
                                    err
                                )));
                            }
                        }
                        drop(work);
                        Ok(())
                    });
                });
                // Wait for finishing prepare flashback.
                tokio::time::timeout(Duration::from_secs(FLASHBACK_TIMEOUT), prepare_wg.wait())
                    .map_err(|e| {
                        Error::NotPreparedFlashback(format!(
                            "prepare flashback timeout err is {}",
                            e
                        ))
                    })
                    .await?;

                let commit_ts = pd_client
                    .get_tso()
                    .await
                    .expect("failed to get commit_ts from PD");
                // Flashback to version.
                let debugger = debugger.lock().await;
                let flashback_wg = WaitGroup::new();
                filtered_region_ids.iter().for_each(|&region_id| {
                    let debugger = debugger.clone();
                    let work = flashback_wg.clone().work();
                    runtime.spawn(async move {
                        let check = debugger
                            .region_flashback_to_version(region_id, version, start_ts, commit_ts);
                        match check.await {
                            Ok(_) => {}
                            Err(err) => {
                                return Err(Error::FlashbackFailed(format!(
                                    "flashback failed err is {}",
                                    err
                                )));
                            }
                        }
                        drop(work);
                        Ok(())
                    });
                });

                // Wait for finish.
                tokio::time::timeout(Duration::from_secs(FLASHBACK_TIMEOUT), flashback_wg.wait())
                    .map_err(|e| Error::FlashbackFailed(format!("flashback timeout err is {}", e)))
                    .await?;
                Ok(FlashbackToVersionResponse::default())
            })
            .map(|res| res.unwrap());

        self.handle_response(ctx, sink, f, "debug_flashback_to_version");
    }
}

// Check if region's `key_range` intersects with `key_range_limit`.
pub fn check_intersect_of_range(key_range: &KeyRange, key_range_limit: &KeyRange) -> bool {
    if !key_range.get_end_key().is_empty()
        && !key_range_limit.get_start_key().is_empty()
        && key_range.get_end_key() <= key_range_limit.get_start_key()
    {
        return false;
    }
    if !key_range_limit.get_end_key().is_empty()
        && !key_range.get_start_key().is_empty()
        && key_range_limit.get_end_key() < key_range.get_start_key()
    {
        return false;
    }
    true
}
mod region_size_response {
    pub type Entry = kvproto::debugpb::RegionSizeResponseEntry;
}

mod list_fail_points_response {
    pub type Entry = kvproto::debugpb::ListFailPointsResponseEntry;
}

pub struct WaitGroup {
    running: AtomicUsize,
    on_finish_all: std::sync::Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
}

impl WaitGroup {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            running: AtomicUsize::new(0),
            on_finish_all: std::sync::Mutex::default(),
        })
    }

    fn work_done(&self) {
        let last = self.running.fetch_sub(1, Ordering::SeqCst);
        if last == 1 {
            self.on_finish_all
                .lock()
                .unwrap()
                .drain(..)
                .for_each(|x| x())
        }
    }

    /// wait until all running tasks done.
    pub fn wait(&self) -> BoxFuture<()> {
        // Fast path: no uploading.
        if self.running.load(Ordering::SeqCst) == 0 {
            return Box::pin(futures::future::ready(()));
        }

        let (tx, rx) = oneshot::channel();
        self.on_finish_all.lock().unwrap().push(Box::new(move || {
            // The waiter may timed out.
            let _ = tx.send(());
        }));
        // try to acquire the lock again.
        if self.running.load(Ordering::SeqCst) == 0 {
            return Box::pin(futures::future::ready(()));
        }
        Box::pin(rx.map(|_| ()))
    }

    /// make a work, as long as the return value held, mark a work in the group
    /// is running.
    pub fn work(self: Arc<Self>) -> Work {
        self.running.fetch_add(1, Ordering::SeqCst);
        Work(self)
    }
}

pub struct Work(Arc<WaitGroup>);

impl Drop for Work {
    fn drop(&mut self) {
        self.0.work_done();
    }
}

impl std::fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let running = self.running.load(Ordering::Relaxed);
        f.debug_struct("WaitGroup")
            .field("running", &running)
            .finish()
    }
}
