// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use fail::fail_point;
use futures::{future, SinkExt, TryFutureExt, TryStreamExt};
use grpcio::{
    ClientStreamingSink, DuplexSink, EnvBuilder, RequestStream, RpcContext, RpcStatus,
    RpcStatusCode, Server as GrpcServer, ServerBuilder, UnarySink, WriteFlags,
};
use kvproto::{
    meta_storagepb_grpc::{create_meta_storage, MetaStorage},
    pdpb::*,
    resource_manager,
    resource_manager_grpc::create_resource_manager,
};
use pd_client::Error as PdError;
use security::*;

use super::mocker::*;
use crate::mocker::etcd::EtcdClient;

pub struct Server<C: PdMocker> {
    server: Option<GrpcServer>,
    mocker: PdMock<C>,
}

impl Server<Service> {
    pub fn new(eps_count: usize) -> Server<Service> {
        let mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
        let eps = vec![("127.0.0.1".to_owned(), 0); eps_count];
        let case = Option::None::<Arc<Service>>;
        Self::with_configuration(&mgr, eps, case)
    }

    pub fn default_handler(&self) -> &Service {
        &self.mocker.default_handler
    }
}

impl<C: PdMocker + Send + Sync + 'static> Server<C> {
    pub fn with_case(eps_count: usize, case: Arc<C>) -> Server<C> {
        let mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
        let eps = vec![("127.0.0.1".to_owned(), 0); eps_count];
        Server::with_configuration(&mgr, eps, Some(case))
    }

    pub fn with_configuration(
        mgr: &SecurityManager,
        eps: Vec<(String, u16)>,
        case: Option<Arc<C>>,
    ) -> Server<C> {
        let handler = Arc::new(Service::new());
        let default_handler = Arc::clone(&handler);
        let mocker = PdMock {
            default_handler,
            case,
            tso_logical: Arc::new(AtomicI64::default()),
            etcd_client: EtcdClient::default(),
        };
        let mut server = Server {
            server: None,
            mocker,
        };
        server.start(mgr, eps);
        server
    }

    pub fn start(&mut self, mgr: &SecurityManager, eps: Vec<(String, u16)>) {
        let pd = create_pd(self.mocker.clone());
        let meta_store = create_meta_storage(self.mocker.clone());
        let resource_manager = create_resource_manager(self.mocker.clone());
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)
                .name_prefix(thd_name!("mock-server"))
                .build(),
        );
        let mut sb = ServerBuilder::new(env)
            .register_service(pd)
            .register_service(meta_store)
            .register_service(resource_manager);
        for (host, port) in eps {
            sb = mgr.bind(sb, &host, port);
        }

        let mut server = sb.build().unwrap();
        {
            let addrs: Vec<String> = server
                .bind_addrs()
                .map(|(host, port)| format!("{}:{}", host, port))
                .collect();
            self.mocker.default_handler.set_endpoints(addrs.clone());
            if let Some(case) = self.mocker.case.as_ref() {
                case.set_endpoints(addrs);
            }
        }

        server.start();
        self.server = Some(server);
        // Ensure that server is ready.
        thread::sleep(Duration::from_secs(1));
    }

    pub fn stop(&mut self) {
        self.server
            .take()
            .expect("Server is not started")
            .shutdown();
    }

    pub fn bind_addrs(&self) -> Vec<(String, u16)> {
        self.server
            .as_ref()
            .unwrap()
            .bind_addrs()
            .map(|(host, port)| (host.clone(), port))
            .collect()
    }
}

#[allow(unused_mut)]
fn hijack_unary<F, R, C: PdMocker>(
    mock: &mut PdMock<C>,
    ctx: RpcContext<'_>,
    sink: UnarySink<R>,
    f: F,
) where
    R: Send + 'static,
    F: Fn(&dyn PdMocker) -> Option<Result<R>>,
{
    let resp = mock
        .case
        .as_ref()
        .and_then(|case| f(case.as_ref()))
        .or_else(|| f(mock.default_handler.as_ref()));
    match resp {
        Some(Ok(resp)) => ctx.spawn(
            sink.success(resp)
                .unwrap_or_else(|e| error!("failed to reply: {:?}", e)),
        ),
        Some(Err(err)) => {
            let status = RpcStatus::with_message(RpcStatusCode::UNKNOWN, format!("{:?}", err));
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("failed to reply: {:?}", e)),
            );
        }
        None => {
            let mut status =
                RpcStatus::with_message(RpcStatusCode::UNIMPLEMENTED, "Unimplemented".to_owned());
            #[allow(clippy::redundant_closure_call)]
            (|| {
                fail_point!("connect_leader", |_| {
                    let details = ctx
                        .request_headers()
                        .iter()
                        .find(|(k, _)| *k == "pd-forwarded-host")
                        .map(|(_, v)| std::str::from_utf8(v).unwrap())
                        .unwrap_or("");
                    status =
                        RpcStatus::with_message(RpcStatusCode::UNAVAILABLE, details.to_string());
                })
            })();
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("failed to reply: {:?}", e)),
            );
        }
    }
}

#[derive(Debug)]
struct PdMock<C: PdMocker> {
    default_handler: Arc<Service>,
    case: Option<Arc<C>>,
    tso_logical: Arc<AtomicI64>,
    etcd_client: EtcdClient,
}

impl<C: PdMocker> Clone for PdMock<C> {
    fn clone(&self) -> Self {
        PdMock {
            default_handler: Arc::clone(&self.default_handler),
            case: self.case.clone(),
            tso_logical: self.tso_logical.clone(),
            etcd_client: self.etcd_client.clone(),
        }
    }
}

impl<C: PdMocker + Send + Sync + 'static> MetaStorage for PdMock<C> {
    fn watch(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        req: kvproto::meta_storagepb::WatchRequest,
        sink: grpcio::ServerStreamingSink<kvproto::meta_storagepb::WatchResponse>,
    ) {
        match &self.case {
            Some(x) => {
                x.meta_store_watch(req, sink, &ctx);
            }
            None => grpcio::unimplemented_call!(ctx, sink),
        }
    }

    fn get(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        req: kvproto::meta_storagepb::GetRequest,
        sink: grpcio::UnarySink<kvproto::meta_storagepb::GetResponse>,
    ) {
        hijack_unary(self, ctx, sink, |m| m.meta_store_get(req.clone()))
    }

    fn put(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        req: kvproto::meta_storagepb::PutRequest,
        sink: grpcio::UnarySink<kvproto::meta_storagepb::PutResponse>,
    ) {
        hijack_unary(self, ctx, sink, |m| m.meta_store_put(req.clone()))
    }

    fn delete(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        req: kvproto::meta_storagepb::DeleteRequest,
        sink: grpcio::UnarySink<kvproto::meta_storagepb::DeleteResponse>,
    ) {
        hijack_unary(self, ctx, sink, |m| m.meta_store_delete(req.clone()))
    }
}

impl<C: PdMocker + Send + Sync + 'static> Pd for PdMock<C> {
    fn get_members(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetMembersRequest,
        sink: UnarySink<GetMembersResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_members(&req))
    }

    fn tso(
        &mut self,
        ctx: RpcContext<'_>,
        req: RequestStream<TsoRequest>,
        mut resp: DuplexSink<TsoResponse>,
    ) {
        let header = Service::header();
        let tso_logical = self.tso_logical.clone();
        let fut = async move {
            // Tolerate errors like RpcFinished(None).
            let _ = resp
                .send_all(&mut req.map_ok(move |r| {
                    let logical =
                        tso_logical.fetch_add(r.count as i64, Ordering::SeqCst) + r.count as i64;
                    let mut res = TsoResponse::default();
                    res.set_header(header.clone());
                    res.mut_timestamp().physical = 42;
                    res.mut_timestamp().logical = logical;
                    res.count = r.count;
                    (res, WriteFlags::default())
                }))
                .await;
            let _ = resp.close().await;
        };
        ctx.spawn(fut);
    }

    fn bootstrap(
        &mut self,
        ctx: RpcContext<'_>,
        req: BootstrapRequest,
        sink: UnarySink<BootstrapResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.bootstrap(&req))
    }

    fn is_bootstrapped(
        &mut self,
        ctx: RpcContext<'_>,
        req: IsBootstrappedRequest,
        sink: UnarySink<IsBootstrappedResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.is_bootstrapped(&req))
    }

    fn alloc_id(
        &mut self,
        ctx: RpcContext<'_>,
        req: AllocIdRequest,
        sink: UnarySink<AllocIdResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.alloc_id(&req))
    }

    fn get_store(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetStoreRequest,
        sink: UnarySink<GetStoreResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_store(&req))
    }

    fn put_store(
        &mut self,
        ctx: RpcContext<'_>,
        req: PutStoreRequest,
        sink: UnarySink<PutStoreResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.put_store(&req))
    }

    fn get_all_stores(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetAllStoresRequest,
        sink: UnarySink<GetAllStoresResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_all_stores(&req))
    }

    fn store_heartbeat(
        &mut self,
        ctx: RpcContext<'_>,
        req: StoreHeartbeatRequest,
        sink: UnarySink<StoreHeartbeatResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.store_heartbeat(&req))
    }

    fn report_buckets(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        stream: RequestStream<ReportBucketsRequest>,
        sink: ClientStreamingSink<ReportBucketsResponse>,
    ) {
        let mock = self.clone();
        ctx.spawn(async move {
            let mut stream = stream.map_err(PdError::from);
            while let Ok(Some(req)) = stream.try_next().await {
                let resp = mock
                    .case
                    .as_ref()
                    .and_then(|case| case.report_buckets(&req))
                    .or_else(|| mock.default_handler.report_buckets(&req));
                if let Some(Ok(resp)) = resp {
                    sink.success(resp);
                    break;
                }
            }
        });
    }

    fn region_heartbeat(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<RegionHeartbeatRequest>,
        sink: DuplexSink<RegionHeartbeatResponse>,
    ) {
        let mock = self.clone();
        ctx.spawn(async move {
            let mut stream = stream.map_err(PdError::from).try_filter_map(move |req| {
                let resp = mock
                    .case
                    .as_ref()
                    .and_then(|case| case.region_heartbeat(&req))
                    .or_else(|| mock.default_handler.region_heartbeat(&req));
                match resp {
                    None => future::ok(None),
                    Some(Ok(resp)) => future::ok(Some((resp, WriteFlags::default()))),
                    Some(Err(e)) => future::err(box_err!("{:?}", e)),
                }
            });
            let mut sink = sink.sink_map_err(PdError::from);
            let _ = sink.send_all(&mut stream).await;
            let _ = sink.close().await;
        });
    }

    fn get_region(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetRegionRequest,
        sink: UnarySink<GetRegionResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_region(&req))
    }

    fn get_region_by_id(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetRegionByIdRequest,
        sink: UnarySink<GetRegionResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_region_by_id(&req))
    }

    fn ask_split(
        &mut self,
        ctx: RpcContext<'_>,
        req: AskSplitRequest,
        sink: UnarySink<AskSplitResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.ask_split(&req))
    }

    fn report_split(
        &mut self,
        _: RpcContext<'_>,
        _: ReportSplitRequest,
        _: UnarySink<ReportSplitResponse>,
    ) {
        unimplemented!()
    }

    fn ask_batch_split(
        &mut self,
        ctx: RpcContext<'_>,
        req: AskBatchSplitRequest,
        sink: UnarySink<AskBatchSplitResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.ask_batch_split(&req))
    }

    fn report_batch_split(
        &mut self,
        ctx: RpcContext<'_>,
        req: ReportBatchSplitRequest,
        sink: UnarySink<ReportBatchSplitResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.report_batch_split(&req))
    }

    fn get_cluster_config(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetClusterConfigRequest,
        sink: UnarySink<GetClusterConfigResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_cluster_config(&req))
    }

    fn put_cluster_config(
        &mut self,
        ctx: RpcContext<'_>,
        req: PutClusterConfigRequest,
        sink: UnarySink<PutClusterConfigResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.put_cluster_config(&req))
    }

    fn scatter_region(
        &mut self,
        ctx: RpcContext<'_>,
        req: ScatterRegionRequest,
        sink: UnarySink<ScatterRegionResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.scatter_region(&req))
    }

    fn get_prev_region(
        &mut self,
        _: RpcContext<'_>,
        _: GetRegionRequest,
        _: UnarySink<GetRegionResponse>,
    ) {
        unimplemented!()
    }

    fn get_gc_safe_point(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetGcSafePointRequest,
        sink: UnarySink<GetGcSafePointResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_gc_safe_point(&req))
    }

    fn update_gc_safe_point(
        &mut self,
        ctx: RpcContext<'_>,
        req: UpdateGcSafePointRequest,
        sink: UnarySink<UpdateGcSafePointResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.update_gc_safe_point(&req))
    }

    fn sync_regions(
        &mut self,
        _ctx: RpcContext<'_>,
        _stream: RequestStream<SyncRegionRequest>,
        _sink: DuplexSink<SyncRegionResponse>,
    ) {
        unimplemented!()
    }

    fn get_operator(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetOperatorRequest,
        sink: UnarySink<GetOperatorResponse>,
    ) {
        hijack_unary(self, ctx, sink, |c| c.get_operator(&req))
    }

    fn scan_regions(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: ScanRegionsRequest,
        _sink: UnarySink<ScanRegionsResponse>,
    ) {
        unimplemented!()
    }

    fn update_service_gc_safe_point(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: UpdateServiceGcSafePointRequest,
        _sink: UnarySink<UpdateServiceGcSafePointResponse>,
    ) {
        unimplemented!()
    }

    fn sync_max_ts(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: SyncMaxTsRequest,
        _sink: UnarySink<SyncMaxTsResponse>,
    ) {
        unimplemented!()
    }

    fn split_regions(
        &mut self,
        _: RpcContext<'_>,
        _: kvproto::pdpb::SplitRegionsRequest,
        _: UnarySink<kvproto::pdpb::SplitRegionsResponse>,
    ) {
        unimplemented!()
    }

    fn get_dc_location_info(
        &mut self,
        _: RpcContext<'_>,
        _: kvproto::pdpb::GetDcLocationInfoRequest,
        _: UnarySink<kvproto::pdpb::GetDcLocationInfoResponse>,
    ) {
        unimplemented!()
    }
}

impl<C: PdMocker + Send + Sync + 'static> resource_manager::ResourceManager for PdMock<C> {
    fn acquire_token_buckets(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        stream: grpcio::RequestStream<resource_manager::TokenBucketsRequest>,
        sink: grpcio::DuplexSink<resource_manager::TokenBucketsResponse>,
    ) {
        let mock = self.clone();
        ctx.spawn(async move {
            let mut stream = stream.map_err(PdError::from).try_filter_map(move |req| {
                let resp = mock
                    .case
                    .as_ref()
                    .and_then(|case| case.report_ru_metrics(&req))
                    .or_else(|| mock.default_handler.report_ru_metrics(&req));
                match resp {
                    None => future::ok(None),
                    Some(Ok(resp)) => future::ok(Some((resp, WriteFlags::default()))),
                    Some(Err(e)) => future::err(box_err!("{:?}", e)),
                }
            });
            let mut sink = sink.sink_map_err(PdError::from);
            let _ = sink.send_all(&mut stream).await;
            let _ = sink.close().await;
        });
    }
}
