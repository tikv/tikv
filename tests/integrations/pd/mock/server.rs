// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::{Future, Sink, Stream};
use grpcio::{
    DuplexSink, EnvBuilder, RequestStream, RpcContext, RpcStatus, RpcStatusCode,
    Server as GrpcServer, ServerBuilder, UnarySink, WriteFlags,
};
use pd_client::Error as PdError;
use tikv_util::security::*;

use kvproto::pdpb::*;

use super::mocker::*;

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
            case: case.clone(),
        };
        let mut server = Server {
            server: None,
            mocker,
        };
        server.start(mgr, eps);
        server
    }

    pub fn start(&mut self, mgr: &SecurityManager, eps: Vec<(String, u16)>) {
        let service = create_pd(self.mocker.clone());
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)
                .name_prefix(thd_name!("mock-server"))
                .build(),
        );
        let mut sb = ServerBuilder::new(env).register_service(service);
        for (host, port) in eps {
            sb = mgr.bind(sb, &host, port);
        }

        let mut server = sb.build().unwrap();
        {
            let addrs: Vec<String> = server
                .bind_addrs()
                .iter()
                .map(|addr| format!("{}:{}", addr.0, addr.1))
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
        self.server.take();
    }

    pub fn bind_addrs(&self) -> Vec<(String, u16)> {
        self.server.as_ref().unwrap().bind_addrs().to_vec()
    }
}

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
                .map_err(move |err| error!("failed to reply: {:?}", err)),
        ),
        Some(Err(err)) => {
            let status = RpcStatus::new(RpcStatusCode::UNKNOWN, Some(format!("{:?}", err)));
            ctx.spawn(
                sink.fail(status)
                    .map_err(move |err| error!("failed to reply: {:?}", err)),
            );
        }
        _ => {
            let status = RpcStatus::new(
                RpcStatusCode::UNIMPLEMENTED,
                Some("Unimplemented".to_owned()),
            );
            ctx.spawn(
                sink.fail(status)
                    .map_err(move |err| error!("failed to reply: {:?}", err)),
            );
        }
    }
}

#[derive(Debug)]
struct PdMock<C: PdMocker> {
    default_handler: Arc<Service>,
    case: Option<Arc<C>>,
}

impl<C: PdMocker> Clone for PdMock<C> {
    fn clone(&self) -> Self {
        PdMock {
            default_handler: Arc::clone(&self.default_handler),
            case: self.case.clone(),
        }
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

    fn tso(&mut self, _: RpcContext<'_>, _: RequestStream<TsoRequest>, _: DuplexSink<TsoResponse>) {
        unimplemented!()
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

    fn region_heartbeat(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<RegionHeartbeatRequest>,
        sink: DuplexSink<RegionHeartbeatResponse>,
    ) {
        let mock = self.clone();
        let f = sink
            .sink_map_err(PdError::from)
            .send_all(
                stream
                    .map_err(PdError::from)
                    .and_then(move |req| {
                        let resp = mock
                            .case
                            .as_ref()
                            .and_then(|case| case.region_heartbeat(&req))
                            .or_else(|| mock.default_handler.region_heartbeat(&req));
                        match resp {
                            None => Ok(None),
                            Some(Ok(resp)) => Ok(Some((resp, WriteFlags::default()))),
                            Some(Err(e)) => Err(box_err!("{:?}", e)),
                        }
                    })
                    .filter_map(|o| o),
            )
            .map(|_| ())
            .map_err(|e| error!("failed to handle heartbeat: {:?}", e));
        ctx.spawn(f)
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

    fn ask_split(&mut self, _: RpcContext<'_>, _: AskSplitRequest, _: UnarySink<AskSplitResponse>) {
        unimplemented!()
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
        _ctx: RpcContext<'_>,
        _stream: GetOperatorRequest,
        _sink: UnarySink<GetOperatorResponse>,
    ) {
        unimplemented!()
    }

    fn scan_regions(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: ScanRegionsRequest,
        _sink: UnarySink<ScanRegionsResponse>,
    ) {
        unimplemented!()
    }
}
