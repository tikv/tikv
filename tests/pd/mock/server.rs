// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use futures::{Future, Stream, Sink};
use grpc::{Server as GrpcServer, ServerBuilder, RpcContext, UnarySink, RequestStream, DuplexSink,
           Environment, RpcStatus, RpcStatusCode, WriteFlags};
use tikv::pd::Error as PdError;

use kvproto::pdpb::*;
use kvproto::pdpb_grpc::{self, Pd};

use super::mocker::*;

pub struct Server {
    server: GrpcServer,
}

impl Server {
    pub fn run<C>(eps_count: usize, handler: Arc<Service>, case: Option<Arc<C>>) -> Server
        where C: PdMocker + Send + Sync + 'static
    {
        let eps = vec![("127.0.0.1".to_owned(), 0); eps_count];
        Server::run_with_eps(eps, handler, case)
    }

    pub fn run_with_eps<C>(eps: Vec<(String, u16)>,
                           handler: Arc<Service>,
                           case: Option<Arc<C>>)
                           -> Server
        where C: PdMocker + Send + Sync + 'static
    {
        let m = PdMock {
            default_handler: handler.clone(),
            case: case.clone(),
        };
        let service = pdpb_grpc::create_pd(m);
        let env = Arc::new(Environment::new(1));
        let mut sb = ServerBuilder::new(env).register_service(service);
        for (host, port) in eps {
            sb = sb.bind(host, port);
        }

        let mut server = sb.build().unwrap();
        {
            let addrs = server.bind_addrs();
            handler.set_endpoints(addrs.iter()
                .map(|addr| format!("http://{}:{}", addr.0, addr.1))
                .collect());
            if let Some(case) = case.as_ref() {
                case.set_endpoints(addrs.iter()
                    .map(|addr| format!("http://{}:{}", addr.0, addr.1))
                    .collect());
            }
        }

        server.start();
        Server { server: server }
    }

    pub fn bind_addrs(&self) -> Vec<(String, u16)> {
        self.server.bind_addrs().to_vec()
    }
}

fn hijack_unary<F, R, C: PdMocker>(mock: &PdMock<C>, ctx: RpcContext, sink: UnarySink<R>, f: F)
    where R: Send + 'static,
          F: Fn(&PdMocker) -> Option<Result<R>>
{
    let resp =
        mock.case.as_ref().and_then(|case| f(case.as_ref())).or_else(|| f(mock.default_handler.as_ref()));

    match resp {
        Some(Ok(resp)) => {
            ctx.spawn(sink.success(resp).map_err(move |err| panic!("failed to reply: {:?}", err)))
        }
        Some(Err(err)) => {
            let status = RpcStatus::new(RpcStatusCode::Unknown, Some(format!("{:?}", err)));
            ctx.spawn(sink.fail(status).map_err(move |err| panic!("failed to reply: {:?}", err)));
        }
        _ => {
            let status = RpcStatus::new(RpcStatusCode::Unimplemented,
                                        Some("Unimplemented".to_owned()));
            ctx.spawn(sink.fail(status).map_err(move |err| panic!("failed to reply: {:?}", err)));
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
            default_handler: self.default_handler.clone(),
            case: self.case.clone(),
        }
    }
}

impl<C: PdMocker + Send + Sync + 'static> Pd for PdMock<C> {
    fn get_members(&self,
                   ctx: RpcContext,
                   req: GetMembersRequest,
                   sink: UnarySink<GetMembersResponse>) {
        hijack_unary(self, ctx, sink, |c| c.get_members(&req))
    }

    fn tso(&self, _: RpcContext, _: RequestStream<TsoRequest>, _: DuplexSink<TsoResponse>) {
        unimplemented!()
    }

    fn bootstrap(&self,
                 ctx: RpcContext,
                 req: BootstrapRequest,
                 sink: UnarySink<BootstrapResponse>) {
        hijack_unary(self, ctx, sink, |c| c.bootstrap(&req))
    }

    fn is_bootstrapped(&self,
                       ctx: RpcContext,
                       req: IsBootstrappedRequest,
                       sink: UnarySink<IsBootstrappedResponse>) {
        hijack_unary(self, ctx, sink, |c| c.is_bootstrapped(&req))
    }

    fn alloc_id(&self, ctx: RpcContext, req: AllocIDRequest, sink: UnarySink<AllocIDResponse>) {
        hijack_unary(self, ctx, sink, |c| c.alloc_id(&req))
    }

    fn get_store(&self, ctx: RpcContext, req: GetStoreRequest, sink: UnarySink<GetStoreResponse>) {
        hijack_unary(self, ctx, sink, |c| c.get_store(&req))
    }

    fn put_store(&self, ctx: RpcContext, req: PutStoreRequest, sink: UnarySink<PutStoreResponse>) {
        hijack_unary(self, ctx, sink, |c| c.put_store(&req))
    }

    fn store_heartbeat(&self,
                       ctx: RpcContext,
                       req: StoreHeartbeatRequest,
                       sink: UnarySink<StoreHeartbeatResponse>) {
        hijack_unary(self, ctx, sink, |c| c.store_heartbeat(&req))
    }

    fn region_heartbeat(&self,
                        ctx: RpcContext,
                        stream: RequestStream<RegionHeartbeatRequest>,
                        sink: DuplexSink<RegionHeartbeatResponse>) {
        let mock = self.clone();
        let f = sink.sink_map_err(PdError::from).send_all(stream.map_err(PdError::from).and_then(move |req| {
            match mock.case.as_ref().map_or_else(|| mock.default_handler.region_heartbeat(&req),
            |s| s.region_heartbeat(&req)) {
                None => Ok(None),
                Some(Ok(resp)) => Ok(Some((resp, WriteFlags::default()))),
                Some(Err(e)) => Err(box_err!("{:?}", e)),
            }
        }).filter_map(|o| o)).map(|_| ()).map_err(|e| error!("failed to handle heartbeat: {:?}", e));
        ctx.spawn(f)
    }

    fn get_region(&self,
                  ctx: RpcContext,
                  req: GetRegionRequest,
                  sink: UnarySink<GetRegionResponse>) {
        hijack_unary(self, ctx, sink, |c| c.get_region(&req))
    }

    fn get_region_by_id(&self,
                        ctx: RpcContext,
                        req: GetRegionByIDRequest,
                        sink: UnarySink<GetRegionResponse>) {
        hijack_unary(self, ctx, sink, |c| c.get_region_by_id(&req))
    }

    fn ask_split(&self, ctx: RpcContext, req: AskSplitRequest, sink: UnarySink<AskSplitResponse>) {
        hijack_unary(self, ctx, sink, |c| c.ask_split(&req))
    }

    fn report_split(&self,
                    ctx: RpcContext,
                    req: ReportSplitRequest,
                    sink: UnarySink<ReportSplitResponse>) {
        hijack_unary(self, ctx, sink, |c| c.report_split(&req))
    }

    fn get_cluster_config(&self,
                          ctx: RpcContext,
                          req: GetClusterConfigRequest,
                          sink: UnarySink<GetClusterConfigResponse>) {
        hijack_unary(self, ctx, sink, |c| c.get_cluster_config(&req))
    }

    fn put_cluster_config(&self,
                          ctx: RpcContext,
                          req: PutClusterConfigRequest,
                          sink: UnarySink<PutClusterConfigResponse>) {
        hijack_unary(self, ctx, sink, |c| c.put_cluster_config(&req))
    }
}
