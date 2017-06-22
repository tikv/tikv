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
use std::net::ToSocketAddrs;

use futures::Future;
use grpc::{Server as GrpcServer, ServerBuilder, RpcContext, UnarySink, RequestStream, DuplexSink,
           Environment, RpcStatus, RpcStatusCode};

use kvproto::pdpb::*;
use kvproto::pdpb_grpc::{self, Pd};

use super::mocker::Mocker;
use super::mocker::Service;
use super::mocker::Result;

pub struct Server {
    server: GrpcServer,
}

impl Server {
    pub fn run<C>(eps_count: usize, handler: Arc<Service>, case: Option<Arc<C>>) -> Server
        where C: Mocker + Send + Sync + 'static
    {
        let eps = vec![("127.0.0.1", 0).to_socket_addrs().unwrap().next().unwrap(); eps_count];
        Server::run_with_eps(eps.as_slice(), handler, case)
    }

    pub fn run_with_eps<A, C>(eps: A, handler: Arc<Service>, case: Option<Arc<C>>) -> Server
        where A: ToSocketAddrs,
              C: Mocker + Send + Sync + 'static
    {
        let m = Mock {
            handler: handler.clone(),
            case: case.clone(),
        };
        let service = pdpb_grpc::create_pd(m);
        let env = Arc::new(Environment::new(1));
        let mut sb = ServerBuilder::new(env).register_service(service);
        for ep in eps.to_socket_addrs().unwrap() {
            sb = sb.bind(format!("{}", ep.ip()), ep.port());
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

fn hijack_unary<F, R, C: Mocker>(mock: &Mock<C>, ctx: RpcContext, sink: UnarySink<R>, f: F)
    where R: Send + 'static,
          F: Fn(&Mocker) -> Option<Result<R>>
{
    let resp = mock.case.as_ref().and_then(|case| f(case.as_ref())).or_else(|| f(mock.handler.as_ref()));

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
struct Mock<C: Mocker> {
    handler: Arc<Service>,
    case: Option<Arc<C>>,
}

impl<C: Mocker> Clone for Mock<C> {
    fn clone(&self) -> Self {
        Mock {
            handler: self.handler.clone(),
            case: self.case.clone(),
        }
    }
}

impl<C: Mocker> Pd for Mock<C> {
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
                        req: RegionHeartbeatRequest,
                        sink: UnarySink<RegionHeartbeatResponse>) {
        hijack_unary(self, ctx, sink, |c| c.region_heartbeat(&req))
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
