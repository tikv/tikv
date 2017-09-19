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

use grpc::{RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink};
use futures::{future, Future};
use futures_cpupool::{Builder, CpuPool};
use kvproto::debugpb_grpc;
use kvproto::debugpb::*;

use raftstore::store::Engines;
use raftstore::store::debug::*;

#[derive(Clone)]
pub struct Service {
    pool: CpuPool,
    debugger: Debug,
}

impl Service {
    pub fn new(engines: Engines) -> Service {
        let pool = Builder::new()
            .name_prefix(thd_name!("debugger"))
            .pool_size(1)
            .create();
        let debugger = Debug::new(engines);
        Service { pool, debugger }
    }

    fn handle_response<F, P, M, Q>(
        &self,
        ctx: RpcContext,
        sink: UnarySink<Q>,
        resp: F,
        map: M,
        tag: &'static str,
    ) where
        P: Send + 'static,
        Q: 'static,
        M: FnOnce(P) -> Q + Send + 'static,
        F: Future<Item = P, Error = Error> + Send + 'static,
    {
        let on_error = move |e| {
            error!("{} failed: {:?}", tag, e);
        };
        let f = self.pool.spawn(resp).then(|v| match v {
            Ok(resp) => sink.success(map(resp)).map_err(on_error),
            Err(Error::NotFound(msg)) => {
                let status = RpcStatus::new(RpcStatusCode::NotFound, Some(msg));
                sink.fail(status).map_err(on_error)
            }
            Err(Error::InvalidArgument(msg)) => {
                let status = RpcStatus::new(RpcStatusCode::InvalidArgument, Some(msg));
                sink.fail(status).map_err(on_error)
            }
            Err(Error::Other(e)) => {
                let status = RpcStatus::new(RpcStatusCode::Unknown, Some(format!("{:?}", e)));
                sink.fail(status).map_err(on_error)
            }
        });
        ctx.spawn(f);
    }
}

impl debugpb_grpc::Debug for Service {
    fn get(&self, ctx: RpcContext, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        const TAG: &'static str = "debug_get";

        let cf = req.get_cf();
        let key_encoded = req.take_key_encoded();
        let f = future::ok(self.debugger.clone())
            .and_then(move |debugger| debugger.get(cf, key_encoded.as_slice()));

        let map = |value| {
            let mut resp = GetResponse::new();
            resp.set_value(value);
            resp
        };
        self.handle_response(ctx, sink, f, map, TAG);
    }

    fn mvcc(&self, _: RpcContext, _: MvccRequest, _: UnarySink<MvccResponse>) {
        unimplemented!()
    }

    fn raft_log(&self, _: RpcContext, _: RaftLogRequest, _: UnarySink<RaftLogResponse>) {
        unimplemented!()
    }

    fn region_info(&self, _: RpcContext, _: RegionInfoRequest, _: UnarySink<RegionInfoResponse>) {
        unimplemented!()
    }

    fn size(&self, _: RpcContext, _: SizeRequest, _: UnarySink<SizeResponse>) {
        unimplemented!()
    }

    fn scan(&self, _: RpcContext, _: ScanRequest, _: ServerStreamingSink<ScanResponse>) {
        unimplemented!()
    }
}
