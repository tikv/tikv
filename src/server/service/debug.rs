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

use grpc::{Error as GrpcError, WriteFlags};
use grpc::{RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink};
use futures::{future, stream, Future, Sink};
use futures_cpupool::{Builder, CpuPool};
use kvproto::debugpb_grpc;
use kvproto::debugpb::*;

use raftstore::store::Engines;
use raftstore::store::debug::{Debugger, Error};

#[derive(Clone)]
pub struct Service {
    pool: CpuPool,
    debugger: Debugger,
}

impl Service {
    pub fn new(engines: Engines) -> Service {
        let pool = Builder::new()
            .name_prefix(thd_name!("debugger"))
            .pool_size(1)
            .create();
        let debugger = Debugger::new(engines);
        Service { pool, debugger }
    }

    fn handle_response<F, P>(&self, ctx: RpcContext, sink: UnarySink<P>, resp: F, tag: &'static str)
    where
        P: Send + 'static,
        F: Future<Item = P, Error = Error> + Send + 'static,
    {
        let f = resp.then(|v| match v {
            Ok(resp) => sink.success(resp),
            Err(e) => {
                let status = Service::error_to_status(e);
                sink.fail(status)
            }
        });
        ctx.spawn(f.map_err(move |e| Service::on_grpc_error(tag, e)));
    }

    fn error_to_status(e: Error) -> RpcStatus {
        let (code, msg) = match e {
            Error::NotFound(msg) => (RpcStatusCode::NotFound, Some(msg)),
            Error::InvalidArgument(msg) => (RpcStatusCode::InvalidArgument, Some(msg)),
            Error::Other(e) => (RpcStatusCode::Unknown, Some(format!("{:?}", e))),
        };
        RpcStatus::new(code, msg)
    }

    fn on_grpc_error(tag: &'static str, e: GrpcError) {
        error!("{} failed: {:?}", tag, e);
    }
}

impl debugpb_grpc::Debug for Service {
    fn get(&self, ctx: RpcContext, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        const TAG: &'static str = "debug_get";

        let db = req.get_db();
        let cf = req.take_cf();
        let key = req.take_key();

        let f = self.pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.get(db, &cf, key.as_slice())),
            )
            .map(|value| {
                let mut resp = GetResponse::new();
                resp.set_value(value);
                resp
            });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn raft_log(&self, ctx: RpcContext, req: RaftLogRequest, sink: UnarySink<RaftLogResponse>) {
        const TAG: &'static str = "debug_raft_log";

        let region_id = req.get_region_id();
        let log_index = req.get_log_index();

        let f = self.pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.raft_log(region_id, log_index)),
            )
            .map(|entry| {
                let mut resp = RaftLogResponse::new();
                resp.set_entry(entry);
                resp
            });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn region_info(
        &self,
        ctx: RpcContext,
        req: RegionInfoRequest,
        sink: UnarySink<RegionInfoResponse>,
    ) {
        const TAG: &'static str = "debug_region_log";

        let region_id = req.get_region_id();

        let f = self.pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.region_info(region_id)),
            )
            .map(|(raft_local_state, raft_apply_state, region_state)| {
                let mut resp = RegionInfoResponse::new();
                if let Some(raft_local_state) = raft_local_state {
                    resp.set_raft_local_state(raft_local_state);
                }
                if let Some(raft_apply_state) = raft_apply_state {
                    resp.set_raft_apply_state(raft_apply_state);
                }
                if let Some(region_state) = region_state {
                    resp.set_region_local_state(region_state);
                }
                resp
            });

        self.handle_response(ctx, sink, f, TAG);
    }

    fn region_size(
        &self,
        ctx: RpcContext,
        mut req: RegionSizeRequest,
        sink: UnarySink<RegionSizeResponse>,
    ) {
        const TAG: &'static str = "debug_region_size";

        let region_id = req.get_region_id();
        let cfs = req.take_cfs().into_vec();

        let f = self.pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.region_size(region_id, cfs)),
            )
            .map(|entries| {
                let mut resp = RegionSizeResponse::new();
                resp.set_entries(
                    entries
                        .into_iter()
                        .map(|(cf, size)| {
                            let mut entry = RegionSizeResponse_Entry::new();
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
        &self,
        _: RpcContext,
        req: ScanMvccRequest,
        sink: ServerStreamingSink<ScanMvccResponse>,
    ) {
        let debugger = self.debugger.clone();
        stream::iter(debugger.scan_mvcc(req))


            let (key_and_mvccs, mut sink) = ok_or_close_sink!(debugger.scan_mvcc(req), sink);
        macro_rules! ok_or_close_sink {
            ($result: expr, $sink: expr) => {
                match $result {
                    Ok(t) => (t, $sink),
                    Err(e) => {
                        let status = Service::error_to_status(e);
                        return $sink.fail(status)
                            .map_err(|e| Service::on_grpc_error("scan_mvcc", e))
                            .wait();
                    }
                }
            }
        }

        let deal_future = move || -> Result<(), ()> {
            let (key_and_mvccs, mut sink) = ok_or_close_sink!(debugger.scan_mvcc(req), sink);
            for key_and_mvcc in key_and_mvccs {
                let ((key, mvcc), s) = ok_or_close_sink!(key_and_mvcc, sink);
                let mut resp = ScanMvccResponse::new();
                resp.set_key(key);
                resp.set_info(mvcc);
                match s.send((resp, WriteFlags::default())).wait() {
                    Ok(s) => sink = s,
                    Err(e) => {
                        Service::on_grpc_error("scan_mvcc", e);
                        return Ok(());
                    }
                }
            }
            future::poll_fn(move || sink.close())
                .map_err(|e| Service::on_grpc_error("scan_mvcc", e))
                .wait()
        };
        self.pool.spawn_fn(deal_future).forget();
    }
}
