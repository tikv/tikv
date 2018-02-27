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

use grpc::{Error as GrpcError, RpcContext, ServerStreamingSink, UnarySink, WriteFlags};
use futures::{future, stream, Future, Stream};
use futures_cpupool::{Builder, CpuPool};
use kvproto::debugpb_grpc;
use kvproto::debugpb::*;
use fail;

use raftstore::store::Engines;
use server::debug::{Debugger, Error, Result};

#[derive(Clone)]
pub struct Service {
    cluster_id: u64,
    pool: CpuPool,
    debugger: Debugger,
}

impl Service {
    pub fn new(cluster_id: u64, engines: Engines) -> Service {
        let pool = Builder::new()
            .name_prefix(thd_name!("debugger"))
            .pool_size(1)
            .create();
        let debugger = Debugger::new(engines);
        Service {
            cluster_id,
            pool,
            debugger,
        }
    }

    fn on_grpc_error(tag: &'static str, e: &GrpcError) {
        error!("{} failed: {:?}", tag, e);
    }

    fn error_to_grpc_error(tag: &'static str, e: Error) -> GrpcError {
        let e = GrpcError::RpcFailure(e.into());
        Service::on_grpc_error(tag, &e);
        e
    }
}

impl debugpb_grpc::Debug for Service {
    fn get(&self, ctx: RpcContext, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        const TAG: &str = "debug_get";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

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

        send_response!(ctx, sink, f, Into::into, TAG);
    }

    fn raft_log(&self, ctx: RpcContext, req: RaftLogRequest, sink: UnarySink<RaftLogResponse>) {
        const TAG: &str = "debug_raft_log";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

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

        send_response!(ctx, sink, f, Into::into, TAG);
    }

    fn region_info(
        &self,
        ctx: RpcContext,
        req: RegionInfoRequest,
        sink: UnarySink<RegionInfoResponse>,
    ) {
        const TAG: &str = "debug_region_log";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

        let region_id = req.get_region_id();

        let f = self.pool
            .spawn(
                future::ok(self.debugger.clone())
                    .and_then(move |debugger| debugger.region_info(region_id)),
            )
            .map(|region_info| {
                let mut resp = RegionInfoResponse::new();
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

        send_response!(ctx, sink, f, Into::into, TAG);
    }

    fn region_size(
        &self,
        ctx: RpcContext,
        mut req: RegionSizeRequest,
        sink: UnarySink<RegionSizeResponse>,
    ) {
        const TAG: &str = "debug_region_size";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

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

        send_response!(ctx, sink, f, Into::into, TAG);
    }

    fn scan_mvcc(
        &self,
        ctx: RpcContext,
        mut req: ScanMvccRequest,
        sink: ServerStreamingSink<ScanMvccResponse>,
    ) {
        const TAG: &str = "debug_scan_mvcc";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

        let debugger = self.debugger.clone();
        let from = req.take_from_key();
        let to = req.take_to_key();
        let limit = req.get_limit();
        let future = future::result(debugger.scan_mvcc(&from, &to, limit))
            .map_err(|e| Service::error_to_grpc_error("scan_mvcc", e))
            .and_then(|iter| {
                stream::iter_result(iter)
                    .map_err(|e| Service::error_to_grpc_error("scan_mvcc", e))
                    .map(|(key, mvcc_info)| {
                        let mut resp = ScanMvccResponse::new();
                        resp.set_key(key);
                        resp.set_info(mvcc_info);
                        (resp, WriteFlags::default())
                    })
                    .forward(sink)
                    .map(|_| ())
            })
            .map_err(|e| Service::on_grpc_error("scan_mvcc", &e));
        self.pool.spawn(future).forget();
    }

    fn compact(&self, ctx: RpcContext, req: CompactRequest, sink: UnarySink<CompactResponse>) {
        const TAG: &str = "debug_compact";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

        let debugger = self.debugger.clone();
        let f = self.pool.spawn_fn(move || {
            debugger
                .compact(
                    req.get_db(),
                    req.get_cf(),
                    req.get_from_key(),
                    req.get_to_key(),
                )
                .map(|_| CompactResponse::default())
        });
        send_response!(ctx, sink, f, Into::into, "debug_compact");
    }

    fn inject_fail_point(
        &self,
        ctx: RpcContext,
        mut req: InjectFailPointRequest,
        sink: UnarySink<InjectFailPointResponse>,
    ) {
        const TAG: &str = "debug_inject_fail_point";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

        let f = self.pool.spawn_fn(move || {
            let name = req.take_name();
            if name.is_empty() {
                return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
            }
            let actions = req.get_actions();
            if let Err(e) = fail::cfg(name, actions) {
                return Err(box_err!("{:?}", e));
            }
            Ok(InjectFailPointResponse::new())
        });

        send_response!(ctx, sink, f, Into::into, TAG);
    }

    fn recover_fail_point(
        &self,
        ctx: RpcContext,
        mut req: RecoverFailPointRequest,
        sink: UnarySink<RecoverFailPointResponse>,
    ) {
        const TAG: &str = "debug_recover_fail_point";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

        let f = self.pool.spawn_fn(move || {
            let name = req.take_name();
            if name.is_empty() {
                return Err(Error::InvalidArgument("Failure Type INVALID".to_owned()));
            }
            fail::remove(name);
            Ok(RecoverFailPointResponse::new())
        });

        send_response!(ctx, sink, f, Into::into, TAG);
    }

    fn list_fail_points(
        &self,
        ctx: RpcContext,
        _: ListFailPointsRequest,
        sink: UnarySink<ListFailPointsResponse>,
    ) {
        const TAG: &str = "debug_list_fail_points";
        try_check_header!(ctx, sink, self.cluster_id, TAG);

        let f = self.pool.spawn_fn(move || {
            let list = fail::list().into_iter().map(|(name, actions)| {
                let mut entry = ListFailPointsResponse_Entry::new();
                entry.set_name(name);
                entry.set_actions(actions);
                entry
            });
            let mut resp = ListFailPointsResponse::new();
            resp.set_entries(list.collect());
            Ok(resp) as Result<_>
        });

        send_response!(ctx, sink, f, Into::into, TAG);
    }
}
