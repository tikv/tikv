// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use futures::future::FutureExt;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode};
use kvproto::{logbackuppb::*, metapb::Region};
use tikv_util::{info, warn, worker::Scheduler};

use crate::{
    checkpoint_manager::{GetCheckpointResult, RegionIdWithVersion},
    endpoint::{RegionCheckpointOperation, RegionSet},
    router::TaskSelector,
    try_send, Task,
};

#[derive(Clone)]
pub struct BackupStreamGrpcService {
    endpoint: Scheduler<Task>,
}

impl BackupStreamGrpcService {
    pub fn new(endpoint: Scheduler<Task>) -> Self {
        Self { endpoint }
    }
}

fn id_of(region: &Region) -> RegionIdentity {
    let mut id = RegionIdentity::new();
    id.set_id(region.get_id());
    id.set_epoch_version(region.get_region_epoch().get_version());
    id
}

impl From<RegionIdWithVersion> for RegionIdentity {
    fn from(val: RegionIdWithVersion) -> Self {
        let mut id = RegionIdentity::new();
        id.set_id(val.region_id);
        id.set_epoch_version(val.region_epoch_version);
        id
    }
}

impl LogBackup for BackupStreamGrpcService {
    fn flush_now(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        _req: FlushNowRequest,
        sink: grpcio::UnarySink<FlushNowResponse>,
    ) {
        info!("Client requests force flush."; "cli" => %ctx.peer());
        let mut resp = FlushNowResponse::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let task = Task::ForceFlush(TaskSelector::All, tx);
        if let Err(err) = self.endpoint.schedule(task) {
            ctx.spawn(
                sink.fail(RpcStatus::with_message(
                    RpcStatusCode::INTERNAL,
                    format!(
                        "failed to schedule the command, maybe busy or shutting down: {}",
                        err
                    ),
                ))
                .map(|res| {
                    if let Err(err) = res {
                        warn!("flush_now: failed to send an error response to client"; "err" => %err)
                    }
                }),
            );
            return;
        };

        ctx.spawn(async move {
            while let Some(item) = rx.recv().await {
                let mut res = FlushResult::new();
                res.set_success(item.error.is_none());
                if let Some(err) = item.error {
                    res.set_error_message(err.to_string());
                }
                res.set_task_name(item.task);
                resp.results.push(res);
            }

            if let Err(err) = sink.success(resp.clone()).await {
                warn!("flush_now: failed to send success response to client"; "err" => %err, "resp" => ?resp);
            }
        })
    }

    fn get_last_flush_ts_of_region(
        &mut self,
        _ctx: RpcContext<'_>,
        mut req: GetLastFlushTsOfRegionRequest,
        sink: grpcio::UnarySink<GetLastFlushTsOfRegionResponse>,
    ) {
        let regions = req
            .take_regions()
            .into_iter()
            .map(|id| (id.id, id.epoch_version))
            .collect::<HashSet<_>>();
        let t = Task::RegionCheckpointsOp(RegionCheckpointOperation::Get(
            RegionSet::Regions(regions),
            Box::new(move |rs| {
                let mut resp = GetLastFlushTsOfRegionResponse::new();
                resp.set_checkpoints(
                    rs.into_iter()
                        .map(|r| match r {
                            GetCheckpointResult::Ok { region, checkpoint } => {
                                let mut r = RegionCheckpoint::new();
                                let id = id_of(&region);
                                r.set_region(id);
                                r.set_checkpoint(checkpoint.into_inner());
                                r
                            }
                            GetCheckpointResult::NotFound { id, err } => {
                                let mut r = RegionCheckpoint::new();
                                r.set_region(id.into());
                                r.set_err(err);
                                r
                            }
                            GetCheckpointResult::EpochNotMatch { region, err } => {
                                let mut r = RegionCheckpoint::new();
                                r.set_region(id_of(&region));
                                r.set_err(err);
                                r
                            }
                        })
                        .collect(),
                );
                tokio::spawn(async {
                    if let Err(e) = sink.success(resp).await {
                        warn!("failed to reply grpc resonse."; "err" => %e)
                    }
                });
            }),
        ));
        try_send!(self.endpoint, t);
    }

    fn subscribe_flush_event(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: SubscribeFlushEventRequest,
        #[allow(unused_variables)] sink: grpcio::ServerStreamingSink<SubscribeFlushEventResponse>,
    ) {
        #[cfg(test)]
        panic!("Service should not be used in an unit test");
        #[cfg(not(test))]
        try_send!(
            self.endpoint,
            Task::RegionCheckpointsOp(RegionCheckpointOperation::Subscribe(sink))
        );
    }
}
